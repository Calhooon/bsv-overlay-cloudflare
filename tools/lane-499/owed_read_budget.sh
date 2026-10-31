#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
cd "$root"
: "${TMPDIR:?external scratch directory required}"
export npm_config_cache="${TMPDIR%/}/d2-owed-read-npm-cache"
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$HOME/bsv/targets/D2-OWED-READ}
out_root=${TMPDIR%/}
if [ -d /Volumes/Lanes/out/bsv-low ]; then out_root=/Volumes/Lanes/out/bsv-low/D2-OWED-READ; fi
out=${OWED_READ_OUT:-$out_root/d1-budget-$(date -u +%Y%m%dT%H%M%SZ)}
mkdir -p "$out"
export WRANGLER_LOG_PATH="$out/wrangler-logs"
port=${LANE_BASE:-48760}
inspector=$((port+1))
probe=$((port+2))
if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1 || lsof -nP -iTCP:"$inspector" -sTCP:LISTEN >/dev/null 2>&1 || lsof -nP -iTCP:"$probe" -sTCP:LISTEN >/dev/null 2>&1 || lsof -nP -iTCP:"$((port+3))" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "owed read budget: a requested port is already bound" >&2
  exit 1
fi
state="$out/state"
mkdir -p "$state"
export OWED_SEED_TIME=$(date -u +%s)
build=${OWED_READ_BUILD:-$CARGO_TARGET_DIR/owed-read-worker}
if [ -z "${OWED_READ_BUILD:-}" ]; then
  worker-build crates/low-app-layer --release --out-dir "$build" > "$out/build.log" 2>&1
fi
cat > "$out/worker.mjs" <<EOF
import worker from '$build/worker/shim.mjs'
export default {
  ...worker,
  async fetch(request, env, ctx) {
    let backgroundTasks = 0
    const context = new Proxy(ctx, { get(target, key) {
      if (key === 'waitUntil') return task => { backgroundTasks++; target.waitUntil(task) }
      const value = Reflect.get(target, key, target)
      return typeof value === 'function' ? value.bind(target) : value
    } })
    const entry = typeof worker === 'function' ? new worker(context, env) : worker
    const response = await entry.fetch(request, env, context)
    const headers = new Headers(response.headers)
    headers.set('x-owed-background-tasks', String(backgroundTasks))
    return new Response(response.body, { status: response.status, statusText: response.statusText, headers })
  }
}
EOF
cat > "$out/wrangler.toml" <<EOF
name = "owed-read-budget"
main = "$out/worker.mjs"
compatibility_date = "2024-12-01"
[vars]
AUTH_ENFORCE = "false"
SESSION_LANE = "false"
COURIER_SINK = "http://127.0.0.1:9"
OWED_FLEET_COMPARE_PER_TICK = "0"
[[d1_databases]]
binding = "OVERLAY_DB"
database_name = "low-overlay-db"
database_id = "00000000-0000-0000-0000-000000000000"
EOF
if [ "${OWED_READ_REUSE_STATE:-0}" != 1 ]; then
  cargo run --quiet --manifest-path workers/Cargo.toml -p low-app-layer --example owed_read_seed > "$out/seed.sql"
  # Judged facts are larger than the legacy row fixtures. Keep each local D1
  # seed request below its SQL-text limit as well as its statement limit.
  python3 scripts/d1-load-chunked.py "$out" low-overlay-db "$state" "$out/seed.sql" 80 > "$out/seed.log" 2>&1
fi
jobs=()
kill_tree() {
  local child
  for child in $(pgrep -P "$1" 2>/dev/null); do kill_tree "$child"; done
  kill "$1" 2>/dev/null || true
}
cleanup() { for pid in "${jobs[@]}"; do kill_tree "$pid"; done; }
trap cleanup EXIT
cat > "$out/probe.mjs" <<'JS'
export default { async fetch(request, env) {
  if (request.method !== 'POST') return new Response('ready')
  const { statements, identity } = await request.json()
  if (!statements.every(sql => sql.startsWith('SELECT '))) return new Response('read only', { status: 400 })
  const rows = await env.OVERLAY_DB.batch(statements.map(sql => env.OVERLAY_DB.prepare(sql).bind(identity)))
  return Response.json(rows.map(row => ({ meta: row.meta, rows: row.results })))
} }
JS
cat > "$out/probe.toml" <<EOF
name = "owed-read-metadata"
main = "$out/probe.mjs"
compatibility_date = "2024-12-01"
[[d1_databases]]
binding = "OVERLAY_DB"
database_name = "low-overlay-db"
database_id = "00000000-0000-0000-0000-000000000000"
EOF
(cd "$out" && exec env WRANGLER_SEND_METRICS=false npx wrangler dev --config "$out/probe.toml" --local --port "$probe" --inspector-port "$((port+3))" --ip 127.0.0.1 --persist-to "$state") > "$out/probe.log" 2>&1 &
jobs+=("$!")
(cd "$out" && exec env WRANGLER_SEND_METRICS=false npx wrangler dev --config "$out/wrangler.toml" --local --log-level debug --port "$port" --inspector-port "$inspector" --ip 127.0.0.1 --persist-to "$state") > "$out/worker.log" 2>&1 &
jobs+=("$!")
for ((n=0; n<120; n++)); do
  if curl --silent --fail "http://127.0.0.1:$port/health" > "$out/health.json"; then break; fi
  sleep 1
done
# The budget client owns the inspector connection and enables network events;
# the proxy accepts only one attached client at a time.
result=0
node --experimental-websocket tools/lane-499/owed_read_budget.mjs "http://127.0.0.1:$port" "http://127.0.0.1:$probe" "$inspector" | tee "$out/measurements.log" || result=$?
# This focused tier has no courier control request. The client's inspector and
# D1 requests provide the positive control; any courier host is still refused.
D1_EGRESS_MIN_SEEN=0 node tools/lane-731/egress_guard.mjs "owed-read=$out/worker.log" | tee "$out/egress.log"
node --input-type=module - "$out/worker.log" <<'JS'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
const log = readFileSync(process.argv[2], 'utf8')
const urls = [...log.matchAll(/Network\.requestWillBeSent'[\s\S]*?\burl: '([^']*)'/g)].map(match => new URL(match[1]))
assert.ok(urls.some(url => url.hostname === 'd1'), 'the inspector must observe D1 calls')
const couriers = urls.filter(url => url.hostname === '127.0.0.1' && url.port === '9' || /whatsonchain|bitails|bananablocks|arcade/.test(url.hostname))
assert.equal(couriers.length, 0, 'the read must not attempt a courier, including the sink')
console.log(`PASS egress: observed D1, ${couriers.length} courier requests`)
JS
exit "$result"
