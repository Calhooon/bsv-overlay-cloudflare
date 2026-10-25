#!/usr/bin/env bash
# bsv-low #893 section 5: the owed belt's tick on local D1 (the budget tier's `owed-belt-tick` cell). The READ lane's
# fleet seed (the lived-in identity, 500 rows written through the per-game judge) with the judge's belt dates moved an
# hour back, so the fleet's rows are due; two ticks through `POST /internal/owed-belt-tick` against a courier stand-in
# that counts every call (the worker's COURIER_SINK), then one tick under a stand-in that answers 429 everywhere.
# The CHAINTRACKS binding is a stand-in worker in the same dev session (the tip the seed was judged at).
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
cd "$root"
: "${TMPDIR:?external scratch directory required}"
export npm_config_cache="${TMPDIR%/}/d2-owed-belt-npm-cache"
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$HOME/bsv/targets/d2-owed-belt}
out_root=${TMPDIR%/}
if [ -d /Volumes/Lanes/out/bsv-low ]; then out_root=/Volumes/Lanes/out/bsv-low/D2-OWED-BELT; fi
out=${OWED_BELT_OUT:-$out_root/belt-tick-$(date -u +%Y%m%dT%H%M%SZ)}
mkdir -p "$out"
export WRANGLER_LOG_PATH="$out/wrangler-logs"
port=${LANE_BASE:-48860}
inspector=$((port+1))
standin=$((port+2))
for p in "$port" "$inspector" "$standin"; do
  if lsof -nP -iTCP:"$p" -sTCP:LISTEN >/dev/null 2>&1; then echo "owed belt tick: port $p is already bound" >&2; exit 1; fi
done
state="$out/state"
mkdir -p "$state"
export OWED_SEED_TIME=$(date -u +%s)
build=${OWED_BELT_BUILD:-$CARGO_TARGET_DIR/owed-belt-worker}
if [ -z "${OWED_BELT_BUILD:-}" ]; then
  worker-build crates/low-app-layer --release --out-dir "$build" > "$out/build.log" 2>&1
fi
token=belt-tier-$(date -u +%s)
cat > "$out/wrangler.toml" <<EOF
name = "owed-belt-tick"
main = "$build/worker/shim.mjs"
compatibility_date = "2024-12-01"
[vars]
AUTH_ENFORCE = "false"
SESSION_LANE = "false"
COURIER_SINK = "http://127.0.0.1:$standin"
INTERNAL_TOKEN = "$token"
[[services]]
binding = "CHAINTRACKS"
service = "owed-belt-chaintracks"
[[d1_databases]]
binding = "OVERLAY_DB"
database_name = "low-overlay-db"
database_id = "00000000-0000-0000-0000-000000000000"
EOF
cat > "$out/chaintracks.toml" <<EOF
name = "owed-belt-chaintracks"
main = "$root/tools/lane-893/chaintracks_standin.mjs"
compatibility_date = "2024-12-01"
EOF
cargo run --quiet --manifest-path workers/Cargo.toml -p low-app-layer --example owed_read_seed > "$out/seed.sql"
# the judge dated each owed row from the seed's clock; an hour back, every dated row is due
echo "UPDATE owed_rows SET beltDueAtMs = beltDueAtMs - 3600000 WHERE beltDueAtMs IS NOT NULL;" >> "$out/seed.sql"
python3 scripts/d1-load-chunked.py "$out" low-overlay-db "$state" "$out/seed.sql" 80 > "$out/seed.log" 2>&1
jobs=()
kill_tree() {
  local child
  for child in $(pgrep -P "$1" 2>/dev/null); do kill_tree "$child"; done
  kill "$1" 2>/dev/null || true
}
cleanup() { for pid in "${jobs[@]}"; do kill_tree "$pid"; done; }
trap cleanup EXIT
node tools/lane-893/courier_standin.mjs "$standin" > "$out/standin.log" 2>&1 &
jobs+=("$!")
(cd "$out" && exec env WRANGLER_SEND_METRICS=false npx wrangler dev --config "$out/wrangler.toml" --config "$out/chaintracks.toml" --local --log-level debug --port "$port" --inspector-port "$inspector" --ip 127.0.0.1 --persist-to "$state") > "$out/worker.log" 2>&1 &
jobs+=("$!")
for ((n=0; n<120; n++)); do
  if curl --silent --fail "http://127.0.0.1:$port/health" > "$out/health.json"; then break; fi
  sleep 1
done
node tools/lane-893/owed_belt_tick.mjs "http://127.0.0.1:$port" "http://127.0.0.1:$standin" "$token" | tee "$out/measurements.log"
