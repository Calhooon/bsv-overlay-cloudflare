#!/usr/bin/env bash
# bsv-low #893 section 6.1: the owed-v2 backfill pass on local D1 (the budget tier's `owed-backfill-pass` cell). The READ
# lane's fleet seed (the lived-in identity, 250 results and 270 hops, 500 rows) turned back into an old worker's
# state (the key columns, owed_state and every dirty mark gone), the overlay's certification row written, then
# `POST /internal/owed-backfill-pass` until it stamps: the arming pass, the backfill (cost per identity), the stamped
# replay (one read). CHAINTRACKS is a stand-in worker in the same dev session; the courier stand-in counts calls (none).
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
cd "$root"
: "${TMPDIR:?external scratch directory required}"
export npm_config_cache="${TMPDIR%/}/d2-owed-backfill-npm-cache"
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$HOME/bsv/targets/d2-owed-backfill}
out_root=${TMPDIR%/}
if [ -d /Volumes/Lanes/out/bsv-low ]; then out_root=/Volumes/Lanes/out/bsv-low/D2-OWED-BACKFILL; fi
out=${OWED_BACKFILL_OUT:-$out_root/backfill-pass-$(date -u +%Y%m%dT%H%M%SZ)}
mkdir -p "$out"
export WRANGLER_LOG_PATH="$out/wrangler-logs"
port=${LANE_BASE:-48960}
inspector=$((port+1))
standin=$((port+2))
for p in "$port" "$inspector" "$standin"; do
  if lsof -nP -iTCP:"$p" -sTCP:LISTEN >/dev/null 2>&1; then echo "owed backfill pass: port $p is already bound" >&2; exit 1; fi
done
state="$out/state"
mkdir -p "$state"
export OWED_SEED_TIME=$(date -u +%s)
build=${OWED_BACKFILL_BUILD:-$CARGO_TARGET_DIR/owed-backfill-worker}
if [ -z "${OWED_BACKFILL_BUILD:-}" ]; then
  worker-build crates/low-app-layer --release --out-dir "$build" > "$out/build.log" 2>&1
fi
token=backfill-tier-$(date -u +%s)
cat > "$out/wrangler.toml" <<EOF
name = "owed-backfill-pass"
main = "$build/worker/shim.mjs"
compatibility_date = "2024-12-01"
[vars]
AUTH_ENFORCE = "false"
SESSION_LANE = "false"
COURIER_SINK = "http://127.0.0.1:$standin"
INTERNAL_TOKEN = "$token"
[[services]]
binding = "CHAINTRACKS"
service = "owed-backfill-chaintracks"
[[d1_databases]]
binding = "OVERLAY_DB"
database_name = "low-overlay-db"
database_id = "00000000-0000-0000-0000-000000000000"
EOF
cat > "$out/chaintracks.toml" <<EOF
name = "owed-backfill-chaintracks"
main = "$root/tools/lane-893/chaintracks_standin.mjs"
compatibility_date = "2024-12-01"
EOF
cargo run --quiet --manifest-path workers/Cargo.toml -p low-app-layer --example owed_read_seed > "$out/seed.sql"
# an old worker's rows: no keys, no judgement stamp, no state, no marks; the overlay certified at the gate's figure
cat >> "$out/seed.sql" <<'EOF'
UPDATE owed_rows SET judgedAtMs = NULL, claimable = NULL, gateHeight = NULL, beltDueAtMs = NULL, probeTip = NULL;
DELETE FROM owed_row_words;
DELETE FROM owed_state;
DELETE FROM owed_dirty;
INSERT INTO ops_counters(name, value) VALUES ('overlay_migration_count', 407) ON CONFLICT(name) DO UPDATE SET value = excluded.value;
EOF
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
d1_file="$(find "$state" -path '*D1*' -name '*.sqlite' | head -1)"
node tools/lane-893/owed_backfill_pass.mjs "http://127.0.0.1:$port" "http://127.0.0.1:$standin" "$token" "$d1_file" | tee "$out/measurements.log"
sqlite3 "$(find "$state" -name '*.sqlite' | head -1)" "SELECT identity, rows, truncated FROM owed_state ORDER BY identity; SELECT COUNT(*) FROM owed_rows WHERE judgedAtMs IS NULL; SELECT name FROM view_derivation_state WHERE name='owed-v2'; SELECT COUNT(*) FROM owed_dirty;" | tee "$out/state.log"
