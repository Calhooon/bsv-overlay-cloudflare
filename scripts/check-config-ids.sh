#!/usr/bin/env bash
# check-config-ids.sh (bsv-low M19B-G4, 2026-09-08). This repo is PUBLIC: a committed wrangler config may carry only
# __NAME__ placeholders for Cloudflare resource ids (D1 database ids, KV namespace ids), never the ids themselves.
# The ids are injected at deploy time by name from the operator's private store (bsv-low render-wrangler.sh).
#   scripts/check-config-ids.sh                 scan every committed wrangler*.toml; exit 1 on a raw id
#   scripts/check-config-ids.sh --self-test     fixtures only (no repo file read); run by `make ci`
set -euo pipefail
UUID='[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
HEX32='[0-9a-f]{32}'
# a raw id = an id-bearing key whose quoted value is a UUID or 32 hex chars (placeholders are __LOW_..._ID__)
RAW="^[[:space:]]*(database_id|id|namespace_id|bucket_id|account_id)[[:space:]]*=[[:space:]]*\"(${UUID}|${HEX32})\""

scan_file() { grep -nE "$RAW" "$1" || true; }

scan_repo() {
  local bad=0 f hits
  while IFS= read -r f; do
    hits=$(scan_file "$f")
    if [ -n "$hits" ]; then echo "✗ raw Cloudflare resource id committed in $f:"; echo "$hits" | sed -E 's/"[0-9a-f-]{20,}"/"<id>"/; s/^/    /'; bad=1; fi
  done < <(git ls-files 'crates/*/wrangler*.toml' 'wrangler*.toml' 2>/dev/null)
  [ "$bad" -eq 0 ] && echo "check-config-ids: committed wrangler configs carry placeholders only ✓"
  return $bad
}

self_test() {
  local T ok=0 bad=0; T=$(mktemp -d)
  check() { if eval "$2"; then ok=$((ok+1)); else bad=$((bad+1)); echo "  FAIL: $1"; fi; }
  printf 'database_id = "__LOW_X_D1_ID__"\nid = "__LOW_X_KV_ID__"\n' > "$T/ph.toml"
  printf 'database_id = "11111111-2222-3333-4444-555555555555"\n' > "$T/uuid.toml"
  printf '  id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"  # kv\n' > "$T/hex.toml"
  printf 'name = "low-overlay"\ndatabase_name = "low-overlay-db"\n' > "$T/none.toml"
  printf '# database_id = "11111111-2222-3333-4444-555555555555" in a comment\n' > "$T/comment.toml"
  check "placeholders pass" "[ -z \"\$(scan_file '$T/ph.toml')\" ]"
  check "a UUID database_id is caught" "[ -n \"\$(scan_file '$T/uuid.toml')\" ]"
  check "a 32-hex KV id is caught (indented, commented tail)" "[ -n \"\$(scan_file '$T/hex.toml')\" ]"
  check "a config with no id keys passes" "[ -z \"\$(scan_file '$T/none.toml')\" ]"
  check "an id inside a comment line is not a hit" "[ -z \"\$(scan_file '$T/comment.toml')\" ]"
  rm -rf "$T"; echo "check-config-ids self-test: $ok ok, $bad failed"; [ "$bad" -eq 0 ]
}

case "${1:-}" in
  --self-test) self_test ;;
  "") scan_repo ;;
  *) echo "usage: $0 [--self-test]" >&2; exit 2 ;;
esac
