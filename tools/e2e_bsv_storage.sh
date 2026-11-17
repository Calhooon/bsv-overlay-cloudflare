#!/usr/bin/env bash
# End-to-end smoke test — bsv-storage-cloudflare ↔ rust-overlay ↔ R2.
#
# Verifies the full UHRP production chain works:
#
#     upload ──► bsv-storage ──► R2 (file)
#                     │
#                     └──► rust-overlay /submit (tm_uhrp advert)
#                                │
#                                ▼
#     client ──► /lookup ls_uhrp ──► (BEEF with downloadUrl)
#                                          │
#                                          ▼
#                              R2 download ──► file bytes
#
# What we assert:
#   1. Both workers are live (/health returns 200).
#   2. /lookup ls_uhrp with the record's uhrpUrl returns a non-empty
#      output-list with a BEEF: the advert a client uploaded through
#      bsv-storage was admitted and indexed by the overlay.
#   3. Fetching the record's downloadUrl returns the file bytes (the
#      upload really ended up in R2 and is publicly retrievable).
#
# The caller names the record. `ls_uhrp` has no find-all: a lookup names an
# outpoint, or at least one of uhrpUrl, expiryTime, hostIdentityKey (the
# reference's door), so this script cannot pick a record for itself.
#
#   OVERLAY_URL        the overlay Worker
#   STORAGE_URL        the UHRP storage Worker
#   TEST_UHRP_URL      the advert's UHRP URL: Base58Check of the file's
#                      SHA-256 under the prefix ce00, with or without a
#                      leading "uhrp://"
#   TEST_DOWNLOAD_URL  the downloadUrl that advert carries
#
# Idempotent: reads-only. Safe to run repeatedly.

set -euo pipefail

STORAGE_URL="${STORAGE_URL:-https://<your-storage>.workers.dev}"
OVERLAY_URL="${OVERLAY_URL:-https://<your-overlay>.workers.dev}"
TEST_UHRP_URL="${TEST_UHRP_URL:-}"
TEST_DOWNLOAD_URL="${TEST_DOWNLOAD_URL:-}"

LOOKUP_BODY=""
TMP=""
trap 'rm -f "$LOOKUP_BODY" "$TMP"' EXIT

fail() {
    echo "❌ FAIL: $*" >&2
    exit 1
}

pass() {
    echo "✅ $*"
}

if [ -z "$TEST_UHRP_URL" ] || [ -z "$TEST_DOWNLOAD_URL" ]; then
    fail "set TEST_UHRP_URL and TEST_DOWNLOAD_URL to a record your storage worker advertised (see this script's header)"
fi

echo "────── Step 1: preflight ──────"
for url in "$STORAGE_URL" "$OVERLAY_URL"; do
    code=$(curl -sS -o /dev/null -w "%{http_code}" --max-time 5 "$url/health" || echo "000")
    if [ "$code" != "200" ]; then
        fail "$url/health returned $code (expected 200)"
    fi
    pass "$url/health = 200"
done

echo
echo "────── Step 2: /lookup ls_uhrp for the record's uhrpUrl ──────"
LOOKUP_BODY=$(mktemp)
LOOKUP_QUERY=$(jq -cn --arg u "$TEST_UHRP_URL" '{service: "ls_uhrp", query: {uhrpUrl: $u}}')
LOOKUP_CODE=$(curl -sS -o "$LOOKUP_BODY" -w "%{http_code}" --max-time 10 -X POST \
    -H "Content-Type: application/json" -d "$LOOKUP_QUERY" "$OVERLAY_URL/lookup" || true)
if [ "$LOOKUP_CODE" != "200" ]; then
    # The door's refusal is a 400 whose body says why (an invalid uhrpUrl,
    # an unexpected field): show it, never a bare "0 outputs".
    fail "/lookup ls_uhrp for uhrpUrl=$TEST_UHRP_URL answered $LOOKUP_CODE: $(head -c 300 "$LOOKUP_BODY")"
fi

SPECIFIC_COUNT=$(jq '.outputs | length' "$LOOKUP_BODY")
if [ "$SPECIFIC_COUNT" -lt 1 ]; then
    fail "/lookup ls_uhrp for uhrpUrl=$TEST_UHRP_URL returned $SPECIFIC_COUNT outputs"
fi
BEEF_LEN=$(jq '.outputs[0].beef | length' "$LOOKUP_BODY")
if [ "$BEEF_LEN" -lt 1 ]; then
    fail "/lookup ls_uhrp for uhrpUrl=$TEST_UHRP_URL returned an output with no BEEF"
fi
pass "/lookup returned $SPECIFIC_COUNT output; BEEF = $BEEF_LEN bytes"

echo
echo "────── Step 3: fetch the file bytes from R2 ──────"
TMP=$(mktemp)
R2_CODE=$(curl -sS -o "$TMP" -w "%{http_code}" --max-time 10 "$TEST_DOWNLOAD_URL" || true)
if [ "$R2_CODE" != "200" ]; then
    fail "R2 fetch $TEST_DOWNLOAD_URL returned $R2_CODE"
fi
BYTES=$(wc -c < "$TMP" | tr -d ' ')
if [ "$BYTES" -lt 1 ]; then
    fail "R2 fetch returned 0 bytes"
fi
pass "R2 fetch $TEST_DOWNLOAD_URL = $BYTES bytes"
echo "    content preview: $(head -c 80 "$TMP")"

echo
echo "✅✅✅ E2E ROUND-TRIP PASSED"
echo "    bsv-storage → rust-overlay /submit → D1 → /lookup → R2 download"
echo "    the record $TEST_UHRP_URL is indexed by ls_uhrp and its file is retrievable"
