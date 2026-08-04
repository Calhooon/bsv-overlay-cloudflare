#!/usr/bin/env bash
# RED-verify a pin MECHANICALLY. See docs/RED-VERIFY.md.
#
# Usage:
#   scripts/red-verify.sh <file> <anchor-file> <replacement-file> <test-filter> [pkg]
#
# <anchor-file> holds the EXACT text to replace (the correct code).
# <replacement-file> holds the injected defect. Both are files, not shell
# arguments, so braces, quotes and newlines survive untouched.
#
# The script owns the whole cycle: it backs up the CLEAN file, applies the
# injection itself, confirms it landed, confirms it compiles, runs the suite,
# and always restores. It refuses to say RED unless every precondition held.
#
# Exit codes:
#   0  RED confirmed  — injection applied, compiled, and the named test FAILED
#   1  NOT RED        — applied and compiled, but the test PASSED
#   2  INCONCLUSIVE   — anchor not found, or the injection did not compile
#
# WHY THIS EXISTS. RED-verification lied FOUR distinct ways in the #316
# campaign, and knowing about each did not prevent the next:
#
#   1. The injection was never applied — an anchor string that had drifted, so
#      the "defect" was never in the file and the pass meant nothing.
#   2. The injection did not compile — the suite fails to BUILD, which reads as
#      a failing test to anyone grepping for "FAILED".
#   3. The anchor stopped matching after `cargo fmt` collapsed the target to one
#      line — same as (1), reached via a formatter rather than an edit.
#   4. The "restore" restored the INJECTED file, because the backup was taken
#      after the injection was applied by hand. Found by self-testing this very
#      script — which is the argument for the script.
#
# Rule 12d: the rule has to be in your hands, not your head. And: an unexpected
# GREEN on an injection you expect to fail is itself a signal — never wave it
# through as "the pin must cover it some other way".

set -uo pipefail

FILE="${1:?usage: red-verify.sh <file> <anchor-file> <replacement-file> <test-filter> [pkg]}"
ANCHOR_FILE="${2:?file containing the exact text to replace}"
REPL_FILE="${3:?file containing the injected defect}"
FILTER="${4:?the test name filter to run}"
PKG="${5:-low-watchtower}"

inconclusive() {
  echo "❌ INCONCLUSIVE — $1"
  echo "   NOT a RED result. Nothing has been proven about the pin."
  exit 2
}

[ -f "$FILE" ]        || inconclusive "no such file: $FILE"
[ -f "$ANCHOR_FILE" ] || inconclusive "no such anchor file: $ANCHOR_FILE"
[ -f "$REPL_FILE" ]   || inconclusive "no such replacement file: $REPL_FILE"

# (0) Back up the CLEAN file, before anything is touched.
BACKUP="$(mktemp)"
cp "$FILE" "$BACKUP"
restore() { cp "$BACKUP" "$FILE"; rm -f "$BACKUP"; }
trap restore EXIT

# (1) Apply the injection ourselves — no hand-editing, no drift.
APPLIED=$(FILE="$FILE" ANCHOR_FILE="$ANCHOR_FILE" REPL_FILE="$REPL_FILE" python3 - <<'PY'
import os
src = open(os.environ["FILE"]).read()
anchor = open(os.environ["ANCHOR_FILE"]).read().rstrip("\n")
repl = open(os.environ["REPL_FILE"]).read().rstrip("\n")
n = src.count(anchor)
if n != 1:
    print(f"BAD:{n}")
else:
    open(os.environ["FILE"], "w").write(src.replace(anchor, repl, 1))
    print("OK")
PY
)
case "$APPLIED" in
  OK) ;;
  BAD:0) inconclusive "the anchor text is NOT in $FILE — it has drifted (rustfmt? an edit?). This is failure mode 1/3." ;;
  BAD:*) inconclusive "the anchor text appears ${APPLIED#BAD:} times in $FILE — it must be unique." ;;
  *)     inconclusive "could not apply the injection: $APPLIED" ;;
esac

# (2) Confirm it LANDED — belt and braces over the applier's own report.
if ! diff -q "$BACKUP" "$FILE" >/dev/null 2>&1; then
  echo "✓ injection applied and file differs from clean"
else
  inconclusive "the file is byte-identical to the clean copy — nothing was injected"
fi

# (3) Confirm it COMPILES. A build break is not a failing test.
if ! cargo test -p "$PKG" --no-run >/dev/null 2>&1; then
  inconclusive "the injected code does not COMPILE — a build break is not a RED test"
fi
echo "✓ injected code compiles"

# (4) Run the suite.
OUT="$(cargo test -p "$PKG" "$FILTER" 2>&1)"
echo "$OUT" | grep -E "^test result:" || true

# (5) Only now may the word RED be used.
if echo "$OUT" | grep -qE "test result: FAILED|^test .* FAILED"; then
  echo "✅ RED CONFIRMED — '$FILTER' fails with the defect present."
  echo "$OUT" | grep -E "panicked at|assertion" | head -3
  exit 0
fi
echo "⚠️  NOT RED — the defect is present and compiling, yet '$FILTER' PASSED."
echo "   The pin does not observe this defect. Do not rationalise it."
exit 1
