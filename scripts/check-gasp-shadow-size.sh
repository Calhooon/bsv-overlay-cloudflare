#!/usr/bin/env bash
# GASP's shadow is a cargo feature, `gasp-shadow`, default OFF (bsv-low #657,
# the shadow lens's MEDIUM 1): an `off` Worker links none of A's GASP path.
#
# Builds the raw release wasm of `bsv-overlay-cloudflare` (before
# wasm-bindgen and wasm-opt, so the names section is kept) three times: A1's
# tree (`git archive` of the base, `9d5a75a` unless GASP_SHADOW_BASE is set),
# this tree with the feature off, and this tree with it on. Both trees are
# built under `--remap-path-prefix=<tree>=/src`, so their paths weigh the same.
#
# Fails unless: the `off` build is within GASP_SHADOW_SIZE_TOLERANCE bytes
# (4096 unless set) of A1's and carries none of the shadow's names, and the
# `on` build carries them (the feature is what takes them out).
#
#   bash scripts/check-gasp-shadow-size.sh
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
base=${GASP_SHADOW_BASE:-9d5a75a}
tolerance=${GASP_SHADOW_SIZE_TOLERANCE:-4096}
targets=${CARGO_TARGET_DIR:-$root/target}/gasp-shadow-size
wasm=wasm32-unknown-unknown/release/bsv_overlay_cloudflare.wasm
# The shadow's names; none is in A1's Worker.
names=(gasp_shadow admit_graph judge_dry walk_with_mode)

# A fixed place for A1's tree, so its build is cached between runs.
scratch=$targets/base-tree
rm -rf "$scratch"
mkdir -p "$scratch"
git -C "$root" archive "$base" | tar -x -C "$scratch"

build() { # tree target-dir [cargo args...]
  local tree=$1 dir=$2
  shift 2
  RUSTFLAGS="--remap-path-prefix=$tree=/src" CARGO_TARGET_DIR="$dir" \
    cargo build -q --release --target wasm32-unknown-unknown \
    -p bsv-overlay-cloudflare --manifest-path "$tree/workers/Cargo.toml" "$@"
  wc -c <"$dir/$wasm" | tr -d ' '
}

count() { # file name
  { LC_ALL=C grep -a -o "$2" "$1" || true; } | wc -l | tr -d ' '
}

a1=$(build "$scratch" "$targets/base")
off=$(build "$root" "$targets/tree")
delta=$((off - a1))
echo "gasp-shadow size: A1 $base $a1 B; this tree, feature off, $off B (delta $delta B)"
fail=0
if [ "${delta#-}" -gt "$tolerance" ]; then
  echo "✗ the feature-off Worker is $delta B from A1's (tolerance $tolerance B)"
  fail=1
fi
for name in "${names[@]}"; do
  n=$(count "$targets/tree/$wasm" "$name")
  if [ "$n" != "0" ]; then
    echo "✗ the feature-off Worker links $name ($n names)"
    fail=1
  fi
done
[ "$fail" = 0 ] || exit 1

on=$(build "$root" "$targets/tree" --features gasp-shadow)
echo "gasp-shadow size: this tree, feature on, $on B (+$((on - a1)) B against A1)"
n=$(count "$targets/tree/$wasm" gasp_shadow)
if [ "$n" = "0" ]; then
  echo "✗ the feature-on Worker links no gasp_shadow: the gate is not the feature"
  exit 1
fi
echo "✅ gasp-shadow: off within $tolerance B of A1 and none of its names; on links it ($n names)"
