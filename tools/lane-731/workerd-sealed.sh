#!/bin/sh
# bsv-low #731: the workerd the D1 budget tier's `wrangler dev` runs (`MINIFLARE_WORKERD_PATH`), sealed to the
# loopback by `loopback-only.sb`. Only the RUNTIME is sealed: wrangler's own `[build]` (cargo install worker-build,
# worker-build) and its node process keep the network. D1_REAL_WORKERD is the binary `real_workerd.mjs` resolved.
exec sandbox-exec -f "$(dirname "$0")/loopback-only.sb" "${D1_REAL_WORKERD:?D1_REAL_WORKERD unset}" "$@"
