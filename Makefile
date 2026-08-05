# bsv-overlay-cloudflare — top-level developer entrypoints.
#
# The headline command is `make parity`: stands up the mainline
# `@bsv/overlay-express@2.2.0` reference in Docker + `wrangler dev` locally
# in parity mode + runs the differential harness + writes PARITY_REPORT.md.
# Exit is non-zero on any un-noted divergence.

.PHONY: parity reference-up reference-down reference-logs ci-route \
        wrangler-dev harness test extensions-build e2e-bsv-storage clean help

help:
	@echo "bsv-overlay-cloudflare make targets:"
	@echo "  parity           Full harness run (assumes wrangler dev + reference up)"
	@echo "  reference-up     docker compose up the TS overlay-express 2.2.0 reference on :8090"
	@echo "  reference-down   Tear the reference stack down (keeps volumes)"
	@echo "  reference-logs   Tail overlay-express logs"
	@echo "  wrangler-dev     wrangler dev in parity mode (:8787) — run in a separate shell"
	@echo "  harness          Run parity-harness once (assumes services are up)"
	@echo "  test             cargo test --workspace with memory-storage feature"
	@echo "  ci               THE GATE: tests + clippy --all-targets + both wasm32 builds"
	@echo "  extensions-build cargo build with --features extensions (opt-in Rust superset)"
	@echo "  clean            Wipe reference volumes + wrangler local state"

## -- Reference stack (TS overlay-express 2.2.0 + Mongo + MySQL in Docker) -----

reference-up:
	cd reference && docker compose up -d --build
	@echo "reference coming up on http://localhost:8090 (wait ~15s for mainline init)"

reference-down:
	cd reference && docker compose down

reference-logs:
	docker logs -f reference-overlay-express-1

## -- Rust side (wrangler dev in parity mode) ----------------------------------

# Parity defaults — TOPIC_MANAGERS / LOOKUP_SERVICES unset so the code-side
# defaults apply (tm_ship,tm_slap / ls_ship,ls_slap). This is what the harness
# diffs against mainline. Production deploys inherit wrangler.toml's [vars]
# which set the full dolphinmilk stack.
#
# ENABLE_EXTENSIONS=false: until bsv-low #347 this var was DEAD CONFIG — set in
# both wrangler files and read nowhere in Rust, while this comment claimed it
# disabled the Rust-only superset. It now genuinely gates ONE thing, the piece
# that was a security hole: `x-submit-mode`. With it false, every /submit takes
# the SPV-barred default path regardless of header (`submit_gate.rs`). The other
# listed extensions (/admin/crawlPeers, X-History-Depth, rich admin bodies) are
# still NOT gated by it — do not re-add that claim without adding the code.
wrangler-dev:
	cd crates/overlay-cloudflare && wrangler dev --local --port 8787 --ip 127.0.0.1 \
	    --var TOPIC_MANAGERS:tm_ship,tm_slap \
	    --var LOOKUP_SERVICES:ls_ship,ls_slap \
	    --var ENABLE_EXTENSIONS:false \
	    --var ADMIN_TOKEN:parity-harness-test-token-2026 \
	    --var NODE_NAME:parityref

## -- Parity harness -----------------------------------------------------------

harness:
	cargo run -p parity-harness -- \
	    --ts http://localhost:8090 \
	    --rust http://127.0.0.1:8787 \
	    --corpus ./parity-harness/corpus \
	    --report ./PARITY_REPORT.md

# Headline: compose the whole flow. Runs the harness assuming you've already
# started `make reference-up` and `make wrangler-dev` in separate shells.
# (The two long-running services can't sit inside a single make target cleanly
# because we need to keep them running across repeated harness invocations.)
parity: harness

# Deterministic parity run: wipe reference state (Mongo + MySQL) and local
# wrangler D1, restart the reference stack, then run the harness. Use this
# before committing a PARITY_REPORT.md snapshot — otherwise residual state
# from previous runs pollutes /lookup and GASP corpus entries (the two
# sides admit different subsets of SHIP/SLAP records, so their stores
# drift across repeat submits).
parity-clean:
	cd reference && docker compose down -v
	rm -rf crates/overlay-cloudflare/.wrangler/state
	cd reference && docker compose up -d --build
	@echo "reference reset on :8090; re-run your wrangler-dev and then make harness"

## -- Tests + builds -----------------------------------------------------------

test:
	cargo test --workspace --features bsv-overlay-engine/memory-storage

# THE GATE. Run this, not a hand-typed approximation of it.
#
# Every flag here was earned. `--all-targets` because a clippy run without it
# lints no test target, and a campaign shipped a red clippy while reporting
# "clippy 0" for exactly that reason. `--no-fail-fast` because cargo aborts
# later binaries once one fails, which truncates the failing-test list and has
# three times made a partial result read as a complete one. The wasm32 builds
# because the workers are the deploy artifact and a native build proves nothing
# about them.
#
# Check the SUCCESS MARKER in a separate command — a pipe returns the pipe's
# exit code, never the build's.
ci:
	@set -e; \
	cargo test --workspace --features bsv-overlay-engine/memory-storage --no-fail-fast; \
	cargo clippy --workspace --all-targets --features bsv-overlay-engine/memory-storage -- -D warnings; \
	cargo build -p bsv-overlay-cloudflare --target wasm32-unknown-unknown --release; \
	cargo build -p low-app-layer --target wasm32-unknown-unknown --release; \
	echo "✅ local CI green"

# OPT-IN route-level coverage for the #347 submit gate (Rule 22).
#
# `make ci` cannot reach the /submit ROUTE: it takes a worker::Request and only
# runs on wasm, so the handler's use of the decision seam is unobservable to a
# native cell. The exhaustive `match` on SubmitAction makes deleting the
# refusal a COMPILE error and source pins cover the gate branch — but the
# end-to-end proof that a fabrication is actually refused lives here.
#
# Stands up `wrangler dev --local` in strict mode and drives the real route
# with the real attack shapes (zero-input and junk-input BEEFs, every header
# spelling, the tm_potparty money topic, the kill switch, the operator token).
# Not in `ci` because it needs a wrangler build + a listening port; run it
# before touching submit_gate.rs or the submit route.
ci-route:
	@echo "→ starting wrangler dev on :8791 (strict)…"
	@cd crates/overlay-cloudflare && \
	  npx wrangler dev --local --port 8791 --ip 127.0.0.1 \
	    --var TOPIC_MANAGERS:tm_collected,tm_potparty \
	    --var LOOKUP_SERVICES:ls_collected,ls_potparty \
	    --var SUBMIT_OPERATOR_TOKEN:ci-submit-tok \
	    --var SUBMIT_ENFORCE:true --var ENABLE_EXTENSIONS:true \
	    > /tmp/lane347-ci-route.log 2>&1 & \
	  echo $$! > /tmp/lane347-ci-route.pid
	@until curl -s -m 2 http://127.0.0.1:8791/listTopicManagers >/dev/null 2>&1; \
	  do sleep 3; done; echo "→ up"
	@node tools/lane-347/submit_gate_ci.mjs http://127.0.0.1:8791; \
	  rc=$$?; \
	  kill `cat /tmp/lane347-ci-route.pid` 2>/dev/null; \
	  rm -f /tmp/lane347-ci-route.pid; \
	  exit $$rc

extensions-build:
	cargo build -p bsv-overlay-cloudflare --features extensions

## -- End-to-end ---------------------------------------------------------------

# Round-trip smoke test: bsv-storage-cloudflare ↔ rust-overlay ↔ R2.
# Verifies the full UHRP production chain by querying rust-overlay's
# /lookup ls_uhrp for a record that originated from bsv-storage's
# /advertise flow, then downloading the advertised file from R2.
#
# Override endpoints with STORAGE_URL and OVERLAY_URL env vars; defaults
# target the deployed production workers.
e2e-bsv-storage:
	tools/e2e_bsv_storage.sh

## -- Clean -------------------------------------------------------------------

clean: reference-down
	cd reference && docker compose down -v
	rm -rf crates/overlay-cloudflare/.wrangler/state
	@echo "reference volumes + wrangler state wiped"
