# bsv-overlay-cloudflare — top-level developer entrypoints.
#
# The headline command is `make parity`: stands up the mainline
# `@bsv/overlay-express@2.2.0` reference in Docker + `wrangler dev` locally
# in parity mode + runs the differential harness + writes PARITY_REPORT.md.
# Exit is non-zero on any un-noted divergence.

.PHONY: parity reference-up reference-down reference-logs \
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
# defaults apply (tm_ship,tm_slap / ls_ship,ls_slap). ENABLE_EXTENSIONS=false
# disables the Rust-only superset (/admin/crawlPeers, X-History-Depth,
# X-Submit-Mode, rich admin bodies). This is what the harness diffs against
# mainline. Production deploys inherit wrangler.toml's [vars] which set the
# full dolphinmilk stack.
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
