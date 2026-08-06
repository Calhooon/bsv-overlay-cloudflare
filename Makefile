# bsv-overlay-cloudflare — top-level developer entrypoints.
#
# The headline command is `make parity`: stands up the mainline
# `@bsv/overlay-express@2.2.0` reference in Docker + `wrangler dev` locally
# in parity mode + runs the differential harness + writes PARITY_REPORT.md.
# Exit is non-zero on any un-noted divergence.

.PHONY: parity reference-up reference-down reference-logs ci-route ci-deploy \
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
	@echo "  ci               THE GATE: tests + clippy --all-targets + both wasm32 builds + ci-deploy + ci-route"
	@echo "  ci-route         Route-level /submit + /arc-ingest cells (part of ci; needs :8791-:8794)"
	@echo "  ci-deploy        Real worker-build/wrangler dry-run of every deployable config (part of ci)"
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
	$(MAKE) ci-deploy; \
	$(MAKE) ci-route; \
	echo "✅ local CI green"

# ROUTE-LEVEL coverage for the #347 submit gate AND `/arc-ingest` bearer-auth
# (Rule 22). PART OF `ci`.
#
# Not optional, and that is the finding rather than a preference. `cargo test`
# cannot reach the /submit ROUTE — it takes a worker::Request and only runs on
# wasm — so the handler's USE of the decision seam is invisible to it. Two
# separate re-gate probes proved the consequence: forcing `operator_authed` in
# the route's derivation, and shadowing the gate flag with a rebinding. Both
# COMPILED, both left the native suite fully GREEN, and both fully re-opened
# the CRITICAL. This tier is the only thing that sees either.
#
# This repo has no CI pipeline — `make ci` run locally IS the gate — so a tier
# outside `ci` is a tier nobody runs before push.
#
# Four workers: :8791 strict with extensions on (the main matrix), :8792 with
# ENABLE_EXTENSIONS=false (the kill switch, which was itself a HIGH defect —
# it used to route callers OFF the network gate), :8793 LENIENT
# (SUBMIT_ENFORCE unset) for the #366 census's CLIENT-population leg — the
# unauthenticated-ungated submit that strict :8791 rightly refuses is exactly
# the population the census (and the #347 flip criterion) measures, so it can
# only be driven where it is SERVED — and :8794 with TAAL_API_KEY set, the ONLY
# place `/arc-ingest` is mounted at all (`lib.rs` mirrors mainline and 404s the
# route without it). No network is required by any leg: the /submit public-path
# expectation asserts "never admitted, never 401", which holds as 422 online and
# 502 offline; and every /arc-ingest body is a STATUS callback (no merklePath),
# which reaches the auth gate and the acknowledgement arm without a chaintracks
# lookup. Neither can flake.
#
# :8794 is a SEPARATE worker rather than TAAL_API_KEY on :8791 on purpose:
# that key also arms the Arcade broadcaster on the /submit path, which would put
# a real outbound ARC call inside `make ci` with a bogus key — trading a
# network-free gate for a flaky one.
#
# MODELLING BOUNDARY (stated here as well as at the assertion, Rule 17): that
# public-path expectation is a NEGATIVE predicate. A regression that refused
# `broadcast-gated` with a 400 BEFORE ever reaching the broadcast block would
# still satisfy it, because "never admitted" stays true. Nothing in this tier
# is a POSITIVE control that the gated path actually reaches the broadcast —
# that needs a real funded transaction, which `make ci` must not require. See
# `tools/lane-347/submit_gate_ci.mjs` for the same note at the expectation.
#
# BOUNDED STARTUP + OWNED TEARDOWN (gate LOW-J). Both waits used to be
# `until curl…; do sleep 3; done` — unbounded, untrapped, and on the critical
# path of the ONLY gate this repo has. A worker that cannot bind (stale
# process, colliding dev server, build error) hung `make ci` forever with no
# diagnostic and the wrangler log never surfaced, which is strictly worse than
# failing: a hang is indistinguishable from "still running".
#
# Three things now hold, and each was VERIFIED by breaking it, not by reading:
#
#  1. PRE-FLIGHT. If either port is already bound we refuse immediately, name
#     the holder, and exit non-zero. This is not just a faster timeout: a
#     leftover worker on our port would SILENTLY SERVE this run's expectations
#     from a stale binary, and every leg would pass against code that is not
#     the code under test. Observed for real while fixing this — see (3).
#  2. BOUNDED WAIT, with the MEASURED elapsed time in the message. Each attempt
#     costs up to `curl -m 2` plus `ROUTE_UP_SLEEP`, so the wall bound is
#     ~ROUTE_UP_TRIES × 5s ≈ 5 min, NOT tries × sleep. The first version of
#     this fix printed `tries * sleep` and was wrong by 120s — a false claim in
#     the very code written to make failures honest, so the message now reports
#     what it measured (epoch Rule 10).
#  3. TEARDOWN THAT DOES NOT TRUST THE PID. `npx wrangler dev` is a four-deep
#     tree (npm exec → wrangler → cli.js → workerd) and in a NON-TTY run the
#     wrangler parent can exit 1 while `workerd` keeps the socket. Measured: a
#     green `make ci` left `npm exec wrangler dev --port 8792` orphaned at
#     PPID 1, still LISTENing, in a process group the recipe shell never owned
#     — so a `$!`-based or process-group kill silently freed nothing. Cleanup
#     therefore kills the recorded pid's whole DESCENDANT TREE and then sweeps
#     the two ports it pre-flighted as free, escalating to SIGKILL. Sweeping by
#     port is only safe BECAUSE of (1): pre-flight proved nothing else held
#     them, so anything listening at teardown is ours.
#
# `set -m` is deliberately NOT used. It emits `[1]+ Done(1)` job noise into the
# gate output, and a process-group kill without it would target the recipe
# shell's OWN group — i.e. make itself.
#
# The two workers are started SEQUENTIALLY (strict up, then kill switch) rather
# than concurrently. They previously raced on one cargo target dir and logged
# `Blocking waiting for file lock on package cache` — which serialises anyway,
# so it cost wall-clock, not correctness, while widening the window in which a
# hang looked normal. Measured after the change: zero lock-contention lines in
# either log, and the second build is a warm-cache no-op.
#
# WHAT THIS RELOCATES (Rule 19): a hang became a TIMEOUT, so a machine slow
# enough to need >~5 min for one worker's first response now FAILS the gate
# where it previously (eventually) passed. That trade is deliberate — a false
# red is diagnosable and a hang is not — and the headroom is real: a cold run
# brought BOTH workers up in 1m46s total. Raise `ROUTE_UP_TRIES` rather than
# deleting the bound.
ROUTE_UP_TRIES ?= 60
ROUTE_UP_SLEEP ?= 3
ci-route:
	@set -e; \
	strict_log=/tmp/lane347-route-strict.log; \
	kill_log=/tmp/lane347-route-kill.log; \
	lenient_log=/tmp/lane366-route-lenient.log; \
	arc_log=/tmp/lane-arc-ingest-route.log; \
	job_pids=""; owned_ports=""; \
	kill_tree() { \
	  for _c in $$(pgrep -P "$$1" 2>/dev/null); do kill_tree "$$_c"; done; \
	  kill -TERM "$$1" 2>/dev/null || true; \
	}; \
	cleanup() { \
	  for _p in $$job_pids; do kill_tree "$$_p"; done; \
	  _n=0; \
	  while [ $$_n -lt 10 ]; do \
	    _left=""; \
	    for _pt in $$owned_ports; do \
	      _left="$$_left $$(lsof -nP -tiTCP:$$_pt -sTCP:LISTEN 2>/dev/null || true)"; \
	    done; \
	    _left=$$(echo $$_left); \
	    if [ -z "$$_left" ]; then break; fi; \
	    kill -KILL $$_left 2>/dev/null || true; \
	    _n=$$((_n+1)); sleep 1; \
	  done; \
	  if [ -n "$$_left" ]; then \
	    echo "⚠ ci-route: could not free$$owned_ports (still held by:$$_left)"; \
	  fi; \
	}; \
	trap 'cleanup; exit 130' INT TERM; \
	trap cleanup EXIT; \
	preflight() { \
	  _held=$$(lsof -nP -tiTCP:$$1 -sTCP:LISTEN 2>/dev/null || true); \
	  if [ -n "$$_held" ]; then \
	    echo "✗ ci-route: :$$1 is ALREADY BOUND before we start — refusing to run."; \
	    echo "  A leftover worker would serve this run's expectations from a STALE"; \
	    echo "  binary and every leg would pass against code that is not under test."; \
	    ps -o pid,ppid,command -p $$_held 2>/dev/null || true; \
	    echo "  Free it with:  kill $$_held"; \
	    return 1; \
	  fi; \
	  return 0; \
	}; \
	preflight 8791; \
	preflight 8792; \
	preflight 8793; \
	preflight 8794; \
	owned_ports="8791 8792 8793 8794"; \
	wait_up() { \
	  _port=$$1; _log=$$2; _label=$$3; _i=0; _t0=$$(date +%s); \
	  while [ $$_i -lt $(ROUTE_UP_TRIES) ]; do \
	    if curl -s -m 2 http://127.0.0.1:$$_port/listTopicManagers >/dev/null 2>&1; then \
	      return 0; \
	    fi; \
	    _i=$$((_i+1)); sleep $(ROUTE_UP_SLEEP); \
	  done; \
	  echo ""; \
	  echo "✗ ci-route: the $$_label worker never answered on :$$_port — gave up after"; \
	  echo "  $$(( $$(date +%s) - _t0 ))s ($(ROUTE_UP_TRIES) attempts). The worker build most likely failed;"; \
	  echo "  its wrangler log follows."; \
	  echo "  ──────── $$_log ────────"; \
	  cat "$$_log" 2>/dev/null || echo "  (no log written at $$_log)"; \
	  echo "  ────────────────────────"; \
	  return 1; \
	}; \
	echo "→ starting wrangler dev :8791 (strict)…"; \
	( cd crates/overlay-cloudflare && exec npx wrangler dev --local --port 8791 --ip 127.0.0.1 \
	    --var TOPIC_MANAGERS:tm_collected,tm_potparty \
	    --var LOOKUP_SERVICES:ls_collected,ls_potparty \
	    --var SUBMIT_OPERATOR_TOKEN:ci-submit-tok \
	    --var SUBMIT_ENFORCE:true --var ENABLE_EXTENSIONS:true \
	) > "$$strict_log" 2>&1 & \
	job_pids="$$job_pids $$!"; \
	wait_up 8791 "$$strict_log" strict; \
	echo "→ starting wrangler dev :8792 (kill switch)…"; \
	( cd crates/overlay-cloudflare && exec npx wrangler dev --local --port 8792 --ip 127.0.0.1 \
	    --var TOPIC_MANAGERS:tm_collected,tm_potparty \
	    --var LOOKUP_SERVICES:ls_collected,ls_potparty \
	    --var SUBMIT_OPERATOR_TOKEN:ci-submit-tok \
	    --var SUBMIT_ENFORCE:true --var ENABLE_EXTENSIONS:false \
	) > "$$kill_log" 2>&1 & \
	job_pids="$$job_pids $$!"; \
	wait_up 8792 "$$kill_log" "kill switch"; \
	echo "→ starting wrangler dev :8793 (lenient — #366 census client-population leg)…"; \
	( cd crates/overlay-cloudflare && exec npx wrangler dev --local --port 8793 --ip 127.0.0.1 \
	    --var TOPIC_MANAGERS:tm_collected,tm_potparty \
	    --var LOOKUP_SERVICES:ls_collected,ls_potparty \
	    --var SUBMIT_OPERATOR_TOKEN:ci-submit-tok \
	    --var ENABLE_EXTENSIONS:true \
	) > "$$lenient_log" 2>&1 & \
	job_pids="$$job_pids $$!"; \
	wait_up 8793 "$$lenient_log" "lenient"; \
	echo "→ starting wrangler dev :8794 (arc-ingest — TAAL_API_KEY set, the only place the route is mounted)…"; \
	( cd crates/overlay-cloudflare && exec npx wrangler dev --local --port 8794 --ip 127.0.0.1 \
	    --var TOPIC_MANAGERS:tm_collected,tm_potparty \
	    --var LOOKUP_SERVICES:ls_collected,ls_potparty \
	    --var SUBMIT_OPERATOR_TOKEN:ci-submit-tok \
	    --var ENABLE_EXTENSIONS:true \
	    --var TAAL_API_KEY:ci-arc-ingest-route-tier \
	) > "$$arc_log" 2>&1 & \
	job_pids="$$job_pids $$!"; \
	wait_up 8794 "$$arc_log" "arc-ingest"; \
	echo "→ all four up"; \
	KILL_SWITCH_BASE=http://127.0.0.1:8792 \
	  node tools/lane-347/submit_gate_ci.mjs http://127.0.0.1:8791; \
	CENSUS_LENIENT_BASE=http://127.0.0.1:8793 \
	  node tools/lane-366/census_route_ci.mjs http://127.0.0.1:8791; \
	node tools/lane-arc-ingest/arc_ingest_auth_ci.mjs http://127.0.0.1:8794

# DEPLOY-PATH coverage (bsv-low #348). PART OF `ci`, and the reason is the
# whole issue: `low-app-layer` was UNDEPLOYABLE for a month while `make ci`
# was green every single day.
#
# The gap is structural, not an oversight. `cargo build --target wasm32` is
# perfectly happy with two `worker` majors in one workspace. `worker-build` —
# which runs ONLY at deploy time — is not: it resolves `worker` from the
# WORKSPACE Cargo.lock and takes the LOWEST version present, because its
# per-crate disambiguation is dead code (off-by-one in
# `Lockfile::get_package_version`, `dep.chars().nth(package.len() + 1)` where
# the space is at `package.len()`; verified present in worker-build 0.7.5,
# 0.8.4 and 0.8.5). So the crate wanting the higher version simply cannot be
# built for deploy, and NOTHING inside the gate could see it. A build that only
# the deploy tool can fail, with no deploy step in the gate, has coverage
# "none" — Rule 22's corollary. This target is the missing step.
#
# It runs the REAL thing: `wrangler deploy --dry-run` executes each config's
# own `[build]` command (`cargo install --version ^N worker-build &&
# worker-build --release`) and then bundles the shim, stopping only short of
# upload. A hand-rolled `cargo build` substitute would reproduce exactly the
# blind spot being closed, and a bare `worker-build` would not read the
# wrangler configs — where the toolchain pin actually lives.
#
# ALL THREE deployable configs, not one per crate. `wrangler.toml` and
# `wrangler.low.toml` share the overlay crate but carry SEPARATE pins, and
# `low-overlay` is a live production worker: a pin that drifts in only one file
# is precisely this bug's mirror image. The second overlay build is a warm
# rebuild (~11s), which is cheap enough that "same crate" is not a reason to
# skip a live config.
#
# MEASURED COST (M-series, warm cargo + wasm-opt/esbuild already downloaded):
#   overlay wrangler.toml      12.5s
#   overlay wrangler.low.toml  11.4s
#   low-app-layer              4.2s
#   total                      ~28s
# Cold (first run on a machine) adds a one-off worker-build install (~55s) plus
# wasm-opt/esbuild downloads. Against `ci-route`'s measured 1m46s worker
# startup this is not the expensive part of the gate, so it goes IN `ci`.
#
# NETWORK: needs `npx wrangler` and, on a cold machine, the wasm-opt/esbuild
# downloads — the same dependency `ci-route` already puts in the gate. It does
# NOT need Cloudflare credentials; `--dry-run` never authenticates, and the
# configs' account/database ids are committed placeholders.
#
# The plain `cargo build --target wasm32` steps in `ci` are kept even though
# this target recompiles the same crates: they share the cargo cache (so they
# cost ~0 here) and they give a clean compile error with no toolchain-install
# or npx noise in front of it — and they are the only wasm coverage that
# survives if this target ever has to be skipped offline.
DEPLOY_CONFIGS ?= crates/overlay-cloudflare:wrangler.toml \
                  crates/overlay-cloudflare:wrangler.low.toml \
                  crates/low-app-layer:wrangler.toml
ci-deploy:
	@set -e; \
	vers=$$(awk '/^name = "worker"$$/{getline; gsub(/[^0-9.]/,"",$$0); print}' Cargo.lock | sort -u); \
	nv=$$(printf '%s\n' "$$vers" | sed '/^$$/d' | wc -l | tr -d ' '); \
	if [ "$$nv" != "1" ]; then \
	  echo "✗ ci-deploy: Cargo.lock holds $$nv \`worker\` versions —" $$vers; \
	  echo "  worker-build takes the LOWEST one for EVERY crate in the workspace,"; \
	  echo "  so any crate needing a higher one is undeployable (bsv-low #348)."; \
	  echo "  The workspace may hold exactly ONE \`worker\` version."; \
	  exit 1; \
	fi; \
	pins=$$(sed -n 's/^command = .*--version \^\([0-9][0-9.]*\) worker-build.*/\1/p' \
	    crates/overlay-cloudflare/wrangler.toml \
	    crates/overlay-cloudflare/wrangler.low.toml \
	    crates/low-app-layer/wrangler.toml | sort -u); \
	np=$$(printf '%s\n' "$$pins" | sed '/^$$/d' | wc -l | tr -d ' '); \
	if [ "$$np" != "1" ]; then \
	  echo "✗ ci-deploy: the wrangler [build] worker-build pins disagree —" $$pins; \
	  echo "  every deployable config builds from the SAME workspace lock, so the"; \
	  echo "  pins must match each other and the lock's worker $$vers."; \
	  exit 1; \
	fi; \
	echo "→ ci-deploy preflight ok: worker $$vers, worker-build ^$$pins, 3 configs"; \
	out=$$(mktemp -d /tmp/ci-deploy.XXXXXX); \
	trap 'rm -rf "$$out"' EXIT; \
	for cfg in $(DEPLOY_CONFIGS); do \
	  d=$${cfg%%:*}; f=$${cfg##*:}; \
	  printf '→ deploy dry-run %s/%s … ' "$$d" "$$f"; \
	  if ( cd "$$d" && npx wrangler deploy --config "$$f" --dry-run \
	         --outdir "$$out/bundle" ) > "$$out/log" 2>&1; then \
	    echo "ok"; \
	  else \
	    echo "FAILED"; \
	    echo "✗ ci-deploy: $$d/$$f does not build for DEPLOY. Nothing else in"; \
	    echo "  'make ci' can see this class — do not work around it by skipping"; \
	    echo "  this target. Full wrangler/worker-build output:"; \
	    echo "  ──────── $$d/$$f ────────"; \
	    cat "$$out/log"; \
	    echo "  ────────────────────────"; \
	    exit 1; \
	  fi; \
	done; \
	echo "✅ ci-deploy: all 3 deployable configs built through the real worker-build"

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
