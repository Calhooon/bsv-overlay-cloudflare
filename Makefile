# bsv-overlay-cloudflare — top-level developer entrypoints.
#
# The headline command is `make parity`: stands up the mainline
# `@bsv/overlay-express@2.2.0` reference in Docker + `wrangler dev` locally
# in parity mode + runs the differential harness + writes PARITY_REPORT.md.
# Exit is non-zero on any un-noted divergence.

.PHONY: parity reference-up reference-down reference-logs ci-route ci-deploy ci-d1-budget ownership \
        wrangler-dev harness test e2e-bsv-storage clean help

help:
	@echo "bsv-overlay-cloudflare make targets:"
	@echo "  parity           Full harness run (assumes wrangler dev + reference up)"
	@echo "  reference-up     docker compose up the TS overlay-express 2.2.0 reference on :8090"
	@echo "  reference-down   Tear the reference stack down (keeps volumes)"
	@echo "  reference-logs   Tail overlay-express logs"
	@echo "  wrangler-dev     wrangler dev in parity mode (:8787) — run in a separate shell"
	@echo "  harness          Run parity-harness once (assumes services are up)"
	@echo "  test             cargo test of BOTH workspaces (root: engine crates + harness; workers/: the LOW workers)"
	@echo "  ci               THE GATE: tests + clippy --all-targets + both wasm32 builds + ci-deploy + ci-route"
	@echo "  ownership        The storage ownership check (bsv-low #474): every SQL statement against storage-ownership.json (part of ci)"
	@echo "  ci-route         Route-level /submit + /arc-ingest cells (part of ci; needs nine free ports from LANE_BASE, default :8791-:8799, and :LANE_BASE+11, +12 for NL-6c and NL-6d)"
	@echo "  ci-d1-budget     D1 rows-read/write ceilings per hot route on a fixture D1 (part of ci-route; :LANE_BASE+9, +10)"
	@echo "  ci-deploy        Real worker-build/wrangler dry-run of every deployable config (part of ci)"
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

# TWO workspaces since bsv-low #553. The ROOT one (engine, discovery, the
# parity harness) is what a consumer pins by git rev: it must load from a
# fresh clone with nothing beside it, so it holds no path dependency that
# leaves the repository. `workers/Cargo.toml` holds the two deployable
# workers and the `LOW/proof/v1` replay, which link `low-core`/`low-wire`
# by PATH from the private `../bsv-low` checkout. Every gate below runs
# BOTH: a root-only `cargo test --workspace` no longer reaches a worker.
WORKERS := --manifest-path workers/Cargo.toml

test:
	cargo test --workspace --features bsv-overlay-engine/memory-storage
	cargo test $(WORKERS) --workspace

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
	bash scripts/check-config-ids.sh --self-test; \
	bash scripts/check-config-ids.sh; \
	python3 scripts/check-storage-ownership.py --self-test; \
	python3 scripts/check-storage-ownership.py; \
	cargo test --workspace --features bsv-overlay-engine/memory-storage --no-fail-fast; \
	cargo test $(WORKERS) --workspace --no-fail-fast; \
	cargo clippy --workspace --all-targets --features bsv-overlay-engine/memory-storage -- -D warnings; \
	cargo clippy $(WORKERS) --workspace --all-targets -- -D warnings; \
	cargo build -p bsv-overlay-engine -p bsv-overlay-discovery --target wasm32-unknown-unknown; \
	cargo build $(WORKERS) -p bsv-overlay-cloudflare --target wasm32-unknown-unknown --release; \
	cargo build $(WORKERS) -p low-app-layer --target wasm32-unknown-unknown --release; \
	$(MAKE) ci-deploy; \
	$(MAKE) ci-route; \
	echo "✅ local CI green"

# bsv-low #474: the shared D1's ownership manifest. Every SQL statement of the
# overlay, discovery and app-layer crates against storage-ownership.json (prose:
# docs/STORAGE-OWNERSHIP.md). Python 3 stdlib only. Part of `ci`.
ownership:
	@python3 scripts/check-storage-ownership.py --self-test
	@python3 scripts/check-storage-ownership.py

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
ci-route: ci-d1-budget
	@set -e; \
	B=$${LANE_BASE:-8791}; P1=$$B; P2=$$((B+1)); P3=$$((B+2)); P4=$$((B+3)); P5=$$((B+4)); P6=$$((B+5)); P7=$$((B+6)); P8=$$((B+7)); P9=$$((B+8)); P10=$$((B+11)); P11=$$((B+12)); \
	strict_log=/tmp/lane347-route-strict.log; \
	kill_log=/tmp/lane347-route-kill.log; \
	lenient_log=/tmp/lane366-route-lenient.log; \
	arc_log=/tmp/lane-arc-ingest-route.log; \
	seen_log=/tmp/lane371-route-seen.log; \
	door_log=/tmp/lane-script-route-door.log; \
	door_off_log=/tmp/lane-script-route-door-off.log; \
	ef_log=/tmp/lane-nl6c-route-ef-$$B.log; \
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
	preflight $$P1; \
	preflight $$P2; \
	preflight $$P3; \
	preflight $$P4; \
	preflight $$P5; \
	preflight $$P6; \
	preflight $$P7; \
	preflight $$P8; \
	preflight $$P9; \
	preflight $$P10; \
	preflight $$P11; \
	owned_ports="$$P1 $$P2 $$P3 $$P4 $$P5 $$P6 $$P7 $$P8 $$P9 $$P10 $$P11"; \
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
	echo "→ starting wrangler dev :$$P1 (strict)…"; \
	( cd crates/overlay-cloudflare && exec npx wrangler dev --local --port $$P1 --ip 127.0.0.1 \
	    --var TOPIC_MANAGERS:tm_collected,tm_potparty \
	    --var LOOKUP_SERVICES:ls_collected,ls_potparty \
	    --var SUBMIT_OPERATOR_TOKEN:ci-submit-tok \
	    --var SUBMIT_ENFORCE:true --var ENABLE_EXTENSIONS:true \
	) > "$$strict_log" 2>&1 & \
	job_pids="$$job_pids $$!"; \
	wait_up $$P1 "$$strict_log" strict; \
	echo "→ starting wrangler dev :$$P2 (kill switch)…"; \
	( cd crates/overlay-cloudflare && exec npx wrangler dev --local --port $$P2 --ip 127.0.0.1 \
	    --var TOPIC_MANAGERS:tm_collected,tm_potparty \
	    --var LOOKUP_SERVICES:ls_collected,ls_potparty \
	    --var SUBMIT_OPERATOR_TOKEN:ci-submit-tok \
	    --var SUBMIT_ENFORCE:true --var ENABLE_EXTENSIONS:false \
	) > "$$kill_log" 2>&1 & \
	job_pids="$$job_pids $$!"; \
	wait_up $$P2 "$$kill_log" "kill switch"; \
	echo "→ starting wrangler dev :$$P3 (lenient — #366 census client-population leg)…"; \
	( cd crates/overlay-cloudflare && exec npx wrangler dev --local --port $$P3 --ip 127.0.0.1 \
	    --var TOPIC_MANAGERS:tm_collected,tm_potparty \
	    --var LOOKUP_SERVICES:ls_collected,ls_potparty \
	    --var SUBMIT_OPERATOR_TOKEN:ci-submit-tok \
	    --var ENABLE_EXTENSIONS:true \
	) > "$$lenient_log" 2>&1 & \
	job_pids="$$job_pids $$!"; \
	wait_up $$P3 "$$lenient_log" "lenient"; \
	echo "→ starting wrangler dev :$$P4 (arc-ingest — TAAL_API_KEY set, the only place the route is mounted)…"; \
	( cd crates/overlay-cloudflare && exec npx wrangler dev --local --port $$P4 --ip 127.0.0.1 \
	    --var TOPIC_MANAGERS:tm_collected,tm_potparty \
	    --var LOOKUP_SERVICES:ls_collected,ls_potparty \
	    --var SUBMIT_OPERATOR_TOKEN:ci-submit-tok \
	    --var ENABLE_EXTENSIONS:true \
	    --var TAAL_API_KEY:ci-arc-ingest-route-tier \
	) > "$$arc_log" 2>&1 & \
	job_pids="$$job_pids $$!"; \
	wait_up $$P4 "$$arc_log" "arc-ingest"; \
	echo "→ starting wrangler dev :$$P6 (network_seen — ARCADE_URL points at the lane-371 fixture on :$$P5)…"; \
	( cd crates/overlay-cloudflare && exec npx wrangler dev --local --port $$P6 --ip 127.0.0.1 \
	    --var TOPIC_MANAGERS:tm_collected,tm_potparty \
	    --var LOOKUP_SERVICES:ls_collected,ls_potparty \
	    --var SUBMIT_OPERATOR_TOKEN:ci-submit-tok \
	    --var ENABLE_EXTENSIONS:true \
	    --var ARCADE_URL:http://127.0.0.1:$$P5 \
	) > "$$seen_log" 2>&1 & \
	job_pids="$$job_pids $$!"; \
	wait_up $$P6 "$$seen_log" "network_seen"; \
	echo "→ starting wrangler dev :$$P7 (the script DOOR — bsv-low W-A / #437 step 2: SCRIPT_VERIFY_NETWORK_GATED=true, ARCADE_URL points at the lane-script fixture on :$$P8)…"; \
	( cd crates/overlay-cloudflare && exec npx wrangler dev --local --port $$P7 --ip 127.0.0.1 \
	    --var TOPIC_MANAGERS:tm_collected,tm_potparty \
	    --var LOOKUP_SERVICES:ls_collected,ls_potparty \
	    --var SUBMIT_OPERATOR_TOKEN:ci-submit-tok \
	    --var SUBMIT_ENFORCE:true --var ENABLE_EXTENSIONS:true \
	    --var SCRIPT_VERIFY_NETWORK_GATED:true \
	    --var ARCADE_URL:http://127.0.0.1:$$P8 \
	) > "$$door_log" 2>&1 & \
	job_pids="$$job_pids $$!"; \
	wait_up $$P7 "$$door_log" "script door"; \
	echo "→ starting wrangler dev :$$P9 (the script door's KILL SWITCH — no SCRIPT_VERIFY_NETWORK_GATED; ARCADE_URL → the lane-script fixture on :$$P8)…"; \
	( cd crates/overlay-cloudflare && exec npx wrangler dev --local --port $$P9 --ip 127.0.0.1 \
	    --var TOPIC_MANAGERS:tm_collected,tm_potparty \
	    --var LOOKUP_SERVICES:ls_collected,ls_potparty \
	    --var SUBMIT_OPERATOR_TOKEN:ci-submit-tok \
	    --var SUBMIT_ENFORCE:true --var ENABLE_EXTENSIONS:true \
	    --var ARCADE_URL:http://127.0.0.1:$$P8 \
	) > "$$door_off_log" 2>&1 & \
	job_pids="$$job_pids $$!"; \
	wait_up $$P9 "$$door_off_log" "script door (kill switch)"; \
	echo "→ starting wrangler dev :$$P10 (NL-6c, the EF work bound, and NL-6d, the corroboration legs: ARCADE_URL and CORROBORATOR_URL → the fixture on :$$P11, DUAL_BROADCAST=off)…"; \
	( cd crates/overlay-cloudflare && exec npx wrangler dev --local --port $$P10 --ip 127.0.0.1 \
	    --var TOPIC_MANAGERS:tm_collected,tm_potparty \
	    --var LOOKUP_SERVICES:ls_collected,ls_potparty \
	    --var SUBMIT_OPERATOR_TOKEN:ci-submit-tok \
	    --var SUBMIT_ENFORCE:true --var ENABLE_EXTENSIONS:true \
	    --var ARCADE_URL:http://127.0.0.1:$$P11 \
	    --var DUAL_BROADCAST:off \
	    --var CORROBORATOR_URL:http://127.0.0.1:$$P11 \
	) > "$$ef_log" 2>&1 & \
	job_pids="$$job_pids $$!"; \
	wait_up $$P10 "$$ef_log" "EF work bound"; \
	echo "→ all eight up"; \
	KILL_SWITCH_BASE=http://127.0.0.1:$$P2 \
	  node tools/lane-347/submit_gate_ci.mjs http://127.0.0.1:$$P1; \
	CENSUS_LENIENT_BASE=http://127.0.0.1:$$P3 \
	  node tools/lane-366/census_route_ci.mjs http://127.0.0.1:$$P1; \
	node tools/lane-arc-ingest/arc_ingest_auth_ci.mjs http://127.0.0.1:$$P4; \
	FIXTURE_PORT=$$P5 \
	  node tools/lane-371/network_seen_route_ci.mjs http://127.0.0.1:$$P6; \
	FIXTURE_PORT=$$P8 \
	  node tools/lane-script/script_refusal_route_ci.mjs http://127.0.0.1:$$P7; \
	EXPECT_DOOR=off FIXTURE_PORT=$$P8 \
	  node tools/lane-script/script_refusal_route_ci.mjs http://127.0.0.1:$$P9; \
	node tools/lane-nl6/submit_any_size_ci.mjs http://127.0.0.1:$$P1; \
	FIXTURE_PORT=$$P11 \
	  node tools/lane-nl6c/ef_work_bound_ci.mjs http://127.0.0.1:$$P10; \
	FIXTURE_PORT=$$P11 \
	  node tools/lane-nl6d/corroboration_legs_ci.mjs http://127.0.0.1:$$P10

# bsv-low #499: THE D1 BUDGET TIER. A prerequisite of `ci-route` (so part of `ci`), in its own block.
#
# Every hot route answers what it cost D1 (`Server-Timing: d1;desc="reads=N writes=M stmts=K"`, the request's
# rows ledger, `crates/overlay-cloudflare/src/d1_ledger.rs`). Two workers on a FIXTURE D1 (real SQLite under
# `wrangler dev --local`): the app layer on the seed `crates/low-app-layer/examples/d1_budget_seed.rs` prints
# (the production schema plus identity A's 8 pots, B's 2 and 300 strangers' noise), loaded by
# `wrangler d1 execute --local --persist-to`; the overlay on its own fresh state. `tools/lane-499` drives each
# scenario and reds BY NAME when a figure passes its ceiling (`tools/lane-499/ceilings.json`, the table and the
# margin in `docs/D1-BUDGETS.md`), then pins the brain's recompute ceiling through the route.
# `D1_BUDGET_MEASURE=1 make ci-d1-budget` prints the measured table and enforces nothing.
# Last, on the same overlay worker (after the budget legs, so their figures are untouched): bsv-low #575's
# landing-guard cell (`tools/lane-e1d`), which seeds an OPEN eviction row into that worker's `--persist-to` D1
# and reads its tables back with `wrangler d1 execute --local`: a carried predecessor under an open eviction is
# never landed, and its successor is "not now".
# Then bsv-low #576's dead-letter cell (`tools/lane-e576`, the overlay given `INTERNAL_TOKEN`): a real "not now"
# successor dead-lettered through the local queue and PARKED in `mutation_dead_letters`, the lever's bearer, its
# limit, its one-enqueue claim and its ceiling over seeded letters, a re-driven letter parked again with its
# history, and `/health/invariants.deadLetters`; the lens fold's legs: the new health fields, a stale re-drive
# returned and re-driven, a forced re-drive of an exhausted letter, a bad-base64 replay dead-lettered and parked.
# Then bsv-low #585 door 3's cell (`tools/lane-e585`, the overlay given `MUTATION_QUEUE_INLINE_ROOM:4096`, which
# leaves every other cell's sub-kilobyte messages inline): ~8 KB "not now" letters written to the local R2 bucket,
# parked by KEY, re-driven from R2 and landed (the object gone after the ack), a missing object parked again with its
# class kept, a discard deleting the object, a 500 KB "not now" submission carried whole (#568), and the d3 fold-2's
# legs: a twin acked over a missing object, the orphan sweep through `/__scheduled` (the overlay runs with
# `--test-scheduled`), and a put twice moving the object's `touched` stamp.
#
# Ports: LANE_BASE+9 (app layer) and LANE_BASE+10 (overlay), :8800 and :8801 by default; the same pre-flight,
# bounded wait and owned teardown as `ci-route` (its comment above has the why). No leg needs the network: no
# fixture pot is spent and no hop filed (no courier), the app layer's service bindings are absent (its tip reads
# answer 503 and the views serve without a tip), and the overlay's ARCADE_URL is a closed local port. The logs carry
# the port base (`/tmp/lane499-d1-<LANE_BASE>-app.log`, `-overlay.log`, `-seed.log`; the d3 fold-3, E585-D3-DELTA-M2):
# two tiers on different bases never overwrite each other's logs. The e585 cell runs LAST: its leg 7 fires the whole
# scheduled tick (`/__scheduled`), whose GASP step syncs the worker's hard-coded peers over the network and DEFERS real
# graphs into `gasp_deferred_graphs` in the background; run before the e555 cell it put 18 rows under that cell's
# counts (the captain's re-run of the d3 fold-2, 2026-10-09: 4 e555 FAILs). Nothing runs after it.
ci-d1-budget:
	@set -e; \
	B=$${LANE_BASE:-8791}; PA=$$((B+9)); PO=$$((B+10)); \
	app_log=/tmp/lane499-d1-$$B-app.log; ov_log=/tmp/lane499-d1-$$B-overlay.log; seed_log=/tmp/lane499-d1-$$B-seed.log; \
	state=$$(mktemp -d /tmp/lane499-d1-state.XXXXXX); ov_state=$$(mktemp -d /tmp/lane499-d1-ovstate.XXXXXX); \
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
	  if [ -n "$$_left" ]; then echo "⚠ ci-d1-budget: could not free$$owned_ports (still held by:$$_left)"; fi; \
	  rm -rf "$$state" "$$ov_state"; \
	}; \
	trap 'cleanup; exit 130' INT TERM; \
	trap cleanup EXIT; \
	for _pt in $$PA $$PO; do \
	  _held=$$(lsof -nP -tiTCP:$$_pt -sTCP:LISTEN 2>/dev/null || true); \
	  if [ -n "$$_held" ]; then \
	    echo "✗ ci-d1-budget: :$$_pt is ALREADY BOUND before we start, refusing to run (a stale worker would answer)."; \
	    ps -o pid,ppid,command -p $$_held 2>/dev/null || true; \
	    exit 1; \
	  fi; \
	done; \
	owned_ports="$$PA $$PO"; \
	wait_up() { \
	  _url=$$1; _log=$$2; _label=$$3; _i=0; _t0=$$(date +%s); \
	  while [ $$_i -lt $(ROUTE_UP_TRIES) ]; do \
	    if curl -s -m 2 -o /dev/null "$$_url"; then return 0; fi; \
	    _i=$$((_i+1)); sleep $(ROUTE_UP_SLEEP); \
	  done; \
	  echo "✗ ci-d1-budget: the $$_label worker never answered at $$_url after $$(( $$(date +%s) - _t0 ))s; its log:"; \
	  cat "$$_log" 2>/dev/null || true; \
	  return 1; \
	}; \
	echo "→ ci-d1-budget: the fixture D1 (seed + wrangler d1 execute --local)…"; \
	cargo run -q $(WORKERS) -p low-app-layer --example d1_budget_seed > "$$state/seed.sql"; \
	( cd crates/low-app-layer && npx wrangler d1 execute low-overlay-db --local --persist-to "$$state" --file "$$state/seed.sql" ) > "$$seed_log" 2>&1 \
	  || { echo "✗ ci-d1-budget: the seed did not load"; cat "$$seed_log"; exit 1; }; \
	echo "→ starting wrangler dev :$$PA (the app layer on the fixture D1)…"; \
	( cd crates/low-app-layer && exec npx wrangler dev --local --port $$PA --ip 127.0.0.1 --persist-to "$$state" \
	    --var AUTH_ENFORCE:false --var SESSION_LANE:false \
	    --var INTERNAL_TOKEN:ci-internal-tok \
	) > "$$app_log" 2>&1 & \
	job_pids="$$job_pids $$!"; \
	wait_up http://127.0.0.1:$$PA/health "$$app_log" "app layer"; \
	echo "→ starting wrangler dev :$$PO (the overlay, tm_collected, ARCADE_URL a closed port)…"; \
	( cd crates/overlay-cloudflare && exec npx wrangler dev --local --test-scheduled --port $$PO --ip 127.0.0.1 --persist-to "$$ov_state" \
	    --var TOPIC_MANAGERS:tm_collected,tm_potparty \
	    --var LOOKUP_SERVICES:ls_collected,ls_potparty \
	    --var SUBMIT_OPERATOR_TOKEN:ci-submit-tok \
	    --var SUBMIT_ENFORCE:true --var ENABLE_EXTENSIONS:true \
	    --var ARCADE_URL:http://127.0.0.1:9 \
	    --var INTERNAL_TOKEN:ci-internal-tok \
	    --var MUTATION_QUEUE_INLINE_ROOM:4096 \
	) > "$$ov_log" 2>&1 & \
	job_pids="$$job_pids $$!"; \
	wait_up http://127.0.0.1:$$PO/listTopicManagers "$$ov_log" "overlay"; \
	node tools/lane-499/d1_budget_route_ci.mjs http://127.0.0.1:$$PA http://127.0.0.1:$$PO; \
	python3 scripts/d1-census.py --self-test; \
	python3 scripts/d1-census.py --app http://127.0.0.1:$$PA --overlay http://127.0.0.1:$$PO; \
	node tools/lane-e1d/landing_guard_route_ci.mjs http://127.0.0.1:$$PO "$$ov_state"; \
	node tools/lane-e576/dead_letter_route_ci.mjs http://127.0.0.1:$$PO "$$ov_state"; \
	node tools/lane-e555/deferred_graphs_route_ci.mjs http://127.0.0.1:$$PO "$$ov_state"; \
	node tools/lane-e585/beef_blobs_route_ci.mjs http://127.0.0.1:$$PO "$$ov_state"

# DEPLOY-PATH coverage (bsv-low #348). PART OF `ci`, and the reason is the
# whole issue: `low-app-layer` was UNDEPLOYABLE for a month while `make ci`
# was green every single day.
#
# The gap is structural, not an oversight. `cargo build --target wasm32` is
# perfectly happy with two `worker` majors in one workspace. `worker-build` —
# which runs ONLY at deploy time — is not: it resolves `worker` from the
# WORKSPACE Cargo.lock (since bsv-low #553 that is `workers/Cargo.lock`, the
# lock of the workspace both workers belong to; the root lock holds no
# `worker` at all) and takes the LOWEST version present, because its
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
# configs' account/database ids are committed placeholders (enforced by
# scripts/check-config-ids.sh at the top of `ci` since bsv-low M19B-G4; the
# real ids are injected at deploy time by name, bsv-low render-wrangler.sh).
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
	bash scripts/check-workspaces.sh; \
	rootw=$$(grep -c '^name = "worker"$$' Cargo.lock || true); \
	if [ "$$rootw" != "0" ]; then \
	  echo "✗ ci-deploy: the ROOT workspace must build from this repository alone"; \
	  echo "  (bsv-low #553: consumers pin the engine crates by git rev). Its Cargo.lock"; \
	  echo "  may hold no \`worker\`: that belongs to workers/Cargo.toml."; \
	  echo "  Root-lock worker entries: $$rootw"; \
	  exit 1; \
	fi; \
	vers=$$(awk '/^name = "worker"$$/{getline; gsub(/[^0-9.]/,"",$$0); print}' workers/Cargo.lock | sort -u); \
	nv=$$(printf '%s\n' "$$vers" | sed '/^$$/d' | wc -l | tr -d ' '); \
	if [ "$$nv" != "1" ]; then \
	  echo "✗ ci-deploy: workers/Cargo.lock holds $$nv \`worker\` versions:" $$vers; \
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

# `extensions-build` is gone (bsv-low #553 lens L2): `bsv-overlay-cloudflare` has no `extensions` cargo feature, so the target could only fail; the superset is the RUNTIME var ENABLE_EXTENSIONS=true, compiled into every build.

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
