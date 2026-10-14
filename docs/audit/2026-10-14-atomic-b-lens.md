# Lens: atomic admission B (engine "0.1.1")

Lane `atomic-admission-b-lens`, review only. Subject: branch `atomic-admission-b` at
`4e9ba1325f5ec68536e7bb4470703d0b1ddc0b7b`, six commits over the tag
`engine-l2-74-2026-10-12` (peeled `5fb2895`). Read against the design's section 4
(`zanaadu` `origin/main:docs/audit/2026-10-13-eve/atomic-admission-design.md`
lines 1417 to 1460) and the lane's record `docs/audit/2026-10-14-atomic-b.md`.
Worktree `/Users/johncalhoun/bsv/wt-overlay-atomic-b-lens`,
`CARGO_TARGET_DIR=/Volumes/Lanes/targets/engine-atomic-b-lens` (seeded from
`engine-atomic-b`). Nothing of the subject was changed; one scratch probe test was
run and reverted (finding 2), one scratch worktree on `origin/main` was made and
removed (section 6).

## VERDICT: ACCEPT WITH CORRECTIONS

The engine change is what section 4 asks for, at the right place, and its nine
witnesses are honest: each goes RED for the reason it names and GREEN on the fix.
One correction is HIGH and blocks the release as named: the version 0.1.1 is
already taken, on main and on crates.io. Two are MEDIUM: a carried predecessor's
refusal reaches its successor untyped, and the Zanaadu door as it stands answers
a refusal-only report 503 "not now".

## Corrections

1. **HIGH. The version 0.1.1 is taken.** `crates/overlay-engine/Cargo.toml:4`
   sets `version = "0.1.1"` (replacing `version.workspace = true`) and
   `CHANGELOG.md:3` heads the entry `bsv-overlay-engine 0.1.1`. On the engine's
   main, `v0.1.1` is already tagged (`4634090`, "release: 0.1.1", 2026-10-12,
   PRs #17 to #19, no `check_*` hooks), main's workspace is at `0.1.5`, and
   `cargo search bsv-overlay-engine` answers `0.1.5`. crates.io will refuse
   0.1.1 again, and a node that pins `=0.1.1` from the registry gets a
   different engine without the hooks. Zanaadu pins `=0.1.0`
   (`overlay-lib/Cargo.toml:193`, `:240`). **Fix:** release B as the next free
   version off main (0.1.6): keep `version.workspace = true`, bump the
   workspace, put the changelog entry under `## [0.1.6]`. That carries 0.1.1
   to 0.1.5 to the line with it; if the owner wants B without them, it can only
   be a git or path pin of this branch, never a crates.io 0.1.1. The owner
   decides which; the branch's version name cannot ship.

2. **MEDIUM. A carried predecessor's refusal reaches its successor untyped.**
   `engine.rs:2605-2611` (`land_carried`): the predecessor's own submit runs its
   own checks (good: its apply writes nothing), but its report is folded into
   `Err(report.summary())`; `successors_waiting` turns that into a wait, and
   Phase 3 reports the successor's topic as a `predecessor_not_landed` FAULT.
   The outer `refused` is empty, the typed site survives only as text inside
   the fault's error string. Proven with a scratch probe (genesis landed, then
   `unproven_beef(nodes, 2, 0)` with a service refusing node 1's
   `check_admitted`):
   `refused=[] faults=[MutationFault { topic: "tm_head_chain", site: "predecessor_not_landed", error: "... submitted first from the BEEF, it did not land (...: tm_head_chain/lookup_service.check_admitted (ls_lens): lens refusal)" }] landed=[]`,
   store unchanged (`["0:spent=false"]`). `predecessor_not_landed` is the
   site Zanaadu maps to 503 not-now (`submit_rejections.rs:540`) and this
   repo's dead-letter class `not_now` (`dead_letters.rs:128`): a deterministic
   refusal is retried forever, and the door's 422 is reached only by parsing
   text, which is what B exists to stop. **Fix:** `land_carried` returns the
   inner report's `refused`; `successors_waiting` pushes them into the outer
   `report.refused` (same topic, service, site, error) and the successor's
   topic is held as refused, not waiting. Add a tenth witness of exactly this
   probe's shape.

3. **MEDIUM (Zanaadu handoff). A refusal-only report answers 503 "not now".**
   `zanaadu overlay-lib/src/submit_rejections.rs:538-541`:
   `else if report.faults.iter().all(|f| f.site == NOT_NOW_SITE)` is
   vacuously TRUE when `faults` is empty, which is every refusal-only report.
   So on the re-pin, before handoff item 3 lands, a refusal is a retryable
   503. The replay paths do the same: `pf_reconcile.rs:1076`,
   `pf_replay.rs:3623`, `pf_submit_log.rs:935` return it as an error to retry,
   and this repo's queue consumer (`crates/overlay-cloudflare/src/lib.rs:2554-2565`)
   logs "0 fault(s)" and `msg.retry()`s it with dead-letter class `fault`.
   Not reachable today (no service implements a check), reachable the moment
   one does. **Fix:** the handoff's item 3 must read: test `refused` FIRST,
   before the not-now arm; items 1 and 3 ship in one Zanaadu release; every
   replay or queue consumer treats a refusal as terminal (ack or dead-letter
   as refused, never retry). Add one sentence to `MutationReport::is_durable`'s
   doc (`engine.rs:617`): a caller classifies `refused` before `faults`.

4. **LOW. A refused topic's STEAK still lists its outputs.** The witness
   asserts it (`atomic_admission_tests.rs:268`,
   `steak[REGISTRY].outputs_to_admit == [0, 2]`). `Engine::submit`
   (`engine.rs:1476-1495`) returns that STEAK `Ok` with only a warn, and
   `sync_advertisements` (`engine.rs:4684`) reads it as what was advertised.
   This is the existing fault convention, and `refused` is engine-internal
   (`MutationReport` and `AdmissionRefusal` derive no `Serialize`; nothing of
   it reaches the wire), so the wire is unchanged. But the record's "STEAKs
   unchanged" should say it: for a refused topic the STEAK is the judgement,
   not an admission. **Fix:** one doc line on `Engine::submit` and on
   `MutationReport::refused`; clearing the refused topic's STEAK entry would be
   a behavior change and is the owner's call.

5. **LOW. The record does not name every effect before the checks.** Named
   in section 1 below. Of those, two are missing from the record: Phase 2's
   ARC broadcast and SHIP propagation (`engine.rs:3645` onward, `CurrentTx`
   only) run before the checks, so a refused transaction is still broadcast
   and propagated; and after a carried predecessor lands, the successor is
   re-validated with a REAL context (`engine.rs:2424-2433`), a second
   judge-time manager call. **Fix:** add both to the record; handoff item 2
   (pf head checks inside judgement) must hold on that re-validation too.

6. **LOW. Purity is documented, not enforced.** See section 2. **Fix:** none
   in the engine; say in the handoff that the per-service table tests and the
   D1 byte-equality fixture are the only things that can catch a check that
   drifts from its hook.

7. **LOW, pre-existing class. GASP finalize retries a refusal every sync.**
   `engine.rs:5572-5590`: a refused finalize submit is "not durable", stops
   the rest of its graph and sets `all_landed = false`. Before B the same body
   hook-faulted the same way; B makes it classifiable, so GASP could treat it
   as terminal later. Note only.

8. **LOW. "Additive" holds for every known consumer, not strictly by semver.**
   A new pub field on the exhaustive pub struct `MutationReport`, and new
   `Clone, PartialEq, Eq` on `LookupServiceError` (they constrain future
   variants). Every struct literal found uses `..Default`: this repo
   `beef_door_replay.rs:414`, `:830`; Zanaadu `submit_policy.rs:77`,
   `submit_rejections.rs:1121`, `:1606`, `:1617`, `routes.rs:6141`. **Fix:**
   say "additive for all known consumers" in the changelog.

## 1. The call site (read)

`submit_counted_inner`: `run_validation` (Phase 1 judge, Phase 2 broadcast),
`successors_waiting` (lines 1684-1702), the carried predecessor's real
re-judgement (1709-1732), then preflight: `check_spent` at **1756**,
`check_admitted` at **1770**, the sort at 1783, and PHASE 3 at **1788**. The first
apply write is `mark_utxo_as_spent` at 1876-1883; with no previous coin, the
later `insert_output`. No storage write is made by the engine itself between
the start of the submit and line 1788 except through the items below (a grep of
every write method over those lines found none):

- **Manager writes at judge time** (the design's known exception, section 1):
  `validate_topic` with the default (real) context inside `run_validation`
  (`engine.rs:3626`); for Zanaadu, the pf managers' `advance_head_and_record`.
  Again on the re-validation after a carried landing (`engine.rs:2424-2433`).
  For a carried predecessor's own submit, its real re-judge (`engine.rs:1711`).
- **A carried predecessor's landing**: a full nested submit, its own Phase 3
  included, inside `successors_waiting` (`land_carried`, `engine.rs:2517`). Its
  own checks run before its own apply; what it lands stays if the successor is
  then refused (it is its own valid admission).
- **Outward, not store writes**: Phase 2's ARC broadcast and SHIP peer
  propagation (`CurrentTx`), the caller's `landing_guard`, and the in-memory
  `not_landed` set (`hold_unapplied`, `engine.rs:2626`, a `RefCell`, not storage).

Sibling isolation (`e657_refused_topic_skips_apply_while_sibling_lands`): the
Phase 3 loop skips dupes, then failed, then `refused_topics.contains` calls
`hold_unapplied` and `continue`s (`engine.rs:1804-1807`), before any write of
that topic; the sibling's iteration is untouched. The test submits
`[REGISTRY, PROOF]`, asserts the registry's previous coin is held unspent, one
registry row, one proof row, applied rows only for PROOF, and one LS index row
written once for PROOF. It goes RED with only the preflight disabled
(`refused topic lost its unspent previous coin`). The preflight's skip set
(dupe, failed, read fault, waits) equals Phase 3's, and its spending input index
(`v.previous_coins[prev_idx]`) equals Phase 3's (`engine.rs:1893`).

## 2. Purity (read)

Documented only (`lookup_service.rs:58-80`). The one real enforcement is the
signature: synchronous `&self`, so on Workers no D1, R2, queue or fetch call can
be AWAITED inside a check. Not prevented: interior mutability (the witnesses'
own probes write through `Mutex`), a fire-and-forget `spawn_local` doing I/O, a
panic (it aborts the submit after the manager's judge-time writes), and the
real risk, a check that accepts what its hook refuses (back to the partial
admission) or refuses what it accepts (liveness). No engine test can catch any
of these for a real service; only Zanaadu's per-service table tests and the
`8cc1e1b4` D1 byte-equality fixture can (handoff items 1 and 4).

## 3. The report (read and run)

`MutationReport::refused: Vec<AdmissionRefusal { topic, service, site:
&'static str, error: LookupServiceError }>`, sorted by (topic, service, site),
separate from `faults`; `is_durable()` false if either is nonempty; `summary()`
appends `topic/site (service): error`. Engine-internal: neither struct derives
`Serialize`, so it never reaches the STEAK on the wire (finding 4). A door reads
`report.refused[i].site`, never text; the one gap is finding 2. On
non-refusing paths every existing word is unchanged: no file under
`crates/overlay-engine/tests/` changed (`git diff --stat` of the tag to HEAD over
that directory is empty), and the full suite is green over it (section 5); the
e592 witness prints `refused: []` inside an otherwise identical report.

## 4. The witnesses (run)

GREEN at `4e9ba13`: `cargo test -p bsv-overlay-engine --lib e657_`: **9 passed,
0 failed, 0 ignored**. RED, each pin's tests grafted onto its parent's source:

| Pin | Parent source | RED quoted |
| --- | --- | --- |
| `0db0ec4` hooks | tag `5fb2895` + its lookup test | `E0599: no method named check_admitted` / `check_spent` (lookup_service.rs:252, :253 at the graft point) |
| `57b2a7e` preflight | `0db0ec4` + its test file and include | 1 passed, 5 FAILED: `:377:9 one admit check per output and service left 0 right 2`; `:297:5 identical bytes changed the word` (first report a notify fault, second an applied topic); `:407:9 real apply after dry run must check left 0 right 2`; `:266:5 refused submit changed stored bytes (before 7495, after 14005)`; `:418:5` spent refusal |
| `301126a` report | `57b2a7e` + its test file | `E0609: no field refused on type &MutationReport` (first at :246:16, the shared `refused()` helper; the record cites :436) |
| `557a517` ordering | itself, preflight loop disabled (`filter(\|_\| false)`) | 1 passed, 8 FAILED, incl. `:586:9 every topic checked before any apply left 0 right 3` and `:607:5 refused topic lost its unspent previous coin` |

`301126a` alone: 7 passed. `557a517` restored: 9 passed. Every runtime RED of
the record reproduces, same lines, same byte counts.

Byte equality is the whole store: `snapshot` (`atomic_admission_tests.rs:136`)
serializes every persisted `MemoryStorage` table (outputs, transactions, proven,
applied, sync_state, created_at, peer_health, proven_at, deferred_graphs,
peer_yieldless, clock_secs) plus the LS index, each sorted canonically. It omits
only the seven `deferred_graph_*` counters and fault switches, which are
instrumentation, not stored rows.

## 5. The release (run)

- Diff, tag to HEAD: 9 files, the change, its tests, the changelog, the version
  in `crates/overlay-engine/Cargo.toml` and both locks (version line only). No
  file of `overlay-cloudflare`, `low-app-layer`, `overlay-discovery` or the
  harness changed. Public API: a default method pair, a new report field, a new
  struct, three derives (finding 8). The version itself: finding 1.
- `CARGO_PROFILE_TEST_OPT_LEVEL=1 RUST_TEST_THREADS=1 cargo test --workspace`:
  **1,611 passed, 0 failed, 66 ignored** (confirmed). The 66: `live_worker` 34,
  `wallet_e2e` 15, `agent_e2e` 8, `live_smoke` 5 (all need a deployed overlay
  or wallet), 2 ignored `bsv_overlay_engine` doc-tests, 1 UHRP live parity
  (`UHRP_LIVE_PARITY=1`, discovery), 1 fixture writer
  (`script_verification`, "writes the committed lane fixtures ... run on purpose").
  Log `/tmp/lens-atomic-b-workspace.log`.
- `cargo clippy --workspace --all-targets -- -D warnings`: pass.
  `cargo fmt --all -- --check`, `rustfmt --edition 2021 --check` on the included
  test file, `git diff --check` over the range: pass.
- `cargo build -p bsv-overlay-engine --target wasm32-unknown-unknown --release`:
  pass. `make ci-deploy`: preflight ok (worker 0.8.5, worker-build ^0.8, 3
  configs), dry runs ok for `overlay-cloudflare/wrangler.toml`,
  `wrangler.low.toml`, `low-app-layer/wrangler.toml`. No deploy.
- e592: `cargo test -p bsv-overlay-engine --test engine_walk_budget --
  --nocapture`: 10 passed; NotNow peak 13,312,995 (7.0x), NetworkAccepted peak
  15,211,900 (8.0x) for the 1,900,242-byte body, estimate 995,603,656 against
  the unchanged 50,331,648 budget: the record's numbers exactly. The ledger is
  unchanged (no test file touched).

## 6. The keeper's cherry-pick (run)

`git merge-tree --write-tree origin/main HEAD` (main `ec2a50d`, 241 commits past
the tag): engine.rs and storage.rs auto-merge; **conflicts in `CHANGELOG.md`,
`Cargo.lock`, `crates/overlay-engine/Cargo.toml`, `workers/Cargo.lock`**, all
version metadata from `ba90023` (main is at workspace 0.1.5). Scratch
cherry-pick of the six commits onto `origin/main` in a throwaway worktree:
`0db0ec4`, `57b2a7e`, `301126a`, `557a517`, `4e9ba13` clean; `ba90023` conflicts
in exactly those four files (resolved to main's side for the scratch). On the
result: e657 **9 passed**; `CARGO_PROFILE_TEST_OPT_LEVEL=1 cargo test -p
bsv-overlay-engine --features memory-storage` **476 passed, 0 failed, 65
ignored**; `cargo clippy -p bsv-overlay-engine --all-targets --features
memory-storage -- -D warnings` pass. The keeper resolves the four files as
finding 1's fix (workspace version, the next free number, the changelog under
it). Scratch worktree and its target removed.
