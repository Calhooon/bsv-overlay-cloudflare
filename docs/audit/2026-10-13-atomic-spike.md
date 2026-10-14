# Atomic admission spike: bsv-low #657

Branch `atomic-admission-spike`, base `970aad1291f5ea81cf9afeada0a725f218b0df48`.
Design read whole from zanaadu `c6346916:docs/audit/2026-10-13-eve/atomic-admission-design.md`.
No engine API or apply behavior changed. No publish, push or deploy.

## Harness and scope

The existing D1 route tier (`make ci-d1-budget`) uses `wrangler dev --local`;
`tools/lane-ns2/owed_do_pins.mjs` also uses Wrangler's Miniflare directly.
`tools/lane-657/d1_pins.mjs` follows that resolution pattern and runs a real
D1 binding's `batch()` inside a workerd fetch invocation, over isolated local
SQLite. This is local D1 emulation, not a hosted D1 measurement. No Rust worker
build, network service deployment or account resource is needed. Runtime read:
Miniflare `5.20261006.2-beta`, Node `22.18.0`, compatibility date `2024-12-01`
(the engine worker's date). The runner always disposes Miniflare, even on RED.

Repository rules checked: `CLAUDE.md`, `RELEASING.md`, storage ownership and the
existing harness pattern. No RULES/CAPTAIN/AGENTS page or active git hooks found;
no formal commit subject convention found, so subjects follow the issue-led log.
Fixture SQL creates only its private local tables, no engine migration.
The engine's own `D1Storage::insert_output` currently executes the output row
and transaction/BEEF row in separate calls (`d1_storage.rs:419` onward), unlike
the consumer backend described by the design. The new probe measures the JS
D1 binding's batch primitive directly, not a Rust admission unit or the
workers-rs error conversion. This spike adds no admission API.

## 1. Batch rollback and preconditions

Run with Node 22 on PATH:

```
npx --yes --package wrangler@4.150.0 -c 'node tools/lane-657/d1_pins.mjs --red-rollback'
npx --yes --package wrangler@4.150.0 -c 'node tools/lane-657/d1_pins.mjs'
```

First command was RED (exit 1): deliberately expected `[{id:1}]` after the
second statement failed; actual `[]`. Second command GREEN (exit 0): both the
first and third effects absent; the guard table empty. False (missing row and spent row) and true branches
of each working conditional guard are exercised, not just a constant failure.
The runner also asserts the error marker and the nested cause text.

Verbatim fixture schema and working conditional CHECK:

```sql
CREATE TABLE held (id INTEGER PRIMARY KEY, spent INTEGER NOT NULL);
CREATE TABLE admission_guard (ok INTEGER NOT NULL CONSTRAINT atomic_admission_precondition CHECK (ok = 1));
INSERT INTO admission_guard SELECT EXISTS (SELECT 1 FROM held WHERE id = 7 AND spent = 0);
```

The guard is statement two between `INSERT INTO effects VALUES (1)` and
`INSERT INTO effects VALUES (3)`. It always attempts one row, so a missing or
spent coin yields 0 and trips the named CHECK. A held unspent coin yields 1
and both effects commit. Production must manage successful guard rows within
the same batch (the fixture clears them between cases).

Also works (an empty scalar subquery yields NULL):

```sql
INSERT INTO admission_guard VALUES ((SELECT 1 FROM held WHERE id = 7 AND spent = 0));
```

Also works:

```sql
CREATE TABLE trigger_guard (ok INTEGER NOT NULL);
CREATE TRIGGER require_held BEFORE INSERT ON trigger_guard WHEN NEW.ok != 1 BEGIN SELECT RAISE(ABORT, 'atomic_admission_precondition'); END;
INSERT INTO trigger_guard SELECT EXISTS (SELECT 1 FROM held WHERE id = 7 AND spent = 0);
```

Does NOT abort: `INSERT INTO admission_guard SELECT 0 WHERE EXISTS (SELECT 1
FROM held WHERE id = 7 AND spent = 0)` with no held row. It inserts zero rows,
so neither NOT NULL nor CHECK runs; both effects commit. Direct `SELECT
RAISE(ABORT, 'atomic_admission_precondition')` outside a trigger fails with
`D1_ERROR: RAISE() may only be used within a trigger-program: SQLITE_ERROR`.

Observed error objects have `name: "Error"`, `message` and `cause` (also an
Error with the same message minus the `D1_ERROR: ` prefix). Messages:

- `D1_ERROR: CHECK constraint failed: atomic_admission_precondition: SQLITE_CONSTRAINT (extended: SQLITE_CONSTRAINT_CHECK)`
- `D1_ERROR: NOT NULL constraint failed: admission_guard.ok: SQLITE_CONSTRAINT (extended: SQLITE_CONSTRAINT_NOTNULL)`
- `D1_ERROR: atomic_admission_precondition: SQLITE_CONSTRAINT (extended: SQLITE_CONSTRAINT_TRIGGER)`

The dedicated constraint/trigger name is distinguishable in error text. It is
not a typed `CommitError::Precondition` from D1. The eventual backend must map
only its reserved guard marker, with a hosted and workers-rs error-format pin;
other constraint/storage errors must not be classified as preconditions.

## 2. Per-invocation query accounting

Wrangler `4.150.0` supplies the measured Miniflare version above. Commands:

```
npx --yes --package wrangler@4.150.0 -c 'node tools/lane-657/d1_pins.mjs --red-budget'
npx --yes --package wrangler@4.150.0 -c 'node tools/lane-657/d1_pins.mjs --budget'
```

The RED command exits 1: it expects enforcement at 1,000 statements, but the
single batch of 1,001 returns every `SELECT 1 AS ok` successfully. The normal
command exits 0, recording the absence of local enforcement, not asserting a
hosted accounting answer. Each row below is ONE separate Worker invocation;
all batches within the row run serially in that same invocation.

| Batches | Statements each | Successful statements | Error |
| --- | --- | --- | --- |
| 1 | 1,001 | 1,001 | null |
| 11 | 100 | 1,100 | null |
| 1,001 | 1 | 1,001 | null |

Thus local D1 enforces neither interpretation of a 1,000 query invocation
limit. **Hosted accounting, 1 versus N: UNVERIFIED.** Do not use these results
to pick a submit statement allowance or claim batch calls evade that limit.
The [D1 limits page](https://developers.cloudflare.com/d1/platform/limits/)
read on 2026-10-13 still lists 1,000 Paid / 50 Free and individual-statement
limits within batches. It does not settle this experiment. The
[batch documentation](https://developers.cloudflare.com/d1/worker-api/d1-database/)
states rollback on statement failure, consistent with the local measurement.

No hosted resource created: account/token environment variables are absent.
The owner was asked for the private credential source and an authorized
existing probe Worker. Scratch database creation/deletion is already authorized
by the brief; no further permission for that alone is needed. The no-deploy
constraint remains. The missing measurement requires a single HOSTED Worker
invocation running this budget loop against an isolated scratch D1, first
confirming its plan and configured subrequest allowance. Direct D1 REST queries
or a local Worker with a remote D1 binding do not by themselves measure the
hosted caller's per-invocation limit. No production database should be used.

Next hosted read: run width 1 and width 100 batches to the enforced boundary,
then width 1,001 in one batch, capturing completed batches/statements and the
exact thrown error. Run rollback pins against that same scratch binding, then
delete the scratch database and verify deletion. Until an authorized hosted
invocation and credential source are available, this part is blocked.

## 3. Engine witnesses, RED and retained

The engine's native MemoryStorage harness, as a private child test module
(`storage.rs:1339`), includes `src/atomic_admission_tests.rs`. This avoids adding
an API just to inspect private tables. The two synthetic topic managers model
registry TYPE 2 `[0, 2]` over a held coin and a proof topic admitting `[2]`.
If the coin disappears the registry manager returns TYPE 1 `[0]`. The lookup
hook refuses registry output 2 by payload, not by call count, on every attempt;
it accepts and indexes the other outputs. One service avoids hook-order noise.
The held parent and submitted fixture are generated, parseable BEEFs with
synthetic merkle paths and no
network access, submitted as `HistoricalTxNoSpv`, like the design's apply path.
It is not the captured Zanaadu transaction or a cryptographic proof test.

The canonical snapshot sorts all keys and serialized rows, and compares BYTES
of outputs (including spent/consumedBy), transactions/BEEF, proven flags,
applied rows, created timestamps, proof heights, sync state, peer health,
deferred graphs, yieldless streaks, the logical clock and the lookup index.
Only fault switches and instrumentation counters are excluded. No public
Storage/Engine/LookupService API or implementation behavior was changed.

Tests in `crates/overlay-engine/src/atomic_admission_tests.rs`:

- `e657_refused_submit_leaves_store_byte_equal`, line 247, ignored with
  `bsv-low #657: RED until A stages all admission writes and rolls back lookup refusal`.
- `e657_same_bytes_same_word_after_refusal`, line 277, ignored with
  `bsv-low #657: RED until A leaves a refused submit unchanged so identical bytes get the same word`.

Executed both explicitly:

```
export CARGO_TARGET_DIR=/Volumes/Lanes/targets/engine-atomic-spike
cargo test -p bsv-overlay-engine --features memory-storage --lib e657_ -- --ignored --nocapture
```

Exit 101, 2 failed at the intended assertions, not at setup or compilation:

- Byte-equality assertion at line 258: canonical snapshot before 7,495 bytes,
  after 18,952 bytes. The test first confirms exactly the intended lookup
  refusal and registry STEAK `[0, 2]`.
- Same-word assertion at line 290: first report has the registry lookup
  refusal and proof topic applied; second report has no faults, registry
  applied and proof deduplicated. The test also requires equal STEAKs and
  unchanged bytes after both calls, once report equality can pass. The same
  `TaggedBEEF` instance is passed to both submits.

These are acceptance tests, not `should_panic` tests. They must remain ignored
until A makes their laws true. When the typed refusal API lands, adapt the
fixture's current fault-site check to that typed refusal without weakening
byte equality, the second-topic coverage or same-word equality.

## The build's first PR

This spike must not freeze an API while hosted accounting remains unknown.
First complete the hosted scratch measurement and preserve its boundary/error
record. The design's first implementation PR is B: a pure lookup preflight for
admitted and spent payloads before any engine mutation, and a typed refusal in
the report, default-compatible hooks, with no-write and same-word regression
pins and documentation of the writing-manager limitation. B does not establish
whole-submit atomicity for managers that write while judging, storage failures,
or service writes; do not unignore these A witnesses merely to ship B.

A's first implementation slice should provide a staged unit with read-your-writes
and a guarded all-or-nothing commit, first in MemoryStorage and the engine's D1
backend. Include the managers' and lookup services' writes across every topic,
map only the reserved guard marker to Precondition, and run external effects
only after commit. Hold BEEF once. Turn on these exact two witnesses when they
pass; then add each statement's fault, concurrent guard loss, #559, carried
predecessors and after-commit acceptance tests from the design. No pass-through
bridge may be presented as an atomicity guarantee.

## Verification

The explicit RED witness run compiled and failed both new assertions (above).
The first normal `cargo test -p bsv-overlay-engine --features memory-storage`
run skipped both witnesses, passed 205 library tests, and failed the existing
`engine::tests::nl6e_the_script_walk_is_linear_in_a_bump_s_leaves` at its 10 s
wall-clock deadline for 4,096 proven parents (830,907 bytes). It stopped before
integration tests. This lane changes no code on that path; an isolated rerun
is recorded below. No timing bound was relaxed.


The isolated scaling-test rerun also failed at the 4,096-parent deadline.
A base comparison then temporarily used `storage.rs` verbatim from
`970aad1291f5ea81cf9afeada0a725f218b0df48` in THIS worktree, excluding the new
module (all other compiled Rust source was already base-identical). It failed
the same test's 10 s deadline at 8,192 parents; the 4,096 case completed in
4.142 s. An EXIT trap restored the committed witness include. This establishes
a base failure on this host, not a new atomic-admission regression. No sibling
worktree or main checkout was changed.

The final witness fixture uses parseable BEEF for the held parent too. Both
witnesses were rerun after that refinement and remain RED at the byte and
report equality assertions, with the final sizes/lines listed above. All four
D1 modes were rerun on the final code with pinned Wrangler: deliberately RED
rollback and budget modes exit 1 with AssertionError; real rollback and local
budget-observation modes exit 0. The rollback pin additionally checks a held
but spent row, all three error markers and their nested causes.


Remaining engine suite:

```
cargo test -p bsv-overlay-engine --features memory-storage -- --skip nl6e_the_script_walk_is_linear_in_a_bump_s_leaves
```

Exit 0: 461 passed, 67 ignored, 1 filtered out, no failures; unit tests,
integration tests and doctests complete. The excluded test
is the baseline timeout established above, not a new ignored test in the tree.
Both #657 witnesses remain ignored by their issue/reason attributes. The normal
unfiltered suite is NOT claimed green on this host. No whole-workspace, worker
route, wasm, deploy, publish or live/hosted tests were run for this test-only
spike. Formatting (`cargo fmt --all -- --check` and explicit rustfmt of the
included test file) and `git diff --check` pass.


`cargo clippy -p bsv-overlay-engine --features memory-storage --lib --tests --
-D warnings` passes. Its first run required placing the private test module
at the end of MemoryStorage (`items_after_test_module`); the module was moved,
not the lint suppressed. Formatting was corrected and checked again.

## Handoff

STATUS blocked only on the hosted D1 experiment: no hosted rollback/error shape
or per-invocation 1-versus-N measurement is claimed. Credential source and an
authorized hosted invocation compatible with no deploy are still needed. The
local probes, two retained RED witnesses and implementation-first-PR record
are complete on the branch. The engine's main checkout was not modified;
no stash, push, publish or deploy was used. The unrelated baseline timing
failure prevents claiming an unfiltered green engine suite on this host.
