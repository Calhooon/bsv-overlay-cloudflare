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

## 1. Batch rollback and preconditions

Run with Node 22 on PATH:

```
npx --yes --package wrangler@4.150.0 -c 'node tools/lane-657/d1_pins.mjs --red-rollback'
npx --yes --package wrangler@4.150.0 -c 'node tools/lane-657/d1_pins.mjs'
```

First command was RED (exit 1): deliberately expected `[{id:1}]` after the
second statement failed; actual `[]`. Second command GREEN (exit 0): both the
first and third effects absent; the guard table empty. False and true branches
of each working conditional guard are exercised, not just a constant failure.

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
