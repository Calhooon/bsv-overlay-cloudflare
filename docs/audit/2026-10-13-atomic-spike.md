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
npx --yes --package wrangler -c 'node tools/lane-657/d1_pins.mjs --red-rollback'
npx --yes --package wrangler -c 'node tools/lane-657/d1_pins.mjs'
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
