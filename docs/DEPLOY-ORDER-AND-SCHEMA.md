# Deploy order and D1 schema ownership

Two Cloudflare Workers in this repo share ONE D1 database:

| worker | crate | schema role |
|---|---|---|
| **overlay** (`bsv-overlay-cloudflare`) | `crates/overlay-cloudflare` | **owns** `OVERLAY_MIGRATIONS`; applies them via `ensure_overlay_migrations` |
| **app-layer** (`low-app-layer`) | `crates/low-app-layer` | read-only consumer; runs **no** migration list |

Written down because the ordering hazard between them is invisible from
either side's tests, and one instance of it was a total outage on four
money-visible endpoints.

## The hazard, precisely

`ensure_overlay_migrations` runs **on the overlay's cold start, not at
deploy**. `low-app-layer` has never referenced `OVERLAY_MIGRATIONS` outside
`tests/`. So a newly added column is present only after *some request has
warmed an overlay isolate* — and until then any app-layer query naming that
column fails to PREPARE with `no such column`.

That is not an operator mistake waiting to happen; it is a **cold isolate**,
which no runbook step controls. The observed shape (bsv-low #283, migration
98 `potparty_records.sigValid`): `/results`, `/recovery-view`, `/refund-view`
and `/live-view` — every recovery surface at once — return 500. Strictly
worse than the attack the column defends against, which is the trade epoch
Rule 6 exists to forbid.

## The rule

> **A column the app-layer READS must be added by a statement the app-layer
> can also ISSUE.** Adding it to `OVERLAY_MIGRATIONS` alone is not enough.

`low_app_layer::schema` implements this. Each entry in `LATCH_COLUMN_ALTERS`
is one additive, idempotent `ALTER TABLE … ADD COLUMN`, issued at most once
per isolate, with duplicate-column errors ignored exactly as `run_migrations`
ignores them, and **pinned byte-identical to the overlay's migration** by
`every_app_layer_alter_is_byte_identical_to_its_overlay_migration`. It is
deliberately *a list of independent statements*, not a migration runner —
there is no ordering, no versioning and no failure bookkeeping, because
`ADD COLUMN` has only two outcomes. The overlay owns the schema and must keep
owning it.

| column | issue | read by |
|---|---|---|
| `potparty_records.sigValid` | #283 | `/results`, `/recovery-view`, `/refund-view`, `/live-view` |
| `hopparty_records.markerValid` | #362 | `/hops-view` (its LEADING sort key) |

`markerValid` is the sharper case for the rule: `/hops-view` does not merely
sort by it, it has no other way to answer `markerVerified` at all — the
read-time verification that used to compute it (two ECDSA verifies plus a
BRC-42 derivation per row, per request, behind a 150-row budget) was deleted
when the verdict moved to admission. On a cold isolate without the column the
route is a flat 503.

Why this rather than the alternatives:

- *A deploy-order note* does not survive a cold isolate.
- *A capability probe + fallback SQL* (read-both/write-new, epoch Rule 14) is
  correct, but makes eight pure SQL-builder functions read process-global
  state, which is how a suite becomes order-dependent.
- *The additive ALTER* has no ordering in which anything is lost: whichever
  side runs first, the other is a no-op; if both race, one sees "duplicate
  column" and ignores it — the same race the overlay already tolerates across
  its own isolates.

## Recommended deploy order (still recommended, now not load-bearing)

1. **overlay** first, then send it one request to warm an isolate and apply
   migrations;
2. **app-layer** second;
3. client last, per the release runbook in `bsv-low`.

With the rule above, getting this wrong costs nothing. Without it, step 1 is
the difference between a working recovery surface and a 500.

## Checklist when adding a column

- [ ] append the `ALTER` to `OVERLAY_MIGRATIONS` (append-only — never DROP or
      RENAME; D1 migrations re-execute on every cold start)
- [ ] bump `OVERLAY_MIGRATION_COUNT` — it is a LITERAL on purpose (epoch
      Rule 9); `OVERLAY_MIGRATIONS.len()` there would move both sides of
      `migrations_are_valid_sql`'s equality at once and assert nothing
- [ ] if **any app-layer query names the column**, add it to
      `low_app_layer::schema::LATCH_COLUMN_ALTERS` (the byte-identity pin is
      driven off that list, so a new entry with no matching migration fails
      the suite)
- [ ] make it NULLABLE and decide, in writing, what a pre-migration `NULL`
      means to every reader
- [ ] if the column is a LATCHED VERDICT, file the re-latch pass with it, and
      say plainly that pre-migration rows are PERMANENT absent that pass. The
      criterion is *every row's value equals the predicate recomputed at the
      pass's own version* — never `WHERE … IS NULL`, which structurally skips
      the rows a transient predicate fault would have created (#355, #367)
