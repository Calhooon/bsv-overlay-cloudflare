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

`low_app_layer::schema` implements this for `sigValid`: one additive,
idempotent `ALTER TABLE … ADD COLUMN`, at most once per isolate,
duplicate-column errors ignored exactly as `run_migrations` ignores them, and
**pinned byte-identical to the overlay's migration** by
`the_app_layer_alter_is_byte_identical_to_the_overlay_migration`. It is
deliberately *one statement*, not a migration runner — the overlay owns the
schema and must keep owning it.

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
- [ ] bump `OVERLAY_MIGRATION_COUNT`
- [ ] if **any app-layer query names the column**, add it to
      `low_app_layer::schema` and pin the two statements equal
- [ ] make it NULLABLE and decide, in writing, what a pre-migration `NULL`
      means to every reader
