# D1 budgets (bsv-low #499)

The overlay's engine and the app layer's brain share ONE D1 per environment: a single writer with per-query row
limits. Three incidents in three weeks were each a per-request count nobody saw until D1 refused: the callback
flood (2026-09-01, wedged re-presents fanned into per-callback writes), the overload by rows read (2026-09-07, one
header select reading 965,918 rows per call until an index landed), the t=0 burst (fleet loop 11, 18 pairs opening
at once against the owed recompute). This page is the budget each hot route now carries, how it is measured, and
what a route whose rows grow with the users does next.

## The ledger

Every D1 statement answers `meta.rows_read` and `meta.rows_written`. Both workers sum them per request
(`crates/overlay-cloudflare/src/d1_ledger.rs`; the app layer compiles the same file) and serve the sum two ways:

- on the answer: `Server-Timing: d1;desc="reads=N writes=M stmts=K"`, appended to the route's own timing (the
  overlay's `/submit` segments stay), exposed to the browser by CORS;
- on the health surface: `d1Budget` on the overlay's `/health/invariants` and the app layer's `/health`, per route
  the request count and the running maximum of each figure since the isolate booted, plus `unscoped` (statements
  awaited outside every request: the `wait_until` refreshes, the courier flush).

The seam is one place per worker: the overlay's `d1::Query` (every engine and discovery statement) and the app
layer's `d1_ledger::Counted` calls (`counted_all`, `counted_first`, `counted_run`, `counted_batch`). A statement
awaited through the bare `worker` API is not counted, so a new bare call is a hole no ceiling sees: count a D1
read or it did not happen.

The overlay applies its migrations once per isolate inside the first request it serves, so that request's figures
carry them (thousands of rows read on a fresh database); the tier warms each worker before it measures.

The view actor (`BoardView`) computes `/results` and `/leaderboard` DETACHED from any request and serves a held
copy, so the forwarding request costs D1 nothing and the compute's figures ride the answer as a second segment,
`d1view`, recorded under `/results:view` and `/leaderboard:view`.

## The ceilings (CI)

`make ci-d1-budget` (a prerequisite of `make ci-route`, so part of `make ci`) serves the app layer on a fixture D1
(real SQLite under `wrangler dev --local`, seeded by `crates/low-app-layer/examples/d1_budget_seed.rs`: identity A
with 8 unspent pots, B with 2, and 300 strangers with one pot and both seats' party rows each) and the overlay on
fresh state, drives each scenario once (`tools/lane-499/d1_budget_route_ci.mjs`) and reds BY NAME when a figure
passes its ceiling (`tools/lane-499/ceilings.json`). The noise is what makes a ceiling mean something: a query that
loses its index reads the strangers' rows.

Measured 2026-10-07 on `b1ad9a1` plus lane 499; identical at 300 and at 1200 strangers. Margin: reads measured x 1.5
rounded up, at least measured + 4; writes measured + 2, and a route that measured 0 writes keeps 0 (a read route
that starts writing reds); statements measured + 2 (a loop that reads per row over A's 8 pots passes it at once).

| scenario | route | measured reads / writes / stmts | ceiling reads / writes / stmts |
|---|---|---|---|
| utxo-status-8 | `/utxo-status?outpoints=` A's 8 pots | 8 / 0 / 1 | 12 / 0 / 3 |
| pots-view-8 | `/pots-view?outpoints=` A's 8 pots | 8 / 0 / 1 | 12 / 0 / 3 |
| results-view-compute | `/results?identity=A`, the actor's compute (`d1view`) | 191 / 0 / 6 | 287 / 0 / 8 |
| results-forward | `/results?identity=A`, the forwarding request (`d1`) | 0 / 0 / 0 | 4 / 0 / 2 |
| owed-first-read | `/owed?identity=A`, the first read (compute on read, its write) | 352 / 18 / 26 | 528 / 20 / 28 |
| owed-served-read | `/owed?identity=A`, a served read | 17 / 0 / 2 | 26 / 0 / 4 |
| submit-admit-1 | `/submit`, an operator `historical-tx-no-spv` submit admitting one tm_collected output | 0 / 13 / 7 | 4 / 15 / 9 |

A ceiling is raised only with a new measurement (`D1_BUDGET_MEASURE=1 make ci-d1-budget` prints the table and
enforces nothing) and its reason written here. Not in this tier: the tower's case reads (`low-watchtower` lives in
bsv-low, its D1 is its own), and the figures on wasm at RUNTIME on Cloudflare's D1 (the tier is workerd's local
SQLite, whose `rows_read` counts the same way as far as the platform documents it; the census below is what reads
the real ones).

The identity routes of M29-3 do not exist yet. Their rows carry a budget from birth: each lands with a scenario
here and in `ceilings.json` in the change that adds the route.

| scenario (to fill) | route | measured reads / writes / stmts | ceiling reads / writes / stmts |
|---|---|---|---|
| identities-list | `/identities` | (measured on its fixture) | (measured + the margin) |
| identity-one | `/identity/:ik` | (measured on its fixture) | (measured + the margin) |
| identity-pic | `/identity/pic/:hash` | (measured on its fixture) | (measured + the margin) |

## The brain's recompute, bounded

`/owed` is computed on write (the pot-changed, hop-changed and filing hooks) and on a stale read. Each identity may
recompute at most 12 times a minute on one isolate (`owed::OWED_RECOMPUTES_PER_IDENTITY_PER_WINDOW`, the window
fixed from its first recompute). Past it the ask is SHED to the served snapshot: nothing walks, the reader gets the
rows `owed_rows` holds, and `/health.owed.recomputeShed` counts it (`recomputeAtCeiling` names how many identities
sit at the ceiling now). The levers are the ones the brain already had: the hook marks the identity stale before it
asks and a shed walk never clears the mark, so the first read past the window recomputes; the in-flight lock still
folds twins. Nothing new is persisted. The first read of an identity always runs (no snapshot exists to serve) and
counts. What a shed costs: a change that lands while its identity is over the ceiling reaches the page at that
identity's next read past the window, not by a push. Pins: `the_thirteenth_recompute_of_one_identity_inside_a_minute_is_shed_to_the_snapshot`,
`every_owed_recompute_caller_asks_the_ceiling_before_it_walks` (`owed.rs`), and the tier's route leg (13
announcements of B's pot inside a minute: 12 recomputes run per seat, each seat's 13th shed).

## The split

When the census says a route's rows scale with the user count (its maximum grows as identities, pots or markers
are added while the request's own shape stays the same), that route moves to a per-identity Durable Object or to a
materialized row (M23's storage split), never to a bigger D1. A bigger D1 raises the limit everyone shares and
leaves the curve as it is; a per-identity object or a row written on change makes the read's cost a function of
one identity's rows, which is the only quantity a consumer-scale stack can hold flat. The census is the evidence:
the same route's maximum read at two populations, with the request held fixed, is the test (the CI fixture runs it
at 300 and 1200 strangers: no hot route moved).

## The census (before every promotion)

```
scripts/d1-census.py --overlay https://<overlay> --app https://<app-layer> --samples 5
```

prints, per surface, each route's request count and maxima beside its CI ceiling (advisory against production
rows). A ledger is one isolate's since it booted; `--samples` reads again to reach more isolates and keeps the
largest maxima. The promotion checklist carries the table, taken on beta at the fleet's load after a loop, and
any route over its fixture ceiling is read against the split above before the promotion goes.
