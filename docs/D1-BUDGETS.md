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
  awaited outside every request and every keyed scope: the courier flush, the overlay's queue and scheduled
  handlers). Three keys are not routes: `owed-recompute` (each `/owed` walk that runs after the answer, in
  `wait_until`: the hooks', the claims' and the stale read's refresh), `(boot)` (the once-per-isolate schema
  work, below) and `/results:view` / `/leaderboard:view` (the view actor).

The WHOLE fetch body of each worker runs under the ledger: in the app layer the internal hooks
(`/internal/pot-changed`, `/internal/hop-changed`, `/internal/tip-changed`, `/internal/lobby-changed`,
`/internal/armed-pots`, keyed `/internal`) and the BRC-103 front door's own replies too, so every answer carries
the segment. A handler error that escapes the body is answered as the runtime would (500, `INTERNAL SERVER
ERROR`) with its figures stamped. The segment is stamped after signing and sealing, so it rides outside the
signature and the lane's MAC: a diagnostic, never an attested figure.

The seam is one place per worker: the overlay's `d1::Query` (every engine and discovery statement) and the app
layer's `d1_ledger::Counted` calls (`counted_all`, `counted_first`, `counted_run`, `counted_batch`). A statement
awaited through the bare `worker` API is not counted, so a new bare call is a hole no ceiling sees: count a D1
read or it did not happen. The pin `no_d1_statement_is_awaited_outside_the_ledger` (`d1_ledger.rs`, run in both
crates) reds on any `all()`, `run()`, `raw()`, `first(..)` or `batch(..)` outside that file.

The overlay applies its migrations (and the app layer its latch columns) once per isolate inside the first
request it serves. That work is keyed `(boot)`, not under the route that woke the isolate (the fixture's first
request read 5909 rows by it), so a cold isolate's `/submit` or `/lookup` is not inflated; the tier still warms
each worker before it measures.

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
bsv-low, its D1 is its own: a bsv-low follow-up, filed by CAP, for `GET /case/:gameId/:potTxid/:vout` and the
case writes beside it), and the figures on wasm at RUNTIME on Cloudflare's D1 (the tier is workerd's local
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
asks, and a shed marks it stale AGAIN (`owed::owed_shed_plan`, one one-row UPDATE), so the first read past the
window recomputes; the in-flight lock still folds twins. The second mark is the lens fold's M1: a walk in flight
when the 13th hook marked stale writes `stale = 0` over that mark (its stamp is its start), and the folded rerun
is the one shed; without the re-mark a new claimable row stayed off the page up to 15 minutes plus the next read.
Nothing new is persisted. The first read of an identity always runs (no snapshot exists to serve) and counts.
What a shed costs: a shed sends no push, so a change that lands while its identity is over the ceiling reaches the
page at the page's next read past the window (mount, a tip, a socket reconnect, an event; the page has no timer).
B4: a claimable row may be delayed, never lost. The ceiling is PER ISOLATE: hooks reach whichever isolate the
platform picks, so the fleet bound is 12 times the live isolates, and a storm is when more spin up; it brakes one
isolate's herd, it is not a D1 budget (`/health.owed.recomputesPerIdentityPerMinute` is this isolate's). A walk on
another isolate that began before a shed and lands after it can still clear the re-mark; the 5 and 15 minute age
rules then recompute. With #567 (a read-aged read serves the stored snapshot and refreshes after): that refresh can
itself be shed, with no push, so the two delays stack on one read; an inline compute that fixes #567 is a new
`owed_recompute(` caller and must ask the ceiling (the two-caller pin reds until it does). Pins:
`the_thirteenth_recompute_of_one_identity_inside_a_minute_is_shed_to_the_snapshot`,
`a_shed_rerun_after_a_walk_cleared_the_hooks_mark_leaves_the_identity_stale_for_the_next_read`,
`every_owed_recompute_caller_asks_the_ceiling_before_it_walks` (`owed.rs`), and the tier's route leg (13
announcements of B's pot inside a minute: 12 recomputes run per seat, each seat's 13th shed, every hook answer
stamped, the walks keyed `owed-recompute`).

## The split

When the census says a route's rows scale with the user count (its maximum grows as identities, pots or markers
are added while the request's own shape stays the same), that route moves to a per-identity Durable Object or to a
materialized row (M23's storage split), never to a bigger D1. A bigger D1 raises the limit everyone shares and
leaves the curve as it is; a per-identity object or a row written on change makes the read's cost a function of
one identity's rows, which is the only quantity a consumer-scale stack can hold flat. The census is the evidence:
the same route's maximum read at two populations, with the request held fixed, is the test (the CI fixture runs it
at 300 and 1200 strangers: no hot route moved). What that evidence covers: the fixture's noise fills
`pot_records` and `potparty_records` only, so it says nothing of the result and collected markers, hops,
refusals, evictions or `pot_beefs`. One `/owed` read does grow with the FLEET: `OWED_EVICTIONS_WINDOW_SQL` reads
every unreadmitted eviction of the last 24 hours (the fixture holds none). And `/owed` grows with ONE identity's
history by design (its pots, hops and filings, read in chunked IN lists): a 2,000-pot identity's first read sits
far above the advisory 528, which the split allows (a function of one identity's rows); read a census figure over
it against that identity's history before calling it a fleet curve.

## The census (before every promotion)

```
scripts/d1-census.py --overlay https://<overlay> --app https://<app-layer> --samples 5
```

prints, per surface, each route's request count and maxima beside its CI ceiling (advisory against production
rows). A ledger is one isolate's since it booted; `--samples` reads again to reach more isolates and keeps the
largest maxima. This is a SUBSTITUTE for the issue's census, accepted by CAP for now: it misses every isolate it
does not land on and reads `unscoped` as one running total. The platform's query insights
(`d1QueriesAdaptiveGroups`) are the promotion-time read, and the beta census TABLE is written by CAP after the
deploy, at the fleet's load after a loop. The promotion checklist carries the table, taken on beta at the fleet's load after a loop, and
any route over its fixture ceiling is read against the split above before the promotion goes.
