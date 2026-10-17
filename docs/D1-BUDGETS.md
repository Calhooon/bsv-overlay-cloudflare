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
| utxo-status-8 | `/utxo-status?outpoints=` A's 8 pots | 16 / 0 / 2 | 24 / 0 / 4 |
| pots-view-8 | `/pots-view?outpoints=` A's 8 pots | 8 / 0 / 1 | 12 / 0 / 3 |
| results-view-compute | `/results?identity=A`, the actor's compute (`d1view`) | 191 / 0 / 6 | 287 / 0 / 8 |
| results-forward | `/results?identity=A`, the forwarding request (`d1`) | 0 / 0 / 0 | 4 / 0 / 2 |
| owed-first-read | `/owed?identity=A`, the first read (compute on read, its write) | 363 / 18 / 28 | 528 / 20 / 28 |
| owed-served-read | `/owed?identity=A`, a served read | 17 / 0 / 2 | 26 / 0 / 4 |
| spent-any-20 | `/spent-any?outpoints=` 20 known spends of proven fleet transactions (the index-proof read) | 120 / 0 / 5 | 180 / 0 / 7 |
| submit-admit-1 | `/submit`, an operator `historical-tx-no-spv` submit admitting one tm_collected output | 0 / 13 / 7 | 4 / 15 / 9 |

| record-potparty-filing | `/record?kind=potparty`, signed stranger naming A's live pot | 11 / 7 / 6 | 17 / 9 / 8 |
| pot-changed-hook-1 | `/internal/pot-changed`, B's pot (no decoded params, two marker-named seats) | 143 / 4 / 10 | 207 / 6 / 10 |
| pot-changed-hook-decoded-8 | `/internal/pot-changed`, A's 8 decoded pots, one latched seat each | 270 / 49 / 28 | 398 / 51 / 28 |
| pot-changed-hook-marker | `/internal/pot-changed`, the undecoded pot whose markers name 5 identities | 159 / 9 / 13 | 231 / 11 / 13 |
| pot-changed-hook-8x19 | `/internal/pot-changed`, 7 decoded pots (2 seats each) + the marker pot, 19 identities with owed-state rows | 342 / 72 / 42 | 506 / 74 / 42 |
| pot-announce-drain-backlog | `/internal/pot-announce-drain`, the first pass over a backlog of 120 due and 6 exhausted parked wake-ups (four rounds of 24) | 220 / 105 / 104 | 330 / 107 / 106 |
| pot-announce-drain | `/internal/pot-announce-drain`, the second pass over the 24 due left (two rounds, the second reads none) | 55 / 25 / 29 | 110 / 35 / 31 |
| pot-changed-hook-full-refused | `/internal/pot-changed`, 8 new rooms at a full table of 2,048 live rows none may displace: refused whole, 503 (the failed batch's own reads are not reported) | 16 / 2 / 33 | 24 / 4 / 35 |
| pot-changed-hook-full-room | `/internal/pot-changed`, the same 8 rooms once 8 rows are exhausted: the room made of them (two full-table scans, largest 2,097 rows) | 4206 / 35 / 44 | 6309 / 37 / 46 |

E649 L4 (2026-10-13, lane E649-HOOK): the pot hook's own answer is held to a ceiling (it had none: leg 4 checked
only that it carried a `d1` segment). Measured with `D1_BUDGET_MEASURE=1 make ci-d1-budget` on the fixture D1:
7 reads / 0 writes / 6 statements, the SAME figure on `57751b4` (the base) with the harness change alone: the
fixture's pot `bb..:0` has no decoded params, so it takes the marker branch, which never gathered. What changed is
the round trips, which the ledger does not count: the base ran its 6 statements one after another, L4 runs them
as two batches (the decoded row and three marker arms; the two stale marks), pinned natively by
`pot_hook::tests::e649_hook_b_*`. Writes take measured + 2 (the hook is a write: a seat with an `owed_state` row
is written by its stale mark). Not in the fixture: a decoded, attributed pot (the path that gathered `/results`
per seat on the base and now reads one seat-marker batch).

E649 L4 fold (2026-10-13, the review's M1): the hook's maximum bodies and the park's drain are held too, each with a
per-statement ceiling (`maxReads`, measured: hook-1 6, decoded-8 66, marker 12, 8x19 92, drain 24; ceilings 11,
99, 18, 138, 36), on the fleet-shaped fixture (`e649_hook_rows` in the seed). Since H1 every hook scenario WRITES
its parked wake-ups in its one write batch (a park row is 3 rows written: the table, its key and the due index), so
hook-1 measures 3 writes where it measured 0. Since H2 the marker arms are read only for an outpoint that needs them,
one batch per arm: decoded-8 reads no arm; 8x19 reads the three arms over its one marker outpoint. The largest
statement of decoded-8 and 8x19 is the seat-marker read over the decoded pots (66 and 92 rows). Not measured here:
a body of eight marker-only outpoints (the review's 32 + 19 statement shape; with H2 it is 8 + 3 x 8 reads and one
write batch) and a marker skewed by 1,000 planted identities (the review's probe; data-sensitive on the base too).

E649 L4 fold-2 (2026-10-13, the delta review's N1 and N3): every hook write batch with a park now opens with ONE
statement on the one-row `pot_announce_seq` (the park tokens, N1; one write) that reads the parked table's room
under its ceiling once, a LIMITed scan of at most 2,048 rows (N3); each park reads the counter's row. At this
fixture's 126 parked rows that scan is the largest statement of every hook scenario (127 rows read), so the hook
reads grow by about the parked rows: at a FULL table a hook reads up to 2,048 more rows (about $0.000002 a hook at
D1's read price), where a row count kept by triggers would have cost two row WRITES per park and delivery (a
thousand times the price per row). The drain takes rounds while the backlog fills them (N3):
`pot-announce-drain-backlog` measures four rounds of 24 (104 statements, 24 rows at most per statement), the
second pass two (the full round, then a read of none).

E649 L4 fold-3 (2026-10-13, the second delta review's N6): a wake-up is never acknowledged unparked, so the ceiling
is held for the batch WHOLE, after its parks: the counter statement moves to after the parks and does the same one
LIMITed scan (now of at most 2,048 + the batch's parks), and two statements follow it. The GUARD reads the counter
row alone under the ceiling; over it, at most the excess (one body's 24) of the rows that may make room, and with
too few it faults the batch (D1 rolls it back; the hook answers 503). The ROOM-MAKING delete has a limit of 0 under
the ceiling; over it, it deletes the excess, the oldest exhausted rows first, then the oldest live rows that failed
at least 3 deliveries, each named LOST. So every hook batch with a park is two statements more than fold-2, the
same one scan: hook-1 10, decoded-8 28, marker 13, 8x19 42 statements, each at its ceiling. At a FULL table
(`pot-changed-hook-full-*`): a body with no room to make is refused whole (503; D1 reports no figures for the failed
batch, so its own scans are not in the ledger), and a body whose room is made of exhausted rows reads 4,206 rows
(the counter's count and the delete's ORDER BY over the rows that may make room, each about the table). Both are
the ceiling's cost only: under it the guard reads the counter row and the delete's limit is 0.

E649 L4 fold-4 (2026-10-13, the third delta review's N10): the guard's LIMIT is of ELIGIBLE rows found, not of rows
VISITED. Its scan is unindexed (the live rows' `attempts` has no index; one would cost a write per park and per
retry) and stops at the excess-th eligible row, so it visits up to the whole table. With the eligible rows first in
the scan order it reads 12 rows (`pot-changed-hook-full-room`); with them at the TAIL it reads about the table
(2,052 by the review), and the room-making body reads 6,247 rows (`pot-changed-hook-full-room-tail`, measured: the
counter's scan, the guard's and the delete's, three scans of about the table; maxReads 2,097). The REFUSAL's 16
reported reads are not its total: the failed batch's counter scan (up to 2,056 rows) and its guard, which with no
eligible row visits every row (2,059 by the review through a D1 binding), are real and unreported (the local
wrangler CLI reports no rows read, so the tier states them and does not measure them): about 4,132 rows a refusal.
So a hook at a full table reads at most three scans of the table (about 6,300 rows, under a cent per thousand
hooks); the table is bounded at 2,048 + one body's 24, and no whole-hook latency failure was measured (a refusal
answered in 5 ms, the tail body in 5 ms on the local tier).

NS2 fold (2026-10-12): the real signed filing is measured under `/record`, including the pot stale mark and the
filer mark. SHED-M1 removes the three indexed party enumeration reads entirely; named parties do no work until
their own read or armed walk. The tier pins exactly one filing walk (the filer) and one victim walk on its own
next read. The filing baseline is 11 reads / 7 writes / 6 statements on the 300-stranger fixture; the existing
margin rule gives 17 / 9 / 8. Its detached filer walk retains the `owed-recompute` ledger.

`/utxo-status` re-measured 2026-10-13 (bsv-low E649 L6, `make ci-d1-budget`, local fixture D1): 16 reads / 0 writes / 2 statements, from 8 / 0 / 1. The added statement is the eviction ledger read (`logic::evictions_by_txid_sql`, `pot_evictions WHERE txid IN (..)` on its primary key, one chunk of distinct txids per 45): one row read per distinct requested txid, so at most 64 more per request at the route's 64-outpoint cap. It writes nothing. The margin rule gives 24 / 0 / 4.

Owed first read re-measured 2026-10-12 after the #610 fold rebased onto main `13f0e66`: 363 reads / 18 writes / 28 statements (`make ci-d1-budget`, local fixture D1). The markerless cursor and candidates reads, #610, account for the two additional statements. The existing 28-statement ceiling is retained: zero statement margin; it is a bar, not a knob. Other scenario baselines above remain the 2026-10-07 measurements.

A ceiling is raised only with a new measurement (`D1_BUDGET_MEASURE=1 make ci-d1-budget` prints the table and
enforces nothing) and its reason written here. Not in this tier: the tower's case reads (`low-watchtower` lives in
bsv-low, its D1 is its own: a bsv-low follow-up, filed by CAP, for `GET /case/:gameId/:potTxid/:vout` and the
case writes beside it), and the figures on wasm at RUNTIME on Cloudflare's D1 (the tier is workerd's local
SQLite, whose `rows_read` counts the same way as far as the platform documents it; the census below is what reads
the real ones).

The identity routes of M29-3 (bsv-low #532) are inside the ledger: the picture (`/identity/pic/:hash`) and the
kill route (`POST /internal/identity/kill`) are answered before the front door but inside `serve()`, so they are
counted and stamped like every other answer. Their keys: `/identities`, `/identity` (the single and verify
forms), `/identity/pic`, `/internal/identity/kill` (listed before `/identity` and `/internal`, whose heads they
start with). They hold no D1 handle (`rows_read_ceiling_499_no_identity_route_reads_d1`, a source pin), so each
reads 0 / 0 / 0; their other stores (the resolver, the kill list) are bounded by
`rows_read_ceiling_499_store_calls_per_route`. The rows below get a tier scenario once the tier mounts a
resolver binding (it mounts none today).

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

NS2PD2-L1 (2026-10-12): `/owed` keeps the 28-statement ceiling. External
spender resolution skips `tx_any_scope`: the recompute already missed the
index and read its admitted pots and refused JOINs. That spender goes to the
couriers without another scope or held-anchor query; other callers retain
the first-party scope query. The recorded fixture baseline remains 26.

## Derived write cost (#638, D1B-L4)

Every `pot_records` UPDATE fires `derive_pot_update`: six derived statements per
updated pot (three keyed deletes and three indexed insert-selects), about ten
rows read and two to four written at the beta fixture's shape. A bulk update
multiplies that cost per affected row. The `/submit` fixture still measures 13
writes against its unchanged 15-write ceiling. These lookup rows move the join
cost to the source write; the write census must count that maintenance too.

## #638 merged measurement (D1BD-M1/L3, 2026-10-12)

On the branch rebased onto main `17418b1`, the complete merged fixture measures
`/owed` first read at **331 rows read, 18 rows written, 30 statements**. The
28-statement pin reds (30 > 28); the captain authorizes a reset to the measured
**30**, with the 528-read and 20-write ceilings unchanged. Main's markerless
reads and the branch's cursor/hydration statements are both present. The trade
is #638's original **15,323 → 311** rows-read reduction (about fifty-fold) for
two additional statements; with main's appended fixture and markerless reads,
the measured merged tip is **331** rather than the branch-only 311.

The same merged run measures `/results:view` 256 reads / 8 statements, served
`/owed` 18 reads / 2 statements, detached `owed-recompute` max 216 over 24 walks,
and 177 identical cursor/hydration pages, max 160/287 reads. Refund, recovery,
and backup reads are 143, 401, and 10. These merged figures supersede the
branch-only and inherited recorded baselines for this landing.

## `/leaderboard:view`, the bounded candidate read (bsv-low #642, 2026-10-17)

Before #642 every page walked the whole board four times (the #403 spine, the page owners' pots, the era hands,
the hands owners' pots, each a full `chain_wins_cte`): 80,189 rows over 16 statements on beta (OBS-RC-D1ROWS). The
page is now ranked over ONE candidate read, the last `LEADERBOARD_CANDIDATE_WINS` (2,000) counted wins, walked
backwards on `idx_pot_records_chain_wins` and stopped at the cap (`logic::chain_wins_candidates_sql`). The tier is
`make ci-d1-board` (a prerequisite of `ci-route`): the shipped pipeline runs natively over the fleet-shaped board
(`tests/support/d1_board.rs`) and records every statement of three requests; local D1 replays each, rows identical.

| board | request | statements | reads (largest statement) | ceiling (`maxReads` per request / per statement) |
|---|---|---|---|---|
| 1,500 wins | page 1, limit 50 | 108 | 28,403 (17,834) | 58,574 / 37,502 |
| 1,500 wins | page 1, limit 200 | 167 | 34,361 (17,834) | 70,289 / 37,502 |
| 1,500 wins | page 2 by cursor, limit 50 | 53 | 22,747 (17,834) | 58,574 / 37,502 |
| 1,500 wins | the whole-board read, page 1, limit 50 | 112 | 76,577 (20,323) | must exceed 58,574 |
| 3,000 wins | page 1, limit 50 | 143 | 39,049 (25,001) | 58,574 / 37,502 |
| 3,000 wins | page 1, limit 200 | 217 | 46,859 (25,001) | 70,289 / 37,502 |
| 3,000 wins | page 2 by cursor, limit 50 | 73 | 31,589 (25,001) | 58,574 / 37,502 |
| 3,000 wins | the whole-board read, page 1, limit 50 | 212 | 151,781 (40,697) | must exceed 58,574 |

Ceilings are the larger board's figure times 1.5 rounded up (`LEADERBOARD_VIEW_MAX_READS`,
`LEADERBOARD_VIEW_MAX_READS_PAGE_MAX`, `LEADERBOARD_STATEMENT_MAX_READS`). The candidate read is flat past the
cap (25,001 at 3,000 wins and at any larger board); the rest of a page follows its owners' pots inside the window.
Stated: the statement COUNT is the page's own (statuses, classification and markers chunked at 45 pots), unchanged
by #642 and not held to a ceiling here; #674 (`docs/DESIGN-674-LEADERBOARD-WRITE-TIME.md`) is the read whose
reads follow the page alone.

## #693 fold-2 and #723: the latch range read and the spent-any proof read (2026-10-17)

The W1-OWED-693 re-lens found two statements no scenario measured: the owed recompute's `proofmined:` latch range
read (one statement per 45 durable spenders, now with one range more for the reorg tombstone) and `/spent-any`'s
index-proof read (`hopsweep::sweep_proofs_sql`, a PK seek of at most 20 spenders). Both are plan-pinned scan-free
(`tests/d1_fleet_plans.rs`: the latch read at arity 46, the proof read at 20). Measured with
`D1_BUDGET_MEASURE=1 make ci-d1-budget` on the fixture D1:

- The fixture gives A one swept hop on its first game whose sweep is a DURABLE spender (its bytes in
  `owed_spender_bytes`) with a `proofmined:` latch past the grace (`d1_budget_seed.rs`
  `identity_a_durable_spender_rows`); the tier asserts the row is served from the latch (`confirmedSource: "proof"`)
  and A now has 9 rows. `owed-first-read` measures **382 / 20 / 39** (largest statement 160), against **340 / 18 /
  30** for the same tier without those rows: the durable-spender path is 9 statements (the spender store, the latch
  range read and the reads a swept hop brings). Ceiling by the margin: **573 / 22 / 41**, `maxReads` 573. The served
  read measures 20 / 0 / 2 under its unchanged 26 / 0 / 4. The detached `owed-recompute` maximum (held to the same
  statement figure) measured 37.
- `spent-any-20` (new): twenty fresh probe memos name twenty proven fleet transactions as spenders with no confirmed
  word, so each row is a known spend and the one proof read confirms it (`spentConfirmedSource: "index-proof"`,
  asserted). Measured **120 / 0 / 5**, largest statement 40; ceiling **180 / 0 / 7**, `maxReads` 60. Census key
  `/spent-any` (advisory in production).
