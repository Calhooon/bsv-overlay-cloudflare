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
| utxo-status-8 | `/utxo-status?outpoints=` A's 8 pots | 24 / 0 / 3 | 36 / 0 / 5 |
| pots-view-8 | `/pots-view?outpoints=` A's 8 pots | 8 / 0 / 1 | 12 / 0 / 3 |
| results-view-compute | `/results?identity=A`, the actor's compute (`d1view`) | 191 / 0 / 6 | 287 / 0 / 8 |
| results-forward | `/results?identity=A`, the forwarding request (`d1`) | 0 / 0 / 0 | 4 / 0 / 2 |
| owed-first-read | `/owed?identity=A`, the first read (compute on read, its write) | 363 / 18 / 28 | 528 / 20 / 28 |
| owed-served-read | `/owed?identity=A`, a served read | 17 / 0 / 2 | 26 / 0 / 4 |
| spent-any-20 | `/spent-any?outpoints=` 20 known spends of proven fleet transactions (the index-proof read) | 140 / 0 / 5 | 210 / 0 / 7 |
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

`/utxo-status` and `/spent-any` re-measured 2026-10-18 (bsv-low #617, the LOW words map L1, `make ci-d1-budget`, local fixture D1): `/utxo-status` 24 reads / 0 writes / 3 statements, from 16 / 0 / 2; `/spent-any` 140 / 0 / 5, from 120 / 0 / 5 (its `maxReads` 40 unchanged). Each entry now serves `fundingWord`, the tracker word of the outpoint's own creating txid, so the word read (`tracker_host::stored::READ`, `tx_words` by primary key over `json_each`) names the entries' txids beside their spenders: one row read per distinct requested txid that holds a word, at most 64 more at the route's 64-outpoint cap. `/utxo-status` had no word read before a spender existed, hence its added statement; `/spent-any` already read words for its spenders, so its statement count holds. Neither writes. The margin rule gives 36 / 0 / 5 and 210 / 0 / 7.

Owed first read re-measured 2026-10-12 after the #610 fold rebased onto main `13f0e66`: 363 reads / 18 writes / 28 statements (`make ci-d1-budget`, local fixture D1). The markerless cursor and candidates reads, #610, account for the two additional statements. The existing 28-statement ceiling is retained: zero statement margin; it is a bar, not a knob. Other scenario baselines above remain the 2026-10-07 measurements.

D2-OWED-READ-FOLD (2026-10-25, ruling 1, bsv-low record 1277): the focused
`OWED_READ_ONLY=1 make ci-d1-budget` pin serves 500 stored rows judged from 250 results and 270 hops.
The backfilled read measures 2000 reads / 0 writes / 3 statements, maxReads 1999;
its ceiling remains 3000 / 0 / 3, maxReads 2006, with zero courier calls and background tasks.
An unbackfilled read writes one wanted mark: exactly one INSERT, at most five physical D1 rows
(the row, its indexes and the AUTOINCREMENT sequence). D1 measures five, so ruling 1 replaces
the former one-physical-write ceiling with five. The full request's four reads plus that INSERT
remain recorded separately in `requestMeasured`; the wanted ceiling counts the INSERT alone.
The pin checks D1 statement metadata, the single application mark, zero courier calls and zero
background task registrations. The additional 500-row, 750-word-role stress measurement
(3250 reads, maxReads 3249) remains separate from the backfilled ceiling.

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
## The hot passes' reads, bounded (bsv-low #643, #644, #645, W1.6, 2026-10-17)

Measured on the budget tier's fleet fixture in local D1 (`d1_budget_seed`, then `tools/lane-499/cursor_pin.mjs`,
which `make ci-d1-budget` runs): 22,591 `pot_records` rows (6,944 unspent) and 27,000 `network_seen` latches.
Rows are D1's own `rows_read` for the shipped statement. Before is `154e53b`.

| issue | statement | before | after | ceiling |
|---|---|---|---|---|
| #644 | the signers backfill's candidates (`d1_discovery::settle_signers_candidates_sql`, each tick) | 22,591 | 2 | 6 |
| #644 | the payouts backfill's candidates (`d1_discovery::spender_payouts_candidates_sql`, each tick) | 22,591 | 2 | 6 |
| #643 | the app layer's tip mark (`owed::OWED_STALE_ON_TIP_SQL`, once per block), its subquery at a block whose window holds no pot | 6,944 | 1 | 5 |
| #645 | `/health/invariants.networkSeenTotal` (`ops::NETWORK_SEEN_TOTAL_SQL`) | 27,000 | 1 | 5 |
| #645 | the count's backfill on a migration re-run, its SELECT | | 1 | 5 |

How. #644: two partial indexes (`idx_pot_signers_pending`, `idx_pot_payouts_pending`) hold exactly the rows each
pass can still change, under the pass's own predicates, so the read is the pending set (none on the fixture) and a
latched row leaves it; the SQL is unchanged. #643: `idx_pot_unspent_recovery` on `(spent, recoveryHeight)`, partial
on `recoveryHeight IS NOT NULL`, makes the mark an index range over the block's seven-block window; at a block whose
window holds every one of the fixture's 6,944 unspent pots the mark reads 19,927 rows, the same rows as before (the
pots it must mark and their party rows). #645: `count_network_seen`, one row kept by `network_seen`'s insert and
delete triggers and backfilled once behind its own row.

Pins: `crates/low-app-layer/tests/d1_hot_bounds.rs` (the answer equals the same SQL over the old plan, the table
read `NOT INDEXED`; no fleet scan, with and without `ANALYZE`; the count equal to `COUNT(*)` through a backfill, the
latch's repeats and deletes and a re-run of the list), each RED on `154e53b`; the four statements in
`d1_fleet_plans.rs` `hot_routes_never_scan_fleet_tables`; the D1 ceilings in `tests/support/cursor_measure.rs`.
Stated: the two backfill candidate reads were measured with no row pending (the fixture holds none); with a backlog
they read the backlog plus about two. The write cost: a `pot_records` row with a recovery height writes one more
index entry at its insert and at its spend, a settle verdict and a spend of a decoded pot enter one partial index
until their pass latches them, and every `network_seen` latch writes the count's row. The route tier's write
ceilings were not re-measured in this lane unless the gate below says so.

## `/leaderboard:view`, the board tables (bsv-low #674, 2026-10-17)

The ranking is stored at the write (`derived_board.rs`: `board_wins`, `board_owners`, `board_hands`, `board_total`,
kept by triggers in the settle hooks' own statements). The page reads `board_owners` by its rank index, the page
owners' `board_wins` rows by owner and the hands by `board_hands`' rank index; steps 3 to 6 (statuses,
classification, markers, the #685 decoration) are unchanged. The candidate read's 2,000-win window is gone: the body
ranks every counted win (`candidatesBounded: false`, `candidates` the era's count). Same tier, same boards; the
requests are the SHIPPED dispatcher's (the table read), #642's candidate read and the whole-board read recorded
beside them.

| board | request | statements | reads (largest statement) | ceiling (`maxReads` per request / per statement) |
|---|---|---|---|---|
| 1,500 wins | page 1, limit 50 | 113 | 11,801 (987) | 33,879 / 2,757 |
| 1,500 wins | page 1, limit 200 | 174 | 19,004 (987) | 52,343 / 2,757 |
| 1,500 wins | page 2 by cursor, limit 50 | 59 | 5,771 (492) | 33,879 / 2,757 |
| 1,500 wins | #642's candidate read, page 1, limit 50 | 108 | 28,403 (17,834) | record |
| 1,500 wins | the whole-board read, page 1, limit 50 | 112 | 76,577 (20,323) | must exceed 33,879 |
| 3,000 wins | page 1, limit 50 | 213 | 22,586 (1,838) | 33,879 / 2,757 |
| 3,000 wins | page 1, limit 200 | 293 | 34,895 (1,838) | 52,343 / 2,757 |
| 3,000 wins | page 2 by cursor, limit 50 | 114 | 11,841 (928) | 33,879 / 2,757 |
| 3,000 wins | #642's candidate read, page 1, limit 50 | 143 | 39,049 (25,001) | record |
| 3,000 wins | the whole-board read, page 1, limit 50 | 212 | 151,781 (40,697) | must exceed 33,879 |

The table above is the read as first built (`eaf122e`): NOT flat. The body carries every counted win of every owner
on the page (the `chainWins` anchors and the evidence), and the page read each owner's `board_wins` rows and then the
statuses, classification, markers and proof decoration of every one of those pots: about 12 reads a page-owner win
(the default page's 50 owners hold 945 counted wins at 1,500 and 1,843 at 3,000), past the default ceiling at about
2,700 such wins and past #642's own read at about 3,250 (the #674 lens, MEDIUM-1).

### The lens fold: the page reads the owners' cards (flat)

Each `board_wins` row carries `frag`, the rows the page tail reads for that win (its status and decoded `pot_records`
columns, its first four result markers, each marker's proof pointers and posted bundle), and each `board_owners` row
carries `card`, its wins and their frags (`derived_board.rs`; the triggers keep both, the drift pin holds each frag
equal to the app layer's own statements run from scratch and each card equal to its owner's wins). The page reads
the owner page, one card row per page owner, the hands, and the hands' pots beyond the page: never the page owners'
per-win rows. An owner whose card does not hold every win (a card past 1,500,000 bytes, a frag past 200,000) is read
by its wins as before.

| board | request | statements | reads (largest statement) | ceiling |
|---|---|---|---|---|
| 1,500 wins | page 1, limit 50 | 8 | 323 (100) | 728 / 600 |
| 1,500 wins | page 1, limit 200 | 8 | 978 (400) | 1,505 / 600 |
| 1,500 wins | page 2 by cursor, limit 50 | 9 | 485 (100) | 728 / 600 |
| 3,000 wins | page 1, limit 50 | 8 | 323 (100) | 728 / 600 |
| 3,000 wins | page 1, limit 200 | 8 | 1,003 (400) | 1,505 / 600 |
| 3,000 wins | page 2 by cursor, limit 50 | 9 | 467 (100) | 728 / 600 |

The default page reads 323 rows on both boards (asserted equal by `ci-d1-board`'s `flat.json`): a constant of the page
(the owner page, its cards, the hands and their off-page pots), whatever its owners' wins. Limit 200 and the cursor
page differ between the boards by the hands' off-page pots and the cursor's rank (one index row per owner ranked above
it, NOTE-2), both bounded by the page and the owner count, not the wins. Ceilings: the larger figure times 1.5 rounded
up (`LEADERBOARD_VIEW_MAX_READS` 728, `_PAGE_MAX` 1,505, `LEADERBOARD_STATEMENT_MAX_READS` 600); #642's candidate read
(28,403 and 39,049) and the first table read (11,801 and 22,586) are each past them. The writes: each
settle-hook statement adds its triggers' statements, each keyed by the written row (`EXPLAIN QUERY PLAN` pinned in
`d1_fleet_plans.rs`, `the_board_table_read_and_its_triggers_never_scan`); not measured on D1 here. Since the fold a
counted win's insert also builds its frag (its pot's markers, at most four proof pointers and one posted bundle per
marker) and writes its owner's card; a result marker, proof pointer or posted bundle write refreshes the frags of the
counted pots it names, and a frag-only `pot_records` column (`spentHeight`, the decoded params) its pot's frag. The
`ci-d1-budget` tier's per-hook write ceilings were measured before the fold and are owed a re-run.

### The cutoff inside the derivation (bsv-low #674, lane W2-LB-674-CUTOFF, 2026-10-18)

Beta and prod both set `WRITTEN_OFF_BEFORE_MS`, and the route read the tables only with no cutoff, so the table read
above served nothing on either: beta's `?limit=50` stayed at 34,077 rows (`docs/audit/OVERLAY-WINDOW-W2-2026-10-17.md`
in bsv-low). The board is now derived under the cutoff the app layer's scheduled pass stamps into `board_era`
(`derived_board.rs`: `board_win_era` refuses a win before it, `board_era_update` re-derives the board when it moves),
and the route reads the tables when the stamp is its own cutoff. The ready statement reads the `board_era` row twice
(4 rows where it read 2), so the uncut default page is 325. `ci-d1-board` replays each board again under a cutoff kept
from its `2/5 * wins`th win on, stamped as the seed's last statement (the re-derivation runs on local D1):

| board | request | statements | reads (largest statement) | ceiling |
|---|---|---|---|---|
| 1,500 wins | page 1, limit 50 | 8 | 325 (100) | 728 / 600 |
| 1,500 wins | page 1, limit 200 | 8 | 980 (400) | 1,505 / 600 |
| 1,500 wins | page 2 by cursor, limit 50 | 9 | 487 (100) | 728 / 600 |
| 3,000 wins | page 1, limit 50 | 8 | 325 (100) | 728 / 600 |
| 3,000 wins | page 1, limit 200 | 8 | 1,005 (400) | 1,505 / 600 |
| 3,000 wins | page 2 by cursor, limit 50 | 9 | 469 (100) | 728 / 600 |
| 1,500 wins, cut (900 era wins) | page 1, limit 50 | 8 | 304 (100) | 728 / 600 |
| 1,500 wins, cut | page 1, limit 200 | 7 | 875 (400) | 1,505 / 600 |
| 1,500 wins, cut | page 2 by cursor, limit 50 | 9 | 473 (100) | 728 / 600 |
| 1,500 wins, cut | #642's candidate read, page 1, limit 50 | 68 | 17,851 (11,386) | record |
| 3,000 wins, cut (1,800 era wins) | page 1, limit 50 | 8 | 303 (100) | 728 / 600 |
| 3,000 wins, cut | page 1, limit 200 | 8 | 1,005 (400) | 1,505 / 600 |
| 3,000 wins, cut | page 2 by cursor, limit 50 | 9 | 505 (100) | 728 / 600 |
| 3,000 wins, cut | #642's candidate read, page 1, limit 50 | 133 | 35,450 (22,807) | record |

The uncut default page is still asserted equal across the boards (325). Under the cutoff the two boards' default
pages differ by one row (304 and 303), in the statuses of the hands' pots beyond the page: one of the 1,500 board's
twelve has a witnessed spender, so its `network_seen` LEFT JOIN finds a row. That is the off-page hands' data, not
the page owners' wins, so the cut pair is held flat over the page's own five statements (the ready read, the owner
page, the cards' two chunks and the hands: 255 reads on both, `flatHead`). The writes: every `board_wins` insert
asks `board_win_era`'s condition (the `board_era` row by its key, twice; `EXPLAIN QUERY PLAN` pinned); a write that
derives a win before the cutoff still computes the row the guard then refuses (a written-off pot is rarely written);
a cutoff's move re-derives the board in one statement (`board_era_update`, the cost of the `board-v1` backfill over
the era's wins), once per move and only on the scheduled pass.

## The census's per-tick whole-table reads, bounded (NS-COST-BAR-1, the owner's cost word of 2026-10-18)

Source: the 24-hour D1 census of 2026-10-17 (2026-10-17T00:03:52Z to 2026-10-18T00:03:52Z, bsv-low
`docs/audit/D1-CENSUS-{PROD,BETA}-24H-2026-10-17.md`): prod read 73 M rows that day, beta 122 M. Measured on the
budget tier's fleet fixture in local D1 (`d1_budget_seed`, then `tools/lane-499/cursor_pin.mjs`, which `make
ci-d1-budget` runs; each line prints D1's own `rows_read`, the before shape beside the shipped one). The fixture now
carries beta's shapes: 20,837 `pot_beefs` rows over every latch, 21,724 `transactions` (2,719 retired), 221 watched
txids over every GC arm. Before is `221a203` (PR #86).

| statement (file) | census, beta / prod, rows a day | fixture before | fixture after | ceiling |
|---|---|---|---|---|
| the proofless-watch GC (`ops::PROOFLESS_WATCH_GC_SQL`, each tick), its read set | 30.8 M / 2.6 M | 63,638 | 389 | 1,000 |
| `retiredUnprovableTotal` (`ops::RETIRED_UNPROVABLE_TOTAL_SQL`, each `/health/invariants`) | 10.4 M / 0.15 M | 20,839 | 1 | 5 |
| `txRetiredTotal` (`ops::TX_RETIRED_TOTAL_SQL`) | 7.5 M / 0.54 M | 17,438 | 1 | 5 |
| the expired-unproven total (`proof_window::EXPIRED_COUNT_SQL`) | 9.5 M / 0.37 M | 17,438 | 1 | 5 |
| the index janitor's backlog (`ops::INDEX_JANITOR_BACKLOG_SQL`), as of two hours on | 5.2 M / 0.12 M | 13,887 | 8 | 10 |
| the hopparty relatch census (`d1_discovery::hopparty_relatch_census_sql`), mid-sweep | 1.1 M / 0.11 M | 7,552 | 3,777 | 5,000 |
| the potparty relatch census (`potparty_relatch_census_sql`), mid-sweep, the fixture's worst case | 1.4 M / 0.15 M | 15,360 | 23,018 | 25,000 |
| each count's backfill on a re-run, its SELECT | | | 1 | 5 |

How. The GC's four filtered `IN (SELECT ..)` lists were each a scan of a store, rebuilt every tick; they are two
primary-key `EXISTS` probes of each watched txid (the orphan arm already probed the key), the same rows deleted.
The three counts are trigger-kept rows (the #645 shape), but `pot_beefs` and `transactions` are written by `INSERT
OR REPLACE`, whose implicit delete fires no delete trigger unless `recursive_triggers` is on (measured: a +1/-1 row
reads 2 for 1 row), so each source's triggers keep a MEMBERSHIP set (delete the key, then insert it while the row
qualifies: right either way) and the count row is kept by the set's own two triggers. The backlog's predicate is the
clock's (an hour old): three class rows of unspent pots kept by `pot_records`' triggers (no REPLACE there, #638) less
the hour's young pots, an index range (`idx_pot_unspent_created`, `INDEXED BY`: without statistics the planner took
every unspent pot by `spent = ?`). The relatch census is a rowid range plus a partial index of the NULL tier.

Stated. (1) The backlog is measured two hours on because the seed stamps every pot this hour; read at once, the
young range holds every unspent fixture pot. (2) The relatch census still reads the rows after its cursor, half the
table on average over a sweep (it feeds the relatch log line only); its NULL tier is a partial index, so the census is
cheaper only where the tier is small: production's `/record` writes `sigValid` and `markerValid` at filing and the
relatch converges the legacy rows, but the fixture's potparty rows carry none, so there the census is worse than the
aggregate (23,018 against 15,360). (3) The write cost: a `pot_beefs` or `transactions` write probes its set by key
and writes the set and the count row only when the row's latch moves; a `pot_records` insert, delete, spend or class
change writes one or two class rows; the four relatch tables and the young pots each enter one partial index. (4) A
row whose txid is NULL is not in a count (no writer writes one). (5) The two sets and their count rows are derived:
wiped together or not at all (`storage-ownership.json`).

### The rows-read-per-day invariant

The overlay counts its own rows read durably: `d1_rows_read_<family>_total`, one per tick step (the 19 labels in
`tick.rs`, `setup` before the first) and one for `/health/invariants`, from D1's `meta.rows_read` at the one seam
(`d1_ledger::passes`, `label`), served as counters and as `/health/invariants.d1RowsRead {byFamily, total}`. A day's
figure is the delta of a day. The budget is `tools/lane-499/ceilings.json` `d1RowsReadPerDay`: prod 20 M and beta
60 M rows a day over LOW's own databases, the keeper's `rust-chaintracks` reported beside and never inside, with the
census's 3x rule beside it unchanged. Why: the census's listed tags held 8.9 M a day on prod `low-overlay-db`, about
3.8 M after PR #86 and this lane (20 M is about 5x, room for the 105 unlisted tags and the 3x margin); beta 105.5 M
over its LOW databases, about 16.7 M after (60 M is about 3.5x). Work a step hands to `wait_until` is polled outside
the scope and is no pass's.

### The keeper's two statements (rust-chaintracks, filed by the captain)

`SELECT COUNT(*) as cnt FROM headers` (`storage.rs` `SQL_COUNT_HEADERS`, `get_info`, every `/getInfo`) read 971,547
rows each time: 61.2 M rows a day on prod at 63 calls, 1.9 M on beta at 2. Its callers on LOW's side are the
monitor's chaintracks lag row (bsv-low #631, `workers/low-monitor/src/lib.rs` `read_chaintracks`, one `/getInfo`
every `*/5` tick through the `CHAINTRACKS` binding to prod `rust-chaintracks`, deployed about 22:13Z on 2026-10-17,
so the census saw about 22 of its calls), the runner's `scripts/runner/patterns/chaintracks-gate.py` and the fleet
cell `app/e2e/matrix/invariants.mjs`. At the monitor's full cadence (288 a day) the count alone is about 280 M rows
a day. The monitor reads `heightLive`, `liveLagBlocks`, `lastSyncError` and `syncSchemaFault`, which only `/getInfo`
serves, so no cheaper field exists on LOW's side: the ask is the keeper's. Measured on local D1: `COUNT(*)` over
2,000 headers reads 2,000 rows, `MAX(header_id)` (the INTEGER PRIMARY KEY) reads 1.

`UPDATE chain_split_signal SET depth = ?, tick = tick + 1 WHERE id = 1` (`rungs.rs`, each rung observation) read
14,472 rows each time (13.2 M a day on beta at 914 calls, 2.8 M on prod at 193): its `AFTER UPDATE OF tick` trigger
`chain_split_detect` (`migrations/0010_rung_tips.sql`) evaluates the view `chain_rung_pairs`, a self-join of the
recursive `chain_rung_ancestry` (each rung's tip walked 400 headers deep) on every poll, whether or not a tip moved.
Reproduced on local D1 with the keeper's migrations, 2,000 headers and three rung tips: 14,457 rows per UPDATE, one
read of `chain_rung_pairs` 14,448, of `chain_rung_ancestry` 2,416 (1,203 rows); the resolve arm is skipped when no
split is open.

The ask, as the captain may file it on the keeper's repository:

> rust-chaintracks reads about 64 M D1 rows a day on prod in two statements (bsv-low census 2026-10-17). (1)
> `get_info`'s `SELECT COUNT(*) as cnt FROM headers` reads every header (971,547) on each `/getInfo`; LOW's monitor
> calls it every 5 minutes (about 280 M rows a day at full cadence): serve `headerCount` from a counter row kept by
> the header writes, or from the sync state the sync already writes, or as `MAX(header_id)` if gaps are acceptable
> (1 row read). (2) `chain_split_detect` recomputes the 400-deep rung ancestry on every rung poll (14,472 rows per
> `UPDATE chain_split_signal`, 13.2 M a day on beta): bump `tick` only when a rung tip changed, or bound the walk to
> the split depth window.


## Owed filing at the write (D2-OWED-WRITERS-A)

`owed-judge-filing` measures `record_post::file_verified` on local D1: a verified potrefund INSERT, its trigger marks, and both seats' derived rows commit in one batch. The fixture includes the shared fleet and the 250-result/270-hop history; setup is excluded from the request figures. Measured 2026-10-25T18:38:14Z with Rust 1.95.0, Node 22.18.0 and Wrangler 4.148.0: **118 reads, 36 writes, 59 statements, maxReads 7**. The ceiling is 177 reads, 38 writes, 61 statements and maxReads 11: a 1.5x read margin rounded up, plus two writes and two statements.

The native export runs the production filing core and records every SQL batch, including preflight reads. Set `OWED_FILING_BUDGET_OUT` to a JSON path under `/Volumes/Lanes/out/bsv-low/` (or `$TMPDIR` when the volume is absent), then run `cargo +1.95.0 test --manifest-path workers/Cargo.toml -p low-app-layer --lib owed893_export_filing_budget -- --ignored` with a lane-private `CARGO_TARGET_DIR`. Run `node tools/lane-499/owed_judge_filing.mjs "$OWED_FILING_BUDGET_OUT"` with Node 22, `D1_BUDGET_WRANGLER_PACKAGE` pointing to the installed Wrangler package.json, and `D1_BUDGET_STATE` pointing to a fresh local D1 directory under the same output root. It loads the production schema and fixture, replays the captured batches, checks both owed rows, and holds D1's row accounting against `ceilings.json`. `D1_BUDGET_MEASURE=1` records a new measurement without applying the ceiling. Use fresh state for each run.
