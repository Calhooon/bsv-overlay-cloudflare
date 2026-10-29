# D2-ACTOR-B-DELTA-LENS-8: the delta lens on D2-ACTOR-B-FOLD-12 (PR 218), FOLD-13 (PR 219, bsv-low #958) and FOLD-14 (PR 220, #954 item 9)

Written 2026-10-29 by lane D2-ACTOR-B-DELTA-LENS-8 (Wave E, row E3). This is a review document only. No product code
changed. The PR is for the captain to copy and close; it is not to be merged.

What was reviewed: the combined delta `c08be8b51..800d8b2e3`, judged cold. It is 12 files, +2,460 / -163.
- `3227310ba` is FOLD-12 (PR 218).
- `2b3066add` is FOLD-13 (PR 219).
- `800d8b2e3` is FOLD-14 (PR 220), main's tip at this lens. Its land had reached the vultr tier at this lens
  (`lands/overlay-220.log`, step 5).

What it was read against:
- the seventh lens (`D2-ACTOR-B-delta-7-2026-10-29.md` in bsv-low);
- bsv-low #954, #956 and #958;
- the three briefs, the three PR bodies and the three REPORTs;
- the design, sections 3 to 7;
- the cutover doc, sections 1 to 5;
- records 1512 to 1518.

## VERDICT: LAND (0 HIGH, 2 new MEDIUM, 1 new LOW, 5 NOTE); fold both MEDIUMs before Phase B

**FOLD-12 closes the seventh lens's four findings.**
- NEW7-M1: the swept stamp is `CAST(?2 AS INTEGER)` and every belt-state read goes through `ms_of`.
- NEW7-L1: the two range reads are cursor windows.
- NEW7-L3: each identity carries its own `also`.
- NEW7-L2: a tip-only pair is counted apart.

**FOLD-14's one read round trip is sound.** It returns the same rows as the two reads, and it stays within D1's bounds
as far as this lens can read and measure.

**FOLD-13 fixes #958's wrong-direction heal.** The reconcile no longer re-applies a stored D1 row into the actor. Its
re-apply re-reads the record.

**Two new MEDIUMs, both in FOLD-13, both readable before Phase B.**
- **NEW8-M1.** The reconcile compares a FRESH D1 judgement against the actor's STORED rows. When the actor's rows were
  judged at an older tip, every gated row's `blocksToGate` differs. The reconcile counts that as a judge disagreement,
  which is the Phase B entry STOP (`reconcileDiffs` flat). Run below.
- **NEW8-M2.** The two new alarm phases are bounded by GAMES, not by D1 calls. An identity with no game costs two calls
  and counts nothing. One fresh reconcile is 64 games, about 1,000 statements natively, and runs in the same invocation
  as the belt tick and the re-judge. The re-judge saves its cursor only at the end of a tick. Run below.

**No money verdict moves.** NEW8-M1 heals in the right direction: it re-applies from the record into the actor. NEW8-M2
costs liveness and the window's reads, not rows.

| Item | Verdict | One line |
|---|---|---|
| 1 FOLD-12 NEW7-M1 | **FIXED** | `SWEPT_MEMO_WRITE_SQL` (`owed_events.rs:108`) writes `CAST(?2 AS INTEGER)`. Every cursor read in the sweep, the consumer and the memos goes through `ms_of`. `fold12_a` is green with the stamp stored as a REAL. I grepped every `json_object`/`json_set`/`json_array` with a bind in both Workers' owed paths. The one integer left is `DIRTY_SNAPSHOT_SQL`'s `'tickAtMs',?1` (`owed_judge.rs:133`), and nothing reads it back (NOTE-2). |
| 2 FOLD-12 L1, L3, L2 | **FIXED (L2 narrow, see NEW8-M1)** | No cursor skips a note for good: a note named before the cursor is read when the round wraps, within `ceil(N/32)` ticks. Both reads are primary-key ranges with no sort. `for_identity` (`owed_actor.rs:4911`) samples each event once in each actor whose plan names it (NOTE-4). `tip_lag_rows` (`:3167`) counts a pair apart only when `facts.tip` or the `probeTip` column differ and the verdict is equal, so `probeTip`-only differences are included. A real tip lag also moves `blocksToGate`, which `diff_rows` compares, so that shape is still counted as a diff (NEW8-M1). The reason for not building the probeTip floor is SOUND: a floor at the stored tip would judge above a tip a reorg had lowered. |
| 3 FOLD-13 reconcile | **NOT SOUND (NEW8-M1)** | The fresh judge is bounded at 64 games, 16 a call, and acknowledges nothing. The rest of the games are marked `backfill` and left out of the compare, which is sound. Two stale cases are handled differently. A stale D1 is named `stale-d1` (`fold13_a` green). A stale ACTOR is reported as `a judge disagreement` and counted `reconcileDiffs`: run below, `facts.blocksToGate d1=97 actor=100`. |
| 4 FOLD-13 re-judge | **NOT SOUND (NEW8-M2); the stamp and labels SOUND (NEW8-L1)** | 32 games a tick. The same version costs one primary-key read and no write. A faulted identity is retried and counted, and skipped at its third fault. `version` is written at the start of a run and `done` only at its end, and `rejudge_passed` needs both, so the stamp effectively moves only at the end (`fold13_b` green). The pace has no D1-call bound: identities with no game are free (4,003 calls in one tick for 2,000 of them, run), and the cursor is saved only at the end of a tick (`:4769`). `d1JudgedUnder` labels a pre-run identity with the previous run's version without checking that run passed it (NEW8-L1). After this fold a stale row can still read as a disagreement: in the reconcile (NEW8-M1, a stale actor) and on the operator route, whose `diff` compares stored rows against stored rows. The route now names which side is stale. |
| 5 FOLD-14 one read | **FIXED** | `marked_select` (`:870`) is 10,796 bytes with binds `?1` to `?6`. `page_select` is 11,748 bytes with `?1` and `?3` to `?8`. Each compound stays at or under 5 terms (`identity_games`' five `UNION ALL` arms, unchanged). Both `TOOBIG` fallbacks are kept, and `fold14_a` and `fold14_b` show byte-equal rows on the 600-game seat. The four stages sum to the apply (`fold14_c`). A joined caller is answered when the pass ends, not after the turn (`fold14_d`). It still waits for every chunk of the holder's pass (NOTE-1). The page's `at` is read as an integer, which is safe on today's columns (NOTE-3). |
| 6 Together | **CONSISTENT** | `schema.rs` is untouched; `ACTOR_TABLES_VERSION` is `applied-per-game-1` and `JUDGE_VERSION` is `fold11-2026-10-29`, both unmoved, so the digest has not moved. That is right, because none of the three folds changes what a judgement stores. `COUNTERS` and the overlay's `OWED_ACTOR` both hold 32 equal names. `health_select` serves the four new names; the five names it omits predate this delta. The sweep row holds `at`, `g`, `lostAfter` and `pruneAfter`; the consumer row holds `horizonMs`. The first deploy of this main onto beta, which carries no `owed-judge:version` row, starts D1's re-judge once, as #958 asks. The window table is amended below. |

## How this was verified

**Build.** Worktree `~/bsv/wt-overlay-actor-b-lens` at `800d8b2e3`, `CARGO_TARGET_DIR=/Volumes/Lanes/targets/actor-b-lens`.
Evidence is in `/Volumes/Lanes/out/wt-overlay-actor-b-lens/delta8/`.
- `low-app-layer --lib --no-run`: ok.
- `fold12`: 4 passed. `fold13`: 3 passed. `fold14`: 4 passed.
- `-- owed` whole: 236 passed, 1 ignored, in 332 s.

**Scratch pins.** Four were appended to `owed_actor/tests.rs`, run, then removed. The diff is in
`lens8-scratch-pins.diff`; the results are in `lens8-scratch-results.txt`.
- **a.** `fold13_a`'s port with its tip made settable, on `d1_fleet` identity `02ee…`, row `a200…:0` (`blocksToGate`
  100). The actor is bootstrapped at TIP, and D1's stored row is made stale.
  - Reconciled at TIP: `stale-d1`, no diff.
  - Reconciled 3 blocks later: `RECONCILE DIFF … (a judge disagreement: D1 judged fresh over 1 games) … a200…:0
    [facts.blocksToGate d1=97 actor=100]`, with one game re-applied.
- **b.** 2,000 `owed_state` identities holding no game: ONE `d1_rejudge_step` made 4,003 record calls.
- **c.** `d1_fleet`, four re-judge ticks of 32 games: 279 calls / 542 statements, 238 / 473, 38 / 145, 134 / 292. One
  reconcile of an identity whose stored rows all differ: 64 games judged, 54 marked, 1,013 statements.
- **d.** The size of the one statement, as given in item 5.

**Read, not run.** These were checked by reading the code:
- the column affinities behind the page's `at` (all `INTEGER`; the writers bind `i64`);
- the D1 bind path (`Bind::Int` becomes `JsValue::from_f64`, as the seventh lens cites it);
- the cutover doc's Phase B entry (`reconcileDiffs` flat over the window).

**Not run:**
- the other lib pieces (`lb`, `tracker`, the rest);
- the integration binaries;
- clippy;
- the overlay crate's `applayer_counters` pin (the lists were compared by script instead);
- the budget tier and the workerd cells;
- the lanes' RED-on-base claims (taken as stated);
- anything on wasm32, workerd's D1 or beta.

D1's per-invocation query bound for a Durable Object alarm, and whether a batch's statements count against it
individually, is the repository's own reading (CLAUDE.md, #576's "1,000 queries per invocation"). It was not measured.

## New findings

### NEW8-M1 (MEDIUM, fold before Phase B): the reconcile's fresh compare is one-sided, so a stale actor reads as a judge disagreement

**Where:** `owed_actor.rs:4411` to `:4448`. `stored_diff` picks the games. They are judged fresh in D1 at
`reconcile_env`'s present tip. Then `fresh_diff = diff_rows(&fresh, &actor_side)`, where `actor_side` is the actor's
STORED rows, judged at its last apply's tip and over the record as that apply read it.

**How it fails.** `blocksToGate` is stored in `facts` and compared (`CLOCK_FACTS` drops only `tip`). The actor's tip
alarm re-judges a game only when its gate enters the window, and D1's tip marks reach only gates 6 blocks out. So a
gated row's `blocksToGate` on the actor lags by the blocks since that game's last apply. Once a game is picked, because
its stored D1 row differs for any reason, the fresh D1 row is a block-current judgement and the actor's is not. The
remaining difference is counted as a judge disagreement (`owed_actor_reconcile_diffs_total`, the line `a judge
disagreement`). Run above.

The same one-sidedness applies to a mark the actor has not applied yet, since the fresh D1 reads the write. The reconcile
reads no held-games partition, unlike the delivery compare's `heldDiffs` (NEW2-L1).

**Why it matters.** `reconcileDiffs` flat over the window is the cutover's Phase B entry (`CUTOVER-D2-ACTOR` line 79).
The window right after this deploy is exactly when the reconcile picks games: D1 is stale until its re-judge passes. And
once the re-judge has passed an identity, D1's rows are judged at the re-judge's tip and the actor's at its own, so the
stored rows differ on every gated row and those games are picked too.

The operator route's `diff` compares stored against stored (`rows_view`, `:3089`), so it shows the same
`blocksToGate`-only difference whenever the actor's bootstrap and D1's re-judge of an identity ran at different tips.
That is the window's closing read for `02b2f90e…`.

FOLD-12's tip-lag counter does not catch this, because it counts only pairs whose verdict is equal.

**Fold (one line):** judge both sides at one tip before comparing (re-apply the actor over the picked games under the
reconcile's tip, then compare fresh with fresh, as the delivery compare does). Or classify a difference whose fields are
only tip-derived (`tip`, `probeTip`, `blocksToGate`, `tipUnknown`), between two different judged tips, as a tip lag, on
the reconcile and on the route.

**Pin:** scratch a above, with RED on `800d8b2e3` read as the diff counted: `facts.blocksToGate d1=97 actor=100`.

### NEW8-M2 (MEDIUM, fold before the window reads `d1Rejudge`): the alarm's two new phases are bounded by games, not by D1 calls

**Where:**
- `d1_rejudge_step` (`owed_actor.rs:4634`): the loop's guard is `report.games < 32`. The `games.is_empty()` branch
  (`:4727`) counts an identity and continues, and the state is saved once, after the loop (`:4769`).
- `reconcile_step`: up to `RECONCILE_JUDGE_GAMES_MAX` (64) games judged inline.
- `alarm_env` (`owed_belt.rs`): the drain, the belt tick, the re-judge and the reconcile run in ONE invocation.

**How it fails.**
- An identity of the union with no game (an `owed_state` row with no party, hop, evidence or row left) costs two D1
  calls and no game, so a run of them is walked in one tick without bound. Run: 2,000 such identities took 4,003 calls in
  one tick.
- Every identity also costs one empty page at its end.
- On the fleet fixture a 32-game tick is up to 542 statements. One reconcile whose identity's stored rows all differ
  (the #958 window, or an unbootstrapped actor) judges 64 games in 1,013 statements. The belt tick itself is 314
  statements (`owed-belt-tick`).
- Against the 1,000 queries per invocation the repository reads, one alarm in the window can pass the bound. A tick that
  dies saves nothing, so the next one walks the same identities again, and the reconcile after it shares the dying
  invocation.

How many such identities there are on beta was not read. Whether the bound counts statements or calls was not measured.

**Fold (one line):** bound the step by D1 calls as well as games (count an identity with no game, at most N identities a
tick), save the cursor after each identity, and cap the reconcile's fresh judge at one judge call (16 games) a step,
marking the rest.

**Pin:** scratch b above (4,003 calls in one tick), RED on `800d8b2e3` against a call bound.

### NEW8-L1 (LOW): `d1JudgedUnder` names the previous run's version without checking that the previous run passed the identity

**Where:** `d1_judged_under` (`owed_actor.rs:4582`): `computed_at < startedAtMs` answers `previous.version`.

**What happens.** `previous.done` is recorded but never read, and the previous run's cursor is not kept. An identity
judged before a run that was itself replaced mid-walk (a second version move inside one run) is labelled with a version
its rows were never judged under. This is display only, on the operator route.

**Fold (one line):** answer `previous.version` only when `previous.done`, else `"unknown"`.

## NOTEs

**NOTE-1: a joined caller waits for the whole pass.** `Burst::answer` runs after `apply_burst` returns. An event whose
games sit in the pass's first 64-game chunk waits for the other chunks' reads. Its reported stages are the pass's. This
only matters for a burst wider than one chunk.

**NOTE-2: the last D1-REAL instance in the owed path.** `DIRTY_SNAPSHOT_SQL`'s `'tickAtMs',?1` stores a REAL, and no
reader reads it. `owed.dirty.asOfMs` serves the `tickedAtMs` column as it stands. Both predate this delta (`5db446bb`).

**NOTE-3: the page's `at` is read with `as_i64`** (`read_marked`, from `json_group_array(json_array(gameId,at))` built in
SQLite). It is safe today: every source column (`createdAt`, `evictedAt`, `updatedAtMs`) has `INTEGER` affinity and
integral writers. A REAL `at` would read 0 and end the bootstrap at that page, so reading it through `ms_of` would make
it robust.

**NOTE-4: per-identity samples.** An event whose plan names two identities is sampled once in each actor (`fold12_c`:
four samples for two events over two identities). That is one sample per event per identity whose rows it makes
visible. This is by design.

**NOTE-5: the re-judge's cost.** The re-judge's rows-read cost (the lane's estimate of 4.3 M rows a day while a run
lasts) remains an estimate. Statements per tick were measured natively (up to 542), not rows on D1. Read the cost bar
(`/health/invariants.costBar`) on the first run.

## What a further fold must change (one line each)

- **NEW8-M1:** compare fresh with fresh at one tip on the reconcile, and on the route name a difference that is only
  tip-derived between two judged tips as a tip lag. Pin: scratch a.
- **NEW8-M2:** bound the re-judge by calls, save it per identity, and cap the reconcile's fresh judge at 16 games a step.
  Pin: scratch b.
- **NEW8-L1:** `previous.version` only when `previous.done`.

## What the window on this main must read before Phase B

The seventh lens's rows are kept. The amended rows are marked.

| Bar | Cell or read | Figure |
|---|---|---|
| D1 re-judge (new, #958) | `/health.owedActor.d1Rejudge`, `/health/invariants.owedActor.d1Rejudge`, the `[owed-alarm]` line's `d1Rejudge` | `done: true` under `fold11-2026-10-29`, `faults` flat, `skipped` 0. A tick whose `d1Rejudge` is an error, or `identities` rising with `games` 0, is NEW8-M2. Read the cost bar while it runs. |
| Compare (amended) | `diffsTotal` against `heldDiffsTotal` and `tipLagDiffsTotal` | `diffsTotal` flat. Any diff here is a STOP: both sides are judged at one tip. |
| Reconcile (amended) | `reconcileDiffs`, `reconcileStaleTotal`, each `[owed-actor] RECONCILE DIFF` line | Until NEW8-M1 is folded, a DIFF line whose detail names only `facts.blocksToGate` (or `tip`, `probeTip`, `tipUnknown`) is a tip lag, not a STOP. Any other field is a STOP. `stale` may rise until `d1Rejudge` is done. |
| Route (amended) | `GET /internal/owed-actor-rows?identity=02b2f90e11a3af178c38f303f01dd93debfb2fc76e52401155eb735dde98eadacb` on a fresh bootstrap after `d1Rejudge.done` | `d1Rejudge.passed: true`, `d1JudgedUnder` `fold11-2026-10-29`, and `diff` empty, or naming only `blocksToGate` on gated rows (both sides' tips stated) until NEW8-M1 is folded. The ten old-era payouts are absent on both sides. |
| Sweep (#956, amended) | `sweepPassed`, `sweepStalled`, `sweepLost`, `lostNoted` | Near zero a tick. NEW7-M1 is folded, so a `sweepStalled` that rises every tick on one pair is now the consumer. |
| Phase C ack | `directAcked` | 0 in Phases A and B. |
| Apply stages (new) | `stages.{read,judge,write,wait}MsP95`, `classes.*.{…}MsP95` beside `applyMsP95` on pair 20 | `applyMsP95` under 300 ms. The stages name the tail: `readMs` is the one D1 round trip since FOLD-14. |
| First visibility | `rowsVisibleMsP95` with the fleet bootstrapped first (stage 2c) | under 1 s |
| Backstop | `backstopLagMsP95` | under 60 s. NEW7-L3 is folded, so a multi-identity batch no longer over-samples. |
| Re-judge of actors | `/health.owedActor.rejudged` | flat on this deploy: `JUDGE_VERSION` did not move. |
