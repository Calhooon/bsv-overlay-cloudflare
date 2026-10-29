# D2-ACTOR-B-DELTA-LENS-9: the delta lens on D2-ACTOR-B-FOLD-15 (PR 222, bsv-low #958)

Written 2026-10-29 by lane D2-ACTOR-B-DELTA-LENS-9 (Wave E, row E3). This is a review document only. No product code
changed. The PR is for the captain to copy and close; it is not to be merged.

What was reviewed: the delta `986531bd5..d57bc0fa0` (FOLD-15 over the landed FOLD-14), 6 files, +1,227 / -158, judged
cold. It was read against:
- the eighth lens (`D2-ACTOR-B-delta-8-2026-10-29.md` in bsv-low);
- the FOLD-15 brief, PR 222's REPORT and bsv-low #958;
- the cutover doc, section 5;
- records 1521 to 1525.

## VERDICT: LAND (0 HIGH, 1 new MEDIUM, 1 new LOW, 4 NOTE); fold NEW9-M1 before Phase B

FOLD-15 closes the eighth lens's three findings as asked:
- **NEW8-M1.** Both sides are now judged fresh at one tip, and the route serves `tipLag` apart from `diff`.
- **NEW8-M2.** The re-judge is bounded by calls (32 identities opened a tick, `ceil(pace / 16)` judge calls) and saved
  per identity.
- **NEW8-L1.** `d1JudgedUnder` names the previous version only when the previous run is done.

**One new MEDIUM, in the fold of NEW8-M1 itself.** The new held partition is too wide. A game holding ANY pending mark
at the reconcile's start is put in `heldDiffs`. But both judges read the record AFTER that mark was written, so such a
mark is no reason for the two fresh judgements to differ. A genuine judge disagreement on such a game is counted as a
held note, not a STOP. Run below: the actor held a press that D1 wrote off, and it went from `reconcileDiffs` 1 to
`heldDiffs` 1 when one `age` mark was added. That is the north star's failure: a disagreement between judges is a STOP,
never a note.

**One new LOW.** The ceiling of 32 rests on the 949-statement figure, and that figure leaves out the alarm's first phase:
the drain's sweep publishes up to 64 pairs in a batch of about 65 statements.

**No judgement and no schema move.** The JUDGE_VERSION is unmoved, and that is right: no judgement changes.

| Item | Verdict | One line |
|---|---|---|
| 1 NEW8-M1 (fresh with fresh, held partition, route `tipLag`) | **NOT SOUND (NEW9-M1)** | The tip lag is no longer a diff: `fold15_a` is green, and the actor's `actor_apply` carries the port's tip (`owed_judge.rs:483`). The re-apply is one apply of at most 16 games, inside the reconcile's one-call budget. A verdict change that coincides with a tip move is NOT swallowed by `tip_derived_rows`, because it needs equality once only `blocksToGate` and `tipUnknown` are taken out. But the `held` set includes every game holding a mark at the start (`RECONCILE_MARKS_SQL`), and `disagree` is partitioned by it (`owed_actor.rs`, `reconcile_fresh`, `partition(is_held)`). Run: scratch a. |
| 2 NEW8-M2 (bounded by calls, saved per identity, one fresh-judge call) | **FIXED; the ceiling's reason NOT proven (NEW9-L1)** | `fold15_b`: 2,000 no-game identities, 32 opened, 66 calls / 99 statements a tick. `fold15_c`: the fresh judge makes 1 call (16 games judged, 484 marked on the whale). The worst re-judge tick at 32 is 635 statements, with the reconcile yielding (alarm 949); at 16 it is 325, with the reconcile at 282 (alarm 921). A dying tick loses at most the identity in flight: the state rides the next read's batch, which I read in the code and `fold15_b`'s `die_after` leg shows. The drain phase is outside the 949 (NEW9-L1). |
| 3 NEW8-L1 and NOTE-3 | **FIXED** | `d1_judged_under` requires `previous.done == true` (`fold15_d` green). `read_marked` reads the page's `at` through `ms_of`. |
| 4 Together | **CONSISTENT** | `schema.rs` and the overlay crate are untouched. `ACTOR_TABLES_VERSION` is `applied-per-game-1` and `JUDGE_VERSION` is `fold11-2026-10-29`, both unmoved, so the digest has not moved. No counter was added: the delta uses `COUNTERS[5,6,7,17,28,29]`, and the 32-name lists are unchanged in both Workers. `fold12` (4), `fold13` (3) and `fold14` (4) are green; the `-- owed` piece is 241 passed, 1 ignored. FOLD-12's sweep stamp, FOLD-13's never-re-apply-a-stored-row and FOLD-14's one read are all unloosened: the reconcile's re-apply re-reads the record, never a stored row. |

## How this was verified

**Build.** Worktree `~/bsv/wt-overlay-actor-b-lens` at `d57bc0fa0`,
`CARGO_TARGET_DIR=/Volumes/Lanes/targets/actor-b-lens`. Evidence is in `/Volumes/Lanes/out/wt-overlay-actor-b-lens/delta9/`.
- `low-app-layer --lib --no-run`: ok.
- `fold15`: 5 passed, run with `--nocapture`. Its printed figures are quoted in item 2 (`fold15.txt`).
- `fold12`, `fold13` and `fold14`: 11 passed (`fold12-14.txt`).
- `-- owed` whole: 241 passed, 0 failed, 1 ignored, in 347 s (`owed-piece.txt`).

**Scratch pin a.** One pin was appended to `owed_actor/tests.rs`, run, then removed. The diff is in
`lens9-scratch-pin-a.diff`; the output is in `scratch-a.txt`.
- The setup is `fold13_a`'s fixture (`fold11_g_fixture`). D1 is judged under the era, so the hop press is written off.
  The actor is bootstrapped and re-applied under no era (`actor_era: Some(None)`), so it keeps the press. This is a
  genuine judge disagreement, the one `fold13_a` itself plants.
- **Control, no mark:** `diff=true reconcileDiffs=1 heldDiffs=0`, and the line `RECONCILE DIFF owner: 1 outpoints
  differ (a judge disagreement …) 8d00…01:0 [d1=absent actor=present]`.
- **With one `age` mark** on the identity's games, written before the reconcile: `diff=false reconcileDiffs=0
  heldDiffs=1`, and the line `RECONCILE DIFF (held mark; not a STOP) owner game ad00…01 … [d1=absent actor=present]`.

**Read, not run.**
- The drain phase's statement count in the alarm. `drain_env(env, false)` runs `sweep_publish` when the queue is bound,
  and its seq batch is one `SEQ_BUMP_SQL` plus one `read_sql` per pair, at most 64 pairs (`overlay-cloudflare/src/owed_events.rs:488-492`).
- The yield rule (`owed_belt.rs`, `reconcile_yields`) and the pace lever's plan and read.

**Not run:**
- the other lib pieces (`lb`, `tracker`, the rest);
- the 54 integration binaries;
- clippy;
- the budget tier and the workerd cells;
- the lane's RED-on-base claims (taken as stated);
- anything on wasm32, workerd's D1 or beta.

How D1's 1,000-queries-per-invocation limit counts a batch's statements is the repository's own reading and was not
measured.

## New findings

### NEW9-M1 (MEDIUM, fold before Phase B): a mark pending at the reconcile's start turns a judge disagreement into a held note

**Where.** In `reconcile_fresh` (`owed_actor.rs`), `held` is first filled from `RECONCILE_MARKS_SQL` (every game of the
pick holding any mark). It is then extended by `HELD_GAMES_SQL` above the bound (marks written while the two judged).
`diff_rows(&a, &b).into_iter().partition(is_held)` puts every difference on a `held` game into `held_diffs`.

**Why that is wrong for `disagree`.** A mark pending at the start was written before both fresh judgements read the
record:
- the D1 judge hydrates the games from D1;
- the actor's warm apply reads the marks' txids in its one statement (FOLD-14's `marked_select`) and reaches the
  fixpoint.

So the start mark explains a difference between the actor's STORED rows and its re-apply (`missed`). It does not explain
a difference between the two FRESH judgements. Only a mark written while they judged (`HELD_GAMES_SQL`, generation above
the bound) does, and that is exactly the delivery compare's NEW2-L1 rule.

**How often.** Pending marks at the start are not rare on the games the reconcile picks:
- The pick is "the stored rows differ", and the commonest reason for that is a mark not yet handled.
- `age` marks wait for the drain's idle room.
- A fault-held subject waits up to 6 h.
- The reconcile itself marks every game past its 16 as `backfill`. On the whale that is 484 games (`fold15_c`), and each
  of those, when picked next, has its disagreement turned into a held note until the consumer drains it.

**What it costs.** A real disagreement, run above as a press the actor serves and D1 writes off, is counted
`heldDiffs`. The line says "not a STOP", and `reconcileDiffs` (the Phase B entry) stays flat. The delivery compare is
sampled one event in eight, so the reconcile is the backstop that would catch a persisting disagreement. No row moves:
display and the STOP signal only.

**Fold (one line):** partition `disagree` (fresh D1 against the fresh actor) only by the games `HELD_GAMES_SQL` names
above the bound, and keep the start-mark set for `missed` alone.

**Pin:** scratch a. It is RED on `d57bc0fa0`: `left: 0 right: 1` (`reconcileDiffs` with one `age` mark), against the
control's 1.

### NEW9-L1 (LOW): the alarm's statement bound is measured without its first phase, and its margin is 51

**Where.** `alarm_env` (`owed_belt.rs`) runs four phases: `owed.drain`, the belt tick, `owed.d1_rejudge`, and
`owed.reconcile`. The drain phase, with the queue bound, is `sweep_publish`. Its publish batch is `SEQ_BUMP_SQL` plus one
read per pair, up to 64 pairs: about 65 statements, plus the sweep's own reads and writes and `snapshot_dirty`.

**What happens.** FOLD-15's ceiling of 32 games a tick is justified by "the belt tick 314 + the re-judge 635 = 949 under
1,000", and that sum leaves the drain out. In the coincidence of a stalled or lagging consumer (the sweep's full 64
pairs), a full belt tick and a two-call re-judge tick on a dense seat, the alarm is about 949 + 70, past 1,000. The
figures are also the fixture's: a judge call's commit grows with the rows of its 16 games, bounded only by the identity's
500-row ceiling.

**If the limit binds.** The alarm's later statements fault, and those are the re-judge's second judge call. The re-judge
counts the identity faulted and retries it. At its third consecutive fault it SKIPS that identity for the run, which
leaves D1 stale there. That is named `stale-d1` by the reconcile, never re-applied, but the route's diff stays until a
later run.

**Fold (one line):** count the drain's statements into the bound. Either let a tick whose sweep published more than a
few pairs make one judge call, or hold the ceiling at 16 until the alarm's statements are read on D1.

## NOTEs

**NOTE-1: the captain's contention question. The lever removes the contention only when it pauses the walk.**
- The yield is the reconcile's, not the actors'. A tick with two judge calls skips the reconcile.
- A judge call's commit is one atomic batch, and on the whale a tick at 32 is about 635 statements in two such batches.
  The actors' D1 round trips still queue behind those batches.
- Record 1523's 732 ms `readMsP95`, against the 15 ms statement, is that queueing.
- `gamesPerTick: 0` removes it, which is the window's stage 2e as record 1525 plans. At 16 it is halved, not removed.
- The ceiling's reason (949 under 1,000) holds on the fixture with the reconcile yielding, but it does not cover the
  drain (NEW9-L1).

**NOTE-2: while the walk runs at 32, the reconcile hardly runs.** On beta's seats (hundreds of games each) nearly every
tick makes two judge calls and yields the reconcile. A `reconcileDiffs` read flat over a window with the walk at 32 is
flat because nothing was compared. The Phase B read needs the walk paused, or at 16 or below, AND
`owed_actor_reconcile_compared_total` rising.

**NOTE-3: doc drift.** Four places still describe the pace as 16 games and one judge call, as if that were the fixed
pace:
- the doc of `D1_REJUDGE_GAMES_PER_TICK`: "16 since FOLD-15, ONE judge call … 11,520 games a day, the whale … in about
  130 minutes";
- `d1_rejudge_step`'s doc: "ONE judge call";
- the `alarm_env` comment: "16 games, one judge call";
- the doc's "AT MOST 32 games of the re-judge, or the belt tick, one judge call and the reconcile's one".

The const, the render vars and CLAUDE.md all say 32, which is two calls with the reconcile yielding. This is cosmetic.

**NOTE-4: every reconcile step that picks a game now writes the actor** (one re-apply), where FOLD-13 re-applied only on
a disagreement. The cost is one actor apply per 22.5 minutes, within the apply bound, and it is stated by the lane.
`reapplied` and `republished` count only the `missed` case.

## What a further fold must change (one line each)

- **NEW9-M1:** partition `disagree` by `HELD_GAMES_SQL` above the bound only, and keep the start marks for `missed`. Pin:
  scratch a.
- **NEW9-L1:** put the drain's statements into the alarm's bound (one judge call on a tick whose sweep published), or
  hold the ceiling at 16 until it is read on D1.

## What the window on this main must read before Phase B

The eighth lens's rows are kept. The amended rows are marked.

| Bar | Cell or read | Figure |
|---|---|---|
| Reconcile (amended) | `reconcileDiffs`, `heldDiffsTotal`, `tipLagDiffsTotal`, `reconcileStaleTotal`, each `[owed-actor] RECONCILE` line | Both sides are judged at one tip now, so any `RECONCILE DIFF … a judge disagreement` or `… the actor missed the record` is a STOP. `TIP LAG` and `stale-d1` are not. Until NEW9-M1 is folded, also read each `RECONCILE DIFF (held mark; not a STOP)` line: one whose detail names a verdict field (`family`, `claimable`, `facts.claim`, `d1=absent actor=present`) on a game with no traffic in that minute is a STOP read by hand. The compare must have run: `owed_actor_reconcile_compared_total` rising over the read (NOTE-2). |
| D1 re-judge (amended) | `/health.owedActor.d1Rejudge` {`paused`, `gamesPerTick`}, the `[owed-alarm]` line's `d1Rejudge.judgeCalls` and `reconcile.yielded` | Paused (`gamesPerTick` 0) during the legs and the bars read. Resumed at 16 or below while the reconcile must run (at 32 it yields almost every tick). `faults` flat, `skipped` 0. A `skipped` rising after a fault run is NEW9-L1. `done` under `fold11-2026-10-29` is Phase A's hygiene, not a Phase B gate (record 1524). |
| Route (amended) | `GET /internal/owed-actor-rows?identity=02b2f90e11…` | `diff` empty. A gated row whose two sides were judged at different tips is in `tipLag`, with `tipLagTips` naming both. `d1JudgedUnder` is the stamp's version once `d1Rejudge.passed` is true, else `unknown` (never a version from an unfinished run). |
| Bars (amended) | `classes.direct.applyMsP95`, `rowsVisibleDirectMsP95`, `stages.{read,wait,judge,write}MsP95`; `backstopLagMsP95` apart | Read with the re-judge PAUSED: the 732 ms `readMsP95` of record 1523 was queueing behind its commits (NOTE-1). Direct apply under 300 ms, direct visibility under 1 s, backstop under 60 s. |
| Alarm statements (new) | the `[owed-alarm]` line's phases, while the walk runs | No `d1Rejudge` error and no reconcile error that names the platform's query limit. One would be NEW9-L1. |
| Phase C ack | `directAcked` | 0 in Phases A and B. |
| Sweep | `sweepPassed`, `sweepStalled`, `sweepLost` | Near zero a tick (record 1523: 16 in 3.5 h). |
| Re-judge of actors | `/health.owedActor.rejudged` | Flat: `JUDGE_VERSION` did not move. |
