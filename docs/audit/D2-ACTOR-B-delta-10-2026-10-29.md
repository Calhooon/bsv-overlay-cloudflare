# D2-ACTOR-B-DELTA-LENS-10: the delta lens on D2-ACTOR-B-FOLD-16 (PR 224, bsv-low #958)

Written 2026-10-29 by lane D2-ACTOR-B-DELTA-LENS-10 (Wave E, row E3). This is a review document only. No product code
changed. The PR is for the captain to copy and close; it is not to be merged.

What was reviewed: the delta `5f820ebf9..53675f4b` (FOLD-16 over the landed FOLD-15), 7 files, +558 / -45, judged
cold. It was read against:
- the ninth lens (`D2-ACTOR-B-delta-9-2026-10-29.md` in bsv-low) and its scratch pin a;
- the FOLD-16 brief, and the captain's summary of the lane's commit (the lane wrote no REPORT);
- bsv-low #958 and the cutover doc, section 5.

## VERDICT: LAND (0 HIGH, 0 MEDIUM, 1 new LOW, 3 NOTE)

FOLD-16 closes the ninth lens's two findings:
- **NEW9-M1 is FIXED.** A mark pending at the reconcile's start no longer turns a judge disagreement into a held note.
- **NEW9-L1 is FIXED.** A drain that did work holds the re-judge to one judge call and makes the reconcile yield. The
  worst alarm on the fixture is 915 statements.

**No path files a fresh-against-fresh verdict difference as tip lag, stale or missed.** One narrow path still files it
as held: a mark landing after the actor's read and before `HELD_GAMES_SQL` is read (NEW10-L1). This is the delivery
compare's own NEW2-L1 rule, now shared by the reconcile. Every held line still carries the differing fields, so the
window can read it by hand.

**No judgement, schema or counter moved.** `JUDGE_VERSION` is unmoved, which is right: no judgement changed.

| Item | Verdict | One line |
|---|---|---|
| 1 NEW9-M1's fold | **FIXED** | `disagree` is partitioned by `during` alone (`HELD_GAMES_SQL` above the bound, `owed_actor.rs`, `reconcile_fresh`). The start marks reach only `missed` (`held_any`). `fold16_a` runs the lens's scratch a and its inverse: the control is `(diff true, reconcileDiffs 1, heldDiffs 0)`; with an `age` start mark it is still `(true, 1, 0)` with the line "a judge disagreement"; a mark written during the judge gives `(false, 0, 1)`, "held mark; not a STOP". `tip_derived_rows` reaches only `missed`, and `stale_outpoints` only counts (it excludes every fresh difference), so neither can swallow a verdict difference. The one residual is NEW10-L1. |
| 2 NEW9-L1's fold | **FIXED** | `rejudge_calls_max(pace, drain_bound)` caps the tick at 1 judge call when the drain swept, judged or faulted (`owed_belt.rs` `alarm_env`, `map_or(true, …)`). `reconcile_yields` then yields on that one call. `fold16_b`: a full 64-pair drain is 276 statements, the worst re-judge tick under it 325, so the alarm is 276 + 314 + 325 = 915. With no re-judge call the whale's reconcile is 282, and the alarm 872. The lens's scratch measured the drain's raw statements by the marks waiting: 0 pairs 6, 1 pair 12, 8 pairs 40, 64 pairs 264, 200 marks 264 (the cap). The idle-drain alarm at 32 is therefore 6 + 314 + 635 = 955, plus that drain's few counter rows: under 1,000 with a margin of about 40. |
| 3 Together | **CONSISTENT** | `schema.rs` and the overlay crate are untouched. `ACTOR_TABLES_VERSION` is `applied-per-game-1` and `JUDGE_VERSION` is `fold11-2026-10-29`, both unmoved. No counter was added: `drainBound` and `swept` are report fields, so the lists are unchanged in both Workers. The NOTE-3 drift is gone from the four code sites. One CLAUDE.md sentence is still stale (NOTE-3 below). FOLD-15 is not loosened: the fresh compare at one tip, `tipLag` on the route, the pace lever and `d1JudgedUnder` all hold (`fold15` 5 of 5 green), and the reconcile's re-apply still re-reads the record. The re-apply's body now carries no `atMs` and no `seq` (item 5). It is no dupe, carries no bound, and so is always applied. |

## How this was verified

**Build.** Worktree `~/bsv/wt-overlay-actor-b-lens` at `53675f4b`,
`CARGO_TARGET_DIR=/Volumes/Lanes/targets/actor-b-lens`. Evidence is in `/Volumes/Lanes/out/wt-overlay-actor-b-lens/delta10/`.
- `low-app-layer --lib --no-run`: ok.
- `-- fold16 fold15 --nocapture`: 8 passed in 270 s, with the printed figures quoted above (`fold15-16.txt`).
- `-- owed` whole: 244 passed, 0 failed, 1 ignored, in 449 s (`owed-piece.txt`).

**Scratch pin a.** One pin was appended to `owed_actor/tests.rs`, run, then removed; the tree is clean
(`lens10-scratch-pin-a.diff`, `scratch-a.txt`). It runs `fold16_b`'s drain measurement with 0, 1, 8, 64 and 200 marks
waiting and prints the raw statements, without the counter deltas. The ninth lens's own scratch a no longer compiles,
because `Fold13` has a new field, `mark_during`. `fold16_a` is that scratch a, verbatim in its setup, with the inverse
leg added, and it was run.

**Read, not run:**
- `D1Judge::actor_apply`'s tip. The tip is the port's, else `tip_memoised`. The reconcile's port is built with
  `tip_memoised` too, so both sides take the same memo.
- `reconcile_apply_body`, and the object's `samples_bars` gate (wasm32 only).
- The order of the batch: `HELD_GAMES_SQL` is read before `RECONCILE_MARK_REST_SQL`, and the rest's marks name only games
  outside the judged set.

**Not run:**
- the other lib pieces (`lb`, `tracker`, the rest);
- the 54 integration binaries;
- clippy native and wasm32;
- the budget tier and the workerd cells;
- the lane's RED-on-base claims (taken as stated);
- anything on wasm32, workerd's D1 or beta.

The 314 statements of the belt tick are the budget tier's constant, not measured in `fold16_b`, and the tick's
`publish_noted` and `direct_held` are not shown to be inside it. How D1 counts a batch's statements against its 1,000 is
the repository's reading and was not measured.

## New findings

### NEW10-L1 (LOW): a mark landing after both judges read, and before `HELD_GAMES_SQL` is read, still holds a disagreement

**Where.** In `reconcile_fresh`, `during` is read in the batch AFTER the actor's re-apply answered. The batch is
`RECONCILE_D1_ROWS_SQL`, then `HELD_GAMES_SQL` with `generation > bound`.

**Why it holds a genuine difference.** A mark written after the actor's closure read the record, but before that
batch, is neither judgement's input. The two fresh judgements read the same record, so their difference is a judge
disagreement, yet it is filed `heldDiffs`, "not a STOP".

**How narrow it is.** The window is the actor's judge and commit plus one round trip: about 0.3 s release p95 on the
fixture, up to the 3 s seen on beta. It needs a mark on that very game inside that window, and the next pick of the
identity under the reconcile's one-a-22.5-minutes walk. A persisting disagreement on a game marked every few seconds can
be held more than once. The delivery compare has had the same tail since NEW2-L1, and it was accepted there.

**The window's reading.** Each held line names the differing fields (`diff_detail`). One that names a verdict field on
a game is a STOP read by hand.

**Fold (one line):** let the actor's answer report the greatest mark generation its closure read, and hold only on
`bound < generation <= that`.

## NOTEs

**NOTE-1: an actor miss on a start-mark game is not counted.** `missed` excludes every game holding a start mark,
because the actor's stored rows are then expected stale. An unmarked write that the actor missed on such a game is
healed by the re-apply, as every re-apply heals it, but it is not counted a diff or `republished`. This is by design:
with a start mark pending, a miss cannot be told apart. It is not a judge disagreement, which is still caught by
`disagree`.

**NOTE-2: with a working drain the reconcile yields at 16 too; amends the ninth lens's window table.** Under FOLD-16,
`reconcile_yields` is `judge_calls > 1 || (drain_bound && judge_calls >= 1)`. Item 5's own finding names the sweep as
re-publishing the reconcile's `backfill` marks (484 on the whale), so on beta the drain is busy on most ticks. While a
re-judge run lasts and the sweep publishes, the reconcile runs only on a tick whose re-judge made NO call. That means the
walk is paused (`gamesPerTick` 0), or done.

Two places say "at 16 or below" alone: the ninth lens's window table, and `D1_REJUDGE_GAMES_PER_TICK`'s doc ("paused
(`0`) or at 16 or below"). Neither is enough. CLAUDE.md's NOTE-2 line has the sweep condition and is right.

**NOTE-3: one stale sentence in CLAUDE.md.** The FOLD-15 NEW8-M2 paragraph still ends "The drain's sweep is not
counted." FOLD-16 counts it. This is cosmetic.

## What the window on this main must read before Phase B

The ninth lens's rows are kept. The amended rows are marked.

| Bar | Cell or read | Figure |
|---|---|---|
| Reconcile (amended) | `reconcileDiffs`, `heldDiffsTotal`, `tipLagDiffsTotal`, `reconcileStaleTotal`, each `[owed-actor] RECONCILE` line | Any `RECONCILE DIFF … a judge disagreement` or `… the actor missed the record` is a STOP, a start mark no longer excusing it. `TIP LAG` and `stale-d1` are not. A `RECONCILE DIFF (held mark; not a STOP)` line now means a mark written during the compare (NEW10-L1). One that names a verdict field (`family`, `claimable`, `facts.claim`, `d1=absent actor=present`) is read by hand as a STOP. The compare must have run: `owed_actor_reconcile_compared_total` must be rising over the read. |
| D1 re-judge (amended) | `/health.owedActor.d1Rejudge` {`paused`, `gamesPerTick`}, the `[owed-alarm]` line's `d1Rejudge.judgeCalls`, `drainBound`, `drain.swept` and `reconcile.yielded` | Paused (`gamesPerTick` 0) during the reconcile read, or the run `done`. At 16 with `drainBound` true the reconcile still yields (NOTE-2). `faults` flat, `skipped` 0. |
| Alarm statements (amended) | the `[owed-alarm]` line's phases | No `d1Rejudge` or `reconcile` error naming the platform's query limit. On a tick with `drain.swept` above 0, `d1Rejudge.judgeCalls` must be 1 or less. |
| Route | `GET /internal/owed-actor-rows?identity=02b2f90e11…` | `diff` empty. A gated row judged at two tips is in `tipLag`. `d1JudgedUnder` is the stamp's version once `d1Rejudge.passed` is true. |
| Bars (amended) | `classes.direct.applyMsP95`, `rowsVisibleDirectMsP95`, `stages.*MsP95`; `backstopLagMsP95` apart | Read with the re-judge paused. On this main the reconcile's re-apply and the sweep's pass sample nothing (item 5), so the `direct` class is the writers' alone. A 3 s multi-game sample there is now a finding. Targets: direct apply under 300 ms, direct visibility under 1 s, backstop under 60 s. |
| Phase C ack | `directAcked` | 0 in Phases A and B. |
| Sweep | `sweepPassed`, `sweepStalled`, `sweepLost` | Near zero a tick. |
| Re-judge of actors | `/health.owedActor.rejudged` | Flat: `JUDGE_VERSION` did not move. |
