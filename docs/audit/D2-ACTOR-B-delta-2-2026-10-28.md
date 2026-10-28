# D2-ACTOR-B-DELTA-LENS-2: the second delta lens on overlay PR 200 after its third fold (bsv-low #912)

Written 2026-10-28 by lane D2-ACTOR-B-DELTA-LENS-2 (Wave E, row E3). This is a review document only: no product
code changed, and nothing was committed to PR 200.

What was reviewed:
- PR 200 at `lane/D2-ACTOR-B`, tip `e3e70b65`.
- The third fold, one commit over `42dd7f7e` (`git diff 42dd7f7e..e3e70b65`, 12 files, +1,024 / -103).

What it was read against:
- bsv-low `docs/audit/D2-ACTOR-B-delta-2026-10-28.md` (the first delta lens: NOT LAND, NEW-H1, NEW-M1 to NEW-M3,
  NEW-L1 to NEW-L6, five PARTIAL items).
- `D2-ACTOR-B-lens-2026-10-28.md` (the from-scratch lens).
- The third fold's brief (`docs/lanes/D2-ACTOR-B-FOLD-3-2026-10-28.md`) and its REPORT.
- PR 200's body ("Lens folds" and the delta-folds section).
- `CUTOVER-D2-ACTOR-2026-10-28.md` (the Phase C rollback, now two renders).
- `WIPE-REHEARSAL-2-2026-10-23.md` and `WIPE-REHEARSAL-3-2026-10-28.md`.

## VERDICT: NOT LAND (0 new HIGH, 1 new MEDIUM, 5 new LOW)

The fold closes what the first delta lens found, with one exception:
- **NEW-H1 is CLOSED in both phases.** Both of that lens's probes fail on `42dd7f7e` and pass on the tip. My own
  probes also pass on the tip: a downgrade written just before the acknowledgement batch, in Phase B and on a Phase C
  redelivery. In each, the mark survives and the actor read withholds the press.
- **NEW-M1 and NEW-M2 are CLOSED.** Each leaves a stated residual (below).
- **NEW-M3 is PARTIAL.** The D1 read now refuses presses during the FIRST rollback after a Phase C epoch. But on the
  SECOND rollback (or after any earlier stamped run of the pass, such as the beta rehearsal the Linux tier asks for),
  the `owed-v3` pass stamps on its first call having re-judged nothing. The D1 read then serves the flip's stale rows
  as presses. This is NEW2-M1 below, a one-statement fold.

What this means for deployment:
- **Phases A and B can be deployed as rendered.** NEW2-M1 is only on the Phase C rollback path.
- **Phase C waits for NEW2-M1.** The Phase B window should read its DIFFs knowing NEW2-L1: a DIFF whose game holds a
  pending mark is expected, not a STOP.

### How this was verified

All runs were native debug builds, in two scratch worktrees under `/Volumes/Lanes/tmp`: `e3e70b65` (the tip) and
`42dd7f7e` (the base). Both were removed afterwards. The evidence is in `/Volumes/Lanes/out/wt-overlay-actor-b-lens/delta2/`:
- `lens2-probes-tip.diff` and `lens2-probes-base.diff`: the probe sources.
- `lens2-probes.out`: every figure quoted here.

Results on the tip:
- `--lib d2_actor_b_fold3`: 8 passed.
- The suite pieces, which match the fold's REPORT:
  - `-- owed`: 147 passed, 1 ignored.
  - `-- lb`: 15 passed, 2 ignored.
  - `-- tracker`: 29 passed.
  - `-- --skip owed --skip tracker --skip lb`: 546 passed, 1 ignored.
- Six probes of my own, `lens2_p1` to `lens2_p6`: P1 to P4 pass, P5 FAILS (NEW2-M1), and P6 measures NEW2-L1.
- A revert mutant: with the Phase C redelivery skip put back into `apply`, the fold's
  `d2_actor_b_fold3_h1_b` goes RED, and so does my `lens2_p3`. So `h1_b` is a real pin. (I checked this because its
  helper might have passed on the D1 read's new rollback refusal alone. It does not: it reads through
  `read_with(.., false)`, the actor read.)

Results on the base:
- The first delta lens's `delta_probe_a` FAILS: "marks left 0", and `b3..00:0` is served `claimable: true`.
- Its `delta_probe_b` FAILS: "redelivery Ok(6) marks left 0", and `b3..00:0` is served `claimable: true`.
- My `lens2_base_m1` FAILS: one actor read of a new player writes 1 mark.
- My `lens2_base_m3` FAILS: the D1 read after a Phase C delivery serves `b3..04:0` and `b3..05:0` as presses from the
  flip's rows.

So NEW-H1 (a), NEW-H1 (b), NEW-M1 and NEW-M3 (its first-epoch case) are each RED on `42dd7f7e` and green on
`e3e70b65`.

Not re-run here:
- The other fold pins' REDs, which the fold made with revert mutants.
- Clippy, wasm32, `make ownership`, `make app-layer-door` and the integration binaries. The fold reports all of them
  green.

## The nine items

| Item | Verdict | Why |
|---|---|---|
| NEW-H1 | CLOSED | **Phase B:** `actor_then_d1` reads `marks_bound` (`owed_judge.rs:598`) before `actor_deliver`, and the D1 judge acknowledges `min(bound, seen)` (`owed_judge.rs:772`). `phase_b_find` does the same (`owed_belt.rs:893`). **Phase C:** the redelivery skip is gone, so every delivery re-applies over the record as it stands, and `as_the_judge` acknowledges up to the `seen` it read before the call (`owed_actor.rs:2216-2263`). **The D1-only judges** are refused under `OWED_ACTOR_READ`: the drain (`owed_judge.rs:1067`), the hooks' drain (`writers.rs:89`), the `owed-v2` pass (`owed_backfill.rs:757`) and its lever (409, except `?aged=1`). The `owed-v3` pass acknowledges nothing (bound 0). Probes: both delta probes RED to green; P1 and P2 green (a mark written just before the ack batch survives and the actor read withholds); the skip mutant is RED. Residual: NEW2-L1. |
| NEW-M1 | CLOSED | The actor read (`read_with(.., false)`) sets `known` from the identity probe and writes no wanted mark (`owed_read.rs:153-158`, `object.rs:1004`). P4: 0 marks over two reads, `known_to_d1` true. A new player's actor is bootstrapped by the read's own arm (`object.rs:1037-1041`) or its first event, so there is no loop. Residual (NOTE-1): in Phase C, such a player never gets a D1 `owed_state`. So each actor read runs the identity probe (one indexed statement), and `/health.owedActor.activeIdentities` undercounts. |
| NEW-M2 | CLOSED | An index-only change keeps the rows (`schema_plan_kept`, `object.rs:117`; `actor_shape`, `owed_actor.rs:604`: tables, UNIQUE indexes and triggers in the shape, plain `CREATE INDEX` out). A column added moves the shape, so it rebuilds. The bootstrap is paced: 60 pages a minute fleet-wide, through the shipped statements under SQLite, and one actor's pages 1 s apart. The PR body states the cutover rule for digest-moving deploys. Residuals: NEW2-L3 (fairness and the budget row's write rate) and NEW2-L4 (a failed `CREATE` is stamped anyway). |
| NEW-M3 | PARTIAL | Every Phase C acknowledgement writes `owed-actor-judged` and deletes the `owed-v3` stamp (`owed_actor.rs:2265-2266`). The D1 read withholds every press while the first stands without the second (`owed_read.rs:131`, `:273`). A deployment that was never in Phase C holds no mark, so it gets no false refusal. But the ack deletes only `OWED_V3_RESTART_SQL[0]`, the stamp, and leaves the pass's cursor row `rollback:owed-v3` at its end: NEW2-M1. |
| H1 | CLOSED | The chunk case was closed by the second fold (the delta lens's probe 1: 500 = 500 rows over 65 chunks). The redelivery case is closed by NEW-H1 (b). |
| H3 | CLOSED | Option (a), actor first, is now sound in both orders: the acknowledgement bound is read before the actor reads the record, in the consumer and in the belt's Phase B find. |
| M1 | CLOSED | The Phase B gate is now coverage, not two counts. The bootstrap lever folds a pass into `owed-actor:coverage` (`coverage_fold`, `owed_actor.rs:1742`), with `complete`, `contiguous` and `uncovered`, served on `/health.owedActor.coverage`. A call out of order breaks `contiguous`. |
| M3 | CLOSED | Through NEW-M1: the gate is D1's identity probe, and nothing is minted or marked. |
| M9 | PARTIAL | The lever's resumable state is the defect NEW2-M1 names. A stamped pass leaves `{"after": <last identity>}`, and nothing but `{"restart": true}` clears it. The cutover doc's step (2) does not say to restart, and says the stamp is "cleared only by the lever's own restart", which the fold made false. |

## New findings

### NEW2-M1 (MEDIUM): a second rollback's `owed-v3` pass stamps without re-judging, and the D1 read serves the flip's stale presses

**Where:**
- `owed_actor.rs:2266`: the Phase C ack runs `OWED_V3_RESTART_SQL[0]`, which deletes the stamp, and not `[1]`, which
  deletes the cursor row.
- `owed_actor.rs:2372-2375`: the pass's final call saves `{"after": <last identity>, "open": null}` beside the stamp.

**What happens:** the next pass with the stamp absent resumes at that cursor. `RECONCILE_NEXT_SQL` finds no identity
after the last one, so the pass writes the stamp at once, having judged nothing.

**The failing input**, `lens2_p5` on the stranded-hops fixture:
1. Phase C epoch 1, then rollback 1: the pass runs to its stamp.
2. Phase C epoch 2: a stranger spends `b3..00:0`. The actor downgrades it (its row's `claimable` is NULL) and the
   marks are acknowledged (0 left).
3. Rollback 2, render 1: one call of `owed_v3_pass` reports `stamped true games 0 rows 0`.
4. Render 2, the D1 read: it serves presses `["b3..00:0", "b3..04:0", "b3..05:0"]`. `b3..00:0` is the hop sweep a
   stranger already spent. Its D1 row was judged before the spend, and no mark is pending.

**What it breaks:**
- "claimable only from a judged row with no pending downgrade", on the rollback path.
- The PR body's and the cutover doc's statement that a rollback "fails safe to no press, never to a stale one".

**How it is reached:** two ways, both ordinary.
- Any second Phase C epoch.
- One Phase C epoch after a stamped run of the pass in Phase A or B. The lever allows that run whenever
  `OWED_D1_JUDGE` is on, and the Linux tier's item 12 (the rollback rehearsal on beta) does exactly this.

**Fold:** run both `OWED_V3_RESTART_SQL` statements in the Phase C ack batch. Or key the pass's state to the epoch it
re-judges, so a pass started with the stamp absent never resumes a stamped cursor. Keep `lens2_p5` as the pin (RED on
`e3e70b65`). Also correct the cutover doc's "cleared only by the lever's own restart".

### NEW2-L1: in Phase B, the compare counts NEW-H1's held mark as a DIFF

`actor_then_d1` compares D1's rows with the actor's after the D1 judge ran (`owed_actor.rs:2147`, `compare` at
`:2153`). When a mark lands during the actor's apply, the D1 judge has judged the newer evidence, the actor has not,
and the mark stands. That is now the designed state, not a defect. But `compare` counts it.

P6 measures it: `owed_actor_diffs_total` goes from 0 to 1, with the log line
`[owed-actor] DIFF owner game 9a00..: 1 outpoints of 6 games, first b3..00:0`.

The cutover's beta-window rule ("any DIFF is a STOP") therefore trips on an ordinary concurrent write.

**Fold:** skip, or count apart, each game that holds a mark of generation above the bound. Or state in the window's
rule that a DIFF whose game held a mark at the compare is not a STOP.

### NEW2-L2: under the read flag, a wipe's part 1 leaves `owed-v2` unstamped, and its pass is refused

WIPE-REHEARSAL-2's part 1 step 7 runs `DELETE FROM view_derivation_state`. That deletes the `owed-v2` stamp, and also
`owed-actor-judged` and `owed-v3`. After it, the `owed-v2` backfill would re-derive D1's `owed_rows`. But under
`OWED_ACTOR_READ` it now stops at once (`Stop::ActorRead`, `owed_backfill.rs:757`), and the lever answers 409.

The consequences:
- In Phase B, D1's mirror (the compare's other side and the rollback's source) is rebuilt only by events. The compare
  then counts DIFFs against bootstrapped actors.
- A Phase B rollback reads an identity with no `owed_state` as `pending` plus a wanted mark, which is safe but slow.

The wipe's own levers (`owed-actor-identities`, `owed-actor-reset`) are unaffected.

**Fold:** state it in WIPE-REHEARSAL-3. Under the read flag the mirror is rebuilt by the `owed-v3` pass (allowed, and
it acknowledges nothing) with `{"restart": true}`, never by `owed-v2`. Alternatively, run part 1 with
`OWED_ACTOR_READ=false`.

### NEW2-L3: the bootstrap budget has no fairness, and its waiters write one hot row

`boot_pace` (`owed_actor.rs:628`) gives each identity a FIXED slot in the next minute. An actor running early in the
minute takes a page every 1 s, so it can take most of the 60 tokens. Actors whose slot falls late wait until the
early ones are done. Total throughput stays bounded at 60 pages a minute, but the order is by hash, so the last actor
waits for the whole rebuild.

Each waiting actor also wakes once a minute and runs the two-statement take against the one row
`owed-actor:boot-budget`. N waiting actors is N writes a minute to that row, and each over-budget take still counts.

**Fold:** state the cost and order, or let a waiter re-read before taking.

### NEW2-L4: a failed index statement is stamped anyway

`ensure` logs a failed schema statement and goes on (`object.rs:384-386`), then writes the render's `digest` and
`shape`. The index-only path now drops EVERY held index before it creates the render's. So a `CREATE` that fails
leaves that actor without the index until the next digest move. Any statement that names it in `INDEXED BY` then
fails on every apply. This is as before for HIGH-4's path; the index-only path widens it to every digest move.

**Fold:** stamp `digest` and `shape` only when every statement of the plan answered.

### NEW2-L5: NEW-L1's fold re-applies a deterministically faulting tip window on every alarm

`tipJudged` now moves only when every apply answered (`object.rs:306-325`). That is right for a transient fault. But
when one game of the window faults deterministically, `(tipJudged, tip]` is re-selected, and every game of it
re-applied, on each tip alarm, with no end and no count. This was reasoned from the code, not run.

**Fold:** advance past the windows that answered, and count the one that did not (its game named).

### NOTE-1

Phase C's new players: see NEW-M1's residual in the table.

### NOTE-2

Dropping the skip re-runs `publish_belt_asks` on every redelivery (`ASKS_CLEAR_SQL` then `ASK_PUT_SQL` over the
actor's rows). These are idempotent writes to the ask rows. Courier calls stay the belt's alone, at 24 a tick. This
is compute and D1 writes only.

## The pillars, again

- **Nothing here moves money.** The fold writes owed rows, marks, the ledger's rows and two
  `view_derivation_state` names.
- **`claimable` only from a fixpoint-committed judged row with no pending downgrade.**
  - It holds on the actor's read in Phases B and C (NEW-H1 closed).
  - It holds on the D1 read during a first rollback (NEW-M3's refusal).
  - It fails on the D1 read after a second rollback: NEW2-M1.
- **No courier in a hot path.** The fold adds D1 statements and Durable Object alarms only. The belt is still the one
  courier asker.
- **No tune given as a fix.**
  - `BOOTSTRAP_PAGES_PER_MINUTE` (60) and `BOOTSTRAP_PAGE_DELAY_MS` (1 ms to 1 s) are new pacing bounds, asked for by
    NEW-M2.
  - No wall was widened, no retry added and no batch enlarged.
  - Dropping the redelivery skip spends compute for correctness. It is not a tune.

## The LOWs of the first delta lens

- **NEW-L1: folded.** It has a residual, NEW2-L5.
- **NEW-L2: folded.** The copy carries `epoch`, and a read serves no copy of another era, or one with none
  (`kv_copy_of_era`).
- **NEW-L3: folded.** The coverage pass is the gate.
- **NEW-L4: stands, stated.** The whole-identity copy is taken on every apply.
- **NEW-L5: stands, stated.** Phase B's inline service time is unmeasured. It remains Linux tier item 4.
- **NEW-L6: folded.** `owed_actor_deferred_total` is counted at the start and `owed_actor_deferred_done_total` at the
  end, both served.
- **The from-scratch lens's LOWs:**
  - LOW-1 stands: no actor families on the cost bar.
  - LOW-2 is partly folded.
  - LOW-3 stands.
  - LOW-7 is as the first delta lens stated.
  - NOTE-1 to NOTE-3 still hold.

## What a further fold must change (one line each)

- **NEW2-M1:** the Phase C ack also deletes `rollback:owed-v3` (or the pass's state is keyed to its epoch); pin
  `lens2_p5`; correct the cutover doc's "cleared only by the lever's own restart".
- **NEW2-L1:** the Phase B compare skips, or counts apart, a game holding a mark above the bound; or the window's STOP
  rule says so.
- **NEW2-L2:** WIPE-REHEARSAL-3 names `owed-v3 {"restart": true}` as the mirror's rebuild under the read flag.
- **NEW2-L3:** state the budget's order and row cost, or re-read before a waiter's take.
- **NEW2-L4:** stamp `digest` and `shape` only when the whole plan applied.
- **NEW2-L5:** move `tipJudged` past the answered windows, and count the faulting one.

## What the Linux tier at the land must exercise

The first delta lens's twelve items, kept or amended:
1. **Kept.** The DO migration on a real deploy, and the first touch after `schema.rs` moves. Amended: an INDEX-ONLY
   move keeps the rows and serves no `pending`, and a column added rebuilds.
2. **Kept as a regression check.** More than 16 games under one event in Phase C.
3. **Kept.** A record fault on a fresh actor's second round, with the `{t}_snap` restore in a real DO SQLite.
4. **Kept** (NEW-L5). The consumer's per-message service time in Phase A (deferred) and in Phase B (actor, then D1,
   inline), against `owed-queue-backlog`.
5. **Amended.** The bootstrap lever paged to its end, then a second pass whose `coverage` reads `complete`,
   `contiguous` and `uncovered` 0.
6. **Kept.** A real KV copy read by the client's parser. Add: a copy of another `epoch` is not served.
7. **Kept.** The direct call's visibility p95 under wasm32.
8. **Kept.** A Phase C belt tick against the counting courier stand-in.
9. **Kept.** NEW-H1's shapes on a real DO and D1:
   - a downgrade written during a Phase B actor apply;
   - a Phase C delivery whose ack batch faults after the actor answered, then a downgrade, then the redelivery.

   Add P1's and P2's shape: the downgrade written just before the ack batch. In each, the read withholds.
10. **Kept.** A Phase C new player's reads: 0 D1 writes per read, and no sweep re-judge.
11. **Amended.** A digest-moving deploy over a populated fleet: the budget's pages a minute, the time until the last
    press is served again, and the D1 writes a minute on `owed-actor:boot-budget` (NEW2-L3).
12. **Amended.** The rollback rehearsal on beta, run TWICE: a Phase C epoch, rollback, a second Phase C epoch, a
    second rollback. The second pass must re-judge every identity before it stamps (NEW2-M1). Read the D1 rows served
    before and after each stamp.

## What the actor's beta window must read

- **The compare's counters.** `comparedTotal` advancing while `diffsTotal` and `faultsTotal` stay flat. Any DIFF line
  is read by its game: in Phase B, a DIFF whose game held a pending mark at the compare is NEW2-L1, not a STOP; any
  other DIFF is a STOP.
- **Phase A's deferred applies.** `deferred` against `deferredDone`: the difference is what `wait_until` cut.
- **The bootstrap's coverage.** `/health.owedActor.coverage`: `complete`, `contiguous`, `uncovered` 0, and
  `startedAtMs` after the last deploy.
- **The queue's service time.** `owedEvents.serviceMs` and `owed-queue-backlog` in the first hour of Phase B.
- **Era and digest moves.** `epochResets`, and the time until reads stop serving `pending`.
- **Before any Phase C rollback.** The pass's state row `rollback:owed-v3` must be absent when the rollback starts,
  until NEW2-M1 is folded (or call `{"restart": true}` first).
- **D1 cost.** D1 rows read an hour, before and after the deploy (LOW-1 stands).
