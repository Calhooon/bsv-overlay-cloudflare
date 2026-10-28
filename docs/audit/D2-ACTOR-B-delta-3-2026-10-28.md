# D2-ACTOR-B-DELTA-LENS-3: the third delta lens on overlay PR 200 after its fourth fold (bsv-low #912)

Written 2026-10-28 by lane D2-ACTOR-B-DELTA-LENS-3 (Wave E, row E3). This is a review document only: no product code
changed, and nothing was committed to PR 200.

What was reviewed:
- PR 200 at `lane/D2-ACTOR-B`, tip `5a88734d`.
- The fourth fold: one commit over `e3e70b65` (`git diff e3e70b65..5a88734d`, 5 files, +633 / -76).

What it was read against:
- bsv-low `docs/audit/D2-ACTOR-B-delta-2-2026-10-28.md` (the second delta lens: NOT LAND, with NEW2-M1 and NEW2-L1 to
  NEW2-L5).
- The fourth fold's brief and its REPORT.
- PR 200's body: its rollback paragraph and its "Second delta lens folds" section.
- `CUTOVER-D2-ACTOR-2026-10-28.md` at bsv-low `9f3bb594`.
- `WIPE-REHEARSAL-3-2026-10-28.md`.

The lens is narrow: the fold's diff and the six items. Every earlier verdict stands, because the diff touches none of
them except NEW-M3 and M9, which NEW2-M1 closes.

## VERDICT: LAND (0 new HIGH, 0 new MEDIUM, 3 new LOW, 2 NOTE)

**NEW2-M1 is CLOSED, and NEW-M3 and M9 are CLOSED with it.**
- The fold's pin goes RED on `e3e70b65`, grafted there: "rollback 2's pass stamped having re-judged 0 games; the D1
  read serves [b3..00:0, b3..04:0, b3..05:0]".
- It is GREEN on the tip.
- With the fold's one added statement removed from the tip, the pin and two of my probes go RED.

**The five LOWs are folded.** Three of the folds leave a residual of their own: LOW-1, LOW-2 and LOW-3 below. None of
them can serve a press over a pending downgrade, and none puts a courier in a hot path.

What this means for deployment:
- **Phases A, B and C may land as rendered.**
- The three LOWs are what the beta window and the rollback rehearsal must read (the last section).

### How this was verified

All runs were native debug builds, in two scratch worktrees: `~/bsv/wt-lens3-tip` at `5a88734d` and
`~/bsv/wt-lens3-base` at `e3e70b65`. Each was removed afterwards. The evidence is in
`/Volumes/Lanes/out/wt-overlay-actor-b-lens/delta3/`:
- `lens3-probes-tip.diff`: my probes.
- `lens3-m1-graft-base.diff`: the fold's M1 pin grafted onto the base.
- `lens3-probes.out`: every figure quoted here.

Results on the tip:
- `--lib -- lens3 fold4`: 9 passed (the fold's 5 pins and my 4 probes).
- The `-- owed` suite piece: 156 passed, 1 ignored. That is the fold's 152 plus my 4.
- The fold's five pins are green.

Results on the base: the M1 pin, grafted, FAILS with the message quoted above.

The mutant: I removed `Stmt::new(OWED_V3_RESTART_SQL[1], vec![])` from the Phase C ack (`owed_actor.rs:2422`). The fold's
`m1` goes RED, and so do my `lens3_pa` ("epoch 2: stamped having judged 0 of 6") and `lens3_pb`. So the pin is real,
and it rests on the one added statement.

Not re-run here:
- The other suite pieces (`-- lb`, `-- tracker`, the rest).
- The integration binaries, clippy, wasm32, `make ownership` and `make app-layer-door`. The fold reports all of them
  green.
- Real D1, a real Durable Object and wasm32. In particular, `RETURNING` on D1's conditional upsert (NEW2-L3) and the DO
  alarm path are not run here.

## The six items

| Item | Verdict | Why |
|---|---|---|
| NEW2-M1 (and NEW-M3, M9) | CLOSED | **What the ack clears:** the Phase C ack batch (`as_the_judge`, `owed_actor.rs:2406-2423`) runs both statements of `OWED_V3_RESTART_SQL`, the `owed-v3` stamp AND the cursor row `rollback:owed-v3`. It does this beside `ACK_DIRTY_SQL` and `PHASE_C_MARK_SQL`, in one D1 batch. A fault anywhere in that batch rolls all four back, and the message is handed back, so no partial state is left. **Can a pass with the stamp absent resume a stamped cursor?** Only two writers delete the stamp: the ack and the lever's restart. Both now delete the cursor too. So after any Phase C epoch the pass starts from `''`. The one remaining way to that state is a wipe's `DELETE FROM view_derivation_state` with `owed_belt_state` kept; WIPE-REHEARSAL-3 now names the restart for it. **Probes:** `lens3_pa` runs three Phase C epochs, a rollback after each, every pass WITHOUT restart and 1 game a call. Strangers spend `b3..00:0` in epoch 2 and `b3..04:0` in epoch 3, each acknowledged with 0 marks left. Each pass judges 6 of 6 games in 7 calls before it stamps. The D1 read serves `[..04:0, ..05:0]` after epoch 2 and `[..05:0]` after epoch 3, never a spent hop. `lens3_pb`: a Phase C ack lands between two calls of a pass; the cursor row is gone, and the next call walks from the first identity (6 games, no press over `..00:0`). The residual is NOTE-1. |
| NEW2-L1 | FOLDED | `compare` (`owed_actor.rs:2265`) runs `HELD_GAMES_SQL` (`:2257`) in the same batch as D1's rows, then splits the DIFFs by game (`:2312`). A DIFF on a game holding a mark above the bound goes to `owed_actor_compare_held_total`. That counter is seeded by the overlay (`applayer_counters::OWED_ACTOR`, now 19 entries) and served as `/health.owedActor.heldDiffsTotal` and `/health/invariants.owedActor.heldDiffs`. Its line reads `DIFF (held mark, NEW2-L1; not a STOP)`. `owed_actor_diffs_total` stays flat, as the fold's `l1` shows (it was the lens's P6), and the next delivery shows no DIFF of either class. If the held statement answers nothing, the DIFF counts as a DIFF, which is the safe side. The residual is LOW-3. |
| NEW2-L3 | FOLDED | The take is one conditional upsert with `RETURNING` (`owed_actor.rs:630`). A full minute's take writes nothing and answers no row; `boot_taken` reads that as `i64::MAX`, so the page waits. The pin `l3` checks this under SQLite: the 61st take's `changes()` is 0 and the row's count stays 60. The order (a fixed slot per identity hash, no queue) is stated in the code and in the PR body. `RETURNING` on real D1 is unverified. |
| NEW2-L4 | FOLDED | `ensure_schema` (`object.rs:122`) stamps `schema` and `shape` only when every statement answered. The pin `l4` is green, and `local()` has no stamp outside the call. The residual is LOW-2: on the REBUILD path the retry is a whole rebuild on every new instance. |
| NEW2-L5 | FOLDED | `tip_window` (`owed_actor.rs:1380`) always stamps `tipJudged` at the tip. A faulting chunk is re-applied one game at a time, and only the faulting game is carried in `meta.tipFaults`. At its third consecutive fault the game is parked (`:1397`). Each alarm with a fault counts once (`owed_actor_tip_faults_total`, served as `tipFaults`), and its line names the game. `apply_route` clears a game's entry when an apply of it lands, and the rebuild clears `tipFaults`. The pin `l5` is green. Answered games are never re-applied for a faulting one, and none is left unjudged by the split. The residual is LOW-1: a fault that belongs to the whole actor, not to one game, parks every game of the window. |
| NEW2-L2 | STATED | The PR body says that under `OWED_ACTOR_READ` the mirror is rebuilt by the `owed-v3` pass with `{"restart": true}`, and that the `owed-v2` lever answers 409 there. WIPE-REHEARSAL-3 (line 48) says the same. |

## New findings

### LOW-1: a fault of the whole actor parks every game in the tip window, and in Phase C little re-judges a parked gate

**Where:** `owed_actor.rs:1380-1434` (`tip_window`), with the park at `:1397`, and the alarm at `object.rs:326-366`.

**The problem:** the park counts faults per game, but nothing tells a fault of the GAME from a fault of the ACTOR. A
tip apply fails for every game when the actor's D1 record is unreachable, because `apply_route` takes
`D1Sql::of(&self.env)` and `apply_and_publish_in_turn` writes through it.

**The failing input,** `lens3_pd`: a window of 130 games, and three alarms in a row in which every apply faults.
- Each alarm makes 133 applies: 3 chunks, then 130 singles. Before the fold it made 3.
- All 130 games are parked after the third alarm.
- The fault then clears, and a later window selects `g000` again (its gate opens). The tip alarm applies only the new
  game. `g000` stays parked.

Tip alarms come at the next gate's time, or every block while a courier-dated sweep is held. So about 30 minutes of D1
trouble is enough.

**What re-judges a parked game:** only an apply by another road: an event on the game's sources, or the reconcile.
- In Phases A and B, the hook's `tip` mark (`MARK_TIP_SQL`) also reaches the game's gate, because D1's `owed_rows`
  carry its `gateHeight`.
- In Phase C, D1's `owed_rows` are not written. So a game created in Phase C has no D1 gate to mark. Its refund-due
  press, which a gate opening creates without any source write, then waits for the reconcile. The reconcile samples 64
  identities a day fleet-wide (`RECONCILE_SAMPLE_PER_DAY`), so the wait can be days.

**What it cannot do:** serve a wrong press.
- A parked row keeps its last judgement.
- The actor's read serves through `Snapshot::body`, which still degrades a courier-only sweep against the present tip
  (`owed_read.rs:264`) and withholds over a pending downgrade.
- So the cost is liveness (a delayed refund press) and the 1+1/64 fault amplification, never a stale press.

**Fold:** do not count a fault toward the park in an alarm where no single apply landed. That case is the actor's
fault, not a game's: carry the games without counting them. Also serve the parked count (for example
`owed_actor_tip_parked_total`, or the parked list in `meta`).

### LOW-2: on the rebuild path, a plan statement that always fails rebuilds and re-bootstraps the actor on every new instance

**Where:** `object.rs:420-445` (`ensure_schema`'s call, `ready` set either way) and `schema_plan_kept`
(`object.rs:147-177`).

**The problem:** when a digest move is a rebuild (the held shape differs), a failing statement now leaves `schema` and
`shape` at their old values. Each new instance of the object computes the same rebuild again. That rebuild:
- drops the mirror's families, the judge's tables, `txfacts`, `applied` and the snapshots;
- clears `bootstrapped`;
- sets `bootCursor` 0.

So a statement that fails deterministically for the render (a DO SQLite refusal that rusqlite accepts, say) means:
- every actor serves `pending` for as long as the render stands;
- every actor re-spends the fleet's bootstrap budget (60 pages a minute) from the start at each new instance;
- an actor whose instances recycle faster than its bootstrap never finishes.

This is reasoned from the code, not run on a Durable Object.

It is the safe direction for money: `pending` serves no press. But nothing counts it. The only signal is the log line
"the plan did not apply whole; digest and shape NOT stamped".

**Fold:** count it (`owed_actor_schema_partial_total`, served). Then either stamp `shape` when every TABLE statement
answered, so the retry is the index-only path that keeps the rows, or keep the old rule for a failed `CREATE INDEX` on
the rebuild path. Stamp the digest only when the plan is whole, as now.

### LOW-3: the apart counter hides a real divergence on any game that holds a mark at the compare

**Where:** `owed_actor.rs:2312`. The split is by GAME only. It never asks whether the diffing outpoint is one the
standing mark could change.

So an actor bug on a game is counted apart (not a STOP) on every compare at which that game holds a mark above the
bound. A one-off coincidence is caught at the next compare: the fold's own `l1` sees no DIFF on the next delivery. But
a hot game, with writes landing during each apply, can hide a divergence for as long as it stays hot.

**Fold:** no code change is needed for the land. The window's rule should add that a `DIFF (held mark ...)` line
naming the SAME game across successive compares (three, say), or `heldDiffsTotal` rising faster than `comparedTotal`'s
share of concurrent writes, is a STOP to look at. Optionally, re-compare a held game once after its mark is applied.

### NOTE-1: an acknowledgement inside a pass call is overwritten by that call's save

`lens3_pc` measures the mechanism. The ack's two deletes landed between a call's state read and its save
(`owed_actor.rs:2506`). The save recreated `rollback:owed-v3` at the call's position (`open: [owner, 9a..01]`), so the
pass then resumes past the games it judged before the ack.

To lose a re-judge this way, a Phase C acknowledgement must run DURING render 1's pass. That means an old-render consumer
still in flight, and its message's evidence would have to be written after the pass judged that game. Under an
immediate deploy (LOW's `wrangler deploy`), every message an old consumer still holds was enqueued before the deploy,
so its evidence predates the pass, and the pass reads it.

So it is unreachable as LOW deploys. It is reachable only under a gradual (split-version) deployment of render 1.

If that ever changes, the fold is a run token: the restart writes one, each save is conditional on it, and a missing
token restarts the walk. Nothing to fold now. The cutover doc should say that render 1 is an immediate deploy.

### NOTE-2: a carried, unparked tip fault is retried only when an alarm is armed

`arm_tip_alarm` arms at the next gate above the tip, or the next block while a courier-dated sweep is held. A carried
game whose own gate is at or below the tip, on an actor with no later gate and no held sweep, waits for an event. That
was so before the fold too: NEW-L1's open window had the same arming. Stated only.

## The fourth fold's additions, judged cold

- **The Phase C ack batch** writes four statements in one D1 transaction:
  - the generation-bounded acknowledgement (`ACK_DIRTY_SQL`, bound `seen` read before the actor's call);
  - `owed-actor-judged`;
  - the stamp's delete;
  - the cursor's delete.

  A fault between them is a rolled-back batch and a handed-back message. Each Phase C ack pays two primary-key deletes
  more. That is negligible.
- **The apart counter's rule:** see LOW-3. It hides nothing permanently. It can hide a hot game's divergence for as long
  as the game is hot.
- **The stamp rule on an actor whose plan always fails:** this instance does not loop, because `ready` is set.
  - On the index-only path, each new instance re-drops and re-creates the indexes, which is cheap.
  - On the rebuild path, each new instance rebuilds and re-bootstraps (LOW-2).
- **The tip window's advance:** no answered game is left unjudged. A chunk that faults is re-applied game by game, and
  every game that lands clears its entry. What the advance can leave unjudged is a PARKED game's later gate (LOW-1).

**The pillars:**
- `claimable` comes only from a fixpoint-committed judged row with no pending downgrade. This holds in Phase B and in
  Phase C on the actor's read (unchanged), and on the D1 read after one, two or three rollbacks (`lens3_pa`, passes
  called without restart).
- No courier in a hot path: the fold adds D1 statements and DO `meta` writes only.
- No tune given as a fix: `TIP_FAULTS_MAX` (3) is a new bound for a new mechanism, and nothing was widened.

## What a further fold must change (one line each)

- **LOW-1:** an alarm in which no single apply landed carries its games without counting them toward the park; serve
  the parked count.
- **LOW-2:** count an unstamped plan, and on the rebuild path let a failed index retry as index-only (stamp `shape` when
  the tables applied).
- **LOW-3:** the window's rule: the same game's held DIFF across successive compares is a STOP to look at.
- **NOTE-1:** the cutover doc says Phase C's rollback render 1 is an immediate deploy, never a gradual one.

## What the actor's beta window and the rollback rehearsal must read (amended from the second lens)

Kept from the second lens: the compare's counters, Phase A's deferred figures, the coverage, the queue's service time,
the era and digest moves, and the D1 rows an hour either side of the deploy.

Amended:
1. **The rehearsal runs twice**, as the cutover doc now says.
   - For the second rollback, call the pass `{}` ONCE before any restart, and record that it does NOT answer
     `stamped: true` with `games: 0`. That is the fold's fix read live.
   - Then call it with `{"restart": true}` and run it to its stamp, as the doc's rule says.
   - Record the D1 presses served before and after each stamp.
2. **Phase B DIFFs:** `diffsTotal` stays flat (the STOP). For `heldDiffsTotal`, read the games named in its lines; the
   same game named on successive compares is a STOP to look at (LOW-3).
3. **`tipFaults`** (`/health.owedActor.tipFaults`, `/health/invariants.owedActor.tipFaults`) and the
   `TIP WINDOW FAULT` lines: any alarm that parks games is a finding to record. An alarm that parks every game of its
   window means a fault of the whole actor (LOW-1). In Phase C, check that a parked game's gate press appears on its
   next event.
4. **After any deploy that moves the schema digest:** look for the log line "NOT stamped" (LOW-2). If it appears, the
   render's plan does not apply on a real DO: STOP and fix the plan, because every actor reads `pending` until it does.
5. **The bootstrap budget row** `owed-actor:boot-budget`: during a rebuild it takes one write a token and no write per
   waiter (NEW2-L3, on real D1).
6. **Render 1 of a Phase C rollback** is an immediate deploy (NOTE-1).
