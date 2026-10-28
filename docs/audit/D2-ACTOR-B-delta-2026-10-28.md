# D2-ACTOR-B-DELTA-LENS: the delta lens on overlay PR 200 after its two folds (bsv-low #912)

Written 2026-10-28 by lane D2-ACTOR-B-DELTA-LENS (Wave E, row E3). This is a review document only: no product code
changed.

What was reviewed:
- PR 200 at `lane/D2-ACTOR-B`, tip `42dd7f7e`.
- The HIGH fold `ee48085c` and the MEDIUM fold `42dd7f7e`, both on top of `8af15944`.

What it was read against:
- bsv-low `docs/audit/D2-ACTOR-B-lens-2026-10-28.md` (the from-scratch lens: 4 HIGH, 9 MEDIUM, 7 LOW, 3 NOTE).
- The two fold briefs and their REPORTs.
- `DESIGN-D2-ACTOR-2026-10-26.md` sections 4 to 8.
- `CUTOVER-D2-ACTOR-2026-10-28.md`.
- `WIPE-REHEARSAL-3-2026-10-28.md`.
- bsv-low `app/src/lib/owed.ts`.

## VERDICT: NOT LAND (1 new HIGH, 3 new MEDIUM, 6 new LOW)

The folds did what the lens asked for most of its findings:
- **HIGH-2 and HIGH-4 are closed.** The lens's probes 2 and 3, re-run on the tip with their assertions reversed,
  pass. A digest change now rebuilds `owed_rows` (15 columns). Of 10 faulted applies, none keeps a partial
  judgement.
- **Eight of the nine MEDIUMs are closed or narrowed to a stated residual.**
- **HIGH-1's failing input is closed.** Probe 1 on the fleet whale `03dd..dd` sends 1,033 games in 65 chunks under
  one event `(pot, 7)`. No chunk is skipped, and the actor stores the D1 judge's 500 rows.

One defect remains in the rule the folds rest on: **which marks an acknowledgement may remove.** Two probes on the tip
reproduce it:
- In Phase B, the D1 judge acknowledges a downgrade mark that was written while the actor was applying.
- In Phase C, a redelivery acknowledges a downgrade mark that is newer than the actor's apply.

In both cases the read then serves the actor's stale claimable row, with nothing pending. This breaks D2-L1 ("never
a press over a pending downgrade"), which is the rule HIGH-3 was about. So HIGH-1 and HIGH-3 are PARTIAL.

What this means for deployment:
- **Phase A can still be deployed as rendered.** The actor serves no read, and its apply runs off the consumer.
- **Phases B and C wait for NEW-H1.** Phase C also waits for NEW-M1 and NEW-M3.

### How this was verified

All runs were native debug builds, in a scratch worktree of `42dd7f7e`, removed afterwards. The probe source is in
`/Volumes/Lanes/out/wt-overlay-actor-b-lens/delta/delta-probes.diff`, and its output in `delta-probes.out` in the
same directory. Nothing was committed to PR 200.

- `--lib d2_actor_b`: 25 passed. That is the PR's 13 pins, the HIGH fold's 4 and the MEDIUM fold's 8.
- `--lib -- owed`: 138 passed, 1 ignored. This matches the fold's count of 139.
- The lens's probes 1, 2 and 3, re-run with their assertions reversed, all pass (the figures are above).
- Two new probes, `delta_probe_a` and `delta_probe_b`, both FAIL on the tip; their failures are the defect (NEW-H1).

On the folds' RED evidence:
- The HIGH pins' REDs on `8af15944` are the fold's own record (`fold-red-on-8af15944.txt`, with two inert
  stand-ins).
- The MEDIUM pins' REDs are revert mutants on the tip (`fold2-red-mutants.txt`).
- I did not re-run either RED set. The lens's original probes pass on `8af15944` (the lens's run) and fail-safe on
  the tip (this run), and that is the RED-then-GREEN evidence I rely on for the HIGHs.
- MEDIUM-5's RED is a source pin only. Its reset (`deleteAll` inside the turn) has not run in a real Durable Object.

## The 13 findings

| Finding | Verdict | Why |
|---|---|---|
| HIGH-1 | PARTIAL | The chunk case is closed: `applied (kind, seq, game)`, Phase C acknowledges `appliedGames` only, and probe 1 gives 500 = 500 rows over 65 chunks. But a game "applied by an earlier delivery" is acknowledged up to a generation read at the REDELIVERY, so a mark newer than the actor's apply is removed unjudged: NEW-H1 (b). |
| HIGH-2 | CLOSED | `Noting` stages each round's commit, and `commit_fixpoint` (`owed_actor.rs:992`) runs only the fixpoint's commit, in one synchronous batch on `DoSql` (no yield to JS between the copy, the commit and the restore). Probe 3: 0 of 10 partial judgements kept. Pin h2 covers a read during a slow apply and a commit cut half way. Its cost is NEW-L4. |
| HIGH-3 | PARTIAL | Option (a) is built: `actor_then_d1` (`owed_actor.rs:2052`) applies the actor first and hands the message back on an actor fault, and belt finds reach the actor in Phases A and B (`owed_belt.rs:882`). But the D1 judge's acknowledgement bound is read AFTER the actor's apply (`owed_judge.rs:605`): NEW-H1 (a). |
| HIGH-4 | CLOSED | `schema_plan` (`object.rs:110`) drops the judge's tables, `txfacts`, `applied` and the copies, clears `bootstrapped`, re-arms the bootstrap, and the read serves `pending` until it ends. Probe 2: 15 columns. The fleet-wide cost of a rebuild is NEW-M2. |
| MEDIUM-1 | PARTIAL | The bootstrap now runs in Phase A (the first event arms it, and there is a bearer lever, 50 identities a call). The read serves `pending` plus the display-only copy while it runs. But the Phase B gate the fold names, `bootstraps` against `activeIdentities`, compares a count of completions with a count of identities: it does not prove coverage (NEW-L3). |
| MEDIUM-2 | CLOSED | `fallback_body` (`owed_actor.rs:1241`) goes through `Snapshot::body` with `v`, and every row has `claimable: false`, `claimReason` and `displayOnly`. No copy gives a 503. bsv-low's `parseOwedBody`, run verbatim, keeps 6 of 6 rows with 0 presses (`fold2-m2-client-parse.txt`). |
| MEDIUM-3 | PARTIAL | A stranger's read mints nothing: `/rows` on a fresh object writes nothing, and `read_snapshot` reads only D1's state and marks. But the gate, `known_to_d1` (`object.rs:904`), is D1's `owed_state` row, which nothing writes in Phase C: NEW-M1. |
| MEDIUM-4 | CLOSED | `GATED_GAMES_SQL` (`owed_actor.rs:1322`) selects `(tipJudged, tip]`, and the next alarm is set at the next gate. The courier-dated sweep arm is safe: re-judging at a new tip over a memo below it gives `unspent-predates-tip`, not claimable (`owed.rs:2555`), so the row leaves the selection. The one residual is NEW-L1. |
| MEDIUM-5 | CLOSED | The epoch is `STORAGE_EPOCH\|WRITTEN_OFF_BEFORE_MS` (`owed_actor.rs:1293`), checked on fetch and alarm, and a mismatch resets the actor inside its turn. Its source matches WIPE-3 item 5: the era bump at part 2's step 6 comes after the wipe, so an actor re-minted between step 4a and the wipe holds the old stamp and is reset at the bump. The residual is the KV copy (NEW-L2). |
| MEDIUM-6 | CLOSED for Phase A | Phase A's apply and compare run after the batch under `ctx.wait_until` (at most 256 per isolate, otherwise inline). Measured service time: 22 ms deferred against 1,091 ms inline (native, whale). Phase B now carries the actor inline by HIGH-3's design, which Phase A cannot measure (NEW-L5). |
| MEDIUM-7 | CLOSED | The bars are durable per class (`owed-actor:bars:{direct,queue,mined}` in the belt's ledger), the actor's own oldest-unapplied age is served, and bar (2) is restated as the queue's figure. Under concurrent flushes a sample can be lost (read-modify-write); this is stated. |
| MEDIUM-8 | CLOSED | `/health.owedActor` serves `comparedTotal`, `diffsTotal` and `faultsTotal` from the durable counters (`HEALTH_SQL`, `owed_actor.rs:1661`), asserted in m7's body. Each DIFF line names its identity and game. In Phase B this compare would count NEW-H1 (a). |
| MEDIUM-9 | PARTIAL | The `owed-v3` lever exists: bearer `INTERNAL_TOKEN`, 409 while the D1 judge is off, 256 games a call, resumable, stamped, pinned by m9. But nothing stops a rollback from serving D1's stale rows before the stamp, and the cutover's rollback line is one render: NEW-M3. |

## New findings

### NEW-H1 (HIGH): an acknowledgement can remove a mark the actor never judged, in Phase B and in Phase C

D2-L1's withholding exists only while a downgrade mark is pending. Both acknowledgement paths can delete a mark whose
evidence the actor's last apply never read. Once the mark is gone, the read serves the actor's older claimable row,
and nothing re-drives it: the source write's own event finds no mark for its pair, so it routes nothing. The row
stands until another event names the game, the 64-a-day reconcile samples the identity, or (in Phase C) the age
repair reaches it.

**(a) Phase B.** `actor_then_d1` (`owed_actor.rs:2052`) awaits the actor's apply, then calls `judge_subjects`. That
reads `MARKS_SQL` and takes its `seen` generation at `owed_judge.rs:605`, which is after the actor read the record. A
downgrade written during the actor's apply (for example, a competitor's spend of the hop) is therefore:
1. judged by D1, which downgrades D1's row;
2. acknowledged by D1, so the mark is gone;
3. never seen by the actor, whose row the read serves.

The same ordering applies to `phase_b_find` (`owed_belt.rs:882`: the facts, the actor, then `judge_with_write`). It
also applies to the two D1-only judges a Phase B render still allows: the `owed-v2` lever (gated only on
`OWED_D1_JUDGE`) and the `owed-v3` pass.

Failing input, `delta_probe_a`, run on the stranded-hops fixture in Phase B:
1. The first delivery serves `b3..00:0` claimable, which is correct.
2. A pending `age` mark is planted, and `SPENT_BY_A_STRANGER` is executed inside the actor call, after its apply
   returns.
3. Result: D1's row is downgraded (`claimable` NULL), the marks left are 0, and the read serves `b3..00:0`
   `claimable: true`.

The window is the actor's apply, which can be seconds on a whale over D1. The compare after the D1 judge counts it as
a DIFF, so in Phase B it is visible but not prevented.

**(b) Phase C.** `as_the_judge` (`owed_actor.rs:2134`) reads `seen` at `:2147`, at the time of THIS delivery.
`apply` (`owed_actor.rs:834-839`) skips every game the event's earlier delivery applied, but reports it in
`appliedGames`, so it is acknowledged up to the new `seen`. The sequence:
1. A delivery whose actor apply landed is handed back before its acknowledgement landed. This happens when the
   `ACK_DIRTY_SQL` batch faults, when the 60 s message wall falls after the actor answered, or when the queue
   redelivers (it is at-least-once).
2. A downgrade lands on the game before the redelivery.
3. The redelivery acknowledges that downgrade unjudged.

Failing input, `delta_probe_b`:
1. Event `(pot, 7)` is applied by the actor with its acknowledgement lost.
2. `SPENT_BY_A_STRANGER` writes 2 marks.
3. The redelivery answers `Ok`, 6 subjects, and leaves 0 marks; the read serves `b3..00:0` `claimable: true`.

On `8af15944` the whole-event skip had the same flaw. The fold made it explicit through `appliedGames`.

**Fold:**
- **Phase B:** read the identity's marks' maximum generation BEFORE the actor's call, and bound the D1 judge's
  acknowledgement by it (pass it into `judge_with_write`, or acknowledge separately with that bound). Do the same in
  `phase_b_find`. Refuse the `owed-v2` lever and the `owed-v3` pass while `OWED_ACTOR_READ` is on, or route them
  through `actor_then_d1`.
- **Phase C:** store, beside each `applied (kind, seq, game)` row, the generation the original delivery acknowledged,
  and on a skip acknowledge at most that. Or drop the skip: a re-apply reads the record as it stands and is the same
  judgement or a newer one.

**Pins:** the two probes, kept as written, each RED on `42dd7f7e`.

### NEW-M1 (MEDIUM): in Phase C, an identity first seen after the flip loops on wanted marks

`read_snapshot` (`object.rs:914`) calls the actor only if `known_to_d1`: either D1 has an `owed_state` row, or
`snapshot.wanted` is set. In Phase C nothing writes D1's `owed_state`. Only the D1 judge's commit creates it
(`STATE_CREATE_SQL`), and `as_the_judge` writes no D1 row.

So for an identity whose first party or hop row lands after the flip:
1. Every read takes the identity-probe arm of `owed_read::read_with` and writes `MARK_WANTED_SQL`
   (`owed_read.rs:126`).
2. The body is `pending: true` with `retryAfterMs: 1500`.
3. The sweep re-publishes the wanted envelope, which expands to every game of the identity (32 `wanted`, the rest
   `backfill`), and the actor re-applies all of them.
4. This repeats for as long as the client keeps reading.

The rows served are correct (they are the actor's), so no money is at risk. The cost is one D1 write per read and a
whole-identity re-judge per sweep tick, for every new player from the flip onward. This was reasoned from the code,
not run.

**Fold:** in the actor read, gate on the identity probe (`OWED_IDENTITY_PROBE_SQL`: a party, hop or evicted-party row)
and write no wanted mark when the actor serves the identity. Alternatively, have the actor's Phase C apply write the
identity's D1 `owed_state` row.

### NEW-M2 (MEDIUM): a digest change or an era move withholds every press fleet-wide, and the rebuild is unpaced

What moves the digest: `schema::DIGEST` covers every mirrored family's DDL and the judge's tables, indexes included.
Any overlay migration that adds or edits an index on `owed_rows`, `owed_dirty` or a party table therefore moves it.
The October folds did this several times. A move of `WRITTEN_OFF_BEFORE_MS` (the #375 write-off cutoff) resets every
actor the same way (MEDIUM-5).

What happens on a move:
- Each actor drops its judge tables at its next touch.
- Its read serves `pending` with the display-only copy.
- In Phase C there is no D1 fallback, so no press is offered until that actor's own bootstrap ends.
- The bootstraps are paced only by touches. Each one pages its whole game set back to back, on a 1 ms alarm
  (`object.rs:691`). Each page is a full D1 closure with the BEEF hex.
- Nothing bounds how many actors bootstrap at once across the fleet. Every read in Phases B and C touches its actor,
  so the reads themselves start the storm.

The lens's MEDIUM-1 concern (every active identity bootstrapping against the one D1) therefore comes back at every
such deploy, not only at the Phase B flip.

**Fold:**
- Rebuild only what moved: an index-only change runs its own `DROP INDEX` / `CREATE INDEX` and keeps the rows.
- Pace the bootstrap pages: a delay per page, and a fleet budget read from D1.
- Cutover rule: no digest-moving or era-moving deploy in Phase C without the bootstrap lever paged to its end and its
  coverage read (NEW-L3).

### NEW-M3 (MEDIUM): the Phase C rollback can serve D1's stale rows as presses before `owed-v3` is stamped

`CUTOVER-D2-ACTOR-2026-10-28.md` section 4 (line 100) rolls back with `OWED_ACTOR_READ=false` plus
`OWED_D1_JUDGE=true` in ONE render. Two things make that unsafe:
- The lever refuses with 409 until the D1 judge is on, so the pass can only start after that render.
- No read path checks the `owed-v3` stamp.

So from that render until the pass stamps `owed-v3`, the D1 read serves D1's `owed_rows` as they stood at the
Phase C flip. During Phase C the actor acknowledged every mark, so none is pending and nothing withholds those stale
presses. Running the pass under a Phase B render instead hits NEW-H1 (a): the pass acknowledges with the D1 judge
alone while the read serves the actor.

**Fold:**
- The rollback is two renders. First `OWED_D1_JUDGE=true` with `OWED_ACTOR_READ=true`, with the pass acknowledging
  under the actor's bound (NEW-H1's fold) or not acknowledging at all. Then `OWED_ACTOR_READ=false`, once the stamp
  is read.
- State this in the cutover doc. Or make the D1 read withhold presses while a Phase C marker is set and `owed-v3` is
  absent.

### NEW-L1: the tip alarm advances `tipJudged` past a faulted apply

At `object.rs:292-294` (`let _ = self.apply_route(...)`, then `tipJudged = t`), a gate that opened during an actor
fault is not selected again. In Phase C the D1 tip marking (`MARK_TIP_SQL`) reads D1's frozen `owed_rows`, so that
refund's press waits for another event on its game. This delays liveness only; no money is at risk.

**Fold:** advance `tipJudged` only over the windows whose applies answered `Ok`.

### NEW-L2: the KV copy carries no era

`kv_copy_body` (`owed_actor.rs:1221`) stamps no epoch, and the epoch reset does not delete the KV key. After an era
reset or a HIGH-4 rebuild, the fallback displays the previous era's rows (display only, not claimable) until the
bootstrap ends.

**Fold:** stamp the epoch in the copy, and serve no copy of another era.

### NEW-L3: `owed_actor_bootstraps_total` is not a coverage figure

The counter counts completions. That includes re-bootstraps after a rebuild or a reset, and identities with no rows,
so `bootstraps >= activeIdentities` can hold while active identities remain unbootstrapped.
`activeIdentities` is also a `COUNT(*)` over `owed_state` on each `/health` read.

**Fold:** gate Phase B on a full pass of `POST /internal/owed-actor-bootstrap` in which every answer is already
bootstrapped (`armed` 0), or serve a distinct count.

### NEW-L4: the HIGH-2 copy is the whole identity on every apply

On every apply, `snapshot_stmts` (`owed_actor.rs:947`) copies every row of the identity in four tables, even when the
commit judges one game:
- About 1.5 to 2 thousand rows written per apply on the whale.
- The release apply p95 moved from 302 to 322 ms, against the cutover's 300 ms apply bar, before any D1 hop.

**Fold:** `ctx.storage.transactionSync` called through `js_sys::Reflect` (workers-rs does not wrap it), or a copy
bounded to the games plus the ceiling's possible victims.

### NEW-L5: Phase B's serial consumer carries the actor inline, unmeasured

HIGH-3 option (a) makes every Phase B message apply the actor, then the D1 judge. The fold measured 1,091 ms p50
native inline for the whale, against D1 alone. Phase A's 22 ms deferred figure says nothing about this, so
`owed-queue-backlog` at the Phase B flip is unmeasured.

This is a measurement gap, not a code defect. It is Linux tier item 4, amended below.

### NEW-L6: a deferred Phase A apply cut by `wait_until`'s platform bound is lost uncounted

When this happens there is no compare and no fault counted (`owed_actor.rs:2017`, `run_deferred`). As a result,
Phase A's `comparedTotal` under-covers the window by an unknown amount.

**Fold:** count `owed_actor_deferred_total` at the start of each apply and a `done` count at its end, and serve both.

## The pillars, again

- **Nothing here moves money.** The actor and the levers write owed rows, marks and memos only.
- **`claimable` only from a judged row.** This holds for every row the actor commits: the fixpoint alone, restored on
  a fault (HIGH-2). The fallback rows are never claimable (MEDIUM-2). NEW-H1 does not break this rule: the row it
  serves was judged at a fixpoint. It breaks D2-L1 instead.
- **No courier in a hot path.** The actor asks chaintracks only (header rechecks and canonical roots, through
  `EnvOutside`). The belt is the only courier asker in every phase.
- **No tune given as a fix.** Neither fold widened a wall, added a retry or enlarged a batch. The new constants are new
  bounds: `APPLIED_KEEP_MS` 7 d, `BLOCK_MS`, `BARS_FLUSH_*`, `DEFERRED_MAX` 256, `BOOTSTRAP_PER_CALL` 50.
- **A name is never authority.** The actor is addressed by the read's authenticated, lowercased key (`actor_name`:
  66-hex). Its `/bootstrap` and `/apply` bodies carry an identity, but only from the app layer's own stub of that same
  key, and the object has no public route.
- **Stated, not changed:** the object does not compare a body's identity with its own name (workers-rs exposes no
  name for an id).

## The LOWs and NOTEs of the first lens

- **LOW-1 stands.** No actor or queue families are on the cost bar; it is stated as not built.
- **LOW-2 is partly folded.** `applied` is pruned after 7 days (`APPLIED_PRUNE_SQL`). The `txfacts` `event` rows and
  the mirror rows of txids no longer named stand.
- **LOW-3 stands.** In Phase C, `PROOF_LATCH_MOVED_DELETE_SQL` lands in the mirror only.
- **LOW-4 is folded for Phase C.** The `owed-v2` lever answers 409 while the D1 judge is off. Its Phase B half is
  part of NEW-H1 (a).
- **LOW-5 is folded.** The read takes D1's state and marks only.
- **LOW-6 is folded.** The bootstrap pages on a keyset over `(at, gameId)`.
- **LOW-7 is partly folded.** The direct call is split. `apply` still truncates at 64 without saying so, but Phase C
  acknowledges only `appliedGames`, and the consumer's chunks are 16, so nothing is lost.
- **NOTE-1 stands.** The turn is a mutex held across the outside reads.
- **NOTE-2 stands.** No pin runs a long-lived mirror whose other games' records moved.
- **NOTE-3 still holds**, re-checked on the tip:
  - the names;
  - no D1 migration;
  - the levers behind `INTERNAL_TOKEN`;
  - one writer of D1 `owed_rows` in each phase, NEW-H1 aside.

## What a further fold must change (one line each)

- **NEW-H1:** bound every acknowledgement by the marks' generation read before the actor's apply (Phase B, the belt's
  Phase B find), and by the generation stored with the game's `applied` row on a Phase C skip (or drop the skip);
  refuse the D1-only levers while `OWED_ACTOR_READ` is on; pins `delta_probe_a`, `delta_probe_b`.
- **NEW-M1:** gate the actor read on the identity probe, with no wanted mark, or have Phase C's apply write D1's
  `owed_state`.
- **NEW-M2:** an index-only digest change keeps the rows; bootstrap pages paced with a fleet budget; the cutover rule
  for digest-moving deploys in Phase C.
- **NEW-M3:** the rollback in two renders (judge on with the actor read, the pass to its stamp, then the read flip),
  written into the cutover doc and enforced in the D1 read, or stated.
- **NEW-L1 to NEW-L6:** each one line, as above.

## What the Linux tier at the land must exercise

The lens's eight items, kept or amended, then four new ones:
1. **Kept.** The DO migration on a real deploy. Also the first touch of an existing actor after `schema.rs` moves:
   the rebuild, the `pending` read, the bootstrap to its end.
2. **Kept, now a regression check.** More than 16 games of one identity under one event in Phase C. The rows must
   equal the D1 judge's (they do natively).
3. **Kept.** A record fault on a fresh actor's second round leaves the rows unchanged, and `{t}_snap` / restore run
   inside a real Durable Object's SQLite (`CREATE TABLE ... AS`, `INSERT ... SELECT *` in column order).
4. **Amended.** The consumer's per-message service time on the fleet seed over D1, in Phase A (deferred) AND in
   Phase B (actor then D1, inline), against `owed-queue-backlog` (NEW-L5).
5. **Amended.** The bootstrap lever paged to its end on the fleet seed: its duration, its D1 rows read per second,
   the reads served while it runs, and a second pass answering `armed` 0 (NEW-L3).
6. **Kept, met natively.** A KV-copy body read by the client's parser; repeat it with a real KV read.
7. **Kept.** The direct call's visibility p95 under wasm32 and the platform hop, now with the copy's +20 ms
   (NEW-L4).
8. **Kept.** A Phase C belt tick and its hand-back against the counting courier stand-in.
9. **New.** NEW-H1's two shapes on a real DO and D1:
   - a downgrade written during a Phase B actor apply;
   - a Phase C delivery whose acknowledgement batch faults after the actor answered, then a downgrade, then the
     redelivery.

   In both, the read must withhold the press.
10. **New.** A Phase C read of an identity first seen after the flip: the D1 writes per read and the sweep's re-judges
    (NEW-M1).
11. **New.** A digest-moving deploy over a populated fleet: the D1 rows per second of the concurrent bootstraps and
    the time until the last press is served again (NEW-M2).
12. **New.** The rollback rehearsal on beta in the order NEW-M3 states, reading the D1 rows served before and after
    the `owed-v3` stamp.

## What the actor's beta window must read

- **The compare's counters.** `/health.owedActor.comparedTotal` must advance while `diffsTotal` and `faultsTotal` stay
  flat, from the window's first read to its last. Any DIFF is a STOP that names its identity and game; in Phase B, a
  DIFF is how NEW-H1 (a) would show. In Phase A, also read `owed_actor_deferred_total` against `comparedTotal`
  (NEW-L6).
- **The bootstrap's coverage.** Page the bootstrap lever until `next` is null, then run a second full pass answering
  `armed` 0 (NEW-L3). `bootstraps` against `activeIdentities` is a hint only.
- **The queue's service time.** `owed-queue-backlog` and `owedEvents.serviceMs` against the pre-actor window's 4.6 to
  7.1 s, and again in the first hour of Phase B (NEW-L5).
- **The bars.** `rowsVisibleMsP95`, `rowsVisibleDirectMsP95`, `collectAfterMinedMsP95` and
  `oldestUnappliedEventAgeMs` are the fleet's durable figures now. A null `collectAfterMinedMsP95` means no mined
  sample in the window, not a pass.
- **Era moves.** `epochResets` after any era bump, and the reads served pending until those actors' bootstraps end
  (NEW-M2, NEW-L2).
- **D1 cost.** D1 rows read per hour, before and after the deploy (LOW-1 stands).
