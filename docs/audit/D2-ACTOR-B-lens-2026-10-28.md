# D2-ACTOR-B-LENS: a cold review of overlay PR 200 (the owed actor per identity, bsv-low #912)

Written 2026-10-28T04:35Z by lane D2-ACTOR-B-LENS (Wave E, row E3). Reviewed: `lane/D2-ACTOR-B` at `8af15944`
(PR 200, rebased onto main `1aada952`). Read against bsv-low's `DESIGN-D2-ACTOR-2026-10-26.md`, its audit
`D2-ACTOR-AUDIT-2026-10-26.md`, the lane contracts `D2-ACTOR-B-2026-10-27.md` and `D2-ACTOR-B-CONT-2026-10-28.md`,
`CUTOVER-D2-ACTOR-2026-10-28.md` and `WIPE-REHEARSAL-3-2026-10-28.md` section 2. Docs only: no product code changed.

## VERDICT: NOT SOUND (4 HIGH, 9 MEDIUM, 7 LOW, 3 NOTE)

The shape is right: one actor per lowercased identity key, the pure judge over a mirror that the byte-identity pin
proves on the frozen fixtures, the belt as the only courier asker in Phase C, the flags in the render, and the DO
migration appended. Four defects break the actor's own guarantees, though, and two of them can serve a press the D1
judge would not serve:

1. the redelivery key skips the second chunk of one event;
2. a non-fixpoint judgement is persisted and served;
3. Phase B serves the actor's rows under D1's acknowledgements;
4. a schema digest change keeps the old judge tables.

Phase A can be deployed as rendered: the actor writes nothing anyone reads, and its fault never fails a message. But
HIGH-1 and MEDIUM-6 should be folded before the Phase A window, so that the window measures the real cost and does not
count false diffs. Phase B and Phase C must wait until every HIGH is folded.

### How this was verified

I ran the PR's 13 `d2_actor_b` pins natively at the tip in a scratch worktree, and all are green. The byte-identity pin
covers 10 identities and 1,715 rows. The fleet apply (debug build) read p50 1,033 ms and p95 2,628 ms.

I also ran three lens probes. They are scratch tests appended to `owed_actor/tests.rs` and not committed. Their source
and output are kept at `/Volumes/Lanes/out/wt-overlay-actor-b-lens/lens-probes.{diff,out}`. Each probe passes on
`8af15944`, and each pass proves its defect.

## HIGH

### HIGH-1: one event's second chunk is skipped as a redelivery, and Phase C acknowledges its marks unjudged

Where:
- `crates/low-app-layer/src/owed_actor.rs:751-761`: `applied` is keyed on `(kind, seq)` alone.
- `owed_judge.rs:313` (`D1Judge::actor_deliver`): sends the event's own kind and seq on every call.
- `owed_events.rs:623` and `:647`: the consumer chunks one identity's games 16 at a time, then retries one subject at a
  time after a chunk fault.
- `owed_actor.rs:1489-1516` (`as_the_judge`): acknowledges every chunk's marks after the actor's answer.

What fails: one message can route more than 16 games of one identity. This happens with `tip`/`gate`, `age`/`age`,
`backfill`, a wanted envelope (32 games), or an identity route. It also happens on any chunk fault, because the
consumer then retries the chunk as singles. Every apply after the first carries the same `(kind, seq)`, so the actor
returns `skipped_redelivery` with nothing applied. In Phase C, `as_the_judge` then deletes those games' marks: they are
never judged, `pending` goes false, and the stale rows stand until another event, the tip alarm, or the 64-a-day
reconcile reaches them. In Phase A, `beside` compares the actor's stale rows with the D1 judge's fresh ones and counts
false diffs.

Failing input (probe 1): the fleet whale `03dd..dd` (1,033 games). The first apply of `("pot", 7)` judged; the second,
for the other games under the same `("pot", 7)`, answered `skipped_redelivery = true`. The D1 judge stored 500 rows;
the actor stored 128.

Why no pin caught it: the byte-identity pin deletes `applied` between its chunks (`owed_actor/tests.rs:273`). The test
harness's `actor_deliver` passes no seq at all (`tests.rs` `WithActor`).

Fold: key the idempotency on `(kind, seq, game)`, writing one `applied` row per game applied, so a game is skipped only
if it was itself applied. Then a pin that runs the shipped `judge_plan` with 20 games of one identity under one event,
in Phase C.

### HIGH-2: a non-fixpoint round's judgement is written, served mid-apply, and kept on a fault

Where:
- `owed_actor.rs:786-830`: every closure round runs `judge_subjects`, which commits `owed_rows` to the actor's SQLite.
- `object.rs:34-36`: "a statement that faults ends the batch and its earlier statements stand".
- `object.rs:438` (`rows_route`): reads the rows without taking the turn.

What fails: a fresh actor, or any event that names a new key, needs at least two rounds. The rounds have awaits between
them (the D1 read, chaintracks). During that time, a `/rows` read serves round 1's judgement, which was made over a
partial mirror. If round 2 faults (a D1 fault, more than 8 rounds, more than 4,096 txids), round 1's judgement STAYS.
Phase C's retry heals it on a later attempt. Phase B's `beside` swallows the fault, so there the partial judgement stays
until the next event touches those games. CLAUDE.md's section claims "past either a fault, never a judgement over a
partial mirror"; the code does not do that.

Failing input (probe 3): every fixture identity with a record that faults on round 2. 10 of 10 kept a partial
judgement. 4 of them held claimable rows the D1 judge does not hold. For example, `d1_fleet 02000..00` had claimable
`d1..0258:0`, `d1..02d0:0`, `d1..0348:0`, `d1..03c0:0`, `d1..1c98:0` and `d1..1d10:0` only in the partial judgement.
This breaks "claimable only from a judged row" in substance: the row was judged over evidence the closure had not
finished copying.

Fold:
- Judge the non-fixpoint rounds without committing their rows: either a dry judge that returns the plan, or a scratch
  table swapped in at the fixpoint in one synchronous batch.
- Snapshot the games' rows before the apply and restore them on any `Err`.
- Have `/rows` and the KV copy read only committed fixpoint rows.
- Pin: a round-2 record fault leaves the rows exactly as before the apply.

### HIGH-3: Phase B serves the actor's rows, but the marks that withhold a press are acknowledged by the D1 judge

Where:
- `owed_events.rs:363-366`: in Phases A and B the D1 judge commits and acknowledges the marks, then `beside` applies the
  actor.
- `owed_actor.rs:1410-1419`: an actor fault is counted and dropped.
- `object.rs:742`: the read keeps D1's `pending`.
- `owed_belt.rs:997-1006`: in Phases A and B a belt find is judged in D1 and never delivered to the actor; its marks
  survive only until the 120 s sweep re-publishes them.

What fails: D2-L1's rule is "never serve a press over a pending downgrade". Under `OWED_ACTOR_READ=true` with
`OWED_D1_JUDGE=true`, a downgrade mark (a competitor's spend, an eviction) is gone the moment the D1 judge commits. Two
cases follow:
- Until the actor's apply lands (seconds, behind the turn), the read serves the actor's older claimable row with
  `pending` false.
- If the actor's apply faults, or is skipped (HIGH-1), the stale press is served indefinitely. Only the next event for
  that game, or the reconcile's sample (64 identities a day), heals it.

The design asked for `pending` to come from the actor's ack watermark against the newest routed `seq`. The PR says
plainly that it does not do this. That statement is the defect, not a mitigation.

Fold, either of:
- in Phase B, acknowledge the marks only after BOTH judges applied, and hand the message back on an actor fault (the
  same rule as Phase C); or
- serve `pending` and the downgrade withholding from the actor's own ack watermark: a game whose newest routed seq is
  above the actor's applied seq is withheld.

Also deliver Phase A/B belt finds to the actor, as Phase C does. Pin: a Phase B read between the D1 judge's commit and
a faulted actor apply serves no press for the downgraded game.

### HIGH-4: a schema digest change rebuilds the mirror but keeps the judge's own tables in their old shape

Where: `object.rs:89-102` (`schema_plan`). On a new `schema::DIGEST` it drops only `FAMILIES` and `txfacts`. `owed_rows`,
`owed_row_words`, `owed_state` and `owed_dirty`, and their indexes and triggers, are recreated with
`CREATE ... IF NOT EXISTS`, which does nothing for a table that already exists.

What fails: the generated schema follows the overlay's migration list. The pin goes red until `schema.rs` is written
again, and that part works. But the October fold lanes added columns to `owed_rows` five times (`probeTip`,
`gateHeight`, `beltDueAtMs`, `judgedAtMs` and the rest). At the first deploy after such a migration, every existing
actor keeps the old `owed_rows`, and every judge write or `ROWS_SQL` that names the new column faults. Phase C then has
no judge: every message is held, then dead-lettered. In Phase B the actor's rows freeze.

Simply dropping those tables would be no better. `meta.bootstrapped` stays set, so games without a fresh event would
lose their rows, and the read would serve a short list as complete (Rule 23).

Failing input (probe 2): an `owed_rows(identity, outpoint)` built under an older DDL, then `schema_plan(Some("older"))`.
The columns afterwards are still `["identity", "outpoint"]`.

Fold: on a digest change, drop and recreate the judge's tables too, clear `bootstrapped`, and re-arm the bootstrap. In
Phase B and C, serve `pending` until the bootstrap ends. Pin: an actor holding the previous digest's tables applies
under the new one and stores the D1 judge's rows.

## MEDIUM

### MEDIUM-1: the Phase B flip blanks every owed list, and Phase A never proves the bootstrap

Where: `object.rs:438-456` (the bootstrap starts only from `/rows`), `object.rs:752-756` (a not-bootstrapped actor
answers `pending` with no rows and no KV copy), and `object.rs:480-536` (16 games per alarm).

Phase A's read never calls the actor, and neither applies nor the reconcile's `/stored` set `bootstrapped`. So at the
flip to B, every identity reads `pending` with no rows until its bootstrap pages through. That is 38 sequential alarms
for a 600-game seat, each a closure of up to 8 D1 batches with the BEEF bytes. Every active identity starts its
bootstrap at once, against the one D1, which is the contention the actor was meant to end.

Fold:
- Bootstrap in Phase A: on the first apply, or by a lever paged by the identity list.
- Gate Phase B's entry on `bootstraps` covering the active identities.
- While bootstrapping, serve the KV copy, or D1's rows display-only.

### MEDIUM-2: the fallback bodies are not the owed wire shape, so the client rejects all of them

Where: `owed_actor.rs:1043-1065` (`display_from_copy`), `object.rs:752-756`, `routes.rs:3299`.

The KV copy holds `ROWS_SQL` rows: `facts` as JSON text, `claimable` at the top level. It is served with no `v`, and
the no-copy and not-bootstrapped answers carry `rows: null`. bsv-low's `app/src/lib/owed.ts:85-87` returns `null` for
any body whose `v` is not the wire version, or whose `rows` is not an array. So:
- MEDIUM-9's copy is never displayed;
- `pending: true` is never read as pending;
- the only thing that keeps a copy row from pressing is the parse failure. If such a row were ever parsed,
  `facts.claimable` would be absent, which the client reads as claimable; it is the claim's absence that would save it.

Fold: build all three answers through `owed_read::Snapshot::body` (the wire `v`, facts as an object), with each copy
row's `facts.claimable` set false and `facts.claimReason` set. For the pending answers, use the wire's own pending form
(`pending: true` with an empty array is what the client reads as "re-read"). Pin: the client's parser, or a Rust
mirror of it, accepts each body.

### MEDIUM-3: a bare read mints a durable actor for any key; LOW-6's bound does not hold for reads

Where: `object.rs:438-456`, `:237-264` (every fetch runs the mirror's DDL, about 27 tables with their indexes), and
`read_snapshot` (`object.rs:737`), which calls the actor without asking D1 whether it knows the identity.

What fails: under `OWED_ACTOR_READ=true`, an authenticated `GET /owed` for a fresh key mints a SQLite object. It
writes the schema and `meta`, arms an alarm, and runs one bootstrap read of D1. `AUTH_ENFORCE=true` limits a caller to
its own key, but a key costs one BRC-103 handshake: no door cap and no chain fee. Such actors are in no wipe list
(`RESET_IDENTITIES_SQL` reads D1) and are never reconciled. One stranger read costs one DO request, about 60 rows
written, one alarm, and one D1 statement, held forever.

Fold: in `read_snapshot`, answer from D1's snapshot (an unknown identity's empty-and-pending body) when D1 holds no
`owed_state` or party row for the key. Do not call the actor in that case. Also make `rows_route` write nothing for an
identity it has never applied.

### MEDIUM-4: the tip alarm re-judges every open-gate game every ten minutes, for ever

Where: `object.rs:563` (`GATED_GAMES_SQL`: `gateHeight<=?1` with no lower bound) and `:209-231`.

A refund whose gate has opened keeps its `gateHeight`. So every actor holding one re-applies up to 64 games every
`TIP_ALARM_MS` (600 s), each a D1 closure with its BEEF bytes, as long as the row exists. The D1 rule it replaces marks
only `gateHeight BETWEEN tip-6 AND tip` (`owed_judge/writers.rs:7`).

Fold: keep the last tip judged in `meta` and select `gateHeight` in (last tip, tip], plus the courier-dated sweeps with
`probeTip < tip`. Arm the next alarm at the next gate, not every 10 minutes.

### MEDIUM-5: the wipe's order lets an actor re-hydrate from the old era and serve it afterwards

Where: `object.rs:183-189` and `WIPE-REHEARSAL-3` step 4a (the reset BEFORE part 2's A statements).

Between the reset and the wipe of D1, the queue consumer, the sweep, the belt's Phase C hand-back, a Phase B read or
the actor's own re-armed alarm can re-mint or re-apply the actor from the pre-wipe D1. After the wipe, that actor holds
old-era rows with `bootstrapped` set. Events from the wiped D1 refresh only the games they name, and the identity is in
neither the next identity list nor the reconcile's walk. The KV copy has the same residue: the doc deletes it by hand.

Fold: store D1's storage epoch (`STORAGE_EPOCH`, or a `view_derivation_state` stamp the wipe moves) in `meta` at the
bootstrap. Compare it on every apply and read, and reset on a mismatch. Or reset again after the wipe, from the list
read before it.

### MEDIUM-6: Phase A puts the actor's whole apply into the serial consumer, unmeasured on D1

Where: `owed_events.rs:363-366` (`beside`, awaited inside `deliver`), `owed_actor.rs:1366` (`CONSUMER_CALL_WALL_MS`,
20 s), `:1421-1430` (a whole-identity `ROWS_SQL` of D1 per delivery), and `:282-284` with `:547` (every apply re-reads
and re-decodes every BEEF of its games from D1; the `txfacts` events are stored but `TXFACTS_STORED_SQL` prefers the
copy's own `stored` facts).

What fails: the consumer runs at `max_concurrency = 1`, and the pre-actor window of record 1462 already read 4.6 to
7.1 s per message. Each delivery in Phase A adds:
- the actor's 24-statement D1 batch, with BEEF hex;
- the in-actor judge: a release p95 of 302 ms natively on the whale, before any D1 hop;
- a read of up to 500 rows.

`owed-queue-backlog` (500 and falling) can go red from the compare alone, and nothing measured it on D1.

Fold: run the compare detached (`wait_until`), or on a sample, so the consumer's critical path is the D1 judge alone.
Prefer the `event` txfacts over a re-decode. The Linux tier measures a consumer message with and without the actor.

### MEDIUM-7: the served bars are one isolate's samples, and bar (2) has no direct call for overlay writes

Where: `owed_actor.rs:1133-1142` (`thread_local BARS`) and `routes.rs:7024-7045`.

Problems with what is served:
- `/health` reports the isolate it lands on. `collectAfterMinedMsP95` is fed only in queue-consumer isolates, so a
  fetch isolate serves null, which the harness reads as NOT-PROVEN.
- `rowsVisibleMsP95` mixes the record door's fast direct calls with the queue path.
- In Phases A and B, `oldestUnappliedEventAgeMs` falls back to D1's oldest un-acked mark. That is the D1 judge's
  backlog, so a wedged actor reads green.
- The design's bar (2), an INDEX latch visible within 1 s, rests on the writer's direct call. The overlay's writers make
  none; they reach the actor only through the serial queue.

Fold:
- Write the bars durably, per class (direct, queue, mined), the way lane A's consumer row does, and serve the fleet's
  figures.
- Serve the actor's own oldest unapplied age.
- Either add a direct call from the overlay (a service binding to an app-layer lever, or a cross-script DO binding), or
  restate bar (2) as the queue's figure.

### MEDIUM-8: the cutover's gate reads the sampled reconcile, not the every-judge compare

Where: `routes.rs:7052-7056`, `owed_actor.rs:1433`, and `CUTOVER` section 3's entry condition.

At 64 identities a day, a one-hour Phase A window reconciles about 2.7 identities, so "`reconcileDiffs` flat" is nearly
vacuous. The per-delivery compare is the design's "compared on every judge". It is not on `/health.owedActor`.

Fold: serve `comparedTotal`, `diffsTotal` and `faultsTotal` (the durable counters) on `/health.owedActor`. Add them to
the cutover's Phase B entry, as zero diffs and zero faults over the window. This compare would have caught HIGH-1 on
beta.

### MEDIUM-9: Phase C's rollback has no lever

Where: `CUTOVER` section 4 (the rollback needs the `owed-v3` backfill).

No `owed-v3` exists in the tree. A rollback from C without it serves D1's `owed_rows` as they stood at the flip, with
the actor having acknowledged every mark since, so nothing withholds those stale presses.

Fold: an `owed-v3` stamp and pass that re-judges every identity with the D1 judge from D1's own evidence. Do not copy
the actor's rows: the evidence is the record. Phase C waits for it, together with a rehearsal of the rollback on beta.

## LOW

- **LOW-1 (LOW-7 not built).** The cost bar has no actor or queue families: DO requests and duration, SQLite rows
  read and written, KV writes (one a second per active identity), and Phase A's doubled D1 reads. LOW as the design
  classed it, but MEDIUM-6's cost is not visible anywhere until it is built.
- **LOW-2.** Nothing in the per-identity SQLite is pruned. `applied` grows by one row per event for ever
  (`owed_actor.rs:549-552`), `txfacts` `event` rows are never deleted (`:545`), and mirror rows of txids no longer
  named stay. Bound: a per-kind seq watermark in place of `applied`, and a sweep of `txfacts` and family rows outside
  the last closure.
- **LOW-3.** In Phase C, the judge's `PROOF_LATCH_MOVED_DELETE_SQL` lands only in the mirror. D1's moved latch is never
  deleted, so R2-HIGH-1 (a)'s durable refusal is lost: the latch is re-copied and its root re-checked on every apply.
  Fold: hand the delete back to D1 with the apply, as the belt's find statements are.
- **LOW-4.** D1 judge callers that do not read the flag remain in Phase C: the backfill pass and its lever
  (`owed_backfill.rs:387`), and the no-binding fallbacks. They are harmless while the stamp stands; state it, and gate
  the lever on `OWED_D1_JUDGE`.
- **LOW-5.** The Phase B read runs D1's whole `owed_read::read` (2,000 rows read on the fleet identity) and then throws
  its rows away (`object.rs:742`). It needs only `PENDING_SQL`.
- **LOW-6.** The bootstrap pages with `LIMIT 16 OFFSET n` over a union that moves (`object.rs:485-529`). A game that
  leaves the union between two pages shifts the pages and skips a game. Fold: a keyset cursor on `(at, gameId)`.
- **LOW-7.** `apply` silently truncates games past 64 (`owed_actor.rs:748`). `schedule_direct` groups a batch's
  subjects with no bound (`object.rs:698-704`). Fold: split at the caller, or return the rest.

## NOTE

- **NOTE-1.** The section is a mutex held across the D1 read and chaintracks, not "outside reads first, then a
  synchronous section". This is equivalent for ordering: the slow-chaintracks pin goes red with the turn removed. But
  it serialises one identity's applies behind its slowest outside read, and the reads (`/rows`, the KV copy) bypass it
  (HIGH-2).
- **NOTE-2.** The byte-identity pin covers a FRESH mirror per identity. No pin runs a long-lived mirror whose other
  games' records moved; the fixpoint argument covers it by reasoning only.
- **NOTE-3.** Checked and sound:
  - the names (`OwedActor`, `OWED_ACTOR`, `v4-owed-actor` appended as `new_sqlite_classes`, `OWED_COPY` placeholders
    in both renders);
  - no D1 migration (the count stays 473), and the queue names are unchanged;
  - the levers sit behind `INTERNAL_TOKEN`, and the reset is bounded at 50 a call, with `delete_alarm` after
    `deleteAll` (NOTE-C);
  - LOW-1's lowercased key;
  - one writer of D1 `owed_rows` at each phase (LOW-4 aside);
  - in Phase C the belt is the only courier asker, and the actor makes no courier call;
  - no tune given as a fix: the 20 s and 2 s walls are new bounds, not widened ones.

## What a fold must change (one line each)

- HIGH-1: idempotency per `(kind, seq, game)`; a pin through `judge_plan` with more than 16 games of one identity.
- HIGH-2: commit only the fixpoint round's rows; restore the games' rows on `Err`; `/rows` reads committed rows only.
- HIGH-3: Phase B acknowledges after both judges (an actor fault hands the message back), or `pending` comes from the
  actor's watermark; belt finds reach the actor in Phases A and B.
- HIGH-4: a digest change rebuilds the judge's tables and clears `bootstrapped`.
- MEDIUM-1: bootstrap in Phase A, and gate Phase B on it.
- MEDIUM-2: fallback bodies through `Snapshot::body`, with `v` and `facts.claimable: false`.
- MEDIUM-3: no actor call for an identity D1 does not know.
- MEDIUM-4: the tip alarm selects (last tip, tip] and arms at the next gate.
- MEDIUM-5: an epoch stamp in `meta`, reset on a mismatch.
- MEDIUM-6: the compare off the consumer's critical path; prefer the `event` facts.
- MEDIUM-7: durable per-class bars, the actor's own oldest age, and a direct call or a restated bar (2).
- MEDIUM-8: serve the compare's durable counters and gate Phase B on them.
- MEDIUM-9: an `owed-v3` re-judge lever before Phase C.

## What the Linux tier at the land must exercise

1. The DO migration on a real deploy, and the first touch of a pre-existing actor after `schema.rs` changes (HIGH-4).
2. One consumer message with more than 16 games of one identity, in Phase C (HIGH-1). The rows must equal the D1
   judge's.
3. A record fault injected on a fresh actor's second round; the rows must be unchanged (HIGH-2).
4. The consumer's per-message service time with and without `beside` on the fleet seed (MEDIUM-6), against
   `owed-queue-backlog`.
5. The Phase B flip on the fleet seed: the bootstrap's duration, its D1 rows read, and the read's answers while it runs
   (MEDIUM-1, MEDIUM-2).
6. A KV-copy body read by the client's parser (MEDIUM-2).
7. The direct call's visibility p95 under wasm32 and the platform hop. The in-process release p95 is already 302 ms, so
   the cutover's 300 ms apply bar is at risk before any D1 round trip.
8. A Phase C belt tick, with its hand-back, against the counting courier stand-in.

## What the actor's beta window must read

- `owed_actor_diffs_total`, `owed_actor_faults_total` and `owed_actor_compared_total` on `/health/invariants`, from the
  window's first read to its last. Any diff or fault is a STOP that names its identity (MEDIUM-8).
- `owed-queue-backlog` and `owedEvents.serviceMs` against the pre-actor window's 4.6 to 7.1 s (MEDIUM-6).
- `owedActor.bootstraps` against the active identities before any Phase B flip (MEDIUM-1).
- The `/health.owedActor` bars from more than one isolate: a null `collectAfterMinedMsP95` is the isolate sampling
  (MEDIUM-7), not a pass.
- D1 rows read per hour, before and after the deploy (LOW-1, until the cost bar carries the actor).
