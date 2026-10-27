# D2-ACTOR-A-LENS: the lens on PR 184 (the `owed-events` queue in both Workers; bsv-low #911, Wave E row E2)

Lane D2-ACTOR-A-LENS, written 2026-10-27T19:50Z, a COLD from-scratch review of overlay PR 184 at the branch `lane/D2-ACTOR-A`'s tip
`59d98ec677e85e58fc7cf383c4ab7fc90dfdb34b` (six commits over main `c51e161d`; a clean three-way merge onto main
`eaaee0ec`, no conflict, the beta storage line of `eaaee0ec` kept). Read in the brief's order: bsv-low's
`DESIGN-D2-ACTOR-2026-10-26.md` (the second cut, sections 3, 4, 6, 7, 11, 12), `docs/audit/D2-ACTOR-AUDIT-2026-10-26.md`
(HIGH-1, HIGH-2, MEDIUM-1, MEDIUM-7, MEDIUM-8, LOW-3, LOW-5), the lane brief `docs/lanes/D2-ACTOR-A-2026-10-27.md`, the
ledger's records 1434 and 1436 (the port exhaustion and the lane's REPORT), then the PR whole: the three configs, the
event (`overlay-discovery/src/owed_event.rs`, `vectors/owed_event_v1.json`), the publish (`overlay-cloudflare/src/owed_events.rs`,
compiled into the app layer by `#[path]`), the 29 triggers (`derived_owed_triggers.rs`), every note and publish call site in
both Workers, `pot_changes.rs`'s three flush functions, `tracker_events.rs`, the record door (`record_post.rs`,
`record_post/owed_write.rs`, `record_post/latency.rs`, `handfile.rs`), the consumer (`low-app-layer/src/owed_events.rs`),
the judge (`owed_judge.rs`, `owed_judge/writers.rs`), the sweep (`owed_belt.rs`), the hooks (`routes.rs`,
`internal_events.rs`, `tracker_host/runtime.rs`), the dead letters' lever (`dead_letters.rs`), the migrations (`d1/mod.rs`),
the cost bar (`d1_ledger.rs`, `cost_bar.rs`), the ownership manifest, the tier's record-door cell, both pin files, the
migration pin; workers-rs 0.8.7's `delay.rs` and `queue.rs`; Miniflare 5.20261006's queue and D1 plugins (the ones the
tree's `wrangler` 4.148 runs). Every file and line below is the PR's tip.

What was run, in a scratch checkout of the tip under `/Volumes/Lanes/tmp` with its own target dir, in the foreground:
`cargo test --manifest-path crates/overlay-cloudflare/Cargo.toml --lib owed_events` (6 passed) and
`cargo test --manifest-path crates/low-app-layer/Cargo.toml --test d2_actor_a` (7 passed), then ONE mutant (the `seen`
writer's note removed at `ops.rs:1158`): the six overlay pins stay green (MEDIUM-3 below). The PR's CI is green on every
job (`test` 31 min). No server was started, no tier was run on the Studio, no classifier stopped a turn, no product code
was changed.

## Verdict: SOUND (0 HIGH, 3 MEDIUM, 9 LOW, 7 NOTE)

The shape is the design's second cut as folded: the event is keyed on the source row and routed by the consumer through
the marks the trigger wrote (HIGH-1); `seq` is one counter row bumped in the batch that reads the facts, so a larger `seq`
carries facts at least as new per row (MEDIUM-1; pinned by `d2_actor_a_d`); every overlay publish is inside the writer's
invocation before its answer, the request's under a 1.5 s wall, the three flush functions first in their task
(HIGH-2); the un-acked sweep re-publishes within one tick (HIGH-2's second belt); the record door and the handfile batch
publish and answer with no judge inside the request (section 11); the felt's announce is untouched (MEDIUM-7); the ages
are served (MEDIUM-8); the DLQ has its consumer and the lever (LOW-3); `max_batch_size = 1`, `max_batch_timeout = 0`
(LOW-5). Nothing here moves money: the judge is the same pure derivation over the same D1 tables, the read is untouched,
the press's money proof is the wallet's landing word as before. The three MEDIUMs are each foldable in a line at the
land; none is a loss of an event. What the design did not give lane A, and this PR therefore lacks, is a serialisation
of the judge per identity: that is MEDIUM-1, and its cheap lane-A answer is a config line until lane B's actor.

## Findings

### MEDIUM-1: two consumers judging one identity can leave a stale row with its marks acknowledged

**Where.** `crates/low-app-layer/wrangler.toml:261-266` (the `low-owed-events` consumer: no `max_concurrency`; the
platform's default scales to 250 by backlog); `crates/low-app-layer/src/owed_judge.rs:433-446` (`MARKS_SQL` read, `seen`
= the max generation, then the hydrate, each a separate D1 statement) and `:498-543` (`GAME_DELETE_SQL`, the rows,
`ACK_DIRTY_SQL` bounded by `seen`, one batch); `crates/low-app-layer/src/owed_events.rs:173-187` (`route` calls
`deliver` per identity with no lock).

**The failing input.** A seat's JOIN is admitted (W1, `pot` gen 10) and its event E1 is delivered to consumer A, which
reads `seen = 10` and hydrates. The JOIN's SEEN latch or the settle's `spentConfirmed` lands (W2, gen 11, the downgrade
class) and E2 is delivered to consumer B while A is still hydrating. B reads 10 and 11, hydrates after W2, writes the hop
`spent` with no press, acks both marks. A then runs its batch: `GAME_DELETE_SQL` removes B's rows and A's rows derived
before W2 go in, with the sweep press offered again; A's ack deletes nothing. No mark is pending, so the read's
pending-downgrade rule withholds nothing: the dead press stands until the identity's next event or the age repair
(24 h). The last event of a hand (the settle's MINED latch) has no follower. The class existed on main between a hook's
inline judge and the drain; the PR makes it the common case, since every event is a judge and a hand's end publishes
four or five events per seat inside a second.

**The fold (one line each).** For lane A: `max_concurrency = 1` on the `low-owed-events` consumer in both app-layer
renders (one judge at a time fleet-wide; at L50's 2,000 events an hour and a judge under a second that is under 60 percent
of one consumer; the tier measures it), with the pin: two events for one identity under a `Db` whose hydrate of the first
yields until the second committed derive the second's rows. For lane B: the actor's per-identity serial apply is the
end state (the design's MEDIUM-2); until then the serial consumer is the only serialiser. Not chosen: an optimistic guard
on `owed_state.recomputeSeq` inside the batch (D1 cannot abort a batch on a predicate; it needs every row statement
conditioned, a larger change than the config line).

### MEDIUM-2: a pair whose judge faults for good is a 120 s loop with a growing park and no backoff

**Where.** `crates/low-app-layer/src/owed_events.rs:448-457` (a judge `Err` hands the message back; `max_retries = 3`
with no `retry_delay` at `wrangler.toml:265`, so three immediate retries), `:229-259` (the sweep re-publishes any pair
whose marks waited one tick, with no memory of why they waited), `:479-542` (the DLQ park keyed by `messageId`),
`crates/overlay-cloudflare/src/owed_events.rs:37` (`DEAD_LETTERS_MAX` 2,000).

**The failing input.** One subject whose hydrate or row write errs deterministically (a stored row that does not parse,
a statement a later schema refuses). Its event faults four deliveries in a few seconds and is parked; its marks stay;
120 s later the sweep re-publishes the pair under a new message, which faults four times and parks under a new
`messageId`. Per such pair: 2,880 judge attempts and 720 parked rows a day; one pair fills the 2,000-row park in under
three days, after which every DLQ letter of every pair is refused and lost after its hundred 60 s retries (LOW-5);
`owed_events_judge_faults_total` climbs with nothing to read the cause from. The old drain re-tried a faulting subject
each pass too, but it parked nothing and cost one attempt a tick.

**The fold.** A per-pair fault memo on the #825 conflict memo's shape: the consumer records (pair, consecutive faults,
last fault) and the sweep skips a pair past three faults for a doubling period (120 s to 6 h), served on
`/health.owedEvents` with the pairs held; and the DLQ park keyed by (source, sourceKey) carrying `faults` and `redrives`
rather than one row per message. Both are bounds, not tunes.

### MEDIUM-3: the write contract's coverage rests on reading; one door path is outside it and the pin does not see a writer that stops noting

**The enumeration.** The 29 triggers (`derived_owed_triggers.rs`) write 15 (table, source) pairs
(`owed_event.rs:29-45`). Every production writer of those tables was matched to a note: `pot_records` through
`pot_changes::note` (`pot_changes.rs:27-29`, every pot writer's existing note, the pin
`every_detached_pot_writing_task_ships_its_own_notes` unchanged); the overlay's stores of party, hop, refund, result,
collected and the BEEF latches (`d1_discovery.rs:3445-6703`, the production half of that file; its writes after `:7367`
are tests); the eviction and the readmission (`admit_fast.rs:1153,1366,1542,1610`, `eviction_write.rs:119,146`); the
SEEN latch (`ops.rs:1158`); the refusal upsert (`submit_refusals.rs:234`); the app layer's record door and handfile
(`record_post/owed_write.rs:33-56`, `handfile.rs:995-1006`), the word write (`tracker_host/runtime.rs:712-717`), the
tip's marks (`internal_events.rs:680-690`), the hooks (`routes.rs:3050-3055`, `:3248-3255`). Four writers mark with no
note:

- (a) **the #798 chain latch**: `record_post.rs:1600-1613` latches the poster's own CHAIN rows `refundValid = 1` by their
  outpoints (the `refund` trigger marks the chain row's `txid`), and the door then notes the FILING's txid
  (`record_post.rs:2373`, `owed_write.rs:36`), whose row was never written: zero marks, skipped. The chain row's mark waits
  for the sweep (120 s and a consumer) where PR 182's detached judge judged it inside the request's own `wait_until`.
  A refund press on that path arrives two minutes later than on main. Fold: `potrefund_chain_noop` answers the latched
  `(txid, vout)` and the door notes each as `refund`.
- (b) the backfill's evidence writes (`owed_backfill.rs:153,166`, a scheduled pass): the sweep covers them; stated.
- (c) the refusal ledger's 48 h GC delete (`submit_refusals.rs:246-248`): outside the judge's 24 h window, harmless; the
  sweep acks it.
- (d) the eviction's missing-evidence arm (`eviction_write.rs:128-131`): its subjects are the evicted twin's seats and
  the hop's marker identities, which the `eviction` event's own subjects name (`derived_owed_triggers.rs:150-155`), and the
  judge's ack is per subject whatever the source (`owed_judge.rs:184`): covered, stated.

**The pin.** `owed_events_tests.rs:194-250` (`d2_actor_a_a_every_trigger_writer_publishes_one_event_with_its_rows_facts`)
writes each table directly under SQLite and publishes a hand-written note: it proves the trigger-to-pair spelling and
the publish's mechanics, not that a production writer notes. Run with the `seen` writer's note removed (`ops.rs:1158`),
all six overlay pins pass. Its doc comment and the PR body claim more than it holds. Fold: keep the pin under a truthful
name and add a source pin over the writers' call sites (the shape of `every_detached_pot_writing_task_ships_its_own_notes`:
each table's production writer function contains its note), or accept this enumeration as the record.

### LOW-1: an app layer deployed before overlay migration 473 has no judge path at all

`crates/low-app-layer/src/owed_events.rs:321-323` (`configured()` is the binding alone), `owed_belt.rs:955-960` (the drain
takes the sweep arm when configured and returns its fault), the publish batch's `owed_event_seq` and
`INDEXED BY idx_released_missing_hop` (`overlay-cloudflare/src/owed_events.rs:45,63`). With the binding bound and the
overlay still at 469, every publish faults (no such table, no such index), the sweep faults, the inline judge is skipped:
filings land with no judge until the overlay deploys. The window's order is stated (the overlay first, its certified
count 473 on `/health`), the FOLD-7 precedent accepted the same class. Fold: gate `configured()` on the certified count
at or past 473, read once per isolate, as the backfill gates on `OWED_TRIGGERS_MIGRATION_COUNT`.

### LOW-2: `queueOps` is counted and written, never served

`d1_ledger.rs:481-487` (the upsert's eighth bind) against `cost_bar.rs:33` (`COST_DAILY_READ_SQL` selects seven columns).
The cost bar's family the design asked for (LOW-7) is in the row and absent from `/health/invariants.costBar`. Fold:
select and serve it beside `statements`.

### LOW-3: the DLQ park and its handler have no pin, and no tier reaches them

`low-app-layer/src/owed_events.rs:54` (`DEAD_LETTER_PARK_SQL`, an `INSERT ... SELECT ... WHERE ... ON CONFLICT ... RETURNING`
run only when a message dead-letters) and `:479-542`: no test file names the statement; the record-door cell binds the DLQ
and never fills it. Fold: a pin running the park, the held read and the full-table refusal over real SQLite (#576's
shape).

### LOW-4: the re-drive is unbounded and a re-parked letter is a new row

`overlay-cloudflare/src/owed_events.rs:508-558`: a re-driven letter that faults again parks under a fresh `messageId`;
the `redrives` column (`:35`) is never written; no `MAX_REDRIVES`, no exhausted state (#576 has both). Operator-driven,
cost only. Fold: key the park by pair (MEDIUM-2's fold) and stop at three.

### LOW-5: the DLQ park's retry window is about 100 minutes, not #576's 48 hours

`low-app-layer/src/owed_events.rs:522-536`: a park that faults is handed back with a 60 s delay, which overrides the
consumer's `retry_delay = 300`, for `max_retries = 100`. A D1 outage past 100 minutes loses the letter; its marks stay
and the sweep re-publishes. State it in the register sentence, or take #576's doubling backoff (a shape, not a tune).

### LOW-6: every message pays a CHAINTRACKS read and a schema ensure before it knows it has a subject; identity and `txfacts` events are sent with no marks check

`low-app-layer/src/owed_events.rs:399-403` (the tip read and `ensure_latch_columns` precede `route`; the ensure is once
per isolate, the tip read is not memoised: `routes.rs:629-650`), `overlay-cloudflare/src/owed_events.rs:273-275,332-334`
(a note with no facts read is sent whatever its marks), `routes.rs:3248-3255` (the hop hook publishes one `identity`
event per identity). A hook over an identity with nothing pending costs a message, a consumer invocation, a tracker call
and two D1 writes (the age row and the counters). Fold: route first and read the tip only when a subject is judged; the
read's 30 s isolate memo for the tip.

### LOW-7: a publish dropped at the 1.5 s answer wall is not counted

`overlay-cloudflare/src/owed_events.rs:478-494`: the `Right` arm logs and returns the frames; `COUNTER_FAULTS` is bumped
only inside `publish_env`, which the drop never reaches. `publishFaults` on both health reads undercounts exactly the
HIGH-2 case the wall exists for. Fold: count the drop under its own name.

### LOW-8: a note made at plan time can be published by another invocation before its batch lands

`eviction_write.rs:119,146` and `submit_refusals.rs:234` note while building their queries; the note set is one
thread-local per isolate (`overlay-cloudflare/src/owed_events.rs:138-149`), drained whole by any request's exit
(`lib.rs:647`). A concurrent request's publish between the plan and the commit reads zero marks and skips; the mark then
waits for the sweep. Latency only, a narrow window. Fold: note after the batch answered, as `pot_changes::note`'s callers do.

### LOW-9: with no queue binding the note set is never drained

`overlay-cloudflare/src/owed_events.rs:430-433`: no port, the frames are answered back and the notes stay. All three
shipped configs bind the producer; a dev or test config without it grows the set by distinct keys, `txfacts` values
included, for the isolate's life. Fold: drain and drop the notes when there is no port (the marks are the record).

## Notes (stated, nothing to fold at the land)

- **NOTE-1, the queue replay's publish.** The S2 consumer publishes in its detached flush after its acks
  (`overlay-cloudflare/src/lib.rs:2185`, `pot_changes::flush` inside `ctx.wait_until`), not before the ack as section
  3 words it for a replay; the 30 s window, then the sweep. The design's loss bound holds; the wording does not.
- **NOTE-2, `txfacts` has no belt.** No trigger marks it (`owed_event.rs:59-61`); a publish dropped at the wall loses it;
  lane A's consumer acks it untouched (`low-app-layer/src/owed_events.rs:95-97`). Lane B's actor must take its facts from
  the bytes at bootstrap and reconcile, never from the stream alone.
- **NOTE-3, the age repair is now always judged.** Its 128 marks per scheduled pass (`owed_belt.rs:948-951`,
  `owed_backfill.rs:50,252`) are swept within a tick and judged by the consumer, about 12,300 subjects a day, about 2.3 M
  rows read at the judge's measured 190 rows a subject, where before they filled the drain's idle room. Inside prod's
  20 M budget; the Linux tier's belt cells should show it.
- **NOTE-4, item 4: the publish opens no socket.** In Miniflare 5.20261006 (the tree's `wrangler` 4.148) the producer
  binding is a service binding to `queues:queue:<id>` whose broker is a Durable Object inside workerd
  (`broker.worker.js`), and the D1 binding a service to `database.worker.js`, also inside workerd: a `sendBatch` and a
  batch are in-process service calls, no TCP. In production the Queue binding is the runtime's RPC and the fallback POST
  the runtime's `fetch`; an isolate owns no socket to leak. What the overlay's dev session added under the tier: one more
  D1 batch and one counters batch per request, and messages accumulating in the broker's object with no consumer bound
  in that session (memory and disk inside workerd, never a socket). Record 1434's 15,146 CLOSE_WAIT and FIN_WAIT_2 pairs
  sit between wrangler's node proxy and workerd on the INBOUND side, the known class of a cell polling through the proxy
  without closing; the publish does not touch that side. The lane's doubt is answered: the production shape cannot leak
  this way, and the local publish did not either.
- **NOTE-5, the answer wall's timer.** `publish_before_answer` races a `worker::Delay` (`overlay-cloudflare/src/owed_events.rs:481-485`);
  `Delay` has no `Drop` (workers-rs 0.8.7 `delay.rs:26-80`), so a publish that wins leaves its 1.5 s timer to fire into a
  dropped closure: the tree's existing pattern (17 other uses). The empty fast path never polls the wall (`select` polls
  the publish first), so a request with nothing noted sets no timer.
- **NOTE-6, the tier's polls.** `tools/lane-908/record_door.mjs:96,100` widen the cell's waits to 40 s and 60 s because
  the consumer runs the seven 10 s fake judges one at a time (`max_batch_size = 1`); a harness bound, not a product wall.
  No product wall is widened by the PR: the 1.5 s answer wall is a new bound on the request, the DLQ's 60 s a shape.
- **NOTE-7, item 3, the word frames and #934.** The POST stays only as the fallback (`pot_changes.rs:257-258,350-352,411-412`,
  `lib.rs:647-649`: a frame the queue did not take is requeued for the flush's POST; a frame both sent and posted is
  ingested twice, idempotently). The consumer ingests the frame through the tracker's own write (`runtime::event` to
  `write_word`, `tracker_host/runtime.rs:712-717`), forgets its own note and routes it. #934's 100 s hang cannot recur in a
  request: the request's whole cost is the publish under its wall. In the consumer the judge has no wall
  (`consume_one`, `route`, `judge_subjects`; the platform's queue handler bound is 15 minutes): a judge that waits on D1
  waits and occupies a consumer, and the platform scales more consumers against the same D1 (MEDIUM-1's `max_concurrency`
  bounds that); a judge that errs is retried three times at once, then parked, its marks swept (MEDIUM-2).

## The checklist, item by item

1. **The write contract.** HIGH-1: the event is the source row (`owed_event.rs:73-89`), the consumer routes by
   `ROUTE_MARKS_SQL` on `idx_owed_dirty_source_key` (`low-app-layer/src/owed_events.rs:26`) and acks in the judge's own
   batch after the rows (`owed_judge.rs:538`, pinned by `d2_actor_a_c`: the ack is in the batch that writes `owed_rows`,
   none before it). HIGH-2: the request publishes before its answer (`lib.rs:647-649`), the three flush functions first in
   their task (`pot_changes.rs:257,350,411`), every detached `wait_until` writer rides `flush_inline_within`'s `Ship` arm;
   its `Park` arm (past the wall) parks the frames and notes nothing, the marks wait for the sweep. MEDIUM-1: `SEQ_BUMP_SQL`
   (`owed_events.rs:45`) is the first statement of the one batch, `RETURNING n`, never a rowid (`d2_actor_a_d`). A cancelled
   or refused publish leaves the mark (`d2_actor_a_g`, both crates) and the sweep ships it within one tick
   (`SWEEP_AFTER_MS` = 120 s, `d2_actor_a_e`). The coverage gaps are MEDIUM-3.
2. **The record door.** `record_post.rs:2367-2377` and `handfile.rs:993-1013`: the publish after `file_verified`, before the
   answer, bounded by the 1.5 s wall; PR 182's `detached::schedule` only without the binding; `Server-Timing:
   write;dur=<request less enqueue>, enqueue;dur=<publish>` (`latency.rs:79-88`; `write` therefore includes the
   verification); `record.enqueueMsP50/95` served beside the answer quantiles (`record_post.rs:1877-1882`). The lane's
   figures (seven doors 10 to 111 ms with the consumer's judge held 10 s, enqueue 2 to 15 ms) were measured in raw
   Miniflare on the Studio; the Linux tier re-measures them against the moved ceilings.
3. **The word frames.** NOTE-7.
4. **The port exhaustion.** NOTE-4.
5. **Migrations and pins.** Four appended at 470 to 473 (`d1/mod.rs:2460-2465`): two CREATEs, one index, one additive
   ALTER whose re-run is benign (`d1/mod.rs:300-305`); no DROP, no RENAME; the 469 before them untouched (fingerprint
   889,593,182 over the prefix, 1,223,941,910 over the list, count 473: `tests/owed_derived_sqlite.rs:72-81`). The
   ownership manifest names `owed_event_seq` (the overlay's, transient) and `owed_event_dead_letters` (the app layer's, the
   overlay creating and deleting), and adds the overlay as a reader of `owed_belt_state`, `hopsweep_records` and
   `tx_words`. bsv-low at the window: its migration pin to 473 and 1,223,941,910 in its own commit; the harness's
   invariants rows for `/health.owedEvents` and `/health/invariants.owedEvents` (`publishedTotal`, `consumedTotal`,
   `ackedMarks`, `unackedMarks`, `oldestUnackedMarkAgeMs`, `oldestEnqueueAgeMs`, `dlqTotal`, `publishFaults`,
   `judgeFaults`, `sweptTotal`) and the cost bar's `queueOps` once LOW-2 serves it; the queues per env: prod
   `low-owed-events` and `low-owed-events-dlq` at the promotion, beta `low-owed-events-beta` and `low-owed-events-beta-dlq`
   (created 19:20Z), the generic overlay config `owed-events` and `owed-events-dlq`; the deploy order (the overlay with 473
   on `/health`, then the app layer; LOW-1 is what a wrong order costs).
6. **B1 to B5 and the pillars.** No money moves: the judge is `derive_owed_rows` over the same tables, the press's proof is
   the wallet's landing word, the read is `owed_read` untouched (no read file in the diff). No courier in a hot path: the
   consumer's one external read is the CHAINTRACKS binding (LOW-6 bounds it); the belt's find is unchanged and stays lane
   B's. No tune: no existing wall is widened in product code (NOTE-6), no retry is added as a fix (the DLQ's shape is #576's;
   MEDIUM-2 asks for a bound, not a retry), no batch is enlarged (`max_batch_size = 1`).

## What the Linux tier at the land must exercise

- `make ci-d1-budget` whole, with the record-door cell on the queue (the seven doors under 300 ms, `write;dur` and
  `enqueue;dur` apart, `record.enqueueMsP50/95` and `owedEvents.consumedTotal` at or past 7 served, the planted un-acked
  mark re-published by the belt alarm and acknowledged) against the moved `record-door-*` ceilings (handfile reads 23 to
  38 and statements 21 to 27, potparty 3 to 8 and 4 to 7, the others alike); and the belt cells (`owed-belt-tick`, `-next`,
  `-429`, `owed-tip-mark`, `owed-belt-tick-fleet`) on the queue-configured app layer, which now re-publish instead of
  judging (their reads should fall; NOTE-3's age marks show in the consumer's counters).
- The overlay-to-app-layer road is not delivered under `wrangler dev` (the lane's answer, confirmed by the cell's shape:
  one Miniflare, the app layer's own producer and consumer), so the tier proves the consumer only. Beta proves the road:
  after both deploys, a filing's `Server-Timing` carrying `enqueue;dur`, the overlay's `/health/invariants.owedEvents.publishedTotal`
  rising with the app layer's `/health.owedEvents.consumedTotal`, `oldestEnqueueAgeMs` under the bar, `unackedMarks` not
  growing across ticks, and a MINED callback's row flipping within bar (2) read from the row's `judgedAtMs` against the
  word's `at`.
- MEDIUM-1's pin (two events, one identity, a hydrate that yields until the other committed) and MEDIUM-2's (a pair
  faulting four deliveries is held by the sweep at a doubling period) belong with their folds, under `--test d2_actor_a`.
- The DLQ once (LOW-3): a message whose judge errs four times parks one row; the lever with `family: owed-events`
  re-drives it; `dlqTotal` and `redriven` move.

## The folds, one line each

MEDIUM-1 `max_concurrency = 1` on the `low-owed-events` consumer in both app-layer renders, with its pin; MEDIUM-2 a
per-pair fault memo with a doubling sweep period and a park keyed by pair; MEDIUM-3 the chain latch's rows noted as
`refund`, and the pin's claim made true by a source pin over the writers or its name made modest; LOW-1 `configured()`
gated on the certified count; LOW-2 `queueOps` selected and served; LOW-3 a SQLite pin over the park; LOW-4 three
re-drives then exhausted; LOW-5 state the 100 minutes or take the doubling backoff; LOW-6 route before the tip read;
LOW-7 count the wall drop; LOW-8 note after the batch; LOW-9 drop the notes with no port.
