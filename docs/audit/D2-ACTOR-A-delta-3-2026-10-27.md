# D2-ACTOR-A-DELTA-LENS-3: the third delta lens on PR 184 (bsv-low #911, Wave E row E2)

Lane D2-ACTOR-A-DELTA-LENS-3, written 2026-10-28T00:18Z. A delta review of overlay PR 184 (`lane/D2-ACTOR-A`) at
`f20070a1`, over the second delta lens's tip `7bb95ea2` (bsv-low `docs/audit/D2-ACTOR-A-delta-2-2026-10-27.md`: NOT
LAND, NEW2-M1, LOW-1 to 3, NOTE-1 to 4). It covers two things. First the fold (`e05fa283` and its CLAUDE.md line
`f20070a1`). Second the rebase onto main `139fdf36` (PR 191, the detached word judge), which is inside
`git diff 7bb95ea2..f20070a1`. The pending rebase onto `2c7a46f1` (PR 194, the push coalescer) is question 3. Every
file and line below is at `f20070a1`. No server was started, no tier was run, no product code was changed.

## Verdict: LAND (0 HIGH, 0 MEDIUM, 3 LOW, 3 NOTE), with one change required at the land's rebase (question 3)

NEW2-M1 is folded as asked. Held subjects are left out inside the mark reads, so they never take a place of the 64. A
message that judged nothing publishes no continuation. An all-held message ends acked, with its marks standing, and is
counted. LOW-1 and LOW-2 of the second lens are folded. The rebase joins the queue to PR 191 correctly. With the
binding configured, the queue's consumer is the only judge of a word's subjects. Without it, every path is main's. One
item is not a finding on this tip but must be done at the land: once the branch is rebased onto `2c7a46f1`, the
consumer's judge only QUEUES its pushes, and the consumer never flushes them (question 3, R1).

## 1. NEW2-M1's fold (question 1): holds

- **The SQL.** `ROUTE_MARKS_SQL` (`crates/low-app-layer/src/owed_events.rs:34`) and `ROUTE_IDENTITY_SQL` (`:36`) add
  `NOT EXISTS (SELECT 1 FROM owed_belt_state WHERE name='owed-events:fault:subject '||identity||'/'||gameId AND
  json_extract(cursor,'$.heldUntilMs')>?now)`. That is one primary-key probe per mark on the existing index walk.
  `fold3_m1_d` asserts the plan uses the source-key or identity index, never `SCAN owed_belt_state`, and probes the memo
  `USING` its key (`tests/d2_actor_a.rs`, around line 1088). A memo below the bound has `heldUntilMs = now` (written by
  `note_fault_named`, `:173`), so it never counts as held. A wanted envelope's name ends in `/` and is never held.
  Cost: the walk skips only held marks, so it grows with the held marks ahead in the pair, not with the pair.
- **The held count.** `ROUTE_HELD_SQL` (`:39`) and `ROUTE_IDENTITY_HELD_SQL` (`:41`) count the distinct held subjects
  among the first `ROUTE_READ_LIMIT` marks. They run in the same `run_batch` as the marks (`plan`, `:454-475`), so the
  two reads see one snapshot. The count is bounded by the window. It is a figure for the counters only and no rule
  reads it except `all_held`.
- **The continuation.** `continues` (`:409`) is `more && subjects > 0`. It is called at both sites: the publish
  (`consume_one`, `:1040`) and the counter (`:1125`). `more` is now computed over unheld marks only (`:476`), so a
  window of held marks no longer sets it. 64 poisoned subjects at a pair's head now cost one index walk per delivery,
  not a chain of messages. `fold3_m1_a` is RED on `7bb95ea2` (`left: (0, 0, 64, true) right: (1, 0, 64, false)`, per
  the fold's REPORT).
- **The all-held end.** In `consume_one` (`:1033-1034`), when every read mark was held, `plan.is_empty()` and the
  message returns `empty_routed()` (`:399`): no tip read, no judge, no continuation. `queue` acks it and counts
  `owed_events_all_held_total` when `all_held` holds (`:415`, `:1128`). The marks stand because only a judge's own
  batch deletes them (`ACK_DIRTY_SQL`, generation-bounded). Each held subject keeps its own memo. The pair's own memo
  is untouched (the message is `Ok`). After the hold ends, the sweep re-publishes the pair: its 120 s tick reads the
  backlog, and the delta-2 fold only skips pairs whose every subject is still held (`fold3_m1_c`).
- **Every subject held, and a new mark lands between the read and the ack.** Holds. The ack deletes no mark. Two cases:
  - The new mark is a held subject's: it waits out the hold like the others.
  - The new mark is an unheld subject's: its writer notes and publishes its own pair, which is serialised behind this
    message (`max_concurrency = 1`). If that publish is lost, the sweep re-publishes the pair after a tick.

  Either way the mark is judged, never dropped. The same holds for a new mark landing behind a continuation.
- **The pins.** `fold3_m1_a` (64 held subjects plus one live: the live one is judged, `more` false, no continuation),
  `_b` (all held: acked, counted, marks standing, no continuation), `_c` (the sweep after the hold), `_d` (the plans).
  All four are declared and run here (`--list`, then the suite).

## 2. The rebase join with PR 191 (question 2): holds

- **With the queue configured.**
  - `event_held` lands the word through `land_word` (main's #934 write). It then notes `("word", txid)` when
    `owed_events::configured` (`crates/low-app-layer/src/tracker_host/runtime.rs:711`).
  - `ingest` reads `queued` once (`:899`). Its judge closure returns early under the queue, so it starts no detached
    judge and claims no `DETACHED_JUDGE`. It publishes before the answer (`:936`; `publish_before_answer` is bounded
    at 1.5 s).
  - The scheduled `tick` reads `queued` (`:1001`), skips `judge_word` under it (`:1006`) and publishes at its end
    (`:1011`).
  - The consumer judges the word's subjects: it ingests the frame (`owed_events.rs:957`) when the event carries one,
    then routes the `word` pair's marks.
- **Without the binding** (or before the overlay certifies 473), `configured` is false. Then `ingest` claims the
  isolate's one detached judge (`:908`) as on main, and `tick` judges inline as on main.
- **`configured` changing mid-request.** A reading of `true` holds for the isolate's life, and a `false` is re-read
  every 60 s (`owed_events.rs:883-907`). So the only possible flip is false to true between `ingest`'s read and
  `event_held`'s. That request both judges detached and notes the pair, which is double coverage, never neither.
- **The door's answer order.** The write is durable before the answer, and the publish is bounded before the answer.
  A publish that is lost after the write (a fault, or the 1.5 s wall: `owed_events_publish_wall_drops_total`) leaves
  the marks the `tx_words` triggers wrote. The sweep re-publishes them past one tick, as the design says. The judge's
  20 s wall (`OWED_DRAIN_WALL_MS`) belongs only to the unqueued detached judge.
- **The queue behind.** Under the queue the scheduled drain does not judge: `drain_env` re-publishes through
  `sweep_publish` (`crates/low-app-layer/src/owed_belt.rs:957-962`). The marks are durable until a judge's batch
  acknowledges them, so a slow queue delays judgement and loses nothing. The ruling's "nothing is lost" holds on this
  tree. The cost of the delay is stated in NOTE-1.
- **Health.** `/health.owedEvents` is served (`crates/low-app-layer/src/routes.rs:6993`), now with `allHeld`
  (`owed_events.rs:1306`), and so is `/health/invariants.owedEvents` (`crates/overlay-cloudflare/src/ops.rs:2179`).
  There is no `/health.owedActor` on this branch; that name belongs to lane B. The detached judge's three counters are
  mirrored (`runtime.rs`, `tracker_mirrored_counter_names`).

## 3. The pending rebase onto `2c7a46f1` (PR 194, the push coalescer) (question 3): one change required

On main, `D1Judge::push` only QUEUES the push (`push_coalesce::queue_seat_push`, a thread-local per isolate). The
queue is sent by `flush_after` or `flush_queued`, which main calls at these sites:
- the fetch door (`serve`, after the router; `flush_counters_if_due`);
- `/internal/owed-backfill-pass` and `/internal/owed-belt-tick`;
- the pot, hop and tip hooks;
- the three scheduled tasks' ends;
- the belt actor (`BoardView::flush_pushes`);
- the record door's detached judge.

On this branch (`f20070a1`) the push is still direct (`owed_judge.rs:271-272`, `first_party_push`), so this tip has no
defect. After the rebase, though, the consumer judges through `D1Judge` (`consume_one`, `owed_events.rs:1037`), and
under the queue it is the ONLY judge (the doors, hooks, word ingest, tick and drain all hand off to it). Nothing
flushes in the queue handler (`owed_events::queue`, `:1047-1170`, and the `#[event(queue)]` wrapper in
`crates/low-app-layer/src/lib.rs:371-377`, which holds `_ctx`). Every `owed-changed` push then waits in the consumer
isolate until some later fetch in that same isolate flushes it, or is lost when the isolate ends. In a queue-only
isolate that loss is likely. Money does not move, but every seat's live push under the queue is delayed or lost.

**R1, the change at the land's rebase.** In `crates/low-app-layer/src/lib.rs`'s `queue` event, after
`owed_events::queue(batch, env.clone()).await` and the cost-window write, add
`push_coalesce::flush_queued(env.clone()).await;`. Do NOT use `wait_until`: per message the queue's wall is the
handler's. The flush also needs a pin of the shape main uses for the scheduled tasks (`owed_backfill.rs:1313-1315`'s
source pin): the wrapper's text must contain the flush after `owed_events::queue`.

The flush sits outside the 60 s per-message wall. Under `max_concurrency = 1` it adds the coalescer's send time to each
batch, and `serviceMs` does not count it. That cost is stated, not changed.

Separately, NOTE-3: main's own PR 191 path (the unqueued detached judge in `ingest`) also queues its pushes inside a
`wait_until` that starts after `serve`'s `flush_after` has already looked at an empty queue. Its pushes wait for the
isolate's next answer. That is main's, and it is unchanged by this PR. The land can close it in the same rebase by
calling `push_coalesce::flush_queued(env).await` at the end of the detached task in `runtime.rs` `ingest`, after
`DETACHED_JUDGE.release()`.

## 4. LOW-1, LOW-2 of the second lens, the cell, the migrations, and what is beyond the diff (question 4)

- **LOW-1 (the wall notes its subjects), folded.** `set_in_flight` brackets each `deliver` (`:621-623`, `:648-650`).
  `queue` clears `IN_FLIGHT` before each message (`:1076`) and takes it in the wall's branch (`:1098`).
  `note_wall_subjects` (`:227`) notes each subject in flight and hands the message back after the longest hold (at
  least 60 s). With nothing in flight it notes the pair (the plan's reads, the ingest). The thread-local is safe
  because the consumer runs one message at a time per isolate. A judge batch that lands after the drop clears the
  memo at its next landing (NOTE-3 of the second lens).
- **LOW-2 (a stale memo), folded.** `stale_memo_faults` (`:190`) counts from 0 when `now > heldUntilMs + 6 h`, and
  every write carries `heldUntilMs` (`:172`). The same rule serves the pair's memo, which is sound.
- **The cell `tools/lane-911/poisoned_subject.mjs`** (unrun here, by the lane's rule). It is wired in `ci-d1-budget`
  after the belt tier (`Makefile:681-682`, under `$$seal`) and reads the record-door cell's exported fixture.
  - Sealed: ARCADE_URL, COURIER_SINK and CORROBORATOR_URL point at port 9, DUAL_BROADCAST is off, there is no
    RELAY_URL, and CHAINTRACKS is an in-process stub.
  - Its scratch lives under `TMPDIR` and is removed in `finally`.
  - It asserts the subject's own memo, the all-held counter, `owed_events_continued_total` flat, and the poisoned mark
    standing.

  See LOW-3 for its scale.
- **Migrations.** `OWED_EVENTS_MIGRATION_COUNT` stays 473 (`owed_events.rs:862`). The fold edits no migration (its
  diff touches no `migrations` source). The fingerprint stays 1,823,919,051 from the delta-2 fold, and O10's
  `--expect-migrations 473` is unchanged.
- **Beyond the diff.** LOW-1 and LOW-2 below.

### LOW-1 (new to this lens; present since the PR's first fold): the consumer's word ingest leaves a note that nobody publishes

The consumer's `ingest_frame` (`owed_events.rs:957`) calls `runtime::event`, whose `event_held` notes `("word", txid)`
into the isolate's `NOTES` (`runtime.rs:711`). The consumer publishes `NOTES` only on a continuation (`:1040-1042`).
`queue` does not drain them at the batch's end. So in a consumer isolate, `NOTES` grows by one entry per word frame
ingested (`crates/overlay-cloudflare/src/owed_events.rs:186`, an unbounded `BTreeMap`) until a continuation happens or
the isolate recycles. A later continuation then re-sends every stale word pair. A pair whose marks are gone is skipped
by the read; a pair whose marks still stand (held, or past 64) becomes one extra message. The failing input: a run of
word events with no continuation, which is the common case. Memory grows per isolate, and the result is duplicates,
never a loss. **Fold:** in `ingest_frame`, after the event, call `publish::forget("word", &txid)`, because this very
message routes that pair next.

### LOW-2: an envelope's expansion is not in the held count

`ROUTE_HELD_SQL` counts only the first read's window. The expansion of a wanted envelope (`plan`, `:488-500`) leaves
held subjects out but counts none. So an envelope whose expanded subjects are all held is acked as an ordinary empty
plan: `allHeld` is not bumped and `subjectHeld` undercounts. This affects counters only. The marks and holds behave as
in section 1. **Fold, optional:** count the expansion's held subjects with `ROUTE_IDENTITY_HELD_SQL` in the same loop.

### LOW-3: the cell runs one poisoned subject, not the cap

The second lens asked for 65 subjects, 64 of them faulting, at the cap. The cell runs three honest subjects and one
poisoned one. The cap's scale is pinned natively (`fold3_m1_a`, real SQLite with the shipped statements), but not on
workerd D1 through the real consumer. The Miniflare consumer options also omit `maxConcurrency`, which does not matter
for a serial cell. Accept as is, or grow the cell to 65 at the land.

## 5. B1 to B5 for the honest seat (question 5)

No money moves on these paths: the judge is `derive_owed_rows` over the same tables, and the read and its D2-L1
downgrade rule are untouched. B1, B2, B4 and B5 hold. B3 holds in money and is delayed in liveness:
- **A held subject:** its own press waits out its hold (120 s doubling to 6 h; counting from 0 again after 6 h quiet).
  Its neighbours in a shared pair are judged on the same delivery, however many are held (NEW2-M1 closed).
- **An all-held message:** acked. The held presses wait for their hold. A neighbour marked meanwhile comes through its
  own publish or the sweep.
- **A word frame under the queue:** the word is durable and served at its write. Its subjects' rows are judged by the
  consumer behind the queue's backlog. Until then a press over a pending downgrade mark is withheld at the read, never
  served stale.
- **The queue behind:** every judged row lags by the backlog. Nothing is acknowledged unjudged, and the sweep
  re-publishes. After R1, the seat's `owed-changed` push follows the consumer's batch. Before R1, on a rebased tree, it
  would wait for a fetch in that isolate.

## 6. What the land must show (question 6)

**The Linux tier** (`make ci-d1-budget` whole, on the rebased tree with R1):
- the record-door cell on the one consumer: the doors under 300 ms, `consumedTotal` at or past 7, the planted un-acked
  mark re-published and acknowledged;
- the belt cells with `sweepHeld` 0 and `faultMemo.rows` 0;
- the dead-letter queue once: a judge failing four times parks one row, the lever re-drives it, the stale return
  happens after the hour, and the discard takes the row;
- the poisoned-subject cell: the subject memo, `allHeld` at least 1, `continued` flat;
- a word frame through `/internal/tx-word` with the binding: no `tracker_detached_judge_runs_total`, the `word` event
  consumed;
- R1's pin.

**Only beta can show:**
- `serviceMs` and `oldestEnqueueAgeMs` under a loop's mix (the rate ruling's gate);
- `publishedTotal` against `consumedTotal` across the two Workers;
- `allHeld` and `subjectHeld` on live pairs;
- the wall's behaviour on wasm32;
- the consumer isolate's `NOTES` growth (LOW-1) and the pushes after R1 reaching seats (the relay's `push` health);
- the overlay certified at 473 (1,823,919,051) BEFORE the app layer deploys.

## Notes

- **NOTE-1:** while the queue is behind by more than a tick, the sweep re-publishes up to 64 pairs every 120 s, with
  no read of the consumer's backlog age. The duplicates are cheap (an empty plan) but they queue ahead of new events
  and cost queue operations. The second lens stated this (section 2), and it is unchanged.
- **NOTE-2:** `judge_plan` still reads the subject memos after the plan (`:578-588`). Since the plan already excludes
  held subjects, that read only catches a hold set between the two statements. NOTE-1 of the second lens stands: it
  is one statement per message.
- **NOTE-3:** main's detached word judge strands its queued pushes until the isolate's next answer (section 3). That
  is main's; the fix is in section 3.

## What was run (at `f20070a1`, `CARGO_TARGET_DIR=/Volumes/Lanes/targets/d2-actor-a`, `TMPDIR=/Volumes/Lanes/tmp`, foreground)

- `cargo test --manifest-path workers/Cargo.toml -p bsv-overlay-cloudflare --lib`: 1,045 passed, 1 ignored, 0 failed.
- `low-app-layer` lib, the test binary in four filtered runs: `routes` 79 (2 ignored), `owed` 114 (1 ignored),
  `tracker` 29, and `--skip routes --skip owed --skip tracker` 475 (1 ignored). All 698 listed tests ran, 0 failed.
- `low-app-layer` integration: `d2_actor_a` 20, `owed_judge_sqlite` 17, `owed_derived_sqlite` 5,
  `owed_backfill_sqlite` 18, `owed_belt_sqlite` 7, `d1_fleet_plans` 11. All passed.
- `cargo clippy -p <crate> --all-targets -- -D warnings` (native) and `--target wasm32-unknown-unknown -- -D warnings`
  on `low-app-layer` and `bsv-overlay-cloudflare`: clean, four times.
- Not run: any tier cell (`ci-d1-budget`, `ci-route`), wrangler, or the rebase onto `2c7a46f1` (question 3 is read from
  `git diff 139fdf36..origin/main`).
