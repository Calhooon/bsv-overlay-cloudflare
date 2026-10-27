# D2-ACTOR-A-DELTA-LENS: the delta lens on the fold of PR 184 (bsv-low #911, Wave E row E2)

Lane D2-ACTOR-A-DELTA-LENS, written 2026-10-27T21:54Z. A delta review of the fold of overlay PR 184 (`lane/D2-ACTOR-A`,
the lens's three MEDIUMs and nine LOWs) at the branch tip `7ad5279f`, over the lensed tip `59d98ec6`
(`git log --oneline 59d98ec6..7ad5279f`: 955c6a11 M1, cd96e16f M2 with LOW-3/4/5, cb7205ef M3, f577e3f3 LOW-8,
59c0c407 LOW-7 and LOW-9, f7cbb99d LOW-2, de185fa7 LOW-1, ce6c3e39 LOW-6, 7ad5279f the CLAUDE.md line). Checked
against bsv-low `docs/audit/D2-ACTOR-A-lens-2026-10-27.md` (SOUND, 0/3/9/7), `docs/DESIGN-D2-ACTOR-2026-10-26.md`
sections 3, 4 and 7, `docs/FLEET-LOOP-2026-10-27-L52.md` (S1, S2) and the fold lane's REPORT
(`~/low-logs/wave-e/lanes/D2-ACTOR-A-FOLD.out`). Every file and line below is at `7ad5279f`. No server was started,
no tier was run, no product code changed; one scratch test file was written, run and deleted (NEW-M1 below).

## Verdict: NOT LAND (0 HIGH, 1 MEDIUM, 4 LOW, 4 NOTE)

The fold does what the lens asked, item for item (below). ONE new MEDIUM, beyond the lens and present on `59d98ec6`
too: the consumer routes by `(source, sourceKey)` and stops a message's whole plan at the first subject whose judge
faults, so on the SHARED pairs (`age`/`age`, `tip`/`gate`, `tip`/`untipped`, `backfill`/`owed-v2`) one subject whose
judge faults for good holds every other subject marked under that pair. Main's drain judged one subject a call and
collected the fault. Under the lens's own MEDIUM-2 premise (a deterministic judge fault), the fold bounds the cost of
the faulting pair but not the cost to its neighbours. The fold is one function's change (below). Nothing moves money
either way; it delays honest presses.

## NEW-M1 (MEDIUM): one poisoned subject holds a shared-key pair, and every subject behind it

**Where.** `crates/low-app-layer/src/owed_events.rs:412` (`judge_plan`: `let report = deliver(db, &identity,
chunk).await?;` ends the plan on the first faulting chunk; no other subject of the message is judged); the plan's
order is the pair's marks by generation (`ROUTE_MARKS_SQL`, `owed_events.rs:32`). The shared keys:
`owed_backfill.rs:252` (`AGE_MARK_SQL`: `'age','age'`, at most 128 pending, `ORDER BY judgedAtMs`),
`owed_judge/writers.rs:7` (`MARK_TIP_SQL`: `'tip','gate'`), `:22` (`'tip','untipped'`), `owed_backfill.rs:240`
(`'backfill','owed-v2'`). Main's drain for contrast: `owed_judge.rs:928` (one subject a `judge_subjects` call, a fault
pushed to `report.faults`, the loop goes on).

**The failing input.** Subject P (identity BAD, game g1) whose judge errs every time (the lens's MEDIUM-2 premise: a
stored row that does not parse, a statement a later schema refuses). Its last landed judgement ages; within 24 h
`AGE_MARK_SQL` marks it, and since it orders by `judgedAtMs` P is among the OLDEST age marks, so its generation leads
the `age`/`age` pair. Every age event (each sweep, each continuation) plans P first, faults on P, and judges nothing
after it; P's mark is never acknowledged; once 128 age marks are pending behind it, `AGE_MARK_SQL`'s
`LIMIT max(0, 128 - pending)` marks none: the fleet's age repair stops. The same holds for `tip`/`gate`: a seat G whose
refund gate the tip reaches has only the tip's mark; behind P it is never judged, its stored row stays not-claimable
(the tip mark is not of the downgrade class, so the read serves the old row), and G's refund press waits until some
other event of G's own identity routes it. The fold's MEDIUM-2 memo then HOLDS the whole shared pair from the sweep for
up to 6 h (`owed_events.rs:485-503`), so G is not even re-asked by the belt in that time.

**Run** (scratch file `tests/zz_delta_hol_scratch.rs`, deleted after the run, over `support/owed_db.rs` with a
`SqlPort` wrapper that refuses only BAD's `INSERT INTO owed_rows`): two stranded hops (BAD/g1, GOOD/g2), both marked
under one shared pair, BAD's the older generation; five `owed_events::route` calls of the pair, then one
`owed_judge::drain_dirty(&p, 64)` over the same marks:

```
("age", "age"): consumer x5: GOOD rows 0, GOOD marks 1
("age", "age"): drain once: faults 1, GOOD rows 1
("tip", "gate"): consumer x5: GOOD rows 0, GOOD marks 1
("tip", "gate"): drain once: faults 1, GOOD rows 1
```

With the binding configured the drain no longer judges (`owed_belt.rs:957`), so on the queue GOOD is judged only by
an event of its own identity.

**The fold, in one sentence.** `judge_plan` re-judges a faulting chunk one subject at a time, skips a subject whose
judge faults alone (its mark stays, its fault memo keyed by `(identity, game)` rather than by the pair, the sweep
holding that subject only), acknowledges the rest through their own batches, and answers `Err` only when no subject of
the message landed; pin: the run above, GOOD judged on the first delivery of each shared pair, RED on `7ad5279f`.

## 1. The three MEDIUMs' folds

**M1 (two judges of one identity), folded as asked.** `max_concurrency = 1` on the `owed-events` consumer AND its
dead letters' consumer in both app-layer renders (`crates/low-app-layer/wrangler.toml:269`, `:280` prod; `:395`,
`:406` beta); `CONSUMER_MAX_CONCURRENCY` (`owed_events.rs:29`) read by the pin. The pin
`d2_actor_a_fold_m1_one_consumer_at_a_time_derives_the_second_events_rows` (`tests/d2_actor_a.rs:345`) runs the
lens's interleaving through a `Held` port (the first judge's row batch gated until the second committed): two at a
time the first's stale rows stand with every mark acknowledged, one at a time the second's rows stand, and both
renders carry the line (RED at the renders before, GREEN after, as the lane reports; the render assertion is the only
part a config can turn RED, the interleaving half asserts the race's outcome and is green on either tip by design).
What it does NOT serialise, stated: the belt's find (`owed_belt.rs:718`, `judge_with_write`) and the backfill
(`owed_backfill.rs:387`) still judge outside the consumer, as they judged beside the drain on main: the same stale
class between those writers and the consumer (pre-existing, unwidened). Lane B's per-identity serial apply (the
design's MEDIUM-2) is still the end state: it serialises every judge of one identity and runs identities in parallel.

What `max_concurrency = 1` costs (LOW-A below): one consumer, one message at a time, fleet-wide, with no per-message
wall (`consume_one`, `owed_events.rs:716`; the platform's queue bound is 15 min).

**M2 (the faulting pair), folded as asked, with NEW-M1's gap.** The memo (`note_fault`, `owed_events.rs:128`): one
read and one write per fault, on the fault path only; the read is not atomic with the write, safe under the single
consumer of M1. The arithmetic (`fault_hold_ms`): faults 1 and 2 no hold, then 120 s doubling, 6 h from the 11th fault
(120 s x 2^8 = 30,720 s capped at 21,600 s); the message is handed back with the hold
(`retry_with_options`, clamped 1 to 43,200 s, the platform's 12 h; `owed_events.rs:791`), the sweep leaves a held pair
out. A landed judge clears the memo and resolves the letter in the consumer's batch row. The park: ONE row per pair
(`DEAD_LETTERS_CREATE`, PK `(source, sourceKey)`, `overlay-cloudflare/src/owed_events.rs:42`; an undecodable body
keyed `('', messageId)`, never by txid alone), `parks` counted once a message, `lastFault` from the memo, `status`
`parked`/`redriven`/`exhausted` (`DEAD_LETTER_PARK_SQL`, `owed_events.rs:65`); the lever marks `redriven` and counts
`redrives` (`DEAD_LETTERS_REDRIVEN_SQL`, overlay `owed_events.rs:543`), the next park past 3 is `exhausted`, never read
by the lever again (`DEAD_LETTERS_READ_SQL`, `:539`, `status='parked'`). A poisoned pair now costs about 4 deliveries a
hold and one park row; the lane's "13 attempts a day" comes from a pin that runs at most ONE message of the pair in
flight (`tests/d2_actor_a.rs:474`, `if flight.is_none() && t >= next_sweep`); in production the sweep is independent of
a message in flight, and a hand-back due at the hold's end meets the sweep's tick at about the same moment, so two
messages of the pair can overlap (NOTE-2). Bounded either way.

**M3 (the coverage), folded as asked.** `potrefund_chain_noop` notes each latched chain row as `refund` by its own
txid after the latch's batch (`record_post.rs:1612-1617`), pinned by
`record_post::tests::d2_actor_a_fold_m3_the_chain_latch_notes_its_rows_as_refund`. The source pin
(`overlay-cloudflare/src/owed_events_tests.rs`, the `writers` table at `:291-327`) holds 35 writer entries in both
Workers to their note, plus LOW-8's order (no note in `plan`, `evidence_inserts`, `write_queries`; the eviction's note
after `counted_batch`). The lane reports it RED on both mutants (the chain-latch note removed; the lens's `seen` note
removed); not re-run here. Stated: a "contains" pin, it does not see a note on one branch only, nor the refusal's note
order (`submit_refusals.rs:283-292` is right by reading).

## 2. The schema

Migrations 470 to 473 are PR 184's own: `git branch -r --contains 59d98ec6` names only `origin/lane/D2-ACTOR-A`; main
`eaaee0ec` pins 469 (fingerprint 889,593,182), as does `c51e161d`, the lib_rev beta has run since record 1421 (so beta's
certified count is 469, not 409 as the brief has it; prod 409 since record 1394); no record shows a render at 470 or
past. So the in-place edit of `owed_event_dead_letters` is safe for every deployed D1. One edge, stated: a D1 that ran
the OLD 470 to 473 (a local tier state or a scratch D1 from the lane's or the lens's runs) keeps the old table under
`CREATE TABLE IF NOT EXISTS` while the new fingerprint re-certifies the list, and its park then faults on `ON
CONFLICT(source,sourceKey)`: tiers start from fresh state; nothing remote ran it.

The app layer reads two counts: `OWED_TRIGGERS_MIGRATION_COUNT` stays 409 (`owed_backfill.rs:321`, the backfill's
gate, unchanged), and the queue's new gate `OWED_EVENTS_MIGRATION_COUNT` = 473 (`owed_events.rs:577`) against
`overlay_migration_count`. The overlay certifies count 473, fingerprint 751,808,187
(`tests/owed_derived_sqlite.rs:79-82`; the prefix 469 still 889,593,182).

bsv-low pins NO overlay fingerprint: `scripts/check-migration-pins.py` checks the root SDK pins, the frozen money
fixture hashes and the era seed pins, nothing of the overlay's list; `app/e2e` holds overlay counts only in frozen
invariant snapshots (`fixtures/l37-invariants/*`, `matrix/fixtures/invariants-overlay-beta-2026-10-17.json`), which no
check compares. The one reader is `scripts/promote-legs.py` O10 (`--expect-migrations N`, the COUNT compared, the
fingerprint printed). So "bsv-low's migration pin has to move" means: the window and the promotion pass
`--expect-migrations 473` (where 409 was passed), and bsv-low's CLAUDE.md prose names 473; the fingerprint moves
nowhere in bsv-low.

## 3. The nine LOWs

- LOW-1 folded: `configured()` is async, the binding AND the certified count at or past 473, a found count held for the
  isolate, a short one re-read each 60 s (`owed_events.rs:598`); every caller awaits it (`internal_events.rs:680`,
  `owed_belt.rs:957`, `handfile.rs:995`, `record_post.rs:2377`, `routes.rs:3050`, `:3248`, `tracker_host/runtime.rs:712`).
  Delta NOTE-3: its pin asserts EQUALITY with the overlay's list length (`tests/d2_actor_a.rs:593`), so the next
  appended migration reds it and invites raising the gate to 474 where `>= 473` is the rule.
- LOW-2 folded: `queueOps` selected (`cost_bar.rs:33`) and served beside `statements` (`:100`).
- LOW-3 folded: the park runs over real SQLite in `d2_actor_a_fold_l3_*` (`tests/d2_actor_a.rs:529`).
- LOW-4 folded: three re-drives then `exhausted`. Delta LOW-C: a `redriven` row whose message neither parks again
  nor lands a judge (a lost message; a landed judge whose consumer batch row faulted) stays `redriven` for good, never
  read by the lever (no stale return, #576 has one at 1 h) and holding one of the 2,000 places; and no discard lever
  takes the `owed-events` family (`dead_letters.rs:1554` routes only the re-drive), so an `exhausted` row leaves only
  when its pair's judge lands. The marks stay either way; places only.
- LOW-5 folded: the hand-back is the letter's age, 60 s to 30 min (`park_retry_delay_s`, `owed_events.rs:251`), about
  48 h over the 100 retries. It reads `msg.timestamp()` (the lane's own unverified item: whether the platform's
  timestamp of a DLQ message is its first send or its move to the DLQ; either reading keeps the delay in 60 s to 30 min).
- LOW-6 folded, with delta LOW-B: the plan precedes the tip read and an identity note is sent only with a mark
  (`READ_IDENTITY_SQL`, overlay `owed_events.rs:94`); `txfacts` still always sent (lane B's mirror). The tip is now an
  isolate memo of 30 s (`tip_memoised`, `owed_events.rs:700`), and the `tip` events carry no height
  (`internal_events.rs:683`, pairs `tip`/`gate` and `tip`/`untipped`): see LOW-B.
- LOW-7 folded: the wall drop counted on the isolate, written with its next publish
  (`owed_events_publish_wall_drops_total`, served `publishWallDrops`); an isolate that ends first loses its count (stated).
- LOW-8 folded: the eviction's notes after its batch (`eviction_write.rs` `plan_pairs`, called after `counted_batch`),
  `evict_core_with_abort` after the open marker answered, the refusal's after each upsert.
- LOW-9 folded: no port, the notes drained and dropped (`publish_env`).

## 4. Beyond the lens: the diff whole

`git diff 59d98ec6..7ad5279f --stat`: 20 files, +1,136 / -122. New served words: `/health.owedEvents.publishWallDrops`,
`faultHeld`, `sweepHeld`, `faultMemo {rows, held, maxFaults, last, holdAfter, holdBaseMs, holdCapMs}`,
`deadLetters {<status>: n}`; the lever's answer `maxRedrives`; `/health/invariants.costBar` `queueOps`. New counters
(seeded, `COUNTERS` 10 to 13): `owed_events_fault_held_total`, `owed_events_sweep_held_total`,
`owed_events_publish_wall_drops_total`. New log lines: `[owed-events] the overlay's certified migration count ... the
inline judge stands`, `PARKED dead letter <id> (<status>)`, `handed back in <n> s`. New `owed_belt_state` rows
`owed-events:fault:<source> <key>`. Render: `max_concurrency = 1` on four consumers; no var, no binding added. No
trigger text changed. No migration number moved (473); one CREATE edited in place (section 2). Ownership manifest and
`docs/STORAGE-OWNERSHIP.md`: the dead letters re-described. Nothing else.

**LOW-A, the serial consumer under L52's rate.** L52 (FLEET-LOOP S2) logged 1,369 `/internal/tx-word` calls in
16:05Z to 16:15Z and 1,469 in 16:15Z to 16:25Z, about 8,000 an hour, the ok ones 2.4 to 3.1 s each and most of the
rest canceled at about 100 s; under PR 184 each frame is a `word` event on the ONE serial consumer, beside every pot,
party, filing and tip event. One consumer keeps up only if a message's service time stays under about 0.45 s at that
rate; if a word ingest costs what main's handler cost (unmeasured: the consumer ingests and routes, main's handler also
judged), the backlog grows for the loop's length and a MINED callback's Collect press (bar (3), 5 s) waits behind it;
a message that hangs as S2's did holds the whole queue for its duration (no per-message wall in `consume_one`). Not a
MEDIUM only because the service time is unmeasured. The fold, if the tier or beta shows it: race `consume_one`
against a wall (hand back with a delay on breach), and keep the word frames off the serial lane (their own queue, or
lane B before L53). The served gauge is `oldestEnqueueAgeMs`.

**LOW-B, the tip memo delays a gate by a block.** A block's hook writes `MARK_TIP_SQL` at height h and publishes
`tip`/`gate` (no height in the event); a warm consumer (a judge within the last 30 s, the common case under load)
judges those subjects at its memo's h-1, so a row whose gate is exactly h is judged closed and its mark acknowledged;
the next block's mark (window h-5..h+1) re-judges it. A refund press one block (about 10 min) later than on
`59d98ec6`, whose consumer read the tip fresh per message. Fold: a `tip` event (or any event older than the memo) reads
the tip fresh, or the hook's height rides the event's facts.

**LOW-C**, above under LOW-4.

## 5. B1 to B5 for the honest seat

No money moves on this path, before or after the fold: the judge is `derive_owed_rows` over the same tables, the read
(`owed_read`) is untouched, the press's proof is the wallet's landing word and the pre-signed refund the wallet holds.
B1 (no griefer profit), B2 (winner paid or made whole), B4 (no double-dip) and B5 are untouched: the queue decides only
WHEN a row is re-judged. B3 (an honest disconnect is refunded) holds in money and is delayed in liveness:
- a lost message: the mark stays, the sweep re-publishes within a tick (before and after the fold);
- a poisoned pair: before the fold, 2,880 attempts a day and the park filling; after it, about 4 deliveries a hold and
  one row; but on a SHARED pair (NEW-M1) the honest seat behind the poisoned subject is not judged at all until its own
  identity's next event, before and after the fold, and the fold's hold keeps even the sweep off it for up to 6 h;
- a park: the marks stay, nothing of the seat is lost; a re-drive replays the same pair, three times at most;
- the tip memo (LOW-B): a refund gate opens one block late at worst.
The downgrade class still withholds a dead press at the read whenever a downgrade mark is pending, so a delay never
serves a press the evidence has withdrawn, except M1's interleaving class for the belt and the backfill (pre-existing).

## 6. What the land and the window must show

The Linux tier at the land: `make ci-d1-budget` whole with the record-door cell on the one consumer (the seven doors
under 300 ms, `consumedTotal` at or past 7, the planted un-acked mark re-published and acknowledged); the belt cells
re-publishing with `sweepHeld` 0 and `faultMemo.rows` 0; the dead-letter queue once (a judge failing four times parks
ONE row with `lastFault`, the lever with `family: owed-events` marks it `redriven`, a fourth park reads `exhausted`);
NEW-M1's pin once folded; and the consumer's per-message service time recorded (LOW-A's figure).
Only beta can show: the overlay's `/health/invariants.owedEvents.publishedTotal` rising beside the app layer's
`/health.owedEvents.consumedTotal` (the cross-Worker road no `wrangler dev` session delivers), `oldestEnqueueAgeMs`
under a loop's load (LOW-A), `unackedMarks` flat across ticks, the overlay certified at 473 BEFORE the app layer (the
gate keeps the inline judge until it reads 473), and the queues `low-owed-events-beta` and `low-owed-events-beta-dlq`
(created 2026-10-27, record 1436) bound by the beta render. Prod's pair is created at the promotion, with
`--expect-migrations 473`.

## Notes

- NOTE-1: the M1 pin's interleaving half proves the class, not the config; only its render half can go RED.
- NOTE-2: the M2 pin models one message of a pair in flight; the sweep and a delayed hand-back can overlap in
  production, so the attempts a day may be up to about twice the pin's figure (still bounded by the doubling hold).
- NOTE-3: LOW-1's equality pin (section 3).
- NOTE-4: the memo's read-then-write in `note_fault` relies on M1's single consumer; under lane B it needs the actor's
  serialisation or one statement.

## What was run (at `7ad5279f`, `CARGO_TARGET_DIR=/Volumes/Lanes/targets/d2-actor-a`, foreground)

- `cargo test --manifest-path crates/overlay-cloudflare/Cargo.toml`: lib 1,038 passed, 1 ignored; every integration
  binary ok, 0 failed.
- `low-app-layer` lib, in four filtered runs of its test binary: `owed` 113 passed (1 ignored); `--skip owed` with
  `logic:: routes:: results:: identity::` 245 (2 ignored); `live_view:: record_post:: txany:: hops_view::
  tracker_host:: auth::` 169 (1 ignored); the rest 166: 693 passed, 4 ignored, 0 failed.
- `low-app-layer` integration: `d2_actor_a` 12, `owed_judge_sqlite` 15, `owed_derived_sqlite` 5,
  `owed_backfill_sqlite` 18, `owed_belt_sqlite` 7; all passed.
- `cargo clippy --all-targets -- -D warnings` and `--lib --target wasm32-unknown-unknown -- -D warnings` on both worker
  crates: exit 0, four times.
- The scratch NEW-M1 run above (one test, passed as written: it asserts the hold), its file deleted.
