# D2-ACTOR-A-DELTA-LENS-2: the second delta lens on the fold of PR 184 (bsv-low #911, Wave E row E2)

Lane D2-ACTOR-A-DELTA-LENS-2, written 2026-10-27T23:19Z. A delta review of the fold of overlay PR 184
(`lane/D2-ACTOR-A`) at `7bb95ea2`, over the first delta lens's tip `7ad5279f` (`git diff 7ad5279f..7bb95ea2`: 1c9f6060
the fold, 7bb95ea2 a clippy fix to its pin; 9 files, +689 / -50). Checked against bsv-low
`docs/audit/D2-ACTOR-A-delta-2026-10-27.md` (NOT LAND: NEW-M1, LOW-A, B, C, NOTE-1 to 4), the fold lane's REPORT
(`~/low-logs/wave-e/lanes/D2-ACTOR-A-FOLD-2.out`) and `docs/DESIGN-D2-ACTOR-2026-10-26.md` sections 3, 4 and 8. Every
file and line below is at `7bb95ea2`. No server was started, no tier was run, no product code changed; one scratch test
was appended to `tests/d2_actor_a.rs`, run and removed with `git checkout` (NEW2-M1 below).

## Verdict: NOT LAND (0 HIGH, 1 MEDIUM, 3 LOW, 4 NOTE)

The fold does what the first delta lens asked for one poisoned subject: the pin's shapes (a poisoned subject beside
others, under `age`/`age` and `tip`/`gate`) are judged as asked, and LOW-A, B and C and NOTE-3 are folded. ONE new
MEDIUM, introduced by the fold: held subjects still occupy the plan's 64-game cap, and a message that judged nothing
still publishes its continuation. So 64 poisoned subjects at the head of a shared pair (a) starve every subject behind
them, which is NEW-M1 again at the cap's scale, and (b) turn each delivery that meets them inside their hold into an
`Ok` with `more = true`, which publishes a continuation that plans the same 64. That chain of no-op messages runs on
the ONE serial consumer for as long as the holds re-arm. On `7ad5279f` the same input was an `Err` under the pair's
memo, so it was bounded. The fold is one function's change (below). Nothing moves money; it delays honest presses and
costs the serial consumer's time.

## NEW2-M1 (MEDIUM): 64 held subjects at a shared pair's head starve the pair and spin its continuation

**Where.** `crates/low-app-layer/src/owed_events.rs:410` (`plan`: the first `MAX_GAMES_PER_MESSAGE` (64) distinct
games in generation order are taken before any memo is read; `more = true` past them). `:453` (`judge_plan` reads the
subject memos only after the plan is cut, skips each held one, `out.held += 1`; `:563` answers `Err` only when nothing
landed AND some subject faulted, so an all-held plan is `Ok`). `:919-921` (`consume_one`: `if routed.more` publishes
the pair again, whether or not anything landed). `:988` (the all-held `Ok` resolves nothing but is ACKED).

**Why the head stays the same.** A held subject's mark is never acknowledged, so it keeps its generation:
`AGE_MARK_SQL` (`owed_backfill.rs:252`) skips a subject that already holds an age mark, so the oldest-judged poisoned
subjects keep the oldest generations of `age`/`age` for good. `MARK_TIP_SQL` (`owed_judge/writers.rs:7`) and
`MARK_UNTIPPED_SQL` (`:22`) re-mark with a fresh generation inside one statement (`GROUP BY identity,gameId`, and the
untipped index order with `LIMIT 128`), so whether an honest subject sorts behind 64 poisoned ones there depends on
its key, not its age. The natural deterministic fault is per IDENTITY (the judge hydrates and caps one identity's rows;
a stored row that does not parse; a statement a later schema refuses), so one whale identity with 64 or more aged
games is enough for `age`/`age`.

**The failing input, run.** A scratch test over `support/owed_db.rs`: 64 stranded hops of one identity (games
`bad00` to `bad63`, `fail_bind = "bad"`, so every judge write naming them faults) and one honest identity's stranded
hop `g1` seeded after them, all moved onto one shared pair; six `owed_events::route` calls one second apart, then the
sweep with the marks aged four ticks:

```
age/age delivery 1: Err(injected fault on the subject bad) good_rows=0
age/age delivery 2: Err(injected fault on the subject bad) good_rows=0
age/age delivery 3: Err(injected fault on the subject bad) good_rows=0
age/age delivery 4: Ok subjects=0 faults=0 held=64 more=true good_rows=0
age/age delivery 5: Ok subjects=0 faults=0 held=64 more=true good_rows=0
age/age delivery 6: Ok subjects=0 faults=0 held=64 more=true good_rows=0
age/age sweep: [("age", "age")]
tip/gate delivery 1: Err(injected fault on the subject bad) good_rows=0
tip/gate delivery 2: Err(injected fault on the subject bad) good_rows=0
tip/gate delivery 3: Err(injected fault on the subject bad) good_rows=0
tip/gate delivery 4: Ok subjects=0 faults=0 held=64 more=true good_rows=0
tip/gate delivery 5: Ok subjects=0 faults=0 held=64 more=true good_rows=0
tip/gate delivery 6: Ok subjects=0 faults=0 held=64 more=true good_rows=0
tip/gate sweep: [("tip", "gate")]
```

The honest `g1` is never judged. Each of deliveries 1 to 3 made 4 chunk judges and 64 single judges (68 calls). From
delivery 4 every delivery is an acked `Ok` with `more = true`, and in the Worker (`consume_one`, `:919`) each one
publishes a continuation of the same pair. The sweep still re-publishes the pair: the honest subject's mark is among
the rows it reads, so the pair is not "every subject held" (`unheld`, `:197`).

**What it costs in production.** Any delivery of the pair inside the subjects' hold starts a chain: each block's
`tip`/`gate` event, a sweep tick between the pair memo's end and the next fault, a hand-back. A chain message arriving
while the subjects are held is `Ok` and publishes the next one. A chain message arriving just after the holds end
faults (68 judge calls), re-arms them with a longer hold and is handed back under the pair memo's delay; the other
chains keep spinning. So the population of chains grows with the pair's direct publishes (about 6 an hour for
`tip`/`gate`) and shrinks only when one message collects four `Err`s, while each chain message costs the serial
consumer its plan read, its memo read and a queue send. On `7ad5279f` the same input was an `Err` on every delivery
(the pair's memo, 4 deliveries a hold): bounded, and the honest subject was starved in the same way.

**The fold, in one sentence.** `plan` reads the subject memos of the marks it reads and does not count a held
subject toward the 64 (so the plan reaches past them to the next unheld subjects), and `consume_one` publishes a
continuation only when the message landed at least one subject; pin: the run above, `g1` judged on delivery 4 and no
continuation from an all-held plan, RED on `7bb95ea2`.

## 1. NEW-M1's fold (question 1)

- **The re-judge cost.** A chunk of N subjects (N at most `OWED_JUDGE_SUBJECTS_PER_INVOCATION`, 16) that faults costs
  N + 1 judge calls plus two statements per faulting single (`note_fault_named`, a read and a write, `:143`). A message
  holds at most 64 games, so at most 4 chunks: 4 + 64 = 68 judge calls and 128 memo statements in the worst case (every
  subject faulting, as in the run). Holds, bounded, under the 60 s wall for any D1 that answers in about 0.6 s a call;
  under a slow outage the wall stops it (LOW-1 below for what the wall then notes). The memo read added to every
  message (`:468`, one `json_each` primary-key read of up to 64 names) is one statement more a message on the serial
  consumer (NOTE-1).
- **The subject memo's key and arithmetic.** `subject_memo_name` (`:101`): `owed-events:fault:subject <identity>/<game>`,
  inside the prefix range the health reads already take, never equal to a pair's name (a pair's source is never
  `subject`). The hold is the pair's `fault_hold_ms` (faults 1 and 2 none, then 120 s doubling, 6 h from the 11th).
  Holds. A landed subject's memo is cleared only when the consumer lands it (`:553`); see LOW-2.
- **The sweep's pair rule.** A pair with one held and one new subject is re-published (`unheld`, `:197`: held only when
  every subject read is held). Holds, and is pinned. The sweep reads at most 256 rows a tick, so a pair whose read rows
  are all held is left out even when unread rows behind them are not; the next wrap reads past them. Holds for
  liveness only because the cursor wraps.
- **`Err` only when nothing landed.** A message whose every subject is held is `Ok` (`:563`, no fault recorded), acked,
  resolves nothing (`:988`). With `more = false` that is correct and ends there. With `more = true` it is NEW2-M1.
- **The pin's shapes.** `d2_actor_a_fold2_m1_a_poisoned_subject_never_holds_a_shared_pair` (`tests/d2_actor_a.rs:682`)
  covers two poisoned subjects among six, both shared pairs, the hold, the sweep's exclusion and the lift. It has no
  shape past the 64 cap, which is where NEW2-M1 lives.

## 2. The rate (question 2, for the captain)

**The figure.** One serial consumer serves 3,600,000 / s messages an hour at s ms a message. L52's word frames alone
were about 8,000 an hour; every pot, party, filing, tip and identity event, each continuation and the sweep's
re-publishes ride the same lane. So the lane keeps up only if s stays under about 450 ms over the whole mix. Main's
`/internal/tx-word` handler took 2.4 to 3.1 s an ok call (L52 S2). The consumer's s is unmeasured; it ingests the
frame AND routes and judges its marks, so there is no reason to expect it below main's figure.

**What happens when it falls behind.**
- The frames are not lost: each is a queue message, retained by the platform (the default 4 days, far past a loop)
  and, past 4 deliveries, parked in `owed_event_dead_letters` by pair. But the word frames are now ingested ONLY by
  the consumer (`consume_one`, `:904`; they left the `/internal/tx-word` POST), so a backlog delays the TRACKER'S WORD
  WRITES themselves, fleet-wide, not just the owed judgement. `/tx-any` and `/utxo-status` serve the older word for as
  long as the backlog lasts. A frame not yet ingested has no mark, so the sweep cannot re-publish it.
- The marks are not lost: they stay in `owed_dirty`. The scheduled drain does NOT judge them while the binding is
  configured (`owed_belt.rs:957`), unlike main; only the consumer does. So the rows bar (design section 8, rows under
  1 s p95) fails by the backlog's age, and every press over a pending downgrade mark stays withheld at the read (the
  D2-L1 rule) for that time. A refund or sweep press that waits on a non-downgrade mark (a filing, a tip's gate) waits
  as long. No money moves wrongly; liveness only.
- The sweep amplifies: once the backlog is older than one tick (120 s), every pending pair it reads is re-published, up
  to 64 a tick, about 1,900 extra messages an hour. Each is cheap when the original was consumed first (an empty plan),
  but they queue ahead of new events.
- The write bar (300 ms p95 at the doors) holds: the doors publish and answer.

**What the window and L53 would read.** `/health.owedEvents.oldestEnqueueAgeMs` climbing for the loop's length,
`serviceMs` near the per-message figure, `unackedMarks` and `oldestUnackedMarkAgeMs` climbing, `sweptTotal` at 64 a
tick, `publishedTotal` running ahead of `consumedTotal`; the app layer's word-dependent cells (a SEEN or MINED word
served late) failing on time, not on value.

**Option 1: land with `max_concurrency = 1`** (correctness first; the rate is lane B's).
B1, B2, B4, B5 hold (the queue decides only WHEN a row is judged). B3 holds in money, not in liveness under load:
the honest seat's refund press, its sweep press and its served words lag by the backlog. M1's race stays closed for
the consumer (belt and backfill pre-existing). L53 on this lane reads a known bottleneck: the queue age climbing, the
rows bar failing, words late; it measures s, which is worth having, but it cannot pass the bar if s is above 450 ms.

**Option 2: land with a bounded concurrency (8) under a per-identity serial rule built now.** The rule the consumer
can keep today without the actor: a D1 lease per identity (`owed_belt_state` `owed-events:lease:<identity>`, taken by
compare-and-set before an identity's chunk, released in its judge's own batch, a TTL of twice the message wall); a
subject whose identity is leased is skipped with its mark kept and the message sent again with a short delay (a send
with `delaySeconds`, not a `retry`, so contention never spends `max_retries`). At 8 the lane serves 9,300 to 12,000
messages an hour at main's 2.4 to 3.1 s, past L52's rate. B1 to B5 hold only if the lease serialises every consumer
judge of an identity: a judge dropped by the wall can still land after its lease expired (#559's limit 5), which is
M1's stale class again, narrowed by the TTL, not closed; the actor closes it. It is a second serialiser to build,
pin, tier and then retire under lane B. L53 would then read the bar met if s is under about 3.6 s, and any lease
contention as `held` subjects.

**Recommendation.** Option 1, with two conditions: PR 184 lands at `max_concurrency = 1` for correctness, and L53 (and
the prod promotion) runs on it only once beta's `serviceMs` under a loop's mix is measured under about 400 ms, or lane B
has landed. If the captain needs L53 at L52's rate before lane B, take Option 2 as its own lane, not as a fold of this
PR. A cheaper middle step either way: move the word frames' INGEST off the serial lane (their own queue, any
concurrency: the tracker's write is per txid and needs no judge serialisation), publishing their marks' `word` pairs
onto `owed-events`; then a backlog delays the judge only, never the served words.

## 3. LOW-B, LOW-C, NOTE-3 and the schema (question 3)

- **LOW-B, folded.** `tip_reads_fresh` (`:884`) is `source == "tip"`, read at `:915`: both `tip`/`gate` and
  `tip`/`untipped` read the tip fresh; every other event keeps the 30 s memo, whose worst is a gate judged one block
  late until the next tip event, as the fold's comment states. Holds. Its pin is the pure function and a source shape;
  the fresh read itself is not executed natively (stated).
- **LOW-C, folded.** `redrivenAt` in the CREATE (`overlay-cloudflare/src/owed_events.rs:43`); the re-drive stamps it
  (`:551`); the lever returns a `redriven` row older than the hour to `parked` first (`:560`, `:666`) and re-drives it
  in the same call; `internal_discard` checks the bearer first (`dead_letters.rs:1697`), then routes
  `family: owed-events` (`:1702`) to `discard_dead_letters` (1 to 50 keys, `parked` or `exhausted` only, `:566`, marks
  untouched); the ownership manifest grants the overlay the DELETE. Holds. NOTE-2 for the re-drive count.
- **NOTE-3, folded.** The gate pin holds the publish's statements inside the first 473 migrations and lets the list
  grow (`tests/d2_actor_a.rs:596`). Holds.
- **The second in-place CREATE edit.** Safe for every deployed D1: migrations 470 to 473 are PR 184's own and shipped
  nowhere (beta's certified count 469 since record 1421, prod 409); the first delta lens's edge (a scratch or tier D1
  that ran an older 470 to 473 keeps the older table under `CREATE TABLE IF NOT EXISTS`) applies again, now also as a
  missing `redrivenAt` on the lever's UPDATE: tiers start fresh. The count stays 473; the fingerprint is
  1,823,919,051 (`tests/owed_derived_sqlite.rs:83`, the 469 prefix still 889,593,182). The app layer's gate reads the
  COUNT (`OWED_EVENTS_MIGRATION_COUNT` = 473, `ready_at`, `>=`), never the fingerprint; `promote-legs.py` O10 compares
  the count (`--expect-migrations 473`) and prints the fingerprint, which should read 1,823,919,051. Nothing in bsv-low
  pins the fingerprint.

## 4. The diff whole (question 4)

New served words on `/health.owedEvents`: `serviceMs`, `subjectFaults`, `subjectHeld`, `consumerWallDrops`,
`deadLettersStaleReturned`, `deadLettersDiscarded`; the consumer row gains `serviceMs`; the re-drive lever's answer
gains `staleReturned`; the discard lever answers `{family, discarded, notFound}`. Five new counters, seeded (overlay
`COUNTERS` 13 to 18) and read by `HEALTH_COUNTERS_SQL`. New memo rows `owed-events:fault:subject <identity>/<game>`.
New log lines: the wall's `passed the 60000 ms message wall`, the memo clear's fault, the stale return, `DISCARDED dead
letter`. `deltas` now drops zero counters before the bump (`:1027`). No trigger text changed, no migration number
moved, no render var or binding added. The test port's `fail_bind` is test-only. Nothing else.

**LOW-1 (the wall notes the pair, not the subject).** `:976`: a message past the 60 s wall notes the PAIR's memo and is
handed back in a fixed 60 s (`:980`), not after the memo's hold. A subject whose judge outlasts the wall every time
(a whale identity's slow judge) sits at the same place in its shared pair's plan, so every delivery stops at it: the
subjects after it are never judged (NEW-M1's class through the wall), the pair memo holds the pair from the sweep, and
the message dead-letters after four deliveries in about three minutes. Fold: note the memo of the subject in flight
when the wall falls (`judge_plan` records it before each `deliver`), and hand back with the memo's hold.

**LOW-2 (a memo outlives a landing outside the consumer).** A subject memo is cleared only when the consumer lands that
subject (`:553`); the pair memo only through `resolved_statements`. A subject later judged by the belt's find
(`judge_with_write`) or the backfill keeps its row with its old `faults`, so its next transient fault on the consumer
counts on from there and can hold it 6 h at once. No memo row ever expires. Fold: a memo whose `lastFaultAtMs` is older
than its own cap (6 h) past `heldUntilMs` counts from 0, or the judge's own batch deletes the subject's memo.

**LOW-3 (the rate, stated for the record).** Question 2: at s above about 450 ms the one serial lane falls behind
L52's rate and delays the served words, not only the judge. A design item for the captain, not a fold of this PR.

## 5. B1 to B5 for the honest seat (question 5)

No money moves on this path: the judge is `derive_owed_rows` over the same tables; the read is untouched; the press's
proof is the wallet's word and its pre-signed refund. B1, B2, B4, B5 untouched. B3 holds in money, delayed in liveness:
- a held subject: its own press waits out its hold (120 s to 6 h); its neighbours in a shared pair are judged, unless
  64 or more held subjects lead the pair (NEW2-M1: the neighbour waits for an event of its own identity);
- a lost message: the mark stays; the sweep re-publishes within a tick (an un-ingested word frame has no mark: its
  message is its only copy until it parks);
- a stale re-drive: returned to `parked` by the next lever call after the hour and re-driven; a second copy routes by
  the same marks, harmless;
- the serial rate (question 2): every press and word lags by the backlog.
The downgrade class still withholds a dead press at the read while its mark is pending, so a delay never serves a
press the evidence has withdrawn.

## 6. What the land must show (question 6)

The Linux tier at the land: `make ci-d1-budget` whole with the record-door cell on the one consumer (the doors under
300 ms, `consumedTotal` at or past 7, the planted un-acked mark re-published and acknowledged, `serviceMs` recorded);
the belt cells with `sweepHeld` 0 and `faultMemo.rows` 0; the dead-letter queue once (a judge failing four times parks
one row, the lever re-drives it, a lost re-drive returned after the hour, the discard lever takes a parked row). No
poisoned-subject tier cell exists in `tools/`; NEW2-M1's fold should add one (65 subjects on `age`/`age`, 64 of them
faulting, the 65th judged on the first delivery past the hold, `owed_events_continued_total` flat). Only beta can
show: `publishedTotal` beside `consumedTotal` across the two Workers, `serviceMs` and `oldestEnqueueAgeMs` under a
loop's mix (question 2's gate), the wall's runtime behaviour on wasm32, the fresh tip read, both levers' D1 calls, and
the overlay certified at 473 (fingerprint 1,823,919,051) BEFORE the app layer.

## Notes

- NOTE-1: the per-message subject memo read (`:468`) is one more D1 statement on every message of the serial lane,
  even when no memo exists; a fold could skip it unless the pair has faulted before.
- NOTE-2: the stale return keeps `redrives` and the re-drive does not check it (`:551`), so a letter whose re-drives
  keep getting lost is re-driven past `MAX_REDRIVES` (it becomes `exhausted` only at a park). Operator-driven and
  harmless (a re-drive routes by the marks).
- NOTE-3: a message dropped by the wall may still land its judge batch later (stated in the fold's comment); the
  generation-bounded ack makes that idempotent.
- NOTE-4: the first delta lens's NOTE-4 stands for the subject memo too: `note_fault_named` reads then writes, safe
  only under the single consumer; under Option 2 or lane B it needs one statement.

## What was run (at `7bb95ea2`, `CARGO_TARGET_DIR=/Volumes/Lanes/targets/d2-actor-a`, foreground)

- `cargo test --manifest-path crates/overlay-cloudflare/Cargo.toml`: lib 1,038 passed, 1 ignored; integration
  binaries 1 + 10 + 9 + 1 passed; 0 failed.
- `low-app-layer` lib, four filtered runs of its test binary: `owed` 113 (1 ignored); `--skip owed` with `logic::
  routes:: results:: identity::` 245 (2 ignored); `live_view:: record_post:: txany:: hops_view:: tracker_host:: auth::`
  169 (1 ignored); the rest 166: 693 passed, 4 ignored, 0 failed.
- `low-app-layer` integration: `d2_actor_a` 15, `owed_judge_sqlite` 15, `owed_derived_sqlite` 5,
  `owed_backfill_sqlite` 18, `owed_belt_sqlite` 7; all passed.
- `cargo clippy --all-targets -- -D warnings` and `--lib --target wasm32-unknown-unknown -- -D warnings` on both worker
  crates: exit 0, four times.
- The scratch NEW2-M1 run above (one test, printing, passed), removed with `git checkout`.
