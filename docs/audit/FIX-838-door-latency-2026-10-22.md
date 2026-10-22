# FIX-838: the gated door's answer under the fleet's load (loop 44)

bsv-low #838 with #521, #411, #488; Wave B row B-DOOR-1. Lane `lane/FIX-838`, base `00a5e613`. Written
2026-10-22 (UTC). The harness's 20 s settle is the door's contract and is not raised here.

## Sources

- The beta overlay's logs for the loop's window, `low-overlay-beta`, 2026-10-22T15:49:00Z to 17:19:00Z:
  59,921 events, 860 `POST /submit` (835 `broadcast-gated`, 25 `historical-tx-no-spv`), 10,735 `POST /lookup`,
  64 `GET /health/invariants` (`scripts/worker-logs.py --all --json`). Kept under
  `/Volumes/Lanes/out/wt-overlay-838/` with the derived tables.
- The harness archive `~/low-archive/vultr/full-2026-10-22_15-50-41` (33 pairs): the two red cells'
  `playwright-report.json` (pair 2 `spec-admit-fast-refusal`, pair 35 `spec-engine-door`) and every
  `[tlog] perf overlay.submit.gated` line (170 gated submits from 17 pairs; the other pairs logged none).
- Beta's D1 (`low-overlay-db-beta`, read only): table sizes and the plan and `rows_read` of one predicate.

Server-side step times are read from each request's own log lines, offset from the request's start
(the event's timestamp less its `wallTimeMs`). `wallTimeMs` itself includes the request's `wait_until` work and
is not the answer's time: the answer is the `POST /submit -> NNN` line.

## Finding 1: the two red cells timed out on `/health/invariants`, not on `/submit`

`admit-fast-refusal.spec.ts:62` and `engine-script-refusal.spec.ts:62` are the NAMES of a `settled(..)` over
`GET /health/invariants` (`counters()`, spec line 82): two answers that agree, 250 ms apart, inside 20 s. The
`SettleError`'s `last` response is a 200 carrying `d1;desc="reads=3789 writes=1 stmts=31 maxReads=805"` (pair 2)
and `reads=3785 writes=1 stmts=31 maxReads=807` (pair 35): the invariants read. The gated submits of the same
cell answered in 1,549 ms and 2,049 ms.

`/health/invariants` in the window (64 answers): wall p50 5,434 ms, p95 13,839 ms, max 20,832 ms, on 17 to 297 ms
of CPU. Its 31 statements were awaited one after another (`ops.rs:1749` on the base) and its own rows-read
counter was a WRITE awaited before the answer (`ops.rs:1982` on the base), so each answer paid the shared D1's
queue 32 times. Two such answers do not fit in 20 s once each passes 10 s.

## Finding 2: the gated door's answer, step by step (server-side, 694 answers with an answer line)

| step (from the request's log lines) | n | p50 ms | p95 ms | max ms |
|---|---|---|---|---|
| 1 parse + script door (start to the door line) | 585 | 189 | 789 | 2,917 |
| 2 the Arcade ladder (door line to Arcade's answer) | 507 | 98 | 227 | 1,344 |
| 3 the admission decision (Arcade's answer to the `admitted` line: `note_held`, the ledger, the latch) | 473 | 118 | 661 | 1,377 |
| 4 the engine's write and its predecessor reads, the write-side guard, the S2 decision (to the answer) | 520 | 2,566 | 12,903 | 23,116 |
| 4b the same, answered by the write-side guard (422/502, no topic line) | 10 | 5,819 | 15,382 | 15,382 |
| the answer (start to the answer line) | 694 | 2,781 | 14,649 | 42,118 |
| the dual push (#413), the pending watch, the corroborations, the fan-out | | after or beside the answer, in `wait_until` | | |

Answers: 601 x 200, 74 x 502, 13 x 400, 6 x 422. Past 10 s: 76 (11.0 %); past 12 s: 65 (9.4 %); past 20 s: 12. The
client's network leg (116 harness submits matched to their server answers by start time): p50 80 ms, p95 1,566 ms.

The S2 enqueue ran on no answer of the window (no "Phase-3 NOT DURABLE" or "replay QUEUED" line in the 59,921
events), and NL-6c deferred none ("deferred as" absent). The dual push and the pending watch were already in
`ctx.wait_until` (their lines interleave with, or follow, the answer). Nothing on the answer's path waited on a
network push.

Per pair (the harness's own figures, client-side; the client's `tlog` truncates `Server-Timing` after
`arcade-broadcast`, so the engine's segment is not in these lines; `arcade` 0 is a submit whose ladder did not
post, Arcade already holding the subject):

| pair | gated submits | total p50 ms | total p95 ms | total max ms | arcade p50 ms | arcade p95 ms |
|---|---|---|---|---|---|---|
| pair-1 | 1 | 6481 | 6481 | 6481 | 0 | 0 |
| pair-2 | 11 | 14800 | 24202 | 24202 | 71 | 91 |
| pair-3 | 11 | 9956 | 19826 | 19826 | 35 | 101 |
| pair-4 | 3 | 18527 | 18907 | 18907 | 81 | 81 |
| pair-5 | 2 | 4824 | 4887 | 4887 | 71 | 74 |
| pair-17 | 3 | 17487 | 17880 | 17880 | 62 | 70 |
| pair-18 | 3 | 16448 | 20168 | 20168 | 68 | 86 |
| pair-26 | 14 | 3137 | 17036 | 17555 | 71 | 187 |
| pair-27 | 15 | 10531 | 20130 | 21078 | 80 | 117 |
| pair-28 | 11 | 8672 | 25009 | 25009 | 0 | 97 |
| pair-29 | 14 | 7803 | 20678 | 20931 | 0 | 78 |
| pair-30 | 21 | 6092 | 25005 | 25006 | 0 | 85 |
| pair-31 | 14 | 6355 | 18595 | 18660 | 0 | 99 |
| pair-32 | 9 | 15303 | 20652 | 20652 | 59 | 143 |
| pair-33 | 11 | 8362 | 13314 | 13314 | 76 | 156 |
| pair-34 | 14 | 8088 | 25010 | 25013 | 0 | 89 |
| pair-35 | 13 | 11980 | 24471 | 24512 | 0 | 86 |
| all | 170 | 8334 | 24390 | 25013 | 81 | 143 |

Five of the 170 are the client's own 25 s timeout (pairs 28, 30 x2, 34 x2, all JOINs at 15:54:48Z to 15:54:59Z,
the fleet's burst).

## The serialization found

1. **The engine's write, one D1 statement after another, under one shared D1's queue** (`routes.rs:1770` on
   the base). The door's CPU is milliseconds; its answer waits on the statements of `Engine::submit` and its
   predecessor question, each queued behind the fleet's (the refusal cell's child A: 12 statements in 1,052 ms,
   about 88 ms each; a JOIN's write at the burst 16 to 23 s). Nothing bounded that wait: the answer was as late
   as D1 was busy, 42,118 ms at worst (a JOIN whose write outran its eviction, answered 422).
2. **The eviction's release of spend pointers walked every unconfirmed pot** (`admit_fast.rs:340` and its call
   at `admit_fast.rs:481` on the base, run inline by the write-side guard's re-evict at `routes.rs:1860`). The
   hop-sweep 422 of pair 1 (16:25:49Z, 6,481 ms) answered `reads=10968 writes=56 stmts=86 maxReads=7895`: its
   largest statement was `UPDATE pot_records SET .. WHERE spendingTxid = ? AND spentConfirmed = 0`, which SQLite
   planned on `idx_pot_records_confirmed_height (spentConfirmed=?)`, the index of its constant term. Measured on
   beta's D1: 7,804 of its 32,956 pots unconfirmed now; the predicate reads 7,805 rows as shipped and 0 with the
   term unindexed. Its SELECT sibling (`select_pot_spends_sql`) searched `idx_pot_spending` all along.
3. **`/health/invariants`'s 31 reads in sequence and its write before the answer** (Finding 1).

## The fixes

| fix | where | pin (RED first) |
|---|---|---|
| The release's guard term is unindexed (`AND +spentConfirmed = 0`, the house's idiom at `d1_discovery.rs:2324`): the UPDATE searches `idx_pot_spending` by the spender. 111,798 VM steps to 465 on the fleet fixture (22,268 unconfirmed pots). No migration. | `admit_fast.rs:343` | `d1_fleet_plans.rs:924` `fix838_the_eviction_release_searches_by_the_spender` (RED on the base: `SEARCH pot_records USING INDEX idx_pot_records_confirmed_height (spentConfirmed=?)`) |
| `/health/invariants` issues its 22 independent reads in ONE join (`concurrently!`, a no-dependency join, `concurrent.rs`), so an answer pays the queue's wait about once; its rows-read bump runs in `ctx.wait_until` after the answer. The body is built from the same values in the same order. | `ops.rs:1761`, `ops.rs:1981`, `concurrent.rs:55` | `ops.rs:2018` `fix838_the_invariants_reads_are_issued_together_and_its_write_is_after_the_answer` (RED on the base: no join); `concurrent.rs:110` (every future started before the first finishes) |
| THE ANSWER DEADLINE of the gated arm: the engine's write is raced against what is left of 12 s from the arm's start (`IN_REQUEST_ANSWER_MS`, `answer_ms_left`). Past it the request stops waiting, never the write: `within_deadline` hands the write back unfinished, the route gives it to `ctx.wait_until` (`finish_past_deadline`: the write, the E1D landing guard, the write-side guard's re-evict, the notes), then defers the submission as NL-6c does (the bytes at rest, the job, the same 202 and poll), its job's queue message delayed 45 s past the platform's 30 s end of that `wait_until`, the put itself capped at 4 s (past it, the deferral's own retryable 503). The race ends by 16 s from the arm's start (restated by the fold, below: that bounds the write's WAIT and the put, not the whole answer). | `routes.rs:911`, `routes.rs:1866`, `routes.rs:1874`, `routes.rs:1881`, `routes.rs:763`; `ef_deferred.rs:118`, `:123`, `:128`, `:148`, `:612`, `:792`; `lib.rs:351`, `lib.rs:1811` (the engine shared as an `Rc`) | `ef_deferred.rs:1239` (loop 44's 24.4 s JOIN on a virtual clock: answered at 16,000 ms, the write then finished with its own report at 24,400 ms), `:1277`, `:1307` (the `ceilings.json` row `doorAnswer` by name), `:1337` (the route's wiring). The API does not exist on the base; RED against two revert mutants: the deadline counted from the write's start (`left: 12000 right: 10300`) and the race taken off the gated arm ("only the gated arm in the request races"). |
| The budget tier holds `/health/invariants` by name: its D1 figures (writes 0 at the answer: a write put back before it reds) and the cells' settle shape, two answers 250 ms apart, against a wall-time ceiling. | `tools/lane-499/d1_budget_route_ci.mjs` leg 3b; `ceilings.json` `scenarios.health-invariants` | the tier: measured on the fixture (LANE_BASE 38380) reads 52, writes 0, stmts 29, maxReads 8, two answers in 325 ms; ceilings 78 / 0 / 31 / 12 and 5,000 ms by the file's margin rule |

No wire change: the deferral is NL-6c's 202 body byte for byte (the client's `steakAdmission` reads it as
`unreadable`, `ok` with `admitted: null`, never a re-present); its census reason is NL-6c's (`Queued`). No served
word is new. No migration.

## Stated, not fixed here

- The gated arm's pre-engine D1 reads (`note_held`, the ledger read, the `network_seen` latch, a readmission)
  are not under the deadline: they are awaited before it starts, p95 661 ms, max 1,377 ms in the window. A D1
  stall there delays the race's start; the deadline then answers at once (`answer_ms_left` is 0).
- On the deadline's DONE path the write-side guard's re-evict (and its release, now indexed) still runs inline
  before the 422, as before.
- `hand_markers` by `potTxid` is a SCAN (no index; 1,489 rows on beta): one COUNT per eviction, and a copy and a
  delete when it holds the pot. An index is a migration; not taken.
- A deferred submission's job re-runs the whole arm 45 s later (Arcade re-presented, already known; the engine's
  write a dedup when the request's landed), the D1 cost of a deferral. A request's write cut by the platform's
  30 s end of `wait_until` is finished by the job's run (every engine write is idempotent, #559).
- The gated arm is not driven by the budget tier: it needs Arcade, and its background jobs reach mainnet hosts
  the tier's loopback seal refuses. Its bound is pinned natively over a virtual clock and recorded in
  `ceilings.json` `doorAnswer` with loop 44's figures. Not deployed, not measured on beta after the change: the
  next loop is the measurement.
- Whether D1 runs one isolate's concurrent statements in parallel or in its queue's order is not documented; the
  join removes the request's own serial waits either way. Workers' limit of six simultaneous open connections
  does not name D1; not measured.
- #521's option (1) and #411 and #488 (the app layer's views and the client's fallbacks) are not this lane's
  change: #411's views and #488's client hold are the app layer's and the client's.

## The lens's folds (FIX-838-FOLD)

The lens (`docs/audit/FIX-838-lens-2026-10-22.md` in bsv-low) read the lane SOUND WITH FOLDS (0 HIGH, 2 MEDIUM, 3 LOW,
7 NOTE). Every fold below is pinned; the source pins (`src/fix838_fold.rs`) compile on 53119867 and are RED there by
their own assertion; the driven pins (`ef_deferred::fix838::fix838f_*`) use API the fold adds.

| finding | fold | pin (RED on 53119867) |
|---|---|---|
| MEDIUM-1: a past-deadline write that faults lost S2's replay (the job's run re-asks Arcade and can settle without replaying) | `finish_past_deadline` (`routes.rs:764`) is handed the bytes and the mode (`routes.rs:1989`) and, after the write-side guard, takes S2's decision on the write's own report (`routes.rs:884`, `ef_deferred::past_deadline_ack`, `ef_deferred.rs:182`, one derivation: `queue::mutation_ack`): not durable, the idempotent replay is enqueued marked `gated`, counted `submit_mutation_{fault,queued,refused}_total` as in the request; a replay the queue refuses, and a write the platform cuts (no report), are the deferred job's, put at rest before the 202 (a dropped isolate leaves it). A re-evicted subject enqueues nothing (the consumer would skip it). | `fix838f_m1_a_past_deadline_write_that_faults_enqueues_its_s2_replay` (RED: "MEDIUM-1: the past-deadline write's report takes S2's decision"); `fix838f_m1_a_past_deadline_report_that_is_not_durable_is_enqueued` (a real faulted `MutationReport` enqueued once, a durable one never, a refusal named) |
| MEDIUM-2: "the answer leaves by 16 s" is not a bound the route holds | RESTATED, not moved: the race covers the engine write's WAIT and the put (`raceBoundMs` 16,000, `ceilings.json` `doorAnswer`, which no longer states `ceilingMs`); the pre-engine steps (`preEngineInRequest`) and, on a write inside the deadline, every post-write statement awaited before the answer (`postWriteInRequest`, each named by its call) are outside it. A past-deadline answer leaves by 16 s from the arm's start when its pre-engine reads end inside 12 s (else those reads' time plus 4 s). The answer's whole time is the next loop's per-step table: the tier cannot drive the gated arm (Arcade, mainnet hosts), so the bound is pinned natively and by the source, not measured on the tier's fixture. | `fix838f_m2_the_rows_bound_is_what_the_route_holds` (every `crate::` call awaited in `submit_parts` between the write's outcome and the S2 decision, outside a `wait_until`, and the #413 belt's two reads, must be named in the row; RED: "MEDIUM-2: the row states no answer ceiling the route does not hold") |
| LOW-1: a write the platform cuts is never guarded | a RESUMED run (the job) refused by #612's stored echo under an OPEN eviction (`broadcaster::STORED_ECHO_UNDER_EVICTION`, `broadcaster.rs:1379`) reads the open row again and runs the guard's re-evict (`routes.rs:1880`), idempotent; the request's own arm is unchanged | `fix838f_l1_a_resumed_run_refused_by_the_stored_echo_re_evicts` (RED: "the stored echo's refusal has one name") |
| LOW-2: past the deadline a faulted door readmission was not retried | `finish_past_deadline` mirrors the request's arm (`routes.rs:813`): the refused memo, then the second `readmit_if_evicted` on a stale open row, or the re-evict on a fresh refusal | `fix838f_l2_a_faulted_door_readmission_is_retried_past_the_deadline` (RED: "LOW-2: the guard is not skipped on a faulted door readmission") |
| LOW-3: the deferral's line misnamed the past-deadline cause | `deferral_answer` takes a `DeferralCause` (`ef_deferred.rs:194`), named in its line (`cause ef-work` / `cause answer-deadline`); a past-deadline 202 bumps `submit_deferred_past_deadline_total` (`ops.rs:55`, seeded at 0) | `fix838f_l3_the_deferral_names_its_cause_and_is_counted_apart` (RED: "LOW-3: the past-deadline deferral names its cause"); `fix838f_l3_each_deferral_names_its_cause` |

The NOTEs, stated. NOTE-1: the route's wiring is pinned by source text, the race by the shipped helper on a virtual
clock (the gated arm is wasm only and needs Arcade). NOTE-2: the tier's `health-invariants` row reds the base by
`writes`, not by time (local D1 has no queue). NOTE-3: a past-deadline write's statements, its guard's and now its
S2 enqueue run outside the request's `Scoped` poll, on `unscoped`: the `/submit` route's D1 figures and budget
census no longer carry them. NOTE-4: the invariants' rows-read bump can be lost when the platform ends its task;
display only. NOTE-5: a re-presentation within 10 minutes of an open job sends no delayed message and runs its own
write beside the background one, #612's idempotent duplicate (one head); the fold's S2 replay is a third such
writer, the same class. NOTE-6: orphan objects a capped put leaves under `ef-deferred/` are swept by
`beef_blob_sweep`. NOTE-7: the overlay pins no toolchain; the gate of record is clippy 1.95.0.
