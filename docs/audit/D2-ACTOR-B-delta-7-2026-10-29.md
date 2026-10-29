# D2-ACTOR-B-DELTA-LENS-7: the delta lens on FIX-956 (PR 215) and D2-ACTOR-B-FOLD-11 (PR 216; bsv-low #954, #955, #956, #957)

Written 2026-10-29 by lane D2-ACTOR-B-DELTA-LENS-7 (Wave E, row E3). This is a review document only. No product code
changed. This PR is for the captain to copy and close; it is not to be merged.

What was reviewed: the combined delta `d28cc5b1b..f54826b0`, judged cold.
- `ad37a5cd8` is FIX-956 (PR 215). It landed as `287aa1728` (record 1511; `lands/overlay-215.log`).
- `f54826b0` is FOLD-11 (PR 216), rebased by the captain onto FIX-956 (record 1510). It was moved onto main as
  `fd57eb831`, the same tree. Its land was still running when this was written.

The diff covers 15 files under `crates/low-app-layer`, `crates/overlay-cloudflare`, `docs` and the manifest:
+2,371 / -205.

What it was read against:
- the sixth lens (`D2-ACTOR-B-delta-6-2026-10-29.md` in bsv-low);
- issues #956 and #957;
- both briefs and both REPORTs;
- the design, sections 3 to 7, and the cutover doc;
- records 1504 to 1511.

## VERDICT: LAND (0 HIGH, 1 new MEDIUM, 3 new LOW, 5 NOTE)

**FOLD-11 closes all four of the sixth lens's findings and #957.**
- NEW6-M1 is closed: the warm read reads every family over the games' whole key set. It is run green in both
  directions.
- NEW6-M2 is closed. A memo'd or over-bound message goes to the one-message arm, each message's fate is set by its own
  plan, and the wall notes the subjects that were in flight.
- NEW6-L1 is closed: each event is sampled by its own first apply.
- NEW6-L2 is closed. The stamp is now a judge-version constant, it was moved in this lane, and re-judge pages yield to
  cold pages.
- #957 is closed. The era is now one rule across the released-hop evidence road, and `tip` is a clock fact.

Neither judge can be shown to disagree with the other on the same inputs on this tip. Every road I read, and the two I
ran, give the same rows.

**FIX-956's horizon rule is sound.** It re-publishes only a mark the consumer has passed, or one past the long bound. A
mark whose message is in flight can be re-published only as a duplicate, and a lost mark waits at most the long bound.

**NEW7-M1: FIX-956's "at most once a pass" stamp is inert on D1.** D1 binds a `Bind::Int` as a JS number, so
`json_object('sweptAtMs',?2)` stores `...0`. The reader's `as_i64` then reads no stamp. This is the D1-REAL class
again: FOLD-7's `asOfMs`, FOLD-8's NEW4-M1. The native pins bind integers and stay green.

What it costs:
- On D1, every pair the horizon passed is re-published on every tick until its re-publish is consumed.
- A stalled consumer's pairs are re-published every tick instead of once per 600 s, which is five times as often.
- There is no money effect. The cost is a direct call and a queue message per re-publish, up to 64 a tick.

Fold M1 before the window's sweep figure is read as #956's closing read.

**Phase B gate.** Both judges agree on what this tip serves. Phase B may follow on this main once the window reads the
table at the end, M1 included.

| Item | Verdict | One line |
|---|---|---|
| 1 FIX-956's horizon | **NOT SOUND (the stamp, NEW7-M1); the rule SOUND** | `sweep_bound` (`owed_events.rs:111`) is one bind; the plan is a SEARCH on `idx_owed_dirty_backlog (markedAtMs>? AND markedAtMs<?)` (run). The horizon is the greatest publish `atMs` acked, never lowered (`:157`, pin `s956_f2`). In-flight re-publishes are duplicates only (NOTE-2). A lost mark waits at most 600 s plus a tick. The swept stamp never blocks on D1 (M1, run). `resolved_statements` carries FIX-956's two clears inside FOLD-11's own-plan resolve. The day's prune scans its range (L1). |
| 2 Lost notes; Phase C ack | **SOUND** | The overlay's one INSERT (`owed_events.rs:185`) is granted, and the ownership check passes (1,833 references). A quiet isolate's wall-dropped note waits for its next publish or the long bound; that is stated in the code and in CLAUDE.md. Phase C's ack (`owed_actor.rs:4470`, called from `owed_events.rs:2426`) covers only `appliedGames`, up to the generation read before the call. Generations are AUTOINCREMENT in commit order, so a mark written during the call has a higher one and stands. Nothing is acked in Phases A and B (`acks()` is false while the flag is unset), on a wall, on a fault, or on a held answer. Counters and health hold in both Workers. |
| 3 NEW6-M1 | **FIXED** | The narrowed read takes `keys.txids` whole (`owed_actor.rs:1285`), and `fetched` is the whole key set. That reaches the opponent's party rows by `potTxid`, the reorg clear's `hop_chain_probes` by hop txid, the proof flips of `transactions`, and the evicted twins. `fold11_a` and `fold11_b` are green. The record still takes 2 statements and the statement text is still 10,488 bytes, but the rows read are now the games' whole closure again (NOTE-5). |
| 4 NEW6-M2 | **FIXED** | `batch_core` (`owed_events.rs:1582`) sends a message with a memo (any memo, hold or not) to its own arm, caps the pass at one message's bound, sets each fate by its own plan (a message whose every subject was held resolves nothing), and runs `wall_fate` on both arms. `fold11_c` and `fold11_d` are green. A wall still costs every batched message one delivery (NOTE-1). |
| 5 L1, L2 | **FIXED (L1 over-samples, NEW7-L3)** | L1 samples each event by its own first apply (`object.rs:893`). The faulted tail is no longer under-sampled, but one `also` serves the whole batch across identities (L3). L2: `JUDGE_VERSION` (`owed_actor.rs:3399`) is moved by this lane, re-judges are capped at 30 pages a minute, and their slots fall in the second half of the minute. `fold11_e` and `fold11_f` are green. |
| 6 #957 | **FIXED (D1 was right; NEW7-L2)** | D1 was right. Beta's cutoff is 2026-10-26 15:22:33Z (`WRITTEN_OFF_BEFORE_MS = 1793028153000`) and the hop's marker is 2026-10-21, so that hop is old-era: no Collect. `before_era` (`rank.rs:32`) now gates the evidence road (`hydrate.rs:313`, `:372`). Both judges read `era_ms` from `written_off_before_ms_env`. `fold11_g` writes the hop off in both judges. Why D1 held no row on beta is not established (not run against beta). Dropping `tip` hides no verdict: `claimable`, `blocksToGate`, `tipUnknown` and `chainWait` are still compared. It does hide a stale judged tip (L2). `fold11_h` is green. |
| 7 Together | **CONSISTENT** | `schema.rs` is untouched and `ACTOR_TABLES_VERSION` is unchanged (`applied-per-game-1`), so the digest has not moved. The counter lists match in both Workers: `owed_events` 25 and the actor 28 against the overlay's `OWED_ACTOR` 28. `resolved_statements` carries both lanes' clears. The consumer row carries `horizonMs` beside the batch's fields, and the coalesced arm's `Fate::Ack` updates the horizon (`owed_events.rs:1873`, `:2004`). The window table is amended below. |

## How this was verified

**Build.** Worktree `~/bsv/wt-overlay-actor-b-lens` at `f54826b0`, `CARGO_TARGET_DIR=/Volumes/Lanes/targets/actor-b-lens`.
Evidence is in `/Volumes/Lanes/out/wt-overlay-actor-b-lens/delta7/`.
- `--lib --no-run` and `--test d2_actor_a --no-run`: ok.
- `s956`: 10 passed. `fold11` (lib): 6 passed. `d2_actor_a fold11`: 2 passed.
- `-- owed` whole: 225 passed, 1 ignored, in 316 s.
- `d2_actor_a` whole: 25 passed.
- `d2_actor_e_` with `--nocapture`: 8 passed. A warm apply is 2 record statements, 1 round, 0 misses.
- `scripts/check-storage-ownership.py`: ok.

**Plans and VM steps** (sqlite3 over the shipped statements, `plan.txt`):
- the sweep's walk is an index range;
- `LOST_READ_SQL` is a primary-key range plus `USE TEMP B-TREE FOR ORDER BY`: 40,614 VM steps over 5,000 notes;
- `SWEPT_PRUNE_SQL`: 100,023 VM steps over 20,000 young stamps, none prunable.

**Scratch pin** (appended to `owed_events/tests.rs`, run, then removed; output in `lens7-scratch-swept-stamp.txt`).
It is `s956_b`'s shape, with the stamp re-stored as D1 stores a JS-number bind.
- The stored row reads `{"sweptAtMs":1800000240001.0}`.
- 3 re-publishes in the three ticks the stamp should block.
- 10 re-publishes in ten stalled ticks, where the rule allows 2.
- The pin FAILS (`left: 3, right: 0`).

**Not run:**
- the other lib pieces (`lb`, `tracker`, the rest);
- the integration binaries other than `d2_actor_a`;
- clippy;
- the budget tier and the workerd cells;
- the base build for the lanes' RED claims (taken as each lane states them);
- anything on wasm32, workerd's D1 or beta.

M1's REAL bind is asserted from the D1 binding's code (`routes.rs:3170`, `Bind::Int(n) => JsValue::from_f64`) and the
fleet's earlier measurement (FOLD-7 and FOLD-8). It was not re-measured on workerd here.

## New findings

### NEW7-M1 (MEDIUM, fold before the sweep figure is read as #956's close): the swept stamp is stored as a REAL on D1 and never blocks

**Where:**
- `crates/low-app-layer/src/owed_events.rs:96` (`SWEPT_MEMO_WRITE_SQL`: `json_object('sweptAtMs',?2)`);
- `:1087` (`swept_at`: `c["sweptAtMs"].as_i64()`);
- `routes.rs:3170` (the D1 port binds every `Bind::Int` as `JsValue::from_f64`).

**How it fails.** On D1 the stamp is `{"sweptAtMs":1793….0}`. serde reads a float, `as_i64` answers `None`, and
`swept_blocks` is never asked. So `out.once` is always 0. The two cases:
- **A passed pair whose re-publish is still queued** is re-published on every tick until that message is consumed.
  Before FIX-956 that was the pattern on every tick-old mark; now it is narrowed to the passed set.
- **A stalled consumer's pairs** are re-published every 120 s, not every 600 s, up to 64 a tick. Each one is a direct
  call and a queue message.

The other FIX-956 values are not affected:
- `horizonMs` is written in serde-built JSON text, and `horizon_of` falls back to `as_f64`.
- The lost notes' cursor is serde text.

**Fold (one line):** write `json_object('sweptAtMs',CAST(?2 AS INTEGER))` and read the stamp as `horizon_of` reads the
horizon (an `as_f64` fallback).

**Pin:** `s956_b` with the stamp re-stored as REAL (the scratch above), RED on `f54826b0` with `left: 3 right: 0`.

### NEW7-L1 (LOW): the sweep's two new range reads scan their whole range every tick

**Where:** `owed_events.rs:103` (`LOST_READ_SQL`) and `:99` (`SWEPT_PRUNE_SQL`).

**What happens.** `owed_belt_state` has only its `name` key. So each tick:
- the lost read reads every note in its range, to sort by `tickedAtMs`;
- the prune reads every stamp, to find 64 that are a day old.

In steady state both ranges are nearly empty: the landed judge clears the stamps, and the tick clears the notes. In a
Queues or consumer outage they grow with the backlog's distinct pairs. Each tick reads them all: 100,023 VM steps for
20,000 stamps, against prod's 20 M rows a day.

**Also.** Lost notes held by a fault memo keep their places among the 32 a tick. Newer notes then fall to the long
bound, which is a delay, not a loss.

**Fold (one line):** key the stamps and notes by time, for example `owed-events:lost:<tickedAtMs> <pair>`, or walk a
cursor over the range, so each tick reads at most what it takes.

### NEW7-L2 (LOW): the compare no longer sees a judged-tip difference, and the read degrades by that tip

**What changed.** `tip` joined `CLOCK_FACTS` (`owed_actor.rs:2745`). The stored `probeTip` column was never compared
(`diff_rows`). Every verdict the tip moves is still compared: `claimable`, `blocksToGate`, `tipUnknown`, and the
end-of-derive `chainWait`. So no money verdict is hidden.

**What is hidden.** An actor whose judged tip lags D1's now compares equal. Yet the read's #669 degrade
(`degrade_stale_sweeps` against `probeTip`) serves a courier-dependent sweep withheld from that actor where D1's fresher
row presses it. This is the safe direction, the sixth lens's NOTE-1 made unobservable. The probeTip floor is still not
built.

**Fold (one line):** count a row pair that differs only in `tip` or `probeTip` apart, as `heldDiffs` is counted (a
lag, not a STOP), instead of dropping it.

### NEW7-L3 (LOW): one coalesced `EventCtx` serves every identity in the batch, so its events are sampled once per identity

**Where:** `coalesced_event` (`owed_events.rs:1462`) builds ONE context, with `also` set to every other event of the
batch. Every identity's apply in the pass carries it (`D1Judge::with_event`). The object then runs `first_apply_of` for
each `also` event (`object.rs:893`).

**The effect.** An event that names another identity was never applied in this actor, so it reads as a first apply. It
is sampled here as warm and recorded in this actor's `applied`. A batch of k identities samples each event up to k
times. The direction is over-sampling, weighted by identity count, not the sixth lens's under-sampling, so the
backstop bar cannot read green over a red tail on this account. It can read red over one slow batch.

This was read, not run.

**Fold (one line):** give each identity's apply only the `also` events whose plan names that identity.

## NOTEs

**NOTE-1: a batch wall hands back every message.** At the batch's wall:
- a taken message without an in-flight subject gets `Retry { delay_s: None }`;
- an `Alone` message is handed back unjudged (`walled` drains `singles`).

Each of those spends one of the three retries. The next delivery leaves the noted subject's messages to their own arm
(any memo makes a message `Alone`), so an innocent message loses one attempt per distinct slow subject it is batched
with. Under a slow D1 an innocent message can reach the DLQ. Its marks are then the sweep's, so nothing is lost.

**NOTE-2: the horizon's duplicates.** A mark's message can be re-published while still queued in three cases:
- a faulted message retried behind a later acked one (the queue has no retry delay, and the 120 s margin covers only a
  short backlog);
- a continuation event, which carries no stamp;
- every mark once the consumer's lag passes 600 s.

Each case is a duplicate, deduplicated by the seq check. The horizon is the fleet's maximum, so with
`OWED_CONSUMER_CONCURRENCY` above 1 (Phase C) one consumer's acks pass another's in-flight batch. The cutover doc
should say so. A future-skewed `atMs` would raise the horizon for good: back to pre-FIX re-publishing, never a loss.

**NOTE-3: #957's two fallbacks.** The evidence road dates a released hop by its own `pot_records` row and falls back to
`evictedAt` only when that row is absent. The could-not-judge road (`MISSING_SQL`) dates by `evictedAt` alone, so an
old-era hop evicted in the new era keeps its could-not-judge row. That row is display only (`claimable: false`, no
Collect). Both judges read it alike.

**NOTE-4: `JUDGE_VERSION` is moved by hand.** If a later judgement change forgets to move it, every bootstrapped actor
keeps the old judgement. The only belts are the compare (one event in eight) and the reconcile (64 identities a day).

**NOTE-5: M1's cost.** The warm apply is still 2 record statements (pin `e_a`: 1 round, 0 misses). The rows it reads
from D1 are now the games' whole closure, the pre-E full read's rows, not the event's own. #954's apply wall (629 ms
p95 on beta, record 1506) is round trips, which M1 does not add. The rows-read figure was not measured on D1.

## What a further fold must change (one line each)

- **NEW7-M1:** `CAST(?2 AS INTEGER)` in `SWEPT_MEMO_WRITE_SQL`, and an `as_f64` fallback where the stamp is read. Pin:
  `s956_b` over a REAL-stored stamp.
- **NEW7-L1:** the lost read and the prune read at most what they take (a time-keyed name, or a cursor).
- **NEW7-L2:** count a tip-only difference apart instead of dropping it, and build NOTE-1's probeTip floor.
- **NEW7-L3:** each identity's apply carries only the `also` events naming it.

## What the window on this main must read before Phase B

The sixth lens's rows are kept. The amended rows are marked.

| Bar | Cell or read | Figure |
|---|---|---|
| Compare (amended) | `diffsTotal` against `heldDiffsTotal`, `comparedTotal`, `reconcileDiffs`; the operator route `GET /internal/owed-actor-rows?identity=02b2f90e11a3af178c38f303f01dd93debfb2fc76e52401155eb735dde98eadacb` on the fresh bootstrap | `diff` empty for 02b2f90e…: `80d96c7a…:0` absent on both sides (old era); `329dba34…:0` equal (`tip` is a clock fact now). `diffsTotal` flat. Any diff now is a STOP: no unmarked-writer class remains. A tip-only lag no longer shows (NEW7-L2). |
| Sweep (new, #956) | `/health/invariants.owedEvents` `sweptTotal`, `sweepPassed`, `sweepStalled`, `sweepLost`, `lostNoted`; `/health.owedEvents.horizonAgeMs` | Near zero a tick on a quiet beta. `horizonAgeMs` grows on a quiet beta, which is normal. Until NEW7-M1 is folded, a `sweepStalled` rising every tick on one pair is M1, not the consumer. |
| Phase C ack (new) | `directAcked` | **0** in Phases A and B. Any other value is a defect. |
| Apply, warm hand | `applyMsP95` on pair 20 | under 300 ms (d28cc5b1b: 629). M1 adds rows read, not round trips. |
| First visibility | `rowsVisibleMsP95` with the fleet bootstrapped first (stage 2c) | under 1 s |
| Backstop (amended) | `backstopLagMsP95` beside `direct.faults` and `consumedTotal` | under 60 s. It can now read high over one slow multi-identity batch (NEW7-L3), but never low over a red tail. |
| Direct walls | `direct {calls, walls, faults}` | walls are the hands' own, near 0 |
| Coalescing (amended) | `owed_events_consumer_wall_drops_total`, `owed_events_dlq_total` | wall drops 0. A wall now notes its subjects and holds them (NEW6-M2 folded). A DLQ rise beside a wall is NOTE-1. |
| Re-judge (amended) | `/health.owedActor.rejudged` after the deploy | equal to the bootstrapped fleet once: the judge version moved in this tip. On the NEXT deploy with the same `JUDGE_VERSION` it stays flat. |
| Cold rehearsal | `coldMsP95` for a 600-game seat | under 60 s, read while the fleet re-judges (cold pages first) |
| Evidence | `hop_chain_probes` `<txid>:<vout>` with `spentConfirmed` NULL | each one dated, none standing past 6 h without a look |
