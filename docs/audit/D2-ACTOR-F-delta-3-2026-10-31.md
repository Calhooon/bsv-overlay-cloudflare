VERDICT: NOT LAND

# D2-ACTOR-F third delta lens: overlay PR 250 after fold 3 (a0834e27 → 44e2f992), bsv-low #959 and #954

**What was read.**
- The second delta lens, in full.
- `git diff a0834e27...HEAD`, file by file (9 files, +1,218 / -117).
- The carried path as it now stands:
  - `owed_events::direct_with_class` and `WorkerDirect`;
  - `owed_actor::{closure_wanted, pass_closure_select, closure_arm, closure_for, read_pass_closure, closure_round, ClosureRead::merge, txids_in_row, KEY_COLUMNS}`;
  - the object's answer (`object.rs:1010-1035`);
  - `PendingBars::note_answer` and `health_body`.
- PR 255's diff (`lane/FIX-1018-BELT-DATE`, `51a6bfa7`) and PR 254's merged diff.
- The fold brief and its evidence: `/Volumes/Lanes/out/bsv-low/D2-ACTOR-F-FOLD-3/`.

**What was run** (a private target dir, Studio load 16 to 24):
- The whole `owed_actor` piece of `-p low-app-layer --lib` in two pieces:
  - `owed_actor::tests::d2_actor`: 103 passed in 296 s;
  - the rest of `owed_actor`: 92 passed in 357 s;
  - that is all 195 green.
- `d2_actor_f_fold3`, `fold7_c`, `fold10_j` and `fold14_c`: 10 passed.
- A throwaway probe of the closure statement's length, deleted afterwards. The worktree is clean.

**What was not run.** The rest of the lib, the integration binaries, clippy, `f_cell`, and the budget tier.

**Verdict.** The second delta lens's two MEDIUMs are closed as ruled:
- A closure is read only for a filing that names a key beyond its own.
- The pass makes one statement, of 32 keys at most.
- The sweep reads none.
- The stage and the counter are served.

Two LOWs remain:
1. The one statement runs in sequence before every call of the pass, with no wall.
2. The closure stage's served path is proven only through a test double, and a faulted closure read is silent.

Neither affects correctness.

## Findings

### LOW-1: the pass's closure statement runs before ALL the pass's calls, with no wall, so every event coalesced with a filing waits one D1 round trip it does not need

**Where.** `crates/low-app-layer/src/owed_events.rs:2927-2950`. `port.closure_read(stmt).await` runs after the per-event loop and before `coalesce_direct`/`join_all` (`:2986`). It is raced against nothing. `DIRECT_CALL_WALL_MS` bounds each actor call, not this read.

**Why it matters.**
- The ruling said the statement is "concurrent with nothing else of the pass and never in sequence before the calls". The fold reads it once (the per-event sequence is gone), but it still precedes every body, including bodies that want no closure.
- The overlay publishes `party` itself (`d1_discovery.rs:5922`, `:5988`). An overlay flush (`POST /internal/owed-direct`, up to 64 events) that carries one JOIN's party filing beside SEENs and words for other seats makes every one of those warm calls wait on the filing's closure.

**Scenario.** Under #959's flood (D1 read p95 719 ms):
- a warm SEEN coalesced with a party filing becomes visible one round trip (about 700 ms) later than it would alone;
- a D1 stall on the 54 KB statement holds every body of the pass until the platform's 30 s `wait_until`, with nothing logged.

**Pre-existing, stated.** The loop already reads a non-carried event's marks (`port.subjects`) one event at a time before any call. The closure adds one more round trip to that prelude.

**Fix.**
1. Start the closure read concurrently with the calls of the bodies that want none: split `bodies` by whether any of their events had `closure_wanted`, and `join` the closure-then-filings future with `join_all` of the rest.
2. Race the read against a bound, for example `DIRECT_CALL_WALL_MS`. On the bound, go on without closures; each actor reads its own miss once, as with a fault.
3. Pin: a pass of one filing and one warm SEEN, with the closure port never answering. The SEEN's call is made and answered.

### LOW-2: the closure stage is served only by a test double, and a faulted or declined closure read is invisible

**Where.**
- `object.rs:1020-1022` echoes `closureMs` into the object's answer before its own `note_answer_as`. That code is wasm-only and runs in no native pin.
- `fold3_medium1_b`'s port re-implements the echo (`tests.rs:15787-15789`).
- `fold3_nit3`'s stage check hands `note_answer` a literal answer (`tests.rs:15957-15961`).
- `f_cell` (`tools/lane-912/owed_carried_flood.mjs`) was not run on this tip (the PR body says so). It asserts `readMsP95` and `judgeMsP95`, never `closureMsP95` or `directClosures`.
- In `direct_with_class`, a closure read that answers `None` is dropped silently (`read_pass_closure`'s `.ok()?`, `owed_events.rs:2939-2948`):
  - no counter;
  - no log line (`log_direct_fault` reads only call faults);
  - its time goes to `report.closure_ms`, which nothing serves or logs.
- A closure that `closure_for` declines (past 32 KB) likewise carries no `closureMs`.

**Scenario.** On beta, the 54 KB statement starts failing for every pass, for example from a D1 timeout, or a family added that pushes it past D1's limit (NIT-1). Then:
- every filing falls back to the actor's own miss read, as designed;
- the window reads `directClosures` flat and `closureMsP95` null;
- that is indistinguishable from "no filing was made", and the 1,000 ms visibility bar is judged on a path the window cannot see.

**Fix.**
1. Count `owed_actor_direct_closure_faults_total` (and declines) and log the first fault once a minute, beside `log_direct_fault`.
2. Make `f_cell` assert `classes.direct.closureMsP95` non-null and `directClosures == 32` after the bars' minute.
3. Run `f_cell` on the tip before the land.

### NIT-1: the one statement is 54,152 bytes, unpinned against D1's 100,000-byte statement limit

**Measured.** `pass_closure_select()` is 54,152 bytes, with 5 binds, at 32 keys (measured on this tip). `closure_arm` interpolates each family's weight expression three times (`owed_actor.rs`, the `format!` in `closure_arm`).

**Risk.** A family or column added to `FAMILIES` grows the statement toward D1's limit. Rusqlite does not enforce that limit, so every native pin would stay green while D1 refuses the statement, and that refusal is silent (LOW-2).

**Fix.** Pin `pass_closure_select().len() < 90_000`, or compute the weight once per arm with a sub-select.

### NIT-2: a family's running bound (1 MB) is far above what a closure keeps (32 KB)

**Where.** `CLOSURE_FAMILY_MAX_BYTES = 1_000_000` applies per family, over 24 families. The `pot_beefs` and `transactions` arms weigh hex at 2 per byte. So one answer row can carry several MB that `closure_for` then drops at `CLOSURE_MAX_BYTES` (32 KB per event).

**Impact.** Bounded and correct, but on a pass whose keys name large rows it is wasted D1 egress and decode.

**Fix.** Bound each family nearer the kept size, for example 32 KB times the pass's filings, capped at 1 MB.

### NIT-3: leaving the apply's own game ids out of the judge's bind keys narrows the safety net

**Where.** `owed_actor.rs:2304-2309`.

**What changed.** Since the key-column inversion, a bound txid is the walk's fallback for any key a non-key column names. A txid that the judge binds, that no key column names, and that equals one of the apply's own game ids is no longer fetched.

**Status.** Unreached: no judge statement binds a game id in a txid position, and game ids are `Key::Game`. But the exception is not stated where the rule is stated.

**Fix.** State it at `txids_in_row`/`KEY_COLUMNS`, or pin that no judge statement binds one of `games` against a txid column.

### NIT-4: doc and PR-body claims drift from the evidence

**CLAUDE.md** gives the native flood as three runs:
- closure p50 2.3 to 2.6 ms, p95 3.0 to 3.1;
- apply p95 7.5 to 7.8;
- first visibility p95 10.1 to 10.9.

**The evidence file** `fold3_flood-release.txt` holds three runs, one of them noisy:
- closure p50 2.6 to 2.8, p95 2.9 to 6.9;
- apply p95 7.4 to 34.3;
- first visibility p95 10.3 to 40.8.

**The PR body** says "four runs, one noisy". It also:
- says "Four folds are in this PR" but lists three;
- still reads "Based on main `99e456a6`", while main is `a72adab0` (PR 254 merged).

**Fix.** Quote the evidence file's runs in CLAUDE.md, and correct the body's fold count and base.

## The five answers

### 1. The closure's shape as ruled

**The predicate.** `owed_actor::closure_wanted` holds when:
- the source is in `CLOSURE_FILING_SOURCES` (`party`, `collected`, `result`, `sweep`, `refund`), AND
- `closure_keys(payload)` names a txid other than the ones in the event's own `source_key`.

A SEEN, a word, a refusal, a `pot`, `hop`, `eviction` or `beef` event, and every own source read none.

**Where it runs.** In `direct_with_class` (`owed_events.rs:2927`), after every event's subjects and before `coalesce_direct` and all calls. It is ONE statement and one round trip:
- `pass_closure_select`: `WITH k AS MATERIALIZED`, the `p` prefixes, `HW_COLUMN`, then one scalar `closure_arm` subquery per family, with no compound SELECT;
- it is not concurrent with the calls, and not under a wall (LOW-1).

**The key cap.**
- `pass_closure_statement` takes the first 32 keys of the union (`BTreeSet` order, so lexicographic, not per filing).
- `closure_for` puts an event's unread keys in `notCarried`, and its rows past a weight bound there too (never an empty row).
- `ClosureRead::rows_of` declines any miss naming a not-carried key. That miss reads the record once in the actor.

**A warm SEEN, word or refusal reads nothing.** `fold3_medium1_a` counts the record port's calls over the whole pass (subjects, closure, actor apply): 0 for each. Green, run here.

**The sweep reads nothing.** `if class != SWEEP_CLASS` (`:2927`). `fold3_medium2` covers it: 64 carried filings swept, 0 closure statements, drain 148 statements. Green, run here.

**The stage.** Served as `classes.<c>.closureMsP95` and `stages.closureMsP95` (`APPLY_STAGES`' sixth ring). It is sampled only when an answer carries `closureMs`; an absent field adds no sample (`note_answer`, `owed_actor.rs:4891`), so non-filings do not dilute it.

Two caveats for the window:
- Under a pass's fan-out of `FLOOD_CONCURRENCY` (8) or more, the sample goes to `classes.flood`. `stages.closureMsP95` (direct plus queue only) then does not show it.
- "Non-null on the real path" is not verified: the object's echo is wasm-only and `f_cell` was not run (LOW-2).

**The counter.** `owed_actor_direct_closures_total` is bumped per filing given a closure and served at `/health.owedActor.direct.closures` and `/health/invariants.owedActor.directClosures`.

### 2. The flood figures, translated to D1

**What the native numbers measure.** The native microseconds and milliseconds are the Studio's SQLite. The closure's native 2.6 ms is mostly preparing a 54 KB statement.

**A NEW filing on D1.** Its write-to-visibility path is:
1. the filing's own write and the publish's batch (`SEQ_BUMP_SQL` plus the carrier's `read_sql`), before the answer;
2. after the answer, the pass's closure statement: one D1 round trip;
3. the actor's Durable Object call, whose warm apply reads no D1 when the closure is taken (`recordCalls 0`, `closureRounds 1`);
4. the push through the gate (a DO subrequest, then the relay). This is outside the bars.

That is the same D1 round-trip count as main (door batch, then the actor's one compound read). What changed:
- the second round trip now sits before the actor's turn instead of inside it;
- it is shared by a pass's filings;
- it is no longer charged to `readMs`.

**A warm SEEN, word or refusal** is the door's batch alone (main: two). This is the real gain, unless LOW-1's coalescing puts it behind a filing's closure.

**Quiet beta (D1 20 to 50 ms per round trip).**
- First visibility of a new filing: about 2 to 3 round trips plus the warm apply's judge and write. The native apply was 6 to 7 ms; beta's direct judge has measured about 100 ms of CPU in the wasm Phase C window (FIX-983).
- So about 150 to 350 ms, inside the 1,000 ms bar.
- The 300 ms apply bar excludes the closure (`applyMs` starts after it), so it is reachable if the outside stage (CHAINTRACKS) stays short.

**Under #959's flood.** 32 door requests are 32 passes (fan-out 1, class `direct`), so 32 closure statements serialised on one D1. Expect:
- `closureMsP95` to show what #959 showed as `readMsP95`, about 700 ms;
- `readMsP95` to show 0;
- first visibility p95 of about 700 ms + the door's enqueue + the apply. The 1,000 ms bar is marginal there.

**The figures the window must read**, from `/health.owedActor`:
- `classes.direct.closureMsP95` (and `classes.flood.closureMsP95` if any pass fanned out to 8 or more);
- `classes.direct.readMsP95`, expected 0 for carried applies;
- `applyMsP95`, at most 300;
- `rowsVisibleDirectMsP95` and `rowsVisibleMsP95`, at most 1,000;
- `direct.closures` against the filings made;
- the door's `Server-Timing enqueue`.

If `direct.closures` is flat while filings are made, the closure path is not running (LOW-2), and the visibility figure measures the fallback.

### 3. The three red-in-piece tests

**Which three, and why.** `fold7_c`, `fold10_j` and `fold14_c`. They were not flaky. Each was a real failure that the fold's own changes caused, and each was fixed by amending the test (PR body, "Amended"):
- `d2_actor_b_fold7_c`'s growing chain named its next txid in `tx_words.hint_source`, which is no key since the inversion. Moved to `hop_chain_probes.spendingTxid`.
- `fold10_j` holds the counter list's length, which went from 32 to 33.
- `fold14_c` asserts that every stage sums to `applyMs`. `closureMs` is the caller's stage, outside `applyMs`, so it is now skipped (`tests.rs:8917-8919`).

**What "green alone" meant.** It was the amended tests run alone after the fix. The piece was not re-run on the tip.

**Re-run here.** I ran the whole `owed_actor` set in two pieces at load 16 to 24: all 195 passed (296 s + 357 s). That shows no order dependence and no load flake. No test fault needs fixing before the land. The piece only needs splitting under the 600 s cap.

### 4. The rebase ahead

**The brief's premise.** `origin/main` (`a72adab0`) carries PR 254 (#1016) only. PR 255 (#1018, `lane/FIX-1018-BELT-DATE` at `51a6bfa7`) and PR 253 are still OPEN.

**Against main.** `git merge-tree --write-tree HEAD origin/main` is clean. PR 254 touches `admit_fast.rs`, `tests/fix1016.rs` and CLAUDE.md, none of the carried path.

**Against PR 255.**
- Textual conflicts in `object.rs` (both add a field to `ApplyBody` and to the alarm's body literal), `owed_actor/tests.rs` and CLAUDE.md.
- `owed_actor.rs` itself merges cleanly (255 touches only `ASK_PUT_SQL`).

**In substance, there is no conflict on the belt date.**
- 255's rule lives in `owed_judge::row_statements` and `kept_belt_due`. It reads the STORED `owed_rows.beltDueAtMs` (`STORED_BELT_DUE_SQL`) and keeps the earlier date. Its ledger rule is in `ASK_PUT_SQL`.
- A carried item can only write a mirror family: `carried::item_statements` refuses any table not in `FAMILIES` (`carried.rs:287-292`).
- `owed_rows` is the judge's own table, never a family. So a carried upsert never writes `beltDueAtMs`, and the carried apply's judge goes through the same `row_statements`, which applies 255's keep.
- `beltAsked` reaches the actor only via `D1Judge::actor_apply` (the belt's hand-back). The direct pass's carried bodies carry none, so they date nothing fresh. That is correct: a writer's event is no ask.

**One interaction to re-prove after the rebase.**
- 255 adds `STORED_BELT_DUE_SQL` to the marks' batch in `judge_with_write_bounded`. In the actor, that read runs under `Noting`, and its answer column `outpoint` is a `KEY_COLUMNS` entry since FOLD-3.
- The outpoints are of the judged games' own stored rows. Their txids should already be in the warm mirror's closure (it starts from the last judgement's txids), so no miss round should follow.
- Re-run `d2_actor_f_a`, `fold3_medium1_a`/`b`, `fold_l2_a`, `fold3_nit1` and `f_cell` on the rebased tip. A spurious miss would show as `recordCalls 1` there.

### 5. Everything else

**The generation high-water and the closed flag** are unchanged from fold 1:
- `HW_COLUMN` (now also the closure statement's `hw` column, same snapshot), `READ_BOUND_TRIM_SQL` and the per-key `read-key`;
- `OPEN_MARK_SQL` / `OPEN_CLEAR_SQL`.

`closure_round` merges closures by high-water. Every closure in one body comes from that pass's one statement, so a merge never unites two snapshots. It still declines a closure older than any applied item.

**The budget tier's record-door ceilings: unchanged (estimate).**
- The door's batch is `SEQ_BUMP_SQL` plus `read_sql`, byte for byte as on `a0834e27` and main.
- The tier binds no `OWED_ACTOR`, so `direct_events_as` returns before any closure.
- The cells that do bind the actor (`f_cell`, the direct flood, the alarm cell) are not ceiling cells. Only `f_cell` exercises the closure, and it was not run on this tip.
- The land's vultr tier decides.

**Served shape changes, all additive:**
- `classes.*.closureMsP95` and `stages.closureMsP95`;
- `owedActor.direct.closures`;
- `/health/invariants.owedActor.directClosures`;
- the counter lists, now 33 in both Workers (pinned equal);
- the answer's `closureMs`.

`HEALTH_SQL`'s counter list grows by one name.

**Lost error.** The closure read's fault (LOW-2).

**Docs.**
- CLAUDE.md's flood figures differ from the evidence file (NIT-4).
- The CLAUDE.md claim "On D1 a NEW filing's visibility path is … two D1 round trips … a warm SEEN, word or refusal is the door's batch alone" matches the code, except for LOW-1's coalescing.

**PR body completeness.** It carries the lens, delta 1 and delta 2, each finding with its answer and pin, and the fold-3 table. Missing or stale:
- the fold count (NIT-4) and the base;
- `f_cell` was not run;
- the gates do not include a whole-piece re-run of `owed_actor` on the tip. This lens supplies that run: 195 of 195 green.

REPORT: STATUS done
REPORT: VERDICT NOT LAND
REPORT: FINDINGS HIGH 0 MEDIUM 0 LOW 2 NIT 4
REPORT: DOC docs/audit/D2-ACTOR-F-delta-3-2026-10-31.md
