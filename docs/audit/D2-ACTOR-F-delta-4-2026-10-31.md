VERDICT: LAND

# D2-ACTOR-F fourth delta lens: overlay PR 250 after fold 4 (44e2f992 → 1fcbf2d5), bsv-low #959 and #954

**What was read.**
- The third delta lens, in full.
- The fold-4 diff. 44e2f992 is not in the tip's history: the PR was rebased onto main `a72adab0` (PR 254). Fold 3 as rebased is `a48d5adb`, so fold 4 is `git diff a48d5adb HEAD`: 8 files, +753 / -71, the two commits `a112f719` and `1fcbf2d5`. I also checked `git diff 44e2f992 a48d5adb`: it holds only PR 254's three files.
- The carried path as it now stands:
  - `owed_events::direct_with_class`, `call_bodies`, `ClosureStep`, `WorkerDirect`, `closure_fault_line` / `log_closure_fault`, `direct_events_as`;
  - `owed_actor::{closure_wanted, closure_arm, pass_closure_select, pass_closure_statement, closure_family_bound, closure_for, read_pass_closure}`;
  - `Noting` and the apply's key walk;
  - `object.rs`'s `publish_pushes`;
  - `COUNTERS`, `health_select!`, `health_body`, and the overlay's `OWED_ACTOR` / `owed_actor_view`;
  - `f_cell` (`tools/lane-912/owed_carried_flood.mjs`) and its fixture (`record_door_fixture.rs`).
- PR 255's tip (`cc2a1839`) and PR 253's tip (`7c798001`).
- The fold's evidence: `/Volumes/Lanes/out/bsv-low/D2-ACTOR-F-FOLD-4/` (`fold4-red-mutants.txt`, `vultr/tier.log`).
- The PR body.

**What was run.** A private target dir, Studio load about 6.
- `-p low-app-layer --lib d2_actor_f`: 32 passed. These are every carried pin, with fold 4's five among them.
- `fold7_c`, `fold10_j`, `fold14_c`: 3 passed.
- `git merge-tree --write-tree` of the tip against PR 255: conflicts in `object.rs`, `owed_actor/tests.rs` and CLAUDE.md.
- `git merge-tree --write-tree` of the tip against PR 253: clean.

**What was not run.** The rest of the lib, the integration binaries, clippy, `f_cell`, and the budget tier. For the last two I read the fold's vultr log.

**Verdict.** The third delta lens's two LOWs are closed:
- The bodies that want no closure are called at once.
- The closure read runs beside them, under the 2 s wall, and the filings follow it with or without a closure.
- A faulted, walled or declined read is counted, served and logged.

NIT-1 to NIT-4 of the third lens are closed as reported. What remains is five NITs: counting edges, a cell comment, and stale text. Nothing affects correctness or the served judgement.

## Findings

### NIT-1: a malformed closure answer is counted as a DECLINE, not a fault, and the decline pin drives exactly that case

**Where.**
- `crates/low-app-layer/src/owed_actor.rs:1201-1203`: `json_of(..).ok()?`, `got.as_array()?`. A family column that does not parse, or is missing, makes `closure_for` answer `None`.
- `crates/low-app-layer/src/owed_events.rs:3029`: `None => step.declines += 1`.
- `fold4_low2_a`'s "declined" case is `{"hw": 1, "f0": "{"}`. That is a shape fault, not a closure past `CLOSURE_MAX_BYTES`.
- No pin drives a real size decline through `direct_with`. `fold2_medium1_*` proves the drop at `closure_for` only.

**Scenario.** A family is added or renamed so that the statement's answer lacks or garbles an `f<n>` column. Then every filing's closure is dropped and `closureDeclines` rises, while `closureFaults` stays 0. The window reads "closures too big" where the truth is "the statement's answer is broken".

**Fix.**
1. Make `closure_for` answer `Result<Option<Value>, String>`: a shape failure is `Err`, counted as a fault with its text; a size drop is `Ok(None)`, counted as a decline.
2. Re-aim the pin's decline case at a closure past 32 KB.

### NIT-2: the served closure stage excludes the reads that walled or faulted

**Where.** `owed_events.rs:3022-3036`. `closureMs` is attached to a filing's facts only on `Ok`. A walled read (2,000 ms) or a faulted read adds no sample.

**Scenario.** Under a D1 stall, `classes.direct.closureMsP95` shows only the reads that answered inside 2 s, so the stall is invisible in the stage. It is visible only in `direct.closureFaults`.

**Fix.** Either:
- state it at `APPLY_STAGES` / in the cutover doc ("read `closureFaults` beside `closureMsP95`"), or
- attach the walled time to the filings' bodies as `closureMs` too, with a `closureWalled` mark.

### NIT-3: `f_cell`'s comment says the served bars carry the flood's samples; the run served two

**Where.** `tools/lane-912/owed_carried_flood.mjs:18-20`: "its warm actor's apply flushes the isolate's pending samples, the flood's among them".

**What the run shows.** `vultr/tier.log:1546` serves `classes.direct {"applySamples":2, "visibleSamples":2, "closureMsP95":6, ...}`.
- The two samples are seat 1's own: its flood apply and its extra filing.
- The other 31 actors' pending bars were never flushed. Each `OwedActor` holds its own `PENDING_BARS`, and an object flushes only at its own next answer after the minute.

**The other figures.** The first visibility (`a.at - t0`) and the door's `enqueue;dur` print `p50 = p95 = max` (375 ms and 39 ms). They are single values, not distributions: workerd advances `Date.now()` only on I/O, and the 32 doors and actors share one process. CLAUDE.md and the PR body quote them as p95s.

**Impact.** The assertions hold (non-null, 32 closures, 0 faults), and LOW-2 asked only for the real path to be served, which it is. But the comment and the doc imply more than the run measured.

**Fix.**
1. Correct the comment.
2. Print, and assert at least 1, `classes.direct.closureSamples` or `applySamples`.
3. Describe 375 ms as "all 32 visible by", not "p95".

### NIT-4: stale text in the PR body and CLAUDE.md

1. PR body line 8 tells the rebase to watch for "a spurious miss from 255's `STORED_BELT_DUE_SQL`". PR 255's tip `cc2a1839` has no `STORED_BELT_DUE_SQL`: its kept-date rule is inside `ROW_UPSERT_SQL` (`KEPT_DUE_SQL`, bind `?16`), and `GAME_DELETE_SQL` gained a third bind (the written outpoints). Answer 5 says what to watch instead.
2. PR body item 4 says "(both Workers' lists, now 33)", then "35" in the next sentence.
3. CLAUDE.md's carried paragraph keeps "(both Workers' lists, 33)" before the fold-4 sentence that says 35.

**Fix.** Edit the three sentences.

### NIT-5 (stated, no change needed): one identity's filing and non-filing events in one pass are now two bodies

**Where.** `owed_events.rs:2980-2995`. `rest` and `filings` are coalesced apart, so an identity holding both gets two concurrent actor calls (two applies, serialised by its turn). `fan_out` counts that identity twice (`:2994`), so a pass reaches `FLOOD_CONCURRENCY` slightly sooner.

**Why it is acceptable.**
- The SEEN's apply judges the game over a mirror that does not yet hold the filing's rows. The filing's body then re-judges it.
- That is exactly the state two passes a moment apart would give.
- The filing's mark is a plain mark, so nothing is withheld in between, and no press appears that the filing would remove: the interim judgement is the pre-filing one.

**Fix.** None. One sentence at the split, so the next lens need not re-derive it.

## The six answers

### 1. LOW-1: closed

**Where it is decided.** `owed_events.rs:2970-3049`.
- `wanted` is `closure_wanted(ev)`, or `None` for the sweep's class.
- `kept` is split into `filings` and `rest`.
- `immediate = call_bodies(port, rest_bodies, ..)` is built and joined, NOT awaited first.
- The `after_closure` future races `port.closure_read(stmt)` against `port.sleep_ms(DIRECT_CALL_WALL_MS)` (`futures_util::future::select`, `:3005-3016`), then coalesces the filings and calls them.
- The two run under `futures_util::future::join(immediate, after_closure)` (`:3048`).
- `WorkerDirect::sleep_ms` is `worker::Delay` (`:3190`). A port without a timer never walls; only the tests' ports lack one.

**On the bound, the filings are CALLED with no closure; nothing is dropped to the queue.** The `Err` arm (`:3036-3039`) only sets `step.faults` and `step.fault`. The code then falls through to `coalesce_direct(ctx_of(&events))` and `call_bodies` (`:3043-3045`), so each filing's actor reads its own miss once, as on a fault.

**The pin.** `fold4_low1_a` (`tests.rs:16360`), on tokio's paused clock, with the closure port pending forever:
- the SEEN is called at virtual 0 ms, with no closure;
- the filing is not called before the wall, then is called at exactly `DIRECT_CALL_WALL_MS` with no closure;
- the counts are `(calls, faults, walls, closure_calls, closure_faults, closures) = (2,0,0,1,1,0)`;
- the fault text names the wall, and `closure_ms == Some(2000)`.

**What it is RED against.**
- The base's sequenced read: RED at its first assertion (`fold4-red-mutants.txt`: "the SEEN's call was made inside the wall: []").
- A mutant keeping the concurrency but dropping the wall: RED at `timeout(wall*2, pass)` ("the closure read is raced against its wall").

Run here: green. `fold4_low1_b` covers the in-time closure riding the filing's body, also green.

**What remains, by the platform.** A walled D1 statement is not cancelled (a dropped future does not abort the fetch). Under a D1 stall it keeps occupying D1 while the filings' own miss reads queue behind it. The bound is on the pass, not on D1's work.

### 2. LOW-2: closed. Fault and decline are distinguishable, with one edge (NIT-1)

**Counted.** `DirectReport::{closure_faults, closure_fault, closure_declines}` (`owed_events.rs:2631-2637`). `direct_events_as` adds `COUNTERS[33]` (`owed_actor_direct_closure_faults_total`) and `COUNTERS[34]` (`..._declines_total`) to its counter batch (`:3236-3237`).

**Both lists hold them.** They are in both Workers' lists (`owed_actor.rs` `COUNTERS: [&str; 35]`; the overlay's `applayer_counters::OWED_ACTOR: [&str; 35]`). `fold10_j` and `fold4_low2_a` pin both lists.

**Served on `/health`.** They are in `health_select!`'s counter list, and `health_body` serves `direct.closureFaults` and `direct.closureDeclines` (`owed_actor.rs:5508`).

**Served on `/health/invariants`.** `owed_actor_view` serves `directClosureFaults` and `directClosureDeclines` (`applayer_counters.rs:181-182`). `fold4_low2_a` asserts the view, the snapshot SQL's names, and the source lines.

**Logged.** `log_closure_fault` is gated once a minute per isolate on its own `thread_local` (`:2670-2696`), so it is not shared with `log_direct_fault`'s gate. `fold4_low2_a` asserts 30 s silent and 61 s due.

**Distinguishable?** Yes, for what reaches the counters:
- a transport or D1 error, the wall, and "the statement answered no row" are faults (`read_pass_closure`, `owed_actor.rs:1262-1270`);
- a closure past 32 KB is a decline;
- the edge is that a malformed answer is also a decline (NIT-1).

**`closureMs` from the real path.** The object echoes the body's `closureMs` into its answer (wasm only). `f_cell` on the tip served `classes.direct.closureMsP95: 6` and `stages.closureMsP95: 6` (`tier.log:1546`), from the real workerd actor. The samples are seat 1's two (NIT-3).

**The counter's movement.** `f_cell` polls `owed_actor_direct_closures_total` up to `>= 32`, then asserts it moved by exactly 32 with 0 faults (`tier.log:1544`).

### 3. The cell's figures

**Is it the real path?** Yes:
- real workerd, through Miniflare under the tier's pinned wrangler 4.148;
- the production app-layer build and its records door (`POST /record`);
- the production `OwedActor`, behind the read-only `AlarmPin` wrapper;
- a local D1 holding the exported fleet fixture;
- `OWED_EVENTS` as a producer only, so the direct call is the one road;
- the closure where it now lives: one statement per pass, in `direct_events_as` after the door's answer.

**The seats.** 32 identities, each with its own game, are bootstrapped cold first. Each then files a NEW held `collected` marker naming a payout txid no row named before.
- Seat 0's payout is a stored BEEF of about 20 KB (`realistic_payout_beef`: a 19,900-byte data-script parent and a one-in, one-out spend), carried as decoded `__facts`.
- The other 31 seats' payouts have no stored BEEF.
- So 31 closures are a few rows each, and one is about 20 KB of hex read and about 1 KB of facts kept.
- That is the mild case: a beta filing whose payout and marker transactions are both stored, or whose family rows are heavier, weighs more. The tier's fixture does not exercise the 720,960 family bound or the 32 KB drop.

**The figures.** Each door request is its own pass (fan-out 1, class `direct`):
- 32 closure statements on one local SQLite, served p95 6 ms;
- apply p50 5, p95 6, max 7 ms;
- `readMs` 0, `recordCalls` 0, `closureRounds` 1 on all 32.

The 375 ms "first visibility" is the actors' answer stamps less the flood's start, one value for all 32 (NIT-3). It contains:
- the doors' 166 ms (all 32 answered);
- the 32 passes' closure statements, which local D1 serialises;
- the DO calls.

It is not a beta figure. Local D1 is one process with no network round trip.

**What the beta window must read to call the bars met under real D1,** from `/health.owedActor` after at least a minute of filings, with sample counts:

1. **The apply bar (300 ms).**
   - `applyMsP95 <= 300` and `classes.direct.applyMsP95 <= 300`, with `classes.direct.applySamples` large enough to mean something (tens, not 2).
   - `classes.direct.readMsP95 ~ 0` and `outsideMsP95` small: the carried path, not the fallback.
2. **The visibility bar (1,000 ms).**
   - `rowsVisibleDirectMsP95 <= 1000` and `rowsVisibleMsP95 <= 1000`, with `visibleSamples` counted.
   - Visibility is measured from the event's write time to the apply, so it CONTAINS the closure statement, which precedes the call.
3. **The closure path is running.**
   - `direct.closures` rises with the filings made.
   - `direct.closureFaults` is 0 or rare, and `direct.closureDeclines` is near 0.
   - `classes.direct.closureMsP95` is read beside `closureFaults`, because walled reads are not sampled (NIT-2).
   - If `closures` is flat or `closureFaults` rises, the visibility figure measures each actor's own miss read, not this path.
4. **The flood.** Under #959's flood, expect `closureMsP95` to carry the serialised-D1 latency #959 read as `readMsP95` (about 700 ms). The 1,000 ms visibility bar is then marginal: about 700 ms closure, plus the door's enqueue, plus the apply.
   - A pass that fans out to 8 or more lands in `classes.flood`, not `direct`.
   - The window must also read the door's `Server-Timing enqueue` and `/health.record.enqueueMsP95`.
5. **Sample coverage.** The fleet's bars hold only what actors flushed: each object flushes at its own next answer after a minute, so an actor answering once leaves its sample unflushed. Read the samples counts before trusting a p95.

### 4. The three tests: the amendments were right; no pin was weakened

**`fold7_c`.** Since fold 3 the key rule reads key columns only; `tx_words.hint_source` is free text, by design. The amendment moved the growing chain's "next txid" into `hop_chain_probes.spendingTxid`, a key column read through the probe range of each txid.
- The pin's property still holds: an apply whose closure grows past its bound un-stamps, re-arms and holds.
- It is driven through a column the judge actually follows.
- Keeping the chain in `hint_source` would pin a key rule the design removed.

**`fold10_j`.** It holds the exact counter list. It grew to 33 (fold 3), then 35 (fold 4), by design. The assertion is still the full literal list.

**`fold14_c`.** "The stages sum to `applyMs`" now skips `closureMs`.
- This is right: the closure is the caller's stage, measured in the direct pass before the call, so it is outside `applyMs` by design (CLAUDE.md, `APPLY_STAGES`).
- The other five stages (`read`, `wait`, `judge`, `write`, `outside`) are still summed and served.
- `fold3_nit3` and `f_cell` cover `closureMs`'s serving separately.

**Fold 4's own amendment.** `fold3_medium1_b` now expects two bodies: the SEEN's first with no closure, the filings' after it with one. This is the LOW-1 behaviour itself. The pin still asserts the pass's whole record cost is one statement, and that each filing's apply took its closure and equals a cold apply's rows.

All three ran green here, beside the 32 `d2_actor_f` pins.

### 5. The rebase ahead

**PR 255 (FIX-1018, `cc2a1839`).** Textual conflicts only: `object.rs` (`ApplyBody` gains `belt_asked`; 250 adds its carried fields, and both touch the alarm's body literal), `owed_actor/tests.rs`, CLAUDE.md. `owed_judge.rs` and `owed_actor.rs` merge cleanly.

In substance, PR 250 cannot overwrite a kept belt date:
- 255's rule lives in `ROW_UPSERT_SQL`'s `DO UPDATE` (`KEPT_DUE_SQL`: the stored, earlier `beltDueAtMs` kept unless `?16`, "the belt just asked").
- `owed_rows` is not a mirror family, and `carried::item_statements` refuses any table outside `FAMILIES`. So no carried item and no closure row can carry or write `beltDueAtMs`.
- The carried apply's judge writes through the same `row_statements`, so it gets the kept-date rule against the actor's own stored row.
- `beltAsked` reaches the actor only from `D1Judge::actor_apply` (the belt's hand-back). The direct pass's `apply_body` sets none, so a writer's carried event dates nothing fresh, which is correct.
- 255's `belt_asked_scope` in `apply_route` sees an empty list for a carried body.

**What the captain's rebase must watch.**
1. `Noting::is_commit` matches the commit batch by `s.sql == GAME_DELETE_SQL`. 255 changes that constant's text (adding `outpoint NOT IN ?3`) and moves the delete AFTER the upserts in the same batch. Detection still works, because it compares against the constant, not a literal, and checks presence, not position.
   - `Noting` scans every text bind of the staged commit for txids. 255's new `?3`, the written outpoints, names only the txids `?2` of each upsert already names, so no new key and no spurious miss round should follow.
   - Prove it on the rebased tip: `d2_actor_f_a` and `fold_l2_a` (`recordCalls 0`), `fold3_medium1_a`/`b`, `fold3_nit1`, `fold7_c`, and `f_cell` (`recordCalls [0]`, `readMs` 0).
2. `commit_fixpoint`'s restore must still restore after 255's reordered batch. A DO batch is not atomic, and the delete now runs last. Run `d2_actor_b_fold_h2_*`.
3. Merge `ApplyBody` keeping both `belt_asked` and the carried fields, each `#[serde(default)]`. Add `belt_asked: Vec::new()` to the alarm's literal beside 250's fields.

**PR 253 (FIX-1017, `7c798001`).** It merges cleanly.
- `f_cell` has NO push assertion: `RELAY_URL` is sealed, and the log shows `[push] not configured ... event dropped`. So "the gate's two-window rule" cannot break it.
- The actor's `owed-changed` pushes leave through `publish_pushes`, then `ctx.wait_until(push_coalesce::flush_queued(..))` (`object.rs:1076-1084`). That is after the answer, never inside the measured apply, so a longer gate hold does not move `applyMs` or the visibility stamps.
- 253 adds `/health.push.gate` through a memoised gate fetch (once per 10 s per isolate). `f_cell`'s `/health` polls then make one `BOARD_VIEW` subrequest, which the egress guard reads as a binding call. Harmless, but the egress tally changes.
- Re-run `f_cell` and the #952 / #989 / #1002 cells after both land.

### 6. Everything else

**The budget tier.** The vultr log (`tier.log`): `make ci-d1-budget` exit 0 in 1,446 s, 353 PASS, 0 FAIL, the egress guard clean.
- The record-door rows are unchanged: `record-potparty-filing` reads 4/7, writes 13/16, statements 4/6; "PASS all seven doors". Every door's `d1;desc` (handfile, potparty, potrefund, result, collected, held, hopsweep) is inside its ceiling.
- No `ceilings.json` changed in the PR: `git diff a72adab0 HEAD -- tools/lane-499/ceilings.json` is empty.
- The log names its tree, not its commit (`fold4-111614/tree`). `1fcbf2d5` touched only `tests.rs`, `storage-ownership.json` and CLAUDE.md after `a112f719`, so either commit is the same Worker build.
- The land's own tier remains the gate of record.

**Served shape.** Additive only:
- `direct.closureFaults`, `direct.closureDeclines`;
- `/health/invariants.owedActor.directClosureFaults` and `directClosureDeclines`;
- the counter lists at 35 in both Workers;
- `HEALTH_SQL`'s counter list plus two names.

No key was renamed or removed.

**`storage-ownership.json`.** It grants `owed_actor.rs`'s SELECT count 11 → 12, adding the second dynamic hit of `closure_arm` (the read-back by rowid).

**The rewritten statement (NIT-1 of the third lens).** It is the same selection in a different shape:
- weight once, over `SELECT DISTINCT t.rowid, {weight} FROM {from}`;
- then `CROSS JOIN {table} t ON t.rowid = w.wi`, with the row's JSON built from the outer `t`;
- the running sum still `ORDER BY ci`.

The closure-equality pins over rusqlite (`fold_l2_a`, `fold2_medium1_a`/`b`, `fold3_nit2`) and `f_cell` on workerd answer the same rows. The statement is 31,263 bytes, pinned under 90,000.

**NIT-2 of the third lens.** `closure_family_bound(filings)` is 720,960 for one filing and capped at 1 MB from two filings up. The fold's reason for not using 32 KB per filing is sound: the 6x weight over-estimates the JSON, and a decodable BEEF up to 256 KB would otherwise be cut. A family holding two heavy rows for one filing (two 256 KB BEEFs) carries the second as not-carried, and the actor reads that miss once. That is correct, by fallback.

**Lost errors.** None new.
- `DirectPort::closure_read`'s default now answers `Err("this port reads no closure")`, which would count a closure fault. Only test ports use the default; `WorkerDirect` overrides it.
- The walled D1 statement is not cancelled (answer 1).

**Doc claims.** Every claim matches the code, except NIT-3 (the cell's "the flood's among them" and the p95 wording) and NIT-4 (the stale counts and `STORED_BELT_DUE_SQL`).

**PR body completeness.** It carries every lens's findings with their answers and pins, the fold-4 table, the rebase base, and the vultr tier with `f_cell`'s figures. Missing: NIT-4's corrections, and a line that the served bars on `f_cell` held two samples.

REPORT: STATUS done
REPORT: VERDICT LAND
REPORT: FINDINGS HIGH 0 MEDIUM 0 LOW 0 NIT 5
REPORT: DOC docs/audit/D2-ACTOR-F-delta-4-2026-10-31.md
