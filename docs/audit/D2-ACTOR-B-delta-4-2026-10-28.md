# D2-ACTOR-B-DELTA-LENS-4: the delta lens on overlay PR 206 (D2-ACTOR-B-FOLD-7, bsv-low #912)

Written 2026-10-28 by lane D2-ACTOR-B-DELTA-LENS-4 (Wave E, row E3). This is a review document only. No product code
changed, and nothing was committed to PR 206.

What was reviewed:
- PR 206, `lane/D2-ACTOR-B-FOLD-7` at `a1010ad0`: one commit over main `d910ad5d3`, 9 files, +1,378 / -143.

What it was read against:
- The fold's brief `docs/lanes/D2-ACTOR-B-FOLD-7-2026-10-28.md` and its three amendments (items 1 to 12).
- The fold's REPORT.
- DECISION-LOG records 1478 and 1479.
- DESIGN-D2-ACTOR section 6, and Rule 23.
- The third delta lens `D2-ACTOR-B-delta-3-2026-10-28.md`.
- workers-rs 0.8.7 `durable.rs:873`, where an `i64` is an offset from now.

## VERDICT: LAND (0 HIGH, 2 new MEDIUM, 4 new LOW, 2 NOTE)

**Both beta defects are fixed:**
- **The alarms.** RED on `d910ad5d` and GREEN on `a1010ad0` on workerd.
- **The cold closure.** An unseen identity's first event is held, never a 503.

**The two MEDIUMs do not serve a press over a pending downgrade, and they lose no money.**
- **NEW4-M1** is a cost control that does nothing on D1. It has been inert since FOLD-4, and it is reachable for the
  first time now that the alarms fire.
- **NEW4-M2** is a cold actor that can be stuck for good with no served figure saying so.

**What this means for deployment:**
- Fold both MEDIUMs before Phase B, and before any digest-moving or era-moving deploy on prod.
- Beta may take the build for the lever re-run and the cold rehearsal. Read the amended list at the end.

| Item | Verdict | One line |
|---|---|---|
| 1 The alarms | **FIXED** | Every `set_alarm` goes through the two helpers. `fold7_h` and `fold7_i` are RED on base and GREEN on tip on workerd. A dead alarm is repaired by the lever and by a held event. The tip alarm's "earlier wins" pulls a 56-year alarm back. |
| 2 The held cold path | **SOUND, with M2 and L1** | The hold is durable before the 202 and acknowledged as designed, and Rule 23 holds. A stuck actor is invisible (M2). The drain is unbounded (L1). |
| 3 The closure bound by games | **BOUNDED** | The identity-wide marker key is gone. A cold 600-game bootstrap reads 4,200 record rows; a warm one-game apply reads 5. No row is lost today (NOTE-2). |
| 4 The cold bar | **SERVED, bar unasserted** | `coldMsP95`, `coldMsMax` and `coldSamples` are served, and `asOfMs` is read as an integer. The 60 s bar is a single-actor bar and cannot be met during a mass bootstrap (NOTE-1). `flushedAtMs` has the `asOfMs` defect (L3). |
| 5 Cost and tier | **NOT AS STATED (M1)** | The fleet's 60-pages-a-minute budget never refuses on D1. Per-held-event D1 writes: L4. |
| 6 The rest | **SOUND** | The counter list is 24 in both Workers, and the cell ships in `tools/lane-912/` (no `tools/lane-fold7/`). What the fixtures fake is below. |

## How this was verified

Two trees were used: this worktree at `d910ad5d3` (the base) and a scratch worktree `~/bsv/wt-actor-b-lens-tip` at
`a1010ad0`. Both were built under `CARGO_TARGET_DIR=/Volumes/Lanes/targets/actor-b-lens`. The evidence is in
`/Volumes/Lanes/out/wt-overlay-actor-b-lens/`.

- **wasm builds.** `worker-build --release` of `crates/low-app-layer` in each tree (`wb-tip.log`, `wb-base.log`).
- **The workerd cell.** The fixture was exported by `export_record_door_fixture` (1 passed). The tip's
  `tools/lane-912/owed_actor_alarm.mjs` was run on wrangler 4.54's Miniflare with `NS2_APP_BUILD` pointing at each
  build.
  - **Tip** (`fold7-cell-tip.log`): exit 0, every leg PASS.
    - `fold7_h (a)`: a 1-game seat bootstrapped in 56 ms after one alarm.
    - `fold7_h (b)`: a 118-game seat paged by its own alarms, 8 fired, done in 7,212 ms.
    - `fold7_h (c)`: a held 202, the held row applied 7,400 ms after the event.
    - `fold7_i (lever)` and `fold7_i (held event)`: "armed under a dead alarm (Some(3586408029398)): re-armed now", then
      8 alarms and done.
  - **Base** (`fold7-cell-base.log`): exit 1, "(a) the armed bootstrap: the alarm 3586408071100 is within now
    1793204035557 + 0 + 2000 (it is 57 years out)".
  - The cell stops at (a), so `fold7_i` is not reached on base; its RED is the fold's quote.
- **Native pins (tip).**
  - `d2_actor_b_fold7`: 8 passed. `fold7_a` printed "38 pages, native work 2347 ms ... under the pace about 39347 ms".
  - `owed_actor`: 52 passed.
  - `owed --skip owed_actor`: 114 passed, 1 ignored.
  - The rest of the lib suite and the 54 integration binaries were not run here; the fold's counts stand.
- **A scratch measurement**, added to the tip's `tests.rs` and removed afterwards. It took the 600-game chained seat
  cold, 16 games an apply in the bootstrap's order, then 50 one-game applies:
  - "38 pages, 76 rounds, 4200 record rows"
  - "50 one-game applies after: 250 record rows"
- **The D1 bind probe** (`budget-probe/`, wrangler 4.54 Miniflare D1):
  - `SELECT typeof(?1), ?1/60000, json_object('x',?1)` bound with `1793190803706` answers `real`, `29886513.3951`,
    `{"x":1793190803706.0}`.
  - The shipped `BOOT_BUDGET_TAKE_SQL`, 70 takes inside one minute: every take answers `taken: 1`. The row holds
    `tickedAtMs 29886513.115` (`real`).
  - The same statement with `CAST(?1 AS INTEGER)/60000`: takes 1 to 60, then no row (refused).

## New findings

### NEW4-M1 (MEDIUM): the fleet's bootstrap budget never refuses on D1

**Where:** `crates/low-app-layer/src/owed_actor.rs:643` (`BOOT_BUDGET_TAKE_SQL`), bound by `object.rs:855-865` with
`Bind::Int(now)`.

**The defect.** `routes::D1Sql` binds every `Bind::Int` as a JS number (`routes.rs:3168`, `JsValue::from_f64`), and D1
stores it as a REAL. That is the same fact the fold used to fix `asOfMs`. So `?1/60000` is a fractional "minute":
`29886513.3951`, then `29886513.395216666`, and so on, a different value every millisecond.
`owed_belt_state.tickedAtMs=excluded.tickedAtMs` is therefore never true. Every take resets the count to `'1'` and
answers `taken: 1`.

**The failing input.** 70 takes in one minute on Miniflare's D1 all answer `taken: 1`. The control with the minute cast
to an integer answers 1 to 60 and then refuses.

**What it means:**
- `boot_pace` never returns a wait on D1. Its `alarm_at(wait)` branch is reachable only in the native pins, which bind
  integers.
- Every bootstrapping actor pages at one page a second, with no fleet cap. N actors page at N pages a second against
  the D1 the overlay shares.
- The third lens's window item 5 ("one write a token and no write per waiter, on real D1") reads a budget that does
  not exist.

**When it started.** It came in with FOLD-4 (`6e1e4eb6`, NEW2-L3). It went unseen because no OwedActor alarm ever
fired before this fold, so no page ever took a token. On beta, record 1479 found no `owed-actor:boot-budget` row.
After this fold, the lever's re-run on beta's 156 actors pages them unthrottled, up to 50 at once (one lever call).

**The fix.** Bind or compute the minute as an integer: `CAST(?1 AS INTEGER)/60000`, or bind the minute computed in
Rust. Add a pin that binds `?1` as `f64`, the way `fold7_f` does.

### NEW4-M2 (MEDIUM): a cold or re-armed actor that cannot finish its bootstrap is stuck for good, and nothing served says so

**Where:**
- `object.rs:332-336`: a `bootstrap_page` error logs and calls `alarm_in(60_000)`. There is no counter and no park.
- `owed_actor.rs:1427`: the drain's `apply(...)?` ends the drain with the rows standing.
- `owed_actor.rs:896-907`: the local fixpoint errs past `CLOSURE_KEYS_MAX`.

**The failing input.** A page, or a held event, whose closure faults on every attempt:
- its games' closure passes 4,096 txids, reached through `txids_in_value` harvesting every 64-hex string of a row;
- or a record statement that always faults for that identity.

**What happens:**
1. The cursor stays on the last page, and the page and the drain are retried every 60 s for ever.
2. The actor is never stamped `bootstrapped`.
3. Every later event for the identity is held: one more `meta` row each, and one D1 counter write each.
4. Every read of that seat serves the KV copy as display only, or a 503 `pending`.
5. In Phase C that seat is served no press until an operator acts.

**Why nothing shows it.** A held answer is acknowledged in Phase B (`actor_then_d1`, `owed_actor.rs:2733`) and in
Phase C (`as_the_judge`, `:2886`). So:
- the consumer's fault memo, its hold and its dead letters never see it;
- `faultsTotal` does not move;
- `oldestUnappliedEventAgeMs` does not move: it counts actor faults in Phases A and B, and un-acknowledged marks in
  Phase C.

Before the fold the same state was a 503 on every delivery, counted and aged. The served signals left are `held`
rising and `bootstraps` not rising, and neither names the identity.

**The fix:**
- Count every bootstrap-page and drain fault (`owed_actor_bootstrap_faults_total`).
- Keep one D1 row per holding actor (`owed_belt_state` `owed-actor:holding:<identity>`, set at the first hold and
  deleted by the stamp). Serve its count and its oldest age on `/health.owedActor`.
- Park a page that has faulted N times, and name it in its line.

### NEW4-L1 (LOW): the held list and the drain are unbounded

**Where:**
- `HOLD_UNLESS_BOOTSTRAPPED_SQL` (`owed_actor.rs:1226`) writes one row per (seq, kind, games digest), with no bound.
- `drain_held` (`:1372-1450`) applies each row separately, inside one turn and one alarm invocation. Rows naming the
  same game are not merged.
- `STAMP_BOOTSTRAPPED_SQL` refuses while any row stands.

**The failing input.** Events for the identity arriving faster than the drain applies them. Each arrival is held while
the drain awaits D1 (the hold runs outside the turn). The drain loop never empties, so the actor is not stamped until
the platform ends the alarm invocation. That leaves the applied rows deleted, the rest standing, and the cursor on the
last page, so the next alarm re-pages it.

**Today's cost.** Measured: a one-game apply is 1 round and 5 record rows. A drain of K rows is K applies.

**The fix.** Drain the union of the held games in chunks of 16 (the apply reads the record as it stands, so one apply
of a game covers every held event naming it), bound the drain per alarm, and carry the rest to the next alarm.

### NEW4-L2 (LOW): `alarm_is_dead` calls a paced wait dead

**Where:** `owed_actor.rs:2010-2012` versus `boot_pace` (`:652-663`).

**The defect.** An alarm is "dead" past now + 1 s + 60 s. `boot_pace`'s wait is the next minute plus up to 59.999 s,
which is up to about 120 s out. A held event or a lever call during a legitimate paced wait therefore re-arms the
bootstrap at once, counts `owed_actor_alarm_rearmed_total`, and spends one budget take.

This is moot while M1 holds (no wait is ever set on D1). Once M1 is folded, the counter over-counts during every mass
bootstrap.

**The fix.** Make the bound the pace's longest wait plus slack (about 125 s), or store the expected page time in `meta`
and compare against that.

### NEW4-L3 (LOW): `classes.*.flushedAtMs` is served null, the same defect as `asOfMs`

**Where:**
- The bars rows' `tickedAtMs` is bound by `Bind::Int`, so it is stored as a REAL.
- `health_select!` (`owed_actor.rs:2188`) renders it through `json_object(..,'tickedAtMs',tickedAtMs)` as
  `1793….0`.
- `fleet_bars` reads it with `as_i64()` (`:2140`), which is `None` for a JSON float.

**The effect.** Every class's `flushedAtMs` is null when served from the snapshot, `cold` included. The fold fixed
`asOfMs` alone.

**The fix.** `json_ms` at `:2140`, or `CAST(tickedAtMs AS INTEGER)` in `health_select!`.

### NEW4-L4 (LOW): held events cost one D1 write each, and a re-armed actor's tip window re-holds every alarm

**Where:**
- `object.rs:692-705`: the Held arm of `apply_route` writes `owed_actor_apply_held_total` with its own D1 statement
  per held answer.
- `object.rs:341-372`: the tip window runs on every alarm while `META_GATE` stands.

**The effect.** A re-armed actor (bootstrapped before, `META_GATE` set) runs `tip_window` on each page alarm, once a
second. That calls `apply_route`, which re-holds its claimable courier-dated sweep games: `GATED_GAMES_SQL` takes those
games whatever `tipJudged` says. Each chunk re-holds (`n+1`) and writes a counter. So `held` counts alarms, not
events.

**The fix.** Note the held count in the isolate's bars and flush it, as the samples are flushed. Skip the tip window
while `bootCursor` stands: the drain judges at the tip anyway.

### NOTE-1: the cold bar's arithmetic

The 60 s bar for a 600-game seat is not asserted anywhere. `fold7_a`'s `cold_ms` is on the mirror's fixed clock (it
printed 1,000 ms), and the workerd cell measures a 118-game seat (7.4 s).

**One actor alone, under the pace as it stands:**
- 38 pages with 37 page delays of 1 s: 37 s before any work.
- Native work for the 600-game seat: 2.3 s.
- On beta, the 23 s left under the bar is about 0.6 s a page for two record rounds of D1 batches plus wasm work. The
  bar is reachable, but beta must measure it (the fold's rehearsal).

**During a mass bootstrap, once M1 works as designed:**
- The fleet has 60 pages a minute.
- 156 actors take at least 156 pages, plus the seat's 38: at least 194 pages, about 3.2 minutes at 60 a minute. More
  if the others hold more than one page.
- So `coldMsP95` under 60 s is a single-actor bar. During a lever pass, or after a digest or era move, a heavy seat's
  cold time is the fleet's page count over 60 a minute.
- Read `coldMsP95` only from a rehearsal with one cold actor. During a mass bootstrap, read `coldMsMax` against that
  arithmetic.

### NOTE-2: the closure keyed by games loses no row today, and nothing pins that

Two keys changed:
- `pot_records` lost its `spendingTxid` key (`owed_actor.rs:190-194`), and the local walk keys a pot row by its own
  `txid` only (`:844-846`).
- The marker key is now the domain's (`:830-836`, `selection(hopparty_records) AND t.identity=?1`).

No judge statement selects `pot_records` by `spendingTxid` in a `WHERE` or `ON`. The hydration's
`spendingTxid` reads are projected columns, and `rank.rs:309` joins `pot_beefs` on it, a txid the judge's answers
name. So the record rounds (`Noting`) still pull every row the judge reads. The byte-identity pin (10 identities, 1,715
rows) and `fold7_a` (600 games against the D1 judge) agree.

The residual: a later judge statement that reads `pot_records` by `spendingTxid`, or reads a marker of another game by
`markerRowid`, would diverge in the actor alone. `the_mirror_names_every_table_the_judge_reads` checks tables, not key
columns. The fix: a pin that every column in a judge statement's `WHERE` or `ON` over a family is one of that family's
keys.

## What each item reads, cold

**1. The alarms.**
- `object.rs` holds two `set_alarm` calls, both inside the helpers: `:775-784` `alarm_in` (an offset) and `:786-795`
  `alarm_at` (a stamp less now, 0 if passed).
- The sites:
  - `:335` the page retry, `alarm_in(60_000)`
  - `:484` the rebuild, `alarm_in(0)`
  - `:550` the dead-alarm re-arm, `alarm_in(0)`
  - `:558` the first arm, `alarm_in(0)`
  - `:589` the tip alarm, `alarm_at(at)`
  - `:735` the KV copy, `alarm_in(1_000)`
  - `:870` the pace's wait, `alarm_at(wait)`
  - `:890` the next page, `alarm_in(1_000)`
- No site passes a stamp to `alarm_in`. The source pin `fold7_h_every_alarm_is_an_offset` counts the two.
- `get_alarm()` answers an epoch stamp, and `alarm_is_dead` compares it to now as an epoch stamp.
- **The migration edge.**
  - The lever's `/bootstrap` reaches `arm_bootstrap` (`:539`). With the cursor set and no stamp, it reads the alarm,
    re-arms one that is none or far, and answers `rearmed`.
  - A held event reaches the same function from `apply_route`'s Held arm (`:692-694`).
  - Both are proved on workerd by `fold7_i`.
  - An actor that receives neither stays dead. The lever's full pass reaches every identity D1 knows, so the re-run is
    a required step, not optional.
- **The tip alarm.** `held.is_none_or(|h| h > at)` (`:588`) replaces a later alarm with the gate, so a 56-year alarm
  is pulled back and an earlier page or KV alarm is kept. That is the right direction.

**2. The held cold path.**
- **The hold.** One synchronous DO SQL statement (`HOLD_UNLESS_BOOTSTRAPPED_SQL`). The 202 leaves under the output
  gate, after the row is durable.
- **The consumer.**
  - `call_apply` accepts 202 (`object.rs:981-982`).
  - Phase A's `beside` drops a held answer with no sample and no fault (`owed_actor.rs:2640`).
  - Phase B filters it, then the D1 judge acknowledges (`:2733`).
  - Phase C acknowledges `heldGames` up to the generation read before the call (`:2886`).
  - None of these retries it or counts a fault.
- **The drain.** At the short page, inside one turn, the held events are applied in name order, which is seq order
  (20-digit seq). A row is deleted only by the value that was applied, so a re-hold that lands meanwhile stands and is
  applied on the next pass. The stamp refuses while any row stands.
- **A game the bootstrap already paged:** applied twice. That is harmless, because the apply reads the record as it
  stands.
- **A game it had not reached yet:** the keyset is `MAX(at)` newest first (`owed_judge.rs:986-1003`). A write during
  the bootstrap lifts the game's `at` above the cursor, so later pages skip it. Its event is held and the drain applies
  it. So the design admits double applies and needs the held event to avoid a missed one. The held event is that
  belt.
- **An actor reset between the hold and the drain.** This happens on an era move or `/reset`. The held rows go with
  `deleteAll`, the marks are already acknowledged, and the next bootstrap reads every game of the identity union. The
  reset loses nothing that a re-bootstrap does not cover.
- **Duplicates** collapse on their name (`n+1`, the first `atMs` kept).
- **The bound:** none (L1).
- **Rule 23 holds.** The read serves the actor only when its `/rows` answers `bootstrapped: true` (`object.rs:1178`).
  Otherwise it serves the KV copy as display only (`claimable: false`, `pending: true`), or a 503 `pending` when there
  is no copy. A re-armed actor's unstamp moves it to that fallback in the same batch as its hold.
- **A fault mid-bootstrap:**
  - A page fault leaves the cursor on the page that faulted, and the next alarm in 60 s re-runs it.
  - A drain fault leaves the cursor on the last page and the unapplied rows standing, and the next alarm re-pages and
    re-drains.
- **The loop case:** M2.
- **A re-arm re-pages the whole identity.** For a 600-game seat that is 38 pages, 4,200 record rows, and at least 37 s
  of fallback reads. In the steady state the closure is one round (`fold7_d`: at most 20 record rows), so a re-arm
  needs record growth deeper than 8 rounds between two events.

**3. The closure bound by games.**
- **The identity-wide key is gone.** The marker statement is the domain's, filtered to the identity (`:830-836`).
- **The rows read:**
  - A steady one-game apply on the 600-game seat: 1 round and 5 record rows (measured; `fold7_d` asserts under 20).
  - A cold bootstrap: 4,200 record rows, against the seat's 3,000 in the record.
- **The ceilings are unchanged:** `CLOSURE_ROUNDS_MAX` 8 and `CLOSURE_KEYS_MAX` 4,096. The local walk now runs to its
  own fixpoint with no round limit, bounded by `CLOSURE_KEYS_MAX` alone. That is the reason a retry progresses.
- **A lost row:** none today (NOTE-2).

**4. The cold bar.**
- One sample per bootstrap that drained held events: now less the least of each held event's `atMs`, or its hold time
  where `atMs` is absent. It is noted into the `cold` class's visible ring and flushed at once.
- An actor bootstrapped by the lever with no held event gives no sample.
- `coldSamples` is the ring's length (at most 256), not a total.
- The bar's arithmetic is NOTE-1.
- `asOfMs` is written with `CAST(?1 AS INTEGER)` (`:2202`) and read either way through `json_ms` (`:2290`). Pinned by
  `fold7_f`, which binds an `f64`.

**5. The cost and the tier.**
- The fold's tier row (`health-dirty-snapshot` 7/7 reads, 5/5 statements, largest 2/5) was not re-run here. The diff
  adds one name to `HEALTH_SQL`'s `IN` lists, on the writers' side only. The `/health` read is the same two
  primary-key rows.
- **The consumer's 20 s wall:** a held answer is one DO statement, one `get_alarm`, and one D1 counter write. That is
  well inside the direct call's 2 s.
- **The budget row:** M1. With M1 folded, contention is one conditional upsert, which D1 serializes, so no increment is
  lost. The cap holds at 60 a minute.
- **A full beta bootstrap's reads.** At 4,200 rows for the heaviest 600-game seat, and record rows roughly 1.4 times a
  seat's rows, 156 mostly small actors read well under 1 M rows. Beta's day budget is 60 M. This is an estimate from
  the native fixture; D1's billed `rows_read` counts index steps too.

**6. Everything else.**
- `applayer_counters.rs`: `OWED_ACTOR` 24 entries, equal to `owed_actor::COUNTERS` (pinned), with three view fields.
- The Makefile: the cell after the #911 cell in `ci-d1-budget`, under the seal.
- `CLAUDE.md`: the fold's paragraph.
- `storage-ownership.json`: the marker SELECT, count 8.
- **What the fixtures prove:**
  - `fold7_chained_seat`: 600 hop markers chained by change, each with a covenant pot and a party row.
  - The cold path closes against the D1 judge byte for byte.
- **What the fixtures fake:**
  - Outside reads go through `Fakes`.
  - The mirror's clock is fixed, so cold times are meaningless natively.
  - `Holding` answers `bootstrapArmed: true` without `arm_bootstrap`.
  - `fold7_c`'s record growth is a 20-link chain of `tx_words.legacy_body` `"next"` txids. Only the key walk's
    harvesting follows that chain; the judge does not.
- **The workerd cell is the behaviour proof** for the alarms. It wraps the production class (`AlarmPin`), lets workerd
  fire the alarm, and adds only `/pin/state` and `/pin/dead-arm`.
- `tools/lane-fold7/` did not ship.

## What a further fold must change (one line each)

- **M1:** cast the minute to an integer in `BOOT_BUDGET_TAKE_SQL`, with a pin that binds `?1` as `f64` and is RED on
  `a1010ad0`.
- **M2:** count bootstrap-page and drain faults, serve the holding actors' count and oldest age from one D1 row each,
  and park a page that faults N times.
- **L1:** drain the union of held games in chunks of 16, bounded per alarm.
- **L2:** move `alarm_is_dead`'s bound past `boot_pace`'s longest wait.
- **L3:** read `tickedAtMs` with `json_ms`, or cast it in `health_select!`.
- **L4:** count held answers in the isolate's bars, and skip the tip window while `bootCursor` stands.
- **NOTE-2:** add a pin that every judge statement's `WHERE` or `ON` column over a family is a family key.

## What the beta window, the lever run, the live cold rehearsal and the rollback rehearsal must read (amended from the third lens)

Kept from the third lens: items 1 to 4 and 6, the compare's counters, Phase A's deferred figures, the coverage, the
queue's service time, the era and digest moves, and the D1 rows an hour either side of the deploy.

Amended:
1. **The lever re-run after the deploy** (beta's 156 actors armed under dead alarms). Read:
   - each `/bootstrap` answer `rearmed: true`;
   - `/health.owedActor.alarmRearmed` up by the actors still armed;
   - `bootstraps` rising to the lever's `armed` count within minutes, not hours;
   - a second full pass whose coverage is `complete`, `contiguous` and `uncovered 0`.
   An `uncovered` count that stays above 0 after the bootstraps should have ended is M2's stuck actor. Name it from
   the `[owed-actor] bootstrap page:` log lines.
2. **The budget row, replacing the third lens's item 5.** Read `owed-actor:boot-budget` in beta's D1 during the lever's
   bootstrap (`wrangler d1 execute ... --remote`). On this build, `tickedAtMs` is a fraction and `cursor` stays `'1'`:
   that is M1, and no throttle is in force. Record the D1 rows read in that hour. After M1's fold, the row must read an
   integer minute and a cursor that reaches 60.
3. **The live cold rehearsal (the fold's recipe, pair 20's seat).** Read:
   - the 202 `held: true, bootstrapArmed: true`;
   - `held` up by the events of one hand;
   - one `COLD <identity>` line;
   - `coldSamples` up by 1;
   - `coldMsP95` under 60 s with ONE cold actor (NOTE-1);
   - `faultsTotal` flat;
   - `comparedTotal` advancing with `diffsTotal` flat after the drain.
   Also read the seat's `/owed` during the hold: display only or 503 `pending`, never a press (Rule 23).
4. **A stuck actor (M2).** Until M2 is folded, during every window grep the app layer's log for
   `[owed-actor] bootstrap page:` and `did not close` / `passed 4096`. Any identity that repeats across 60 s alarms is
   a STOP: its seat serves no press.
5. **`flushedAtMs` is null on this build (L3).** Read each class's flush time from the bars rows in D1, not from
   `/health`.
6. **The rollback rehearsal**, as the third lens says, plus one read: after render 1 (`OWED_D1_JUDGE=true`), a held
   actor's events are still acknowledged by the D1 judge in Phase B. Confirm the D1 rows serve the seat's presses while
   its actor holds.
