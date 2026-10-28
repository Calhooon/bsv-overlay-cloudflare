# D2-ACTOR-B-DELTA-LENS-5: the delta lens on the three folds after PR 206 (FOLD-8, D2-ACTOR-D, FOLD-9; bsv-low #912, #952, #953)

Written 2026-10-28 by lane D2-ACTOR-B-DELTA-LENS-5 (Wave E, row E3). This is a review document only. No product code
changed. This PR is for the captain to copy and close; it is not to be merged.

What was reviewed: the combined delta `046b721e1..e99472091`, on the landed main `e99472091`. Its tree is identical
to PR 210's tip `a5c46cc2` (`git rev-parse` gives tree `2e58d11f` for both). 26 files, +3,930 / -231. The delta is
three commits:

| PR | Fold | Head | Landed as |
|---|---|---|---|
| 208 | FOLD-8 | `1ea480d9` | `171d1ca3e` (range-diff: identical) |
| 209 | D2-ACTOR-D | `6964a620` | `dc2dd47b0` (range-diff: identical) |
| 210 | FOLD-9 | `a5c46cc2` | `e99472091` (same tree) |

What it was read against:
- the fourth lens (`docs/audit/D2-ACTOR-B-delta-4-2026-10-28.md` in bsv-low);
- the three briefs with their message amendments, and the three REPORTs;
- issues #952 and #953;
- DECISION-LOG records 1484 to 1493;
- the design's sections 5 to 7;
- the cutover doc `CUTOVER-D2-ACTOR-2026-10-28.md`.

## VERDICT: LAND (0 HIGH, 2 new MEDIUM, 5 new LOW, 4 NOTE)

Each fold does what its brief asked:
- **FOLD-8:** the fourth lens's M1 (the budget) is real on D1, and M2 (the stuck actor) is counted, served and parked.
- **FOLD-9:** #953's divergence is closed for a courier-found spend.
- **D2-ACTOR-D:** every app-layer and overlay writer's frame reaches the actor directly, with one visible sample per
  event.

Neither MEDIUM serves a press over a pending downgrade, and neither loses money:
- **NEW5-M1** makes Phase A's and B's STOP figure (`diffsTotal`) fire on an ordinary block, and delays a gate's press
  in Phases B and C by up to a block interval. Fold it before Phase B.
- **NEW5-M2** withholds a hop's sweep press for ever when a courier-found spend never confirms (before FOLD-9 the hop
  healed in 2 hours). It is a liveness defect with no lever. Fold it before Phase C.

The window on this main, the lever pass and the cold rehearsal may proceed. Read the amended list at the end.

| Item | Verdict | One line |
|---|---|---|
| 1 The budget and the stuck actor (FOLD-8 M1, M2, L1-L4, NOTE-2) | **FIXED** | The minute is `CAST(?1 AS INTEGER)/60000`, pinned with an `f64` bind. The holding row is a writer-side subquery, so the health read is unchanged. The park lands after 5 faults and the lever clears it. Rule 23 holds while parked. The LOWs are folded and NOTE-2 is pinned lexically. `fold7_a`'s wall-clock bar reds under load (NEW5-L1). |
| 2 Bars by the build, the rows route (items 9, 10) | **SOUND, with L4** | A new build's first flush replaces the row and the read filters by `libRev`. The route is bearer-first, one identity, read-only, and uses the reconcile's projection. A replace drops `oldestUnappliedAtMs` (NEW5-L4). |
| 3 Phase A's bound, the diff lines (items 11, 12) | **SOUND, superseded in part** | D2-ACTOR-D makes Phase A acknowledge to the bound and turns most Phase A deliveries into dupes. The diff lines name three fields with both values. The apart counter can absorb a real divergence on a game that is always marked (NOTE-2 below). |
| 4 The courier find reachable by the actor (FOLD-9) | **#953 FIXED, NEW5-M2 opened** | The evidence row is durable and reached by `ProbeRange`. The reconcile carries the tip. An unconfirmed find has no retirement path (M2). Pot-outpoint evidence and the TTL sweep cost: NOTE-3. The ceiling 81 to 153 is the measured shape. |
| 5 The write path (D2-ACTOR-D) | **SOUND in shape, NEW5-M1, L2, L3, L5** | The one site (`publish_env`) and the coverage dupe rule are sound for marked inputs. The tip is an unmarked input the dupe ignores (M1). Visibility of walled calls is unsampled (L5). Calls are not coalesced (L3). Discards are uncounted (L2). |
| 6 The three together | **CONSISTENT** | No fold undid another, except FOLD-8's Phase A `judge_subjects`, which D2-ACTOR-D deliberately replaced with `judge_subjects_acked_to` and whose pin `fold8_j` it amended. Counters 26 = 26 in both Workers. The digest did not move. One new tier cell (`owed_direct_flood.mjs`). |

## How this was verified

**Build and target.** Worktree `~/bsv/wt-overlay-actor-b-lens` at `a5c46cc2`, then `e99472091` (same tree), built
under `CARGO_TARGET_DIR=/Volumes/Lanes/targets/actor-b-lens`. Evidence is in
`/Volumes/Lanes/out/wt-overlay-actor-b-lens/delta5/`. The host load average was about 30 throughout.

**App layer lib suite, in foreground pieces (the test binary run directly):**

| Piece | Result | Note |
|---|---|---|
| `owed_actor` | 76 passed, 1 failed in 280 s | The failure is `d2_actor_b_fold7_a`: "one cold actor under the pace: 78252 ms", native work 41,252 ms. Run alone it passes in 25.7 s (NEW5-L1). |
| `owed --skip owed_actor` | 114 passed, 1 ignored | |
| `--skip owed` | 588 passed, 3 ignored, in 705 s | |

**Overlay lib.** `hop_probe_memos owed_events applayer_counters change_flush pot_changes`: 36 passed.

**Scratch pin for NEW5-M1.** It was appended to `owed_actor/tests.rs` and removed after the run. The patch and its
output are in `delta5/lens5-scratch-stale-tip.*`. Its result is under NEW5-M1.

**Not run:**
- the 54 integration binaries;
- clippy;
- the budget tier: the whole `ci-d1-budget` (its `health-dirty-snapshot` row is inside the main route script and
  cannot run alone), while beta's window was building on this host;
- workerd cells.

The health READ's statements did not change in the delta. `HEALTH_CONSUMER_SQL`, `HEALTH_TOTALS_SQL` and
`HEALTH_STATE_SQL` are untouched. Only the writers' `HEALTH_SNAPSHOT_SQL` grew:
- FOLD-8: the holding row's range `COUNT`;
- D2-ACTOR-D: two counter names.

Each fold measured the row at 7/7 reads, 5/5 statements and largest 2/5 on its own tree. That is stated here, not
re-measured.

## New findings

### NEW5-M1 (MEDIUM): the dupe rule treats the marks as the only input, so a direct apply at a stale tip swallows a block's `tip` event

**Where:**
- The direct pass judges at the isolate's 30 s memo: `crates/low-app-layer/src/owed_events.rs:1478`,
  `tip_memoised(env, false)`.
- The tip hook schedules that pass without seeding the memo from the height it was just given:
  `crates/low-app-layer/src/internal_events.rs:701-713`, after `write_tip_marks`.
- The seq check compares generations only and never the tip: `crates/low-app-layer/src/owed_actor.rs:1306`
  (`covered_at`), and `COVERED_SQL` at `:616`.

**The failing input.** A block at height T.
1. `/internal/tip-changed` writes the gate marks for `[T-6, T]`, publishes `tip/gate` and `tip/untipped`, and schedules
   the direct pass.
2. The hook's isolate read the tip less than 30 s earlier, so its memo holds T-1.
3. The direct apply reads `marks_bound` (which includes the tip marks), judges at T-1, and records coverage at or past
   the tip marks' generations.
4. The consumer's delivery of the `tip` event reads the tip fresh (`tip_reads_fresh`, LOW-B) and names its bound.
   That bound is at or below the coverage, so the delivery is a DUPE and the actor is never applied at T.

The scratch pin (`stranded()` fixture) applies at `TIP-1` with no bound, then delivers at `TIP` with the bound. The
result: `skipped=true`, and the game's stored row keeps `probeTip` 899,999 against the delivery's 900,000.

**What it means:**
- **Phases A and B.** The D1 judge opens a gate whose height is T. The actor's dupe keeps it closed. No mark sits
  above the bound, so the sampled compare counts a REAL diff (`owed_actor_diffs_total`, the cutover's STOP) for an
  ordinary block.
- **Phases B and C.** The press waits for the actor's own tip alarm. `arm_tip_alarm` is fed the stale tip, so the
  alarm comes about 600 s per height later (`next_tip_alarm_ms`), or the press waits for the game's next event. Phase
  C acknowledges the tip marks on the dupe, so no other event carries them.
- The same holds for any direct apply whose tip read faulted (`held_tip`), and for the overlay's hand-off path
  (`/internal/owed-direct`, the same `direct_events`).

**The fix:**
- The direct pass reads the tip fresh for a `tip` event (`tip_reads_fresh`), and the tip hook seeds the memo with the
  announced height.
- Belt and braces: record the covering apply's tip beside its generation, and answer a delivery as a dupe only when
  its tip is no greater.
- Pin: the scratch pin's shape, RED on `e99472091`.

### NEW5-M2 (MEDIUM): a courier find of an UNCONFIRMED spend is durable evidence with no way to retire it

**Where:**
- Evidence is written on any spent answer: `crates/low-app-layer/src/owed_belt.rs:470`. The seed also writes
  `spentConfirmed` NULL for every standing find: `owed_backfill.rs:260`.
- The TTL sweep spares it while the outpoint's `pot_records` row stands:
  `crates/overlay-cloudflare/src/hop_probe_memos.rs:91`.
- Only an Outpoint look that finds the outpoint UNSPENT clears it (`find.cleared`). But once the hop is judged
  `unbound` ("spent outside this game"), the belt asks the SPENDER, never the outpoint (`owed_belt.rs:304`, `ask_of`).
  A Spender ask whose raw no courier serves returns `None` and touches nothing (`:520`).
- `belt_due` dates such a row only while the spender's bytes are not held (`owed_judge.rs:900`). After the bytes' 8-day
  sweep it is re-dated, re-asked and unanswered, with the backoff growing to 6 h, for ever.

**The failing input.** The belt finds hop H spent by mempool transaction X: a foreign spend the index never sees.
Then X never mines (dropped below the relay fee, or losing a double spend).
- **Before FOLD-9:** the memo aged out at 2 h, H was judged `hop-stranded` by the index, the belt asked the outpoint,
  found it unspent, and the sweep press came back.
- **Now:** H stays `unbound` with no press, the evidence stands, and no lever removes it short of SQL. If X lost to the
  owner's own unfiled sweep, the payout's "home on chain" is never shown either.

Money is safe in the sense that no wrong press is served. But an honest press is withheld with nothing to heal it,
which is the direction Rule 23 forbids for the list.

**The fix (one line):** spare only CONFIRMED evidence (`spentConfirmed = 1`) from the TTL sweep. Alternatively, date
an Outpoint re-ask (at the belt's backoff) for an `unbound` row whose evidence is unconfirmed, and let a later find
refresh `spentConfirmed`. Pin: a dropped spend restores the press.

### NEW5-L1 (LOW): `fold7_a`'s cold-bar assertion is a wall-clock bar in a debug, parallel suite

**Where:** `crates/low-app-layer/src/owed_actor/tests.rs:3788`.

**What happened.** At a load average of 30 it measured native work of 41,252 ms (2,347 ms in the fourth lens's run)
and failed at 78,252 ms. It passes alone. FOLD-9's REPORT saw the same failure at 60,035 ms. A gate that reds under
the fleet's ordinary host load will be re-run until green, which hides a real regression.

**The fix:** time the native work in a release-profile test, or keep the arithmetic as a printed measurement and assert
the page count only.

### NEW5-L2 (LOW): the direct set is isolate-wide, so the queue consumer's discard can drop a concurrent writer's direct call, uncounted

**Where:**
- `consume_one` ends with `publish::take_direct()` (`owed_events.rs:1103`).
- A fetch in the same isolate publishes (`publish_env` awaits D1 and `sendBatch`) and only then calls
  `schedule_direct`.

**The race.** A consumer batch interleaving at one of those awaits takes and discards the writer's held events. That
writer's actors are then reached only by the queue, the loss is counted nowhere, and its sample shifts to
`backstopLagMsP95`.

**The same root, harmless.** The belt's `tick_env` publishes (`owed_belt.rs:1517`) and never takes its set. It sits
until another invocation in that isolate takes it, up to `DIRECT_HELD_MAX`, and past that later holds are dropped.

**The fix:** return the sent events from the publish to its caller instead of an isolate thread-local, or count a
discard.

### NEW5-L3 (LOW): direct calls carry no bound and are not coalesced per identity

**Where:** `direct_with` (`owed_events.rs:1412-1440`) builds one body per (event, identity, 16-game chunk), and
`join_all` sends them at once.

**The effect.** One write of k events naming one identity (a JOIN: its pot, party, hop and word pairs) queues k full
applies in that actor's turn. Each later one would have been covered by the first. The consumer's own call for the
identity waits behind them under `CONSUMER_CALL_WALL_MS` (20 s). Past that, Phase B counts a FAULT, the message is
handed back, and the fault memo holds the pair from its third.

Before D2-ACTOR-D the consumer delivered serially, so an actor never saw concurrent calls. The apply count is the same
as before; what changed is that the calls are now concurrent. Not measured at a loop's rate.

**The fix:**
- Merge each identity's games across the pass's events into one call.
- Name the pass's bound (the plan already reads the marks), so a call covered by an earlier apply in the turn is a dupe.
- Record each event's `(kind, seq)` for the first-apply rule.

### NEW5-L4 (LOW): a deploy erases `oldestUnappliedEventAgeMs`

**Where:** `merge_bars_row` (`owed_actor.rs:2672-2679`) replaces a row of another build whole.

**The effect.** The queue row's `oldestUnappliedAtMs` (an actor apply in Phase A or B that faulted, with no later apply
of that identity) is dropped at the new build's first flush. The figure then reads fresh until a new fault, although
the identity is still unapplied.

**The fix:** carry `oldestUnappliedAtMs` across the replace.

### NEW5-L5 (LOW): `rowsVisibleMsP95` cannot see an event whose direct call walled

**Where:**
- `direct_with` counts a walled call and notes no sample (`owed_events.rs:1443`).
- The object goes on and applies, recording the event's `(kind, seq)`. Its own visibility sample goes to the object's
  local meter only (`object.rs:820`, served on `GET /bars`), never the fleet's rows.
- The consumer's later delivery is then not a first apply and samples nothing (`samples_visible`).

**The effect.** The fleet's first-visibility p95 is computed over the events whose apply answered inside 2 s. The slow
tail is visible only as `direct.walls`. This is reasoned from the code: the platform's behaviour for a DO request whose
caller dropped it is assumed to continue, as the fleet's own note on dropped race futures says.

**The fix:** the object notes its first-apply visibility into the durable bars itself (it already flushes bars for held
answers). Until then, the window reads `walls / calls` beside the p95.

## NOTEs

**NOTE-1: `held_tip` reuses a previous height.** FOLD-9 item 6 (`owed_actor.rs:1431-1445`) judges a tipless body at
the greater of the actor's rows' `probeTip` and `meta.tipJudged`. That is the captain's ruling. It is stated here
because the D1 judge refuses a tipless subject ("previous judgement stands"), and TIP-877's NEW-LOW-2 says "we do not
reuse a previous height as a floor for a current gate". The two judges therefore differ in kind while a tip read
faults:
- the held tip only delays an opening;
- a reorg below a gate the held tip opened would serve a refund press the network refuses.

That is a refused broadcast, not a loss.

**NOTE-2: the STOP is statistical, and the apart counter can absorb a persistent divergence.**
- `compare_sampled` (`owed_actor.rs:3367`) compares one event in eight.
- A diff is counted apart (`heldDiffsTotal`) whenever its game holds a mark above the bound at the compare.
- A real divergence on a game that is always marked when compared (a busy table) is never a `diffsTotal`.

The reconcile's digest (64 identities a day) is then the only real-diff signal. Read `reconcileDiffs` and the ratio of
`heldDiffsTotal` to `comparedTotal` beside `diffsTotal`. Phase B's window is about 8 times longer for the same
evidence.

**NOTE-3: FOLD-9's evidence beyond hops, and the sweep's cost.**
- `probe_find` writes evidence for any Outpoint ask, including an `unbound` POT row with no named spender. The judge
  reads evidence for hops only, so such a row is written, spared while its pot row stands, and never read.
- `HOP_PROBE_MEMO_EXPIRE_SQL` re-checks every standing evidence row on every pass, with an `EXISTS` on `pot_records`
  and no bound. That cost grows with every find ever made.
- The `owed-belt-tick-fleet` writes went 54 to 110: about 7 a find (the evidence row and the mark with its index
  entries, the mark acknowledged in the D1 judge's batch). That is the measured shape, not room taken. The ceiling of
  153 is below the tier's usual 1.5 times (165), which is conservative.

**NOTE-4: what still rides the queue alone, and the cutover doc.**
- A `word` event carrying its frame (every tracker word the overlay publishes since #911, the MINED word included)
  skips the direct call (`direct_with`), so `collectAfterMinedMsP95` is still the serial consumer's figure.
- So do a `txfacts` event, a consumer continuation, events past 64 a pass or 512 held, and an overlay without
  `APP_LAYER_URL`.

The cutover doc (bsv-low) is out of date in two places:
- it still says "bar 2 is the queue's figure (the overlay has no binding to the actor)";
- it does not move `OWED_CONSUMER_CONCURRENCY` with Phase C.

Both are bsv-low's to amend.

## What each item reads, cold

**1. The budget and the stuck actor.**
- **M1.** `BOOT_BUDGET_TAKE_SQL` (`owed_actor.rs:660`) casts in the VALUES and compares integer minutes. `fold8_a`
  binds an `f64`.
- **M2.** Every page or drain fault is counted (`owed_actor_bootstrap_faults_total`).
  - The holding row is put once (`HOLDING_PUT_SQL ... DO NOTHING`) and deleted by the stamp.
  - The snapshot counts the holding rows over the range `owed-actor:holding:` to `owed-actor:holding;`. That is O(the
    holding actors), once a minute fleet-wide, on the writers' side.
  - The fifth consecutive fault parks the page (`BOOT_FAULTS_PARK`). The alarm stops retrying it, the lever clears the
    park, and the lever answers `parked: true` once.
  - A parked actor is unbootstrapped, so its seat reads the copy as display only, or a 503 `pending` (Rule 23,
    `fold8_b`).
- **L1.** The drain takes the union of held games, 16 a chunk, 8 chunks an alarm.
- **L2.** `BOOT_ALARM_DEAD_AFTER_MS` is past the paced wait.
- **L3.** `CAST(tickedAtMs AS INTEGER)` in the snapshot.
- **L4.** `note_held` goes into the isolate's bars, and the tip window is skipped under a boot cursor.
- **NOTE-2 (fourth lens).** `fold8_g` is lexical over the judge's 69 family reads on the frozen fixtures.

**2. The bars by the build and the operator route.**
- **`libRev`** is `OVERLAY_LIB_REV`, emitted by the app layer's own `build.rs` (git HEAD, or `BUILD_LIB_REV`), so two
  builds differ.
- **The old rows.** They stay stored until each class's first flush of the new build and are filtered out of the read
  meanwhile. During a rolling deploy two builds replace each other's rows (samples are lost, never mixed).
- **The route** `GET /internal/owed-actor-rows`:
  - 401 before any read;
  - one 66-hex identity (`actor_name`);
  - D1's `RECONCILE_D1_ROWS_SQL` beside the actor's `/stored`, which writes nothing;
  - `diff_rows` and `rows_digest` project the same columns, the facts less `CLOCK_FACTS`.

  An identity D1 does not know still makes a DO call (no storage written). It is behind the bearer.

**3. Phase A's bound and the diff lines.**
- **Phase A** (`owed_events::deliver`, `:388-395`) reads `marks_bound` before the D1 judge and acknowledges to it. A
  bound read fault now hands the message back (it was `.ok()` in FOLD-8).
- **The deferred `beside`** names the bound. A delivery the direct call covered is a dupe, and the compare then reads
  the actor's rows from that direct apply. Those rows were judged at the direct call's time and tip (M1).
- **A mark between the two judges** is a held diff (`HELD_GAMES_SQL`). What remains a real diff:
  - a divergence on a game with no mark above the bound;
  - an unmarked input that differs between the two judgements: the tip (M1), and the clock (memo freshness,
    `HOP_STRANDED_AFTER_MS`) across the consumer's lag.
- **The diff lines** carry `diff_detail`: three outpoints, each differing field with both values.

**4. The courier find.**
- **The evidence key** is `<txid>:<vout>`. It is reached by `ProbeRange` (`outpoint >= txid AND < txid||'~'` covers
  both `.` and `:`). The reorg tombstone is in the ranges (`owed_actor.rs:398`).
- **The judge** reads evidence after the memos, a memo winning (`hydrate.rs:708-730`).
- **Never swept while the hop stands:** `HOP_PROBE_MEMO_EXPIRE_SQL`'s `NOT (substr(outpoint,65,1)=':' AND EXISTS
  pot_records)`. `PROOF_LATCH_MOVED_DELETE_SQL` names only `proofmined:` keys.
- **The reorg clear** (`HOP_PROBE_MEMO_REORG_CLEAR_SQL`) still takes confirmed evidence. The hop is then
  `hop-stranded`, pressable on the index's word (D-H1) until the belt re-asks it: a refused broadcast at worst. That is
  better than before the fold, when the memo did the same every 2 h.
- **The bytes** are reached by the spender txid the evidence names (harvested), with no new column. The record rows read
  per apply on the 600-game seat are unchanged (24 statements, 5 rows, FOLD-9's `fold9_h`).
- **The find is an event:**
  - Phase A: its mark is acknowledged in the D1 judge's batch, the actor called directly (`belt-find`);
  - Phase B: `phase_b_find`, under the bound;
  - Phase C: the mark stands for the consumer, whose delivery is a dupe of the direct `actor_apply`.
- **The cold walk of the judge's inputs:**

| Input | Verdict | How |
|---|---|---|
| Probe memos and the home and proof latches | REACHABLE | `PROBE_PREFIXES` |
| `tx_words` | REACHABLE | Its txids are harvested from every mirrored row. Phase B's read replaces its words with D1's anyway. |
| `network_seen`, `owed_spender_bytes`, `transactions`, `pot_beefs` | REACHABLE | By txid |
| `hopsweep_records` | REACHABLE | By the hop's txid, any game |
| `submit_refusals` | REACHABLE | By the hop's txid |
| `owed_row_words` | NOT A FAMILY | The actor's own; the read joins D1's words |
| The previous `owed_rows` | NOT A FAMILY | Each judge's own. The belt's backoff (`beltDueAtMs`) is outside the digest. |
| The tip | UNREACHABLE AND DIVERGING | It is an outside read, and it is not covered by the dupe (M1). |
| The clock | UNREACHABLE, HARMLESS TODAY | It moves the families across the consumer's lag. The design leaves time-only transitions to the belt's dating. |

**5. The write path.**
- **The one site.** `publish_env` holds every event it SENT. The callers that hand the set on:
  - the record door and the handfile batch (`schedule_direct`; the old filing-driven `object::schedule_direct` is
    removed);
  - the pot-, hop- and tip-changed hooks;
  - `/internal/tx-word` (`tracker_host/runtime.rs:938`);
  - the scheduled word tick (`direct_held`, inline);
  - the un-acked mark sweep (`sweep_publish`, inline);
  - the overlay's three flush points (`ship_owed_direct`, which `pot_changes::flush` reaches by `direct_is_empty`
    after the dispatch exit's publish).
- **The wall.** 2 s per call, counted (`owed_actor_direct_walls_total`). A call runs in `wait_until` after the answer,
  so it never fails a write.
- **One visible sample per event.** Only `applied && firstApply` samples. `rowsVisibleMsP95` is direct plus queue;
  `backstopLagMsP95` is beside it with its 60 s bar (L5 bounds what the p95 can see).
- **The coverage rule is sound for the marks.** Generations are AUTOINCREMENT and D1 commits serially, so every mark
  at or below the covering apply's read was committed before its record read. It is unsound for the tip (M1).
- **The consumer's cost.** Native, debug: p50 100 to 31 ms and max 124 to 46 ms (D2-ACTOR-D's `d_d`, not re-run here).
  The compare runs on one event in eight (NOTE-2).
- **`max_concurrency` 1** has its reason at both config lines. The knob is pinned equal, and the consumer names a value
  above 1 under the D1 judge on every batch. Out-of-order seqs: `d_e`.
- **At a loop's rate (125 frames a minute):** the D1 cost per event is about the same as before. The direct pass adds
  one marks read per event, and the Phase A dupe saves the deferred closure. One identity's actor now sees concurrent
  applies (L3). In the overlay, `ship_owed_direct` is awaited before the words and notes in each flush. In
  `pot_changes::flush` the 20 s wall is computed after it (`pot_changes.rs:420-423`), so with the app layer slow, up
  to 8 chunks of 2 s stack on the 20 s. That is stated, and it sits with L3's fix (race the hand-off against the
  flush's deadline).

**6. Together.**
- Range-diff: FOLD-8 and D2-ACTOR-D landed identical to their PR heads, and FOLD-9's tree is main's.
- D2-ACTOR-D replaced FOLD-8's Phase A `judge_subjects` and its `.ok()` bound, and amended `fold8_j`. That was
  deliberate, and the pin was re-proved.
- The counters: `OWED_ACTOR` 26 in the overlay, `COUNTERS` 26 in the app layer. The health snapshot names
  `bootstrap_faults` and `direct_walls`.
- The digest: `schema.rs`, `owed_actor/schema.rs` and `ACTOR_TABLES_VERSION` are unchanged. The coverage reuses
  `applied`.
- `tools/lane-912/`: `owed_direct_flood.mjs` is added after the FOLD-7 cell. Its wall was not measured here (the fold's
  max was 369 to 390 ms for 32 identities, plus its Miniflare start).

## What a further fold must change (one line each)

- **NEW5-M1:** the direct pass reads the tip fresh for a `tip` event, and the hook seeds the memo; the coverage
  records its tip and a later-tip delivery applies. Pin: RED on `e99472091`.
- **NEW5-M2:** retire unconfirmed evidence (TTL-sweep it), or date an Outpoint re-ask for an `unbound` row resting on
  it. Pin: a dropped spend restores the press.
- **NEW5-L1:** release-profile timing, or assert the page count only.
- **NEW5-L2:** the sent events returned to the caller, not an isolate thread-local; or count the discard.
- **NEW5-L3:** coalesce per identity with the pass's bound; race `ship_owed_direct` against the flush's deadline.
- **NEW5-L4:** carry `oldestUnappliedAtMs` across a build's replace.
- **NEW5-L5:** the object notes its first-apply visibility into the durable bars.
- **The cutover doc (bsv-low):** bar 2 is now direct plus queue; `OWED_CONSUMER_CONCURRENCY` moves with Phase C; the
  STOP is sampled one in eight.

## What the window, the lever pass, the cold rehearsal, Phase B's compare and the rollback rehearsal must read (amended from the fourth lens)

Kept from the fourth lens: items 1 and 3, item 6, the coverage, and the D1 rows an hour either side of the deploy.

Amended:

1. **The budget row**, replacing the fourth lens's item 2. Read `owed-actor:boot-budget` in beta's D1 during the
   lever's pass: `tickedAtMs` an INTEGER minute, and `cursor` reaching 60 and refusing. Read `bootstraps` rising at most
   60 pages a minute fleet-wide.

2. **The stuck actor**, replacing item 4. Read:
   - `/health.owedActor.holding {count, oldestMs}` falling to 0 after the bootstraps;
   - `bootstrapFaults` flat;
   - no `[owed-actor] bootstrap PARKED` line.

   Any PARKED line names a seat that serves no press until the lever clears it: a STOP.

3. **`flushedAtMs`** is an integer on this build (FOLD-8 L3). Read it from `/health`.

4. **Bar 2 (D2-ACTOR-D).** Read:
   - `rowsVisibleMsP95` under 1 s, beside `direct.walls / direct.calls` (L5: walled events are not in the p95);
   - `backstopLagMsP95` under 60 s;
   - `rowsVisibleDirectMsP95`;
   - `collectAfterMinedMsP95`, which is still the queue's (NOTE-4).

   A `walls` count rising during a burst is L3.

5. **Phase B's compare** (Phase A on this build). `comparedTotal` advances at about one event in eight. Read
   `diffsTotal` and `faultsTotal` flat. Then, for every `DIFF` line:
   - check whether its game's differing field is a gate or `probeTip` at the block just found (NEW5-M1: an ordinary
     block, not a divergence);
   - grep the tip hook's time against the DIFF lines' times;
   - read `heldDiffsTotal / comparedTotal` and `reconcileDiffs` beside them (NOTE-2).

   Until M1 is folded, a non-zero `diffsTotal` must be read field by field through `GET /internal/owed-actor-rows`
   before it is called a STOP or a pass.

6. **The live cold rehearsal**, as the fourth lens says. `coldMsP95` is read from a single cold actor, and
   `faultsTotal` and `bootstrapFaults` stay flat.

7. **The courier find (FOLD-9).** Read:
   - the age repair's `[owed-backfill] find evidence: N row(s) seeded` lines, until the cursor is `~`;
   - `GET /internal/owed-actor-rows` for `0247bf52…` (record 1484's identity), showing `diff: []` and equal digests;
   - `RECONCILE DIFF` lines: none of the `unbound` / `hop-stranded` class;
   - for NEW5-M2, any `unbound` row whose `chainSpender` has no `tx_words` row and no bytes after 8 days. That is a
     candidate dropped spend, listed for the operator.

8. **The rollback rehearsal**, as the fourth lens says, plus one read. After render 1 (`OWED_D1_JUDGE=true`), confirm
   that the direct calls go on (`direct.calls` rising) and that the consumer's deliveries are dupes. `comparedTotal`
   then advances on sampled seqs only.
