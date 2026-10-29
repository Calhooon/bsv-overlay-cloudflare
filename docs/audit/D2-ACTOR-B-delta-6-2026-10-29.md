# D2-ACTOR-B-DELTA-LENS-6: the delta lens on FOLD-10 (PR 212) and D2-ACTOR-E (PR 213; bsv-low #912, #952, #953, #954)

Written 2026-10-29 by lane D2-ACTOR-B-DELTA-LENS-6 (Wave E, row E3). This is a review document only. No product code
changed. This PR is for the captain to copy and close; it is not to be merged.

What was reviewed: the combined delta `e99472091..26f7443c6`, judged cold.
- `663a31d09` is FOLD-10 (PR 212).
- `26f7443c6` is D2-ACTOR-E (PR 213), rebased onto FOLD-10.
- The land log (`~/low-logs/wave-e/lands/overlay-213.log`) says the head was already on main and nothing was
  rebased: the landed tree is `26f7443c6`.

The diff covers 17 files under `crates/low-app-layer`, `crates/overlay-cloudflare` and `tools/lane-912`, +2,888 / -109.

What it was read against:
- the fifth lens (`D2-ACTOR-B-delta-5-2026-10-28.md` in bsv-low);
- both briefs, and item 8 as messaged;
- design sections 5 to 7;
- the cutover doc;
- issues #954, #955 and #956;
- both lanes' REPORTs.

## VERDICT: LAND (0 HIGH, 2 new MEDIUM, 2 new LOW, 3 NOTE)

**FOLD-10** closes both of the fifth lens's MEDIUMs and all five of its LOWs, plus the captain's item 8. Its build
stamp (`judgedLibRev`) is written at a bootstrap's end only. That is the right place, and it errs only toward one
extra re-judge.

**D2-ACTOR-E** does what #954 asked:
- the D1 statements per apply and per page;
- one judge pass for a burst;
- one delivery per identity for a batch.

It makes two judgement-level changes the lane listed as "unverified".

**NEW6-M1 is the first, and the north star's question.** A warm game's narrowed apply re-reads only the rows its
event and its pending marks name. Nine of the 24 mirrored families are written by statements no trigger marks. After
such a write, the actor's judgement differs from the D1 judge's over the full closure. Run, not reasoned: the
overlay's reorg clear, then an ordinary event, leaves the actor `unbound` with no press while D1 serves the
index-unspent `hop-stranded` press. The pre-E read (the whole closure) agrees with D1. Neither judge errs on money in
that run, but the two judges disagree, which is the Phase B STOP. The sampled compare and the reconcile both see it.
Fold it before Phase B.

**NEW6-M2 is the second.** The coalesced consumer batch has no subject bound and one 60 s wall that notes nothing,
which bypasses three earlier folds in the backstop path the flood leg exercises. Fold it before the flood leg is read
as a bar.

**Phase A may proceed on this main:** the window, the lever pass and the cold rehearsal. The rows D1 judges are still
the ones served. Expect NEW6-M1 to show as real `diffsTotal` after a reorg clear, a proof refutation or an opponent's
filing. Do not read the compare as flat until M1 is folded.

| Item | Verdict | One line |
|---|---|---|
| 1 NEW5-M1 (the tip) | **FIXED** | The hook seeds the memo (`internal_events.rs:691`), a `tip` event's direct pass reads fresh (`owed_events.rs:1862`, `direct_reads_fresh`), and coverage records its tip (`covered-tip`, `COVERED_SQL` at `owed_actor.rs:627`). `fold10_a` and `fold10_b` are green here, RED per the lane on `e99472091` with the fix inert. Other kinds still read the isolate's 30 s memo (NOTE-1): both judges read it alike, so this is no diff. |
| 2 NEW5-M2 (unconfirmed evidence) | **FIXED** | `rests_on_unconfirmed_find` dates the row (`owed_judge.rs:897`) and asks the OUTPOINT (`owed_belt.rs:317`). Backoff runs 10 min doubling to 6 h, 3 calls an outpoint, 24 a tick: bounded per hop. The row stays `unbound` while couriers say spent. An unspent look clears the evidence and the press returns in that find's batch (`fold10_c`), so up to 6 h after the drop (it was 2 h before FOLD-9). The TTL sweep is unchanged and spares evidence while the hop stands. `d2fold_high3_b` now covers the confirmed case, and `fold10_c` pins the unconfirmed one. |
| 3 Item 8 (`judgedLibRev`) | **SOUND, with L2** | The stamp is written only at `BootStep::Done` (`object.rs:1127`). `arm_bootstrap` re-arms on the lever and after an applied event, and is counted once per actor per build (a running re-judge keeps its cursor and is not re-counted). The rows served meanwhile are the old build's (no `pending`). No event is held during a re-judge (`bootstrapped` stays set), so FOLD-7's hold rule never applies there. The opposite lie costs one extra paced bootstrap and is harmless. Counters are 28 in both Workers (27 after FOLD-10, plus E's `closure_misses`). |
| 4 Fifth lens's LOWs, faults logged | **FIXED (L2 partly)** | L1: the page count is asserted always, the per-page work in release only (`fold10_d`). L2: `take_direct_where` takes the consumer's own pair only, and the belt calls its own set (`direct_held`); a writer publishing the consumer's very pair in the same isolate still loses its direct call (stated). L3: `coalesce_direct` builds one call per identity, and the overlay's hand-off shares one wall (`change_flush.rs`). L4: `oldestUnappliedAtMs` is carried across a replace (`fold10_g`). L5: the object notes its own first-apply visibility, `noted: true` (`fold10_h`). Item 8: the first fault's text is logged once a minute (`fold10_i`). |
| 5 D2-ACTOR-E (#954) | **NOT SOUND at the judgement (NEW6-M1); cost SOUND; M2, L1** | The statements per warm apply (25 to 2) and per page (50 to 3) hold as read. The compound statement is 10,488 bytes (D1's bound is 100 KB), at most 30 arguments a call, with no compound SELECT on the main path and a TOOBIG fallback. Burst answers are exact (below). The consumer coalesces per identity (M2, L1). Cold re-publishes: an event born before the bootstrap stamp samples nothing warm. No D1 wall-time measured. |
| 6 Together | **CONSISTENT** | E kept FOLD-10's `covered-tip`, `take_direct_where`, `coalesce_direct` (adding txids) and the tip's fresh read (`consume_batch` reads fresh when any message is a tip). Neither lane moved the digest: `schema.rs` and `ACTOR_TABLES_VERSION` (`applied-per-game-1`) are unchanged, and E's `full` stamps are `applied` rows. The lane-912 cells name the routes the code serves; E added `owed_burst_one_identity.mjs`. Neither lane touched the sweep (`SWEEP_AFTER_MS` = 120 s), so #956 stays open (NOTE-3). |

## How this was verified

**Build.** Worktree `~/bsv/wt-overlay-actor-b-lens` at `26f7443c6`, `CARGO_TARGET_DIR=/Volumes/Lanes/targets/actor-b-lens`.
The host load average was 12 to 14. Evidence is in `/Volumes/Lanes/out/wt-overlay-actor-b-lens/delta6/`.

**The two lanes' pins** (`fold10_` and `d2_actor_e_`, the lib test binary run directly): 19 passed, 0 failed, in 26 s.

**Scratch pins.** Both were appended to `owed_actor/tests.rs` and removed after the run; the output is in
`delta6/lens6-scratch-unmarked-write.txt`.
- **NEW6-M1:** the scratch pin FAILS as quoted below.
- **Statement length:** `aggregate_select(true, all)` is 10,488 bytes.

**Not run:**
- the four lib pieces whole;
- the 54 integration binaries;
- clippy;
- the budget tier and the workerd cells (`owed_burst_one_identity.mjs`);
- the base build for the lanes' RED-on-base claims (they are stated as each lane stated them);
- anything on wasm32 or beta's D1.

## New findings

### NEW6-M1 (MEDIUM, fold before Phase B): the narrowed apply diverges from the D1 judge after any write no trigger marks

**Where:**
- `crates/low-app-layer/src/owed_actor.rs:1214-1265`, the `warm` and `narrow` block of `apply_with`. A game whose
  last full read is under `FULL_REFRESH_EVERY_MS` (`:1088`, 600 s) old, applied with `touch`, reads txid-keyed
  families only for the event's txids plus the txids of its pending marks' source keys (`NARROW_MARKS_SQL`). Every
  other key the mirror holds is treated as already fetched (`fetched = keys.clone()` on the first round), so its row
  is never re-read.
- Phase A's deferred and inline apply gets `touch` from `marks_bound_keys` (`owed_judge.rs:691`). The direct pass,
  `consume_batch` and the burst get it from the events.

**Which families no trigger marks:** 9 of the 24 in `FAMILIES` (`owed_actor.rs:119`).
- `transactions`. Its proof flips have no trigger: `TRANSACTION_UNPROVE_SQL` and `MARK_TX_PROVEN_AT_SQL` at
  `overlay-cloudflare/src/d1_storage.rs:243` and `:251`.
- `hop_chain_probes`. The reorg clear `HOP_PROBE_MEMO_REORG_CLEAR_SQL` (`hop_probe_memos.rs:82`) deletes CONFIRMED
  evidence and the `proofmined:` latches fleet-wide, with no mark. The TTL sweep also has no mark.
- `hand_markers`, `proof_markers`, `proof_posts`.
- `pot_records_evicted` and `potparty_records_evicted`. These are reached through the eviction's mark.
- `hop_pot_links` and `pot_hop_seats`. These are re-read through the mirror's markers on every narrowed apply.

There is also a cross-identity case: a marked table that marks only its own subject. `PARTY_INSERT` marks only the
filer (`own_subject!`), so an opponent's party row, which the actor reaches through `potTxid`, changes with no mark on
this identity's games.

The D1 judge reads every one of these fresh on its next judgement of the game, whatever mark triggers it.

**The failing input (run on `26f7443c6`).**
1. `fold9_fixture` holds beta's #953 hop: the index row is unspent and the evidence `<hop>:0` is CONFIRMED.
2. The D1 judge runs, then the actor's full apply (the game is now warm). Both judge `unbound`, no press.
3. The overlay's reorg clear runs. It deletes 1 row and leaves 0 marks.
4. One ordinary event follows in the same game (a word on another txid). The D1 judge runs `judge_subjects`, and the
   actor runs `apply_with(touch = [that txid])`.

The results:

```
d1           ["hop-stranded", 1, "index-unspent", null, null, false, null]
actor        ["unbound", null, "spent", "6f09...2282", null, true, "the hop was spent by a transaction ..."]   narrowed true, misses 0
full closure ["hop-stranded", 1, "index-unspent", null, null, false, null]   (apply() with no txids: the read before D2-ACTOR-E)
```

**What it means:**
- **Not a 10-minute lag.** The lane calls this a lag of up to 10 min, and the scratch shows otherwise. The narrowed
  apply stores a judgement made over stale rows, and that judgement stands until the game's next apply after the
  window (a full read) or the reconcile. For a quiet game that is unbounded. Before D2-ACTOR-E the same event re-read
  the whole closure and agreed with D1.
- **Phase A** serves D1, so nothing wrong is served. The compare reads a REAL diff, because no mark sits above the
  bound: the unmarked write left none. The compare samples one event in eight, and the reconcile catches it (its
  re-apply is a full read). So the STOP fires on ordinary operations: a reorg clear, a stitched bump refuted or
  latched, an opponent's filing.
- **Phase B and C** serve the actor's row:
  - **The run** withholds the honest D-H1 press (the safe direction).
  - **The reverse direction is reachable** (reasoned, not run). Take a filed sweep whose payout is claimable on
    `transactions.has_proof` (`hydrate.rs:780`). The engine's refutation (`TRANSACTION_UNPROVE_SQL`) withdraws it in
    D1, and the warm actor keeps serving it.
- **Phase A, one more path.** A deferred apply that the platform cuts out of `wait_until` loses its narrowed refresh
  for good, because its marks were already acknowledged by the D1 judge. Before the fold, the game's next apply
  re-read everything.

**The fix (one line):** in the narrowed read, read the UNMARKED families over the game's whole key set (still one
statement; the marked families over the event's keys); or have every unmarked writer (the reorg clear, the
unprove/prove latch, the party insert's opponents) write a mark or end the game's warmth. Pin: the scratch pin's shape,
RED on `26f7443c6`.

### NEW6-M2 (MEDIUM, fold before the flood leg is read as a bar): the coalesced consumer batch is unbounded and its wall notes nothing

**Where:**
- `crates/low-app-layer/src/owed_events.rs:1212` (`consume_batch`);
- `:1298-1390` (the batch arm of `queue`);
- `wrangler.toml` (`max_batch_size = 16` in both renders).

**What is wrong:**
- **No subject bound.** Each message's plan is bounded (`ROUTE_READ_LIMIT`, 64 games a message). `Plan::merged`
  sums up to 16 of them: up to 1,024 subject games for one identity, judged as 64 chunks of D1 judge plus actor
  apply. All of that runs under ONE `CONSUMER_MESSAGE_WALL_MS` (60 s).
- **The wall notes nothing.** Past the wall, all 16 messages are handed back after 60 s with no subject memo and no
  pair memo. The single-message arm has both: `note_wall_subjects`, the delta-2 lens's LOW-1, and `note_fault`.
  - So a subject that walls is never HELD (NEW-M1 and NEW2-M1's hold never arms).
  - Every message batched with it walls too. After `max_retries` (3) they go to the DLQ, and the sweep re-publishes
    their marks into the same batch shape.
  - Progress is partial: what the judge committed before the wall is acknowledged. A slow subject at the front of the
    merged order starves the other 15.
- **Pair memos are not honoured or are wrongly cleared.** The batch arm resolves EVERY message's pair
  (`resolved_statements`, `:1325`) on the MERGED routing (`subjects > 0 || held == 0`). A message whose own subjects
  were all held has its pair's fault memo and dead letter cleared, which the single-message rule ("one whose every
  subject was held resolves nothing") forbids. A held pair's memo is not read before the batch either.

The `Err` path falls back to one message at a time, which is sound. Only the wall and the resolve are not.

**The fix (one line):** cap the merged plan at one message's bound (64 games an identity, or the 256 marks), note the
in-flight subjects at the batch's wall as the single arm does, and resolve each message by its OWN plan's routing.
Pin: 16 messages for one identity whose first subject never answers. They should end with that subject held and the
other 15 judged; on `26f7443c6` all 16 are walled.

### NEW6-L1 (LOW): a coalesced delivery samples by its OLDEST event, so the backstop bar can read better than the truth

**Where:** `owed_events.rs:1187` (`coalesced_event`) gives the actor one `EventCtx`, the oldest message's kind, seq
and write time. The object computes `firstApply` and `warmAtBirth` on that event alone (`object.rs:876-886`). The
other events' `(kind, seq)` are never recorded in `applied` by this delivery.

**The effect.** Take a batch whose oldest event was applied by its direct call (the common case) or born before the
bootstrap stamp. That batch answers `firstApply: false` or `warmAtBirth: false` and samples NOTHING. The others in the
batch may be events whose direct call faulted: the backstop's own tail, the very thing `backstopLagMsP95` measures.
Their first apply happens here, unsampled.

The lane's statement ("one figure, its oldest event's, the batch's worst") holds only when the oldest event is itself
a first apply. Otherwise the bar under-samples the tail and can read green while the tail is red. This answers the
lane's unverified item 3: yes.

**The fix (one line):** pass the batch's events as `also`, and sample per event (each one's own first-apply and birth),
or sample the oldest FIRST-APPLY event.

### NEW6-L2 (LOW): every deploy re-judges the fleet through the cold bootstrap's budget, with no priority

**Where:** `lib_rev()` (`owed_actor.rs:3338`) is the build's git HEAD. `rejudge_due` therefore fires for every
bootstrapped actor on every app-layer deploy, after its next event or the lever. Each re-judge takes pages from the
fleet's one budget, 60 a minute (`boot_pace`, `owed_actor.rs:684`, ordered by hash slot).

**The effect.**
- A genuinely COLD seat (a new player, held events, the read display-only or a 503 `pending`) competes for pages
  with every re-judging actor. After the window's lever pass arms the fleet, the cold seat's 38 pages for 600 games
  share 60 pages a minute with the whole fleet's re-judge.
- Meanwhile a re-judging actor serves the old build's rows. An actor with neither an event nor the lever keeps them
  indefinitely, which is #955's case narrowed rather than closed.

**The fix (one line):** take a cold bootstrap's page before a re-judge's (a priority in `boot_pace`), and stamp
`judgedLibRev` from a judge-version constant moved only when the judgement changes, not from git HEAD.

## NOTEs

**NOTE-1: the tip memo across isolates.** `seed_tip` reaches only the isolate that serves `/internal/tip-changed`.
- A non-`tip` direct call or consumer delivery in another isolate, up to 30 s after a block, judges at the previous
  height.
- In Phases B and C it records `covered-tip` at that lower height, re-judging a game the block's own delivery judged
  at T. A gate press opened at T can close until the actor's tip alarm or the game's next apply.
- The D1 judge reads the same memo, so this is no diff. It predates both lanes (lane A's LOW-B).
- A floor would remove it: never judge a game below its stored `probeTip`.

**NOTE-2: the lane's unverified item 2 (Phase A's narrowed apply covers nothing).** No ordering applies an older row
over a newer one:
- the actor never writes event-carried rows, only D1's rows as read inside the object's turn;
- turns serialize;
- a later turn reads later.

What is lost is only D2-ACTOR-D's dupe in Phase A: a warm consumer delivery is re-applied narrowly. The burst's
answers are exact:
- each item's seq check and first-apply verdict are read before the pass;
- its `applied_games` are its own games;
- a fault in any chunk answers every item `Err` (no acknowledgement);
- each caller's bound was read before it queued, so before the pass read.

**NOTE-3: #956.** Neither lane touched the sweep (`SWEEP_AFTER_MS`, one belt tick, 120 s). It still re-publishes
every mark left un-acknowledged for more than 120 s. On a quiet beta with the consumer coalescing, a backlog's
re-publishes now land in the same batch as their originals, and the merged plan absorbs them (one judge pass an
identity). That makes them cheaper, not fewer. #956 stays open.

## What a further fold must change (one line each)

- **NEW6-M1:** the narrowed read reads the unmarked families over the game's whole key set, or every unmarked writer
  marks or ends warmth. Pin: reorg clear, then an ordinary event, narrowed equals D1.
- **NEW6-M2:** cap the merged plan at one message's bound, note the in-flight subjects at the batch wall, and resolve
  each message by its own plan.
- **NEW6-L1:** a coalesced delivery records every event in `also` and samples per event (or by its oldest first apply).
- **NEW6-L2:** cold pages before re-judge pages in `boot_pace`; `judgedLibRev` from a judge-version constant.
- **NOTE-1 (optional):** never judge a game below its stored `probeTip`.

## What the window on this main must read

Kept from the fifth lens: the budget row, the stuck actor, the coverage, and the D1 rows an hour either side of the
deploy.

| Bar | Cell or read | Figure |
|---|---|---|
| Apply, warm hand | `/health.owedActor.applyMsP95` on one warm hand (pair 20) | under 300 ms (#954: 950) |
| First visibility | `rowsVisibleMsP95` under `spec-owed-door-flood` and on one warm hand | under 1 s (#954: 9.2 s direct, 177 s flood) |
| Backstop | `backstopLagMsP95` on the flood leg | under 60 s (#954: 199 s, 273 s). Read it beside `direct.faults` and the queue's `consumedTotal`: NEW6-L1 can hide the faulted tail. |
| Direct walls | `direct {calls, walls, faults}` on the flood leg | walls are the hands' own, near 0 (#954: 64 of 164) |
| Coalescing | `owed_events_coalesced_batches_total`, `owed_events_consumer_wall_drops_total` | coalesced rising; wall drops 0. Any wall drop on the flood leg is NEW6-M2. |
| Closure misses | `owed_actor_closure_misses_total` | flat on a warm hand |
| Cold rehearsal | `coldMsP95` for a 600-game seat | under 60 s. Not in the hour after a deploy's lever pass (NEW6-L2). |
| Re-judge | `/health.owedActor.rejudged` after the deploy and the bootstrap lever's pass (not the reset lever) | equal to the bootstrapped fleet. `coverage` complete and contiguous. |
| Compare | `diffsTotal` against `heldDiffsTotal` and `comparedTotal`; `reconcileDiffs` | flat. A real diff after a reorg clear, a proof refute or an opponent's filing is NEW6-M1, not noise. Phase B is gated on it. |
| Evidence | `hop_chain_probes` rows `<txid>:<vout>` with `spentConfirmed` NULL, and their `beltDueAtMs` | each dated; none standing past 6 h without a look (NEW5-M2's fold) |
