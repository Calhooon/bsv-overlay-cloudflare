VERDICT: NOT SOUND

# D2-ACTOR-F lens: overlay PR 250 (lane/D2-ACTOR-F, tip 3223be64), bsv-low #959 and #954

A from-scratch review of `git diff origin/main...3223be64`: 11 files, +1,053 / -43. In this PR the frame and the direct
call carry the writer's row, and a warm actor upserts it under a generation guard and judges with no D1 read. I read
bsv-low #959, the design's section 7 paragraph on D2-ACTOR-F, the lane brief, the PR body and the budget-tier comment,
then the diff file by file.

I ran two scratch pins against the PR's own test harness (`owed_actor::tests::d2_actor_f`, with `e_warm_seat`,
`published`, `mark`, `applied`, the real `apply_with` and the real publish statements under SQLite). Both were RED,
and both are quoted below. Neither pin is committed. The second pin's diff is at
`/Volumes/Lanes/out/bsv-low/D2-ACTOR-F-LENS/scratch-pins.diff`; the first pin's body is given in full under MEDIUM-1.

## Findings

### MEDIUM-1: the generation guard's `read` bound is replaced by a lower one, so a late, older carried row overwrites a newer row

`crates/low-app-layer/src/owed_actor.rs:1636-1643` (the narrowed path's `bound = marks...max().unwrap_or(0)`) and
`:1472` (the paged path, the same `unwrap_or(0)`), recorded at `:1797-1810` (`READ_BOUND_CLEAR_SQL` and then
`APPLIED_SQL` kind `read`). The guard reads that row at `carried.rs:42` (`HELD_GENERATION_SQL`).

- **The defect.** The `read` bound recorded for a game is the greatest generation among the marks *still pending* at
  the read, not the snapshot the read saw. It is also written by delete-then-insert, so it can go down. Once a game's
  marks are acknowledged (in Phase C the direct and consumer acknowledgements; in Phase B the D1 judge), the next warm
  narrowed apply or bootstrap page of that game reads the newest rows but records `read = 0`.
- **Failure scenario (one sentence).** A carried call published before a newer write lands after that write was
  applied and acknowledged and one more warm apply of the game ran. Its older rows pass the guard (`0 < g`) and replace
  the newer domain. The judge then derives from the old row, and the gate counts the older mark as applied, so nothing
  withholds.
- **Reproduced.** The scratch pin `lens_f_read_bound_regresses_under_an_acked_mark`:
  - Steps: two `network_seen` writes of one txid (`old`, then `new`), each marked and published by the shipped
    publish. Then the newer write applied by the narrowed read, its mark deleted (the acknowledgement), the same game
    applied once more by the narrowed read, and finally the older event's carried call.
  - Output: `LENS read rows after the no-mark apply: [0, 40, 40, ...]`, then `LENS carried=true seenAt=old`, and
    `assertion failed: LENS: an older carried row overwrote the newer one, left: "old" right: "new"`.
  - The PR's own `d2_actor_f_c` covers only a read bound taken while the newer mark is still pending.

  The first pin, in full:
  ```rust
  let (record, local, games) = e_warm_seat(20).await;
  let pot = h("c8", 7);
  // seen 'old' marked + published -> older; UPDATE seenAt='new', marked + published -> newer
  applied(&record, &local, &games[7..8], &newer, false).await;   // read = the newer mark's generation
  record.conn.execute("DELETE FROM owed_dirty", []).unwrap();   // the acknowledgement
  applied(&record, &local, &games[7..8], &newer, false).await;   // no mark pending: read = 0
  applied(&record, &local, &games[7..8], &older, true).await;    // the late carried call: seenAt = "old"
  ```
- **Why it matters for money.** Two carried families can withdraw a press in a newer write, and an older carried row
  undoes that withdrawal:
  - `tx_words`: #598's `trackerTerminal` refusal of a funding txid. The older non-terminal word would bring the refund
    press back.
  - `network_seen`: a SEEN latch the eviction deletes. The older row would bring back `fundingHeld: seen`.
- **The window, stated.** It is narrow. It needs a direct call still in flight after the newer write's apply, its
  acknowledgement and a further warm apply. A dropped direct-call future does not abort the Durable Object request,
  and under the flood the turn wait is 765 ms at p95. It stays until the game's next apply. The older event's own
  queue delivery heals it only if that delivery comes after the stale call.
- **Fix.**
  1. Record the read's bound as the record's AUTOINCREMENT high-water of `owed_dirty`
     (`(SELECT seq FROM sqlite_sequence WHERE name='owed_dirty')`, in the same statement as the read), never the
     pending marks' maximum.
  2. Never lower it: drop `READ_BOUND_CLEAR_SQL` and keep the greatest per game.
  3. Commit the reproduction above as a pin.

### MEDIUM-2: the carried path treats every key the mirror names as already read, but a faulted apply leaves mirror rows whose keys were never read

`crates/low-app-layer/src/owed_actor.rs:1668-1673` (`fetched = keys.clone()`, whose comment says "each apply ends at a
fixpoint over what it read") and `:1699-1702` (the carried round reads nothing).

- **Why the comment's premise is false.** Two writes persist mirror rows before the fixpoint, and the HIGH-2
  snapshot-and-restore covers neither of them:
  - `write_round` runs once per round (`:1724-1731`).
  - `carried_upsert` (`:1895-1934`) writes before the closure.

  So an apply that faults after one of those writes leaves rows that name keys no read covered: a transient D1 fault on
  the miss read, the closure bound, or a commit fault.
- **Failure scenario (one sentence).** A carried collected filing whose miss read faults leaves the marker row (naming
  `payTxid`) in the mirror. The next carried apply of that game makes zero record calls and judges without any family
  keyed by `payTxid`, and its exact marks then open the read's gate.
- **Reproduced.** The scratch pin `lens_f_a_faulted_carried_apply_leaves_keys_never_read`:
  - Steps: a collected filing with a `tx_words` row for its `payTxid` in the record. The first carried apply runs
    under a record port that faults every batch; the second runs under the real record.
  - Output: `LENS first (record faulted): Err("D1_ERROR: lens transient")`, then
    `LENS second: carried=true calls=0 payTxid tx_words rows in the mirror=0`, and
    `assertion failed: LENS: the payout's word was never read`.
  - On this fixture the stored judgement still equals a cold apply's (`LENS judged equal to cold: true`), because its
    row type does not read that word. A family the judge does read (a refusal word, a `submit_refusals` row, a
    spender's facts) would be judged absent.
- **Healing.** The narrowed read (main's direct path) reads the mirror's whole key set and heals this. The carried path
  does not.
- **Fix.** Take the carried path only when the mirror is known closed:
  1. Clear a `meta` flag (or a per-game stamp) as the first local write of every apply, before `carried_upsert` and
     before any `write_round`.
  2. Set it only in the fixpoint commit's batch.
  3. With the flag clear, take the narrowed read.

  Pin the reproduction.

### LOW-1: on the direct call, the carried apply drops FOLD-11's refresh of the unmarked families, while the gate now counts its marks as applied

`owed_actor.rs:1604` (the warm narrowed read is skipped when `carried_applied` is set).

- **Failure scenario (one sentence).** After an unmarked write (a reorg clear of confirmed evidence,
  `TRANSACTION_UNPROVE_SQL`, the TTL sweep), a carried event judges the game from the unrefreshed mirror. On main the
  direct call's narrowed read refreshed it. The served judgement stands until the event's queue delivery, if that comes
  after.
- **How bad.** The carried apply makes nothing worse than the judgement that stood before it. But CLAUDE.md's FOLD-11
  rule ("a warm game's judgement after ANY write equals the D1 judge's over the full closure") is no longer true of the
  direct call. carried.rs states this, but CLAUDE.md and the design paragraph do not.
- **Fix.** State the exception at FOLD-11's rule in CLAUDE.md and in the design paragraph.

### LOW-2: the lane's bars are not measured on the real path, and the flood shape #959 measured likely still reads once

PR body, under "Measured": natively, a counting port.

- **What was measured.** Record-call counts only, through a fake port. There is no Durable Object fetch or alarm, no
  wasm32 run and no stage latency.
- **Why the flood likely does not move.**
  - #959 closes on `readMs` 0 for a warm game and green direct bars under the flood cell.
  - By the PR's own pin `d2_actor_f_b`, a new filing still makes one record read, because its marker and payout txids
    are new keys. A door flood is new filings.
  - The repo's own flood cell publishes `identity` events, which carry nothing.
- **Failure scenario (one sentence).** The lane lands, and the flood's read p95 stays about 719 ms, because every
  filing in it misses once.
- **Fix.** A workerd cell that publishes carried filings, with `readMs` asserted, before claiming the bar. Or carry the
  new keys' rows (the stated next step).

### NIT-1

`crates/low-app-layer/src/owed_events.rs:2621`: `pub acked: u64,    /// D2-ACTOR-F ...`. The doc comment sits on the
previous field's line. rustfmt will move it.

### NIT-2

`owed_events.rs` `coalesce_direct` attaches the identity's whole `Carried` to every 64-game chunk body. Each chunk's
`carried_upsert` then returns `Err` ("names no game of the apply") for any item of another chunk, logs a line and falls
back to the narrowed read. That is safe, but the carried path is lost for any identity past 64 games in one pass.
`apply_burst` already filters items per chunk; do the same here.

### NIT-3

`READ_PARTY_SQL` (`crates/overlay-cloudflare/src/owed_events.rs`) takes its `hop_pot_links` domain from the current
rows' `potTxid`, but `derive_party_update` re-derives the OLD pot's links. A party UPDATE that moved `potTxid` would
carry an incomplete domain. No writer does that today; state it at the constant.

### NIT-4

A coalesced `/apply` body can carry up to 64 events × 32 KB, about 2 MB, and so can the overlay's
`/internal/owed-direct` body. That is within the platform's limits but not stated beside `PAYLOAD_MAX_BYTES`.

## The eight answers

### 1. Authority

**Who can write a row into a mirror.** Only the publish's own D1 read can produce a payload. `row_and_payload!` and
`READ_PARTY_SQL` take the rows from the record in the statement that reads the pair's marks
(`overlay-cloudflare/src/owed_events.rs:83-120`, `carry_payload` at `:518-540`). The payload reaches the actor three
ways:

- the app layer's direct pass;
- the overlay's `POST /internal/owed-direct`, behind the internal bearer;
- the actor's `/apply` over the binding.

The object takes `carried` only when the body has no `bound` (`object.rs:867`), so the queue's delivery never carries.
A stranger's filing can only put into a mirror what the door already wrote to D1. The carried path adds no authority.

**Validation.** `carried::item_statements` refuses:

- a table outside `FAMILIES`;
- a BEEF family;
- a key column the table lacks;
- a key tuple of the wrong width;
- a row whose columns are not exactly `__rowid` plus the mirror's columns;
- a row outside its own domain.

**The generation.**

- Minted by `owed_dirty`'s AUTOINCREMENT in the writer's trigger transaction.
- Taken as the greatest of the pair's marks read in the same statement as the rows (`Carried::of_event`,
  `carried.rs:96-100`).
- Compared in `HELD_GENERATION_SQL` (`carried.rs:42`) against `payload-gen` for the domain and `read`/`covered` for the
  games.
- Stored as `applied` rows (`record_statements`, and `owed_actor.rs:1797-1810`).

**Two writers racing.** Two carried writes of one domain are ordered correctly by `payload-gen`. A carried write
against a narrowed read is ordered by `read`, except that `read` can regress (MEDIUM-1). When the two disagree, the
payload is trusted over the durable row until the next record read of that domain.

### 2. Completeness

**How "holds the closure" is decided.**

- The game is warm: its whole closure was read within `FULL_REFRESH_EVERY_MS` (`WARM_GAMES_SQL`, `:1550`).
- Every key the mirror names is taken as read (`fetched = keys.clone()`, `:1672`).
- A key the carried rows newly name is read in round 2, in one record call (`d2_actor_f_b`).

**Where it can fail.** A family absent for a key the mirror names goes unnoticed when an earlier apply faulted
(MEDIUM-2, reproduced). The unmarked families (`hop_chain_probes`, `transactions` and `pot_beefs` flags, released
evidence, a word's untriggered columns) are not refreshed by a carried apply (LOW-1). The narrowed read refreshed them.

**The cold miss.** A cold game, a page, no `touch`, or any item the mirror cannot take all go to the narrowed or full
read. That is one record call per round, as before. A carried warm apply is 0 calls, or 1 when keys are new. The PR's
"never two" holds only when round 2's rows name no further unread key; otherwise there is a third round, as on main's
path.

**What a mirror wrong about its completeness produces.** A judgement missing those rows, stored and served, with the
carried marks counted applied at the read.

### 3. Byte identity

**What pins it.**

- `d2_actor_f_a` and `_b` compare the carried apply's stored rows with a cold full apply's, byte for byte, on the
  fixture.
- `d2_actor_f_e` pins every carrier's row columns against `schema::columns`.
- Every integer column the carriers hold is declared INTEGER in the mirror. So a REAL that `json_object` renders (for
  example `….0`) is stored back as an integer, the same as the normal read.

**What does not.** `fix986_d`, `fix997_a`, the compare's digests and the frozen fixtures never take the carried path,
so they say nothing about it.

**What the payload does not carry.** These come from the mirror or a miss read:

- `pot_records` and its evicted twin;
- `hopparty_records`;
- `pot_hop_seats`;
- `released_hop_evidence` and `released_hop_evidence_missing`;
- `pot_evictions`;
- `hop_chain_probes`;
- `owed_spender_bytes`;
- `hand_markers`, `proof_markers`, `proof_posts`;
- the BEEF families' `txfacts`.

### 4. Delivery

- **A carried frame twice.** Every item is skipped by `payload-gen`. The marks are re-recorded (`ON CONFLICT DO
  NOTHING`) and the judge re-runs over the same mirror, producing the same rows.
- **Out of order.** Guarded by `payload-gen` or `read`, except for the regression in MEDIUM-1.
- **After a narrowed direct apply.** Guarded by `read` while the newer mark is pending (pin `c`); not guarded once it
  is acknowledged (MEDIUM-1).
- **The queue delivery after a carried apply.** Never a dupe, because a carried apply records no `covered`. It takes
  the narrowed read, covers the event and acknowledges it.
- **A frame without a row** (an old producer, a replay, a word frame, `pot` / `hop` / `eviction` / `beef`).
  `carried_subjects` returns `None`, so the marks are read and the narrowed read runs. This is pinned by
  `d2_actor_f_a`'s main road (1 call) and by `d2_actor_f_d`.

### 5. Bounds

- **Payload size.** Capped at 32 KB (`PAYLOAD_MAX_BYTES`, `owed_events.rs:510`). Past the cap nothing is carried.
- **BEEF.** Never carried: no BEEF table is a carrier, and `item_statements` refuses BEEF families.
- **A party with many seats.** One marker's rows plus its pots' links; the cap bounds it.
- **Mirror growth.** Per carried event per game: one `payload-mark`, one `payload-gen` and one `read` row in
  `applied`, pruned after a week. Carried `hop_pot_links` are already in the mirror's `potTxid` domain.
- **Budget tier** (PR comment, run 6, 352 PASS at 3223be64):
  - `record-potparty-filing`: reads 4/7, writes 13/16, statements 4/6.
  - `submit-admit-1`: reads 6/8, writes 22/27, statements 11/12, largest statement 2/2.
  - The two ceilings the branch went red on (4 > 2 and 14 > 12) were fixed on the branch.
- **Cost at 33 pairs.** Not measured. Each carried event is at most 32 KB more on the queue, and the direct body grows
  with it (NIT-4).

### 6. The bars

**Where they are served.** The stages are the existing `readMs`, `waitMs`, `judgeMs`, `writeMs` and `outsideMs`. The
carried upsert's time is added to `writeMs`, and `answer_of` gains `carried`. No new served bar.

**What the lane claims.** Record-call counts only (0 warm, 1 for new keys), measured natively through a counting port.
There is no Durable Object fetch or alarm, no workerd cell, no wasm32 run, and no apply or first-visibility p95 under
the flood fixture (LOW-2).

### 7. Bootstrap and reconcile

Unchanged:

- Pages never carry (`opts.page.is_none()`, `:1576`).
- Held events drop `carried` (`skip_serializing`).
- `reconcile_apply_body` carries none.
- The fresh compare never calls a carried apply.

**Row shape.** A carried apply commits through the same `commit_fixpoint`, so its rows carry `judgedAtMs` and the
same row shape. The compare reads the same shape.

**`read_generation`.** A carried apply answers `None` (`:1772-1775`), so the reconcile's held partition is uncut for
it. That is FIX-960's absent rule.

### 8. Anything else

- No panic path: every parse is `?` / `Option`.
- The fallback log prints the error text and table names only, never a row body.
- No unbounded loop: the carried round is the closure's round 0, still under `CLOSURE_ROUNDS_MAX`.
- **Served shapes, both additive:** `/rows` gains `coveredExact`, and `/apply` gains `carried`.
  - `Snapshot::applied_by_actor` is safe: AUTOINCREMENT never reuses a generation, so an exact mark matches only its
    own mark.
  - Its premise depends on MEDIUM-1 and MEDIUM-2.
- **Phase C.** Carried bodies now skip the direct acknowledgement, so the marks wait for the consumer. With
  `coveredExact` the read does not withhold for them. `owed_events_direct_acked_total` stays flat for carried events.
  This is correct; it should be stated beside FIX-956's sweep paragraph.
- `event_txids` correctly strips `payload` and `subjects`, so the narrowed read's touch is unchanged.

REPORT: STATUS done
REPORT: VERDICT NOT SOUND
REPORT: FINDINGS HIGH 0 MEDIUM 2 LOW 2 NIT 4
REPORT: DOC docs/audit/D2-ACTOR-F-lens-2026-10-31.md
