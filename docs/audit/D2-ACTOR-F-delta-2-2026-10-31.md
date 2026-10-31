VERDICT: NOT LAND

# D2-ACTOR-F second delta lens: overlay PR 250 after fold 2 (006e6f24 → a0834e27), bsv-low #959 and #954

**What was read.**
- The first delta lens, whole.
- `git diff 006e6f24...HEAD`, file by file: 15 files, +1,217 / -172.
- The carried path as it now stands: `owed_actor.rs`, `owed_actor/carried.rs`, both crates' `owed_events.rs`, the
  direct route, the belt's drain.
- The fold's evidence in `/Volumes/Lanes/out/bsv-low/D2-ACTOR-F-FOLD-2/`.

**What was not done.** Nothing was run in this lane: no test, no tier, no cell. Every figure below is the fold's own
log, or arithmetic from the statements as written.

**Verdict.** The first lens's two MEDIUMs are closed as written:
- MEDIUM-1: no statement on the publish reads a BEEF, and the closure builds no value past D1's bound.
- MEDIUM-2: the door's statement is `read_sql`'s again.

The fold moved the closure read from the door to the direct pass. That brings two new MEDIUMs.
- **The closure is read for EVERY carried event.** This includes warm SEENs, words and refusals that need nothing. The
  reads run one after another, before any actor is called. So the D1 round trip that #959 measured is still on the
  write-to-visibility path; it has moved out of the actor's served `read` stage, which now shows 0.
- **The sweep pays the same cost inside the belt alarm.** That breaks the statement figure NEW9-L1's alarm bound rests
  on.

## Findings

### MEDIUM-1: the direct pass reads a 25-statement closure for every carried event, warm or new, one event after another, before any actor call; the D1 read is moved, not removed, and no served figure shows it

**Where.** `crates/low-app-layer/src/owed_events.rs:2909-2915`:

```rust
if ev.facts.get("payload").is_some() {
    report.carried += 1;
    if let Some(c) = port.closure(&ev.facts).await { ... }
}
```

- This is inside the per-event `for` loop. The actor calls (`join_all`, `:2945`) start only after the loop ends.
- `WorkerDirect::closure` (`:3014`) always calls `owed_actor::read_closure`.
- `closure_statements` (`owed_actor.rs`, the fold's new function) answers `None` only when the payload names no key.
  Every carrier row names at least its own txid, so every carried event reads a closure: 1 high-water statement plus
  24 family statements, in one D1 batch.

**What it costs.**

1. **A warm SEEN, word or refusal pays a batch it never uses.** These are the carriers the first lens's LOW-1 showed
   name no new key once the key rule excludes `proof_root` and friends. Their closure keys are the event's own txid,
   which a warm mirror already holds, so `closure_round` never runs and the closure is discarded.
   - The batch still reads every family of that txid.
   - It reads the stored BEEF's hex (up to 512 KB) and decodes it in the app layer.
   - `d2_actor_f_a` (`owed_actor/tests.rs:14356`, the PR's headline "a warm carried apply reads no D1") times the
     actor's apply alone. In production the same SEEN now costs one D1 batch in the direct pass. On `3223be64` it
     cost none, and on main it cost one compound read.
   - Every tracker word the `/internal/tx-word` ingest publishes is such an event.
2. **The read sits on the visibility path, but no stage shows it.**
   - The object's `rowsVisible` is measured from the event's write time, so the closure's time is inside it.
   - It is outside `read`, `wait`, `judge`, `write` and `outside`. Those are what `/health.owedActor.stages` serves,
     and what `f_cell` asserts (read p95 0).
   - `DirectReport::closures` is counted but neither served nor bumped (`direct_events_as`'s deltas, `:3040`).
   - The fold's own first-visibility figures do not improve:

     | Build | Runs | First visibility p95 (ms) |
     |---|---|---|
     | `006e6f24` (closure on the door) | 2 | 1,123 and 1,374 |
     | tip | 3 | 1,383, 960 and 1,421 |

     The door's `enqueue;dur` fell (832–1,147 ms down to 28–45 ms) because the read moved behind the answer, not
     because it went away.
   - Under #959's flood the 32 direct passes make 32 D1 batches of 25 statements, where main made 32 single
     compound reads. The serialisation #959 measured (read p95 719 ms) is unchanged in round trips and larger in
     statements.
3. **A multi-event body pays the closures one after another.**
   - The overlay's hand-off `POST /internal/owed-direct` (`direct_route`, `:3104`) carries up to 64 events. Its
     SEENs and words carry payloads, so up to 64 D1 batches run in sequence before the first actor is called.
   - At D1's ~20–50 ms that is 1.3–3.2 s. At the flood's 700 ms it is about 45 s, past `wait_until`'s 30 s. The pass
     is then lost with no actor called, and the queue is the only path left.
   - On `006e6f24` and on main the same body read nothing per event before the concurrent calls.

**Scenario.** On beta under an ordinary word tick, each SEEN or word the overlay flushes costs one extra D1 batch. That
batch reads and decodes the pot's BEEF. None of it is used, and the served stages read 0.

**Fix.**
1. Read the closure only when the payload names a key other than the event's own key. In practice that means a
   party, collected, result, sweep or refund filing; never a seen, word or refusal.
2. Read the closures of one pass concurrently, or in one batch, under the pass's wall. Never read them in sequence
   before the calls.
3. Serve the closure's time as its own direct-class stage (`closureMs`) and bump `report.closures`.
4. Extend `d2_actor_f_a` to the direct pass, counting the WorkerDirect port's D1 calls for a warm SEEN (expect 0).

### MEDIUM-2: the drain's sweep pass reads up to 64 closures inside the belt alarm, about 1,600 statements, breaking NEW9-L1's 280-statement drain figure

**Where.** `sweep_publish` (`owed_events.rs:1475-1490`) re-publishes up to `SWEEP_PAIRS_PER_TICK` (64) pairs. It goes
through `publish_with`, which carries each carrier's payload (`carry_payload`, overlay `owed_events.rs:631`), then
`direct_events_as(.., SWEEP_CLASS)`. Each carried pair in that pass reads its 25-statement closure (MEDIUM-1's loop).
This runs inside the belt actor's alarm (`drain_env`, `owed_belt.rs:1481`) under `OWED_DRAIN_WALL_MS` 20 s.

**What it breaks.**
- `rejudge_calls_max` and `reconcile_yields` (`owed_actor.rs:6045-6055`) budget the alarm on "the drain's sweep, its
  publish batch and its direct pass's mark reads … about 280 statements for a full 64-pair sweep" (`fold16_b`).
- With 64 carried pairs the drain adds up to 64 × 25 = 1,600 statements, less the ~2 mark reads a carried event no
  longer makes.
- The alarm's worst case becomes about 1,700 (drain) + 314 (belt tick) + one judge call. That is past the 1,000 queries
  per invocation the repo budgets against (#576, FOLD-15, FOLD-16). The same alarm runs the belt tick: the one place
  that asks couriers, and its finds' writes.

**Scenario.** A consumer stalls for 600 s in Phase C, so the sweep takes the oldest 64 pairs a tick, mostly words and
SEENs (carriers). In each such tick:
- the alarm's later statements meet D1's per-invocation refusal;
- 64 sequential closure batches precede the sweep's actor calls, under a 20 s wall that a slow D1 can exhaust before
  the first call.

**Fix.**
1. Read no closure in the `SWEEP_CLASS` pass. The actor reads its own misses once, as it did before the fold.
2. Or bound the closures per pass and count them in the alarm's bound.
3. Then re-measure `fold16_b` with carried pairs in the sweep.

### NIT-1: the key rule is a deny-list, not "txid columns only"

`txids_in_row` (`owed_actor.rs`, `NOT_KEY_COLUMNS`) excludes five named columns at any depth and still scans every
other string value for exact 64-hex runs. Columns still scanned as text include:
- `submit_refusals.reason` (Arcade's own text; a double-spend reason can name the competing txid);
- `tx_words.hint_source` (the `fold7_c` fixture now names its chain there);
- `network_seen.seenSource`;
- `owed_spender_bytes.bytesJson`;
- the markers' free-text columns.

A column added later that holds a non-key hash makes misses again. This costs reads only, never correctness. The
brief's "by txid COLUMN only" is not what was built.

**Fix.** Either state the deny-list as the rule, or invert it to each family's txid columns plus the judge's answers.

### NIT-2: the closure's whole response is unbounded

Each row and each BEEF is bounded: a row's weight is at most 196,608, and a BEEF is read only when under 262,144 bytes.
The number of keys is not.
- A 32 KB payload can name about 500 txids, so one family statement can answer about 500 BEEF hexes of up to 512 KB
  each before `closure_of` drops the closure at 32 KB.
- This is the same class as the actor's own record read of those keys, which is pre-existing and unbounded. The fold's
  CLAUDE.md states the >1 MB BEEF `read_round` fault as pre-existing, the owner's own, with a bsv-low follow-up.

**Fix.** State it, or cap the keys per closure (for example 32) and mark the rest not carried.

### NIT-3: `closures` is counted but never served

`DirectReport::closures` is computed but appears in no counter, log line or served field. So nothing in production says
how often the closure was read, or how often it was taken (`closureRounds` is per answer only).

## The six answers

### 1. Where is the closure now?

**Who reads it.** The app layer's direct pass, `owed_events::direct_with_class` (`owed_events.rs:2909-2915`), through
`DirectPort::closure`. `WorkerDirect` implements that as `owed_actor::read_closure(&self.sql, facts)`.

**What it reads.** ONE D1 batch (`closure_statements`):
- `CLOSURE_HW_SQL` (`SELECT seq … FROM sqlite_sequence WHERE name='owed_dirty'`);
- one `closure_select` per family, 24 in all: `WITH k AS MATERIALIZED (json_each(?1)), p AS MATERIALIZED …`, each
  family's txid seek;
- a BEEF row read for its length, its hex only when under 262,144 bytes, decoded by `closure_of` into `__facts`.

**When it runs.**
- For a writer's event: in the writer's `wait_until` after its answer (`schedule_direct`).
- For the overlay's events: in `direct_route`'s `wait_until`.
- For the belt's finds: inline (`direct_held`).
- For the sweep: inline in the belt alarm.

**Its bounds.**
- `CLOSURE_ROW_MAX_WEIGHT` and `CLOSURE_BEEF_MAX_BYTES` bound each value before it is built. A txid past either is
  `notCarried`.
- `CLOSURE_MAX_BYTES` (32 KB) bounds the parsed closure, which is dropped past it.

**What a fault costs.** `read_closure` answers `None`, and the actor's miss round reads the record once. A fault never
touches the publish batch or the answer.

**So how is the apply zero-read?** It reads zero because the read already happened, in the direct pass, before the
call. It is a second D1 round trip after the answer, run for every carried event and in sequence (MEDIUM-1).

**Did the lane measure first visibility with it?** The fold's first visibility (`f_cell`'s answer time less the flood's
start) includes it: tip p95 960–1,421 ms against 1,123–1,374 on `006e6f24`, local workerd, one D1 process. No figure
isolates the closure's time, and the served stages exclude it. So the claim "a NEW filing reads nothing" is true of the
actor's apply and of no D1 path.

### 2. MEDIUM-1 (the first lens's): closed

**No BEEF column reaches the publish.** `statement_of` is `read_sql` (overlay `owed_events.rs:524`), and
`carry_payload` no longer touches a closure. Both `owed_closure.rs` modules are deleted.

**The closure cannot build an oversized value.**
- It tests `length(t.beef)<=?3` before `hex(t.beef)`.
- Its row JSON is gated on the weighted length (`?4`).
- A row over the bound answers `r` NULL, and its `k` txids go to `notCarried`. A BEEF with no hex and a positive length
  goes there too, and `rows_of` declines any miss naming one, so the narrowed read takes it.
- D1 binding the bound integers as REAL does not change these comparisons.

**The pins drive the real path.** `fold2_medium1_a` (`tests.rs:15223`) runs `d1_length_limit` (rusqlite
`SQLITE_LIMIT_LENGTH` 2,000,000) with a 1.1 MB `zeroblob` BEEF:
- both pairs publish;
- the closure declines the big txid;
- the honest filing applies through the closure.

`fold2_medium1_b` covers `__facts`, never `__beef`. RED on `006e6f24`: "string or blob too big", recorded in
`fold2-red-on-006e6f24.txt`; I read the log, not re-run.

**What remains.** The actor's own last-resort read of a >1 MB BEEF (pre-existing, stated by the fold, owner's own) and
NIT-2.

### 3. MEDIUM-2 (the first lens's): closed for the door; the cost moved (new MEDIUM-1 and MEDIUM-2)

**The door is the carrier's statement alone.** `fold2_medium2` pins every carrier's door statement as `read_sql`'s;
the party statement was 16,128 bytes before. The `storage-ownership.json` entries for the old closure are gone and the
new `closure_select` is pinned.

**The record-door ceilings hold.**
- The door's batch is again `SEQ_BUMP_SQL` plus the carrier's `read_sql`, byte for byte the statements of `3223be64`
  and main.
- The budget tier binds no owed actor (`direct_events_as` returns before the pass with no `OWED_ACTOR` binding), so no
  closure runs there either.
- Estimated rows read per filing, unchanged from main: the record-door cells' 7 to 24 total, largest statement 2 to 8.

**What the tier does not see.** It does not measure the closure's production cost. That is about 25 statements and,
for a collected filing with two keys, roughly 25 to 60 rows read plus the payout BEEF's bytes. It is paid per carried
event in the direct pass.

### 4. LOW-1 (the first lens's): closed for its two cases; the rule is a deny-list

**The cases are closed.** `txids_in_row` skips `NOT_KEY_COLUMNS` (`proof_root`, `header_hash`, `proof_hex`,
`legacy_body`, `refusedTxid`) at any depth. It is used for:
- the mirror's key walk (`keys_of_mirror`);
- the judge's answers (`Noting`);
- the event's touch (`event_txids`, which also strips `closure`);
- the closure's keys.

So a MINED word and a refusal make no miss. `fold2_low1` covers this, and it also pins that no judge statement reads by
one of those columns.

**The rule is not txid columns only.** Columns still scanned as text include `submit_refusals.reason`,
`tx_words.hint_source`, `network_seen.seenSource`, `owed_spender_bytes.bytesJson` and the markers' text columns (NIT-1).

### 5. The cell

**What is real.**
- `f_cell` runs the production records door, the direct pass (closure included, in the door's `wait_until`) and the
  production `OwedActor`.
- Its "recordCalls 0, readMs 0, closureRounds 1" is real for the actor.
- Seat 0's payout holds a ~20 KB stored BEEF carried as `__facts`.
- After the bars' minute it asserts non-null served stages and `classes.direct` samples. The fold's green run served
  read p95 0, judge p95 22, write p95 40 ms, and `classes.direct` with 7 samples and visible p95 133 ms.

**What the figures leave out.**
- The served and answered stages exclude the closure batch.
- The cell's own first visibility includes it but does not separate it, and it does not improve on `006e6f24`.
- The cell has no warm-SEEN or word event through the direct pass, which is where the closure is pure added cost.
- It runs one local D1, so it cannot show #959's serialisation.

So the figures describe the actor's apply faithfully. They do not describe the write-to-visibility D1 cost of the path
with the closure where it now is.

### 6. What the fold introduced

**`fold7_c` fixture.** The growing chain's next txid moved from `tx_words.legacy_body` to `hint_source`
(`fold7_chained_seat`), because `legacy_body` is now a non-key column. This is test-only and correct. It also shows
`hint_source` is still a scanned column (NIT-1).

**Served shapes.**
- `/apply` and its answer are unchanged.
- `facts.closure` now appears only on direct bodies, never on a queue message.
- `DirectReport::closures` is new and unserved (NIT-3).
- The overlay's `PAYLOAD_MAX_BYTES` doc restates the body bound: 64 × 64 KB, about 4 MB.

**Hot-route statements.** The door gains none and loses the closure. Two paths gain statements:
- the direct pass gains a 25-statement batch per carried event (MEDIUM-1);
- the belt alarm's sweep gains up to 1,600 (MEDIUM-2).

**Lost errors.** A closure fault is silent: `read_closure`'s `.ok()?` logs nothing and counts nothing. Its fallback is
safe (one record read).

**Doc claims the code does not keep.**
- CLAUDE.md's "A NEW filing reads nothing" holds for the actor only; the D1 read precedes it in the direct pass.
- The CLAUDE.md section does not say that warm carried events pay the closure too.
- `rejudge_calls_max`'s ~280-statement drain figure no longer holds with carried sweep pairs.

**Unchanged from the first fold:**
- the generation high-water (`HW_COLUMN`, `READ_BOUND_TRIM_SQL`, per-key `read-key`);
- the open/closed mark (`OPEN_MARK_SQL` / `OPEN_CLEAR_SQL`).

`closure_round` now refuses a closure older than ANY applied item (NIT-3 of the first lens), which is correct. LOW-3
(`sqlite_sequence` on remote D1, seq 223,842) and the restore rule are stated at `HW_COLUMN`. NIT-1 and NIT-4 of the
first lens are stated at `READ_BOUND_KIND` and `OPEN_MARK_SQL`.

REPORT: STATUS done
REPORT: VERDICT NOT LAND
REPORT: FINDINGS HIGH 0 MEDIUM 2 LOW 0 NIT 3
REPORT: DOC docs/audit/D2-ACTOR-F-delta-2-2026-10-31.md
