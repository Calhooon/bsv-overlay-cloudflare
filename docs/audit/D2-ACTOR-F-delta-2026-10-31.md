VERDICT: NOT LAND

# D2-ACTOR-F delta lens: overlay PR 250 after the fold (3223be64 → 006e6f24), bsv-low #959 and #954

The branch was rebased between the lens and the fold, so `3223be64` is not an ancestor of the tip. The fold is
`a1d77f39..006e6f24` (3df8dfbf, 350f2301, d07a7d28, 02849597, ec8bd71f, 006e6f24): 16 files, +1,612 / -78. I read
the lens whole, that diff file by file, and the carried path as it stands (`owed_actor.rs`, `owed_actor/carried.rs`,
both `owed_closure.rs`, the publish in `crates/overlay-cloudflare/src/owed_events.rs`, the direct pass in
`crates/low-app-layer/src/owed_events.rs`, `tools/lane-912/owed_carried_flood.mjs`).

**Measured.** One scratch pin (not committed; the file was restored) ran the shipped publish statements natively over
`e_warm_seat(20)`. A separate `sqlite3` run checked AUTOINCREMENT's conflict arm. The figures are in
`/Volumes/Lanes/out/bsv-low/D2-ACTOR-F-DELTA-LENS/closure-cost.txt`.

**Verdict.** The lens's MEDIUM-1 and MEDIUM-2 are closed. The closure the fold added to close LOW-2 brings two new
MEDIUMs, both in the door's publish statement.

## Findings

### MEDIUM-1 (new): the closure reads every named txid's stored BEEF as hex with no value bound, so one large BEEF faults the whole publish batch, for good

`crates/low-app-layer/src/owed_actor.rs:955` (`closure_column`, built from `row_json` → `compound_columns`, which
emits `hex(t.beef)` at `:921` for `pot_beefs` and `transactions`) is evaluated inside each carrier's publish statement
(`owed_closure.rs`, `statement_of` at `crates/overlay-cloudflare/src/owed_events.rs:523`). The 32 KB check
(`carry_payload`, `:573`) runs in Rust, after D1 has already built the value.

**What happens.**
- D1 bounds a string or BLOB at 2,000,000 bytes. The actor's own record read has TOOBIG fallbacks for exactly this
  case (`read_round`: "the BEEFs row by row", then "one statement a family"); the closure has none.
- A named txid whose stored BEEF is about 1 MB or more makes the carrier statement fail with TOOBIG.
- `publish_with` runs every note's statement in ONE D1 batch, and on any error marks every note unsent
  (`owed_events.rs:604`).
- `sweep_publish` notes a faulted re-publish lost again (`crates/low-app-layer/src/owed_events.rs:1487`), and the next
  tick takes lost notes whatever their stamp.

**Failure scenario.** A stranger stores a large BEEF through the ungated `/submit` (any topic that admits on output
shape keeps it in `transactions`). They then file a `held`/`collected` marker naming that txid as the payout; the door
accepts any 32-byte pay txid, and the fixture's own seats use `[0xa0+n; 32]`.
- Their own publish faults, and their pair goes to the sweep.
- From then on, every sweep tick's re-publish batch (up to 64 pairs, up to 32 lost notes) faults with it.
- Every honest pair in those batches stays unpublished. In Phase C their marks never reach an actor, and their reads
  withhold the press.

**Measured.** Natively, a 1.1 MB payout BEEF made the collected filing's closure value 2,200,635 bytes, past D1's
bound. D1's enforcement was not run (local SQLite has no 2 MB bound).

**Fix.**
1. Make the closure unable to fault. Answer NULL when any named BEEF is long, for example
   `CASE WHEN EXISTS (SELECT 1 FROM pot_beefs|transactions WHERE txid IN keys AND length(beef) > 12000) THEN NULL ELSE … END`
   (a closure past 32 KB is dropped anyway). Or leave the BEEF families out of the closure and have
   `ClosureRead::rows_of` decline when a BEEF row exists.
2. Read the closure in its own statement, so a fault costs only the closure, never the batch.
3. Pin it with a `SqliteSql` whose `SQLITE_LIMIT_LENGTH` is 2,000,000.

### MEDIUM-2 (new): the closure adds about 50 times the work to the door's publish statement, and none of it is measured

**The statements.** A filing at the door still runs one publish batch: `SEQ_BUMP_SQL` plus one carrier statement, with
no new statement. The carrier statement is now wrapped by `with_closure` (`owed_actor.rs:984`).

**Measured natively** (VM steps, `rusqlite` `StatementStatus`):

| Filing | Statement length (bytes) | VM steps | Full-scan steps |
|---|---|---|---|
| collected | 761 → 15,301 | 111 → 5,641 | 0 → 6 |
| party | 1,588 → 16,128 | 194 → 9,372 | 0 → 6 |

- The key set (`json_tree(y.payload)`, `DISTINCT`) is a correlated subquery written into every Txid arm and the probe
  range. It is evaluated about 40 times per row, and D1 counts ephemeral-table probes as rows read (FIX-1004-FOLD's
  finding).
- The closure re-reads the pot's whole neighbourhood: party rows, links, seats, refunds, results, the pot row, its
  word, its SEEN, its BEEF hex.
- This all runs before the answer (`publish_before_answer`, 1.5 s bound).

**The tier was not run on the tip.** Its record-door ceilings are tight. For the largest statement's reads: `potparty`
3 (measured 2), `collected` 5 (3), `result` 11 (7), `potrefund` 12 (8); total reads are 7 to 24. The closure's
statement reads every family of the named pot, so I expect `record-door-potparty`, `record-potparty-filing`,
`record-door-potrefund` and probably `record-door-collected` to go red.

**The work moves, it is not removed.** The fold moves D1 work from the actor's apply to the door, inside the same D1
that serialises everything. That serialisation is #959's own contention, and `f_cell`'s local workerd cannot show it.
In the fold's own evidence, the flood's first visibility p50 rose from 360 ms (closure off) to 487 ms (closure on), and
the doors answered in 468 ms against 437 ms. These are single runs, but they point the wrong way.

**Fix.**
1. Materialise the keys once (`WITH k AS MATERIALIZED (…)`).
2. Leave out the families the payload already carries, and the BEEF families (MEDIUM-1).
3. Run `make ci-d1-budget` on the tip and re-record the record-door ceilings with the reason.
4. Report the door's `enqueue;dur` p95 under the flood.

### LOW-1: the overlay's closure module says its carriers never miss, but a word or a refusal names new keys

**The claim.** `crates/overlay-cloudflare/src/owed_closure.rs:5`: the SEEN latch, a word and a refusal "name no txid
their own key does not, so a warm actor's apply of them makes no miss".

**Why it is false.** The actor takes every 64-hex run in any non-blob column of a mirror row as a key (`txids_in`,
`keys_of_mirror`).
- A `tx_words` row carries `proof_root` and `header_hash`, both new on every MINED word.
- A `submit_refusals` row carries `refusedTxid`.

**Failure scenario.** Every MINED word and every refusal the overlay carries makes one record read in its miss round.
The overlay carries no closure, so it cannot be served from one. This is the FIX-974 mined-class path; the read was
already there at D2-ACTOR-F, but the fold's doc now denies it.

**Fix.** State it. Or restrict key extraction to txid columns. Or give the overlay's carriers a closure too.

### LOW-2: `f_cell` does not measure what `/health.owedActor` serves, and its seats are the best case

**What it reads instead.** The cell's figures come from each `/apply` answer, which the `AlarmPin` wrapper records:
- apply time as the sum of the stages;
- first visibility as the wrapper's `Date.now()` at the answer less the flood's start.

**What `/health` served.** In its own green run (`f_cell-green.txt`), `/health.owedActor.stages.*` was all `null` and
`classes.direct.applySamples` was 0. The cell asserts only that the keys are present.

**Why the seats are the best case.** The seats' payout txids have no stored rows, so each closure is about 500 bytes.
A production payout whose BEEF is stored and over about 16 KB drops the closure and reads once.

**Fix.**
1. Flush the bars (or wait out the minute) and assert non-null served p95s.
2. Add a seat whose payout holds a stored BEEF of realistic size.

### LOW-3: the high-water bound reads `sqlite_sequence`, which nothing had read on remote D1

`HW_COLUMN` is in every record read the actor makes (`aggregate_select`, `marked_select`, the sequential fallback). It
has run only on native SQLite and local workerd.
- **If D1's authorizer refused the table,** every actor apply would fault from the first deploy.
- **If D1 is restored to an earlier point (Time Travel),** `sqlite_sequence` goes back while each mirror keeps its
  higher `read` bound. Carried items below the old high-water are then skipped as "held" until the era resets.

**Fix.**
1. Before the deploy, the captain runs one read-only remote probe:
   `SELECT seq FROM sqlite_sequence WHERE name='owed_dirty'`.
2. The restore runbook resets the actors as the wipe's step 4a does.

### NIT-1: the high-water guard assumes the read covered the item's domain; in Phase A it may not have

The read bound is recorded per game (`owed_actor.rs:2049`). `HELD_GENERATION_SQL` (`carried.rs:51`) applies it to
every carried item of that game, including a domain the read never named.

**Failure scenario.** In Phase A the D1 judge acknowledges a mark before the actor reads it. Another event's narrowed
read of the game then records the high-water without the filing's txid in its key set. The filing's late carried call
is skipped as held, and its marks are recorded applied.
- In Phases B and C a mark is acknowledged only after an actor read that included its txid (`mk_t`), so this cannot
  happen there.
- Phase A serves nothing, and the event's queue delivery heals it.

**Fix.** Restate the premise at `READ_BOUND_KIND`, or record `read` per key from the read's `keys` answer.

### NIT-2: the closure and the actor find keys by different rules

The closure keys are string values that are exactly 64 hex characters (`closure_column`). The actor's keys are 64-hex
runs inside any string (`txids_in`). Where they differ, the closure is declined (`rows_of` needs a subset), so this is
safe, but it is unstated.

### NIT-3: `closure_round` can take a closure older than one the apply wrote

`closure_round` (`owed_actor.rs:2149`) takes the newest closure that covers every miss key. It checks only applied
items keyed by those txids and `read-key`. In a coalesced body it can take a closure older than another applied item's
event, or than the game's `read` bound. The domain's own event heals it. Requiring `hw` at or above every applied
item's `g` closes it.

### NIT-4: the open mark is per game, while txid-keyed domains are shared between an identity's games

The open mark is per game (`OPEN_MARK_SQL`), but txid-keyed domains (`tx_words`, `network_seen`, `pot_records`) are
shared between games of one identity. No carried item today writes a shared domain naming a new key, so this is
unreached. State it.

## The six answers

### 1. MEDIUM-1: closed

**AUTOINCREMENT in both Workers.**
- `crates/overlay-cloudflare/src/derived_owed.rs:29`, `crates/low-app-layer/src/schema.rs:178`, and the actor mirror
  (`owed_actor/schema.rs:267`).
- The production conflict arm is `DO UPDATE SET generation=excluded.generation`. It advances `sqlite_sequence`
  (checked with `sqlite3`: after a conflicting upsert the sequence reads 3, and a `DELETE` leaves it there).
- No production writer sets a generation by hand. The `MAX+1` form appears only in test helpers.

**An absent row.** It reads NULL, and `hw_of` (`:1260`) makes that 0. That happens only on a database that never
minted a mark, where no carried item can exist, so a NULL never lets an older row pass. The reverse risk, a restored
sequence, is LOW-3.

**Read in the same statement as the rows.**
- `HW_COLUMN` is the first column of `aggregate_select` and `marked_select_of`.
- The sequential fallback reads it in the same D1 batch (one transaction).

**Never lowered.** `READ_BOUND_CLEAR_SQL` is gone. `READ_BOUND_TRIM_SQL` (`carried.rs:44`) deletes only lower rows, and
`APPLIED_SQL` is `DO NOTHING`, so `MAX` never falls. Only the 7-day prune removes a bound. Each path records it:
- paged: `:1823`;
- narrowed: `:1840`;
- full or first `read_round`: `:1961`;
- reconcile re-apply: the normal path;
- a carried apply records no `read` (`:2049`), only `read-key` for its miss rounds.

**Pins.** The lens's reproduction is `fold_m1_a`; `fold_m1_c` covers an acknowledgement before the actor read, and
`fold_m1_b` the per-key bound. Traced by hand: the acknowledged-then-narrowed read now records `hw` at or above the
newer mark, and the late item at `g` is skipped. The residual is NIT-1.

### 2. MEDIUM-2: closed

**Order of writes.** `OPEN_COUNT_SQL` and `OPEN_MARK_SQL` run in one local batch (`:1770`). That batch comes before
`carried_upsert` and before any `write_round_of`. The only early `Ok` return (`:1701`) precedes it.

**Clearing.** `OPEN_CLEAR_SQL` (`:2130`) is the last statement of the final batch, after `commit_fixpoint`. The brief's
wording is inverted (the code marks the games open first and clears them last), but the effect is the same.
- A fault anywhere between leaves the games open. `!open` then gates the carried path, and the next apply takes the
  narrowed read over the mirror's whole key set (pinned by `fold_m2`).
- `APPLIED_PRUNE_SQL` never prunes an open mark.

**Interleaving.** Two applies of one actor cannot interleave. Every apply holds the object's turn (`object.rs:873`,
`:1226` pass `&self.turn`, a futures `Mutex`), and so do the era reset and `/reset`. A burst's chunks run in sequence,
each with its own mark. The residual is NIT-4.

### 3. The closure

**Who adds it.** The app layer's publish adds it to every carrier: party, collected, refund, result, sweep, seen,
word, refusal (`CARRIERS`), through `owed_closure::statement` → `with_closure`. It is on in production for every
app-layer publish of those sources: the records door, the handfile batch, the tx-word ingest, the hooks, and the
sweep's re-publishes. The overlay's publish adds none (`owed_closure.rs` answers `None`).

**What it carries.** Each of the 24 `FAMILIES` selections over the exact-64-hex string values of the payload, with
games `'[]'`, markers `'[]'` and `identity ''`. That is the miss round's read: a miss round has no games
(`read_games` empty), and `closure_round` declines a round that names a marker. It carries:
- the BEEF families' hex, so `write_round_of` decodes the `txfacts`;
- the high-water.

**What it does not carry**, so the record read runs:
- the marker-keyed links and seats;
- keys found inside longer strings (NIT-2, LOW-1);
- keys that only the judge's statements name.

The outside (word views, canonical roots) is still asked by the actor, timed under `outsideMs`.

**What turns it on.** The source being a carrier on the app layer. `d2_actor_f_b` and `fold_m2` remove
`facts.closure` by hand to get the one-read path; `f_cell-red-no-closure.txt` is that path run in the cell.

### 4. The bound

**The order.** The payload is checked first (over 32 KB: no payload, the narrowed read). The closure is added only
while the payload and the closure together stay within 32 KB (`owed_events.rs:573`); otherwise it is dropped and the
miss reads once.

**Party filing over a pot with many links.** The links are per pot and small; this fits.

**Settle word.** The closure names the settle's txid and so reads its stored BEEF, typically past 32 KB, and is
dropped. A word names no new txid on the app layer anyway, so nothing is lost.

**Bodies.** A queue message is one event of at most 32 KB of carried bytes plus its facts, under 128 KB. A coalesced
direct `/apply` or `/internal/owed-direct` body is at most 64 × 32 KB, about 2 MB (stated, NIT-4 of the lens).

**What breaks the bound.** Everything above is checked after D1 has answered. A BEEF large enough to pass D1's own
value bound faults the statement before any of it runs (MEDIUM-1).

### 5. The cell

**It is the real path.**
- Miniflare runs the production app-layer build, its records door and the production `OwedActor`, behind a
  pass-through wrapper.
- The direct call comes from the door's `wait_until`. The queue is a producer only. The cell binds Phase B flags.
- The closure is on, because it is the production `statement_of`.

**What the figures are.** Its figures (apply p95 9 ms, first visibility p95 492 ms, `readMs` 0, `recordCalls` 0) come
from each actor answer as the wrapper recorded it. They are not `/health.owedActor.classes.direct`, which served
`null` and 0 samples in the same run (LOW-2).

**What "closure off" is.** The same cell run with no closure (`f_cell-red-no-closure.txt`: one record call, read p95
69 ms, apply p95 75 ms). It is kept as the RED evidence and the baseline for the zero-read claim.
- Both runs use one local D1 process, so neither shows D1 serialisation under a flood. That was #959's measured cost,
  the 719 ms read p95.
- The visibility p50 moved from 360 to 487 ms with the closure on (MEDIUM-2).

### 6. What the fold introduced

**Served shapes.** All additive: `/apply` gains `closureRounds` and `recordCalls`, the event's facts gain `closure`,
and `ApplyReport` gains `was_open`, `closure_rounds` and `record_calls`.

**Hot route.** The door's carrier statement is about 20 times longer and does about 50 times the work, unmeasured
against the tier (MEDIUM-2).

**A lost error.** One large BEEF named by a carrier now faults that invocation's whole publish, and in the sweep a
whole tick's re-publish, again on every tick (MEDIUM-1).

**Panic paths.** None found: `closure.expect("checked")` is guarded by `is_some`, and every parse is `?` or `Option`.

**Doc claims the code does not keep.**
- The overlay's "no miss" claim (LOW-1).
- CLAUDE.md's "a NEW filing reads nothing" holds only when the named txids' stored rows, BEEF hex included, fit 32 KB
  beside the payload, and no named txid's BEEF passes D1's value bound.
- The commit message's "the stages served" is key presence only (LOW-2).

REPORT: STATUS done
REPORT: VERDICT NOT LAND
REPORT: FINDINGS HIGH 0 MEDIUM 2 LOW 3 NIT 4
REPORT: DOC docs/audit/D2-ACTOR-F-delta-2026-10-31.md
