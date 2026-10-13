//! bsv-low #649 M1: THE PARKED CHANGES. A background flush of an admit-fast job (the pending watch, the
//! `/arc-ingest` refusal and witness jobs) that reaches its flush with less than
//! [`crate::pot_changes::FLUSH_WINDOW_FLOOR_MS`] of the `waitUntil` window left starts no POST. Until the fold it
//! left its notes in the isolate's sets (`pot_changes::CHANGES`, `hop_changes::CHANGES`), which only a later
//! invocation on THAT isolate drains: a lone eviction followed by no such invocation lost its push for good, and
//! the felt waited on an unrelated wake-up. The skip counter counted jobs, not the withheld entries, and none of
//! them entered the note-back accounting.
//!
//! Now the skip COPIES the pot and hop sets and PARKS the copy in D1 (`change_parked`, one row per entry, keyed by
//! its set and the entry, so a re-park is one row with its generation bumped) as its FIRST durable write; only
//! the entries the park landed are then taken out of the isolate's sets (the delta lens's M1: a cancellation at
//! any await leaves every note in D1, in memory or both). A park that faults, or an entry refused at the table's
//! ceiling, stays in the sets. The scheduled tick (every isolate's cron, `*/15`) delivers the ELIGIBLE rows
//! through the same flush loop (`ship_chunks`, chunked at the app layer's bound) and deletes ONLY the rows of POSTs
//! the app layer accepted, at the generation it read (N1); a refused row backs off and leaves the drain after
//! [`CHANGE_PARK_MAX_ATTEMPTS`], then is swept (N2); a deferred row stays parked for the next tick. So a withheld
//! entry is at rest until a POST of it is accepted or its attempts are spent: no isolate needed.
//!
//! Why not the existing note-back accounting (`*_deferred_total`, `*_resent_total`, `*_noted_back_unresent`,
//! `change_flush::NotedBack`): it is per-isolate memory by design (`change_flush.rs`, "THE NOTE-BACK IS NOT
//! DURABLE"), and its `resent` is counted only by the isolate that noted the entry back, so a parked entry
//! delivered by another isolate's cron would read as lost there for good. The parked set has its own counters, in
//! entries: `admit_fast_flush_withheld_total` (every entry a skip withheld), `change_parked_total` (parked in D1),
//! `change_park_faults_total` (a park write that faulted: those entries were never taken out of the isolate's
//! sets, logged), `change_park_full_total` (refused at the table's ceiling, kept in the sets), `change_unparked_total` (delivered from the table and deleted), `change_park_refused_total`,
//! `change_park_exhausted_total` and `change_park_swept_total`. The gauge is the table itself (on
//! `/health/invariants.changeParked`, by set, active and exhausted, each count capped), read from D1, never an
//! isolate's.
//!
//! Stated: the tracker frames (`tracker_events`, telemetry, no money view) withheld by a skip are NOT parked: they
//! stay in the isolate's memory as before. The lobby set has its own flush and is not drained by this one. The cron's
//! delivery runs before the GASP step under its own wall ([`DRAIN_WALL_MS`]) and at most [`DRAIN_MAX_ROWS`] rows
//! a set a tick; a POST in flight is not raced (as every flush: `change_flush.rs`). A delete that faults after an
//! accepted POST leaves the row, which is delivered again (over-notification is harmless: the app layer re-reads).
use worker::*;

/// The parked set (bsv-low #649 M1). Transient: a lost row costs one push the felt's own reads recover.
pub const CHANGE_PARKED_CREATE: &str = "CREATE TABLE IF NOT EXISTS change_parked (\
     kind TEXT NOT NULL, item TEXT NOT NULL, parked_at_ms INTEGER NOT NULL, \
     PRIMARY KEY (kind, item))";
/// bsv-low #649 N1 (the delta lens): the row's GENERATION, a new intent token at every park (D2-M1: [`PARK_SQL`]),
/// so a delivery deletes only the intent it read. Appended as its own migration (#638: never a DROP or a RENAME; the runner ignores the re-run's
/// "duplicate column").
pub const CHANGE_PARKED_ADD_GENERATION: &str =
    "ALTER TABLE change_parked ADD COLUMN generation INTEGER NOT NULL DEFAULT 0";
/// bsv-low #649 N2 (the delta lens): a row's RETRY state. `attempts` counts the refused POSTs of its current
/// generation, `next_attempt_at_ms` is when the drain may select it again (a park sets it to the park's stamp, so
/// the drain stays oldest first; a row parked before N2 reads 0, due at once and first), `exhausted` takes it out of the drain after [`CHANGE_PARK_MAX_ATTEMPTS`]
/// refusals (its `next_attempt_at_ms` then the exhaustion's stamp, which the sweep ages it from). Appended
/// migrations (#638).
pub const CHANGE_PARKED_ADD_ATTEMPTS: &str =
    "ALTER TABLE change_parked ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0";
pub const CHANGE_PARKED_ADD_NEXT_ATTEMPT: &str =
    "ALTER TABLE change_parked ADD COLUMN next_attempt_at_ms INTEGER NOT NULL DEFAULT 0";
pub const CHANGE_PARKED_ADD_EXHAUSTED: &str =
    "ALTER TABLE change_parked ADD COLUMN exhausted INTEGER NOT NULL DEFAULT 0";
/// A park: one row per (set, entry); a re-park keeps the first stamp and gives the row a NEW generation (N1): a
/// change parked while an earlier delivery of the same entry is in flight is a new intent, which that delivery's
/// delete does not take. The generation is an INTENT TOKEN never reused for a key (the second delta lens, D2-M1):
/// every insert and every re-park writes `stamp * 2048 + nonce` (`?3` the park's stamp in ms, the nonce SQLite's
/// `random()` masked to 11 bits; under 2^53 until the year 2109, so it reads back exact through D1's numbers), and a
/// re-park takes the larger of that and the held token plus one. Until the fold an insert took the column's default
/// 0, so a key deleted by one delivery and parked again was K/0 again, and a second delivery that had read K/0
/// deleted the new intent. A token is reused only by a park of the same key in the same millisecond with the same
/// nonce after the earlier row was deleted (stated: clocks of two isolates that disagree can make two stamps equal). A re-park is a new intent for the retry state too (N2): its attempts start again and it is
/// eligible at once (an exhausted row comes back). A NEW key is parked only while the table holds fewer than
/// `?4` rows ([`CHANGE_PARKED_MAX_ROWS`], read by a LIMITed scan: at most `?4` rows), so the newest are refused
/// when it is full (no row returned: the caller keeps that entry in memory and counts it).
pub const PARK_SQL: &str =
    "INSERT INTO change_parked (kind, item, parked_at_ms, next_attempt_at_ms, generation) \
     SELECT ?1, ?2, ?3, ?3, CAST(?3 AS INTEGER) * 2048 + (random() & 2047) \
     WHERE EXISTS (SELECT 1 FROM change_parked WHERE kind = ?1 AND item = ?2) \
        OR (SELECT COUNT(*) FROM (SELECT 1 FROM change_parked LIMIT ?4)) < ?4 \
     ON CONFLICT(kind, item) DO UPDATE SET \
        generation = MAX(excluded.generation, change_parked.generation + 1), \
        attempts = 0, exhausted = 0, \
        next_attempt_at_ms = MIN(change_parked.next_attempt_at_ms, excluded.next_attempt_at_ms) \
     RETURNING item";
/// The ELIGIBLE rows of one set (not exhausted, due by `?2`), the most overdue first, each with the generation
/// and the attempts the delivery read (N2: a refused row backs off, so it never holds the head of the drain).
pub const PARKED_READ_SQL: &str = "SELECT item, generation, attempts FROM change_parked \
     WHERE kind = ?1 AND exhausted = 0 AND next_attempt_at_ms <= ?2 \
     ORDER BY next_attempt_at_ms, item LIMIT ?3";
/// One delivered entry, deleted ONLY at the generation its delivery read (N1). It RETURNS the row it deleted
/// (D2-L1: a row re-parked since is not deleted and not counted).
pub const UNPARK_SQL: &str =
    "DELETE FROM change_parked WHERE kind = ?1 AND item = ?2 AND generation = ?3 RETURNING item";
/// One refused entry (N2), its attempt RESERVED in one statement (the second delta lens, D2-L3): the count goes up
/// by one and its exhaustion (`?5` [`CHANGE_PARK_MAX_ATTEMPTS`]) and next eligibility (`?4` now, then `?6`
/// [`CHANGE_PARK_BACKOFF_BASE_MS`] doubled per earlier attempt, at most `?7` [`CHANGE_PARK_BACKOFF_MAX_MS`]) are
/// derived in SQL from the count it moves, never from the delivery's read; only at the generation the delivery
/// read (a re-park since is a new intent with its own attempts) and only while the row is not exhausted (an
/// exhaustion is one transition). It RETURNS the row it changed (D2-L1). [`refusal_backoff`] is its pure mirror,
/// pinned equal. Until the fold the next time and the exhaustion were the delivery's, computed from its read, so two
/// overlapping deliveries that read `attempts = 6` left `attempts = 8, exhausted = 0`.
pub const REFUSE_SQL: &str = "UPDATE change_parked \
     SET attempts = attempts + 1, \
        exhausted = (attempts + 1 >= ?5), \
        next_attempt_at_ms = CASE WHEN attempts + 1 >= ?5 THEN ?4 \
           ELSE ?4 + MIN(?6 << MIN(MAX(attempts, 0), 20), ?7) END \
     WHERE kind = ?1 AND item = ?2 AND generation = ?3 AND exhausted = 0 RETURNING exhausted";
/// The sweep of one set (N2): exhausted rows older than `?2`, at most `?3` a tick.
pub const SWEEP_EXHAUSTED_SQL: &str = "DELETE FROM change_parked WHERE rowid IN (\
     SELECT rowid FROM change_parked WHERE kind = ?1 AND exhausted = 1 AND next_attempt_at_ms <= ?2 \
     ORDER BY next_attempt_at_ms LIMIT ?3) RETURNING item";
/// bsv-low #649 N3 (the delta lens): the drain's ORDER. `(kind, item)` (the primary key) cannot serve the drain's
/// order, so it scanned and sorted the whole set (a temp B-tree, 90,412 VM steps for 128 rows of 10,001). This
/// index serves [`PARKED_READ_SQL`] (equality on the set and `exhausted`, a range and the order on
/// `next_attempt_at_ms, item`), [`SWEEP_EXHAUSTED_SQL`] and the gauge's bounded counts. Appended (#638).
pub const CHANGE_PARKED_DRAIN_INDEX: &str = "CREATE INDEX IF NOT EXISTS change_parked_drain \
     ON change_parked(kind, exhausted, next_attempt_at_ms, item)";
/// The gauge (N3: BOUNDED): each set's eligible-or-backing-off and exhausted rows, each count read through the
/// drain's index and stopped at `?1` ([`HEALTH_COUNT_CAP`]); a count equal to the cap reads "at least".
pub const PARKED_COUNT_SQL: &str = "SELECT \
     (SELECT COUNT(*) FROM (SELECT 1 FROM change_parked WHERE kind = 'pot' AND exhausted = 0 LIMIT ?1)) AS pot, \
     (SELECT COUNT(*) FROM (SELECT 1 FROM change_parked WHERE kind = 'hop' AND exhausted = 0 LIMIT ?1)) AS hop, \
     (SELECT COUNT(*) FROM (SELECT 1 FROM change_parked WHERE kind = 'pot' AND exhausted = 1 LIMIT ?1)) AS pot_exhausted, \
     (SELECT COUNT(*) FROM (SELECT 1 FROM change_parked WHERE kind = 'hop' AND exhausted = 1 LIMIT ?1)) AS hop_exhausted";
/// N3: where the gauge stops counting (at most four times this many index entries a read).
pub const HEALTH_COUNT_CAP: i64 = 256;

/// N2: the refusals a row's generation gets before it leaves the drain.
pub const CHANGE_PARK_MAX_ATTEMPTS: i64 = 8;
/// N2: the first back-off after a refusal (a tick at `*/15`), doubled per attempt.
pub const CHANGE_PARK_BACKOFF_BASE_MS: u64 = 15 * 60 * 1000;
/// N2: the longest back-off.
pub const CHANGE_PARK_BACKOFF_MAX_MS: u64 = 24 * 60 * 60 * 1000;
/// N2: how long an exhausted row is kept (for the gauge and an operator's read) before the sweep deletes it.
pub const CHANGE_PARK_EXHAUSTED_KEEP_MS: u64 = 7 * 24 * 60 * 60 * 1000;
/// N2: the most rows the table holds, both sets and exhausted rows together. Full, a NEW entry is not parked
/// (kept in the isolate's set, counted `change_park_full_total`); a re-park of a held key always lands.
pub const CHANGE_PARKED_MAX_ROWS: i64 = 2048;

/// PURE (N2): a refusal of a row holding `attempts` at `now_ms`: `(next_attempt_at_ms, exhausted)`. The mirror of
/// [`REFUSE_SQL`], which is what the drain runs (D2-L3).
pub fn refusal_backoff(attempts: i64, now_ms: u64) -> (u64, bool) {
    let after = attempts.saturating_add(1);
    if after >= CHANGE_PARK_MAX_ATTEMPTS {
        return (now_ms, true);
    }
    let shift = attempts.clamp(0, 20) as u32;
    let wait = CHANGE_PARK_BACKOFF_BASE_MS
        .saturating_mul(1u64 << shift)
        .min(CHANGE_PARK_BACKOFF_MAX_MS);
    (now_ms + wait, false)
}

/// The pot set's rows (`txid:vout`).
pub const KIND_POT: &str = "pot";
/// The hop set's rows (an identity key).
pub const KIND_HOP: &str = "hop";

/// The most rows of one set the cron delivers a tick (16 POSTs of 8: one flush's bound).
pub const DRAIN_MAX_ROWS: usize =
    crate::change_flush::CHANGE_CHUNK * crate::change_flush::CHANGE_MAX_CHUNKS;
/// The wall the cron's delivery may spend STARTING POSTs, both sets together.
pub const DRAIN_WALL_MS: u64 = 10_000;

pub const COUNTER_ADMIT_FAST_FLUSH_WITHHELD: &str = "admit_fast_flush_withheld_total";
pub const COUNTER_CHANGE_PARKED: &str = "change_parked_total";
pub const COUNTER_CHANGE_PARK_FAULTS: &str = "change_park_faults_total";
pub const COUNTER_CHANGE_UNPARKED: &str = "change_unparked_total";
/// N2: entries not parked because the table was full (kept in the isolate's sets).
pub const COUNTER_CHANGE_PARK_FULL: &str = "change_park_full_total";
/// N2: parked entries whose POST was refused (each refusal of each entry), backed off.
pub const COUNTER_CHANGE_PARK_REFUSED: &str = "change_park_refused_total";
/// N2: parked entries taken out of the drain after their last refusal.
pub const COUNTER_CHANGE_PARK_EXHAUSTED: &str = "change_park_exhausted_total";
/// N2: exhausted rows the sweep deleted.
pub const COUNTER_CHANGE_PARK_SWEPT: &str = "change_park_swept_total";

/// A pot entry's row key.
pub fn pot_item(txid: &str, vout: u32) -> String {
    format!("{}:{vout}", txid.to_ascii_lowercase())
}

/// A pot row key read back (`None`: not one this module wrote; it is left parked and logged).
pub fn parse_pot_item(item: &str) -> Option<(String, u32)> {
    let (txid, vout) = item.rsplit_once(':')?;
    if txid.len() != 64 || !txid.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    Some((txid.to_ascii_lowercase(), vout.parse().ok()?))
}

/// The skip's world (bsv-low #649 M1): a port so the pins run the shipped [`park_withheld`] over a recorded fake.
pub(crate) trait ParkPort {
    /// Park `rows` (`(set, entry)`, both sets) in ONE batch: one statement each, one D1 round trip. Answers, per
    /// row, whether it is parked (`false`: refused, the table full).
    async fn park(
        &self,
        rows: &[(&'static str, String)],
        now_ms: u64,
    ) -> std::result::Result<Vec<bool>, String>;
    async fn bump(&self, counter: &'static str, delta: u64);
    fn log(&self, line: String);
}

/// THE SKIP of a background flush (bsv-low #649 M1; the delta lens's M1). The pot and hop sets are COPIED, the
/// copy is PARKED in D1 as the skip's FIRST durable write (one batch, both sets), and only once the park landed
/// are the parked entries taken out of the isolate's sets, each only at the version the copy read (D2-M2: a note of
/// the same key made while the park was pending stays); the counters come after. So a cancellation at any
/// await leaves every note in memory, in D1, or in both (a note in both is delivered twice: harmless, the app
/// layer re-reads). A park that faults takes nothing out of memory, counted and logged.
pub(crate) async fn park_withheld<P: ParkPort>(port: &P, remaining_ms: u64, now_ms: u64) {
    let pots = crate::pot_changes::snapshot();
    let hops = crate::hop_changes::snapshot();
    let mut rows: Vec<(&'static str, String)> = pots
        .iter()
        .map(|((t, v), _)| (KIND_POT, pot_item(t, *v)))
        .collect();
    rows.extend(hops.iter().map(|(h, _)| (KIND_HOP, h.clone())));
    let withheld = rows.len() as u64;
    let (mut parked, mut faulted, mut full) = (0u64, 0u64, 0u64);
    if withheld > 0 {
        match port.park(&rows, now_ms).await {
            Ok(landed) => {
                let landed_at = |i: usize| landed.get(i).copied().unwrap_or(false);
                let parked_pots: Vec<((String, u32), u64)> = pots
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| landed_at(*i))
                    .map(|(_, p)| p.clone())
                    .collect();
                let parked_hops: Vec<(String, u64)> = hops
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| landed_at(pots.len() + *i))
                    .map(|(_, h)| h.clone())
                    .collect();
                crate::pot_changes::forget(&parked_pots);
                crate::hop_changes::forget(&parked_hops);
                parked = (parked_pots.len() + parked_hops.len()) as u64;
                full = withheld - parked;
                if full > 0 {
                    port.log(format!("[pot-changes] {full} entr(ies) NOT parked: the parked table is full ({CHANGE_PARKED_MAX_ROWS} rows); kept in the isolate's sets"));
                }
            }
            Err(e) => {
                faulted = withheld;
                port.log(format!("[pot-changes] park of {withheld} entr(ies) FAULTED ({e}); kept in the isolate's sets"));
            }
        }
    }
    port.bump(crate::ops::COUNTER_ADMIT_FAST_FLUSH_SKIPPED_NO_WINDOW, 1)
        .await;
    if withheld == 0 {
        port.log(format!("[pot-changes] background flush SKIPPED: {remaining_ms} ms left of the wait_until window; nothing withheld"));
        return;
    }
    port.bump(COUNTER_ADMIT_FAST_FLUSH_WITHHELD, withheld).await;
    if parked > 0 {
        port.bump(COUNTER_CHANGE_PARKED, parked).await;
    }
    if faulted > 0 {
        port.bump(COUNTER_CHANGE_PARK_FAULTS, faulted).await;
    }
    if full > 0 {
        port.bump(COUNTER_CHANGE_PARK_FULL, full).await;
    }
    port.log(format!(
        "[pot-changes] background flush SKIPPED: {remaining_ms} ms left of the wait_until window (floor {} ms); {withheld} entr(ies) withheld, {parked} parked in D1 for the cron, {} kept in memory",
        crate::pot_changes::FLUSH_WINDOW_FLOOR_MS,
        faulted + full
    ));
}

/// The worker's [`ParkPort`]: D1.
pub(crate) struct WorkerPark<'a> {
    pub db: &'a D1Database,
}

impl ParkPort for WorkerPark<'_> {
    async fn park(
        &self,
        rows: &[(&'static str, String)],
        now_ms: u64,
    ) -> std::result::Result<Vec<bool>, String> {
        let mut stmts = Vec::with_capacity(rows.len());
        for (kind, item) in rows {
            let stmt = self
                .db
                .prepare(PARK_SQL)
                .bind(&[
                    (*kind).into(),
                    item.as_str().into(),
                    (now_ms as f64).into(),
                    (CHANGE_PARKED_MAX_ROWS as f64).into(),
                ])
                .map_err(|e| e.to_string())?;
            stmts.push(stmt);
        }
        let results = crate::d1_ledger::counted_batch(self.db, stmts)
            .await
            .map_err(|e| e.to_string())?;
        Ok(results
            .iter()
            .map(|r| {
                r.results::<serde_json::Value>()
                    .map(|v| !v.is_empty())
                    .unwrap_or(false)
            })
            .collect())
    }
    async fn bump(&self, counter: &'static str, delta: u64) {
        crate::ops::bump_counter(self.db, counter, delta).await;
    }
    fn log(&self, line: String) {
        console_log!("{line}");
    }
}

/// One parked row as the drain read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParkedRow {
    pub item: String,
    pub generation: i64,
    pub attempts: i64,
}

/// The cron's delivery of the parked set (bsv-low #649 M1): a port so the pins run the shipped
/// [`deliver_parked`] over a fake transport, or over the SHIPPED statements under SQLite.
pub(crate) trait UnparkPort {
    /// The eligible rows of one set at `now_ms` ([`PARKED_READ_SQL`]).
    async fn read(
        &self,
        kind: &'static str,
        now_ms: u64,
        limit: usize,
    ) -> std::result::Result<Vec<ParkedRow>, String>;
    async fn post(&self, kind: &'static str, body: String) -> std::result::Result<usize, ()>;
    /// Delete the delivered rows, each only at the generation it was read at. Answers the rows it DELETED (D2-L1).
    async fn unpark(
        &self,
        kind: &'static str,
        rows: &[(String, i64)],
    ) -> std::result::Result<u64, String>;
    /// Back the refused rows off at `now_ms` ([`REFUSE_SQL`], each `(entry, generation)`: the attempt reserved and
    /// its back-off derived in the statement, D2-L3). Answers `(backed off, of which exhausted)`, the rows the
    /// statements CHANGED (D2-L1).
    async fn refuse(
        &self,
        kind: &'static str,
        rows: &[(String, i64)],
        now_ms: u64,
    ) -> std::result::Result<(u64, u64), String>;
    /// Delete at most `limit` exhausted rows of one set older than `before_ms` ([`SWEEP_EXHAUSTED_SQL`]).
    async fn sweep(
        &self,
        kind: &'static str,
        before_ms: u64,
        limit: usize,
    ) -> std::result::Result<u64, String>;
    fn now_ms(&self) -> u64;
    async fn bump(&self, counter: &'static str, delta: u64);
    fn log(&self, line: String);
}

/// What one delivery did, by entries.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Delivered {
    pub read: u64,
    pub unparked: u64,
    /// Read and still parked (refused, deferred past the wall, or a delete that faulted).
    pub kept: u64,
    /// N2: refused this pass (backed off), of which `exhausted` left the drain.
    pub refused: u64,
    pub exhausted: u64,
    /// N2: exhausted rows deleted by the sweep.
    pub swept: u64,
}

/// THE DELIVERY of the parked set (bsv-low #649 M1; N2): per set, the exhausted rows past
/// [`CHANGE_PARK_EXHAUSTED_KEEP_MS`] are swept, then the ELIGIBLE rows (due, not exhausted, most overdue first)
/// go through the one flush loop (`change_flush::ship_chunks`, the set's own body); the rows of ACCEPTED POSTs
/// are deleted at the generation read, the rows of REFUSED POSTs (and a pot row that does not read) are backed
/// off ([`refusal_backoff`]) and leave the drain at their last attempt, the rows past the wall are untouched.
pub(crate) async fn deliver_parked<P: UnparkPort>(port: &P) -> Delivered {
    let mut out = Delivered::default();
    let now_ms = port.now_ms();
    let deadline_ms = now_ms + DRAIN_WALL_MS;
    for kind in [KIND_HOP, KIND_POT] {
        match port
            .sweep(
                kind,
                now_ms.saturating_sub(CHANGE_PARK_EXHAUSTED_KEEP_MS),
                DRAIN_MAX_ROWS,
            )
            .await
        {
            Ok(n) => out.swept += n,
            Err(e) => port.log(format!(
                "[change-park] sweep of the exhausted {kind} rows faulted ({e}); next tick"
            )),
        }
        let read = match port.read(kind, now_ms, DRAIN_MAX_ROWS).await {
            Ok(read) => read,
            Err(e) => {
                port.log(format!(
                    "[change-park] read of the parked {kind} set faulted ({e}); next tick"
                ));
                continue;
            }
        };
        if read.is_empty() {
            continue;
        }
        let by_item: std::collections::BTreeMap<&str, &ParkedRow> =
            read.iter().map(|r| (r.item.as_str(), r)).collect();
        out.read += read.len() as u64;
        let items: Vec<String> = read.iter().map(|r| r.item.clone()).collect();
        let (delivered, mut refused): (Vec<String>, Vec<String>) = if kind == KIND_POT {
            let pots: Vec<(String, u32)> = items.iter().filter_map(|i| parse_pot_item(i)).collect();
            let unreadable: Vec<String> = items
                .iter()
                .filter(|i| parse_pot_item(i).is_none())
                .cloned()
                .collect();
            if !unreadable.is_empty() {
                port.log(format!(
                    "[change-park] {} parked pot row(s) do not read; refused",
                    unreadable.len()
                ));
            }
            let (fresh, retry): (Vec<_>, Vec<_>) = pots.into_iter().partition(|(t, v)| {
                by_item
                    .get(pot_item(t, *v).as_str())
                    .is_some_and(|r| r.attempts == 0)
            });
            let (delivered, failed) = ship_isolating(
                fresh,
                retry,
                crate::pot_changes::body_json,
                |body| port.post(kind, body),
                || port.now_ms(),
                deadline_ms,
            )
            .await;
            let mut refused: Vec<String> = failed.iter().map(|(t, v)| pot_item(t, *v)).collect();
            refused.extend(unreadable);
            (
                delivered.iter().map(|(t, v)| pot_item(t, *v)).collect(),
                refused,
            )
        } else {
            let (fresh, retry): (Vec<String>, Vec<String>) = items
                .iter()
                .cloned()
                .partition(|i| by_item.get(i.as_str()).is_some_and(|r| r.attempts == 0));
            ship_isolating(
                fresh,
                retry,
                crate::hop_changes::body_json,
                |body| port.post(kind, body),
                || port.now_ms(),
                deadline_ms,
            )
            .await
        };
        refused.retain(|i| by_item.contains_key(i.as_str()));
        let mut kept = read.len() as u64;
        // D2-L1: every tally below is of the rows a statement CHANGED (a row re-parked since its read is untouched
        // by the generation-guarded statements, and stays parked: kept).
        if !delivered.is_empty() {
            let rows: Vec<(String, i64)> = delivered
                .iter()
                .filter_map(|i| by_item.get(i.as_str()).map(|r| (i.clone(), r.generation)))
                .collect();
            match port.unpark(kind, &rows).await {
                Ok(deleted) => {
                    out.unparked += deleted;
                    kept -= deleted;
                }
                Err(e) => port.log(format!("[change-park] delete of {} delivered {kind} row(s) faulted ({e}); they are sent again", rows.len())),
            }
        }
        if !refused.is_empty() {
            let now = port.now_ms();
            let rows: Vec<(String, i64)> = refused
                .iter()
                .map(|i| (i.clone(), by_item[i.as_str()].generation))
                .collect();
            match port.refuse(kind, &rows, now).await {
                Ok((backed_off, exhausted)) => {
                    out.refused += backed_off;
                    out.exhausted += exhausted;
                }
                Err(e) => port.log(format!("[change-park] back-off of {} refused {kind} row(s) faulted ({e}); they are tried again next tick", rows.len())),
            }
        }
        out.kept += kept;
    }
    for (counter, n) in [
        (COUNTER_CHANGE_UNPARKED, out.unparked),
        (COUNTER_CHANGE_PARK_REFUSED, out.refused),
        (COUNTER_CHANGE_PARK_EXHAUSTED, out.exhausted),
        (COUNTER_CHANGE_PARK_SWEPT, out.swept),
    ] {
        if n > 0 {
            port.bump(counter, n).await;
        }
    }
    if out.read > 0 || out.swept > 0 {
        port.log(format!(
            "[change-park] delivered {} parked entr(ies), {} kept parked ({} refused and backed off, {} exhausted), {} exhausted swept",
            out.unparked, out.kept, out.refused, out.exhausted, out.swept
        ));
    }
    out
}

/// N2: the fresh rows (never refused) through the one flush loop in chunks; then each row a POST already
/// REFUSED, ALONE (one POST each, inside the same chunk count and wall), so a refused row cannot take the rows
/// chunked with it down with it: a chunk is refused whole. Rows past the bound are neither (untouched).
/// Answers `(delivered, refused)`.
async fn ship_isolating<T, B, P, Fut, C>(
    fresh: Vec<T>,
    retry: Vec<T>,
    body: B,
    mut post: P,
    now_ms: C,
    deadline_ms: u64,
) -> (Vec<T>, Vec<T>)
where
    T: Clone,
    B: Fn(&[T]) -> String,
    P: FnMut(String) -> Fut,
    Fut: std::future::Future<Output = std::result::Result<usize, ()>>,
    C: Fn() -> u64,
{
    let shipped =
        crate::change_flush::ship_chunks(fresh, &body, &mut post, &now_ms, deadline_ms).await;
    let (mut delivered, mut failed, mut posts) = (shipped.delivered, shipped.failed, shipped.posts);
    for row in retry {
        if posts >= crate::change_flush::CHANGE_MAX_CHUNKS || now_ms() >= deadline_ms {
            break;
        }
        posts += 1;
        match post(body(std::slice::from_ref(&row))).await {
            Ok(_) => delivered.push(row),
            Err(()) => failed.push(row),
        }
    }
    (delivered, failed)
}

/// The worker's [`UnparkPort`]: D1 and the app layer's `/internal/{pot,hop}-changed`.
struct WorkerUnpark<'a> {
    env: &'a Env,
    db: &'a D1Database,
    url: String,
    token: String,
}

impl UnparkPort for WorkerUnpark<'_> {
    async fn read(
        &self,
        kind: &'static str,
        now_ms: u64,
        limit: usize,
    ) -> std::result::Result<Vec<ParkedRow>, String> {
        #[derive(serde::Deserialize)]
        struct Row {
            item: String,
            generation: f64,
            attempts: f64,
        }
        let rows = crate::d1::Query::new(PARKED_READ_SQL)
            .bind(kind)
            .bind(now_ms as f64)
            .bind(limit as i64)
            .fetch_all::<Row>(self.db)
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| ParkedRow {
                item: r.item,
                generation: r.generation as i64,
                attempts: r.attempts as i64,
            })
            .collect())
    }
    async fn post(&self, kind: &'static str, body: String) -> std::result::Result<usize, ()> {
        let path = if kind == KIND_POT {
            "/internal/pot-changed"
        } else {
            "/internal/hop-changed"
        };
        crate::change_flush::post_body(self.env, &self.url, &self.token, path, "change-park", body)
            .await
    }
    async fn unpark(
        &self,
        kind: &'static str,
        rows: &[(String, i64)],
    ) -> std::result::Result<u64, String> {
        let mut stmts = Vec::with_capacity(rows.len());
        for (item, generation) in rows {
            stmts.push(
                self.db
                    .prepare(UNPARK_SQL)
                    .bind(&[
                        kind.into(),
                        item.as_str().into(),
                        (*generation as f64).into(),
                    ])
                    .map_err(|e| e.to_string())?,
            );
        }
        let results = crate::d1_ledger::counted_batch(self.db, stmts)
            .await
            .map_err(|e| e.to_string())?;
        Ok(results
            .iter()
            .map(|r| {
                r.results::<serde_json::Value>()
                    .map(|v| v.len() as u64)
                    .unwrap_or(0)
            })
            .sum())
    }
    async fn refuse(
        &self,
        kind: &'static str,
        rows: &[(String, i64)],
        now_ms: u64,
    ) -> std::result::Result<(u64, u64), String> {
        let mut stmts = Vec::with_capacity(rows.len());
        for (item, generation) in rows {
            stmts.push(
                self.db
                    .prepare(REFUSE_SQL)
                    .bind(&[
                        kind.into(),
                        item.as_str().into(),
                        (*generation as f64).into(),
                        (now_ms as f64).into(),
                        (CHANGE_PARK_MAX_ATTEMPTS as f64).into(),
                        (CHANGE_PARK_BACKOFF_BASE_MS as f64).into(),
                        (CHANGE_PARK_BACKOFF_MAX_MS as f64).into(),
                    ])
                    .map_err(|e| e.to_string())?,
            );
        }
        let results = crate::d1_ledger::counted_batch(self.db, stmts)
            .await
            .map_err(|e| e.to_string())?;
        let returned: Vec<serde_json::Value> = results
            .iter()
            .flat_map(|r| r.results::<serde_json::Value>().unwrap_or_default())
            .collect();
        let exhausted = returned
            .iter()
            .filter(|v| v.get("exhausted").and_then(|e| e.as_f64()) == Some(1.0))
            .count();
        Ok((returned.len() as u64, exhausted as u64))
    }
    async fn sweep(
        &self,
        kind: &'static str,
        before_ms: u64,
        limit: usize,
    ) -> std::result::Result<u64, String> {
        crate::d1::Query::new(SWEEP_EXHAUSTED_SQL)
            .bind(kind)
            .bind(before_ms as f64)
            .bind(limit as i64)
            .fetch_all::<serde_json::Value>(self.db)
            .await
            .map(|rows| rows.len() as u64)
    }
    fn now_ms(&self) -> u64 {
        Date::now().as_millis()
    }
    async fn bump(&self, counter: &'static str, delta: u64) {
        crate::ops::bump_counter(self.db, counter, delta).await;
    }
    fn log(&self, line: String) {
        console_log!("{line}");
    }
}

/// The scheduled tick's delivery (bsv-low #649 M1). Unconfigured (`APP_LAYER_URL` / `INTERNAL_TOKEN`): nothing
/// is read, the rows stay parked.
pub async fn deliver_on_tick(env: &Env, db: &D1Database) {
    let Some((url, token)) = crate::change_flush::configured(env) else {
        return;
    };
    deliver_parked(&WorkerUnpark {
        env,
        db,
        url,
        token,
    })
    .await;
}

/// `/health/invariants.changeParked`: the parked rows by set, read from D1 (`null` when the read faults). N3: each
/// count stops at [`HEALTH_COUNT_CAP`] (`capped`: a figure equal to it reads "at least").
pub async fn health_json(db: &D1Database) -> serde_json::Value {
    #[derive(serde::Deserialize)]
    struct Row {
        pot: f64,
        hop: f64,
        pot_exhausted: f64,
        hop_exhausted: f64,
    }
    match crate::d1::Query::new(PARKED_COUNT_SQL)
        .bind(HEALTH_COUNT_CAP)
        .fetch_optional::<Row>(db)
        .await
    {
        Ok(Some(r)) => serde_json::json!({
            "pot": r.pot as u64,
            "hop": r.hop as u64,
            "potExhausted": r.pot_exhausted as u64,
            "hopExhausted": r.hop_exhausted as u64,
            "capped": HEALTH_COUNT_CAP,
            "maxRows": CHANGE_PARKED_MAX_ROWS,
            "maxAttempts": CHANGE_PARK_MAX_ATTEMPTS,
            "exhaustedKeepMs": CHANGE_PARK_EXHAUSTED_KEEP_MS,
            "drainMaxRows": DRAIN_MAX_ROWS,
            "drainWallMs": DRAIN_WALL_MS,
        }),
        _ => serde_json::Value::Null,
    }
}

/// The unconfigured drain's transport (no `APP_LAYER_URL`): no POST leaves; every one is refused.
struct NoTransport<'a>(WorkerUnpark<'a>);

impl UnparkPort for NoTransport<'_> {
    async fn read(
        &self,
        kind: &'static str,
        now_ms: u64,
        limit: usize,
    ) -> std::result::Result<Vec<ParkedRow>, String> {
        self.0.read(kind, now_ms, limit).await
    }
    async fn post(&self, _kind: &'static str, _body: String) -> std::result::Result<usize, ()> {
        Err(())
    }
    async fn unpark(
        &self,
        kind: &'static str,
        rows: &[(String, i64)],
    ) -> std::result::Result<u64, String> {
        self.0.unpark(kind, rows).await
    }
    async fn refuse(
        &self,
        kind: &'static str,
        rows: &[(String, i64)],
        now_ms: u64,
    ) -> std::result::Result<(u64, u64), String> {
        self.0.refuse(kind, rows, now_ms).await
    }
    async fn sweep(
        &self,
        kind: &'static str,
        before_ms: u64,
        limit: usize,
    ) -> std::result::Result<u64, String> {
        self.0.sweep(kind, before_ms, limit).await
    }
    fn now_ms(&self) -> u64 {
        self.0.now_ms()
    }
    async fn bump(&self, counter: &'static str, delta: u64) {
        self.0.bump(counter, delta).await
    }
    fn log(&self, line: String) {
        self.0.log(line)
    }
}

/// `POST /internal/change-park` (bsv-low #649 N3; bearer `INTERNAL_TOKEN`): ONE step of the parked set's shipped
/// code on demand, so the D1 budget tier measures it on a populated table (each answer carries its `d1`
/// Server-Timing). Body `{"op":"park","items":["<txid>:<vout>",..],"hops"?:["<identity>",..]}` (the pot items and
/// the hop identities noted, then the shipped skip [`park_withheld`]: what this isolate holds is parked with them;
/// the hops since D2-L2, so the route tier re-parks both sets during a delivery's POST), `{"op":"drain"}` (the shipped
/// [`deliver_parked`]; with no `APP_LAYER_URL` no POST leaves and each read row is refused and backed off) or
/// `{"op":"health"}` (the gauge).
pub async fn internal_lever(
    mut req: worker::Request,
    env: &Env,
) -> worker::Result<worker::Response> {
    let authorization = req.headers().get("authorization").ok().flatten();
    let secret = env.secret("INTERNAL_TOKEN").ok().map(|s| s.to_string());
    if !crate::tip_pass::bearer_ok(authorization.as_deref(), secret.as_deref()) {
        return worker::Response::error("unauthorized", 401);
    }
    #[derive(serde::Deserialize)]
    struct Body {
        op: String,
        #[serde(default)]
        items: Vec<String>,
        #[serde(default)]
        hops: Vec<String>,
    }
    let Ok(body) = serde_json::from_slice::<Body>(&req.bytes().await?) else {
        return worker::Response::error(
            "body must be {\"op\": \"park\" | \"drain\" | \"health\"}",
            400,
        );
    };
    let db = env.d1("OVERLAY_DB")?;
    crate::d1::ensure_overlay_migrations(&db)
        .await
        .map_err(worker::Error::from)?;
    match body.op.as_str() {
        "park" => {
            let pots: Vec<(String, u32)> = body
                .items
                .iter()
                .filter_map(|i| parse_pot_item(i))
                .collect();
            let hops_ok = body
                .hops
                .iter()
                .all(|h| h.len() == 66 && h.bytes().all(|b| b.is_ascii_hexdigit()));
            if pots.len() != body.items.len()
                || pots.len() + body.hops.len() > DRAIN_MAX_ROWS
                || !hops_ok
            {
                return worker::Response::error(
                    "items: \"<txid>:<vout>\", hops: 66-hex identities, at most 128 together",
                    400,
                );
            }
            for (t, v) in &pots {
                crate::pot_changes::note(t, *v);
            }
            for h in &body.hops {
                crate::hop_changes::note(h, h);
            }
            park_withheld(&WorkerPark { db: &db }, 0, Date::now().as_millis()).await;
            let left = crate::pot_changes::snapshot();
            let kept = pots
                .iter()
                .filter(|p| left.iter().any(|(k, _)| k == *p))
                .count();
            worker::Response::from_json(
                &serde_json::json!({ "op": "park", "items": pots.len(), "keptInMemory": kept }),
            )
        }
        "drain" => {
            let configured = crate::change_flush::configured(env);
            let d = match configured {
                Some((url, token)) => {
                    deliver_parked(&WorkerUnpark {
                        env,
                        db: &db,
                        url,
                        token,
                    })
                    .await
                }
                None => {
                    deliver_parked(&NoTransport(WorkerUnpark {
                        env,
                        db: &db,
                        url: String::new(),
                        token: String::new(),
                    }))
                    .await
                }
            };
            worker::Response::from_json(&serde_json::json!({
                "op": "drain", "read": d.read, "unparked": d.unparked, "kept": d.kept,
                "refused": d.refused, "exhausted": d.exhausted, "swept": d.swept,
            }))
        }
        "health" => worker::Response::from_json(&health_json(&db).await),
        _ => worker::Response::error("op: park | drain | health", 400),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    fn run<F: std::future::Future>(f: F) -> F::Output {
        let mut f = std::pin::pin!(f);
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        match f.as_mut().poll(&mut cx) {
            std::task::Poll::Ready(v) => v,
            std::task::Poll::Pending => panic!("the fakes answer at once"),
        }
    }

    /// The skip's world, recorded: D1's parked rows and counters as a model; `fail` makes every park fault.
    #[derive(Default)]
    struct ModelPark {
        rows: RefCell<Vec<(String, String)>>,
        counters: RefCell<Vec<(&'static str, u64)>>,
        fail: bool,
    }

    impl ParkPort for ModelPark {
        async fn park(
            &self,
            rows: &[(&'static str, String)],
            _now_ms: u64,
        ) -> std::result::Result<Vec<bool>, String> {
            if self.fail {
                return Err("D1 down".into());
            }
            for (k, i) in rows {
                self.rows.borrow_mut().push((k.to_string(), i.clone()));
            }
            Ok(vec![true; rows.len()])
        }
        async fn bump(&self, counter: &'static str, delta: u64) {
            self.counters.borrow_mut().push((counter, delta));
        }
        fn log(&self, _line: String) {}
    }

    const ID: &str = "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    /// bsv-low #649 M1 (the lens's thread-local skip): a background flush skipped for want of window took the eviction's
    /// pot note and the hop note OUT of the isolate's memory into durable rows, counted in ENTRIES. On `c8e4d8f` the
    /// skip only logged and counted the job: the notes stayed in the thread-local sets, nothing durable.
    #[test]
    fn e649_m1_a_skipped_flush_parks_its_notes_durably_and_counts_the_entries() {
        let txid = "AB".repeat(32);
        crate::pot_changes::note(&txid, 3);
        crate::hop_changes::note(ID, ID);
        let port = ModelPark::default();
        run(park_withheld(&port, 2_000, 1_000));
        assert_eq!(
            *port.rows.borrow(),
            vec![
                ("pot".to_string(), format!("{}:3", "ab".repeat(32))),
                ("hop".to_string(), ID.to_string())
            ],
            "the withheld pot and hop notes are parked in D1"
        );
        assert!(
            crate::pot_changes::drain().is_empty() && crate::hop_changes::drain().is_empty(),
            "and no longer in memory only"
        );
        assert_eq!(
            *port.counters.borrow(),
            vec![
                (crate::ops::COUNTER_ADMIT_FAST_FLUSH_SKIPPED_NO_WINDOW, 1),
                (COUNTER_ADMIT_FAST_FLUSH_WITHHELD, 2),
                (COUNTER_CHANGE_PARKED, 2),
            ],
            "the skip is counted once, the withheld and parked ENTRIES by their number"
        );
        // a park that faults keeps the notes in the isolate's sets (the old behaviour), counted
        crate::pot_changes::note(&txid, 4);
        let faulting = ModelPark {
            fail: true,
            ..Default::default()
        };
        run(park_withheld(&faulting, 2_000, 1_000));
        assert!(faulting.rows.borrow().is_empty());
        assert_eq!(
            crate::pot_changes::drain(),
            vec![("ab".repeat(32), 4)],
            "a faulted park notes back"
        );
        assert!(faulting
            .counters
            .borrow()
            .contains(&(COUNTER_CHANGE_PARK_FAULTS, 1)));
    }

    /// The skip's world with ONE await that never answers: `hang` names it (`park`, or the n-th counter bump).
    struct HangingPark {
        rows: RefCell<Vec<(String, String)>>,
        bumps: RefCell<usize>,
        hang: &'static str,
    }

    impl ParkPort for HangingPark {
        async fn park(
            &self,
            rows: &[(&'static str, String)],
            _now_ms: u64,
        ) -> std::result::Result<Vec<bool>, String> {
            if self.hang == "park" {
                std::future::pending::<()>().await;
            }
            for (k, i) in rows {
                self.rows.borrow_mut().push((k.to_string(), i.clone()));
            }
            Ok(vec![true; rows.len()])
        }
        async fn bump(&self, _counter: &'static str, _delta: u64) {
            let n = *self.bumps.borrow();
            *self.bumps.borrow_mut() += 1;
            if (self.hang == "first counter" && n == 0) || (self.hang == "second counter" && n == 1)
            {
                std::future::pending::<()>().await;
            }
        }
        fn log(&self, _line: String) {}
    }

    /// bsv-low #649 M1 (the delta lens's three cancellation boundaries): the skip is cancelled while its first
    /// counter, its second counter or its park is pending (polled once, then dropped, as the runtime drops a
    /// `waitUntil` task). Every withheld note must still exist, in D1 or in the isolate's sets. On `1516ed5` the skip
    /// drained both sets BEFORE the counters and the park: at the first two boundaries nothing was left anywhere.
    #[test]
    fn e649_m1_a_skip_cancelled_at_any_await_keeps_every_note() {
        let txid = "AB".repeat(32);
        let pot = format!("{}:5", "ab".repeat(32));
        for hang in ["first counter", "second counter", "park"] {
            let _ = crate::pot_changes::drain();
            let _ = crate::hop_changes::drain();
            crate::pot_changes::note(&txid, 5);
            crate::hop_changes::note(ID, ID);
            let port = HangingPark {
                rows: Default::default(),
                bumps: RefCell::new(0),
                hang,
            };
            {
                let mut fut = std::pin::pin!(park_withheld(&port, 2_000, 1_000));
                let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
                assert!(
                    std::future::Future::poll(fut.as_mut(), &mut cx).is_pending(),
                    "{hang}: the skip is suspended there"
                );
            } // dropped: the runtime cancelled the task
            let durable = port.rows.borrow().clone();
            let memory_pots = crate::pot_changes::drain();
            let memory_hops = crate::hop_changes::drain();
            let pot_kept = durable.contains(&("pot".to_string(), pot.clone()))
                || memory_pots.contains(&("ab".repeat(32), 5));
            let hop_kept = durable.contains(&("hop".to_string(), ID.to_string()))
                || memory_hops.contains(&ID.to_string());
            assert!(
                pot_kept && hop_kept,
                "cancelled at the {hang}: durable={} memory={}: a withheld note was destroyed",
                durable.len(),
                memory_pots.len() + memory_hops.len()
            );
        }
    }

    /// bsv-low #649 M1 (structural): the park is the skip's FIRST durable write and the sets are cleared only after
    /// it answered: no `drain()` in the skip, `forget` only in the park's `Ok` arm, every counter after the park.
    #[test]
    fn e649_m1_the_park_is_the_skips_first_durable_write() {
        let sq = |s: &str| s.split_whitespace().collect::<String>();
        let src = sq(include_str!("change_park.rs"));
        let body = &src[src.find("pub(crate)asyncfnpark_withheld<").unwrap()..];
        let body = &body[..body.find("///Theworker's[`ParkPort`]").unwrap()];
        assert!(
            !body.contains("drain()"),
            "the skip never drains: it copies, parks, then forgets"
        );
        let park = body
            .find("port.park(&rows,now_ms).await")
            .expect("the park");
        let ok = body.find("Ok(landed)=>{").expect("the park's Ok arm");
        let forget = body
            .find("crate::pot_changes::forget(&parked_pots);crate::hop_changes::forget(&parked_hops);")
            .expect("forgotten only once parked");
        assert!(
            ok < forget && body.matches("forget(").count() == 2,
            "the sets are cleared only in the park's Ok arm, of what it landed"
        );
        let first_bump = body.find("port.bump(").unwrap();
        assert!(
            park < ok && forget < first_bump,
            "the park is the first await; the counters after it"
        );
    }

    /// The cron's world: the parked rows and a transport whose `fail_hop`/`fail_pot_chunk` POSTs are refused.
    struct ModelUnpark {
        rows: RefCell<Vec<(String, String)>>,
        posts: RefCell<Vec<(String, String)>>,
        fail_pot_chunk: Option<usize>,
        pot_posts: RefCell<usize>,
        unparked: RefCell<u64>,
    }

    impl UnparkPort for ModelUnpark {
        async fn read(
            &self,
            kind: &'static str,
            _now_ms: u64,
            limit: usize,
        ) -> std::result::Result<Vec<ParkedRow>, String> {
            Ok(self
                .rows
                .borrow()
                .iter()
                .filter(|(k, _)| k == kind)
                .take(limit)
                .map(|(_, i)| ParkedRow {
                    item: i.clone(),
                    generation: 0,
                    attempts: 0,
                })
                .collect())
        }
        async fn post(&self, kind: &'static str, body: String) -> std::result::Result<usize, ()> {
            self.posts.borrow_mut().push((kind.to_string(), body));
            if kind == KIND_POT {
                let n = *self.pot_posts.borrow();
                *self.pot_posts.borrow_mut() += 1;
                if Some(n) == self.fail_pot_chunk {
                    return Err(());
                }
            }
            Ok(0)
        }
        async fn unpark(
            &self,
            kind: &'static str,
            rows: &[(String, i64)],
        ) -> std::result::Result<u64, String> {
            let before = self.rows.borrow().len();
            self.rows
                .borrow_mut()
                .retain(|(k, i)| !(k == kind && rows.iter().any(|(r, _)| r == i)));
            Ok((before - self.rows.borrow().len()) as u64)
        }
        async fn refuse(
            &self,
            _kind: &'static str,
            rows: &[(String, i64)],
            _now_ms: u64,
        ) -> std::result::Result<(u64, u64), String> {
            Ok((rows.len() as u64, 0))
        }
        async fn sweep(
            &self,
            _kind: &'static str,
            _before_ms: u64,
            _limit: usize,
        ) -> std::result::Result<u64, String> {
            Ok(0)
        }
        fn now_ms(&self) -> u64 {
            0
        }
        async fn bump(&self, counter: &'static str, delta: u64) {
            if counter == COUNTER_CHANGE_UNPARKED {
                *self.unparked.borrow_mut() += delta;
            }
        }
        fn log(&self, _line: String) {}
    }

    /// bsv-low #649 M1: the cron delivers the parked rows through the one flush loop and deletes ONLY the rows of
    /// accepted POSTs; a refused POST's rows stay parked for the next tick, then go.
    #[test]
    fn e649_m1_the_cron_delivers_the_parked_rows_and_keeps_what_was_not_accepted() {
        let mut rows: Vec<(String, String)> = (0..10u32)
            .map(|v| ("pot".to_string(), pot_item(&"cd".repeat(32), v)))
            .collect();
        rows.push(("hop".into(), ID.into()));
        let port = ModelUnpark {
            rows: RefCell::new(rows),
            posts: Default::default(),
            fail_pot_chunk: Some(1),
            pot_posts: RefCell::new(0),
            unparked: RefCell::new(0),
        };
        let d = run(deliver_parked(&port));
        assert_eq!(
            d,
            Delivered {
                read: 11,
                unparked: 9,
                kept: 2,
                refused: 2,
                ..Default::default()
            },
            "the hop and the first pot chunk of 8 delivered"
        );
        assert_eq!(
            port.rows.borrow().len(),
            2,
            "the refused chunk's two rows stay parked"
        );
        assert!(
            port.posts.borrow()[0].1.contains(ID),
            "the hop body is the set's own"
        );
        assert!(
            port.posts.borrow()[1].1.contains("\"outpoints\""),
            "the pot body is the set's own"
        );
        let again = ModelUnpark {
            fail_pot_chunk: None,
            posts: Default::default(),
            pot_posts: RefCell::new(0),
            unparked: RefCell::new(0),
            rows: RefCell::new(port.rows.take()),
        };
        assert_eq!(
            run(deliver_parked(&again)),
            Delivered {
                read: 2,
                unparked: 2,
                ..Default::default()
            }
        );
        assert!(
            again.rows.borrow().is_empty(),
            "the next tick delivers the rest"
        );
        assert_eq!(*port.unparked.borrow() + *again.unparked.borrow(), 11);
    }

    const MAX: i64 = CHANGE_PARKED_MAX_ROWS;

    /// The shipped park (it RETURNS the row it parked): `true` when parked.
    fn park_sql<P: rusqlite::Params>(
        conn: &rusqlite::Connection,
        params: P,
    ) -> rusqlite::Result<bool> {
        let mut q = conn.prepare(PARK_SQL)?;
        let n = q.query_map(params, |r| r.get::<_, String>(0))?.count();
        Ok(n == 1)
    }

    /// [`REFUSE_SQL`]'s binds, as the worker binds them.
    fn refuse_params(
        kind: &str,
        item: &str,
        generation: i64,
        now_ms: u64,
    ) -> impl rusqlite::Params {
        (
            kind.to_string(),
            item.to_string(),
            generation,
            now_ms as i64,
            CHANGE_PARK_MAX_ATTEMPTS,
            CHANGE_PARK_BACKOFF_BASE_MS as i64,
            CHANGE_PARK_BACKOFF_MAX_MS as i64,
        )
    }

    /// A shipped statement that RETURNS its changed rows (D2-L1), run: the rows it changed.
    fn exec_returning<P: rusqlite::Params>(
        conn: &rusqlite::Connection,
        sql: &str,
        params: P,
    ) -> usize {
        let mut q = conn.prepare(sql).unwrap();
        q.query_map(params, |_| Ok(())).unwrap().count()
    }

    /// The table as the migrations build it: every `change_parked` statement of the SHIPPED list, in its order.
    fn parked_db() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        let mine: Vec<&str> = crate::d1::OVERLAY_MIGRATIONS
            .iter()
            .copied()
            .filter(|m| m.contains("change_parked"))
            .collect();
        assert!(mine.contains(&CHANGE_PARKED_CREATE) && mine.len() >= 5);
        for m in mine {
            conn.execute_batch(m).unwrap();
        }
        conn
    }

    /// The cron's world over the SHIPPED statements under SQLite; the app layer refuses every POST naming an entry
    /// in `refuse`; `clock` is the tick's time.
    struct SqlitePort {
        conn: rusqlite::Connection,
        refuse: std::collections::BTreeSet<String>,
        clock: std::cell::Cell<u64>,
        posted: RefCell<Vec<String>>,
        refused_posts: RefCell<usize>,
        /// D2-L1: every POST re-parks the rows it carries before it answers (a later change during the POST).
        repark: bool,
        /// D2-L3: the rows the last read answered, and a read to answer INSTEAD of the table (an overlapping
        /// delivery that read before another's back-off landed).
        last_read: RefCell<Vec<ParkedRow>>,
        stale_read: RefCell<Option<Vec<ParkedRow>>>,
    }

    impl UnparkPort for SqlitePort {
        async fn read(
            &self,
            kind: &'static str,
            now_ms: u64,
            limit: usize,
        ) -> std::result::Result<Vec<ParkedRow>, String> {
            if let Some(stale) = self.stale_read.borrow().clone() {
                return Ok(stale.into_iter().filter(|_| kind == KIND_POT).collect());
            }
            let mut q = self.conn.prepare(PARKED_READ_SQL).unwrap();
            let rows: Vec<ParkedRow> = q
                .query_map(rusqlite::params![kind, now_ms as i64, limit as i64], |r| {
                    Ok(ParkedRow {
                        item: r.get(0)?,
                        generation: r.get(1)?,
                        attempts: r.get(2)?,
                    })
                })
                .unwrap()
                .map(|r| r.unwrap())
                .collect();
            if !rows.is_empty() {
                *self.last_read.borrow_mut() = rows.clone();
            }
            Ok(rows)
        }
        async fn post(&self, kind: &'static str, body: String) -> std::result::Result<usize, ()> {
            self.posted.borrow_mut().push(body.clone());
            if self.repark {
                let items: Vec<String> = self
                    .conn
                    .prepare("SELECT item FROM change_parked WHERE kind = ?1")
                    .unwrap()
                    .query_map([kind], |r| r.get(0))
                    .unwrap()
                    .map(|r| r.unwrap())
                    .collect();
                for i in items {
                    let carried = if kind == KIND_POT {
                        parse_pot_item(&i).is_some_and(|(t, _)| body.contains(&t))
                    } else {
                        body.contains(&i)
                    };
                    if carried {
                        park_sql(
                            &self.conn,
                            rusqlite::params![kind, i, self.clock.get() as i64 + 1, MAX],
                        )
                        .unwrap();
                    }
                }
            }
            if self.refuse.iter().any(|t| body.contains(t.as_str())) {
                *self.refused_posts.borrow_mut() += 1;
                return Err(());
            }
            Ok(0)
        }
        async fn unpark(
            &self,
            kind: &'static str,
            rows: &[(String, i64)],
        ) -> std::result::Result<u64, String> {
            let mut n = 0u64;
            for (i, g) in rows {
                let mut q = self.conn.prepare(UNPARK_SQL).unwrap();
                n += q
                    .query_map(rusqlite::params![kind, i, g], |r| r.get::<_, String>(0))
                    .unwrap()
                    .count() as u64;
            }
            Ok(n)
        }
        async fn refuse(
            &self,
            kind: &'static str,
            rows: &[(String, i64)],
            now_ms: u64,
        ) -> std::result::Result<(u64, u64), String> {
            let (mut n, mut ex_n) = (0u64, 0u64);
            for (i, g) in rows {
                let mut q = self.conn.prepare(REFUSE_SQL).unwrap();
                for e in q
                    .query_map(refuse_params(kind, i, *g, now_ms), |r| r.get::<_, i64>(0))
                    .unwrap()
                {
                    n += 1;
                    ex_n += (e.unwrap() == 1) as u64;
                }
            }
            Ok((n, ex_n))
        }
        async fn sweep(
            &self,
            kind: &'static str,
            before_ms: u64,
            limit: usize,
        ) -> std::result::Result<u64, String> {
            let mut q = self.conn.prepare(SWEEP_EXHAUSTED_SQL).unwrap();
            let n = q
                .query_map(
                    rusqlite::params![kind, before_ms as i64, limit as i64],
                    |r| r.get::<_, String>(0),
                )
                .unwrap()
                .count();
            Ok(n as u64)
        }
        fn now_ms(&self) -> u64 {
            self.clock.get()
        }
        async fn bump(&self, _counter: &'static str, _delta: u64) {}
        fn log(&self, _line: String) {}
    }

    fn rows_of(conn: &rusqlite::Connection, sql: &str) -> i64 {
        conn.query_row(sql, [], |r| r.get(0)).unwrap()
    }

    const TICK_MS: u64 = 15 * 60 * 1000;

    /// bsv-low #649 N2 (the delta lens's refused prefix, the SHIPPED statements and the shipped delivery): 129 pot
    /// rows, the first 128 refused for good, the 129th acceptable. The 129th must be delivered within a bounded
    /// number of ticks, and the refused rows must leave the drain and the table. On `1516ed5` three passes made 48
    /// refused POSTs, kept all 129 rows and never POSTed the 129th.
    #[test]
    fn e649_n2_a_refused_prefix_never_starves_a_later_row_and_is_swept() {
        let conn = parked_db();
        let txid = |i: u32| format!("{:064x}", i + 1);
        for i in 0..129u32 {
            park_sql(
                &conn,
                rusqlite::params![KIND_POT, pot_item(&txid(i), 0), 1_000 + i as i64, MAX],
            )
            .unwrap();
        }
        let port = SqlitePort {
            conn,
            refuse: (0..128u32).map(txid).collect(),
            clock: std::cell::Cell::new(10_000),
            posted: Default::default(),
            refused_posts: RefCell::new(0),
            repark: false,
            last_read: Default::default(),
            stale_read: Default::default(),
        };
        let last = txid(128);
        let mut delivered_at = None;
        for tick in 0..3u64 {
            port.clock.set(10_000 + tick * TICK_MS);
            run(deliver_parked(&port));
            if delivered_at.is_none() && port.posted.borrow().iter().any(|b| b.contains(&last)) {
                delivered_at = Some(tick);
            }
        }
        assert!(
            delivered_at.is_some(),
            "the 129th row was never POSTed in 3 ticks: {} refused POSTs, {} rows kept",
            port.refused_posts.borrow(),
            rows_of(&port.conn, "SELECT COUNT(*) FROM change_parked")
        );
        assert_eq!(
            rows_of(
                &port.conn,
                "SELECT COUNT(*) FROM change_parked WHERE exhausted = 0"
            ),
            128,
            "the 129th delivered and deleted; the refused rows backed off"
        );
        // the refused rows exhaust at their 8th refusal and the sweep takes them a week later
        for tick in 3..400u64 {
            port.clock.set(10_000 + tick * TICK_MS);
            run(deliver_parked(&port));
        }
        assert_eq!(
            rows_of(
                &port.conn,
                "SELECT COUNT(*) FROM change_parked WHERE exhausted = 0"
            ),
            0,
            "every refused row left the drain"
        );
        let posts = *port.refused_posts.borrow();
        assert_eq!(
            posts,
            16 + 128 * (CHANGE_PARK_MAX_ATTEMPTS as usize - 1),
            "each refused row was POSTed exactly its bounded attempts (once in its chunk, then alone)"
        );
        port.clock
            .set(10_000 + 400 * TICK_MS + CHANGE_PARK_EXHAUSTED_KEEP_MS);
        run(deliver_parked(&port));
        assert_eq!(
            rows_of(&port.conn, "SELECT COUNT(*) FROM change_parked"),
            0,
            "a week after its exhaustion each refused row is swept"
        );
    }

    /// bsv-low #649 N2: the TOTAL bound. A full table refuses the NEWEST key (no row returned, kept in memory by
    /// the skip, counted `change_park_full_total`), while a re-park of a held key still lands, bumps its generation
    /// and resets its retry state.
    #[test]
    fn e649_n2_a_full_table_refuses_the_newest_and_a_held_key_still_re_parks() {
        let conn = parked_db();
        let park = |item: &str, at: i64, max: i64| -> bool {
            let mut q = conn.prepare(PARK_SQL).unwrap();
            q.query_map(rusqlite::params![KIND_POT, item, at, max], |r| {
                r.get::<_, String>(0)
            })
            .unwrap()
            .count()
                == 1
        };
        assert!(park("a:0", 10, 2) && park("b:0", 20, 2));
        assert!(!park("c:0", 30, 2), "full: the newest is refused");
        let g0: i64 = conn
            .query_row(
                "SELECT generation FROM change_parked WHERE item = 'a:0'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        conn.execute(
            "UPDATE change_parked SET attempts = 8, exhausted = 1, next_attempt_at_ms = 99999 WHERE item = 'a:0'",
            [],
        )
        .unwrap();
        assert!(park("a:0", 40, 2), "a held key re-parks when full");
        let (g, a, ex, next): (i64, i64, i64, i64) = conn
            .query_row(
                "SELECT generation, attempts, exhausted, next_attempt_at_ms FROM change_parked WHERE item = 'a:0'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert!(g > g0, "a new intent token: {g} after {g0}");
        assert_eq!((a, ex, next), (0, 0, 40), "a new intent: eligible now");
        // the skip keeps what was refused in memory and counts it
        let _ = crate::pot_changes::drain();
        let _ = crate::hop_changes::drain();
        let t = "cd".repeat(32);
        crate::pot_changes::note(&t, 0);
        crate::pot_changes::note(&t, 1);
        struct Half(RefCell<Vec<(&'static str, u64)>>);
        impl ParkPort for Half {
            async fn park(
                &self,
                rows: &[(&'static str, String)],
                _now_ms: u64,
            ) -> std::result::Result<Vec<bool>, String> {
                Ok((0..rows.len()).map(|i| i == 0).collect())
            }
            async fn bump(&self, c: &'static str, d: u64) {
                self.0.borrow_mut().push((c, d));
            }
            fn log(&self, _line: String) {}
        }
        let port = Half(Default::default());
        run(park_withheld(&port, 2_000, 1_000));
        assert_eq!(
            crate::pot_changes::drain(),
            vec![(t.clone(), 1)],
            "the refused one stays in memory"
        );
        assert!(port.0.borrow().contains(&(COUNTER_CHANGE_PARK_FULL, 1)));
        assert!(port.0.borrow().contains(&(COUNTER_CHANGE_PARKED, 1)));
        // backoff is bounded and doubles
        assert_eq!(refusal_backoff(0, 0), (CHANGE_PARK_BACKOFF_BASE_MS, false));
        assert_eq!(
            refusal_backoff(1, 0),
            (2 * CHANGE_PARK_BACKOFF_BASE_MS, false)
        );
        assert_eq!(refusal_backoff(CHANGE_PARK_MAX_ATTEMPTS - 1, 7), (7, true));
        assert!(refusal_backoff(6, 0).0 <= CHANGE_PARK_BACKOFF_MAX_MS);
    }

    /// The plan and the VM steps of one shipped statement.
    fn plan_and_steps<P: rusqlite::Params + Clone>(
        conn: &rusqlite::Connection,
        sql: &str,
        params: P,
    ) -> (String, i64) {
        let mut q = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
        let plan: Vec<String> = q
            .query_map(params.clone(), |r| r.get::<_, String>(3))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        let mut q = conn.prepare(sql).unwrap();
        let n = q.query_map(params, |_| Ok(())).unwrap().count();
        assert!(n <= 10_001);
        let steps = q.get_status(rusqlite::StatementStatus::VmStep) as i64;
        (plan.join(" | "), steps)
    }

    /// bsv-low #649 N3 (the delta lens's 10,001 rows): the drain, the sweep and the gauge read through the drain's
    /// index, with NO temp B-tree and no scan of the whole set, and their work is bounded by what they return, not
    /// by the backlog. On `1516ed5` the drain's plan was `SEARCH change_parked USING INDEX
    /// sqlite_autoindex_change_parked_1 (kind=?) | USE TEMP B-TREE FOR ORDER BY` (90,412 VM steps for 128 rows).
    #[test]
    fn e649_n3_the_drain_is_served_by_its_index_on_a_populated_table() {
        let conn = parked_db();
        assert!(crate::d1::OVERLAY_MIGRATIONS.contains(&CHANGE_PARKED_DRAIN_INDEX));
        conn.execute_batch("BEGIN").unwrap();
        for i in 0..10_001i64 {
            park_sql(
                &conn,
                rusqlite::params![KIND_POT, format!("{i:064x}:0"), 1_000 + i, 1_000_000],
            )
            .unwrap();
        }
        conn.execute_batch("COMMIT").unwrap();
        let (drain, drain_steps) = plan_and_steps(
            &conn,
            PARKED_READ_SQL,
            rusqlite::params![KIND_POT, 1_000_000_000i64, DRAIN_MAX_ROWS as i64],
        );
        assert!(
            drain.contains("change_parked_drain") && !drain.contains("TEMP B-TREE"),
            "the drain's plan: {drain}"
        );
        assert!(
            drain_steps < 4_000,
            "128 rows of 10,001 in {drain_steps} VM steps"
        );
        let (sweep, sweep_steps) = plan_and_steps(
            &conn,
            "SELECT rowid FROM change_parked WHERE kind = ?1 AND exhausted = 1 AND next_attempt_at_ms <= ?2 \
             ORDER BY next_attempt_at_ms LIMIT ?3",
            rusqlite::params![KIND_POT, 1_000_000_000i64, DRAIN_MAX_ROWS as i64],
        );
        assert!(
            sweep.contains("change_parked_drain")
                && !sweep.contains("TEMP B-TREE")
                && sweep_steps < 200,
            "the sweep's selection: {sweep} in {sweep_steps} VM steps"
        );
        let (gauge, gauge_steps) =
            plan_and_steps(&conn, PARKED_COUNT_SQL, rusqlite::params![HEALTH_COUNT_CAP]);
        assert!(
            gauge_steps < 16 * HEALTH_COUNT_CAP,
            "the gauge stops at its cap: {gauge} in {gauge_steps} VM steps"
        );
        // the park's ceiling read stops at the ceiling
        let (_, park_steps) = plan_and_steps(
            &conn,
            PARK_SQL,
            rusqlite::params![KIND_POT, "new:0", 2_000_000i64, MAX],
        );
        assert!(
            park_steps < 8 * MAX + 400,
            "a park on a full table reads at most its ceiling: {park_steps} VM steps"
        );
    }

    /// bsv-low #649 M1: the SHIPPED statements under real SQLite: a re-park is one row with its first stamp, the read
    /// is oldest first and bounded, the delete takes the one row; the table is in the migrations; the entry keys
    /// read back.
    #[test]
    fn e649_m1_the_shipped_statements_under_sqlite() {
        let conn = parked_db();
        for (item, at) in [("b:1", 20), ("a:0", 10), ("b:1", 30), ("c:2", 15)] {
            park_sql(&conn, rusqlite::params![KIND_POT, item, at, MAX]).unwrap();
        }
        park_sql(&conn, rusqlite::params![KIND_HOP, ID, 5, MAX]).unwrap();
        let read = |limit: i64| -> Vec<String> {
            let mut q = conn.prepare(PARKED_READ_SQL).unwrap();
            q.query_map(rusqlite::params![KIND_POT, 1_000, limit], |r| r.get(0))
                .unwrap()
                .map(|r| r.unwrap())
                .collect()
        };
        assert_eq!(
            read(10),
            vec!["a:0", "c:2", "b:1"],
            "oldest first, a re-park keeps its first stamp"
        );
        assert_eq!(read(2), vec!["a:0", "c:2"]);
        let g: i64 = conn
            .query_row(
                "SELECT generation FROM change_parked WHERE item = 'c:2'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            exec_returning(&conn, UNPARK_SQL, rusqlite::params![KIND_POT, "c:2", 0]),
            0,
            "D2-M1: a parked row's generation is its intent token, never the default 0"
        );
        exec_returning(&conn, UNPARK_SQL, rusqlite::params![KIND_POT, "c:2", g]);
        assert_eq!(read(10), vec!["a:0", "b:1"]);
        let counts: (i64, i64, i64, i64) = conn
            .query_row(PARKED_COUNT_SQL, rusqlite::params![HEALTH_COUNT_CAP], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
            })
            .unwrap();
        assert_eq!(counts, (2, 1, 0, 0));
        let t = "EF".repeat(32);
        assert_eq!(parse_pot_item(&pot_item(&t, 7)), Some(("ef".repeat(32), 7)));
        assert_eq!(parse_pot_item("zz:1"), None);
    }

    /// bsv-low #649 N1 (the delta lens's race, the SHIPPED statements under SQLite): park K at 100, a delivery
    /// selects it, K is parked AGAIN at 200 (a later change), then the first delivery's POST is accepted and it
    /// unparks what it read. The later change must still be parked. On `1516ed5` the re-park was ignored and the
    /// key-only DELETE erased the sole row: state 2 had no pending notification.
    #[test]
    fn e649_n1_a_re_park_during_a_delivery_survives_that_deliverys_delete() {
        let conn = parked_db();
        let k = pot_item(&"ab".repeat(32), 0);
        park_sql(&conn, rusqlite::params![KIND_POT, k, 100, MAX]).unwrap();
        let selected: Vec<(String, i64)> = {
            let mut q = conn.prepare(PARKED_READ_SQL).unwrap();
            q.query_map(rusqlite::params![KIND_POT, 150, 128], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
        };
        assert_eq!(selected.len(), 1);
        park_sql(&conn, rusqlite::params![KIND_POT, k, 200, MAX]).unwrap();
        for (item, generation) in &selected {
            exec_returning(
                &conn,
                UNPARK_SQL,
                rusqlite::params![KIND_POT, item, generation],
            );
        }
        let left: i64 = conn
            .query_row("SELECT COUNT(*) FROM change_parked", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            left, 1,
            "the change parked at 200 during the delivery of 100 is still parked"
        );
        // and its own delivery takes it
        let again: (String, i64) = conn
            .query_row(
                PARKED_READ_SQL,
                rusqlite::params![KIND_POT, 250, 128],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        exec_returning(
            &conn,
            UNPARK_SQL,
            rusqlite::params![KIND_POT, again.0, again.1],
        );
        let left: i64 = conn
            .query_row("SELECT COUNT(*) FROM change_parked", [], |r| r.get(0))
            .unwrap();
        assert_eq!(left, 0);
    }

    /// bsv-low #649 D2-M1 (the second delta lens, the SHIPPED statements under SQLite, both sets): two deliveries
    /// select K at the same generation, delivery A's accepted POST deletes it, a LATER park inserts K again, then
    /// delivery B's accepted POST deletes what IT read. The later park is a new intent and must still be parked. On
    /// `a80e197` an insert took the default generation 0 again, so B's guarded DELETE removed the new row.
    #[test]
    fn e649_d2_m1_a_key_parked_again_after_its_delete_is_a_new_intent() {
        for (kind, k) in [
            (KIND_POT, pot_item(&"ab".repeat(32), 0)),
            (KIND_HOP, ID.to_string()),
        ] {
            let conn = parked_db();
            park_sql(&conn, rusqlite::params![kind, k, 100, MAX]).unwrap();
            let select = || -> (String, i64) {
                conn.query_row(PARKED_READ_SQL, rusqlite::params![kind, 1_000, 128], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })
                .unwrap()
            };
            let a = select();
            let b = select();
            exec_returning(&conn, UNPARK_SQL, rusqlite::params![kind, a.0, a.1]);
            park_sql(&conn, rusqlite::params![kind, k, 300, MAX]).unwrap();
            exec_returning(&conn, UNPARK_SQL, rusqlite::params![kind, b.0, b.1]);
            let left = rows_of(&conn, "SELECT COUNT(*) FROM change_parked");
            assert_eq!(
                left, 1,
                "{kind}: the key parked at 300 after delivery A deleted its row was erased by delivery B's delete"
            );
        }
    }

    /// bsv-low #649 D2-M2 (the second delta lens): while the skip's park is pending, its row is delivered and deleted
    /// (another isolate's cron) and a LATER mutation notes the SAME pot and hop keys again. When the park answers,
    /// the skip must forget only the notes it parked, never the later ones. On `a80e197` the sets held keys with no
    /// version and `forget` removed the later notes: `durable=0 memory_pot=0 memory_hop=0`.
    #[test]
    fn e649_d2_m2_a_later_note_of_the_same_key_survives_the_parks_forget() {
        struct LaterNote(RefCell<Vec<(String, String)>>);
        impl ParkPort for LaterNote {
            async fn park(
                &self,
                rows: &[(&'static str, String)],
                _now_ms: u64,
            ) -> std::result::Result<Vec<bool>, String> {
                // the park commits, the cron delivers and deletes the row, then a later mutation notes it again
                crate::pot_changes::note(&"ab".repeat(32), 5);
                crate::hop_changes::note(ID, ID);
                self.0.borrow_mut().clear();
                Ok(vec![true; rows.len()])
            }
            async fn bump(&self, _counter: &'static str, _delta: u64) {}
            fn log(&self, _line: String) {}
        }
        let _ = crate::pot_changes::drain();
        let _ = crate::hop_changes::drain();
        crate::pot_changes::note(&"AB".repeat(32), 5);
        crate::hop_changes::note(ID, ID);
        let port = LaterNote(Default::default());
        run(park_withheld(&port, 2_000, 1_000));
        let durable = port.0.borrow().len();
        let memory_pot = crate::pot_changes::drain().len();
        let memory_hop = crate::hop_changes::drain().len();
        assert_eq!(
            (durable, memory_pot, memory_hop),
            (0, 1, 1),
            "durable={durable} memory_pot={memory_pot} memory_hop={memory_hop}: the later notes were forgotten"
        );
    }

    /// bsv-low #649 D2-L1 (the second delta lens): a guarded delete or back-off that changed no row is not counted.
    /// One pot and one hop row; every POST re-parks the rows it carries before it answers (a later change), so the
    /// delivery's generation-guarded DELETE (accepted) and UPDATE (refused) each touch nothing. The tally must say
    /// so: both rows kept, none unparked, none refused. On `a80e197` the delivery counted the rows it ASKED for:
    /// `read=2 unparked=2 kept=0` with both rows still parked.
    #[test]
    fn e649_d2_l1_a_guarded_no_op_is_not_counted_as_a_change() {
        let conn = parked_db();
        park_sql(
            &conn,
            rusqlite::params![KIND_POT, pot_item(&"ab".repeat(32), 0), 100, MAX],
        )
        .unwrap();
        park_sql(&conn, rusqlite::params![KIND_HOP, ID, 100, MAX]).unwrap();
        let mut port = SqlitePort {
            conn,
            refuse: Default::default(),
            clock: std::cell::Cell::new(10_000),
            posted: Default::default(),
            refused_posts: RefCell::new(0),
            repark: true,
            last_read: Default::default(),
            stale_read: Default::default(),
        };
        let d = run(deliver_parked(&port));
        assert_eq!(
            (d.read, d.unparked, d.kept),
            (2, 0, 2),
            "accepted: read={} unparked={} kept={} with {} row(s) still parked",
            d.read,
            d.unparked,
            d.kept,
            rows_of(&port.conn, "SELECT COUNT(*) FROM change_parked")
        );
        assert_eq!(rows_of(&port.conn, "SELECT COUNT(*) FROM change_parked"), 2);
        // refused this time, re-parked during the POST: the back-off touches nothing and counts nothing
        port.refuse = ["ab".repeat(32), ID.to_string()].into_iter().collect();
        port.clock.set(20_000);
        let d = run(deliver_parked(&port));
        assert_eq!(
            (d.read, d.refused, d.exhausted, d.kept),
            (2, 0, 0, 2),
            "refused: read={} refused={} exhausted={} kept={}",
            d.read,
            d.refused,
            d.exhausted,
            d.kept
        );
        assert_eq!(
            rows_of(
                &port.conn,
                "SELECT COUNT(*) FROM change_parked WHERE attempts = 0"
            ),
            2,
            "the re-parked rows' new intents carry no refusal"
        );
    }

    /// bsv-low #649 D2-L3 (the second delta lens, the SHIPPED delivery over the SHIPPED statements): two deliveries
    /// overlap (the cron and the authenticated drain lever) and both read one refused pot row at `attempts = 6`;
    /// both POSTs are refused. The row's eighth refusal must exhaust it, counted once; a third overlapping delivery
    /// on the same stale read changes nothing. On `a80e197` each back-off assigned the exhaustion its delivery
    /// computed from the stale read: `attempts=8 exhausted=0`, still eligible.
    #[test]
    fn e649_d2_l3_overlapping_refusals_exhaust_at_the_eighth_attempt() {
        let conn = parked_db();
        let t = "ab".repeat(32);
        park_sql(
            &conn,
            rusqlite::params![KIND_POT, pot_item(&t, 0), 100, MAX],
        )
        .unwrap();
        conn.execute(
            "UPDATE change_parked SET attempts = 6, next_attempt_at_ms = 0",
            [],
        )
        .unwrap();
        let port = SqlitePort {
            conn,
            refuse: [t].into_iter().collect(),
            clock: std::cell::Cell::new(10_000),
            posted: Default::default(),
            refused_posts: RefCell::new(0),
            repark: false,
            last_read: Default::default(),
            stale_read: Default::default(),
        };
        let a = run(deliver_parked(&port));
        // delivery B read the row before A's back-off landed
        *port.stale_read.borrow_mut() = Some(port.last_read.borrow().clone());
        assert_eq!(port.last_read.borrow()[0].attempts, 6);
        let b = run(deliver_parked(&port));
        let (attempts, exhausted): (i64, i64) = port
            .conn
            .query_row("SELECT attempts, exhausted FROM change_parked", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!(
            (attempts, exhausted),
            (CHANGE_PARK_MAX_ATTEMPTS, 1),
            "attempts={attempts} exhausted={exhausted}: the eighth refusal did not exhaust the row"
        );
        assert_eq!(
            (a.refused, a.exhausted, b.refused, b.exhausted),
            (1, 0, 1, 1)
        );
        let c = run(deliver_parked(&port));
        assert_eq!(
            (c.refused, c.exhausted),
            (0, 0),
            "an exhausted row is not backed off or exhausted again"
        );
        let attempts: i64 = port
            .conn
            .query_row("SELECT attempts FROM change_parked", [], |r| r.get(0))
            .unwrap();
        assert_eq!(attempts, CHANGE_PARK_MAX_ATTEMPTS);
        // the statement and its pure mirror agree at every count
        for held in 0..CHANGE_PARK_MAX_ATTEMPTS {
            port.conn
                .execute(
                    "UPDATE change_parked SET attempts = ?1, exhausted = 0",
                    [held],
                )
                .unwrap();
            let g: i64 = port
                .conn
                .query_row("SELECT generation FROM change_parked", [], |r| r.get(0))
                .unwrap();
            let item = pot_item(&"ab".repeat(32), 0);
            assert_eq!(
                exec_returning(
                    &port.conn,
                    REFUSE_SQL,
                    refuse_params(KIND_POT, &item, g, 5_000)
                ),
                1
            );
            let (next, ex): (i64, i64) = port
                .conn
                .query_row(
                    "SELECT next_attempt_at_ms, exhausted FROM change_parked",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .unwrap();
            assert_eq!(
                (next as u64, ex == 1),
                refusal_backoff(held, 5_000),
                "at attempts {held}"
            );
        }
    }

    /// bsv-low #649 N4 (the delta lens's documentation drift): the Skip variant and the skip counter say what a skip
    /// does now: a successful park takes the notes out of memory, a failed park keeps them, the tracker frames stay.
    /// On `1516ed5` the variant said "nothing drained" and the counter "The notes stay in the isolate's sets".
    #[test]
    fn e649_n4_the_skip_docs_name_the_park_the_failed_park_and_the_tracker_frames() {
        let doc_before = |src: &str, item: &str| -> String {
            let at = src.find(item).expect(item);
            let lines: Vec<&str> = src[..at]
                .lines()
                .rev()
                .skip_while(|l| l.trim().is_empty())
                .take_while(|l| l.trim_start().starts_with("///"))
                .collect();
            lines
                .into_iter()
                .rev()
                .map(|l| l.trim_start().trim_start_matches("///").trim())
                .collect::<Vec<_>>()
                .join(" ")
        };
        let skip = doc_before(
            include_str!("pot_changes.rs"),
            "    Skip { remaining_ms: u64 },",
        );
        let counter = doc_before(
            include_str!("ops.rs"),
            "pub const COUNTER_ADMIT_FAST_FLUSH_SKIPPED_NO_WINDOW",
        );
        for (name, doc) in [("the Skip variant", &skip), ("the skip counter", &counter)] {
            assert!(
                !doc.contains("nothing drained"),
                "{name} still says nothing is drained: {doc}"
            );
            assert!(
                !doc.contains("The notes stay in the isolate's sets"),
                "{name} still says the notes stay: {doc}"
            );
            assert!(doc.contains("PARKED in D1"), "{name} names the park: {doc}");
            assert!(
                doc.contains("successful park takes them out of the isolate's sets"),
                "{name}: a successful park: {doc}"
            );
            assert!(
                doc.contains("leaves them in the sets"),
                "{name}: a failed park: {doc}"
            );
            assert!(
                doc.contains("tracker frames"),
                "{name}: the tracker frames: {doc}"
            );
        }
    }

    /// bsv-low #649 M1 (structural): the skip parks through the shipped function, the tick delivers, the counters
    /// are served.
    #[test]
    fn e649_m1_the_skip_parks_and_the_tick_delivers() {
        let sq = |s: &str| s.split_whitespace().collect::<String>();
        let pots = sq(include_str!("pot_changes.rs"));
        let within = &pots[pots.find("pubasyncfnflush_inline_within(").unwrap()..];
        let within = &within[..within.find("pubfnflush<").unwrap()];
        let skip = within.find("BackgroundFlush::Skip{").unwrap();
        let ship = within.find("BackgroundFlush::Ship{").unwrap();
        assert!(
            within[skip..ship].contains("crate::change_park::park_withheld(&port,remaining_ms,"),
            "the skip parks"
        );
        let lib = sq(include_str!("lib.rs"));
        let tick = &lib[lib.find("asyncfnscheduled(").unwrap()..];
        let deliver = tick
            .find("crate::change_park::deliver_on_tick(&env,&ops_db).await;")
            .expect("the tick delivers");
        let gasp = tick
            .find("engine.start_gasp_sync(),")
            .expect("the GASP step");
        assert!(deliver < gasp, "before the GASP step and its belt");
        let ops = include_str!("ops.rs");
        for c in [
            "COUNTER_ADMIT_FAST_FLUSH_WITHHELD",
            "COUNTER_CHANGE_PARKED",
            "COUNTER_CHANGE_PARK_FAULTS",
            "COUNTER_CHANGE_UNPARKED",
            "COUNTER_CHANGE_PARK_FULL",
            "COUNTER_CHANGE_PARK_REFUSED",
            "COUNTER_CHANGE_PARK_EXHAUSTED",
            "COUNTER_CHANGE_PARK_SWEPT",
        ] {
            assert!(
                ops.contains(&format!("crate::change_park::{c},")),
                "{c} is served from 0"
            );
        }
        assert!(
            ops.contains("\"changeParked\": change_parked,"),
            "the gauge is served"
        );
    }
}
