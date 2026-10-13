//! bsv-low #649 M1: THE PARKED CHANGES. A background flush of an admit-fast job (the pending watch, the
//! `/arc-ingest` refusal and witness jobs) that reaches its flush with less than
//! [`crate::pot_changes::FLUSH_WINDOW_FLOOR_MS`] of the `waitUntil` window left starts no POST. Until the fold it
//! left its notes in the isolate's sets (`pot_changes::CHANGES`, `hop_changes::CHANGES`), which only a later
//! invocation on THAT isolate drains: a lone eviction followed by no such invocation lost its push for good, and
//! the felt waited on an unrelated wake-up. The skip counter counted jobs, not the withheld entries, and none of
//! them entered the note-back accounting.
//!
//! Now the skip DRAINS the pot and hop sets and PARKS what it withheld in D1 (`change_parked`, one row per entry,
//! keyed by its set and the entry, so a re-park is one row). The scheduled tick (every isolate's cron, `*/15`)
//! delivers the parked rows through the same flush loop (`ship_chunks`, chunked at the app layer's bound) and
//! deletes ONLY the rows of POSTs the app layer accepted; a failed or deferred row stays parked for the next tick.
//! So a withheld entry is at rest until a POST of it is accepted: at least once, no isolate needed.
//!
//! Why not the existing note-back accounting (`*_deferred_total`, `*_resent_total`, `*_noted_back_unresent`,
//! `change_flush::NotedBack`): it is per-isolate memory by design (`change_flush.rs`, "THE NOTE-BACK IS NOT
//! DURABLE"), and its `resent` is counted only by the isolate that noted the entry back, so a parked entry
//! delivered by another isolate's cron would read as lost there for good. The parked set has its own counters, in
//! entries: `admit_fast_flush_withheld_total` (every entry a skip withheld), `change_parked_total` (parked in D1),
//! `change_park_faults_total` (a park write that faulted: those entries are noted back into the isolate's sets,
//! the old behaviour, logged), `change_unparked_total` (delivered from the table and deleted). The gauge is the
//! table itself (`parked` on `/health/invariants.changeParked`, by set), a durable count read from D1, never an
//! isolate's.
//!
//! Stated: the tracker events (`tracker_events`, telemetry frames, no money view) withheld by a skip stay in the
//! isolate's memory as before. The lobby set has its own flush and is not drained by this one. The cron's
//! delivery runs before the GASP step under its own wall ([`DRAIN_WALL_MS`]) and at most [`DRAIN_MAX_ROWS`] rows
//! a set a tick; a POST in flight is not raced (as every flush: `change_flush.rs`). A delete that faults after an
//! accepted POST leaves the row, which is delivered again (over-notification is harmless: the app layer re-reads).
use worker::*;

/// The parked set (bsv-low #649 M1). Transient: a lost row costs one push the felt's own reads recover.
pub const CHANGE_PARKED_CREATE: &str = "CREATE TABLE IF NOT EXISTS change_parked (\
     kind TEXT NOT NULL, item TEXT NOT NULL, parked_at_ms INTEGER NOT NULL, \
     PRIMARY KEY (kind, item))";
/// bsv-low #649 N1 (the delta lens): the row's GENERATION, bumped by every re-park, so a delivery deletes only the
/// intent it read. Appended as its own migration (#638: never a DROP or a RENAME; the runner ignores the re-run's
/// "duplicate column").
pub const CHANGE_PARKED_ADD_GENERATION: &str =
    "ALTER TABLE change_parked ADD COLUMN generation INTEGER NOT NULL DEFAULT 0";
/// A park: one row per (set, entry); a re-park keeps the first stamp (its place in the oldest-first drain) and
/// BUMPS the generation (N1): a change parked while an earlier delivery of the same entry is in flight is a new
/// intent, which that delivery's delete does not take.
pub const PARK_SQL: &str =
    "INSERT INTO change_parked (kind, item, parked_at_ms) VALUES (?1, ?2, ?3) \
     ON CONFLICT(kind, item) DO UPDATE SET generation = change_parked.generation + 1";
/// The oldest parked entries of one set, each with the generation the delivery read.
pub const PARKED_READ_SQL: &str = "SELECT item, generation FROM change_parked WHERE kind = ?1 \
     ORDER BY parked_at_ms, item LIMIT ?2";
/// One delivered entry, deleted ONLY at the generation its delivery read (N1).
pub const UNPARK_SQL: &str =
    "DELETE FROM change_parked WHERE kind = ?1 AND item = ?2 AND generation = ?3";
/// The gauge, by set.
pub const PARKED_COUNT_SQL: &str = "SELECT kind, COUNT(*) AS n FROM change_parked GROUP BY kind";

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
    /// Park `rows` (`(set, entry)`, both sets) in ONE batch: one statement each, one D1 round trip.
    async fn park(
        &self,
        rows: &[(&'static str, String)],
        now_ms: u64,
    ) -> std::result::Result<(), String>;
    async fn bump(&self, counter: &'static str, delta: u64);
    fn log(&self, line: String);
}

/// THE SKIP of a background flush (bsv-low #649 M1; the delta lens's M1). The pot and hop sets are COPIED, the
/// copy is PARKED in D1 as the skip's FIRST durable write (one batch, both sets), and only once the park landed
/// are the parked entries taken out of the isolate's sets; the counters come after. So a cancellation at any
/// await leaves every note in memory, in D1, or in both (a note in both is delivered twice: harmless, the app
/// layer re-reads). A park that faults takes nothing out of memory, counted and logged.
pub(crate) async fn park_withheld<P: ParkPort>(port: &P, remaining_ms: u64, now_ms: u64) {
    let pots = crate::pot_changes::snapshot();
    let hops = crate::hop_changes::snapshot();
    let mut rows: Vec<(&'static str, String)> = pots
        .iter()
        .map(|(t, v)| (KIND_POT, pot_item(t, *v)))
        .collect();
    rows.extend(hops.iter().map(|h| (KIND_HOP, h.clone())));
    let withheld = rows.len() as u64;
    let (mut parked, mut faulted) = (0u64, 0u64);
    if withheld > 0 {
        match port.park(&rows, now_ms).await {
            Ok(()) => {
                crate::pot_changes::forget(&pots);
                crate::hop_changes::forget(&hops);
                parked = withheld;
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
    port.log(format!(
        "[pot-changes] background flush SKIPPED: {remaining_ms} ms left of the wait_until window (floor {} ms); {withheld} entr(ies) withheld, {parked} parked in D1 for the cron, {faulted} kept in memory",
        crate::pot_changes::FLUSH_WINDOW_FLOOR_MS
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
    ) -> std::result::Result<(), String> {
        let mut stmts = Vec::with_capacity(rows.len());
        for (kind, item) in rows {
            let stmt = self
                .db
                .prepare(PARK_SQL)
                .bind(&[(*kind).into(), item.as_str().into(), (now_ms as f64).into()])
                .map_err(|e| e.to_string())?;
            stmts.push(stmt);
        }
        crate::d1_ledger::counted_batch(self.db, stmts)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
    async fn bump(&self, counter: &'static str, delta: u64) {
        crate::ops::bump_counter(self.db, counter, delta).await;
    }
    fn log(&self, line: String) {
        console_log!("{line}");
    }
}

/// The cron's delivery of the parked set (bsv-low #649 M1): a port so the pins run the shipped
/// [`deliver_parked`] over a fake transport.
pub(crate) trait UnparkPort {
    /// The oldest rows of one set: `(entry, generation)`.
    async fn read(
        &self,
        kind: &'static str,
        limit: usize,
    ) -> std::result::Result<Vec<(String, i64)>, String>;
    async fn post(&self, kind: &'static str, body: String) -> std::result::Result<usize, ()>;
    /// Delete the delivered rows, each only at the generation it was read at.
    async fn unpark(
        &self,
        kind: &'static str,
        rows: &[(String, i64)],
    ) -> std::result::Result<(), String>;
    fn now_ms(&self) -> u64;
    async fn bump(&self, counter: &'static str, delta: u64);
    fn log(&self, line: String);
}

/// What one delivery did, by entries.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Delivered {
    pub read: u64,
    pub unparked: u64,
    pub kept: u64,
}

/// THE DELIVERY of the parked set (bsv-low #649 M1): each set's oldest rows through the one flush loop
/// (`change_flush::ship_chunks`, the set's own body), the rows of ACCEPTED POSTs deleted, the rest kept parked.
pub(crate) async fn deliver_parked<P: UnparkPort>(port: &P) -> Delivered {
    let mut out = Delivered::default();
    let deadline_ms = port.now_ms() + DRAIN_WALL_MS;
    for kind in [KIND_HOP, KIND_POT] {
        let read = match port.read(kind, DRAIN_MAX_ROWS).await {
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
        let items: Vec<String> = read.iter().map(|(i, _)| i.clone()).collect();
        let generation: std::collections::BTreeMap<&str, i64> =
            read.iter().map(|(i, g)| (i.as_str(), *g)).collect();
        out.read += items.len() as u64;
        let delivered: Vec<String> = if kind == KIND_POT {
            let pots: Vec<(String, u32)> = items.iter().filter_map(|i| parse_pot_item(i)).collect();
            if pots.len() != items.len() {
                port.log(format!(
                    "[change-park] {} parked pot row(s) do not read; left parked",
                    items.len() - pots.len()
                ));
            }
            let shipped = crate::change_flush::ship_chunks(
                pots,
                crate::pot_changes::body_json,
                |body| port.post(kind, body),
                || port.now_ms(),
                deadline_ms,
            )
            .await;
            shipped
                .delivered
                .iter()
                .map(|(t, v)| pot_item(t, *v))
                .collect()
        } else {
            let shipped = crate::change_flush::ship_chunks(
                items.clone(),
                crate::hop_changes::body_json,
                |body| port.post(kind, body),
                || port.now_ms(),
                deadline_ms,
            )
            .await;
            shipped.delivered
        };
        if delivered.is_empty() {
            out.kept += items.len() as u64;
            continue;
        }
        let delivered: Vec<(String, i64)> = delivered
            .into_iter()
            .map(|i| {
                let g = generation.get(i.as_str()).copied().unwrap_or_default();
                (i, g)
            })
            .collect();
        match port.unpark(kind, &delivered).await {
            Ok(()) => {
                out.unparked += delivered.len() as u64;
                out.kept += (items.len() - delivered.len()) as u64;
            }
            Err(e) => {
                port.log(format!("[change-park] delete of {} delivered {kind} row(s) faulted ({e}); they are sent again", delivered.len()));
                out.kept += items.len() as u64;
            }
        }
    }
    if out.unparked > 0 {
        port.bump(COUNTER_CHANGE_UNPARKED, out.unparked).await;
    }
    if out.read > 0 {
        port.log(format!(
            "[change-park] delivered {} parked entr(ies), {} kept parked for the next tick",
            out.unparked, out.kept
        ));
    }
    out
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
        limit: usize,
    ) -> std::result::Result<Vec<(String, i64)>, String> {
        #[derive(serde::Deserialize)]
        struct Row {
            item: String,
            generation: f64,
        }
        let rows = crate::d1::Query::new(PARKED_READ_SQL)
            .bind(kind)
            .bind(limit as i64)
            .fetch_all::<Row>(self.db)
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.item, r.generation as i64))
            .collect())
    }
    async fn post(&self, kind: &'static str, body: String) -> std::result::Result<usize, ()> {
        let (path, tag) = if kind == KIND_POT {
            ("/internal/pot-changed", "change-park")
        } else {
            ("/internal/hop-changed", "change-park")
        };
        crate::change_flush::post_body(self.env, &self.url, &self.token, path, tag, body).await
    }
    async fn unpark(
        &self,
        kind: &'static str,
        rows: &[(String, i64)],
    ) -> std::result::Result<(), String> {
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
        crate::d1_ledger::counted_batch(self.db, stmts)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
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

/// `/health/invariants.changeParked`: the parked rows by set, read from D1 (`null` when the read faults).
pub async fn health_json(db: &D1Database) -> serde_json::Value {
    #[derive(serde::Deserialize)]
    struct Row {
        kind: String,
        n: f64,
    }
    match crate::d1::Query::new(PARKED_COUNT_SQL)
        .fetch_all::<Row>(db)
        .await
    {
        Ok(rows) => {
            let mut pot = 0u64;
            let mut hop = 0u64;
            for r in rows {
                match r.kind.as_str() {
                    KIND_POT => pot = r.n as u64,
                    KIND_HOP => hop = r.n as u64,
                    _ => {}
                }
            }
            serde_json::json!({ "pot": pot, "hop": hop, "drainMaxRows": DRAIN_MAX_ROWS, "drainWallMs": DRAIN_WALL_MS })
        }
        Err(_) => serde_json::Value::Null,
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
        ) -> std::result::Result<(), String> {
            if self.fail {
                return Err("D1 down".into());
            }
            for (k, i) in rows {
                self.rows.borrow_mut().push((k.to_string(), i.clone()));
            }
            Ok(())
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
        ) -> std::result::Result<(), String> {
            if self.hang == "park" {
                std::future::pending::<()>().await;
            }
            for (k, i) in rows {
                self.rows.borrow_mut().push((k.to_string(), i.clone()));
            }
            Ok(())
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
        let ok = body
            .find("Ok(())=>{crate::pot_changes::forget(&pots);crate::hop_changes::forget(&hops);")
            .expect("forgotten only once parked");
        let first_bump = body.find("port.bump(").unwrap();
        assert!(
            park < ok && park < first_bump,
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
            limit: usize,
        ) -> std::result::Result<Vec<(String, i64)>, String> {
            Ok(self
                .rows
                .borrow()
                .iter()
                .filter(|(k, _)| k == kind)
                .take(limit)
                .map(|(_, i)| (i.clone(), 0))
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
        ) -> std::result::Result<(), String> {
            self.rows
                .borrow_mut()
                .retain(|(k, i)| !(k == kind && rows.iter().any(|(r, _)| r == i)));
            Ok(())
        }
        fn now_ms(&self) -> u64 {
            0
        }
        async fn bump(&self, counter: &'static str, delta: u64) {
            assert_eq!(counter, COUNTER_CHANGE_UNPARKED);
            *self.unparked.borrow_mut() += delta;
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
                kept: 2
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
                kept: 0
            }
        );
        assert!(
            again.rows.borrow().is_empty(),
            "the next tick delivers the rest"
        );
        assert_eq!(*port.unparked.borrow() + *again.unparked.borrow(), 11);
    }

    /// bsv-low #649 M1: the SHIPPED statements under real SQLite: a re-park is one row with its first stamp, the read
    /// is oldest first and bounded, the delete takes the one row; the table is in the migrations; the entry keys
    /// read back.
    #[test]
    fn e649_m1_the_shipped_statements_under_sqlite() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        assert!(crate::d1::OVERLAY_MIGRATIONS.contains(&CHANGE_PARKED_CREATE));
        conn.execute_batch(CHANGE_PARKED_CREATE).unwrap();
        conn.execute_batch(CHANGE_PARKED_ADD_GENERATION).unwrap();
        for (item, at) in [("b:1", 20), ("a:0", 10), ("b:1", 30), ("c:2", 15)] {
            conn.execute(PARK_SQL, rusqlite::params![KIND_POT, item, at])
                .unwrap();
        }
        conn.execute(PARK_SQL, rusqlite::params![KIND_HOP, ID, 5])
            .unwrap();
        let read = |limit: i64| -> Vec<String> {
            let mut q = conn.prepare(PARKED_READ_SQL).unwrap();
            q.query_map(rusqlite::params![KIND_POT, limit], |r| r.get(0))
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
        conn.execute(UNPARK_SQL, rusqlite::params![KIND_POT, "c:2", 0])
            .unwrap();
        assert_eq!(read(10), vec!["a:0", "b:1"]);
        let mut q = conn.prepare(PARKED_COUNT_SQL).unwrap();
        let counts: Vec<(String, i64)> = q
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert!(counts.contains(&("pot".into(), 2)) && counts.contains(&("hop".into(), 1)));
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
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        for m in [CHANGE_PARKED_CREATE, CHANGE_PARKED_ADD_GENERATION] {
            assert!(crate::d1::OVERLAY_MIGRATIONS.contains(&m));
            conn.execute_batch(m).unwrap();
        }
        let k = pot_item(&"ab".repeat(32), 0);
        conn.execute(PARK_SQL, rusqlite::params![KIND_POT, k, 100])
            .unwrap();
        let selected: Vec<(String, i64)> = {
            let mut q = conn.prepare(PARKED_READ_SQL).unwrap();
            q.query_map(rusqlite::params![KIND_POT, 128], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
        };
        assert_eq!(selected.len(), 1);
        conn.execute(PARK_SQL, rusqlite::params![KIND_POT, k, 200])
            .unwrap();
        for (item, generation) in &selected {
            conn.execute(UNPARK_SQL, rusqlite::params![KIND_POT, item, generation])
                .unwrap();
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
            .query_row(PARKED_READ_SQL, rusqlite::params![KIND_POT, 128], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        conn.execute(UNPARK_SQL, rusqlite::params![KIND_POT, again.0, again.1])
            .unwrap();
        let left: i64 = conn
            .query_row("SELECT COUNT(*) FROM change_parked", [], |r| r.get(0))
            .unwrap();
        assert_eq!(left, 0);
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
