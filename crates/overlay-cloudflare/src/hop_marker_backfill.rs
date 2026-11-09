//! The one-time backfill of the hop markers the index already holds (bsv-low #1146).
//!
//! `tm_lowfund` admits every P2PKH output of a hop body into `pot_records` and its bytes into `pot_beefs`, but a hop
//! reaches the hops view and the owed list only through its `hopparty_records` row. Since MARKER-LANDING the hopparty
//! lookup service writes that row from the hop's OWN body at the hop's admission under `tm_lowfund`; every hop
//! admitted before that landing, whose client marker submit was refused or never made, sits in the index with its
//! marker in its body and no row. A re-submit cannot repair one: an applied transaction is a dupe in
//! `Engine::submit` and the lookup services are not told again.
//!
//! THE RULE: a hop body the index holds carries its own marker; read it once, through the one builder
//! (`overlay_discovery::hopparty::lookup_service::records_for_body`, the `tm_lowfund` arm's) and the one store
//! (`HoppartyStorage::store_record`, `hopparty_insert_query`: `markerValid` by the one gate, `INSERT OR IGNORE` on the
//! marker's own outpoint). The derived links (`derive_hop_insert`) and the owed dirty mark (`owed_dirty_hopparty_records_insert`)
//! fire on the INSERT exactly as on the live path, and the insert's builder notes the `hop` pair for the owed events'
//! publish at the pass's flush.
//!
//! One pass ([`run_pass`]): one state read; when `done`, nothing else. Else one page of `pot_records` rows past the
//! cursor ([`PAGE_SQL`], at most [`HOP_MARKER_BACKFILL_ROWS`] rows, `lockKind` `p2pkh` or not yet decoded, in rowid
//! order), each row naming whether it is its txid's FIRST such row, whether a hopparty row already names the txid,
//! whether `pot_beefs` holds the body and the vouts the index holds for the txid. A txid's first row, unmarked, with a
//! body, is the work: at most [`HOP_MARKER_BACKFILL_BODIES`] bodies a pass, each parsed hash-bound by its own txid
//! under `STORED_BEEF_LIMITS`, its records kept where `hopVout` names an output the index holds (the stake; the change
//! names nothing). A txid with no body is counted `missingBeef` and passed (the index's bytes only, never a courier); a
//! body that does not parse `unparseable` and passed; a body naming no held output `noMarker` and passed. A read or
//! write FAULT stops the pass before that txid (its cursor kept), so the txid is retried by the next pass. The page
//! coming back short (and the pass not stopped) stamps `done`: from then on a pass is one primary-key read.

use async_trait::async_trait;
use overlay_discovery::hopparty::storage::HoppartyRecord;
use overlay_engine::beef_limits;

use crate::d1::Query;

/// Rows of `pot_records` a pass reads past its cursor (env `HOP_MARKER_BACKFILL_ROWS`).
pub const HOP_MARKER_BACKFILL_ROWS: u32 = 256;
/// Hop bodies a pass reads and parses (env `HOP_MARKER_BACKFILL_BODIES`).
pub const HOP_MARKER_BACKFILL_BODIES: u32 = 32;

/// The one state row.
pub const STATE_SQL: &str =
    "SELECT cursorRowid, done, scanned, bodies, stored, invalid, noMarker, missingBeef, \
     unparseable, faults FROM hop_marker_backfill_state WHERE id = 1";

/// The pass's page: `pot_records` rows past the cursor (`?1`), rowid ascending, at most `?2`, each with its txid's
/// facts read by key (the txid's first such row by the primary key's index, a hopparty row by its primary key, the
/// body by `pot_beefs`' primary key, the vouts the index holds by the primary key's index). Reads in proportion to the
/// page, never the table.
pub const PAGE_SQL: &str = "SELECT r.rowid AS rid, r.txid AS txid, \
     (r.rowid = (SELECT MIN(p.rowid) FROM pot_records p WHERE p.txid = r.txid \
                 AND (p.lockKind = 'p2pkh' OR p.lockKind IS NULL))) AS first, \
     EXISTS (SELECT 1 FROM hopparty_records h WHERE h.txid = r.txid) AS marked, \
     EXISTS (SELECT 1 FROM pot_beefs b WHERE b.txid = r.txid) AS held, \
     (SELECT group_concat(p.outputIndex) FROM pot_records p WHERE p.txid = r.txid) AS vouts \
     FROM pot_records r \
     WHERE r.rowid > CAST(?1 AS INTEGER) AND (r.lockKind = 'p2pkh' OR r.lockKind IS NULL) \
     ORDER BY r.rowid LIMIT CAST(?2 AS INTEGER)";

/// The pass's one state write: the cursor and `done` never move back (two passes at once keep the furthest), the
/// counts are added.
pub const SAVE_SQL: &str = "INSERT INTO hop_marker_backfill_state \
     (id, cursorRowid, done, scanned, bodies, stored, invalid, noMarker, missingBeef, unparseable, faults) \
     VALUES (1, CAST(?1 AS INTEGER), CAST(?2 AS INTEGER), CAST(?3 AS INTEGER), CAST(?4 AS INTEGER), \
             CAST(?5 AS INTEGER), CAST(?6 AS INTEGER), CAST(?7 AS INTEGER), CAST(?8 AS INTEGER), \
             CAST(?9 AS INTEGER), CAST(?10 AS INTEGER)) \
     ON CONFLICT(id) DO UPDATE SET \
       cursorRowid = MAX(cursorRowid, excluded.cursorRowid), done = MAX(done, excluded.done), \
       scanned = scanned + excluded.scanned, bodies = bodies + excluded.bodies, \
       stored = stored + excluded.stored, invalid = invalid + excluded.invalid, \
       noMarker = noMarker + excluded.noMarker, missingBeef = missingBeef + excluded.missingBeef, \
       unparseable = unparseable + excluded.unparseable, faults = faults + excluded.faults";

/// The counts of a pass, or the durable totals of every pass.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Counts {
    /// `pot_records` rows read.
    pub scanned: u64,
    /// Hop bodies read and parsed.
    pub bodies: u64,
    /// Marker records stored through the hopparty store.
    pub stored: u64,
    /// Of `stored`, the records the one gate latched `markerValid = 0` (stored all the same: a hint, never a gate).
    pub invalid: u64,
    /// Bodies naming no marker for an output the index holds.
    pub no_marker: u64,
    /// Txids whose body `pot_beefs` does not hold.
    pub missing_beef: u64,
    /// Bodies `STORED_BEEF_LIMITS` refused.
    pub unparseable: u64,
    /// Read or write faults (the pass stopped there; the txid is retried).
    pub faults: u64,
}

/// The state row: the cursor, the done flag and the durable totals.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct State {
    /// The last `pot_records` rowid a pass finished.
    pub cursor: i64,
    /// The page came back short: the backfill is over.
    pub done: bool,
    /// Every pass's counts, added.
    pub totals: Counts,
}

/// One row of the pass's page ([`PAGE_SQL`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageRow {
    /// The `pot_records` rowid.
    pub rowid: i64,
    /// The hop txid.
    pub txid: String,
    /// The txid's first `p2pkh` (or undecoded) row: the row the txid is judged at.
    pub first: bool,
    /// A hopparty row already names the txid.
    pub marked: bool,
    /// `pot_beefs` holds the txid's body.
    pub held: bool,
    /// The output indexes the index holds for the txid.
    pub vouts: Vec<u32>,
}

/// One pass's answer.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PassReport {
    /// The backfill was already done: the pass read the state row and nothing else.
    pub already_done: bool,
    /// This pass stamped `done`.
    pub done: bool,
    /// The cursor after the pass.
    pub cursor: i64,
    /// This pass's counts.
    pub counts: Counts,
}

/// The pass's reads and writes: the D1 one in production ([`D1Port`]), the shipped statements under real SQLite in
/// the pins.
#[async_trait(?Send)]
pub trait Port {
    /// [`STATE_SQL`]: `None` before any pass.
    async fn state(&self) -> Result<Option<State>, String>;
    /// [`PAGE_SQL`] past `after`, at most `rows`.
    async fn page(&self, after: i64, rows: u32) -> Result<Vec<PageRow>, String>;
    /// The stored body of `txid` (`pot_beefs`), `None` when absent.
    async fn beef(&self, txid: &str) -> Result<Option<Vec<u8>>, String>;
    /// Store `record` through the hopparty store (`hopparty_insert_query`).
    async fn store(&self, record: &HoppartyRecord) -> Result<(), String>;
    /// [`SAVE_SQL`].
    async fn save(&self, cursor: i64, done: bool, counts: &Counts) -> Result<(), String>;
}

/// The records of `beef` (the stored body of `txid`) whose `hopVout` names an output the index holds (`vouts`): the
/// one builder, `records_for_body`, the `tm_lowfund` arm's. `Err` when the body does not parse hash-bound.
pub fn records_of_body(
    beef: &[u8],
    txid: &str,
    vouts: &[u32],
) -> Result<Vec<HoppartyRecord>, String> {
    let tx = beef_limits::transaction_from_beef(beef, Some(txid), &beef_limits::STORED_BEEF_LIMITS)
        .map_err(|e| e.to_string())?;
    Ok(
        overlay_discovery::hopparty::lookup_service::records_for_body(&tx, beef)
            .into_iter()
            .filter(|r| r.output_index != r.hop_vout && vouts.contains(&r.hop_vout))
            .collect(),
    )
}

/// One bounded pass (module doc).
pub async fn run_pass(port: &dyn Port, rows: u32, bodies: u32) -> PassReport {
    let mut report = PassReport::default();
    let state = match port.state().await {
        Ok(s) => s.unwrap_or_default(),
        Err(e) => {
            crate::proof_fetcher::push_log(&format!(
                "[hop-marker-backfill] state read failed: {e}"
            ));
            report.counts.faults += 1;
            return report;
        }
    };
    report.cursor = state.cursor;
    if state.done {
        report.already_done = true;
        report.done = true;
        return report;
    }
    let page = match port.page(state.cursor, rows).await {
        Ok(p) => p,
        Err(e) => {
            crate::proof_fetcher::push_log(&format!("[hop-marker-backfill] page read failed: {e}"));
            report.counts.faults += 1;
            let _ = port.save(state.cursor, false, &report.counts).await;
            return report;
        }
    };
    let mut cursor = state.cursor;
    let mut stopped = false;
    let c = &mut report.counts;
    'rows: for row in &page {
        if row.first && !row.marked {
            if !row.held {
                c.missing_beef += 1;
            } else {
                if c.bodies >= u64::from(bodies) {
                    stopped = true;
                    break;
                }
                let beef = match port.beef(&row.txid).await {
                    Ok(Some(b)) => b,
                    Ok(None) => {
                        c.missing_beef += 1;
                        cursor = row.rowid;
                        c.scanned += 1;
                        continue;
                    }
                    Err(e) => {
                        crate::proof_fetcher::push_log(&format!(
                            "[hop-marker-backfill] {} beef read failed: {e}",
                            row.txid
                        ));
                        c.faults += 1;
                        stopped = true;
                        break;
                    }
                };
                c.bodies += 1;
                match records_of_body(&beef, &row.txid, &row.vouts) {
                    Err(e) => {
                        crate::proof_fetcher::push_log(&format!(
                            "[hop-marker-backfill] {} stored beef unparseable, passed: {e}",
                            row.txid
                        ));
                        c.unparseable += 1;
                    }
                    Ok(records) if records.is_empty() => c.no_marker += 1,
                    Ok(records) => {
                        for record in &records {
                            if let Err(e) = port.store(record).await {
                                crate::proof_fetcher::push_log(&format!(
                                    "[hop-marker-backfill] {}:{} store failed: {e}",
                                    record.txid, record.output_index
                                ));
                                c.faults += 1;
                                stopped = true;
                                break 'rows;
                            }
                            c.stored += 1;
                            if !overlay_discovery::hopparty::validity::record_marker_valid(record) {
                                c.invalid += 1;
                            }
                        }
                    }
                }
            }
        }
        cursor = row.rowid;
        c.scanned += 1;
    }
    report.done = !stopped && (page.len() as u64) < u64::from(rows);
    report.cursor = cursor;
    if let Err(e) = port.save(cursor, report.done, &report.counts).await {
        crate::proof_fetcher::push_log(&format!("[hop-marker-backfill] state write failed: {e}"));
        report.counts.faults += 1;
        report.done = false;
    }
    report
}

/// The pass's bounds from the env, else the defaults (a decimal integer at least 1).
pub fn bounds_from_env(env: &worker::Env) -> (u32, u32) {
    let read = |name: &str, default: u32| {
        env.var(name)
            .ok()
            .and_then(|v| v.to_string().trim().parse::<u32>().ok())
            .filter(|n| *n >= 1)
            .unwrap_or(default)
    };
    (
        read("HOP_MARKER_BACKFILL_ROWS", HOP_MARKER_BACKFILL_ROWS),
        read("HOP_MARKER_BACKFILL_BODIES", HOP_MARKER_BACKFILL_BODIES),
    )
}

#[derive(serde::Deserialize)]
struct StateRow {
    #[serde(rename = "cursorRowid")]
    cursor: f64,
    done: f64,
    scanned: f64,
    bodies: f64,
    stored: f64,
    invalid: f64,
    #[serde(rename = "noMarker")]
    no_marker: f64,
    #[serde(rename = "missingBeef")]
    missing_beef: f64,
    unparseable: f64,
    faults: f64,
}

impl StateRow {
    fn into_state(self) -> State {
        State {
            cursor: self.cursor as i64,
            done: self.done != 0.0,
            totals: Counts {
                scanned: self.scanned as u64,
                bodies: self.bodies as u64,
                stored: self.stored as u64,
                invalid: self.invalid as u64,
                no_marker: self.no_marker as u64,
                missing_beef: self.missing_beef as u64,
                unparseable: self.unparseable as u64,
                faults: self.faults as u64,
            },
        }
    }
}

#[derive(serde::Deserialize)]
struct PageDbRow {
    rid: f64,
    txid: String,
    first: Option<f64>,
    marked: f64,
    held: f64,
    vouts: Option<String>,
}

/// The vouts `group_concat` answered.
pub fn parse_vouts(vouts: Option<&str>) -> Vec<u32> {
    vouts
        .unwrap_or("")
        .split(',')
        .filter_map(|v| v.trim().parse::<u32>().ok())
        .collect()
}

/// [`SAVE_SQL`] with its binds.
pub fn save_query(cursor: i64, done: bool, c: &Counts) -> Query {
    Query::new(SAVE_SQL)
        .bind(cursor)
        .bind(i64::from(done))
        .bind(c.scanned as i64)
        .bind(c.bodies as i64)
        .bind(c.stored as i64)
        .bind(c.invalid as i64)
        .bind(c.no_marker as i64)
        .bind(c.missing_beef as i64)
        .bind(c.unparseable as i64)
        .bind(c.faults as i64)
}

/// The D1 port: the statements above, the body through the pot store's `get_beef`, the record through
/// `D1HoppartyStorage::store_record`.
pub struct D1Port<'a> {
    /// The database.
    pub db: &'a worker::D1Database,
    /// The pot store (its `get_beef`).
    pub pots: &'a dyn overlay_discovery::pot::storage::PotStorage,
    /// The hopparty store (its `store_record`).
    pub hops: &'a dyn overlay_discovery::hopparty::storage::HoppartyStorage,
}

#[async_trait(?Send)]
impl Port for D1Port<'_> {
    async fn state(&self) -> Result<Option<State>, String> {
        Ok(Query::new(STATE_SQL)
            .fetch_optional::<StateRow>(self.db)
            .await?
            .map(StateRow::into_state))
    }
    async fn page(&self, after: i64, rows: u32) -> Result<Vec<PageRow>, String> {
        let rows: Vec<PageDbRow> = Query::new(PAGE_SQL)
            .bind(after)
            .bind(rows)
            .fetch_all(self.db)
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| PageRow {
                rowid: r.rid as i64,
                txid: r.txid,
                first: r.first.unwrap_or(0.0) != 0.0,
                marked: r.marked != 0.0,
                held: r.held != 0.0,
                vouts: parse_vouts(r.vouts.as_deref()),
            })
            .collect())
    }
    async fn beef(&self, txid: &str) -> Result<Option<Vec<u8>>, String> {
        self.pots.get_beef(txid).await.map_err(|e| e.to_string())
    }
    async fn store(&self, record: &HoppartyRecord) -> Result<(), String> {
        self.hops
            .store_record(record)
            .await
            .map_err(|e| e.to_string())
    }
    async fn save(&self, cursor: i64, done: bool, counts: &Counts) -> Result<(), String> {
        save_query(cursor, done, counts).execute(self.db).await
    }
}

/// One pass over D1, logged (the scheduled tick and `/admin/complete-proofs` call it).
pub async fn run_d1_pass(env: &worker::Env, db: std::rc::Rc<worker::D1Database>) -> PassReport {
    let pots = crate::d1_discovery::D1PotStorage::new(db.clone());
    let hops = crate::d1_discovery::D1HoppartyStorage::new(db.clone());
    let (rows, bodies) = bounds_from_env(env);
    let report = run_pass(
        &D1Port {
            db: &db,
            pots: &pots,
            hops: &hops,
        },
        rows,
        bodies,
    )
    .await;
    log_report(&report);
    report
}

/// The pass's log line.
pub fn log_report(r: &PassReport) {
    let c = &r.counts;
    crate::proof_fetcher::push_log(&format!(
        "[hop-marker-backfill] cursor={} done={} already_done={} scanned={} bodies={} stored={} invalid={} \
         no_marker={} missing_beef={} unparseable={} faults={}",
        r.cursor,
        r.done,
        r.already_done,
        c.scanned,
        c.bodies,
        c.stored,
        c.invalid,
        c.no_marker,
        c.missing_beef,
        c.unparseable,
        c.faults
    ));
}

/// The pass's JSON (the admin route's answer).
pub fn report_json(r: &PassReport) -> serde_json::Value {
    let c = &r.counts;
    serde_json::json!({
        "cursor": r.cursor,
        "done": r.done,
        "alreadyDone": r.already_done,
        "scanned": c.scanned,
        "bodies": c.bodies,
        "stored": c.stored,
        "invalid": c.invalid,
        "noMarker": c.no_marker,
        "missingBeef": c.missing_beef,
        "unparseable": c.unparseable,
        "faults": c.faults,
    })
}

/// `/health/invariants.hopMarkerBackfill`: the state row (`null` before the first pass; `readable: false` when it
/// does not read).
pub async fn health_json(db: &worker::D1Database) -> serde_json::Value {
    match Query::new(STATE_SQL).fetch_optional::<StateRow>(db).await {
        Ok(None) => serde_json::Value::Null,
        Ok(Some(r)) => state_json(&r.into_state()),
        Err(e) => serde_json::json!({ "readable": false, "fault": e }),
    }
}

/// The served shape of the state row.
pub fn state_json(s: &State) -> serde_json::Value {
    let c = &s.totals;
    serde_json::json!({
        "readable": true,
        "cursor": s.cursor,
        "done": s.done,
        "scanned": c.scanned,
        "bodies": c.bodies,
        "stored": c.stored,
        "invalid": c.invalid,
        "noMarker": c.no_marker,
        "missingBeef": c.missing_beef,
        "unparseable": c.unparseable,
        "faults": c.faults,
    })
}

#[cfg(test)]
mod tests;
