//! admit-fast (bsv-low, the owner's admission model, 2026-09-15;
//! `docs/DESIGN-ADMIT-FAST-2026-09-15.md`): the Arcade callback is the
//! WITNESS path and a refusal EVICTS everywhere, reversibly.
//!
//! Step 1 (this module + the `/arc-ingest` arms):
//!   * `SEEN_ON_NETWORK` and above LATCH `network_seen` (today the status-only
//!     callback only acknowledged; the witness came from the route's own poll).
//!   * `DOUBLE_SPEND_ATTEMPTED` / `REJECTED` / `SEEN_IN_ORPHAN_MEMPOOL` run the
//!     shared EVIDENCE CHECK after the 200: Arcade's LIVE word first (the
//!     bearer of `/arc-ingest` is the public subject txid, so a stranger can
//!     plant any status), then BOTH indexers — a refusal is corroborated only
//!     when Arcade's live word is fatal-or-missing AND both indexers say
//!     absent (the #212/#213/#214 bar: a stale Arcade REJECTED of a tx an
//!     indexer holds, the 2026-07-20/21 class, evicts nothing).
//!   * A pushed MINED proof for an evicted txid READMITS it before the stitch:
//!     the chain overrules a courier.
//!
//! EVICTION is a SHADOW MOVE, not a flag: every row keyed by the txid (the pot
//! row, the engine's outputs / applied_transactions / transactions rows, the
//! lobby advert, and the six marker families keyed by `potTxid`) moves into a
//! `<table>_evicted` twin (the source's columns, read from `PRAGMA table_info`
//! at run time so a later `ALTER TABLE … ADD COLUMN` on the source heals the
//! twin, plus `af_evictedAt`/`af_reason`), so EVERY reader of the source table
//! (55 sites across the overlay and the app layer) is correct by construction
//! — a vanished pot leaves every view, a refund backup for it is never served
//! as valid — and READMISSION is the same move back, byte for byte. `pot_beefs`
//! is never moved (its rows are never deleted by rule: the bytes for the
//! readmit). A `pot_evictions` ledger row records every eviction and its
//! readmission for `/health` and the operator.
//!
//! Arcade names each subject on its own callback (a refused hop and its refused
//! JOIN each get their own push), so no dependent cascade is computed here.
//!
//! Step 3 (`pending_watch_job`, behind `ADMIT_FAST`): `/submit(broadcast-gated)`
//! ANSWERS on Arcade's synchronous accept (the door's script walk, then
//! Arcade's validation) and admits PENDING; the SEEN witness that used to be
//! polled on the wire is this module's PENDING WATCH in `ctx.wait_until` — a
//! live look at 2..18 s: SEEN latches `network_seen` (counted, with its
//! latency); a FATAL look runs the same evidence check and a corroborated
//! refusal EVICTS; silence is counted and left to the callbacks and the passes.
use crate::d1::{QVal, Query};
use async_trait::async_trait;
use worker::D1Database;

/// What a status-only callback means for the index (PURE; pinned).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallbackAction {
    /// The push says the network holds it: VERIFY live, then latch the witness.
    /// (The `/arc-ingest` bearer is the public subject txid — a stranger can push
    /// any status for any txid — so a push only ever TRIGGERS a live read.)
    LatchSeen,
    /// The push says refused (REJECTED, DOUBLE_SPEND_ATTEMPTED, an orphan view):
    /// Arcade's LIVE word first, then both indexers; evict only on corroboration.
    EvidenceCheck(String),
    /// A lifecycle status below SEEN (RECEIVED, STORED, ANNOUNCED …): nothing to do.
    Ignore,
}

const SEEN_OR_BETTER: &[&str] = &[
    "SEEN_ON_NETWORK",
    "SEEN_MULTIPLE_NODES",
    "MINED",
    "IMMUTABLE",
];

pub fn callback_action(tx_status: &str) -> CallbackAction {
    let s = tx_status.trim().to_ascii_uppercase();
    if SEEN_OR_BETTER.contains(&s.as_str()) {
        return CallbackAction::LatchSeen;
    }
    match s.as_str() {
        "DOUBLE_SPEND_ATTEMPTED" | "REJECTED" | "SEEN_IN_ORPHAN_MEMPOOL" => {
            CallbackAction::EvidenceCheck(s)
        }
        _ => CallbackAction::Ignore,
    }
}

/// The tables a txid can own rows in, and the columns that key them.
/// `txid` = the row IS this transaction (or its output); `potTxid` = the row is
/// a marker ABOUT this pot. A table or column that does not exist is skipped
/// (the column list is read at run time).
pub const MOVED_TABLES: &[(&str, &[&str])] = &[
    ("pot_records", &["txid"]),
    ("outputs", &["txid"]),
    ("applied_transactions", &["txid"]),
    ("transactions", &["txid"]),
    ("low_records", &["txid"]),
    ("potparty_records", &["txid", "potTxid"]),
    ("potrefund_records", &["txid", "potTxid"]),
    ("result_markers", &["txid", "potTxid"]),
    ("result_markers_v2", &["txid", "potTxid"]),
    ("hand_markers", &["txid", "potTxid"]),
    ("lb_marker_rows", &["txid", "potTxid"]),
];

/// A SQL identifier this module will interpolate (PRAGMA cannot bind names).
pub fn ident_ok(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.bytes()
            .next()
            .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
        && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

pub fn shadow_table(table: &str) -> String {
    format!("{table}_evicted")
}

/// One `PRAGMA table_info` row.
#[derive(Debug, Clone, serde::Deserialize, PartialEq, Eq)]
pub struct ColumnInfo {
    pub name: String,
    #[serde(rename = "type", default)]
    pub ty: String,
}

fn quoted(cols: &[ColumnInfo]) -> String {
    cols.iter()
        .map(|c| format!("\"{}\"", c.name))
        .collect::<Vec<_>>()
        .join(", ")
}

/// PURE: the twin's CREATE (the source's columns with their declared types,
/// no constraints — a twin never enforces; the source does on the way back).
pub fn create_shadow_sql(table: &str, cols: &[ColumnInfo]) -> String {
    let body = cols
        .iter()
        .map(|c| {
            format!(
                "\"{}\" {}",
                c.name,
                if c.ty.is_empty() {
                    "TEXT"
                } else {
                    c.ty.as_str()
                }
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "CREATE TABLE IF NOT EXISTS \"{}\" (af_evictedAt INTEGER NOT NULL, af_reason TEXT NOT NULL, {body})",
        shadow_table(table)
    )
}

/// PURE: a column the source gained after the twin was created.
pub fn heal_shadow_sql(table: &str, col: &ColumnInfo) -> String {
    format!(
        "ALTER TABLE \"{}\" ADD COLUMN \"{}\" {}",
        shadow_table(table),
        col.name,
        if col.ty.is_empty() {
            "TEXT"
        } else {
            col.ty.as_str()
        }
    )
}

/// PURE: copy the keyed rows into the twin (binds: evictedAt, reason, key), then delete them (bind: key).
pub fn move_sql(table: &str, key: &str, cols: &[ColumnInfo]) -> (String, String) {
    let q = quoted(cols);
    (
        format!(
            "INSERT INTO \"{}\" (af_evictedAt, af_reason, {q}) SELECT ?, ?, {q} FROM \"{table}\" WHERE \"{key}\" = ?",
            shadow_table(table)
        ),
        format!("DELETE FROM \"{table}\" WHERE \"{key}\" = ?"),
    )
}

/// PURE: copy the keyed rows back (bind: key), then drop them from the twin (bind: key).
pub fn restore_sql(table: &str, key: &str, cols: &[ColumnInfo]) -> (String, String) {
    let q = quoted(cols);
    (
        format!(
            "INSERT OR IGNORE INTO \"{table}\" ({q}) SELECT {q} FROM \"{}\" WHERE \"{key}\" = ?",
            shadow_table(table)
        ),
        format!(
            "DELETE FROM \"{}\" WHERE \"{key}\" = ?",
            shadow_table(table)
        ),
    )
}

/// The columns a table has NOW (empty when the table does not exist).
pub async fn table_columns(db: &D1Database, table: &str) -> Vec<ColumnInfo> {
    if !ident_ok(table) {
        return Vec::new();
    }
    Query::new(format!("PRAGMA table_info(\"{table}\")"))
        .fetch_all::<ColumnInfo>(db)
        .await
        .unwrap_or_default()
}

async fn count_keyed(db: &D1Database, table: &str, key: &str, txid: &str) -> u64 {
    #[derive(serde::Deserialize)]
    struct C {
        c: i64,
    }
    Query::new(format!(
        "SELECT COUNT(*) AS c FROM \"{table}\" WHERE \"{key}\" = ?"
    ))
    .bind(txid)
    .fetch_optional::<C>(db)
    .await
    .ok()
    .flatten()
    .map(|r| r.c.max(0) as u64)
    .unwrap_or(0)
}

/// Make sure the twin exists and carries every column the source has now (over the shadow storage: D1 in
/// production, SQLite under the pins).
async fn ensure_shadow(db: &dyn ShadowDb, table: &str, cols: &[ColumnInfo]) -> Result<(), String> {
    let twin = shadow_table(table);
    let have = db.columns(&twin).await?;
    if have.is_empty() {
        return db.exec(&create_shadow_sql(table, cols), vec![]).await;
    }
    for c in cols {
        if !have.iter().any(|h| h.name == c.name) {
            db.exec(&heal_shadow_sql(table, c), vec![]).await?;
        }
    }
    Ok(())
}

#[derive(serde::Deserialize)]
struct VoutRow {
    #[serde(rename = "outputIndex")]
    output_index: i64,
}

async fn vouts_of(db: &D1Database, table: &str, txid: &str) -> Vec<u32> {
    Query::new(format!(
        "SELECT \"outputIndex\" FROM \"{table}\" WHERE \"txid\" = ?"
    ))
    .bind(txid)
    .fetch_all::<VoutRow>(db)
    .await
    .unwrap_or_default()
    .into_iter()
    .map(|r| r.output_index.max(0) as u32)
    .collect()
}

// ── the spends an evicted tx left on OTHER rows (joinRefusedVoidsHand, 2026-09-15) ──
//
// A tx the index admitted also MARKED the outputs it consumed: `pot_records`
// rows carry `spent = 1, spendingTxid = <it>` (a JOIN on the two hops it
// spends — the hops are `tm_lowfund` rows of that table; a settle or a refund
// on its pot). Those rows are keyed by the INPUT's txid, so the shadow move
// above leaves them behind — and the cell found p1's hop still "spent by" the
// evicted JOIN on `/utxo-status`, so the seat's own scanner refused to sweep a
// hop the network held unspent. The eviction RELEASES every such pointer
// (recorded on the ledger row) and a readmission RE-MARKS them — only where
// nothing newer took the outpoint meanwhile (a sweep that won the race
// stands; the chain decides). Only UNCONFIRMED pointers move: a merkle-proven
// spend is positive evidence no courier's absence may demote (the storage's
// own never-clobber idiom); a confirmed row is counted and left. The engine's
// `outputs.consumedBy` is NOT touched: LOW's topic managers retain no coins,
// so a consumed hop's row is deep-deleted at the JOIN's admission and never
// carries it (the 2026-09-15 review's H1 — the first cut edited it as a
// string list; the engine stores outpoint objects).

/// One spend pointer the eviction released: the row and the txid that named it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReleasedSpend {
    pub table: String,
    pub txid: String,
    pub vout: u32,
}

/// PURE: the `pot_records` rows whose recorded spender is the txid (bind:
/// txid, lowercase — every writer stores lowercase hex; the plain equality
/// keeps `idx_pot_spending`), with their confirmation so a proven pointer can
/// be counted and left alone.
pub fn select_pot_spends_sql(cols: &[ColumnInfo]) -> String {
    let confirmed = if cols.iter().any(|c| c.name == "spentConfirmed") {
        "spentConfirmed"
    } else {
        "0 AS spentConfirmed"
    };
    format!("SELECT txid, outputIndex, {confirmed} FROM pot_records WHERE spendingTxid = ?")
}

/// PURE: release the spend pointer on every UNCONFIRMED `pot_records` row the
/// txid spent (bind: txid, lowercase) — the spend group and, when present, the
/// verdict group that rides it (`verdictTxid == spendingTxid` is the readers'
/// guard; a spender that left takes its verdict with it). Only columns the
/// table has; a `spentConfirmed = 1` row (a merkle-proven spend) never moves.
pub fn release_pot_spends_sql(cols: &[ColumnInfo]) -> String {
    let has = |n: &str| cols.iter().any(|c| c.name == n);
    let mut sets = vec!["spent = 0".to_string(), "spendingTxid = NULL".to_string()];
    for (col, val) in [
        ("spentAt", "NULL"),
        ("spentHeight", "NULL"),
        ("spenderFinal", "NULL"),
        ("verdict", "NULL"),
        ("verdictTxid", "NULL"),
        ("settleSigners", "NULL"),
    ] {
        if has(col) {
            sets.push(format!("{col} = {val}"));
        }
    }
    let guard = if has("spentConfirmed") {
        " AND spentConfirmed = 0"
    } else {
        ""
    };
    format!(
        "UPDATE pot_records SET {} WHERE spendingTxid = ?{guard}",
        sets.join(", ")
    )
}

/// PURE: re-mark one released `pot_records` spend on readmission (binds: the
/// spender txid, [spentAt seconds when the table has the column — the bool],
/// the row's txid, vout) — only if nothing newer spent the outpoint meanwhile.
/// The pointer comes back UNCONFIRMED with no verdict group even though a
/// MINED proof triggered the readmission: the chaser confirms it and
/// `mark_verdict_for_spender` writes the verdict at confirm time, as for any
/// unconfirmed pointer (stated, accepted).
pub fn remark_pot_spend_sql(cols: &[ColumnInfo]) -> (String, bool) {
    let with_at = cols.iter().any(|c| c.name == "spentAt");
    let spent_at = if with_at { ", spentAt = ?" } else { "" };
    (
        format!(
            "UPDATE pot_records SET spent = 1, spendingTxid = ?{spent_at} WHERE txid = ? AND outputIndex = ? AND spendingTxid IS NULL"
        ),
        with_at,
    )
}

#[derive(serde::Deserialize)]
struct PotSpendRow {
    txid: String,
    #[serde(rename = "outputIndex")]
    output_index: i64,
    #[serde(rename = "spentConfirmed", default)]
    spent_confirmed: i64,
}

/// Release every UNCONFIRMED spend pointer the txid left on other rows; the
/// list goes on the ledger row for the readmission. Never fails the eviction:
/// a faulted read releases nothing (logged), a faulted write leaves the rows
/// as they were; a confirmed pointer is counted and kept.
async fn release_spends_of(db: &D1Database, txid: &str) -> Vec<ReleasedSpend> {
    let mut released = Vec::new();
    let pot_cols = table_columns(db, "pot_records").await;
    if !pot_cols.iter().any(|c| c.name == "spendingTxid") {
        return released;
    }
    let rows = match Query::new(select_pot_spends_sql(&pot_cols))
        .bind(txid)
        .fetch_all::<PotSpendRow>(db)
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            worker::console_log!(
                "[admit-fast] evict {txid}: reading its pot_records spends failed: {e} — nothing released"
            );
            return released;
        }
    };
    let (kept, movable): (Vec<_>, Vec<_>) = rows.into_iter().partition(|r| r.spent_confirmed != 0);
    if !kept.is_empty() {
        worker::console_log!(
            "[admit-fast] evict {txid}: {} CONFIRMED spend pointer(s) kept (a proven spend is never released on a courier's word)",
            kept.len()
        );
    }
    if movable.is_empty() {
        return released;
    }
    match Query::new(release_pot_spends_sql(&pot_cols))
        .bind(txid)
        .execute(db)
        .await
    {
        Ok(()) => {
            for r in movable {
                crate::pot_changes::note(&r.txid, r.output_index.max(0) as u32);
                released.push(ReleasedSpend {
                    table: "pot_records".into(),
                    txid: r.txid.to_ascii_lowercase(),
                    vout: r.output_index.max(0) as u32,
                });
            }
        }
        Err(e) => worker::console_log!(
            "[admit-fast] evict {txid}: releasing its pot_records spends failed: {e}"
        ),
    }
    released
}

/// Re-mark the spends a readmitted tx had left (the ledger row's list): a
/// `pot_records` row only where nothing newer spent it. Best-effort.
async fn remark_spends(
    db: &D1Database,
    txid: &str,
    released: &[ReleasedSpend],
    now_ms: u64,
) -> u64 {
    let mut remarked = 0u64;
    let pot_cols = table_columns(db, "pot_records").await;
    let (pot_sql, with_at) = remark_pot_spend_sql(&pot_cols);
    for r in released {
        let ok = match r.table.as_str() {
            "pot_records" => {
                let mut q = Query::new(pot_sql.clone()).bind(txid);
                if with_at {
                    q = q.bind(QVal::Int((now_ms / 1000) as i64));
                }
                q.bind(r.txid.as_str())
                    .bind(QVal::Int(r.vout as i64))
                    .execute(db)
                    .await
                    .is_ok()
            }
            _ => false,
        };
        if ok {
            remarked += 1;
            if r.table == "pot_records" {
                crate::pot_changes::note(&r.txid, r.vout);
            }
        }
    }
    remarked
}

/// The columns a table has NOW, or the fault that hid them (bsv-low loop 18, 2026-09-21: `table_columns`'s
/// `unwrap_or_default` read a faulted PRAGMA as "no such table", and the eviction skipped the table in silence).
async fn table_columns_checked(db: &D1Database, table: &str) -> Result<Vec<ColumnInfo>, String> {
    if !ident_ok(table) {
        return Err(format!("{table}: not an identifier"));
    }
    Query::new(format!("PRAGMA table_info(\"{table}\")"))
        .fetch_all::<ColumnInfo>(db)
        .await
}

/// The keyed rows a table holds NOW, or the fault (never a silent zero: loop 18; a COUNT that answers no row at
/// all is a fault too — the gate's NIT).
async fn count_keyed_checked(db: &D1Database, table: &str, key: &str, txid: &str) -> Result<u64, String> {
    #[derive(serde::Deserialize)]
    struct C {
        c: i64,
    }
    match Query::new(format!(
        "SELECT COUNT(*) AS c FROM \"{table}\" WHERE \"{key}\" = ?"
    ))
    .bind(txid)
    .fetch_optional::<C>(db)
    .await?
    {
        Some(c) => Ok(c.c.max(0) as u64),
        None => Err(format!("{table}: COUNT answered no row")),
    }
}

/// THE SHADOW STORAGE (bsv-low loop 18, the gate's NIT on a source-shape-only pin): the two passes of the
/// shadow move run over this trait — D1 in production, real SQLite under the pins — so the PASSES themselves are
/// pinned (a row that lands between the first pass and the verification pass is moved by the verification pass,
/// under the real logic, not a hand-driven copy of its SQL). Every method is fail-LOUD.
#[async_trait(?Send)]
pub trait ShadowDb {
    async fn columns(&self, table: &str) -> Result<Vec<ColumnInfo>, String>;
    async fn count(&self, table: &str, key: &str, txid: &str) -> Result<u64, String>;
    /// The `outputIndex` values the keyed rows carry (asked only of the tables that have the column).
    async fn vouts(&self, table: &str, txid: &str) -> Result<Vec<u32>, String>;
    async fn exec(&self, sql: &str, binds: Vec<QVal>) -> Result<(), String>;
    /// One optional integer (the column aliased `v`), for the ledger's readmission stamp.
    async fn query_i64(&self, sql: &str, binds: Vec<QVal>) -> Result<Option<i64>, String>;
    /// The test seam: runs once between the first pass and the verification pass (a late-landing write, or a
    /// readmission overtaking the pass). A no-op in production.
    async fn between_passes(&self) {}
}

#[async_trait(?Send)]
impl ShadowDb for D1Database {
    async fn columns(&self, table: &str) -> Result<Vec<ColumnInfo>, String> {
        table_columns_checked(self, table).await
    }
    async fn count(&self, table: &str, key: &str, txid: &str) -> Result<u64, String> {
        count_keyed_checked(self, table, key, txid).await
    }
    async fn vouts(&self, table: &str, txid: &str) -> Result<Vec<u32>, String> {
        if !ident_ok(table) {
            return Err(format!("{table}: not an identifier"));
        }
        Ok(Query::new(format!(
            "SELECT \"outputIndex\" FROM \"{table}\" WHERE \"txid\" = ?"
        ))
        .bind(txid)
        .fetch_all::<VoutRow>(self)
        .await?
        .into_iter()
        .map(|r| r.output_index.max(0) as u32)
        .collect())
    }
    async fn exec(&self, sql: &str, binds: Vec<QVal>) -> Result<(), String> {
        let mut q = Query::new(sql);
        for b in binds {
            q = q.bind(b);
        }
        q.execute(self).await
    }
    async fn query_i64(&self, sql: &str, binds: Vec<QVal>) -> Result<Option<i64>, String> {
        #[derive(serde::Deserialize)]
        struct V {
            v: Option<i64>,
        }
        let mut q = Query::new(sql);
        for b in binds {
            q = q.bind(b);
        }
        Ok(q.fetch_optional::<V>(self).await?.and_then(|r| r.v))
    }
}

/// The tables whose moved rows are NOTED (the pots room and the lobby learn): both carry `outputIndex`.
const NOTED_TABLES: &[&str] = &["pot_records", "low_records"];

/// Copy the keyed rows into the twin (healed to the source's columns first), then delete them; the rows moved and,
/// for a noted table, the `outputIndex` values they carried (the gate's MEDIUM-1: a survivor the verification
/// pass moves must be noted too, and `vouts_of` read before the loop cannot see it). Fail-LOUD: every faulted step
/// is the caller's to record (loop 18: a skipped step used to read as a clean move). Re-runnable: a second pass
/// over rows that landed after the first copies them beside the first pass's twins (a twin never enforces; the
/// restore's `INSERT OR IGNORE` collapses a duplicate on the way back).
async fn move_keyed(
    db: &dyn ShadowDb,
    table: &str,
    key: &str,
    txid: &str,
    cols: &[ColumnInfo],
    reason: &str,
    now_ms: u64,
) -> Result<(u64, Vec<u32>), String> {
    let n = db.count(table, key, txid).await?;
    if n == 0 {
        return Ok((0, Vec::new()));
    }
    let vouts = if NOTED_TABLES.contains(&table) && key == "txid" {
        db.vouts(table, txid).await?
    } else {
        Vec::new()
    };
    ensure_shadow(db, table, cols)
        .await
        .map_err(|e| format!("twin: {e}"))?;
    let (ins, del) = move_sql(table, key, cols);
    db.exec(
        &ins,
        vec![
            QVal::Int(now_ms as i64),
            QVal::Text(reason.to_string()),
            QVal::Text(txid.to_string()),
        ],
    )
    .await
    .map_err(|e| format!("copy: {e}"))?;
    db.exec(&del, vec![QVal::Text(txid.to_string())])
        .await
        .map_err(|e| format!("delete after the copy: {e}"))?;
    Ok((n, vouts))
}

/// What the two passes of a shadow move did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PassReport {
    /// Rows moved by both passes.
    pub moved: u64,
    /// The verification pass's verdicts that leave the eviction INCOMPLETE (a table still holding the txid after
    /// the second move, a read that faulted, a second move that failed).
    pub incomplete: Vec<String>,
    /// First-pass faults the verification pass HEALED (the gate's LOW-2: a healed fault is not an incomplete
    /// eviction; it is said, not alarmed).
    pub retried: Vec<String>,
    /// The pot outpoints the moves carried (both passes; the notes' input).
    pub pot_vouts: Vec<u32>,
    /// The lobby adverts the moves carried (both passes).
    pub advert_vouts: Vec<u32>,
    /// What the verification pass found and did, for the caller's log (the passes never touch the console: they
    /// run under real SQLite in the pins, where no JS console exists).
    pub notes: Vec<String>,
}

/// THE TWO PASSES of the shadow move, over the shadow storage (loop 18). Pass 1 moves what each table holds when
/// its step runs; a write landing after a table's step survives it (pair 11's JOIN: the pot row and the
/// `tm_lowfund` applied row landed after their steps while `outputs`, `transactions` and the `tm_pot` applied row
/// moved). The VERIFICATION pass re-counts every table, moves a survivor once more, and names what still holds
/// the txid or could not be counted. `skip_verification` is the readmission guard (the gate's LOW-4): a MINED
/// proof that readmitted the txid AFTER this pass began restored the rows on purpose — a second move would undo
/// the chain's word.
pub async fn shadow_move_passes(
    db: &dyn ShadowDb,
    txid: &str,
    reason: &str,
    now_ms: u64,
    skip_verification: bool,
) -> PassReport {
    let mut report = PassReport::default();
    let mut pass1_faults: Vec<(String, String)> = Vec::new();
    let mut moved_tables: Vec<(&str, &str, Vec<ColumnInfo>)> = Vec::new();
    let take = |report: &mut PassReport, table: &str, n: u64, vouts: Vec<u32>| {
        report.moved += n;
        let into = if table == "pot_records" {
            &mut report.pot_vouts
        } else if table == "low_records" {
            &mut report.advert_vouts
        } else {
            return;
        };
        for v in vouts {
            if !into.contains(&v) {
                into.push(v);
            }
        }
    };
    // pass 1: the move, table by table (the columns read once, reused by the verification pass)
    for (table, keys) in MOVED_TABLES {
        let cols = match db.columns(table).await {
            Ok(c) => c,
            Err(e) => {
                pass1_faults.push((table.to_string(), format!("columns unreadable ({e})")));
                continue;
            }
        };
        if cols.is_empty() {
            continue; // the table does not exist on this database
        }
        for key in keys.iter() {
            if !cols.iter().any(|c| c.name == *key) {
                continue;
            }
            match move_keyed(db, table, key, txid, &cols, reason, now_ms).await {
                Ok((n, vouts)) => take(&mut report, table, n, vouts),
                Err(e) => pass1_faults.push((format!("{table} by {key}"), e)),
            }
            moved_tables.push((table, key, cols.clone()));
        }
    }
    db.between_passes().await;
    if skip_verification {
        // a readmission stamped after this pass began: its restore stands; the first pass's faults are its own
        for (where_, e) in pass1_faults {
            report.incomplete.push(format!("{where_}: {e} (no verification pass: a readmission landed under this pass)"));
        }
        return report;
    }
    // THE VERIFICATION PASS
    for (table, key, cols) in &moved_tables {
        let where_ = format!("{table} by {key}");
        let faulted_before = pass1_faults.iter().any(|(w, _)| *w == where_);
        let verdict: Result<(), String> = match db.count(table, key, txid).await {
            Ok(0) => Ok(()),
            Ok(n) => {
                report.notes.push(format!(
                    "{n} row(s) in {table} by {key} landed after the move (the admission write outran the eviction) — moved"
                ));
                match move_keyed(db, table, key, txid, cols, reason, now_ms).await {
                    Ok((m, vouts)) => {
                        take(&mut report, table, m, vouts);
                        match db.count(table, key, txid).await {
                            Ok(0) => Ok(()),
                            Ok(left) => Err(format!("{left} row(s) still present after the second move")),
                            Err(e) => Err(format!("the recount faulted ({e})")),
                        }
                    }
                    Err(e) => Err(format!("the second move failed ({e})")),
                }
            }
            Err(e) => Err(format!("the verification count faulted ({e})")),
        };
        match verdict {
            Ok(()) => {
                if faulted_before {
                    let e = pass1_faults
                        .iter()
                        .find(|(w, _)| *w == where_)
                        .map(|(_, e)| e.clone())
                        .unwrap_or_default();
                    report.retried.push(format!("{where_}: first pass {e}; the verification pass found it clean"));
                }
            }
            Err(e) => report.incomplete.push(format!("{where_}: {e}")),
        }
    }
    // a first-pass fault on a table the verification pass never saw (its columns were unreadable in pass 1)
    for (where_, e) in pass1_faults {
        if !moved_tables.iter().any(|(t, k, _)| format!("{t} by {k}") == where_) {
            report.incomplete.push(format!("{where_}: {e}"));
        }
    }
    report
}

/// THE OPEN MARKER (bsv-low loop 18, 2026-09-21, pair 11's JOIN `3b14f0e6…`): the ledger row is written BEFORE
/// the table loop, so every admission writer sees the eviction from its first moment (`open_eviction`). Binds:
/// txid, reason, evictedAt. An OPEN row (not yet readmitted) keeps its FIRST stamp and its counts — a concurrent
/// second eviction of the same txid (two callbacks and the pending watch race for one refusal) neither moves the
/// stamp nor resets the ledger; a row READMITTED BEFORE this pass's stamp is re-opened with the new stamp (the
/// chain overruled a courier, then the network refused it again); a readmission stamped AFTER this pass's stamp
/// (`?3`) is KEPT — the chain's word landed under the pass, which yields to it (round 2 of the gate, NEW-1).
/// PURE: the SQL, pinned under real SQLite below.
pub const OPEN_EVICTION_MARKER_SQL: &str = "INSERT INTO pot_evictions (txid, reason, evictedAt, readmittedAt, rowsMoved, releasedSpends) VALUES (?1, ?2, ?3, NULL, 0, '[]') \
     ON CONFLICT(txid) DO UPDATE SET reason = excluded.reason, \
     evictedAt = CASE WHEN pot_evictions.readmittedAt IS NULL OR pot_evictions.readmittedAt > ?3 THEN pot_evictions.evictedAt ELSE excluded.evictedAt END, \
     rowsMoved = CASE WHEN pot_evictions.readmittedAt IS NULL OR pot_evictions.readmittedAt > ?3 THEN pot_evictions.rowsMoved ELSE 0 END, \
     releasedSpends = CASE WHEN pot_evictions.readmittedAt IS NULL OR pot_evictions.readmittedAt > ?3 THEN pot_evictions.releasedSpends ELSE '[]' END, \
     readmittedAt = CASE WHEN pot_evictions.readmittedAt IS NOT NULL AND pot_evictions.readmittedAt > ?3 THEN pot_evictions.readmittedAt ELSE NULL END";

/// THE CLOSE (the end of a pass): the rows this pass moved are ADDED to the open row's count and the released
/// list written (the caller passes the UNION of the ledger's list and this pass's: the gate's LOW-1). An upsert,
/// so a pass whose open marker faulted still leaves the ledger row. A readmission stamped AFTER this pass began
/// (`?3` is the pass's stamp) is KEPT (the gate's LOW-4: the chain's word landed under the pass); an older stamp is
/// re-opened. Binds: ?1 txid, ?2 reason, ?3 evictedAt (the pass's now_ms), ?4 rowsMoved, ?5 releasedSpends.
pub const CLOSE_EVICTION_MARKER_SQL: &str = "INSERT INTO pot_evictions (txid, reason, evictedAt, readmittedAt, rowsMoved, releasedSpends) VALUES (?1, ?2, ?3, NULL, ?4, ?5) \
     ON CONFLICT(txid) DO UPDATE SET reason = excluded.reason, \
     evictedAt = CASE WHEN pot_evictions.readmittedAt IS NULL OR pot_evictions.readmittedAt > ?3 THEN pot_evictions.evictedAt ELSE excluded.evictedAt END, \
     rowsMoved = CASE WHEN pot_evictions.readmittedAt IS NULL OR pot_evictions.readmittedAt > ?3 THEN pot_evictions.rowsMoved ELSE 0 END + excluded.rowsMoved, \
     releasedSpends = COALESCE(NULLIF(excluded.releasedSpends, '[]'), CASE WHEN pot_evictions.readmittedAt IS NULL OR pot_evictions.readmittedAt > ?3 THEN pot_evictions.releasedSpends ELSE '[]' END), \
     readmittedAt = CASE WHEN pot_evictions.readmittedAt IS NOT NULL AND pot_evictions.readmittedAt > ?3 THEN pot_evictions.readmittedAt ELSE NULL END";

/// The guard's read: an eviction the ledger holds OPEN for the txid (bind: txid).
pub const OPEN_EVICTION_SQL: &str =
    "SELECT reason, evictedAt FROM pot_evictions WHERE txid = ? AND readmittedAt IS NULL";

/// The readmission-under-the-pass read (the gate's LOW-4): a `readmittedAt` stamped after the pass's own stamp.
pub const READMITTED_AFTER_SQL: &str =
    "SELECT readmittedAt AS v FROM pot_evictions WHERE txid = ?1 AND readmittedAt IS NOT NULL AND readmittedAt > ?2";

/// The ledger's released list for the merge (the gate's LOW-1).
pub const RELEASED_SPENDS_SQL: &str = "SELECT releasedSpends FROM pot_evictions WHERE txid = ?";

/// An eviction the ledger holds OPEN (not readmitted): what every admission writer must honour.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenEviction {
    pub reason: String,
    pub evicted_at_ms: u64,
}

#[derive(serde::Deserialize)]
struct OpenEvictionRow {
    reason: String,
    #[serde(rename = "evictedAt")]
    evicted_at: i64,
}

/// THE WRITE-SIDE GUARD (loop 18): is this txid under an OPEN eviction? Asked by the gated submit at the door
/// (the answer is recorded; the network's FRESH word decides: an accept readmits, a refusal stands — the gate's
/// HIGH-2: a blind door refusal made a wrong eviction permanent, and a public-bearer push could open one for a
/// txid the network never saw), AGAIN after its engine write (a write that outran the eviction's table loop
/// re-evicts what it wrote), by the ungated arms before their write (no ladder there: an open eviction refuses),
/// and by the queue consumer before and after a replay (a replay never resurrects an evicted pot). Fail-LOUD: an
/// unreadable ledger is the caller's to name and count, never a silent "none".
pub async fn open_eviction(db: &D1Database, txid: &str) -> Result<Option<OpenEviction>, String> {
    let txid = txid.to_ascii_lowercase();
    let row = Query::new(OPEN_EVICTION_SQL)
        .bind(txid.as_str())
        .fetch_optional::<OpenEvictionRow>(db)
        .await?;
    Ok(row.map(|r| OpenEviction {
        reason: r.reason,
        evicted_at_ms: r.evicted_at.max(0) as u64,
    }))
}

/// The refusal's STATUS WORD alone (`REJECTED`, `DOUBLE_SPEND_ATTEMPTED`), for a client-facing line: the reason
/// text embeds Arcade's `extraInfo`, and the client's already-known belt classifies on text (the gate's NIT).
pub fn refusal_status_word(reason: &str) -> &str {
    reason
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .find(|w| !w.is_empty())
        .unwrap_or("REJECTED")
}

/// PURE (the gate's LOW-1): the union of the ledger's released list and this pass's, by (table, txid, vout).
pub fn merge_released(existing_json: Option<&str>, fresh: &[ReleasedSpend]) -> Vec<ReleasedSpend> {
    let mut out: Vec<ReleasedSpend> = existing_json
        .and_then(|j| serde_json::from_str::<Vec<ReleasedSpend>>(j).ok())
        .unwrap_or_default();
    for r in fresh {
        if !out
            .iter()
            .any(|o| o.table == r.table && o.txid == r.txid && o.vout == r.vout)
        {
            out.push(r.clone());
        }
    }
    out
}

#[derive(serde::Deserialize)]
struct ReleasedJsonRow {
    #[serde(rename = "releasedSpends", default)]
    released_spends: Option<String>,
}

/// The twin RESTORE, over the shadow storage (loop 18, the gate's NEW-1/NEW-2): every twin row of the txid copied
/// back (`INSERT OR IGNORE`: idempotent) and dropped from the twin, the twin healed to the source's columns first
/// (a migration between the eviction and the readmission must not strand the restore); fail-LOUD per table (the
/// faults returned, never a silent partial restore that stamps the ledger). Used by the readmission and by an
/// eviction pass a readmission overtook (its own moves undone: the chain's word stands).
pub async fn restore_twins(db: &dyn ShadowDb, txid: &str) -> (u64, Vec<String>) {
    let mut restored = 0u64;
    let mut faults: Vec<String> = Vec::new();
    for (table, keys) in MOVED_TABLES {
        let twin = shadow_table(table);
        let cols = match db.columns(table).await {
            Ok(c) => c,
            Err(e) => {
                faults.push(format!("{table}: columns unreadable ({e})"));
                continue;
            }
        };
        if cols.is_empty() {
            continue;
        }
        let twin_cols = match db.columns(&twin).await {
            Ok(c) => c,
            Err(e) => {
                faults.push(format!("{twin}: columns unreadable ({e})"));
                continue;
            }
        };
        if twin_cols.is_empty() {
            continue; // no twin yet: nothing to restore
        }
        for key in keys.iter() {
            if !cols.iter().any(|c| c.name == *key) {
                continue;
            }
            let n = match db.count(&twin, key, txid).await {
                Ok(n) => n,
                Err(e) => {
                    faults.push(format!("{twin} by {key}: count faulted ({e})"));
                    continue;
                }
            };
            if n == 0 {
                continue;
            }
            if let Err(e) = ensure_shadow(db, table, &cols).await {
                faults.push(format!("{twin}: heal failed ({e})"));
                continue;
            }
            let (ins, del) = restore_sql(table, key, &cols);
            if let Err(e) = db.exec(&ins, vec![QVal::Text(txid.to_string())]).await {
                faults.push(format!("{table} by {key}: copy back failed ({e})"));
                continue;
            }
            // `INSERT OR IGNORE` swallows a NOT NULL/CHECK drop as silently as a duplicate (a NOT NULL column added
            // to the source after the eviction lands NULL from the healed twin): a copy back that landed NOTHING
            // keeps the twin's rows and names it — never a deleted twin over an empty source
            match db.count(table, key, txid).await {
                Ok(0) => {
                    faults.push(format!(
                        "{table} by {key}: the copy back landed 0 of {n} row(s) (a constraint dropped them) — the twin keeps them"
                    ));
                    continue;
                }
                Ok(_) => {}
                Err(e) => {
                    faults.push(format!("{table} by {key}: the count after the copy faulted ({e}) — the twin keeps its rows"));
                    continue;
                }
            }
            if let Err(e) = db.exec(&del, vec![QVal::Text(txid.to_string())]).await {
                faults.push(format!("{twin} by {key}: drop after the copy failed ({e})"));
                continue;
            }
            restored += n;
        }
    }
    (restored, faults)
}

/// What an eviction's core did (the marker, the passes, the readmission checks), for the D1 wrapper.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct EvictionCore {
    pub moved: u64,
    /// Everything that leaves the eviction INCOMPLETE (a faulted marker, the verification pass's verdicts, a
    /// faulted restore when yielding).
    pub faults: Vec<String>,
    pub retried: Vec<String>,
    pub notes: Vec<String>,
    pub pot_vouts: Vec<u32>,
    pub advert_vouts: Vec<u32>,
    /// A readmission (the chain's word) stamped after this pass began: `Some(stamp)`. Found BEFORE the passes
    /// → nothing moved; found AFTER them → the pass's own moves were restored. The wrapper releases no spends
    /// and the close keeps the stamp.
    pub yielded_to_readmission: Option<i64>,
    /// Rows the yield restored (the pass's own moves, undone).
    pub restored: u64,
}

/// The ledger's readmission stamp newer than a pass's own (the gate's LOW-4 / NEW-1), over the shadow storage.
async fn readmitted_after(db: &dyn ShadowDb, txid: &str, now_ms: u64) -> Result<Option<i64>, String> {
    db.query_i64(
        READMITTED_AFTER_SQL,
        vec![QVal::Text(txid.to_string()), QVal::Int(now_ms as i64)],
    )
    .await
}

/// THE EVICTION'S CORE, over the shadow storage (loop 18, round 2 of the gate: NEW-1). The order is the
/// invariant: the OPEN marker, the readmission check (a readmission stamped after this pass's stamp means the
/// chain's word already stands: nothing moves), the two passes, the readmission check AGAIN (a readmission that
/// landed DURING the passes restored rows the passes then moved: undo the pass's own moves — a restore is
/// idempotent — so the ledger's word and the tables agree at the end of every pass; the pre-round-1 code left
/// OPEN + evicted, which healed on the next accept; a close that kept the stamp over moved rows would have been
/// permanent). A readmission landing after the second check restores the twins itself, and the close keeps its
/// stamp: consistent by construction.
pub async fn evict_core(db: &dyn ShadowDb, txid: &str, reason: &str, now_ms: u64) -> EvictionCore {
    let mut core = EvictionCore::default();
    if let Err(e) = db
        .exec(
            OPEN_EVICTION_MARKER_SQL,
            vec![
                QVal::Text(txid.to_string()),
                QVal::Text(reason.to_string()),
                QVal::Int(now_ms as i64),
            ],
        )
        .await
    {
        core.faults.push(format!("the open marker failed ({e})"));
    }
    match readmitted_after(db, txid, now_ms).await {
        Ok(Some(at)) => {
            core.notes.push(format!("a readmission at {at} ms stands over this pass ({now_ms} ms): nothing moved"));
            core.yielded_to_readmission = Some(at);
            return core;
        }
        Ok(None) => {}
        Err(e) => core.notes.push(format!("the readmission read faulted before the passes ({e}) — the passes run")),
    }
    let report = shadow_move_passes(db, txid, reason, now_ms, false).await;
    core.moved = report.moved;
    core.faults.extend(report.incomplete);
    core.retried = report.retried;
    core.notes.extend(report.notes);
    core.pot_vouts = report.pot_vouts;
    core.advert_vouts = report.advert_vouts;
    match readmitted_after(db, txid, now_ms).await {
        Ok(Some(at)) => {
            let (restored, faults) = restore_twins(db, txid).await;
            core.notes.push(format!(
                "a readmission at {at} ms landed under this pass ({now_ms} ms): the pass's {} row(s) restored again; the chain's word stands",
                core.moved
            ));
            core.restored = restored;
            core.faults.extend(faults.into_iter().map(|f| format!("yielding to the readmission: {f}")));
            core.yielded_to_readmission = Some(at);
        }
        Ok(None) => {}
        Err(e) => core.faults.push(format!("the readmission read faulted after the passes ({e})")),
    }
    core
}

/// Evict every row the txid owns, everywhere, into the twins; note the pot and
/// lobby changes so the seats and the lobby learn; record the ledger row.
/// Returns the rows moved (0 = nothing held this txid).
///
/// bsv-low loop 18 (2026-09-21, pair 11's JOIN `3b14f0e6…`): the two callback jobs' evictions ran their table
/// steps WHILE the subject's own Phase-3 write was still landing (a 12.8 s engine submit under the fleet's t=0
/// burst; Arcade's REJECTED push had arrived 200 ms after its sync accept), so `outputs`, `transactions` and the
/// `tm_pot` applied row moved while the `pot_records` row and the `tm_lowfund` applied row, written after their
/// steps, stayed: `ls_pot` said `known` for the rest of the cell and neither felt voided. The core (`evict_core`)
/// is the fix: the ledger row written FIRST as an OPEN marker (every admission writer sees the eviction from its
/// first moment, `open_eviction`), the two passes with the VERIFICATION pass (a survivor moved once more and
/// NOTED; what still holds the txid, or could not be read, leaves the eviction INCOMPLETE — logged and counted,
/// never a silent success), and the readmission checks around them (the chain's word overtaking a pass is
/// honoured, never undone). This wrapper adds the D1-only halves: the released spend pointers (skipped when the
/// pass yielded), the ledger's close, the counters, the notes and the log.
pub async fn evict_txid_everywhere(db: &D1Database, txid: &str, reason: &str, now_ms: u64) -> u64 {
    let txid = txid.to_ascii_lowercase();
    let mut pot_vouts = vouts_of(db, "pot_records", &txid).await;
    let mut advert_vouts = vouts_of(db, "low_records", &txid).await;
    let core = evict_core(db, &txid, reason, now_ms).await;
    let moved = core.moved;
    for v in &core.pot_vouts {
        if !pot_vouts.contains(v) {
            pot_vouts.push(*v);
        }
    }
    for v in &core.advert_vouts {
        if !advert_vouts.contains(v) {
            advert_vouts.push(*v);
        }
    }
    for n in &core.notes {
        worker::console_log!("[admit-fast] evict {txid}: {n}");
    }
    for r in &core.retried {
        worker::console_log!("[admit-fast] evict {txid}: retried — {r}");
    }
    for v in &pot_vouts {
        crate::pot_changes::note(&txid, *v);
    }
    for v in &advert_vouts {
        crate::lobby_changes::note_evicted(&txid, *v);
    }
    // the spends it left on the rows it consumed (its hops, its pot) — released, and MERGED with the ledger's
    // list; NOT when the pass yielded to a readmission (the readmission re-marked them: they stand)
    let released = if core.yielded_to_readmission.is_some() {
        Vec::new()
    } else {
        release_spends_of(db, &txid).await
    };
    let existing_json = match Query::new(RELEASED_SPENDS_SQL)
        .bind(txid.as_str())
        .fetch_optional::<ReleasedJsonRow>(db)
        .await
    {
        Ok(row) => row.and_then(|r| r.released_spends),
        Err(e) => {
            worker::console_log!("[admit-fast] evict {txid}: the ledger's released list could not be read ({e}) — this pass's list is written");
            None
        }
    };
    let merged = merge_released(existing_json.as_deref(), &released);
    let released_json = serde_json::to_string(&merged).unwrap_or_else(|_| "[]".into());
    // THE CLOSE: the pass's rows added to the open row (an upsert: a faulted marker still leaves the row); a
    // readmission stamped after this pass's stamp is kept (the pass yielded to it above)
    if let Err(e) = Query::new(CLOSE_EVICTION_MARKER_SQL)
        .bind(txid.as_str())
        .bind(reason)
        .bind(QVal::Int(now_ms as i64))
        .bind(QVal::Int(moved as i64))
        .bind(released_json.as_str())
        .execute(db)
        .await
    {
        worker::console_log!("[admit-fast] evict {txid}: the ledger row failed: {e}");
    }
    if let Some(at) = core.yielded_to_readmission {
        crate::ops::bump_counter(db, crate::ops::COUNTER_ADMIT_FAST_EVICT_YIELDED, 1).await;
        worker::console_log!(
            "[admit-fast] evict {txid} YIELDED to a readmission at {at} ms ({} row(s) restored): the chain's word stands, the ledger stays readmitted",
            core.restored
        );
    }
    if !core.faults.is_empty() {
        crate::ops::bump_counter(db, crate::ops::COUNTER_ADMIT_FAST_EVICT_INCOMPLETE, 1).await;
        worker::console_log!(
            "[admit-fast] evict {txid} INCOMPLETE ({} fault(s): {}) — the open marker stands; the write-side guard and the next eviction converge",
            core.faults.len(),
            core.faults.join("; ")
        );
    }
    // bsv-low loop 11 (the app layer's gate, LOW-3): the notes above can be flushed by a CONCURRENT request's end on
    // this isolate before the ledger row exists (the set is isolate-global), and the app layer's recompute then finds
    // no eviction in its window and writes the hop as young; note the pot and the released hops ONCE MORE after the
    // row is written (the set dedupes; a second flush after the row is what the derivation needs).
    for v in &pot_vouts {
        crate::pot_changes::note(&txid, *v);
    }
    for r in &released {
        crate::pot_changes::note(&r.txid, r.vout);
    }
    worker::console_log!(
        "[admit-fast] evicted {txid} everywhere: {moved} row(s) moved, {} spend pointer(s) released ({reason})",
        released.len()
    );
    moved
}

#[derive(serde::Deserialize)]
struct EvictedRow {
    reason: String,
    #[serde(rename = "releasedSpends", default)]
    released_spends: Option<String>,
}

/// If the txid is in the ledger as evicted and not yet readmitted, move every
/// twin row back and mark the readmission. `true` when a readmission happened.
/// bsv-low #451 (the second gate's HIGH-1): the `refused` verdict memo for an evicted txid — `/tx-any` serves it as
/// `present: false` with zero courier calls; a readmission deletes it (`readmit_if_evicted`). Fail-soft.
pub async fn write_refused_verdict(db: &D1Database, txid: &str, now_ms: i64, reason: &str) {
    if let Err(e) = Query::new(crate::proof_fetcher::TX_ANY_VERDICT_UPSERT_SQL)
        .bind(txid)
        .bind(now_ms)
        .bind("refused")
        .bind(reason)
        .execute(db)
        .await
    {
        worker::console_log!("[admit-fast] verdict memo write failed for {txid}: {e}");
    }
}

pub async fn readmit_if_evicted(db: &D1Database, txid: &str, now_ms: u64) -> bool {
    let txid = txid.to_ascii_lowercase();
    // the second gate's MEDIUM-3b: a readmission (a pushed MINED proof — the chain overrules a courier) deletes the
    // verdict memo first, so no stale `present: false` outlives the row's return
    if let Err(e) = Query::new(crate::proof_fetcher::TX_ANY_VERDICT_DELETE_SQL)
        .bind(txid.as_str())
        .execute(db)
        .await
    {
        worker::console_log!("[admit-fast] readmit {txid}: verdict memo delete failed ({e})");
    }
    let row = match Query::new(
        "SELECT reason, releasedSpends FROM pot_evictions WHERE txid = ? AND readmittedAt IS NULL",
    )
    .bind(txid.as_str())
    .fetch_optional::<EvictedRow>(db)
    .await
    {
        Ok(row) => row,
        Err(e) => {
            worker::console_log!(
                "[admit-fast] readmit {txid}: the ledger could not be read ({e}) — nothing readmitted this pass"
            );
            return false;
        }
    };
    let Some(row) = row else {
        return false;
    };
    // loop 18, round 2 of the gate (NEW-2): the restore is fail-LOUD and the stamp follows a CLEAN restore only —
    // a faulted table leaves the row OPEN (counted; the next fresh accept or MINED push runs it again), never a
    // closed ledger over rows still in a twin (the phantom-applied class)
    let (restored, faults) = restore_twins(db, &txid).await;
    if !faults.is_empty() {
        crate::ops::bump_counter(db, crate::ops::COUNTER_ADMIT_FAST_READMIT_INCOMPLETE, 1).await;
        worker::console_log!(
            "[admit-fast] readmit {txid} INCOMPLETE ({} fault(s): {}; {restored} row(s) restored) — the row stays OPEN; the next accept or proof runs it again",
            faults.len(),
            faults.join("; ")
        );
        return false;
    }
    for v in vouts_of(db, "pot_records", &txid).await {
        crate::pot_changes::note(&txid, v);
    }
    for v in vouts_of(db, "low_records", &txid).await {
        crate::lobby_changes::note_admitted(&txid, v);
    }
    // the spends it had left on the rows it consumed — re-marked where nothing newer took them
    let released: Vec<ReleasedSpend> = row
        .released_spends
        .as_deref()
        .and_then(|j| serde_json::from_str(j).ok())
        .unwrap_or_default();
    let remarked = remark_spends(db, &txid, &released, now_ms).await;
    if let Err(e) = Query::new("UPDATE pot_evictions SET readmittedAt = ? WHERE txid = ? AND readmittedAt IS NULL")
        .bind(QVal::Int(now_ms as i64))
        .bind(txid.as_str())
        .execute(db)
        .await
    {
        worker::console_log!("[admit-fast] readmit {txid}: the stamp failed ({e}) — the row stays open; the rows are back");
    }
    // the belt (NEW-1, the other direction): a pass that opened DURING the restore may have moved the restored
    // rows again before the stamp — the pass's own post-check restores them, and so does this one (idempotent)
    let mut in_twins = 0u64;
    for (table, keys) in MOVED_TABLES {
        let twin = shadow_table(table);
        for key in keys.iter() {
            in_twins += count_keyed(db, &twin, key, &txid).await;
        }
    }
    let mut again = 0u64;
    if in_twins > 0 {
        let (r2, f2) = restore_twins(db, &txid).await;
        again = r2;
        worker::console_log!(
            "[admit-fast] readmit {txid}: {in_twins} twin row(s) reappeared under the restore (a concurrent pass) — restored again ({r2}; {} fault(s))",
            f2.len()
        );
    }
    worker::console_log!(
        "[admit-fast] READMITTED {txid} on the pushed proof: {restored} row(s) restored (+{again} under the belt), {remarked}/{} spend pointer(s) re-marked (was evicted: {})",
        released.len(),
        row.reason
    );
    true
}

/// What the evidence check concluded about a refusal callback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvidenceVerdict {
    /// Arcade's live word is fatal, or Arcade does not hold it AND both indexers say absent.
    Refused(String),
    /// Somebody holds it: the callback was stale or planted; nothing changes.
    Present,
    /// A courier faulted: nothing changes, counted.
    Uncertain(String),
}

/// A refusal that rests on ABSENCE (Arcade does not hold it, or faulted, and both indexers say absent) needs an
/// admission at least this old (bsv-low loop 18, the gate's HIGH-2): the indexers lag a young tx by design, so
/// "absent everywhere" seconds after an admission is the expected shape of a valid tx, not a refusal; and a push
/// naming a txid the index never admitted (the public bearer: anyone who knows a JOIN's txid before it is
/// broadcast) must never open an eviction on it. Arcade's OWN fatal word (REJECTED, DOUBLE_SPEND) needs no age:
/// it is a positive verdict on the bytes. The retire pass keeps its own 48 h floor for its own question.
pub const REFUSAL_ABSENT_MIN_AGE_MS: u64 = 10 * 60_000;

/// PURE: fold the three couriers' words (pinned). `young`: the admission is younger than
/// `REFUSAL_ABSENT_MIN_AGE_MS`, or the index holds no admission at all.
pub fn evidence_verdict(
    arcade: &crate::proof_fetcher::ArcadeLook,
    bitails: Option<bool>,
    woc: Option<bool>,
    young: bool,
) -> EvidenceVerdict {
    use crate::proof_fetcher::{ArcadeLook, NetworkPresence};
    match arcade {
        ArcadeLook::Present => EvidenceVerdict::Present,
        // A refusal needs TWO sources: Arcade's live word (fatal, or not
        // holding it) AND both indexers' definitive absence. One broadcaster's
        // REJECTED is never the network's verdict (#214: Arcade held txs
        // MINED in 958776 at a sticky REJECTED), and an indexer that holds
        // the tx is a witness against the refusal.
        ArcadeLook::Fatal(..) | ArcadeLook::Missing | ArcadeLook::Fault => {
            match crate::proof_fetcher::classify_presence(bitails, woc) {
                NetworkPresence::Present => EvidenceVerdict::Present,
                // Absence-based (Arcade missing or faulted, not its own fatal word): a young or never-admitted
                // txid is UNCERTAIN (counted), never refused (the gate's HIGH-2).
                NetworkPresence::Absent
                    if young && matches!(arcade, ArcadeLook::Missing | ArcadeLook::Fault) =>
                {
                    EvidenceVerdict::Uncertain(
                        "a young or unknown admission: an absence-based refusal needs Arcade's own word or an older admission"
                            .to_string(),
                    )
                }
                NetworkPresence::Absent => EvidenceVerdict::Refused(match arcade {
                    ArcadeLook::Fatal(status, extra) => {
                        format!("arcade live {status}: {extra}; both indexers absent")
                    }
                    ArcadeLook::Missing => "arcade missing, both indexers absent".to_string(),
                    _ => "arcade faulted, both indexers absent".to_string(),
                }),
                NetworkPresence::Inconclusive => {
                    EvidenceVerdict::Uncertain("a courier faulted".to_string())
                }
            }
        }
    }
}

/// What the deferred evidence check needs (cloned into the `wait_until` job).
#[derive(Clone)]
pub struct EvidenceEnv {
    pub arcade_base: String,
    pub woc_api_key: Option<String>,
    /// The worker env (Clone): the job opens `OVERLAY_DB` itself (a D1 handle is not Clone).
    pub env: worker::Env,
}

impl EvidenceEnv {
    pub fn from_env(env: &worker::Env) -> Self {
        Self {
            arcade_base: env
                .var("ARCADE_URL")
                .ok()
                .map(|v| v.to_string())
                .unwrap_or_else(|| crate::proof_fetcher::DEFAULT_ARCADE_URL.to_string()),
            woc_api_key: env.secret("WOC_API_KEY").ok().map(|s| s.to_string()),
            env: env.clone(),
        }
    }
}

/// The shared evidence check: Arcade LIVE first (the webhook's word is only a
/// fallback behind an Arcade outage: `fold_arcade_look`), then both indexers.
pub async fn evidence_check(
    env: &EvidenceEnv,
    txid: &str,
    webhook: (String, String),
) -> EvidenceVerdict {
    let look = crate::proof_fetcher::arcade_look(&env.arcade_base, txid, Some(webhook)).await;
    let (bitails, woc) = match look {
        // Arcade holds it: nothing to corroborate. Every other word (fatal,
        // missing, a fault) is one source; the indexers are the second.
        crate::proof_fetcher::ArcadeLook::Present => (None, None),
        _ => {
            let b = crate::proof_fetcher::bitails_presence(
                crate::proof_fetcher::DEFAULT_BITAILS_BASE,
                txid,
            )
            .await;
            let w = crate::proof_fetcher::woc_presence(
                crate::proof_fetcher::DEFAULT_WOC_BASE,
                env.woc_api_key.as_deref(),
                txid,
            )
            .await;
            (b, w)
        }
    };
    let young = match &look {
        crate::proof_fetcher::ArcadeLook::Missing | crate::proof_fetcher::ArcadeLook::Fault => {
            match env.env.d1("OVERLAY_DB") {
                Ok(db) => {
                    let now_ms = worker::Date::now().as_millis();
                    admission_age_ms(&db, txid, now_ms)
                        .await
                        .is_none_or(|age| age < REFUSAL_ABSENT_MIN_AGE_MS)
                }
                Err(_) => true, // no ledger to ask: the conservative word
            }
        }
        _ => false,
    };
    evidence_verdict(&look, bitails, woc, young)
}

#[derive(serde::Deserialize)]
struct AgeRow {
    s: Option<f64>,
}

/// How long ago the index admitted this txid (ms), from the engine's own stamp (`outputs.score`, the admission's
/// ms) with the pot row's `createdAt` (seconds) as the fallback; `None` when the index holds no admission.
pub async fn admission_age_ms(db: &D1Database, txid: &str, now_ms: u64) -> Option<u64> {
    let txid = txid.to_ascii_lowercase();
    let stamp_ms: Option<f64> = match Query::new("SELECT MIN(score) AS s FROM outputs WHERE txid = ?")
        .bind(txid.as_str())
        .fetch_optional::<AgeRow>(db)
        .await
    {
        Ok(Some(AgeRow { s: Some(s) })) if s > 0.0 => Some(s),
        _ => match Query::new("SELECT MIN(createdAt) AS s FROM pot_records WHERE txid = ?")
            .bind(txid.as_str())
            .fetch_optional::<AgeRow>(db)
            .await
        {
            Ok(Some(AgeRow { s: Some(s) })) if s > 0.0 => Some(s * 1000.0),
            // a spender (a settle, a refund, a sweep) admits no output and holds no pot row: its BEEF's
            // first-store stamp (seconds) is the third rung (the gate's NEW-6)
            _ => match Query::new("SELECT createdAt AS s FROM pot_beefs WHERE txid = ?")
                .bind(txid.as_str())
                .fetch_optional::<AgeRow>(db)
                .await
            {
                Ok(Some(AgeRow { s: Some(s) })) if s > 0.0 => Some(s * 1000.0),
                _ => None,
            },
        },
    };
    stamp_ms.map(|s| (now_ms as f64 - s).max(0.0) as u64)
}

/// The deferred job the callback route spawns for a SEEN+ push: Arcade LIVE
/// must say SEEN+ (one GET) before `network_seen` latches — the push is a
/// trigger, never the witness (the public bearer).
pub async fn witness_job(env: EvidenceEnv, txid: String, pushed_status: String) {
    let Ok(db) = env.env.d1("OVERLAY_DB") else {
        worker::console_log!(
            "[admit-fast] witness push for {txid}: no OVERLAY_DB in the job; nothing changes"
        );
        return;
    };
    let arcade = crate::broadcaster::ArcadeBroadcaster::new(env.arcade_base.clone());
    if arcade.network_witnessed(&txid).await {
        crate::ops::latch_network_seen(&db, &txid).await;
        // step 4: a background latch ships its own pots-room push.
        crate::pot_changes::flush_inline(env.env.clone()).await;
        crate::ops::bump_counter(&db, crate::ops::COUNTER_ARC_INGEST_SEEN_LATCHED, 1).await;
        worker::console_log!(
            "[admit-fast] {txid} {pushed_status}: verified live — network_seen latched by the push"
        );
    } else {
        crate::ops::bump_counter(&db, crate::ops::COUNTER_ARC_INGEST_PUSH_UNVERIFIED, 1).await;
        worker::console_log!("[admit-fast] {txid} {pushed_status}: Arcade live does NOT say SEEN — the push changed nothing (a plant or a stale word)");
    }
}

/// The deferred job the callback route spawns for a refusal: check, then evict
/// on a corroborated refusal; count an uncertain one.
pub async fn refusal_job(env: EvidenceEnv, txid: String, webhook: (String, String), now_ms: u64) {
    let verdict = evidence_check(&env, &txid, webhook.clone()).await;
    let Ok(db) = env.env.d1("OVERLAY_DB") else {
        worker::console_log!(
            "[admit-fast] refusal for {txid}: no OVERLAY_DB in the job; nothing changes"
        );
        return;
    };
    let db = &db;
    match verdict {
        EvidenceVerdict::Refused(reason) => {
            let moved =
                evict_txid_everywhere(db, &txid, &format!("{} ({reason})", webhook.0), now_ms)
                    .await;
            // the second gate's HIGH-1 (bsv-low #451): the evidence is HERE — `/tx-any` answers `present:false`
            // for the evicted JOIN at once (the felt's `broadcast-unaccepted` latch), never `null` for an hour
            write_refused_verdict(
                db,
                &txid,
                now_ms as i64,
                &format!("{} ({reason})", webhook.0),
            )
            .await;
            // the eviction is the event the felt voids on: ship its notes now
            // (the route's own flush drained before this job ran)
            crate::pot_changes::flush_inline(env.env.clone()).await;
            crate::ops::bump_counter(db, crate::ops::COUNTER_ARC_INGEST_EVICTED, 1).await;
            worker::console_log!("[admit-fast] refusal {} for {txid} CORROBORATED ({reason}) — evicted {moved} row(s)", webhook.0);
        }
        EvidenceVerdict::Present => {
            worker::console_log!("[admit-fast] refusal {} for {txid} NOT corroborated: a courier still holds it — kept", webhook.0);
            crate::ops::bump_counter(db, crate::ops::COUNTER_ARC_INGEST_REFUSAL_UNCORROBORATED, 1)
                .await;
        }
        EvidenceVerdict::Uncertain(why) => {
            worker::console_log!(
                "[admit-fast] refusal {} for {txid} UNCERTAIN ({why}) — kept, counted",
                webhook.0
            );
            crate::ops::bump_counter(db, crate::ops::COUNTER_ARC_INGEST_REFUSAL_UNCORROBORATED, 1)
                .await;
        }
    }
}

/// admit-fast step 3: when the pending watch LOOKS at Arcade after a pending
/// admission — sleeps between looks, so the looks land at 2, 4, 6, 8, 11, 14
/// and 18 s (≈ 20 s with the GETs: under the isolate's post-response
/// ceiling; the old wire poll saw SEEN on its 4th 2-s look, we22).
pub const PENDING_WATCH_SLEEPS_MS: [u64; 7] = [2_000, 2_000, 2_000, 2_000, 3_000, 3_000, 4_000];

/// The PENDING WATCH behind a fast (or #397 pending) admission — the witness
/// that used to be polled on the wire, in the background:
///   * SEEN or better → `network_seen` latched, counted with its latency
///     (`submit_pending_seen_ms_total / submit_pending_seen_latched_total`);
///   * a FATAL look (REJECTED / DOUBLE_SPEND_ATTEMPTED) → the shared evidence
///     check (Arcade live again, then both indexers) → a CORROBORATED refusal
///     evicts everywhere (the shadow move), counted; an uncorroborated one is
///     counted and kept (the callbacks and the passes own it);
///   * an ORPHAN look keeps watching (the #413 dual push is feeding the parents
///     to the second broadcaster meanwhile); still an orphan at the end → counted;
///   * unknown / below SEEN → keep watching; silent at the end → counted.
///
/// Every look is one GET; the latch is the only write on the happy path.
pub async fn pending_watch_job(env: EvidenceEnv, txid: String, admitted_at_ms: f64) {
    let Ok(db) = env.env.d1("OVERLAY_DB") else {
        worker::console_log!(
            "[admit-fast] pending watch for {txid}: no OVERLAY_DB in the job; the passes own it"
        );
        return;
    };
    // (an Rc: the watch-end proof read below builds the D1 pot store over the same handle; every `&db` below
    // deref-coerces to `&D1Database`)
    let db = std::rc::Rc::new(db);
    let arcade = crate::broadcaster::ArcadeBroadcaster::new(env.arcade_base.clone());
    let mut last = crate::broadcaster::WitnessLook::Unknown;
    for sleep in PENDING_WATCH_SLEEPS_MS {
        crate::broadcaster::sleep_ms(sleep).await;
        last = arcade.witness_look(&txid).await;
        match &last {
            crate::broadcaster::WitnessLook::Seen(status) => {
                crate::ops::latch_network_seen(&db, &txid).await;
                // step 4: a background latch ships its own pots-room push.
                crate::pot_changes::flush_inline(env.env.clone()).await;
                let ms = (worker::js_sys::Date::now() - admitted_at_ms).max(0.0) as u64;
                crate::ops::bump_counter(&db, crate::ops::COUNTER_SUBMIT_PENDING_SEEN_LATCHED, 1)
                    .await;
                crate::ops::bump_counter(&db, crate::ops::COUNTER_SUBMIT_PENDING_SEEN_MS, ms).await;
                worker::console_log!(
                    "[admit-fast] {txid} {status} {ms} ms after the admission — network_seen latched by the pending watch"
                );
                return;
            }
            crate::broadcaster::WitnessLook::Fatal(status, extra) => {
                let now_ms = worker::Date::now().as_millis();
                match evidence_check(&env, &txid, (status.clone(), extra.clone())).await {
                    EvidenceVerdict::Refused(reason) => {
                        let moved = evict_txid_everywhere(
                            &db,
                            &txid,
                            &format!("{status} ({reason})"),
                            now_ms,
                        )
                        .await;
                        // the second gate's HIGH-1: the verdict memo, written where the evidence is
                        write_refused_verdict(
                            &db,
                            &txid,
                            now_ms as i64,
                            &format!("{status} ({reason})"),
                        )
                        .await;
                        // the eviction is the event the felt voids on: ship its
                        // notes now (the route's flush drained before the watch)
                        crate::pot_changes::flush_inline(env.env.clone()).await;
                        crate::ops::bump_counter(
                            &db,
                            crate::ops::COUNTER_SUBMIT_PENDING_EVICTED,
                            1,
                        )
                        .await;
                        worker::console_log!(
                            "[admit-fast] {txid} {status} CORROBORATED ({reason}) — evicted {moved} row(s) by the pending watch"
                        );
                    }
                    EvidenceVerdict::Present => {
                        crate::ops::bump_counter(
                            &db,
                            crate::ops::COUNTER_SUBMIT_PENDING_REFUSAL_UNCORROBORATED,
                            1,
                        )
                        .await;
                        worker::console_log!(
                            "[admit-fast] {txid} {status} NOT corroborated: a courier still holds it — kept (the #214 class)"
                        );
                    }
                    EvidenceVerdict::Uncertain(why) => {
                        crate::ops::bump_counter(
                            &db,
                            crate::ops::COUNTER_SUBMIT_PENDING_REFUSAL_UNCORROBORATED,
                            1,
                        )
                        .await;
                        worker::console_log!(
                            "[admit-fast] {txid} {status} UNCERTAIN ({why}) — kept, counted"
                        );
                    }
                }
                return;
            }
            crate::broadcaster::WitnessLook::Orphan(_)
            | crate::broadcaster::WitnessLook::Pending(_)
            | crate::broadcaster::WitnessLook::Unknown => {}
        }
    }
    // fleet loop 15 (2026-09-20, pair 1's refund): a spend that MINED before this index ever saw it — the tower
    // parked it at arm time through TAAL, the node promoted it at H, the client re-presented it after the block —
    // gets Arcade's STORED ECHO (ACCEPTED_BY_NETWORK, never MINED: its status froze at the first presentation)
    // and no callback (already known: none registered), so nothing ever pushes its proof; the 30-minute backstop
    // pass was the only path, and every consumer (the owed row, the felt) waited on a block that had come. The
    // watch's end runs the single-spend completion NOW: ONE indexed read names the pots this txid spends
    // unconfirmed (a JOIN, a funding, a marker: nothing to do, no courier asked); for a recorded spender, the
    // EXHAUSTIVE proof ladder (Arcade's frozen word must not stop it), chaintracks-verified, the pass's guarded
    // CAS, the push — once per txid per isolate-minute (a re-present flood of a known spender is not a courier
    // amplifier). The couriers are asked only when the txid IS a recorded unconfirmed spender.
    let confirmed_now = match confirm_spend_now(&env, db.clone(), &txid).await {
        ConfirmNow::Confirmed {
            rows,
            cas_missed,
            cas_faults,
            height,
        } if rows >= 1 => {
            crate::ops::bump_counter(&db, crate::ops::COUNTER_SUBMIT_PENDING_CONFIRMED_NOW, 1).await;
            crate::pot_changes::flush_inline(env.env.clone()).await;
            worker::console_log!(
                "[admit-fast] {txid} CONFIRMED by the watch's own proof read (height {height:?}; {rows} row(s), {cas_missed} CAS miss(es), {cas_faults} CAS fault(s)) — the block had come before the index saw the spend"
            );
            true
        }
        // every pointer moved off this txid between the read and the CAS: a proof of a tx no pot row relates to
        // any more — nothing confirmed, nothing pushed, the watch counts as silent (the delta-verify's N1)
        ConfirmNow::Confirmed { cas_missed, .. } => {
            worker::console_log!(
                "[admit-fast] {txid} the watch's proof read verified but every pointer had moved ({cas_missed} CAS miss(es)) — nothing confirmed"
            );
            false
        }
        ConfirmNow::Unmined => {
            worker::console_log!(
                "[admit-fast] {txid} the watch's proof read: no verified proof yet (unmined so far) — the passes own it"
            );
            false
        }
        ConfirmNow::NoUnconfirmedSpend | ConfirmNow::Memoised => false,
        ConfirmNow::Fault(why) => {
            worker::console_log!(
                "[admit-fast] {txid} the watch's proof read faulted ({why}) — the passes own it"
            );
            false
        }
    };
    if !confirmed_now {
        let name = if matches!(last, crate::broadcaster::WitnessLook::Orphan(_)) {
            crate::ops::COUNTER_SUBMIT_PENDING_ORPHAN
        } else {
            crate::ops::COUNTER_SUBMIT_PENDING_SILENT
        };
        crate::ops::bump_counter(&db, name, 1).await;
        worker::console_log!(
            "[admit-fast] {txid} unwitnessed after the pending watch ({last:?}) — the callbacks, the completion and the reconcile passes own it"
        );
    }
}

/// What the watch's own proof read found for one spender txid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfirmNow {
    /// no pot names this txid as an unconfirmed spender (nothing to confirm)
    NoUnconfirmedSpend,
    /// the couriers were asked for this txid inside the last minute on this isolate (a re-present flood of a
    /// known spender is not a courier amplifier); the passes own it
    Memoised,
    /// the couriers hold no chaintracks-verified proof yet (honestly unmined so far)
    Unmined,
    /// a verified proof: the rows' confirmations latched through the guarded CAS (a CAS read fault on one row is
    /// counted in `cas_faults` and the loop continues — the cron's shape; the rows that confirmed stay confirmed)
    Confirmed {
        rows: usize,
        cas_missed: usize,
        cas_faults: usize,
        height: Option<u64>,
    },
    /// a read fault (a courier, chaintracks, or the store): retryable, never a verdict
    Fault(String),
}

/// PURE over its two ports (the store and the fetcher): confirm every pot outpoint whose recorded, unconfirmed
/// spender is `txid` from ONE chaintracks-verified proof read — the spend-confirmation pass's per-row step, scoped
/// to a single spender so the pending watch can run it at its end. The CAS is the pass's own
/// (`mark_confirmed_for_spender`: a moved pointer confirms nothing and is counted).
pub async fn confirm_spend_now_with(
    pot_storage: &dyn overlay_discovery::pot::storage::PotStorage,
    fetcher: &dyn overlay_engine::gasp::AncestorFetcher,
    txid: &str,
) -> ConfirmNow {
    let rows = match pot_storage.find_unconfirmed_by_spending_txid(txid).await {
        Ok(r) => r,
        Err(e) => return ConfirmNow::Fault(format!("spender lookup: {e}")),
    };
    confirm_spend_now_rows(pot_storage, fetcher, txid, rows).await
}

/// The step after the spender lookup (the rows already read once — the delta-verify's N2: the wiring reads them
/// for the memo and hands them here, never a second read).
pub async fn confirm_spend_now_rows(
    pot_storage: &dyn overlay_discovery::pot::storage::PotStorage,
    fetcher: &dyn overlay_engine::gasp::AncestorFetcher,
    txid: &str,
    rows: Vec<overlay_discovery::pot::storage::PotRecord>,
) -> ConfirmNow {
    if rows.is_empty() {
        return ConfirmNow::NoUnconfirmedSpend;
    }
    // EXHAUSTIVE: Arcade's word for this txid is exactly what froze (the review's MED-3); the detailed ask stops
    // on a fresh "held unmined" from Arcade and would never reach Bitails or WoC for the class this exists for.
    let bump_hex = match fetcher.verified_proof_for_exhaustive(txid).await {
        Ok(Some(b)) => b,
        Ok(None) => return ConfirmNow::Unmined,
        Err(e) => return ConfirmNow::Fault(format!("proof read: {e}")),
    };
    let height = bsv_rs::transaction::MerklePath::from_hex(&bump_hex)
        .ok()
        .map(|mp| u64::from(mp.block_height));
    let (mut confirmed, mut cas_missed, mut cas_faults) = (0usize, 0usize, 0usize);
    for rec in rows {
        match pot_storage
            .mark_confirmed_for_spender(&rec.txid, rec.output_index, txid, height)
            .await
        {
            Ok(true) => {
                confirmed += 1;
                // (the D1 store notes the outpoint itself inside its CAS; the note set dedupes — this one is for
                // a store that does not, and the pots-room push reads the set once)
                crate::pot_changes::note(&rec.txid, rec.output_index);
            }
            Ok(false) => cas_missed += 1,
            // count and continue (the cron's shape): the rows that confirmed stay confirmed and are pushed
            Err(e) => {
                cas_faults += 1;
                worker::console_log!(
                    "[admit-fast] {txid} confirm CAS on {}:{} faulted: {e}",
                    rec.txid,
                    rec.output_index
                );
            }
        }
    }
    if confirmed == 0 && cas_missed == 0 {
        return ConfirmNow::Fault(format!("confirm CAS faulted on every row ({cas_faults})"));
    }
    ConfirmNow::Confirmed {
        rows: confirmed,
        cas_missed,
        cas_faults,
        height,
    }
}

thread_local! {
    /// The watch-end confirms asked of the couriers on this isolate, by txid → the ask's ms (the re-present memo).
    static CONFIRM_NOW_ASKED: std::cell::RefCell<std::collections::HashMap<String, f64>> =
        std::cell::RefCell::new(std::collections::HashMap::new());
}
/// One courier ask per txid per isolate-minute. The memo records the ATTEMPT, not an answer: a transport fault
/// inside the ask also holds the memo (fail-safe — the 30-minute backstop owns the row); a memo hit is never
/// "we have a fresh answer".
pub const CONFIRM_NOW_MEMO_MS: f64 = 60_000.0;

/// PURE: does the memo still hold at `now_ms` for an ask made at `asked_ms`?
pub fn confirm_now_memoised(asked_ms: Option<f64>, now_ms: f64) -> bool {
    asked_ms.is_some_and(|t| now_ms - t < CONFIRM_NOW_MEMO_MS)
}

/// The watch's wiring of [`confirm_spend_now_with`]: the D1 pot store and the courier proof ladder (the budget
/// counts ASKS, one here; the ladder's own rungs and chaintracks reads sit outside it). The memo keeps a
/// re-present flood of one known spender from asking the couriers more than once a minute per isolate; the
/// spender lookup runs first so a txid that spends no recorded pot never touches the memo or the couriers.
async fn confirm_spend_now(
    env: &EvidenceEnv,
    db: std::rc::Rc<worker::D1Database>,
    txid: &str,
) -> ConfirmNow {
    use overlay_discovery::pot::storage::PotStorage;
    let store = crate::d1_discovery::D1PotStorage::new(db.clone());
    let rows = match store.find_unconfirmed_by_spending_txid(txid).await {
        Ok(r) => r,
        Err(e) => return ConfirmNow::Fault(format!("spender lookup: {e}")),
    };
    if rows.is_empty() {
        return ConfirmNow::NoUnconfirmedSpend;
    }
    let now_ms = worker::js_sys::Date::now();
    let memoised = CONFIRM_NOW_ASKED.with(|m| {
        let mut m = m.borrow_mut();
        m.retain(|_, t| now_ms - *t < CONFIRM_NOW_MEMO_MS);
        if confirm_now_memoised(m.get(txid).copied(), now_ms) {
            true
        } else {
            m.insert(txid.to_string(), now_ms);
            false
        }
    });
    if memoised {
        worker::console_log!(
            "[admit-fast] {txid} the couriers were asked inside the last minute on this isolate — not again; the passes own it"
        );
        return ConfirmNow::Memoised;
    }
    crate::ops::bump_counter(&db, crate::ops::COUNTER_SUBMIT_PENDING_CONFIRM_ASKED, 1).await;
    let fetcher = crate::courier_fetcher(&env.env, crate::lookup_service_chain_tracker(&env.env))
        .with_budget(1);
    confirm_spend_now_rows(&store, &fetcher, txid, rows).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proof_fetcher::ArcadeLook;
    use async_trait::async_trait;
    use overlay_discovery::pot::storage::PotStorage;

    // ── fleet loop 15 (2026-09-20): the watch's own proof read confirms a spend that mined before the index saw it ──
    struct ProofOnlyFetcher {
        bump: Option<String>,
        fault: bool,
    }
    #[async_trait(?Send)]
    impl overlay_engine::gasp::AncestorFetcher for ProofOnlyFetcher {
        async fn fetch_ancestor(
            &self,
            txid: &str,
        ) -> Result<overlay_engine::gasp::FetchedAncestor, overlay_engine::gasp::GASPError>
        {
            Err(overlay_engine::gasp::GASPError::NodeNotFound(
                txid.to_string(),
            ))
        }
        async fn verified_proof_for_detailed(&self, _txid: &str) -> Result<Option<String>, String> {
            if self.fault {
                return Err("chaintracks read starved".into());
            }
            Ok(self.bump.clone())
        }
    }
    fn pot_rec(txid: &str) -> overlay_discovery::pot::storage::PotRecord {
        overlay_discovery::pot::storage::PotRecord {
            txid: txid.to_string(),
            output_index: 0,
            pot_sats: Some(40_000),
            params_decoded: true,
            ..Default::default()
        }
    }
    fn bump_hex(txid: &str, height: u32) -> String {
        bsv_rs::transaction::MerklePath::new_unchecked(
            height,
            vec![vec![bsv_rs::transaction::MerklePathLeaf::new_txid(
                0,
                txid.to_string(),
            )]],
        )
        .expect("a one-leaf bump")
        .to_hex()
    }

    /// The class: the index records the spend unconfirmed (a re-present after the block; Arcade's echo never says
    /// MINED and no callback is registered), and the watch's own proof read confirms it at once — the height from
    /// the bump, the guarded CAS, the outpoint noted for the pots-room push. A fetcher without a proof leaves the
    /// row honestly unconfirmed; a read fault is a fault, never a verdict; a spender nothing names is nothing to do.
    #[tokio::test]
    async fn the_watchs_own_proof_read_confirms_a_spend_that_mined_before_the_index_saw_it() {
        let store = overlay_discovery::pot::storage::MemoryPotStorage::new();
        let pot = "ab".repeat(32);
        let spender = "cd".repeat(32);
        store.store_record(&pot_rec(&pot)).await.unwrap();
        store
            .mark_spent(&pot, 0, &spender, false, None, None, Some(false))
            .await
            .unwrap();
        // no proof yet: honestly unmined, nothing written
        let none = ProofOnlyFetcher {
            bump: None,
            fault: false,
        };
        assert_eq!(
            confirm_spend_now_with(&store, &none, &spender).await,
            ConfirmNow::Unmined
        );
        let r = store.get_spent_status(&pot, 0).await.unwrap().unwrap();
        assert!(!r.spent_confirmed);
        // a read fault: a fault, never a verdict
        let faulty = ProofOnlyFetcher {
            bump: None,
            fault: true,
        };
        assert!(matches!(
            confirm_spend_now_with(&store, &faulty, &spender).await,
            ConfirmNow::Fault(_)
        ));
        // the proof: confirmed now, at the bump's height
        let proven = ProofOnlyFetcher {
            bump: Some(bump_hex(&spender, 967_603)),
            fault: false,
        };
        assert_eq!(
            confirm_spend_now_with(&store, &proven, &spender).await,
            ConfirmNow::Confirmed { rows: 1, cas_missed: 0, cas_faults: 0, height: Some(967_603) }
        );
        let r = store.get_spent_status(&pot, 0).await.unwrap().unwrap();
        assert!(r.spent_confirmed);
        assert_eq!(r.spent_height, Some(967_603));
        // nothing names a stranger's txid as an unconfirmed spender
        assert_eq!(
            confirm_spend_now_with(&store, &proven, &"ef".repeat(32)).await,
            ConfirmNow::NoUnconfirmedSpend
        );
        // and the confirmed row is not confirmed twice (the finder skips confirmed rows)
        assert_eq!(
            confirm_spend_now_with(&store, &proven, &spender).await,
            ConfirmNow::NoUnconfirmedSpend
        );
    }

    /// The memo: one courier ask per txid per isolate-minute (the review's MED-4: a re-present flood of a known
    /// spender must not amplify into courier reads); a never-asked txid asks, an aged memo asks again.
    #[test]
    fn the_watch_end_confirm_asks_the_couriers_once_a_minute_per_txid() {
        assert!(!confirm_now_memoised(None, 1e12));
        assert!(confirm_now_memoised(Some(1e12), 1e12 + 1.0));
        assert!(confirm_now_memoised(Some(1e12), 1e12 + CONFIRM_NOW_MEMO_MS - 1.0));
        assert!(!confirm_now_memoised(Some(1e12), 1e12 + CONFIRM_NOW_MEMO_MS));
    }

    /// The watch fits the post-response budget (gate LOW-1: eviction of the
    /// isolate must not silently skip the latch) and looks early (the old wire
    /// poll's cadence: SEEN typically by the 4th 2-s look). Fleet loop 15: the
    /// watch's END adds the single-spend confirm AFTER the sleeps (one indexed
    /// read; for a recorded spender, the exhaustive proof ladder's reads) — an
    /// isolate evicted inside it loses only a confirm the 30-minute backstop
    /// pass still owns (fail-safe), and the silent/orphan counter is bumped only
    /// when the confirm did not confirm.
    #[test]
    fn the_pending_watch_fits_the_post_response_budget_and_looks_early() {
        let total: u64 = PENDING_WATCH_SLEEPS_MS.iter().sum();
        assert!(total <= 20_000, "{total} ms of sleeps");
        assert_eq!(PENDING_WATCH_SLEEPS_MS[0], 2_000, "the first look at 2 s");
        assert!(
            PENDING_WATCH_SLEEPS_MS[..3].iter().all(|&s| s == 2_000),
            "2-s cadence through 6 s"
        );
    }

    #[test]
    fn the_callback_decision_table() {
        for s in [
            "SEEN_ON_NETWORK",
            "seen_multiple_nodes",
            "MINED",
            "IMMUTABLE",
        ] {
            assert_eq!(callback_action(s), CallbackAction::LatchSeen, "{s}");
        }
        assert_eq!(
            callback_action("DOUBLE_SPEND_ATTEMPTED"),
            CallbackAction::EvidenceCheck("DOUBLE_SPEND_ATTEMPTED".into()),
            "a double-spend PUSH is a claim under the public bearer: corroborated live, never acted on alone"
        );
        assert_eq!(
            callback_action("rejected"),
            CallbackAction::EvidenceCheck("REJECTED".into())
        );
        assert_eq!(
            callback_action("SEEN_IN_ORPHAN_MEMPOOL"),
            CallbackAction::EvidenceCheck("SEEN_IN_ORPHAN_MEMPOOL".into())
        );
        for s in [
            "RECEIVED",
            "STORED",
            "ANNOUNCED_TO_NETWORK",
            "REQUESTED_BY_NETWORK",
            "SENT_TO_NETWORK",
            "ACCEPTED_BY_NETWORK",
            "",
            "reorg_unmined",
        ] {
            assert_eq!(callback_action(s), CallbackAction::Ignore, "{s}");
        }
    }

    #[test]
    fn the_evidence_verdict_never_evicts_on_one_courier_and_never_on_a_fault() {
        // Arcade's live fatal word is ONE source: alone (the indexers not
        // asked or faulting) it is uncertain, never a refusal.
        assert!(matches!(
            evidence_verdict(
                &ArcadeLook::Fatal("REJECTED".into(), "x".into()),
                None,
                None,
                false
            ),
            EvidenceVerdict::Uncertain(_)
        ));
        // Fatal + both indexers definitively absent: the corroborated refusal.
        assert!(matches!(
            evidence_verdict(
                &ArcadeLook::Fatal("DOUBLE_SPEND_ATTEMPTED".into(), "x".into()),
                Some(false),
                Some(false),
                false
            ),
            EvidenceVerdict::Refused(_)
        ));
        // Fatal + an indexer HOLDS it: the #214 class (a stale Arcade
        // REJECTED of a tx the network holds) — kept.
        assert_eq!(
            evidence_verdict(
                &ArcadeLook::Fatal("REJECTED".into(), "x".into()),
                Some(false),
                Some(true),
                false
            ),
            EvidenceVerdict::Present
        );
        assert_eq!(
            evidence_verdict(&ArcadeLook::Present, Some(false), Some(false), false),
            EvidenceVerdict::Present,
            "Arcade live holds it: a planted REJECTED changes nothing"
        );
        assert!(
            matches!(
                evidence_verdict(&ArcadeLook::Missing, Some(false), Some(false), false),
                EvidenceVerdict::Refused(_)
            ),
            "missing + both absent"
        );
        assert_eq!(
            evidence_verdict(&ArcadeLook::Missing, Some(true), Some(false), false),
            EvidenceVerdict::Present,
            "one indexer holds it"
        );
        assert!(
            matches!(
                evidence_verdict(&ArcadeLook::Missing, None, Some(false), false),
                EvidenceVerdict::Uncertain(_)
            ),
            "a courier fault is never absence"
        );
        assert!(
            matches!(
                evidence_verdict(&ArcadeLook::Fault, Some(false), Some(false), false),
                EvidenceVerdict::Refused(_)
            ),
            "Arcade down + both indexers definitive absent"
        );
        assert!(matches!(
            evidence_verdict(&ArcadeLook::Fault, None, None, false),
            EvidenceVerdict::Uncertain(_)
        ));
    }

    #[test]
    fn identifiers_are_validated_before_interpolation() {
        assert!(ident_ok("pot_records") && ident_ok("_x1"));
        for bad in [
            "",
            "1abc",
            "a-b",
            "a b",
            "a;",
            "pot_records\" --",
            &"a".repeat(65),
        ] {
            assert!(!ident_ok(bad), "{bad:?}");
        }
        assert_eq!(shadow_table("pot_records"), "pot_records_evicted");
    }

    fn shipped_conn() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().expect("open in-memory sqlite");
        for sql in crate::d1::OVERLAY_MIGRATIONS {
            if let Err(e) = conn.execute_batch(sql) {
                let msg = e.to_string().to_ascii_lowercase();
                assert!(
                    msg.contains("duplicate column"),
                    "migration failed under real SQLite: {e}\n{sql}"
                );
            }
        }
        conn
    }

    fn cols(conn: &rusqlite::Connection, table: &str) -> Vec<ColumnInfo> {
        let mut st = conn
            .prepare(&format!("PRAGMA table_info(\"{table}\")"))
            .unwrap();
        st.query_map([], |r| {
            Ok(ColumnInfo {
                name: r.get(1)?,
                ty: r.get::<_, String>(2).unwrap_or_default(),
            })
        })
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
    }

    fn count(conn: &rusqlite::Connection, table: &str, key: &str, txid: &str) -> i64 {
        conn.query_row(
            &format!("SELECT COUNT(*) FROM \"{table}\" WHERE \"{key}\" = ?1"),
            [txid],
            |r| r.get(0),
        )
        .unwrap()
    }

    /// The shadow move under the REAL shipped schema: rows keyed by the txid
    /// leave every source table (a reader sees nothing), the twins hold them
    /// with the reason, the restore brings them back byte for byte, and a
    /// column added to a source AFTER the twin exists is healed onto the twin.
    #[test]
    fn the_shadow_move_evicts_everywhere_and_restores_byte_for_byte_under_the_shipped_schema() {
        let conn = shipped_conn();
        let pot = "ab".repeat(32);
        let other = "cd".repeat(32);
        conn.execute("INSERT INTO pot_records (txid, outputIndex, spent, spendingTxid, createdAt, lockKind, pubA, stakeA) VALUES (?1, 0, 0, NULL, 1700000000, 'covenant', '02aa', 20000)", [&pot]).unwrap();
        conn.execute("INSERT INTO pot_records (txid, outputIndex, spent, createdAt) VALUES (?1, 0, 0, 1700000001)", [&other]).unwrap();
        conn.execute("INSERT INTO outputs (txid, outputIndex, outputScript, topic, satoshis, spent) VALUES (?1, 0, X'51', 'tm_pot', 40000, 0)", [&pot]).unwrap();
        conn.execute(
            "INSERT INTO applied_transactions (txid, topic) VALUES (?1, 'tm_pot')",
            [&pot],
        )
        .unwrap();
        let party_cols = cols(&conn, "potparty_records");
        assert!(
            party_cols.iter().any(|c| c.name == "potTxid"),
            "the party table keys on the pot"
        );
        conn.execute(
            "INSERT INTO potparty_records (txid, outputIndex, identity, opponentIdentity, gameId, potTxid, potVout, recoveryHeight, sigHex, createdAt) VALUES ('11', 0, '02bb', '02cc', 'gg', ?1, 0, 900000, '30', 1700000002)",
            [&pot],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO pot_beefs (txid, beef, createdAt) VALUES (?1, X'0100beef', 1700000000)",
            [&pot],
        )
        .unwrap();
        let before: Vec<(String, i64, Option<String>, i64)> = conn
            .query_row(
                "SELECT lockKind, stakeA, spendingTxid, createdAt FROM pot_records WHERE txid = ?1",
                [&pot],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .map(|t| vec![t])
            .unwrap();
        // ---- evict: the exact SQL the D1 path runs, per table and key ----
        let now = 1_700_000_100_000i64;
        let mut moved = 0;
        for (table, keys) in MOVED_TABLES {
            let c = cols(&conn, table);
            if c.is_empty() {
                continue;
            }
            for key in keys.iter() {
                if !c.iter().any(|x| x.name == *key) {
                    continue;
                }
                let n = count(&conn, table, key, &pot);
                if n == 0 {
                    continue;
                }
                conn.execute_batch(&create_shadow_sql(table, &c)).unwrap();
                let (ins, del) = move_sql(table, key, &c);
                conn.execute(
                    &ins,
                    rusqlite::params![now, "REJECTED (arcade live REJECTED)", &pot],
                )
                .unwrap();
                conn.execute(&del, [&pot]).unwrap();
                moved += n;
            }
        }
        assert_eq!(
            moved, 4,
            "the pot row, its output, its applied row, its party marker"
        );
        assert_eq!(
            count(&conn, "pot_records", "txid", &pot),
            0,
            "every reader of pot_records sees nothing"
        );
        assert_eq!(
            count(&conn, "pot_records", "txid", &other),
            1,
            "another pot is untouched"
        );
        assert_eq!(count(&conn, "outputs", "txid", &pot), 0);
        assert_eq!(
            count(&conn, "potparty_records", "potTxid", &pot),
            0,
            "the marker about the pot is gone from the recovery listing"
        );
        assert_eq!(
            count(&conn, "pot_beefs", "txid", &pot),
            1,
            "the bytes stay (never deleted): the readmit's source"
        );
        assert_eq!(count(&conn, "pot_records_evicted", "txid", &pot), 1);
        let reason: String = conn
            .query_row(
                "SELECT af_reason FROM pot_records_evicted WHERE txid = ?1",
                [&pot],
                |r| r.get(0),
            )
            .unwrap();
        assert!(reason.starts_with("REJECTED"));
        // ---- a column the source gains AFTER the twin exists: healed before the next move ----
        conn.execute_batch("ALTER TABLE pot_records ADD COLUMN af_test_later TEXT")
            .unwrap();
        let c2 = cols(&conn, "pot_records");
        let twin_cols = cols(&conn, "pot_records_evicted");
        for col in &c2 {
            if !twin_cols.iter().any(|t| t.name == col.name) {
                conn.execute_batch(&heal_shadow_sql("pot_records", col))
                    .unwrap();
            }
        }
        assert!(cols(&conn, "pot_records_evicted")
            .iter()
            .any(|c| c.name == "af_test_later"));
        // ---- restore: byte for byte ----
        for (table, keys) in MOVED_TABLES {
            let c = cols(&conn, table);
            if c.is_empty() || cols(&conn, &shadow_table(table)).is_empty() {
                continue;
            }
            for key in keys.iter() {
                if !c.iter().any(|x| x.name == *key) {
                    continue;
                }
                let (ins, del) = restore_sql(table, key, &c);
                conn.execute(&ins, [&pot]).unwrap();
                conn.execute(&del, [&pot]).unwrap();
            }
        }
        let after: Vec<(String, i64, Option<String>, i64)> = conn
            .query_row(
                "SELECT lockKind, stakeA, spendingTxid, createdAt FROM pot_records WHERE txid = ?1",
                [&pot],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .map(|t| vec![t])
            .unwrap();
        assert_eq!(after, before, "the pot row came back as it was");
        assert_eq!(count(&conn, "outputs", "txid", &pot), 1);
        assert_eq!(count(&conn, "applied_transactions", "txid", &pot), 1);
        assert_eq!(count(&conn, "potparty_records", "potTxid", &pot), 1);
        assert_eq!(
            count(&conn, "pot_records_evicted", "txid", &pot),
            0,
            "the twin is empty again"
        );
        let script: Vec<u8> = conn
            .query_row(
                "SELECT outputScript FROM outputs WHERE txid = ?1",
                [&pot],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(script, vec![0x51], "a BLOB column survives the round trip");
        // ---- the ledger table exists in the shipped schema ----
        conn.execute("INSERT INTO pot_evictions (txid, reason, evictedAt, readmittedAt, rowsMoved) VALUES (?1, 'x', 1, NULL, 4)", [&pot]).unwrap();
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pot_evictions WHERE readmittedAt IS NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 1);
    }

    /// Structural: the callback route latches SEEN, evicts a double spend at
    /// once, defers a rejection to the evidence job, and readmits before the
    /// proof stitch — each BEFORE the `arc_terminal` record / the engine.
    #[test]
    fn the_callback_route_wires_the_witness_the_eviction_and_the_readmission() {
        // comment-stripped (round 2 of the loop-18 gate, NEW-7): "1. engine `transactions` stitch" is a comment
        // the raw text matched first, which made the readmit-before-stitch half vacuous
        let src: String = include_str!("routes.rs")
            .lines()
            .map(|l| l.split("//").next().unwrap_or(""))
            .collect::<Vec<_>>()
            .join("\n");
        let src = src.as_str();
        let f = &src[src.find("pub async fn arc_ingest(").unwrap()..];
        let f = &f[..f.find("pub async fn admin_evict_outpoint").unwrap()];
        let status_arm = f
            .find("ArcIngestBody::StatusOnly {")
            .expect("the status-only arm");
        let action = f
            .find("crate::admit_fast::callback_action(&tx_status)")
            .expect("the decision");
        let witness = f
            .find("crate::admit_fast::witness_job(")
            .expect("a SEEN push is VERIFIED live before it latches");
        let job = f
            .find("crate::admit_fast::refusal_job(")
            .expect("a refusal push defers to the evidence job");
        let terminal = f
            .find("crate::ops::record_arc_terminal(")
            .expect("the evidence record");
        assert!(
            status_arm < action && action < witness && witness < job && job < terminal,
            "decision → the witness job → the refusal job → the terminal record"
        );
        assert!(
            !f.contains("crate::ops::latch_network_seen(db, &txid)"),
            "the route never latches on a push's word alone (the public bearer)"
        );
        assert!(
            !f.contains("crate::admit_fast::evict_txid_everywhere("),
            "the route never evicts on a push's word alone"
        );
        let proof_arm = f.find("ArcIngestBody::Proof {").expect("the proof arm");
        let readmit = f
            .find("crate::admit_fast::readmit_if_evicted(")
            .expect("a pushed proof readmits");
        let stitch = f[readmit..].find("engine").expect("the engine after") + readmit;
        assert!(
            proof_arm < readmit && readmit < stitch,
            "readmit before the engine stitches"
        );
        // bsv-low loop 18 (the gate's MEDIUM-2): the readmission closes an open eviction — the ledger's state
        // gates admission now — so it runs only AFTER the pushed merklePath verified against chaintracks (the
        // route's bearer is the public txid; a garbage proof must never open the door).
        let verified = f
            .find("crate::proof_fetcher::verify_bump(tracker, &merkle_path, &txid)")
            .expect("the bump verification");
        assert!(verified < readmit, "the readmission follows the verified bump, never precedes it");
    }

    /// The spends an evicted tx left on the rows it CONSUMED are released at
    /// eviction (the hop reads unspent again — the seat's own sweep may run)
    /// and re-marked on readmission, only where nothing newer took the
    /// outpoint; a CONFIRMED pointer never moves; a second eviction keeps the
    /// first list. Under the REAL shipped schema.
    #[test]
    fn the_eviction_releases_the_spends_the_tx_left_on_its_inputs_and_the_readmission_re_marks_them(
    ) {
        let conn = shipped_conn();
        let join = "ab".repeat(32);
        let hop = "cd".repeat(32);
        let proven_pot = "aa".repeat(32);
        let other_hop = "ef".repeat(32);
        let other_spender = "12".repeat(32);
        conn.execute(
            "INSERT INTO pot_records (txid, outputIndex, spent, spendingTxid, spentConfirmed, spenderFinal, verdict, verdictTxid, createdAt) VALUES (?1, 0, 1, ?2, 0, 1, 'x', ?2, 1700000000)",
            rusqlite::params![&hop, &join],
        )
        .unwrap();
        // a MINED spend by the same txid (a merkle-proven pointer): never released
        conn.execute(
            "INSERT INTO pot_records (txid, outputIndex, spent, spendingTxid, spentConfirmed, createdAt) VALUES (?1, 0, 1, ?2, 1, 1700000000)",
            rusqlite::params![&proven_pot, &join],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO pot_records (txid, outputIndex, spent, spendingTxid, createdAt) VALUES (?1, 0, 1, ?2, 1700000000)",
            rusqlite::params![&other_hop, &other_spender],
        )
        .unwrap();
        // ---- release: the exact SQL the D1 path runs ----
        let pot_cols = cols(&conn, "pot_records");
        let rows: Vec<(String, i64, i64)> = conn
            .prepare(&select_pot_spends_sql(&pot_cols))
            .unwrap()
            .query_map([&join], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(
            rows,
            vec![(hop.clone(), 0, 0), (proven_pot.clone(), 0, 1)],
            "the JOIN's rows with their confirmation, never the other spender's"
        );
        conn.execute(&release_pot_spends_sql(&pot_cols), [&join])
            .unwrap();
        let (spent, spender, spender_final, verdict): (i64, Option<String>, Option<i64>, Option<String>) =
            conn.query_row(
                "SELECT spent, spendingTxid, spenderFinal, verdict FROM pot_records WHERE txid = ?1",
                [&hop],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!(
            (spent, spender, spender_final, verdict),
            (0, None, None, None),
            "the hop reads unspent again; the spend and verdict groups left with the spender"
        );
        let proven: (i64, Option<String>, i64) = conn
            .query_row(
                "SELECT spent, spendingTxid, spentConfirmed FROM pot_records WHERE txid = ?1",
                [&proven_pot],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            proven,
            (1, Some(join.clone()), 1),
            "a CONFIRMED pointer is positive evidence: never released on a courier's absence"
        );
        let other: (i64, Option<String>) = conn
            .query_row(
                "SELECT spent, spendingTxid FROM pot_records WHERE txid = ?1",
                [&other_hop],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            other,
            (1, Some(other_spender.clone())),
            "another spender's row is untouched"
        );
        // ---- the ledger column; a second eviction that releases nothing keeps the first list ----
        let first = "[{\"table\":\"pot_records\",\"txid\":\"cd\",\"vout\":0}]";
        let upsert = "INSERT INTO pot_evictions (txid, reason, evictedAt, readmittedAt, rowsMoved, releasedSpends) VALUES (?1, 'x', ?2, NULL, 0, ?3) \
             ON CONFLICT(txid) DO UPDATE SET reason = excluded.reason, evictedAt = excluded.evictedAt, readmittedAt = NULL, rowsMoved = excluded.rowsMoved, \
             releasedSpends = COALESCE(NULLIF(excluded.releasedSpends, '[]'), pot_evictions.releasedSpends)";
        conn.execute(upsert, rusqlite::params![&join, 1, first])
            .unwrap();
        conn.execute(upsert, rusqlite::params![&join, 2, "[]"])
            .unwrap();
        let kept: String = conn
            .query_row(
                "SELECT releasedSpends FROM pot_evictions WHERE txid = ?1",
                [&join],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            kept, first,
            "the callback's second eviction (nothing left to release) keeps the watch's list"
        );
        let released: Vec<ReleasedSpend> = serde_json::from_str(&kept).unwrap();
        assert_eq!(
            released,
            vec![ReleasedSpend {
                table: "pot_records".into(),
                txid: "cd".into(),
                vout: 0
            }]
        );
        // ---- re-mark: the freed hop is marked again; a hop something newer spent is left alone ----
        let (remark, with_at) = remark_pot_spend_sql(&pot_cols);
        assert!(with_at, "the shipped schema carries spentAt");
        conn.execute(&remark, rusqlite::params![&join, 1_700_000_200i64, &hop, 0])
            .unwrap();
        let (spent, spender, at): (i64, Option<String>, Option<i64>) = conn
            .query_row(
                "SELECT spent, spendingTxid, spentAt FROM pot_records WHERE txid = ?1",
                [&hop],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            (spent, spender, at),
            (1, Some(join.clone()), Some(1_700_000_200))
        );
        conn.execute(
            "UPDATE pot_records SET spendingTxid = ?1 WHERE txid = ?2",
            rusqlite::params![&other_spender, &hop],
        )
        .unwrap();
        conn.execute(&remark, rusqlite::params![&join, 1_700_000_300i64, &hop, 0])
            .unwrap();
        let spender: Option<String> = conn
            .query_row(
                "SELECT spendingTxid FROM pot_records WHERE txid = ?1",
                [&hop],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            spender,
            Some(other_spender.clone()),
            "a newer spender stands: the chain decides"
        );
    }

    /// Structural: the eviction releases the consumed spends (before the
    /// ledger row records them), the readmission re-marks them (after the
    /// rows are back).
    #[test]
    fn the_eviction_releases_and_the_readmission_re_marks_the_consumed_spends() {
        let src = include_str!("admit_fast.rs");
        let evict = &src[src.find("pub async fn evict_txid_everywhere(").unwrap()..];
        let evict = &evict[..evict.find("struct EvictedRow").unwrap()];
        let release = evict
            .find("release_spends_of(db, &txid)")
            .expect("the eviction releases");
        let ledger = evict
            .find("Query::new(CLOSE_EVICTION_MARKER_SQL)")
            .expect("the ledger row's close");
        assert!(
            release < ledger,
            "released BEFORE the close records the list"
        );
        assert!(
            CLOSE_EVICTION_MARKER_SQL.contains("COALESCE(NULLIF(excluded.releasedSpends, '[]')"),
            "a second pass that releases nothing keeps the first list (pinned under SQLite in the marker test)"
        );
        let readmit = &src[src.find("pub async fn readmit_if_evicted(").unwrap()..];
        let readmit = &readmit[..readmit.find("pub enum EvidenceVerdict").unwrap()];
        let restore = readmit.find("restore_twins(db, &txid)").expect("the restore");
        let remark = readmit
            .find("remark_spends(db, &txid, &released, now_ms)")
            .expect("the re-mark");
        assert!(restore < remark, "re-marked AFTER the rows are back");
        // the eviction is the event the felt voids on: BOTH jobs ship the pot
        // notes right after it (the route's own flush drained before the job)
        // fleet loop 15 (2026-09-20): measured on WHITESPACE-SQUASHED text — a rustfmt reflow of an unrelated
        // call moved the raw distance past the bound and a formatter red'd this pin; a formatter must never be
        // able to red (or green) a source pin, so the distance is a fact of the tokens, not the line breaks
        let squash = |s: &str| s.split_whitespace().collect::<String>();
        // comments stripped too (the delta-verify's N3): a doc line above the flush can never red this pin, and
        // a commented-out flush can never green it — the 700 is a bound on CODE
        let code_only = |s: &str| {
            s.lines()
                .map(|l| l.split("//").next().unwrap_or(""))
                .collect::<Vec<_>>()
                .join("\n")
        };
        let jobs = squash(&code_only(
            &src[src.find("pub async fn refusal_job(").unwrap()..src.find("#[cfg(test)]").unwrap()],
        ));
        let mut from = 0;
        let mut evictions = 0;
        while let Some(i) = jobs[from..].find("evict_txid_everywhere(") {
            let at = from + i;
            let fl = jobs[at..]
                .find("crate::pot_changes::flush_inline(")
                .expect("a flush after the eviction");
            assert!(
                fl < 700,
                "the flush sits right after the eviction (at +{fl}, squashed)"
            );
            evictions += 1;
            from = at + 1;
        }
        assert_eq!(evictions, 2, "the refusal job and the pending watch");
    }

    /// bsv-low loop 18 (2026-09-21, pair 11): the OPEN marker under real SQLite — written first, the FIRST open
    /// stamp kept across a concurrent second eviction, the pass's rows ADDED by the close, an earlier released
    /// list never erased by a later empty one, the guard's read true while open and silent once readmitted, and
    /// a readmitted row re-opened with the new stamp and fresh counts.
    #[test]
    fn the_open_marker_keeps_the_first_open_stamp_and_reopens_after_a_readmission() {
        use rusqlite::OptionalExtension;
        let conn = shipped_conn();
        let pot = "ab".repeat(32);
        let open = |at: i64, reason: &str| {
            conn.execute(OPEN_EVICTION_MARKER_SQL, rusqlite::params![&pot, reason, at])
                .unwrap()
        };
        let close = |at: i64, reason: &str, moved: i64, released: &str| {
            conn.execute(
                CLOSE_EVICTION_MARKER_SQL,
                rusqlite::params![&pot, reason, at, moved, released],
            )
            .unwrap()
        };
        let row = || {
            conn.query_row(
                "SELECT reason, evictedAt, readmittedAt, rowsMoved, releasedSpends FROM pot_evictions WHERE txid = ?1",
                [&pot],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, Option<i64>>(2)?,
                        r.get::<_, i64>(3)?,
                        r.get::<_, Option<String>>(4)?,
                    ))
                },
            )
            .unwrap()
        };
        let guard = || {
            conn.query_row(OPEN_EVICTION_SQL, [&pot], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
            })
            .optional()
            .unwrap()
        };
        assert_eq!(guard(), None, "nothing open before any eviction");
        open(1_000, "REJECTED (a)");
        assert_eq!(row(), ("REJECTED (a)".into(), 1_000, None, 0, Some("[]".into())));
        assert_eq!(guard(), Some(("REJECTED (a)".into(), 1_000)), "open from the marker on");
        // a concurrent second eviction of the same txid: the first open stamp stands
        open(1_500, "REJECTED (b)");
        assert_eq!(row().1, 1_000, "the first open stamp is kept");
        // the close ADDS the pass's rows and records the list; a later close with '[]' keeps it
        close(1_000, "REJECTED (a)", 3, r#"[{"table":"pot_records","txid":"cd","vout":0}]"#);
        close(1_500, "REJECTED (b)", 2, "[]");
        let r = row();
        assert_eq!((r.1, r.3), (1_000, 5), "both passes' rows added; the stamp unmoved");
        assert_eq!(
            r.4.as_deref(),
            Some(r#"[{"table":"pot_records","txid":"cd","vout":0}]"#),
            "an empty later list never erases the first"
        );
        // readmitted: the guard sees nothing; a new eviction re-opens with the NEW stamp and fresh counts
        conn.execute("UPDATE pot_evictions SET readmittedAt = 2000 WHERE txid = ?1", [&pot])
            .unwrap();
        assert_eq!(guard(), None, "a readmitted row is not open");
        open(3_000, "REJECTED (c)");
        assert_eq!(row(), ("REJECTED (c)".into(), 3_000, None, 0, Some("[]".into())));
        close(3_000, "REJECTED (c)", 1, "[]");
        assert_eq!(row().3, 1, "fresh counts after a readmission");
        // a close with NO open marker (the marker faulted) still leaves the row
        let other = "cd".repeat(32);
        conn.execute(
            CLOSE_EVICTION_MARKER_SQL,
            rusqlite::params![&other, "REJECTED (d)", 4_000i64, 2i64, "[]"],
        )
        .unwrap();
        let n: i64 = conn
            .query_row(
                "SELECT rowsMoved FROM pot_evictions WHERE txid = ?1 AND readmittedAt IS NULL",
                [&other],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 2);
    }

    /// bsv-low loop 18 (pair 11's JOIN `3b14f0e6…`): rows the subject's own admission write lands AFTER a
    /// table's move step survive the first pass — the pre-fix end state (`outputs` and the `tm_pot` applied row
    /// in the twins, the pot row and the `tm_lowfund` applied row still served). The verification pass is the
    /// SAME move SQL re-run: it takes the survivors, the sources end empty, the twins hold both passes' rows,
    /// and the restore brings everything back once.
    #[test]
    fn a_row_that_lands_after_its_tables_move_is_taken_by_the_verification_pass() {
        let conn = shipped_conn();
        let pot = "ab".repeat(32);
        conn.execute("INSERT INTO outputs (txid, outputIndex, outputScript, topic, satoshis, spent) VALUES (?1, 0, X'51', 'tm_pot', 40000, 0)", [&pot]).unwrap();
        conn.execute(
            "INSERT INTO applied_transactions (txid, topic) VALUES (?1, 'tm_pot')",
            [&pot],
        )
        .unwrap();
        let mv = |table: &str| {
            let c = cols(&conn, table);
            conn.execute_batch(&create_shadow_sql(table, &c)).unwrap();
            let (ins, del) = move_sql(table, "txid", &c);
            conn.execute(&ins, rusqlite::params![1_000i64, "REJECTED", &pot])
                .unwrap();
            conn.execute(&del, [&pot]).unwrap();
        };
        // pass 1 sees what the write had landed so far
        for t in ["pot_records", "outputs", "applied_transactions"] {
            mv(t);
        }
        assert_eq!(count(&conn, "outputs", "txid", &pot), 0);
        assert_eq!(count(&conn, "applied_transactions_evicted", "txid", &pot), 1);
        // …then the write lands the pot row and the tm_lowfund applied row (pair 11's 12.8 s Phase 3)
        conn.execute("INSERT INTO pot_records (txid, outputIndex, spent, createdAt, lockKind) VALUES (?1, 0, 0, 1789959998, 'covenant')", [&pot]).unwrap();
        conn.execute(
            "INSERT INTO applied_transactions (txid, topic) VALUES (?1, 'tm_lowfund')",
            [&pot],
        )
        .unwrap();
        assert_eq!(
            count(&conn, "pot_records", "txid", &pot),
            1,
            "the pre-fix end state: a pot the index still knows"
        );
        // the verification pass: the same move, re-run, takes the survivors
        for t in ["pot_records", "outputs", "applied_transactions"] {
            mv(t);
        }
        assert_eq!(count(&conn, "pot_records", "txid", &pot), 0);
        assert_eq!(count(&conn, "applied_transactions", "txid", &pot), 0);
        assert_eq!(count(&conn, "pot_records_evicted", "txid", &pot), 1);
        assert_eq!(
            count(&conn, "applied_transactions_evicted", "txid", &pot),
            2,
            "both passes' applied rows in the twin"
        );
        // the restore brings both applied rows back, the pot row and the output once
        for t in ["pot_records", "applied_transactions", "outputs"] {
            let c = cols(&conn, t);
            let (ins, del) = restore_sql(t, "txid", &c);
            conn.execute(&ins, [&pot]).unwrap();
            conn.execute(&del, [&pot]).unwrap();
        }
        assert_eq!(count(&conn, "pot_records", "txid", &pot), 1);
        assert_eq!(count(&conn, "applied_transactions", "txid", &pot), 2);
        assert_eq!(count(&conn, "outputs", "txid", &pot), 1);
        assert_eq!(count(&conn, "pot_records_evicted", "txid", &pot), 0);
    }

    /// Structural (loop 18): the OPEN marker is written BEFORE the table loop, the verification pass runs
    /// AFTER it, the close after the release; the loop reads through the checked variants only (a faulted
    /// PRAGMA or COUNT is recorded, never a silent zero), and an incomplete pass is counted.
    #[test]
    fn the_eviction_marks_first_verifies_after_and_reads_fail_loud() {
        let src = include_str!("admit_fast.rs");
        // the core: the marker → the readmission check → the passes → the readmission check again → the restore
        let core = &src[src.find("pub async fn evict_core(").unwrap()..];
        let core = &core[..core.find("\n}\n").unwrap()];
        let marker = core.find("OPEN_EVICTION_MARKER_SQL").expect("the open marker");
        let pre = core.find("match readmitted_after(db, txid, now_ms).await").expect("the readmission check before the passes");
        let passes = core
            .find("shadow_move_passes(db, txid, reason, now_ms, false)")
            .expect("the two passes, over the shadow storage");
        let post = core[passes..].find("match readmitted_after(db, txid, now_ms).await").expect("the readmission check after the passes") + passes;
        let restore = core[post..].find("restore_twins(db, txid)").expect("the pass yields: its own moves restored") + post;
        assert!(marker < pre && pre < passes && passes < post && post < restore, "marker → check → passes → check → restore");
        assert!(core[pre..passes].contains("return core;"), "a readmission found BEFORE the passes: nothing moves");
        // the wrapper: the core → the release unless the pass yielded → the merge → the close → the counters
        let evict = &src[src.find("pub async fn evict_txid_everywhere(").unwrap()..];
        let evict = &evict[..evict.find("struct EvictedRow").unwrap()];
        let c = evict.find("evict_core(db, &txid, reason, now_ms)").expect("the core");
        let release = evict.find("release_spends_of(db, &txid)").expect("the release");
        let merge = evict.find("merge_released(").expect("the released list merged");
        let close = evict.find("Query::new(CLOSE_EVICTION_MARKER_SQL)").expect("the close");
        assert!(c < release && release < merge && merge < close, "core → release → merge → close");
        assert!(
            evict[..release].contains("core.yielded_to_readmission.is_some()"),
            "no release when the pass yielded to a readmission (the readmission re-marked them)"
        );
        assert!(evict.contains("COUNTER_ADMIT_FAST_EVICT_INCOMPLETE"), "an incomplete pass is counted");
        assert!(evict.contains("COUNTER_ADMIT_FAST_EVICT_YIELDED"), "a yielded pass is counted");
        // the passes never touch the console and read through the shadow storage only
        let passes_src = &src[src.find("pub async fn shadow_move_passes(").unwrap()..];
        let passes_src = &passes_src[..passes_src.find("\n}\n").unwrap()];
        let looping = passes_src.find("for (table, keys) in MOVED_TABLES").expect("the table loop");
        let seam = passes_src.find("db.between_passes().await").expect("the seam");
        let verify = passes_src.find("THE VERIFICATION PASS").expect("the verification pass");
        assert!(looping < seam && seam < verify, "pass 1 → the seam → the verification pass");
        for f in [core, passes_src, &src[src.find("pub async fn restore_twins(").unwrap()..][..2_000]] {
            assert!(!f.contains("console_log!"), "the generic halves never touch the console");
            assert!(!f.contains("count_keyed(db") && !f.contains("table_columns(db"), "the generic halves read through the shadow storage only");
        }
        // the readmission stamps only after a CLEAN restore (NEW-2), never over a faulted table
        let readmit = &src[src.find("pub async fn readmit_if_evicted(").unwrap()..];
        let readmit = &readmit[..readmit.find("\n}\n").unwrap()];
        let restore_at = readmit.find("restore_twins(db, &txid)").expect("the restore");
        let incomplete = readmit.find("COUNTER_ADMIT_FAST_READMIT_INCOMPLETE").expect("the incomplete counter");
        let stamp = readmit.find("UPDATE pot_evictions SET readmittedAt = ? WHERE txid = ? AND readmittedAt IS NULL").expect("the stamp");
        assert!(restore_at < incomplete && incomplete < stamp, "restore → (faults: open, counted, return) → the stamp");
        assert!(readmit[incomplete..stamp].contains("return false;"), "a faulted restore never stamps");
        // the guard's read is the ledger's open row, never the twins (a twin can hold duplicates; the ledger is one row)
        assert!(OPEN_EVICTION_SQL.contains("readmittedAt IS NULL"));
    }

    /// bsv-low loop 18 (the gate's HIGH-2 b): an ABSENCE-based refusal (Arcade missing or faulted, both indexers
    /// absent) needs an admission older than `REFUSAL_ABSENT_MIN_AGE_MS`; a young or never-admitted txid is
    /// UNCERTAIN (a push naming a JOIN's txid before it is broadcast opens nothing). Arcade's own fatal word needs
    /// no age.
    #[test]
    fn an_absence_based_refusal_needs_an_old_admission_but_arcades_own_word_needs_none() {
        assert!(matches!(
            evidence_verdict(&ArcadeLook::Missing, Some(false), Some(false), true),
            EvidenceVerdict::Uncertain(_)
        ));
        assert!(matches!(
            evidence_verdict(&ArcadeLook::Fault, Some(false), Some(false), true),
            EvidenceVerdict::Uncertain(_)
        ));
        assert!(matches!(
            evidence_verdict(&ArcadeLook::Missing, Some(false), Some(false), false),
            EvidenceVerdict::Refused(_)
        ));
        assert!(matches!(
            evidence_verdict(
                &ArcadeLook::Fatal("REJECTED".into(), "UTXO_SPENT".into()),
                Some(false),
                Some(false),
                true
            ),
            EvidenceVerdict::Refused(_)
        ));
        // a courier that holds it wins over youth either way
        assert_eq!(
            evidence_verdict(&ArcadeLook::Missing, Some(true), Some(false), true),
            EvidenceVerdict::Present
        );
        assert_eq!(REFUSAL_ABSENT_MIN_AGE_MS, 10 * 60_000);
    }

    /// The shadow storage over real SQLite (the shipped schema): the passes run their REAL logic here. The seam
    /// `between_passes` lands a late write (pair 11's shape); `fail_first_copy_of` makes one table's first copy
    /// fault (the gate's LOW-2).
    type LateWrite = Box<dyn FnOnce(&rusqlite::Connection)>;
    struct SqliteShadow {
        conn: rusqlite::Connection,
        late: std::cell::RefCell<Option<LateWrite>>,
        fail_first_copy_of: std::cell::RefCell<Option<String>>,
    }
    impl SqliteShadow {
        fn new(conn: rusqlite::Connection) -> Self {
            Self { conn, late: std::cell::RefCell::new(None), fail_first_copy_of: std::cell::RefCell::new(None) }
        }
    }
    fn to_sql(v: &QVal) -> rusqlite::types::Value {
        match v {
            QVal::Null => rusqlite::types::Value::Null,
            QVal::Int(i) => rusqlite::types::Value::Integer(*i),
            QVal::Text(t) => rusqlite::types::Value::Text(t.clone()),
            QVal::Bool(b) => rusqlite::types::Value::Integer(i64::from(*b)),
            QVal::Blob(b) => rusqlite::types::Value::Blob(b.clone()),
            QVal::Float(f) => rusqlite::types::Value::Real(*f),
        }
    }
    #[async_trait(?Send)]
    impl ShadowDb for SqliteShadow {
        async fn columns(&self, table: &str) -> Result<Vec<ColumnInfo>, String> {
            Ok(cols(&self.conn, table))
        }
        async fn count(&self, table: &str, key: &str, txid: &str) -> Result<u64, String> {
            Ok(count(&self.conn, table, key, txid).max(0) as u64)
        }
        async fn vouts(&self, table: &str, txid: &str) -> Result<Vec<u32>, String> {
            let mut st = self
                .conn
                .prepare(&format!("SELECT \"outputIndex\" FROM \"{table}\" WHERE \"txid\" = ?1"))
                .map_err(|e| e.to_string())?;
            let rows = st
                .query_map([txid], |r| r.get::<_, i64>(0))
                .map_err(|e| e.to_string())?
                .map(|r| r.map(|v| v.max(0) as u32))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| e.to_string())?;
            Ok(rows)
        }
        async fn exec(&self, sql: &str, binds: Vec<QVal>) -> Result<(), String> {
            if let Some(rest) = sql.strip_prefix("INSERT INTO \"") {
                let table = rest.split('"').next().unwrap_or("");
                if let Some(t) = self.fail_first_copy_of.borrow_mut().take() {
                    if table == format!("{t}_evicted") {
                        return Err("simulated D1 fault on the first copy".to_string());
                    }
                    *self.fail_first_copy_of.borrow_mut() = Some(t);
                }
            }
            let params: Vec<rusqlite::types::Value> = binds.iter().map(to_sql).collect();
            self.conn
                .execute(sql, rusqlite::params_from_iter(params.iter()))
                .map(|_| ())
                .map_err(|e| e.to_string())
        }
        async fn query_i64(&self, sql: &str, binds: Vec<QVal>) -> Result<Option<i64>, String> {
            use rusqlite::OptionalExtension;
            let params: Vec<rusqlite::types::Value> = binds.iter().map(to_sql).collect();
            self.conn
                .query_row(sql, rusqlite::params_from_iter(params.iter()), |r| r.get::<_, Option<i64>>(0))
                .optional()
                .map(|o| o.flatten())
                .map_err(|e| e.to_string())
        }
        async fn between_passes(&self) {
            if let Some(f) = self.late.borrow_mut().take() {
                f(&self.conn);
            }
        }
    }

    /// bsv-low loop 18 (pair 11's JOIN `3b14f0e6…`), pinned under the REAL passes: rows the subject's own
    /// admission write lands AFTER the first pass survive it (the pre-fix end state, reproduced with the
    /// verification pass switched off); the verification pass takes them, notes their outpoints, and the
    /// eviction ends complete.
    #[tokio::test]
    async fn the_verification_pass_takes_a_row_that_lands_between_the_passes_under_the_real_logic() {
        let pot = "ab".repeat(32);
        let seed = |conn: &rusqlite::Connection| {
            conn.execute("INSERT INTO outputs (txid, outputIndex, outputScript, topic, satoshis, spent) VALUES (?1, 0, X'51', 'tm_pot', 40000, 0)", [&pot]).unwrap();
            conn.execute("INSERT INTO applied_transactions (txid, topic) VALUES (?1, 'tm_pot')", [&pot]).unwrap();
        };
        // THE RED CONTROL: the pre-fix shape (no verification pass) leaves the late rows served
        let db = SqliteShadow::new(shipped_conn());
        seed(&db.conn);
        let pot_c = pot.clone();
        *db.late.borrow_mut() = Some(Box::new(move |c: &rusqlite::Connection| late_rows(c, &pot_c)));
        let r0 = shadow_move_passes(&db, &pot, "REJECTED", 1_000, true).await;
        assert_eq!(r0.moved, 2);
        assert_eq!(count(&db.conn, "pot_records", "txid", &pot), 1, "the pre-fix end state: a pot the index still knows");
        assert_eq!(count(&db.conn, "applied_transactions", "txid", &pot), 1);
        assert!(r0.pot_vouts.is_empty(), "nothing noted for the survivor");
        // THE FIX: the verification pass takes the survivors and notes the pot's outpoint
        let db = SqliteShadow::new(shipped_conn());
        seed(&db.conn);
        let pot_c = pot.clone();
        *db.late.borrow_mut() = Some(Box::new(move |c: &rusqlite::Connection| late_rows(c, &pot_c)));
        let r1 = shadow_move_passes(&db, &pot, "REJECTED", 1_000, false).await;
        assert_eq!(r1.moved, 4, "both passes' rows");
        assert_eq!(count(&db.conn, "pot_records", "txid", &pot), 0);
        assert_eq!(count(&db.conn, "applied_transactions", "txid", &pot), 0);
        assert_eq!(count(&db.conn, "outputs", "txid", &pot), 0);
        assert_eq!(count(&db.conn, "pot_records_evicted", "txid", &pot), 1);
        assert_eq!(count(&db.conn, "applied_transactions_evicted", "txid", &pot), 2);
        assert_eq!(r1.pot_vouts, vec![0], "the survivor's outpoint is noted (the gate's MEDIUM-1)");
        assert!(r1.incomplete.is_empty(), "complete: {:?}", r1.incomplete);
        assert!(r1.retried.is_empty());
        // and the restore brings everything back once
        for t in ["pot_records", "applied_transactions", "outputs"] {
            let c = cols(&db.conn, t);
            let (ins, del) = restore_sql(t, "txid", &c);
            db.conn.execute(&ins, [&pot]).unwrap();
            db.conn.execute(&del, [&pot]).unwrap();
        }
        assert_eq!(count(&db.conn, "pot_records", "txid", &pot), 1);
        assert_eq!(count(&db.conn, "applied_transactions", "txid", &pot), 2);
        assert_eq!(count(&db.conn, "outputs", "txid", &pot), 1);
        assert_eq!(count(&db.conn, "pot_records_evicted", "txid", &pot), 0);
    }
    fn late_rows(conn: &rusqlite::Connection, pot: &str) {
        conn.execute("INSERT INTO pot_records (txid, outputIndex, spent, createdAt, lockKind) VALUES (?1, 0, 0, 1789959998, 'covenant')", [pot]).unwrap();
        conn.execute("INSERT INTO applied_transactions (txid, topic) VALUES (?1, 'tm_lowfund')", [pot]).unwrap();
    }

    /// The gate's LOW-2: a first-pass fault the verification pass HEALS is said (`retried`), not alarmed
    /// (`incomplete`); a fault the verification pass cannot heal is incomplete.
    #[tokio::test]
    async fn a_first_pass_fault_healed_by_the_verification_pass_is_retried_not_incomplete() {
        let pot = "ab".repeat(32);
        let db = SqliteShadow::new(shipped_conn());
        db.conn.execute("INSERT INTO outputs (txid, outputIndex, outputScript, topic, satoshis, spent) VALUES (?1, 0, X'51', 'tm_pot', 40000, 0)", [&pot]).unwrap();
        db.conn.execute("INSERT INTO pot_records (txid, outputIndex, spent, createdAt, lockKind) VALUES (?1, 0, 0, 1789959998, 'covenant')", [&pot]).unwrap();
        *db.fail_first_copy_of.borrow_mut() = Some("pot_records".to_string());
        let r = shadow_move_passes(&db, &pot, "REJECTED", 1_000, false).await;
        assert_eq!(count(&db.conn, "pot_records", "txid", &pot), 0, "healed by the verification pass");
        assert_eq!(r.moved, 2);
        assert_eq!(r.retried.len(), 1, "{:?}", r.retried);
        assert!(r.retried[0].starts_with("pot_records by txid: first pass copy: simulated"), "{:?}", r.retried);
        assert!(r.incomplete.is_empty(), "{:?}", r.incomplete);
        assert_eq!(r.pot_vouts, vec![0]);
    }

    /// The gate's LOW-4 under real SQLite: a readmission stamped AFTER the pass's own stamp is kept by the close
    /// (and re-counted as fresh by the next pass), while an older readmission is re-opened.
    #[test]
    fn a_readmission_that_lands_under_the_pass_is_kept_by_the_close() {
        use rusqlite::OptionalExtension;
        let conn = shipped_conn();
        let pot = "ab".repeat(32);
        conn.execute(OPEN_EVICTION_MARKER_SQL, rusqlite::params![&pot, "REJECTED (a)", 1_000i64]).unwrap();
        // the chain's word lands under the pass: readmittedAt 1_500 > the pass's 1_000
        conn.execute("UPDATE pot_evictions SET readmittedAt = 1500 WHERE txid = ?1", [&pot]).unwrap();
        let under: Option<i64> = conn
            .query_row(READMITTED_AFTER_SQL, rusqlite::params![&pot, 1_000i64], |r| r.get(0))
            .optional()
            .unwrap();
        assert_eq!(under, Some(1_500), "the pass sees the readmission that landed under it");
        conn.execute(CLOSE_EVICTION_MARKER_SQL, rusqlite::params![&pot, "REJECTED (a)", 1_000i64, 3i64, "[]"]).unwrap();
        let (readmitted, moved): (Option<i64>, i64) = conn
            .query_row("SELECT readmittedAt, rowsMoved FROM pot_evictions WHERE txid = ?1", [&pot], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap();
        assert_eq!(readmitted, Some(1_500), "the close keeps a readmission stamped after the pass");
        assert_eq!(moved, 3);
        let open: Option<String> = conn
            .query_row(OPEN_EVICTION_SQL, [&pot], |r| r.get(0))
            .optional()
            .unwrap();
        assert_eq!(open, None, "not open: the chain's word stands");
        // an OLDER readmission (before the pass's stamp) is re-opened by the close, as before
        conn.execute("UPDATE pot_evictions SET readmittedAt = 900 WHERE txid = ?1", [&pot]).unwrap();
        conn.execute(CLOSE_EVICTION_MARKER_SQL, rusqlite::params![&pot, "REJECTED (b)", 2_000i64, 1i64, "[]"]).unwrap();
        let open: Option<String> = conn
            .query_row(OPEN_EVICTION_SQL, [&pot], |r| r.get(0))
            .optional()
            .unwrap();
        assert_eq!(open.as_deref(), Some("REJECTED (b)"), "re-opened with fresh counts");
        let moved: i64 = conn.query_row("SELECT rowsMoved FROM pot_evictions WHERE txid = ?1", [&pot], |r| r.get(0)).unwrap();
        assert_eq!(moved, 1);
    }

    /// The gate's LOW-1: the released list is a UNION across passes (a re-eviction releasing only hop B keeps hop A).
    #[test]
    fn the_released_list_merges_across_passes() {
        let a = ReleasedSpend { table: "pot_records".into(), txid: "aa".repeat(32), vout: 0 };
        let b = ReleasedSpend { table: "pot_records".into(), txid: "bb".repeat(32), vout: 0 };
        let first = serde_json::to_string(&vec![a.clone()]).unwrap();
        let merged = merge_released(Some(&first), std::slice::from_ref(&b));
        assert_eq!(merged, vec![a.clone(), b.clone()]);
        let again = merge_released(Some(&serde_json::to_string(&merged).unwrap()), std::slice::from_ref(&a));
        assert_eq!(again, vec![a.clone(), b.clone()], "no duplicate");
        assert_eq!(merge_released(None, std::slice::from_ref(&b)), vec![b.clone()]);
        assert_eq!(merge_released(Some("not json"), std::slice::from_ref(&a)), vec![a]);
    }

    /// The gate's NIT: the client-facing line carries the status WORD only (never Arcade's extraInfo).
    #[test]
    fn the_refusal_status_word_is_the_first_token() {
        assert_eq!(refusal_status_word("REJECTED (arcade live REJECTED: UTXO_SPENT (70): x; both indexers absent)"), "REJECTED");
        assert_eq!(refusal_status_word("DOUBLE_SPEND_ATTEMPTED (arcade live …)"), "DOUBLE_SPEND_ATTEMPTED");
        assert_eq!(refusal_status_word(""), "REJECTED");
        assert_eq!(refusal_status_word("(257) already known"), "257");
    }

    /// Round 2 of the loop-18 gate (NEW-1), under the REAL core: a readmission (the chain's word) landing UNDER a
    /// pass — the twins restored and the ledger stamped between pass 1 and the verification pass — is honoured:
    /// the pass's own moves are restored again, the ledger stays readmitted, the tables and the ledger agree. The
    /// control: the passes alone (pre-round-2) leave the restored rows re-moved under a closed ledger.
    #[tokio::test]
    async fn an_eviction_overtaken_by_a_readmission_restores_its_own_moves() {
        let pot = "ab".repeat(32);
        let seed = |conn: &rusqlite::Connection| {
            conn.execute("INSERT INTO outputs (txid, outputIndex, outputScript, topic, satoshis, spent) VALUES (?1, 0, X'51', 'tm_pot', 40000, 0)", [&pot]).unwrap();
            conn.execute("INSERT INTO pot_records (txid, outputIndex, spent, createdAt, lockKind) VALUES (?1, 0, 0, 1789959998, 'covenant')", [&pot]).unwrap();
            conn.execute("INSERT INTO applied_transactions (txid, topic) VALUES (?1, 'tm_pot')", [&pot]).unwrap();
        };
        // the readmission under the pass: restore every twin row, stamp the ledger AFTER the pass's stamp (1_000)
        let readmit_under = |conn: &rusqlite::Connection, pot: &str| {
            for t in ["pot_records", "outputs", "applied_transactions"] {
                let c = cols(conn, t);
                let (ins, del) = restore_sql(t, "txid", &c);
                conn.execute(&ins, [pot]).unwrap();
                conn.execute(&del, [pot]).unwrap();
            }
            conn.execute("UPDATE pot_evictions SET readmittedAt = 1500 WHERE txid = ?1", [pot]).unwrap();
        };
        // THE CONTROL: the passes alone re-move the restored rows
        let db = SqliteShadow::new(shipped_conn());
        seed(&db.conn);
        db.conn.execute(OPEN_EVICTION_MARKER_SQL, rusqlite::params![&pot, "REJECTED", 1_000i64]).unwrap();
        let pot_c = pot.clone();
        *db.late.borrow_mut() = Some(Box::new(move |c: &rusqlite::Connection| readmit_under(c, &pot_c)));
        let r = shadow_move_passes(&db, &pot, "REJECTED", 1_000, false).await;
        assert!(r.moved >= 3);
        assert_eq!(count(&db.conn, "pot_records", "txid", &pot), 0, "the control: the restored rows re-moved");
        // THE FIX: the core checks for the readmission after the passes and restores its own moves
        let db = SqliteShadow::new(shipped_conn());
        seed(&db.conn);
        let pot_c = pot.clone();
        *db.late.borrow_mut() = Some(Box::new(move |c: &rusqlite::Connection| readmit_under(c, &pot_c)));
        let core = evict_core(&db, &pot, "REJECTED", 1_000).await;
        assert_eq!(core.yielded_to_readmission, Some(1_500), "the readmission under the pass was seen");
        assert!(core.restored >= 3, "the pass's own moves restored again: {:?}", core);
        assert!(core.faults.is_empty(), "{:?}", core.faults);
        assert_eq!(count(&db.conn, "pot_records", "txid", &pot), 1);
        assert_eq!(count(&db.conn, "outputs", "txid", &pot), 1);
        assert_eq!(count(&db.conn, "applied_transactions", "txid", &pot), 1);
        assert_eq!(count(&db.conn, "pot_records_evicted", "txid", &pot), 0);
        // the wrapper's close keeps the stamp: the ledger says readmitted and the tables hold the rows
        db.conn.execute(CLOSE_EVICTION_MARKER_SQL, rusqlite::params![&pot, "REJECTED", 1_000i64, core.moved as i64, "[]"]).unwrap();
        let readmitted: Option<i64> = db.conn.query_row("SELECT readmittedAt FROM pot_evictions WHERE txid = ?1", [&pot], |r| r.get(0)).unwrap();
        assert_eq!(readmitted, Some(1_500));
    }

    /// Round 2 (NEW-1): a readmission stamped after the pass's own stamp but BEFORE the passes run (the clock read,
    /// then the marker, then the check): nothing moves — the chain's word already stands.
    #[tokio::test]
    async fn an_eviction_that_starts_under_a_readmission_moves_nothing() {
        let pot = "ab".repeat(32);
        let db = SqliteShadow::new(shipped_conn());
        db.conn.execute("INSERT INTO pot_records (txid, outputIndex, spent, createdAt, lockKind) VALUES (?1, 0, 0, 1789959998, 'covenant')", [&pot]).unwrap();
        db.conn.execute(OPEN_EVICTION_MARKER_SQL, rusqlite::params![&pot, "REJECTED", 900i64]).unwrap();
        db.conn.execute("UPDATE pot_evictions SET readmittedAt = 1200 WHERE txid = ?1", [&pot]).unwrap();
        let core = evict_core(&db, &pot, "REJECTED", 1_000).await;
        assert_eq!(core.yielded_to_readmission, Some(1_200));
        assert_eq!(core.moved, 0);
        assert_eq!(count(&db.conn, "pot_records", "txid", &pot), 1, "untouched");
    }

    /// Round 2 (NEW-2): the restore heals a twin that lacks a column the source gained (a migration between the
    /// eviction and the readmission), is idempotent, and names a faulted table instead of hiding it.
    #[tokio::test]
    async fn restore_twins_heals_the_twin_and_is_fail_loud() {
        let pot = "ab".repeat(32);
        let db = SqliteShadow::new(shipped_conn());
        db.conn.execute("INSERT INTO pot_records (txid, outputIndex, spent, createdAt, lockKind) VALUES (?1, 0, 0, 1789959998, 'covenant')", [&pot]).unwrap();
        let r = shadow_move_passes(&db, &pot, "REJECTED", 1_000, false).await;
        assert_eq!(r.moved, 1);
        // the source gains a column after the eviction
        db.conn.execute_batch("ALTER TABLE pot_records ADD COLUMN loop18_new TEXT").unwrap();
        assert!(!cols(&db.conn, "pot_records_evicted").iter().any(|c| c.name == "loop18_new"));
        let (restored, faults) = restore_twins(&db, &pot).await;
        assert_eq!((restored, faults.len()), (1, 0), "{faults:?}");
        assert!(cols(&db.conn, "pot_records_evicted").iter().any(|c| c.name == "loop18_new"), "the twin healed");
        assert_eq!(count(&db.conn, "pot_records", "txid", &pot), 1);
        assert_eq!(count(&db.conn, "pot_records_evicted", "txid", &pot), 0);
        // idempotent
        let (again, faults) = restore_twins(&db, &pot).await;
        assert_eq!((again, faults.len()), (0, 0));
        // a faulted copy is named (the twin dropped from under the restore)
        let r = shadow_move_passes(&db, &pot, "REJECTED", 2_000, false).await;
        assert_eq!(r.moved, 1);
        db.conn.execute_batch("DROP TABLE pot_records_evicted").unwrap();
        db.conn.execute_batch("CREATE TABLE pot_records_evicted (af_evictedAt INTEGER NOT NULL, af_reason TEXT NOT NULL, txid TEXT)").unwrap();
        db.conn.execute("INSERT INTO pot_records_evicted (af_evictedAt, af_reason, txid) VALUES (1, 'x', ?1)", [&pot]).unwrap();
        // the twin holds a row but lacks the source's NOT NULL columns: the copy back must fault, named
        let (restored, faults) = restore_twins(&db, &pot).await;
        assert_eq!(restored, 0);
        assert!(
            faults.iter().any(|f| f.starts_with("pot_records by txid: the copy back landed 0 of 1 row(s)")),
            "{faults:?}"
        );
        assert_eq!(count(&db.conn, "pot_records_evicted", "txid", &pot), 1, "the twin keeps the row it could not land");
    }
}
