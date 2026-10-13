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
use overlay_engine::beef_limits;

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

/// P0-2d (bsv-stack-lean #52): through the one verdict function
/// (`overlay_discovery::arcade_words`), the gate's own reading: seen, in a
/// block (STUMP_PROCESSING) or mined triggers the live read; a refusal or any
/// ORPHAN word is corroborated; pending, parked and an undefined word do
/// nothing.
pub fn callback_action(tx_status: &str) -> CallbackAction {
    use overlay_discovery::arcade_words::{arcade_verdict, ArcadeVerdict};
    match arcade_verdict(tx_status) {
        v if v.network_holds() => CallbackAction::LatchSeen,
        ArcadeVerdict::Rejected | ArcadeVerdict::Conflict | ArcadeVerdict::Orphan => {
            CallbackAction::EvidenceCheck(tx_status.trim().to_ascii_uppercase())
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
    /// `PRAGMA table_info`'s `notnull` (round 4 of the loop-18 gate).
    #[serde(default)]
    pub notnull: i64,
    /// `PRAGMA table_info`'s `dflt_value`: the column's DEFAULT as SQL text (round 4). A twin healed with it
    /// materialises the default for its older rows, and the restore coalesces a NOT NULL column onto it, so a
    /// column added after an eviction never lands NULL (which `INSERT OR IGNORE` would drop in silence).
    #[serde(default)]
    pub dflt_value: Option<String>,
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

/// PURE: a default SQLite accepts on `ADD COLUMN` (a number, a quoted string, NULL, or a bare keyword such as
/// CURRENT_TIMESTAMP). An expression default (`datetime('now')`, which PRAGMA hands back without its
/// parentheses) is refused there; the restore's COALESCE still lands it (round 5, N7).
pub fn literal_default(d: &str) -> bool {
    let d = d.trim();
    if d.is_empty() {
        return false;
    }
    if d.eq_ignore_ascii_case("NULL") || d.eq_ignore_ascii_case("CURRENT_TIMESTAMP") {
        return true;
    }
    if d.starts_with('\'') && d.ends_with('\'') && d.len() >= 2 {
        return true;
    }
    d.parse::<f64>().is_ok()
}

/// PURE: a column the source gained after the twin was created, added to the twin with its literal default.
pub fn heal_shadow_sql(table: &str, col: &ColumnInfo) -> String {
    let default = match col.dflt_value.as_deref() {
        Some(d) if literal_default(d) => format!(" DEFAULT {d}"),
        _ => String::new(),
    };
    format!(
        "ALTER TABLE \"{}\" ADD COLUMN \"{}\" {}{default}",
        shadow_table(table),
        col.name,
        if col.ty.is_empty() {
            "TEXT"
        } else {
            col.ty.as_str()
        }
    )
}

/// The restore's SELECT list: a NOT NULL column with a DEFAULT is coalesced onto it (a twin healed before the
/// heal carried defaults holds NULL there; an explicit NULL would be dropped by `INSERT OR IGNORE`).
fn restore_select(cols: &[ColumnInfo]) -> String {
    cols.iter()
        .map(|c| match c.dflt_value.as_deref() {
            Some(d) if c.notnull != 0 && !d.is_empty() => format!("COALESCE(\"{}\", {d})", c.name),
            _ => format!("\"{}\"", c.name),
        })
        .collect::<Vec<_>>()
        .join(", ")
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
    let sel = restore_select(cols);
    (
        format!(
            "INSERT OR IGNORE INTO \"{table}\" ({q}) SELECT {sel} FROM \"{}\" WHERE \"{key}\" = ?",
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

/// PURE (bsv-low #605): write a CONFIRMED competitor as the spender of one outpoint the evicted tx consumed (binds:
/// the competitor txid, the outpoint's txid, its vout, the evicted txid; all lowercase), BEFORE the eviction releases
/// that tx's pointers, so the release (`WHERE spendingTxid = <evicted>`) no longer finds the row and the hop is never
/// served `spent:false, spendingTxid:null` while the pass knows its spender. Guarded: only a row whose pointer is the
/// evicted tx or none, and only an UNCONFIRMED one (a `spentConfirmed = 1` pointer never moves on a courier's word).
/// The pointer is UNCONFIRMED: the conflict pass holds a courier's claimed height, never a verified proof, so the
/// #186 chaser latches it once the competitor's proof verifies (its candidates are `spent = 1, spentConfirmed = 0`
/// rows aged on `spentAt`). The verdict group and the old spender's height and finality are cleared (they were the
/// evicted tx's). `RETURNING 1 AS v` turns a hit into a row.
pub fn record_conflict_spender_sql(cols: &[ColumnInfo]) -> String {
    let has = |n: &str| cols.iter().any(|c| c.name == n);
    let mut sets = vec!["spent = 1".to_string(), "spendingTxid = ?".to_string()];
    for (col, val) in [
        ("spentAt", "unixepoch()"),
        ("firstSpentAt", "COALESCE(firstSpentAt, unixepoch())"),
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
        " AND COALESCE(spentConfirmed, 0) = 0"
    } else {
        ""
    };
    format!(
        "UPDATE pot_records SET {} WHERE txid = ? AND outputIndex = ? AND (spendingTxid = ? OR spendingTxid IS NULL){guard} RETURNING 1 AS v",
        sets.join(", ")
    )
}

/// bsv-low #605: run [`record_conflict_spender_sql`] for one outpoint; `Ok(true)` when the row now names the
/// competitor, `Ok(false)` when no row matched (no such row, a confirmed pointer, or another spender's), `Err` on a
/// read or write fault. The pots room hears the change.
pub async fn record_conflict_spender(
    db: &dyn ShadowDb,
    outpoint_txid: &str,
    vout: u32,
    evicted: &str,
    competitor: &str,
) -> Result<bool, String> {
    let cols = db.columns("pot_records").await?;
    if !cols.iter().any(|c| c.name == "spendingTxid") {
        return Ok(false);
    }
    let outpoint_txid = outpoint_txid.to_ascii_lowercase();
    let hit = db
        .query_i64(
            &record_conflict_spender_sql(&cols),
            vec![
                QVal::Text(competitor.to_ascii_lowercase()),
                QVal::Text(outpoint_txid.clone()),
                QVal::Int(i64::from(vout)),
                QVal::Text(evicted.to_ascii_lowercase()),
            ],
        )
        .await?
        .is_some();
    if hit {
        crate::pot_changes::note(&outpoint_txid, vout);
    }
    Ok(hit)
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
     ON CONFLICT(txid) DO UPDATE SET \
     reason = CASE WHEN pot_evictions.readmittedAt IS NOT NULL AND pot_evictions.readmittedAt > ?3 THEN pot_evictions.reason ELSE excluded.reason END, \
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
     ON CONFLICT(txid) DO UPDATE SET \
     reason = CASE WHEN pot_evictions.readmittedAt IS NOT NULL AND pot_evictions.readmittedAt > ?3 THEN pot_evictions.reason ELSE excluded.reason END, \
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

/// THE PRE-WRITE GUARD over what the engine is about to LAND first from a submit's BEEF (bsv-low #575, the E1D
/// delta-2 fold, L2; installed as `Engine::set_landing_guard` at every engine this worker builds): the subject's
/// ungated arms refuse an evicted subject before their write, and a landing is refused the same way, at every door.
/// An OPEN eviction of the body refuses it (counted, logged): nothing of it is written, no lookup service is told,
/// and the successor's topic answers "not now" (the S2 queued ack; its replays ask again). An unreadable ledger
/// refuses too (counted, fail-loud: a landing nobody can clear is not made blind).
///
/// A GATED door's accept of the successor does NOT readmit the predecessor's open row (unlike the subject's own,
/// readmitted on that same accept): the accept is the network's word on the SUCCESSOR, whose EF carries the
/// predecessor's outputs and not the predecessor, and an accept can be a pending one Arcade later reports
/// `SEEN_IN_ORPHAN_MEMPOOL` (the pending watch keeps watching an orphan): it does not say the network holds the
/// predecessor. Its row is readmitted by its OWN word (its submit at the gated door, a MINED proof, the operator's
/// `/admin/readmit`); the successor converges on a replay after that, or waits for its dead letter's re-drive.
pub async fn landing_guard(db: &D1Database, txid: &str, topic: &str) -> Result<(), String> {
    match open_eviction(db, txid).await {
        Ok(None) => Ok(()),
        Ok(Some(ev)) => {
            crate::ops::bump_counter(db, crate::ops::COUNTER_LANDING_REFUSED_EVICTED, 1).await;
            worker::console_log!(
                "{txid}, carried by a successor into {topic}, is under an OPEN eviction (at {} ms — {}): not landed, the successor waits",
                ev.evicted_at_ms,
                ev.reason
            );
            Err(format!(
                "under an open eviction ({})",
                refusal_status_word(&ev.reason)
            ))
        }
        Err(e) => {
            crate::ops::bump_counter(db, crate::ops::COUNTER_ADMIT_FAST_LEDGER_UNREADABLE, 1).await;
            worker::console_log!(
                "the eviction ledger could not be read for {txid}, carried by a successor into {topic} ({e}): not landed"
            );
            Err(format!("the eviction ledger could not be read ({e})"))
        }
    }
}

/// THE WRITE-SIDE GUARD over what the engine LANDED first from a submit's BEEF (lane E1D's delta fold, L2;
/// `MutationReport::landed_predecessors`): each landed predecessor is an admission write of its own, which no
/// caller asked for, so it is asked AFTER the write too, as every admission writer's write is (the belt behind
/// [`landing_guard`], which refused every row open before the landing): a landed txid under an OPEN eviction is
/// re-evicted (what the landing wrote moves to the twins), counted
/// and logged. An unreadable ledger is counted and logged (fail-loud) and changes no answer: a retry would not ask
/// again (the subject's replay dedups and lands nothing). It never returns early, so the pot notes of a re-eviction
/// ride the caller's own exit flush (the dispatch's one exit; the queue batch's end).
pub async fn guard_landed(db: &D1Database, landed: &[(String, String)], origin: &str) {
    for (txid, topic) in landed {
        match open_eviction(db, txid).await {
            Ok(Some(ev)) => {
                let now_ms = worker::Date::now().as_millis();
                let outcome = evict_txid_everywhere(db, txid, &ev.reason, now_ms).await;
                crate::ops::bump_counter(db, crate::ops::COUNTER_ADMIT_FAST_REEVICTED_AFTER_WRITE, 1).await;
                worker::console_log!(
                    "{origin}: {txid}, landed first into {topic} from a successor's BEEF, is under an OPEN eviction (at {} ms — {}): re-evicted {} row(s)",
                    ev.evicted_at_ms,
                    ev.reason,
                    outcome.moved
                );
            }
            Ok(None) => {}
            Err(e) => {
                crate::ops::bump_counter(db, crate::ops::COUNTER_ADMIT_FAST_LEDGER_UNREADABLE, 1).await;
                worker::console_log!(
                    "{origin}: the eviction ledger could not be read for {txid}, landed first into {topic} ({e}); not asked again"
                );
            }
        }
    }
}

/// The refused verdict memo for a txid (round 5 of the gate, N1): the door's readmission deletes the memo FIRST, so
/// a `refused` row present after the write was written by a pass that completed after the door — the network's
/// fresh word, never the door's stale one. `Err` when the memo could not be read.
pub async fn refused_memo_exists(db: &D1Database, txid: &str) -> Result<bool, String> {
    let v = db
        .query_i64(
            "SELECT 1 AS v FROM tx_any_verdicts WHERE txid = ?1 AND kind = 'refused'",
            vec![QVal::Text(txid.to_ascii_lowercase())],
        )
        .await?;
    Ok(v.is_some())
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
/// back (`INSERT OR IGNORE`: idempotent) over the source columns the twin holds (a column added after the eviction
/// takes its DEFAULT) and dropped from the twin; fail-LOUD per table (the faults returned, never a silent partial
/// restore that stamps the ledger). Used by the readmission and by an eviction pass a readmission overtook (its
/// own moves undone: the chain's word stands).
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
        // the restore names the SOURCE columns the twin actually holds (round 3 of the gate): a column the source
        // gained after the eviction is omitted, so SQLite applies its DEFAULT instead of the NULL a healed twin
        // would have supplied — `INSERT OR IGNORE` would have dropped that row in silence
        let restore_cols: Vec<ColumnInfo> = cols
            .iter()
            .filter(|c| twin_cols.iter().any(|t| t.name == c.name))
            .cloned()
            .collect();
        for key in keys.iter() {
            if !restore_cols.iter().any(|c| c.name == *key) {
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
            let (ins, del) = restore_sql(table, key, &restore_cols);
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
    /// The yield's OWN restore faults (a subset of `faults`): the wrapper re-opens the row only for these, and only
    /// while the twins still hold the txid (round 4 of the gate).
    pub restore_faults: Vec<String>,
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
            core.restore_faults = faults.iter().map(|f| format!("yielding to the readmission: {f}")).collect();
            core.faults.extend(core.restore_faults.iter().cloned());
            core.yielded_to_readmission = Some(at);
        }
        Ok(None) => {}
        Err(e) => core.faults.push(format!("the readmission read faulted after the passes ({e})")),
    }
    core
}

/// What an eviction did, for its callers.
pub struct EvictionOutcome {
    /// Rows moved by the passes.
    pub moved: u64,
    /// The pass yielded to a readmission (the chain's word stands; the pot is served): the callers write no
    /// `refused` memo over it and count no eviction (round 3 of the gate).
    pub yielded: bool,
}

/// The rows the twins still hold for a txid (D1): the re-open gate (round 4 of the gate). `(count, unreadable)`.
async fn twin_rows_of(db: &D1Database, txid: &str) -> (u64, bool) {
    let mut total = 0u64;
    let mut unreadable = false;
    for (table, keys) in MOVED_TABLES {
        let twin = shadow_table(table);
        let cols = match table_columns_checked(db, &twin).await {
            Ok(c) => c,
            Err(_) => {
                unreadable = true;
                continue;
            }
        };
        if cols.is_empty() {
            continue;
        }
        for key in keys.iter() {
            if !cols.iter().any(|c| c.name == *key) {
                continue;
            }
            match count_keyed_checked(db, &twin, key, txid).await {
                Ok(n) => total += n,
                Err(_) => unreadable = true,
            }
        }
    }
    (total, unreadable)
}

/// Evict every row the txid owns, everywhere, into the twins; note the pot and
/// lobby changes so the seats and the lobby learn; record the ledger row.
/// Returns what it did (`EvictionOutcome`).
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
pub async fn evict_txid_everywhere(
    db: &D1Database,
    txid: &str,
    reason: &str,
    asked_ms: u64,
) -> EvictionOutcome {
    evict_txid_everywhere_repointed(db, txid, reason, asked_ms, &[]).await
}

/// [`evict_txid_everywhere`] for the conflict pass (bsv-low #605, the E606 lens's M1): `repointed` are the outpoints
/// the pass wrote the confirmed competitor on BEFORE this eviction (`record_conflict_spender`), so the release no
/// longer finds them; they are merged into the ledger's `releasedSpends` beside the released ones, because the app
/// layer keys an evicted game's home on exactly that list (`owed.rs` `evicted_pot_homes`). The readmission's re-mark
/// of such an entry is a guarded no-op (`remark_pot_spend_sql`: `spendingTxid IS NULL`; the pointer is the
/// competitor's); the way back is the chaser's displacement.
pub async fn evict_txid_everywhere_repointed(
    db: &D1Database,
    txid: &str,
    reason: &str,
    asked_ms: u64,
    repointed: &[ReleasedSpend],
) -> EvictionOutcome {
    let txid = txid.to_ascii_lowercase();
    // THE STAMP POLICY (round 4 of the gate): the ledger's stamp is `asked_ms`, the moment the refusal's EVIDENCE
    // began (the callback's arrival, the watch's look) — a readmission whose stamp is newer than that (the
    // readmission stamps with a fresh clock at its own UPDATE) may have landed while the evidence was being
    // gathered, and the evidence then predates the chain's word: the pass yields. A readmission older than the
    // evidence's start is a genuine earlier readmission followed by a fresh corroborated refusal: re-opened.
    let now_ms = asked_ms;
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
    // the lobby learns an EVICTION only when one stands (round 3 of the gate); a pass that yielded restored the
    // adverts it had moved, so the lobby learns them ADMITTED again (round 4)
    if core.yielded_to_readmission.is_none() {
        for v in &advert_vouts {
            crate::lobby_changes::note_evicted(&txid, *v);
        }
    } else {
        for v in &advert_vouts {
            crate::lobby_changes::note_admitted(&txid, *v);
        }
    }
    // the spends it left on the rows it consumed (its hops, its pot) — released, and MERGED with the ledger's
    // list; NOT when the pass yielded to a readmission (the readmission re-marked them: they stand)
    let mut released = if core.yielded_to_readmission.is_some() {
        Vec::new()
    } else {
        release_spends_of(db, &txid).await
    };
    released.extend(repointed.iter().cloned());
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
        // round 3 (residual B) narrowed by round 4: a yield whose OWN restore faulted may have left rows in the
        // twins under a KEPT stamp — never a closed ledger over twin rows. Re-open ONLY then, and only while the
        // twins still hold the txid (a spurious open row reads as a refused JOIN on the owed list): the next
        // accept or proof runs the restore again
        if !core.restore_faults.is_empty() {
            let (in_twins, twins_unreadable) = twin_rows_of(db, &txid).await;
            if in_twins > 0 || twins_unreadable {
                if let Err(e) = Query::new("UPDATE pot_evictions SET readmittedAt = NULL WHERE txid = ?")
                    .bind(txid.as_str())
                    .execute(db)
                    .await
                {
                    worker::console_log!("[admit-fast] evict {txid}: re-opening after a faulted yield failed ({e})");
                }
                crate::ops::bump_counter(db, crate::ops::COUNTER_ADMIT_FAST_READMIT_INCOMPLETE, 1).await;
                worker::console_log!(
                    "[admit-fast] evict {txid}: the yield's restore faulted with {in_twins} row(s) still in the twins (unreadable: {twins_unreadable}) — the row re-opened"
                );
            }
        }
    }
    if !core.faults.is_empty() {
        crate::ops::bump_counter(db, crate::ops::COUNTER_ADMIT_FAST_EVICT_INCOMPLETE, 1).await;
        worker::console_log!(
            "[admit-fast] evict {txid} INCOMPLETE ({} fault(s): {}) — {}",
            core.faults.len(),
            core.faults.join("; "),
            if core.yielded_to_readmission.is_some() && core.restore_faults.is_empty() {
                "the ledger keeps the readmission (the pass yielded); the faults are the pass's own"
            } else {
                "the row is OPEN; the next accept, proof or eviction converges"
            }
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
        "[admit-fast] evicted {txid} everywhere: {moved} row(s) moved, {} spend pointer(s) released ({reason}; stamped {asked_ms} ms, the evidence's start)",
        released.len()
    );
    EvictionOutcome {
        moved,
        yielded: core.yielded_to_readmission.is_some(),
    }
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
    // bsv-low #486 (lens fold): the spends are back on the hops, so the door's refusal of an earlier copy is stale
    crate::submit_refusals::retire_admitted(db, &txid).await;
    // the stamp is THIS moment's clock (round 3, residual A): a pass whose clock read predates this restore but
    // whose marker lands after it must read the readmission as newer
    let stamp_ms = worker::Date::now().as_millis();
    if let Err(e) = Query::new("UPDATE pot_evictions SET readmittedAt = ? WHERE txid = ? AND readmittedAt IS NULL")
        .bind(QVal::Int(stamp_ms as i64))
        .bind(txid.as_str())
        .execute(db)
        .await
    {
        worker::console_log!("[admit-fast] readmit {txid}: the stamp failed ({e}) — the row stays open (the rows are back; the next accept or proof stamps it)");
        crate::ops::bump_counter(db, crate::ops::COUNTER_ADMIT_FAST_READMIT_INCOMPLETE, 1).await;
        return false;
    }
    // the belt (NEW-1, the other direction): a pass that opened DURING the restore may have moved the restored
    // rows again before the stamp — the pass's own post-check restores them, and so does this one (idempotent).
    // Gated on the row still carrying THIS stamp (round 5, N2): a pass whose evidence began after this stamp
    // re-opened the row on purpose (a fresh refusal owns the rows); the count is the checked one (N3).
    let (in_twins, twins_unreadable) = twin_rows_of(db, &txid).await;
    let mut again = 0u64;
    if in_twins > 0 || twins_unreadable {
        let still_mine = db
            .query_i64(
                "SELECT readmittedAt AS v FROM pot_evictions WHERE txid = ?1 AND readmittedAt = ?2",
                vec![QVal::Text(txid.clone()), QVal::Int(stamp_ms as i64)],
            )
            .await;
        match still_mine {
            Ok(Some(_)) => {}
            Ok(None) => {
                worker::console_log!(
                    "[admit-fast] readmit {txid}: {in_twins} twin row(s) under the restore but the row no longer carries this readmission's stamp (a newer pass re-opened it) — the belt stands down; that pass owns the rows"
                );
                return true;
            }
            Err(e) => {
                worker::console_log!("[admit-fast] readmit {txid}: the belt's ledger read faulted ({e}) — the belt runs");
            }
        }
        let (r2, f2) = restore_twins(db, &txid).await;
        again = r2;
        worker::console_log!(
            "[admit-fast] readmit {txid}: {in_twins} twin row(s) reappeared under the restore (a concurrent pass) — restored again ({r2}; {} fault(s))",
            f2.len()
        );
        for v in vouts_of(db, "pot_records", &txid).await {
            crate::pot_changes::note(&txid, v);
        }
        for v in vouts_of(db, "low_records", &txid).await {
            crate::lobby_changes::note_admitted(&txid, v);
        }
        // the pointers a concurrent pass released after this readmission's re-mark (its close merged them into the
        // ledger's list): re-marked again, idempotent (round 5, N4)
        if let Ok(Some(ledger)) = Query::new(RELEASED_SPENDS_SQL)
            .bind(txid.as_str())
            .fetch_optional::<ReleasedJsonRow>(db)
            .await
        {
            let list: Vec<ReleasedSpend> = ledger
                .released_spends
                .as_deref()
                .and_then(|j| serde_json::from_str(j).ok())
                .unwrap_or_default();
            let n = remark_spends(db, &txid, &list, now_ms).await;
            worker::console_log!("[admit-fast] readmit {txid}: {n}/{} spend pointer(s) re-marked again after the belt", list.len());
        }
        if !f2.is_empty() {
            // round 3 (residual B), narrowed by round 4: never a stamped readmission over twin rows — re-open
            // only while the twins still hold the txid, count, the next pass restores
            let (left, unreadable) = twin_rows_of(db, &txid).await;
            if left > 0 || unreadable {
                if let Err(e) = Query::new("UPDATE pot_evictions SET readmittedAt = NULL WHERE txid = ?")
                    .bind(txid.as_str())
                    .execute(db)
                    .await
                {
                    worker::console_log!("[admit-fast] readmit {txid}: re-opening after the belt's faulted restore failed ({e})");
                }
                crate::ops::bump_counter(db, crate::ops::COUNTER_ADMIT_FAST_READMIT_INCOMPLETE, 1).await;
                return false;
            }
        }
    }
    worker::console_log!(
        "[admit-fast] READMITTED {txid} on the pushed proof: {restored} row(s) restored (+{again} under the belt), {remarked}/{} spend pointer(s) re-marked (was evicted: {})",
        released.len(),
        row.reason
    );
    true
}

/// #602: indexer confirmation is distinct from a broadcaster's stored echo.
#[async_trait(?Send)]
pub trait ConflictPresence {
    async fn presence(&self, txid: &str) -> Option<crate::proof_fetcher::NetworkPresence>;
}

pub struct CourierConflictPresence {
    pub woc_api_key: Option<String>,
}

#[async_trait(?Send)]
impl ConflictPresence for CourierConflictPresence {
    async fn presence(&self, txid: &str) -> Option<crate::proof_fetcher::NetworkPresence> {
        use crate::proof_fetcher::*;
        let b = bitails_presence(DEFAULT_BITAILS_BASE, txid).await;
        let w = woc_presence(DEFAULT_WOC_BASE, self.woc_api_key.as_deref(), txid).await;
        Some(classify_presence(b, w))
    }
}

/// What a conflict eviction did (bsv-low #649 M2): the pass either evicted, or YIELDED to a readmission that
/// landed under it (the chain's word stands, the pot is served). A yielded pass is neither logged nor counted as an
/// eviction: it does not stand.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictEvictOutcome {
    Evicted,
    Yielded,
}

/// The mutation port: the existing reversible eviction, and (bsv-low #605) the competitor written as the spender of
/// the outpoint it took, before the eviction releases that outpoint's pointer.
#[async_trait(?Send)]
pub trait ConflictEviction {
    /// `repointed`: the outpoints [`Self::record_conflict_spender`] moved to the competitor, which the ledger's
    /// `releasedSpends` must still name (the E606 lens's M1). Answers whether the eviction stands or yielded
    /// (bsv-low #649 M2).
    async fn evict_conflict(
        &self,
        txid: &str,
        reason: &str,
        now_ms: u64,
        repointed: &[ReleasedSpend],
    ) -> Result<ConflictEvictOutcome, String>;
    async fn record_conflict_spender(
        &self,
        outpoint_txid: &str,
        vout: u32,
        evicted: &str,
        competitor: &str,
    ) -> Result<bool, String>;
}

#[async_trait(?Send)]
impl ConflictEviction for D1Database {
    async fn record_conflict_spender(
        &self,
        outpoint_txid: &str,
        vout: u32,
        evicted: &str,
        competitor: &str,
    ) -> Result<bool, String> {
        record_conflict_spender(self, outpoint_txid, vout, evicted, competitor).await
    }
    async fn evict_conflict(
        &self,
        txid: &str,
        reason: &str,
        now_ms: u64,
        repointed: &[ReleasedSpend],
    ) -> Result<ConflictEvictOutcome, String> {
        let result = evict_txid_everywhere_repointed(self, txid, reason, now_ms, repointed).await;
        if result.yielded {
            // bsv-low #649 M2: the yield is the caller's to know (no eviction log, no eviction counted)
            return Ok(ConflictEvictOutcome::Yielded);
        }
        write_refused_verdict(self, txid, now_ms as i64, reason).await;
        Ok(ConflictEvictOutcome::Evicted)
    }
}

/// Walk the subject's actual inputs. A spender hint alone is never a refusal:
/// its content-addressed raw must consume the same outpoint and an indexer must
/// name a positive mined height. Faults leave the pot available for a later pass.
pub async fn converge_confirmed_conflict(
    store: &dyn ConflictEviction,
    fetcher: &dyn overlay_engine::gasp::AncestorFetcher,
    presence: &dyn ConflictPresence,
    txid: &str,
    raw: &[u8],
    now_ms: u64,
) -> Result<bool, String> {
    use crate::proof_fetcher::NetworkPresence;
    let subject = bsv_rs::transaction::Transaction::from_binary(raw).map_err(|e| e.to_string())?;
    if !subject.id().eq_ignore_ascii_case(txid) {
        return Err("subject raw txid mismatch".into());
    }
    let mut fault = None;
    // Rotate the first input so a bounded pass cannot permanently starve a
    // later input of a large subject. An unavailable input cannot hide another.
    let start = (now_ms as usize) % subject.inputs.len().max(1);
    for input in subject
        .inputs
        .iter()
        .cycle()
        .skip(start)
        .take(subject.inputs.len())
    {
        let Some(parent) = input.source_txid.as_deref() else {
            continue;
        };
        let vout = input.source_output_index;
        let competitor = match fetcher.resolve_spender(parent, vout).await {
            Ok(Some(txid)) => txid,
            Ok(None) => continue,
            Err(e) => {
                fault = Some(e);
                continue;
            }
        };
        if competitor.eq_ignore_ascii_case(txid) {
            continue;
        }
        let bound_raw = match fetcher.spender_binding_raw(&competitor, parent, vout).await {
            Ok(Some(raw)) => raw,
            Ok(None) => continue,
            Err(e) => {
                fault = Some(e);
                continue;
            }
        };
        let bound =
            bsv_rs::transaction::Transaction::from_hex(&bound_raw).map_err(|e| e.to_string())?;
        if !bound.id().eq_ignore_ascii_case(&competitor)
            || !crate::proof_fetcher::tx_consumes_outpoint(&bound_raw, parent, vout)?
        {
            continue;
        }
        // #602 fold (LOW-3, stated): the courier's `blockheight` is trusted as the
        // chain's word, unverified against chaintracks; an orphaned block's height
        // would evict a live pot once, and the pot's pushed MINED proof readmits it.
        if let Some(NetworkPresence::Present {
            height: Some(height),
        }) = presence.presence(&competitor).await
        {
            if height == 0 {
                continue;
            }
            // bsv-low #605: the competitor is the outpoint's spender, written BEFORE the eviction releases the
            // evicted tx's pointers (the release then passes this row by); a fault writes nothing and evicts nothing,
            // the next pass asks again. A miss (no such row, e.g. the JOIN's plain wallet funding; a confirmed
            // pointer; another spender's) leaves the eviction to go on as before. The re-pointed outpoint is handed
            // to the eviction so its ledger still names it (the E606 lens's M1). Stated, filed: a write of the
            // evicted tx's pointer landing between this re-point and the release (milliseconds; `mark_spent_sql`
            // has no from-guard) resets the hop, which the release then nulls (the repair pass's class, bsv-low
            // #607); and the stale mark of a hop whose first judgement had no bytes (bsv-low #608).
            let repointed = if store
                .record_conflict_spender(parent, vout, txid, &competitor)
                .await?
            {
                vec![ReleasedSpend {
                    table: "pot_records".into(),
                    txid: parent.to_ascii_lowercase(),
                    vout,
                }]
            } else {
                Vec::new()
            };
            let outcome = store
                .evict_conflict(
                    txid,
                    &format!("input {parent}:{vout} spent by confirmed competitor {competitor} at {height}"),
                    now_ms,
                    &repointed,
                )
                .await?;
            if outcome == ConflictEvictOutcome::Yielded {
                // bsv-low #649 M2: overtaken by a readmission; the eviction does not stand, so it is neither logged
                // as one nor counted (the wrapper counted the yield itself, `admit_fast_evict_yielded_total`)
                crate::proof_fetcher::push_log(&format!(
                    "[conflict] {txid}: eviction YIELDED to a readmission, {parent}:{vout} taken by {competitor} at {height}; not counted"
                ));
                return Ok(false);
            }
            // one line per eviction (the lens's L2: the miss was logged on its own for every plain funding input)
            crate::proof_fetcher::push_log(&format!(
                "[conflict] {txid}: evicted, {parent}:{vout} taken by {competitor} at {height}; re-pointed: {}",
                if repointed.is_empty() { "no (no unconfirmed pot row named the evicted tx or none)" } else { "yes" }
            ));
            return Ok(true);
        }
    }
    match fault {
        Some(e) => Err(e),
        None => Ok(false),
    }
}

/// Includes already-retired and readmitted pots: proof retirement must not
/// remove an unconfirmed live pot from the conflict re-check population.
pub const CONFLICT_CANDIDATES_SQL: &str = "SELECT b.txid, hex(b.beef) AS beef FROM pot_beefs b \
     WHERE b.proof_verified = 0 AND EXISTS (SELECT 1 FROM pot_records p WHERE p.txid = b.txid) \
     ORDER BY RANDOM() LIMIT 8";

/// #602 fold (MEDIUM-1): what one conflict pass did, counted and on the tick's line.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ConflictPassSummary {
    pub scanned: u64,
    pub evicted: u64,
    pub faults: u64,
}

impl ConflictPassSummary {
    /// One candidate's re-check: `Ok(true)` evicted, `Err` a courier or parse fault.
    pub fn note(&mut self, result: &Result<bool, String>) {
        self.scanned += 1;
        match result {
            Ok(true) => self.evicted += 1,
            Ok(false) => {}
            Err(_) => self.faults += 1,
        }
    }

    /// The counters this pass bumps (served from 0 in `read_counters`).
    pub fn counter_deltas(&self) -> [(&'static str, u64); 3] {
        [
            (
                crate::ops::COUNTER_ADMIT_FAST_CONFLICT_SCANNED,
                self.scanned,
            ),
            (
                crate::ops::COUNTER_ADMIT_FAST_CONFLICT_EVICTED,
                self.evicted,
            ),
            (crate::ops::COUNTER_ADMIT_FAST_CONFLICT_FAULTS, self.faults),
        ]
    }
}

/// One conflict pass's world, candidate by candidate (bsv-low #649 H1): a port so the pins run the shipped
/// [`run_conflict_candidates`] over a fake whose second candidate's courier never answers.
pub(crate) trait ConflictPassPort {
    /// Re-check one candidate (its stored BEEF read, then [`converge_confirmed_conflict`]): `Ok(true)` evicted.
    async fn check(&self, txid: &str, beef: Option<String>) -> Result<bool, String>;
    /// Persist one candidate's counter deltas (the durable `ops_counters` rows).
    async fn persist(&self, deltas: &ConflictPassSummary);
    /// bsv-low #649 L1 (the delta lens): the pass's DELIVERY of the notes its evictions made, its one delivery
    /// await and its LAST effect: nothing is persisted after it (each candidate's deltas were persisted before
    /// the next candidate's courier).
    async fn flush(&self);
}

/// THE LOOP of a conflict pass (bsv-low #649 H1): each candidate's deltas are PERSISTED once it completes, before
/// the next candidate's courier calls start. Until the fold the pass bumped its counters once after the loop, so an
/// eviction already made (the shadow move, the memo, the log) waited for every later candidate's network calls,
/// which have no timeout of their own: a task cancelled there lost `admit_fast_conflict_evicted_total` for a pot
/// already out of the candidate set (it is never selected again), and an eviction not counted reads as none.
pub(crate) async fn run_conflict_candidates<P: ConflictPassPort>(
    port: &P,
    rows: Vec<(String, Option<String>)>,
) -> ConflictPassSummary {
    let mut summary = ConflictPassSummary::default();
    for (txid, beef) in rows {
        let result = port.check(&txid, beef).await;
        let mut one = ConflictPassSummary::default();
        one.note(&result);
        port.persist(&one).await;
        summary.scanned += one.scanned;
        summary.evicted += one.evicted;
        summary.faults += one.faults;
    }
    port.flush().await;
    summary
}

/// The worker's [`ConflictPassPort`]: D1, the courier fetcher (its 24-call budget shared by the pass) and presence.
struct WorkerConflictPass<'a> {
    db: &'a D1Database,
    fetcher: crate::proof_fetcher::ChainProofFetcher,
    presence: CourierConflictPresence,
}

impl ConflictPassPort for WorkerConflictPass<'_> {
    async fn check(&self, txid: &str, beef: Option<String>) -> Result<bool, String> {
        let Some(raw) =
            crate::d1::beef_of_hex_column(beef).and_then(|b| crate::ef::proven_subject_raw(&b))
        else {
            // #602 delta LOW-B: a pot the pass cannot read is a fault, never skipped unseen.
            worker::console_log!(
                "[conflict] {} stored BEEF does not read, retried next pass",
                txid.get(..16).unwrap_or(txid)
            );
            return Err("stored BEEF does not read".into());
        };
        let result = converge_confirmed_conflict(
            self.db,
            &self.fetcher,
            &self.presence,
            txid,
            &raw,
            worker::Date::now().as_millis(),
        )
        .await;
        if let Err(e) = &result {
            worker::console_log!("[conflict] {txid} re-check failed, retried next pass: {e}");
        }
        result
    }
    async fn persist(&self, deltas: &ConflictPassSummary) {
        bump_conflict_counters(self.db, deltas).await;
    }
    /// The pass's notes are delivered by its caller's own flush at its end (`lib.rs`: the scheduled tick's last
    /// `pot_changes::flush`, and the dispatch's one exit for `/admin/complete-proofs`); this pass holds no `Env`.
    async fn flush(&self) {}
}

pub async fn confirmed_conflict_pass(
    db: &D1Database,
    woc_api_key: Option<&str>,
) -> ConflictPassSummary {
    #[derive(serde::Deserialize)]
    struct Candidate {
        txid: String,
        beef: Option<String>,
    }
    let rows = match Query::new(CONFLICT_CANDIDATES_SQL)
        .fetch_all::<Candidate>(db)
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            worker::console_log!("[conflict] candidate read failed: {e}");
            let summary = ConflictPassSummary {
                faults: 1,
                ..Default::default()
            };
            bump_conflict_counters(db, &summary).await;
            return summary;
        }
    };
    let port = WorkerConflictPass {
        db,
        fetcher: crate::proof_fetcher::ChainProofFetcher::new(None)
            .with_woc_api_key(woc_api_key.map(str::to_owned))
            .with_budget(24),
        presence: CourierConflictPresence {
            woc_api_key: woc_api_key.map(str::to_owned),
        },
    };
    run_conflict_candidates(&port, rows.into_iter().map(|r| (r.txid, r.beef)).collect()).await
}

/// The pass's counters, a zero delta skipped (bsv-low #649 H1: written once per candidate, so a write per counter
/// that moved, at most two a candidate).
async fn bump_conflict_counters(db: &D1Database, summary: &ConflictPassSummary) {
    for (name, delta) in summary.counter_deltas() {
        if delta > 0 {
            crate::ops::bump_counter(db, name, delta).await;
        }
    }
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
            match crate::proof_fetcher::classify_presence(bitails.map(crate::proof_fetcher::bool_presence), woc.map(crate::proof_fetcher::bool_presence)) {
                NetworkPresence::Present { .. } => EvidenceVerdict::Present,
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
    evidence_verdict(
        &look,
        bitails.map(crate::proof_fetcher::presence_bool),
        woc.map(crate::proof_fetcher::presence_bool),
        young,
    )
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
        // #519 (the delta-verify's observation): an EVICTED subject's outputs live in the twin with the admission's
        // own stamp — without this rung a hop or a marker the eviction moved read "young" forever at the door
        _ => match Query::new("SELECT MIN(score) AS s FROM outputs_evicted WHERE txid = ?")
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
        },
    };
    stamp_ms.map(|s| (now_ms as f64 - s).max(0.0) as u64)
}

/// What a corroborated refusal's settle does to the world, after the shadow move (bsv-low #649). A port so the
/// pins run the shipped [`settle_corroborated_refusal`] over a fake whose flush never answers.
pub(crate) trait RefusalSettle {
    /// The refused-verdict memo (`write_refused_verdict`).
    async fn write_memo(&self, reason: &str);
    /// One bump of the eviction counter.
    async fn bump(&self, counter: &'static str);
    /// The background flush of the pot / hop / tracker notes the eviction queued.
    async fn flush(&self);
    fn log(&self, line: String);
}

/// The lines a settle logs (each caller's own words).
pub(crate) struct SettleLines {
    /// The memo was not written: the pass yielded to a readmission.
    pub no_memo: String,
    /// CORROBORATED, the pass yielded: nothing evicted.
    pub yielded: String,
    /// CORROBORATED and evicted.
    pub evicted: String,
}

/// THE SETTLE of a corroborated refusal, after `evict_txid_everywhere` (the pending watch and the `/arc-ingest`
/// refusal job). Unless the pass yielded to a readmission: the refused-verdict memo, then the eviction COUNTED
/// and LOGGED; only then the flush (bsv-low #649). The flush waits on the app layer, which can outlast the
/// `waitUntil` window (beta, 2026-10-13: `/internal/pot-changed` at 12 to 34.6 s a call); a count placed after
/// it was cancelled with the task, and an eviction not counted reads as none (Rule 13).
pub(crate) async fn settle_corroborated_refusal<P: RefusalSettle>(
    port: &P,
    yielded: bool,
    counter: &'static str,
    memo_reason: &str,
    lines: SettleLines,
) {
    if yielded {
        port.log(lines.no_memo);
    } else {
        port.write_memo(memo_reason).await;
    }
    if yielded {
        port.log(lines.yielded);
    } else {
        port.bump(counter).await;
        port.log(lines.evicted);
    }
    port.flush().await;
}

/// The Worker's [`RefusalSettle`]: D1, the console, and the windowed background flush.
struct WorkerSettle<'a> {
    db: &'a D1Database,
    env: &'a worker::Env,
    txid: &'a str,
    now_ms: u64,
    /// When the `wait_until` job started (the flush's window is read from it).
    started_ms: u64,
}

impl RefusalSettle for WorkerSettle<'_> {
    async fn write_memo(&self, reason: &str) {
        write_refused_verdict(self.db, self.txid, self.now_ms as i64, reason).await;
    }
    async fn bump(&self, counter: &'static str) {
        crate::ops::bump_counter(self.db, counter, 1).await;
    }
    async fn flush(&self) {
        crate::pot_changes::flush_inline_within(self.env.clone(), self.started_ms).await;
    }
    fn log(&self, line: String) {
        worker::console_log!("{line}");
    }
}

/// The deferred job the callback route spawns for a SEEN+ push: Arcade LIVE
/// must say SEEN+ (one GET) before `network_seen` latches — the push is a
/// trigger, never the witness (the public bearer).
pub async fn witness_job(env: EvidenceEnv, txid: String, pushed_status: String) {
    // bsv-low #649: the job's start, the origin of its flush's window
    let started_ms = worker::Date::now().as_millis();
    let Ok(db) = env.env.d1("OVERLAY_DB") else {
        worker::console_log!(
            "[admit-fast] witness push for {txid}: no OVERLAY_DB in the job; nothing changes"
        );
        return;
    };
    let arcade = crate::broadcaster::ArcadeBroadcaster::new(env.arcade_base.clone());
    if arcade.network_witnessed(&txid).await {
        crate::ops::latch_network_seen(&db, &txid).await;
        crate::ops::bump_counter(&db, crate::ops::COUNTER_ARC_INGEST_SEEN_LATCHED, 1).await;
        worker::console_log!(
            "[admit-fast] {txid} {pushed_status}: verified live — network_seen latched by the push"
        );
        // step 4: a background latch ships its own pots-room push, AFTER its count (bsv-low #649: a flush the
        // window cancels must not take the count with it)
        crate::pot_changes::flush_inline_within(env.env.clone(), started_ms).await;
    } else {
        crate::ops::bump_counter(&db, crate::ops::COUNTER_ARC_INGEST_PUSH_UNVERIFIED, 1).await;
        worker::console_log!("[admit-fast] {txid} {pushed_status}: Arcade live does NOT say SEEN — the push changed nothing (a plant or a stale word)");
    }
}

/// The deferred job the callback route spawns for a refusal: check, then evict
/// on a corroborated refusal; count an uncertain one.
pub async fn refusal_job(env: EvidenceEnv, txid: String, webhook: (String, String), now_ms: u64) {
    // bsv-low #649: the job's start, the origin of its flush's window
    let started_ms = worker::Date::now().as_millis();
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
            let outcome =
                evict_txid_everywhere(db, &txid, &format!("{} ({reason})", webhook.0), now_ms)
                    .await;
            let moved = outcome.moved;
            // the second gate's HIGH-1 (bsv-low #451): the evidence is HERE — `/tx-any` answers `present:false`
            // for the evicted JOIN at once (the felt's `broadcast-unaccepted` latch), never `null` for an hour.
            // Round 3 of the loop-18 gate: never over a pass that YIELDED to a readmission (the pot stands).
            // The eviction is the event the felt voids on: the settle ships its notes (the route's own flush
            // drained before this job ran), AFTER the memo and the count (bsv-low #649).
            let port = WorkerSettle {
                db,
                env: &env.env,
                txid: &txid,
                now_ms,
                started_ms,
            };
            let status = &webhook.0;
            settle_corroborated_refusal(
                &port,
                outcome.yielded,
                crate::ops::COUNTER_ARC_INGEST_EVICTED,
                &format!("{status} ({reason})"),
                SettleLines {
                    no_memo: format!("[admit-fast] refusal {status} for {txid}: the eviction yielded to a readmission — no refused memo"),
                    yielded: format!("[admit-fast] refusal {status} for {txid} CORROBORATED ({reason}) — the pass YIELDED to a readmission; nothing evicted"),
                    evicted: format!("[admit-fast] refusal {status} for {txid} CORROBORATED ({reason}) — evicted {moved} row(s)"),
                },
            )
            .await;
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
    // bsv-low #649: the watch's start, the origin of its flushes' window
    let started_ms = worker::Date::now().as_millis();
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
                let ms = (worker::js_sys::Date::now() - admitted_at_ms).max(0.0) as u64;
                crate::ops::bump_counter(&db, crate::ops::COUNTER_SUBMIT_PENDING_SEEN_LATCHED, 1)
                    .await;
                crate::ops::bump_counter(&db, crate::ops::COUNTER_SUBMIT_PENDING_SEEN_MS, ms).await;
                worker::console_log!(
                    "[admit-fast] {txid} {status} {ms} ms after the admission — network_seen latched by the pending watch"
                );
                // step 4: a background latch ships its own pots-room push, AFTER its counts (bsv-low #649)
                crate::pot_changes::flush_inline_within(env.env.clone(), started_ms).await;
                return;
            }
            crate::broadcaster::WitnessLook::Fatal(status, extra) => {
                let now_ms = worker::Date::now().as_millis();
                match evidence_check(&env, &txid, (status.clone(), extra.clone())).await {
                    EvidenceVerdict::Refused(reason) => {
                        let outcome = evict_txid_everywhere(
                            &db,
                            &txid,
                            &format!("{status} ({reason})"),
                            now_ms,
                        )
                        .await;
                        let moved = outcome.moved;
                        // the second gate's HIGH-1: the verdict memo, written where the evidence is — never
                        // over a pass that yielded to a readmission (round 3 of the loop-18 gate). The eviction
                        // is the event the felt voids on: the settle ships its notes (the route's flush drained
                        // before the watch), AFTER the memo and the count (bsv-low #649).
                        let port = WorkerSettle {
                            db: &db,
                            env: &env.env,
                            txid: &txid,
                            now_ms,
                            started_ms,
                        };
                        settle_corroborated_refusal(
                            &port,
                            outcome.yielded,
                            crate::ops::COUNTER_SUBMIT_PENDING_EVICTED,
                            &format!("{status} ({reason})"),
                            SettleLines {
                                no_memo: format!("[admit-fast] {txid} {status}: the eviction yielded to a readmission — no refused memo"),
                                yielded: format!("[admit-fast] {txid} {status} CORROBORATED ({reason}) — the pass YIELDED to a readmission; nothing evicted by the pending watch"),
                                evicted: format!("[admit-fast] {txid} {status} CORROBORATED ({reason}) — evicted {moved} row(s) by the pending watch"),
                            },
                        )
                        .await;
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
            worker::console_log!(
                "[admit-fast] {txid} CONFIRMED by the watch's own proof read (height {height:?}; {rows} row(s), {cas_missed} CAS miss(es), {cas_faults} CAS fault(s)) — the block had come before the index saw the spend"
            );
            // bsv-low #649: the push after the count and the log, inside what is left of the watch's window
            crate::pot_changes::flush_inline_within(env.env.clone(), started_ms).await;
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
    let height = beef_limits::merkle_path_from_hex(&bump_hex, beef_limits::COURIER_PROOF_MAX_BYTES)
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
                notnull: r.get::<_, i64>(3).unwrap_or(0),
                dflt_value: r.get::<_, Option<String>>(4).unwrap_or(None),
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
        // round 5 (N7): an advert row too — `low_records.createdAt` carries an EXPRESSION default, so its restore's
        // COALESCE runs under real SQLite here
        conn.execute("INSERT INTO low_records (recordType, txid, outputIndex, hostIdentity, gameId) VALUES ('lobby', ?1, 1, '02aa', 'g1')", [&pot]).unwrap();
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
            moved, 5,
            "the pot row, its output, its applied row, its party marker, its advert row (round 5, N7)"
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
        // round 5 (N7): the advert row's own end state — moved, then restored with its coalesced createdAt
        assert_eq!(count(&conn, "low_records", "txid", &pot), 1, "the advert row restored (its expression-default createdAt coalesced)");
        assert_eq!(count(&conn, "low_records_evicted", "txid", &pot), 0);
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
            // bsv-low #649: the flush is the shipped settle's last step (after the memo and the count), pinned
            // by `e649_a_c_*`; here the settle sits right after the eviction
            let fl = jobs[at..]
                .find("settle_corroborated_refusal(")
                .expect("the settle (memo, count, flush) after the eviction");
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

    /// The E606 lens's M1 (structural; `e605_a` runs the shipped statements over the test store): the D1 conflict
    /// eviction hands the re-pointed outpoints to the wrapper, which adds them to the released list BEFORE the merge
    /// and the close, so the ledger's `releasedSpends` still names the hop the competitor took.
    #[test]
    fn e606_m1_the_conflict_eviction_writes_the_repointed_hop_into_the_ledger() {
        let src = include_str!("admit_fast.rs");
        let imp = &src[src.find("impl ConflictEviction for D1Database").unwrap()..];
        let imp = &imp[..imp
            .find("pub async fn converge_confirmed_conflict")
            .unwrap()];
        assert!(
            imp.contains("evict_txid_everywhere_repointed(self, txid, reason, now_ms, repointed)"),
            "the D1 store hands the re-pointed outpoints to the eviction"
        );
        let w = &src[src
            .find("pub async fn evict_txid_everywhere_repointed(")
            .unwrap()..];
        let w = &w[..w.find("struct EvictedRow").unwrap()];
        let release = w.find("release_spends_of(db, &txid)").expect("the release");
        let add = w
            .find("released.extend(repointed.iter().cloned())")
            .expect("the re-pointed added");
        let merge = w.find("merge_released(").expect("the merge");
        let close = w
            .find("Query::new(CLOSE_EVICTION_MARKER_SQL)")
            .expect("the close");
        assert!(
            release < add && add < merge && merge < close,
            "release, add, merge, close"
        );
        let pass = &src[src
            .find("pub async fn converge_confirmed_conflict(")
            .unwrap()..];
        let pass: String = pass[..pass.find("\n}\n").unwrap()]
            .split_whitespace()
            .collect();
        assert!(
            pass.contains("now_ms,&repointed,)"),
            "the pass hands what it re-pointed to the eviction"
        );
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
        let squashed: String = evict.split_whitespace().collect();
        assert!(
            squashed.contains("ifcore.yielded_to_readmission.is_none(){forvin&advert_vouts{crate::lobby_changes::note_evicted("),
            "the lobby learns an eviction only when one stands (measured on squashed text: a formatter never reds a pin)"
        );
        let yielded_at = evict.find("COUNTER_ADMIT_FAST_EVICT_YIELDED").unwrap();
        let reopen = &evict[yielded_at..];
        assert!(
            reopen.contains("if !core.restore_faults.is_empty() {") && reopen.contains("twin_rows_of(db, &txid)"),
            "a faulted yield re-opens the row only for the yield's OWN restore faults and only while the twins hold the txid (round 4)"
        );
        // the two jobs write no refused memo and count no eviction over a yielded pass (round 3, finding 3; round 4)
        // bsv-low #649: both jobs hand `outcome.yielded` to ONE settle, whose guards these are
        // bsv-low #649 L2: the CALL's own arguments, comment-stripped and split at the top level, never a token
        // found somewhere after it (a decoy `let _unused_yield = (outcome.yielded,);` passed that)
        for (job_name, counter) in [
            (
                "pub async fn refusal_job(",
                "crate::ops::COUNTER_ARC_INGEST_EVICTED",
            ),
            (
                "pub async fn pending_watch_job(",
                "crate::ops::COUNTER_SUBMIT_PENDING_EVICTED",
            ),
        ] {
            let job = &src[src.find(job_name).unwrap()..];
            let job = &job[..job.find("\n}\n").unwrap()];
            let args = call_args(job, "settle_corroborated_refusal(");
            assert_eq!(
                args.len(),
                5,
                "{job_name}: the settle's five arguments: {args:?}"
            );
            assert_eq!(
                args[0], "&port",
                "{job_name}: the settle runs over the job's port"
            );
            assert_eq!(
                args[1], "outcome.yielded",
                "{job_name}: the settle is told THIS pass's yield"
            );
            assert_eq!(
                args[2], counter,
                "{job_name}: settled under its own eviction counter"
            );
            assert!(
                args[4].starts_with("SettleLines{"),
                "{job_name}: the job's words"
            );
            let evict = job
                .find("let outcome =")
                .expect("the eviction's outcome is bound");
            assert!(
                self::squashed(&job[evict..]).starts_with("letoutcome=evict_txid_everywhere("),
                "{job_name}: `outcome` is the eviction's"
            );
            assert!(
                evict < job.find("settle_corroborated_refusal(").unwrap(),
                "{job_name}: bound before the settle"
            );
            assert_eq!(
                job.matches("let outcome").count(),
                1,
                "{job_name}: one `outcome` binding, no shadowing"
            );
        }
        {
            let job_name = "pub(crate) async fn settle_corroborated_refusal<";
            let counter = "port.bump(counter)";
            let job = &src[src.find(job_name).unwrap()..];
            let job = &job[..job.find("\n}\n").unwrap()];
            let guard = job.find("if yielded {").expect("the yield guard");
            let memo = job[guard..].find("port.write_memo(").expect("the memo") + guard;
            assert!(job[guard..memo].contains("} else {"), "{job_name}: the memo sits in the else of the yield guard");
            let cnt = job.find(counter).expect("the eviction counter");
            let cnt_guard = job[..cnt]
                .rfind("if yielded {")
                .expect("the counter's yield guard");
            let between = &job[cnt_guard..cnt];
            assert!(
                between.matches("} else {").count() == 1 && !between.contains("port.write_memo("),
                "{job_name}: the eviction counter sits in the else of ITS OWN yield guard (round 5, N8: the memo's guard is not it)"
            );
        }
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
        // a marker with an OLDER stamp keeps the readmission AND its reason (round 3, NIT 6)
        conn.execute(OPEN_EVICTION_MARKER_SQL, rusqlite::params![&pot, "REJECTED (late)", 1_100i64]).unwrap();
        let (kept_reason, kept_at): (String, Option<i64>) = conn
            .query_row("SELECT reason, readmittedAt FROM pot_evictions WHERE txid = ?1", [&pot], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap();
        assert_eq!((kept_reason.as_str(), kept_at), ("REJECTED (a)", Some(1_500)));
        conn.execute(CLOSE_EVICTION_MARKER_SQL, rusqlite::params![&pot, "REJECTED (closing)", 1_000i64, 3i64, "[]"]).unwrap();
        let (readmitted, moved, reason): (Option<i64>, i64, String) = conn
            .query_row("SELECT readmittedAt, rowsMoved, reason FROM pot_evictions WHERE txid = ?1", [&pot], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap();
        assert_eq!(readmitted, Some(1_500), "the close keeps a readmission stamped after the pass");
        assert_eq!(moved, 3);
        assert_eq!(reason, "REJECTED (a)", "and keeps the reason too (round 4, the CLOSE half of NIT 6)");
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

    /// Round 2 (NEW-2) + rounds 3 and 4: the restore lands a row whose twin lacks a NOT NULL DEFAULT column the
    /// source gained after the eviction (the column takes its default; a later heal carries the default; a NULL
    /// in a twin healed before that is coalesced onto it), is idempotent, and names a table whose copy back landed
    /// nothing instead of deleting its twin rows.
    #[tokio::test]
    async fn restore_twins_lands_the_default_of_a_column_added_after_the_eviction_and_is_fail_loud() {
        let pot = "ab".repeat(32);
        let db = SqliteShadow::new(shipped_conn());
        db.conn.execute("INSERT INTO pot_records (txid, outputIndex, spent, createdAt, lockKind) VALUES (?1, 0, 0, 1789959998, 'covenant')", [&pot]).unwrap();
        let r = shadow_move_passes(&db, &pot, "REJECTED", 1_000, false).await;
        assert_eq!(r.moved, 1);
        // the source gains a NOT NULL DEFAULT column after the eviction (the schema's own class: has_proof,
        // spentConfirmed, paramsDecoded)
        db.conn.execute_batch("ALTER TABLE pot_records ADD COLUMN loop18_new TEXT NOT NULL DEFAULT 'd'").unwrap();
        assert!(!cols(&db.conn, "pot_records_evicted").iter().any(|c| c.name == "loop18_new"));
        let (restored, faults) = restore_twins(&db, &pot).await;
        assert_eq!((restored, faults.len()), (1, 0), "{faults:?}");
        assert!(
            !cols(&db.conn, "pot_records_evicted").iter().any(|c| c.name == "loop18_new"),
            "the restore never heals the twin: the added column is omitted and takes the source's default"
        );
        assert_eq!(count(&db.conn, "pot_records", "txid", &pot), 1, "the row LANDED (an explicit NULL would have been dropped)");
        let new_col: String = db.conn.query_row("SELECT loop18_new FROM pot_records WHERE txid = ?1", [&pot], |r| r.get(0)).unwrap();
        assert_eq!(new_col, "d", "the added column carries its DEFAULT");
        // a twin HEALED after the migration (a later eviction) carries the default for its older rows too
        let r = shadow_move_passes(&db, &pot, "REJECTED", 2_000, false).await;
        assert_eq!(r.moved, 1);
        let twin_cols = cols(&db.conn, "pot_records_evicted");
        let healed = twin_cols.iter().find(|c| c.name == "loop18_new").expect("the later eviction healed the twin");
        assert_eq!(healed.dflt_value.as_deref(), Some("'d'"), "healed WITH the default (round 4)");
        // and even a twin row holding NULL there (a twin healed before the heal carried defaults) restores onto the default
        db.conn.execute("UPDATE pot_records_evicted SET loop18_new = NULL WHERE txid = ?1", [&pot]).unwrap();
        let (restored, faults) = restore_twins(&db, &pot).await;
        assert_eq!((restored, faults.len()), (1, 0), "{faults:?}");
        let new_col: String = db.conn.query_row("SELECT loop18_new FROM pot_records WHERE txid = ?1", [&pot], |r| r.get(0)).unwrap();
        assert_eq!(new_col, "d", "COALESCEd onto the default");
        // idempotent
        let (again, faults) = restore_twins(&db, &pot).await;
        assert_eq!((again, faults.len()), (0, 0));
        // a copy back that lands nothing is named and the twin keeps its row (the twin dropped from under the restore)
        let r = shadow_move_passes(&db, &pot, "REJECTED", 3_000, false).await;
        assert_eq!(r.moved, 1);
        db.conn.execute_batch("DROP TABLE pot_records_evicted").unwrap();
        db.conn.execute_batch("CREATE TABLE pot_records_evicted (af_evictedAt INTEGER NOT NULL, af_reason TEXT NOT NULL, txid TEXT)").unwrap();
        db.conn.execute("INSERT INTO pot_records_evicted (af_evictedAt, af_reason, txid) VALUES (1, 'x', ?1)", [&pot]).unwrap();
        let (restored, faults) = restore_twins(&db, &pot).await;
        assert_eq!(restored, 0);
        assert!(
            faults.iter().any(|f| f.starts_with("pot_records by txid: the copy back landed 0 of 1 row(s)")),
            "{faults:?}"
        );
        assert_eq!(count(&db.conn, "pot_records_evicted", "txid", &pot), 1, "the twin keeps the row it could not land");
    }

    /// Round 5 (N7): the heal carries only a LITERAL default; an expression default is left to the restore's COALESCE.
    #[test]
    fn the_heal_carries_a_literal_default_only() {
        for d in ["0", "-1", "1.5", "'d'", "''", "NULL", "null", "CURRENT_TIMESTAMP"] {
            assert!(literal_default(d), "{d}");
        }
        for d in ["datetime('now')", "(datetime('now'))", "", "  ", "abc", "1; DROP TABLE x"] {
            assert!(!literal_default(d), "{d}");
        }
        let lit = ColumnInfo { name: "c".into(), ty: "INTEGER".into(), notnull: 1, dflt_value: Some("0".into()) };
        assert_eq!(heal_shadow_sql("pot_records", &lit), "ALTER TABLE \"pot_records_evicted\" ADD COLUMN \"c\" INTEGER DEFAULT 0");
        let expr = ColumnInfo { name: "createdAt".into(), ty: "TEXT".into(), notnull: 1, dflt_value: Some("datetime('now')".into()) };
        assert_eq!(heal_shadow_sql("low_records", &expr), "ALTER TABLE \"low_records_evicted\" ADD COLUMN \"createdAt\" TEXT");
        // the restore coalesces the NOT NULL expression default all the same (legal in a SELECT)
        let (ins, _) = restore_sql("low_records", "txid", std::slice::from_ref(&expr));
        assert!(ins.contains("COALESCE(\"createdAt\", datetime('now'))"), "{ins}");
    }

    // lane E1D's delta fold (L2, the delta lens on `0da3a82`): a predecessor the engine LANDS first from a
    // submit's BEEF is an admission write the eviction ledger was never asked about (an evicted txid's rows are
    // shadow-moved, so it reads "not landed" and a successor carrying it landed it again under the OPEN row; the
    // post-write guard re-evicted only the subject). Every submit that reads a report now guards what it landed,
    // and the guard re-evicts under an open row. RED on `0da3a82` (no `guard_landed`).
    #[test]
    fn e1d_delta_l2_every_submit_with_a_report_guards_what_it_landed() {
        for (file, src) in [("routes.rs", include_str!("routes.rs")), ("lib.rs", include_str!("lib.rs"))] {
            // a call, never a comment that names one
            // the E592 lens fold: the queue's replay names its breach policy (`submit_with_report_under`)
            let calls: Vec<usize> = src
                .match_indices(".submit_with_report(")
                .chain(src.match_indices(".submit_with_report_under("))
                .map(|(at, _)| at)
                .filter(|&at| !src[src[..at].rfind('\n').map_or(0, |nl| nl + 1)..at].trim_start().starts_with("//"))
                .collect();
            assert!(!calls.is_empty(), "{file}: no submit with a report");
            for at in calls {
                let after = &src[at..];
                let window = &after[..after.len().min(1_500)];
                assert!(
                    window.contains("crate::admit_fast::guard_landed(") && window.contains("landed_predecessors"),
                    "{file}: the submit at byte {at} does not guard what it landed"
                );
            }
        }
        let src = include_str!("admit_fast.rs");
        let guard = &src[src.find("pub async fn guard_landed(").unwrap()..];
        let guard = &guard[..guard.find("\n}\n").unwrap()];
        let open = guard.find("open_eviction(db, txid)").expect("the ledger is read per landed txid");
        let evict = guard.find("evict_txid_everywhere(db, txid, &ev.reason").expect("an open row re-evicts");
        assert!(open < evict);
        assert!(guard.contains("COUNTER_ADMIT_FAST_REEVICTED_AFTER_WRITE"), "counted");
        assert!(guard.contains("COUNTER_ADMIT_FAST_LEDGER_UNREADABLE"), "an unreadable ledger is counted");
    }

    /// bsv-low #649: the settle's world, recorded; its flush HANGS (never answers), as the app layer's
    /// `/internal/pot-changed` did past the `waitUntil` window on beta (12 to 34.6 s a call).
    struct HangingFlush {
        events: std::cell::RefCell<Vec<String>>,
    }

    impl RefusalSettle for HangingFlush {
        async fn write_memo(&self, reason: &str) {
            self.events.borrow_mut().push(format!("memo {reason}"));
        }
        async fn bump(&self, counter: &'static str) {
            self.events.borrow_mut().push(format!("bump {counter}"));
        }
        async fn flush(&self) {
            self.events.borrow_mut().push("flush".into());
            std::future::pending::<()>().await
        }
        fn log(&self, line: String) {
            self.events.borrow_mut().push(format!("log {line}"));
        }
    }

    /// Run the SHIPPED settle until its flush hangs (one poll: every other step of the fake answers at once) and
    /// return what was done by then, read while the settle is still parked inside the flush: the runtime's
    /// cancellation of the `waitUntil` task lands there.
    fn settle_until_the_flush_hangs(yielded: bool, counter: &'static str) -> Vec<String> {
        let port = HangingFlush {
            events: Default::default(),
        };
        let lines = SettleLines {
            no_memo: "no-memo".into(),
            yielded: "yielded".into(),
            evicted: "evicted".into(),
        };
        let mut fut = std::pin::pin!(settle_corroborated_refusal(
            &port,
            yielded,
            counter,
            "REJECTED (spent)",
            lines
        ));
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(
            std::future::Future::poll(fut.as_mut(), &mut cx).is_pending(),
            "the flush hangs, so the settle has not returned"
        );
        let done = port.events.borrow().clone();
        done
    }

    /// bsv-low #649 (a) and (c): a flush that hangs past its wall (the task cancelled inside it) leaves the memo
    /// written, the eviction COUNTED and LOGGED, for the pending watch's counter and the `/arc-ingest` refusal
    /// job's alike; over a yielded pass neither the memo nor the count. Both jobs call the shipped settle with
    /// their own counter, after their eviction, and their refused arm flushes, counts and memos only through it.
    #[test]
    fn e649_a_c_the_count_and_the_memo_land_before_a_flush_that_hangs() {
        for counter in [
            crate::ops::COUNTER_SUBMIT_PENDING_EVICTED,
            crate::ops::COUNTER_ARC_INGEST_EVICTED,
        ] {
            let done = settle_until_the_flush_hangs(false, counter);
            assert_eq!(
                done,
                vec![
                    "memo REJECTED (spent)".to_string(),
                    format!("bump {counter}"),
                    "log evicted".to_string(),
                    "flush".to_string(),
                ],
                "{counter}: the memo, the count and the log land BEFORE the flush (a hanging flush cancelled with the task takes none of them)"
            );
            let yielded = settle_until_the_flush_hangs(true, counter);
            assert_eq!(
                yielded,
                vec![
                    "log no-memo".to_string(),
                    "log yielded".to_string(),
                    "flush".to_string()
                ],
                "{counter}: a yielded pass writes no memo and counts nothing"
            );
        }
        let src = include_str!("admit_fast.rs");
        for (job_name, counter) in [
            (
                "pub async fn pending_watch_job(",
                "crate::ops::COUNTER_SUBMIT_PENDING_EVICTED,",
            ),
            (
                "pub async fn refusal_job(",
                "crate::ops::COUNTER_ARC_INGEST_EVICTED,",
            ),
        ] {
            let job = &src[src.find(job_name).expect("the job")..];
            let job = &job[..job.find("\n}\n").expect("the job ends")];
            assert!(
                job.contains("let started_ms = worker::Date::now().as_millis();"),
                "{job_name}: the job records its start"
            );
            let evict = job.find("evict_txid_everywhere(").expect("the eviction");
            let settle = job
                .find("settle_corroborated_refusal(")
                .expect("the shipped settle");
            assert!(
                evict < settle,
                "{job_name}: the settle runs after the eviction"
            );
            assert!(
                job[settle..settle + 400].contains(counter),
                "{job_name}: settled under {counter}"
            );
            let arm_end = job[evict..]
                .find("EvidenceVerdict::Present")
                .expect("the next arm")
                + evict;
            let arm = &job[evict..arm_end];
            assert!(
                !arm.contains("flush_inline")
                    && !arm.contains("bump_counter(")
                    && !arm.contains("write_refused_verdict("),
                "{job_name}: the refused arm flushes, counts and memos only through the settle"
            );
        }
    }

    /// bsv-low #649 (b): a background flush gets ONE wall bounded by what remains of the `waitUntil` window:
    /// 25 s into the 30 it is given at most the 5 s left (not a fresh 20), under the floor it is skipped (and
    /// counted, nothing drained), and a fresh job keeps the 20 s budget. The tracker events share that deadline;
    /// every flush of an admit-fast background job is the windowed one.
    #[test]
    fn e649_b_a_background_flush_is_bounded_by_the_window_left() {
        use crate::pot_changes::{
            background_flush_plan, BackgroundFlush, FLUSH_WINDOW_FLOOR_MS, WAIT_UNTIL_WINDOW_MS,
        };
        let t0 = 1_000_000u64;
        assert_eq!(WAIT_UNTIL_WINDOW_MS, 30_000);
        assert_eq!(
            background_flush_plan(t0, t0),
            BackgroundFlush::Ship {
                deadline_ms: t0 + crate::change_flush::FLUSH_WALL_BUDGET_MS
            },
            "a fresh job keeps the flush's own budget"
        );
        match background_flush_plan(t0, t0 + 25_000) {
            BackgroundFlush::Ship { deadline_ms } => assert!(
                deadline_ms <= t0 + WAIT_UNTIL_WINDOW_MS,
                "25 s into the window the flush is given at most the remainder: deadline at +{} ms of the job's 30 000",
                deadline_ms - t0
            ),
            BackgroundFlush::Skip { remaining_ms } => {
                assert!(remaining_ms < FLUSH_WINDOW_FLOOR_MS, "skipped only under the floor ({remaining_ms} ms left)")
            }
        }
        assert_eq!(
            background_flush_plan(t0, t0 + WAIT_UNTIL_WINDOW_MS - FLUSH_WINDOW_FLOOR_MS + 1),
            BackgroundFlush::Skip {
                remaining_ms: FLUSH_WINDOW_FLOOR_MS - 1
            },
            "under the floor the flush is skipped"
        );
        assert_eq!(
            background_flush_plan(t0, t0 + 31_000),
            BackgroundFlush::Skip { remaining_ms: 0 }
        );
        let squash = |s: &str| s.split_whitespace().collect::<String>();
        let pots = squash(include_str!("pot_changes.rs"));
        let within = &pots[pots
            .find("pubasyncfnflush_inline_within(")
            .expect("the windowed flush")..];
        let within = &within[..within.find("pubfnflush<").expect("ends")];
        // bsv-low #649 M1: a skip POSTs nothing and PARKS its notes durably (`change_park::park_withheld`, which
        // counts the skip and the withheld entries; its own pins run it); the Ship arm alone drains to POST
        let skip_arm = &within[within.find("BackgroundFlush::Skip{").unwrap()
            ..within.find("BackgroundFlush::Ship{").unwrap()];
        assert!(
            skip_arm.contains("crate::change_park::park_withheld(") && !skip_arm.contains("ship(") && !skip_arm.contains("drain()"),
            "a skip starts no POST and parks its notes (bsv-low #649 M1), never left in the isolate's sets alone"
        );
        assert!(
            within.contains("tracker_events::ship_until(&env,tracker_events,deadline_ms)")
                && !within.contains("tracker_events::ship(&"),
            "the tracker events share the one deadline, not 20 s of their own on top"
        );
        let src = include_str!("admit_fast.rs");
        let code = &src[..src.find("#[cfg(test)]").unwrap()];
        assert_eq!(
            code.matches("pot_changes::flush_inline(").count(),
            0,
            "no unwindowed flush left in a background job"
        );
        assert_eq!(
            code.matches("pot_changes::flush_inline_within(").count(),
            4,
            "the settle's port, the witness job, the watch's SEEN latch and its proof read"
        );
    }

    /// bsv-low #649 M2 (structural; `e649_m2_*` in `tests/e602_convergence.rs` runs the pass over a yielding port):
    /// the D1 adapter answers the wrapper's yield as `Yielded` BEFORE any refused memo, never `Ok(())` over it.
    #[test]
    fn e649_m2_the_d1_adapter_carries_the_yield() {
        let src = include_str!("admit_fast.rs");
        let imp = &src[src.find("impl ConflictEviction for D1Database").unwrap()..];
        let imp: String = imp[..imp
            .find("pub async fn converge_confirmed_conflict")
            .unwrap()]
            .lines()
            .map(|l| l.split("//").next().unwrap_or(""))
            .collect::<String>()
            .split_whitespace()
            .collect();
        let y = imp
            .find("ifresult.yielded{returnOk(ConflictEvictOutcome::Yielded);}")
            .expect("the D1 adapter answers the yield");
        let memo = imp.find("write_refused_verdict(").expect("the memo");
        let ev = imp
            .find("Ok(ConflictEvictOutcome::Evicted)")
            .expect("the eviction answered");
        assert!(
            y < memo && memo < ev,
            "yield answered first, then the memo, then Evicted"
        );
    }

    /// bsv-low #649 H1: the conflict pass's world, recorded; the SECOND candidate's courier never answers.
    struct HangingSecondCourier {
        events: std::cell::RefCell<Vec<String>>,
    }

    impl ConflictPassPort for HangingSecondCourier {
        async fn check(&self, txid: &str, _beef: Option<String>) -> Result<bool, String> {
            self.events.borrow_mut().push(format!("check {txid}"));
            if txid == "first" {
                self.events.borrow_mut().push("first evicted".into());
                return Ok(true);
            }
            std::future::pending::<()>().await;
            unreachable!()
        }
        async fn persist(&self, d: &ConflictPassSummary) {
            self.events.borrow_mut().push(format!(
                "persist scanned={} evicted={} faults={}",
                d.scanned, d.evicted, d.faults
            ));
        }
        async fn flush(&self) {
            self.events.borrow_mut().push("flush".into());
        }
    }

    /// bsv-low #649 L1: the conflict pass's world where EVERY candidate completes (the first and third evicted,
    /// the second's re-check faulted) and the delivery completes too.
    struct CompletingConflict {
        events: std::cell::RefCell<Vec<String>>,
    }

    impl ConflictPassPort for CompletingConflict {
        async fn check(&self, txid: &str, _beef: Option<String>) -> Result<bool, String> {
            self.events.borrow_mut().push(format!("check {txid}"));
            match txid {
                "second" => Err("courier fault".into()),
                _ => Ok(true),
            }
        }
        async fn persist(&self, d: &ConflictPassSummary) {
            self.events
                .borrow_mut()
                .push(format!("persist {}/{}/{}", d.scanned, d.evicted, d.faults));
        }
        async fn flush(&self) {
            self.events.borrow_mut().push("flush".into());
        }
    }

    /// bsv-low #649 L1 (the delta lens's appended persist in the conflict loop): the shipped pass, run to its END
    /// over three candidates and a delivery that complete, persists each candidate ONCE, in order, before the
    /// next candidate's check, and its delivery await is its last effect: nothing persisted after it. And over the
    /// loop's WHOLE body: the delivery await is the last statement before the answer, and each iteration ends with
    /// pure additions after its persist. The lens's mutant (`port.flush().await; port.persist(&summary).await;`
    /// appended) passed every pin on `1516ed5`; here it is RED on the extra `persist 3/2/1` and on the body.
    #[test]
    fn e649_l1_the_conflict_pass_persists_nothing_after_its_delivery_await() {
        let port = CompletingConflict {
            events: Default::default(),
        };
        let rows = vec![
            ("first".to_string(), None),
            ("second".to_string(), None),
            ("third".to_string(), None),
        ];
        let mut fut = std::pin::pin!(run_conflict_candidates(&port, rows));
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        let std::task::Poll::Ready(summary) = std::future::Future::poll(fut.as_mut(), &mut cx)
        else {
            panic!("every step of the fake answers at once: the pass completes")
        };
        assert_eq!(
            (summary.scanned, summary.evicted, summary.faults),
            (3, 2, 1)
        );
        assert_eq!(
            *port.events.borrow(),
            vec![
                "check first",
                "persist 1/1/0",
                "check second",
                "persist 1/0/1",
                "check third",
                "persist 1/1/0",
                "flush",
            ],
            "each candidate persisted once before the next check; the delivery is the pass's LAST effect"
        );
        let src = include_str!("admit_fast.rs");
        let l = &src[src
            .find("pub(crate) async fn run_conflict_candidates<")
            .unwrap()..];
        let l = squashed(&l[..l.find("\n}\n").unwrap() + 3]);
        let flush = "port.flush().await;";
        assert_eq!(l.matches("port.flush()").count(), 1, "one delivery await");
        assert_eq!(
            &l[l.find(flush).unwrap() + flush.len()..],
            "summary}",
            "nothing after the conflict pass's delivery await but its answer"
        );
        assert_eq!(
            l.matches(".persist(").count(),
            1,
            "one persist: each candidate's own"
        );
        let persist = "port.persist(&one).await;";
        let after = &l[l.find(persist).unwrap() + persist.len()..l.find(flush).unwrap()];
        assert_eq!(
            after,
            "summary.scanned+=one.scanned;summary.evicted+=one.evicted;summary.faults+=one.faults;}",
            "an iteration ends with pure additions after its persist"
        );
    }

    /// bsv-low #649 H1 (the lens's hanging second courier): candidate 1 is evicted, candidate 2's courier hangs, and
    /// the runtime cancels the task there. The shipped loop must have PERSISTED candidate 1's eviction count before
    /// candidate 2's network work started. On `c8e4d8f` the pass bumped once after the loop: nothing persisted.
    #[test]
    fn e649_h1_a_conflict_eviction_is_counted_before_the_next_candidates_courier() {
        let port = HangingSecondCourier {
            events: Default::default(),
        };
        let rows = vec![("first".to_string(), None), ("second".to_string(), None)];
        let mut fut = std::pin::pin!(run_conflict_candidates(&port, rows));
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(
            std::future::Future::poll(fut.as_mut(), &mut cx).is_pending(),
            "the second courier hangs, so the pass has not returned"
        );
        let done = port.events.borrow().clone();
        assert_eq!(
            done,
            vec![
                "check first".to_string(),
                "first evicted".to_string(),
                "persist scanned=1 evicted=1 faults=0".to_string(),
                "check second".to_string(),
            ],
            "the first eviction is counted durably before the second candidate's courier is asked"
        );
    }

    /// bsv-low #649 H1 (structural): the production pass runs the shipped loop, and the loop persists inside it.
    #[test]
    fn e649_h1_the_pass_runs_the_shipped_loop() {
        let src = include_str!("admit_fast.rs");
        let pass = &src[src.find("pub async fn confirmed_conflict_pass(").unwrap()..];
        let pass = &pass[..pass.find("\n}\n").unwrap()];
        assert!(
            pass.contains("run_conflict_candidates(&port,"),
            "the pass runs the shipped loop"
        );
        assert_eq!(
            pass.matches("bump_conflict_counters(").count(),
            1,
            "only the candidate read's fault is bumped by the pass itself"
        );
        let l = &src[src
            .find("pub(crate) async fn run_conflict_candidates<")
            .unwrap()..];
        let l = &l[..l.find("\n}\n").unwrap()];
        let check = l.find("port.check(").unwrap();
        let persist = l
            .find("port.persist(&one).await")
            .expect("each candidate persisted");
        let close = l.find("summary.scanned += one.scanned").unwrap();
        assert!(
            check < persist && persist < close,
            "persisted inside the loop, after its own check"
        );
    }

    /// bsv-low #649 L1: the settle's world, recorded; its flush COMPLETES, so whatever the settle does after the
    /// delivery await is executed and seen (the hanging fake stops at the flush and cannot see a suffix).
    struct CompletingFlush {
        events: std::cell::RefCell<Vec<String>>,
    }

    impl RefusalSettle for CompletingFlush {
        async fn write_memo(&self, reason: &str) {
            self.events.borrow_mut().push(format!("memo {reason}"));
        }
        async fn bump(&self, counter: &'static str) {
            self.events.borrow_mut().push(format!("bump {counter}"));
        }
        async fn flush(&self) {
            self.events.borrow_mut().push("flush".into());
        }
        fn log(&self, line: String) {
            self.events.borrow_mut().push(format!("log {line}"));
        }
    }

    /// The code of `text` with `//` comments and every whitespace removed.
    fn squashed(text: &str) -> String {
        text.lines()
            .map(|l| l.split("//").next().unwrap_or(""))
            .collect::<String>()
            .split_whitespace()
            .collect()
    }

    /// bsv-low #649 L2: the top-level arguments of the first call `open` (ending in `(`) in `text`, each
    /// comment-stripped and squashed.
    fn call_args(text: &str, open: &str) -> Vec<String> {
        let code = squashed(text);
        let open = squashed(open);
        let start = code.find(&open).expect("the call") + open.len();
        let (mut depth, mut args, mut cur) = (0i32, Vec::new(), String::new());
        for ch in code[start..].chars() {
            match ch {
                '(' | '{' | '[' => depth += 1,
                ')' | '}' | ']' if depth == 0 => break,
                ')' | '}' | ']' => depth -= 1,
                ',' if depth == 0 => {
                    args.push(std::mem::take(&mut cur));
                    continue;
                }
                _ => {}
            }
            cur.push(ch);
        }
        if !cur.is_empty() {
            args.push(cur);
        }
        args
    }

    /// bsv-low #649 L1 (the lens's appended bump): the shipped settle, run to its END over a flush that completes,
    /// does NOTHING after the delivery await, yielded or not, under both jobs' counters; and over the WHOLE of both
    /// settle jobs, nothing is persisted or logged on the settled path after the settle's call: its arm ends at the
    /// call, and so does the job (the refusal job's match is its last statement; the watch returns after it).
    #[test]
    fn e649_l1_nothing_is_persisted_or_logged_after_the_delivery_await() {
        for counter in [
            crate::ops::COUNTER_SUBMIT_PENDING_EVICTED,
            crate::ops::COUNTER_ARC_INGEST_EVICTED,
        ] {
            for yielded in [false, true] {
                let port = CompletingFlush {
                    events: Default::default(),
                };
                let lines = SettleLines {
                    no_memo: "no-memo".into(),
                    yielded: "yielded".into(),
                    evicted: "evicted".into(),
                };
                let mut fut = std::pin::pin!(settle_corroborated_refusal(
                    &port,
                    yielded,
                    counter,
                    "REJECTED (spent)",
                    lines
                ));
                let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
                assert!(
                    std::future::Future::poll(fut.as_mut(), &mut cx).is_ready(),
                    "every step of the fake answers at once: the settle completes"
                );
                let done = port.events.borrow().clone();
                let expected: Vec<String> = if yielded {
                    vec!["log no-memo".into(), "log yielded".into(), "flush".into()]
                } else {
                    vec![
                        "memo REJECTED (spent)".into(),
                        format!("bump {counter}"),
                        "log evicted".into(),
                        "flush".into(),
                    ]
                };
                assert_eq!(
                    done, expected,
                    "{counter} yielded={yielded}: the flush is the settle's LAST effect"
                );
            }
        }
        let src = include_str!("admit_fast.rs");
        // the settle: its body ends at the delivery await
        let settle = &src[src
            .find("pub(crate) async fn settle_corroborated_refusal<")
            .unwrap()..];
        let settle = squashed(&settle[..settle.find("\n}\n").unwrap() + 3]);
        let after = &settle[settle
            .find("port.flush().await;")
            .expect("the delivery await")
            + "port.flush().await;".len()..];
        assert_eq!(after, "}", "nothing after the settle's delivery await");
        assert_eq!(
            settle.matches("port.flush()").count(),
            1,
            "one delivery await in the settle"
        );
        // both jobs: the settled arm ends at the settle's call
        for (job_name, tail_after_match) in [
            ("pub async fn refusal_job(", "}}"),
            ("pub async fn pending_watch_job(", ""),
        ] {
            let job = &src[src.find(job_name).expect("the job")..];
            let job = squashed(&job[..job.find("\n}\n").expect("the job ends")]);
            let call = job
                .find("settle_corroborated_refusal(")
                .expect("the settle call");
            let call_end =
                job[call..].find(".await;").expect("the call is awaited") + call + ".await;".len();
            let next_arm = job[call_end..]
                .find("EvidenceVerdict::Present=>")
                .expect("the next arm")
                + call_end;
            assert_eq!(
                &job[call_end..next_arm],
                "}",
                "{job_name}: the settled arm ends at the settle's call (nothing persisted or logged after its delivery)"
            );
            if job_name.starts_with("pub async fn refusal_job") {
                let last = job
                    .rfind("COUNTER_ARC_INGEST_REFUSAL_UNCORROBORATED,1).await;")
                    .expect("the last arm's count")
                    + "COUNTER_ARC_INGEST_REFUSAL_UNCORROBORATED,1).await;".len();
                assert_eq!(
                    &job[last..],
                    tail_after_match,
                    "the refusal job's match is its last statement"
                );
            } else {
                let uncertain = job[call_end..]
                    .find("EvidenceVerdict::Uncertain(why)=>")
                    .unwrap()
                    + call_end;
                let orphan = job[uncertain..]
                    .find("crate::broadcaster::WitnessLook::Orphan(_)")
                    .unwrap()
                    + uncertain;
                assert!(
                    job[uncertain..orphan].ends_with("}}return;}"),
                    "the watch returns right after the evidence match: its settled path reaches no later count or log"
                );
            }
        }
    }
}
