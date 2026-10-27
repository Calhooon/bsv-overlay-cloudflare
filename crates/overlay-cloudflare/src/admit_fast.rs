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

pub use crate::eviction_write::{
    EVIDENCE_INSERT_SQL, MISSING_INSERT_SQL, RELEASED_GAME_READ_SQL, RELEASED_HOP_READ_SQL,
    SEAT_KEYS_SQL,
};

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

/// P0-2d (bsv-stack-lean #52): through the one reading of Arcade's words
/// (bsv-tracker's since #624), the gate's own reading: seen, in a block
/// (STUMP_PROCESSING) or mined triggers the live read; a refusal or the
/// orphan view (SEEN_IN_ORPHAN_MEMPOOL) is corroborated; pending, parked and
/// an undefined word do nothing.
///
/// bsv-low #619 (L3): the push's word as the tracker's hint, the action the
/// host's policy over it (`door_words::callback`).
pub fn callback_action(tx_status: &str) -> CallbackAction {
    crate::door_words::callback(crate::door_words::arcade_hint(tx_status, "", 0).as_ref())
}

/// bsv-low #836 fold (the lens's L1): whether a push names Arcade's non-final word. Counted only
/// (`arc_ingest_lock_time_not_final_total`): the push itself is [`callback_action`]'s, the evidence check, whose LIVE
/// look decides (a real non-final word refuses nothing there while the chain's bound holds), so a planted text buys
/// nothing. The bearer is the public subject txid.
pub fn push_names_lock_time(tx_status: &str, extra_info: &str) -> bool {
    crate::door_words::lock_time_not_final(tx_status, extra_info).is_some()
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

/// The name of `table`'s shadow twin, which holds its evicted rows.
pub fn shadow_table(table: &str) -> String {
    format!("{table}_evicted")
}

/// One `PRAGMA table_info` row.
#[derive(Debug, Clone, serde::Deserialize, PartialEq, Eq)]
pub struct ColumnInfo {
    /// The column's name.
    pub name: String,
    /// The column's declared type.
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
pub(crate) async fn ensure_shadow(
    db: &dyn ShadowDb,
    table: &str,
    cols: &[ColumnInfo],
) -> Result<(), String> {
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
    /// The table whose row the pointer was on.
    pub table: String,
    /// The txid of the outpoint the row keys.
    pub txid: String,
    /// The outpoint's output index.
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
    // bsv-low #838: the guard's term is UNINDEXED (`+`), so the planner searches `idx_pot_spending` by the
    // spender. Without it SQLite chose `idx_pot_records_confirmed_height (spentConfirmed=?)` and walked every
    // unconfirmed pot (loop 44's door answer `maxReads=7895`); the read beside it was planned by the spender.
    let guard = if has("spentConfirmed") {
        " AND +spentConfirmed = 0"
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
async fn count_keyed_checked(
    db: &D1Database,
    table: &str,
    key: &str,
    txid: &str,
) -> Result<u64, String> {
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
    /// The columns of `table`, as `PRAGMA table_info` reports them.
    async fn columns(&self, table: &str) -> Result<Vec<ColumnInfo>, String>;
    /// How many rows of `table` carry `txid` in the column `key`.
    async fn count(&self, table: &str, key: &str, txid: &str) -> Result<u64, String>;
    /// The `outputIndex` values the keyed rows carry (asked only of the tables that have the column).
    async fn vouts(&self, table: &str, txid: &str) -> Result<Vec<u32>, String>;
    /// Run one write with its binds.
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
            report.incomplete.push(format!(
                "{where_}: {e} (no verification pass: a readmission landed under this pass)"
            ));
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
                            Ok(left) => {
                                Err(format!("{left} row(s) still present after the second move"))
                            }
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
                    report.retried.push(format!(
                        "{where_}: first pass {e}; the verification pass found it clean"
                    ));
                }
            }
            Err(e) => report.incomplete.push(format!("{where_}: {e}")),
        }
    }
    // a first-pass fault on a table the verification pass never saw (its columns were unreadable in pass 1)
    for (where_, e) in pass1_faults {
        if !moved_tables
            .iter()
            .any(|(t, k, _)| format!("{t} by {k}") == where_)
        {
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

/// A clean readmission supersedes released-hop evidence through the ledger, preserving every evidence row.
pub const READMIT_EVICTION_SQL: &str =
    "UPDATE pot_evictions SET readmittedAt = ? WHERE txid = ? AND readmittedAt IS NULL";

/// An eviction the ledger holds OPEN (not readmitted): what every admission writer must honour.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenEviction {
    /// Why the txid was evicted.
    pub reason: String,
    /// When it was evicted, in milliseconds.
    pub evicted_at_ms: u64,
}

/// bsv-low #637: the index's two words that it holds a subject's admission NOW (the #413 belt's statements in
/// `routes.rs`): its `transactions` row, else its `pot_records` row (a pot whose last output a spend consumed keeps
/// its pot row and loses its `transactions` row, the engine's `delete_output`).
pub(crate) const STANDING_ADMISSION_SQL: [&str; 2] = [
    "SELECT 1 AS one FROM transactions WHERE txid = lower(?)",
    "SELECT 1 AS one FROM pot_records WHERE txid = lower(?) LIMIT 1",
];

#[derive(serde::Deserialize)]
struct StandingRow {
    #[allow(dead_code)]
    one: f64,
}

/// bsv-low #637: does the index hold `txid`'s admission now? Asked only by the witnessed mode's duplicate echo
/// whose claim's corroboration was inconclusive ([`crate::broadcaster::duplicate_of_a_standing_admission`]), at most
/// two key reads; a read that faults is "no" (the 502 stands, the conservative answer).
pub(crate) async fn index_holds_admission(db: &D1Database, txid: &str) -> bool {
    for sql in STANDING_ADMISSION_SQL {
        match Query::new(sql).bind(txid).fetch_optional::<StandingRow>(db).await {
            Ok(Some(_)) => return true,
            Ok(None) => {}
            Err(_) => return false,
        }
    }
    false
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
                settle_reevicted(db, txid, &outcome, &ev.reason, origin).await;
                crate::ops::bump_counter(
                    db,
                    crate::ops::COUNTER_ADMIT_FAST_REEVICTED_AFTER_WRITE,
                    1,
                )
                .await;
                worker::console_log!(
                    "{origin}: {txid}, landed first into {topic} from a successor's BEEF, is under an OPEN eviction (at {} ms — {}): re-evicted {} row(s)",
                    ev.evicted_at_ms,
                    ev.reason,
                    outcome.moved
                );
            }
            Ok(None) => {}
            Err(e) => {
                crate::ops::bump_counter(db, crate::ops::COUNTER_ADMIT_FAST_LEDGER_UNREADABLE, 1)
                    .await;
                worker::console_log!(
                    "{origin}: the eviction ledger could not be read for {txid}, landed first into {topic} ({e}); not asked again"
                );
            }
        }
    }
}

/// bsv-low #920: the memo a re-eviction after a write reads before it settles: the kind of the txid's `/tx-any`
/// verdict memo, if any.
pub const REEVICTED_MEMO_KIND_SQL: &str = "SELECT kind FROM tx_any_verdicts WHERE txid = ?";

/// PURE (bsv-low #920): does a re-eviction after a write still owe the refusal's memo? Never over a pass that
/// yielded to a readmission (the chain's word stands, as `settle_corroborated_refusal` reads it); never over a
/// memo a settle already wrote (`refused`, the retire pass's `retired`, the request-time `unconfirmable` the upsert
/// keeps). An unreadable memo is owed: the upsert is idempotent, and a missing memo is the loss #920 measured.
pub fn reevicted_owes_memo(yielded: bool, kind: Option<&str>) -> bool {
    !yielded && !matches!(kind, Some("refused" | "retired" | "unconfirmable"))
}

/// What the write-side guard's settle touches after its re-eviction (bsv-low #920): D1 in production, real SQLite
/// under the pins (`settle_reevicted_with`).
pub(crate) trait ReevictedPort {
    /// The kind of the txid's verdict memo (`REEVICTED_MEMO_KIND_SQL`).
    async fn memo_kind(&self) -> Result<Option<String>, String>;
    /// The memo and its write counter, one batch.
    async fn commit(&self, queries: Vec<Query>) -> Result<(), String>;
    fn log(&self, line: String);
}

/// THE WRITE-SIDE GUARD'S SETTLE (bsv-low #920, L51 pair 17, JOIN `55ee1d0a…`): after a write that outran an open
/// eviction has been re-evicted, the refusal's `/tx-any` memo is written when no settle wrote it. Before #920 the
/// guard moved the rows and counted `admit_fast_reevicted_after_write_total`, but only the refusal's own job (the
/// `/arc-ingest` push, the pending watch) wrote the memo, AFTER its eviction: when that job ran while the admission's
/// write was still in flight (#838's past-deadline landing, 20 s under the fleet's D1 queue) it found no rows, and a
/// job the platform cut after its OPEN marker left the marker and nothing else; the landing then re-evicted with no
/// memo, and the subject read as unknown until a re-present aged past the floor. The landing IS the trigger: the
/// open marker on the ledger is how a refusal in flight reaches the write it outran. The memo's reason is the
/// ledger's own (the evidence's words); its write counter is the memo's, the eviction's count stays the caller's.
pub(crate) async fn settle_reevicted_with<P: ReevictedPort>(
    port: &P,
    txid: &str,
    now_ms: u64,
    outcome: &EvictionOutcome,
    reason: &str,
    origin: &str,
) {
    if outcome.yielded {
        return;
    }
    let kind = match port.memo_kind().await {
        Ok(kind) => kind,
        Err(e) => {
            port.log(format!(
                "{origin}: the verdict memo could not be read for {txid} after its re-eviction ({e}): written"
            ));
            None
        }
    };
    if !reevicted_owes_memo(outcome.yielded, kind.as_deref()) {
        return;
    }
    let queries = vec![
        Query::new(crate::proof_fetcher::TX_ANY_VERDICT_UPSERT_SQL)
            .bind(txid)
            .bind(now_ms)
            .bind("refused")
            .bind(reason),
        Query::new(crate::ops::BUMP_COUNTER_SQL)
            .bind(crate::applayer_counters::COUNTER_TXANY_MEMO_WRITE_REFUSED)
            .bind(1),
    ];
    match port.commit(queries).await {
        Ok(()) => port.log(format!(
            "{origin}: {txid} re-evicted after its write: the refusal's memo written ({reason}; bsv-low #920)"
        )),
        Err(e) => port.log(format!(
            "{origin}: {txid} re-evicted after its write: the refusal's memo FAILED ({e})"
        )),
    }
}

struct D1Reevicted<'a> {
    db: &'a D1Database,
    txid: &'a str,
}

impl ReevictedPort for D1Reevicted<'_> {
    async fn memo_kind(&self) -> Result<Option<String>, String> {
        #[derive(serde::Deserialize)]
        struct Kind {
            kind: Option<String>,
        }
        Ok(Query::new(REEVICTED_MEMO_KIND_SQL)
            .bind(self.txid)
            .fetch_optional::<Kind>(self.db)
            .await?
            .and_then(|k| k.kind))
    }
    async fn commit(&self, queries: Vec<Query>) -> Result<(), String> {
        let statements = queries
            .iter()
            .map(|query| query.prepare(self.db))
            .collect::<Result<Vec<_>, _>>()?;
        crate::d1_ledger::counted_batch(self.db, statements)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }
    fn log(&self, line: String) {
        worker::console_log!("{line}");
    }
}

/// [`settle_reevicted_with`] over D1: called by every write-side guard right after its `evict_txid_everywhere`
/// (the request's, the past-deadline landing's, the queue replay's, a landed predecessor's, a resumed run's) and by
/// the Rejected arm's re-run under an open eviction.
pub async fn settle_reevicted(
    db: &D1Database,
    txid: &str,
    outcome: &EvictionOutcome,
    reason: &str,
    origin: &str,
) {
    let txid = txid.to_ascii_lowercase();
    let port = D1Reevicted { db, txid: &txid };
    let now_ms = worker::Date::now().as_millis();
    settle_reevicted_with(&port, &txid, now_ms, outcome, reason, origin).await;
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
    /// Rows moved by both passes.
    pub moved: u64,
    /// Everything that leaves the eviction INCOMPLETE (a faulted marker, the verification pass's verdicts, a
    /// faulted restore when yielding).
    pub faults: Vec<String>,
    /// Faults of the opening pass that the verification pass healed.
    pub retried: Vec<String>,
    /// What the verification pass found and did, for the caller's log.
    pub notes: Vec<String>,
    /// The pot outpoints the moves carried.
    pub pot_vouts: Vec<u32>,
    /// The lobby adverts the moves carried.
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
pub(crate) async fn readmitted_after(
    db: &dyn ShadowDb,
    txid: &str,
    now_ms: u64,
) -> Result<Option<i64>, String> {
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
    evict_core_with_abort(db, txid, reason, now_ms, None)
        .await
        .0
}

async fn evict_core_with_abort(
    db: &dyn ShadowDb,
    txid: &str,
    reason: &str,
    now_ms: u64,
    abort_rows: Option<(
        &dyn crate::admission_abort::AbortRows,
        crate::admission_abort::AbortingManagers,
    )>,
) -> (EvictionCore, Option<crate::admission_abort::PendingAbort>) {
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
            core.notes.push(format!(
                "a readmission at {at} ms stands over this pass ({now_ms} ms): nothing moved"
            ));
            core.yielded_to_readmission = Some(at);
            return (core, None);
        }
        Ok(None) => {}
        Err(e) => core.notes.push(format!(
            "the readmission read faulted before the passes ({e}) — the passes run"
        )),
    }
    // The marker must stand before the abort snapshots any admission rows.
    let abort = match abort_rows {
        Some((rows, managers)) => {
            Some(crate::admission_abort::read_before_eviction(rows, txid, managers).await)
        }
        None => None,
    };
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
            core.restore_faults = faults
                .iter()
                .map(|f| format!("yielding to the readmission: {f}"))
                .collect();
            core.faults.extend(core.restore_faults.iter().cloned());
            core.yielded_to_readmission = Some(at);
        }
        Ok(None) => {}
        Err(e) => core.faults.push(format!(
            "the readmission read faulted after the passes ({e})"
        )),
    }
    (core, abort)
}

/// What an eviction did, for its callers.
pub struct EvictionOutcome {
    /// Rows moved by the passes.
    pub moved: u64,
    /// The pass yielded to a readmission (the chain's word stands; the pot is served): the callers write no
    /// `refused` memo over it and count no eviction (round 3 of the gate).
    pub yielded: bool,
    /// bsv-low #831 (the FIX-831-B lens's LOW-2): the pass left work undone, a row the passes could not move or
    /// read back, or the release of the spend pointers faulted (the hops may still name the evicted txid). Logged
    /// and counted `admit_fast_evict_incomplete_total` here as before; a caller that counts endings counts this one
    /// as a fault, never an ending.
    pub incomplete: bool,
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
    let (core, abort, released) =
        crate::eviction_write::evict(db, &txid, reason, now_ms, repointed).await;
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
    // The move, releasedSpends, close and evidence have committed together before any manager is told.
    // Finish the ledger close before awaiting a manager (IDENT-ENGINE-LENS LOW-1).
    // the reference tells its managers `abortAdmissibleOutputs` when a `current-tx` broadcast throws after they
    // identified their outputs (`Engine.ts:1219-1223` at ts-stack f999e0c1a); this door learns the network's refusal
    // after the write, here: told once per evicted admission, never when the pass yielded to a readmission
    if core.yielded_to_readmission.is_none() {
        if let Some(abort) = abort.as_ref() {
            if abort.skipped_empty_beef() {
                crate::ops::bump_counter(db, crate::ops::COUNTER_ADMISSION_ABORT_EMPTY_BEEF, 1)
                    .await;
            }
            crate::admission_abort::tell(abort).await;
        }
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
                if let Err(e) =
                    Query::new("UPDATE pot_evictions SET readmittedAt = NULL WHERE txid = ?")
                        .bind(txid.as_str())
                        .execute(db)
                        .await
                {
                    worker::console_log!(
                        "[admit-fast] evict {txid}: re-opening after a faulted yield failed ({e})"
                    );
                }
                crate::ops::bump_counter(db, crate::ops::COUNTER_ADMIT_FAST_READMIT_INCOMPLETE, 1)
                    .await;
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
        incomplete: !core.faults.is_empty(),
    }
}

#[derive(serde::Deserialize)]
struct EvictedRow {
    reason: String,
    #[serde(rename = "releasedSpends", default)]
    released_spends: Option<String>,
}

/// bsv-low #451 (the second gate's HIGH-1): the `refused` verdict memo for an evicted txid — `/tx-any` serves it as
/// `present: false` with zero courier calls; a readmission deletes it (`readmit_if_evicted`). Fail-soft.
pub async fn write_refused_verdict(db: &D1Database, txid: &str, now_ms: i64, reason: &str) {
    match Query::new(crate::proof_fetcher::TX_ANY_VERDICT_UPSERT_SQL)
        .bind(txid)
        .bind(now_ms)
        .bind("refused")
        .bind(reason)
        .execute(db)
        .await
    {
        // bsv-low #626: the memo write counted by its kind (one per eviction).
        Ok(()) => {
            crate::ops::bump_counter(
                db,
                crate::applayer_counters::COUNTER_TXANY_MEMO_WRITE_REFUSED,
                1,
            )
            .await
        }
        Err(e) => worker::console_log!("[admit-fast] verdict memo write failed for {txid}: {e}"),
    }
}

/// bsv-low #837 (FIX-837): the D1 wiring of [`overlay_discovery::pot::lowfund_topic_manager::record_join_hop_spends`]
/// for a road that puts a pot in the index without the door's `tm_lowfund` judgement of its JOIN (a readmission, the
/// operator's `/admin/readmit`, a re-present the engine dedups): the held JOIN's hop rows are marked spent by it from
/// its own stored bytes. Fail-soft and idempotent (a txid that is no held JOIN reads its stored bytes and stops);
/// logged only when it marks a hop or faults. The count of hops marked.
pub async fn record_join_hop_spends_on(db: &D1Database, txid: &str, road: &str) -> usize {
    use worker::wasm_bindgen::JsCast;
    // a second handle on the same binding (the store owns an `Rc`; `D1Database` is not `Clone`)
    let handle = D1Database::unchecked_from_js(db.as_ref().clone());
    let store = crate::d1_discovery::D1PotStorage::new(std::rc::Rc::new(handle));
    let got =
        overlay_discovery::pot::lowfund_topic_manager::record_join_hop_spends(&store, txid).await;
    if !got.marked.is_empty() || got.faults > 0 || got.deferred > 0 {
        worker::console_log!(
            "[join-hops] {road} {txid}: {} hop spend(s) recorded from the JOIN's stored bytes ({}), {} already pointed, {} fault(s), {} input(s) read, {} past the bound left to the discovery pass",
            got.marked.len(),
            got.marked
                .iter()
                .map(|o| format!("{}:{}", o.txid, o.output_index))
                .collect::<Vec<_>>()
                .join(", "),
            got.already_spent,
            got.faults,
            got.examined,
            got.deferred
        );
    }
    got.marked.len()
}

/// If the txid is in the ledger as evicted and not yet readmitted, move every
/// twin row back and mark the readmission. `true` when a readmission happened.
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
    if let Err(e) = Query::new(READMIT_EVICTION_SQL)
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
                if let Err(e) =
                    Query::new("UPDATE pot_evictions SET readmittedAt = NULL WHERE txid = ?")
                        .bind(txid.as_str())
                        .execute(db)
                        .await
                {
                    worker::console_log!("[admit-fast] readmit {txid}: re-opening after the belt's faulted restore failed ({e})");
                }
                crate::ops::bump_counter(db, crate::ops::COUNTER_ADMIT_FAST_READMIT_INCOMPLETE, 1)
                    .await;
                return false;
            }
        }
    }
    // bsv-low #837: a readmitted JOIN's hops may never have been pointed at it (the twins hold what the eviction
    // moved, the released list what it had): record them from its own stored bytes, as the door's judgement would
    record_join_hop_spends_on(db, &txid, "readmit").await;
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
    /// What the indexers say of `txid`; `None` when the presence could not be read.
    async fn presence(&self, txid: &str) -> Option<crate::proof_fetcher::NetworkPresence>;
}

/// The production [`ConflictPresence`]: asks Bitails and WhatsOnChain and classifies the two answers.
pub struct CourierConflictPresence {
    /// The courier sources to ask.
    pub sources: crate::proof_fetcher::ProofSources,
    /// The WhatsOnChain API key, if one is configured.
    pub woc_api_key: Option<String>,
    /// bsv-low #834 (FIX-834-MERGE): the janitor pass's paced courier counters, when the pass runs in the janitor.
    pub(crate) counters: Option<crate::courier_counters::Counters>,
}

#[async_trait(?Send)]
impl ConflictPresence for CourierConflictPresence {
    async fn presence(&self, txid: &str) -> Option<crate::proof_fetcher::NetworkPresence> {
        // bsv-low #868: the slot beside WoC's node is Bitails' positive, else BananaBlocks' native view
        use crate::door_words::{courier_hint, WOC_SOURCE};
        let (source, b) = self
            .sources
            .mempool_witness_presence_counted(txid, self.counters.as_ref())
            .await;
        let w = self
            .sources
            .woc_presence_counted(self.woc_api_key.as_deref(), txid, self.counters.as_ref())
            .await;
        // bsv-low #619 (L3): the competitor's courier words as the tracker's hints, folded by the host's bar (a
        // courier's mined claim is #602's confirmation, D1's stated trust)
        Some(crate::door_words::courier_presence(
            &courier_hint(source, b, 0),
            &courier_hint(WOC_SOURCE, w, 0),
        ))
    }
}

/// What a conflict eviction did (bsv-low #649 M2): the pass either evicted, or YIELDED to a readmission that
/// landed under it (the chain's word stands, the pot is served). A yielded pass is neither logged nor counted as an
/// eviction: it does not stand.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictEvictOutcome {
    /// The eviction stands.
    Evicted,
    /// A readmission landed under the pass; the pot is served.
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
    /// Write `competitor` as the spender of the outpoint `outpoint_txid:vout` that `evicted` named; `Ok(true)` when the row now names the competitor ([`record_conflict_spender`]).
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
                    &conflict_eviction_reason(parent, vout, &competitor, height),
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
///
/// bsv-low #825 (B-FLEET-4): a pot the fault memo HOLDS ([`CONFLICT_FAULT_MEMO_PUT_SQL`]: at least
/// [`CONFLICT_FAULT_MEMO_BOUND`] consecutive faulted re-checks, held [`CONFLICT_FAULT_MEMO_HOLD_MS`] from the last)
/// is not drawn while its hold runs, so the eight places go to pots the couriers can answer. A hold never keeps out a
/// pot the index holds SEEN (`network_seen`) or funded (a proof in `pot_beefs` or `transactions`, `has_proof = 1`),
/// read here at the draw, whatever the memo says. `?1` now, in ms.
pub const CONFLICT_CANDIDATES_SQL: &str = "SELECT b.txid, hex(b.beef) AS beef FROM pot_beefs b \
     WHERE b.proof_verified = 0 AND EXISTS (SELECT 1 FROM pot_records p WHERE p.txid = b.txid) \
     AND NOT EXISTS (SELECT 1 FROM conflict_fault_memo m WHERE m.txid = b.txid AND m.held_until > ?1 \
       AND b.has_proof = 0 \
       AND NOT EXISTS (SELECT 1 FROM network_seen s WHERE s.txid = b.txid) \
       AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.txid = b.txid AND t.has_proof = 1)) \
     ORDER BY RANDOM() LIMIT 8";

/// bsv-low #825 (B-FLEET-4): the conflict pass's per-pot fault memo, one row per pot whose LAST re-check faulted:
/// its consecutive faults, the first and last fault's time (ms), the hold's end (`held_until`, ms; NULL until the
/// bound is met), how many holds were armed, and the last fault's words (at most 160 characters). Transient: a lost
/// row costs the pot [`CONFLICT_FAULT_MEMO_BOUND`] more faulted draws before it is held again.
pub const CONFLICT_FAULT_MEMO_CREATE: &str = "CREATE TABLE IF NOT EXISTS conflict_fault_memo (txid TEXT PRIMARY KEY, faults INTEGER NOT NULL, first_fault_at INTEGER NOT NULL, last_fault_at INTEGER NOT NULL, held_until INTEGER, holds INTEGER NOT NULL DEFAULT 0, last_fault TEXT)";

/// bsv-low #825: consecutive faulted re-checks after which a pot is held out of the draw. A clean re-check (no
/// fault, evicted or not) deletes the row, so the count is CONSECUTIVE.
pub const CONFLICT_FAULT_MEMO_BOUND: i64 = 3;

/// bsv-low #825: how long a hold keeps a pot out of the draw, from the fault that armed it: 24 h. Past it the pot is
/// drawn again; a fault there re-arms the hold at once (the count is past the bound), a clean re-check ends it. So a
/// void pot costs one courier re-check a day after its first three, where it cost one every few ticks.
pub const CONFLICT_FAULT_MEMO_HOLD_MS: i64 = 24 * 60 * 60 * 1000;

/// bsv-low #825: a row whose last fault is older than this and whose hold has ended is swept (7 days): the pot left
/// the candidate set (proven, evicted) or stopped faulting without a clean re-check deleting its row.
pub const CONFLICT_FAULT_MEMO_SWEEP_AFTER_MS: i64 = 7 * 24 * 60 * 60 * 1000;

/// bsv-low #825: rows the sweep deletes at most a pass.
pub const CONFLICT_FAULT_MEMO_SWEEP_LIMIT: i64 = 64;

/// bsv-low #825: one faulted re-check noted. `?1` txid, `?2` now (ms), `?3` [`CONFLICT_FAULT_MEMO_BOUND`], `?4`
/// [`CONFLICT_FAULT_MEMO_HOLD_MS`], `?5` the fault's words. The hold is armed when the consecutive count meets the
/// bound AND the index holds the pot neither SEEN nor funded (the draw reads the same again). Returns the row.
pub const CONFLICT_FAULT_MEMO_PUT_SQL: &str = "INSERT INTO conflict_fault_memo \
     (txid, faults, first_fault_at, last_fault_at, held_until, holds, last_fault) \
     VALUES (?1, 1, ?2, ?2, \
       CASE WHEN 1 >= ?3 \
         AND NOT EXISTS (SELECT 1 FROM network_seen WHERE txid = ?1) \
         AND NOT EXISTS (SELECT 1 FROM pot_beefs WHERE txid = ?1 AND has_proof = 1) \
         AND NOT EXISTS (SELECT 1 FROM transactions WHERE txid = ?1 AND has_proof = 1) \
       THEN ?2 + ?4 END, \
       CASE WHEN 1 >= ?3 \
         AND NOT EXISTS (SELECT 1 FROM network_seen WHERE txid = ?1) \
         AND NOT EXISTS (SELECT 1 FROM pot_beefs WHERE txid = ?1 AND has_proof = 1) \
         AND NOT EXISTS (SELECT 1 FROM transactions WHERE txid = ?1 AND has_proof = 1) \
       THEN 1 ELSE 0 END, ?5) \
     ON CONFLICT(txid) DO UPDATE SET \
       faults = conflict_fault_memo.faults + 1, \
       last_fault_at = excluded.last_fault_at, \
       last_fault = excluded.last_fault, \
       held_until = CASE WHEN conflict_fault_memo.faults + 1 >= ?3 \
         AND NOT EXISTS (SELECT 1 FROM network_seen WHERE txid = ?1) \
         AND NOT EXISTS (SELECT 1 FROM pot_beefs WHERE txid = ?1 AND has_proof = 1) \
         AND NOT EXISTS (SELECT 1 FROM transactions WHERE txid = ?1 AND has_proof = 1) \
       THEN ?2 + ?4 ELSE conflict_fault_memo.held_until END, \
       holds = conflict_fault_memo.holds + CASE WHEN conflict_fault_memo.faults + 1 >= ?3 \
         AND NOT EXISTS (SELECT 1 FROM network_seen WHERE txid = ?1) \
         AND NOT EXISTS (SELECT 1 FROM pot_beefs WHERE txid = ?1 AND has_proof = 1) \
         AND NOT EXISTS (SELECT 1 FROM transactions WHERE txid = ?1 AND has_proof = 1) \
       THEN 1 ELSE 0 END \
     RETURNING faults, held_until";

/// bsv-low #825: a clean re-check ends the pot's run of faults. `?1` txid. The memo's row only: never the pot.
pub const CONFLICT_FAULT_MEMO_CLEAR_SQL: &str = "DELETE FROM conflict_fault_memo WHERE txid = ?1";

/// bsv-low #825: the memo's bounded sweep, at most [`CONFLICT_FAULT_MEMO_SWEEP_LIMIT`] rows a pass. `?1` now less
/// [`CONFLICT_FAULT_MEMO_SWEEP_AFTER_MS`], `?2` now, `?3` the limit. The memo's rows only: never a pot.
pub const CONFLICT_FAULT_MEMO_SWEEP_SQL: &str = "DELETE FROM conflict_fault_memo WHERE txid IN \
     (SELECT txid FROM conflict_fault_memo WHERE last_fault_at < ?1 \
       AND (held_until IS NULL OR held_until <= ?2) LIMIT ?3)";

/// bsv-low #825: the memo on `/health/invariants.conflictPass.faultMemo`. `?1` now (ms). `held` counts the rows
/// whose hold runs and that the draw keeps out (neither SEEN nor funded), `nextReleaseAt` the soonest hold's end.
pub const CONFLICT_FAULT_MEMO_HEALTH_SQL: &str = "SELECT COUNT(*) AS rows, \
     COALESCE(SUM(CASE WHEN m.held_until > ?1 \
       AND NOT EXISTS (SELECT 1 FROM network_seen s WHERE s.txid = m.txid) \
       AND NOT EXISTS (SELECT 1 FROM pot_beefs b WHERE b.txid = m.txid AND b.has_proof = 1) \
       AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.txid = m.txid AND t.has_proof = 1) \
       THEN 1 ELSE 0 END), 0) AS held, \
     MIN(CASE WHEN m.held_until > ?1 THEN m.held_until END) AS next_release_at, \
     COALESCE(MAX(m.faults), 0) AS max_faults \
     FROM conflict_fault_memo m";

/// bsv-low #825: what one faulted re-check's memo write answered: `Some(held_until)` when this write ARMED a hold.
pub fn conflict_hold_armed(now_ms: i64, held_until: Option<i64>) -> Option<i64> {
    held_until.filter(|h| *h == now_ms + CONFLICT_FAULT_MEMO_HOLD_MS)
}

/// bsv-low #825: the fault's words as the memo keeps them (at most 160 characters, on a char boundary).
pub fn conflict_fault_words(fault: &str) -> String {
    fault.chars().take(160).collect()
}

/// #602 fold (MEDIUM-1): what one conflict pass did, counted and on the tick's line.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ConflictPassSummary {
    /// Candidates the pass re-checked.
    pub scanned: u64,
    /// Candidates it evicted.
    pub evicted: u64,
    /// Re-checks that faulted (retried next pass).
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
    /// bsv-low #825: note the re-check's outcome in the pot's fault memo ([`CONFLICT_FAULT_MEMO_PUT_SQL`] on a
    /// fault, [`CONFLICT_FAULT_MEMO_CLEAR_SQL`] on a clean re-check), before its counters are persisted.
    async fn memo(&self, _txid: &str, _result: &Result<bool, String>) {}
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
        port.memo(&txid, &result).await;
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
    async fn memo(&self, txid: &str, result: &Result<bool, String>) {
        note_conflict_fault_memo(
            self.db,
            txid,
            result,
            worker::Date::now().as_millis() as i64,
        )
        .await;
    }
    async fn persist(&self, deltas: &ConflictPassSummary) {
        bump_conflict_counters(self.db, deltas).await;
    }
    /// The pass's notes are delivered by its caller's own flush at its end (`lib.rs`: the scheduled tick's last
    /// `pot_changes::flush`, and the dispatch's one exit for `/admin/complete-proofs`); this pass holds no `Env`.
    async fn flush(&self) {}
}

/// One confirmed-conflict pass over up to eight unconfirmed live pots ([`CONFLICT_CANDIDATES_SQL`]): each re-checked against the couriers and evicted when a confirmed competitor conflicts with it.
/// bsv-low #834 (FIX-834-MERGE): the conflict pass's courier fetcher, its own 24-call budget and circuit, its asks
/// through the janitor pass's ONE pacer when `counters` is given (the scheduled tick and `/admin/complete-proofs`):
/// before it the pass built an unpaced fetcher, so on the merged main (#837's JOINs giving it inputs to read) a WoC
/// answering 429 was asked three more times by this fetcher's own circuit after the pass had backed WoC off, and
/// pruned Bitails' per-output non-answers were asked for every candidate (the budget tier's `janitor-pass-woc-429`:
/// 24 asks past the pacer, WoC 6 and Bitails 24 where the paced pass makes 3 and 3).
pub(crate) fn conflict_pass_fetcher(
    sources: crate::proof_fetcher::ProofSources,
    woc_api_key: Option<&str>,
    counters: Option<crate::courier_counters::Counters>,
) -> crate::proof_fetcher::ChainProofFetcher {
    let fetcher = crate::proof_fetcher::ChainProofFetcher::new(None)
        .with_sources(sources)
        .with_woc_api_key(woc_api_key.map(str::to_owned))
        .with_budget(24);
    match counters {
        Some(counters) => fetcher.with_courier_counters(counters),
        None => fetcher,
    }
}

pub(crate) async fn confirmed_conflict_pass(
    db: &D1Database,
    woc_api_key: Option<&str>,
    sources: crate::proof_fetcher::ProofSources,
    counters: Option<crate::courier_counters::Counters>,
) -> ConflictPassSummary {
    #[derive(serde::Deserialize)]
    struct Candidate {
        txid: String,
        beef: Option<String>,
    }
    let now = worker::Date::now().as_millis() as i64;
    // bsv-low #825: the memo's bounded sweep, then the draw that leaves the held pots out
    if let Err(e) = Query::new(CONFLICT_FAULT_MEMO_SWEEP_SQL)
        .bind(now - CONFLICT_FAULT_MEMO_SWEEP_AFTER_MS)
        .bind(now)
        .bind(CONFLICT_FAULT_MEMO_SWEEP_LIMIT)
        .execute(db)
        .await
    {
        worker::console_log!("[conflict] fault memo sweep deferred: {e}");
    }
    let rows = match Query::new(CONFLICT_CANDIDATES_SQL)
        .bind(now)
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
        fetcher: conflict_pass_fetcher(sources, woc_api_key, counters.clone()),
        presence: CourierConflictPresence {
            sources,
            woc_api_key: woc_api_key.map(str::to_owned),
            counters,
        },
    };
    run_conflict_candidates(&port, rows.into_iter().map(|r| (r.txid, r.beef)).collect()).await
}

/// bsv-low #825: one re-check noted in the pot's fault memo. A fault adds one to the pot's consecutive count and
/// arms the hold at the bound (counted `admit_fast_conflict_held_total`, logged once an arming); a clean re-check
/// deletes the memo's row. A memo fault is logged and swallowed: the pot is drawn as before.
async fn note_conflict_fault_memo(
    db: &D1Database,
    txid: &str,
    result: &Result<bool, String>,
    now: i64,
) {
    #[derive(serde::Deserialize)]
    struct Put {
        faults: f64,
        held_until: Option<f64>,
    }
    match result {
        Ok(_) => {
            if let Err(e) = Query::new(CONFLICT_FAULT_MEMO_CLEAR_SQL)
                .bind(txid)
                .execute(db)
                .await
            {
                worker::console_log!("[conflict] {txid} fault memo clear failed: {e}");
            }
        }
        Err(fault) => {
            let put = Query::new(CONFLICT_FAULT_MEMO_PUT_SQL)
                .bind(txid)
                .bind(now)
                .bind(CONFLICT_FAULT_MEMO_BOUND)
                .bind(CONFLICT_FAULT_MEMO_HOLD_MS)
                .bind(conflict_fault_words(fault))
                .fetch_optional::<Put>(db)
                .await;
            match put {
                Ok(Some(row)) => {
                    if let Some(until) = conflict_hold_armed(now, row.held_until.map(|h| h as i64))
                    {
                        crate::ops::bump_counter(
                            db,
                            crate::ops::COUNTER_ADMIT_FAST_CONFLICT_HELD,
                            1,
                        )
                        .await;
                        worker::console_log!(
                            "[conflict] {txid} HELD out of the draw until {until}: {} consecutive faulted re-checks (bound {CONFLICT_FAULT_MEMO_BOUND})",
                            row.faults as i64
                        );
                    }
                }
                Ok(None) => {}
                Err(e) => worker::console_log!("[conflict] {txid} fault memo write failed: {e}"),
            }
        }
    }
}

/// bsv-low #825: `/health/invariants.conflictPass.faultMemo`: the memo's rows, the pots it holds out of the draw now,
/// the soonest hold's end, the longest run of faults, and its bounds. `readable: false` = the table is unreadable.
pub(crate) async fn conflict_fault_memo_health(db: &D1Database, now: i64) -> serde_json::Value {
    #[derive(serde::Deserialize)]
    struct Health {
        rows: f64,
        held: f64,
        next_release_at: Option<f64>,
        max_faults: f64,
    }
    let bounds = serde_json::json!({
        "faultsBeforeHold": CONFLICT_FAULT_MEMO_BOUND,
        "holdMs": CONFLICT_FAULT_MEMO_HOLD_MS,
        "sweepAfterMs": CONFLICT_FAULT_MEMO_SWEEP_AFTER_MS,
        "sweepLimit": CONFLICT_FAULT_MEMO_SWEEP_LIMIT,
        "neverHeld": "a pot the index holds SEEN (network_seen) or funded (has_proof)",
    });
    match Query::new(CONFLICT_FAULT_MEMO_HEALTH_SQL)
        .bind(now)
        .fetch_optional::<Health>(db)
        .await
    {
        Ok(h) => {
            let h = h.unwrap_or(Health {
                rows: 0.0,
                held: 0.0,
                next_release_at: None,
                max_faults: 0.0,
            });
            serde_json::json!({
                "readable": true,
                "rows": h.rows as i64,
                "held": h.held as i64,
                "nextReleaseAt": h.next_release_at.map(|v| v as i64),
                "maxConsecutiveFaults": h.max_faults as i64,
                "bounds": bounds,
            })
        }
        Err(e) => serde_json::json!({ "readable": false, "fault": e, "bounds": bounds }),
    }
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

/// PURE (bsv-low #740): a refusal on this look rests on ABSENCE, so it takes the age floor: Arcade does not hold
/// the txid, or could not be asked, or could not be asked and the RECORDED word filled it ([`ArcadeLook::Recorded`]:
/// a pushed status on the public `/arc-ingest` bearer, or a status the watch read earlier, is not Arcade's own word
/// now). Only Arcade's live fatal word ([`ArcadeLook::Fatal`]) refuses a young or never-admitted txid.
///
/// [`ArcadeLook::Recorded`]: crate::proof_fetcher::ArcadeLook::Recorded
/// [`ArcadeLook::Fatal`]: crate::proof_fetcher::ArcadeLook::Fatal
pub(crate) fn refusal_rests_on_absence(arcade: &crate::proof_fetcher::ArcadeLook) -> bool {
    use crate::proof_fetcher::ArcadeLook;
    matches!(
        arcade,
        ArcadeLook::Missing | ArcadeLook::Fault | ArcadeLook::Recorded(..)
    )
}

/// bsv-low #845: the evidence check's word when Arcade holds the txid below SEEN (RECEIVED, SENT_TO_NETWORK,
/// ACCEPTED_BY_NETWORK: [`ArcadeLook::Holding`]) and no indexer holds it: a hint that promotes nothing.
///
/// [`ArcadeLook::Holding`]: crate::proof_fetcher::ArcadeLook::Holding
pub const HOLDING_UNCERTAIN_REASON: &str =
    "arcade holds it below SEEN (a hint that promotes nothing): neither presence nor a refusal; both indexers absent";

/// bsv-low #836: the evidence check's word on Arcade's non-final REJECTED (TX_LOCK_TIME (35)) with no indexer
/// holding the txid: "not now", never a refusal; the peer's height, when the text names it, rides as a hint.
pub fn not_final_uncertain_reason(status: &str, extra_info: &str) -> String {
    let nf = crate::door_words::lock_time_not_final(status, extra_info);
    let heights = match nf.as_ref() {
        Some(n) => format!(
            " (lock height {}, Arcade's peer at {})",
            n.lock_height.map_or("?".into(), |h| h.to_string()),
            n.peer_height.map_or("?".into(), |h| h.to_string())
        ),
        None => String::new(),
    };
    format!(
        "arcade {status} TX_LOCK_TIME: not final against Arcade's peer's chain view{heights}, a time, never a verdict; held, not final; both indexers absent"
    )
}

/// PURE: fold the three couriers' words (pinned). `young`: the admission is younger than
/// `REFUSAL_ABSENT_MIN_AGE_MS`, or the index holds no admission at all. The pinned tables' entrance: each indexer as
/// a bare yes or no (`door_words::courier_of_bool`), judged by [`evidence_verdict_of`].
pub fn evidence_verdict(
    arcade: &crate::proof_fetcher::ArcadeLook,
    bitails: Option<bool>,
    woc: Option<bool>,
    young: bool,
) -> EvidenceVerdict {
    use crate::door_words::{courier_of_bool, BITAILS_SOURCE, WOC_SOURCE};
    evidence_verdict_of(
        arcade,
        &courier_of_bool(BITAILS_SOURCE, bitails),
        &courier_of_bool(WOC_SOURCE, woc),
        young,
    )
}

/// PURE (bsv-low #619, L3): the evidence check's verdict over Arcade's look (the host's policy over Arcade's hint,
/// `door_words::live_look`) and the two indexers' words as the tracker's hints (`door_words::courier_hint`), folded
/// by the host's two-source bar (`door_words::courier_presence`). The policy is the host's, unchanged: a refusal
/// needs two sources, an absence-based one the age floor.
pub fn evidence_verdict_of(
    arcade: &crate::proof_fetcher::ArcadeLook,
    bitails: &crate::door_words::CourierWord,
    woc: &crate::door_words::CourierWord,
    young: bool,
) -> EvidenceVerdict {
    use crate::proof_fetcher::{ArcadeLook, NetworkPresence};
    match arcade {
        ArcadeLook::Present => EvidenceVerdict::Present,
        // bsv-low #836: Arcade's LIVE non-final word, inside the chain's bound (`door_words::bound_look`), is a time,
        // never a refusal: only the indexers' presence speaks, and nothing is refused. A RECORDED lock-time word is
        // the public bearer's and reads as any recorded word (#740's floor; the lens's L1).
        ArcadeLook::NotFinal(status, extra) => {
            never_refused(bitails, woc, not_final_uncertain_reason(status, extra))
        }
        // bsv-low #845: Arcade's rank below SEEN is a hint that promotes nothing: never presence, never a refusal.
        ArcadeLook::Holding(_) => never_refused(bitails, woc, HOLDING_UNCERTAIN_REASON.to_string()),
        // A refusal needs TWO sources: Arcade's live word (fatal, or not
        // holding it) AND both indexers' definitive absence. One broadcaster's
        // REJECTED is never the network's verdict (#214: Arcade held txs
        // MINED in 958776 at a sticky REJECTED), and an indexer that holds
        // the tx is a witness against the refusal.
        ArcadeLook::Fatal(..)
        | ArcadeLook::Recorded(..)
        | ArcadeLook::Missing
        | ArcadeLook::Fault => {
            match crate::door_words::courier_presence(bitails, woc) {
                NetworkPresence::Present { .. } => EvidenceVerdict::Present,
                // Absence-based (Arcade missing or faulted, its recorded word included: not its own fatal word):
                // a young or never-admitted txid is UNCERTAIN (counted), never refused (the gate's HIGH-2; #740).
                NetworkPresence::Absent if young && refusal_rests_on_absence(arcade) =>
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
                    // bsv-low #621 (the W2-WORDS-L5 lens, MEDIUM-1): Arcade could not be asked and the recorded
                    // word (a public-bearer push) filled it: the same refusal, but its reason names its provenance,
                    // so the served chain word is no word (`eviction_chain_word`'s absence marks), never `rejected`
                    ArcadeLook::Recorded(status, extra) => {
                        format!("arcade faulted, recorded {status}: {extra}; both indexers absent")
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

/// PURE (bsv-low #836, #845): the verdict on a look that refuses nothing: an indexer that holds the txid is
/// presence; otherwise `Uncertain` (`why`, or a courier fault), never `Refused`.
fn never_refused(
    bitails: &crate::door_words::CourierWord,
    woc: &crate::door_words::CourierWord,
    why: String,
) -> EvidenceVerdict {
    use crate::proof_fetcher::NetworkPresence;
    match crate::door_words::courier_presence(bitails, woc) {
        NetworkPresence::Present { .. } => EvidenceVerdict::Present,
        NetworkPresence::Absent => EvidenceVerdict::Uncertain(why),
        NetworkPresence::Inconclusive => {
            EvidenceVerdict::Uncertain("a courier faulted".to_string())
        }
    }
}

/// PURE (bsv-low #621): the conflict pass's ledger reason. Its served chain word is `conflicted`
/// (`overlay_discovery::pot::storage::eviction_chain_word`, pinned).
pub fn conflict_eviction_reason(
    parent: &str,
    vout: u32,
    competitor: &str,
    height: impl std::fmt::Display,
) -> String {
    format!("input {parent}:{vout} spent by confirmed competitor {competitor} at {height}")
}

/// PURE (bsv-low #621): the door's ledger reason for a subject the ladder refused again under an open eviction
/// (Arcade's own refusal). Its served chain word is `rejected` (pinned).
pub fn ladder_rerefusal_reason(reason: &str) -> String {
    format!("REJECTED ({reason}; re-refused at the ladder)")
}

/// What the deferred evidence check needs (cloned into the `wait_until` job).
#[derive(Clone)]
pub struct EvidenceEnv {
    /// The Arcade base URL (`ARCADE_URL`, or the default).
    pub arcade_base: String,
    /// The WhatsOnChain API key, if one is configured.
    pub woc_api_key: Option<String>,
    /// The worker env (Clone): the job opens `OVERLAY_DB` itself (a D1 handle is not Clone).
    pub env: worker::Env,
}

impl EvidenceEnv {
    /// The evidence environment from the Worker's environment.
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
    let sources = crate::proof_fetcher::ProofSources::from_env(&env.env);
    let mut look = sources
        .arcade_look(&env.arcade_base, txid, Some(webhook))
        .await;
    // bsv-low #836 fold (the lens's M1): a non-final word past the chain's bound is Arcade's refusal again
    if matches!(look, crate::proof_fetcher::ArcadeLook::NotFinal(..)) {
        let tracker = crate::lookup_service_chain_tracker(&env.env);
        let bound = match env.env.d1("OVERLAY_DB") {
            Ok(db) => stored_lock_bound(tracker.as_deref(), &db, txid).await,
            Err(_) => crate::door_words::LockBound {
                lock: None,
                tip: crate::door_words::chain_tip(tracker.as_deref()).await,
                now_secs: crate::door_words::now_secs(),
            },
        };
        look = crate::door_words::bound_look(look, &bound);
    }
    // bsv-low #619 (L3): each indexer's answer as the tracker's hint from its source (its mined height kept on the
    // hint, where the bare yes or no dropped it; the verdict reads presence either way)
    let indexers = async {
        // bsv-low #868: the slot beside WoC's node is Bitails' positive, else BananaBlocks' native view
        use crate::door_words::{courier_hint, WOC_SOURCE};
        let (source, b) = sources.mempool_witness_presence_counted(txid, None).await;
        let w = sources.woc_presence(env.woc_api_key.as_deref(), txid).await;
        (courier_hint(source, b, 0), courier_hint(WOC_SOURCE, w, 0))
    };
    let age = async {
        match env.env.d1("OVERLAY_DB") {
            Ok(db) => {
                let now_ms = worker::Date::now().as_millis();
                admission_age_ms(&db, txid, now_ms).await
            }
            Err(_) => None, // no ledger to ask: the conservative word (young)
        }
    };
    verdict_of_look(&look, indexers, age).await
}

/// The evidence check after the look (bsv-low #740: the seam the pins run under real SQLite). `indexers` and
/// `admission_age` are LAZY: the indexers are asked only when Arcade does not hold the txid, the admission's age
/// only when the refusal would rest on absence. An unknown age (no admission, no ledger) is young.
pub(crate) async fn verdict_of_look(
    look: &crate::proof_fetcher::ArcadeLook,
    indexers: impl std::future::Future<
        Output = (
            crate::door_words::CourierWord,
            crate::door_words::CourierWord,
        ),
    >,
    admission_age: impl std::future::Future<Output = Option<u64>>,
) -> EvidenceVerdict {
    use crate::door_words::CourierWord;
    let (bitails, woc) = match look {
        // Arcade holds it: nothing to corroborate. Every other word (fatal,
        // missing, a fault) is one source; the indexers are the second.
        crate::proof_fetcher::ArcadeLook::Present => (CourierWord::Fault, CourierWord::Fault),
        _ => indexers.await,
    };
    let young = refusal_rests_on_absence(look)
        && admission_age
            .await
            .is_none_or(|age| age < REFUSAL_ABSENT_MIN_AGE_MS);
    evidence_verdict_of(look, &bitails, &woc, young)
}

/// bsv-low #836 fold (the lens's M1): where the subject's own bytes are read for its lock, in order: the engine's
/// store, then the pot store (`?` the txid, lowercase). The first BEEF that carries the subject decides.
pub(crate) const SUBJECT_BEEF_SQL: [&str; 2] = [
    "SELECT hex(beef) AS beef FROM transactions WHERE txid = ?",
    "SELECT hex(beef) AS beef FROM pot_beefs WHERE txid = ?",
];

#[derive(serde::Deserialize)]
struct SubjectBeefRow {
    beef: Option<String>,
}

/// bsv-low #836 fold (the lens's M1): what bounds a non-final word's hold for `txid`: its own lock from its stored
/// bytes (`SUBJECT_BEEF_SQL`; `None` when no store holds them or a read faults), the chaintracks tip (`None`: no
/// tracker, or a fault) and the clock. Asked only of a non-final look (at most two reads and one tip).
pub async fn stored_lock_bound(
    tracker: Option<&dyn bsv_rs::transaction::ChainTracker>,
    db: &worker::D1Database,
    txid: &str,
) -> crate::door_words::LockBound {
    let key = txid.to_ascii_lowercase();
    let mut lock = None;
    for sql in SUBJECT_BEEF_SQL {
        let row = crate::d1::Query::new(sql)
            .bind(key.as_str())
            .fetch_optional::<SubjectBeefRow>(db)
            .await;
        if let Some(found) = row
            .ok()
            .flatten()
            .and_then(|r| crate::d1::beef_of_hex_column(r.beef))
            .and_then(|beef| crate::door_words::SubjectLock::of_beef(&beef, &key))
        {
            lock = Some(found);
            break;
        }
    }
    crate::door_words::LockBound {
        lock,
        tip: crate::door_words::chain_tip(tracker).await,
        now_secs: crate::door_words::now_secs(),
    }
}

#[derive(serde::Deserialize)]
struct AgeRow {
    s: Option<f64>,
}

/// The admission's stamp, rung by rung (`?` the txid, lowercase), each with its unit's factor to ms: the engine's
/// own stamp (`outputs.score`, ms), the twin's for an EVICTED subject (#519, the delta-verify's observation: its
/// outputs live in the twin with the admission's own stamp; without this rung a hop or a marker the eviction moved
/// read "young" forever at the door), the pot row's `createdAt` (seconds), and a spender's BEEF first-store stamp
/// (seconds; a settle, a refund, a sweep admits no output and holds no pot row: the gate's NEW-6).
pub(crate) const ADMISSION_AGE_RUNGS: [(&str, f64); 4] = [
    ("SELECT MIN(score) AS s FROM outputs WHERE txid = ?", 1.0),
    ("SELECT MIN(score) AS s FROM outputs_evicted WHERE txid = ?", 1.0),
    ("SELECT MIN(createdAt) AS s FROM pot_records WHERE txid = ?", 1000.0),
    ("SELECT createdAt AS s FROM pot_beefs WHERE txid = ?", 1000.0),
];

/// How long ago the index admitted this txid (ms), from the first rung of [`ADMISSION_AGE_RUNGS`] that holds a
/// positive stamp; `None` when the index holds no admission. `read` asks one rung's statement (a fault, no row or a
/// NULL is no stamp).
pub(crate) async fn admission_age_over<F, Fut>(read: F, now_ms: u64) -> Option<u64>
where
    F: Fn(&'static str) -> Fut,
    Fut: std::future::Future<Output = Option<f64>>,
{
    for (sql, to_ms) in ADMISSION_AGE_RUNGS {
        if let Some(s) = read(sql).await {
            if s > 0.0 {
                return Some((now_ms as f64 - s * to_ms).max(0.0) as u64);
            }
        }
    }
    None
}

/// How long ago the index admitted this txid (ms), over D1 (`admission_age_over`, private).
pub async fn admission_age_ms(db: &D1Database, txid: &str, now_ms: u64) -> Option<u64> {
    let txid = txid.to_ascii_lowercase();
    let txid = txid.as_str();
    admission_age_over(
        |sql| async move {
            match Query::new(sql).bind(txid).fetch_optional::<AgeRow>(db).await {
                Ok(Some(AgeRow { s })) => s,
                _ => None,
            }
        },
        now_ms,
    )
    .await
}

/// What a corroborated refusal's settle does to the world, after the shadow move (bsv-low #649). A port so the
/// pins run the shipped [`settle_corroborated_refusal`] over a fake whose flush never answers.
pub(crate) trait RefusalSettle {
    /// Persist the verdict memo and both counters in one batch before delivery.
    async fn record(&self, counter: &'static str, reason: &str) -> Result<(), String>;
    /// One watch counter outside the atomic refusal record.
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
/// refusal job). Unless the pass yielded to a readmission: the refused-verdict memo and counters in ONE batch,
/// then the log; only then the flush (bsv-low #649). The flush waits on the app layer, which can outlast the
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
        port.log(lines.yielded);
    } else {
        match port.record(counter, memo_reason).await {
            Ok(()) => port.log(lines.evicted),
            Err(error) => port.log(format!(
                "[admit-fast] refusal facts for {counter} failed: {error}"
            )),
        }
    }
    port.flush().await;
}

// The smallest batch that retains the existing memo-write tally: the memo and the two counter rows.
fn refusal_fact_queries(
    txid: &str,
    now_ms: u64,
    counter: &'static str,
    reason: &str,
) -> [Query; 3] {
    [
        Query::new(crate::proof_fetcher::TX_ANY_VERDICT_UPSERT_SQL)
            .bind(txid)
            .bind(now_ms)
            .bind("refused")
            .bind(reason),
        Query::new(crate::ops::BUMP_COUNTER_SQL)
            .bind(counter)
            .bind(1),
        Query::new(crate::ops::BUMP_COUNTER_SQL)
            .bind(crate::applayer_counters::COUNTER_TXANY_MEMO_WRITE_REFUSED)
            .bind(1),
    ]
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
    async fn record(&self, counter: &'static str, reason: &str) -> Result<(), String> {
        let statements = refusal_fact_queries(self.txid, self.now_ms, counter, reason)
            .iter()
            .map(|query| query.prepare(self.db))
            .collect::<Result<Vec<_>, _>>()?;
        crate::d1_ledger::counted_batch(self.db, statements)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
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
    let arcade = crate::broadcaster::ArcadeBroadcaster::new(env.arcade_base.clone())
        .with_proof_sources(crate::proof_fetcher::ProofSources::from_env(&env.env));
    if let Some(at_ms) = arcade.network_seen_stamp(&txid).await {
        crate::ops::latch_network_seen(&db, &txid, crate::ops::SeenSource::Arcade, at_ms).await;
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

/// External boundaries of the pending watch. The production job and the cancellation pin run one loop.
trait PendingWatch: RefusalSettle {
    fn txid(&self) -> &str;
    async fn sleep(&self, ms: u64);
    async fn look(&self) -> crate::broadcaster::WitnessLook;
    async fn evidence(&self, webhook: (String, String)) -> EvidenceVerdict;
    async fn evict(&self, reason: &str) -> EvictionOutcome;
    async fn seen(&self, status: &str, at_ms: Option<i64>, admitted_at_ms: f64);
}

struct WorkerWatch<'a> {
    env: &'a EvidenceEnv,
    db: &'a D1Database,
    txid: &'a str,
    started_ms: u64,
    refusal_ms: std::cell::Cell<u64>,
    arcade: crate::broadcaster::ArcadeBroadcaster,
    tracker: Option<std::rc::Rc<dyn bsv_rs::transaction::ChainTracker>>,
}

impl WorkerWatch<'_> {
    fn settle(&self) -> WorkerSettle<'_> {
        WorkerSettle {
            db: self.db,
            env: &self.env.env,
            txid: self.txid,
            now_ms: self.refusal_ms.get(),
            started_ms: self.started_ms,
        }
    }
}

impl RefusalSettle for WorkerWatch<'_> {
    async fn record(&self, counter: &'static str, reason: &str) -> Result<(), String> {
        self.settle().record(counter, reason).await
    }
    async fn bump(&self, counter: &'static str) {
        self.settle().bump(counter).await;
    }
    async fn flush(&self) {
        self.settle().flush().await;
    }
    fn log(&self, line: String) {
        self.settle().log(line);
    }
}

impl PendingWatch for WorkerWatch<'_> {
    fn txid(&self) -> &str {
        self.txid
    }
    async fn sleep(&self, ms: u64) {
        crate::broadcaster::sleep_ms(ms).await;
    }
    async fn look(&self) -> crate::broadcaster::WitnessLook {
        let mut last = self.arcade.witness_look(self.txid).await;
        if let crate::broadcaster::WitnessLook::NotFinal(..) = &last {
            let bound = stored_lock_bound(self.tracker.as_deref(), self.db, self.txid).await;
            last = crate::broadcaster::bound_witness_look(last, &bound);
        }
        last
    }
    async fn evidence(&self, webhook: (String, String)) -> EvidenceVerdict {
        self.refusal_ms.set(worker::Date::now().as_millis());
        evidence_check(self.env, self.txid, webhook).await
    }
    async fn evict(&self, reason: &str) -> EvictionOutcome {
        evict_txid_everywhere(self.db, self.txid, reason, self.refusal_ms.get()).await
    }
    async fn seen(&self, status: &str, at_ms: Option<i64>, admitted_at_ms: f64) {
        crate::ops::latch_network_seen(self.db, self.txid, crate::ops::SeenSource::Arcade, at_ms)
            .await;
        let ms = (worker::js_sys::Date::now() - admitted_at_ms).max(0.0) as u64;
        crate::ops::bump_counter(self.db, crate::ops::COUNTER_SUBMIT_PENDING_SEEN_LATCHED, 1).await;
        crate::ops::bump_counter(self.db, crate::ops::COUNTER_SUBMIT_PENDING_SEEN_MS, ms).await;
        self.log(format!("[admit-fast] {} {status} {ms} ms after the admission; network_seen latched by the pending watch", self.txid));
        self.flush().await;
    }
}

async fn pending_watch_job_with<P: PendingWatch>(
    port: &P,
    admitted_at_ms: f64,
) -> Option<crate::broadcaster::WitnessLook> {
    let txid = port.txid();
    let mut last = crate::broadcaster::WitnessLook::Unknown;
    // bsv-low #836: a non-final look is counted once a watch
    let mut not_final_counted = false;
    for sleep in PENDING_WATCH_SLEEPS_MS {
        port.sleep(sleep).await;
        last = port.look().await;
        match &last {
            crate::broadcaster::WitnessLook::Seen(status, at_ms) => {
                port.seen(status, *at_ms, admitted_at_ms).await;
                return None;
            }
            crate::broadcaster::WitnessLook::Fatal(status, extra) => {
                match port.evidence((status.clone(), extra.clone())).await {
                    EvidenceVerdict::Refused(reason) => {
                        let outcome = port.evict(&format!("{status} ({reason})")).await;
                        let moved = outcome.moved;
                        // the second gate's HIGH-1: the verdict memo, written where the evidence is; never
                        // over a pass that yielded to a readmission (round 3 of the loop-18 gate). The eviction
                        // is the event the felt voids on: the settle ships its notes (the route's flush drained
                        // before the watch), AFTER the memo and the count (bsv-low #649).
                        settle_corroborated_refusal(
                            port,
                            outcome.yielded,
                            crate::ops::COUNTER_SUBMIT_PENDING_EVICTED,
                            &format!("{status} ({reason})"),
                            SettleLines {
                                no_memo: format!("[admit-fast] {txid} {status}: the eviction yielded to a readmission; no refused memo"),
                                yielded: format!("[admit-fast] {txid} {status} CORROBORATED ({reason}); the pass YIELDED to a readmission; nothing evicted by the pending watch"),
                                evicted: format!("[admit-fast] {txid} {status} CORROBORATED ({reason}); evicted {moved} row(s) by the pending watch"),
                            },
                        )
                        .await;
                    }
                    EvidenceVerdict::Present => {
                        port.bump(crate::ops::COUNTER_SUBMIT_PENDING_REFUSAL_UNCORROBORATED)
                            .await;
                        port.log(format!(
                            "[admit-fast] {txid} {status} NOT corroborated: a courier still holds it; kept (the #214 class)"
                        ));
                    }
                    EvidenceVerdict::Uncertain(why) => {
                        port.bump(crate::ops::COUNTER_SUBMIT_PENDING_REFUSAL_UNCORROBORATED)
                            .await;
                        port.log(format!(
                            "[admit-fast] {txid} {status} UNCERTAIN ({why}); kept, counted"
                        ));
                    }
                }
                return None;
            }
            crate::broadcaster::WitnessLook::Orphan(_)
            | crate::broadcaster::WitnessLook::Pending(_)
            | crate::broadcaster::WitnessLook::Unknown => {}
            // bsv-low #836: Arcade's non-final REJECTED (TX_LOCK_TIME (35)): held, not final. No evidence check,
            // no eviction; the watch keeps looking (a later look may be SEEN) and ends with the subject unlatched;
            // the callbacks and the passes re-look after the lock height passes.
            crate::broadcaster::WitnessLook::NotFinal(status, _, nf) => {
                if !not_final_counted {
                    not_final_counted = true;
                    port.bump(crate::ops::COUNTER_SUBMIT_PENDING_LOCK_TIME_NOT_FINAL)
                        .await;
                    port.log(watch_not_final_line(txid, status, nf));
                }
            }
        }
    }
    Some(last)
}

/// The PENDING WATCH behind a fast (or #397 pending) admission: the witness
/// that used to be polled on the wire, in the background:
///   * SEEN or better → `network_seen` latched, counted with its latency
///     (`submit_pending_seen_ms_total / submit_pending_seen_latched_total`);
///   * a FATAL look (REJECTED / DOUBLE_SPEND_ATTEMPTED) → the shared evidence
///     check (Arcade live again, then both indexers) → a CORROBORATED refusal
///     evicts everywhere (the shadow move), counted; an uncorroborated one is
///     counted and kept (the callbacks and the passes own it);
///   * an ORPHAN look keeps watching (the #413 dual push is feeding the parents
///     to the second broadcaster meanwhile); still an orphan at the end → counted;
///   * unknown / below SEEN → keep watching; silent at the end → counted, ONLY while the subject's
///     `network_seen` is still unlatched (bsv-low #822: a subject the #413 dual push or a callback latched is
///     `latched-elsewhere`, `watch_end_counter`).
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
    let arcade = crate::broadcaster::ArcadeBroadcaster::new(env.arcade_base.clone())
        .with_proof_sources(crate::proof_fetcher::ProofSources::from_env(&env.env));
    let port = WorkerWatch {
        env: &env,
        db: &db,
        txid: &txid,
        started_ms,
        refusal_ms: std::cell::Cell::new(started_ms),
        arcade,
        tracker: crate::lookup_service_chain_tracker(&env.env),
    };
    let Some(last) = pending_watch_job_with(&port, admitted_at_ms).await else {
        return;
    };
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
            crate::ops::bump_counter(&db, crate::ops::COUNTER_SUBMIT_PENDING_CONFIRMED_NOW, 1)
                .await;
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
        // bsv-low #822: the subject's OWN latch decides silent, never Arcade's last look alone (the #413 dual push
        // latches on TAAL's or GorillaPool's SEEN while Arcade's rank sits below it)
        let latched = crate::ops::network_seen_latched(&db, &txid).await;
        if let Err(why) = &latched {
            worker::console_log!(
                "[admit-fast] {txid} the watch-end latch read faulted ({why}) — counted by Arcade's last look"
            );
        }
        let name = watch_end_counter(&last, latched.as_ref().ok().copied());
        crate::ops::bump_counter(&db, name, 1).await;
        if name == crate::ops::COUNTER_SUBMIT_PENDING_LATCHED_ELSEWHERE {
            worker::console_log!(
                "[admit-fast] {txid} Arcade unwitnessed after the pending watch ({last:?}) but network_seen is latched elsewhere — not silent"
            );
        } else {
            worker::console_log!(
                "[admit-fast] {txid} unwitnessed after the pending watch ({last:?}) — the callbacks, the completion and the reconcile passes own it"
            );
        }
    }
}

/// PURE (bsv-low #836): the pending watch's line for a non-final look: the word, the lock height and Arcade's
/// PEER's height (a hint beside the word, never a gate input), Arcade's text scrubbed as #519 does.
pub(crate) fn watch_not_final_line(
    txid: &str,
    status: &str,
    nf: &crate::door_words::NotFinal,
) -> String {
    let height = |h: Option<u64>| h.map_or("?".to_string(), |h| h.to_string());
    format!(
        "[admit-fast] {txid} {status} TX_LOCK_TIME (lock height {}, Arcade's peer at {}): held, not final: no evidence check, no eviction; the watch keeps looking ({})",
        height(nf.lock_height),
        height(nf.peer_height),
        nf.reason
    )
}

/// bsv-low #822: the counter the pending watch's END bumps for a subject it neither latched nor confirmed. `latched`
/// is the subject's `network_seen` read at the end (`None`: the read faulted). A LATCHED subject (another path said
/// SEEN: the #413 dual push, a callback) is `latched-elsewhere`, never silent nor orphan; an unlatched one keeps
/// the look's class (an orphan view, else silent); a faulted read counts by the look, as before #822.
pub(crate) fn watch_end_counter(
    last: &crate::broadcaster::WitnessLook,
    latched: Option<bool>,
) -> &'static str {
    if latched == Some(true) {
        crate::ops::COUNTER_SUBMIT_PENDING_LATCHED_ELSEWHERE
    } else if matches!(last, crate::broadcaster::WitnessLook::Orphan(_)) {
        crate::ops::COUNTER_SUBMIT_PENDING_ORPHAN
    } else {
        crate::ops::COUNTER_SUBMIT_PENDING_SILENT
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
        /// Rows whose confirmation latched.
        rows: usize,
        /// Rows whose CAS did not hit (the row moved meanwhile).
        cas_missed: usize,
        /// Rows whose CAS read faulted.
        cas_faults: usize,
        /// The block height the proof names, if known.
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
    /// bsv-low #837 (FIX-837) pin (a3): every road that puts a pot in the index without the door's `tm_lowfund`
    /// judgement of its JOIN records its hop spends through the ONE shared function
    /// (`overlay_discovery::pot::lowfund_topic_manager::record_join_hop_spends`): the readmission (after its rows are
    /// back, before its closing line), `/admin/readmit` in both arms, the door's re-present of a JOIN deduped under
    /// `tm_pot`, and the discovery pass's pot candidate. RED with any one call removed.
    #[test]
    fn fix837_a3_every_off_door_road_records_the_joins_hop_spends_through_the_one_function() {
        let squash = |s: &str| s.split_whitespace().collect::<String>();
        let fast = include_str!("admit_fast.rs");
        let helper = &fast[fast.find("pub async fn record_join_hop_spends_on(").unwrap()..];
        let helper = &helper[..helper.find("\n}\n").unwrap()];
        assert!(squash(helper).contains(&squash(
            "overlay_discovery::pot::lowfund_topic_manager::record_join_hop_spends(&store, txid)"
        )));
        let readmit = &fast[fast.find("pub async fn readmit_if_evicted(").unwrap()..];
        let readmit = &readmit[..readmit.find("\n}\n").unwrap()];
        let call = readmit
            .find("record_join_hop_spends_on(db, &txid, \"readmit\")")
            .expect("the readmission records its JOIN's hop spends");
        assert!(call < readmit.find("READMITTED {txid} on the pushed proof").unwrap());
        assert!(call > readmit.find("let (restored, faults) = restore_twins(db, &txid).await;").unwrap());
        let routes = include_str!("routes.rs");
        let admin = &routes[routes.find("POST /admin/readmit txid={txid} topic={topic}").unwrap()..];
        let admin = &admin[..admin.find("\n}\n").unwrap()];
        assert_eq!(
            admin
                .matches("crate::admit_fast::record_join_hop_spends_on(&db, &txid, \"POST /admin/readmit\")")
                .count(),
            2,
            "both arms of /admin/readmit"
        );
        let door = &routes[routes.find("async fn submit_parts(").unwrap()..];
        let door = &door[..door.find("\n}\n").unwrap()];
        assert!(squash(door).contains(&squash(
            "crate::admit_fast::record_join_hop_spends_on(&db, &txid, \"POST /submit re-present\")"
        )));
        let fetcher = include_str!("proof_fetcher.rs");
        let pass = &fetcher[fetcher.find("async fn discover_spend_for_row(").unwrap()..];
        let pass = &pass[..pass.find("\n}\n").unwrap()];
        assert!(squash(pass).contains(&squash(
            "overlay_discovery::pot::lowfund_topic_manager::record_join_hop_spends_bounded(pot_storage, &rec.txid, allowance, join_hop_rotation()"
        )));
    }

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
            ConfirmNow::Confirmed {
                rows: 1,
                cas_missed: 0,
                cas_faults: 0,
                height: Some(967_603)
            }
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
        assert!(confirm_now_memoised(
            Some(1e12),
            1e12 + CONFIRM_NOW_MEMO_MS - 1.0
        ));
        assert!(!confirm_now_memoised(
            Some(1e12),
            1e12 + CONFIRM_NOW_MEMO_MS
        ));
    }

    /// The watch fits the post-response budget (gate LOW-1: eviction of the
    /// isolate must not silently skip the latch) and looks early (the old wire
    /// poll's cadence: SEEN typically by the 4th 2-s look). Fleet loop 15: the
    /// watch's END adds the single-spend confirm AFTER the sleeps (one indexed
    /// read; for a recorded spender, the exhaustive proof ladder's reads) — an
    /// isolate evicted inside it loses only a confirm the 30-minute backstop
    /// pass still owns (fail-safe), and the silent/orphan counter is bumped only
    /// when the confirm did not confirm.
    /// bsv-low #822 (loop 42, pair 3): a subject the #413 dual push latched SEEN while Arcade's rank sat at
    /// ACCEPTED_BY_NETWORK counts `latched-elsewhere` at the watch's end, never silent; an unlatched one stays silent
    /// (or orphan); a faulted latch read counts by the look.
    #[test]
    fn fix822_the_watch_end_over_a_pre_latched_subject_counts_latched_elsewhere_never_silent() {
        use crate::broadcaster::WitnessLook;
        let accepted = WitnessLook::Pending("ACCEPTED_BY_NETWORK".into());
        let orphan = WitnessLook::Orphan("SEEN_IN_ORPHAN_MEMPOOL".into());
        for look in [&accepted, &orphan, &WitnessLook::Unknown] {
            assert_eq!(
                watch_end_counter(look, Some(true)),
                crate::ops::COUNTER_SUBMIT_PENDING_LATCHED_ELSEWHERE,
                "{look:?}: a latched subject is never silent"
            );
        }
        assert_eq!(
            watch_end_counter(&accepted, Some(false)),
            crate::ops::COUNTER_SUBMIT_PENDING_SILENT
        );
        assert_eq!(
            watch_end_counter(&orphan, Some(false)),
            crate::ops::COUNTER_SUBMIT_PENDING_ORPHAN
        );
        assert_eq!(
            watch_end_counter(&accepted, None),
            crate::ops::COUNTER_SUBMIT_PENDING_SILENT,
            "a faulted latch read counts by the look"
        );
        // the job reads the subject's latch before it counts, and counts through this function alone
        let src = include_str!("admit_fast.rs");
        let job = &src[src.find("pub async fn pending_watch_job(").unwrap()..];
        let job = &job[..job.find("\n}\n").unwrap()];
        let read = job
            .find("crate::ops::network_seen_latched(&db, &txid).await")
            .expect("the watch end reads the subject's latch");
        let count = job
            .find("let name = watch_end_counter(&last, latched.as_ref().ok().copied());")
            .expect("the watch end counts through watch_end_counter");
        assert!(read < count, "the latch is read before the count");
        assert!(
            !job.contains("COUNTER_SUBMIT_PENDING_SILENT"),
            "no direct silent count in the job"
        );
    }

    /// bsv-low #822: the latch read is the latch table's own primary key, lowercased as the latch writes it.
    #[test]
    fn fix822_the_latch_read_finds_the_latch_under_real_sqlite() {
        let conn = shipped_conn();
        let txid = "AB".repeat(32);
        let read = |conn: &rusqlite::Connection| {
            conn.query_row(crate::ops::NETWORK_SEEN_LATCHED_SQL, [&txid], |_| Ok(()))
                .is_ok()
        };
        assert!(!read(&conn), "unlatched");
        conn.execute(
            crate::ops::NETWORK_SEEN_INSERT_SQL,
            rusqlite::params![txid, "taal", 1i64],
        )
        .unwrap();
        assert!(
            read(&conn),
            "latched by another path (the #413 dual push's TAAL)"
        );
    }

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

    /// bsv-low #621 (L5): every producer of an eviction's ledger reason, driven through its REAL format, serves the
    /// chain word the eviction rests on: the conflict pass `conflicted`; Arcade's own terminal word (the callback's
    /// and the watch's `{status} ({reason})` over `evidence_verdict`'s Fatal arm, and the door's re-refusal)
    /// `rejected`; an eviction on ABSENCE alone (Arcade missing or faulted, both indexers absent) NO word, so the
    /// felt never voids on it. RED on main: every open row served `rejected`.
    #[test]
    fn i621_every_eviction_reason_serves_the_chain_word_it_rests_on_never_an_absence() {
        use overlay_discovery::pot::storage::eviction_chain_word;
        let refused = |look: ArcadeLook| match evidence_verdict(&look, Some(false), Some(false), false) {
            EvidenceVerdict::Refused(r) => r,
            other => panic!("a refusal: {other:?}"),
        };
        let fatal = refused(ArcadeLook::Fatal("DOUBLE_SPEND_ATTEMPTED".into(), "utxo spent by tx".into()));
        let missing = refused(ArcadeLook::Missing);
        let faulted = refused(ArcadeLook::Fault);
        // the W2-WORDS-L5 lens, MEDIUM-1: Arcade faulted and the recorded (pushed) word filled it: no chain word,
        // even when the recorded text carries Arcade's live mark
        let recorded = refused(ArcadeLook::Recorded("REJECTED".into(), "arcade live REJECTED: x".into()));
        // the callback's and the watch's wrap (`refusal_job`, `pending_watch_job`)
        for status in ["REJECTED", "DOUBLE_SPEND_ATTEMPTED", "SEEN_ON_NETWORK"] {
            assert_eq!(eviction_chain_word(&format!("{status} ({recorded})")), None, "a recorded word behind a fault: no word");
            assert_eq!(eviction_chain_word(&format!("{status} ({fatal})")), Some("rejected"));
            assert_eq!(eviction_chain_word(&format!("{status} ({missing})")), None, "absence alone: no word");
            assert_eq!(eviction_chain_word(&format!("{status} ({faulted})")), None, "absence alone: no word");
        }
        assert_eq!(
            eviction_chain_word(&ladder_rerefusal_reason("Arcade REJECTED bad script")),
            Some("rejected")
        );
        assert_eq!(
            eviction_chain_word(&conflict_eviction_reason(&"aa".repeat(32), 1, &"bb".repeat(32), 967_673_u64)),
            Some("conflicted")
        );
        // a broadcaster's text inside another reason never reads as a conflict; an unknown reason is no word
        assert_eq!(
            eviction_chain_word(&format!("REJECTED ({})", refused(ArcadeLook::Fatal("REJECTED".into(), "input x:0 spent by confirmed competitor y at 1".into())))),
            Some("rejected")
        );
        assert_eq!(eviction_chain_word("fixture"), None);
        assert_eq!(eviction_chain_word(""), None);
        // the producers' sources use the pure formats (a reformat cannot slip past the classifier)
        let src = include_str!("admit_fast.rs");
        assert!(src.contains("&conflict_eviction_reason(parent, vout, &competitor, height),"));
        assert!(include_str!("routes.rs").contains("&crate::admit_fast::ladder_rerefusal_reason(&reason),"));
        assert!(src.contains("&format!(\"{} ({reason})\", webhook.0)"));
        assert!(src.contains("&format!(\"{status} ({reason})\")"));
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
        assert_eq!(
            count(&conn, "low_records", "txid", &pot),
            1,
            "the advert row restored (its expression-default createdAt coalesced)"
        );
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
        // bsv-low #836 fold (the lens's L1): the decision reads the status word alone again
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
        assert!(
            verified < readmit,
            "the readmission follows the verified bump, never precedes it"
        );
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
        let write = include_str!("eviction_write.rs");
        let evict = &write[write.find("pub(crate) fn plan(").unwrap()..];
        let evict = &evict[..evict.find("\n}\n").unwrap()];
        let release = evict
            .find("release_pot_spends_sql(cols)")
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
        let restore = readmit
            .find("restore_twins(db, &txid)")
            .expect("the restore");
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
        for (name, eviction) in [
            ("pub async fn refusal_job(", "evict_txid_everywhere("),
            ("async fn pending_watch_job_with<", "port.evict("),
        ] {
            let job = &src[src.find(name).unwrap()..];
            let job = &job[..job.find("\n}\n").unwrap()];
            let job = squash(&code_only(job));
            let at = job.find(eviction).expect("the eviction");
            let settle = job[at..]
                .find("settle_corroborated_refusal(")
                .expect("the settle after the eviction");
            assert!(
                settle < 700,
                "{name}: the settle sits after the eviction at +{settle}"
            );
        }
        let adapter = &src[src.find("impl PendingWatch for WorkerWatch").unwrap()..];
        assert!(squash(adapter)
            .contains("evict_txid_everywhere(self.db,self.txid,reason,self.refusal_ms.get())"));
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
            conn.execute(
                OPEN_EVICTION_MARKER_SQL,
                rusqlite::params![&pot, reason, at],
            )
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
        assert_eq!(
            row(),
            ("REJECTED (a)".into(), 1_000, None, 0, Some("[]".into()))
        );
        assert_eq!(
            guard(),
            Some(("REJECTED (a)".into(), 1_000)),
            "open from the marker on"
        );
        // a concurrent second eviction of the same txid: the first open stamp stands
        open(1_500, "REJECTED (b)");
        assert_eq!(row().1, 1_000, "the first open stamp is kept");
        // the close ADDS the pass's rows and records the list; a later close with '[]' keeps it
        close(
            1_000,
            "REJECTED (a)",
            3,
            r#"[{"table":"pot_records","txid":"cd","vout":0}]"#,
        );
        close(1_500, "REJECTED (b)", 2, "[]");
        let r = row();
        assert_eq!(
            (r.1, r.3),
            (1_000, 5),
            "both passes' rows added; the stamp unmoved"
        );
        assert_eq!(
            r.4.as_deref(),
            Some(r#"[{"table":"pot_records","txid":"cd","vout":0}]"#),
            "an empty later list never erases the first"
        );
        // readmitted: the guard sees nothing; a new eviction re-opens with the NEW stamp and fresh counts
        conn.execute(
            "UPDATE pot_evictions SET readmittedAt = 2000 WHERE txid = ?1",
            [&pot],
        )
        .unwrap();
        assert_eq!(guard(), None, "a readmitted row is not open");
        open(3_000, "REJECTED (c)");
        assert_eq!(
            row(),
            ("REJECTED (c)".into(), 3_000, None, 0, Some("[]".into()))
        );
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
        assert_eq!(
            count(&conn, "applied_transactions_evicted", "txid", &pot),
            1
        );
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
        assert!(w.contains("crate::eviction_write::evict(db, &txid, reason, now_ms, repointed)"));
        let write = include_str!("eviction_write.rs");
        let snapshot = &write[write.find("async fn snapshot(").unwrap()..];
        let release = snapshot
            .find("select_pot_spends_sql(cols)")
            .expect("the spends read");
        let add = snapshot
            .find("out.released.extend(repointed.iter().cloned())")
            .expect("the re-pointed added");
        let merge = snapshot.find("merge_released(").expect("the merge");
        assert!(
            release < add && add < merge,
            "read the spends, add the re-pointed ones, merge"
        );
        let plan = &write[write.find("pub(crate) fn plan(").unwrap()..];
        assert!(
            plan.find("merge_releases_sql(cols)").unwrap()
                < plan.find("Query::new(CLOSE_EVICTION_MARKER_SQL)").unwrap()
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
        let core = &src[src.find("async fn evict_core_with_abort(").unwrap()..];
        let core = &core[..core.find("\n}\n").unwrap()];
        let marker = core
            .find("OPEN_EVICTION_MARKER_SQL")
            .expect("the open marker");
        let pre = core
            .find("match readmitted_after(db, txid, now_ms).await")
            .expect("the readmission check before the passes");
        let passes = core
            .find("shadow_move_passes(db, txid, reason, now_ms, false)")
            .expect("the two passes, over the shadow storage");
        let post = core[passes..]
            .find("match readmitted_after(db, txid, now_ms).await")
            .expect("the readmission check after the passes")
            + passes;
        let restore = core[post..]
            .find("restore_twins(db, txid)")
            .expect("the pass yields: its own moves restored")
            + post;
        assert!(
            marker < pre && pre < passes && passes < post && post < restore,
            "marker → check → passes → check → restore"
        );
        assert!(
            core[pre..passes].contains("return (core, None);"),
            "a readmission found BEFORE the passes: nothing moves"
        );
        // The wrapper awaits the atomic move and close before the counters and notifications.
        let evict = &src[src.find("pub async fn evict_txid_everywhere(").unwrap()..];
        let evict = &evict[..evict.find("struct EvictedRow").unwrap()];
        let c = evict
            .find("crate::eviction_write::evict(")
            .expect("the atomic core");
        let atomic = include_str!("eviction_write.rs");
        let plan = &atomic[atomic.find("pub(crate) fn plan(").unwrap()..];
        assert!(
            plan.find("merge_releases_sql(cols)").unwrap()
                < plan.find("release_pot_spends_sql(cols)").unwrap()
                && plan.find("release_pot_spends_sql(cols)").unwrap()
                    < plan.find("Query::new(CLOSE_EVICTION_MARKER_SQL)").unwrap(),
            "merge the current release list, release, close in one batch"
        );
        assert!(
            c < evict.find("COUNTER_ADMIT_FAST_EVICT_INCOMPLETE").unwrap()
                && plan.find("READMISSION_FENCE_SQL").unwrap() < plan.find("move_sql(").unwrap(),
            "a readmission fences the batch before any move or release"
        );
        assert!(
            evict.contains("COUNTER_ADMIT_FAST_EVICT_INCOMPLETE"),
            "an incomplete pass is counted"
        );
        assert!(
            evict.contains("COUNTER_ADMIT_FAST_EVICT_YIELDED"),
            "a yielded pass is counted"
        );
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
                "async fn pending_watch_job_with<",
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
                args[0],
                if job_name.contains("_with<") {
                    "port"
                } else {
                    "&port"
                },
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
                self::squashed(&job[evict..]).starts_with(if job_name.contains("_with<") {
                    "letoutcome=port.evict("
                } else {
                    "letoutcome=evict_txid_everywhere("
                }),
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
            let job = &src[src.find(job_name).unwrap()..];
            let job = self::squashed(&job[..job.find("\n}\n").unwrap()]);
            let guard = job.find("ifyielded{").expect("the yield guard");
            let facts = job.find("port.record(counter,memo_reason)").expect("the atomic facts");
            assert!(job[guard..facts].contains("}else{match"), "a yielded pass records neither fact");
            assert_eq!(job.matches("port.record(").count(), 1, "one batch under the yield guard");
            assert!(!job.contains("port.bump(") && !job.contains("port.write_memo("));
        }
        // the passes never touch the console and read through the shadow storage only
        let passes_src = &src[src.find("pub async fn shadow_move_passes(").unwrap()..];
        let passes_src = &passes_src[..passes_src.find("\n}\n").unwrap()];
        let looping = passes_src
            .find("for (table, keys) in MOVED_TABLES")
            .expect("the table loop");
        let seam = passes_src
            .find("db.between_passes().await")
            .expect("the seam");
        let verify = passes_src
            .find("THE VERIFICATION PASS")
            .expect("the verification pass");
        assert!(
            looping < seam && seam < verify,
            "pass 1 → the seam → the verification pass"
        );
        for f in [
            core,
            passes_src,
            &src[src.find("pub async fn restore_twins(").unwrap()..][..2_000],
        ] {
            assert!(
                !f.contains("console_log!"),
                "the generic halves never touch the console"
            );
            assert!(
                !f.contains("count_keyed(db") && !f.contains("table_columns(db"),
                "the generic halves read through the shadow storage only"
            );
        }
        // the readmission stamps only after a CLEAN restore (NEW-2), never over a faulted table
        let readmit = &src[src.find("pub async fn readmit_if_evicted(").unwrap()..];
        let readmit = &readmit[..readmit.find("\n}\n").unwrap()];
        let restore_at = readmit
            .find("restore_twins(db, &txid)")
            .expect("the restore");
        let incomplete = readmit
            .find("COUNTER_ADMIT_FAST_READMIT_INCOMPLETE")
            .expect("the incomplete counter");
        let stamp = readmit
            .find("Query::new(READMIT_EVICTION_SQL)")
            .expect("the stamp");
        assert!(
            restore_at < incomplete && incomplete < stamp,
            "restore → (faults: open, counted, return) → the stamp"
        );
        assert!(
            readmit[incomplete..stamp].contains("return false;"),
            "a faulted restore never stamps"
        );
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
        on_open: std::cell::RefCell<Option<LateWrite>>,
        fail_first_copy_of: std::cell::RefCell<Option<String>>,
    }
    impl SqliteShadow {
        fn new(conn: rusqlite::Connection) -> Self {
            Self {
                conn,
                late: std::cell::RefCell::new(None),
                on_open: std::cell::RefCell::new(None),
                fail_first_copy_of: std::cell::RefCell::new(None),
            }
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
                .prepare(&format!(
                    "SELECT \"outputIndex\" FROM \"{table}\" WHERE \"txid\" = ?1"
                ))
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
            let result = self
                .conn
                .execute(sql, rusqlite::params_from_iter(params.iter()))
                .map(|_| ())
                .map_err(|e| e.to_string());
            if sql == OPEN_EVICTION_MARKER_SQL && result.is_ok() {
                if let Some(write) = self.on_open.borrow_mut().take() {
                    write(&self.conn);
                }
            }
            result
        }
        async fn query_i64(&self, sql: &str, binds: Vec<QVal>) -> Result<Option<i64>, String> {
            use rusqlite::OptionalExtension;
            let params: Vec<rusqlite::types::Value> = binds.iter().map(to_sql).collect();
            self.conn
                .query_row(sql, rusqlite::params_from_iter(params.iter()), |r| {
                    r.get::<_, Option<i64>>(0)
                })
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

    #[async_trait(?Send)]
    impl crate::admission_abort::AbortRows for SqliteShadow {
        async fn admitted_outputs(&self, txid: &str) -> Result<Vec<(String, u32)>, String> {
            let mut st = self
                .conn
                .prepare(&crate::admission_abort::ADMITTED_OUTPUTS_SQL.replace('?', "?1"))
                .map_err(|e| e.to_string())?;
            let rows = st
                .query_map([txid], |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?.max(0) as u32))
                })
                .map_err(|e| e.to_string())?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| e.to_string());
            rows
        }
        async fn stored_beef(&self, txid: &str) -> Result<Option<Vec<u8>>, String> {
            use rusqlite::OptionalExtension;
            crate::admission_abort::read_held_beef(|sql| async move {
                self.conn
                    .query_row(&sql.replace('?', "?1"), [txid], |r| {
                        r.get::<_, Option<String>>(0)
                    })
                    .optional()
                    .map(|beef| crate::d1::beef_of_hex_column(beef.flatten()))
                    .map_err(|e| e.to_string())
            })
            .await
        }
    }

    /// A stateful manager (bsv-low #558): counts each abort and its outputs.
    /// Each abort a manager was told: the BEEF and the outputs.
    type I558Told = std::rc::Rc<std::cell::RefCell<Vec<(Vec<u8>, Vec<u32>)>>>;
    struct I558Counting(I558Told);
    #[async_trait(?Send)]
    impl overlay_engine::topic_manager::TopicManager for I558Counting {
        async fn identify_admissible_outputs(
            &self,
            _: &bsv_rs::transaction::Transaction,
            _: &[u8],
            _: Option<&[u8]>,
            _: overlay_engine::types::SubmitMode,
            _: &overlay_engine::types::TopicAdmittanceContext,
        ) -> Result<
            overlay_engine::types::AdmittanceInstructions,
            overlay_engine::topic_manager::TopicManagerError,
        > {
            Ok(Default::default())
        }
        async fn abort_admissible_outputs(
            &self,
            beef: &[u8],
            outputs: &[u32],
        ) -> Result<(), overlay_engine::topic_manager::TopicManagerError> {
            self.0.borrow_mut().push((beef.to_vec(), outputs.to_vec()));
            Ok(())
        }
        fn aborts_admissions(&self) -> bool {
            true
        }
        async fn get_documentation(&self) -> String {
            String::new()
        }
        async fn get_metadata(&self) -> overlay_engine::types::ServiceMetadata {
            Default::default()
        }
    }

    #[tokio::test]
    async fn ident_fold_l2_a_row_landing_with_open_is_told() {
        let db = SqliteShadow::new(shipped_conn());
        let txid = "c7".repeat(32);
        let landing = txid.clone();
        *db.on_open.borrow_mut() = Some(Box::new(move |conn| {
            conn.execute("INSERT INTO outputs (txid, outputIndex, outputScript, topic, satoshis, spent) VALUES (?1, 7, X'51', 'tm_count', 1000, 0)", [&landing]).unwrap();
            conn.execute(
                "INSERT INTO transactions (txid, beef) VALUES (?1, X'0100BEEF')",
                [&landing],
            )
            .unwrap();
        }));
        let told = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let managers: crate::admission_abort::AbortingManagers =
            vec![("tm_count".to_string(), Box::new(I558Counting(told.clone())))];
        let (core, abort) =
            evict_core_with_abort(&db, &txid, "REJECTED", 1_000, Some((&db, managers))).await;
        assert!(core.faults.is_empty(), "{core:?}");
        assert!(core.yielded_to_readmission.is_none());
        assert_eq!(count(&db.conn, "outputs", "txid", &txid), 0);
        assert_eq!(
            crate::admission_abort::tell(abort.as_ref().unwrap()).await,
            1
        );
        assert_eq!(*told.borrow(), vec![(vec![1, 0, 0xbe, 0xef], vec![7])]);
    }

    #[test]
    fn ident_fold_l2_the_abort_read_is_inside_the_open_marker() {
        let src = include_str!("admit_fast.rs");
        let core = &src[src.find("async fn evict_core_with_abort(").unwrap()..];
        let marker = core.find("OPEN_EVICTION_MARKER_SQL").unwrap();
        let read = core
            .find("crate::admission_abort::read_before_eviction(")
            .unwrap();
        let passes = core
            .find("shadow_move_passes(db, txid, reason, now_ms, false)")
            .unwrap();
        assert!(
            marker < read && read < passes,
            "the abort read must follow the OPEN marker and precede the moves"
        );
    }

    #[tokio::test]
    async fn ident_fold_l2_a_twin_beef_still_names_the_admission() {
        use crate::admission_abort::AbortRows;
        let db = SqliteShadow::new(shipped_conn());
        let txid = "a7".repeat(32);
        db.conn
            .execute(
                "INSERT INTO transactions (txid, beef) VALUES (?1, X'0100BEEF')",
                [&txid],
            )
            .unwrap();
        let core = evict_core(&db, &txid, "REJECTED", 1_000).await;
        assert!(core.faults.is_empty(), "{core:?}");
        assert_eq!(
            db.stored_beef(&txid).await.unwrap(),
            Some(vec![1, 0, 0xbe, 0xef]),
            "a half-faulted eviction must read the BEEF from the twin"
        );
    }

    #[tokio::test]
    async fn ident_fold_l2_a_pot_beef_still_names_the_admission() {
        use crate::admission_abort::AbortRows;
        let db = SqliteShadow::new(shipped_conn());
        let txid = "b7".repeat(32);
        db.conn
            .execute(
                "INSERT INTO pot_beefs (txid, beef, createdAt) VALUES (?1, X'0100BEEF', 1)",
                [&txid],
            )
            .unwrap();
        assert_eq!(
            db.stored_beef(&txid).await.unwrap(),
            Some(vec![1, 0, 0xbe, 0xef]),
            "the abort must also read the BEEF retained in pot_beefs"
        );
    }

    #[tokio::test]
    async fn ident_fold_l2_an_empty_or_faulted_beef_tells_nobody() {
        struct MissingBeef(bool);
        #[async_trait(?Send)]
        impl crate::admission_abort::AbortRows for MissingBeef {
            async fn admitted_outputs(&self, _: &str) -> Result<Vec<(String, u32)>, String> {
                Ok(vec![("tm_count".to_string(), 0)])
            }
            async fn stored_beef(&self, _: &str) -> Result<Option<Vec<u8>>, String> {
                if self.0 {
                    Err("read fault".to_string())
                } else {
                    Ok(Some(Vec::new()))
                }
            }
        }
        for fault in [false, true] {
            let told = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
            let abort = crate::admission_abort::read_before_eviction(
                &MissingBeef(fault),
                "missing",
                vec![("tm_count".to_string(), Box::new(I558Counting(told.clone())))],
            )
            .await;
            assert!(
                abort.skipped_empty_beef(),
                "the wrapper counts the missing body"
            );
            assert_eq!(
                crate::admission_abort::tell(&abort).await,
                0,
                "an empty BEEF names no admission, so no manager is told"
            );
            assert!(told.borrow().is_empty());
        }
    }

    #[test]
    fn ident_fold_l1_the_abort_is_after_the_ledger_close() {
        let src = include_str!("admit_fast.rs");
        let wrapper = &src[src.find("pub async fn evict_txid_everywhere_repointed(").unwrap()..];
        let wrapper = &wrapper[..wrapper.find("\n}\n").unwrap()];
        let close = wrapper.find("crate::eviction_write::evict(").unwrap();
        let close_finished = close + wrapper[close..].find(".await;").unwrap();
        let tell = wrapper.find("crate::admission_abort::tell(").unwrap();
        assert!(
            close_finished < tell,
            "finish the atomic move, release and ledger close before awaiting the abort"
        );
    }

    /// The eviction's sequence (`evict_txid_everywhere_repointed`: OPEN, the abort's read, the passes, the tell
    /// after the ledger closes unless the pass yielded) over the shipped schema and passes, a counting manager under `tm_count`. An
    /// evicted admission tells it ONCE, with the outputs the index held under its topic (not another topic's) and
    /// the stored BEEF; a second eviction of the same txid finds no live row and tells nothing; a pass that
    /// yields to a readmission tells nothing; an admission never evicted is never told. The wiring's order is
    /// pinned on the source. Does not compile on the base (65027163): `admission_abort` does not exist there.
    #[tokio::test]
    async fn i558_the_eviction_tells_an_aborting_manager_once() {
        let told = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let managers = || -> crate::admission_abort::AbortingManagers {
            vec![("tm_count".to_string(), Box::new(I558Counting(told.clone())))]
        };
        async fn evict_with(
            db: &SqliteShadow,
            txid: &str,
            managers: crate::admission_abort::AbortingManagers,
        ) -> u32 {
            let (core, abort) =
                evict_core_with_abort(db, txid, "REJECTED", 1_000, Some((db, managers))).await;
            if core.yielded_to_readmission.is_none() {
                crate::admission_abort::tell(abort.as_ref().unwrap()).await
            } else {
                0
            }
        }
        let seed = |conn: &rusqlite::Connection, txid: &str| {
            for (vout, topic) in [(1, "tm_count"), (0, "tm_count"), (0, "tm_other")] {
                conn.execute(
                    "INSERT INTO outputs (txid, outputIndex, outputScript, topic, satoshis, spent) VALUES (?1, ?2, X'51', ?3, 1000, 0)",
                    rusqlite::params![txid, vout, topic],
                )
                .unwrap();
            }
            conn.execute(
                "INSERT INTO transactions (txid, beef) VALUES (?1, X'0100BEEF')",
                [txid],
            )
            .unwrap();
        };
        let db = SqliteShadow::new(shipped_conn());
        let (evicted, clean, readmitted) = ("a5".repeat(32), "c1".repeat(32), "e7".repeat(32));
        seed(&db.conn, &evicted);
        seed(&db.conn, &clean);
        seed(&db.conn, &readmitted);

        assert_eq!(evict_with(&db, &evicted, managers()).await, 1);
        assert_eq!(
            *told.borrow(),
            vec![(vec![0x01, 0x00, 0xbe, 0xef], vec![0, 1])],
            "once, its own topic's outputs and the stored BEEF"
        );
        assert_eq!(count(&db.conn, "outputs", "txid", &evicted), 0, "evicted");
        assert_eq!(
            evict_with(&db, &evicted, managers()).await,
            0,
            "evicted again: no live row"
        );

        db.conn
            .execute(
                "INSERT INTO pot_evictions (txid, reason, evictedAt, readmittedAt, rowsMoved, releasedSpends) VALUES (?1, 'REJECTED', 500, 2000, 0, '[]')",
                [&readmitted],
            )
            .unwrap();
        assert_eq!(
            evict_with(&db, &readmitted, managers()).await,
            0,
            "the pass yielded"
        );
        assert_eq!(
            count(&db.conn, "outputs", "txid", &readmitted),
            3,
            "nothing moved"
        );

        assert_eq!(
            told.borrow().len(),
            1,
            "never for the clean or the readmitted"
        );
        assert_eq!(count(&db.conn, "outputs", "txid", &clean), 3);

        // no manager that aborts: no read at all
        struct Unreadable;
        #[async_trait(?Send)]
        impl crate::admission_abort::AbortRows for Unreadable {
            async fn admitted_outputs(&self, _: &str) -> Result<Vec<(String, u32)>, String> {
                panic!("read with no aborting manager")
            }
            async fn stored_beef(&self, _: &str) -> Result<Option<Vec<u8>>, String> {
                panic!("read with no aborting manager")
            }
        }
        let none =
            crate::admission_abort::read_before_eviction(&Unreadable, &clean, Vec::new()).await;
        assert!(none.admissions().is_empty());

        // The wrapper calls the ordered core and gates the tell on no readmission.
        let src = include_str!("admit_fast.rs");
        let w = &src[src
            .find("pub async fn evict_txid_everywhere_repointed(")
            .unwrap()..];
        let w = &w[..w.find("\n}\n").unwrap()];
        assert!(w.contains("crate::eviction_write::evict("));
        let atomic = include_str!("eviction_write.rs");
        assert!(atomic.contains("crate::admission_abort::managers_that_abort()"));
        let close = w.find("crate::eviction_write::evict(").unwrap();
        let tell_gate = close
            + w[close..]
                .find("if core.yielded_to_readmission.is_none()")
                .expect("the tell, gated on the yield");
        let tell = w.find("crate::admission_abort::tell(").unwrap();
        let after = w
            .find("if let Some(at) = core.yielded_to_readmission")
            .unwrap();
        assert!(
            tell_gate < tell && tell < after,
            "the tell stays inside the no-readmission arm"
        );
        assert!(w[tell_gate..tell].contains("abort.skipped_empty_beef()"));
        assert!(w[tell_gate..tell].contains("COUNTER_ADMISSION_ABORT_EMPTY_BEEF"));
    }

    /// bsv-low loop 18 (pair 11's JOIN `3b14f0e6…`), pinned under the REAL passes: rows the subject's own
    /// admission write lands AFTER the first pass survive it (the pre-fix end state, reproduced with the
    /// verification pass switched off); the verification pass takes them, notes their outpoints, and the
    /// eviction ends complete.
    #[tokio::test]
    async fn the_verification_pass_takes_a_row_that_lands_between_the_passes_under_the_real_logic()
    {
        let pot = "ab".repeat(32);
        let seed = |conn: &rusqlite::Connection| {
            conn.execute("INSERT INTO outputs (txid, outputIndex, outputScript, topic, satoshis, spent) VALUES (?1, 0, X'51', 'tm_pot', 40000, 0)", [&pot]).unwrap();
            conn.execute(
                "INSERT INTO applied_transactions (txid, topic) VALUES (?1, 'tm_pot')",
                [&pot],
            )
            .unwrap();
        };
        // THE RED CONTROL: the pre-fix shape (no verification pass) leaves the late rows served
        let db = SqliteShadow::new(shipped_conn());
        seed(&db.conn);
        let pot_c = pot.clone();
        *db.late.borrow_mut() = Some(Box::new(move |c: &rusqlite::Connection| {
            late_rows(c, &pot_c)
        }));
        let r0 = shadow_move_passes(&db, &pot, "REJECTED", 1_000, true).await;
        assert_eq!(r0.moved, 2);
        assert_eq!(
            count(&db.conn, "pot_records", "txid", &pot),
            1,
            "the pre-fix end state: a pot the index still knows"
        );
        assert_eq!(count(&db.conn, "applied_transactions", "txid", &pot), 1);
        assert!(r0.pot_vouts.is_empty(), "nothing noted for the survivor");
        // THE FIX: the verification pass takes the survivors and notes the pot's outpoint
        let db = SqliteShadow::new(shipped_conn());
        seed(&db.conn);
        let pot_c = pot.clone();
        *db.late.borrow_mut() = Some(Box::new(move |c: &rusqlite::Connection| {
            late_rows(c, &pot_c)
        }));
        let r1 = shadow_move_passes(&db, &pot, "REJECTED", 1_000, false).await;
        assert_eq!(r1.moved, 4, "both passes' rows");
        assert_eq!(count(&db.conn, "pot_records", "txid", &pot), 0);
        assert_eq!(count(&db.conn, "applied_transactions", "txid", &pot), 0);
        assert_eq!(count(&db.conn, "outputs", "txid", &pot), 0);
        assert_eq!(count(&db.conn, "pot_records_evicted", "txid", &pot), 1);
        assert_eq!(
            count(&db.conn, "applied_transactions_evicted", "txid", &pot),
            2
        );
        assert_eq!(
            r1.pot_vouts,
            vec![0],
            "the survivor's outpoint is noted (the gate's MEDIUM-1)"
        );
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
        conn.execute(
            "INSERT INTO applied_transactions (txid, topic) VALUES (?1, 'tm_lowfund')",
            [pot],
        )
        .unwrap();
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
        assert_eq!(
            count(&db.conn, "pot_records", "txid", &pot),
            0,
            "healed by the verification pass"
        );
        assert_eq!(r.moved, 2);
        assert_eq!(r.retried.len(), 1, "{:?}", r.retried);
        assert!(
            r.retried[0].starts_with("pot_records by txid: first pass copy: simulated"),
            "{:?}",
            r.retried
        );
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
        conn.execute(
            OPEN_EVICTION_MARKER_SQL,
            rusqlite::params![&pot, "REJECTED (a)", 1_000i64],
        )
        .unwrap();
        // the chain's word lands under the pass: readmittedAt 1_500 > the pass's 1_000
        conn.execute(
            "UPDATE pot_evictions SET readmittedAt = 1500 WHERE txid = ?1",
            [&pot],
        )
        .unwrap();
        let under: Option<i64> = conn
            .query_row(
                READMITTED_AFTER_SQL,
                rusqlite::params![&pot, 1_000i64],
                |r| r.get(0),
            )
            .optional()
            .unwrap();
        assert_eq!(
            under,
            Some(1_500),
            "the pass sees the readmission that landed under it"
        );
        // a marker with an OLDER stamp keeps the readmission AND its reason (round 3, NIT 6)
        conn.execute(
            OPEN_EVICTION_MARKER_SQL,
            rusqlite::params![&pot, "REJECTED (late)", 1_100i64],
        )
        .unwrap();
        let (kept_reason, kept_at): (String, Option<i64>) = conn
            .query_row(
                "SELECT reason, readmittedAt FROM pot_evictions WHERE txid = ?1",
                [&pot],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            (kept_reason.as_str(), kept_at),
            ("REJECTED (a)", Some(1_500))
        );
        conn.execute(
            CLOSE_EVICTION_MARKER_SQL,
            rusqlite::params![&pot, "REJECTED (closing)", 1_000i64, 3i64, "[]"],
        )
        .unwrap();
        let (readmitted, moved, reason): (Option<i64>, i64, String) = conn
            .query_row(
                "SELECT readmittedAt, rowsMoved, reason FROM pot_evictions WHERE txid = ?1",
                [&pot],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            readmitted,
            Some(1_500),
            "the close keeps a readmission stamped after the pass"
        );
        assert_eq!(moved, 3);
        assert_eq!(
            reason, "REJECTED (a)",
            "and keeps the reason too (round 4, the CLOSE half of NIT 6)"
        );
        let open: Option<String> = conn
            .query_row(OPEN_EVICTION_SQL, [&pot], |r| r.get(0))
            .optional()
            .unwrap();
        assert_eq!(open, None, "not open: the chain's word stands");
        // an OLDER readmission (before the pass's stamp) is re-opened by the close, as before
        conn.execute(
            "UPDATE pot_evictions SET readmittedAt = 900 WHERE txid = ?1",
            [&pot],
        )
        .unwrap();
        conn.execute(
            CLOSE_EVICTION_MARKER_SQL,
            rusqlite::params![&pot, "REJECTED (b)", 2_000i64, 1i64, "[]"],
        )
        .unwrap();
        let open: Option<String> = conn
            .query_row(OPEN_EVICTION_SQL, [&pot], |r| r.get(0))
            .optional()
            .unwrap();
        assert_eq!(
            open.as_deref(),
            Some("REJECTED (b)"),
            "re-opened with fresh counts"
        );
        let moved: i64 = conn
            .query_row(
                "SELECT rowsMoved FROM pot_evictions WHERE txid = ?1",
                [&pot],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(moved, 1);
    }

    /// The gate's LOW-1: the released list is a UNION across passes (a re-eviction releasing only hop B keeps hop A).
    #[test]
    fn the_released_list_merges_across_passes() {
        let a = ReleasedSpend {
            table: "pot_records".into(),
            txid: "aa".repeat(32),
            vout: 0,
        };
        let b = ReleasedSpend {
            table: "pot_records".into(),
            txid: "bb".repeat(32),
            vout: 0,
        };
        let first = serde_json::to_string(&vec![a.clone()]).unwrap();
        let merged = merge_released(Some(&first), std::slice::from_ref(&b));
        assert_eq!(merged, vec![a.clone(), b.clone()]);
        let again = merge_released(
            Some(&serde_json::to_string(&merged).unwrap()),
            std::slice::from_ref(&a),
        );
        assert_eq!(again, vec![a.clone(), b.clone()], "no duplicate");
        assert_eq!(
            merge_released(None, std::slice::from_ref(&b)),
            vec![b.clone()]
        );
        assert_eq!(
            merge_released(Some("not json"), std::slice::from_ref(&a)),
            vec![a]
        );
    }

    /// The gate's NIT: the client-facing line carries the status WORD only (never Arcade's extraInfo).
    #[test]
    fn the_refusal_status_word_is_the_first_token() {
        assert_eq!(
            refusal_status_word(
                "REJECTED (arcade live REJECTED: UTXO_SPENT (70): x; both indexers absent)"
            ),
            "REJECTED"
        );
        assert_eq!(
            refusal_status_word("DOUBLE_SPEND_ATTEMPTED (arcade live …)"),
            "DOUBLE_SPEND_ATTEMPTED"
        );
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
            conn.execute(
                "INSERT INTO applied_transactions (txid, topic) VALUES (?1, 'tm_pot')",
                [&pot],
            )
            .unwrap();
        };
        // the readmission under the pass: restore every twin row, stamp the ledger AFTER the pass's stamp (1_000)
        let readmit_under = |conn: &rusqlite::Connection, pot: &str| {
            for t in ["pot_records", "outputs", "applied_transactions"] {
                let c = cols(conn, t);
                let (ins, del) = restore_sql(t, "txid", &c);
                conn.execute(&ins, [pot]).unwrap();
                conn.execute(&del, [pot]).unwrap();
            }
            conn.execute(
                "UPDATE pot_evictions SET readmittedAt = 1500 WHERE txid = ?1",
                [pot],
            )
            .unwrap();
        };
        // THE CONTROL: the passes alone re-move the restored rows
        let db = SqliteShadow::new(shipped_conn());
        seed(&db.conn);
        db.conn
            .execute(
                OPEN_EVICTION_MARKER_SQL,
                rusqlite::params![&pot, "REJECTED", 1_000i64],
            )
            .unwrap();
        let pot_c = pot.clone();
        *db.late.borrow_mut() = Some(Box::new(move |c: &rusqlite::Connection| {
            readmit_under(c, &pot_c)
        }));
        let r = shadow_move_passes(&db, &pot, "REJECTED", 1_000, false).await;
        assert!(r.moved >= 3);
        assert_eq!(
            count(&db.conn, "pot_records", "txid", &pot),
            0,
            "the control: the restored rows re-moved"
        );
        // THE FIX: the core checks for the readmission after the passes and restores its own moves
        let db = SqliteShadow::new(shipped_conn());
        seed(&db.conn);
        let pot_c = pot.clone();
        *db.late.borrow_mut() = Some(Box::new(move |c: &rusqlite::Connection| {
            readmit_under(c, &pot_c)
        }));
        let core = evict_core(&db, &pot, "REJECTED", 1_000).await;
        assert_eq!(
            core.yielded_to_readmission,
            Some(1_500),
            "the readmission under the pass was seen"
        );
        assert!(
            core.restored >= 3,
            "the pass's own moves restored again: {:?}",
            core
        );
        assert!(core.faults.is_empty(), "{:?}", core.faults);
        assert_eq!(count(&db.conn, "pot_records", "txid", &pot), 1);
        assert_eq!(count(&db.conn, "outputs", "txid", &pot), 1);
        assert_eq!(count(&db.conn, "applied_transactions", "txid", &pot), 1);
        assert_eq!(count(&db.conn, "pot_records_evicted", "txid", &pot), 0);
        // the wrapper's close keeps the stamp: the ledger says readmitted and the tables hold the rows
        db.conn
            .execute(
                CLOSE_EVICTION_MARKER_SQL,
                rusqlite::params![&pot, "REJECTED", 1_000i64, core.moved as i64, "[]"],
            )
            .unwrap();
        let readmitted: Option<i64> = db
            .conn
            .query_row(
                "SELECT readmittedAt FROM pot_evictions WHERE txid = ?1",
                [&pot],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(readmitted, Some(1_500));
    }

    /// Round 2 (NEW-1): a readmission stamped after the pass's own stamp but BEFORE the passes run (the clock read,
    /// then the marker, then the check): nothing moves — the chain's word already stands.
    #[tokio::test]
    async fn an_eviction_that_starts_under_a_readmission_moves_nothing() {
        let pot = "ab".repeat(32);
        let db = SqliteShadow::new(shipped_conn());
        db.conn.execute("INSERT INTO pot_records (txid, outputIndex, spent, createdAt, lockKind) VALUES (?1, 0, 0, 1789959998, 'covenant')", [&pot]).unwrap();
        db.conn
            .execute(
                OPEN_EVICTION_MARKER_SQL,
                rusqlite::params![&pot, "REJECTED", 900i64],
            )
            .unwrap();
        db.conn
            .execute(
                "UPDATE pot_evictions SET readmittedAt = 1200 WHERE txid = ?1",
                [&pot],
            )
            .unwrap();
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
    async fn restore_twins_lands_the_default_of_a_column_added_after_the_eviction_and_is_fail_loud()
    {
        let pot = "ab".repeat(32);
        let db = SqliteShadow::new(shipped_conn());
        db.conn.execute("INSERT INTO pot_records (txid, outputIndex, spent, createdAt, lockKind) VALUES (?1, 0, 0, 1789959998, 'covenant')", [&pot]).unwrap();
        let r = shadow_move_passes(&db, &pot, "REJECTED", 1_000, false).await;
        assert_eq!(r.moved, 1);
        // the source gains a NOT NULL DEFAULT column after the eviction (the schema's own class: has_proof,
        // spentConfirmed, paramsDecoded)
        db.conn
            .execute_batch(
                "ALTER TABLE pot_records ADD COLUMN loop18_new TEXT NOT NULL DEFAULT 'd'",
            )
            .unwrap();
        assert!(!cols(&db.conn, "pot_records_evicted")
            .iter()
            .any(|c| c.name == "loop18_new"));
        let (restored, faults) = restore_twins(&db, &pot).await;
        assert_eq!((restored, faults.len()), (1, 0), "{faults:?}");
        assert!(
            !cols(&db.conn, "pot_records_evicted").iter().any(|c| c.name == "loop18_new"),
            "the restore never heals the twin: the added column is omitted and takes the source's default"
        );
        assert_eq!(
            count(&db.conn, "pot_records", "txid", &pot),
            1,
            "the row LANDED (an explicit NULL would have been dropped)"
        );
        let new_col: String = db
            .conn
            .query_row(
                "SELECT loop18_new FROM pot_records WHERE txid = ?1",
                [&pot],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(new_col, "d", "the added column carries its DEFAULT");
        // a twin HEALED after the migration (a later eviction) carries the default for its older rows too
        let r = shadow_move_passes(&db, &pot, "REJECTED", 2_000, false).await;
        assert_eq!(r.moved, 1);
        let twin_cols = cols(&db.conn, "pot_records_evicted");
        let healed = twin_cols
            .iter()
            .find(|c| c.name == "loop18_new")
            .expect("the later eviction healed the twin");
        assert_eq!(
            healed.dflt_value.as_deref(),
            Some("'d'"),
            "healed WITH the default (round 4)"
        );
        // and even a twin row holding NULL there (a twin healed before the heal carried defaults) restores onto the default
        db.conn
            .execute(
                "UPDATE pot_records_evicted SET loop18_new = NULL WHERE txid = ?1",
                [&pot],
            )
            .unwrap();
        let (restored, faults) = restore_twins(&db, &pot).await;
        assert_eq!((restored, faults.len()), (1, 0), "{faults:?}");
        let new_col: String = db
            .conn
            .query_row(
                "SELECT loop18_new FROM pot_records WHERE txid = ?1",
                [&pot],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(new_col, "d", "COALESCEd onto the default");
        // idempotent
        let (again, faults) = restore_twins(&db, &pot).await;
        assert_eq!((again, faults.len()), (0, 0));
        // a copy back that lands nothing is named and the twin keeps its row (the twin dropped from under the restore)
        let r = shadow_move_passes(&db, &pot, "REJECTED", 3_000, false).await;
        assert_eq!(r.moved, 1);
        db.conn
            .execute_batch("DROP TABLE pot_records_evicted")
            .unwrap();
        db.conn.execute_batch("CREATE TABLE pot_records_evicted (af_evictedAt INTEGER NOT NULL, af_reason TEXT NOT NULL, txid TEXT)").unwrap();
        db.conn.execute("INSERT INTO pot_records_evicted (af_evictedAt, af_reason, txid) VALUES (1, 'x', ?1)", [&pot]).unwrap();
        let (restored, faults) = restore_twins(&db, &pot).await;
        assert_eq!(restored, 0);
        assert!(
            faults
                .iter()
                .any(|f| f.starts_with("pot_records by txid: the copy back landed 0 of 1 row(s)")),
            "{faults:?}"
        );
        assert_eq!(
            count(&db.conn, "pot_records_evicted", "txid", &pot),
            1,
            "the twin keeps the row it could not land"
        );
    }

    /// Round 5 (N7): the heal carries only a LITERAL default; an expression default is left to the restore's COALESCE.
    #[test]
    fn the_heal_carries_a_literal_default_only() {
        for d in [
            "0",
            "-1",
            "1.5",
            "'d'",
            "''",
            "NULL",
            "null",
            "CURRENT_TIMESTAMP",
        ] {
            assert!(literal_default(d), "{d}");
        }
        for d in [
            "datetime('now')",
            "(datetime('now'))",
            "",
            "  ",
            "abc",
            "1; DROP TABLE x",
        ] {
            assert!(!literal_default(d), "{d}");
        }
        let lit = ColumnInfo {
            name: "c".into(),
            ty: "INTEGER".into(),
            notnull: 1,
            dflt_value: Some("0".into()),
        };
        assert_eq!(
            heal_shadow_sql("pot_records", &lit),
            "ALTER TABLE \"pot_records_evicted\" ADD COLUMN \"c\" INTEGER DEFAULT 0"
        );
        let expr = ColumnInfo {
            name: "createdAt".into(),
            ty: "TEXT".into(),
            notnull: 1,
            dflt_value: Some("datetime('now')".into()),
        };
        assert_eq!(
            heal_shadow_sql("low_records", &expr),
            "ALTER TABLE \"low_records_evicted\" ADD COLUMN \"createdAt\" TEXT"
        );
        // the restore coalesces the NOT NULL expression default all the same (legal in a SELECT)
        let (ins, _) = restore_sql("low_records", "txid", std::slice::from_ref(&expr));
        assert!(
            ins.contains("COALESCE(\"createdAt\", datetime('now'))"),
            "{ins}"
        );
    }

    // lane E1D's delta fold (L2, the delta lens on `0da3a82`): a predecessor the engine LANDS first from a
    // submit's BEEF is an admission write the eviction ledger was never asked about (an evicted txid's rows are
    // shadow-moved, so it reads "not landed" and a successor carrying it landed it again under the OPEN row; the
    // post-write guard re-evicted only the subject). Every submit that reads a report now guards what it landed,
    // and the guard re-evicts under an open row. RED on `0da3a82` (no `guard_landed`).
    #[test]
    fn e1d_delta_l2_every_submit_with_a_report_guards_what_it_landed() {
        for (file, src) in [
            ("routes.rs", include_str!("routes.rs")),
            ("lib.rs", include_str!("lib.rs")),
        ] {
            // a call, never a comment that names one
            // the E592 lens fold: the queue's replay names its breach policy (`submit_with_report_under`)
            let calls: Vec<usize> = src
                .match_indices(".submit_with_report(")
                .chain(src.match_indices(".submit_with_report_under("))
                .map(|(at, _)| at)
                .filter(|&at| {
                    !src[src[..at].rfind('\n').map_or(0, |nl| nl + 1)..at]
                        .trim_start()
                        .starts_with("//")
                })
                .collect();
            assert!(!calls.is_empty(), "{file}: no submit with a report");
            for at in calls {
                // bsv-low #838: the gated arm's raced write. Done, its report is the route's own (guarded below,
                // after `match engine_outcome`); past the answer deadline, `finish_past_deadline` guards it
                let before = &src[at.saturating_sub(120)..at];
                if file == "routes.rs" && before.contains("Box::pin(async move { write_engine") {
                    let fin = &src[src.find("async fn finish_past_deadline<").unwrap()..];
                    let fin = &fin[..fin.find("\n}\n").unwrap()];
                    assert!(
                        fin.contains("crate::admit_fast::guard_landed(")
                            && fin.contains("landed_predecessors"),
                        "the past-deadline write guards what it landed"
                    );
                    let done = &src[src
                        .find("let (steak, mutation_report) = match engine_outcome {")
                        .unwrap()..];
                    let done = &done[..done.len().min(1_500)];
                    assert!(
                        done.contains("crate::admit_fast::guard_landed(")
                            && done.contains("landed_predecessors"),
                        "the raced write's report is guarded in the request"
                    );
                    continue;
                }
                let after = &src[at..];
                let window = &after[..after.len().min(1_500)];
                assert!(
                    window.contains("crate::admit_fast::guard_landed(")
                        && window.contains("landed_predecessors"),
                    "{file}: the submit at byte {at} does not guard what it landed"
                );
            }
        }
        let src = include_str!("admit_fast.rs");
        let guard = &src[src.find("pub async fn guard_landed(").unwrap()..];
        let guard = &guard[..guard.find("\n}\n").unwrap()];
        let open = guard
            .find("open_eviction(db, txid)")
            .expect("the ledger is read per landed txid");
        let evict = guard
            .find("evict_txid_everywhere(db, txid, &ev.reason")
            .expect("an open row re-evicts");
        assert!(open < evict);
        assert!(
            guard.contains("COUNTER_ADMIT_FAST_REEVICTED_AFTER_WRITE"),
            "counted"
        );
        assert!(
            guard.contains("COUNTER_ADMIT_FAST_LEDGER_UNREADABLE"),
            "an unreadable ledger is counted"
        );
    }

    struct D2Watch {
        store: SqliteShadow,
        clock: std::cell::Cell<u64>,
        posts: std::cell::Cell<usize>,
        batches: std::cell::RefCell<Vec<usize>>,
        pending: std::cell::RefCell<Vec<(String, u32)>>,
    }

    impl D2Watch {
        fn delay(&self, ms: u64) -> impl std::future::Future<Output = ()> + '_ {
            let until = self.clock.get() + ms;
            async move {
                self.clock.set(until);
            }
        }

        fn execute(&self, query: Query) {
            let params: Vec<_> = query.params().iter().map(to_sql).collect();
            self.store
                .conn
                .execute(query.sql(), rusqlite::params_from_iter(params))
                .unwrap();
        }

        fn bump_value(&self, name: &str, delta: u64) {
            self.execute(
                Query::new(crate::ops::BUMP_COUNTER_SQL)
                    .bind(name)
                    .bind(delta),
            );
        }

        fn total(&self, name: &str) -> u64 {
            self.store
                .conn
                .query_row(
                    "SELECT value FROM ops_counters WHERE name = ?",
                    [name],
                    |r| r.get(0),
                )
                .unwrap_or(0)
        }
    }

    impl RefusalSettle for D2Watch {
        async fn record(&self, counter: &'static str, reason: &str) -> Result<(), String> {
            let queries = refusal_fact_queries(self.txid(), self.clock.get(), counter, reason);
            let tx = self.store.conn.unchecked_transaction().unwrap();
            for query in &queries {
                let params: Vec<_> = query.params().iter().map(to_sql).collect();
                tx.execute(query.sql(), rusqlite::params_from_iter(params))
                    .unwrap();
            }
            tx.commit().unwrap();
            self.batches.borrow_mut().push(queries.len());
            Ok(())
        }
        async fn bump(&self, counter: &'static str) {
            self.bump_value(counter, 1);
        }
        async fn flush(&self) {
            let crate::pot_changes::BackgroundFlush::Ship { deadline_ms } =
                crate::pot_changes::background_flush_plan(0, self.clock.get())
            else {
                panic!("the fixture leaves time for delivery");
            };
            let items = self.pending.take();
            let shipped = crate::change_flush::ship_chunks_with(
                items,
                |chunk| serde_json::to_string(chunk).unwrap(),
                |_| async {
                    self.posts.set(self.posts.get() + 1);
                    if self.posts.get() == 1 {
                        self.clock.set(self.clock.get() + 100);
                        Ok(0)
                    } else {
                        std::future::pending().await
                    }
                },
                || self.clock.get(),
                deadline_ms,
                |ms| self.delay(ms),
            )
            .await;
            let mut batch = crate::change_flush::FlushBatch::default();
            crate::pot_changes::settle_and_park(&mut batch, &shipped);
            batch.flush_with(self, self.clock.get()).await;
        }
        fn log(&self, _: String) {}
    }

    impl crate::change_flush::FlushPort for D2Watch {
        async fn commit(&self, queries: Vec<Query>) -> Result<(), String> {
            self.batches.borrow_mut().push(queries.len());
            self.clock.set(self.clock.get() + 2_900);
            crate::change_flush::tests::commit_queries(&self.store.conn, queries)
        }
        fn log(&self, _: String) {}
    }

    impl PendingWatch for D2Watch {
        fn txid(&self) -> &str {
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        }
        async fn sleep(&self, ms: u64) {
            self.delay(ms).await;
        }
        async fn look(&self) -> crate::broadcaster::WitnessLook {
            crate::broadcaster::WitnessLook::Fatal("REJECTED".into(), "spent".into())
        }
        async fn evidence(&self, _: (String, String)) -> EvidenceVerdict {
            self.clock.set(25_000);
            evidence_verdict(
                &ArcadeLook::Fatal("REJECTED".into(), "spent".into()),
                Some(false),
                Some(false),
                false,
            )
        }
        async fn evict(&self, reason: &str) -> EvictionOutcome {
            let core = evict_core(&self.store, self.txid(), reason, self.clock.get()).await;
            assert!(core.faults.is_empty(), "{:?}", core.faults);
            self.pending.replace(
                core.pot_vouts
                    .iter()
                    .map(|v| (self.txid().to_string(), *v))
                    .collect(),
            );
            EvictionOutcome {
                moved: core.moved,
                yielded: core.yielded_to_readmission.is_some(),
                incomplete: false,
            }
        }
        async fn seen(&self, _: &str, _: Option<i64>, _: f64) {
            panic!("the fixture's broadcaster refuses");
        }
    }

    /// The production watch loop, eviction SQL and delivery loop under a clock that kills the job at 30 s.
    /// One chunk lands, the next POST hangs: all nine remaining outpoints must be counted and parked first.
    #[test]
    fn d2_649_pending_watch_keeps_facts_and_defers_a_hung_flush_before_platform_cancel() {
        let port = D2Watch {
            store: SqliteShadow::new(shipped_conn()),
            clock: std::cell::Cell::new(0),
            posts: std::cell::Cell::new(0),
            batches: Default::default(),
            pending: Default::default(),
        };
        for vout in 0..17 {
            port.store.conn.execute("INSERT INTO pot_records (txid, outputIndex, spent, createdAt) VALUES (?, ?, 0, 1)",
                rusqlite::params![port.txid(), vout]).unwrap();
        }
        let completed = crate::change_flush::run(crate::race_or_deadline(
            pending_watch_job_with(&port, 0.0),
            port.delay(crate::pot_changes::WAIT_UNTIL_WINDOW_MS),
        ));
        let memo: (String, String) = port
            .store
            .conn
            .query_row(
                "SELECT kind, evidence FROM tx_any_verdicts WHERE txid = ?",
                [port.txid()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(memo.0, "refused");
        assert!(memo.1.contains("spent"));
        assert_eq!(port.total(crate::ops::COUNTER_SUBMIT_PENDING_EVICTED), 1);
        assert_eq!(
            port.total(crate::applayer_counters::COUNTER_TXANY_MEMO_WRITE_REFUSED),
            1
        );
        assert_eq!(
            *port.batches.borrow(),
            vec![3, 13],
            "one facts batch before delivery, one accounting batch after"
        );
        assert!(
            completed.is_some(),
            "the pending watch was cancelled at {} ms before accounting for its flush",
            port.clock.get()
        );
        assert!(port.clock.get() < crate::pot_changes::WAIT_UNTIL_WINDOW_MS);
        assert_eq!(port.posts.get(), 2);
        assert_eq!(
            port.total(crate::pot_changes::COUNTER_POT_CHANGED_DEFERRED),
            9
        );
        let parked: i64 = port
            .store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM change_parked WHERE kind = 'pot'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(parked, 9);
        let word_deadline = port.clock.get() + 20_000;
        let (_, frames) = crate::change_flush::run(crate::race_or_deadline(
            crate::tracker_events::ship_in_order_with(
                vec![serde_json::json!({"txid": port.txid(), "kind": "seen", "retry": true})],
                |_| std::future::pending::<Result<bool, String>>(),
                |_| async {},
                || port.clock.get(),
                word_deadline,
                |ms| port.delay(ms),
            ),
            port.delay(crate::pot_changes::WAIT_UNTIL_WINDOW_MS),
        ))
        .expect("the word deadline also precedes platform cancellation");
        assert_eq!(frames.len(), 1);
        let mut batch = crate::change_flush::FlushBatch::default();
        crate::tracker_events::park_walled(&mut batch, frames);
        crate::change_flush::run(batch.flush_with(&port, port.clock.get()));
        let words: i64 = port
            .store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM change_parked WHERE kind = 'word'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(words, 1, "a timed-out retry is parked, never discarded");
    }

    /// bsv-low #649: the settle's world, recorded; its flush HANGS (never answers), as the app layer's
    /// `/internal/pot-changed` did past the `waitUntil` window on beta (12 to 34.6 s a call).
    struct HangingFlush {
        events: std::cell::RefCell<Vec<String>>,
    }

    impl RefusalSettle for HangingFlush {
        async fn record(&self, counter: &'static str, reason: &str) -> Result<(), String> {
            self.events.borrow_mut().push(format!("memo {reason}"));
            self.events.borrow_mut().push(format!("bump {counter}"));
            Ok(())
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
                "async fn pending_watch_job_with<",
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
                if job_name.contains("_with<") {
                    src.contains(
                        "let Some(last) = pending_watch_job_with(&port, admitted_at_ms).await",
                    ) && src.contains("let started_ms = worker::Date::now().as_millis();")
                } else {
                    job.contains("let started_ms = worker::Date::now().as_millis();")
                },
                "{job_name}: the job records its start"
            );
            let evict = job
                .find("evict_txid_everywhere(")
                .or_else(|| job.find("port.evict("))
                .expect("the eviction");
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
            within.contains("tracker_events::ship_then(&env,tracker_events,")
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
            3,
            "the settle's port (also the watch's SEEN latch), the witness job and the watch's proof read"
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

    /// bsv-low #825 (B-FLEET-4): the conflict pass's world where each candidate's outcome is noted in the fault memo.
    struct MemoRecorder {
        events: std::cell::RefCell<Vec<String>>,
    }

    impl ConflictPassPort for MemoRecorder {
        async fn check(&self, txid: &str, _beef: Option<String>) -> Result<bool, String> {
            match txid {
                "void" => Err("courier exhausted".into()),
                "gone" => Ok(true),
                _ => Ok(false),
            }
        }
        async fn memo(&self, txid: &str, result: &Result<bool, String>) {
            self.events.borrow_mut().push(format!(
                "memo {txid} {}",
                if result.is_err() { "fault" } else { "clean" }
            ));
        }
        async fn persist(&self, d: &ConflictPassSummary) {
            self.events
                .borrow_mut()
                .push(format!("persist {}/{}/{}", d.scanned, d.evicted, d.faults));
        }
        async fn flush(&self) {}
    }

    /// bsv-low #825 (B-FLEET-4): the shipped pass notes EVERY candidate's outcome in the fault memo, the fault and the
    /// clean re-checks alike (a clean one ends the pot's run), before that candidate's counters; and the worker's
    /// port writes the shipped memo statements, its pass sweeps the memo and binds the draw's time. RED with the
    /// loop's memo call removed: no `memo` event.
    #[test]
    fn b_fleet_4_the_pass_notes_each_outcome_in_the_fault_memo() {
        let port = MemoRecorder {
            events: Default::default(),
        };
        let rows = vec![
            ("void".to_string(), None),
            ("quiet".to_string(), None),
            ("gone".to_string(), None),
        ];
        let mut fut = std::pin::pin!(run_conflict_candidates(&port, rows));
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        let std::task::Poll::Ready(summary) = std::future::Future::poll(fut.as_mut(), &mut cx)
        else {
            panic!("the fake answers at once")
        };
        assert_eq!(
            (summary.scanned, summary.evicted, summary.faults),
            (3, 1, 1),
            "scanned stays the denominator: one a re-check"
        );
        assert_eq!(
            *port.events.borrow(),
            vec![
                "memo void fault",
                "persist 1/0/1",
                "memo quiet clean",
                "persist 1/0/0",
                "memo gone clean",
                "persist 1/1/0",
            ]
        );
        let src = include_str!("admit_fast.rs");
        let code = squashed(&src[..src.find("#[cfg(test)]").unwrap()]);
        let port_impl = &code[code
            .find("implConflictPassPortforWorkerConflictPass<'_>{")
            .expect("the worker's port")..];
        let port_impl = &port_impl[..port_impl
            .find("pub(crate)fnconflict_pass_fetcher(")
            .unwrap()];
        assert!(
            port_impl.contains(
                "asyncfnmemo(&self,txid:&str,result:&Result<bool,String>){note_conflict_fault_memo(self.db,txid,result,"
            ),
            "the worker's port writes the memo"
        );
        let note = &code[code
            .find("asyncfnnote_conflict_fault_memo(")
            .expect("the memo's writer")..];
        let note = &note[..note
            .find("pub(crate)asyncfnconflict_fault_memo_health(")
            .unwrap()];
        for step in [
            "Query::new(CONFLICT_FAULT_MEMO_CLEAR_SQL).bind(txid)",
            "Query::new(CONFLICT_FAULT_MEMO_PUT_SQL).bind(txid).bind(now).bind(CONFLICT_FAULT_MEMO_BOUND).bind(CONFLICT_FAULT_MEMO_HOLD_MS).bind(conflict_fault_words(fault))",
            "crate::ops::bump_counter(db,crate::ops::COUNTER_ADMIT_FAST_CONFLICT_HELD,1,)",
        ] {
            assert!(note.contains(step), "the memo's writer: {step}");
        }
        let pass = &code[code
            .find("pub(crate)asyncfnconfirmed_conflict_pass(")
            .expect("the pass")..];
        assert!(pass.contains(
            "Query::new(CONFLICT_FAULT_MEMO_SWEEP_SQL).bind(now-CONFLICT_FAULT_MEMO_SWEEP_AFTER_MS).bind(now).bind(CONFLICT_FAULT_MEMO_SWEEP_LIMIT)"
        ));
        assert!(pass
            .contains("Query::new(CONFLICT_CANDIDATES_SQL).bind(now).fetch_all::<Candidate>(db)"));
        assert!(
            crate::d1::OVERLAY_MIGRATIONS.contains(&CONFLICT_FAULT_MEMO_CREATE),
            "the memo's table is migrated"
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
        let pass = &src[src.find("pub(crate) async fn confirmed_conflict_pass(").unwrap()..];
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
        async fn record(&self, counter: &'static str, reason: &str) -> Result<(), String> {
            self.events.borrow_mut().push(format!("memo {reason}"));
            self.events.borrow_mut().push(format!("bump {counter}"));
            Ok(())
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
            ("async fn pending_watch_job_with<", ""),
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
                    job[uncertain..orphan].ends_with("}}returnNone;}"),
                    "the watch returns right after the evidence match: its settled path reaches no later count or log"
                );
            }
        }
    }

    // ── bsv-low #740: a pushed status for a subject whose live look faulted takes the absence floor ──

    /// What one refusal's evidence did under real SQLite: the verdict, the ledger row's reason (if one was opened),
    /// the uncorroborated counter, and the subject's rows still live (`outputs`, `applied_transactions`).
    struct I740Run {
        verdict: EvidenceVerdict,
        ledger_reason: Option<String>,
        uncorroborated: i64,
        live_rows: i64,
    }

    /// The pinned shape: the subject's admission (one output and its applied row, stamped `admitted_ms` ago), or
    /// none; both indexers answer definitively absent (`Some(false)`); the refusal callback's status is `pushed`.
    /// The look goes through the SHIPPED fold (`fold_arcade_look`), the decision through the SHIPPED seam
    /// (`verdict_of_look`, the admission age read over `ADMISSION_AGE_RUNGS`), a refusal through the SHIPPED core
    /// (`evict_core`, the ledger's open marker first) with the callback's reason wrap, and a kept refusal bumps the
    /// callback's counter by the SHIPPED statement: the three arms of `refusal_job` after its check.
    async fn i740_run(live: ArcadeLook, admitted_ms_ago: Option<u64>) -> I740Run {
        use rusqlite::OptionalExtension;
        let txid = "74".repeat(32);
        let now_ms: u64 = 1_800_000_000_000;
        let conn = shipped_conn();
        if let Some(ago) = admitted_ms_ago {
            conn.execute(
                "INSERT INTO outputs (txid, outputIndex, outputScript, topic, satoshis, spent, score) VALUES (?1, 0, X'51', 'tm_pot', 40000, 0, ?2)",
                rusqlite::params![&txid, (now_ms - ago) as f64],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO applied_transactions (txid, topic) VALUES (?1, 'tm_pot')",
                [&txid],
            )
            .unwrap();
        }
        let db = SqliteShadow::new(conn);
        let pushed = ("REJECTED".to_string(), "pushed extra".to_string());
        let look = crate::proof_fetcher::fold_arcade_look(live, Some(pushed.clone()));
        let indexers = async {
            (
                crate::door_words::CourierWord::Absent,
                crate::door_words::CourierWord::Absent,
            )
        };
        let conn = &db.conn;
        let txid_ref = txid.as_str();
        let age = admission_age_over(
            |sql| async move {
                conn.query_row(sql, [txid_ref], |r| r.get::<_, Option<f64>>(0))
                    .optional()
                    .ok()
                    .flatten()
                    .flatten()
            },
            now_ms,
        );
        let verdict = verdict_of_look(&look, indexers, age).await;
        match &verdict {
            EvidenceVerdict::Refused(reason) => {
                let core = evict_core(&db, &txid, &format!("{} ({reason})", pushed.0), now_ms).await;
                assert!(core.faults.is_empty(), "{:?}", core.faults);
            }
            EvidenceVerdict::Present | EvidenceVerdict::Uncertain(_) => {
                db.conn
                    .execute(
                        crate::ops::BUMP_COUNTER_SQL,
                        rusqlite::params![crate::ops::COUNTER_ARC_INGEST_REFUSAL_UNCORROBORATED, 1i64],
                    )
                    .unwrap();
            }
        }
        let ledger_reason = db
            .conn
            .query_row(
                "SELECT reason FROM pot_evictions WHERE txid = ?1",
                [&txid],
                |r| r.get::<_, String>(0),
            )
            .optional()
            .unwrap();
        let uncorroborated = db
            .conn
            .query_row(
                "SELECT value FROM ops_counters WHERE name = ?1",
                [crate::ops::COUNTER_ARC_INGEST_REFUSAL_UNCORROBORATED],
                |r| r.get::<_, i64>(0),
            )
            .optional()
            .unwrap()
            .unwrap_or(0);
        let live_rows = count(&db.conn, "outputs", "txid", &txid)
            + count(&db.conn, "applied_transactions", "txid", &txid);
        I740Run {
            verdict,
            ledger_reason,
            uncorroborated,
            live_rows,
        }
    }

    /// The served eviction word of a ledger reason (`/utxo-status.evicted.word`, `ls_pot spentStatus.evicted.word`).
    fn i740_word(reason: &str) -> Option<&'static str> {
        overlay_discovery::pot::storage::eviction_chain_word(reason)
    }

    /// bsv-low #740 (1): the live look faulted, the pushed REJECTED filled it, both indexers are absent, and the
    /// subject was admitted a minute ago: a refusal on absence alone, under the floor. Nothing is evicted, the
    /// refusal is counted uncorroborated, every row of the subject is still live.
    #[tokio::test]
    async fn i740_a_pushed_status_under_a_faulted_look_on_a_young_admission_evicts_nothing() {
        let run = i740_run(ArcadeLook::Fault, Some(60_000)).await;
        assert!(
            matches!(run.verdict, EvidenceVerdict::Uncertain(_)),
            "the pushed word under a faulted look is not Arcade's own word: {:?}",
            run.verdict
        );
        assert_eq!(run.ledger_reason, None, "no eviction opened");
        assert_eq!(run.uncorroborated, 1);
        assert_eq!(run.live_rows, 2, "the subject's output and applied row stay live");
    }

    /// bsv-low #740 (2): the same with a subject the index never admitted: nothing evicted, counted, no ledger row.
    #[tokio::test]
    async fn i740_b_a_pushed_status_under_a_faulted_look_on_a_never_admitted_subject_opens_no_eviction() {
        let run = i740_run(ArcadeLook::Fault, None).await;
        assert!(
            matches!(run.verdict, EvidenceVerdict::Uncertain(_)),
            "{:?}",
            run.verdict
        );
        assert_eq!(run.ledger_reason, None, "no eviction ledger row is opened");
        assert_eq!(run.uncorroborated, 1);
    }

    /// bsv-low #740 (3): the same with the subject admitted past the floor: the absence-based refusal stands, as a
    /// faulted look with no pushed word yields it, and the ledger reason serves the ABSENCE shape (no chain word),
    /// never Arcade's `rejected`.
    #[tokio::test]
    async fn i740_c_a_pushed_status_under_a_faulted_look_on_an_aged_admission_is_the_absence_refusal() {
        let run = i740_run(ArcadeLook::Fault, Some(REFUSAL_ABSENT_MIN_AGE_MS + 60_000)).await;
        let EvidenceVerdict::Refused(reason) = &run.verdict else {
            panic!("an aged admission keeps the absence refusal: {:?}", run.verdict);
        };
        assert!(reason.starts_with("arcade faulted, recorded REJECTED: "), "{reason}");
        let ledger = run.ledger_reason.expect("the eviction is opened");
        assert_eq!(ledger, format!("REJECTED ({reason})"));
        assert_eq!(i740_word(&ledger), None, "an eviction on absence serves no chain word");
        assert_eq!(run.uncorroborated, 0);
        assert_eq!(run.live_rows, 0, "the shadow move took the subject's rows");
        // the same aged subject under a faulted look with NO pushed word: the same decision, the same served word
        let bare = evidence_verdict(&ArcadeLook::Fault, Some(false), Some(false), false);
        let EvidenceVerdict::Refused(bare) = bare else {
            panic!("{bare:?}")
        };
        assert_eq!(i740_word(&format!("REJECTED ({bare})")), None);
    }

    /// bsv-low #740 (4), the guard against widening: Arcade's OWN live REJECTED, a young subject, both indexers
    /// absent: evicted as before (Arcade's own word needs no age), served `rejected`.
    #[tokio::test]
    async fn i740_d_a_live_rejected_on_a_young_admission_still_evicts_and_serves_rejected() {
        let run = i740_run(
            ArcadeLook::Fatal("REJECTED".into(), "live extra".into()),
            Some(60_000),
        )
        .await;
        let EvidenceVerdict::Refused(reason) = &run.verdict else {
            panic!("Arcade's own word needs no age: {:?}", run.verdict);
        };
        assert_eq!(reason, "arcade live REJECTED: live extra; both indexers absent");
        let ledger = run.ledger_reason.expect("the eviction is opened");
        assert_eq!(i740_word(&ledger), Some("rejected"));
        assert_eq!(run.uncorroborated, 0);
        assert_eq!(run.live_rows, 0);
    }

    // ── bsv-low #831 half 2: the retire pass ENDS a ghost pot (the eviction), and looks again at its backlog ──

    /// The retire pass's world over real SQLite (the shipped schema): the SHIPPED steps (`retire_candidate`,
    /// `end_retired_pot`, `run_retire_backlog`) run here as the worker runs them. Arcade's live word and the two
    /// indexers are given per txid (default: Arcade 404, both indexers absent); the look folds the recorded word the
    /// SHIPPED way (`fold_arcade_look`). The eviction is `evict_txid_everywhere_repointed`'s steps over the same
    /// connection: the SHIPPED core (`evict_core`: the OPEN marker first, the two passes), the SHIPPED release
    /// statements (`select_pot_spends_sql`, `release_pot_spends_sql`), the SHIPPED merge and close
    /// (`merge_released`, `CLOSE_EVICTION_MARKER_SQL`); its notes and counters are the wrapper's D1-only halves.
    mod i831 {
        use super::*;
        use crate::proof_fetcher::{
            end_retired_pot, fold_arcade_look, retire_backlog_start, retire_candidate,
            retire_eviction_reason, retire_rotation_key, run_retire_backlog, ArcadeLook,
            NetworkPresence, RetireBacklogSummary, RetireBudget, RetireCandidateRow, RetireEnd,
            RetireEndState, RetireEviction, RetirePort, RetireStore, RetireSummary,
            RETIRE_ARM_ROTATION_SALT, RETIRE_BACKLOG_CANDIDATES_SQL, RETIRE_BACKLOG_LIMIT,
            RETIRE_BACKLOG_WINDOW, RETIRE_CANDIDATES_POT_BEEFS_SQL,
            RETIRE_CANDIDATES_TRANSACTIONS_SQL, RETIRE_END_RESERVE_STATEMENTS,
            RETIRE_END_STATE_SQL, RETIRE_ENGINE_LATCH_CLEAR_SQL, RETIRE_LATCH_CLEAR_SQL,
            RETIRE_PASS_STATEMENT_BUDGET, RETIRE_POT_BEEFS_SQL, RETIRE_REASON_MARK,
            RETIRE_TRANSACTIONS_SQL, TX_ANY_VERDICT_DELETE_SQL, TX_ANY_VERDICT_UPSERT_SQL,
        };
        use overlay_discovery::pot::storage::eviction_chain_word;
        use std::cell::RefCell;
        use std::collections::HashMap;

        pub(super) const NOW: i64 = 1_800_000_000_000;
        pub(super) const HOUR: i64 = 3_600_000;

        type Word = (ArcadeLook, Option<NetworkPresence>, Option<NetworkPresence>);

        pub(super) struct SqliteRetire {
            pub(super) db: SqliteShadow,
            words: RefCell<HashMap<String, Word>>,
            /// Every courier call, in order: `arcade <txid>`, `bitails <txid>`, `woc <txid>`.
            pub(super) asked: RefCell<Vec<String>>,
            pub(super) persisted: RefCell<Vec<(&'static str, u64)>>,
            /// The backlog read answers this instead of its SQL (the belt's pin).
            answer_rows: RefCell<Option<Vec<RetireCandidateRow>>>,
            /// The backlog read's start key (`None`: the shipped one, `retire_backlog_start`).
            start: RefCell<Option<String>>,
            /// The FIX-831-B lens's MEDIUM-2: every statement the port issued (the shipped `evict_core`'s through a
            /// counting `ShadowDb`, the wrapper's and the port's own by their shape), the budget's count.
            pub(super) stmts: std::cell::Cell<u64>,
            /// The largest ending in statements (the reserve's pin).
            pub(super) largest_ending: std::cell::Cell<u64>,
            /// The FIX-831-B lens's LOW-1: run after the courier look of a txid (a proof landing mid-pass).
            pub(super) after_look: RefCell<Option<AfterLook>>,
            /// The FIX-831-B lens's LOW-2: the eviction answers incomplete (a release that faulted).
            pub(super) incomplete: std::cell::Cell<bool>,
            /// bsv-low #836 fold: the chain's bound a non-final look is asked against.
            pub(super) bound: std::cell::Cell<crate::door_words::LockBound>,
        }

        /// The LOW-1 pin's hook: run over the connection and the txid after the courier look.
        pub(super) type AfterLook = Box<dyn Fn(&rusqlite::Connection, &str)>;

        /// A `ShadowDb` that counts every statement it is asked to run (the lens's counting shape).
        struct Counting<'a>(&'a SqliteShadow, &'a std::cell::Cell<u64>);
        impl Counting<'_> {
            fn bump(&self) {
                self.1.set(self.1.get() + 1);
            }
        }
        #[async_trait(?Send)]
        impl ShadowDb for Counting<'_> {
            async fn columns(&self, t: &str) -> Result<Vec<ColumnInfo>, String> {
                self.bump();
                self.0.columns(t).await
            }
            async fn count(&self, t: &str, k: &str, x: &str) -> Result<u64, String> {
                self.bump();
                self.0.count(t, k, x).await
            }
            async fn vouts(&self, t: &str, x: &str) -> Result<Vec<u32>, String> {
                self.bump();
                self.0.vouts(t, x).await
            }
            async fn exec(&self, q: &str, b: Vec<QVal>) -> Result<(), String> {
                self.bump();
                self.0.exec(q, b).await
            }
            async fn query_i64(&self, q: &str, b: Vec<QVal>) -> Result<Option<i64>, String> {
                self.bump();
                self.0.query_i64(q, b).await
            }
        }

        impl SqliteRetire {
            pub(super) fn new() -> Self {
                Self {
                    db: SqliteShadow::new(shipped_conn()),
                    words: RefCell::new(HashMap::new()),
                    asked: RefCell::new(Vec::new()),
                    persisted: RefCell::new(Vec::new()),
                    answer_rows: RefCell::new(None),
                    start: RefCell::new(None),
                    stmts: std::cell::Cell::new(0),
                    largest_ending: std::cell::Cell::new(0),
                    after_look: RefCell::new(None),
                    incomplete: std::cell::Cell::new(false),
                    bound: std::cell::Cell::new(crate::door_words::LockBound::default()),
                }
            }
            fn stmt(&self, n: u64) {
                self.stmts.set(self.stmts.get() + n);
            }
            pub(super) fn say(
                &self,
                txid: &str,
                arcade: ArcadeLook,
                bitails: Option<NetworkPresence>,
                woc: Option<NetworkPresence>,
            ) {
                self.words
                    .borrow_mut()
                    .insert(txid.to_string(), (arcade, bitails, woc));
            }
            fn word(&self, txid: &str) -> Word {
                self.words.borrow().get(txid).cloned().unwrap_or((
                    ArcadeLook::Missing,
                    Some(NetworkPresence::Absent),
                    Some(NetworkPresence::Absent),
                ))
            }
            pub(super) fn conn(&self) -> &rusqlite::Connection {
                &self.db.conn
            }
            pub(super) fn total(&self, name: &str) -> u64 {
                self.persisted
                    .borrow()
                    .iter()
                    .filter(|(n, _)| *n == name)
                    .map(|(_, d)| d)
                    .sum()
            }
        }

        impl RetirePort for SqliteRetire {
            async fn arcade(&self, txid: &str, evidence: Option<(String, String)>) -> ArcadeLook {
                self.asked.borrow_mut().push(format!("arcade {txid}"));
                if let Some(hook) = self.after_look.borrow().as_ref() {
                    hook(self.conn(), txid);
                }
                fold_arcade_look(self.word(txid).0, evidence)
            }
            async fn lock_bound(&self, txid: &str) -> crate::door_words::LockBound {
                self.asked.borrow_mut().push(format!("lock_bound {txid}"));
                self.bound.get()
            }
            async fn bitails(&self, txid: &str) -> Option<NetworkPresence> {
                self.asked.borrow_mut().push(format!("bitails {txid}"));
                self.word(txid).1
            }
            async fn woc(&self, txid: &str) -> Option<NetworkPresence> {
                self.asked.borrow_mut().push(format!("woc {txid}"));
                self.word(txid).2
            }
            async fn latch(
                &self,
                store: RetireStore,
                txid: &str,
                reason: &str,
                now_ms: i64,
            ) -> Result<(), String> {
                let r = match store {
                    RetireStore::Transactions => self.conn().execute(
                        RETIRE_TRANSACTIONS_SQL,
                        rusqlite::params![now_ms, reason, txid],
                    ),
                    RetireStore::PotBeefs => self.conn().execute(RETIRE_POT_BEEFS_SQL, [txid]),
                };
                self.stmt(1);
                r.map(|_| ()).map_err(|e| e.to_string())
            }
            async fn latched(
                &self,
                _store: RetireStore,
                txid: &str,
                reason: &str,
                _look: &ArcadeLook,
                now_ms: i64,
            ) {
                let kind = crate::proof_fetcher::verdict_kind_of_retire_reason(reason);
                self.conn()
                    .execute(
                        TX_ANY_VERDICT_UPSERT_SQL,
                        rusqlite::params![txid, now_ms, kind, reason],
                    )
                    .unwrap();
                // the worker's: the memo upsert and its counter (and `arc_terminal` on a live fatal word)
                self.stmt(2);
            }
            async fn end_state(&self, txid: &str) -> Result<RetireEndState, String> {
                self.stmt(1);
                self.conn()
                    .query_row(RETIRE_END_STATE_SQL, [txid], |r| {
                        Ok(RetireEndState {
                            live: r.get::<_, i64>(0)? as f64,
                            proven: r.get::<_, i64>(1)? as f64,
                            latched: r.get::<_, i64>(2)? as f64,
                        })
                    })
                    .map_err(|e| e.to_string())
            }
            async fn evict(
                &self,
                txid: &str,
                reason: &str,
                asked_ms: u64,
            ) -> Result<RetireEviction, String> {
                let before = self.stmts.get();
                let core =
                    evict_core(&Counting(&self.db, &self.stmts), txid, reason, asked_ms).await;
                assert!(core.faults.is_empty(), "{:?}", core.faults);
                // the D1 wrapper's own statements by its shape (`evict_txid_everywhere_repointed`): `vouts_of` twice
                // (its columns and its read each), the release's columns, read and write, the released list's read,
                // the close
                self.stmt(9);
                if core.yielded_to_readmission.is_some() {
                    return Ok(RetireEviction::Yielded);
                }
                let pot_cols = cols(self.conn(), "pot_records");
                let rows: Vec<(String, i64, i64)> = self
                    .conn()
                    .prepare(&select_pot_spends_sql(&pot_cols))
                    .unwrap()
                    .query_map([txid], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                    .unwrap()
                    .map(Result::unwrap)
                    .collect();
                self.conn()
                    .execute(&release_pot_spends_sql(&pot_cols), [txid])
                    .unwrap();
                let released: Vec<ReleasedSpend> = rows
                    .into_iter()
                    .filter(|r| r.2 == 0)
                    .map(|r| ReleasedSpend {
                        table: "pot_records".into(),
                        txid: r.0,
                        vout: r.1 as u32,
                    })
                    .collect();
                use rusqlite::OptionalExtension;
                let existing: Option<String> = self
                    .conn()
                    .query_row(RELEASED_SPENDS_SQL, [txid], |r| r.get(0))
                    .optional()
                    .unwrap()
                    .flatten();
                let merged = merge_released(existing.as_deref(), &released);
                self.conn()
                    .execute(
                        CLOSE_EVICTION_MARKER_SQL,
                        rusqlite::params![
                            txid,
                            reason,
                            asked_ms as i64,
                            core.moved as i64,
                            serde_json::to_string(&merged).unwrap()
                        ],
                    )
                    .unwrap();
                let ending = self.stmts.get() - before;
                self.largest_ending
                    .set(self.largest_ending.get().max(ending));
                if self.incomplete.get() {
                    return Ok(RetireEviction::Incomplete);
                }
                Ok(RetireEviction::Stands)
            }
            async fn backlog_candidates(
                &self,
                now_ms: i64,
                start: &str,
            ) -> Result<Vec<RetireCandidateRow>, String> {
                if let Some(rows) = self.answer_rows.borrow_mut().take() {
                    return Ok(rows);
                }
                let start = self
                    .start
                    .borrow()
                    .clone()
                    .unwrap_or_else(|| start.to_string());
                self.stmt(1);
                read_candidates(
                    self.conn(),
                    RETIRE_BACKLOG_CANDIDATES_SQL,
                    rusqlite::params![now_ms, start],
                )
            }
            async fn clear_latch(&self, txid: &str) -> Result<(), String> {
                self.stmt(3);
                self.conn()
                    .execute(RETIRE_LATCH_CLEAR_SQL, [txid])
                    .map_err(|e| e.to_string())?;
                self.conn()
                    .execute(RETIRE_ENGINE_LATCH_CLEAR_SQL, [txid])
                    .map_err(|e| e.to_string())?;
                self.conn()
                    .execute(TX_ANY_VERDICT_DELETE_SQL, [txid])
                    .map_err(|e| e.to_string())?;
                Ok(())
            }
            async fn persist(&self, deltas: &[(&'static str, u64)]) {
                self.stmt(deltas.iter().filter(|(_, d)| *d > 0).count() as u64);
                self.persisted.borrow_mut().extend_from_slice(deltas);
            }
            fn statements(&self) -> u64 {
                self.stmts.get()
            }
            fn now_ms(&self) -> i64 {
                NOW
            }
        }

        fn read_candidates(
            conn: &rusqlite::Connection,
            sql: &str,
            params: impl rusqlite::Params,
        ) -> Result<Vec<RetireCandidateRow>, String> {
            let mut st = conn.prepare(sql).map_err(|e| e.to_string())?;
            let rows = st
                .query_map(params, |r| {
                    Ok(RetireCandidateRow {
                        txid: r.get(0)?,
                        age_secs: r.get::<_, f64>(1)?,
                        term_status: r.get(2)?,
                        term_extra: r.get(3)?,
                    })
                })
                .map_err(|e| e.to_string())?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| e.to_string())?;
            Ok(rows)
        }

        /// #831's shape: a JOIN admitted pending (its covenant pot row, its output and two applied rows, its engine
        /// row, its stored BEEF unproven, `admitted_ago_ms` old on the watch's clock) that spent two hops (their
        /// rows live, their pointers naming it, unconfirmed). `latched`: the pot_beefs row already latched
        /// `structurally_unprovable` and off the watch (the backlog's shape).
        pub(super) fn seed_ghost_join(
            conn: &rusqlite::Connection,
            join: &str,
            hops: &[&str],
            admitted_ago_ms: i64,
            latched: bool,
        ) {
            let at = NOW - admitted_ago_ms;
            conn.execute(
                "INSERT INTO pot_records (txid, outputIndex, spent, createdAt, lockKind, pubA, stakeA) VALUES (?1, 0, 0, ?2, 'covenant', '02aa', 9900)",
                rusqlite::params![join, at / 1000],
            )
            .unwrap();
            for hop in hops {
                conn.execute(
                    "INSERT INTO pot_records (txid, outputIndex, spent, spendingTxid, spentConfirmed, spentAt, createdAt) VALUES (?1, 0, 1, ?2, 0, ?3, ?4)",
                    rusqlite::params![hop, join, at / 1000, at / 1000 - 60],
                )
                .unwrap();
            }
            conn.execute("INSERT INTO outputs (txid, outputIndex, outputScript, topic, satoshis, spent) VALUES (?1, 0, X'51', 'tm_pot', 19800, 0)", [join]).unwrap();
            for topic in ["tm_pot", "tm_lowfund"] {
                conn.execute(
                    "INSERT INTO applied_transactions (txid, topic) VALUES (?1, ?2)",
                    rusqlite::params![join, topic],
                )
                .unwrap();
            }
            conn.execute(
                "INSERT INTO transactions (txid, beef, has_proof, created_at) VALUES (?1, X'00', 0, ?2)",
                rusqlite::params![join, at / 1000],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO pot_beefs (txid, beef, createdAt, has_proof, structurally_unprovable) VALUES (?1, X'0100beef', ?2, 0, ?3)",
                rusqlite::params![join, at / 1000, if latched { Some(1) } else { None }],
            )
            .unwrap();
            if !latched {
                conn.execute(
                    "INSERT INTO proofless_watch (txid, first_seen_ms) VALUES (?1, ?2)",
                    rusqlite::params![join, at],
                )
                .unwrap();
            }
        }

        /// The retire pass's two arms over the shipped candidate reads: every candidate through the shipped step,
        /// under the shipped budget (the `pot_beefs` arm's rotation started at the bottom of the key space).
        pub(super) async fn retire_pot_beefs(port: &SqliteRetire) -> RetireSummary {
            let budget = RetireBudget::begin(port);
            retire_arms(port, &budget, "").await
        }

        /// The two arms under `budget`, the `pot_beefs` arm's rotation from `arm_start` (the pass's own order:
        /// the engine arm, then the `pot_beefs` arm).
        pub(super) async fn retire_arms(
            port: &SqliteRetire,
            budget: &RetireBudget,
            arm_start: &str,
        ) -> RetireSummary {
            let mut summary = RetireSummary::default();
            let rows = read_candidates(
                port.conn(),
                RETIRE_CANDIDATES_TRANSACTIONS_SQL,
                rusqlite::params![NOW],
            )
            .unwrap();
            port.stmt(1);
            for row in rows {
                retire_candidate(port, budget, RetireStore::Transactions, &row, &mut summary).await;
            }
            let rows = read_candidates(
                port.conn(),
                RETIRE_CANDIDATES_POT_BEEFS_SQL,
                rusqlite::params![NOW, arm_start],
            )
            .unwrap();
            port.stmt(1);
            for row in rows {
                retire_candidate(port, budget, RetireStore::PotBeefs, &row, &mut summary).await;
            }
            summary
        }

        /// One backlog pass under a fresh shipped budget.
        pub(super) async fn backlog(port: &SqliteRetire) -> RetireBacklogSummary {
            let budget = RetireBudget::begin(port);
            run_retire_backlog(port, &budget).await
        }

        /// One backlog pass with the statement budget lifted (the pins of the page's own bounds).
        pub(super) async fn backlog_unbounded(port: &SqliteRetire) -> RetireBacklogSummary {
            let budget = RetireBudget::with_limit(port, u64::MAX);
            run_retire_backlog(port, &budget).await
        }

        pub(super) fn ledger(
            conn: &rusqlite::Connection,
            txid: &str,
        ) -> Option<(String, Option<i64>, Vec<ReleasedSpend>)> {
            use rusqlite::OptionalExtension;
            conn.query_row(
                "SELECT reason, readmittedAt, releasedSpends FROM pot_evictions WHERE txid = ?1",
                [txid],
                |r| {
                    let json: Option<String> = r.get(2)?;
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        json.and_then(|j| serde_json::from_str(&j).ok())
                            .unwrap_or_default(),
                    ))
                },
            )
            .optional()
            .unwrap()
        }

        pub(super) fn hop(conn: &rusqlite::Connection, txid: &str) -> (i64, Option<String>) {
            conn.query_row(
                "SELECT spent, spendingTxid FROM pot_records WHERE txid = ?1",
                [txid],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap()
        }

        fn latch_of(conn: &rusqlite::Connection, txid: &str) -> Option<i64> {
            conn.query_row(
                "SELECT structurally_unprovable FROM pot_beefs WHERE txid = ?1",
                [txid],
                |r| r.get(0),
            )
            .unwrap()
        }

        fn memo_kind(conn: &rusqlite::Connection, txid: &str) -> Option<String> {
            use rusqlite::OptionalExtension;
            conn.query_row(
                "SELECT kind FROM tx_any_verdicts WHERE txid = ?1",
                [txid],
                |r| r.get(0),
            )
            .optional()
            .unwrap()
        }

        /// The ended pot's end state, common to (a) and (b): every row of the JOIN shadow-moved, the pot_beefs bytes
        /// kept and latched, both hops released into the ledger's list, the ledger OPEN under the retire reason, and
        /// that reason serving no chain word.
        fn assert_ended(port: &SqliteRetire, join: &str, hops: &[&str], verdict_head: &str) {
            let c = port.conn();
            assert_eq!(
                count(c, "pot_records", "txid", join),
                0,
                "the pot row left the index"
            );
            assert_eq!(
                count(c, "pot_records_evicted", "txid", join),
                1,
                "shadow-moved, not deleted"
            );
            assert_eq!(count(c, "outputs", "txid", join), 0);
            assert_eq!(count(c, "applied_transactions", "txid", join), 0);
            assert_eq!(count(c, "transactions", "txid", join), 0);
            assert_eq!(
                count(c, "pot_beefs", "txid", join),
                1,
                "the bytes stay: the readmission's source"
            );
            assert_eq!(latch_of(c, join), Some(1));
            for h in hops {
                assert_eq!(hop(c, h), (0, None), "the JOIN's spend of {h} released");
            }
            let (reason, readmitted, released) = ledger(c, join).expect("the ledger row");
            assert_eq!(readmitted, None, "open");
            assert!(reason.starts_with(RETIRE_REASON_MARK), "{reason}");
            assert!(reason.contains(verdict_head), "{reason}");
            assert_eq!(
                eviction_chain_word(&reason),
                None,
                "a retire eviction serves no chain word: {reason}"
            );
            let mut got: Vec<String> = released.iter().map(|r| r.txid.clone()).collect();
            got.sort();
            let mut want: Vec<String> = hops.iter().map(|h| h.to_string()).collect();
            want.sort();
            assert_eq!(got, want, "releasedSpends names both hops");
            assert_eq!(
                port.total(crate::ops::COUNTER_RETIRE_EVICTED)
                    + port.total(crate::ops::COUNTER_RETIRE_BACKLOG_EVICTED),
                1,
                "one ending counted, by the arm or the backlog that made it"
            );
        }

        /// bsv-low #836 fold (the lens's M1): the retire pass bounds Arcade's non-final word by the chain, through the
        /// shipped `retire_evidence` over the port: inside the window (the subject's own lock height against the tip,
        /// plus `NOT_FINAL_LAG_BLOCKS`) it is held and the 1 h arm is not reached; past it the word is Arcade's fatal
        /// word again and takes the 1 h terminal arm, as any other; a subject FINAL by its bytes is never held; the
        /// bound is asked of a non-final look only.
        #[tokio::test]
        async fn fix836f_m1_the_retire_pass_bounds_a_non_final_look_by_the_chain() {
            use crate::door_words::{LockBound, SubjectLock};
            use crate::proof_fetcher::retire_evidence;
            let port = SqliteRetire::new();
            let refund = "f8".repeat(32);
            let extra = "TX_LOCK_TIME (35): lock time (972185) as block height is not less than block height (972172)";
            port.say(
                &refund,
                ArcadeLook::NotFinal("REJECTED".into(), extra.into()),
                Some(NetworkPresence::Absent),
                Some(NetworkPresence::Absent),
            );
            let at = |tip: u64, lock: SubjectLock| LockBound {
                lock: Some(lock),
                tip: Some(tip),
                now_secs: 0,
            };
            port.bound.set(at(972_185, SubjectLock::Height(972_185)));
            let held = retire_evidence(&port, &refund, None).await;
            assert!(matches!(held.look, ArcadeLook::NotFinal(..)), "{:?}", held.look);
            assert_eq!(crate::proof_fetcher::retire_verdict_of(&held.look, &held.bitails, &held.woc, 7_200), None);
            // the last tip inside the window, then the first past it
            port.bound.set(at(972_208, SubjectLock::Height(972_185)));
            let edge = retire_evidence(&port, &refund, None).await;
            assert!(matches!(edge.look, ArcadeLook::NotFinal(..)), "{:?}", edge.look);
            port.bound.set(at(972_209, SubjectLock::Height(972_185)));
            let past = retire_evidence(&port, &refund, None).await;
            assert_eq!(past.look, ArcadeLook::Fatal("REJECTED".into(), extra.into()));
            let reason = crate::proof_fetcher::retire_verdict_of(&past.look, &past.bitails, &past.woc, 7_200)
                .expect("past the lag the word is Arcade's own: the 1 h terminal arm");
            assert!(reason.starts_with("arcade REJECTED: TX_LOCK_TIME (35)"), "{reason}");
            // a subject final by its bytes is never held, even at the loop 44 tip
            port.bound.set(at(972_185, SubjectLock::Final));
            let final_ = retire_evidence(&port, &refund, None).await;
            assert!(matches!(final_.look, ArcadeLook::Fatal(..)), "{:?}", final_.look);
            // no tip (no tracker, or a fault): held, fail-safe; the 48 h absence arm still ends it
            port.bound.set(LockBound {
                lock: Some(SubjectLock::Height(972_185)),
                tip: None,
                now_secs: 0,
            });
            let blind = retire_evidence(&port, &refund, None).await;
            assert!(matches!(blind.look, ArcadeLook::NotFinal(..)));
            assert!(crate::proof_fetcher::retire_verdict_of(&blind.look, &blind.bitails, &blind.woc, 49 * 3_600).is_some());
            // the bound is asked of a non-final look only
            let other = "f9".repeat(32);
            port.say(&other, ArcadeLook::Missing, Some(NetworkPresence::Absent), Some(NetworkPresence::Absent));
            let _ = retire_evidence(&port, &other, None).await;
            let asked = port.asked.borrow();
            assert_eq!(asked.iter().filter(|a| a.starts_with("lock_bound")).count(), 5, "{asked:?}");
            assert!(!asked.iter().any(|a| *a == format!("lock_bound {other}")), "{asked:?}");
        }

        /// (a) A never-held JOIN admitted pending, 49 h proofless, Arcade 404 and both indexers absent: the retire
        /// pass's absence verdict latches it AND evicts it (where `659e54cc` only latched it). A `pot_beefs` row with
        /// no pot row (not a pot) and a `transactions`-only row are latched and nothing more.
        #[tokio::test]
        async fn i831_a_a_never_held_pot_at_the_48h_absence_verdict_is_evicted_by_the_retire_pass()
        {
            let port = SqliteRetire::new();
            let join = "83".repeat(32);
            let hops = ["a1".repeat(32), "b1".repeat(32)];
            let hop_refs: Vec<&str> = hops.iter().map(String::as_str).collect();
            seed_ghost_join(port.conn(), &join, &hop_refs, 49 * HOUR, false);
            // the controls: a stored BEEF that keys no pot row, and an engine row alone, both as dead
            let refund = "e1".repeat(32);
            port.conn()
                .execute(
                    "INSERT INTO pot_beefs (txid, beef, createdAt, has_proof) VALUES (?1, X'00', ?2, 0)",
                    rusqlite::params![refund, (NOW - 49 * HOUR) / 1000],
                )
                .unwrap();
            port.conn()
                .execute(
                    "INSERT INTO proofless_watch (txid, first_seen_ms) VALUES (?1, ?2)",
                    rusqlite::params![refund, NOW - 49 * HOUR],
                )
                .unwrap();
            let summary = retire_pot_beefs(&port).await;
            // the JOIN is a candidate of both stores: the engine arm retires it first and (the FIX-831-B lens's
            // MEDIUM-1) latches its stored BEEF and ends it in the same step, so the pot_beefs arm no longer reads
            // it; the refund is the pot_beefs arm's alone
            assert_ended(&port, &join, &hop_refs, "network-absent ");
            assert_eq!(summary.retired, 2, "{summary:?}");
            assert_eq!(
                summary.evicted_on_retire, 1,
                "only the live pot is ended: {summary:?}"
            );
            assert_eq!(summary.evict_faults, 0);
            assert_eq!(
                memo_kind(port.conn(), &join).as_deref(),
                Some("retired"),
                "the retire memo stands"
            );
            assert_eq!(latch_of(port.conn(), &refund), Some(1));
            assert_eq!(ledger(port.conn(), &refund), None, "no pot, no eviction");
        }

        /// (b) The same pot 2 h proofless with Arcade's LIVE fatal word and both indexers absent: the 1 h arm retires
        /// and ends it; its reason carries Arcade's word and still serves no chain word.
        #[tokio::test]
        async fn i831_b_a_live_fatal_word_at_1h_ends_the_pot_likewise() {
            let port = SqliteRetire::new();
            let join = "84".repeat(32);
            let hops = ["a2".repeat(32), "b2".repeat(32)];
            let hop_refs: Vec<&str> = hops.iter().map(String::as_str).collect();
            seed_ghost_join(port.conn(), &join, &hop_refs, 2 * HOUR, false);
            port.say(
                &join,
                ArcadeLook::Fatal(
                    "REJECTED".into(),
                    "arcade live REJECTED: missing inputs".into(),
                ),
                Some(NetworkPresence::Absent),
                Some(NetworkPresence::Absent),
            );
            let summary = retire_pot_beefs(&port).await;
            assert_eq!(summary.evicted_on_retire, 1, "{summary:?}");
            assert_ended(&port, &join, &hop_refs, "arcade REJECTED: ");
            assert_eq!(memo_kind(port.conn(), &join).as_deref(), Some("refused"));
            // the same pot under a fault only (no word, no record): kept, never ended
            let port = SqliteRetire::new();
            seed_ghost_join(port.conn(), &join, &hop_refs, 2 * HOUR, false);
            port.say(&join, ArcadeLook::Fault, None, None);
            let summary = retire_pot_beefs(&port).await;
            assert_eq!(
                (summary.retired, summary.evicted_on_retire),
                (0, 0),
                "{summary:?}"
            );
            assert_eq!(count(port.conn(), "pot_records", "txid", &join), 1);
        }

        /// (c) The backlog pass leaves a latched pot the fresh look finds HELD: an indexer holds it (in a mempool),
        /// or WoC names its block, or Arcade's live word is non-terminal. Each is left alone, its latch and memo
        /// cleared, its rows and its hops' pointers untouched, counted `left`; beside them, a latched pot the look
        /// finds absent at 49 h is evicted (the pass does evict).
        #[tokio::test]
        async fn i831_c_a_latched_pot_the_re_look_finds_mined_or_held_is_left_alone() {
            let port = SqliteRetire::new();
            let held = [
                (
                    "c1".repeat(32),
                    ArcadeLook::Missing,
                    Some(NetworkPresence::Present { height: None }),
                    None,
                ),
                (
                    "c2".repeat(32),
                    ArcadeLook::Missing,
                    Some(NetworkPresence::Absent),
                    Some(NetworkPresence::Present {
                        height: Some(918_000),
                    }),
                ),
                ("c3".repeat(32), ArcadeLook::Present, None, None),
            ];
            for (i, (pot, arcade, bitails, woc)) in held.iter().enumerate() {
                let h = format!("{:02x}", 0xd0 + i).repeat(32);
                seed_ghost_join(port.conn(), pot, &[&h], 49 * HOUR, true);
                port.conn()
                    .execute(
                        TX_ANY_VERDICT_UPSERT_SQL,
                        rusqlite::params![pot, NOW - HOUR, "retired", "stale"],
                    )
                    .unwrap();
                port.say(pot, arcade.clone(), *bitails, *woc);
            }
            let dead = "c9".repeat(32);
            let dead_hop = "d9".repeat(32);
            seed_ghost_join(port.conn(), &dead, &[&dead_hop], 49 * HOUR, true);
            *port.start.borrow_mut() = Some(String::new());
            let summary = backlog(&port).await;
            assert_eq!(
                summary,
                RetireBacklogSummary {
                    scanned: 4,
                    evicted: 1,
                    left: 3,
                    kept: 0,
                    faults: 0,
                    deferred: 0,
                },
            );
            for (i, (pot, ..)) in held.iter().enumerate() {
                let h = format!("{:02x}", 0xd0 + i).repeat(32);
                assert_eq!(
                    count(port.conn(), "pot_records", "txid", pot),
                    1,
                    "{pot} left alone"
                );
                assert_eq!(
                    hop(port.conn(), &h),
                    (1, Some(pot.clone())),
                    "its hop's pointer stands"
                );
                assert_eq!(ledger(port.conn(), pot), None, "no eviction");
                assert_eq!(
                    latch_of(port.conn(), pot),
                    None,
                    "the latch cleared: the proof machinery takes it back"
                );
                assert_eq!(
                    memo_kind(port.conn(), pot),
                    None,
                    "the stale memo goes with the latch"
                );
            }
            assert_eq!(
                count(port.conn(), "pot_records", "txid", &dead),
                0,
                "the absent one is evicted"
            );
            assert_eq!(hop(port.conn(), &dead_hop), (0, None));
            assert_eq!(port.total(crate::ops::COUNTER_RETIRE_BACKLOG_SCANNED), 4);
            assert_eq!(port.total(crate::ops::COUNTER_RETIRE_BACKLOG_LEFT), 3);
            assert_eq!(port.total(crate::ops::COUNTER_RETIRE_BACKLOG_EVICTED), 1);
        }

        /// (d) The backlog pass never evicts on the memo alone (the lens's L-H2-1): a pot latched with a `retired`
        /// memo 49 h old and an `arc_terminal` REJECTED on record is asked again, Arcade first, then both indexers,
        /// BEFORE any decision; a fresh mined look leaves it; a look where every courier faults keeps it latched
        /// and live (the recorded word takes the absence arm, and absence needs both indexers).
        #[tokio::test]
        async fn i831_d_the_backlog_pass_never_evicts_on_the_memo_alone() {
            let port = SqliteRetire::new();
            let mined = "f1".repeat(32);
            let blind = "f2".repeat(32);
            for pot in [&mined, &blind] {
                seed_ghost_join(port.conn(), pot, &[], 49 * HOUR, true);
                port.conn()
                    .execute(
                        TX_ANY_VERDICT_UPSERT_SQL,
                        rusqlite::params![pot, NOW - 40 * HOUR, "retired", "network-absent"],
                    )
                    .unwrap();
                port.conn()
                    .execute(
                        "INSERT INTO arc_terminal (txid, status, extra, first_ms) VALUES (?1, 'REJECTED', 'recorded', ?2)",
                        rusqlite::params![pot, NOW - 40 * HOUR],
                    )
                    .unwrap();
            }
            port.say(
                &mined,
                ArcadeLook::Missing,
                Some(NetworkPresence::Absent),
                Some(NetworkPresence::Present {
                    height: Some(918_100),
                }),
            );
            port.say(&blind, ArcadeLook::Fault, None, None);
            *port.start.borrow_mut() = Some(String::new());
            let summary = backlog(&port).await;
            assert_eq!(
                summary,
                RetireBacklogSummary {
                    scanned: 2,
                    evicted: 0,
                    left: 1,
                    kept: 1,
                    faults: 0,
                    deferred: 0,
                },
            );
            assert_eq!(
                *port.asked.borrow(),
                vec![
                    format!("arcade {mined}"),
                    format!("bitails {mined}"),
                    format!("woc {mined}"),
                    format!("arcade {blind}"),
                    format!("bitails {blind}"),
                    format!("woc {blind}"),
                ],
                "every candidate is looked at again before any decision"
            );
            for pot in [&mined, &blind] {
                assert_eq!(
                    count(port.conn(), "pot_records", "txid", pot),
                    1,
                    "{pot} stays"
                );
                assert_eq!(ledger(port.conn(), pot), None);
            }
            assert_eq!(
                latch_of(port.conn(), &blind),
                Some(1),
                "no answer: still latched, looked at again later"
            );
            assert_eq!(memo_kind(port.conn(), &blind).as_deref(), Some("retired"));
            assert_eq!(latch_of(port.conn(), &mined), None);
        }

        /// (e) A MINED push after the retire eviction readmits the pot byte for byte: the readmission's steps over
        /// the same connection (the SHIPPED `restore_twins`, the SHIPPED re-mark of the ledger's `releasedSpends`,
        /// the stamp). Every row the JOIN keyed comes back exactly as it was; each hop's pointer names the JOIN again,
        /// unconfirmed (the re-mark's stated shape); the ledger is closed; the chain word stays none.
        #[tokio::test]
        async fn i831_e_a_mined_push_after_the_retire_eviction_readmits_byte_for_byte() {
            let port = SqliteRetire::new();
            let join = "85".repeat(32);
            let hops = ["a5".repeat(32), "b5".repeat(32)];
            let hop_refs: Vec<&str> = hops.iter().map(String::as_str).collect();
            seed_ghost_join(port.conn(), &join, &hop_refs, 49 * HOUR, false);
            let snapshot =
                |c: &rusqlite::Connection| -> Vec<(String, Vec<rusqlite::types::Value>)> {
                    let mut out = Vec::new();
                    for (table, key) in [
                        ("pot_records", "txid"),
                        ("outputs", "txid"),
                        ("applied_transactions", "txid"),
                        ("transactions", "txid"),
                    ] {
                        let mut st = c
                            .prepare(&format!(
                                "SELECT * FROM \"{table}\" WHERE \"{key}\" = ?1 ORDER BY rowid"
                            ))
                            .unwrap();
                        let n = st.column_count();
                        let rows: Vec<Vec<rusqlite::types::Value>> = st
                            .query_map([&join], |r| {
                                (0..n)
                                    .map(|i| r.get::<_, rusqlite::types::Value>(i))
                                    .collect()
                            })
                            .unwrap()
                            .map(Result::unwrap)
                            .collect();
                        for row in rows {
                            out.push((table.to_string(), row));
                        }
                    }
                    out.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
                    out
                };
            // the JOIN's engine row latched (`retired_ms`, its reason) as the transactions arm latches it, so the rows
            // the eviction then finds are the ones it must give back (since the FIX-831-B lens's MEDIUM-1 that arm
            // ends the pot in the same step, pinned by `i831_h`; here the pot_beefs arm ends it, the latch held)
            let reason = crate::proof_fetcher::retire_verdict(
                &ArcadeLook::Missing,
                Some(false),
                Some(false),
                49 * 3_600,
            )
            .expect("the absence verdict");
            port.latch(RetireStore::Transactions, &join, &reason, NOW)
                .await
                .unwrap();
            let before = snapshot(port.conn());
            assert_eq!(
                before.len(),
                5,
                "the pot row, its output, two applied rows, its engine row"
            );
            let summary = retire_pot_beefs(&port).await;
            assert_eq!(summary.evicted_on_retire, 1);
            assert!(snapshot(port.conn()).is_empty(), "evicted");
            // ---- the readmission (`readmit_if_evicted` on a MINED push) ----
            let (_, _, released) = ledger(port.conn(), &join).expect("the ledger row");
            let (restored, faults) = restore_twins(&port.db, &join).await;
            assert!(faults.is_empty(), "{faults:?}");
            assert_eq!(restored, 5);
            let (remark, with_at) = remark_pot_spend_sql(&cols(port.conn(), "pot_records"));
            assert!(with_at);
            for r in &released {
                port.conn()
                    .execute(
                        &remark,
                        rusqlite::params![&join, NOW / 1000, &r.txid, r.vout],
                    )
                    .unwrap();
            }
            port.conn()
                .execute(
                    "UPDATE pot_evictions SET readmittedAt = ?1 WHERE txid = ?2 AND readmittedAt IS NULL",
                    rusqlite::params![NOW + 1, &join],
                )
                .unwrap();
            assert_eq!(
                snapshot(port.conn()),
                before,
                "every row the JOIN keyed, byte for byte"
            );
            for h in &hops {
                assert_eq!(
                    hop(port.conn(), h),
                    (1, Some(join.clone())),
                    "the hop's pointer names the JOIN again"
                );
            }
            for t in [
                "pot_records_evicted",
                "outputs_evicted",
                "applied_transactions_evicted",
                "transactions_evicted",
            ] {
                assert_eq!(count(port.conn(), t, "txid", &join), 0, "{t} empty again");
            }
            let (reason, readmitted, _) = ledger(port.conn(), &join).unwrap();
            assert_eq!(readmitted, Some(NOW + 1));
            assert_eq!(eviction_chain_word(&reason), None);
        }

        /// (f) Both arms' retire reasons map to no chain word in `eviction_chain_word`, whatever text Arcade's
        /// `extraInfo` echoes after the mark (a rejection mark, a conflict mark); and the mark is one of the absence
        /// marks, read before the rejection marks.
        #[test]
        fn i831_f_the_retire_eviction_reason_maps_to_no_chain_word() {
            use crate::proof_fetcher::{retire_verdict, RETIRE_ABSENT_MIN_AGE_SECS};
            let absent = Some(false);
            let arms = [
                retire_verdict(
                    &ArcadeLook::Missing,
                    absent,
                    absent,
                    RETIRE_ABSENT_MIN_AGE_SECS,
                )
                .unwrap(),
                retire_verdict(
                    &ArcadeLook::Recorded("REJECTED".into(), "arcade live REJECTED".into()),
                    absent,
                    absent,
                    RETIRE_ABSENT_MIN_AGE_SECS,
                )
                .unwrap(),
                retire_verdict(
                    &ArcadeLook::Fatal("REJECTED".into(), "bad".into()),
                    absent,
                    absent,
                    3_600,
                )
                .unwrap(),
                retire_verdict(
                    &ArcadeLook::Fatal(
                        "DOUBLE_SPEND_ATTEMPTED".into(),
                        "arcade live x; re-refused at the ladder)".into(),
                    ),
                    absent,
                    absent,
                    3_600,
                )
                .unwrap(),
                retire_verdict(
                    &ArcadeLook::Fatal(
                        "REJECTED".into(),
                        "input aa:0 spent by confirmed competitor bb at 1".into(),
                    ),
                    absent,
                    absent,
                    3_600,
                )
                .unwrap(),
            ];
            for verdict in arms {
                let reason = retire_eviction_reason(&verdict);
                assert!(reason.starts_with(RETIRE_REASON_MARK), "{reason}");
                assert_eq!(eviction_chain_word(&reason), None, "{reason}");
            }
            assert!(
                overlay_discovery::pot::storage::ABSENCE_REASON_MARKS.contains(&RETIRE_REASON_MARK)
            );
            // the other producers' words are unchanged by the mark
            assert_eq!(
                eviction_chain_word("REJECTED (arcade live REJECTED: x; both indexers absent)"),
                Some("rejected")
            );
            assert_eq!(
                eviction_chain_word(&conflict_eviction_reason(
                    &"aa".repeat(32),
                    1,
                    &"bb".repeat(32),
                    1
                )),
                Some("conflicted")
            );
        }

        /// (g) The backlog pass's bounds and its counters on the summary line: at most eight candidates a pass (20
        /// latched live pots drain 8, 8, 4, 0, each pass's deltas persisted per candidate); the read is a window of
        /// 32 txids of each set from a start key that moves (the FIX-831-B lens's MEDIUM-2: it was 256; 40 latched
        /// non-pots below a live pot hide it from a pass started at the bottom and not from one started past them); a
        /// port answering more than eight is cut at eight; the two callers print `end_line` beside the conflict pass's
        /// words, and the six counters are seeded. (The statement budget is `i831_l`'s; here it is lifted.)
        #[tokio::test]
        async fn i831_g_the_backlog_pass_is_bounded_and_counted_on_the_summary_line() {
            assert_eq!(RETIRE_BACKLOG_LIMIT, 8);
            assert_eq!(RETIRE_BACKLOG_WINDOW, 32);
            assert!(
                RETIRE_BACKLOG_CANDIDATES_SQL.contains("ORDER BY (txid < ?2), txid LIMIT 32) s")
            );
            assert_eq!(
                RETIRE_BACKLOG_CANDIDATES_SQL.matches("LIMIT 32").count(),
                5,
                "each set's page from the start and from the bottom (the wrap), and the merged page"
            );
            assert!(
                RETIRE_BACKLOG_CANDIDATES_SQL.ends_with("ORDER BY (b.txid < ?2), b.txid LIMIT 8")
            );
            let port = SqliteRetire::new();
            for i in 0..20 {
                let pot = format!("{:064x}", 0x1000 + i);
                seed_ghost_join(port.conn(), &pot, &[], 49 * HOUR, true);
            }
            *port.start.borrow_mut() = Some(String::new());
            let mut evicted = Vec::new();
            for _ in 0..4 {
                evicted.push(backlog_unbounded(&port).await.evicted);
            }
            assert_eq!(evicted, vec![8, 8, 4, 0]);
            assert_eq!(port.total(crate::ops::COUNTER_RETIRE_BACKLOG_SCANNED), 20);
            assert_eq!(port.total(crate::ops::COUNTER_RETIRE_BACKLOG_EVICTED), 20);
            assert_eq!(
                port.persisted.borrow().len(),
                40,
                "two deltas a candidate, persisted per candidate"
            );
            // the window: 40 latched non-pots below one live pot
            let port = SqliteRetire::new();
            for i in 0..40 {
                port.conn()
                    .execute(
                        "INSERT INTO pot_beefs (txid, beef, createdAt, has_proof, structurally_unprovable) VALUES (?1, X'00', 1, 0, 1)",
                        [format!("{:064x}", i)],
                    )
                    .unwrap();
            }
            let pot = format!("{:064x}", 0x9999);
            seed_ghost_join(port.conn(), &pot, &[], 49 * HOUR, true);
            *port.start.borrow_mut() = Some(String::new());
            assert_eq!(
                backlog(&port).await.scanned,
                0,
                "past the window from the bottom"
            );
            *port.start.borrow_mut() = Some(format!("{:064x}", 10));
            assert_eq!(backlog(&port).await.evicted, 1, "inside a later window");
            // the wrap (the lens's NOTE-3): a pot whose txid is BELOW the start is read past the top
            let port = SqliteRetire::new();
            let low = format!("{:064x}", 1);
            seed_ghost_join(port.conn(), &low, &[], 49 * HOUR, true);
            *port.start.borrow_mut() = Some("f".repeat(16));
            assert_eq!(
                backlog(&port).await.evicted,
                1,
                "a start above the set wraps to its bottom"
            );
            // the start key moves with the clock and is a 16-digit hex key
            let a = retire_backlog_start(NOW);
            assert_eq!(a.len(), 16);
            assert!(a
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()));
            assert_ne!(a, retire_backlog_start(NOW + 60_000));
            // the belt: a port answering twenty rows is walked for eight
            let port = SqliteRetire::new();
            let rows: Vec<RetireCandidateRow> = (0..20)
                .map(|i| RetireCandidateRow {
                    txid: format!("{:064x}", 0x5000 + i),
                    age_secs: 0.0,
                    term_status: None,
                    term_extra: None,
                })
                .collect();
            *port.answer_rows.borrow_mut() = Some(rows);
            let s = backlog(&port).await;
            assert_eq!((s.scanned, s.kept), (8, 8), "{s:?}");
            // the summary line's words, printed by both callers after the conflict pass's
            let mut summary = RetireSummary {
                evicted_on_retire: 2,
                evict_faults: 1,
                ..RetireSummary::default()
            };
            summary.backlog = RetireBacklogSummary {
                scanned: 8,
                evicted: 3,
                left: 2,
                kept: 2,
                faults: 1,
                deferred: 0,
            };
            assert_eq!(
                summary.end_line(),
                "evicted_on_retire=2 evict_faults=1 end_deferred=0 statements=0 backlog: scanned=8 evicted=3 left=2 kept=2 faults=1 deferred=0"
            );
            for (name, src) in [
                ("lib.rs", include_str!("lib.rs")),
                ("tick.rs", include_str!("tick.rs")),
            ] {
                let src: String = src.chars().filter(|c| !c.is_whitespace()).collect();
                assert!(
                    src.contains("conflict:scanned={}evicted={}faults={}{}\",")
                        && src.contains("retire.conflict.faults,retire.end_line(),);"),
                    "{name} prints the retire pass's end words"
                );
            }
            let ops: String = include_str!("ops.rs")
                .chars()
                .filter(|c| !c.is_whitespace())
                .collect();
            assert!(ops.contains(
                "COUNTER_ADMIT_FAST_CONFLICT_FAULTS,//bsv-low#831half2:theretirepass'sevictionsanditsbacklogpassCOUNTER_RETIRE_EVICTED,COUNTER_RETIRE_BACKLOG_SCANNED,COUNTER_RETIRE_BACKLOG_EVICTED,COUNTER_RETIRE_BACKLOG_LEFT,COUNTER_RETIRE_EVICT_FAULTS,COUNTER_RETIRE_END_DEFERRED,"
            ), "the five counters are served from 0");
        }

        /// The shipped enrolment of both stores and the shipped watch GC, as `refresh_proofless_watch` runs them
        /// after the retire step.
        fn watch_refresh(conn: &rusqlite::Connection) {
            for table in ["pot_beefs", "transactions"] {
                conn.execute(&crate::ops::proofless_watch_enrol_sql(table), [NOW])
                    .unwrap();
            }
            conn.execute(crate::ops::PROOFLESS_WATCH_GC_SQL, [])
                .unwrap();
        }

        /// One whole retire pass as `run_retire_pass` runs it (the arms, the backlog, the deferred count), under
        /// `budget`, the arm's rotation from `arm_start` and the backlog's from `backlog_start`.
        async fn retire_pass(
            port: &SqliteRetire,
            budget: &RetireBudget,
            arm_start: &str,
            backlog_start: &str,
        ) -> RetireSummary {
            let mut summary = retire_arms(port, budget, arm_start).await;
            *port.start.borrow_mut() = Some(backlog_start.to_string());
            summary.backlog = run_retire_backlog(port, budget).await;
            crate::proof_fetcher::persist_retire_deferred(port, &summary).await;
            summary.statements = budget.spent(port);
            summary
        }

        /// (h) The FIX-831-B lens's MEDIUM-1, its own repro shape (`lens_repro_the_engine_arm_retires_first_and_the_pot
        /// _escapes`): eight OLDER uncertain `pot_beefs` rows on the watch and a 49 h ghost JOIN. On `ef9186df` the
        /// engine arm retired the JOIN's engine row, the pot_beefs arm's eight were the older rows, the watch GC then
        /// dropped the shared row, and the pot stayed live through every later pass and backlog (`watched after GC:
        /// 0; latch: None; live pot rows: 1; ledger: None`). Now the engine arm latches the pot's stored BEEF and
        /// ends it in the same step. And the population that escaped before the fold (engine row retired, stored
        /// BEEF unlatched, no watch row) is the backlog's: its page reads the engine's retired set too, and it ends
        /// such a pot (latching it first) or, when the fresh look finds it held, clears BOTH latches so the enrolment
        /// watches it again.
        #[tokio::test]
        async fn i831_h_a_pot_whose_engine_row_the_engine_arm_retires_is_ended_never_escaped() {
            let port = SqliteRetire::new();
            for i in 0..8 {
                let b = format!("{:064x}", 0xaa00 + i);
                port.conn().execute(
                    "INSERT INTO pot_beefs (txid, beef, createdAt, has_proof) VALUES (?1, X'00', ?2, 0)",
                    rusqlite::params![b, (NOW - 60 * HOUR) / 1000]).unwrap();
                port.conn()
                    .execute(
                        "INSERT INTO proofless_watch (txid, first_seen_ms) VALUES (?1, ?2)",
                        rusqlite::params![b, NOW - 60 * HOUR],
                    )
                    .unwrap();
                port.say(&b, ArcadeLook::Missing, Some(NetworkPresence::Absent), None);
            }
            let join = "86".repeat(32);
            let hop = "a6".repeat(32);
            seed_ghost_join(port.conn(), &join, &[&hop], 49 * HOUR, false);
            let s = retire_pot_beefs(&port).await;
            assert_eq!(
                (s.retired, s.evicted_on_retire, s.kept_uncertain),
                (1, 1, 8),
                "the engine arm retires the JOIN and ends its pot in the same step: {s:?}"
            );
            watch_refresh(port.conn());
            for _ in 0..3 {
                retire_pot_beefs(&port).await;
                *port.start.borrow_mut() = Some(String::new());
                backlog(&port).await;
                watch_refresh(port.conn());
            }
            assert_ended(&port, &join, &[&hop], "network-absent ");
            assert_eq!(
                count(port.conn(), "transactions_evicted", "txid", &join),
                1,
                "the engine row went with the pot"
            );

            // the escaped population: the engine row retired by the base's arm, the stored BEEF unlatched, no watch
            // row (the GC dropped it), the pot live; and one such pot the network holds
            let port = SqliteRetire::new();
            let ghost = "87".repeat(32);
            let held = "88".repeat(32);
            for (txid, h) in [(&ghost, "a7".repeat(32)), (&held, "b8".repeat(32))] {
                seed_ghost_join(port.conn(), txid, &[&h], 49 * HOUR, false);
                port.conn()
                    .execute(
                        RETIRE_TRANSACTIONS_SQL,
                        rusqlite::params![
                            NOW - HOUR,
                            "network-absent 172800s: arcade 404 + both indexers 404",
                            txid
                        ],
                    )
                    .unwrap();
            }
            watch_refresh(port.conn());
            assert_eq!(
                count(port.conn(), "proofless_watch", "txid", &ghost),
                0,
                "off the watch: the escape"
            );
            assert_eq!(latch_of(port.conn(), &ghost), None);
            // the arms cannot reach it (no watch row): the base's state for good
            let s = retire_pot_beefs(&port).await;
            assert_eq!(s.scanned, 0, "{s:?}");
            port.say(
                &held,
                ArcadeLook::Missing,
                Some(NetworkPresence::Absent),
                Some(NetworkPresence::Present { height: None }),
            );
            *port.start.borrow_mut() = Some(String::new());
            let b = backlog(&port).await;
            assert_eq!((b.scanned, b.evicted, b.left), (2, 1, 1), "{b:?}");
            assert_ended(&port, &ghost, &[&"a7".repeat(32)], "network-absent ");
            let retired: Option<i64> = port
                .conn()
                .query_row(
                    "SELECT retired_ms FROM transactions WHERE txid = ?1",
                    [&held],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(retired, None, "the held pot's engine latch cleared");
            assert_eq!(
                count(port.conn(), "pot_records", "txid", &held),
                1,
                "the held pot stays"
            );
            assert_eq!(ledger(port.conn(), &held), None);
            watch_refresh(port.conn());
            assert_eq!(
                count(port.conn(), "proofless_watch", "txid", &held),
                1,
                "watched again"
            );
            // the escaped pots' page reads the engine's retired set
            assert!(RETIRE_BACKLOG_CANDIDATES_SQL
                .contains("FROM transactions_retired_set WHERE txid >= ?2"));
            assert!(RETIRE_ENGINE_LATCH_CLEAR_SQL
                .contains("EXISTS (SELECT 1 FROM pot_records p WHERE p.txid = ?1)"));
        }

        /// (i) The FIX-831-B lens's LOW-1: a verified proof that lands after the look and before the eviction wins,
        /// on both arms and in the backlog. On `ef9186df` the latch was a no-op (`has_proof = 0` guard), the port still
        /// answered `Ok`, and the pot was evicted: a mined pot shadow-moved with no MINED push coming.
        #[tokio::test]
        async fn i831_i_a_proof_landing_mid_pass_wins_over_the_eviction() {
            for (engine_arm, table) in [
                (true, "pot_beefs"),
                (true, "transactions"),
                (false, "pot_beefs"),
            ] {
                let port = SqliteRetire::new();
                let join = "89".repeat(32);
                seed_ghost_join(
                    port.conn(),
                    &join,
                    &["a9".repeat(32).as_str()],
                    49 * HOUR,
                    false,
                );
                if !engine_arm {
                    // the pot_beefs arm alone: no engine row
                    port.conn()
                        .execute("DELETE FROM transactions WHERE txid = ?1", [&join])
                        .unwrap();
                }
                let t = table.to_string();
                *port.after_look.borrow_mut() = Some(Box::new(move |c, txid| {
                    c.execute(
                        &format!("UPDATE {t} SET has_proof = 1 WHERE txid = ?1"),
                        [txid],
                    )
                    .unwrap();
                }));
                let s = retire_pot_beefs(&port).await;
                assert_eq!(s.evicted_on_retire, 0, "{table} proven mid-pass: {s:?}");
                assert_eq!(count(port.conn(), "pot_records", "txid", &join), 1);
                assert_eq!(ledger(port.conn(), &join), None, "nothing evicted");
            }
            // the backlog: a latched pot whose proof lands during the fresh look
            let port = SqliteRetire::new();
            let join = "8a".repeat(32);
            seed_ghost_join(port.conn(), &join, &[], 49 * HOUR, true);
            *port.after_look.borrow_mut() = Some(Box::new(|c, txid| {
                c.execute("UPDATE pot_beefs SET has_proof = 1 WHERE txid = ?1", [txid])
                    .unwrap();
            }));
            *port.start.borrow_mut() = Some(String::new());
            let b = backlog(&port).await;
            assert_eq!((b.scanned, b.evicted, b.kept), (1, 0, 1), "{b:?}");
            assert_eq!(ledger(port.conn(), &join), None);
            assert_eq!(
                end_retired_pot(&port, &RetireBudget::begin(&port), &join, "x", NOW).await,
                RetireEnd::Proven
            );
        }

        /// (j) The FIX-831-B lens's LOW-2: an eviction that left work undone (a row not moved, or the release of the
        /// spend pointers faulted) is a FAULT, never an ending, on both arms and in the backlog. The wrapper answers it
        /// (`EvictionOutcome::incomplete`, the release's fault included) and the worker's port maps it.
        #[tokio::test]
        async fn i831_j_an_incomplete_eviction_is_counted_a_fault_never_an_ending() {
            let port = SqliteRetire::new();
            let join = "8b".repeat(32);
            seed_ghost_join(port.conn(), &join, &[], 49 * HOUR, false);
            port.incomplete.set(true);
            let s = retire_pot_beefs(&port).await;
            assert_eq!((s.evicted_on_retire, s.evict_faults), (0, 1), "{s:?}");
            assert_eq!(port.total(crate::ops::COUNTER_RETIRE_EVICTED), 0);
            assert_eq!(port.total(crate::ops::COUNTER_RETIRE_EVICT_FAULTS), 1);
            let port = SqliteRetire::new();
            seed_ghost_join(port.conn(), &join, &[], 49 * HOUR, true);
            port.incomplete.set(true);
            *port.start.borrow_mut() = Some(String::new());
            let b = backlog(&port).await;
            assert_eq!((b.evicted, b.faults), (0, 1), "{b:?}");
            assert_eq!(port.total(crate::ops::COUNTER_RETIRE_BACKLOG_EVICTED), 0);
            // the wrapper and the worker's port, by their source
            let me: String = include_str!("admit_fast.rs")
                .chars()
                .filter(|c| !c.is_whitespace())
                .collect();
            assert!(me.contains("incomplete:!core.faults.is_empty()||release_faulted,"));
            assert!(me.contains(
                "let(mutreleased,release_faulted)=ifcore.yielded_to_readmission.is_some(){"
            ));
            let pf: String = include_str!("proof_fetcher.rs")
                .chars()
                .filter(|c| !c.is_whitespace())
                .collect();
            assert!(pf.contains("}elseifoutcome.incomplete{RetireEviction::Incomplete}else{RetireEviction::Stands})"));
        }

        /// (k) The FIX-831-B lens's LOW-4: the pot_beefs arm ROTATES. Eight OLDER rows the verdict keeps every pass
        /// (Arcade holds each at a pre-SEEN rank) sat at the head of the base's oldest-first `LIMIT 8` for good, and a
        /// younger 49 h ghost pot behind them was never read; now the arm starts at a key that moves every pass and the
        /// ghost is reached and ended.
        #[tokio::test]
        async fn i831_k_eight_kept_rows_never_starve_a_younger_candidate() {
            let port = SqliteRetire::new();
            for i in 0..8 {
                let b = format!("{:064x}", 0xbb00 + i);
                port.conn().execute(
                    "INSERT INTO pot_beefs (txid, beef, createdAt, has_proof) VALUES (?1, X'00', ?2, 0)",
                    rusqlite::params![b, (NOW - 90 * HOUR) / 1000]).unwrap();
                port.conn()
                    .execute(
                        "INSERT INTO proofless_watch (txid, first_seen_ms) VALUES (?1, ?2)",
                        rusqlite::params![b, NOW - 90 * HOUR],
                    )
                    .unwrap();
                port.say(&b, ArcadeLook::Present, None, None);
            }
            let ghost = "8c".repeat(32);
            seed_ghost_join(port.conn(), &ghost, &[], 49 * HOUR, false);
            port.conn()
                .execute("DELETE FROM transactions WHERE txid = ?1", [&ghost])
                .unwrap();
            let mut ended_at = None;
            for k in 0..8 {
                let budget = RetireBudget::begin(&port);
                let start = retire_rotation_key(NOW + k * 15 * 60_000, RETIRE_ARM_ROTATION_SALT);
                let s = retire_arms(&port, &budget, &start).await;
                if s.evicted_on_retire == 1 {
                    ended_at = Some(k);
                    break;
                }
                assert_eq!(s.kept_present, 8, "{s:?}");
            }
            assert!(
                ended_at.is_some(),
                "the younger ghost is reached by a rotating start"
            );
            assert_eq!(count(port.conn(), "pot_records", "txid", &ghost), 0);
            assert!(
                RETIRE_CANDIDATES_POT_BEEFS_SQL.ends_with("ORDER BY (w.txid < ?2), w.txid LIMIT 8")
            );
            assert_ne!(
                retire_rotation_key(NOW, RETIRE_ARM_ROTATION_SALT),
                retire_backlog_start(NOW),
                "the arm and the backlog start apart"
            );
            let lib: String = include_str!("proof_fetcher.rs")
                .chars()
                .filter(|c| !c.is_whitespace())
                .collect();
            assert!(lib.contains("letarm_start=retire_rotation_key(now,RETIRE_ARM_ROTATION_SALT);"));
            assert!(lib.contains("ifstore==RetireStore::PotBeefs{q=q.bind(arm_start.as_str());}"));
        }

        /// (l) The FIX-831-B lens's MEDIUM-2: the pass's statement budget. 24 watched ghost JOINs (two hops each, both
        /// stores) and 16 latched live pots, every one absent at 49 h: the base ended 8 + 8 + 8 of them in one pass,
        /// about 1,200 statements in the worker. Now one pass spends at most the budget and its three tail statements,
        /// the largest candidate step stays inside the reserve, the rest is deferred and counted once, and the passes
        /// that follow end every pot.
        #[tokio::test]
        async fn i831_l_one_pass_spends_at_most_its_statement_budget_and_the_rest_follows() {
            assert_eq!(RETIRE_PASS_STATEMENT_BUDGET, 480);
            assert_eq!(RETIRE_END_RESERVE_STATEMENTS, 96);
            assert_eq!(RETIRE_BACKLOG_WINDOW, 32);
            let port = SqliteRetire::new();
            // txids spread over the key space (as real ones are), so the rotating starts reach every one
            let key = |salt: u64, i: i64| {
                format!("{}{}", retire_rotation_key(i, salt << 32), "0".repeat(48))
            };
            let mut pots = Vec::new();
            for i in 0..24 {
                let join = key(1, i);
                let hops = [key(2, i), key(3, i)];
                seed_ghost_join(port.conn(), &join, &[&hops[0], &hops[1]], 49 * HOUR, false);
                pots.push(join);
            }
            for i in 0..16 {
                let join = key(4, i);
                seed_ghost_join(port.conn(), &join, &[&key(5, i)], 49 * HOUR, true);
                pots.push(join);
            }
            let mut passes = Vec::new();
            for k in 0..60 {
                let budget = RetireBudget::begin(&port);
                let before = port.stmts.get();
                let at = NOW + k * 15 * 60_000;
                let s = retire_pass(
                    &port,
                    &budget,
                    &retire_rotation_key(at, RETIRE_ARM_ROTATION_SALT),
                    &retire_backlog_start(at),
                )
                .await;
                let spent = port.stmts.get() - before;
                assert!(
                    spent <= RETIRE_PASS_STATEMENT_BUDGET + 3,
                    "a pass spent {spent} statements: {s:?}"
                );
                passes.push((
                    spent,
                    s.evicted_on_retire + s.backlog.evicted,
                    s.end_deferred + s.backlog.deferred,
                ));
                watch_refresh(port.conn());
                if pots
                    .iter()
                    .all(|p| count(port.conn(), "pot_records", "txid", p) == 0)
                {
                    break;
                }
            }
            eprintln!(
                "i831_l passes (statements, endings, deferred): {passes:?}; largest ending {}",
                port.largest_ending.get()
            );
            assert!(passes[0].2 > 0, "the first pass defers: {passes:?}");
            assert!(
                passes[0].1 < 24,
                "it ends fewer than the base's 24: {passes:?}"
            );
            assert!(
                port.largest_ending.get() + 8 <= RETIRE_END_RESERVE_STATEMENTS,
                "the largest ending ({}) and its step's own statements sit inside the reserve",
                port.largest_ending.get()
            );
            for p in &pots {
                assert_eq!(
                    count(port.conn(), "pot_records", "txid", p),
                    0,
                    "{p} ended by a later pass"
                );
                assert!(ledger(port.conn(), p).is_some());
            }
            let deferred: u64 = passes.iter().map(|p| p.2 as u64).sum();
            assert_eq!(
                port.total(crate::ops::COUNTER_RETIRE_END_DEFERRED),
                deferred,
                "counted once a pass"
            );
            assert_eq!(
                port.total(crate::ops::COUNTER_RETIRE_EVICTED)
                    + port.total(crate::ops::COUNTER_RETIRE_BACKLOG_EVICTED),
                40
            );
        }

        /// The end step itself: a fault of the live read or of the eviction is a `Fault`, never an eviction.
        #[tokio::test]
        async fn i831_the_end_step_answers_no_live_pot_for_a_non_pot() {
            let port = SqliteRetire::new();
            assert_eq!(
                end_retired_pot(
                    &port,
                    &RetireBudget::begin(&port),
                    &"77".repeat(32),
                    "x",
                    NOW
                )
                .await,
                RetireEnd::NoLivePot
            );
        }
    }

    /// bsv-low #920: the write-side guard's settle over real SQLite (the shipped schema and the shipped memo SQL).
    struct SqliteReevicted<'a> {
        conn: &'a rusqlite::Connection,
        txid: &'a str,
        commits: std::cell::Cell<usize>,
    }
    impl ReevictedPort for SqliteReevicted<'_> {
        async fn memo_kind(&self) -> Result<Option<String>, String> {
            use rusqlite::OptionalExtension;
            self.conn
                .query_row(REEVICTED_MEMO_KIND_SQL, [self.txid], |r| {
                    r.get::<_, Option<String>>(0)
                })
                .optional()
                .map(|k| k.flatten())
                .map_err(|e| e.to_string())
        }
        async fn commit(&self, queries: Vec<Query>) -> Result<(), String> {
            let tx = self
                .conn
                .unchecked_transaction()
                .map_err(|e| e.to_string())?;
            for query in &queries {
                let params: Vec<_> = query.params().iter().map(to_sql).collect();
                tx.execute(query.sql(), rusqlite::params_from_iter(params))
                    .map_err(|e| e.to_string())?;
            }
            tx.commit().map_err(|e| e.to_string())?;
            self.commits.set(self.commits.get() + 1);
            Ok(())
        }
        fn log(&self, _: String) {}
    }

    fn fix920_counter(conn: &rusqlite::Connection, name: &str) -> i64 {
        conn.query_row(
            "SELECT value FROM ops_counters WHERE name = ?1",
            [name],
            |r| r.get(0),
        )
        .unwrap_or(0)
    }

    /// The app layer's `/tx-any` memo read (`low-app-layer` `txany.rs`, `VERDICT_MEMO_READ_SQL`, verbatim): what it
    /// serves as `source: "memo"`, `present: false`, `terminal: true` for a `refused` row.
    const FIX920_APP_MEMO_READ_SQL: &str =
        "SELECT txid, verdictAtMs, inputOutpoint, spenderTxid, kind, evidence FROM tx_any_verdicts WHERE txid = ?";

    fn fix920_served_memo(conn: &rusqlite::Connection, txid: &str) -> Option<(String, String)> {
        use rusqlite::OptionalExtension;
        conn.query_row(FIX920_APP_MEMO_READ_SQL, [txid], |r| {
            Ok((r.get::<_, String>(4)?, r.get::<_, String>(5)?))
        })
        .optional()
        .unwrap()
    }

    /// The write-side guard as every site runs it (`routes::finish_past_deadline`'s arm, the request's, a landed
    /// predecessor's): the ledger's open row, the eviction under its reason, the settle, the count.
    async fn fix920_landing_guard(db: &SqliteShadow, txid: &str, now_ms: u64) -> Option<u64> {
        use rusqlite::OptionalExtension;
        let reason: Option<String> = db
            .conn
            .query_row(OPEN_EVICTION_SQL, [txid], |r| r.get(0))
            .optional()
            .unwrap();
        let reason = reason?;
        let core = evict_core(db, txid, &reason, now_ms).await;
        let outcome = EvictionOutcome {
            moved: core.moved,
            yielded: core.yielded_to_readmission.is_some(),
            incomplete: !core.faults.is_empty(),
        };
        let port = SqliteReevicted {
            conn: &db.conn,
            txid,
            commits: std::cell::Cell::new(0),
        };
        settle_reevicted_with(&port, txid, now_ms, &outcome, &reason, "pin").await;
        db.conn
            .execute(
                crate::ops::BUMP_COUNTER_SQL,
                rusqlite::params![crate::ops::COUNTER_ADMIT_FAST_REEVICTED_AFTER_WRITE, 1],
            )
            .unwrap();
        Some(outcome.moved)
    }

    fn fix920_land(conn: &rusqlite::Connection, txid: &str) {
        conn.execute("INSERT INTO outputs (txid, outputIndex, outputScript, topic, satoshis, spent) VALUES (?1, 0, X'51', 'tm_pot', 40000, 0)", [txid]).unwrap();
        conn.execute("INSERT INTO pot_records (txid, outputIndex, spent, createdAt, lockKind) VALUES (?1, 0, 0, 1789959998, 'covenant')", [txid]).unwrap();
        conn.execute(
            "INSERT INTO applied_transactions (txid, topic) VALUES (?1, 'tm_pot')",
            [txid],
        )
        .unwrap();
    }

    /// bsv-low #920 (a), L51 pair 17's order: Arcade's REJECTED push is judged (Arcade's own live word, both
    /// indexers absent: refused, no age floor) and its eviction runs while the admission's engine write is still in
    /// flight (#838's past-deadline landing, 20 s later): the push's pass finds no rows and its job is cut after
    /// its OPEN marker (no memo, no count). The write then LANDS, and its write-side guard reads the open row it
    /// outran: the rows move to the twins, the refusal's memo (the app layer's `/tx-any` source) and the counts are
    /// written. RED with the settle made inert (main's guard: the rows moved, the count bumped, NO memo, so
    /// `/tx-any` served the index's unknown for thirteen minutes).
    #[tokio::test]
    async fn fix920_a_a_refusal_in_flight_completes_at_the_admissions_landing() {
        let txid = "55".repeat(32);
        let reason = "REJECTED (arcade live REJECTED: UTXO_SPENT (70): x; both indexers absent)";
        let db = SqliteShadow::new(shipped_conn());
        // the push's evidence check: Arcade's own live word, the admission unknown (its write in flight)
        let verdict = verdict_of_look(
            &ArcadeLook::Fatal("REJECTED".into(), "UTXO_SPENT (70)".into()),
            async {
                (
                    crate::door_words::CourierWord::Absent,
                    crate::door_words::CourierWord::Absent,
                )
            },
            async { None },
        )
        .await;
        assert!(
            matches!(verdict, EvidenceVerdict::Refused(_)),
            "Arcade's own fatal word needs no age: {verdict:?}"
        );
        // the push's eviction before the landing: its marker, nothing to move; the job cut before its settle
        let push = evict_core(&db, &txid, reason, 1_000).await;
        assert_eq!(push.moved, 0, "the write is still in flight: {push:?}");
        assert_eq!(
            fix920_served_memo(&db.conn, &txid),
            None,
            "the cut job wrote no memo"
        );
        // the admission's write lands after the answer, then its guard
        fix920_land(&db.conn, &txid);
        let moved = fix920_landing_guard(&db, &txid, 21_000).await;
        assert!(
            moved.is_some_and(|m| m >= 3),
            "the landed rows re-evicted: {moved:?}"
        );
        for t in ["outputs", "pot_records", "applied_transactions"] {
            assert_eq!(
                count(&db.conn, t, "txid", &txid),
                0,
                "{t} still holds the evicted txid"
            );
        }
        assert_eq!(
            fix920_served_memo(&db.conn, &txid),
            Some(("refused".to_string(), reason.to_string())),
            "the app layer's /tx-any serves the refusal memo after the landing"
        );
        assert_eq!(
            fix920_counter(
                &db.conn,
                crate::ops::COUNTER_ADMIT_FAST_REEVICTED_AFTER_WRITE
            ),
            1
        );
        assert_eq!(
            fix920_counter(
                &db.conn,
                crate::applayer_counters::COUNTER_TXANY_MEMO_WRITE_REFUSED
            ),
            1
        );
        // a second guard over the same landing (the deferred job's run, a re-present) moves nothing, writes no
        // second memo
        assert_eq!(fix920_landing_guard(&db, &txid, 40_000).await, Some(0));
        assert_eq!(
            fix920_counter(
                &db.conn,
                crate::applayer_counters::COUNTER_TXANY_MEMO_WRITE_REFUSED
            ),
            1,
            "the memo is written once"
        );
    }

    /// bsv-low #920 (b): the push AFTER the landing (today's path) is unchanged: its own settle (the shipped
    /// `settle_corroborated_refusal`'s batch) writes the memo and its count, and a later write-side guard over the
    /// same txid writes no second memo; a guard whose pass yielded to a readmission writes none at all; a memo
    /// another settle wrote (`retired`, `unconfirmable`) is never overwritten.
    #[tokio::test]
    async fn fix920_b_the_push_after_the_landing_is_unchanged_and_the_memo_is_written_once() {
        let txid = "56".repeat(32);
        let reason = "REJECTED (arcade live REJECTED: UTXO_SPENT (70): x; both indexers absent)";
        let db = SqliteShadow::new(shipped_conn());
        fix920_land(&db.conn, &txid);
        let push = evict_core(&db, &txid, reason, 1_000).await;
        assert!(push.moved >= 3);
        let tx = db.conn.unchecked_transaction().unwrap();
        for q in refusal_fact_queries(&txid, 1_000, crate::ops::COUNTER_ARC_INGEST_EVICTED, reason)
        {
            let params: Vec<_> = q.params().iter().map(to_sql).collect();
            tx.execute(q.sql(), rusqlite::params_from_iter(params))
                .unwrap();
        }
        tx.commit().unwrap();
        assert_eq!(
            fix920_counter(&db.conn, crate::ops::COUNTER_ARC_INGEST_EVICTED),
            1
        );
        assert_eq!(fix920_landing_guard(&db, &txid, 2_000).await, Some(0));
        assert_eq!(
            fix920_counter(
                &db.conn,
                crate::applayer_counters::COUNTER_TXANY_MEMO_WRITE_REFUSED
            ),
            1,
            "the push's settle wrote the memo; the guard writes no second"
        );
        // the decision's table
        assert!(reevicted_owes_memo(false, None));
        assert!(reevicted_owes_memo(false, Some("absent")));
        for kept in ["refused", "retired", "unconfirmable"] {
            assert!(!reevicted_owes_memo(false, Some(kept)), "{kept}");
        }
        assert!(
            !reevicted_owes_memo(true, None),
            "never over a yielded pass"
        );
        let port = SqliteReevicted {
            conn: &db.conn,
            txid: "57",
            commits: std::cell::Cell::new(0),
        };
        let yielded = EvictionOutcome {
            moved: 0,
            yielded: true,
            incomplete: false,
        };
        settle_reevicted_with(&port, "57", 1, &yielded, reason, "pin").await;
        assert_eq!(port.commits.get(), 0, "a yielded pass writes nothing");
    }

    /// bsv-low #920: every write-side guard that re-evicts after a write settles the refusal right after its
    /// eviction (the past-deadline landing, the request's guard, a resumed run's, a landed predecessor's) and so does
    /// the Rejected arm's re-run under an open eviction. RED on main (no site settles).
    #[test]
    fn fix920_every_write_side_guard_settles_after_its_eviction() {
        let routes = include_str!("routes.rs");
        let routes = &routes[..routes.find("#[cfg(test)]").unwrap_or(routes.len())];
        let src = include_str!("admit_fast.rs");
        let guard = &src[src.find("pub async fn guard_landed(").unwrap()..];
        let guard = &guard[..guard.find("\n}\n").unwrap()];
        let past = &routes[routes.find("async fn finish_past_deadline<").unwrap()..];
        let past = &past[..past.find("\n}\n").unwrap()];
        for (site, body, needle) in [
            (
                "guard_landed",
                guard,
                "evict_txid_everywhere(db, txid, &ev.reason, now_ms)",
            ),
            (
                "finish_past_deadline",
                past,
                "evict_txid_everywhere(&db, &subject, &ev.reason, now_ms)",
            ),
            (
                "the request's guard",
                routes,
                "evict_txid_everywhere(&ledger_db, subject, &ev.reason, now_ms)",
            ),
            (
                "the resumed run's stored echo",
                routes,
                "STORED_ECHO_UNDER_EVICTION",
            ),
            ("the Rejected arm's re-run", routes, "&crate::admit_fast::ladder_rerefusal_reason(&reason),\n                            worker::Date"),
        ] {
            let at = body
                .find(needle)
                .unwrap_or_else(|| panic!("{site}: {needle}"));
            let window = &body[at..(at + 900).min(body.len())];
            assert!(
                window.contains("settle_reevicted("),
                "{site}: the eviction is not followed by the refusal's settle"
            );
        }
    }

    /// bsv-low #920 (c): the re-present's terminal judgement under an OPEN eviction. The door's probe is Arcade's
    /// own `GET /tx`; with both indexers absent the age floor (Rule 6) is read only when no eviction stands, so the
    /// re-present answers the Rejected arm (the 422 and the memo) young or aged; mempool presence keeps #602's race
    /// word. The evidence check's own rows (#740): Arcade's live fatal word refuses young; a Recorded (pushed) or
    /// faulted look is absence-based and young is "not now". RED with main's rule (absence always reads the age).
    #[tokio::test]
    async fn fix920_c_a_re_present_under_an_open_eviction_on_arcades_own_word_is_refused_young_or_aged(
    ) {
        use crate::broadcaster::{terminal_at_arcade_by_presence, terminal_reads_age, ArcOutcome};
        use crate::proof_fetcher::NetworkPresence;
        let bound = crate::door_words::LockBound::default();
        let txid = "55".repeat(32);
        let judge = |under_open: bool, young_read: bool, presence: NetworkPresence| {
            let young = terminal_reads_age(&presence, under_open) && young_read;
            terminal_at_arcade_by_presence(
                presence,
                young,
                &txid,
                "REJECTED",
                "UTXO_SPENT (70)",
                &bound,
            )
        };
        // under an open eviction: young or aged, both indexers absent → the refusal
        for young in [true, false] {
            assert!(
                matches!(
                    judge(true, young, NetworkPresence::Absent),
                    Ok(ArcOutcome::Rejected(_))
                ),
                "young={young}"
            );
        }
        // no eviction: Rule 6 stands for a first presentation
        assert!(judge(false, true, NetworkPresence::Absent).is_err());
        assert!(matches!(
            judge(false, false, NetworkPresence::Absent),
            Ok(ArcOutcome::Rejected(_))
        ));
        // mempool presence keeps #602's race word under an eviction too; mined presence admits
        assert!(judge(true, true, NetworkPresence::Present { height: None }).is_err());
        assert!(matches!(
            judge(true, true, NetworkPresence::Present { height: Some(1) }),
            Ok(ArcOutcome::Accepted(_))
        ));
        assert!(judge(true, false, NetworkPresence::Inconclusive).is_err());
        // the evidence check (#740): only Arcade's live fatal word refuses a young or unknown admission
        let absent = || async {
            (
                crate::door_words::CourierWord::Absent,
                crate::door_words::CourierWord::Absent,
            )
        };
        let fatal = ArcadeLook::Fatal("REJECTED".into(), "UTXO_SPENT (70)".into());
        let recorded = ArcadeLook::Recorded("REJECTED".into(), "UTXO_SPENT (70)".into());
        assert!(matches!(
            verdict_of_look(&fatal, absent(), async { Some(1_000) }).await,
            EvidenceVerdict::Refused(_)
        ));
        for look in [recorded, ArcadeLook::Fault] {
            assert!(
                matches!(
                    verdict_of_look(&look, absent(), async { Some(1_000) }).await,
                    EvidenceVerdict::Uncertain(_)
                ),
                "{look:?} young is not now"
            );
        }
        // the door's arm reads the floor through the one rule, under the broadcaster's own eviction stamp
        let b = include_str!("broadcaster.rs");
        assert!(b.contains(
            "let young = if terminal_reads_age(&presence, self.evicted_at_ms.is_some()) {"
        ));
    }
}
