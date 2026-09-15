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

/// Make sure the twin exists and carries every column the source has now.
async fn ensure_shadow(db: &D1Database, table: &str, cols: &[ColumnInfo]) -> Result<(), String> {
    let twin = shadow_table(table);
    let have = table_columns(db, &twin).await;
    if have.is_empty() {
        return Query::new(create_shadow_sql(table, cols)).execute(db).await;
    }
    for c in cols {
        if !have.iter().any(|h| h.name == c.name) {
            Query::new(heal_shadow_sql(table, c)).execute(db).await?;
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

/// Evict every row the txid owns, everywhere, into the twins; note the pot and
/// lobby changes so the seats and the lobby learn; record the ledger row.
/// Returns the rows moved (0 = nothing held this txid).
pub async fn evict_txid_everywhere(db: &D1Database, txid: &str, reason: &str, now_ms: u64) -> u64 {
    let txid = txid.to_ascii_lowercase();
    let mut moved = 0u64;
    let pot_vouts = vouts_of(db, "pot_records", &txid).await;
    let advert_vouts = vouts_of(db, "low_records", &txid).await;
    for (table, keys) in MOVED_TABLES {
        let cols = table_columns(db, table).await;
        if cols.is_empty() {
            continue;
        }
        for key in keys.iter() {
            if !cols.iter().any(|c| c.name == *key) {
                continue;
            }
            let n = count_keyed(db, table, key, &txid).await;
            if n == 0 {
                continue;
            }
            if let Err(e) = ensure_shadow(db, table, &cols).await {
                worker::console_log!("[admit-fast] evict {txid}: twin for {table} failed: {e}");
                continue;
            }
            let (ins, del) = move_sql(table, key, &cols);
            let ok = Query::new(ins)
                .bind(QVal::Int(now_ms as i64))
                .bind(reason)
                .bind(txid.as_str())
                .execute(db)
                .await;
            match ok {
                Ok(()) => {
                    if let Err(e) = Query::new(del).bind(txid.as_str()).execute(db).await {
                        worker::console_log!("[admit-fast] evict {txid}: delete from {table} by {key} failed after the copy: {e}");
                    } else {
                        moved += n;
                    }
                }
                Err(e) => worker::console_log!(
                    "[admit-fast] evict {txid}: copy from {table} by {key} failed: {e}"
                ),
            }
        }
    }
    for v in &pot_vouts {
        crate::pot_changes::note(&txid, *v);
    }
    for v in &advert_vouts {
        crate::lobby_changes::note_evicted(&txid, *v);
    }
    // the spends it left on the rows it consumed (its hops, its pot) — released
    let released = release_spends_of(db, &txid).await;
    let released_json = serde_json::to_string(&released).unwrap_or_else(|_| "[]".into());
    if let Err(e) = Query::new(
        "INSERT INTO pot_evictions (txid, reason, evictedAt, readmittedAt, rowsMoved, releasedSpends) VALUES (?, ?, ?, NULL, ?, ?) \
         ON CONFLICT(txid) DO UPDATE SET reason = excluded.reason, evictedAt = excluded.evictedAt, readmittedAt = NULL, rowsMoved = excluded.rowsMoved, \
             releasedSpends = COALESCE(NULLIF(excluded.releasedSpends, '[]'), pot_evictions.releasedSpends)",
    )
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
pub async fn readmit_if_evicted(db: &D1Database, txid: &str, now_ms: u64) -> bool {
    let txid = txid.to_ascii_lowercase();
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
            None
        }
    };
    let Some(row) = row else {
        return false;
    };
    let mut restored = 0u64;
    for (table, keys) in MOVED_TABLES {
        let twin = shadow_table(table);
        let cols = table_columns(db, table).await;
        if cols.is_empty() || table_columns(db, &twin).await.is_empty() {
            continue;
        }
        for key in keys.iter() {
            if !cols.iter().any(|c| c.name == *key) {
                continue;
            }
            let n = count_keyed(db, &twin, key, &txid).await;
            if n == 0 {
                continue;
            }
            let (ins, del) = restore_sql(table, key, &cols);
            match Query::new(ins).bind(txid.as_str()).execute(db).await {
                Ok(()) => {
                    if let Err(e) = Query::new(del).bind(txid.as_str()).execute(db).await {
                        worker::console_log!("[admit-fast] readmit {txid}: drop from {twin} by {key} failed after the copy: {e}");
                    } else {
                        restored += n;
                    }
                }
                Err(e) => worker::console_log!(
                    "[admit-fast] readmit {txid}: copy back into {table} by {key} failed: {e}"
                ),
            }
        }
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
    let _ = Query::new("UPDATE pot_evictions SET readmittedAt = ? WHERE txid = ?")
        .bind(QVal::Int(now_ms as i64))
        .bind(txid.as_str())
        .execute(db)
        .await;
    worker::console_log!(
        "[admit-fast] READMITTED {txid} on the pushed proof: {restored} row(s) restored, {remarked}/{} spend pointer(s) re-marked (was evicted: {})",
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

/// PURE: fold the three couriers' words (pinned).
pub fn evidence_verdict(
    arcade: &crate::proof_fetcher::ArcadeLook,
    bitails: Option<bool>,
    woc: Option<bool>,
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
    evidence_verdict(&look, bitails, woc)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proof_fetcher::ArcadeLook;

    /// The watch fits the post-response budget (gate LOW-1: eviction of the
    /// isolate must not silently skip the latch) and looks early (the old wire
    /// poll's cadence: SEEN typically by the 4th 2-s look).
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
                None
            ),
            EvidenceVerdict::Uncertain(_)
        ));
        // Fatal + both indexers definitively absent: the corroborated refusal.
        assert!(matches!(
            evidence_verdict(
                &ArcadeLook::Fatal("DOUBLE_SPEND_ATTEMPTED".into(), "x".into()),
                Some(false),
                Some(false)
            ),
            EvidenceVerdict::Refused(_)
        ));
        // Fatal + an indexer HOLDS it: the #214 class (a stale Arcade
        // REJECTED of a tx the network holds) — kept.
        assert_eq!(
            evidence_verdict(
                &ArcadeLook::Fatal("REJECTED".into(), "x".into()),
                Some(false),
                Some(true)
            ),
            EvidenceVerdict::Present
        );
        assert_eq!(
            evidence_verdict(&ArcadeLook::Present, Some(false), Some(false)),
            EvidenceVerdict::Present,
            "Arcade live holds it: a planted REJECTED changes nothing"
        );
        assert!(
            matches!(
                evidence_verdict(&ArcadeLook::Missing, Some(false), Some(false)),
                EvidenceVerdict::Refused(_)
            ),
            "missing + both absent"
        );
        assert_eq!(
            evidence_verdict(&ArcadeLook::Missing, Some(true), Some(false)),
            EvidenceVerdict::Present,
            "one indexer holds it"
        );
        assert!(
            matches!(
                evidence_verdict(&ArcadeLook::Missing, None, Some(false)),
                EvidenceVerdict::Uncertain(_)
            ),
            "a courier fault is never absence"
        );
        assert!(
            matches!(
                evidence_verdict(&ArcadeLook::Fault, Some(false), Some(false)),
                EvidenceVerdict::Refused(_)
            ),
            "Arcade down + both indexers definitive absent"
        );
        assert!(matches!(
            evidence_verdict(&ArcadeLook::Fault, None, None),
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
        let src = include_str!("routes.rs");
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
            .find("INSERT INTO pot_evictions")
            .expect("the ledger row");
        assert!(
            release < ledger,
            "released BEFORE the ledger row records the list"
        );
        assert!(
            evict.contains(
                "releasedSpends = COALESCE(NULLIF(excluded.releasedSpends, '[]'), pot_evictions.releasedSpends)"
            ),
            "a second eviction that releases nothing keeps the first list"
        );
        let readmit = &src[src.find("pub async fn readmit_if_evicted(").unwrap()..];
        let readmit = &readmit[..readmit.find("pub enum EvidenceVerdict").unwrap()];
        let restore = readmit.find("restore_sql(").expect("the restore");
        let remark = readmit
            .find("remark_spends(db, &txid, &released, now_ms)")
            .expect("the re-mark");
        assert!(restore < remark, "re-marked AFTER the rows are back");
        // the eviction is the event the felt voids on: BOTH jobs ship the pot
        // notes right after it (the route's own flush drained before the job)
        let jobs =
            &src[src.find("pub async fn refusal_job(").unwrap()..src.find("#[cfg(test)]").unwrap()];
        let mut from = 0;
        let mut evictions = 0;
        while let Some(i) = jobs[from..].find("evict_txid_everywhere(") {
            let at = from + i;
            let fl = jobs[at..]
                .find("crate::pot_changes::flush_inline(")
                .expect("a flush after the eviction");
            assert!(
                fl < 700,
                "the flush sits right after the eviction (at +{fl})"
            );
            evictions += 1;
            from = at + 1;
        }
        assert_eq!(evictions, 2, "the refusal job and the pending watch");
    }
}
