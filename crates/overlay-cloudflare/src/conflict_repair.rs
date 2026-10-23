//! One-time repair of hop pointers released before bsv-low #605 (#607).
//! A fixed ledger window, at most 16 rows per pass, and no courier requests.

use crate::admit_fast::{record_conflict_spender, ShadowDb};
use crate::d1::{QVal, Query};
use async_trait::async_trait;
use worker::{D1Database, Env, Request, Response};

/// Maximum ledger rows examined in one pass, including rows that are skipped.
pub const REPAIR_LIMIT: usize = 16;
/// Singleton cursor, frozen window end and completion time, appended as migration 346.
pub const REPAIR_CREATE: &str = "CREATE TABLE IF NOT EXISTS conflict_spender_repair (id INTEGER PRIMARY KEY CHECK (id = 1), last_rowid INTEGER NOT NULL, through_rowid INTEGER NOT NULL, done_at INTEGER)";
/// The last processed rowid, or -1 after completion.
pub const STATE_SQL: &str = "SELECT CASE WHEN done_at IS NULL THEN last_rowid ELSE -1 END AS v FROM conflict_spender_repair WHERE id = 1";
/// Freeze the historical window once; an existing cursor is never reset.
pub const BEGIN_SQL: &str = "INSERT OR IGNORE INTO conflict_spender_repair (id, last_rowid, through_rowid) SELECT 1, 0, (SELECT COALESCE(MAX(rowid), 0) FROM pot_evictions) WHERE NOT EXISTS (SELECT 1 FROM conflict_spender_repair WHERE id = 1)";
/// Page the ledger itself so unrelated or malformed reasons cannot cause an unbounded scan.
pub const PAGE_SQL: &str = "SELECT rowid AS position, txid, reason, readmittedAt AS readmitted_at FROM pot_evictions WHERE rowid > ?1 AND rowid <= (SELECT through_rowid FROM conflict_spender_repair WHERE id = 1) ORDER BY rowid LIMIT 16";
/// Advance only the cursor this pass read; bind old cursor, new cursor, done and current time.
pub const CHECKPOINT_SQL: &str = "UPDATE conflict_spender_repair SET last_rowid = ?2, done_at = CASE WHEN ?3 THEN ?4 ELSE NULL END WHERE id = 1 AND done_at IS NULL AND last_rowid = ?1 RETURNING 1 AS v";
/// Durable counter names, also seeded at zero by the health read.
pub const COUNTERS: [&str; 4] = [
    "conflict_spender_repair_scanned_total",
    "conflict_spender_repair_repaired_total",
    "conflict_spender_repair_skipped_total",
    "conflict_spender_repair_faults_total",
];

#[derive(Debug, serde::Deserialize)]
/// One ledger row; no stored transaction bytes or courier data are read.
pub struct Eviction {
    /// Stable ledger position within the frozen window.
    pub position: i64,
    /// The evicted transaction.
    pub txid: String,
    /// The recorded eviction reason, parsed only for confirmed conflicts.
    pub reason: String,
    /// Readmitted rows are skipped.
    pub readmitted_at: Option<i64>,
}

/// The one typed read beside the existing guarded spender writer's storage port.
#[async_trait(?Send)]
pub trait RepairDb: ShadowDb {
    /// Read the bounded page after this cursor with [`PAGE_SQL`].
    async fn repair_page(&self, after: i64) -> Result<Vec<Eviction>, String>;
}

#[async_trait(?Send)]
impl RepairDb for D1Database {
    async fn repair_page(&self, after: i64) -> Result<Vec<Eviction>, String> {
        Query::new(PAGE_SQL).bind(after).fetch_all(self).await
    }
}

#[derive(Debug, Default, serde::Serialize)]
/// A single bounded pass, returned by the operator lever and logged by the tick.
pub struct Summary {
    /// Ledger rows examined, including skipped and faulted rows.
    pub scanned: u64,
    /// Hop pointers changed by the guarded writer.
    pub repaired: u64,
    /// Irrelevant/readmitted reasons or rows the writer's guard preserved.
    pub skipped: u64,
    /// Write faults; the failed row remains at the head of the next page.
    pub faults: u64,
    /// Whether the durable completion stamp is present.
    pub done: bool,
}

fn txid_ok(txid: &str) -> bool {
    txid.len() == 64 && txid.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Accept only the reason produced by `conflict_eviction_reason`, including a positive height.
pub fn recorded_competitor(reason: &str) -> Option<(&str, u32, &str)> {
    let (outpoint, word) = reason
        .strip_prefix("input ")?
        .split_once(" spent by confirmed competitor ")?;
    let (parent, vout) = outpoint.split_once(':')?;
    let (competitor, height) = word.split_once(" at ")?;
    let vout = vout.parse::<u32>().ok()?;
    let height = height.parse::<u32>().ok()?;
    (txid_ok(parent) && txid_ok(competitor) && height > 0).then_some((parent, vout, competitor))
}

/// Resume a fixed window of historical rows. A write fault leaves that row for the next pass.
/// A completed window is never reopened, including by the operator lever. The existing writer
/// preserves confirmed/foreign pointers and notifies the pots room only when a pointer changes.
pub async fn repair_pass<D: RepairDb>(db: &D, now_ms: i64) -> Result<Summary, String> {
    let mut cursor = db.query_i64(STATE_SQL, vec![]).await?;
    if cursor.is_none() {
        db.exec(BEGIN_SQL, vec![]).await?;
        cursor = db.query_i64(STATE_SQL, vec![]).await?;
    }
    let cursor = cursor.ok_or("repair cursor missing after initialization")?;
    if cursor < 0 {
        return Ok(Summary {
            done: true,
            ..Summary::default()
        });
    }
    let rows = db.repair_page(cursor).await?;
    let exhausted = rows.len() < REPAIR_LIMIT;
    let mut summary = Summary::default();
    let mut next = cursor;
    for row in rows.into_iter().take(REPAIR_LIMIT) {
        summary.scanned += 1;
        if let Some((parent, vout, competitor)) =
            recorded_competitor(&row.reason).filter(|(_, _, c)| {
                row.readmitted_at.is_none()
                    && txid_ok(&row.txid)
                    && !c.eq_ignore_ascii_case(&row.txid)
            })
        {
            match record_conflict_spender(db, parent, vout, &row.txid, competitor).await {
                Ok(true) => summary.repaired += 1,
                Ok(false) => summary.skipped += 1,
                Err(_) => {
                    summary.faults += 1;
                    break;
                }
            }
        } else {
            summary.skipped += 1;
        }
        next = row.position;
    }
    let done = exhausted && summary.faults == 0;
    // Compare the starting cursor so overlapping operator/tick passes cannot move it backwards.
    let checkpoint = db
        .query_i64(
            CHECKPOINT_SQL,
            vec![
                QVal::Int(cursor),
                QVal::Int(next),
                QVal::Bool(done),
                QVal::Int(now_ms),
            ],
        )
        .await?;
    summary.done = if checkpoint.is_some() {
        done
    } else {
        db.query_i64(STATE_SQL, vec![]).await? == Some(-1)
    };
    Ok(summary)
}

/// Run the pass and flush its telemetry as one best-effort counter batch.
pub async fn run_counted(db: &D1Database) -> Result<Summary, String> {
    let result = repair_pass(db, worker::Date::now().as_millis() as i64).await;
    let counts = match &result {
        Ok(s) => [s.scanned, s.repaired, s.skipped, s.faults],
        Err(_) => [0, 0, 0, 1],
    };
    crate::ops::bump_counters(
        db,
        COUNTERS
            .into_iter()
            .zip(counts)
            .map(|(name, count)| (name.to_string(), count))
            .collect(),
    )
    .await;
    result
}

/// Routed before shared database initialization: an invalid bearer receives 401 before any read.
pub async fn internal_repair(
    mut req: Request,
    env: &Env,
    ctx: &worker::Context,
) -> worker::Result<Response> {
    let authorization = req.headers().get("authorization").ok().flatten();
    let secret = env.secret("INTERNAL_TOKEN").ok().map(|s| s.to_string());
    if !crate::tip_pass::bearer_ok(authorization.as_deref(), secret.as_deref()) {
        return Response::error("unauthorized", 401);
    }
    let raw = req.bytes().await?;
    let body = std::str::from_utf8(&raw).unwrap_or("x").trim();
    if !(body.is_empty() || body == "{}") {
        return Response::error("body must be empty or {}: a pass takes no argument", 400);
    }
    let db = env.d1("OVERLAY_DB")?;
    crate::d1::ensure_overlay_migrations(&db)
        .await
        .map_err(worker::Error::from)?;
    let result = run_counted(&db).await;
    crate::pot_changes::flush(env, |f| ctx.wait_until(f));
    Response::from_json(&result.map_err(worker::Error::from)?)
}
