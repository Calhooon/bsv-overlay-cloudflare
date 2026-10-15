//! Compact GASP fault facts. No partial graph or chunk lives in this table.
use crate::d1::Query;
use overlay_engine::fault_memo::{FaultMemo, FaultMemoBatch, FAULT_MEMO_DUE_LIMIT};
use serde::Deserialize;
use worker::D1Database;

/// The fault memo's table: one compact fault fact per (host, topic, root outpoint).
pub const CREATE: &str = "CREATE TABLE IF NOT EXISTS gasp_fault_memo (
    host TEXT NOT NULL, topic TEXT NOT NULL, outpoint TEXT NOT NULL,
    class TEXT NOT NULL, faults INTEGER NOT NULL, retries INTEGER NOT NULL,
    first_at INTEGER NOT NULL, next_at INTEGER NOT NULL, score INTEGER NOT NULL,
    dead INTEGER NOT NULL, unanchorable INTEGER NOT NULL, cursor_passable INTEGER NOT NULL,
    expiry INTEGER, configured INTEGER NOT NULL,
    PRIMARY KEY(host, topic, outpoint))";
/// The index the due read walks: a peer's live facts by retry time.
pub const DUE_INDEX: &str = "CREATE INDEX IF NOT EXISTS gasp_fault_memo_due ON gasp_fault_memo(host, topic, dead, next_at, score, outpoint)";
/// The index the per-topic cap evicts by: the oldest observation goes.
pub const AGE_INDEX: &str =
    "CREATE INDEX IF NOT EXISTS gasp_fault_memo_age ON gasp_fault_memo(topic, first_at)";
/// The index the expiry sweep reads.
pub const EXPIRY_INDEX: &str = "CREATE INDEX IF NOT EXISTS gasp_fault_memo_expiry ON gasp_fault_memo(expiry) WHERE expiry IS NOT NULL";
/// The per-topic row count the cap reads, kept by the two triggers.
pub const SIZE_CREATE: &str = "CREATE TABLE IF NOT EXISTS gasp_fault_memo_size (topic TEXT PRIMARY KEY, rows INTEGER NOT NULL)";
/// The count avoids scanning twenty thousand rows for every new fact. Both
/// triggers run inside the writer's transaction, including the cap eviction.
pub const DELETE_TRIGGER: &str =
    "CREATE TRIGGER IF NOT EXISTS gasp_fault_memo_delete AFTER DELETE ON gasp_fault_memo BEGIN
    UPDATE gasp_fault_memo_size SET rows = rows - 1 WHERE topic = old.topic;
    END";
/// The trigger that counts an insert and evicts the topic's oldest facts past twenty thousand.
pub const INSERT_TRIGGER: &str =
    "CREATE TRIGGER IF NOT EXISTS gasp_fault_memo_insert AFTER INSERT ON gasp_fault_memo BEGIN
    INSERT INTO gasp_fault_memo_size(topic, rows) VALUES(new.topic, 1)
      ON CONFLICT(topic) DO UPDATE SET rows = rows + 1;
    DELETE FROM gasp_fault_memo WHERE rowid IN (SELECT rowid FROM gasp_fault_memo
      WHERE topic = new.topic ORDER BY first_at, rowid
      LIMIT MAX(0, (SELECT rows FROM gasp_fault_memo_size WHERE topic = new.topic) - 20000));
    END";

/// Write one fault fact (insert, or replace the held one).
pub const PUT: &str = "INSERT INTO gasp_fault_memo
    (host, topic, outpoint, class, faults, retries, first_at, next_at, score, dead, unanchorable, cursor_passable, expiry, configured)
    VALUES (?1, ?2, ?3, ?4, ?5, ?6, unixepoch(), ?7, ?8, ?9, ?10, ?11, ?12, ?13)
    ON CONFLICT(host, topic, outpoint) DO UPDATE SET
      class=excluded.class, faults=excluded.faults, retries=excluded.retries,
      first_at=CASE WHEN gasp_fault_memo.score=excluded.score THEN gasp_fault_memo.first_at ELSE excluded.first_at END,
      next_at=excluded.next_at, score=excluded.score, dead=excluded.dead,
      unanchorable=excluded.unanchorable, cursor_passable=excluded.cursor_passable,
      expiry=excluded.expiry, configured=excluded.configured";
/// Delete one fact.
pub const DELETE: &str = "DELETE FROM gasp_fault_memo WHERE host=?1 AND topic=?2 AND outpoint=?3";
/// Read one fact by its key.
pub const GET: &str =
    "SELECT host AS peer, * FROM gasp_fault_memo WHERE host=?1 AND topic=?2 AND outpoint=?3";
/// Read the facts of a page's outpoints (bind: a JSON array).
pub const PAGE: &str = "SELECT host AS peer, * FROM gasp_fault_memo WHERE host=?1 AND topic=?2 AND outpoint IN (SELECT value FROM json_each(?3))";
/// Read a peer's due live facts, by ascending retry time (bind: the limit).
pub const DUE: &str = "SELECT host AS peer, * FROM gasp_fault_memo WHERE host=?1 AND topic=?2 AND dead=0 AND next_at<=unixepoch() ORDER BY next_at, score, outpoint LIMIT ?3";
/// Reserve one unanchorable retry: a compare-and-set on the retry count, score and class, returning the outpoint when it holds.
pub const RESERVE: &str = "UPDATE gasp_fault_memo SET retries=?4, next_at=?5
    WHERE host=?1 AND topic=?2 AND outpoint=?3 AND dead=0 AND unanchorable=1
      AND retries=?4-1 AND ?4<=?8 AND next_at<=unixepoch() AND score=?6 AND class=?7
    RETURNING outpoint";
/// Delete a topic's fact of a readmitted outpoint.
pub const READMIT: &str =
    "DELETE FROM gasp_fault_memo WHERE topic=?1 AND outpoint=?2 RETURNING outpoint";
/// The memo's total row count.
pub const COUNT: &str = "SELECT COUNT(*) AS rows FROM gasp_fault_memo";
/// Engine 0.1.15 (#56): the absence facts recorded in the last day (by the
/// row's `first_at`),
/// and how many of them are struck (`unanchorable:not_found` or dead). One
/// pass over the memo, which its row bound limits, as `COUNT` above.
pub const PEER_ABSENT_24H: &str = "SELECT COUNT(*) AS recorded,
    COALESCE(SUM(unanchorable=1 OR dead=1), 0) AS struck FROM gasp_fault_memo
    WHERE class='not_found' AND first_at>=unixepoch()-86400";

// Fault rows written by 0.1.9 through 0.1.13 have no nodes or chunks. Keep
// malformed/unknown records in the old store for its ordinary error handling.
/// Fold a peer's legacy fault records (deferred graphs with no nodes) into the memo.
pub const FOLD: &str = "INSERT INTO gasp_fault_memo
    (host, topic, outpoint, class, faults, retries, first_at, next_at, score, dead, unanchorable, cursor_passable, expiry, configured)
    SELECT host, topic, outpoint,
      CASE WHEN json_extract(record,'$.reason') LIKE 'dead:%' THEN substr(json_extract(record,'$.reason'),6)
           WHEN json_extract(record,'$.reason') LIKE 'unanchorable:%' THEN substr(json_extract(record,'$.reason'),14)
           ELSE substr(json_extract(record,'$.reason'),13) END,
      json_extract(record,'$.faults'), COALESCE(json_extract(record,'$.unanchorableRetries'),0),
      created_at, COALESCE(json_extract(record,'$.retryAt'),0), score,
      json_extract(record,'$.reason') LIKE 'dead:%', json_extract(record,'$.reason') LIKE 'unanchorable:%',
      COALESCE(json_extract(record,'$.cursorPassable'),0), json_extract(record,'$.expiry'),
      COALESCE(json_extract(record,'$.configured'),0)
    FROM gasp_deferred_graphs WHERE host=?1 AND topic=?2 AND chunks=0 AND json_valid(record)
      AND COALESCE(json_array_length(record,'$.nodes'),0)=0 AND json_extract(record,'$.faults')>0
      AND (json_extract(record,'$.reason') LIKE 'dead:%' OR json_extract(record,'$.reason') LIKE 'unanchorable:%'
        OR json_extract(record,'$.reason') LIKE 'retry:fault:%')
    ON CONFLICT(host,topic,outpoint) DO NOTHING";
/// Delete the legacy records the fold copied.
pub const FOLD_DELETE: &str = "DELETE FROM gasp_deferred_graphs WHERE host=?1 AND topic=?2 AND chunks=0 AND json_valid(record)
    AND COALESCE(json_array_length(record,'$.nodes'),0)=0 AND json_extract(record,'$.faults')>0
    AND (json_extract(record,'$.reason') LIKE 'dead:%' OR json_extract(record,'$.reason') LIKE 'unanchorable:%'
      OR json_extract(record,'$.reason') LIKE 'retry:fault:%') RETURNING outpoint";

/// Count the folded records in `gasp_fault_memo_folded_total`.
pub const FOLD_COUNT: &str = "INSERT INTO ops_counters(name,value)
    SELECT 'gasp_fault_memo_folded_total',COUNT(*) FROM gasp_deferred_graphs WHERE host=?1 AND topic=?2 AND chunks=0 AND json_valid(record)
    AND COALESCE(json_array_length(record,'$.nodes'),0)=0 AND json_extract(record,'$.faults')>0
    AND (json_extract(record,'$.reason') LIKE 'dead:%' OR json_extract(record,'$.reason') LIKE 'unanchorable:%'
      OR json_extract(record,'$.reason') LIKE 'retry:fault:%') HAVING COUNT(*)>0
    ON CONFLICT(name) DO UPDATE SET value=ops_counters.value+excluded.value";

/// Preserve an expiry skip before removing its fault fact. A later page then
/// costs no node request, including a page served in this same invocation.
pub const REMEMBER_EXPIRED: &str = "INSERT INTO gasp_expired_outpoints(topic,outpoint,keep_until,written_at)
    SELECT topic,outpoint,unixepoch()+?3,unixepoch() FROM gasp_fault_memo
    WHERE (?1 IS NULL OR host=?1) AND (?2 IS NULL OR topic=?2) AND (dead=1 OR unanchorable=1)
      AND expiry<=unixepoch()
    ON CONFLICT(topic,outpoint) DO UPDATE SET keep_until=MAX(gasp_expired_outpoints.keep_until,excluded.keep_until)";
/// Remove expired dead or unanchorable facts, including those the shared expired-outpoint memo names.
pub const SWEEP: &str = "DELETE FROM gasp_fault_memo
    WHERE (?1 IS NULL OR host=?1) AND (?2 IS NULL OR topic=?2) AND (dead=1 OR unanchorable=1)
      AND (expiry<=unixepoch() OR EXISTS (SELECT 1 FROM gasp_expired_outpoints e
        WHERE e.topic=gasp_fault_memo.topic AND e.outpoint=gasp_fault_memo.outpoint AND e.keep_until>unixepoch()))
    RETURNING outpoint";
// Sync slices use the expiry index; the scheduled sweep also reconciles facts
// whose expiry was learned through the shared expired-outpoint memo.
/// Remove expired dead or unanchorable facts by their own expiry only.
pub const SWEEP_EXPIRED: &str = "DELETE FROM gasp_fault_memo
    WHERE (?1 IS NULL OR host=?1) AND (?2 IS NULL OR topic=?2) AND (dead=1 OR unanchorable=1)
      AND expiry<=unixepoch() RETURNING outpoint";

#[derive(Deserialize)]
struct Row {
    peer: String,
    topic: String,
    outpoint: String,
    class: String,
    faults: u32,
    retries: u32,
    first_at: u64,
    next_at: u64,
    score: u64,
    dead: u32,
    unanchorable: u32,
    cursor_passable: u32,
    expiry: Option<u64>,
    configured: u32,
}
impl From<Row> for FaultMemo {
    fn from(r: Row) -> Self {
        Self {
            peer: r.peer,
            topic: r.topic,
            outpoint: r.outpoint,
            class: r.class,
            faults: r.faults,
            retries: r.retries,
            first_at: r.first_at,
            next_at: r.next_at,
            score: r.score,
            dead: r.dead != 0,
            unanchorable: r.unanchorable != 0,
            cursor_passable: r.cursor_passable != 0,
            expiry: r.expiry,
            configured: r.configured != 0,
        }
    }
}

/// [`PUT`]'s query for one fact.
pub fn put_query(r: &FaultMemo) -> Query {
    Query::new(PUT)
        .bind(r.peer.as_str())
        .bind(r.topic.as_str())
        .bind(r.outpoint.as_str())
        .bind(r.class.as_str())
        .bind(r.faults)
        .bind(r.retries)
        .bind(r.next_at)
        .bind(r.score)
        .bind(r.dead)
        .bind(r.unanchorable)
        .bind(r.cursor_passable)
        .bind(r.expiry)
        .bind(r.configured)
}

async fn batch(db: &D1Database, queries: Vec<Query>) -> Result<Vec<worker::D1Result>, String> {
    let statements = queries
        .into_iter()
        .map(|q| q.prepare(db))
        .collect::<Result<Vec<_>, _>>()?;
    crate::d1_ledger::counted_batch(db, statements)
        .await
        .map_err(|e| e.to_string())
}

/// Write one fact and drop the deferred graph it replaces, in one batch.
pub async fn put(db: &D1Database, r: &FaultMemo) -> Result<(), String> {
    batch(
        db,
        vec![
            put_query(r),
            Query::new(crate::gasp_deferred::DEFERRED_GRAPH_DELETE_SQL)
                .bind(r.peer.as_str())
                .bind(r.topic.as_str())
                .bind(r.outpoint.as_str()),
            Query::new(crate::gasp_deferred::DEFERRED_GRAPH_CHUNKS_DELETE_SQL)
                .bind(r.peer.as_str())
                .bind(r.topic.as_str())
                .bind(r.outpoint.as_str()),
        ],
    )
    .await?;
    Ok(())
}

/// One fact, if held.
pub async fn get(
    db: &D1Database,
    host: &str,
    topic: &str,
    outpoint: &str,
) -> Result<Option<FaultMemo>, String> {
    Ok(Query::new(GET)
        .bind(host)
        .bind(topic)
        .bind(outpoint)
        .fetch_optional::<Row>(db)
        .await?
        .map(Into::into))
}

/// Remember and remove the expired facts (`reconcile_shared` also reads the shared memo); the count removed.
pub async fn sweep(
    db: &D1Database,
    host: Option<&str>,
    topic: Option<&str>,
    reconcile_shared: bool,
) -> Result<u64, String> {
    let sweep_sql = if reconcile_shared {
        SWEEP
    } else {
        SWEEP_EXPIRED
    };
    let results = batch(
        db,
        vec![
            Query::new(REMEMBER_EXPIRED)
                .bind(host)
                .bind(topic)
                .bind(overlay_engine::gasp::EXPIRED_OUTPOINT_MEMO_SECS),
            Query::new(sweep_sql).bind(host).bind(topic),
        ],
    )
    .await?;
    Ok(results[1]
        .results::<serde_json::Value>()
        .map_err(|e| e.to_string())?
        .len() as u64)
}

/// The facts of the outpoints asked, or (none asked) a peer's due facts after folding its legacy records and sweeping.
pub async fn read(
    db: &D1Database,
    host: &str,
    topic: &str,
    outpoints: Option<&[String]>,
) -> Result<FaultMemoBatch, String> {
    let mut out = FaultMemoBatch::default();
    let query = if let Some(keys) = outpoints {
        Query::new(PAGE)
            .bind(host)
            .bind(topic)
            .bind(serde_json::to_string(keys).map_err(|e| e.to_string())?)
    } else {
        let results = batch(
            db,
            vec![
                Query::new(FOLD).bind(host).bind(topic),
                Query::new(FOLD_COUNT).bind(host).bind(topic),
                Query::new(FOLD_DELETE).bind(host).bind(topic),
            ],
        )
        .await?;
        out.folded = results[2]
            .results::<serde_json::Value>()
            .map_err(|e| e.to_string())?
            .len() as u64;
        if out.folded > 0 {
            worker::console_log!("GASP fault memo folded={} legacy rows freed", out.folded);
        }
        out.swept = sweep(db, Some(host), Some(topic), out.folded > 0).await?;
        Query::new(DUE)
            .bind(host)
            .bind(topic)
            .bind(FAULT_MEMO_DUE_LIMIT as u64)
    };
    out.records = query
        .fetch_all::<Row>(db)
        .await?
        .into_iter()
        .map(Into::into)
        .collect();
    Ok(out)
}

/// The memo's total row count, for the health block.
pub async fn health(db: &D1Database) -> Result<u64, String> {
    #[derive(Deserialize)]
    struct Count {
        rows: u64,
    }
    Ok(Query::new(COUNT)
        .fetch_optional::<Count>(db)
        .await?
        .map_or(0, |r| r.rows))
}

/// `gaspPeerAbsent24h` (engine 0.1.15): the absence facts the peers' word
/// (or the rungs' beside it) recorded in the last day, and those struck.
pub async fn peer_absent_24h(db: &D1Database) -> Result<serde_json::Value, String> {
    #[derive(Deserialize)]
    struct Absent {
        recorded: u64,
        struck: u64,
    }
    let row = Query::new(PEER_ABSENT_24H)
        .fetch_optional::<Absent>(db)
        .await?;
    let (recorded, struck) = row.map_or((0, 0), |r| (r.recorded, r.struck));
    Ok(serde_json::json!({"recorded": recorded, "struck": struck}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::{params, Connection};

    fn sqlite() -> Connection {
        let db = Connection::open_in_memory().unwrap();
        for sql in crate::d1::OVERLAY_MIGRATIONS {
            if let Err(e) = db.execute_batch(sql) {
                assert!(
                    crate::d1::migration_error_is_benign(sql, &e.to_string()),
                    "{e}: {sql}"
                );
            }
        }
        db
    }

    fn values(q: &Query) -> Vec<rusqlite::types::Value> {
        use crate::d1::QVal;
        q.params()
            .iter()
            .map(|v| match v {
                QVal::Null => rusqlite::types::Value::Null,
                QVal::Int(v) => (*v).into(),
                QVal::Float(v) => (*v).into(),
                QVal::Bool(v) => i64::from(*v).into(),
                QVal::Text(v) => v.clone().into(),
                QVal::Blob(v) => v.clone().into(),
            })
            .collect()
    }

    fn put(db: &Connection, memo: &FaultMemo) {
        let q = put_query(memo);
        db.execute(q.sql(), rusqlite::params_from_iter(values(&q)))
            .unwrap();
    }

    fn memo(n: usize) -> FaultMemo {
        FaultMemo {
            peer: "https://peer.example".into(),
            topic: "tm_test".into(),
            outpoint: format!("{n:064x}.0"),
            class: "not_found".into(),
            faults: 1,
            retries: 0,
            first_at: 0,
            next_at: 0,
            score: n as u64,
            dead: false,
            unanchorable: true,
            cursor_passable: true,
            expiry: None,
            configured: true,
        }
    }

    fn count(db: &Connection) -> usize {
        db.query_row(COUNT, [], |r| r.get(0)).unwrap()
    }

    #[test]
    fn absence_0115_sql_counts_the_days_absences_and_the_struck() {
        let db = sqlite();
        let mut counting = memo(1);
        counting.unanchorable = false;
        counting.cursor_passable = false;
        put(&db, &counting);
        put(&db, &memo(2));
        let mut courier = memo(3);
        courier.class = "courier".into();
        put(&db, &courier);
        put(&db, &memo(4));
        db.execute(
            "UPDATE gasp_fault_memo SET first_at=unixepoch()-90000 WHERE score=4",
            [],
        )
        .unwrap();
        let (recorded, struck): (u64, u64) = db
            .query_row(PEER_ABSENT_24H, [], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap();
        assert_eq!((recorded, struck), (2, 1));
    }

    #[test]
    fn memo_0114_sql_capacity_is_atomic_and_keeps_the_oldest_order() {
        let mut db = sqlite();
        let limit = overlay_engine::fault_memo::FAULT_MEMO_MAX_ROWS;
        let tx = db.transaction().unwrap();
        for n in 0..limit + 2 {
            put(&tx, &memo(n));
        }
        tx.commit().unwrap();
        assert_eq!(count(&db), limit);
        let low: u64 = db
            .query_row("SELECT MIN(score) FROM gasp_fault_memo", [], |r| r.get(0))
            .unwrap();
        assert_eq!(low, 2);
        put(&db, &memo(2));
        assert_eq!(count(&db), limit, "an update consumes no new row");
        db.execute(DELETE, params![memo(2).peer, "tm_test", memo(2).outpoint])
            .unwrap();
        let size: usize = db
            .query_row(
                "SELECT rows FROM gasp_fault_memo_size WHERE topic='tm_test'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(size, limit - 1);
        let partial: usize = db
            .query_row("SELECT COUNT(*) FROM gasp_deferred_graphs", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(partial, 0);
    }

    #[test]
    fn memo_0114_sync_reads_use_indexes_without_sorting_the_memo() {
        let db = sqlite();
        let plan = |sql: &str, values: &[rusqlite::types::Value]| {
            db.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
                .unwrap()
                .query_map(rusqlite::params_from_iter(values), |r| {
                    r.get::<_, String>(3)
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
                .join("\n")
        };
        let pair = [memo(0).peer.into(), "tm_test".to_string().into()];
        let mut due_values = pair.to_vec();
        due_values.push((FAULT_MEMO_DUE_LIMIT as i64).into());
        let due = plan(DUE, &due_values);
        assert!(due.contains("gasp_fault_memo_due"), "{due}");
        assert!(!due.contains("TEMP B-TREE"), "{due}");
        let sweep = plan(SWEEP_EXPIRED, &pair);
        assert!(sweep.contains("gasp_fault_memo_expiry"), "{sweep}");
        let mut page_values = pair.to_vec();
        page_values.push("[]".to_string().into());
        let page = plan(PAGE, &page_values);
        assert!(page.contains("host=? AND topic=? AND outpoint=?"), "{page}");
    }

    fn seed_legacy(db: &Connection, r: &FaultMemo) {
        let record = r.clone().into_record();
        db.execute("INSERT INTO gasp_deferred_graphs(host,topic,outpoint,score,nodes,pending,calls,passes,reason,bytes,record,created_at,updated_at)
            VALUES(?1,?2,?3,?4,0,0,0,0,?5,0,?6,123,123)",
            params![r.peer,r.topic,r.outpoint,r.score,crate::gasp_deferred::retained_reason(&record),serde_json::to_string(&record).unwrap()]).unwrap();
    }

    fn fold(db: &mut Connection) -> usize {
        let tx = db.transaction().unwrap();
        tx.execute(FOLD, params![memo(0).peer, "tm_test"]).unwrap();
        tx.execute(FOLD_COUNT, params![memo(0).peer, "tm_test"])
            .unwrap();
        let removed = tx
            .prepare(FOLD_DELETE)
            .unwrap()
            .query_map(params![memo(0).peer, "tm_test"], |r| r.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .len();
        tx.commit().unwrap();
        removed
    }

    #[test]
    fn memo_0114_sql_legacy_fold_is_counted_once_and_recoverable() {
        let mut db = sqlite();
        let mut r = memo(1);
        r.retries = 3;
        r.next_at = 2_000_000_000;
        r.expiry = Some(3_000_000_000);
        seed_legacy(&db, &r);
        {
            let tx = db.transaction().unwrap();
            tx.execute(FOLD, params![r.peer, r.topic]).unwrap();
            tx.execute(FOLD_COUNT, params![r.peer, r.topic]).unwrap();
            // Aborting a batch retains the legacy row and rolls back its memo.
            tx.rollback().unwrap();
        }
        assert_eq!(count(&db), 0);
        assert_eq!(fold(&mut db), 1);
        assert_eq!(fold(&mut db), 0);
        let values: (String, u32, u64, u64, u64) = db
            .query_row(GET, params![r.peer, r.topic, r.outpoint], |row| {
                Ok((
                    row.get("class")?,
                    row.get("retries")?,
                    row.get("next_at")?,
                    row.get("expiry")?,
                    row.get("first_at")?,
                ))
            })
            .unwrap();
        assert_eq!(
            values,
            ("not_found".into(), 3, 2_000_000_000, 3_000_000_000, 123)
        );
        let folded: u64 = db
            .query_row(
                "SELECT value FROM ops_counters WHERE name='gasp_fault_memo_folded_total'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(folded, 1);
    }

    #[test]
    fn memo_0114_sql_claims_sweep_and_operator_clear_preserve_scope() {
        let db = sqlite();
        let mut r = memo(1);
        put(&db, &r);
        let claim = |r: &FaultMemo| {
            db.prepare(RESERVE)
                .unwrap()
                .query_map(
                    params![
                        r.peer,
                        r.topic,
                        r.outpoint,
                        r.retries,
                        r.next_at,
                        r.score,
                        r.class,
                        overlay_engine::gasp::UNANCHORABLE_RETRIES
                    ],
                    |row| row.get::<_, String>(0),
                )
                .unwrap()
                .count()
        };
        r.retries = 1;
        r.next_at = 3_000_000_000;
        assert_eq!(claim(&r), 1);
        assert_eq!(claim(&r), 0);
        r.retries = 2;
        assert_eq!(claim(&r), 0, "backoff");
        db.execute("UPDATE gasp_fault_memo SET next_at=0", [])
            .unwrap();
        r.score += 1;
        assert_eq!(claim(&r), 0, "stale score");
        r.score -= 1;
        assert_eq!(claim(&r), 1);
        r.dead = true;
        r.expiry = Some(1);
        put(&db, &r);
        let mut other = memo(2);
        other.topic = "tm_other".into();
        put(&db, &other);
        db.execute(
            REMEMBER_EXPIRED,
            params![
                r.peer,
                r.topic,
                overlay_engine::gasp::EXPIRED_OUTPOINT_MEMO_SECS
            ],
        )
        .unwrap();
        let swept = db
            .prepare(SWEEP)
            .unwrap()
            .query_map(params![r.peer, r.topic], |row| row.get::<_, String>(0))
            .unwrap()
            .count();
        assert_eq!(swept, 1);
        assert_eq!(count(&db), 1);
        let remembered: u64 = db
            .query_row(
                "SELECT COUNT(*) FROM gasp_expired_outpoints WHERE topic='tm_test'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(remembered, 1);
        put(&db, &r);
        db.prepare(READMIT)
            .unwrap()
            .query_map(params![r.topic, r.outpoint], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(count(&db), 1);
    }

    #[test]
    fn memo_0114_sql_walk_origin_opens_only_the_age_gate() {
        let db = sqlite();
        use crate::d1_storage::{
            INSERT_OUTPUT_TX_GASP_SQL, INSERT_OUTPUT_TX_SQL, PROOF_CANDIDATES_SQL,
            UPDATE_TX_BEEF_SQL,
        };
        db.execute(INSERT_OUTPUT_TX_SQL, params!["submit", vec![1u8]])
            .unwrap();
        db.execute(INSERT_OUTPUT_TX_GASP_SQL, params!["walk", vec![2u8]])
            .unwrap();
        db.execute(INSERT_OUTPUT_TX_GASP_SQL, params!["retired", vec![3u8]])
            .unwrap();
        db.execute(
            "UPDATE transactions SET retired_ms=1 WHERE txid='retired'",
            [],
        )
        .unwrap();
        let candidates = || {
            db.prepare(PROOF_CANDIDATES_SQL)
                .unwrap()
                .query_map(params![1800, 100], |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, u32>(2)?))
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        };
        assert_eq!(candidates(), vec![("walk".into(), 1)]);
        db.execute(INSERT_OUTPUT_TX_SQL, params!["walk", vec![4u8]])
            .unwrap();
        assert_eq!(
            candidates(),
            vec![("walk".into(), 1)],
            "a re-present preserves its origin"
        );
        db.execute(UPDATE_TX_BEEF_SQL, params!["walk", vec![5u8], 1, 900])
            .unwrap();
        assert!(candidates().is_empty(), "a proven row leaves the pool");
        let origin: String = db
            .query_row(
                "SELECT admission_origin FROM transactions WHERE txid='walk'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(origin, "gasp");
        db.execute(
            "UPDATE transactions SET created_at=unixepoch()-1800 WHERE txid='submit'",
            [],
        )
        .unwrap();
        assert_eq!(candidates(), vec![("submit".into(), 0)]);
    }
}
