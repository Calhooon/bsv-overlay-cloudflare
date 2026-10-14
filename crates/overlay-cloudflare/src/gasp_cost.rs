//! Durable, rolling one-day sync cost readings. These are trait operations,
//! not billed SQL rows; the D1 row ledger remains the row-count source.
use crate::d1::Query;
use overlay_engine::meter::SyncCost;
use serde::Deserialize;
use serde_json::{json, Value};
use worker::D1Database;

pub const CREATE: &str = "CREATE TABLE IF NOT EXISTS gasp_sync_cost (id INTEGER PRIMARY KEY, ended_ms INTEGER NOT NULL, source TEXT NOT NULL, calls_out INTEGER NOT NULL, peer_requests INTEGER NOT NULL, courier_rungs INTEGER NOT NULL, storage_reads INTEGER NOT NULL, storage_writes INTEGER NOT NULL, ms INTEGER NOT NULL)";
pub const INSERT: &str = "INSERT INTO gasp_sync_cost (ended_ms, source, calls_out, peer_requests, courier_rungs, storage_reads, storage_writes, ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)";
pub const PRUNE: &str = "DELETE FROM gasp_sync_cost WHERE ended_ms < ?1 AND id != COALESCE((SELECT MAX(id) FROM gasp_sync_cost WHERE source = 'tick'), -1)";
pub const SUMS: &str = "SELECT COALESCE(SUM(calls_out), 0) AS calls_out, COALESCE(SUM(peer_requests), 0) AS peer_requests, COALESCE(SUM(courier_rungs), 0) AS courier_rungs, COALESCE(SUM(storage_reads), 0) AS storage_reads, COALESCE(SUM(storage_writes), 0) AS storage_writes, COALESCE(SUM(ms), 0) AS ms FROM gasp_sync_cost WHERE ended_ms >= ?1";
pub const LAST_TICK: &str = "SELECT ended_ms, calls_out, peer_requests, courier_rungs, storage_reads, storage_writes, ms FROM gasp_sync_cost WHERE source = 'tick' ORDER BY id DESC LIMIT 1";
const DAY_MS: f64 = 86_400_000.0;

pub async fn record(db: &D1Database, source: &str, cost: SyncCost) {
    let now = js_sys::Date::now();
    let saved = Query::new(INSERT)
        .bind(now)
        .bind(source)
        .bind(cost.calls_out)
        .bind(cost.peer_requests)
        .bind(cost.courier_rungs)
        .bind(cost.storage_reads)
        .bind(cost.storage_writes)
        .bind(cost.ms)
        .execute(db)
        .await;
    if let Err(e) = saved {
        worker::console_error!("GASP cost save failed: {e}");
    }
    if let Err(e) = Query::new(PRUNE).bind(now - DAY_MS).execute(db).await {
        worker::console_error!("GASP cost prune failed: {e}");
    }
}

#[derive(Deserialize, serde::Serialize)]
struct TickCost {
    ended_ms: f64,
    calls_out: f64,
    peer_requests: f64,
    courier_rungs: f64,
    storage_reads: f64,
    storage_writes: f64,
    ms: f64,
}
pub async fn view(db: &D1Database) -> Value {
    let total = Query::new(SUMS)
        .bind(js_sys::Date::now() - DAY_MS)
        .fetch_optional::<SyncCost>(db)
        .await;
    let last = Query::new(LAST_TICK).fetch_optional::<TickCost>(db).await;
    match (total, last) {
        (Ok(total), Ok(last)) => {
            json!({"totals24h": total.unwrap_or_default(), "lastTick": last, "fault": false})
        }
        _ => json!({"fault": true}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn load_0112_cost_sql_rolls_one_day_and_keeps_the_latest_tick() {
        let db = rusqlite::Connection::open_in_memory().unwrap();
        db.execute_batch(CREATE).unwrap();
        for (at, source, calls) in [
            (1, "tick", 99),
            (86_400_001, "tick", 3),
            (86_400_002, "admin", 5),
        ] {
            db.execute(
                INSERT,
                rusqlite::params![at, source, calls, calls - 1, 1, 7, 2, 40],
            )
            .unwrap();
        }
        let calls: u64 = db.query_row(SUMS, [2], |r| r.get(0)).unwrap();
        assert_eq!(calls, 8);
        db.execute(PRUNE, [2]).unwrap();
        let last: u64 = db.query_row(LAST_TICK, [], |r| r.get(1)).unwrap();
        assert_eq!(last, 3);
        let count: u64 = db
            .query_row("SELECT COUNT(*) FROM gasp_sync_cost", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 2);
    }
}
