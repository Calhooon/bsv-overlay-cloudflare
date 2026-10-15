//! One day of route faults (engine 0.2.0, #57): each sync a route fault
//! ended, by peer and class (`overlay_engine::route_fault`). None of them
//! counted against the peer; this is where they are counted instead, so a
//! route that never heals is read on invariants (`gaspRouteFaults24h`).
//! One row per (topic, peer) per sync that met one, pruned past a day.
use crate::d1::Query;
use overlay_engine::engine::GASPSyncResult;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use worker::D1Database;

pub const CREATE: &str = "CREATE TABLE IF NOT EXISTS gasp_route_faults (id INTEGER PRIMARY KEY, at_ms INTEGER NOT NULL, source TEXT NOT NULL, topic TEXT NOT NULL, peer TEXT NOT NULL, class TEXT NOT NULL)";
pub const INDEX: &str =
    "CREATE INDEX IF NOT EXISTS gasp_route_faults_age ON gasp_route_faults(at_ms)";
pub const INSERT: &str =
    "INSERT INTO gasp_route_faults (at_ms, source, topic, peer, class) VALUES (?1, ?2, ?3, ?4, ?5)";
pub const PRUNE: &str = "DELETE FROM gasp_route_faults WHERE at_ms < ?1";
pub const COUNTS: &str = "SELECT peer, class, COUNT(*) AS faults FROM gasp_route_faults WHERE at_ms >= ?1 GROUP BY peer, class ORDER BY peer, class";
const DAY_MS: f64 = 86_400_000.0;

#[derive(Debug, Serialize, Deserialize)]
struct Count {
    peer: String,
    class: String,
    faults: u64,
}

/// Record the route faults of a sync, if it met any. Best-effort.
pub async fn record(db: &D1Database, source: &str, result: Option<&GASPSyncResult>) {
    let Some(result) = result else { return };
    let now = js_sys::Date::now();
    let mut wrote = false;
    for (topic, value) in &result.topics_synced {
        for fault in &value.route_faults {
            wrote = true;
            if let Err(error) = Query::new(INSERT)
                .bind(now)
                .bind(source)
                .bind(topic.as_str())
                .bind(fault.peer.as_str())
                .bind(fault.class.as_str())
                .execute(db)
                .await
            {
                worker::console_error!("GASP route fault save: {error}");
            }
        }
    }
    if wrote {
        if let Err(error) = Query::new(PRUNE).bind(now - DAY_MS).execute(db).await {
            worker::console_error!("GASP route fault prune: {error}");
        }
    }
}

/// `gaspRouteFaults24h`: the day's route faults, in all and by (peer, class).
pub async fn view(db: &D1Database) -> Value {
    match Query::new(COUNTS)
        .bind(js_sys::Date::now() - DAY_MS)
        .fetch_all::<Count>(db)
        .await
    {
        Ok(rows) => json!({
            "total": rows.iter().map(|r| r.faults).sum::<u64>(),
            "byPeerClass": rows,
        }),
        Err(error) => json!({"fault": error}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_057_the_day_counts_route_faults_by_peer_and_class() {
        let db = rusqlite::Connection::open_in_memory().unwrap();
        db.execute_batch(CREATE).unwrap();
        db.execute_batch(INDEX).unwrap();
        let day = 86_400_000i64;
        for (at, peer, class) in [
            (1, "https://old.example", "cf_1042"),
            (day + 1, "https://p.example", "cf_1042"),
            (day + 2, "https://p.example", "cf_1042"),
            (day + 3, "https://p.example", "dns"),
            (day + 4, "https://q.example", "tls"),
        ] {
            db.execute(
                INSERT,
                rusqlite::params![at, "tick", "tm_ship", peer, class],
            )
            .unwrap();
        }
        let rows: Vec<(String, String, u64)> = db
            .prepare(COUNTS)
            .unwrap()
            .query_map([2], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(
            rows,
            [
                ("https://p.example".into(), "cf_1042".into(), 2),
                ("https://p.example".into(), "dns".into(), 1),
                ("https://q.example".into(), "tls".into(), 1),
            ]
        );
        db.execute(PRUNE, [2]).unwrap();
        let left: u64 = db
            .query_row("SELECT COUNT(*) FROM gasp_route_faults", [], |r| r.get(0))
            .unwrap();
        assert_eq!(left, 4, "the row older than a day is pruned");
    }
}
