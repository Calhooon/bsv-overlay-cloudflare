//! One-day speed readings, with per-topic and per-peer detail from completed slices.
use crate::d1::Query;
use overlay_engine::{engine::GASPSyncResult, meter::SyncCost};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use worker::D1Database;

pub const CREATE: &str = "CREATE TABLE IF NOT EXISTS gasp_sync_speed (id INTEGER PRIMARY KEY, ended_ms INTEGER NOT NULL, source TEXT NOT NULL, outputs_walked INTEGER NOT NULL, ms INTEGER NOT NULL, detail TEXT NOT NULL)";
pub const INDEX: &str =
    "CREATE INDEX IF NOT EXISTS gasp_sync_speed_age ON gasp_sync_speed(ended_ms)";
pub const INSERT: &str = "INSERT INTO gasp_sync_speed (ended_ms, source, outputs_walked, ms, detail) VALUES (?1, ?2, ?3, ?4, ?5)";
pub const PRUNE: &str = "DELETE FROM gasp_sync_speed WHERE ended_ms < ?1 AND id != (SELECT MAX(id) FROM gasp_sync_speed)";
pub const LAST: &str = "SELECT ended_ms, source, outputs_walked, ms, detail FROM gasp_sync_speed ORDER BY id DESC LIMIT 1";
pub const TOTAL: &str = "SELECT COALESCE(SUM(outputs_walked), 0) AS outputs_walked, COALESCE(SUM(ms), 0) AS ms FROM gasp_sync_speed WHERE ended_ms >= ?1";
pub const GROUPED: &str = "SELECT json_extract(j.value, '$.topic') AS topic, json_extract(j.value, '$.peer') AS peer, SUM(json_extract(j.value, '$.outputs_walked')) AS outputs_walked, SUM(json_extract(j.value, '$.ms')) AS ms FROM gasp_sync_speed s, json_each(s.detail) j WHERE s.ended_ms >= ?1 GROUP BY topic, peer ORDER BY topic, peer";
const DAY_MS: f64 = 86_400_000.0;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Speed {
    pub outputs_walked: u64,
    pub ms: u64,
    #[serde(default)]
    pub ms_per_1000_outputs: Option<u64>,
}

impl Speed {
    fn from_cost(cost: SyncCost) -> Self {
        Self {
            outputs_walked: cost.outputs_walked,
            ms: cost.ms,
            ms_per_1000_outputs: cost.ms_per_1000_outputs,
        }
    }
    fn calculate(&mut self) {
        let mut cost = SyncCost {
            outputs_walked: self.outputs_walked,
            ms: self.ms,
            ..SyncCost::default()
        };
        cost.refresh_speed();
        self.ms_per_1000_outputs = cost.ms_per_1000_outputs;
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Detail {
    topic: String,
    peer: Option<String>,
    #[serde(flatten)]
    speed: Speed,
}

fn details(result: Option<&GASPSyncResult>) -> Vec<Detail> {
    let mut rows = Vec::new();
    if let Some(result) = result {
        for (topic, value) in &result.topics_synced {
            rows.push(Detail {
                topic: topic.clone(),
                peer: None,
                speed: Speed::from_cost(value.cost),
            });
            for (peer, value) in &value.peer_results {
                rows.push(Detail {
                    topic: topic.clone(),
                    peer: Some(peer.clone()),
                    speed: Speed::from_cost(value.cost),
                });
            }
        }
    }
    rows.sort_by(|a, b| (&a.topic, &a.peer).cmp(&(&b.topic, &b.peer)));
    rows
}

pub async fn record(
    db: &D1Database,
    source: &str,
    cost: SyncCost,
    result: Option<&GASPSyncResult>,
) {
    let now = js_sys::Date::now();
    let detail = match serde_json::to_string(&details(result)) {
        Ok(value) => value,
        Err(error) => {
            worker::console_error!("GASP speed detail: {error}");
            return;
        }
    };
    if let Err(error) = Query::new(INSERT)
        .bind(now)
        .bind(source)
        .bind(cost.outputs_walked)
        .bind(cost.ms)
        .bind(detail.as_str())
        .execute(db)
        .await
    {
        worker::console_error!("GASP speed save: {error}");
    }
    if let Err(error) = Query::new(PRUNE).bind(now - DAY_MS).execute(db).await {
        worker::console_error!("GASP speed prune: {error}");
    }
}

#[derive(Deserialize)]
struct Last {
    ended_ms: f64,
    source: String,
    outputs_walked: u64,
    ms: u64,
    detail: String,
}

pub async fn view(db: &D1Database) -> Value {
    let since = js_sys::Date::now() - DAY_MS;
    let total = Query::new(TOTAL)
        .bind(since)
        .fetch_optional::<Speed>(db)
        .await;
    let last = Query::new(LAST).fetch_optional::<Last>(db).await;
    let grouped = Query::new(GROUPED)
        .bind(since)
        .fetch_all::<Detail>(db)
        .await;
    let (Ok(total), Ok(last), Ok(mut grouped)) = (total, last, grouped) else {
        return json!({"fault": true});
    };
    let mut total = total.unwrap_or_default();
    total.calculate();
    for row in &mut grouped {
        row.speed.calculate();
    }
    let last = match last {
        None => Value::Null,
        Some(last) => {
            let Ok(detail) = serde_json::from_str::<Vec<Detail>>(&last.detail) else {
                return json!({"fault": true});
            };
            let mut speed = Speed {
                outputs_walked: last.outputs_walked,
                ms: last.ms,
                ..Speed::default()
            };
            speed.calculate();
            json!({"ended_ms": last.ended_ms, "source": last.source, "speed": speed, "byTopicPeer": detail})
        }
    };
    json!({"fault": false, "lastSync": last, "totals24h": total, "byTopicPeer24h": grouped})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minutes_0113_speed_sql_keeps_last_and_uses_weighted_sums() {
        let db = rusqlite::Connection::open_in_memory().unwrap();
        db.execute_batch(CREATE).unwrap();
        db.execute_batch(INDEX).unwrap();
        for (at, outputs, ms) in [(1, 1000, 1000), (86_400_001, 4, 250), (86_400_002, 6, 50)] {
            let detail = json!([{"topic":"tm_x", "peer":"https://peer.example", "outputs_walked":outputs, "ms":ms}]);
            db.execute(
                INSERT,
                rusqlite::params![at, "admin", outputs, ms, detail.to_string()],
            )
            .unwrap();
        }
        let mut total = db
            .query_row(TOTAL, [2], |r| {
                Ok(Speed {
                    outputs_walked: r.get(0)?,
                    ms: r.get(1)?,
                    ..Speed::default()
                })
            })
            .unwrap();
        total.calculate();
        assert_eq!(
            (total.outputs_walked, total.ms, total.ms_per_1000_outputs),
            (10, 300, Some(30_000))
        );
        let grouped: (String, String, u64, u64) = db
            .query_row(GROUPED, [2], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
            })
            .unwrap();
        assert_eq!(
            grouped,
            ("tm_x".into(), "https://peer.example".into(), 10, 300)
        );
        db.execute(PRUNE, [86_400_003u64]).unwrap();
        let last: (u64, u64) = db
            .query_row(LAST, [], |r| Ok((r.get(2)?, r.get(3)?)))
            .unwrap();
        assert_eq!(last, (6, 50));
        let count: u64 = db
            .query_row("SELECT COUNT(*) FROM gasp_sync_speed", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }
}
