//! bsv-low #555: the DEFERRED GASP graphs (measured on beta as #582).
//!
//! A graph whose walk passes its per-graph budget (`Engine::set_graph_budget`,
//! [`GASP_GRAPH_BUDGET_CALLS`] calls or [`GASP_GRAPH_BUDGET_MS`] in one pass) is
//! deferred by the engine: its partial walk is kept as ONE row of
//! `gasp_deferred_graphs` per (peer, topic, root outpoint), REPLACED on every
//! deferral, deleted when the graph converges or is dropped, and resumed by the
//! next pass that is served its UTXO. This module holds the table, its three
//! statements (the `Storage` methods in `d1_storage.rs` run them), the health
//! block `/health/invariants.gasp.deferredGraphs` and the counters.

use serde::Deserialize;
use worker::D1Database;

use crate::d1::Query;

/// Calls one graph may make in one pass on this worker. The engine's default
/// ([`overlay_engine::gasp::DEFAULT_GRAPH_BUDGET_CALLS`]).
pub const GASP_GRAPH_BUDGET_CALLS: u32 = overlay_engine::gasp::DEFAULT_GRAPH_BUDGET_CALLS;

/// Wall-clock one graph may take in one pass on this worker, ms: half of the
/// 30 s per-peer budget (`GASP_PEER_SYNC_BUDGET_MS`), so a deep graph leaves
/// the peer's other UTXOs the other half. Under the per-peer budget, as
/// `Engine::set_graph_budget` asks.
pub const GASP_GRAPH_BUDGET_MS: u64 = 15_000;

/// The table: one row per deferred graph. `record` is the engine's
/// `DeferredGraph` as JSON (at most `DEFERRED_GRAPH_MAX_BYTES`, under D1's 2 MB
/// row); the other columns are its summary for the health block. Transient:
/// a lost row costs the walk again from its root, nothing else.
pub const DEFERRED_GRAPHS_CREATE: &str = "CREATE TABLE IF NOT EXISTS gasp_deferred_graphs (
        host TEXT NOT NULL,
        topic TEXT NOT NULL,
        outpoint TEXT NOT NULL,
        score REAL NOT NULL,
        nodes INTEGER NOT NULL,
        pending INTEGER NOT NULL,
        calls INTEGER NOT NULL,
        passes INTEGER NOT NULL,
        reason TEXT NOT NULL,
        bytes INTEGER NOT NULL,
        record TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        PRIMARY KEY (host, topic, outpoint)
    )";

/// Save (replace) one record. Binds: `?1` host, `?2` topic, `?3` outpoint,
/// `?4` score, `?5` nodes, `?6` pending, `?7` calls, `?8` passes, `?9`
/// reason, `?10` bytes, `?11` record. `created_at` is kept from the first
/// deferral (the age); the backend owns the clock.
pub const DEFERRED_GRAPH_UPSERT_SQL: &str = "INSERT INTO gasp_deferred_graphs \
     (host, topic, outpoint, score, nodes, pending, calls, passes, reason, bytes, record, created_at, updated_at) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, unixepoch(), unixepoch()) \
     ON CONFLICT(host, topic, outpoint) DO UPDATE SET \
       score = excluded.score, nodes = excluded.nodes, pending = excluded.pending, \
       calls = excluded.calls, passes = excluded.passes, reason = excluded.reason, \
       bytes = excluded.bytes, record = excluded.record, updated_at = unixepoch()";

/// The records of one (peer, topic), lowest score first.
pub const DEFERRED_GRAPHS_SELECT_SQL: &str = "SELECT record FROM gasp_deferred_graphs \
     WHERE host = ?1 AND topic = ?2 ORDER BY score, outpoint";

/// Delete one record (converged or dropped).
pub const DEFERRED_GRAPH_DELETE_SQL: &str = "DELETE FROM gasp_deferred_graphs \
     WHERE host = ?1 AND topic = ?2 AND outpoint = ?3";

/// The health block lists at most this many graphs, oldest first.
pub const HEALTH_LIST_MAX: u32 = 20;

const HEALTH_COUNT_SQL: &str = "SELECT COUNT(*) AS c FROM gasp_deferred_graphs";

const HEALTH_LIST_SQL: &str =
    "SELECT host, topic, outpoint, nodes, pending, calls, passes, reason, bytes, \
            (unixepoch() - created_at) AS ageSecs \
     FROM gasp_deferred_graphs ORDER BY created_at, outpoint LIMIT ?1";

/// Graphs deferred (a new record, or a resumed graph deferred again).
pub const COUNTER_GASP_GRAPH_DEFERRED: &str = "gasp_graph_deferred_total";
/// Graphs resumed from a record.
pub const COUNTER_GASP_GRAPH_RESUMED: &str = "gasp_graph_resumed_total";
/// Resumed graphs that completed and landed.
pub const COUNTER_GASP_GRAPH_CONVERGED: &str = "gasp_graph_converged_total";
/// Records dropped without converging, all reasons; each reason also has its
/// own counter, [`dropped_counter_name`].
pub const COUNTER_GASP_GRAPH_DROPPED: &str = "gasp_graph_dropped_total";

/// The counter of one drop reason: `gasp_graph_dropped_<reason>_total`.
pub fn dropped_counter_name(reason: &str) -> String {
    format!("gasp_graph_dropped_{reason}_total")
}

/// Every counter name this module serves, for the health block's zero seed.
pub fn counter_names() -> Vec<String> {
    let mut names = vec![
        COUNTER_GASP_GRAPH_DEFERRED.to_string(),
        COUNTER_GASP_GRAPH_RESUMED.to_string(),
        COUNTER_GASP_GRAPH_CONVERGED.to_string(),
        COUNTER_GASP_GRAPH_DROPPED.to_string(),
    ];
    names.extend(
        overlay_engine::gasp::DropReason::ALL
            .iter()
            .map(|r| dropped_counter_name(r.as_str())),
    );
    names
}

/// PURE: the counter bumps of one sync's results, `(name, delta)`, zeros left
/// out.
pub fn counter_deltas(
    results: &std::collections::HashMap<String, overlay_engine::engine::TopicSyncResult>,
) -> Vec<(String, u64)> {
    let mut deltas: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    for r in results.values() {
        *deltas
            .entry(COUNTER_GASP_GRAPH_DEFERRED.into())
            .or_default() += r.deferred_graphs;
        *deltas.entry(COUNTER_GASP_GRAPH_RESUMED.into()).or_default() += r.resumed_graphs;
        *deltas
            .entry(COUNTER_GASP_GRAPH_CONVERGED.into())
            .or_default() += r.converged_graphs;
        for d in &r.dropped_graphs {
            *deltas.entry(COUNTER_GASP_GRAPH_DROPPED.into()).or_default() += 1;
            *deltas.entry(dropped_counter_name(&d.reason)).or_default() += 1;
        }
    }
    deltas.into_iter().filter(|(_, v)| *v > 0).collect()
}

/// Bump the counters of one sync (best effort, as every counter).
pub async fn record_counters(
    db: &D1Database,
    results: &std::collections::HashMap<String, overlay_engine::engine::TopicSyncResult>,
) {
    for (name, delta) in counter_deltas(results) {
        crate::ops::bump_counter(db, &name, delta).await;
    }
}

/// One row of the health list.
#[derive(Debug, Clone, Deserialize)]
pub struct HealthRow {
    pub host: String,
    pub topic: String,
    pub outpoint: String,
    pub nodes: f64,
    pub pending: f64,
    pub calls: f64,
    pub passes: f64,
    pub reason: String,
    pub bytes: f64,
    #[serde(rename = "ageSecs")]
    pub age_secs: Option<f64>,
}

#[derive(Deserialize)]
struct CountRow {
    c: f64,
}

/// PURE: the health block from the count and the oldest rows.
pub fn health_view(count: u64, rows: &[HealthRow]) -> serde_json::Value {
    let n = |v: f64| v.max(0.0) as u64;
    let graphs: Vec<serde_json::Value> = rows
        .iter()
        .map(|r| {
            serde_json::json!({
                "topic": r.topic,
                "peer": r.host,
                "outpoint": r.outpoint,
                "nodes": n(r.nodes),
                "pending": n(r.pending),
                "calls": n(r.calls),
                "passes": n(r.passes),
                "reason": r.reason,
                "bytes": n(r.bytes),
                "ageSecs": r.age_secs.map(n),
            })
        })
        .collect();
    serde_json::json!({
        "readable": true,
        "count": count,
        "oldest": graphs.first().cloned(),
        "graphs": graphs,
        "listed": rows.len(),
        "budget": {
            "calls": GASP_GRAPH_BUDGET_CALLS,
            "ms": GASP_GRAPH_BUDGET_MS,
            "maxPasses": overlay_engine::gasp::DEFERRED_GRAPH_MAX_PASSES,
            "maxBytes": overlay_engine::gasp::DEFERRED_GRAPH_MAX_BYTES,
            "perPeerTopic": overlay_engine::gasp::DEFERRED_GRAPHS_PER_PEER_TOPIC,
        },
    })
}

/// `/health/invariants.gasp.deferredGraphs`: the count, the oldest, and each
/// of the oldest [`HEALTH_LIST_MAX`] graphs {topic, peer, outpoint, nodes,
/// pending, calls, passes, reason, bytes, ageSecs}. `readable: false` when
/// the table cannot be read (a pre-migration isolate), distinct from none.
pub async fn health_json(db: &D1Database) -> serde_json::Value {
    let Ok(count) = Query::new(HEALTH_COUNT_SQL)
        .fetch_optional::<CountRow>(db)
        .await
    else {
        return serde_json::json!({"readable": false});
    };
    let count = count.map_or(0, |r| r.c.max(0.0) as u64);
    let rows = if count > 0 {
        match Query::new(HEALTH_LIST_SQL)
            .bind(HEALTH_LIST_MAX)
            .fetch_all::<HealthRow>(db)
            .await
        {
            Ok(rows) => rows,
            Err(_) => return serde_json::json!({"readable": false, "count": count}),
        }
    } else {
        Vec::new()
    };
    health_view(count, &rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use overlay_engine::engine::{DroppedDeferral, TopicSyncResult};
    use overlay_engine::gasp::{DeferredGraph, PendingInput, WalkedNode};
    use overlay_engine::types::GASPNode;

    fn sqlite() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        for sql in crate::d1::OVERLAY_MIGRATIONS {
            if let Err(e) = conn.execute_batch(sql) {
                assert!(
                    e.to_string()
                        .to_ascii_lowercase()
                        .contains("duplicate column"),
                    "{e}\n{sql}"
                );
            }
        }
        conn
    }

    fn record(outpoint: &str, score: u64, nodes: usize, passes: u32) -> DeferredGraph {
        let node = GASPNode {
            graph_id: outpoint.into(),
            raw_tx: "00".into(),
            output_index: 0,
            proof: Some("ab".into()),
            tx_metadata: None,
            output_metadata: None,
            inputs: None,
        };
        DeferredGraph {
            peer: "https://peer".into(),
            topic: "tm_x".into(),
            outpoint: outpoint.into(),
            score,
            nodes: vec![
                WalkedNode {
                    node,
                    spent_by: None,
                };
                nodes
            ],
            pending: vec![PendingInput {
                outpoint: "aa.0".into(),
                graph_id: outpoint.into(),
                metadata: false,
                spent_by: Some("bb.0".into()),
                parent_proven: false,
            }],
            calls: nodes as u64,
            passes,
            reason: "calls".into(),
        }
    }

    fn upsert(conn: &rusqlite::Connection, r: &DeferredGraph) {
        let json = serde_json::to_string(r).unwrap();
        conn.execute(
            DEFERRED_GRAPH_UPSERT_SQL,
            rusqlite::params![
                r.peer,
                r.topic,
                r.outpoint,
                r.score as f64,
                r.nodes.len() as i64,
                r.pending.len() as i64,
                r.calls as i64,
                r.passes as i64,
                r.reason,
                json.len() as i64,
                json
            ],
        )
        .unwrap();
    }

    fn select(conn: &rusqlite::Connection) -> Vec<DeferredGraph> {
        let mut stmt = conn.prepare(DEFERRED_GRAPHS_SELECT_SQL).unwrap();
        stmt.query_map(rusqlite::params!["https://peer", "tm_x"], |row| {
            row.get::<_, String>(0)
        })
        .unwrap()
        .map(|j| serde_json::from_str(&j.unwrap()).unwrap())
        .collect()
    }

    /// bsv-low #555: the shipped statements under real SQLite over the shipped
    /// migrations: one row per graph REPLACED (never appended), its first
    /// `created_at` kept, the records read back whole lowest score first, a
    /// delete of one leaving the other, and the health list's columns.
    #[test]
    fn e555_the_shipped_statements_replace_one_row_per_graph() {
        let conn = sqlite();
        upsert(&conn, &record("b.0", 2, 1, 1));
        upsert(&conn, &record("a.0", 1, 3, 1));
        conn.execute(
            "UPDATE gasp_deferred_graphs SET created_at = created_at - 600 WHERE outpoint = 'a.0'",
            [],
        )
        .unwrap();
        upsert(&conn, &record("a.0", 1, 6, 2));
        let count: i64 = conn.query_row(HEALTH_COUNT_SQL, [], |r| r.get(0)).unwrap();
        assert_eq!(count, 2, "replaced, never appended");
        let back = select(&conn);
        assert_eq!(
            back.iter()
                .map(|r| (r.outpoint.as_str(), r.nodes.len(), r.passes))
                .collect::<Vec<_>>(),
            vec![("a.0", 6, 2), ("b.0", 1, 1)]
        );
        assert_eq!(back[0].pending[0].spent_by.as_deref(), Some("bb.0"));
        let (outpoint, nodes, passes, age): (String, i64, i64, i64) = conn
            .query_row(HEALTH_LIST_SQL, [HEALTH_LIST_MAX], |r| {
                Ok((r.get(2)?, r.get(3)?, r.get(6)?, r.get(9)?))
            })
            .unwrap();
        assert_eq!((outpoint.as_str(), nodes, passes), ("a.0", 6, 2));
        assert!(
            (600..610).contains(&age),
            "the age is the first deferral's: {age}"
        );
        conn.execute(
            DEFERRED_GRAPH_DELETE_SQL,
            rusqlite::params!["https://peer", "tm_x", "a.0"],
        )
        .unwrap();
        assert_eq!(
            select(&conn)
                .iter()
                .map(|r| r.outpoint.clone())
                .collect::<Vec<_>>(),
            vec!["b.0".to_string()]
        );
    }

    /// bsv-low #555: the health block names the count, the oldest and each
    /// graph {topic, peer, outpoint, nodes, pending, calls, passes, reason,
    /// bytes, ageSecs}, with the budget it runs under.
    #[test]
    fn e555_the_health_block_names_each_deferred_graph() {
        let row = HealthRow {
            host: "https://peer".into(),
            topic: "tm_x".into(),
            outpoint: "a.0".into(),
            nodes: 40.0,
            pending: 3.0,
            calls: 120.0,
            passes: 3.0,
            reason: "time".into(),
            bytes: 51_000.0,
            age_secs: Some(180.0),
        };
        let v = health_view(1, &[row]);
        assert_eq!(v["readable"], true);
        assert_eq!(v["count"], 1);
        assert_eq!(v["oldest"]["outpoint"], "a.0");
        assert_eq!(
            v["graphs"][0],
            serde_json::json!({"topic": "tm_x", "peer": "https://peer", "outpoint": "a.0",
                "nodes": 40, "pending": 3, "calls": 120, "passes": 3, "reason": "time",
                "bytes": 51000, "ageSecs": 180})
        );
        assert_eq!(v["budget"]["calls"], GASP_GRAPH_BUDGET_CALLS);
        assert_eq!(v["budget"]["ms"], GASP_GRAPH_BUDGET_MS);
        let none = health_view(0, &[]);
        assert_eq!(
            (none["count"].clone(), none["oldest"].clone()),
            (serde_json::json!(0), serde_json::Value::Null)
        );
    }

    /// bsv-low #555: the counters of a sync, summed over topics, a drop
    /// counted in all and under its reason; every name seeded.
    #[test]
    fn e555_the_counters_sum_a_sync_and_count_drops_by_reason() {
        let topic = |deferred, resumed, converged, reasons: &[&str]| TopicSyncResult {
            peers: vec![],
            sync_type: "peers".into(),
            errors: vec![],
            pruned_inputs: 0,
            discarded_graphs: 0,
            finalized_graphs: 0,
            deadline_dropped_graphs: 0,
            cursor_moves: vec![],
            deferred_graphs: deferred,
            resumed_graphs: resumed,
            converged_graphs: converged,
            dropped_graphs: reasons
                .iter()
                .map(|r| DroppedDeferral {
                    peer: "p".into(),
                    outpoint: "o".into(),
                    reason: (*r).into(),
                })
                .collect(),
        };
        let results = std::collections::HashMap::from([
            ("tm_a".to_string(), topic(2, 1, 0, &["max_passes"])),
            (
                "tm_b".to_string(),
                topic(0, 1, 1, &["not_served", "max_passes"]),
            ),
        ]);
        assert_eq!(
            counter_deltas(&results),
            vec![
                ("gasp_graph_converged_total".to_string(), 1),
                ("gasp_graph_deferred_total".to_string(), 2),
                ("gasp_graph_dropped_max_passes_total".to_string(), 2),
                ("gasp_graph_dropped_not_served_total".to_string(), 1),
                ("gasp_graph_dropped_total".to_string(), 3),
                ("gasp_graph_resumed_total".to_string(), 2),
            ]
        );
        let names = counter_names();
        assert_eq!(names.len(), 4 + 9);
        assert!(names.contains(&"gasp_graph_dropped_root_proven_total".to_string()));
    }
}
