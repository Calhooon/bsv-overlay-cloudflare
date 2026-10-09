//! bsv-low #555: the DEFERRED GASP graphs (measured on beta as #582).
//!
//! A graph whose walk passes its per-graph budget (`Engine::set_graph_budget`,
//! [`GASP_GRAPH_BUDGET_CALLS`] calls or [`GASP_GRAPH_BUDGET_MS`] in one pass;
//! since bsv-low #586 [`GASP_GRAPH_BUDGET_BYTES`] served or
//! [`GASP_GRAPH_BUDGET_NODES`] appended, `Engine::set_graph_budget_limbs`) is
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

/// Bytes one graph may be SERVED in one pass on this worker (bsv-low #586):
/// the hex of each node's raw transaction and proof, as the peer sends it and
/// a record keeps it. The engine's default
/// ([`overlay_engine::gasp::DEFAULT_GRAPH_BUDGET_BYTES`], 4 MiB). Reached, the
/// graph is DEFERRED as at the calls (reason `bytes`), never dropped or
/// refused: a budget per pass, not a limit.
pub const GASP_GRAPH_BUDGET_BYTES: u64 = overlay_engine::gasp::DEFAULT_GRAPH_BUDGET_BYTES;

/// Nodes one graph may APPEND in one pass on this worker (bsv-low #586). The
/// engine's default ([`overlay_engine::gasp::DEFAULT_GRAPH_BUDGET_NODES`],
/// 64). Reached, the graph is deferred (reason `nodes`).
pub const GASP_GRAPH_BUDGET_NODES: u32 = overlay_engine::gasp::DEFAULT_GRAPH_BUDGET_NODES;

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

/// The most rows the table holds over EVERY peer and topic (bsv-low #555, the
/// lens fold's M3). The engine bounds records per (peer, topic) at 16, and SHIP
/// mode takes its peers from the permissionless `ls_ship`: a stranger who
/// advertises many hosts could fill the one D1 the overlay shares with the app
/// layer (about 1.4 GB a day by the lens's count). A NEW key past this is
/// refused in the upsert itself ([`DEFERRED_GRAPH_UPSERT_SQL`]), counted
/// `too_many`, and its walk goes on under the per-peer budget alone.
pub const DEFERRED_GRAPHS_MAX_ROWS: u32 = 256;

/// The most bytes of `record` the table holds over every row (64 MiB): a save,
/// new or a replacement, that would take the sum past it is refused, as
/// [`DEFERRED_GRAPHS_MAX_ROWS`]. The measured picture graph is about 0.5 MiB.
pub const DEFERRED_GRAPHS_MAX_TOTAL_BYTES: u64 = 64 << 20;

/// The most rows ONE host holds over all its topics (bsv-low #555, the delta
/// fold's D-M1): an eighth of [`DEFERRED_GRAPHS_MAX_ROWS`]. The ceiling was
/// one pool over every peer, and four (host, topic) pairs of a stranger's
/// hosts held all of it; every honest deferral after them was refused
/// (`too_many`) and walked from its root on every tick, the case #555 was
/// built for. A NEW key past it is refused in the upsert, as the global
/// bounds. A "host" is the peer's NORMALIZED ORIGIN (`origin`,
/// `overlay_engine::gasp::peer_origin`; the delta-2 fold's D2-M2): keyed on
/// the URL string, eight adverts of one server (`/?1` .. `/?8`) were eight
/// hosts and filled the ceiling. Applies to DISCOVERED peers only: a
/// configured peer's save is checked against the global bounds alone.
pub const DEFERRED_GRAPHS_MAX_ROWS_PER_HOST: u32 = DEFERRED_GRAPHS_MAX_ROWS / 8;

/// The most rows the records of DISCOVERED peers (`ls_ship`, `configured =
/// 0`) hold together: half of [`DEFERRED_GRAPHS_MAX_ROWS`] (bsv-low #555, the
/// delta-2 fold's D2-M2). The other half is RESERVED for configured peers
/// (`SyncTarget::Peers`), whose saves are checked against the global bounds
/// only: a stranger's adverts, any number of hosts and spellings, no longer
/// refuse a configured peer's deferral (#582's picture graph back to the
/// pre-#555 walk). A record costs a FAST peer only the `calls` budget (100
/// tiny responses, milliseconds), not the 15 s.
pub const DEFERRED_GRAPHS_DISCOVERED_MAX_ROWS: u32 = DEFERRED_GRAPHS_MAX_ROWS / 2;

/// The most bytes of `record` ONE host holds over all its topics (8 MiB, an
/// eighth of [`DEFERRED_GRAPHS_MAX_TOTAL_BYTES`]; about sixteen of the
/// measured picture graph). A save past it is refused, as the global bound.
pub const DEFERRED_GRAPHS_MAX_BYTES_PER_HOST: u64 = DEFERRED_GRAPHS_MAX_TOTAL_BYTES / 8;

/// The most bytes the records of DISCOVERED peers hold together: half of
/// [`DEFERRED_GRAPHS_MAX_TOTAL_BYTES`] (32 MiB), as
/// [`DEFERRED_GRAPHS_DISCOVERED_MAX_ROWS`].
pub const DEFERRED_GRAPHS_DISCOVERED_MAX_BYTES: u64 = DEFERRED_GRAPHS_MAX_TOTAL_BYTES / 2;

/// A row not written for this long is swept by the cron
/// ([`DEFERRED_GRAPHS_SWEEP_SQL`], counted `gasp_graph_dropped_stale_total`):
/// twice `DEFERRED_GRAPH_MAX_PASSES` (60) at the `*/15` cadence of all three
/// configs, 30 h. A live record is rewritten on every pass that resumes it and
/// is dropped by the engine at 60 passes; twice that covers a record HELD BACK
/// behind another's resumes for that one's whole life (the lens fold's M1). A
/// row past it belongs to a peer that never finishes a sync again (dark,
/// quarantined, its advert revoked), which nothing else would ever delete.
pub const DEFERRED_GRAPH_STALE_SECS: u64 =
    2 * overlay_engine::gasp::DEFERRED_GRAPH_MAX_PASSES as u64 * 15 * 60;

/// Save (replace) one record, under the table's global ceiling. Binds: `?1`
/// host (the peer URL, the key), `?2` topic, `?3` outpoint, `?4` score, `?5`
/// nodes, `?6` pending, `?7` calls, `?8` passes, `?9` reason, `?10` bytes,
/// `?11` record, `?12` [`DEFERRED_GRAPHS_MAX_ROWS`], `?13`
/// [`DEFERRED_GRAPHS_MAX_TOTAL_BYTES`], `?14`
/// [`DEFERRED_GRAPHS_MAX_ROWS_PER_HOST`], `?15`
/// [`DEFERRED_GRAPHS_MAX_BYTES_PER_HOST`], `?16` origin
/// (`overlay_engine::gasp::peer_origin` of the host), `?17` configured
/// (1/0), `?18` [`DEFERRED_GRAPHS_DISCOVERED_MAX_ROWS`], `?19`
/// [`DEFERRED_GRAPHS_DISCOVERED_MAX_BYTES`]. `created_at` is kept from the
/// first deferral (the age); the backend owns the clock. The `WHERE` of the
/// `SELECT` re-reads the ceiling in the one statement (as #576's `PARK_SQL`):
/// a held key always replaces within the byte bounds, a new key only under
/// the row bounds too. A CONFIGURED peer's save is checked against the
/// global bounds only; a DISCOVERED peer's also against its origin's share
/// and the discovered half (the delta-2 fold's D2-M2). A refused save
/// returns NO row (`AtCeiling`).
pub const DEFERRED_GRAPH_UPSERT_SQL: &str = "INSERT INTO gasp_deferred_graphs \
     (host, topic, outpoint, score, nodes, pending, calls, passes, reason, bytes, record, created_at, updated_at, \
      origin, configured) \
     SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, unixepoch(), unixepoch(), ?16, ?17 \
     WHERE (EXISTS (SELECT 1 FROM gasp_deferred_graphs WHERE host = ?1 AND topic = ?2 AND outpoint = ?3) \
       OR ((SELECT COUNT(*) FROM gasp_deferred_graphs) < ?12 \
         AND (?17 OR ((SELECT COUNT(*) FROM gasp_deferred_graphs WHERE origin = ?16 AND configured = 0) < ?14 \
           AND (SELECT COUNT(*) FROM gasp_deferred_graphs WHERE configured = 0) < ?18)))) \
     AND (SELECT COALESCE(SUM(bytes), 0) FROM gasp_deferred_graphs \
       WHERE NOT (host = ?1 AND topic = ?2 AND outpoint = ?3)) + ?10 <= ?13 \
     AND (?17 OR ((SELECT COALESCE(SUM(bytes), 0) FROM gasp_deferred_graphs \
         WHERE origin = ?16 AND configured = 0 AND NOT (host = ?1 AND topic = ?2 AND outpoint = ?3)) + ?10 <= ?15 \
       AND (SELECT COALESCE(SUM(bytes), 0) FROM gasp_deferred_graphs \
         WHERE configured = 0 AND NOT (host = ?1 AND topic = ?2 AND outpoint = ?3)) + ?10 <= ?19)) \
     ON CONFLICT(host, topic, outpoint) DO UPDATE SET \
       score = excluded.score, nodes = excluded.nodes, pending = excluded.pending, \
       calls = excluded.calls, passes = excluded.passes, reason = excluded.reason, \
       bytes = excluded.bytes, record = excluded.record, updated_at = unixepoch(), \
       origin = excluded.origin, configured = excluded.configured \
     RETURNING outpoint";

/// The KEYS of one (peer, topic)'s records, lowest score first: a sync reads
/// these up front, and each record only when its UTXO is served (the lens
/// fold's L3: the whole records of a (peer, topic), up to 16 MiB, were read
/// in one result).
pub const DEFERRED_GRAPHS_SELECT_SQL: &str = "SELECT outpoint, score FROM gasp_deferred_graphs \
     WHERE host = ?1 AND topic = ?2 ORDER BY score, outpoint";

/// One record.
pub const DEFERRED_GRAPH_GET_SQL: &str = "SELECT record FROM gasp_deferred_graphs \
     WHERE host = ?1 AND topic = ?2 AND outpoint = ?3";

/// Delete one record (converged or dropped).
pub const DEFERRED_GRAPH_DELETE_SQL: &str = "DELETE FROM gasp_deferred_graphs \
     WHERE host = ?1 AND topic = ?2 AND outpoint = ?3";

/// The cron's sweep of stale rows. Binds: `?1` [`DEFERRED_GRAPH_STALE_SECS`].
pub const DEFERRED_GRAPHS_SWEEP_SQL: &str = "DELETE FROM gasp_deferred_graphs \
     WHERE updated_at < unixepoch() - ?1 RETURNING host, topic, outpoint";

/// Rows swept for age: also counted in [`COUNTER_GASP_GRAPH_DROPPED`].
pub const COUNTER_GASP_GRAPH_DROPPED_STALE: &str = "gasp_graph_dropped_stale_total";

/// Sweep the rows older than [`DEFERRED_GRAPH_STALE_SECS`] (the cron, before
/// its GASP sync): each logged, counted `stale` and in all drops. Best effort.
pub async fn sweep_stale(db: &D1Database) {
    #[derive(Deserialize)]
    struct Swept {
        host: String,
        topic: String,
        outpoint: String,
    }
    match Query::new(DEFERRED_GRAPHS_SWEEP_SQL)
        .bind(DEFERRED_GRAPH_STALE_SECS as f64)
        .fetch_all::<Swept>(db)
        .await
    {
        Ok(rows) if !rows.is_empty() => {
            for r in &rows {
                worker::console_log!(
                    "Scheduled: GASP deferred graph {} of {} for {} SWEPT (stale past {} s, bsv-low #555)",
                    r.outpoint,
                    r.host,
                    r.topic,
                    DEFERRED_GRAPH_STALE_SECS
                );
            }
            let n = rows.len() as u64;
            crate::ops::bump_counter(db, COUNTER_GASP_GRAPH_DROPPED_STALE, n).await;
            crate::ops::bump_counter(db, COUNTER_GASP_GRAPH_DROPPED, n).await;
        }
        Ok(_) => {}
        Err(e) => worker::console_log!("Scheduled: GASP deferred graph sweep failed: {e}"),
    }
}

/// The health block lists at most this many graphs, oldest first.
pub const HEALTH_LIST_MAX: u32 = 20;

const HEALTH_COUNT_SQL: &str =
    "SELECT COUNT(*) AS c, COALESCE(SUM(bytes), 0) AS b FROM gasp_deferred_graphs";

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
        COUNTER_GASP_GRAPH_DROPPED_STALE.to_string(),
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
    b: f64,
}

/// PURE: the health block from the count, the bytes held and the oldest rows.
pub fn health_view(count: u64, total_bytes: u64, rows: &[HealthRow]) -> serde_json::Value {
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
        "totalBytes": total_bytes,
        "oldest": graphs.first().cloned(),
        "graphs": graphs,
        "listed": rows.len(),
        "budget": {
            "calls": GASP_GRAPH_BUDGET_CALLS,
            "ms": GASP_GRAPH_BUDGET_MS,
            "bytesFetched": GASP_GRAPH_BUDGET_BYTES,
            "nodes": GASP_GRAPH_BUDGET_NODES,
            "maxPasses": overlay_engine::gasp::DEFERRED_GRAPH_MAX_PASSES,
            "maxBytes": overlay_engine::gasp::DEFERRED_GRAPH_MAX_BYTES,
            "perPeerTopic": overlay_engine::gasp::DEFERRED_GRAPHS_PER_PEER_TOPIC,
            "maxRows": DEFERRED_GRAPHS_MAX_ROWS,
            "maxTotalBytes": DEFERRED_GRAPHS_MAX_TOTAL_BYTES,
            "maxRowsPerHost": DEFERRED_GRAPHS_MAX_ROWS_PER_HOST,
            "maxBytesPerHost": DEFERRED_GRAPHS_MAX_BYTES_PER_HOST,
            "discoveredMaxRows": DEFERRED_GRAPHS_DISCOVERED_MAX_ROWS,
            "discoveredMaxBytes": DEFERRED_GRAPHS_DISCOVERED_MAX_BYTES,
            "staleSecs": DEFERRED_GRAPH_STALE_SECS,
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
    let (count, total_bytes) = count.map_or((0, 0), |r| (r.c.max(0.0) as u64, r.b.max(0.0) as u64));
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
    health_view(count, total_bytes, &rows)
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
            configured: false,
            idle_faults: 0,
        }
    }

    /// The shipped upsert as `D1Storage::put_deferred_graph` binds it, under
    /// the given ceiling: `true` = saved (a row returned).
    fn upsert_under(
        conn: &rusqlite::Connection,
        r: &DeferredGraph,
        max_rows: u32,
        max_bytes: u64,
    ) -> bool {
        upsert_shared(conn, r, max_rows, max_bytes, max_rows, max_bytes)
    }

    /// The same, with the per-host share too.
    fn upsert_shared(
        conn: &rusqlite::Connection,
        r: &DeferredGraph,
        max_rows: u32,
        max_bytes: u64,
        host_rows: u32,
        host_bytes: u64,
    ) -> bool {
        upsert_reserved(
            conn, r, max_rows, max_bytes, host_rows, host_bytes, max_rows, max_bytes,
        )
    }

    /// The same, with the discovered peers' half too (the delta-2 fold's
    /// D2-M2), bound as `D1Storage::put_deferred_graph` binds it.
    #[allow(clippy::too_many_arguments)]
    fn upsert_reserved(
        conn: &rusqlite::Connection,
        r: &DeferredGraph,
        max_rows: u32,
        max_bytes: u64,
        host_rows: u32,
        host_bytes: u64,
        discovered_rows: u32,
        discovered_bytes: u64,
    ) -> bool {
        let json = serde_json::to_string(r).unwrap();
        let mut stmt = conn.prepare(DEFERRED_GRAPH_UPSERT_SQL).unwrap();
        let mut rows = stmt
            .query(rusqlite::params![
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
                json,
                max_rows,
                max_bytes as i64,
                host_rows,
                host_bytes as i64,
                overlay_engine::gasp::peer_origin(&r.peer),
                r.configured,
                discovered_rows,
                discovered_bytes as i64
            ])
            .unwrap();
        rows.next().unwrap().is_some()
    }

    fn upsert(conn: &rusqlite::Connection, r: &DeferredGraph) {
        assert!(upsert_shared(
            conn,
            r,
            DEFERRED_GRAPHS_MAX_ROWS,
            DEFERRED_GRAPHS_MAX_TOTAL_BYTES,
            DEFERRED_GRAPHS_MAX_ROWS_PER_HOST,
            DEFERRED_GRAPHS_MAX_BYTES_PER_HOST
        ));
    }

    /// The keys, then each record by the get statement.
    fn select(conn: &rusqlite::Connection) -> Vec<DeferredGraph> {
        let mut stmt = conn.prepare(DEFERRED_GRAPHS_SELECT_SQL).unwrap();
        let keys: Vec<String> = stmt
            .query_map(rusqlite::params!["https://peer", "tm_x"], |row| {
                row.get::<_, String>(0)
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();
        keys.iter()
            .map(|k| {
                let json: String = conn
                    .query_row(
                        DEFERRED_GRAPH_GET_SQL,
                        rusqlite::params!["https://peer", "tm_x", k],
                        |row| row.get(0),
                    )
                    .unwrap();
                serde_json::from_str(&json).unwrap()
            })
            .collect()
    }

    fn rows(conn: &rusqlite::Connection) -> Vec<String> {
        let mut stmt = conn
            .prepare("SELECT outpoint FROM gasp_deferred_graphs ORDER BY outpoint")
            .unwrap();
        stmt.query_map([], |row| row.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    /// bsv-low #555, the lens fold's M3: the global ceiling is read in the
    /// upsert itself. Under a 3-row bound a fourth NEW key returns no row
    /// (`AtCeiling`) and writes nothing; a held key still replaces; under a
    /// byte bound a new key and a replacement that would pass it are both
    /// refused, the held row unchanged. On 03e1e17 the upsert had no ceiling
    /// (it wrote every row and returned none).
    #[test]
    fn e555f_m3_the_upsert_refuses_past_the_global_ceiling() {
        let conn = sqlite();
        for (i, o) in ["a.0", "b.0", "c.0"].iter().enumerate() {
            assert!(upsert_under(&conn, &record(o, i as u64, 1, 1), 3, 1 << 20));
        }
        assert!(
            !upsert_under(&conn, &record("d.0", 9, 1, 1), 3, 1 << 20),
            "a 4th key"
        );
        assert_eq!(rows(&conn), ["a.0", "b.0", "c.0"]);
        assert!(
            upsert_under(&conn, &record("b.0", 1, 5, 2), 3, 1 << 20),
            "held: replaced"
        );
        assert_eq!(select(&conn)[1].passes, 2);
        // Bytes: what the three hold now, plus one more small record, is the bound.
        let held: i64 = conn
            .query_row("SELECT SUM(bytes) FROM gasp_deferred_graphs", [], |r| {
                r.get(0)
            })
            .unwrap();
        let small = serde_json::to_string(&record("e.0", 9, 1, 1))
            .unwrap()
            .len() as u64;
        let bound = held as u64 + small;
        assert!(
            !upsert_under(&conn, &record("f.0", 9, 9, 1), 9, bound),
            "too many bytes"
        );
        assert!(
            !upsert_under(&conn, &record("a.0", 0, 9, 2), 9, held as u64),
            "a growth past it"
        );
        assert_eq!(select(&conn)[0].nodes.len(), 1, "the held row unchanged");
        assert!(
            upsert_under(&conn, &record("e.0", 9, 1, 1), 9, bound),
            "exactly at the bound"
        );
        assert_eq!(rows(&conn), ["a.0", "b.0", "c.0", "e.0"]);
        // The shipped bounds.
        assert_eq!(
            (DEFERRED_GRAPHS_MAX_ROWS, DEFERRED_GRAPHS_MAX_TOTAL_BYTES),
            (256, 64 << 20)
        );
    }

    /// bsv-low #555, the delta fold's D-M1: the ceiling is SHARED by host. A
    /// stranger's host at its share (2 rows here, over two topics) is refused
    /// a new key and a growth past its byte share, and still replaces a held
    /// key within it; another host still saves, up to the global bounds. On
    /// 0974be5 the upsert had no per-host bound: one host could take the
    /// whole ceiling and every other host's deferral was refused.
    #[test]
    fn e555d_m1_the_upsert_shares_the_ceiling_by_host() {
        let conn = sqlite();
        let on = |host: &str, topic: &str, o: &str, nodes: usize, bytes: u64| {
            let mut r = record(o, 1, nodes, 1);
            r.peer = host.into();
            r.topic = topic.into();
            upsert_shared(&conn, &r, 5, 1 << 20, 2, bytes)
        };
        assert!(on("https://stranger", "tm_x", "s1.0", 1, 1 << 20));
        assert!(on("https://stranger", "tm_y", "s2.0", 1, 1 << 20));
        assert!(
            !on("https://stranger", "tm_z", "s3.0", 1, 1 << 20),
            "past its rows"
        );
        assert!(
            on("https://stranger", "tm_x", "s1.0", 2, 1 << 20),
            "held: replaced"
        );
        let one = serde_json::to_string(&record("s1.0", 1, 2, 1))
            .unwrap()
            .len() as u64;
        let two = serde_json::to_string(&record("s2.0", 1, 1, 1))
            .unwrap()
            .len() as u64;
        assert!(
            !on("https://stranger", "tm_y", "s2.0", 9, one + two),
            "a growth past its bytes"
        );
        assert!(
            on("https://honest", "tm_x", "h1.0", 1, 1 << 20),
            "another host saves"
        );
        assert!(on("https://honest", "tm_x", "h2.0", 1, 1 << 20));
        assert!(on("https://third", "tm_x", "t1.0", 1, 1 << 20));
        assert!(
            !on("https://fourth", "tm_x", "f1.0", 1, 1 << 20),
            "the global rows"
        );
        assert_eq!(rows(&conn), ["h1.0", "h2.0", "s1.0", "s2.0", "t1.0"]);
        assert_eq!(
            (
                DEFERRED_GRAPHS_MAX_ROWS_PER_HOST,
                DEFERRED_GRAPHS_MAX_BYTES_PER_HOST
            ),
            (32, 8 << 20)
        );
        // The worker binds the shares.
        let storage = include_str!("d1_storage.rs");
        let put = &storage[storage.find("async fn put_deferred_graph").unwrap()..];
        let put = &put[..put.find("async fn find_deferred_graphs").unwrap()];
        assert!(put.contains("DEFERRED_GRAPHS_MAX_ROWS_PER_HOST"));
        assert!(put.contains("DEFERRED_GRAPHS_MAX_BYTES_PER_HOST"));
    }

    /// bsv-low #555, the delta-2 fold's D2-M2 (the lens's DELTA2-4, inverted).
    /// On ef423da eight spellings of one server (`https://evil.example/?0` ..
    /// `?7`), 16 records over two topics each, took all 256 rows, and a
    /// CONFIGURED peer (`overlay-us-1.bsvb.tech`, `tm_uhrp`) was then refused.
    /// Now the spellings are ONE origin and share one share (32 rows), the
    /// discovered peers together hold at most half the rows (128, here with
    /// four subdomains), and the configured peer still saves, up to the
    /// global bound. Bytes: the discovered half refuses a discovered growth
    /// that a configured record of the same size passes.
    #[test]
    fn e555d2_m2_a_configured_peer_saves_with_the_discovered_half_full() {
        let conn = sqlite();
        let save = |host: &str, topic: &str, o: &str, configured: bool| {
            let mut r = record(o, 1, 1, 1);
            r.peer = host.into();
            r.topic = topic.into();
            r.configured = configured;
            let json = serde_json::to_string(&r).unwrap();
            let mut stmt = conn.prepare(DEFERRED_GRAPH_UPSERT_SQL).unwrap();
            let mut rows = stmt
                .query(rusqlite::params![
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
                    json,
                    DEFERRED_GRAPHS_MAX_ROWS,
                    DEFERRED_GRAPHS_MAX_TOTAL_BYTES as i64,
                    DEFERRED_GRAPHS_MAX_ROWS_PER_HOST,
                    DEFERRED_GRAPHS_MAX_BYTES_PER_HOST as i64,
                    overlay_engine::gasp::peer_origin(&r.peer),
                    r.configured,
                    DEFERRED_GRAPHS_DISCOVERED_MAX_ROWS,
                    DEFERRED_GRAPHS_DISCOVERED_MAX_BYTES as i64
                ])
                .unwrap();
            rows.next().unwrap().is_some()
        };
        // DELTA2-4's flood: 8 spellings x 2 topics x 16 records.
        let mut saved = 0;
        for spelling in 0..8 {
            for topic in ["tm_ship", "tm_slap"] {
                for i in 0..16 {
                    let host = format!("https://evil.example/?{spelling}");
                    saved += u32::from(save(
                        &host,
                        topic,
                        &format!("{spelling}{topic}{i}.0"),
                        false,
                    ));
                }
            }
        }
        assert_eq!(
            saved, DEFERRED_GRAPHS_MAX_ROWS_PER_HOST,
            "eight spellings, one share"
        );
        // Three more servers by subdomain fill the discovered half.
        for sub in ["a", "b", "c", "d"] {
            for i in 0..32 {
                save(
                    &format!("https://{sub}.evil.example"),
                    "tm_ship",
                    &format!("{sub}{i}.0"),
                    false,
                );
            }
        }
        let held = rows(&conn).len() as u32;
        assert_eq!(
            held, DEFERRED_GRAPHS_DISCOVERED_MAX_ROWS,
            "the discovered half"
        );
        assert!(
            !save("https://e.evil.example", "tm_ship", "e0.0", false),
            "a 6th discovered host"
        );
        // The configured peer still saves, up to the global bound.
        for i in 0..(DEFERRED_GRAPHS_MAX_ROWS - held) {
            assert!(
                save(
                    "https://overlay-us-1.bsvb.tech",
                    "tm_uhrp",
                    &format!("u{i}.0"),
                    true
                ),
                "configured record {i}"
            );
        }
        assert!(
            !save(
                "https://overlay-us-1.bsvb.tech",
                "tm_uhrp",
                "u-last.0",
                true
            ),
            "the global bound"
        );
        assert_eq!(
            (
                DEFERRED_GRAPHS_DISCOVERED_MAX_ROWS,
                DEFERRED_GRAPHS_DISCOVERED_MAX_BYTES
            ),
            (128, 32 << 20)
        );
        // Bytes: a discovered host at the discovered byte half; a configured
        // record of the same size passes it.
        let conn2 = sqlite();
        let shaped = |host: &str, o: &str, configured: bool| {
            let mut r = record(o, 1, 1, 1);
            r.peer = host.into();
            r.configured = configured;
            r
        };
        let size = serde_json::to_string(&shaped("https://s1", "s1.0", false))
            .unwrap()
            .len() as u64;
        let bytes = |host: &str, o: &str, configured: bool| {
            upsert_reserved(
                &conn2,
                &shaped(host, o, configured),
                99,
                99 << 20,
                99,
                99 << 20,
                99,
                size,
            )
        };
        assert!(bytes("https://s1", "s1.0", false));
        assert!(
            !bytes("https://s2", "s2.0", false),
            "past the discovered bytes"
        );
        assert!(
            bytes("https://configured", "c1.0", true),
            "configured: global bytes only"
        );
        // The worker binds the origin, the class and the half.
        let storage = include_str!("d1_storage.rs");
        let put = &storage[storage.find("async fn put_deferred_graph").unwrap()..];
        let put = &put[..put.find("async fn find_deferred_graphs").unwrap()];
        assert!(put.contains("peer_origin(&record.peer)"));
        assert!(put.contains(".bind(record.configured)"));
        assert!(put.contains("DEFERRED_GRAPHS_DISCOVERED_MAX_ROWS"));
        assert!(put.contains("DEFERRED_GRAPHS_DISCOVERED_MAX_BYTES"));
    }

    /// bsv-low #555, the lens fold's M3: the cron's sweep deletes the rows not
    /// written for [`DEFERRED_GRAPH_STALE_SECS`] (30 h) and returns each, and
    /// leaves the others; the cron calls it before its GASP sync and counts
    /// it. On 03e1e17 nothing deleted a row whose peer never synced again.
    #[test]
    fn e555f_m3_the_sweep_deletes_stale_rows_only() {
        let conn = sqlite();
        upsert(&conn, &record("old.0", 1, 1, 1));
        upsert(&conn, &record("new.0", 2, 1, 1));
        conn.execute(
            "UPDATE gasp_deferred_graphs SET updated_at = unixepoch() - ?1 WHERE outpoint = 'old.0'",
            [DEFERRED_GRAPH_STALE_SECS as i64 + 1],
        )
        .unwrap();
        conn.execute(
            "UPDATE gasp_deferred_graphs SET updated_at = unixepoch() - ?1 WHERE outpoint = 'new.0'",
            [DEFERRED_GRAPH_STALE_SECS as i64 - 60],
        )
        .unwrap();
        let mut stmt = conn.prepare(DEFERRED_GRAPHS_SWEEP_SQL).unwrap();
        let swept: Vec<String> = stmt
            .query_map([DEFERRED_GRAPH_STALE_SECS as i64], |row| row.get(2))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(swept, ["old.0"]);
        assert_eq!(rows(&conn), ["new.0"]);
        assert_eq!(DEFERRED_GRAPH_STALE_SECS, 108_000);
        let lib = include_str!("lib.rs");
        let sweep = lib
            .find("crate::gasp_deferred::sweep_stale(&ops_db).await;")
            .unwrap();
        let sync = lib.find("engine.start_gasp_sync(),\n        crate::broadcaster::sleep_ms(GASP_SYNC_BUDGET_MS)").unwrap();
        assert!(sweep < sync, "the cron sweeps before its GASP sync");
        assert!(counter_names().contains(&COUNTER_GASP_GRAPH_DROPPED_STALE.to_string()));
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
        let (count, bytes): (i64, i64) = conn
            .query_row(HEALTH_COUNT_SQL, [], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap();
        assert_eq!(count, 2, "replaced, never appended");
        let a = serde_json::to_string(&record("a.0", 1, 6, 2))
            .unwrap()
            .len();
        let b = serde_json::to_string(&record("b.0", 2, 1, 1))
            .unwrap()
            .len();
        assert_eq!(bytes as usize, a + b, "totalBytes");
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
        let v = health_view(1, 51_000, &[row]);
        assert_eq!(v["readable"], true);
        assert_eq!(v["count"], 1);
        assert_eq!(v["totalBytes"], 51_000);
        assert_eq!(
            (
                v["budget"]["maxRows"].clone(),
                v["budget"]["maxTotalBytes"].clone(),
                v["budget"]["staleSecs"].clone(),
                v["budget"]["maxRowsPerHost"].clone(),
                v["budget"]["maxBytesPerHost"].clone()
            ),
            (
                serde_json::json!(256),
                serde_json::json!(64u64 << 20),
                serde_json::json!(108_000),
                serde_json::json!(32),
                serde_json::json!(8u64 << 20)
            )
        );
        assert_eq!(v["oldest"]["outpoint"], "a.0");
        assert_eq!(
            v["graphs"][0],
            serde_json::json!({"topic": "tm_x", "peer": "https://peer", "outpoint": "a.0",
                "nodes": 40, "pending": 3, "calls": 120, "passes": 3, "reason": "time",
                "bytes": 51000, "ageSecs": 180})
        );
        assert_eq!(v["budget"]["calls"], GASP_GRAPH_BUDGET_CALLS);
        assert_eq!(v["budget"]["ms"], GASP_GRAPH_BUDGET_MS);
        // bsv-low #586: the two limbs, the engine's defaults.
        assert_eq!(v["budget"]["bytesFetched"], 4u64 << 20);
        assert_eq!(v["budget"]["nodes"], 64);
        let none = health_view(0, 0, &[]);
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
            stalled_graphs: 0,
            held_back_graphs: 0,
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
        assert_eq!(names.len(), 5 + 11);
        assert!(names.contains(&"gasp_graph_dropped_root_proven_total".to_string()));
        // The delta-2 fold's D2-M2: a resumed walk dropped after its idle faults.
        assert!(names.contains(&"gasp_graph_dropped_idle_faults_total".to_string()));
    }
}
