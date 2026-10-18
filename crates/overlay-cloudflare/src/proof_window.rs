//! The bounded proof-window tick and the predicates used by lookup and health.
use crate::d1::Query;
use overlay_engine::proof_window::{
    HeldOutput, PeerProofSummary, PEER_PROOF_ROW_LIMIT, PROOFLESS_FLAG_MS,
};
use serde::Deserialize;
use std::cell::Cell;
use worker::D1Database;

/// The most rows a lookup reads within the proof window.
pub const LOOKUP_WINDOW_LIMIT: usize = 1000;
/// The proof-window pass's wall budget, in milliseconds.
pub const PROOF_WINDOW_BUDGET_MS: u64 = 30_000;

/// Aliases t and w name the transaction and its watch. An existing watch
/// wins; otherwise the durable creation time keeps late enrollment from
/// extending service. Unknown legacy ages are held until observed.
pub(crate) fn held_predicate(now: &str) -> String {
    format!("t.has_proof = 0 AND t.retired_ms IS NULL AND COALESCE(w.first_seen_ms, t.created_at * 1000, 0) < {now} - {PROOFLESS_FLAG_MS}")
}
pub(crate) fn served_predicate(now: &str) -> String {
    format!("t.retired_ms IS NULL AND (t.has_proof = 1 OR COALESCE(w.first_seen_ms, t.created_at * 1000, 0) >= {now} - {PROOFLESS_FLAG_MS})")
}
pub(crate) fn candidates_sql() -> String {
    format!("SELECT o.txid, o.outputIndex AS output_index, o.topic FROM outputs o JOIN transactions t ON t.txid = o.txid LEFT JOIN proofless_watch w ON w.txid = t.txid WHERE {} ORDER BY RANDOM() LIMIT {PEER_PROOF_ROW_LIMIT}", held_predicate("?1"))
}
pub(crate) fn held_count_sql() -> String {
    format!("SELECT COUNT(*) AS c FROM outputs o JOIN transactions t ON t.txid = o.txid LEFT JOIN proofless_watch w ON w.txid = t.txid WHERE {}", held_predicate("?1"))
}
/// Retirement is transaction-wide. Every indexed output must be an expired
/// UHRP advert; a permanent or later-expiring sibling preserves the row.
pub(crate) fn retire_expired_sql() -> String {
    format!("UPDATE transactions SET retired_ms = ?1, retired_reason = 'expired-unproven' WHERE has_proof = 0 AND retired_ms IS NULL AND txid IN (SELECT t.txid FROM transactions t LEFT JOIN proofless_watch w ON w.txid = t.txid WHERE {} AND EXISTS (SELECT 1 FROM outputs o WHERE o.txid = t.txid) AND NOT EXISTS (SELECT 1 FROM outputs o LEFT JOIN uhrp_records u ON u.txid = o.txid AND u.outputIndex = o.outputIndex WHERE o.txid = t.txid AND (u.txid IS NULL OR u.expiryTime = 0 OR u.expiryTime * 1000 >= ?1)) ORDER BY t.txid LIMIT {PEER_PROOF_ROW_LIMIT})", held_predicate("?1"))
}
/// `/health/invariants`' expired-unproven total. NS-COST-BAR-1: one row, the `expired` count the retired set's
/// triggers keep equal to `COUNT(*) FROM transactions WHERE retired_ms IS NOT NULL AND retired_reason =
/// 'expired-unproven'` (`ops::TX_RETIRED_TOTAL_SQL`), never a count of `transactions` (21,723 rows a read on beta).
pub const EXPIRED_COUNT_SQL: &str =
    "SELECT expired AS c FROM count_transactions_retired WHERE id = 1";

#[derive(Deserialize)]
struct Candidate {
    txid: String,
    output_index: f64,
    topic: String,
}

/// One proof-window pass: retire expired unproven rows, then ask peers for the held outputs' proofs, under [`PROOF_WINDOW_BUDGET_MS`].
pub async fn run_pass(engine: &overlay_engine::engine::Engine, db: &D1Database) {
    let progress = Cell::new(PeerProofSummary::default());
    let pass = async {
        let now = js_sys::Date::now() as i64;
        Query::new(retire_expired_sql())
            .bind(now)
            .execute(db)
            .await?;
        let rows: Vec<Candidate> = Query::new(candidates_sql()).bind(now).fetch_all(db).await?;
        let candidates: Vec<_> = rows
            .into_iter()
            .map(|r| HeldOutput {
                txid: r.txid,
                output_index: r.output_index as u32,
                topic: r.topic,
            })
            .collect();
        engine.rerequest_held_proofs(&candidates, &progress).await;
        Ok::<(), String>(())
    };
    match overlay_engine::gasp::race_or_deadline(
        pass,
        crate::broadcaster::sleep_ms(PROOF_WINDOW_BUDGET_MS),
    )
    .await
    {
        Some(Ok(())) => {}
        Some(Err(error)) => crate::proof_fetcher::push_log(&format!(
            "[proof window] pass failed; retry next tick: {error}"
        )),
        None => crate::proof_fetcher::push_log("[proof window] budget exhausted; retry next tick"),
    }
    let summary = progress.get();
    crate::proof_fetcher::push_log(&format!(
        "[proof window] requests={} landed={} refused={} faults={}",
        summary.requests, summary.landed, summary.refused, summary.faults
    ));
    for (name, value) in [
        ("peer_proof_requests_total", summary.requests),
        ("peer_proofs_landed_total", summary.landed),
        ("peer_proof_refused_total", summary.refused),
        ("peer_proof_faults_total", summary.faults),
    ] {
        crate::ops::bump_counter(db, name, value).await;
    }
    crate::ops::refresh_proofless_watch(db).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::d1::QVal;
    use overlay_discovery::uhrp::storage::UHRPQuery;
    use rusqlite::{params, Connection};

    fn database() -> Connection {
        let db = Connection::open_in_memory().unwrap();
        for sql in crate::d1::OVERLAY_MIGRATIONS {
            if let Err(error) = db.execute_batch(sql) {
                assert!(
                    error.to_string().contains("duplicate column"),
                    "{error}: {sql}"
                );
            }
        }
        db
    }

    fn row(db: &Connection, txid: &str, created: Option<i64>, proven: bool, expiry: i64) {
        db.execute("INSERT INTO transactions (txid, beef, has_proof, created_at) VALUES (?1, x'beef', ?2, ?3)", params![txid, proven, created]).unwrap();
        db.execute(
            "INSERT INTO outputs (txid, outputIndex, topic) VALUES (?1, 0, 'tm_uhrp')",
            [txid],
        )
        .unwrap();
        db.execute("INSERT INTO uhrp_records (txid, outputIndex, uhrpUrl, identityKey, downloadUrl, expiryTime, contentLength) VALUES (?1, 0, 'fixture', 'fixture', 'fixture', ?2, 1)", params![txid, expiry]).unwrap();
    }

    fn run_lookup(db: &Connection, query: &Query) -> Vec<String> {
        let values: Vec<rusqlite::types::Value> = query
            .params()
            .iter()
            .map(|v| match v {
                QVal::Text(v) => v.clone().into(),
                QVal::Int(v) => (*v).into(),
                QVal::Float(v) => (*v).into(),
                QVal::Null => rusqlite::types::Value::Null,
                QVal::Blob(v) => v.clone().into(),
                QVal::Bool(v) => i64::from(*v).into(),
            })
            .collect();
        db.prepare(query.sql())
            .unwrap()
            .query_map(rusqlite::params_from_iter(values), |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    #[test]
    fn proof_window_sql_holds_and_releases_without_parent_inference() {
        let db = database();
        let now = PROOFLESS_FLAG_MS * 3;
        let old = (now - PROOFLESS_FLAG_MS) / 1000 - 1;
        row(&db, "aged", Some(old), false, 0);
        row(
            &db,
            "edge",
            Some((now - PROOFLESS_FLAG_MS) / 1000),
            false,
            0,
        );
        row(&db, "proven", Some(old), true, 0);
        row(&db, "watch-young", Some(old), false, 0);
        row(&db, "watch-old", Some(now / 1000), false, 0);
        row(&db, "unknown", None, false, 0);
        db.execute(
            "INSERT INTO proofless_watch VALUES ('watch-young', ?1), ('watch-old', ?2)",
            params![now, old * 1000],
        )
        .unwrap();
        let keys = r#"[["aged",0],["edge",0],["proven",0],["watch-young",0],["watch-old",0],["unknown",0],["proven",1]]"#;
        let engine_lookup = || {
            Query::new(crate::d1_storage::D1Storage::served_outputs_sql())
                .bind(keys)
                .bind(now)
        };
        assert_eq!(
            run_lookup(&db, &engine_lookup()),
            vec!["edge", "proven", "watch-young"]
        );
        let question = UHRPQuery {
            find_all: Some(true),
            include_expired: Some(true),
            now_unix_seconds: Some(0),
            ..Default::default()
        };
        let mut uhrp = run_lookup(&db, &crate::d1_discovery::uhrp_lookup_query(&question, now));
        uhrp.sort();
        assert_eq!(uhrp, vec!["edge", "proven", "watch-young"]);
        let count: i64 = db
            .query_row(&held_count_sql(), [now], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 3);
        db.execute(
            crate::d1_storage::MARK_TX_PROVEN_AT_SQL,
            params![100, "aged"],
        )
        .unwrap();
        assert!(run_lookup(&db, &engine_lookup()).contains(&"aged".into()));
        assert!(
            run_lookup(&db, &crate::d1_discovery::uhrp_lookup_query(&question, now))
                .contains(&"aged".into())
        );
    }

    #[test]
    fn proof_window_expiry_is_bounded_and_preserves_live_siblings() {
        let db = database();
        let now = PROOFLESS_FLAG_MS * 3;
        let old = (now - PROOFLESS_FLAG_MS) / 1000 - 1;
        for (id, created, proof, expiry) in [
            ("expired", old, false, now / 1000 - 1),
            ("proven", old, true, now / 1000 - 1),
            ("young", now / 1000, false, now / 1000 - 1),
            ("permanent", old, false, 0),
            ("boundary", old, false, now / 1000),
            ("siblings", old, false, now / 1000 - 1),
            ("proof-race", old, false, now / 1000 - 1),
        ] {
            row(&db, id, Some(created), proof, expiry);
        }
        db.execute(
            "INSERT INTO outputs (txid, outputIndex, topic) VALUES ('siblings', 1, 'tm_uhrp')",
            [],
        )
        .unwrap();
        db.execute("INSERT INTO uhrp_records (txid, outputIndex, uhrpUrl, identityKey, downloadUrl, expiryTime, contentLength) VALUES ('siblings', 1, 'fixture', 'fixture', 'fixture', ?1, 1)", [now / 1000 + 1]).unwrap();
        db.execute(
            "INSERT INTO proofless_watch VALUES ('expired', ?1)",
            [old * 1000],
        )
        .unwrap();
        db.execute(
            crate::d1_storage::MARK_TX_PROVEN_AT_SQL,
            params![100, "proof-race"],
        )
        .unwrap();
        assert_eq!(db.execute(&retire_expired_sql(), [now]).unwrap(), 1);
        let retired: String = db
            .query_row(
                "SELECT txid FROM transactions WHERE retired_reason = 'expired-unproven'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(retired, "expired");
        assert_eq!(db.execute(&retire_expired_sql(), [now]).unwrap(), 0);
        db.execute(crate::ops::PROOFLESS_WATCH_GC_SQL, []).unwrap();
        let watched: i64 = db
            .query_row("SELECT COUNT(*) FROM proofless_watch", [], |r| r.get(0))
            .unwrap();
        assert_eq!(watched, 0);
        for n in 0..PEER_PROOF_ROW_LIMIT + 5 {
            row(&db, &format!("backlog-{n}"), Some(old), false, 1);
        }
        assert_eq!(
            db.execute(&retire_expired_sql(), [now]).unwrap(),
            PEER_PROOF_ROW_LIMIT
        );
        let total: i64 = db.query_row(EXPIRED_COUNT_SQL, [], |r| r.get(0)).unwrap();
        assert_eq!(total, PEER_PROOF_ROW_LIMIT as i64 + 1);
    }

    #[test]
    fn proof_window_enrollment_preserves_observation_and_gc_tracks_proof() {
        let db = database();
        let now = PROOFLESS_FLAG_MS * 3;
        row(&db, "aged", Some(1), false, 0);
        db.execute(
            &crate::ops::proofless_watch_enrol_sql("transactions"),
            [now],
        )
        .unwrap();
        let stamp: i64 = db
            .query_row(
                "SELECT first_seen_ms FROM proofless_watch WHERE txid = 'aged'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(stamp, 1000);
        db.execute(
            &crate::ops::proofless_watch_enrol_sql("transactions"),
            [now + 1000],
        )
        .unwrap();
        db.execute(
            crate::d1_storage::MARK_TX_PROVEN_AT_SQL,
            params![100, "aged"],
        )
        .unwrap();
        assert_eq!(
            db.execute(crate::ops::PROOFLESS_WATCH_GC_SQL, []).unwrap(),
            1
        );
    }

    #[test]
    fn proof_window_reorg_feed_requires_an_enabled_source() {
        let source = include_str!("tip_pass.rs");
        let pass = &source[source.find("pub async fn run_arcade_reorg_pass(").unwrap()..];
        let gate = pass
            .find(".permits(\"arcade\")")
            .expect("reorg source policy absent");
        assert!(gate < pass.find("ArcadeBlockStatusFeed::new").unwrap());
        assert!(
            pass[gate..].find("return ").unwrap()
                < pass[gate..].find("ArcadeBlockStatusFeed::new").unwrap()
        );
    }

    #[test]
    fn proof_window_advert_spend_requires_enabled_sources() {
        let source = include_str!("advert_lifecycle.rs");
        let pass = &source[source.find("pub async fn run_advert_lifecycle(").unwrap()..];
        let probe = pass.find("confirm_advert_spends(").unwrap();
        for name in ["woc", "bitails"] {
            assert!(
                pass.find(&format!("sources.permits(\"{name}\")"))
                    .expect("advert source policy absent")
                    < probe
            );
        }
        assert!(pass.contains("AdvertSpendSummary::default()"));
    }

    #[test]
    fn proof_window_one_constant_and_one_tick_hook() {
        let core = include_str!("../../overlay-engine/src/proof_window.rs");
        let ops = include_str!("ops.rs");
        assert_eq!(core.matches("pub const PROOFLESS_FLAG_MS:").count(), 1);
        assert!(!ops.contains("pub const PROOFLESS_FLAG_MS:"));
        assert!(ops.contains("pub use overlay_engine::proof_window::PROOFLESS_FLAG_MS;"));
        for field in [
            "heldByWindow",
            "peerProofRequests",
            "peerProofsLanded",
            "retiredExpiredUnprovenTotal",
        ] {
            assert!(ops.contains(field));
        }
        // The shared tick lives in tick.rs (cron and the alarm both run it); lib.rs only declares the module.
        let lib = include_str!("lib.rs");
        assert_eq!(lib.matches("crate::proof_window::run_pass(").count(), 0);
        let tick_src = include_str!("tick.rs");
        assert_eq!(
            tick_src
                .matches("crate::proof_window::run_pass(&engine, &ops_db).await;")
                .count(),
            1
        );
        let begin = tick_src.find("fn run_tick(").unwrap();
        let tick = tick_src[begin..].split("#[cfg(test)]").next().unwrap();
        assert!(
            tick.find("crate::proof_window::run_pass").unwrap()
                < tick.find("engine.start_gasp_sync_metered(").unwrap()
        );
        assert!(crate::d1_storage::D1Storage::served_outputs_sql().contains("json_each(?1)"));
        assert!(candidates_sql().contains(&format!("LIMIT {PEER_PROOF_ROW_LIMIT}")));
    }
}
