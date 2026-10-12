//! bsv-low #638: hot routes must never scan a fleet table.
#[path = "support/d1_fleet.rs"]
mod d1_fleet;
use bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS;
use rusqlite::{Connection, params_from_iter};

fn fixture() -> Connection {
    let db = Connection::open_in_memory().unwrap();
    for sql in OVERLAY_MIGRATIONS {
        if let Err(e) = db.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"), "{e}: {sql}");
        }
    }
    db.execute_batch("BEGIN").unwrap();
    for sql in d1_fleet::rows(1_790_000_000) {
        db.execute_batch(&sql).unwrap_or_else(|e| panic!("{e}: {sql}"));
    }
    db.execute_batch("COMMIT").unwrap();
    db
}

fn plan(db: &Connection, sql: &str) -> Vec<String> {
    let mut stmt = db.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
    let binds = vec![""; stmt.parameter_count()];
    stmt.query_map(params_from_iter(binds), |r| r.get(3)).unwrap().map(Result::unwrap).collect()
}

#[test]
fn hot_routes_never_scan_fleet_tables() {
    let db = fixture();
    let mut failures = Vec::new();
    for (route, sql) in [
        ("/results:view", low_app_layer::results::results_sql(None, 0)),
        ("/results:view hop seats", low_app_layer::results::hop_seat_markers_sql(24)),
        ("owed recompute refund", low_app_layer::refund_view::refund_view_sql(None, 0)),
        ("owed recompute hops", low_app_layer::hops_view::hops_view_sql(false, None, 0)),
        ("owed recompute proofs", low_app_layer::hopsweep::sweep_proofs_sql(16)),
        ("/recovery-view", low_app_layer::logic::recovery_view_sql(None, 0)),
        ("/refund-backups", low_app_layer::refund_backups::refund_backups_sql(None)),
        ("owed recompute spenders", "SELECT DISTINCT lower(txid) FROM pot_records WHERE lockKind = 'covenant' AND lower(txid) IN (?1, ?2)".into()),
        ("owed recompute backups", "SELECT potTxid FROM potrefund_records WHERE refundValid = 1 AND refundRawHex IS NOT NULL AND lower(potTxid) = ?1 AND potVout = ?2 ORDER BY createdAt DESC".into()),
    ] {
        for detail in plan(&db, &sql) {
            if detail.contains("SEARCH x USING INDEX idx_potparty_identity") {
                failures.push(format!("{route}: quadratic exclusion: {detail}"));
            }
            // Aliases are SQLite's table names in these shipped queries.
            let name = detail.strip_prefix("SCAN ").and_then(|s| s.split_whitespace().next());
            if name.is_some_and(|n| ["pot_records", "potparty_records", "hopparty_records", "potrefund_records", "hopsweep_records", "transactions", "result_markers_v2", "proof_markers", "pot_beefs", "network_seen", "hp", "hop", "pot", "x", "pr"].contains(&n)) {
                failures.push(format!("{route}: {detail}"));
            }
        }
    }
    assert!(failures.is_empty(), "fleet scans:\n{}", failures.join("\n"));
}
