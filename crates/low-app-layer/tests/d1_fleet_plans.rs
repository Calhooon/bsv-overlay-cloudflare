//! bsv-low #638: hot routes must never scan a fleet table.
#[path = "support/d1_fleet.rs"]
mod d1_fleet;
use bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS;
use rusqlite::{params_from_iter, Connection};

fn fixture() -> Connection {
    let db = Connection::open_in_memory().unwrap();
    for sql in OVERLAY_MIGRATIONS {
        if let Err(e) = db.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"), "{e}: {sql}");
        }
    }
    db.execute_batch("BEGIN").unwrap();
    for sql in d1_fleet::rows(1_790_000_000) {
        db.execute_batch(&sql)
            .unwrap_or_else(|e| panic!("{e}: {sql}"));
    }
    db.execute_batch("COMMIT").unwrap();
    db
}

fn plan(db: &Connection, sql: &str) -> Vec<String> {
    let mut stmt = db.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
    let binds = vec![""; stmt.parameter_count()];
    stmt.query_map(params_from_iter(binds), |r| r.get(3))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

#[test]
fn hot_routes_never_scan_fleet_tables() {
    let db = fixture();
    println!("shipped migrations: {}", OVERLAY_MIGRATIONS.len());
    let mut failures = Vec::new();
    for (route, sql) in [
        (
            "/results:view",
            low_app_layer::results::results_sql(None, 0),
        ),
        (
            "/results:view hop seats",
            low_app_layer::results::hop_seat_markers_sql(24),
        ),
        (
            "owed recompute refund",
            low_app_layer::refund_view::refund_view_sql(None, 0),
        ),
        (
            "owed recompute hops",
            low_app_layer::hops_view::hops_view_sql(false, None, 0),
        ),
        (
            "owed recompute proofs",
            low_app_layer::hopsweep::sweep_proofs_sql(16),
        ),
        (
            "/recovery-view",
            low_app_layer::logic::recovery_view_sql(None, 0),
        ),
        (
            "/refund-backups",
            low_app_layer::refund_backups::refund_backups_sql(None),
        ),
        (
            "owed recompute spenders",
            low_app_layer::owed::covenant_spenders_sql(16),
        ),
        (
            "owed recompute backups",
            low_app_layer::owed::valid_refunds_sql(16),
        ),
    ] {
        for detail in plan(&db, &sql) {
            if detail.contains("SEARCH x USING INDEX idx_potparty_identity") {
                failures.push(format!("{route}: quadratic exclusion: {detail}"));
            }
            // Aliases are SQLite's table names in these shipped queries.
            let name = detail
                .strip_prefix("SCAN ")
                .and_then(|s| s.split_whitespace().next());
            if name.is_some_and(|n| {
                [
                    "pot_records",
                    "potparty_records",
                    "hopparty_records",
                    "potrefund_records",
                    "hopsweep_records",
                    "transactions",
                    "result_markers_v2",
                    "proof_markers",
                    "pot_beefs",
                    "network_seen",
                    "hp",
                    "hop",
                    "pot",
                    "x",
                    "pr",
                ]
                .contains(&n)
            }) {
                failures.push(format!("{route}: {detail}"));
            }
        }
    }
    assert!(failures.is_empty(), "fleet scans:\n{}", failures.join("\n"));
}

#[test]
fn marker_writer_stores_the_canonical_settle_key() {
    use bsv_overlay_cloudflare::{d1::QVal, d1_discovery::hopparty_write::hopparty_insert_query};
    use overlay_discovery::hopparty::storage::HoppartyRecord;
    let record = HoppartyRecord {
        identity: "02aa".into(),
        opponent_identity: "03bb".into(),
        game_id: "cc".repeat(32),
        hop_vout: 0,
        hop_sats: 1,
        seat_settle_pubkey: "03AB".into(),
        seat_sig_hex: "00".into(),
        identity_sig_hex: "00".into(),
        hop_lock_hex: None,
        hop_sats_on_chain: None,
        container_outputs: 2,
        size_bytes: None,
        fee_sats: None,
        txid: "dd".repeat(32),
        output_index: 1,
        created_at: 1,
    };
    let insert = hopparty_insert_query(&record, 1);
    assert!(matches!(&insert.query().params()[5], QVal::Text(s) if s == "03ab"));
}
