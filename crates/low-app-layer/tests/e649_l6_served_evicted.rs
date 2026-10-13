//! bsv-low E649 L6: the eviction is SERVED. `/utxo-status` carries `evicted: {at, word, readmittedAt}` read from
//! the overlay's eviction ledger (`pot_evictions`) by txid, so a client on a fresh device judges an evicted pot
//! from the answer alone. The ledger rows are written by the overlay's OWN statements (`CLOSE_EVICTION_MARKER_SQL`,
//! the readmission's stamp) under real SQLite, then read by the app layer's shipped statement.
use bsv_overlay_cloudflare::{admit_fast::CLOSE_EVICTION_MARKER_SQL, d1::OVERLAY_MIGRATIONS};
use low_app_layer::logic::{
    attach_evictions, distinct_db_txids, evictions_by_txid_sql, parse_outpoints, utxo_status_body,
    EvictionLedgerRow, OutpointStatus,
};
use rusqlite::Connection;

/// The overlay's readmission stamp, verbatim (`admit_fast.rs`, pinned there by its own source check).
const READMIT_STAMP_SQL: &str =
    "UPDATE pot_evictions SET readmittedAt = ? WHERE txid = ? AND readmittedAt IS NULL";

fn db() -> Connection {
    let db = Connection::open_in_memory().unwrap();
    for sql in OVERLAY_MIGRATIONS {
        if let Err(e) = db.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"), "{e}");
        }
    }
    db
}

/// The route's read and assembly, minus the host: the shipped statement, the shipped attach, the shipped body.
fn served(db: &Connection, param: &str) -> serde_json::Value {
    let ops = parse_outpoints(param).unwrap();
    let txids = distinct_db_txids(&ops);
    let mut q = db.prepare(&evictions_by_txid_sql(txids.len())).unwrap();
    let rows: Vec<EvictionLedgerRow> = q
        .query_map(rusqlite::params_from_iter(txids.iter()), |r| {
            Ok(EvictionLedgerRow {
                txid: r.get::<_, String>(0)?.to_ascii_lowercase(),
                evicted_at: r.get(1)?,
                readmitted_at: r.get(2)?,
            })
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    // an evicted pot has no `pot_records` row (the shadow move): the index's answer is `known:false`
    let mut entries: Vec<OutpointStatus> = ops.iter().map(OutpointStatus::unknown).collect();
    attach_evictions(&mut entries, &rows);
    serde_json::from_str(&utxo_status_body(&entries)).unwrap()
}

#[test]
fn e649_l6_a_an_evicted_pot_carries_the_ledger_row_and_a_readmission_moves_the_word() {
    let db = db();
    let pot = "ab".repeat(32);
    let other = "cd".repeat(32);
    db.execute(
        CLOSE_EVICTION_MARKER_SQL,
        rusqlite::params![
            pot,
            "REJECTED (corroborated)",
            1_760_000_000_000_i64,
            2,
            "[]"
        ],
    )
    .unwrap();

    let v = served(&db, &format!("{pot}.0,{other}.0"));
    assert_eq!(v[0]["known"], false);
    assert_eq!(
        v[0]["evicted"],
        serde_json::json!({"at": 1_760_000_000_000_i64, "word": "rejected", "readmittedAt": null}),
        "an evicted pot's /utxo-status carries the ledger row: {v}"
    );
    assert!(
        v[1].as_object().unwrap().contains_key("evicted") && v[1]["evicted"].is_null(),
        "a txid with no ledger row is served `evicted: null` (read, none): {v}"
    );

    db.execute(
        READMIT_STAMP_SQL,
        rusqlite::params![1_760_000_500_000_i64, pot],
    )
    .unwrap();
    let v = served(&db, &format!("{pot}.0"));
    assert_eq!(v[0]["evicted"]["readmittedAt"], 1_760_000_500_000_i64);
    assert!(
        v[0]["evicted"]["word"].is_null(),
        "a readmitted pot's word moves off `rejected`: {v}"
    );
}

#[test]
fn e649_l6_a_a_surface_that_did_not_read_the_ledger_omits_the_key() {
    let ops = parse_outpoints(&format!("{}.0", "ef".repeat(32))).unwrap();
    let v: serde_json::Value =
        serde_json::from_str(&utxo_status_body(&[OutpointStatus::unknown(&ops[0])])).unwrap();
    assert!(
        !v[0].as_object().unwrap().contains_key("evicted"),
        "not read (another producer, or a faulted read) is ABSENT, never a `null` that reads as no eviction: {v}"
    );
}

#[test]
fn e649_l6_a_the_route_reads_the_ledger_and_writes_nothing() {
    let routes = include_str!("../src/routes.rs");
    let at = routes.find("pub async fn utxo_status(").unwrap();
    let body = &routes[at..at + routes[at..].find("\n}\n").unwrap()];
    assert!(
        body.contains("read_eviction_ledger(&db, &distinct_db_txids(&outpoints))")
            && body.contains("attach_evictions(&mut entries, &ledger)"),
        "/utxo-status reads the eviction ledger and attaches it"
    );
    let sql = evictions_by_txid_sql(3);
    assert!(
        sql.starts_with("SELECT ") && sql.contains("FROM pot_evictions WHERE txid IN (?1, ?2, ?3)")
    );
    for w in ["INSERT", "UPDATE", "DELETE"] {
        assert!(!body.contains(w), "a read path writes nothing (NS-3): {w}");
    }
}
