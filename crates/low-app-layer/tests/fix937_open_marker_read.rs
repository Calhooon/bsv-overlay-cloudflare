//! bsv-low #937 (L52 pair 13, class S3): a read honours a pot's OPEN eviction marker. The gated arm's engine write,
//! deferred past its 12,000 ms answer deadline (#838), landed the pot's `pot_records` row AFTER the eviction's open
//! marker (#513, `admit_fast::OPEN_EVICTION_MARKER_SQL`, written first); until the write-side guard moved it, the
//! row was served held. These pins run the app layer's SHIPPED statements and assembly under real SQLite with the
//! overlay's migrations, the marker and the late row written by the overlay's own statements:
//!
//! * `/utxo-status` (`batch_where_sql`, `assemble_statuses`, the ledger read, `attach_evictions`): the rule in
//!   Rust over the ledger the route already reads, zero statements more;
//! * `/pots-view` (`pots_view_join_sql`): the rule as one SQL conjunct, zero statements more, one primary-key probe
//!   of `pot_evictions` per row served.
use bsv_overlay_cloudflare::admit_fast::{OPEN_EVICTION_MARKER_SQL, READMIT_EVICTION_SQL};
use bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS;
use bsv_overlay_cloudflare::d1_discovery::store_record_sql;
use low_app_layer::logic::{
    assemble_pots_view, assemble_statuses, attach_evictions, batch_where_sql, chunk_outpoints,
    distinct_db_txids, evictions_by_txid_sql, parse_outpoints, pots_view_join_sql,
    utxo_status_body, EvictionLedgerRow, Outpoint, PotRecordRow, PotsViewRow,
};
use rusqlite::{params, Connection};

fn db() -> Connection {
    let db = Connection::open_in_memory().unwrap();
    for sql in OVERLAY_MIGRATIONS {
        if let Err(e) = db.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"), "{e}");
        }
    }
    db
}

fn pot(i: u32) -> String {
    format!("9{i:063x}")
}

/// The overlay's eviction step 1: the OPEN marker, its own statement.
fn open_marker(db: &Connection, txid: &str, reason: &str) {
    db.execute(
        OPEN_EVICTION_MARKER_SQL,
        params![txid, reason, 1_760_000_000_000_i64],
    )
    .unwrap();
}

/// The late write: the pot's row through the overlay's own admission upsert.
fn late_row(db: &Connection, txid: &str) {
    db.execute(
        store_record_sql(),
        params![
            txid,
            0i64,
            0i64,
            Option::<String>::None,
            0i64,
            1_760_000_001_i64,
            Option::<String>::None,
            Option::<String>::None,
            Option::<String>::None,
            Option::<String>::None,
            Option::<String>::None,
            Option::<String>::None,
            Option::<String>::None,
            Option::<i64>::None,
            Option::<i64>::None,
            Option::<i64>::None,
            Option::<i64>::None,
            Option::<i64>::None,
            0i64,
        ],
    )
    .unwrap();
}

/// What one `/utxo-status` request read: its body and the statements it ran.
struct Served {
    body: serde_json::Value,
    pot_statements: usize,
    ledger_statements: usize,
    ledger_rows: usize,
}

/// The route minus the host: the chunked batch read, the assembly, the ledger read, the attach, the body.
fn utxo_status(db: &Connection, ops: &[Outpoint]) -> Served {
    let mut rows: Vec<PotRecordRow> = Vec::new();
    let mut pot_statements = 0;
    for chunk in chunk_outpoints(ops) {
        pot_statements += 1;
        let binds: Vec<rusqlite::types::Value> = chunk
            .iter()
            .flat_map(|o| {
                [
                    rusqlite::types::Value::Text(o.db_txid()),
                    rusqlite::types::Value::Integer(o.vout as i64),
                ]
            })
            .collect();
        let mut stmt = db.prepare(&batch_where_sql(chunk.len())).unwrap();
        rows.extend(
            stmt.query_map(rusqlite::params_from_iter(binds), |r| {
                Ok(PotRecordRow {
                    txid: r.get("txid")?,
                    vout: r.get::<_, i64>("outputIndex")? as u32,
                    spent: r.get::<_, i64>("spent")? != 0,
                    spending_txid: r.get("spendingTxid")?,
                    spent_confirmed: r.get::<_, Option<i64>>("spentConfirmed")?.unwrap_or(0) != 0,
                    spender_final: r.get::<_, Option<i64>>("spenderFinal")?.map(|v| v != 0),
                    spender_seen: r.get::<_, Option<i64>>("spenderSeen")?.map(|v| v != 0),
                    funding_seen: r.get::<_, Option<i64>>("fundingSeen")?.map(|v| v != 0),
                    funding_seen_by: None,
                })
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        );
    }
    let mut entries = assemble_statuses(ops, &rows);
    let txids = distinct_db_txids(ops);
    let mut ledger: Vec<EvictionLedgerRow> = Vec::new();
    let mut ledger_statements = 0;
    for chunk in txids.chunks(low_app_layer::logic::D1_CHUNK_OUTPOINTS) {
        ledger_statements += 1;
        let mut q = db.prepare(&evictions_by_txid_sql(chunk.len())).unwrap();
        ledger.extend(
            q.query_map(rusqlite::params_from_iter(chunk.iter()), |r| {
                Ok(EvictionLedgerRow {
                    txid: r.get(0)?,
                    evicted_at: r.get(1)?,
                    readmitted_at: r.get(2)?,
                    reason: r.get(3)?,
                })
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        );
    }
    let ledger_rows = ledger.len();
    attach_evictions(&mut entries, &ledger);
    Served {
        body: serde_json::from_str(&utxo_status_body(&entries)).unwrap(),
        pot_statements,
        ledger_statements,
        ledger_rows,
    }
}

/// The `/pots-view` route minus the host: the chunked joined read and the assembly.
fn pots_view(
    db: &Connection,
    ops: &[Outpoint],
) -> (Vec<low_app_layer::logic::PotsViewEntry>, usize) {
    let mut rows: Vec<PotsViewRow> = Vec::new();
    let mut statements = 0;
    for chunk in chunk_outpoints(ops) {
        statements += 1;
        let binds: Vec<rusqlite::types::Value> = chunk
            .iter()
            .flat_map(|o| {
                [
                    rusqlite::types::Value::Text(o.db_txid()),
                    rusqlite::types::Value::Integer(o.vout as i64),
                ]
            })
            .collect();
        let mut stmt = db.prepare(&pots_view_join_sql(chunk.len(), None)).unwrap();
        rows.extend(
            stmt.query_map(rusqlite::params_from_iter(binds), |r| {
                Ok(PotsViewRow {
                    record: PotRecordRow {
                        txid: r.get("txid")?,
                        vout: r.get::<_, i64>("outputIndex")? as u32,
                        spent: r.get::<_, i64>("spent")? != 0,
                        spending_txid: r.get("spendingTxid")?,
                        spent_confirmed: r.get::<_, i64>("spentConfirmed")? != 0,
                        spender_final: None,
                        spender_seen: None,
                        funding_seen: None,
                        funding_seen_by: None,
                    },
                    spender_beef_hex: r.get("spenderBeef")?,
                })
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        );
    }
    (assemble_pots_view(ops, &rows), statements)
}

fn ops_of(txids: &[String]) -> Vec<Outpoint> {
    parse_outpoints(
        &txids
            .iter()
            .map(|t| format!("{t}.0"))
            .collect::<Vec<_>>()
            .join(","),
    )
    .unwrap()
}

/// `/utxo-status`: a row written after its open marker is served as the evicted twin is (`known:false`, every
/// fact null, the ledger's word); a pot with no marker stays held. RED on eaaee0ec: `known: true`.
#[test]
fn fix937_a_utxo_status_serves_a_row_under_an_open_marker_evicted() {
    let db = db();
    let (late, held) = (pot(1), pot(2));
    open_marker(
        &db,
        &late,
        "spent by confirmed competitor ab at 900000 (conflict pass)",
    );
    late_row(&db, &late);
    late_row(&db, &held);
    db.execute(
        "INSERT INTO network_seen (txid, seenAt) VALUES (?1, 1)",
        [&late],
    )
    .unwrap();
    let s = utxo_status(&db, &ops_of(&[late.clone(), held.clone()]));
    let e = &s.body[0];
    assert_eq!(e["known"], false, "{e}");
    for k in [
        "spent",
        "spendingTxid",
        "spentConfirmed",
        "spenderSeen",
        "spenderFinal",
        "networkSeen",
        "networkSeenBy",
    ] {
        assert!(
            e.get(k).is_none_or(|v| v.is_null()),
            "{k} is the twin's null: {e}"
        );
    }
    assert_eq!(e["evicted"]["readmittedAt"], serde_json::Value::Null);
    assert_eq!(e["evicted"]["at"], 1_760_000_000_000_i64);
    assert_eq!(s.body[1]["known"], true, "no marker: held");
    assert_eq!(s.body[1]["spent"], false);
    assert!(s.body[1]["evicted"].is_null());
}

/// `/utxo-status`: the readmission's own stamp closes the marker and the row is held at the next read (the route
/// keeps no memo of the ledger).
#[test]
fn fix937_b_utxo_status_a_readmitted_row_is_held() {
    let db = db();
    let p = pot(3);
    open_marker(&db, &p, "REJECTED (arcade live REJECTED: bad script)");
    late_row(&db, &p);
    let ops = ops_of(std::slice::from_ref(&p));
    assert_eq!(utxo_status(&db, &ops).body[0]["known"], false);
    db.execute(READMIT_EVICTION_SQL, params![1_760_000_060_000_i64, p])
        .unwrap();
    let e = &utxo_status(&db, &ops).body[0];
    assert_eq!(e["known"], true, "readmitted: held: {e}");
    assert_eq!(e["spent"], false);
    assert_eq!(e["evicted"]["readmittedAt"], 1_760_000_060_000_i64);
    assert_eq!(e["evicted"]["word"], serde_json::Value::Null);
}

/// The listing shape: 64 pots (the route's cap) with 3 open markers. `/utxo-status` runs its two chunked pot
/// statements and its two chunked ledger statements (64 distinct txids, `D1_CHUNK_OUTPOINTS` 45 a statement; E649
/// L6's read, the same count on eaaee0ec), the ledger returning the 3 marker rows: the rule reads nothing more. `/pots-view` runs its two chunked statements, unchanged in number;
/// the three are served `known:false`, the 61 others held.
#[test]
fn fix937_c_a_listing_of_64_with_3_open_markers_costs_no_statement_more() {
    let db = db();
    let txids: Vec<String> = (0..64).map(pot).collect();
    for t in &txids {
        late_row(&db, t);
    }
    let marked = [&txids[5], &txids[40], &txids[63]];
    for t in marked {
        open_marker(&db, t, "REJECTED (arcade live REJECTED: bad script)");
    }
    let ops = ops_of(&txids);
    let s = utxo_status(&db, &ops);
    assert_eq!(s.pot_statements, 2, "the batch read, unchanged");
    assert_eq!(
        s.ledger_statements, 2,
        "the E649 L6 ledger read, unchanged: the rule rides it"
    );
    assert_eq!(s.ledger_rows, 3, "the ledger read returns the markers only");
    let unknown: Vec<usize> = (0..64).filter(|&i| s.body[i]["known"] == false).collect();
    assert_eq!(unknown, vec![5, 40, 63]);
    let (view, statements) = pots_view(&db, &ops);
    assert_eq!(statements, 2, "/pots-view: no statement more");
    let unknown: Vec<usize> = (0..64).filter(|&i| !view[i].status.known).collect();
    assert_eq!(unknown, vec![5, 40, 63], "/pots-view honours the marker");
    db.execute(
        READMIT_EVICTION_SQL,
        params![1_760_000_060_000_i64, txids[40]],
    )
    .unwrap();
    let (view, _) = pots_view(&db, &ops);
    assert!(view[40].status.known, "/pots-view: readmitted, held");
}

/// The plans: the `/pots-view` conjunct is a primary-key SEARCH of `pot_evictions`, never a scan; the ledger read
/// rides the primary key.
#[test]
fn fix937_d_the_conjunct_and_the_ledger_read_search_the_primary_key() {
    let db = db();
    for sql in [
        pots_view_join_sql(low_app_layer::logic::D1_CHUNK_OUTPOINTS, None),
        pots_view_join_sql(low_app_layer::logic::D1_CHUNK_OUTPOINTS, Some(1)),
        evictions_by_txid_sql(low_app_layer::logic::MAX_OUTPOINTS),
    ] {
        let mut stmt = db.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
        let binds = vec![""; stmt.parameter_count()];
        let plan: Vec<String> = stmt
            .query_map(rusqlite::params_from_iter(binds), |r| r.get(3))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert!(
            plan.iter()
                .any(|d| d.contains("pot_evictions") && d.starts_with("SEARCH")
                    || d.starts_with("SEARCH ev ")),
            "the ledger is searched: {plan:?}"
        );
        assert!(
            !plan.iter().any(|d| d.starts_with("SCAN")),
            "no scan: {plan:?}"
        );
    }
}
