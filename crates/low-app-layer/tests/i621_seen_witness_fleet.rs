//! bsv-low #621 (LOW words map L5) on the FLEET fixture (`support/d1_fleet.rs`): `/utxo-status`'s body through the
//! shipped batch read (`batch_where_sql`), the shipped row mapping (`seen_witness_of_row`), the shipped assembly,
//! the shipped ledger read and the shipped body builder. Every pre-#621 field is byte for byte what `origin/main`
//! (`fe6c83d`) served for the same rows (`MAIN_BODY`, taken from main's own code over the same seed), save the one
//! named change: an eviction resting on ABSENCE alone serves no chain word (main served `rejected`). The added
//! `networkSeenBy` names the latch's broadcaster and time, `source: null` on a latch written before the pair.
#[path = "support/d1_fleet.rs"]
mod d1_fleet;
use bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS;
use low_app_layer::logic::*;
use rusqlite::{params, Connection};
use serde_json::{json, Value};

const NOW: u64 = 1_790_000_000;

/// `origin/main` at `fe6c83d` over the same seed (the latches written by main's own writer).
const MAIN_BODY: &str = r#"[{"evicted":null,"known":true,"networkSeen":true,"reason":null,"spenderFinal":null,"spenderSeen":false,"spendingTxid":null,"spent":false,"spentConfirmed":false,"txid":"d000000000000000000000000000000000000000000000000000000000000000","vout":0},{"evicted":null,"known":true,"networkSeen":true,"reason":null,"spenderFinal":null,"spenderSeen":false,"spendingTxid":null,"spent":false,"spentConfirmed":false,"txid":"d000000000000000000000000000000000000000000000000000000000000001","vout":0},{"evicted":null,"known":true,"networkSeen":false,"reason":null,"spenderFinal":null,"spenderSeen":false,"spendingTxid":null,"spent":false,"spentConfirmed":false,"txid":"d000000000000000000000000000000000000000000000000000000000000002","vout":0},{"evicted":null,"known":true,"networkSeen":false,"reason":null,"spenderFinal":null,"spenderSeen":true,"spendingTxid":"d000000000000000000000000000000000000000000000000000000000000000","spent":true,"spentConfirmed":false,"txid":"d100000000000000000000000000000000000000000000000000000000000000","vout":0},{"evicted":null,"known":true,"networkSeen":false,"reason":null,"spenderFinal":null,"spenderSeen":false,"spendingTxid":null,"spent":true,"spentConfirmed":false,"txid":"d000000000000000000000000000000000000000000000000000000000001b58","vout":0},{"evicted":null,"known":false,"networkSeen":null,"reason":null,"spenderFinal":null,"spenderSeen":null,"spendingTxid":null,"spent":null,"spentConfirmed":null,"txid":"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff","vout":0},{"evicted":{"at":1790000000000,"readmittedAt":null,"word":"rejected"},"known":false,"networkSeen":null,"reason":null,"spenderFinal":null,"spenderSeen":null,"spendingTxid":null,"spent":null,"spentConfirmed":null,"txid":"e500000000000000000000000000000000000000000000000000000000000001","vout":0},{"evicted":{"at":1790000000000,"readmittedAt":null,"word":"rejected"},"known":false,"networkSeen":null,"reason":null,"spenderFinal":null,"spenderSeen":null,"spendingTxid":null,"spent":null,"spentConfirmed":null,"txid":"e500000000000000000000000000000000000000000000000000000000000002","vout":0}]"#;

fn p(i: u32) -> String {
    format!("d0{i:062x}")
}

fn served() -> Value {
    let db = Connection::open_in_memory().unwrap();
    for sql in OVERLAY_MIGRATIONS {
        if let Err(e) = db.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"), "{e}: {sql}");
        }
    }
    db.execute_batch("BEGIN").unwrap();
    for sql in d1_fleet::rows(NOW) {
        d1_fleet::execute_row(&db, &sql);
    }
    db.execute_batch("COMMIT").unwrap();
    // the shipped writer names TAAL; the second pot's latch is a row of before the pair (the writer of its day)
    db.execute(
        bsv_overlay_cloudflare::ops::NETWORK_SEEN_INSERT_SQL,
        params![p(0), bsv_overlay_cloudflare::ops::SeenSource::Taal.wire(), 1_790_000_000_123_i64],
    )
    .unwrap();
    db.execute(
        "INSERT OR IGNORE INTO network_seen (txid, seenAt) VALUES (lower(?), ?)",
        params![p(1), NOW],
    )
    .unwrap();
    for (i, reason) in [
        (1, "REJECTED (arcade live REJECTED: bad; both indexers absent)"),
        (2, "SEEN_ON_NETWORK (arcade missing, both indexers absent)"),
    ] {
        db.execute(
            "INSERT INTO pot_evictions (txid, reason, evictedAt) VALUES (?1, ?2, ?3)",
            params![format!("e5{i:062x}"), reason, 1_790_000_000_000_i64],
        )
        .unwrap();
    }
    let param = [
        p(0),
        p(1),
        p(2),
        format!("d1{:062x}", 0),
        p(7000),
        "ff".repeat(32),
        format!("e5{:062x}", 1),
        format!("e5{:062x}", 2),
    ]
    .map(|t| format!("{t}.0"))
    .join(",");
    let ops = parse_outpoints(&param).unwrap();
    let binds: Vec<rusqlite::types::Value> = ops
        .iter()
        .flat_map(|o| {
            [
                rusqlite::types::Value::Text(o.db_txid()),
                rusqlite::types::Value::Integer(o.vout as i64),
            ]
        })
        .collect();
    let mut stmt = db.prepare(&batch_where_sql(ops.len())).unwrap();
    let rows: Vec<PotRecordRow> = stmt
        .query_map(rusqlite::params_from_iter(binds), |r| {
            let funding_seen = r.get::<_, Option<i64>>("fundingSeen")?.map(|v| v != 0);
            Ok(PotRecordRow {
                txid: r.get("txid")?,
                vout: r.get::<_, i64>("outputIndex")? as u32,
                spent: r.get::<_, i64>("spent")? != 0,
                spending_txid: r.get("spendingTxid")?,
                spent_confirmed: r.get::<_, Option<i64>>("spentConfirmed")?.unwrap_or(0) != 0,
                spender_final: r.get::<_, Option<i64>>("spenderFinal")?.map(|v| v != 0),
                spender_seen: r.get::<_, Option<i64>>("spenderSeen")?.map(|v| v != 0),
                funding_seen,
                funding_seen_by: overlay_discovery::pot::storage::seen_witness_of_row(
                    funding_seen == Some(true),
                    r.get("fundingSeenBy")?,
                    r.get("fundingSeenByAt")?,
                    r.get("fundingSeenAt")?,
                ),
            })
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let mut entries = assemble_statuses(&ops, &rows);
    let txids = distinct_db_txids(&ops);
    let mut q = db.prepare(&evictions_by_txid_sql(txids.len())).unwrap();
    let ev: Vec<EvictionLedgerRow> = q
        .query_map(rusqlite::params_from_iter(txids.iter()), |r| {
            Ok(EvictionLedgerRow {
                txid: r.get(0)?,
                evicted_at: r.get(1)?,
                readmitted_at: r.get(2)?,
                reason: r.get(3)?,
            })
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    attach_evictions(&mut entries, &ev);
    serde_json::from_str(&utxo_status_body(&entries)).unwrap()
}

#[test]
fn i621_the_old_fields_are_mains_bytes_and_the_witness_names_its_source() {
    let body = served();
    // (1) the pre-#621 fields: main's bytes, save the absence eviction's word
    let mut old = body.clone();
    for e in old.as_array_mut().unwrap() {
        e.as_object_mut().unwrap().remove("networkSeenBy");
    }
    let mut main: Value = serde_json::from_str(MAIN_BODY).unwrap();
    assert_eq!(main[7]["evicted"]["word"], "rejected", "main voided on an absence");
    main[7]["evicted"]["word"] = Value::Null;
    assert_eq!(old.to_string(), main.to_string());
    // (2) the witness: named, legacy, unwitnessed (pot and hop), and omitted where `networkSeen` was not read
    let by: Vec<Option<&Value>> = body.as_array().unwrap().iter().map(|e| e.get("networkSeenBy")).collect();
    assert_eq!(by[0], Some(&json!({"word": "seen", "tier": "hint", "source": "taal", "at": 1_790_000_000_123_i64})));
    assert_eq!(
        by[1],
        Some(&json!({"word": "seen", "tier": "hint", "source": null, "at": NOW * 1000})),
        "a latch of before the pair names no source; its time is its seenAt"
    );
    assert_eq!((by[2], by[3], by[4]), (Some(&Value::Null), Some(&Value::Null), Some(&Value::Null)));
    assert_eq!((by[5], by[6], by[7]), (None, None, None), "unknown outpoints: not read, not served");
    // (3) the chain words: a node's verdict voids, an absence does not
    assert_eq!(body[6]["evicted"]["word"], "rejected");
    assert!(body[7]["evicted"]["word"].is_null() && body[7]["evicted"]["readmittedAt"].is_null());
}
