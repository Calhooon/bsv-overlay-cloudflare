//! bsv-low #672 (2026-10-15, RC-L30-COULD-NOT-JUDGE): the durable spender-bytes store, exercised through the
//! PRODUCTION SQL (the consts the Worker binds) against real SQLite — no port fake. The store persists a hop
//! spender's immutable bytes content-addressed by txid at the first hash-verified fetch, so a later budget-cut
//! recompute reads them before its clock and never regresses a JUDGED "spent outside this game" row to "could not
//! judge the spender" (loop 30's first red).

use bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS;
use low_app_layer::owed::{
    StoredSpenderBytes, StoredSpenderOutput, OWED_SPENDER_BYTES_CAP_SWEEP_SQL,
    OWED_SPENDER_BYTES_MAX_JSON, OWED_SPENDER_BYTES_SELECT_SQL, OWED_SPENDER_BYTES_SWEEP_SQL,
    OWED_SPENDER_BYTES_UPSERT_SQL,
};
use rusqlite::{params, Connection};

fn migrated() -> Connection {
    let db = Connection::open_in_memory().unwrap();
    for sql in OVERLAY_MIGRATIONS {
        if let Err(e) = db.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"), "{e}: {sql}");
        }
    }
    db
}

fn bytes(pkh: &str, sats: u64, pot: bool) -> StoredSpenderBytes {
    StoredSpenderBytes {
        inputs: vec![("07".repeat(32), 0)],
        outputs: vec![StoredSpenderOutput {
            vout: 0,
            pkh: Some(pkh.to_string()),
            sats,
            pot,
        }],
    }
}

fn count(db: &Connection) -> i64 {
    db.query_row("SELECT COUNT(*) FROM owed_spender_bytes", [], |r| r.get(0))
        .unwrap()
}

/// Pin 2 (store): a verified spender's outputs persisted once and read back by its PRIMARY KEY, through the
/// production UPSERT/SELECT. A re-write is content-addressed (`ON CONFLICT DO NOTHING`), so the row — and its age
/// stamp — is unchanged: a row written once is never asked of a courier again, whatever a later pass's budget.
#[test]
fn pin2_a_spender_is_persisted_once_and_read_back_content_addressed() {
    let db = migrated();
    let txid = "a402cdfc".to_string() + &"44".repeat(28);
    let b = bytes(&"99".repeat(20), 20_170, false);
    let json = b.to_json_capped().expect("under the size cap");
    db.execute(
        OWED_SPENDER_BYTES_UPSERT_SQL,
        params![&txid, &json, 1_000_i64],
    )
    .unwrap();
    assert_eq!(count(&db), 1);

    // read it back through the production SELECT (the txid as a one-element JSON array), parse, and match
    let list = format!("[\"{txid}\"]");
    let got: (String, String) = db
        .query_row(OWED_SPENDER_BYTES_SELECT_SQL, params![&list], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    assert_eq!(got.0, txid);
    let back = StoredSpenderBytes::from_json(&got.1).expect("round-trips");
    assert_eq!(back, b);

    // a re-write with a LATER stamp is a no-op (content-addressed): the row and its age stamp stand
    let b2 = bytes(&"99".repeat(20), 20_170, false);
    db.execute(
        OWED_SPENDER_BYTES_UPSERT_SQL,
        params![&txid, b2.to_json_capped().unwrap(), 9_999_i64],
    )
    .unwrap();
    assert_eq!(count(&db), 1);
    let stamp: i64 = db
        .query_row(
            "SELECT createdAtMs FROM owed_spender_bytes WHERE txid = ?1",
            params![&txid],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        stamp, 1_000,
        "the first write's stamp stands; the sweep ages the first write"
    );

    // a spender the store LACKS returns nothing (the phases ask a courier for it, as before)
    let miss = format!("[\"{}\"]", "bb".repeat(32));
    let n: i64 = db
        .query_row(
            &format!("SELECT COUNT(*) FROM ({OWED_SPENDER_BYTES_SELECT_SQL})"),
            params![&miss],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(n, 0);
}

/// Pin 5 (bounds): the per-row size cap refuses a pathological spender; the age sweep deletes rows older than the
/// retention and keeps fresh ones, bounded per pass; the row-cap sweep deletes the OLDEST rows beyond the ceiling.
#[test]
fn pin5_the_size_cap_the_age_sweep_and_the_row_cap_sweep_all_bound_the_store() {
    // the size cap: a spender with enough outputs to exceed the per-row JSON cap is not kept
    let huge = StoredSpenderBytes {
        inputs: vec![],
        outputs: (0..200_000)
            .map(|v| StoredSpenderOutput {
                vout: v,
                pkh: Some("ab".repeat(20)),
                sats: 1,
                pot: false,
            })
            .collect(),
    };
    assert!(
        serde_json::to_string(&huge).unwrap().len() > OWED_SPENDER_BYTES_MAX_JSON,
        "the fixture is actually over the cap"
    );
    assert!(
        huge.to_json_capped().is_none(),
        "a spender over the per-row size cap is not persisted"
    );
    let small = bytes(&"99".repeat(20), 20_170, false);
    assert!(small.to_json_capped().is_some(), "a normal spender is kept");

    // the age sweep: one row old, one fresh
    let db = migrated();
    let json = small.to_json_capped().unwrap();
    db.execute(
        OWED_SPENDER_BYTES_UPSERT_SQL,
        params![&("aa".repeat(32)), &json, 1_000_i64],
    )
    .unwrap();
    db.execute(
        OWED_SPENDER_BYTES_UPSERT_SQL,
        params![&("bb".repeat(32)), &json, 5_000_i64],
    )
    .unwrap();
    // cutoff 2_000: the row stamped 1_000 is older, the row stamped 5_000 is fresh
    db.execute(OWED_SPENDER_BYTES_SWEEP_SQL, params![2_000_i64, 256_i64])
        .unwrap();
    assert_eq!(count(&db), 1);
    let kept: String = db
        .query_row("SELECT txid FROM owed_spender_bytes", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        kept,
        "bb".repeat(32),
        "the fresh row is kept, the aged one swept"
    );

    // the row-cap sweep: five rows, a ceiling of 2 → the three OLDEST beyond the cap are deleted, bounded per pass
    let db = migrated();
    for i in 0..5i64 {
        db.execute(
            OWED_SPENDER_BYTES_UPSERT_SQL,
            params![&format!("{i:064x}"), &json, 1_000 + i],
        )
        .unwrap();
    }
    assert_eq!(count(&db), 5);
    db.execute(OWED_SPENDER_BYTES_CAP_SWEEP_SQL, params![2_i64, 256_i64])
        .unwrap();
    assert_eq!(count(&db), 2, "only the newest `cap` rows remain");
    // the two kept are the NEWEST stamps (3, 4)
    let mut keys: Vec<String> = db
        .prepare("SELECT txid FROM owed_spender_bytes ORDER BY createdAtMs")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    keys.sort();
    assert_eq!(keys, vec![format!("{:064x}", 3), format!("{:064x}", 4)]);
}

/// Pin 6 (#638): the store read SEEKS by the PRIMARY KEY — it never SCANs `owed_spender_bytes` (a cost that would
/// grow with the whole fleet's spenders). `EXPLAIN QUERY PLAN` over the production SELECT.
#[test]
fn pin6_the_store_read_seeks_the_primary_key_never_scans_the_table() {
    let db = migrated();
    let json = bytes(&"99".repeat(20), 20_170, false)
        .to_json_capped()
        .unwrap();
    for i in 0..50i64 {
        db.execute(
            OWED_SPENDER_BYTES_UPSERT_SQL,
            params![&format!("{i:064x}"), &json, 1_000 + i],
        )
        .unwrap();
    }
    let list = format!("[\"{:064x}\"]", 7);
    let plan: Vec<String> = db
        .prepare(&format!(
            "EXPLAIN QUERY PLAN {OWED_SPENDER_BYTES_SELECT_SQL}"
        ))
        .unwrap()
        .query_map(params![&list], |r| r.get::<_, String>(3))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    let joined = plan.join(" | ");
    assert!(
        !joined.contains("SCAN owed_spender_bytes"),
        "the content table must be SEARCHED by PK, never SCANNED: {joined}"
    );
    assert!(
        joined.contains("owed_spender_bytes"),
        "the plan names the table: {joined}"
    );
}
