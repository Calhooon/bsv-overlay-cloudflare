//! OWED-1184 (bsv-low #1184): a spender's stored bytes that an INDEX POINTER names (`pot_records.spendingTxid`) are
//! never swept. They are the evidence a judged "spent outside this game" story rests on (#672's rule: a judged owed
//! row never regresses); the eight-day age sweep and the row cap took them, so a fresh judgement of the same record
//! went back to "could not judge". Every pin runs the SHIPPED sweep statements over the overlay's migrations on real
//! SQLite.
#[path = "support/d1_fleet.rs"]
mod d1_fleet;
use bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS;
use low_app_layer::owed::{
    OWED_SPENDER_BYTES_CAP_SWEEP_SQL, OWED_SPENDER_BYTES_RETENTION_MS,
    OWED_SPENDER_BYTES_SWEEP_SQL, OWED_SPENDER_BYTES_UPSERT_SQL,
};
use rusqlite::{params, params_from_iter, Connection};

const NOW: i64 = 1_790_000_000_000;

fn migrated() -> Connection {
    let db = Connection::open_in_memory().unwrap();
    for sql in OVERLAY_MIGRATIONS {
        if let Err(e) = db.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"), "{e}: {sql}");
        }
    }
    db
}

fn txid(tag: &str, n: i64) -> String {
    format!("{tag}{n:062x}")
}

/// A spent hop row of the index naming `spender` (confirmed).
fn pointer(db: &Connection, hop: &str, spender: &str) {
    db.execute(
        "INSERT INTO pot_records(txid,outputIndex,spent,spendingTxid,spentConfirmed,spentHeight,createdAt,lockKind) VALUES (?1,0,1,?2,1,899000,1789000000,'p2pkh')",
        params![hop, spender],
    )
    .unwrap();
}

fn bytes(db: &Connection, spender: &str, at: i64) {
    db.execute(
        OWED_SPENDER_BYTES_UPSERT_SQL,
        params![spender, r#"{"inputs":[],"outputs":[]}"#, at],
    )
    .unwrap();
}

fn swept(db: &Connection, sql: &str, a: i64, b: i64) -> Vec<String> {
    let mut out: Vec<String> = db
        .prepare(sql)
        .unwrap()
        .query_map(params![a, b], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    out.sort();
    out
}

fn held(db: &Connection) -> Vec<String> {
    let mut out: Vec<String> = db
        .prepare("SELECT txid FROM owed_spender_bytes")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    out.sort();
    out
}

/// (a) The age sweep at now plus eight days: a spender an index pointer names is KEPT; a spender no pointer names (a
/// stranger's, an old find no hop row records) is still swept. RED on main: the named spender went too.
#[test]
fn owed1184_a_the_age_sweep_keeps_a_pointer_named_spender_and_sweeps_the_rest() {
    let db = migrated();
    let (named, stranger) = (txid("5e", 1), txid("5f", 2));
    pointer(&db, &txid("b1", 1), &named);
    bytes(&db, &named, NOW);
    bytes(&db, &stranger, NOW);
    let later = NOW + OWED_SPENDER_BYTES_RETENTION_MS + 1_000;
    let cutoff = later - OWED_SPENDER_BYTES_RETENTION_MS;
    assert_eq!(
        swept(&db, OWED_SPENDER_BYTES_SWEEP_SQL, cutoff, 256),
        vec![stranger.clone()],
        "only the unreferenced row is swept"
    );
    assert_eq!(
        held(&db),
        vec![named.clone()],
        "the named spender's bytes are held"
    );
    // a second pass at the same time deletes nothing (the kept row is not re-taken)
    assert!(swept(&db, OWED_SPENDER_BYTES_SWEEP_SQL, cutoff, 256).is_empty());
    // the pointer cleared (a reorg demotes, the overlay re-points): the bytes are an unreferenced row again
    db.execute("UPDATE pot_records SET spendingTxid=NULL, spent=0", [])
        .unwrap();
    assert_eq!(
        swept(&db, OWED_SPENDER_BYTES_SWEEP_SQL, cutoff, 256),
        vec![named]
    );
}

/// (b) The row cap counts UNREFERENCED rows: five strangers and three pointer-named spenders under a ceiling of two
/// leave the two newest strangers and every named spender. The store cannot grow without bound from strangers; the
/// named rows are bounded by the index (one a spent hop or pot). RED on main: the cap took the named rows.
#[test]
fn owed1184_b_the_row_cap_bounds_the_unreferenced_rows_and_keeps_the_named() {
    let db = migrated();
    let mut named = Vec::new();
    for i in 0..3 {
        let s = txid("5e", i);
        pointer(&db, &txid("b1", i), &s);
        // the named rows are the OLDEST: the cap's order would take them first
        bytes(&db, &s, 1_000 + i);
        named.push(s);
    }
    for i in 0..5 {
        bytes(&db, &txid("5f", i), 2_000 + i);
    }
    assert_eq!(
        swept(&db, OWED_SPENDER_BYTES_CAP_SWEEP_SQL, 2, 256),
        vec![txid("5f", 0), txid("5f", 1), txid("5f", 2)],
        "the three oldest strangers beyond the ceiling"
    );
    let mut want = named.clone();
    want.extend([txid("5f", 3), txid("5f", 4)]);
    want.sort();
    assert_eq!(held(&db), want);
    assert!(swept(&db, OWED_SPENDER_BYTES_CAP_SWEEP_SQL, 2, 256).is_empty());
}

fn plan(db: &Connection, sql: &str) -> Vec<String> {
    let mut stmt = db.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
    let binds = vec![0i64; stmt.parameter_count()];
    stmt.query_map(params_from_iter(binds), |r| r.get(3))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

/// (c) #638: neither sweep scans `pot_records` on the fleet-shaped fixture (plus 2,000 stored spenders, half of them
/// named by a hop row): the pointer test is a seek of `idx_pot_spending` per candidate. The sweeps' own walk of
/// `owed_spender_bytes` (by age, by the cap's order) is unchanged from main and bounded by the store's ceiling.
#[test]
fn owed1184_c_the_sweeps_seek_the_pointer_and_never_scan_pot_records() {
    let db = migrated();
    db.execute_batch("BEGIN").unwrap();
    for sql in d1_fleet::rows(1_790_000_000) {
        d1_fleet::execute_row(&db, &sql);
    }
    for i in 0..2_000 {
        let s = txid("5d", i);
        if i % 2 == 0 {
            pointer(&db, &txid("b0", i), &s);
        }
        bytes(&db, &s, NOW + i);
    }
    db.execute_batch("COMMIT").unwrap();
    for sql in [
        OWED_SPENDER_BYTES_SWEEP_SQL,
        OWED_SPENDER_BYTES_CAP_SWEEP_SQL,
    ] {
        let p = plan(&db, sql);
        eprintln!("owed1184_c plan: {p:?}");
        assert!(
            !p.iter().any(|d| d.starts_with("SCAN pot_records")
                || d == "SCAN p"
                || d.starts_with("SCAN p ")),
            "the sweep scans pot_records: {p:?}"
        );
        assert!(
            p.iter().any(|d| d.contains("idx_pot_spending")),
            "the pointer test seeks idx_pot_spending: {p:?}"
        );
    }
}
