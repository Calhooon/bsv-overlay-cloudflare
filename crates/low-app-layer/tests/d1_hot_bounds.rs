//! bsv-low #643, #644, #645 (W1.6): the D1 read bounds on the hot passes, on the fleet-shaped fixture.
//!
//! Each statement keeps the #638 pins: its answer is the answer of the same SQL over the old plan (the table read
//! `NOT INDEXED`, so the row set cannot depend on the new index), its plan reads no fleet table whole, and its
//! measured rows read on local D1 sit under a ceiling (`support/cursor_measure.rs`, `tools/lane-499/cursor_pin.mjs`).
use bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS;
use rusqlite::{params_from_iter, types::Value, Connection};

#[path = "support/d1_fleet.rs"]
mod d1_fleet;

fn migrate(db: &Connection, list: &[&str]) {
    for sql in list {
        if let Err(e) = db.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"), "{e}: {sql}");
        }
    }
}

fn fixture() -> Connection {
    let db = Connection::open_in_memory().unwrap();
    migrate(&db, OVERLAY_MIGRATIONS);
    db.execute_batch("BEGIN").unwrap();
    for sql in d1_fleet::rows(1_790_000_000) {
        d1_fleet::execute_row(&db, &sql);
    }
    db.execute_batch("COMMIT").unwrap();
    db
}

fn plan(db: &Connection, sql: &str) -> Vec<String> {
    let mut stmt = db.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
    let binds = vec![Value::Integer(0); stmt.parameter_count()];
    stmt.query_map(params_from_iter(binds), |r| r.get(3))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn rows(db: &Connection, sql: &str, binds: &[Value]) -> Vec<String> {
    let mut stmt = db.prepare(sql).unwrap();
    let n = stmt.column_count();
    stmt.query_map(params_from_iter(binds.iter()), |r| {
        Ok((0..n)
            .map(|i| format!("{:?}", r.get_ref(i).unwrap()))
            .collect::<Vec<_>>()
            .join("|"))
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

/// The plan again once the planner has statistics (D1 may hold a `sqlite_stat1`): still no fleet scan.
fn assert_no_fleet_scan_analyzed(db: &Connection, what: &str, sql: &str) {
    db.execute_batch("ANALYZE").unwrap();
    let details = plan(db, sql);
    println!("{what} plan after ANALYZE: {details:?}");
    assert_no_fleet_scan(what, &details);
}

fn assert_no_fleet_scan(what: &str, details: &[String]) {
    for d in details {
        let name = d
            .strip_prefix("SCAN ")
            .and_then(|s| s.split_whitespace().next());
        assert!(
            !name.is_some_and(|n| ["pot_records", "p", "pp", "network_seen"].contains(&n)),
            "{what}: a fleet scan: {d} (plan {details:?})"
        );
    }
}

/// Every boundary of the two backfill passes' predicates, on rows no identity view reads (vout 1).
fn seed_backfill_shapes(db: &Connection) {
    let pkh = "11".repeat(20);
    for (
        i,
        (verdict, verdict_txid_is_spender, signers, spender, pay_a, pay_b, pkh_a, pkh_b, spent_at),
    ) in [
        (
            Some("A"),
            true,
            None,
            true,
            None,
            None,
            true,
            true,
            "unixepoch()",
        ),
        (
            Some("B"),
            true,
            None,
            true,
            Some(1),
            None,
            true,
            false,
            "unixepoch() - 7200",
        ),
        (
            Some("A"),
            false,
            None,
            true,
            None,
            Some(1),
            false,
            true,
            "NULL",
        ),
        (
            Some("A"),
            true,
            Some("both"),
            true,
            Some(1),
            Some(1),
            true,
            true,
            "unixepoch()",
        ),
        (
            Some("A"),
            true,
            Some("unresolved"),
            false,
            None,
            None,
            true,
            true,
            "NULL",
        ),
        (
            None,
            true,
            None,
            true,
            None,
            None,
            false,
            false,
            "unixepoch() - 7200",
        ),
        (
            None,
            true,
            None,
            true,
            None,
            None,
            true,
            false,
            "unixepoch()",
        ),
        (
            Some("draw"),
            true,
            None,
            true,
            None,
            None,
            false,
            true,
            "unixepoch() - 7200",
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let txid = format!("f6{i:062x}");
        let spender = spender.then(|| format!("f7{i:062x}"));
        let q = |v: Option<String>| v.map_or("NULL".to_string(), |s| format!("'{s}'"));
        let n = |v: Option<i64>| v.map_or("NULL".to_string(), |s| s.to_string());
        let verdict_txid = if verdict_txid_is_spender {
            spender.clone()
        } else {
            Some("ff".repeat(32))
        };
        db.execute_batch(&format!(
            "INSERT INTO pot_records (txid, outputIndex, spent, spendingTxid, spentAt, verdict, verdictTxid, settleSigners, spenderPayASats, spenderPayBSats, payPkhA, payPkhB, createdAt) \
             VALUES ('{txid}', 1, {}, {}, {spent_at}, {}, {}, {}, {}, {}, {}, {}, 1)",
            u8::from(spender.is_some()),
            q(spender.clone()),
            q(verdict.map(str::to_string)),
            q(verdict.and(verdict_txid)),
            q(signers.map(str::to_string)),
            n(pay_a),
            n(pay_b),
            q(pkh_a.then(|| pkh.clone())),
            q(pkh_b.then(|| pkh.clone())),
        ))
        .unwrap();
    }
}

#[test]
fn w16_644_the_backfill_candidates_are_the_old_set_and_read_no_whole_table() {
    use bsv_overlay_cloudflare::d1_discovery::{
        settle_signers_candidates_sql, spender_payouts_candidates_sql,
    };
    let db = fixture();
    seed_backfill_shapes(&db);
    for (name, sql) in [
        ("signers", settle_signers_candidates_sql(1_000_000)),
        ("payouts", spender_payouts_candidates_sql(1_000_000)),
    ] {
        let old = sql.replacen("FROM pot_records", "FROM pot_records NOT INDEXED", 1);
        assert_ne!(old, sql);
        let (new_rows, old_rows) = (sorted(rows(&db, &sql, &[])), sorted(rows(&db, &old, &[])));
        println!("{name}: {} candidates on the fixture", new_rows.len());
        assert!(!new_rows.is_empty(), "{name}: the fixture holds candidates");
        assert_eq!(
            new_rows, old_rows,
            "{name}: the candidate set is the old plan's"
        );
        // The production limit (`proof_fetcher` passes 20 a tick; the route 4): the same set's head.
        for limit in [4, 20] {
            let shipped = rows(
                &db,
                &sql.replace("LIMIT 1000000", &format!("LIMIT {limit}")),
                &[],
            );
            assert_eq!(shipped.len(), new_rows.len().min(limit));
            assert!(shipped.iter().all(|r| new_rows.contains(r)));
        }
        let details = plan(&db, &sql);
        println!("{name} plan: {details:?}");
        assert_no_fleet_scan(name, &details);
        let mut stmt = db.prepare(&sql).unwrap();
        let _ = stmt.query_map([], |_| Ok(())).unwrap().count();
        assert_eq!(
            stmt.get_status(rusqlite::StatementStatus::FullscanStep),
            0,
            "{name}: no full-scan step"
        );
    }
    for (name, sql) in [
        ("signers", settle_signers_candidates_sql(20)),
        ("payouts", spender_payouts_candidates_sql(20)),
    ] {
        assert_no_fleet_scan_analyzed(&db, name, &sql);
    }
}

#[test]
fn w16_643_the_tip_mark_reads_only_the_pots_the_block_reaches() {
    use low_app_layer::owed::OWED_STALE_ON_TIP_SQL;
    let db = fixture();
    // Spread the fixture's unspent pots over heights so a block's window holds a few (the rest stay at 958846).
    db.execute_batch(
        "UPDATE pot_records SET recoveryHeight = 958700 + (rowid % 300) WHERE spent = 0 AND rowid % 5 = 0; \
         UPDATE pot_records SET recoveryHeight = NULL WHERE spent = 0 AND rowid % 97 = 0",
    )
    .unwrap();
    let at = OWED_STALE_ON_TIP_SQL.find("IN (SELECT").unwrap() + 4;
    let select = &OWED_STALE_ON_TIP_SQL[at..OWED_STALE_ON_TIP_SQL.len() - 1];
    assert!(select
        .starts_with("SELECT DISTINCT pp.identity FROM potparty_records pp JOIN pot_records p "));
    let old = select.replacen("JOIN pot_records p ", "JOIN pot_records p NOT INDEXED ", 1);
    let mut total = 0;
    for tip in [
        958_699i64, 958_700, 958_705, 958_846, 958_852, 958_853, 958_999, 959_100,
    ] {
        let new_rows = sorted(rows(&db, select, &[Value::Integer(tip)]));
        assert_eq!(
            new_rows,
            sorted(rows(&db, &old, &[Value::Integer(tip)])),
            "tip {tip}: the identities marked are the old plan's"
        );
        total += new_rows.len();
    }
    assert!(total > 0, "the windows name identities");
    let details = plan(&db, select);
    println!("tip mark plan: {details:?}");
    assert_no_fleet_scan("tip mark", &details);
    assert!(
        details.iter().any(|d| d
            == "SEARCH p USING INDEX idx_pot_unspent_recovery (spent=? AND recoveryHeight>? AND recoveryHeight<?)"),
        "the block's window is an index range, never every unspent pot: {details:?}"
    );
    // The UPDATE itself runs the same subquery plan.
    let update = plan(&db, OWED_STALE_ON_TIP_SQL);
    assert!(
        update
            .iter()
            .any(|d| d.contains("idx_pot_unspent_recovery")),
        "{update:?}"
    );
    assert_no_fleet_scan_analyzed(&db, "tip mark", select);
    assert!(
        plan(&db, select)
            .iter()
            .any(|d| d.contains("idx_pot_unspent_recovery")),
        "the range holds under statistics"
    );
}

#[test]
fn w16_645_the_seen_total_is_the_count_and_the_read_counts_nothing() {
    use bsv_overlay_cloudflare::ops::{NETWORK_SEEN_INSERT_SQL, NETWORK_SEEN_TOTAL_SQL};
    let count = |db: &Connection| -> i64 {
        db.query_row("SELECT COUNT(*) FROM network_seen", [], |r| r.get(0))
            .unwrap()
    };
    let served = |db: &Connection| -> i64 {
        db.query_row(NETWORK_SEEN_TOTAL_SQL, [], |r| {
            r.get::<_, f64>(0).map(|v| v as i64)
        })
        .unwrap()
    };
    // An EXISTING database: its latches were written before the counter's migrations.
    let at = OVERLAY_MIGRATIONS
        .iter()
        .position(|s| s.contains("CREATE TABLE IF NOT EXISTS network_seen "))
        .unwrap();
    let db = Connection::open_in_memory().unwrap();
    migrate(&db, &OVERLAY_MIGRATIONS[..=at]);
    for i in 0..500u32 {
        db.execute(NETWORK_SEEN_INSERT_SQL, [format!("AB{i:062X}")])
            .unwrap();
    }
    migrate(&db, OVERLAY_MIGRATIONS);
    assert_eq!(
        (served(&db), count(&db)),
        (500, 500),
        "the backfill counts what was held"
    );
    // The fleet fixture, then the latch's own write-once shape (a repeat, any case, is no new row).
    let db = fixture();
    assert_eq!(served(&db), count(&db));
    let before = count(&db);
    for txid in [
        "e4".repeat(32),
        "AA".repeat(32),
        "aa".repeat(32),
        "bb".repeat(32),
    ] {
        db.execute(NETWORK_SEEN_INSERT_SQL, [txid]).unwrap();
        assert_eq!(served(&db), count(&db));
    }
    assert_eq!(
        count(&db),
        before + 3,
        "e4e4.., aa.. (AA.. is its repeat) and bb.."
    );
    // A delete (none ships today; a future reorg clear or eviction move) keeps it exact.
    db.execute(
        "DELETE FROM network_seen WHERE txid = ?1",
        ["aa".repeat(32)],
    )
    .unwrap();
    db.execute("DELETE FROM network_seen WHERE txid LIKE 'e40000%'", [])
        .unwrap();
    assert_eq!(served(&db), count(&db));
    // A re-run of the whole list (a new version) neither recounts nor drifts.
    let mut replays = Vec::new();
    for sql in OVERLAY_MIGRATIONS {
        let Ok(mut stmt) = db.prepare(sql) else {
            replays.push(0); // a re-run ALTER: the duplicate column the runner tolerates
            continue;
        };
        let _ = stmt.raw_execute();
        replays.push(stmt.get_status(rusqlite::StatementStatus::FullscanStep));
    }
    assert_eq!(served(&db), count(&db), "the re-run keeps the count");
    let backfill = OVERLAY_MIGRATIONS
        .iter()
        .position(|s| s.starts_with("INSERT OR IGNORE INTO count_network_seen"))
        .expect("the backfill ships");
    assert_eq!(
        replays[backfill], 0,
        "the stamped backfill scans nothing on a re-run"
    );
    let details = plan(&db, NETWORK_SEEN_TOTAL_SQL);
    println!("seen total plan: {details:?}");
    assert_eq!(
        details,
        ["SEARCH count_network_seen USING INTEGER PRIMARY KEY (rowid=?)"],
        "the read is the one row, never network_seen"
    );
    assert_no_fleet_scan("seen total", &details);
}

// ── NS-COST-BAR-1 (the owner's cost word of 2026-10-18): the per-tick whole-table reads of the 24-hour census ──

/// The proofless-watch GC before NS-COST-BAR-1: four filtered lists built over the whole stores every tick.
const PROOFLESS_WATCH_GC_BEFORE: &str = "DELETE FROM proofless_watch WHERE \
     txid IN (SELECT txid FROM pot_beefs WHERE has_proof = 1) OR \
     txid IN (SELECT txid FROM transactions WHERE has_proof = 1) OR \
     txid IN (SELECT txid FROM pot_beefs WHERE structurally_unprovable = 1) OR \
     txid IN (SELECT txid FROM transactions WHERE retired_ms IS NOT NULL) OR \
     (txid NOT IN (SELECT txid FROM pot_beefs) AND \
      txid NOT IN (SELECT txid FROM transactions))";

/// The rows a DELETE would take, as a SELECT of the same predicate (the read the local-D1 harness measures).
fn gc_as_select(delete: &str) -> String {
    delete.replacen(
        "DELETE FROM proofless_watch WHERE",
        "SELECT txid FROM proofless_watch WHERE",
        1,
    )
}

fn watch_after(db: &Connection, gc: &str) -> Vec<String> {
    db.execute_batch("SAVEPOINT gc").unwrap();
    db.execute(gc, []).unwrap();
    let left = sorted(rows(db, "SELECT txid FROM proofless_watch", &[]));
    db.execute_batch("ROLLBACK TO gc; RELEASE gc").unwrap();
    left
}

#[test]
fn ns_cost_gc_deletes_the_old_rows_and_probes_the_stores_by_key() {
    use bsv_overlay_cloudflare::ops::PROOFLESS_WATCH_GC_SQL;
    let db = fixture();
    let before = watch_after(&db, PROOFLESS_WATCH_GC_BEFORE);
    let after = watch_after(&db, PROOFLESS_WATCH_GC_SQL);
    let held = sorted(rows(&db, "SELECT txid FROM proofless_watch", &[]));
    assert_eq!(
        after, before,
        "the shipped GC keeps exactly the old GC's rows"
    );
    assert!(
        !after.is_empty() && after.len() < held.len(),
        "the fixture exercises both: {} kept of {}",
        after.len(),
        held.len()
    );
    // Every arm's edge, on rows no other pin reads: both stores at once, a NULL latch, a proven row that is also
    // retired, a txid in neither store, and the stores' own NULL keys (a NULL in an `IN` list is a false in the OR).
    db.execute_batch(
        "INSERT INTO pot_beefs (txid, beef, has_proof, structurally_unprovable) VALUES ('f9a', x'00', 0, NULL), ('f9b', x'00', 0, 0), ('f9c', x'00', 1, 1), ('f9d', x'00', 0, 2); \
         INSERT INTO transactions (txid, beef, has_proof, retired_ms) VALUES ('f9a', x'00', 0, NULL), ('f9b', x'00', 0, 5), ('f9e', x'00', 1, NULL), ('f9f', x'00', 0, NULL); \
         INSERT INTO pot_beefs (txid, beef, has_proof, structurally_unprovable) VALUES (NULL, x'00', 1, 1); \
         INSERT INTO proofless_watch (txid, first_seen_ms) VALUES ('f9a', 1), ('f9b', 1), ('f9c', 1), ('f9d', 1), ('f9e', 1), ('f9f', 1), ('f9g', 1), ('F9A', 1);",
    )
    .unwrap();
    assert_eq!(
        watch_after(&db, PROOFLESS_WATCH_GC_SQL),
        watch_after(&db, PROOFLESS_WATCH_GC_BEFORE),
        "the edges, with a NULL pot_beefs key (the orphan arm is NULL for every row)"
    );
    db.execute_batch("DELETE FROM pot_beefs WHERE txid IS NULL")
        .unwrap();
    let edges = watch_after(&db, PROOFLESS_WATCH_GC_SQL);
    assert_eq!(
        edges,
        watch_after(&db, PROOFLESS_WATCH_GC_BEFORE),
        "the edges"
    );
    // f9a: unproven in both; f9b: retired in transactions; f9c: proven and unprovable; f9d: a latch of 2 is not
    // 1; f9e: proven in transactions; f9f: live in transactions; f9g: in neither; F9A: case is a different key.
    for (txid, kept) in [
        ("f9a", true),
        ("f9b", false),
        ("f9c", false),
        ("f9d", true),
        ("f9e", false),
        ("f9f", true),
        ("f9g", false),
        ("F9A", false),
    ] {
        assert_eq!(
            edges.contains(&format!(
                "{:?}",
                rusqlite::types::ValueRef::Text(txid.as_bytes())
            )),
            kept,
            "{txid}: {edges:?}"
        );
    }
    let details = plan(&db, PROOFLESS_WATCH_GC_SQL);
    println!("gc plan: {details:?}");
    for d in &details {
        let name = d
            .strip_prefix("SCAN ")
            .and_then(|s| s.split_whitespace().next());
        assert!(
            name.is_none_or(|n| n == "proofless_watch"),
            "the GC scans the watch alone: {d} (plan {details:?})"
        );
        assert!(
            !d.contains("LIST SUBQUERY"),
            "no list is built over a store: {d} (plan {details:?})"
        );
    }
    for store in ["pot_beefs", "transactions"] {
        assert!(
            details
                .iter()
                .any(|d| d.starts_with(&format!("CORRELATED SCALAR SUBQUERY"))
                    || d.contains(&format!("sqlite_autoindex_{store}_1 (txid=?)"))),
            "{store} is probed by its key: {details:?}"
        );
    }
    assert!(
        plan(&db, PROOFLESS_WATCH_GC_BEFORE)
            .iter()
            .any(|d| d.contains("LIST SUBQUERY")),
        "the old shape built lists (the pin can tell them apart)"
    );
    // The read the harness measures is the same plan (the SELECT may walk the watch through its key's index).
    let measured = plan(&db, &gc_as_select(PROOFLESS_WATCH_GC_SQL));
    assert!(
        measured[0].starts_with("SCAN proofless_watch")
            && details[0].starts_with("SCAN proofless_watch")
    );
    assert_eq!(
        measured[1..],
        details[1..],
        "the measured SELECT is the DELETE's plan"
    );
    assert_no_fleet_scan_analyzed(&db, "gc", PROOFLESS_WATCH_GC_SQL);
}
