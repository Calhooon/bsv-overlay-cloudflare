//! bsv-low #643, #644 (W1.6): the D1 read bounds on the hot passes, on the fleet-shaped fixture.
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
