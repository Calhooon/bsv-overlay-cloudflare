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
    // the latches an existing database holds were written by the writer of their day (before bsv-low #621's pair)
    for i in 0..500u32 {
        db.execute(
            "INSERT OR IGNORE INTO network_seen (txid, seenAt) VALUES (lower(?), unixepoch())",
            [format!("AB{i:062X}")],
        )
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
        db.execute(NETWORK_SEEN_INSERT_SQL, rusqlite::params![txid, "arcade", 1_i64]).unwrap();
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
            details.iter().any(|d| d.starts_with("SEARCH ")
                && d.ends_with(&format!("USING INDEX sqlite_autoindex_{store}_1 (txid=?)"))),
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

/// NS-COST-BAR-1: one trigger-kept count row, served instead of a `COUNT(*)`. Proves on an EXISTING database (rows
/// written before the count's migrations, by `seed_old`) and on the fleet fixture that the served value is the
/// count, that a re-run of the whole list scans nothing at the backfills (`backfills`, by prefix), and that the read
/// is the row's key alone.
fn assert_count_row(
    what: &str,
    served_sql: &str,
    count_sql: &str,
    first_new: &str,
    seed_old: &dyn Fn(&Connection),
    backfills: &[&str],
) {
    let read = |db: &Connection, sql: &str| -> i64 {
        db.query_row(sql, [], |r| r.get::<_, f64>(0).map(|v| v as i64))
            .unwrap()
    };
    let at = OVERLAY_MIGRATIONS
        .iter()
        .position(|s| s.starts_with(first_new))
        .unwrap_or_else(|| panic!("{what}: {first_new}"));
    let db = Connection::open_in_memory().unwrap();
    migrate(&db, &OVERLAY_MIGRATIONS[..at]);
    seed_old(&db);
    let held = read(&db, count_sql);
    assert!(held > 0, "{what}: the old rows hold some");
    migrate(&db, OVERLAY_MIGRATIONS);
    assert_eq!(
        (read(&db, served_sql), read(&db, count_sql)),
        (held, held),
        "{what}: the backfill counts what was held"
    );
    let db = fixture();
    let n = read(&db, count_sql);
    assert!(n > 0, "{what}: the fixture holds some");
    assert_eq!(read(&db, served_sql), n, "{what}: the fixture");
    let mut replays = Vec::new();
    for sql in OVERLAY_MIGRATIONS {
        let Ok(mut stmt) = db.prepare(sql) else {
            replays.push(0);
            continue;
        };
        let _ = stmt.raw_execute();
        replays.push(stmt.get_status(rusqlite::StatementStatus::FullscanStep));
    }
    assert_eq!(
        read(&db, served_sql),
        n,
        "{what}: the re-run keeps the count"
    );
    for prefix in backfills {
        let i = OVERLAY_MIGRATIONS
            .iter()
            .position(|s| s.starts_with(prefix))
            .unwrap_or_else(|| panic!("{what}: the backfill {prefix} ships"));
        assert!(i > at, "{what}: {prefix} after the triggers");
        assert_eq!(replays[i], 0, "{what}: {prefix} scans nothing on a re-run");
    }
    let details = plan(&db, served_sql);
    println!("{what} plan: {details:?}");
    assert_eq!(details.len(), 1, "{what}: one step: {details:?}");
    assert!(
        details[0].starts_with("SEARCH count_")
            && details[0].ends_with("USING INTEGER PRIMARY KEY (rowid=?)"),
        "{what}: the read is the one row: {details:?}"
    );
    assert_no_fleet_scan(what, &details);
}

#[test]
fn ns_cost_unprovable_total_is_the_count_and_the_read_counts_nothing() {
    assert_count_row(
        "retiredUnprovableTotal",
        bsv_overlay_cloudflare::ops::RETIRED_UNPROVABLE_TOTAL_SQL,
        "SELECT COUNT(*) FROM pot_beefs WHERE structurally_unprovable = 1",
        "CREATE TABLE IF NOT EXISTS pot_beefs_unprovable_set",
        &|db| {
            for i in 0..300u32 {
                db.execute(
                    "INSERT INTO pot_beefs (txid, beef, has_proof, structurally_unprovable) VALUES (?1, x'00', 0, ?2)",
                    rusqlite::params![format!("ab{i:062x}"), [Some(1), Some(0), None][i as usize % 3]],
                )
                .unwrap();
            }
        },
        &[
            "INSERT OR IGNORE INTO pot_beefs_unprovable_set",
            "INSERT OR IGNORE INTO count_pot_beefs_unprovable",
        ],
    );
}

#[test]
fn ns_cost_retired_totals_are_the_counts_and_the_reads_count_nothing() {
    let seed = |db: &Connection| {
        for i in 0..300u32 {
            let (ms, reason) = [
                (Some(1i64), Some("expired-unproven")),
                (Some(1), Some("utxo-spent")),
                (Some(1), None),
                (None, Some("expired-unproven")),
            ][i as usize % 4];
            db.execute(
                "INSERT INTO transactions (txid, beef, has_proof, retired_ms, retired_reason) VALUES (?1, x'00', 0, ?2, ?3)",
                rusqlite::params![format!("ac{i:062x}"), ms, reason],
            )
            .unwrap();
        }
    };
    let backfills = [
        "INSERT OR IGNORE INTO transactions_retired_set",
        "INSERT OR IGNORE INTO count_transactions_retired",
    ];
    assert_count_row(
        "txRetiredTotal",
        bsv_overlay_cloudflare::ops::TX_RETIRED_TOTAL_SQL,
        "SELECT COUNT(*) FROM transactions WHERE retired_ms IS NOT NULL",
        "CREATE TABLE IF NOT EXISTS transactions_retired_set",
        &seed,
        &backfills,
    );
    assert_count_row(
        "expiredUnproven",
        bsv_overlay_cloudflare::proof_window::EXPIRED_COUNT_SQL,
        "SELECT COUNT(*) FROM transactions WHERE retired_ms IS NOT NULL AND retired_reason = 'expired-unproven'",
        "CREATE TABLE IF NOT EXISTS transactions_retired_set",
        &seed,
        &backfills,
    );
}

/// The index janitor's backlog before NS-COST-BAR-1.
const INDEX_JANITOR_BACKLOG_BEFORE: &str = "SELECT lockKind, COUNT(*) AS c FROM pot_records \
     WHERE spent = 0 AND (createdAt IS NULL OR createdAt <= unixepoch() - 3600) GROUP BY lockKind";

/// The served three figures, folded as `ops::index_janitor_backlog` folds its rows.
fn backlog_fold(db: &Connection, sql: &str) -> (i64, i64, i64) {
    let mut stmt = db.prepare(sql).unwrap();
    let mut out = (0, 0, 0);
    for row in stmt
        .query_map([], |r| {
            Ok((r.get::<_, Option<String>>(0)?, r.get::<_, f64>(1)? as i64))
        })
        .unwrap()
    {
        let (kind, n) = row.unwrap();
        match kind.as_deref() {
            Some("covenant") => out.0 += n.max(0),
            Some("p2pkh") => out.1 += n.max(0),
            _ => out.2 += n.max(0),
        }
    }
    out
}

#[test]
fn ns_cost_index_janitor_backlog_is_the_old_figure_and_reads_the_hour_alone() {
    use bsv_overlay_cloudflare::ops::{INDEX_JANITOR_BACKLOG_SQL, UNSPENT_KIND_BACKFILL};
    let shapes = |db: &Connection| {
        // Young rows (this hour), an hour old to the second, NULL ages, and the classes' every spelling.
        db.execute_batch(
            "UPDATE pot_records SET createdAt = unixepoch() WHERE spent = 0 AND rowid % 7 = 0; \
             UPDATE pot_records SET createdAt = unixepoch() - 3600 WHERE spent = 0 AND rowid % 11 = 0; \
             UPDATE pot_records SET createdAt = NULL WHERE spent = 0 AND rowid % 13 = 0; \
             UPDATE pot_records SET lockKind = 'p2pkh' WHERE spent = 0 AND rowid % 5 = 0; \
             UPDATE pot_records SET lockKind = NULL WHERE spent = 0 AND rowid % 17 = 0; \
             UPDATE pot_records SET lockKind = 'Covenant' WHERE spent = 0 AND rowid % 19 = 0;",
        )
        .unwrap();
    };
    // An EXISTING database: its pots were written before the count's migrations.
    let at = OVERLAY_MIGRATIONS
        .iter()
        .position(|s| s.starts_with("CREATE TABLE IF NOT EXISTS count_pot_unspent_kind"))
        .unwrap();
    let db = Connection::open_in_memory().unwrap();
    migrate(&db, &OVERLAY_MIGRATIONS[..at]);
    for i in 0..400u32 {
        db.execute(
            "INSERT INTO pot_records (txid, outputIndex, spent, createdAt, lockKind) VALUES (?1, 0, ?2, 1700000000, 'covenant')",
            rusqlite::params![format!("ad{i:062x}"), i % 3 == 0],
        )
        .unwrap();
    }
    shapes(&db);
    let held = backlog_fold(&db, INDEX_JANITOR_BACKLOG_BEFORE);
    migrate(&db, OVERLAY_MIGRATIONS);
    assert_eq!(
        backlog_fold(&db, INDEX_JANITOR_BACKLOG_SQL),
        held,
        "the backfill"
    );
    assert!(held.0 > 0 && held.1 > 0 && held.2 > 0, "{held:?}");
    // The fleet fixture.
    let db = fixture();
    shapes(&db);
    let before = backlog_fold(&db, INDEX_JANITOR_BACKLOG_BEFORE);
    assert!(before.0 > 0 && before.1 > 0 && before.2 > 0, "{before:?}");
    assert_eq!(
        backlog_fold(&db, INDEX_JANITOR_BACKLOG_SQL),
        before,
        "the fleet fixture"
    );
    // A re-run of the list: the stamped backfill scans nothing and the figures hold.
    let mut stmt = db.prepare(UNSPENT_KIND_BACKFILL).unwrap();
    stmt.raw_execute().unwrap();
    assert_eq!(
        stmt.get_status(rusqlite::StatementStatus::FullscanStep),
        0,
        "the stamped backfill scans nothing on a re-run"
    );
    assert!(
        stmt.get_status(rusqlite::StatementStatus::VmStep) < 100,
        "nor steps through the pots"
    );
    drop(stmt);
    migrate(&db, OVERLAY_MIGRATIONS);
    assert_eq!(
        backlog_fold(&db, INDEX_JANITOR_BACKLOG_SQL),
        before,
        "the re-run"
    );
    let details = plan(&db, INDEX_JANITOR_BACKLOG_SQL);
    println!("backlog plan: {details:?}");
    assert_no_fleet_scan("backlog", &details);
    assert!(
        details
            .iter()
            .any(|d| d.contains("USING COVERING INDEX idx_pot_unspent_created (createdAt>?)")),
        "the young rows are the hour's index range: {details:?}"
    );
    assert!(
        !details
            .iter()
            .any(|d| d.contains("idx_pot_spent_unconfirmed")
                || d.contains("idx_pot_unspent_recovery")),
        "never every unspent pot: {details:?}"
    );
    assert_no_fleet_scan_analyzed(&db, "backlog", INDEX_JANITOR_BACKLOG_SQL);
    assert!(
        plan(&db, INDEX_JANITOR_BACKLOG_SQL)
            .iter()
            .any(|d| d.contains("idx_pot_unspent_created")),
        "the range holds under statistics"
    );
}

#[test]
fn ns_cost_relatch_censuses_are_the_old_aggregate_and_read_no_whole_table() {
    use bsv_overlay_cloudflare::d1_discovery as d;
    let db = fixture();
    // Every table's legacy NULL tier, both latched tiers, and rowid gaps (a deleted row) on rows no other pin reads.
    db.execute_batch(
        "UPDATE potparty_records SET sigValid = CASE rowid % 3 WHEN 0 THEN NULL WHEN 1 THEN 0 ELSE 1 END WHERE rowid % 5 = 0; \
         UPDATE hopparty_records SET markerValid = CASE rowid % 3 WHEN 0 THEN NULL WHEN 1 THEN 0 ELSE 1 END WHERE rowid % 5 = 0; \
         DELETE FROM potparty_records WHERE rowid % 997 = 0; \
         DELETE FROM hopparty_records WHERE rowid % 991 = 0;",
    )
    .unwrap();
    for i in 0..500u32 {
        db.execute(
            "INSERT INTO result_markers_v2 (gameId, winner, loser, potTxid, settleTxid, winnerSigHex, loserSigHex, cardsHex, txid, outputIndex, createdAt, claimValid) VALUES (?1, 'w', 'l', ?1, ?1, '00', '00', '00', ?1, 0, 1, ?2)",
            rusqlite::params![format!("af{i:062x}"), [None, Some(0), Some(1)][i as usize % 3]],
        )
        .unwrap();
        db.execute(
            "INSERT INTO hand_markers (gameId, identity, potTxid, cardsHex, txid, outputIndex, sigHex, createdAt, rowValid) VALUES (?1, 'i', ?1, '00', ?1, 0, '00', 1, ?2)",
            rusqlite::params![format!("ae{i:062x}"), [None, Some(0), Some(1)][i as usize % 3]],
        )
        .unwrap();
    }
    for (table, column, sql) in [
        (
            "potparty_records",
            "sigValid",
            d::potparty_relatch_census_sql(),
        ),
        (
            "hopparty_records",
            "markerValid",
            d::hopparty_relatch_census_sql(),
        ),
        (
            "result_markers_v2",
            "claimValid",
            d::result_relatch_census_sql(),
        ),
        ("hand_markers", "rowValid", d::hand_relatch_census_sql()),
    ] {
        let old = format!(
            "SELECT COALESCE(SUM(CASE WHEN rowid > ?1 THEN 1 ELSE 0 END), 0) AS remaining, \
                    COALESCE(SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END), 0) AS stillNull FROM {table}"
        );
        let max: i64 = db
            .query_row(&format!("SELECT MAX(rowid) FROM {table}"), [], |r| r.get(0))
            .unwrap();
        let mut nulls = 0;
        for cursor in [
            -1,
            0,
            1,
            2,
            63,
            64,
            max / 2,
            max - 1,
            max,
            max + 1,
            i64::MAX,
        ] {
            let new = rows(&db, &sql, &[Value::Integer(cursor)]);
            assert_eq!(
                new,
                rows(&db, &old, &[Value::Integer(cursor)]),
                "{table} at {cursor}"
            );
            nulls = db
                .query_row(&sql, [cursor], |r| r.get::<_, i64>(1))
                .unwrap();
        }
        assert!(nulls > 0, "{table}: the fixture holds a NULL tier");
        let details = plan(&db, &sql);
        println!("{table} census plan: {details:?}");
        assert!(
            details
                .iter()
                .all(|d| !d.starts_with("SCAN ") || d == "SCAN CONSTANT ROW"),
            "{table}: no whole-table read: {details:?}"
        );
        assert!(
            details
                .iter()
                .any(|d| d == &format!("SEARCH {table} USING INTEGER PRIMARY KEY (rowid>?)")),
            "{table}: the sweep's rows are a rowid range: {details:?}"
        );
        assert!(
            details
                .iter()
                .any(|d| d.contains("_null (") && d.contains("USING COVERING INDEX")),
            "{table}: the NULL tier is its partial index: {details:?}"
        );
        assert_no_fleet_scan_analyzed(&db, table, &sql);
    }
}

/// bsv-low #470 PRE557 lens fold (HIGH-1): the stamp step's candidate read runs on every scheduled pass. On the
/// fleet fixture (every pot unstamped, the first deploy's shape) and again with every pot stamped but a few, it
/// walks the partial index of the unstamped rows in its two windows and probes `pot_beefs` by key: never a pot
/// table whole, never `pot_beefs` by its latch, under statistics too.
#[test]
fn pre557_the_stamp_candidates_read_two_windows_of_the_null_index() {
    use bsv_overlay_cloudflare::d1_discovery::{
        POT_BLOCK_TIME_SCAN_ROWS, VERIFIED_POTS_WITHOUT_BLOCK_TIME_SQL as SQL,
    };
    let check_plan = |db: &Connection, what: &str| {
        let details = plan(db, SQL);
        println!("{what} plan: {details:?}");
        for d in &details {
            assert!(
                !d.starts_with("SCAN")
                    || d == "SCAN c"
                    || d.contains("subquery")
                    || d.contains("idx_pot_records_block_time_null"),
                "{what}: a fleet scan: {d} (plan {details:?})"
            );
            assert!(
                !d.starts_with("SEARCH b") || d.contains("(txid=?)"),
                "{what}: pot_beefs walked by its latch: {d} (plan {details:?})"
            );
        }
    };
    let steps = |db: &Connection| {
        let mut stmt = db.prepare(SQL).unwrap();
        let mut rows = stmt
            .query(rusqlite::params![
                "80",
                i64::from(POT_BLOCK_TIME_SCAN_ROWS),
                48
            ])
            .unwrap();
        let mut n = 0;
        while rows.next().unwrap().is_some() {
            n += 1;
        }
        drop(rows);
        (n, stmt.get_status(rusqlite::StatementStatus::FullscanStep))
    };
    let db = fixture();
    let pots: i64 = db
        .query_row("SELECT COUNT(*) FROM pot_records", [], |r| r.get(0))
        .unwrap();
    assert!(pots > 1_000, "the fleet fixture: {pots} pot rows");
    check_plan(&db, "unstamped fleet");
    let (n, scan) = steps(&db);
    println!("unstamped fleet: {n} candidates, {scan} scan steps of {pots} pot rows");
    assert!(
        scan <= 2 * POT_BLOCK_TIME_SCAN_ROWS as i32 + 2,
        "two windows of the index: {scan}"
    );
    // the steady state: every pot stamped but a few
    db.execute_batch("UPDATE pot_records SET blockTime = 1 WHERE rowid % 50 <> 0")
        .unwrap();
    let (_, scan) = steps(&db);
    assert!(
        scan <= 2 * POT_BLOCK_TIME_SCAN_ROWS as i32 + 2,
        "steady state: {scan}"
    );
    db.execute_batch("ANALYZE").unwrap();
    check_plan(&db, "after ANALYZE");
    let (_, scan) = steps(&db);
    assert!(
        scan <= 2 * POT_BLOCK_TIME_SCAN_ROWS as i32 + 2,
        "under statistics: {scan}"
    );
}
