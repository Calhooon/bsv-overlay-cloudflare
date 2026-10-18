//! #375 — the pre-launch era write-off, the cross-view pieces:
//!
//! * `/pots-view` row-level behaviour under the cutoff (the one era-filtered
//!   view with no dedicated sqlite suite of its own);
//! * the ANCHOR UNIT pin against the real producer: every `createdAt` the
//!   era predicate anchors on is written by the overlay's
//!   `current_unix_seconds_i64()` — unix SECONDS — while the cutoff is MS,
//!   and the one `* 1000` conversion lives in
//!   [`low_app_layer::logic::era_filter_sql`]. If the writer ever changed
//!   unit, or a view anchored on a differently-uniited column, the
//!   at-cutoff boundary rows in the per-view suites would flip; this file
//!   pins the writer side directly so the failure names the boundary.
//!
//! The per-view drop/keep fixtures live beside each view's own suite
//! (`recovery_view_sqlite.rs`, `refund_view_sqlite.rs`, `live_view_sqlite.rs`,
//! `hops_view_sqlite.rs`, `results_window_sqlite.rs`,
//! `leaderboard_window_sqlite.rs` — all named
//! `the_written_off_era_is_dropped_and_the_unset_cutoff_is_inert`).

use bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS;
use bsv_overlay_cloudflare::d1_discovery::{
    store_record_sql, POT_BLOCK_TIME_SCAN_ROWS, POT_BLOCK_TIME_STAMP_SQL,
    VERIFIED_POTS_WITHOUT_BLOCK_TIME_SQL,
};
use low_app_layer::logic::{assemble_pots_view, pots_view_join_sql, Outpoint, PotsViewRow};
use overlay_discovery::pot::storage::BLOCK_TIME_STAMP_SLACK_SECS;
use rusqlite::{params, Connection};

fn production_schema_db() -> Connection {
    let conn = Connection::open_in_memory().expect("open in-memory sqlite");
    for sql in OVERLAY_MIGRATIONS {
        if let Err(e) = conn.execute_batch(sql) {
            let msg = e.to_string().to_ascii_lowercase();
            assert!(
                msg.contains("duplicate column"),
                "production migration failed under real SQLite: {e}\n{sql}"
            );
        }
    }
    conn
}

fn h64(seed: u8) -> String {
    format!("{seed:02x}").repeat(32)
}

/// Admit a bare pot row via the REAL `store_record_sql()` upsert.
fn admit_pot(conn: &Connection, txid: &str, created_at: i64) {
    conn.execute(
        store_record_sql(),
        params![
            txid,
            0i64,                   // outputIndex
            0i64,                   // spent
            Option::<String>::None, // spendingTxid
            0i64,                   // spentConfirmed
            created_at,             // createdAt
            Option::<String>::None, // lockKind
            Option::<String>::None, // pubA
            Option::<String>::None, // pubB
            Option::<String>::None, // pubTower
            Option::<String>::None, // payPkhA
            Option::<String>::None, // payPkhB
            Option::<String>::None, // rakePkh
            Option::<i64>::None,    // stakeA
            Option::<i64>::None,    // stakeB
            Option::<i64>::None,    // feeSats
            Option::<i64>::None,    // recoveryHeight
            Option::<i64>::None,    // potSats
            0i64,                   // paramsDecoded
        ],
    )
    .expect("store_record_sql");
}

/// Execute the shipped `/pots-view` join for two outpoints, either era arm,
/// binding exactly as the route does (outpoint pairs, then the cutoff LAST
/// iff set).
fn pots_view_rows(conn: &Connection, ops: &[Outpoint], era: Option<i64>) -> Vec<PotsViewRow> {
    let sql = pots_view_join_sql(ops.len(), era);
    let mut stmt = conn
        .prepare(&sql)
        .unwrap_or_else(|e| panic!("pots_view_join_sql({}, {era:?}): {e}\n{sql}", ops.len()));
    let mut binds: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
    for op in ops {
        binds.push(Box::new(op.db_txid()));
        binds.push(Box::new(i64::from(op.vout)));
    }
    if let Some(ms) = era {
        binds.push(Box::new(ms));
    }
    stmt.query_map(
        rusqlite::params_from_iter(binds.iter().map(|b| b.as_ref())),
        |r| {
            Ok(PotsViewRow {
                record: low_app_layer::logic::PotRecordRow {
                    txid: r.get("txid")?,
                    vout: r.get::<_, i64>("outputIndex")? as u32,
                    spent: r.get::<_, i64>("spent")? != 0,
                    spending_txid: r.get("spendingTxid")?,
                    spent_confirmed: r.get::<_, i64>("spentConfirmed")? != 0,
                    spender_final: None,
                    spender_seen: None,
                    funding_seen: None,
                },
                spender_beef_hex: r.get("spenderBeef")?,
            })
        },
    )
    .expect("query")
    .collect::<Result<Vec<_>, _>>()
    .expect("rows")
}

/// #375 through the SHIPPED `/pots-view` SQL: the pre-cutoff pot's row is
/// excluded, so the requested outpoint assembles as the fail-safe
/// `known:false` shape (never a Collect-able status); the at-cutoff pot is
/// KEPT (the `>=` boundary + the seconds→ms unit pin); `None` serves both.
#[test]
fn the_written_off_era_is_dropped_and_the_unset_cutoff_is_inert() {
    let conn = production_schema_db();
    const CUT_MS: i64 = 1_754_500_000_000;
    const CUT_SECS: i64 = CUT_MS / 1000;
    let pre = h64(0xd2);
    let post = h64(0xd3);
    admit_pot(&conn, &pre, CUT_SECS - 1);
    admit_pot(&conn, &post, CUT_SECS);
    let ops = [
        Outpoint {
            txid: pre.clone(),
            vout: 0,
        },
        Outpoint {
            txid: post.clone(),
            vout: 0,
        },
    ];

    let rows = pots_view_rows(&conn, &ops, Some(CUT_MS));
    assert_eq!(
        rows.len(),
        1,
        "only the at-cutoff pot's row survives the era filter"
    );
    assert_eq!(rows[0].record.txid, post, "the >= boundary keeps the row");
    // The route's assembler turns the dropped row into the fail-safe
    // known:false shape for its requested outpoint — never an error, never
    // a fabricated status.
    let entries = assemble_pots_view(&ops, &rows);
    assert!(!entries[0].status.known, "written-off pot answers unknown");
    assert!(entries[1].status.known, "kept pot answers as today");

    // The inert arm: both rows serve.
    let all = pots_view_rows(&conn, &ops, None);
    assert_eq!(all.len(), 2, "None serves every requested row");
}

/// THE UNIT PIN (#375): the writer whose stamps every era anchor compares
/// against — `current_unix_seconds_i64()`, the function `store_record_sql`'s
/// callers and every marker insert bind — produces unix SECONDS, not ms.
/// Bounds: after 2020-01-01 (1_577_836_800) and before the year ~5138
/// (100_000_000_000) — a millisecond stamp "now" is ~1.7e12 and fails the
/// upper bound, a microsecond stamp fails it harder, and a zero/negative
/// clock fails the lower. This is what licenses `era_filter_sql`'s single
/// `* 1000` as THE seconds→ms conversion.
#[test]
fn the_admission_stamp_writer_produces_unix_seconds() {
    let now = overlay_discovery::uhrp::storage::current_unix_seconds_i64();
    assert!(
        now > 1_577_836_800,
        "current_unix_seconds_i64 must be a post-2020 unix-seconds value, got {now}"
    );
    assert!(
        now < 100_000_000_000,
        "current_unix_seconds_i64 must be SECONDS (an ms stamp would be ~1.7e12), got {now}"
    );
}

// ── bsv-low #470 PRE557: a re-indexed pot is stamped from its confirming block, never NOW ──────────

/// The production cutoff (2026-08-08 01:26:29Z), ms, and a re-index's NOW (2026-10-18), seconds.
const PRE557_CUT_MS: i64 = 1_786_152_389_000;
const PRE557_NOW_SECS: i64 = 1_792_300_000;

/// The completion pass's stamp, the SHIPPED statement and binds (`D1PotStorage::stamp_pot_block_time`).
fn stamp_block_time(conn: &Connection, txid: &str, block_secs: i64) -> usize {
    conn.execute(
        POT_BLOCK_TIME_STAMP_SQL,
        params![block_secs, BLOCK_TIME_STAMP_SLACK_SECS as i64, txid],
    )
    .expect("POT_BLOCK_TIME_STAMP_SQL")
}

fn created_and_block_time(conn: &Connection, txid: &str) -> (Option<i64>, Option<i64>) {
    conn.query_row(
        "SELECT createdAt, blockTime FROM pot_records WHERE txid = ?1",
        [txid],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )
    .expect("row")
}

fn one(txid: &str) -> [Outpoint; 1] {
    [Outpoint {
        txid: txid.to_string(),
        vout: 0,
    }]
}

#[test]
fn pre557_a_a_reindexed_pre_cutoff_pot_stays_written_off() {
    let conn = production_schema_db();
    let pot = h64(0xe1);
    admit_pot(&conn, &pot, PRE557_NOW_SECS);
    // what a NOW stamp alone does: the written-off era comes back
    assert_eq!(
        pots_view_rows(&conn, &one(&pot), Some(PRE557_CUT_MS)).len(),
        1
    );

    let block = PRE557_CUT_MS / 1000 - 86_400;
    assert_eq!(stamp_block_time(&conn, &pot, block), 1);
    assert_eq!(
        created_and_block_time(&conn, &pot),
        (Some(block), Some(block))
    );
    assert!(
        pots_view_rows(&conn, &one(&pot), Some(PRE557_CUT_MS)).is_empty(),
        "stamped from its block, the pre-cutoff pot is written off again"
    );
}

#[test]
fn pre557_b_a_reindexed_post_cutoff_pot_keeps_its_real_time() {
    let conn = production_schema_db();
    let pot = h64(0xe2);
    admit_pot(&conn, &pot, PRE557_NOW_SECS);
    let block = PRE557_CUT_MS / 1000 + 30 * 86_400;
    stamp_block_time(&conn, &pot, block);
    assert_eq!(
        created_and_block_time(&conn, &pot),
        (Some(block), Some(block))
    );
    assert_eq!(
        pots_view_rows(&conn, &one(&pot), Some(PRE557_CUT_MS)).len(),
        1
    );
    // once: a later stamp (a re-anchored proof) moves nothing
    assert_eq!(stamp_block_time(&conn, &pot, block - 99_999), 0);
    assert_eq!(
        created_and_block_time(&conn, &pot),
        (Some(block), Some(block))
    );
}

/// A stored pot BEEF row, latched (`proof_verified = 1`) or not.
fn store_pot_beef(conn: &Connection, txid: &str, verified: bool) {
    conn.execute(
        "INSERT INTO pot_beefs (txid, beef, createdAt, proof_verified) VALUES (?1, x'0102', 0, ?2)",
        params![txid, i64::from(verified)],
    )
    .expect("pot_beefs");
}

/// The stamp step's candidates, the SHIPPED statement and binds (`D1PotStorage::verified_pots_without_block_time`).
fn stamp_candidates(conn: &Connection, start_key: &str, limit: i64) -> Vec<String> {
    let mut stmt = conn
        .prepare(VERIFIED_POTS_WITHOUT_BLOCK_TIME_SQL)
        .expect("prepare");
    let rows = stmt
        .query_map(
            params![start_key, i64::from(POT_BLOCK_TIME_SCAN_ROWS), limit],
            |r| r.get::<_, String>(0),
        )
        .expect("query")
        .collect::<Result<Vec<_>, _>>()
        .expect("rows");
    let mut rows = rows;
    rows.sort();
    rows
}

#[test]
fn pre557_c_an_unmined_reindexed_pot_is_now_and_marked() {
    let conn = production_schema_db();
    let pot = h64(0xe3);
    admit_pot(&conn, &pot, PRE557_NOW_SECS);
    store_pot_beef(&conn, &pot, false);
    // an unlatched proof is no stamp candidate: the row is as admitted
    assert!(stamp_candidates(&conn, "00", 48).is_empty());
    assert_eq!(
        created_and_block_time(&conn, &pot),
        (Some(PRE557_NOW_SECS), None),
        "NOW, and no block time"
    );
    assert_eq!(
        pots_view_rows(&conn, &one(&pot), Some(PRE557_CUT_MS)).len(),
        1
    );
}

/// The lens fold, HIGH-1: the stamp step's own read names every LATCHED pot with no block time, whatever
/// latched it (here a push: the row is latched and no completion candidate), never an unlatched one, one
/// with no BEEF or a stamped one; both windows; through the partial index, never a scan of `pot_records`.
#[test]
fn pre557_e_the_stamp_candidates_are_the_latched_unstamped_pots() {
    let conn = production_schema_db();
    let pushed = h64(0x10);
    let unmined = h64(0x20);
    let no_beef = h64(0x30);
    let stamped = h64(0x40);
    let late = h64(0xf0);
    for (txid, beef) in [
        (&pushed, Some(true)),
        (&unmined, Some(false)),
        (&no_beef, None),
        (&stamped, Some(true)),
        (&late, Some(true)),
    ] {
        admit_pot(&conn, txid, PRE557_NOW_SECS);
        if let Some(verified) = beef {
            store_pot_beef(&conn, txid, verified);
        }
    }
    let block = PRE557_CUT_MS / 1000 - 86_400;
    assert_eq!(stamp_block_time(&conn, &stamped, block), 1);
    // a random start past every row: the first window still reaches them
    assert_eq!(
        stamp_candidates(&conn, &"ff".repeat(32), 48),
        vec![pushed.clone(), late.clone()]
    );
    // a start inside the key space: both windows, one answer per pot
    assert_eq!(
        stamp_candidates(&conn, &h64(0x50), 48),
        vec![pushed.clone(), late.clone()]
    );
    assert_eq!(stamp_candidates(&conn, &h64(0x50), 1).len(), 1, "the limit");
    // the stamp takes the pushed pot out of the set, and out of the written-off era's view
    assert_eq!(stamp_block_time(&conn, &pushed, block), 1);
    assert_eq!(stamp_candidates(&conn, "00", 48), vec![late.clone()]);
    assert!(pots_view_rows(&conn, &one(&pushed), Some(PRE557_CUT_MS)).is_empty());

    let mut stmt = conn
        .prepare(&format!(
            "EXPLAIN QUERY PLAN {VERIFIED_POTS_WITHOUT_BLOCK_TIME_SQL}"
        ))
        .unwrap();
    let plan: Vec<String> = stmt
        .query_map(params!["00", 100, 48], |r| r.get(3))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    println!("stamp candidates plan: {plan:?}");
    for d in &plan {
        assert!(
            !d.starts_with("SCAN")
                || d == "SCAN c"
                || d.contains("idx_pot_records_block_time_null")
                || d.contains("subquery"),
            "never a scan of a fleet table: {d} (plan {plan:?})"
        );
        assert!(
            !d.starts_with("SEARCH b") || d.contains("(txid=?)"),
            "pot_beefs is probed by its key, never walked by its latch: {d} (plan {plan:?})"
        );
    }
    assert!(
        plan.iter()
            .filter(|d| d.contains("idx_pot_records_block_time_null"))
            .count()
            == 2,
        "both windows read the partial index of the unstamped rows: {plan:?}"
    );
}

/// The lens's LOW-4 under real SQLite: the shipped statement keeps an admission 7,200 s after its block and
/// moves one 7,201 s after.
#[test]
fn pre557_f_the_slack_boundary_is_7200_kept_and_7201_moved() {
    let conn = production_schema_db();
    let block = PRE557_CUT_MS / 1000 - 86_400;
    let kept = h64(0xe6);
    let moved = h64(0xe7);
    admit_pot(&conn, &kept, block + 7_200);
    admit_pot(&conn, &moved, block + 7_201);
    stamp_block_time(&conn, &kept, block);
    stamp_block_time(&conn, &moved, block);
    assert_eq!(
        created_and_block_time(&conn, &kept),
        (Some(block + 7_200), Some(block))
    );
    assert_eq!(
        created_and_block_time(&conn, &moved),
        (Some(block), Some(block))
    );
}

#[test]
fn pre557_d_a_live_pot_keeps_its_admission_time() {
    let conn = production_schema_db();
    let pot = h64(0xe5);
    let block = PRE557_NOW_SECS;
    admit_pot(&conn, &pot, block + 3_600);
    stamp_block_time(&conn, &pot, block);
    assert_eq!(
        created_and_block_time(&conn, &pot),
        (Some(block + 3_600), Some(block))
    );
}
