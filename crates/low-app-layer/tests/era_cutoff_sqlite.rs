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
use bsv_overlay_cloudflare::d1_discovery::store_record_sql;
use low_app_layer::logic::{assemble_pots_view, pots_view_join_sql, Outpoint, PotsViewRow};
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
