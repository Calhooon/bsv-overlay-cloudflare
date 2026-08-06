//! `/recovery-view` proofs — REAL SQLite, PRODUCTION schema.
//!
//! # Why this file exists (bsv-low #323, written after a production outage)
//!
//! `recovery_view_sql()` shipped SYNTACTICALLY INVALID: a column was carried
//! through three of the query's four projection tiers and dropped by the
//! middle one, so the outer `SELECT` referenced `w.covRecoveryHeight`, a
//! column that did not exist in `w`. Every request with a valid identity
//! returned `database query failed`:
//!
//! ```text
//! no such column: w.covRecoveryHeight at offset 7: SQLITE_ERROR [code: 7500]
//! ```
//!
//! It passed the full suite, three review rounds, two RED-verification passes
//! and a MERGE-CLEAN verdict, because **every pin on that query asserted
//! `sql.contains(…)` against a STRING**. Both assertions were true of text
//! that no database would accept. The query had never been executed by any
//! test at all.
//!
//! That is this campaign's own rule landing on the campaign: *an executable
//! claim is only as strong as the surface it executes against.* We wrote that
//! about a cell that called one of two surfaces; this was the same failure one
//! level deeper — a pin that called neither.
//!
//! **So the rule for this query, and for every query: a `contains` assertion
//! on a SQL string must never again be the only thing standing behind it.**
//! These tests EXECUTE the exact shipped `recovery_view_sql()` against real
//! SQLite carrying the overlay's PRODUCTION migration list, with rows written
//! by the REAL producer SQL (`store_record_sql()` for tm_pot admission,
//! `mark_spent_sql()` for the spend writer, the topic manager's
//! `INSERT OR IGNORE` shape for `potparty_records`) — never hand-fed shapes.

use bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS;
use bsv_overlay_cloudflare::d1_discovery::{mark_spent_sql, store_record_sql};
use low_app_layer::logic::{
    assemble_recovery_view, recovery_view_body, recovery_view_sql, RecoveryRow,
    RECOVERY_VIEW_MAX_ROWS, RECOVERY_VIEW_UNKNOWN_QUOTA,
};
use rusqlite::{params, Connection};

/// A fresh in-memory SQLite carrying the REAL production schema (same
/// tolerance discipline as the sibling suites: only the re-run additive-ALTER
/// error class the production runner tolerates).
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

fn h66(seed: u8) -> String {
    format!("02{}", format!("{seed:02x}").repeat(32))
}

/// Admit a pot via the REAL `store_record_sql()` upsert. `cov_height` Some ⇒
/// a #284-decoded COVENANT row; None ⇒ a legacy/bare row (decoded columns
/// NULL, exactly what a pre-#284 or bare-lock admission leaves).
fn admit_pot(conn: &Connection, txid: &str, created_at: i64, cov_height: Option<i64>) {
    let covenant = cov_height.is_some();
    conn.execute(
        store_record_sql(),
        params![
            txid,
            0i64,                              // outputIndex
            0i64,                              // spent
            Option::<String>::None,            // spendingTxid
            0i64,                              // spentConfirmed
            created_at,                        // createdAt
            covenant.then_some("covenant"),    // lockKind
            covenant.then(|| h66(0x0a)),       // pubA
            covenant.then(|| h66(0x0b)),       // pubB
            covenant.then(|| h66(0x0c)),       // pubTower
            covenant.then(|| "aa".repeat(20)), // payPkhA
            covenant.then(|| "bb".repeat(20)), // payPkhB
            covenant.then(|| "cc".repeat(20)), // rakePkh
            covenant.then_some(500i64),        // stakeA
            covenant.then_some(500i64),        // stakeB
            covenant.then_some(8i64),          // feeSats
            cov_height,                        // recoveryHeight (committed)
            covenant.then_some(1000i64),       // potSats
            // NOT NULL in the production schema — 0 for a bare/legacy row.
            i64::from(covenant), // paramsDecoded
        ],
    )
    .expect("store_record_sql");
}

/// Record a CONFIRMED spend through the REAL `mark_spent_sql()` writer
/// (#371 finality CASE binds ride every variant; NULL here).
fn mark_spent_confirmed(conn: &Connection, txid: &str, spender: &str, height: Option<i64>) {
    conn.execute(
        mark_spent_sql(true, false),
        params![
            spender,
            spender,
            height,
            height,
            spender,
            Option::<i64>::None,
            Option::<i64>::None,
            txid,
            0i64
        ],
    )
    .expect("mark_spent_sql");
}

/// File a potparty marker — the topic manager's `INSERT OR IGNORE` shape.
#[allow(clippy::too_many_arguments)]
fn file_party(
    conn: &Connection,
    identity: &str,
    game_id: &str,
    pot_txid: &str,
    recovery_height: i64,
    marker_txid: &str,
    at: i64,
) {
    conn.execute(
        "INSERT OR IGNORE INTO potparty_records \
         (identity, opponentIdentity, gameId, potTxid, potVout, recoveryHeight, \
          sigHex, txid, outputIndex, createdAt) \
         VALUES (?1, ?2, ?3, ?4, 0, ?5, '3045ab', ?6, 0, ?7)",
        params![
            identity,
            h66(0xbb),
            game_id,
            pot_txid,
            recovery_height,
            marker_txid,
            at
        ],
    )
    .expect("insert potparty_records");
}

/// EXECUTE the shipped `recovery_view_sql()` and map rows exactly as the
/// worker's `RecoveryRowD1` does.
fn query_recovery_rows(conn: &Connection, identity: &str) -> Vec<RecoveryRow> {
    let sql = recovery_view_sql();
    let mut stmt = conn
        .prepare(&sql)
        .unwrap_or_else(|e| panic!("recovery_view_sql did not PREPARE: {e}\n{sql}"));
    stmt.query_map(params![identity], |r| {
        Ok(RecoveryRow {
            game_id: r.get("gameId")?,
            pot_txid: r.get("potTxid")?,
            pot_vout: r.get::<_, i64>("potVout")? as u32,
            recovery_height: r.get::<_, i64>("recoveryHeight")? as u32,
            cov_recovery_height: r
                .get::<_, Option<i64>>("covRecoveryHeight")?
                .map(|v| v as u64),
            opponent_identity: r.get("opponentIdentity")?,
            spent: r.get::<_, Option<i64>>("spent")?.map(|v| v != 0),
            spending_txid: r.get("spendingTxid")?,
            spent_confirmed: r.get::<_, Option<i64>>("spentConfirmed")?.map(|v| v != 0),
            spender_beef_hex: r.get("spenderBeef")?,
            // #343 — through the SHARED predicate, exactly as `RecoveryRowD1`
            // does. Mapping these by hand here would make the cell blind to
            // the one thing it can observe that a unit test cannot: that the
            // shipped SQL really projects the four columns.
            committed_keys: low_app_layer::results::CommittedKeys::from_columns(
                r.get::<_, Option<String>>("covPubA")?.as_deref(),
                r.get::<_, Option<String>>("covPubB")?.as_deref(),
                r.get::<_, Option<String>>("covPayPkhA")?.as_deref(),
                r.get::<_, Option<String>>("covPayPkhB")?.as_deref(),
            ),
        })
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

/// THE REGRESSION TEST for the outage: the shipped query must PREPARE against
/// the production schema. A dropped projection column is a `no such column`
/// error here, not a 500 in production.
#[test]
fn recovery_view_sql_prepares_against_the_production_schema() {
    let conn = production_schema_db();
    let sql = recovery_view_sql();
    conn.prepare(&sql)
        .unwrap_or_else(|e| panic!("recovery_view_sql is not valid SQL: {e}\n{sql}"));
}

/// The covenant-committed height must actually ARRIVE through all four
/// projection tiers — the column whose middle-tier drop caused the outage.
#[test]
fn the_covenant_height_survives_every_projection_tier() {
    let conn = production_schema_db();
    let me = h66(0xa1);
    let pot = h64(0xaa);
    admit_pot(&conn, &pot, 1_000, Some(958_504));
    file_party(
        &conn,
        &me,
        &h64(0x11),
        &pot,
        1,
        "ff".repeat(32).as_str(),
        1_001,
    );

    let rows = query_recovery_rows(&conn, &me);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cov_recovery_height,
        Some(958_504),
        "the covenant height must survive the middle tier"
    );
    // And the ASSEMBLED entry prefers it over the marker's value of 1.
    let (entries, _) = assemble_recovery_view(rows);
    assert_eq!(entries[0].recovery_height, 958_504);
}

/// bsv-low#343 — the pot's COMMITTED covenant keys must survive all four
/// projection tiers too, and arrive on the WIRE.
///
/// This is the wiped-device case the field exists for: the caller has no
/// local money records and no funding BEEF in hand, so today's ownership
/// anchor returns cannot-say and the row is taken on trust. With the
/// committed keys served, the client answers the question itself from its own
/// two derivations (its `[2,'low settle']` key and its `counterparty:'self'`
/// pay-home PKH) — network enforcement instead of platform enforcement.
///
/// Driven end to end through the SHIPPED SQL, the SHIPPED row mapper and the
/// SHIPPED body builder, because a column added to only three of the four
/// projection tiers is exactly the outage the cell above exists for.
#[test]
fn the_committed_covenant_keys_survive_every_projection_tier_and_reach_the_wire() {
    let conn = production_schema_db();
    let me = h66(0xa5);
    let pot = h64(0xc1);
    admit_pot(&conn, &pot, 1_000, Some(958_504));
    file_party(
        &conn,
        &me,
        &h64(0x11),
        &pot,
        1,
        "fe".repeat(32).as_str(),
        1_001,
    );

    let rows = query_recovery_rows(&conn, &me);
    assert_eq!(rows.len(), 1);
    let keys = rows[0]
        .committed_keys
        .clone()
        .expect("a covenant pot's committed keys survive the middle tiers");
    // The values `admit_pot` wrote through the REAL `store_record_sql`.
    assert_eq!(keys.pub_a, h66(0x0a));
    assert_eq!(keys.pub_b, h66(0x0b));
    assert_eq!(keys.pay_pkh_a, "aa".repeat(20));
    assert_eq!(keys.pay_pkh_b, "bb".repeat(20));

    let (entries, _) = assemble_recovery_view(rows);
    let body: serde_json::Value =
        serde_json::from_str(&recovery_view_body(&entries, Some(900_000), false)).unwrap();
    let ck = &body["entries"][0]["committedKeys"];
    assert_eq!(ck["pubA"], serde_json::json!(h66(0x0a)));
    assert_eq!(ck["pubB"], serde_json::json!(h66(0x0b)));
    assert_eq!(ck["payPkhA"], serde_json::json!("aa".repeat(20)));
    assert_eq!(
        ck["payPkhB"],
        serde_json::json!("bb".repeat(20)),
        "the PAY HOMES are the unforgeable half — a set without them would \
         reduce the anchor to a name check"
    );
    // ADDITIVE: nothing a deployed client already reads moved.
    assert_eq!(body["entries"][0]["recoveryHeight"], 958_504);
    assert_eq!(body["entries"][0]["potTxid"], serde_json::json!(pot));
}

/// A pot with no decoded params serves `committedKeys: null` — CANNOT-SAY,
/// which the client must not read as "not yours" (and which is the state a
/// legacy un-backfilled row is in until the #284 pass reaches it).
#[test]
fn a_pot_without_decoded_params_serves_null_committed_keys() {
    let conn = production_schema_db();
    let me = h66(0xa6);
    let pot = h64(0xc2);
    admit_pot(&conn, &pot, 1_000, None); // bare/legacy: decoded columns NULL
    file_party(
        &conn,
        &me,
        &h64(0x11),
        &pot,
        958_600,
        "fd".repeat(32).as_str(),
        1_001,
    );

    let rows = query_recovery_rows(&conn, &me);
    assert!(rows[0].committed_keys.is_none());
    let (entries, _) = assemble_recovery_view(rows);
    let body: serde_json::Value =
        serde_json::from_str(&recovery_view_body(&entries, None, false)).unwrap();
    assert!(
        body["entries"][0]["committedKeys"].is_null(),
        "null, and the KEY IS STILL PRESENT — an absent key and a null one \
         must not be two different states for a consumer to handle"
    );
    assert!(body["entries"][0]
        .as_object()
        .unwrap()
        .contains_key("committedKeys"));
}

/// A pot whose stored params are INCOMPLETE serves null — never a partial
/// set. A consumer handed `{pubA, pubB, payPkhA: null}` would be invited to
/// settle for the forgeable half of the check.
#[test]
fn a_partial_param_set_serves_null_rather_than_half_an_answer() {
    let conn = production_schema_db();
    let me = h66(0xa7);
    let pot = h64(0xc3);
    admit_pot(&conn, &pot, 1_000, Some(958_504));
    // Blank ONE pay home — the unforgeable half — leaving the rest intact.
    conn.execute(
        "UPDATE pot_records SET payPkhB = NULL WHERE txid = ?1",
        params![pot],
    )
    .unwrap();
    file_party(
        &conn,
        &me,
        &h64(0x11),
        &pot,
        1,
        "fc".repeat(32).as_str(),
        1_001,
    );

    let rows = query_recovery_rows(&conn, &me);
    assert!(
        rows[0].committed_keys.is_none(),
        "all four or nothing — a half set is the state a consumer misreads"
    );
}

/// A bare/legacy pot has no committed height: the marker value is served,
/// and the column is NULL rather than absent.
#[test]
fn a_bare_pot_serves_the_marker_height() {
    let conn = production_schema_db();
    let me = h66(0xa2);
    let pot = h64(0xbb);
    admit_pot(&conn, &pot, 1_000, None);
    file_party(
        &conn,
        &me,
        &h64(0x12),
        &pot,
        958_600,
        "fe".repeat(32).as_str(),
        1_001,
    );

    let rows = query_recovery_rows(&conn, &me);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].cov_recovery_height, None);
    let (entries, _) = assemble_recovery_view(rows);
    assert_eq!(entries[0].recovery_height, 958_600);
}

/// The spend status and the spender BEEF join ride back through the window.
#[test]
fn a_confirmed_spend_rides_back_with_its_status() {
    let conn = production_schema_db();
    let me = h66(0xa3);
    let pot = h64(0xcc);
    let spender = h64(0xde);
    admit_pot(&conn, &pot, 1_000, Some(958_700));
    mark_spent_confirmed(&conn, &pot, &spender, Some(957_443));
    file_party(
        &conn,
        &me,
        &h64(0x13),
        &pot,
        1,
        "fd".repeat(32).as_str(),
        1_001,
    );

    let rows = query_recovery_rows(&conn, &me);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].spent, Some(true));
    assert_eq!(rows[0].spending_txid.as_deref(), Some(spender.as_str()));
    assert_eq!(rows[0].spent_confirmed, Some(true));
}

/// The anti-flood window, EXECUTED: duplicate markers for one pot collapse in
/// SQL (oldest wins), so a marker flood cannot occupy more than one slot.
#[test]
fn duplicate_markers_for_one_pot_collapse_in_sql() {
    let conn = production_schema_db();
    let me = h66(0xa4);
    let pot = h64(0xab);
    admit_pot(&conn, &pot, 1_000, Some(958_800));
    // Three markers for the SAME pot — a republish, and a later filer.
    for (i, at) in [1_001i64, 1_002, 1_003].into_iter().enumerate() {
        file_party(
            &conn,
            &me,
            &h64(0x14),
            &pot,
            1,
            format!("{:02x}", 0xc0 + i as u8).repeat(32).as_str(),
            at,
        );
    }
    let rows = query_recovery_rows(&conn, &me);
    assert_eq!(rows.len(), 1, "the SQL itself must collapse to one row");
}

/// The unknown-pot quota, EXECUTED: pots with no `pot_records` row are
/// demoted behind indexed pots except for the reserved slice — so ghost rows
/// cannot take the whole page from real ones.
#[test]
fn unknown_pots_are_demoted_behind_indexed_ones() {
    let conn = production_schema_db();
    let me = h66(0xa5);
    // One INDEXED pot, filed EARLIEST (so a naive newest-first order would
    // rank it last).
    let real = h64(0x01);
    admit_pot(&conn, &real, 1_000, Some(958_900));
    file_party(
        &conn,
        &me,
        &h64(0x21),
        &real,
        1,
        "b0".repeat(32).as_str(),
        1_000,
    );
    // Many UNKNOWN pots (no pot_records row), all NEWER.
    for i in 0..(RECOVERY_VIEW_UNKNOWN_QUOTA + 5) {
        let ghost = format!("{:064x}", 0x9000 + i);
        file_party(
            &conn,
            &me,
            &format!("{:064x}", 0x2200 + i),
            &ghost,
            1,
            &format!("{:064x}", 0xd000 + i),
            2_000 + i as i64,
        );
    }
    let rows = query_recovery_rows(&conn, &me);
    // The indexed pot is never evicted by a ghost flood.
    let real_pos = rows
        .iter()
        .position(|r| r.pot_txid == real)
        .expect("the indexed pot must still be served");

    // The QUOTA's actual meaning: at most `quota` ghosts are promoted into
    // the indexed pot's tier, so no more than that many can rank ahead of it
    // however many are filed. (Promoted ghosts DO outrank it on recency —
    // that is the reservation working as designed, and it is why the bound
    // matters: without it every ghost would rank ahead.)
    assert!(
        real_pos <= RECOVERY_VIEW_UNKNOWN_QUOTA,
        "at most {RECOVERY_VIEW_UNKNOWN_QUOTA} ghosts may rank ahead of an \
         indexed pot; it landed at {real_pos}"
    );
    // The demoted ghosts are still SERVED (never erased) — they sit behind.
    let ghosts = rows
        .iter()
        .filter(|r| r.cov_recovery_height.is_none())
        .count();
    assert_eq!(
        ghosts,
        RECOVERY_VIEW_UNKNOWN_QUOTA + 5,
        "demotion must not DROP a row — a real-but-unindexed pot is exactly \
         what a recovering client needs to see"
    );
}

/// The window is bounded and truncation is DETECTABLE end to end.
#[test]
fn the_window_is_bounded_and_truncation_is_visible() {
    let conn = production_schema_db();
    let me = h66(0xa6);
    for i in 0..(RECOVERY_VIEW_MAX_ROWS + 5) {
        let pot = format!("{:064x}", 0x4000 + i);
        admit_pot(&conn, &pot, 1_000 + i as i64, Some(958_000 + i as i64));
        file_party(
            &conn,
            &me,
            &format!("{:064x}", 0x5000 + i),
            &pot,
            1,
            &format!("{:064x}", 0x6000 + i),
            1_000 + i as i64,
        );
    }
    let rows = query_recovery_rows(&conn, &me);
    assert_eq!(
        rows.len(),
        RECOVERY_VIEW_MAX_ROWS + 1,
        "the SQL fetches MAX_ROWS + 1 as the truncation probe"
    );
    let (entries, truncated) = assemble_recovery_view(rows);
    assert_eq!(entries.len(), RECOVERY_VIEW_MAX_ROWS);
    assert!(truncated, "a page past the cap must report truncated");
}

/// Another identity's markers are never returned (the one bind).
#[test]
fn the_view_is_scoped_to_one_identity() {
    let conn = production_schema_db();
    let me = h66(0xa7);
    let other = h66(0xa8);
    let pot = h64(0x71);
    admit_pot(&conn, &pot, 1_000, Some(959_000));
    file_party(
        &conn,
        &other,
        &h64(0x31),
        &pot,
        1,
        "e0".repeat(32).as_str(),
        1_001,
    );

    assert!(query_recovery_rows(&conn, &me).is_empty());
    assert_eq!(query_recovery_rows(&conn, &other).len(), 1);
}

/// File a potparty marker with an explicit #283 admission-time latch.
///
/// MODELLING BOUNDARY (epoch Rule 17): this SETS `sigValid` instead of
/// deriving it from real signatures, because what this file tests is the
/// ORDERING the column drives. The column's own correctness is established
/// against the frozen artifacts the real client producer emits
/// (`overlay_discovery::potparty::validity`) and through the real writer path
/// (`results_window_sqlite`'s `production_latch`).
#[allow(clippy::too_many_arguments)]
fn file_party_latched(
    conn: &Connection,
    identity: &str,
    game_id: &str,
    pot_txid: &str,
    recovery_height: i64,
    marker_txid: &str,
    at: i64,
    sig_valid: bool,
) {
    conn.execute(
        "INSERT OR IGNORE INTO potparty_records \
         (identity, opponentIdentity, gameId, potTxid, potVout, recoveryHeight, \
          sigHex, txid, outputIndex, createdAt, sigValid) \
         VALUES (?1, ?2, ?3, ?4, 0, ?5, '3045ab', ?6, 0, ?7, ?8)",
        params![
            identity,
            h66(0xbb),
            game_id,
            pot_txid,
            recovery_height,
            marker_txid,
            at,
            i32::from(sig_valid)
        ],
    )
    .expect("insert potparty_records");
}

/// bsv-low #283 on `/recovery-view`. The flood this window's quota was NEVER
/// on the path of (bsv-low#347): one FREE fabricated `pot_records` row per
/// ghost makes every ghost read `unknownPot = 0`, so it lands in tier 0
/// ordered freshest-first and the reserved unknown-pot quota is irrelevant.
/// 200 of them against a 100-row page erase the victim's own pots.
///
/// The bound is not a quota and not a price — it is that a ghost must NAME
/// the victim to appear in this identity-scoped window, and the marker's
/// identity signature binds that name.
#[test]
fn free_ghost_pot_records_cannot_erase_the_recovery_view() {
    let conn = production_schema_db();
    let me = h66(0xa9);
    let mut honest = Vec::new();
    for i in 0..50u64 {
        let pot = format!("{:064x}", 0x7000_u64 + i);
        admit_pot(&conn, &pot, 1_000 + i as i64, Some(959_000));
        file_party_latched(
            &conn,
            &me,
            &h64(0x31),
            &pot,
            1,
            &format!("txH{i:03}"),
            1_000 + i as i64,
            true,
        );
        honest.push(pot);
    }
    for i in 0..200u64 {
        let ghost = format!("{:064x}", 0xdead_0000_u64 + i);
        admit_pot(&conn, &ghost, 9_000 + i as i64, None); // free, fabricated
        file_party_latched(
            &conn,
            &me,
            &h64(0x31),
            &ghost,
            1,
            &format!("txG{i:03}"),
            9_000 + i as i64,
            false,
        );
    }
    let rows = query_recovery_rows(&conn, &me);
    let pots: Vec<&String> = rows.iter().map(|r| &r.pot_txid).collect();
    for pot in &honest {
        assert!(
            pots.contains(&pot),
            "every honest pot survives 200 free ghost pot_records rows: {pot} missing"
        );
    }

    // LEGACY CONTROL (epoch Rule 12a): demote every row to the pre-migration
    // shape and the same flood erases the page.
    conn.execute("UPDATE potparty_records SET sigValid = NULL", [])
        .expect("legacy-ize");
    let legacy = query_recovery_rows(&conn, &me);
    let legacy_pots: Vec<&String> = legacy.iter().map(|r| &r.pot_txid).collect();
    assert!(
        !legacy_pots.contains(&&honest[0]),
        "PRE-#283 CONTROL: the same flood erases an all-legacy page"
    );
}
