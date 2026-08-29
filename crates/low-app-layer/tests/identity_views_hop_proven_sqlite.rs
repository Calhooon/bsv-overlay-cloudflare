//! bsv-low 2026-08-29 — a seat's OWN pot is visible to its identity views
//! WITHOUT a party marker (the run-A "invisible own refund").
//!
//! The three identity-keyed views (`/results`, `/recovery-view`,
//! `/refund-view`) enumerate `logic::party_candidates_sql()`: party rows
//! UNION hop-proven rows — a verified (`markerValid = 1`) `LOW/hopparty`
//! marker whose hop output the JOIN spent, the JOIN's pot decoded with that
//! seat key in its lock. Executed against the production overlay
//! migrations (rusqlite, in-memory); the writers are direct column shapes
//! (each writer has its own producer cell in the overlay crate).

use bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS;
use low_app_layer::logic::recovery_view_sql;
use low_app_layer::refund_view::refund_view_sql;
use low_app_layer::results::results_sql;
use rusqlite::{params, params_from_iter, Connection};

fn production_schema_db() -> Connection {
    let conn = Connection::open_in_memory().expect("open in-memory sqlite");
    for sql in OVERLAY_MIGRATIONS {
        if let Err(e) = conn.execute_batch(sql) {
            let msg = e.to_string().to_ascii_lowercase();
            assert!(
                msg.contains("duplicate column"),
                "migration failed: {e}\n{sql}"
            );
        }
    }
    conn
}

fn h64(seed: u8) -> String {
    format!("{seed:02x}").repeat(32)
}
fn key(seed: u8) -> String {
    format!("02{}", format!("{seed:02x}").repeat(32))
}
fn identity(seed: u8) -> String {
    format!("03{}", format!("{seed:02x}").repeat(32))
}

/// The hop's own `pot_records` row (tm_lowfund), spent by `join` when given.
fn hop_row(conn: &Connection, hop_txid: &str, hop_vout: u32, join: Option<&str>) {
    conn.execute(
        "INSERT OR IGNORE INTO pot_records (txid, outputIndex, spent, spendingTxid, spentConfirmed, createdAt, paramsDecoded) \
         VALUES (?1, ?2, ?3, ?4, ?5, 1000, 0)",
        params![hop_txid, hop_vout, i32::from(join.is_some()), join, i32::from(join.is_some())],
    )
    .expect("hop row");
}

/// The pot (JOIN:0) with decoded committed keys, spent-confirmed by `refund`.
fn pot_row(conn: &Connection, pot: &str, pub_a: &str, pub_b: &str, refund: Option<&str>) {
    conn.execute(
        "INSERT OR IGNORE INTO pot_records (txid, outputIndex, spent, spendingTxid, spentConfirmed, createdAt, \
                                            paramsDecoded, pubA, pubB, recoveryHeight, verdict, verdictTxid) \
         VALUES (?1, 0, ?2, ?3, ?4, 2000, 1, ?5, ?6, 964400, ?7, ?8)",
        params![
            pot,
            i32::from(refund.is_some()),
            refund,
            i32::from(refund.is_some()),
            pub_a,
            pub_b,
            refund.map(|_| "refund"),
            refund,
        ],
    )
    .expect("pot row");
}

/// A `LOW/hopparty` marker riding in the hop tx (marker outpoint = hop tx, vout 1).
#[allow(clippy::too_many_arguments)]
fn hop_marker(
    conn: &Connection,
    identity: &str,
    opponent: &str,
    game: &str,
    hop_txid: &str,
    hop_vout: u32,
    seat_key: &str,
    marker_valid: Option<i64>,
) {
    conn.execute(
        "INSERT OR IGNORE INTO hopparty_records (identity, opponentIdentity, gameId, hopVout, hopSats, \
             seatSettlePubkey, seatSigHex, identitySigHex, containerOutputs, txid, outputIndex, createdAt, markerValid) \
         VALUES (?1, ?2, ?3, ?4, 1000, ?5, 'seatsig', 'idsig', 2, ?6, 1, 1500, ?7)",
        params![identity, opponent, game, hop_vout, seat_key, hop_txid, marker_valid],
    )
    .expect("hop marker");
}

fn party_marker(conn: &Connection, identity: &str, opponent: &str, game: &str, pot: &str) {
    conn.execute(
        "INSERT OR IGNORE INTO potparty_records (identity, opponentIdentity, gameId, potTxid, potVout, \
             recoveryHeight, sigHex, txid, outputIndex, createdAt, sigValid) \
         VALUES (?1, ?2, ?3, ?4, 0, 964400, 'idsig', ?5, 0, 2500, 1)",
        params![identity, opponent, game, pot, h64(0xe0)],
    )
    .expect("party marker");
}

fn pots_of(
    conn: &Connection,
    sql: &str,
    identity: &str,
    era: Option<i64>,
) -> Vec<(String, String, Option<String>)> {
    let mut stmt = conn.prepare(sql).expect("prepare");
    let mut binds: Vec<rusqlite::types::Value> = vec![identity.to_string().into()];
    if let Some(ms) = era {
        binds.push(ms.into());
    }
    stmt.query_map(params_from_iter(binds), |r| {
        Ok((
            r.get::<_, String>("potTxid")?,
            r.get::<_, String>("gameId")?,
            r.get::<_, Option<String>>("spendingTxid")?,
        ))
    })
    .expect("query")
    .collect::<Result<Vec<_>, _>>()
    .expect("rows")
}

/// The run-A shape: seat A's party marker never published; the hop marker
/// (verified at fund time) + the JOIN spend + the decoded pot prove the seat.
fn seed_run_a(conn: &Connection) -> (String, String, String, String) {
    let me = identity(0xaa);
    let opp = identity(0xbb);
    let game = h64(0x45);
    let hop = h64(0x2c);
    let join = h64(0x0e); // the JOIN = the pot funding tx
    let refund = h64(0x97);
    hop_marker(conn, &me, &opp, &game, &hop, 0, &key(0xa1), Some(1));
    hop_row(conn, &hop, 0, Some(&join));
    pot_row(conn, &join, &key(0xa1), &key(0xb1), Some(&refund));
    (me, game, join, refund)
}

#[test]
fn the_seats_own_refunded_pot_is_listed_by_all_three_views_without_a_party_marker() {
    let conn = production_schema_db();
    let (me, game, join, refund) = seed_run_a(&conn);
    for (name, sql) in [
        ("results", results_sql(None, 0)),
        ("recovery-view", recovery_view_sql(None, 0)),
        ("refund-view", refund_view_sql(None, 0)),
    ] {
        let rows = pots_of(&conn, &sql, &me, None);
        assert_eq!(
            rows,
            vec![(join.clone(), game.clone(), Some(refund.clone()))],
            "{name}: the hop-proven pot is the identity's own row, spent by its refund"
        );
    }
    // /results carries the recovery height the credit gate needs, from the
    // decoded lock (the hop marker has none of its own).
    let rh: i64 = conn
        .prepare(&results_sql(None, 0))
        .unwrap()
        .query_row(params![me], |r| r.get("recoveryHeight"))
        .unwrap();
    assert_eq!(rh, 964400);
}

#[test]
fn a_party_marker_still_wins_and_never_doubles_the_row() {
    let conn = production_schema_db();
    let (me, game, join, _) = seed_run_a(&conn);
    party_marker(&conn, &me, &identity(0xbb), &game, &join);
    for sql in [
        results_sql(None, 0),
        recovery_view_sql(None, 0),
        refund_view_sql(None, 0),
    ] {
        let rows = pots_of(&conn, &sql, &me, None);
        assert_eq!(
            rows.len(),
            1,
            "one pot, one row — the party row, not a hop duplicate"
        );
        assert_eq!(rows[0].0, join);
    }
}

#[test]
fn the_hop_arm_keeps_every_bar_of_the_attribution_rule() {
    // unlatched hop marker
    {
        let conn = production_schema_db();
        let me = identity(0xaa);
        hop_marker(
            &conn,
            &me,
            &identity(0xbb),
            &h64(0x45),
            &h64(0x2c),
            0,
            &key(0xa1),
            None,
        );
        hop_row(&conn, &h64(0x2c), 0, Some(&h64(0x0e)));
        pot_row(&conn, &h64(0x0e), &key(0xa1), &key(0xb1), Some(&h64(0x97)));
        assert!(
            pots_of(&conn, &results_sql(None, 0), &me, None).is_empty(),
            "NULL markerValid attributes nothing"
        );
    }
    // latched INVALID hop marker
    {
        let conn = production_schema_db();
        let me = identity(0xaa);
        hop_marker(
            &conn,
            &me,
            &identity(0xbb),
            &h64(0x45),
            &h64(0x2c),
            0,
            &key(0xa1),
            Some(0),
        );
        hop_row(&conn, &h64(0x2c), 0, Some(&h64(0x0e)));
        pot_row(&conn, &h64(0x0e), &key(0xa1), &key(0xb1), Some(&h64(0x97)));
        assert!(
            pots_of(&conn, &results_sql(None, 0), &me, None).is_empty(),
            "a failed latch attributes nothing"
        );
    }
    // the marker's seat key is NOT in the pot's lock
    {
        let conn = production_schema_db();
        let me = identity(0xaa);
        hop_marker(
            &conn,
            &me,
            &identity(0xbb),
            &h64(0x45),
            &h64(0x2c),
            0,
            &key(0x77),
            Some(1),
        );
        hop_row(&conn, &h64(0x2c), 0, Some(&h64(0x0e)));
        pot_row(&conn, &h64(0x0e), &key(0xa1), &key(0xb1), Some(&h64(0x97)));
        assert!(
            pots_of(&conn, &results_sql(None, 0), &me, None).is_empty(),
            "a foreign key attributes nothing"
        );
    }
    // the hop is UNSPENT (no JOIN yet) — there is no pot to list
    {
        let conn = production_schema_db();
        let me = identity(0xaa);
        hop_marker(
            &conn,
            &me,
            &identity(0xbb),
            &h64(0x45),
            &h64(0x2c),
            0,
            &key(0xa1),
            Some(1),
        );
        hop_row(&conn, &h64(0x2c), 0, None);
        pot_row(&conn, &h64(0x0e), &key(0xa1), &key(0xb1), Some(&h64(0x97)));
        assert!(
            pots_of(&conn, &results_sql(None, 0), &me, None).is_empty(),
            "an unspent hop proves no pot"
        );
    }
    // the pot is not decoded — no committed keys to match against
    {
        let conn = production_schema_db();
        let me = identity(0xaa);
        hop_marker(
            &conn,
            &me,
            &identity(0xbb),
            &h64(0x45),
            &h64(0x2c),
            0,
            &key(0xa1),
            Some(1),
        );
        hop_row(&conn, &h64(0x2c), 0, Some(&h64(0x0e)));
        conn.execute(
            "INSERT INTO pot_records (txid, outputIndex, spent, spendingTxid, spentConfirmed, createdAt, paramsDecoded) \
             VALUES (?1, 0, 1, ?2, 1, 2000, 0)",
            params![h64(0x0e), h64(0x97)],
        )
        .unwrap();
        assert!(
            pots_of(&conn, &results_sql(None, 0), &me, None).is_empty(),
            "an undecoded pot cannot be attributed"
        );
    }
}

#[test]
fn both_seats_see_the_pot_through_their_own_hop_markers() {
    let conn = production_schema_db();
    let (me, game, join, _) = seed_run_a(&conn);
    let opp = identity(0xbb);
    // Seat B's hop, also spent by the same JOIN, its key = pubB.
    hop_marker(&conn, &opp, &me, &game, &h64(0x2d), 0, &key(0xb1), Some(1));
    hop_row(&conn, &h64(0x2d), 0, Some(&join));
    assert_eq!(
        pots_of(&conn, &recovery_view_sql(None, 0), &me, None).len(),
        1
    );
    assert_eq!(
        pots_of(&conn, &recovery_view_sql(None, 0), &opp, None).len(),
        1
    );
    // and a stranger sees nothing
    assert!(pots_of(&conn, &recovery_view_sql(None, 0), &identity(0xcc), None).is_empty());
}

#[test]
fn the_era_cutoff_still_binds_as_the_second_parameter() {
    let conn = production_schema_db();
    let (me, _, join, _) = seed_run_a(&conn);
    // pot admitted at createdAt = 2000 s; a cutoff after it hides the pot,
    // a cutoff before it keeps it — bound as [identity, cutoff_ms].
    assert!(pots_of(
        &conn,
        &results_sql(Some(3_000_000), 0),
        &me,
        Some(3_000_000)
    )
    .is_empty());
    assert_eq!(
        pots_of(
            &conn,
            &results_sql(Some(1_000_000), 0),
            &me,
            Some(1_000_000)
        )[0]
        .0,
        join
    );
}
