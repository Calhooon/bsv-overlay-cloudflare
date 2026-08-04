//! `/results` read-window proofs — REAL SQLite, PRODUCTION schema
//! (bsv-low #281).
//!
//! The defect: `results_sql()` used to be
//! `WHERE pp.identity = ? ORDER BY pp.createdAt DESC, pp.rowid DESC LIMIT 100`
//! with no per-pot partition. `tm_potparty` admission is BYTE-FORMAT-ONLY, so
//! anyone can file a marker row naming any identity for one dust `OP_RETURN`;
//! ~110 of them pushed a victim's real pot — including a chain-proven
//! tower-enforced WIN — entirely off `/results`. Erasure of a win is exactly
//! the harm the #276 owner ruling names.
//!
//! These tests EXECUTE the exact shipped `results_sql()` against real SQLite
//! carrying the overlay's PRODUCTION migration list verbatim (imported, not
//! transcribed — a hand-copied `CREATE TABLE` could drift out from under the
//! proof). A test that only pins the SQL string is NOT sufficient: the
//! 2026-07-28 adversarial gate called that out explicitly as the weakness in
//! #230's own F2 test. The LEGACY query is executed alongside, so the defect
//! stays demonstrated in-repo — the RED half of red→green lives permanently
//! beside the fix.

use bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS;
use low_app_layer::results::{
    assemble_results, covenant_params_by_pot, results_sql, seat_markers_sql, PotVerdict,
    ResultsRow, RESULTS_UNKNOWN_POT_QUOTA, SEAT_MARKERS_BINDS_PER_POT, SEAT_MARKERS_PER_KEY,
};
use rusqlite::{params, Connection};

/// `results_sql()` as it shipped BEFORE #281 — kept only so these tests can
/// demonstrate the displacement it permitted.
const LEGACY_RESULTS_SQL: &str = "SELECT pp.gameId, pp.potTxid, pp.potVout, pp.recoveryHeight, \
            pp.opponentIdentity, \
            r.spent, r.spendingTxid, r.spentConfirmed, \
            hex(fb.beef) AS fundingBeef, \
            hex(sb.beef) AS spenderBeef \
     FROM potparty_records pp \
     LEFT JOIN pot_records r ON r.txid = pp.potTxid AND r.outputIndex = pp.potVout \
     LEFT JOIN pot_beefs fb ON fb.txid = lower(pp.potTxid) \
     LEFT JOIN pot_beefs sb ON sb.txid = lower(r.spendingTxid) \
     WHERE pp.identity = ? \
     ORDER BY pp.createdAt DESC, pp.rowid DESC LIMIT 100";

/// A fresh in-memory SQLite carrying the REAL production schema.
///
/// The migration list is applied statement-by-statement exactly as
/// `d1::run_migrations` does, tolerating ONLY the one error class the
/// production runner tolerates (a re-run additive `ALTER TABLE` on a column
/// that already exists). Anything else fails loudly — a silently-skipped
/// migration would be schema drift this proof could not see.
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

/// Insert a pot row whose spend, when present, is CONFIRMED.
///
/// `spentConfirmed` is derived from `spent` here DELIBERATELY and is
/// documented as such: this helper models a settled pot. It used to be the
/// same silent inference `insert_decoded_pot` carried (#323) — the danger is
/// not the derivation, it is an UNDOCUMENTED one that leaves callers
/// believing they exercised a parked row when they never could. The parked
/// shape (`spent = 1, spentConfirmed = 0`) is exercised end to end against
/// the production schema by `a_parked_spend_yields_no_verdict_through_the_real_sql`
/// via `insert_decoded_pot`'s explicit `confirmed` flag; call
/// [`insert_pot_with`] directly if a NON-decoded parked row is ever needed.
fn insert_pot(conn: &Connection, txid: &str, created_at: i64, spent: bool) {
    insert_pot_with(conn, txid, created_at, spent, spent);
}

fn insert_pot_with(conn: &Connection, txid: &str, created_at: i64, spent: bool, confirmed: bool) {
    conn.execute(
        "INSERT OR IGNORE INTO pot_records \
         (txid, outputIndex, spent, spendingTxid, spentConfirmed, createdAt) \
         VALUES (?1, 0, ?2, ?3, ?4, ?5)",
        params![
            txid,
            i32::from(spent),
            if spent { Some(h64(0xfe)) } else { None },
            i32::from(confirmed),
            created_at
        ],
    )
    .expect("insert pot_records");
}

/// File a potparty marker. The production write is `INSERT OR IGNORE` on the
/// marker OUTPOINT, so every distinct `(txid, outputIndex)` lands — which is
/// precisely why anyone can file unlimited rows naming anyone.
fn file_marker(conn: &Connection, identity: &str, pot_txid: &str, marker_txid: &str, at: i64) {
    conn.execute(
        "INSERT OR IGNORE INTO potparty_records \
         (identity, opponentIdentity, gameId, potTxid, potVout, recoveryHeight, \
          sigHex, txid, outputIndex, createdAt) \
         VALUES (?1, ?2, ?3, ?4, 0, 958846, '3045ab', ?5, 0, ?6)",
        params![identity, h64(0xbb), h64(0x11), pot_txid, marker_txid, at],
    )
    .expect("insert potparty_records");
}

/// File a `LOW/potparty/v2` marker — the #230 shape, carrying the SEAT PROOF
/// (`seatSettlePubkey` / `seatSigHex` / `sigHex`) that `my_seat` verifies.
fn file_v2_marker(
    conn: &Connection,
    identity: &str,
    pot_txid: &str,
    marker_txid: &str,
    settle_pubkey: &str,
    at: i64,
) {
    conn.execute(
        "INSERT OR IGNORE INTO potparty_records \
         (identity, opponentIdentity, gameId, potTxid, potVout, recoveryHeight, \
          sigHex, seatSettlePubkey, seatSigHex, txid, outputIndex, createdAt) \
         VALUES (?1, ?2, ?3, ?4, 0, 958846, '3045id', ?5, '3045seat', ?6, 0, ?7)",
        params![
            identity,
            h64(0xbb),
            h64(0x11),
            pot_txid,
            settle_pubkey,
            marker_txid,
            at
        ],
    )
    .expect("insert v2 potparty_records");
}

fn query_pot_txids(conn: &Connection, sql: &str, identity: &str) -> Vec<String> {
    let mut stmt = conn.prepare(sql).expect("prepare");
    stmt.query_map(params![identity], |r| r.get::<_, String>("potTxid"))
        .expect("query")
        .collect::<Result<Vec<_>, _>>()
        .expect("rows")
}

/// Attacker rows naming invented pots — on their own enough to fill the
/// whole window (this is the erasure the gate demonstrated).
const DUST_GHOSTS: u32 = 120;
/// Attacker rows replaying the victim's OWN on-chain marker bytes for its
/// real pot — the variant that needs no forgery whatsoever.
const DUST_REPLAYS: u32 = 60;

/// The victim: one real, funded, SPENT pot (a landed tower-enforced outcome)
/// plus `DUST_GHOSTS + DUST_REPLAYS` attacker rows naming that same identity,
/// every one of them stamped NEWER than the honest marker — recency being the
/// only thing the legacy window ordered on.
fn seed_dust_attack(conn: &Connection, victim: &str) -> String {
    let honest_pot = h64(0xaa);
    insert_pot(conn, &honest_pot, 1_000, true);
    file_marker(conn, victim, &honest_pot, "txHONEST", 1_001);
    for i in 0..DUST_REPLAYS {
        file_marker(
            conn,
            victim,
            &honest_pot,
            &format!("txREPLAY{i:03}"),
            2_000 + i64::from(i),
        );
    }
    for i in 0..DUST_GHOSTS {
        // A pot that was never funded and is absent from `pot_records`. Each
        // is a DISTINCT potTxid, so a per-pot partition alone would not stop
        // them — the pot-existence tier is what does.
        file_marker(
            conn,
            victim,
            &format!("{:064x}", 0xdead_0000_u64 + u64::from(i)),
            &format!("txGHOST{i:03}"),
            3_000 + i64::from(i),
        );
    }
    honest_pot
}

/// RED — the defect, executed. 180 attacker rows erase the victim's real pot
/// from `/results` completely.
#[test]
fn legacy_results_window_erases_the_victims_real_pot() {
    let conn = production_schema_db();
    let victim = format!("02{}", "a1".repeat(32));
    let honest_pot = seed_dust_attack(&conn, &victim);

    let got = query_pot_txids(&conn, LEGACY_RESULTS_SQL, &victim);
    assert_eq!(got.len(), 100, "the legacy window returns a full page…");
    assert_eq!(
        got.iter().filter(|t| **t == honest_pot).count(),
        0,
        "…and the victim's REAL pot — a chain-proven, tower-enforced win — \
         is present ZERO times"
    );
}

/// GREEN — the shipped `results_sql()` over the SAME table state.
#[test]
fn results_window_survives_the_dust_attack() {
    let conn = production_schema_db();
    let victim = format!("02{}", "a1".repeat(32));
    let honest_pot = seed_dust_attack(&conn, &victim);

    let got = query_pot_txids(&conn, &results_sql(), &victim);
    assert_eq!(
        got.iter().filter(|t| **t == honest_pot).count(),
        1,
        "…and the victim's real pot is present exactly ONCE — the 1 honest \
         plus {DUST_REPLAYS} replayed marker rows naming it collapse to one slot"
    );
    // The promoted ghosts are NEWER than the honest pot, so they legitimately
    // sort ahead of it inside the main tier — but they are capped at the
    // reserved quota, so the honest pot can never be pushed off the page.
    assert!(
        got.iter().position(|t| *t == honest_pot).unwrap() <= RESULTS_UNKNOWN_POT_QUOTA,
        "the real pot sits within one quota of the top, whatever the flood"
    );
    // Ghost-pot rows are NOT erased — a pot whose `tm_pot` admission simply
    // has not landed yet (or a legacy pre-pot-index escrow) must stay
    // reachable, so they fill the slots real pots leave over. What is bounded
    // is how many may be PROMOTED ahead of a real pot.
    let at = got.iter().position(|t| *t == honest_pot).unwrap();
    assert!(
        at <= RESULTS_UNKNOWN_POT_QUOTA,
        "the real pot sits within one quota of the top (was: absent), got {at}"
    );
    assert!(
        got[..at].iter().all(|t| t.starts_with("0000")),
        "only promoted ghosts precede the real pot"
    );
}

/// The legitimate use case is preserved: since the window counts POTS, a
/// player with 100 real pots still sees all 100 — even with every one of them
/// dust-replayed.
#[test]
fn a_player_with_100_real_pots_still_sees_all_100() {
    let conn = production_schema_db();
    let victim = format!("02{}", "a1".repeat(32));
    for i in 0..100u32 {
        let pot = format!("{:064x}", 0x0000_1000_u64 + u64::from(i));
        insert_pot(&conn, &pot, 1_000 + i64::from(i), i % 3 == 0);
        file_marker(
            &conn,
            &victim,
            &pot,
            &format!("txM{i:03}"),
            1_000 + i64::from(i),
        );
        file_marker(
            &conn,
            &victim,
            &pot,
            &format!("txD{i:03}"),
            9_000 + i64::from(i),
        );
    }
    let got = query_pot_txids(&conn, &results_sql(), &victim);
    assert_eq!(got.len(), 100, "all 100 real pots returned");
    let unique: std::collections::HashSet<&String> = got.iter().collect();
    assert_eq!(unique.len(), 100, "one row per pot, no duplicates");
}

/// The joined facts still arrive: `/results` is only useful if the pot's
/// spend status and BOTH stored BEEFs survive the rewrite.
///
/// #284: the BEEF joins are now CONDITION-GATED — this row is a LEGACY one
/// (no decoded columns: `pubA IS NULL` opens the funding gate, `verdict IS
/// NULL` opens the spender gate), so BOTH BLOBs must still come back exactly
/// as pre-#284. The decoded-row halves of the gate (no BLOBs fetched /
/// stale-verdict re-open) are proven in the #284 section below.
#[test]
fn the_joined_chain_facts_still_come_back() {
    let conn = production_schema_db();
    let victim = format!("02{}", "a1".repeat(32));
    let pot = h64(0xaa);
    let settle = h64(0xfe);
    insert_pot(&conn, &pot, 1_000, true);
    file_marker(&conn, &victim, &pot, "txHONEST", 1_001);
    conn.execute(
        "INSERT INTO pot_beefs (txid, beef, createdAt) VALUES (?1, ?2, 1)",
        params![pot, vec![0x01u8, 0x02]],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO pot_beefs (txid, beef, createdAt) VALUES (?1, ?2, 1)",
        params![settle, vec![0x03u8, 0x04]],
    )
    .unwrap();

    /// One joined `/results` row, as the route's `ResultsRowD1` reads it.
    struct Joined {
        pot_txid: String,
        spent: i64,
        spending_txid: Option<String>,
        funding_beef: Option<String>,
        spender_beef: Option<String>,
    }

    let sql = results_sql();
    let mut stmt = conn.prepare(&sql).unwrap();
    let rows: Vec<Joined> = stmt
        .query_map(params![victim], |r| {
            Ok(Joined {
                pot_txid: r.get("potTxid")?,
                spent: r.get("spent")?,
                spending_txid: r.get("spendingTxid")?,
                funding_beef: r.get("fundingBeef")?,
                spender_beef: r.get("spenderBeef")?,
            })
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].pot_txid, pot);
    assert_eq!(
        rows[0].spent, 1,
        "spend status survives the subquery rewrite"
    );
    assert_eq!(rows[0].spending_txid.as_deref(), Some(settle.as_str()));
    assert_eq!(
        rows[0].funding_beef.as_deref(),
        Some("0102"),
        "funding BEEF keyed by potTxid"
    );
    assert_eq!(
        rows[0].spender_beef.as_deref(),
        Some("0304"),
        "spender BEEF keyed by spendingTxid"
    );
}

/// A pot ABSENT from `pot_records` (legacy pre-pot-index escrow, or a
/// `tm_pot` admission that has not landed) is still returned, with the
/// fail-safe NULLs the assembler already handles. The existence check is a
/// TIER, never a hard filter — we do not erase a pot the caller may be owed
/// money from.
#[test]
fn an_unindexed_pot_is_demoted_but_never_dropped() {
    let conn = production_schema_db();
    let victim = format!("02{}", "a1".repeat(32));
    let legacy_pot = h64(0xcc);
    file_marker(&conn, &victim, &legacy_pot, "txLEGACY", 1_001);

    let sql = results_sql();
    let mut stmt = conn.prepare(&sql).unwrap();
    let rows: Vec<(String, Option<i64>)> = stmt
        .query_map(params![victim], |r| {
            Ok((
                r.get::<_, String>("potTxid")?,
                r.get::<_, Option<i64>>("spent")?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows.len(), 1, "the unindexed pot is still returned");
    assert_eq!(rows[0].0, legacy_pot);
    assert_eq!(rows[0].1, None, "spend status stays an honest NULL");
}

/// DETERMINISM — the 2026-07-28 gate flagged non-deterministic ordering
/// specifically (its F2 finding was a `LIMIT 1000` with NO `ORDER BY`, where
/// SQLite's arbitrary row order decided whether the honest markers were
/// fetched at all).
///
/// The bar: the answer is a function of the STORED ROWS, never of the query
/// PLAN. This forces SQLite to change plan under identical rows — `ANALYZE`,
/// then indexes on exactly the columns the window orders on — and requires a
/// byte-identical answer. A missing `ORDER BY` at either level survives a
/// text-pinning test but fails this one.
#[test]
fn results_window_is_plan_independent_and_deterministic() {
    let conn = production_schema_db();
    let victim = format!("02{}", "a1".repeat(32));
    for i in 0..40u32 {
        let pot = format!("{:064x}", 0x0000_2000_u64 + u64::from(i));
        insert_pot(&conn, &pot, 5_000 + i64::from(i), i % 2 == 0);
        // Two markers per pot with the SAME createdAt — only the partition's
        // `rowid ASC` tiebreak can pick a representative.
        file_marker(&conn, &victim, &pot, &format!("txA{i:03}"), 7_000);
        file_marker(&conn, &victim, &pot, &format!("txB{i:03}"), 7_000);
        file_marker(
            &conn,
            &victim,
            &format!("{:064x}", 0x0000_9000_u64 + u64::from(i)),
            &format!("txG{i:03}"),
            7_000 + i64::from(i),
        );
    }
    let snapshot = |c: &Connection| -> Vec<(String, String)> {
        let sql = results_sql();
        let mut stmt = c.prepare(&sql).unwrap();
        stmt.query_map(params![victim], |r| {
            Ok((
                r.get::<_, String>("potTxid")?,
                r.get::<_, String>("opponentIdentity")?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
    };

    let baseline = snapshot(&conn);
    assert_eq!(baseline.len(), 80, "40 real pots + 40 unindexed pots");
    assert_eq!(baseline, snapshot(&conn), "repeat runs agree");
    conn.execute_batch("ANALYZE").unwrap();
    assert_eq!(baseline, snapshot(&conn), "stable across ANALYZE");
    conn.execute_batch(
        "CREATE INDEX ix1 ON potparty_records(identity, createdAt DESC); \
         CREATE INDEX ix2 ON potparty_records(potTxid, createdAt ASC); \
         CREATE INDEX ix3 ON pot_records(createdAt); \
         ANALYZE",
    )
    .unwrap();
    assert_eq!(
        baseline,
        snapshot(&conn),
        "stable across a forced plan change — the explicit ORDER BY at every \
         level decides the answer, not SQLite"
    );
    // Every real pot precedes every unindexed one (the existence tier).
    let ghosts: std::collections::HashSet<String> = (0..40u32)
        .map(|i| format!("{:064x}", 0x0000_9000_u64 + u64::from(i)))
        .collect();
    // Exactly RESULTS_UNKNOWN_POT_QUOTA unindexed pots are PROMOTED into the
    // main tier; the rest are demoted behind every indexed pot but still
    // served (never erased).
    let promoted = baseline
        .iter()
        .take_while(|(t, _)| ghosts.contains(t))
        .count();
    assert_eq!(
        promoted, RESULTS_UNKNOWN_POT_QUOTA,
        "the promotion is bounded"
    );
    assert_eq!(
        baseline.iter().filter(|(t, _)| ghosts.contains(t)).count(),
        40,
        "…and the demoted remainder is still served, never erased"
    );
}

// ════════════════════════════════════════════════════════════════════════
// F1 (2026-07-28 re-gate, HIGH) — the SEAT PROOF must not ride on the
// per-pot window.
//
// `/results` has no supplementary seat fetch of its own: `assemble_results`
// built its seat-marker map from the `results_sql` rows. Collapsing that page
// to one row per pot therefore dropped the cost of erasing a tower-enforced
// win from ~110 dust markers to ONE — and NO ordering rule fixes it, because
// #252's backfill publishes honest v2 markers for pots whose txid has been
// public for weeks, so a forged row can always be OLDER.
//
// The fix is `seat_markers_sql`, bound to each pot's COMMITTED KEYS: a forged
// key cannot enter the result set at all. These tests execute that query for
// real, against the same fixture that breaks every ordering heuristic.
// ════════════════════════════════════════════════════════════════════════

/// Fetch seat markers exactly as `routes::results_seat_markers` does — the
/// shipped `seat_markers_sql`, bound `(potTxid, potVout, pubA, pubB)`.
fn seat_marker_keys(
    conn: &Connection,
    pot: &str,
    vout: u32,
    pub_a: &str,
    pub_b: &str,
) -> Vec<String> {
    let sql = seat_markers_sql(1, SEAT_MARKERS_PER_KEY);
    assert_eq!(
        sql.matches('?').count(),
        SEAT_MARKERS_BINDS_PER_POT,
        "one pot ⇒ (potTxid, potVout, pubA, pubB)"
    );
    let mut stmt = conn.prepare(&sql).unwrap();
    stmt.query_map(params![pot, vout, pub_a, pub_b], |r| {
        r.get::<_, String>("seatSettlePubkey")
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

/// THE FIX: a forged v2 marker under a key the pot's lock does NOT commit is
/// filtered out IN SQL — however early it was stamped, however many there
/// are. The honest marker cannot be front-run out of the fetch.
#[test]
fn a_forged_key_cannot_enter_the_seat_marker_fetch() {
    let conn = production_schema_db();
    let victim = format!("02{}", "a1".repeat(32));
    let pot = h64(0xaa);
    let pub_a = format!("02{}", "5e".repeat(32)); // committed in the lock
    let pub_b = format!("03{}", "6f".repeat(32)); // committed in the lock
    let forged = format!("03{}", "f0".repeat(32)); // NOT committed
    insert_pot(&conn, &pot, 1_000, true);

    // 50 forged v2 markers, ALL stamped EARLIER than the honest one — the
    // exact shape that beats `createdAt ASC` inside a per-pot window.
    for i in 0..50u32 {
        file_v2_marker(
            &conn,
            &victim,
            &pot,
            &format!("txFORGED{i:03}"),
            &forged,
            100 + i64::from(i),
        );
    }
    // The honest v2 marker: latest of all, published by the #252 backfill.
    file_v2_marker(&conn, &victim, &pot, "txHONESTV2", &pub_a, 9_000);

    let got = seat_marker_keys(&conn, &pot, 0, &pub_a, &pub_b);
    assert_eq!(
        got,
        vec![pub_a.clone()],
        "only the marker under a COMMITTED key enters the result set — the 50 \
         EARLIER forgeries are filtered out in SQL, so ordering never enters \
         the argument at all"
    );
}

/// …and junk piled on ONE committed key cannot starve the OTHER seat's slot
/// (the `PARTITION BY potTxid, potVout, seatSettlePubkey` half).
#[test]
fn junk_on_one_committed_key_cannot_starve_the_other_seat() {
    let conn = production_schema_db();
    let victim = format!("02{}", "a1".repeat(32));
    let pot = h64(0xaa);
    let pub_a = format!("02{}", "5e".repeat(32));
    let pub_b = format!("03{}", "6f".repeat(32));
    insert_pot(&conn, &pot, 1_000, true);
    // A spammer copies seat A's PUBLIC committed key and floods it.
    for i in 0..60u32 {
        file_v2_marker(
            &conn,
            &victim,
            &pot,
            &format!("txSPAM{i:03}"),
            &pub_a,
            100 + i64::from(i),
        );
    }
    // Seat B's own marker, latest of all.
    file_v2_marker(&conn, &victim, &pot, "txSEATB", &pub_b, 9_000);

    let got = seat_marker_keys(&conn, &pot, 0, &pub_a, &pub_b);
    assert!(
        got.contains(&pub_b),
        "seat B's slot is its own — flooding seat A's key cannot evict it"
    );
    assert!(
        got.iter().filter(|k| **k == pub_a).count() <= 8,
        "seat A's own slot stays bounded (rn <= SEAT_MARKERS_PER_KEY)"
    );
}

/// The seat query is bound to the OUTPOINT, not just the txid: a marker for
/// vout 1 never attributes vout 0.
#[test]
fn the_seat_fetch_is_bound_to_the_pot_outpoint() {
    let conn = production_schema_db();
    let victim = format!("02{}", "a1".repeat(32));
    let txid = h64(0xaa);
    let pub_a = format!("02{}", "5e".repeat(32));
    let pub_b = format!("03{}", "6f".repeat(32));
    insert_pot(&conn, &txid, 1_000, true);
    conn.execute(
        "INSERT OR IGNORE INTO potparty_records \
         (identity, opponentIdentity, gameId, potTxid, potVout, recoveryHeight, \
          sigHex, seatSettlePubkey, seatSigHex, txid, outputIndex, createdAt) \
         VALUES (?1, ?2, ?3, ?4, 1, 958846, '3045id', ?5, '3045seat', 'txVOUT1', 0, 1001)",
        params![victim, h64(0xbb), h64(0x11), txid, pub_a],
    )
    .unwrap();
    assert!(
        seat_marker_keys(&conn, &txid, 0, &pub_a, &pub_b).is_empty(),
        "a vout-1 marker must not attribute vout 0"
    );
    assert_eq!(seat_marker_keys(&conn, &txid, 1, &pub_a, &pub_b).len(), 1);
}

/// `covenant_params_by_pot` is what feeds the committed keys to that query;
/// with no funding bytes there is nothing to bind, so `/results` asks nothing
/// and attributes nothing — never a guess.
#[test]
fn no_funding_bytes_means_no_seat_query() {
    let rows = vec![low_app_layer::results::ResultsRow {
        identity: format!("02{}", "a1".repeat(32)),
        game_id: h64(0x11),
        pot_txid: h64(0xaa),
        pot_vout: 0,
        recovery_height: 958_846,
        opponent_identity: h64(0xbb),
        spent: None,
        spending_txid: None,
        spent_confirmed: None,
        funding_beef_hex: None,
        spender_beef_hex: None,
        seat_settle_pubkey: None,
        seat_sig_hex: None,
        marker_sig_hex: None,
        ..Default::default()
    }];
    assert!(covenant_params_by_pot(&rows).is_empty());
}

// ════════════════════════════════════════════════════════════════════════
// #284 — decoded pot columns: pure column reads, fallback-preserving
// ════════════════════════════════════════════════════════════════════════

/// Real mainnet fixtures (shared with `classifier_real_txs.rs`): the
/// tower-enforced covenant settle 91309122… over the funded pot c571d433…
/// (ground truth: winner-a; committed stakes 2000+2000, fee 400,
/// recoveryHeight 956656).
const ENFORCED_SETTLE_TXID: &str =
    "91309122f5630052f7e57f7db843d26d32ae4426a9dd9b2fc2955f2fab8cf9a6";
const ENFORCED_SETTLE_HEX: &str =
    include_str!("fixtures/91309122f5630052f7e57f7db843d26d32ae4426a9dd9b2fc2955f2fab8cf9a6.hex");
const ENFORCED_FUNDING_TXID: &str =
    "c571d433b8234e225af0c631f076b137b7c164cfa72f86b3e713f9ba67e3b563";
const ENFORCED_FUNDING_HEX: &str =
    include_str!("fixtures/c571d433b8234e225af0c631f076b137b7c164cfa72f86b3e713f9ba67e3b563.hex");

/// Wrap a real raw-tx fixture in a minimal (unproven) BEEF — the bytes
/// `pot_beefs` would durably hold.
fn beef_bytes_of(raw_hex: &str) -> Vec<u8> {
    let tx = bsv_rs::transaction::Transaction::from_hex(raw_hex.trim()).unwrap();
    let mut beef = bsv_rs::transaction::Beef::new();
    beef.merge_transaction(tx);
    beef.to_binary()
}

fn insert_beef(conn: &Connection, txid: &str, beef: &[u8]) {
    conn.execute(
        "INSERT INTO pot_beefs (txid, beef, createdAt) VALUES (?1, ?2, 1)",
        params![txid, beef],
    )
    .unwrap();
}

/// The REAL committed params of the c571d433 pot, hex-encoded exactly as
/// admission stores them (extracted through the shipped decoder).
fn enforced_pot_columns() -> low_app_layer::results::CovenantParams {
    let raw = hex::decode(ENFORCED_FUNDING_HEX.trim()).unwrap();
    let ftx = low_app_layer::results::parse_raw_tx_verified(&raw, ENFORCED_FUNDING_TXID).unwrap();
    low_app_layer::results::extract_covenant_params(&ftx.outputs[0].1).unwrap()
}

/// Insert a pot_records row WITH decoded columns (+ optional verdict).
///
/// `confirmed` is EXPLICIT and independent of `spent_height`. Production
/// decouples them: `mark_spent_sql(confirmed=true, ..)` latches
/// `spentConfirmed = 1` while writing `spentHeight = COALESCE(?, spentHeight)`,
/// which is NULL whenever the confirming caller has no parseable bump. A
/// CONFIRMED spend with a NULL height is therefore a real production state.
/// This helper used to infer `spentConfirmed` from `spent_height.is_some()`,
/// which modelled a state production never produces and left three tests
/// silently exercising an UNCONFIRMED row while their names and comments
/// claimed they were about height (#323).
#[allow(clippy::too_many_arguments)]
fn insert_decoded_pot(
    conn: &Connection,
    txid: &str,
    spending_txid: Option<&str>,
    p: &low_app_layer::results::CovenantParams,
    pot_sats: i64,
    verdict: Option<&str>,
    verdict_txid: Option<&str>,
    spent_height: Option<i64>,
    confirmed: bool,
) {
    conn.execute(
        "INSERT OR IGNORE INTO pot_records \
         (txid, outputIndex, spent, spendingTxid, spentConfirmed, createdAt, \
          lockKind, pubA, pubB, pubTower, payPkhA, payPkhB, rakePkh, \
          stakeA, stakeB, feeSats, recoveryHeight, potSats, paramsDecoded, \
          verdict, verdictTxid, spentHeight) \
         VALUES (?1, 0, ?2, ?3, ?4, 1000, 'covenant', ?5, ?6, ?7, ?8, ?9, ?10, \
                 ?11, ?12, ?13, ?14, ?15, 1, ?16, ?17, ?18)",
        params![
            txid,
            i32::from(spending_txid.is_some()),
            spending_txid,
            i32::from(confirmed),
            hex::encode(p.pub_a),
            hex::encode(p.pub_b),
            hex::encode(p.pub_tower),
            hex::encode(p.pay_pkh_a),
            hex::encode(p.pay_pkh_b),
            hex::encode(p.rake_pkh),
            p.stake_a as i64,
            p.stake_b as i64,
            p.fee_sats as i64,
            p.recovery_height as i64,
            pot_sats,
            verdict,
            verdict_txid,
            spent_height
        ],
    )
    .expect("insert decoded pot_records");
}

/// One joined `/results` row read exactly as `routes::ResultsRowD1` does
/// (converted to the pure `ResultsRow` the assembler consumes).
fn query_results_rows(conn: &Connection, identity: &str) -> Vec<ResultsRow> {
    let sql = results_sql();
    let mut stmt = conn.prepare(&sql).unwrap();
    stmt.query_map(params![identity], |r| {
        Ok(ResultsRow {
            identity: r.get("identity")?,
            game_id: r.get("gameId")?,
            pot_txid: r.get("potTxid")?,
            pot_vout: r.get::<_, i64>("potVout")? as u32,
            recovery_height: r.get::<_, i64>("recoveryHeight")? as u32,
            opponent_identity: r.get("opponentIdentity")?,
            spent: r.get::<_, Option<i64>>("spent")?.map(|v| v != 0),
            spending_txid: r.get("spendingTxid")?,
            spent_confirmed: r.get::<_, Option<i64>>("spentConfirmed")?.map(|v| v != 0),
            funding_beef_hex: r.get("fundingBeef")?,
            spender_beef_hex: r.get("spenderBeef")?,
            seat_settle_pubkey: r.get("seatSettlePubkey")?,
            seat_sig_hex: r.get("seatSigHex")?,
            marker_sig_hex: r.get("sigHex")?,
            lock_kind: r.get("lockKind")?,
            pub_a: r.get("pubA")?,
            pub_b: r.get("pubB")?,
            pub_tower: r.get("pubTower")?,
            pay_pkh_a: r.get("payPkhA")?,
            pay_pkh_b: r.get("payPkhB")?,
            rake_pkh: r.get("rakePkh")?,
            stake_a: r.get::<_, Option<i64>>("stakeA")?.map(|v| v as u64),
            stake_b: r.get::<_, Option<i64>>("stakeB")?.map(|v| v as u64),
            fee_sats: r.get::<_, Option<i64>>("feeSats")?.map(|v| v as u64),
            cov_recovery_height: r
                .get::<_, Option<i64>>("covRecoveryHeight")?
                .map(|v| v as u64),
            pot_sats: r.get::<_, Option<i64>>("potSats")?.map(|v| v as u64),
            verdict: r.get("verdict")?,
            verdict_txid: r.get("verdictTxid")?,
            spent_height: r.get::<_, Option<i64>>("spentHeight")?.map(|v| v as u64),
            spender_proof_verified: r
                .get::<_, Option<i64>>("spenderProofVerified")?
                .map(|v| v != 0),
        })
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

/// The gated joins surface "no BLOB fetched" the same way a missing
/// `pot_beefs` row always has: `hex(NULL)` is the EMPTY STRING in SQLite
/// (never SQL NULL) — shape-identical to pre-#284, and `decode_beef_hex("")`
/// already refuses it downstream.
fn no_blob(v: &Option<String>) -> bool {
    v.as_deref().is_none_or(str::is_empty)
}

/// (1) A decoded-columns row round-trips through `results_sql` and yields
/// the verdict + params WITHOUT any pot_beefs row being read — even when
/// BLOB rows EXIST, the gated joins must not fetch them.
#[test]
fn a_decoded_row_serves_verdict_and_params_with_no_blob_fetch() {
    let conn = production_schema_db();
    let victim = format!("02{}", "a1".repeat(32));
    let p = enforced_pot_columns();
    insert_decoded_pot(
        &conn,
        ENFORCED_FUNDING_TXID,
        Some(ENFORCED_SETTLE_TXID),
        &p,
        4000,
        Some("winner-a"),
        Some(ENFORCED_SETTLE_TXID),
        Some(800_000),
        true,
    );
    file_marker(&conn, &victim, ENFORCED_FUNDING_TXID, "txHONEST", 1_001);
    // BLOB rows EXIST — the gate must leave them untouched.
    insert_beef(
        &conn,
        ENFORCED_FUNDING_TXID,
        &beef_bytes_of(ENFORCED_FUNDING_HEX),
    );
    insert_beef(
        &conn,
        ENFORCED_SETTLE_TXID,
        &beef_bytes_of(ENFORCED_SETTLE_HEX),
    );

    let rows = query_results_rows(&conn, &victim);
    assert_eq!(rows.len(), 1);
    assert!(
        no_blob(&rows[0].funding_beef_hex),
        "no funding BLOB fetched"
    );
    assert!(
        no_blob(&rows[0].spender_beef_hex),
        "no spender BLOB fetched"
    );
    assert_eq!(rows[0].verdict.as_deref(), Some("winner-a"));

    // The full assembler answers from columns alone.
    let entries = assemble_results(
        &victim,
        rows.clone(),
        &std::collections::HashMap::new(),
        &std::collections::HashMap::new(),
    );
    assert_eq!(entries[0].verdict, Some(PotVerdict::WinnerA));
    assert_eq!(entries[0].at_height, Some(800_000));

    // …and the committed keys feed the seat query with no funding bytes
    // (the complement of `no_funding_bytes_means_no_seat_query`).
    let by_pot = covenant_params_by_pot(&rows);
    assert_eq!(
        by_pot.get(&(ENFORCED_FUNDING_TXID.to_string(), 0)),
        Some(&p),
        "decoded columns bind the seat-marker fetch without a BLOB"
    );
}

/// (2) A LEGACY row (columns NULL) still classifies via the BEEF fallback —
/// the pre-#284 path, byte-for-byte.
#[test]
fn a_legacy_row_still_classifies_via_the_beef_fallback() {
    let conn = production_schema_db();
    let victim = format!("02{}", "a1".repeat(32));
    // A pre-#284 pot row: spent pointer, NO decoded columns.
    conn.execute(
        "INSERT OR IGNORE INTO pot_records \
         (txid, outputIndex, spent, spendingTxid, spentConfirmed, createdAt) \
         VALUES (?1, 0, 1, ?2, 1, 1000)",
        params![ENFORCED_FUNDING_TXID, ENFORCED_SETTLE_TXID],
    )
    .unwrap();
    file_marker(&conn, &victim, ENFORCED_FUNDING_TXID, "txHONEST", 1_001);
    insert_beef(
        &conn,
        ENFORCED_FUNDING_TXID,
        &beef_bytes_of(ENFORCED_FUNDING_HEX),
    );
    insert_beef(
        &conn,
        ENFORCED_SETTLE_TXID,
        &beef_bytes_of(ENFORCED_SETTLE_HEX),
    );

    let rows = query_results_rows(&conn, &victim);
    assert_eq!(rows.len(), 1);
    assert!(
        !no_blob(&rows[0].funding_beef_hex),
        "legacy row fetches the BLOBs"
    );
    assert!(!no_blob(&rows[0].spender_beef_hex));
    assert_eq!(rows[0].lock_kind, None);

    let entries = assemble_results(
        &victim,
        rows,
        &std::collections::HashMap::new(),
        &std::collections::HashMap::new(),
    );
    assert_eq!(
        entries[0].verdict,
        Some(PotVerdict::WinnerA),
        "the real tower-enforced settle classifies through the fallback"
    );
}

/// (3) A STALE stored verdict (`verdictTxid <> spendingTxid`) is NOT
/// trusted: the SQL re-opens the spender BLOB and the assembler
/// re-classifies from the column params + hash-verified spender bytes —
/// correcting the stale label instead of serving it.
#[test]
fn a_stale_verdict_is_not_trusted_and_falls_back() {
    let conn = production_schema_db();
    let victim = format!("02{}", "a1".repeat(32));
    let p = enforced_pot_columns();
    // The stored verdict says winner-b, but it was computed for a DIFFERENT
    // (displaced) spender.
    insert_decoded_pot(
        &conn,
        ENFORCED_FUNDING_TXID,
        Some(ENFORCED_SETTLE_TXID),
        &p,
        4000,
        Some("winner-b"),
        Some(&h64(0xd0)), // stale: not the current spendingTxid
        None,
        true,
    );
    file_marker(&conn, &victim, ENFORCED_FUNDING_TXID, "txHONEST", 1_001);
    insert_beef(
        &conn,
        ENFORCED_SETTLE_TXID,
        &beef_bytes_of(ENFORCED_SETTLE_HEX),
    );

    let rows = query_results_rows(&conn, &victim);
    assert_eq!(rows.len(), 1);
    assert!(
        no_blob(&rows[0].funding_beef_hex),
        "params are columns — no funding BLOB"
    );
    assert!(
        !no_blob(&rows[0].spender_beef_hex),
        "a stale verdict re-opens the spender BLOB"
    );

    let entries = assemble_results(
        &victim,
        rows,
        &std::collections::HashMap::new(),
        &std::collections::HashMap::new(),
    );
    assert_eq!(
        entries[0].verdict,
        Some(PotVerdict::WinnerA),
        "the stale winner-b label is ignored; the spend re-classifies to the \
         real winner-a from column params + hash-verified spender bytes"
    );
}

/// A fresh verdict with a MISSING proven height keeps the spender BLOB
/// available as the at.height fallback (the un-confirmed-spend case).
#[test]
fn a_fresh_verdict_without_height_still_fetches_the_spender_blob() {
    let conn = production_schema_db();
    let victim = format!("02{}", "a1".repeat(32));
    let p = enforced_pot_columns();
    insert_decoded_pot(
        &conn,
        ENFORCED_FUNDING_TXID,
        Some(ENFORCED_SETTLE_TXID),
        &p,
        4000,
        Some("winner-a"),
        Some(ENFORCED_SETTLE_TXID),
        None, // spentHeight NULL on a CONFIRMED spend (production shape:
        // a confirming write with no parseable bump leaves it NULL)
        true,
    );
    file_marker(&conn, &victim, ENFORCED_FUNDING_TXID, "txHONEST", 1_001);
    insert_beef(
        &conn,
        ENFORCED_SETTLE_TXID,
        &beef_bytes_of(ENFORCED_SETTLE_HEX),
    );

    let rows = query_results_rows(&conn, &victim);
    assert!(no_blob(&rows[0].funding_beef_hex));
    assert!(
        !no_blob(&rows[0].spender_beef_hex),
        "spentHeight NULL keeps the spender BEEF for the at.height fallback"
    );
    let entries = assemble_results(
        &victim,
        rows,
        &std::collections::HashMap::new(),
        &std::collections::HashMap::new(),
    );
    assert_eq!(
        entries[0].verdict,
        Some(PotVerdict::WinnerA),
        "column verdict trusted"
    );
    assert_eq!(
        entries[0].at_height, None,
        "a proofless spender BEEF honestly yields no height — never a guess"
    );
}

/// bsv-low#304: the spender-BEEF at.height fallback trusts ONLY the
/// VERIFIED proof latch. A spender row whose stored bytes STRUCTURALLY
/// carry a bump (admit-path bytes — possibly attacker-fabricated via the
/// ungated historical/GASP/peer-crawl modes) with `proof_verified = 0`
/// yields NO height (the pre-#304 behavior — the RED half — served the
/// bump's attacker-chosen height); once the overlay's verifying writer
/// latches `proof_verified = 1`, the exact same bytes serve their height.
/// Executes the shipped `results_sql()` on the production schema.
#[test]
fn spender_beef_height_is_gated_on_the_verified_latch() {
    let conn = production_schema_db();
    let victim = format!("02{}", "a1".repeat(32));
    let p = enforced_pot_columns();
    insert_decoded_pot(
        &conn,
        ENFORCED_FUNDING_TXID,
        Some(ENFORCED_SETTLE_TXID),
        &p,
        4000,
        Some("winner-a"),
        Some(ENFORCED_SETTLE_TXID),
        None, // spentHeight NULL — the BEEF fallback is the only height
        // source; the spend itself is CONFIRMED
        true,
    );
    file_marker(&conn, &victim, ENFORCED_FUNDING_TXID, "txHONEST", 1_001);

    // A structurally-BUMPED spender BEEF stored with the default (0)
    // verified latch — exactly what an ungated admit of fake-bumped bytes
    // leaves behind.
    let bumped = {
        let mut tx =
            bsv_rs::transaction::Transaction::from_hex(ENFORCED_SETTLE_HEX.trim()).unwrap();
        let txid = tx.id();
        tx.merkle_path = Some(
            bsv_rs::transaction::MerklePath::new(
                959_000,
                vec![vec![bsv_rs::transaction::MerklePathLeaf::new_txid(0, txid)]],
            )
            .unwrap(),
        );
        tx.to_beef(true).unwrap()
    };
    insert_beef(&conn, ENFORCED_SETTLE_TXID, &bumped);

    let rows = query_results_rows(&conn, &victim);
    assert!(
        !no_blob(&rows[0].spender_beef_hex),
        "the fallback BLOB is fetched"
    );
    assert_eq!(
        rows[0].spender_proof_verified,
        Some(false),
        "the shipped SQL carries the latch"
    );
    let entries = assemble_results(
        &victim,
        rows,
        &std::collections::HashMap::new(),
        &std::collections::HashMap::new(),
    );
    assert_eq!(
        entries[0].at_height, None,
        "an UNVERIFIED structural bump must never serve a height (bsv-low#304)"
    );

    // The overlay's verifying writer latches the row → the same bytes now
    // serve their (verified) height. Verified answers are never weakened.
    conn.execute(
        "UPDATE pot_beefs SET proof_verified = 1 WHERE txid = ?1",
        params![ENFORCED_SETTLE_TXID],
    )
    .unwrap();
    let rows = query_results_rows(&conn, &victim);
    assert_eq!(rows[0].spender_proof_verified, Some(true));
    let entries = assemble_results(
        &victim,
        rows,
        &std::collections::HashMap::new(),
        &std::collections::HashMap::new(),
    );
    assert_eq!(entries[0].at_height, Some(959_000));
}

// ── F3 — the existence tier must not become a filter ────────────────────

/// 100 indexed pots plus ONE newest pot whose `tm_pot` admission is still in
/// flight: a strict tier dropped the fresh pot entirely (it ranked FIRST
/// pre-fix). The reserved quota is what keeps it visible.
#[test]
fn a_fresh_unindexed_pot_is_not_filtered_out_by_the_limit() {
    let conn = production_schema_db();
    let victim = format!("02{}", "a1".repeat(32));
    for i in 0..100u32 {
        let pot = format!("{:064x}", 0x0000_1000_u64 + u64::from(i));
        insert_pot(&conn, &pot, 1_000 + i64::from(i), true);
        file_marker(
            &conn,
            &victim,
            &pot,
            &format!("txM{i:03}"),
            1_000 + i64::from(i),
        );
    }
    let fresh = h64(0xfa);
    file_marker(&conn, &victim, &fresh, "txFRESH", 9_999);

    let got = query_pot_txids(&conn, &results_sql(), &victim);
    assert!(
        got.contains(&fresh),
        "a real-but-unindexed pot must not be filtered out by the window"
    );
}

// ── F4 — ordering tests that fail when their guarantee is removed ───────

/// The PARTITION's `createdAt ASC, rowid ASC` is under test, so the honest
/// (oldest) marker is stored PHYSICALLY LAST. A non-discriminating partition
/// order returns a junk row instead — this cannot pass on incidental order.
#[test]
fn the_oldest_marker_represents_a_pot_even_when_stored_last() {
    let conn = production_schema_db();
    let victim = format!("02{}", "a1".repeat(32));
    let pot = h64(0xaa);
    let honest_game = h64(0x11);
    insert_pot(&conn, &pot, 1_000, true);
    for i in 0..20u32 {
        conn.execute(
            "INSERT OR IGNORE INTO potparty_records \
             (identity, opponentIdentity, gameId, potTxid, potVout, recoveryHeight, \
              sigHex, txid, outputIndex, createdAt) \
             VALUES (?1, ?2, ?3, ?4, 0, 958846, '3045ab', ?5, 0, ?6)",
            params![
                victim,
                h64(0xbb),
                format!("{:064x}", 0xbad_0000_u64 + u64::from(i)), // a DIFFERENT gameId
                pot,
                format!("txJUNK{i:03}"),
                5_000 + i64::from(i)
            ],
        )
        .unwrap();
    }
    // Oldest by createdAt, newest by rowid — the contradiction that makes
    // this test meaningful.
    file_marker(&conn, &victim, &pot, "txHONEST", 1_001);

    let sql = results_sql();
    let mut stmt = conn.prepare(&sql).unwrap();
    let rows: Vec<String> = stmt
        .query_map(params![victim], |r| r.get::<_, String>("gameId"))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        rows,
        vec![honest_game],
        "the OLDEST marker represents the pot — its gameId is what the \
         claims lookup keys on"
    );
}

/// The OUTER ordering is under test: pots are stored OLDEST-POT-FIRST, and
/// the marker stamps run OPPOSITE to the pot stamps, so the promised
/// newest-POT-first answer contradicts insertion order, rowid order, AND
/// marker recency.
#[test]
fn the_outer_order_is_pot_recency_not_storage_order() {
    let conn = production_schema_db();
    let victim = format!("02{}", "a1".repeat(32));
    let mut expect: Vec<String> = Vec::new();
    for i in 0..12u32 {
        let pot = format!("{:064x}", 0x0000_3000_u64 + u64::from(i));
        insert_pot(&conn, &pot, 1_000 + i64::from(i), true);
        file_marker(
            &conn,
            &victim,
            &pot,
            &format!("txM{i:03}"),
            9_000 - i64::from(i),
        );
        expect.push(pot);
    }
    expect.reverse(); // newest POT first
    let got = query_pot_txids(&conn, &results_sql(), &victim);
    assert_eq!(got, expect, "exact newest-pot-first sequence");
}

// ── F6 — the outpoint is the key ────────────────────────────────────────

#[test]
fn two_pots_sharing_a_funding_txid_are_not_collapsed() {
    let conn = production_schema_db();
    let victim = format!("02{}", "a1".repeat(32));
    let txid = h64(0xaa);
    insert_pot(&conn, &txid, 1_000, true);
    conn.execute(
        "INSERT OR IGNORE INTO pot_records \
         (txid, outputIndex, spent, spendingTxid, spentConfirmed, createdAt) \
         VALUES (?1, 1, 0, NULL, 0, 1001)",
        params![txid],
    )
    .unwrap();
    file_marker(&conn, &victim, &txid, "txV0", 1_002);
    conn.execute(
        "INSERT OR IGNORE INTO potparty_records \
         (identity, opponentIdentity, gameId, potTxid, potVout, recoveryHeight, \
          sigHex, txid, outputIndex, createdAt) \
         VALUES (?1, ?2, ?3, ?4, 1, 958846, '3045ab', 'txV1', 0, 1003)",
        params![victim, h64(0xbb), h64(0x11), txid],
    )
    .unwrap();
    assert_eq!(
        query_pot_txids(&conn, &results_sql(), &victim).len(),
        2,
        "distinct outpoints are distinct pots"
    );
}

/// FIX A (iv) — `rn <= SEAT_MARKERS_PER_KEY` enforced BEHAVIOURALLY, not by
/// string match. A spammer that copies seat A's PUBLIC committed key and
/// files rows OLDER than the genuine one must not evict it while it files
/// fewer than the cap; with `rn <= 1` the genuine marker is gone at ONE junk
/// row, and the win it proves becomes `unresolved`.
#[test]
fn the_per_key_slot_cap_keeps_the_genuine_marker_behind_older_junk() {
    let conn = production_schema_db();
    let victim = format!("02{}", "a1".repeat(32));
    let pot = h64(0xaa);
    let pub_a = format!("02{}", "5e".repeat(32));
    let pub_b = format!("03{}", "6f".repeat(32));
    insert_pot(&conn, &pot, 1_000, true);
    // Two junk rows under seat A's own (public) committed key, both OLDER…
    file_v2_marker(&conn, &victim, &pot, "txJUNK0", &pub_a, 100);
    file_v2_marker(&conn, &victim, &pot, "txJUNK1", &pub_a, 101);
    // …and the genuine marker, published later by the #252 backfill.
    file_v2_marker(&conn, &victim, &pot, "txGENUINE", &pub_a, 9_000);

    let sql = seat_markers_sql(1, SEAT_MARKERS_PER_KEY);
    let mut stmt = conn.prepare(&sql).unwrap();
    let got: Vec<String> = stmt
        .query_map(params![pot, 0u32, pub_a, pub_b], |r| {
            r.get::<_, String>("identity")
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    // All three rows come back as CANDIDATES — the app-layer then verifies
    // both signatures and only the genuine one attributes. With a cap of 1
    // only `txJUNK0` would return and the proof would be lost.
    assert_eq!(
        got.len(),
        3,
        "the per-key slot returns a superset; verification decides, not the cap"
    );
}

/// FIX A (iv) — `PARTITION BY potTxid, potVout, seatSettlePubkey` enforced
/// behaviourally. Without `potVout` the two outpoints of one funding txid
/// SHARE a single slot window, so junk piled on vout 0 evicts the genuine
/// marker of vout 1 (one of the two silent bugs on main that this branch
/// fixes).
#[test]
fn the_seat_slot_window_is_per_outpoint_not_per_txid() {
    let conn = production_schema_db();
    let victim = format!("02{}", "a1".repeat(32));
    let txid = h64(0xaa);
    let pub_a = format!("02{}", "5e".repeat(32));
    let pub_b = format!("03{}", "6f".repeat(32));
    insert_pot(&conn, &txid, 1_000, true);
    // Fill vout 0's slot for pubA with junk, all OLDER than the genuine row.
    for i in 0..8u32 {
        file_v2_marker(
            &conn,
            &victim,
            &txid,
            &format!("txJUNK{i}"),
            &pub_a,
            100 + i64::from(i),
        );
    }
    // The genuine marker lives at vout 1 under the SAME committed key.
    conn.execute(
        "INSERT OR IGNORE INTO potparty_records \
         (identity, opponentIdentity, gameId, potTxid, potVout, recoveryHeight, \
          sigHex, seatSettlePubkey, seatSigHex, txid, outputIndex, createdAt) \
         VALUES (?1, ?2, ?3, ?4, 1, 958846, '3045id', ?5, '3045seat', 'txVOUT1', 0, 9000)",
        params![victim, h64(0xbb), h64(0x11), txid, pub_a],
    )
    .unwrap();

    // BIND BOTH OUTPOINTS IN ONE CHUNK — the production path: two pots of the
    // same funding txid on one `/results` page land in the same
    // `seat_markers_sql` call, so the subquery sees BOTH vouts' rows and only
    // `potVout` in the PARTITION keeps their slot windows apart. (Binding a
    // single outpoint would filter the other vout out before the window and
    // prove nothing.)
    let sql = seat_markers_sql(2, SEAT_MARKERS_PER_KEY);
    let mut stmt = conn.prepare(&sql).unwrap();
    let got: Vec<String> = stmt
        .query_map(
            params![txid, 0u32, pub_a, pub_b, txid, 1u32, pub_a, pub_b],
            // `seat_markers_sql` exposes no marker txid; the outpoint +
            // committed key are what `attribute_seats` matches on anyway.
            |r| {
                Ok(format!(
                    "{}:{}",
                    r.get::<_, i64>("potVout")?,
                    r.get::<_, String>("seatSettlePubkey")?
                ))
            },
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(
        got.contains(&format!("1:{pub_a}")),
        "vout 1 has its OWN slot window — 8 junk rows at vout 0 cannot evict \
         the genuine marker of a DIFFERENT outpoint sharing the txid: {got:?}"
    );
}

/// #323 — the parked-spender bar, proven END TO END against the PRODUCTION
/// schema through the real `results_sql()`, not just the pure assembler.
///
/// This is the shape that shipped to production on 7 of 8 refunds: a
/// non-final refund admitted before it mined, so `pot_records` holds
/// `spent = 1, spentConfirmed = 0` and the pointer names a tx that never
/// landed. Before #323 no fixture in this suite could even express it
/// (`insert_pot`/`insert_decoded_pot` both inferred confirmation), so the
/// bar was unproven against the real SQL + schema.
#[test]
fn a_parked_spend_yields_no_verdict_through_the_real_sql() {
    let conn = production_schema_db();
    let victim = format!("02{}", "a1".repeat(32));
    let p = enforced_pot_columns();

    // CONTROL: the identical row, CONFIRMED, classifies — so a green
    // assertion below cannot be an artefact of the fixture never resolving.
    insert_decoded_pot(
        &conn,
        ENFORCED_FUNDING_TXID,
        Some(ENFORCED_SETTLE_TXID),
        &p,
        4000,
        None,
        None,
        Some(800_000),
        true,
    );
    file_marker(&conn, &victim, ENFORCED_FUNDING_TXID, "txHONEST", 1_001);
    insert_beef(
        &conn,
        ENFORCED_SETTLE_TXID,
        &beef_bytes_of(ENFORCED_SETTLE_HEX),
    );
    let confirmed = assemble_results(
        &victim,
        query_results_rows(&conn, &victim),
        &std::collections::HashMap::new(),
        &std::collections::HashMap::new(),
    );
    assert_eq!(confirmed.len(), 1);
    assert!(
        confirmed[0].verdict.is_some(),
        "CONTROL: a confirmed spend must classify, else this test proves nothing"
    );

    // THE DEFECT SHAPE: a second pot, same bytes, spend NOT confirmed.
    let conn2 = production_schema_db();
    insert_decoded_pot(
        &conn2,
        ENFORCED_FUNDING_TXID,
        Some(ENFORCED_SETTLE_TXID),
        &p,
        4000,
        None,
        None,
        None,
        false, // parked: recorded, never mined
    );
    file_marker(&conn2, &victim, ENFORCED_FUNDING_TXID, "txHONEST", 1_001);
    insert_beef(
        &conn2,
        ENFORCED_SETTLE_TXID,
        &beef_bytes_of(ENFORCED_SETTLE_HEX),
    );
    let rows = query_results_rows(&conn2, &victim);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].spent_confirmed,
        Some(false),
        "the production schema really does carry an unconfirmed spend"
    );
    let parked = assemble_results(
        &victim,
        rows,
        &std::collections::HashMap::new(),
        &std::collections::HashMap::new(),
    );
    assert_eq!(parked.len(), 1);
    assert_eq!(
        parked[0].verdict, None,
        "a parked spender must never yield a verdict through the real SQL"
    );
    assert_eq!(parked[0].at_height, None);
    // The pointer facts still SERVE — surface the attempt, never consume it.
    assert_eq!(
        parked[0].settle_txid.as_deref(),
        Some(ENFORCED_SETTLE_TXID.to_ascii_lowercase().as_str())
    );
    assert_eq!(parked[0].spent_confirmed, Some(false));
}

// ════════════════════════════════════════════════════════════════════════
// CHAIN PROVENANCE on `/results` — `covRecoveryHeight` + `potBinding`
// ════════════════════════════════════════════════════════════════════════
//
// THE DEFECT these cells pin (a client-side adversarial gate, 2026-08-04):
// the client's `'recoverable'` money-word and its row-corroboration
// predicate were BOTH derived from data an attacker can write.
//
//  - `results_sql` keys on the BYTE-ADMITTED `identity` column, so anyone can
//    file an `ls_potparty` marker naming a VICTIM's identity, with
//    `recoveryHeight: 1` and ANY real unspent outpoint as `potTxid`, for one
//    dust `OP_RETURN`. `/results` serves that row; the client rendered
//    "Recoverable" off the marker's own `recoveryHeight`.
//  - `spent` / `spentConfirmed` / `at.height` describe whichever pot THE ROW
//    NAMES — i.e. the attacker's own pot, which the attacker can genuinely
//    spend and confirm. Corroborating on them corroborates the attacker.
//
// The server already DECODES the chain-committed truth and simply did not
// serve it. These cells drive the REAL producer chain end to end —
// `results_sql` → `covenant_params_by_pot` → `seat_marker_chunks` →
// `seat_markers_sql` → `assemble_results` → `results_body` — against the
// PRODUCTION schema, and assert on the WIRE BODY (the contract the client
// reads), never on an intermediate struct.

use low_app_layer::results::{
    potparty_protocol, potparty_v2_challenge, results_body, seat_marker_chunks, seatsig_preimage,
    CovenantParams, SeatMarkerRow, SEAT_MARKERS_PER_KEY as SEAT_CAP,
};

/// A real settle keypair: (privkey, 66-hex compressed pubkey lowercase).
fn real_key(seed: u8) -> (bsv_rs::primitives::ec::PrivateKey, String) {
    let mut b = [0u8; 32];
    b[31] = seed;
    let k = bsv_rs::primitives::ec::PrivateKey::from_bytes(&b).unwrap();
    let pk = k.public_key().to_hex().to_ascii_lowercase();
    (k, pk)
}

fn wallet_of(seed: u8) -> bsv_rs::wallet::ProtoWallet {
    let key = bsv_rs::primitives::ec::PrivateKey::from_hex(&format!("{seed:064x}")).unwrap();
    bsv_rs::wallet::ProtoWallet::new(Some(key))
}

fn identity_of(w: &bsv_rs::wallet::ProtoWallet) -> String {
    w.identity_key_hex().to_ascii_lowercase()
}

/// `CovenantParams` committing two REAL settle pubkeys — the shape the
/// overlay's admission-time decode of a funding lock produces.
fn params_committing(pub_a_hex: &str, pub_b_hex: &str, recovery_height: u64) -> CovenantParams {
    let mut pub_a = [0u8; 33];
    pub_a.copy_from_slice(&hex::decode(pub_a_hex).unwrap());
    let mut pub_b = [0u8; 33];
    pub_b.copy_from_slice(&hex::decode(pub_b_hex).unwrap());
    CovenantParams {
        pub_a,
        pub_b,
        pub_tower: [2u8; 33],
        pay_pkh_a: [0xaa; 20],
        pay_pkh_b: [0xbb; 20],
        rake_pkh: [0xcc; 20],
        stake_a: 2_000,
        stake_b: 2_000,
        fee_sats: 400,
        recovery_height,
    }
}

/// File a GENUINELY VERIFIABLE `LOW/potparty/v2` marker into the production
/// `potparty_records` table: `seatSigHex` is a real ECDSA signature by
/// `settle_key` over sha256 of the exact cross-repo preimage, and `sigHex` is
/// a real BRC-42 'anyone' signature by the identity wallet over the exact v2
/// challenge. Both preimage and challenge are built by the SHIPPED functions,
/// so this fixture cannot drift from the verifier.
#[allow(clippy::too_many_arguments)]
fn file_real_v2_marker(
    conn: &Connection,
    settle_key: &bsv_rs::primitives::ec::PrivateKey,
    settle_pub_hex: &str,
    identity_wallet: &bsv_rs::wallet::ProtoWallet,
    opponent_hex: &str,
    game_id: &str,
    pot_txid: &str,
    pot_vout: u32,
    recovery_height: u32,
    marker_txid: &str,
    created_at: i64,
) {
    let identity_hex = identity_of(identity_wallet);
    let preimage = seatsig_preimage(game_id, pot_txid, pot_vout, &identity_hex).unwrap();
    let seat_sig = settle_key
        .sign(&bsv_rs::primitives::hash::sha256(&preimage))
        .unwrap();
    let mut m = SeatMarkerRow {
        identity: identity_hex.clone(),
        opponent_identity: opponent_hex.to_string(),
        game_id: game_id.to_string(),
        pot_txid: pot_txid.to_string(),
        pot_vout,
        recovery_height,
        seat_settle_pubkey: settle_pub_hex.to_string(),
        seat_sig_hex: hex::encode(seat_sig.to_der()),
        identity_sig_hex: String::new(),
    };
    let challenge = potparty_v2_challenge(&m).unwrap();
    let sig = identity_wallet
        .create_signature(bsv_rs::wallet::CreateSignatureArgs {
            data: Some(challenge),
            hash_to_directly_sign: None,
            protocol_id: potparty_protocol(),
            key_id: game_id.to_string(),
            counterparty: Some(bsv_rs::wallet::Counterparty::Anyone),
        })
        .unwrap();
    m.identity_sig_hex = hex::encode(sig.signature);

    conn.execute(
        "INSERT OR IGNORE INTO potparty_records \
         (identity, opponentIdentity, gameId, potTxid, potVout, recoveryHeight, \
          sigHex, seatSettlePubkey, seatSigHex, txid, outputIndex, createdAt) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 0, ?11)",
        params![
            m.identity,
            m.opponent_identity,
            m.game_id,
            m.pot_txid,
            m.pot_vout,
            m.recovery_height,
            m.identity_sig_hex,
            m.seat_settle_pubkey,
            m.seat_sig_hex,
            marker_txid,
            created_at
        ],
    )
    .expect("insert real v2 potparty_records");
}

/// File a bare (v1) marker with an arbitrary identity / gameId / hint — the
/// ATTACKER's primitive. Byte-format admission means this always lands.
#[allow(clippy::too_many_arguments)]
fn file_hostile_marker(
    conn: &Connection,
    identity: &str,
    game_id: &str,
    pot_txid: &str,
    recovery_height: i64,
    marker_txid: &str,
    created_at: i64,
) {
    conn.execute(
        "INSERT OR IGNORE INTO potparty_records \
         (identity, opponentIdentity, gameId, potTxid, potVout, recoveryHeight, \
          sigHex, txid, outputIndex, createdAt) \
         VALUES (?1, ?2, ?3, ?4, 0, ?5, '3045ab', ?6, 0, ?7)",
        params![
            identity,
            h64(0xbb),
            game_id,
            pot_txid,
            recovery_height,
            marker_txid,
            created_at
        ],
    )
    .expect("insert hostile potparty_records");
}

/// The seat-marker fetch EXACTLY as `routes::results_seat_markers` performs
/// it: `covenant_params_by_pot` over the joined rows → `seat_marker_chunks` →
/// the shipped `seat_markers_sql`, bound `(potTxid, potVout, pubA, pubB)`.
/// Driving the real chunker (rather than hand-feeding a marker map) is what
/// makes these cells producer-level proofs rather than primitive-level ones.
fn fetch_seat_markers(
    conn: &Connection,
    rows: &[ResultsRow],
) -> std::collections::HashMap<(String, u32), Vec<SeatMarkerRow>> {
    let mut out: std::collections::HashMap<(String, u32), Vec<SeatMarkerRow>> =
        std::collections::HashMap::new();
    let params_by_pot = covenant_params_by_pot(rows);
    for chunk in seat_marker_chunks(&params_by_pot) {
        let sql = seat_markers_sql(chunk.len(), SEAT_CAP);
        let mut stmt = conn.prepare(&sql).unwrap();
        let mut binds: Vec<rusqlite::types::Value> = Vec::new();
        for b in &chunk {
            binds.push(b.pot_txid.clone().into());
            binds.push(i64::from(b.pot_vout).into());
            binds.push(b.pub_a_hex.clone().into());
            binds.push(b.pub_b_hex.clone().into());
        }
        let fetched = stmt
            .query_map(rusqlite::params_from_iter(binds), |r| {
                Ok(SeatMarkerRow {
                    identity: r.get::<_, String>("identity")?.to_ascii_lowercase(),
                    opponent_identity: r.get::<_, String>("opponentIdentity")?.to_ascii_lowercase(),
                    game_id: r.get::<_, String>("gameId")?.to_ascii_lowercase(),
                    pot_txid: r.get::<_, String>("potTxid")?.to_ascii_lowercase(),
                    pot_vout: r.get::<_, i64>("potVout")? as u32,
                    recovery_height: r.get::<_, i64>("recoveryHeight")? as u32,
                    seat_settle_pubkey: r
                        .get::<_, Option<String>>("seatSettlePubkey")?
                        .unwrap_or_default()
                        .to_ascii_lowercase(),
                    seat_sig_hex: r
                        .get::<_, Option<String>>("seatSigHex")?
                        .unwrap_or_default()
                        .to_ascii_lowercase(),
                    identity_sig_hex: r
                        .get::<_, Option<String>>("sigHex")?
                        .unwrap_or_default()
                        .to_ascii_lowercase(),
                })
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        for m in fetched {
            out.entry((m.pot_txid.clone(), m.pot_vout))
                .or_default()
                .push(m);
        }
    }
    out
}

/// The WHOLE `/results` producer chain, ending at the wire body a client
/// actually parses.
fn results_wire(conn: &Connection, identity_lc: &str) -> serde_json::Value {
    let rows = query_results_rows(conn, identity_lc);
    let seat_markers = fetch_seat_markers(conn, &rows);
    let entries = assemble_results(
        identity_lc,
        rows,
        &std::collections::HashMap::new(),
        &seat_markers,
    );
    serde_json::from_str(&results_body(identity_lc, &entries)).unwrap()
}

/// Find the wire row for a pot txid (the wire lowercases txids).
fn wire_row<'a>(v: &'a serde_json::Value, pot_txid: &str) -> &'a serde_json::Value {
    let want = pot_txid.to_ascii_lowercase();
    v["results"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["potTxid"] == serde_json::json!(want))
        .unwrap_or_else(|| panic!("no /results row for pot {want}"))
}

/// The victim's committed recovery height — deliberately NOT the value any
/// marker in these fixtures carries, so "which field did this come from" is
/// always decidable.
const COMMITTED_HEIGHT: u64 = 958_846;
/// The hint the HONEST marker carries. Different from `COMMITTED_HEIGHT` on
/// purpose: an honest client may legitimately hold a stale hint, and the two
/// fields must stay separately observable.
const HONEST_HINT: u32 = 958_800;
/// The hostile marker's hint — "your refund is claimable NOW".
const HOSTILE_HINT: i64 = 1;

/// (1) POSITIVE CONTROL + the honest case. A covenant-decodable pot the
/// caller holds a committed settle key in serves the COVENANT height as its
/// own field and reports the row as CHAIN-BOUND — while the marker hint keeps
/// its own name and its own (different) value.
///
/// This is also the reachability proof for cells (2)–(4): it shows the
/// `potBinding == "chain"` leg is genuinely reachable through the real
/// producer, so a `"unknown"` assertion elsewhere is evidence and not an
/// input that died three validations earlier.
#[test]
fn a_covenant_pot_the_caller_holds_a_committed_key_in_is_chain_bound() {
    let conn = production_schema_db();
    let (ka, pa) = real_key(41);
    let (_kb, pb) = real_key(42);
    let victim_w = wallet_of(0x51);
    let victim = identity_of(&victim_w);
    let opponent = format!("02{}", "bb".repeat(32));
    let game = h64(0x11);
    let pot = h64(0xaa);
    let p = params_committing(&pa, &pb, COMMITTED_HEIGHT);

    // An UNSPENT covenant pot — the exact state a "Recoverable" word is
    // rendered in, and the state the old code could not check at all.
    insert_decoded_pot(&conn, &pot, None, &p, 4_000, None, None, None, false);
    file_real_v2_marker(
        &conn,
        &ka,
        &pa,
        &victim_w,
        &opponent,
        &game,
        &pot,
        0,
        HONEST_HINT,
        "txHONESTV2",
        1_001,
    );

    let wire = results_wire(&conn, &victim);
    let r = wire_row(&wire, &pot);
    assert_eq!(
        r["recoveryHeight"],
        serde_json::json!(HONEST_HINT),
        "the marker hint keeps its name and value (back-compat)"
    );
    assert_eq!(
        r["covRecoveryHeight"],
        serde_json::json!(COMMITTED_HEIGHT),
        "the COVENANT-committed height is served as its own distinct field"
    );
    assert_eq!(
        r["potBinding"],
        serde_json::json!("chain"),
        "POSITIVE CONTROL: the chain-bound leg is genuinely reachable"
    );
    assert_eq!(r["potBindingSource"], serde_json::json!("chain+seatkey"));
    // Unspent ⇒ no verdict/outcome is asserted; the binding is orthogonal.
    assert!(r["verdict"].is_null());
    assert_eq!(r["outcome"], serde_json::json!("unresolved"));
}

/// (2) **THE ATTACK.** A hostile marker names the VICTIM's identity, an
/// invented `gameId`, `recoveryHeight: 1`, and a REAL covenant pot outpoint
/// that is NOT the victim's — one the attacker genuinely owns a seat in and
/// can spend and confirm at will.
///
/// `/results` must serve that row (the overlay is an index, not an
/// authority — refusing to serve it would be adjudication at the wrong
/// layer) with the hostile hint UNCHANGED, and must report:
///  - `potBinding: "unknown"` — nothing the network settled binds this
///    outpoint to this gameId for this identity;
///  - a `covRecoveryHeight` that is the ATTACKER pot's real committed height,
///    NEVER the hostile hint — which is exactly why the client must gate on
///    `potBinding`, not on the presence of a covenant height.
#[test]
fn a_hostile_marker_naming_a_real_foreign_pot_is_never_chain_bound() {
    let conn = production_schema_db();
    // The ATTACKER's own real covenant pot: their keys, their lock, their
    // committed height. Everything about it is genuine.
    let (katt_a, patt_a) = real_key(77);
    let (_katt_b, patt_b) = real_key(78);
    let attacker_w = wallet_of(0x60);
    let attacker_pot = h64(0xcc);
    let attacker_game = h64(0x33);
    const ATTACKER_COMMITTED_HEIGHT: u64 = 700_000;
    let p_att = params_committing(&patt_a, &patt_b, ATTACKER_COMMITTED_HEIGHT);
    insert_decoded_pot(
        &conn,
        &attacker_pot,
        None,
        &p_att,
        4_000,
        None,
        None,
        None,
        false,
    );
    // The attacker's OWN honest seat marker for their OWN pot — present, and
    // correctly attributing the ATTACKER. It must not leak to the victim.
    file_real_v2_marker(
        &conn,
        &katt_a,
        &patt_a,
        &attacker_w,
        &format!("02{}", "dd".repeat(32)),
        &attacker_game,
        &attacker_pot,
        0,
        700_000,
        "txATTACKERV2",
        900,
    );

    // The forgery: one dust marker naming the VICTIM.
    let victim_w = wallet_of(0x51);
    let victim = identity_of(&victim_w);
    let forged_game = h64(0x44);
    file_hostile_marker(
        &conn,
        &victim,
        &forged_game,
        &attacker_pot,
        HOSTILE_HINT,
        "txFORGERY",
        5_000,
    );

    let wire = results_wire(&conn, &victim);
    let r = wire_row(&wire, &attacker_pot);
    // The row SERVES — the index does not adjudicate.
    assert_eq!(
        r["recoveryHeight"],
        serde_json::json!(HOSTILE_HINT),
        "the marker hint is served verbatim (unchanged, back-compat)"
    );
    // …and is reported as NOT chain-bound. This is the field the client's
    // money word must gate on.
    assert_eq!(
        r["potBinding"],
        serde_json::json!("unknown"),
        "THE ATTACK: nothing the network settled binds this outpoint to this \
         gameId for this identity"
    );
    assert!(r["potBindingSource"].is_null());
    // The covenant height served is the ATTACKER POT's real committed value —
    // never the hostile `1`. A client that gated on `covRecoveryHeight` alone
    // would still be reading a fact about a pot it has no key in, which is
    // precisely why the binding bit exists.
    assert_eq!(
        r["covRecoveryHeight"],
        serde_json::json!(ATTACKER_COMMITTED_HEIGHT),
        "the committed height describes the outpoint the ROW names, and the \
         attacker names the outpoint — `potBinding` is the load-bearing gate"
    );
    assert_ne!(r["covRecoveryHeight"], serde_json::json!(HOSTILE_HINT));
    // Nothing about the outcome is asserted either.
    assert_eq!(r["outcome"], serde_json::json!("unresolved"));

    // …and the attacker's own genuine seat proof for that pot did NOT leak
    // into the victim's row: the seatSig preimage commits the gameId, and the
    // identity signature is the attacker's, not the victim's.
    assert_eq!(wire["results"].as_array().unwrap().len(), 1);
}

/// (2b) The narrower variant: the hostile marker names the VICTIM'S OWN pot
/// under a FABRICATED gameId. The victim's genuine seat proof for that
/// outpoint is in the fetch (the seat query is keyed by OUTPOINT), so a
/// binding check that forgot the gameId filter would inherit it and report
/// the forged row as chain-bound.
#[test]
fn a_hostile_gameid_on_the_victims_own_pot_is_not_chain_bound() {
    let conn = production_schema_db();
    let (ka, pa) = real_key(41);
    let (_kb, pb) = real_key(42);
    let victim_w = wallet_of(0x51);
    let victim = identity_of(&victim_w);
    let opponent = format!("02{}", "bb".repeat(32));
    let real_game = h64(0x11);
    let forged_game = h64(0x99);
    let pot = h64(0xaa);
    let p = params_committing(&pa, &pb, COMMITTED_HEIGHT);
    insert_decoded_pot(&conn, &pot, None, &p, 4_000, None, None, None, false);
    // The victim's GENUINE marker — stamped LATER, so the per-pot window's
    // oldest-first representative is the forgery (the display fields come
    // from the hostile row; the money-relevant answer must not).
    file_real_v2_marker(
        &conn,
        &ka,
        &pa,
        &victim_w,
        &opponent,
        &real_game,
        &pot,
        0,
        HONEST_HINT,
        "txHONESTV2",
        9_000,
    );
    file_hostile_marker(
        &conn,
        &victim,
        &forged_game,
        &pot,
        HOSTILE_HINT,
        "txFORGERY",
        100,
    );

    let wire = results_wire(&conn, &victim);
    // One pot ⇒ one row, and it is the OLDEST marker (the forgery) that
    // supplies the display fields — the documented #281 F1b residual.
    let r = wire_row(&wire, &pot);
    assert_eq!(
        r["gameId"],
        serde_json::json!(forged_game),
        "the oldest marker owns the display fields (documented residual)"
    );
    assert_eq!(r["recoveryHeight"], serde_json::json!(HOSTILE_HINT));
    assert_eq!(
        r["potBinding"],
        serde_json::json!("unknown"),
        "the seatSig preimage commits the gameId — the victim's genuine proof \
         for this OUTPOINT does not bind the attacker's fabricated gameId"
    );
    assert!(r["potBindingSource"].is_null());
    // The covenant height is still the honest chain fact about the pot.
    assert_eq!(
        r["covRecoveryHeight"],
        serde_json::json!(COMMITTED_HEIGHT),
        "a real committed height for a real pot — but unbound to this row"
    );
}

/// (3) A pot that is PRESENT and covenant-decodable but has no verifying seat
/// marker at all: `unknown`, never an assertion. The covenant height still
/// serves (it is an honest fact about the outpoint), which is exactly the
/// separation the honesty pair exists to express.
#[test]
fn a_present_unverdicted_pot_with_no_seat_proof_is_unknown() {
    let conn = production_schema_db();
    let (_ka, pa) = real_key(41);
    let (_kb, pb) = real_key(42);
    let victim = format!("02{}", "a1".repeat(32));
    let pot = h64(0xaa);
    let p = params_committing(&pa, &pb, COMMITTED_HEIGHT);
    insert_decoded_pot(&conn, &pot, None, &p, 4_000, None, None, None, false);
    // A v1 marker only — no seat binding exists for this pot at all.
    file_marker(&conn, &victim, &pot, "txV1ONLY", 1_001);

    let r = results_wire(&conn, &victim);
    let r = wire_row(&r, &pot);
    assert_eq!(r["potBinding"], serde_json::json!("unknown"));
    assert!(r["potBindingSource"].is_null());
    assert_eq!(
        r["covRecoveryHeight"],
        serde_json::json!(COMMITTED_HEIGHT),
        "the committed height is a chain fact about the outpoint regardless"
    );
    assert!(r["verdict"].is_null(), "unspent ⇒ no verdict is asserted");
    assert_eq!(r["outcome"], serde_json::json!("unresolved"));
    // `spent: false` is the index's own record for THIS OUTPOINT — an honest
    // fact, and still not corroboration of the ROW, because the row is what
    // chose the outpoint. Unchanged by this work; asserted so the cell is
    // explicit about which fields do and do not carry provenance.
    assert_eq!(r["spent"], serde_json::json!(false));
}

/// (4) NO `pot_records` ROW AT ALL (an invented / not-yet-admitted pot): the
/// covenant height is `null` and the binding is `unknown`. Nothing is
/// asserted — in particular `spent` stays `null`, never a claimed-unspent.
#[test]
fn an_absent_pot_record_serves_null_height_and_unknown_binding() {
    let conn = production_schema_db();
    let victim = format!("02{}", "a1".repeat(32));
    let invented = h64(0xde);
    file_hostile_marker(
        &conn,
        &victim,
        &h64(0x44),
        &invented,
        HOSTILE_HINT,
        "txINVENTED",
        1_000,
    );

    let wire = results_wire(&conn, &victim);
    let r = wire_row(&wire, &invented);
    assert_eq!(r["recoveryHeight"], serde_json::json!(HOSTILE_HINT));
    assert!(
        r["covRecoveryHeight"].is_null(),
        "no pot_records row ⇒ no committed height — explicit null, never the hint"
    );
    assert_eq!(r["potBinding"], serde_json::json!("unknown"));
    assert!(r["potBindingSource"].is_null());
    assert!(r["spent"].is_null(), "never asserted-unspent");
    assert_eq!(r["outcome"], serde_json::json!("unresolved"));
}

/// (5) A committed height OUTSIDE the usable block-height range (0, or the
/// nLockTime timestamp range) is `null`, not a fake countdown — the SAME
/// range rule `/refund-view` and `/live-view` apply, through the shared
/// `valid_recovery_height` predicate.
#[test]
fn an_unusable_committed_height_serves_null_not_a_fake_countdown() {
    let conn = production_schema_db();
    let (_ka, pa) = real_key(41);
    let (_kb, pb) = real_key(42);
    let victim = format!("02{}", "a1".repeat(32));
    let pot = h64(0xaa);
    // A covenant committing height 0 — "no gate", not "claimable now".
    let p = params_committing(&pa, &pb, 0);
    insert_decoded_pot(&conn, &pot, None, &p, 4_000, None, None, None, false);
    file_marker(&conn, &victim, &pot, "txV1ONLY", 1_001);

    let wire = results_wire(&conn, &victim);
    assert!(
        wire_row(&wire, &pot)["covRecoveryHeight"].is_null(),
        "a committed 0 is not a usable height — null, never a countdown"
    );
}

/// (6) The BEEF fallback leg reaches the binding too: a LEGACY row with no
/// decoded `pot_records` columns resolves its committed params from the
/// hash-verified funding BEEF, and the binding is answered from those. Pins
/// that the hoisted `row_params` resolution did not quietly become
/// columns-only (Rule 6b — the path production still has legacy rows on).
#[test]
fn the_legacy_beef_leg_still_answers_the_binding() {
    let conn = production_schema_db();
    let victim_w = wallet_of(0x51);
    let victim = identity_of(&victim_w);
    // The REAL mainnet pot c571d433 — a genuine covenant lock, decoded here
    // from bytes, with NO decoded columns present.
    conn.execute(
        "INSERT OR IGNORE INTO pot_records (txid, outputIndex, spent, createdAt) \
         VALUES (?1, 0, 0, 1000)",
        params![ENFORCED_FUNDING_TXID],
    )
    .unwrap();
    insert_beef(
        &conn,
        ENFORCED_FUNDING_TXID,
        &beef_bytes_of(ENFORCED_FUNDING_HEX),
    );
    let p = enforced_pot_columns();
    file_marker(&conn, &victim, ENFORCED_FUNDING_TXID, "txV1", 1_001);

    let rows = query_results_rows(&conn, &victim);
    assert!(
        !no_blob(&rows[0].funding_beef_hex),
        "the legacy leg really is exercised (the BLOB was fetched)"
    );
    assert_eq!(rows[0].lock_kind, None, "no decoded columns on this row");
    // The committed params ARE recoverable from the bytes — so the binding's
    // `unknown` below is about the missing seat proof, not a dead path.
    assert_eq!(
        covenant_params_by_pot(&rows).get(&(ENFORCED_FUNDING_TXID.to_string(), 0)),
        Some(&p),
        "positive control: the BEEF leg resolves the committed keys"
    );
    let wire = results_wire(&conn, &victim);
    let r = wire_row(&wire, ENFORCED_FUNDING_TXID);
    // The COMMITTED height is served on this leg too — re-decoded per request
    // from the hash-verified funding lock, not read off a column. Pinned
    // against the REAL mainnet pot's own committed value.
    assert_eq!(
        r["covRecoveryHeight"],
        serde_json::json!(p.recovery_height),
        "the legacy BEEF leg serves the covenant-committed height"
    );
    assert_eq!(
        p.recovery_height, 956_656,
        "the fixture's committed height is the real c571d433 value — pinned so \
         the assertion above cannot move in sympathy with the code"
    );
    assert_eq!(
        r["potBinding"],
        serde_json::json!("unknown"),
        "no seat marker exists for this pot — unknown, never optimistic"
    );
    // …and the seat query was genuinely ISSUED for this pot's committed keys
    // (the chunker produced a bind for it) — the `unknown` above is "we
    // looked and found nothing", not "we never looked".
    let chunks = seat_marker_chunks(&covenant_params_by_pot(&rows));
    assert_eq!(
        chunks
            .iter()
            .flatten()
            .filter(|b| b.pot_txid == ENFORCED_FUNDING_TXID && b.pub_a_hex == hex::encode(p.pub_a))
            .count(),
        1,
        "the committed-key seat query really was issued for this pot"
    );
    assert!(!fetch_seat_markers(&conn, &rows)
        .contains_key(&(ENFORCED_FUNDING_TXID.to_ascii_lowercase(), 0)));
}
