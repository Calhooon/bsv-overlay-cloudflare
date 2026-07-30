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
    ResultsRow, RESULTS_UNKNOWN_POT_QUOTA, SEAT_MARKERS_BINDS_PER_POT,
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

fn insert_pot(conn: &Connection, txid: &str, created_at: i64, spent: bool) {
    conn.execute(
        "INSERT OR IGNORE INTO pot_records \
         (txid, outputIndex, spent, spendingTxid, spentConfirmed, createdAt) \
         VALUES (?1, 0, ?2, ?3, ?4, ?5)",
        params![
            txid,
            i32::from(spent),
            if spent { Some(h64(0xfe)) } else { None },
            i32::from(spent),
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
    let sql = seat_markers_sql(1);
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
const ENFORCED_SETTLE_HEX: &str = include_str!(
    "fixtures/91309122f5630052f7e57f7db843d26d32ae4426a9dd9b2fc2955f2fab8cf9a6.hex"
);
const ENFORCED_FUNDING_TXID: &str =
    "c571d433b8234e225af0c631f076b137b7c164cfa72f86b3e713f9ba67e3b563";
const ENFORCED_FUNDING_HEX: &str = include_str!(
    "fixtures/c571d433b8234e225af0c631f076b137b7c164cfa72f86b3e713f9ba67e3b563.hex"
);

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
    let ftx =
        low_app_layer::results::parse_raw_tx_verified(&raw, ENFORCED_FUNDING_TXID).unwrap();
    low_app_layer::results::extract_covenant_params(&ftx.outputs[0].1).unwrap()
}

/// Insert a pot_records row WITH decoded columns (+ optional verdict).
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
            i32::from(spent_height.is_some()),
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
    );
    file_marker(&conn, &victim, ENFORCED_FUNDING_TXID, "txHONEST", 1_001);
    // BLOB rows EXIST — the gate must leave them untouched.
    insert_beef(&conn, ENFORCED_FUNDING_TXID, &beef_bytes_of(ENFORCED_FUNDING_HEX));
    insert_beef(&conn, ENFORCED_SETTLE_TXID, &beef_bytes_of(ENFORCED_SETTLE_HEX));

    let rows = query_results_rows(&conn, &victim);
    assert_eq!(rows.len(), 1);
    assert!(no_blob(&rows[0].funding_beef_hex), "no funding BLOB fetched");
    assert!(no_blob(&rows[0].spender_beef_hex), "no spender BLOB fetched");
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
    insert_beef(&conn, ENFORCED_FUNDING_TXID, &beef_bytes_of(ENFORCED_FUNDING_HEX));
    insert_beef(&conn, ENFORCED_SETTLE_TXID, &beef_bytes_of(ENFORCED_SETTLE_HEX));

    let rows = query_results_rows(&conn, &victim);
    assert_eq!(rows.len(), 1);
    assert!(!no_blob(&rows[0].funding_beef_hex), "legacy row fetches the BLOBs");
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
    );
    file_marker(&conn, &victim, ENFORCED_FUNDING_TXID, "txHONEST", 1_001);
    insert_beef(&conn, ENFORCED_SETTLE_TXID, &beef_bytes_of(ENFORCED_SETTLE_HEX));

    let rows = query_results_rows(&conn, &victim);
    assert_eq!(rows.len(), 1);
    assert!(no_blob(&rows[0].funding_beef_hex), "params are columns — no funding BLOB");
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
        None, // spentHeight NULL — unconfirmed spend
    );
    file_marker(&conn, &victim, ENFORCED_FUNDING_TXID, "txHONEST", 1_001);
    insert_beef(&conn, ENFORCED_SETTLE_TXID, &beef_bytes_of(ENFORCED_SETTLE_HEX));

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
    assert_eq!(entries[0].verdict, Some(PotVerdict::WinnerA), "column verdict trusted");
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
        None, // spentHeight NULL — the BEEF fallback is the only height source
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
    assert!(!no_blob(&rows[0].spender_beef_hex), "the fallback BLOB is fetched");
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

    let sql = seat_markers_sql(1);
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
    let sql = seat_markers_sql(2);
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
