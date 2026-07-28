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
use low_app_layer::results::{results_sql, RESULTS_MAX_ROWS};
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
    assert_eq!(got.len(), RESULTS_MAX_ROWS, "the window is still full…");
    assert_eq!(
        got.iter().filter(|t| **t == honest_pot).count(),
        1,
        "…and the victim's real pot is present exactly ONCE — the 1 honest \
         plus {DUST_REPLAYS} replayed marker rows naming it collapse to one slot"
    );
    assert_eq!(
        got[0], honest_pot,
        "and it ranks FIRST: the existence tier sinks every row naming a pot \
         the index has never seen below every row naming a real one"
    );
    // Ghost-pot rows are NOT erased — a pot whose `tm_pot` admission simply
    // has not landed yet (or a legacy pre-pot-index escrow) must still be
    // reachable. They only fill the slots real pots left over.
    assert_eq!(
        got[1..].iter().filter(|t| t.starts_with("0000")).count(),
        99,
        "ghosts occupy leftovers only"
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
    let first_ghost = baseline
        .iter()
        .position(|(t, _)| ghosts.contains(t))
        .expect("unindexed pots are still returned");
    assert_eq!(first_ghost, 40, "all 40 real pots rank ahead of all ghosts");
}
