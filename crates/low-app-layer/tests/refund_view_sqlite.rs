//! `/refund-view` proofs — REAL SQLite, PRODUCTION schema (bsv-low #252
//! stage 2a).
//!
//! These tests EXECUTE the exact shipped `refund_view_sql()` against real
//! SQLite carrying the overlay's PRODUCTION migration list verbatim, with
//! rows written by the REAL producer SQL — `store_record_sql()` (tm_pot
//! admission upsert), `mark_spent_sql()` (the spend/verdict writer, exact
//! bind order), and the topic managers' `INSERT OR IGNORE` shapes for
//! `potparty_records` / `potrefund_records` — never hand-fed shapes (the
//! enumeration-defense lesson: test through the real producer path). Each
//! status branch of the honesty table is asserted, plus the gate math, the
//! backup-presence bit, the per-pot collapse, and the row cap.

use bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS;
use bsv_overlay_cloudflare::d1_discovery::{mark_spent_sql, store_record_sql};
use low_app_layer::logic::valid_identity;
use low_app_layer::refund_view::{
    assemble_refund_view, refund_view_body, refund_view_sql, RefundStatus, RefundViewRow,
    SeatAnchor, REFUND_VIEW_MAX_ROWS, REFUND_VIEW_UNKNOWN_POT_QUOTA,
};
use low_app_layer::results::PotVerdict;
use rusqlite::{params, Connection};

/// A fresh in-memory SQLite carrying the REAL production schema (same
/// tolerance discipline as `tests/results_window_sqlite.rs`: only the re-run
/// additive-ALTER error class the production runner tolerates).
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
/// a #284-decoded COVENANT row (params filled producer-shaped); None ⇒ a
/// legacy/bare row (all decoded columns NULL, exactly what a pre-#284 or
/// bare-lock admission leaves).
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
            created_at,                        // createdAt (test-controlled)
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
            i64::from(covenant),               // paramsDecoded
        ],
    )
    .expect("store_record_sql");
}

/// Record a pot spend via the REAL `mark_spent_sql()` — exact bind order
/// (see its doc: `spendingTxid, [verdict, verdictTxid,] [confirmed:
/// spendingTxid, spentHeight, spentHeight,] txid, outputIndex`).
fn mark_spent(
    conn: &Connection,
    pot_txid: &str,
    spender: &str,
    confirmed: bool,
    verdict: Option<&str>,
    spent_height: Option<i64>,
) {
    mark_spent_final(
        conn,
        pot_txid,
        spender,
        confirmed,
        verdict,
        spent_height,
        None,
    );
}

/// The full #371 shape: the finality CASE binds (probe, value, value) ride
/// EVERY `mark_spent_sql` variant, exactly as the D1 impl binds them.
#[allow(clippy::too_many_arguments)]
fn mark_spent_final(
    conn: &Connection,
    pot_txid: &str,
    spender: &str,
    confirmed: bool,
    verdict: Option<&str>,
    spent_height: Option<i64>,
    spender_final: Option<i64>,
) {
    let sql = mark_spent_sql(confirmed, verdict.is_some());
    match (confirmed, verdict) {
        (true, Some(v)) => conn.execute(
            sql,
            params![
                spender,
                v,
                spender,
                Option::<String>::None, // settleSigners (#406) — plain helper writes NULL
                spender,
                spent_height,
                spent_height,
                spender,
                spender_final,
                spender_final,
                pot_txid,
                0i64
            ],
        ),
        (true, None) => conn.execute(
            sql,
            params![
                spender,
                spender,
                spent_height,
                spent_height,
                spender,
                spender_final,
                spender_final,
                pot_txid,
                0i64
            ],
        ),
        (false, Some(v)) => conn.execute(
            sql,
            params![
                spender,
                v,
                spender,
                Option::<String>::None, // settleSigners (#406)
                spender,
                spender_final,
                spender_final,
                pot_txid,
                0i64
            ],
        ),
        (false, None) => conn.execute(
            sql,
            params![
                spender,
                spender,
                spender_final,
                spender_final,
                pot_txid,
                0i64
            ],
        ),
    }
    .expect("mark_spent_sql");
}

/// File a potparty marker — the topic manager's `INSERT OR IGNORE` shape
/// (field values copied from `tests/results_window_sqlite.rs`).
fn file_party(
    conn: &Connection,
    identity: &str,
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
            h64(0x11),
            pot_txid,
            recovery_height,
            marker_txid,
            at
        ],
    )
    .expect("insert potparty_records");
}

/// File a potparty marker with an explicit #283 admission-time latch.
///
/// MODELLING BOUNDARY (epoch Rule 17), stated because it is not obvious: this
/// helper SETS `sigValid` rather than computing it from real signatures. What
/// these cells test is the ORDERING the column drives, not the column's
/// value. The value's correctness is established where it belongs — against
/// the frozen artifacts the real client producer emits
/// (`overlay_discovery::potparty::validity` goldens) and through the real
/// writer path (`results_window_sqlite`'s `production_latch`). A cell here
/// that minted its own signatures would only re-encode this file's model of
/// the wire format.
fn file_party_latched(
    conn: &Connection,
    identity: &str,
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
            h64(0x11),
            pot_txid,
            recovery_height,
            marker_txid,
            at,
            i32::from(sig_valid)
        ],
    )
    .expect("insert potparty_records");
}

/// File a refund-backup marker — `D1PotrefundStorage::store_record`'s
/// `INSERT OR IGNORE` shape (the raw bytes are stored but must NEVER be
/// served by this view).
fn file_refund_backup(conn: &Connection, identity: &str, pot_txid: &str, marker_txid: &str) {
    conn.execute(
        "INSERT OR IGNORE INTO potrefund_records \
         (identity, gameId, potTxid, potVout, refundRawHex, \
          sigHex, txid, outputIndex, createdAt) \
         VALUES (?1, ?2, ?3, 0, '0100de', '3045ab', ?4, 0, ?5)",
        params![identity, h64(0x11), pot_txid, marker_txid, 4_000i64],
    )
    .expect("insert potrefund_records");
}

/// Execute the SHIPPED `refund_view_sql()` and map rows exactly as the route
/// does (same columns, same Option-ness).
fn query_rows(conn: &Connection, identity: &str) -> Vec<RefundViewRow> {
    let sql = refund_view_sql(None);
    let mut stmt = conn.prepare(&sql).expect("prepare refund_view_sql");
    stmt.query_map(params![identity], |r| {
        Ok(RefundViewRow {
            game_id: r.get("gameId")?,
            pot_txid: r.get("potTxid")?,
            pot_vout: r.get::<_, i64>("potVout")? as u32,
            marker_recovery_height: r.get::<_, i64>("recoveryHeight")? as u32,
            cov_recovery_height: r
                .get::<_, Option<i64>>("covRecoveryHeight")?
                .map(|v| v as u64),
            spent: r.get::<_, Option<i64>>("spent")?.map(|v| v != 0),
            spending_txid: r.get("spendingTxid")?,
            spent_confirmed: r.get::<_, Option<i64>>("spentConfirmed")?.map(|v| v != 0),
            verdict: r.get("verdict")?,
            verdict_txid: r.get("verdictTxid")?,
            spent_height: r.get::<_, Option<i64>>("spentHeight")?.map(|v| v as u64),
            backup_marker_present: r.get::<_, i64>("backupMarkerPresent")? != 0,
            // #323 MEDIUM-1 — the second confirmation signal, read through
            // the REAL SQL so the shared bar is proven against the schema.
            spender_proof_verified: r
                .get::<_, Option<i64>>("spenderProofVerified")?
                .map(|v| v != 0),
            // #371 — the third-arm inputs, read through the REAL SQL.
            spender_seen: r.get::<_, Option<i64>>("spenderSeen")?.map(|v| v != 0),
            spender_final: r.get::<_, Option<i64>>("spenderFinal")?.map(|v| v != 0),
            // #217 durable timeline stamps, read by the SAME wire names the
            // route's `RefundViewRowD1` deserializes.
            pot_admitted_at: r.get::<_, Option<i64>>("potAdmittedAt")?,
            first_party_marker_at: r.get::<_, Option<i64>>("firstPartyMarkerAt")?,
            first_spent_at: r.get::<_, Option<i64>>("firstSpentAt")?,
        })
    })
    .expect("query")
    .collect::<Result<Vec<_>, _>>()
    .expect("rows")
}

const GATE: i64 = 900_123;

/// One caller + one covenant pot, unspent — the base fixture most branches
/// start from.
fn seed_armed_pot(conn: &Connection) -> (String, String) {
    let me = h66(0xa1);
    let pot = h64(0xaa);
    admit_pot(conn, &pot, 1_000, Some(GATE));
    file_party(conn, &me, &pot, GATE, "txPARTY", 1_001);
    (me, pot)
}

// ── status branches ─────────────────────────────────────────────────────────

#[test]
fn armed_below_the_gate_and_the_backup_bit_upgrades_the_source() {
    let conn = production_schema_db();
    let (me, pot) = seed_armed_pot(&conn);

    // 45 blocks below the gate, no backup marker.
    let e = &assemble_refund_view(query_rows(&conn, &me), Some(900_078))[0];
    assert_eq!(e.status, RefundStatus::Armed);
    assert_eq!(e.status_source, Some("chain"));
    assert_eq!(e.blocks_to_gate, Some(45));
    assert!(!e.gate_passed);
    assert!(!e.backup_marker_present);
    assert_eq!(e.recovery_height, Some(GATE as u64));
    assert_eq!(e.spent, Some(false));

    // A published backup: same status, marker-aware source.
    file_refund_backup(&conn, &me, &pot, "txBACKUP");
    let e = &assemble_refund_view(query_rows(&conn, &me), Some(900_078))[0];
    assert_eq!(e.status, RefundStatus::Armed);
    assert_eq!(e.status_source, Some("chain+marker"));
    assert!(e.backup_marker_present);
}

#[test]
fn gate_open_at_and_past_the_gate() {
    let conn = production_schema_db();
    let (me, _pot) = seed_armed_pot(&conn);

    for (tip, blocks) in [(GATE, 0u64), (GATE + 77, 0u64)] {
        let e = &assemble_refund_view(query_rows(&conn, &me), Some(tip as u64))[0];
        assert_eq!(e.status, RefundStatus::GateOpen);
        assert_eq!(e.status_source, Some("chain"));
        assert_eq!(
            e.blocks_to_gate,
            Some(blocks),
            "clamped at 0, never negative"
        );
        assert!(e.gate_passed);
    }
}

#[test]
fn landed_on_a_confirmed_decoded_refund_verdict() {
    let conn = production_schema_db();
    let (me, pot) = seed_armed_pot(&conn);
    file_refund_backup(&conn, &me, &pot, "txBACKUP");
    let spender = h64(0xfe);
    mark_spent(&conn, &pot, &spender, true, Some("refund"), Some(900_170));

    let e = &assemble_refund_view(query_rows(&conn, &me), Some(900_200))[0];
    assert_eq!(e.status, RefundStatus::Landed);
    // Chain facts alone decide a landing — the marker bit does not dilute it.
    assert_eq!(e.status_source, Some("chain"));
    assert_eq!(e.verdict, Some(PotVerdict::Refund));
    assert_eq!(e.spending_txid.as_deref(), Some(spender.as_str()));
    assert_eq!(e.spent_confirmed, Some(true));
    assert_eq!(e.spent_height, Some(900_170));
}

#[test]
fn superseded_on_a_confirmed_settle_verdict() {
    let conn = production_schema_db();
    let (me, pot) = seed_armed_pot(&conn);
    mark_spent(
        &conn,
        &pot,
        &h64(0xfe),
        true,
        Some("winner-a"),
        Some(900_150),
    );

    let e = &assemble_refund_view(query_rows(&conn, &me), Some(900_200))[0];
    assert_eq!(e.status, RefundStatus::Superseded);
    assert_eq!(e.status_source, Some("chain"));
    assert_eq!(e.verdict, Some(PotVerdict::WinnerA));
}

/// #371 END-TO-END through the PRODUCTION pipeline: the real `mark_spent_sql`
/// writer latches the spender's finality, the real `NETWORK_SEEN_INSERT_SQL`
/// latch records the overlay's own witness, the real `refund_view_sql` joins
/// both, and the view supersedes an UNCONFIRMED (no flag, no proof) settle at
/// the ruling-3 SEEN bar. Each half alone is then proven insufficient through
/// the same pipeline — the ungated-plant (final, unwitnessed) and the parked
/// refund (witnessed, non-final) both stay UNKNOWN.
#[test]
fn seen_and_final_settle_supersedes_unconfirmed_end_to_end() {
    let conn = production_schema_db();
    let (me, pot) = seed_armed_pot(&conn);
    let settle = h64(0xfe);

    // The overlay's own SEEN witness + a FINAL spender, UNCONFIRMED spend.
    mark_spent_final(&conn, &pot, &settle, false, Some("winner-a"), None, Some(1));
    conn.execute(
        bsv_overlay_cloudflare::ops::NETWORK_SEEN_INSERT_SQL,
        params![settle],
    )
    .expect("network_seen latch");

    let e = &assemble_refund_view(query_rows(&conn, &me), Some(900_200))[0];
    assert_eq!(
        e.spent_confirmed,
        Some(false),
        "no merkle-class signal exists"
    );
    assert_eq!(
        e.status,
        RefundStatus::Superseded,
        "SEEN + FINAL publishes at the ruling-3 bar with no merkle proof"
    );
    assert_eq!(e.status_source, Some("chain"));

    // Half 1 alone — FINAL but UNWITNESSED (the ungated-/submit plant,
    // epoch Rule 21): a fresh pot, same writer, no latch row.
    let pot2 = h64(0xab);
    admit_pot(&conn, &pot2, 1_000, Some(GATE));
    file_party(&conn, &me, &pot2, GATE, "txPARTY2", 1_001);
    let planted = h64(0xfd);
    mark_spent_final(
        &conn,
        &pot2,
        &planted,
        false,
        Some("winner-a"),
        None,
        Some(1),
    );
    let e2 = assemble_refund_view(query_rows(&conn, &me), Some(900_200))
        .into_iter()
        .find(|e| e.pot_txid == pot2)
        .expect("pot2 row");
    assert_eq!(
        e2.status,
        RefundStatus::Unknown,
        "final bytes with no network witness stay behind the merkle bar"
    );

    // Half 2 alone — WITNESSED but NON-FINAL (the parked refund as an
    // indexer reports it): latch present, finality 0.
    let pot3 = h64(0xac);
    admit_pot(&conn, &pot3, 1_000, Some(GATE));
    file_party(&conn, &me, &pot3, GATE, "txPARTY3", 1_001);
    let parked = h64(0xfc);
    mark_spent_final(&conn, &pot3, &parked, false, Some("refund"), None, Some(0));
    conn.execute(
        bsv_overlay_cloudflare::ops::NETWORK_SEEN_INSERT_SQL,
        params![parked],
    )
    .expect("network_seen latch");
    let e3 = assemble_refund_view(query_rows(&conn, &me), Some(900_200))
        .into_iter()
        .find(|e| e.pot_txid == pot3)
        .expect("pot3 row");
    assert_eq!(
        e3.status,
        RefundStatus::Unknown,
        "a witnessed NON-FINAL spender is the parked refund — the #323 bar holds verbatim"
    );
}

#[test]
fn unknown_on_every_incomplete_fact_set() {
    let conn = production_schema_db();
    let me = h66(0xa1);

    // (a) Party marker but the pot was never indexed: spent is genuinely
    // unknown — never asserted unspent, never "armed" on absence of evidence.
    let ghost = h64(0xd0);
    file_party(&conn, &me, &ghost, GATE, "txGHOST", 1_000);
    // (b) Spent but UNCONFIRMED (a displaceable intent), verdict riding it.
    let unconf = h64(0xd1);
    admit_pot(&conn, &unconf, 1_100, Some(GATE));
    file_party(&conn, &me, &unconf, GATE, "txUNCONF", 1_101);
    mark_spent(&conn, &unconf, &h64(0xf1), false, Some("refund"), None);
    // (c) CONFIRMED spend, no decoded verdict at all.
    let noverdict = h64(0xd2);
    admit_pot(&conn, &noverdict, 1_200, Some(GATE));
    file_party(&conn, &me, &noverdict, GATE, "txNOV", 1_201);
    mark_spent(&conn, &noverdict, &h64(0xf2), true, None, Some(900_150));
    // (d) STALE verdict: decoded for spender S1, pointer then moved to S2
    // (unconfirmed pointer change keeps the old verdict columns — the
    // documented `mark_spent_sql` behavior the reader must neutralize), then
    // S2 confirmed WITHOUT a verdict.
    let stale = h64(0xd3);
    admit_pot(&conn, &stale, 1_300, Some(GATE));
    file_party(&conn, &me, &stale, GATE, "txSTALE", 1_301);
    mark_spent(&conn, &stale, &h64(0xf3), false, Some("refund"), None);
    mark_spent(&conn, &stale, &h64(0xf4), true, None, Some(900_160));

    let entries = assemble_refund_view(query_rows(&conn, &me), Some(900_200));
    assert_eq!(entries.len(), 4);
    for e in &entries {
        assert_eq!(
            (e.status, e.status_source),
            (RefundStatus::Unknown, None),
            "pot {} must be unknown/null",
            e.pot_txid
        );
    }
    let ghost_entry = entries.iter().find(|e| e.pot_txid == ghost).unwrap();
    assert_eq!(ghost_entry.spent, None, "never asserted unspent");
    assert_eq!(ghost_entry.verdict, None);
    // (b): the decoded verdict IS served (it is a pure decode of the
    // RECORDED spender, trusted for that pointer — same posture as
    // `/results`), but the STATUS refuses to call it landed while the spend
    // is displaceable.
    let unconf_entry = entries.iter().find(|e| e.pot_txid == unconf).unwrap();
    assert_eq!(unconf_entry.verdict, Some(PotVerdict::Refund));
    assert_eq!(unconf_entry.spent_confirmed, Some(false));
    // (c)/(d): no decoded verdict / a STALE one — served null, never a guess.
    let noverdict_entry = entries.iter().find(|e| e.pot_txid == noverdict).unwrap();
    assert_eq!(noverdict_entry.verdict, None);
    let stale_entry = entries.iter().find(|e| e.pot_txid == stale).unwrap();
    assert_eq!(
        stale_entry.verdict, None,
        "stale verdictTxid must not serve"
    );
    assert_eq!(
        stale_entry.spending_txid.as_deref(),
        Some(h64(0xf4).as_str())
    );
    assert_eq!(stale_entry.spent_confirmed, Some(true));
}

// ── recovery-height sourcing ────────────────────────────────────────────────

#[test]
fn covenant_committed_height_beats_the_marker_hint() {
    let conn = production_schema_db();
    let me = h66(0xa1);
    let pot = h64(0xaa);
    admit_pot(&conn, &pot, 1_000, Some(GATE));
    // The caller's marker (byte-format-admitted) claims a WRONG height.
    file_party(&conn, &me, &pot, 111, "txPARTY", 1_001);

    let e = &assemble_refund_view(query_rows(&conn, &me), Some(900_078))[0];
    assert_eq!(e.recovery_height, Some(GATE as u64), "chain truth wins");
    assert_eq!(e.blocks_to_gate, Some(45));
}

#[test]
fn marker_height_serves_only_when_no_covenant_decode_exists() {
    let conn = production_schema_db();
    let me = h66(0xa1);
    let pot = h64(0xaa);
    admit_pot(&conn, &pot, 1_000, None); // bare/legacy: no decoded columns
    file_party(&conn, &me, &pot, GATE, "txPARTY", 1_001);

    let e = &assemble_refund_view(query_rows(&conn, &me), Some(900_078))[0];
    assert_eq!(e.recovery_height, Some(GATE as u64));
    assert_eq!(e.status, RefundStatus::Armed);
}

// ── backup presence ─────────────────────────────────────────────────────────

#[test]
fn both_seats_backups_never_multiply_rows_and_bytes_never_leak() {
    let conn = production_schema_db();
    let (me, pot) = seed_armed_pot(&conn);
    // BOTH seats publish a backup for the same pot (the schema note says
    // they may) — presence must collapse to one bit, not two rows.
    file_refund_backup(&conn, &me, &pot, "txBACKUP-A");
    file_refund_backup(&conn, &h66(0xbb), &pot, "txBACKUP-B");

    let rows = query_rows(&conn, &me);
    assert_eq!(rows.len(), 1, "EXISTS probe must not multiply the pot row");
    assert!(rows[0].backup_marker_present);

    // Display-only contract: the stored refund bytes NEVER reach the wire.
    let entries = assemble_refund_view(rows, Some(900_078));
    let body = refund_view_body(&me, Some(900_078), &entries);
    assert!(
        !body.contains("0100de"),
        "refundRawHex bytes leaked into the body"
    );
    assert!(!body.contains("refundRawHex"));
}

// ── fail-safe empty + window bounds ─────────────────────────────────────────

#[test]
fn unknown_identity_is_a_well_formed_empty_answer() {
    let conn = production_schema_db();
    seed_armed_pot(&conn);

    // A valid identity with nothing indexed: zero rows, well-formed body.
    let stranger = h66(0xee);
    let rows = query_rows(&conn, &stranger);
    assert!(rows.is_empty());
    let v: serde_json::Value = serde_json::from_str(&refund_view_body(
        &stranger,
        None,
        &assemble_refund_view(rows, None),
    ))
    .unwrap();
    assert_eq!(v["refunds"], serde_json::json!([]));

    // The route's invalid-identity guard (fail-safe-empty 200, never an
    // error) keys on the same `valid_identity` every identity surface uses.
    assert!(!valid_identity(""));
    assert!(!valid_identity("zz"));
    assert!(!valid_identity(&h64(0xaa))); // 64 hex — a txid, not an identity
    assert!(valid_identity(&h66(0xee)));
}

#[test]
fn dust_replays_collapse_to_one_row_per_pot() {
    let conn = production_schema_db();
    let (me, pot) = seed_armed_pot(&conn);
    // Every replay files a DIFFERENT height so the assertion below can tell
    // WHICH row won the partition (delta round 2: with identical heights an
    // ASC→DESC drift in the oldest-representative ORDER BY passed unseen).
    for i in 0..40 {
        file_party(
            &conn,
            &me,
            &pot,
            GATE + 1_000 + i,
            &format!("txREPLAY{i:03}"),
            2_000 + i,
        );
    }
    let rows = query_rows(&conn, &me);
    assert_eq!(
        rows.len(),
        1,
        "one pot ⇒ one row, whatever the replay count"
    );
    // The representative is the OLDEST marker (the honest funding-time one) —
    // the only order an attacker cannot win by simply publishing later.
    assert_eq!(rows[0].marker_recovery_height, GATE as u32);
}

/// The #281 window rules, exercised BEHAVIORALLY against this route's OWN
/// SQL (the structural string pin is not enough — `refund_view_sql` can
/// drift independently of `results_sql`): 60 dust replays of the victim's
/// real pot + 120 markers naming INVENTED pots, every attacker row NEWER
/// than the honest marker. The real pot must survive within one quota of
/// the top; ghost promotion is bounded to the newest
/// [`REFUND_VIEW_UNKNOWN_POT_QUOTA`] unknown pots; the demoted tier fills
/// the rest newest-first.
#[test]
fn unknown_pot_quota_bounds_ghost_promotion_and_the_real_pot_survives() {
    let conn = production_schema_db();
    let (me, honest_pot) = seed_armed_pot(&conn); // indexed, createdAt 1_000
    const REPLAYS: i64 = 60;
    const GHOSTS: u64 = 120;
    for i in 0..REPLAYS {
        file_party(
            &conn,
            &me,
            &honest_pot,
            GATE,
            &format!("txREPLAY{i:03}"),
            2_000 + i,
        );
    }
    let ghost_txid = |i: u64| format!("{:064x}", 0xdead_0000_u64 + i);
    for i in 0..GHOSTS {
        // Never admitted to pot_records — each a distinct partition, so only
        // the existence tier + quota bound them.
        file_party(
            &conn,
            &me,
            &ghost_txid(i),
            GATE,
            &format!("txGHOST{i:03}"),
            3_000 + i as i64,
        );
    }

    let rows = query_rows(&conn, &me);
    assert_eq!(rows.len(), REFUND_VIEW_MAX_ROWS, "page full");
    let pots: Vec<&String> = rows.iter().map(|r| &r.pot_txid).collect();
    assert_eq!(
        pots.iter().filter(|t| ***t == honest_pot).count(),
        1,
        "the real pot survives exactly once (replays collapsed)"
    );
    // Promotion is bounded: every ghost that sorts ahead of the real pot
    // sits inside the reserved quota — and here, with every ghost newer,
    // the quota is EXACTLY consumed: the newest quota-many ghosts.
    let pos = pots.iter().position(|t| **t == honest_pot).unwrap();
    assert_eq!(
        pos, REFUND_VIEW_UNKNOWN_POT_QUOTA,
        "quota-many promoted ghosts, no more"
    );
    let promoted: Vec<String> = (0..REFUND_VIEW_UNKNOWN_POT_QUOTA as u64)
        .map(|k| ghost_txid(GHOSTS - 1 - k))
        .collect();
    assert_eq!(
        pots[..pos].to_vec(),
        promoted.iter().collect::<Vec<_>>(),
        "the promoted slice is the NEWEST ghosts, newest first (eviction ordering)"
    );
    // The demoted tier fills the remainder newest-first: the row after the
    // real pot is the newest UN-promoted ghost, and the oldest ghosts fell
    // off the page entirely.
    assert_eq!(
        *pots[pos + 1],
        ghost_txid(GHOSTS - 1 - REFUND_VIEW_UNKNOWN_POT_QUOTA as u64)
    );
    let dropped = (GHOSTS as usize + 1) - REFUND_VIEW_MAX_ROWS; // 21 oldest ghosts
    for i in 0..dropped as u64 {
        assert!(
            !pots.iter().any(|t| **t == ghost_txid(i)),
            "oldest ghost {i} must have fallen off, not a real pot"
        );
    }
    // Ghost rows arrive with the fail-safe unknown shape (spent null).
    assert!(rows[0].spent.is_none() && rows[0].pot_txid != honest_pot);
}

/// bsv-low #283 on `/refund-view` — the same 120-ghost flood as the cell
/// above, plus the bypass the quota never saw (bsv-low#347): a FREE
/// `pot_records` row per ghost, so every ghost reads `unknownPot = 0`, lands
/// unconditionally in tier 0 ordered freshest-first, and never touches the
/// quota at all. Pre-#283 that erases the page outright — no quota
/// allocation, however clever, bounds it.
///
/// It cannot now, because this window is scoped to ONE identity and the
/// ghost must NAME the victim to appear in it: the marker's identity
/// signature binds that name, so a ghost latches `sigValid = 0` and sorts
/// behind every honest row whatever `pot_records` says about its pot.
///
/// This is the money-visible recovery surface, so both legs are executed:
/// the flood, and the LEGACY control that shows the flood is genuinely
/// capable of erasing the page.
#[test]
fn free_ghost_pot_records_cannot_erase_the_refund_view() {
    const GHOSTS: u64 = 120;
    let ghost_txid = |i: u64| format!("{:064x}", 0xdead_0000_u64 + i);

    // ---- with the latch: the honest pot is FIRST, every ghost behind it.
    let conn = production_schema_db();
    let (me, honest_pot) = seed_armed_pot(&conn);
    conn.execute(
        "UPDATE potparty_records SET sigValid = 1 WHERE potTxid = ?1",
        params![honest_pot],
    )
    .expect("latch the honest marker");
    for i in 0..GHOSTS {
        // A fabricated pot_records row: free, stamped NOW-ish, so the ghost
        // is INDEXED as far as this query can tell.
        conn.execute(
            "INSERT OR IGNORE INTO pot_records (txid, outputIndex, spent, \
             spentConfirmed, createdAt) VALUES (?1, 0, 0, 0, ?2)",
            params![ghost_txid(i), 9_000 + i as i64],
        )
        .expect("free ghost pot_records row");
        file_party_latched(
            &conn,
            &me,
            &ghost_txid(i),
            GATE,
            &format!("txGHOST{i:03}"),
            9_000 + i as i64,
            false,
        );
    }
    let rows = query_rows(&conn, &me);
    assert_eq!(
        rows[0].pot_txid, honest_pot,
        "the victim's real pot is the FIRST row under a 120-ghost flood that \
         bypasses the unknown-pot quota entirely"
    );

    // ---- LEGACY CONTROL: same world, every row pre-migration. The flood
    // erases the honest pot from the page, which is what makes the leg above
    // a measurement rather than a tautology (epoch Rule 12a).
    conn.execute("UPDATE potparty_records SET sigValid = NULL", [])
        .expect("legacy-ize");
    let legacy = query_rows(&conn, &me);
    assert!(
        !legacy.iter().any(|r| r.pot_txid == honest_pot),
        "PRE-#283 CONTROL: the same flood erases an all-legacy page"
    );
}

#[test]
fn row_cap_bounds_the_page_and_a_full_book_survives() {
    let conn = production_schema_db();
    let me = h66(0xa1);
    let n = REFUND_VIEW_MAX_ROWS + 10;
    for i in 0..n {
        let pot = format!("{:064x}", 0x1000_u64 + i as u64);
        admit_pot(&conn, &pot, 1_000 + i as i64, Some(GATE));
        file_party(
            &conn,
            &me,
            &pot,
            GATE,
            &format!("txM{i:03}"),
            1_000 + i as i64,
        );
    }
    let rows = query_rows(&conn, &me);
    assert_eq!(rows.len(), REFUND_VIEW_MAX_ROWS, "hard cap");
    let unique: std::collections::HashSet<&String> = rows.iter().map(|r| &r.pot_txid).collect();
    assert_eq!(unique.len(), REFUND_VIEW_MAX_ROWS, "one row per pot");
    // Newest pots first — the 10 oldest fell off, not the newest.
    assert_eq!(
        rows[0].pot_txid,
        format!("{:064x}", 0x1000_u64 + (n as u64 - 1))
    );
}

// ── #217 durable timeline (the presence audit trail, RECORD half) ───────────
//
// Every cell here drives the SHIPPED producers — `store_record_sql()` for the
// pot admission stamp, the topic manager's INSERT for the marker stamp,
// `mark_spent_sql()` for the hand-end stamp — and reads them back through the
// SHIPPED `refund_view_sql()`. Nothing is hand-fed into `RefundViewRow`.

/// The three stamps arrive with the values their producers wrote, and
/// `seatAnchor` names the INDEX-backed one when the pot is indexed.
#[test]
fn the_timeline_serves_the_stamps_its_producers_wrote() {
    let conn = production_schema_db();
    let me = h66(0xa1);
    let pot = h64(0xaa);
    admit_pot(&conn, &pot, 1_000, Some(GATE));
    file_party(&conn, &me, &pot, GATE, "txPARTY", 1_007);

    // Unspent: the two START stamps are present, the END stamp is not — and
    // absent must not read as a zero time.
    let e = &assemble_refund_view(query_rows(&conn, &me), Some(900_078))[0];
    assert_eq!(e.pot_admitted_at, Some(1_000));
    assert_eq!(e.first_party_marker_at, Some(1_007));
    assert_eq!(
        e.first_spent_at, None,
        "no accepted spend write yet — and null is not 0"
    );
    assert_eq!(e.seat_anchor, Some(SeatAnchor::PotAdmission));

    // A spend lands: the END stamp appears, the START stamps do not move.
    mark_spent(&conn, &pot, &h64(0xfe), true, Some("refund"), Some(900_170));
    let e = &assemble_refund_view(query_rows(&conn, &me), Some(900_200))[0];
    assert_eq!(
        e.pot_admitted_at,
        Some(1_000),
        "start stamps are write-once"
    );
    assert_eq!(e.first_party_marker_at, Some(1_007));
    assert!(
        e.first_spent_at.is_some_and(|t| t > 1_000),
        "the hand-end stamp is the observation time, not the admission time: {:?}",
        e.first_spent_at
    );
    assert_eq!(e.spent_height, Some(900_170));
}

/// A JOIN MISS degrades the anchor honestly instead of fabricating a start.
///
/// The pot is never admitted, so `pot_records` has no row: `potAdmittedAt`
/// and `firstSpentAt` are `null` (spend status genuinely unknown — the row
/// already says `spent: null`) and the anchor falls back to the MARKER-backed
/// stamp, which is exactly the weaker claim a consumer must be told about.
#[test]
fn a_join_miss_falls_back_to_the_marker_anchor_and_says_so() {
    let conn = production_schema_db();
    let me = h66(0xa1);
    let ghost = h64(0xcc);
    file_party(&conn, &me, &ghost, GATE, "txGHOST", 2_222);

    let e = &assemble_refund_view(query_rows(&conn, &me), Some(900_078))[0];
    assert_eq!(e.spent, None, "never asserted unspent");
    assert_eq!(e.pot_admitted_at, None);
    assert_eq!(e.first_spent_at, None);
    assert_eq!(e.first_party_marker_at, Some(2_222));
    assert_eq!(
        e.seat_anchor,
        Some(SeatAnchor::PartyMarker),
        "marker-backed, and the consumer is told which"
    );
    assert_eq!(e.seat_anchor.map(SeatAnchor::as_str), Some("party-marker"));
}

/// The hand-end stamp is WRITE-ONCE across the real spend lifecycle, and the
/// #228 age anchor is NOT SERVED at all.
///
/// A 0-conf pointer, then its confirm, then a reorg-displacing spender: three
/// accepted `mark_spent_sql` writes, each of which re-stamps `spentAt`. The
/// served `firstSpentAt` must survive all three unchanged — the whole reason
/// the durable stamp is a separate column. The sentinel makes MOVED and
/// NOT-MOVED distinguishable without sleeping (all three writes land inside
/// one wall-clock second).
#[test]
fn the_served_hand_end_stamp_survives_the_whole_spend_lifecycle() {
    let conn = production_schema_db();
    let me = h66(0xa1);
    let pot = h64(0xaa);
    admit_pot(&conn, &pot, 1_000, Some(GATE));
    file_party(&conn, &me, &pot, GATE, "txPARTY", 1_007);

    const SENTINEL: i64 = 1_000_000;
    mark_spent(&conn, &pot, &h64(0xfe), false, Some("refund"), None);
    conn.execute(
        "UPDATE pot_records SET spentAt = ?1, firstSpentAt = ?1 WHERE txid = ?2",
        params![SENTINEL, pot],
    )
    .expect("seed both anchors");

    for (spender, height) in [(h64(0xfe), 900_170i64), (h64(0xef), 900_180)] {
        mark_spent(&conn, &pot, &spender, true, None, Some(height));
        let e = &assemble_refund_view(query_rows(&conn, &me), Some(900_200))[0];
        assert_eq!(
            e.first_spent_at,
            Some(SENTINEL),
            "the served hand-end stamp must not move on the {spender} write"
        );
        assert_eq!(e.spent_height, Some(height as u64), "the height DID move");
    }

    // CONTROL, and the point of the cell: the moving anchor really did move,
    // so "did not move" above is a measurement rather than a schema in which
    // nothing changes. Read straight from the table — this view never serves
    // `spentAt`, on purpose.
    let moved: i64 = conn
        .query_row(
            "SELECT spentAt FROM pot_records WHERE txid = ?1",
            params![pot],
            |r| r.get(0),
        )
        .expect("spentAt");
    assert!(
        moved > SENTINEL,
        "the #228 age anchor moved ({moved}) while the durable one held"
    );
    assert!(
        !refund_view_sql(None).contains("spentAt AS"),
        "`spentAt` is a MOVING age gate and must never be projected as an \
         audit stamp — `firstSpentAt` is the served one"
    );
}

/// AUDIT ONLY: no timeline stamp can move a money word.
///
/// Drives the real assembler over the real SQL twice — once with no timeline
/// data reachable at all, once with every stamp present — and asserts the
/// status half is byte-identical. A future edit that fed a stamp into
/// `derive_refund_status` (the bar the issue sets: "must NOT become a money
/// gate") fails here.
#[test]
fn the_timeline_never_moves_the_status() {
    let bare = production_schema_db();
    let stamped = production_schema_db();
    let me = h66(0xa1);
    let pot = h64(0xaa);

    for conn in [&bare, &stamped] {
        admit_pot(conn, &pot, 1_000, Some(GATE));
        file_party(conn, &me, &pot, GATE, "txPARTY", 1_007);
        mark_spent(conn, &pot, &h64(0xfe), true, Some("refund"), Some(900_170));
    }
    // Erase every timeline stamp on the control — nothing else differs.
    bare.execute(
        "UPDATE pot_records SET createdAt = NULL, firstSpentAt = NULL",
        [],
    )
    .expect("strip pot stamps");
    bare.execute("UPDATE potparty_records SET createdAt = NULL", [])
        .expect("strip marker stamp");

    let a = &assemble_refund_view(query_rows(&bare, &me), Some(900_200))[0];
    let b = &assemble_refund_view(query_rows(&stamped, &me), Some(900_200))[0];
    assert_eq!(a.status, b.status);
    assert_eq!(a.status_source, b.status_source);
    assert_eq!(a.verdict, b.verdict);
    assert_eq!(a.gate_passed, b.gate_passed);
    assert_eq!(a.blocks_to_gate, b.blocks_to_gate);
    assert_eq!(a.recovery_height, b.recovery_height);
    assert_eq!(a.status, RefundStatus::Landed, "positive control");
    // …and the two really did differ in the timeline half.
    assert_eq!(a.pot_admitted_at, None);
    assert_eq!(a.seat_anchor, None, "neither stamp known ⇒ no anchor");
    assert!(b.pot_admitted_at.is_some() && b.first_spent_at.is_some());
    assert_eq!(b.seat_anchor, Some(SeatAnchor::PotAdmission));
}

/// The wire block: exact key set, nullability, and the ONE repeated value.
#[test]
fn the_timeline_wire_block_is_the_documented_shape() {
    let conn = production_schema_db();
    let me = h66(0xa1);
    let pot = h64(0xaa);
    admit_pot(&conn, &pot, 1_000, Some(GATE));
    file_party(&conn, &me, &pot, GATE, "txPARTY", 1_007);
    mark_spent(&conn, &pot, &h64(0xfe), true, Some("refund"), Some(900_170));

    let entries = assemble_refund_view(query_rows(&conn, &me), Some(900_200));
    let v: serde_json::Value =
        serde_json::from_str(&refund_view_body(&me, Some(900_200), &entries)).expect("json");
    let row = &v["refunds"][0];
    let t = &row["timeline"];

    let keys: std::collections::BTreeSet<&str> = t
        .as_object()
        .expect("timeline object")
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        [
            "firstPartyMarkerAt",
            "firstSpentAt",
            "potAdmittedAt",
            "seatAnchor",
            "spentHeight",
        ]
        .into_iter()
        .collect::<std::collections::BTreeSet<&str>>(),
        "the timeline key set is a contract"
    );
    assert_eq!(t["potAdmittedAt"], serde_json::json!(1_000));
    assert_eq!(t["firstPartyMarkerAt"], serde_json::json!(1_007));
    assert!(t["firstSpentAt"].is_i64());
    assert_eq!(t["seatAnchor"], serde_json::json!("pot-admission"));
    // ONE column, read once — the repeat can never disagree with the
    // top-level field it mirrors.
    assert_eq!(t["spentHeight"], row["spentHeight"]);
    assert_eq!(t["spentHeight"], serde_json::json!(900_170));

    // A pot with nothing to say says null, never 0 and never a missing key.
    let ghost = h64(0xcc);
    file_party(&conn, &me, &ghost, GATE, "txGHOST", 2_222);
    let entries = assemble_refund_view(query_rows(&conn, &me), Some(900_200));
    let v: serde_json::Value =
        serde_json::from_str(&refund_view_body(&me, Some(900_200), &entries)).expect("json");
    let g = v["refunds"]
        .as_array()
        .expect("rows")
        .iter()
        .find(|r| r["potTxid"] == serde_json::json!(ghost))
        .expect("the ghost row is SERVED, never hidden");
    assert_eq!(g["timeline"]["potAdmittedAt"], serde_json::Value::Null);
    assert_eq!(g["timeline"]["firstSpentAt"], serde_json::Value::Null);
    assert_eq!(g["timeline"]["spentHeight"], serde_json::Value::Null);
    assert_eq!(
        g["timeline"]["seatAnchor"],
        serde_json::json!("party-marker")
    );
}

// ── #375 — the pre-launch era write-off ─────────────────────────────────────

/// The pot-txid page served by the SHIPPED SQL under an era cutoff.
fn refund_pot_txids(conn: &Connection, identity: &str, era: Option<i64>) -> Vec<String> {
    let sql = refund_view_sql(era);
    let mut stmt = conn
        .prepare(&sql)
        .unwrap_or_else(|e| panic!("refund_view_sql({era:?}) did not PREPARE: {e}\n{sql}"));
    let map = |r: &rusqlite::Row| r.get::<_, String>("potTxid");
    let rows = match era {
        Some(ms) => stmt.query_map(params![identity, ms], map),
        None => stmt.query_map(params![identity], map),
    }
    .expect("query");
    rows.collect::<Result<Vec<_>, _>>().expect("rows")
}

/// #375 through the SHIPPED `/refund-view` SQL: a pot admitted one second
/// before the cutoff is DROPPED (its post-cutoff marker cannot resurrect
/// it), the at-cutoff pot is KEPT (the `>=` boundary + the seconds→ms unit
/// pin), an unknown pot anchors on its marker stamp, and `None` serves the
/// full history byte-identically to today.
#[test]
fn the_written_off_era_is_dropped_and_the_unset_cutoff_is_inert() {
    let conn = production_schema_db();
    let me = h66(0xd1);
    const CUT_MS: i64 = 1_754_500_000_000;
    const CUT_SECS: i64 = CUT_MS / 1000;

    let pre = h64(0xd2);
    let post = h64(0xd3);
    admit_pot(&conn, &pre, CUT_SECS - 1, Some(GATE));
    admit_pot(&conn, &post, CUT_SECS, Some(GATE));
    file_party(&conn, &me, &pre, 1, &"e1".repeat(32), CUT_SECS + 5);
    file_party(&conn, &me, &post, 1, &"e2".repeat(32), CUT_SECS + 6);
    let unknown_pre = h64(0xd4);
    let unknown_post = h64(0xd5);
    file_party(&conn, &me, &unknown_pre, 1, &"e3".repeat(32), CUT_SECS - 10);
    file_party(
        &conn,
        &me,
        &unknown_post,
        1,
        &"e4".repeat(32),
        CUT_SECS + 10,
    );

    let served = refund_pot_txids(&conn, &me, Some(CUT_MS));
    assert!(served.contains(&post), "the at-cutoff pot is KEPT (>= bar)");
    assert!(
        served.contains(&unknown_post),
        "a fresh unknown pot with a post-cutoff marker is KEPT"
    );
    assert_eq!(
        served.len(),
        2,
        "the pre-cutoff pot and the pre-cutoff unknown marker are DROPPED: {served:?}"
    );

    let all = refund_pot_txids(&conn, &me, None);
    assert_eq!(all.len(), 4, "None serves the full history: {all:?}");
    for pot in [&pre, &post, &unknown_pre, &unknown_post] {
        assert!(all.contains(pot), "{pot} missing from the None arm");
    }
}
