//! `/live-view` D1-half proofs — REAL SQLite, PRODUCTION schema (bsv-low
//! #252 stage 2a step 3).
//!
//! These tests EXECUTE the exact shipped `live_view_sql()` against real
//! SQLite carrying the overlay's PRODUCTION migration list verbatim, with
//! rows written by the REAL producer SQL — `store_record_sql()` (tm_pot
//! admission upsert), `mark_spent_sql()` (the spend writer, exact bind
//! order), and the topic manager's `INSERT OR IGNORE` shape for
//! `potparty_records` (v1 AND v2, the v2 rows carrying REAL secp256k1 seat
//! signatures + REAL BRC-42 identity signatures) — never hand-fed shapes
//! (the enumeration-defense lesson: test through the real producer path).
//! Asserted: the liveness filter (unspent live, unconfirmed-spend live,
//! confirmed-spend EXCLUDED, join-miss included as unknown), the dust
//! window (per-pot collapse + oldest representative + ghost quota), the
//! gate math, fail-safe empty on a bad identity, the row cap — and the two
//! adversarial-gate cells: the fan-out target selection SURVIVING a
//! quota-many dust window (MEDIUM-2 + HIGH-1b), and the case↔pot binding
//! honesty when two live pots share one gameId (HIGH-1a). The tower
//! transport is pure-function territory (`live_view.rs` unit tests +
//! `run_fanout`'s injectable seam) — nothing here fetches.

use bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS;
use bsv_overlay_cloudflare::d1_discovery::{mark_spent_sql, store_record_sql};
use bsv_rs::wallet::{Counterparty, CreateSignatureArgs, ProtoWallet};
use low_app_layer::live_view::{
    apply_cases, assemble_live_view, candidate_plan, corroborate_rows, fanout_targets,
    keyless_candidates_sql, live_view_body, live_view_sql, quality_order, shape_case,
    CaseProvenance, CaseView, Corroborated, LiveViewRow, LIVE_VIEW_CANDIDATE_ATTEMPTS_PER_POT,
    LIVE_VIEW_MAX_ROWS, LIVE_VIEW_UNKNOWN_POT_QUOTA, LIVE_VIEW_VERIFY_BUDGET,
};
use low_app_layer::logic::valid_identity;
use low_app_layer::results::{
    potparty_v2_challenge, seat_markers_sql, seatsig_preimage, SeatMarkerRow,
    SEAT_MARKERS_BINDS_PER_POT,
};
use rusqlite::{params, Connection};

/// A fresh in-memory SQLite carrying the REAL production schema (same
/// tolerance discipline as `tests/refund_view_sqlite.rs`: only the re-run
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
/// a #284-decoded COVENANT row; None ⇒ a legacy/bare row (decoded columns
/// NULL). `pub_a` overrides the committed seat-A settle key so a test can
/// commit a REAL settle pubkey (the membership pre-filter's input).
fn admit_pot_keys(
    conn: &Connection,
    txid: &str,
    created_at: i64,
    cov_height: Option<i64>,
    pub_a: Option<String>,
) {
    let covenant = cov_height.is_some();
    conn.execute(
        store_record_sql(),
        params![
            txid,
            0i64,                                          // outputIndex
            0i64,                                          // spent
            Option::<String>::None,                        // spendingTxid
            0i64,                                          // spentConfirmed
            created_at,                                    // createdAt
            covenant.then_some("covenant"),                // lockKind
            covenant.then(|| pub_a.unwrap_or(h66(0x0a))),  // pubA
            covenant.then(|| h66(0x0b)),                   // pubB
            covenant.then(|| h66(0x0c)),                   // pubTower
            covenant.then(|| "aa".repeat(20)),             // payPkhA
            covenant.then(|| "bb".repeat(20)),             // payPkhB
            covenant.then(|| "cc".repeat(20)),             // rakePkh
            covenant.then_some(500i64),                    // stakeA
            covenant.then_some(500i64),                    // stakeB
            covenant.then_some(8i64),                      // feeSats
            cov_height,                                    // recoveryHeight (committed)
            covenant.then_some(1000i64),                   // potSats
            i64::from(covenant),                           // paramsDecoded
        ],
    )
    .expect("store_record_sql");
}

fn admit_pot(conn: &Connection, txid: &str, created_at: i64, cov_height: Option<i64>) {
    admit_pot_keys(conn, txid, created_at, cov_height, None);
}

/// Record a pot spend via the REAL `mark_spent_sql()` — exact bind order.
fn mark_spent(
    conn: &Connection,
    pot_txid: &str,
    spender: &str,
    confirmed: bool,
    verdict: Option<&str>,
    spent_height: Option<i64>,
) {
    let sql = mark_spent_sql(confirmed, verdict.is_some());
    match (confirmed, verdict) {
        (true, Some(v)) => conn.execute(
            sql,
            params![spender, v, spender, spender, spent_height, spent_height, pot_txid, 0i64],
        ),
        (true, None) => conn.execute(
            sql,
            params![spender, spender, spent_height, spent_height, pot_txid, 0i64],
        ),
        (false, Some(v)) => conn.execute(sql, params![spender, v, spender, pot_txid, 0i64]),
        (false, None) => conn.execute(sql, params![spender, pot_txid, 0i64]),
    }
    .expect("mark_spent_sql");
}

/// File a v1 potparty marker — the topic manager's `INSERT OR IGNORE` shape
/// (field values copied from `tests/refund_view_sqlite.rs`). v1 carries NO
/// seat-binding fields, so these rows can never corroborate — exactly the
/// dust an attacker mints for free.
fn file_party(
    conn: &Connection,
    identity: &str,
    pot_txid: &str,
    recovery_height: i64,
    marker_txid: &str,
    at: i64,
) {
    file_party_game(conn, identity, &h64(0x11), pot_txid, recovery_height, marker_txid, at);
}

/// v1 marker with an explicit (possibly attacker-chosen) gameId.
fn file_party_game(
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
          sigHex, seatSettlePubkey, seatSigHex, txid, outputIndex, createdAt) \
         VALUES (?1, ?2, ?3, ?4, 0, ?5, '3045ab', NULL, NULL, ?6, 0, ?7)",
        params![identity, h66(0xbb), game_id, pot_txid, recovery_height, marker_txid, at],
    )
    .expect("insert potparty_records");
}

// ── real-crypto v2 markers (the corroboration producer path) ────────────────

/// Deterministic identity wallet (same pinned-root-key crypto the
/// `results.rs` seat-attribution tests use).
fn wallet_of(seed: u8) -> ProtoWallet {
    let key = bsv_rs::primitives::ec::PrivateKey::from_hex(&format!("{seed:064x}")).unwrap();
    ProtoWallet::new(Some(key))
}

fn identity_of(w: &ProtoWallet) -> String {
    w.identity_key_hex().to_ascii_lowercase()
}

/// File a FULLY REAL v2 seat-binding marker through the topic manager's
/// INSERT shape: genuine settle-key ECDSA over the exact cross-repo seat
/// preimage + genuine BRC-42 'anyone' identity signature over the exact v2
/// challenge (`[1,'low potparty']`, keyID = gameId). Returns the settle
/// pubkey so a caller can COMMIT it as the pot's `pubA` (the membership
/// pre-filter's input).
#[allow(clippy::too_many_arguments)]
fn file_party_v2_real(
    conn: &Connection,
    identity_wallet: &ProtoWallet,
    game_id: &str,
    pot_txid: &str,
    recovery_height: u32,
    marker_txid: &str,
    at: i64,
    settle_seed: u8,
) -> String {
    let identity = identity_of(identity_wallet);
    let settle_key = bsv_rs::primitives::ec::PrivateKey::from_bytes(&{
        let mut b = [0u8; 32];
        b[31] = settle_seed;
        b
    })
    .unwrap();
    let settle_pub = settle_key.public_key().to_hex().to_ascii_lowercase();
    let preimage = seatsig_preimage(game_id, pot_txid, 0, &identity).expect("preimage");
    let seat_sig = settle_key.sign(&bsv_rs::primitives::hash::sha256(&preimage)).unwrap();
    let seat_sig_hex = hex::encode(seat_sig.to_der());
    let m = SeatMarkerRow {
        identity: identity.clone(),
        opponent_identity: h66(0xbb),
        game_id: game_id.to_string(),
        pot_txid: pot_txid.to_string(),
        pot_vout: 0,
        recovery_height,
        seat_settle_pubkey: settle_pub.clone(),
        seat_sig_hex: seat_sig_hex.clone(),
        identity_sig_hex: String::new(),
    };
    let challenge = potparty_v2_challenge(&m).expect("challenge");
    let id_sig = identity_wallet
        .create_signature(CreateSignatureArgs {
            data: Some(challenge),
            hash_to_directly_sign: None,
            protocol_id: bsv_rs::wallet::Protocol::new(
                bsv_rs::wallet::SecurityLevel::App,
                "low potparty",
            ),
            key_id: game_id.to_string(),
            counterparty: Some(Counterparty::Anyone),
        })
        .unwrap();
    conn.execute(
        "INSERT OR IGNORE INTO potparty_records \
         (identity, opponentIdentity, gameId, potTxid, potVout, recoveryHeight, \
          sigHex, seatSettlePubkey, seatSigHex, txid, outputIndex, createdAt) \
         VALUES (?1, ?2, ?3, ?4, 0, ?5, ?6, ?7, ?8, ?9, 0, ?10)",
        params![
            identity,
            h66(0xbb),
            game_id,
            pot_txid,
            recovery_height as i64,
            hex::encode(id_sig.signature),
            settle_pub,
            seat_sig_hex,
            marker_txid,
            at
        ],
    )
    .expect("insert v2 potparty_records");
    settle_pub
}

/// Execute the SHIPPED `live_view_sql()` and map rows exactly as the route
/// does (same columns, same Option-ness).
fn query_rows(conn: &Connection, identity: &str) -> Vec<LiveViewRow> {
    let sql = live_view_sql();
    let mut stmt = conn.prepare(&sql).expect("prepare live_view_sql");
    stmt.query_map(params![identity], |r| {
        Ok(LiveViewRow {
            identity: r.get::<_, Option<String>>("identity")?.unwrap_or_default(),
            game_id: r.get("gameId")?,
            pot_txid: r.get("potTxid")?,
            pot_vout: r.get::<_, i64>("potVout")? as u32,
            opponent_identity: r.get("opponentIdentity")?,
            marker_recovery_height: r.get::<_, i64>("recoveryHeight")? as u32,
            cov_recovery_height: r.get::<_, Option<i64>>("covRecoveryHeight")?.map(|v| v as u64),
            identity_sig_hex: r.get("sigHex")?,
            seat_settle_pubkey: r.get("seatSettlePubkey")?,
            seat_sig_hex: r.get("seatSigHex")?,
            cov_pub_a: r.get("covPubA")?,
            cov_pub_b: r.get("covPubB")?,
            spent: r.get::<_, Option<i64>>("spent")?.map(|v| v != 0),
            spending_txid: r.get("spendingTxid")?,
            spent_confirmed: r.get::<_, Option<i64>>("spentConfirmed")?.map(|v| v != 0),
        })
    })
    .expect("query")
    .collect::<Result<Vec<_>, _>>()
    .expect("rows")
}

type CandidateMap = std::collections::HashMap<(String, u32), Vec<SeatMarkerRow>>;

/// The CANDIDATE fetch, executing the SHIPPED queries against real SQLite —
/// the exact two-query delivery `routes::live_view_candidates` performs
/// (`candidate_plan` → `seat_markers_sql` for keyed pots +
/// `keyless_candidates_sql` for the rest, same bind order).
fn fetch_candidates(conn: &Connection, identity: &str, rows: &[LiveViewRow]) -> CandidateMap {
    let plan = candidate_plan(rows);
    let mut out: CandidateMap = std::collections::HashMap::new();
    let mut read = |sql: &str, binds: Vec<rusqlite::types::Value>| {
        let mut stmt = conn.prepare(sql).expect("prepare candidate sql");
        let fetched: Vec<SeatMarkerRow> = stmt
            .query_map(rusqlite::params_from_iter(binds), |r| {
                Ok(SeatMarkerRow {
                    identity: r.get::<_, String>("identity")?.to_ascii_lowercase(),
                    opponent_identity: r
                        .get::<_, String>("opponentIdentity")?
                        .to_ascii_lowercase(),
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
            .expect("candidate query")
            .collect::<Result<Vec<_>, _>>()
            .expect("candidate rows");
        for m in fetched {
            out.entry((m.pot_txid.clone(), m.pot_vout)).or_default().push(m);
        }
    };
    for chunk in &plan.keyed {
        let mut binds: Vec<rusqlite::types::Value> = Vec::new();
        for b in chunk {
            binds.push(b.pot_txid.clone().into());
            binds.push((b.pot_vout as i64).into());
            binds.push(b.pub_a_hex.clone().into());
            binds.push(b.pub_b_hex.clone().into());
        }
        assert!(binds.len() == chunk.len() * SEAT_MARKERS_BINDS_PER_POT);
        read(&seat_markers_sql(chunk.len()), binds);
    }
    for chunk in &plan.keyless {
        let mut binds: Vec<rusqlite::types::Value> = vec![identity.to_string().into()];
        for (txid, vout) in chunk {
            binds.push(txid.clone().into());
            binds.push((*vout as i64).into());
        }
        read(&keyless_candidates_sql(chunk.len()), binds);
    }
    out
}

/// The full read path the route runs: shipped window SQL → shipped candidate
/// queries → corroboration.
fn corroborate(conn: &Connection, identity: &str) -> (Vec<LiveViewRow>, Corroborated) {
    let rows = query_rows(conn, identity);
    let candidates = fetch_candidates(conn, identity, &rows);
    let corr = corroborate_rows(identity, &rows, &candidates);
    (rows, corr)
}

/// Assemble entries the way the route does, corroboration included (the real
/// producer path — every signature round-trips through SQLite).
fn assemble(
    conn: &Connection,
    identity: &str,
    tip: Option<u64>,
) -> Vec<low_app_layer::live_view::LiveEntry> {
    let (rows, corr) = corroborate(conn, identity);
    assemble_live_view(rows, &corr, tip)
}

const GATE: i64 = 900_123;

/// One caller + one covenant pot, unspent — the base fixture (v1 marker:
/// the pre-corroboration legacy shape).
fn seed_live_pot(conn: &Connection) -> (String, String) {
    let me = h66(0xa1);
    let pot = h64(0xaa);
    admit_pot(conn, &pot, 1_000, Some(GATE));
    file_party(conn, &me, &pot, GATE, "txPARTY", 1_001);
    (me, pot)
}

// ── the liveness filter ─────────────────────────────────────────────────────

#[test]
fn unspent_pot_is_live_with_gate_math_and_null_case() {
    let conn = production_schema_db();
    let (me, pot) = seed_live_pot(&conn);

    let entries = assemble(&conn, &me, Some(900_078));
    assert_eq!(entries.len(), 1);
    let e = &entries[0];
    assert_eq!(e.pot_txid, pot);
    assert_eq!(e.spent, Some(false));
    assert_eq!(e.spent_confirmed, Some(false));
    assert_eq!(e.spending_txid, None);
    assert_eq!(e.opponent_identity, Some(h66(0xbb)));
    // A v1 marker cannot corroborate: the opponent claim is served but
    // labeled the forgeable claim it is, and the row is never fan-out
    // eligible.
    assert!(!e.marker_verified);
    assert_eq!(e.opponent_identity_source, Some("marker-unverified"));
    assert_eq!(e.recovery_height, Some(GATE as u64));
    assert_eq!(e.blocks_to_gate, Some(45));
    assert!(!e.gate_passed);
    // The D1 half NEVER asserts a case: unknown until the tower fan-out.
    assert_eq!(e.case, None);
    assert_eq!(e.case_source, CaseProvenance::MarkerUnverified);
}

#[test]
fn unconfirmed_spend_stays_live_with_the_pointer_served() {
    let conn = production_schema_db();
    let (me, pot) = seed_live_pot(&conn);
    let spender = h64(0xfe);
    mark_spent(&conn, &pot, &spender, false, None, None);

    let entries = assemble(&conn, &me, Some(900_200));
    assert_eq!(entries.len(), 1, "a displaceable spend is still a live pot");
    let e = &entries[0];
    assert_eq!(e.spent, Some(true));
    assert_eq!(e.spent_confirmed, Some(false));
    assert_eq!(e.spending_txid.as_deref(), Some(spender.as_str()));
}

#[test]
fn confirmed_spend_is_excluded_even_without_a_verdict() {
    let conn = production_schema_db();
    let (me, pot) = seed_live_pot(&conn);
    // Exclusion keys on spent+spentConfirmed ONLY — a confirmed spend with
    // no decoded verdict is just as settled (the verdict story is
    // /refund-view's and /results').
    mark_spent(&conn, &pot, &h64(0xfe), true, None, Some(900_150));
    assert!(query_rows(&conn, &me).is_empty(), "confirmed spend is not live");

    // And with a verdict, equally excluded.
    let pot2 = h64(0xab);
    admit_pot(&conn, &pot2, 1_100, Some(GATE));
    file_party(&conn, &me, &pot2, GATE, "txPARTY2", 1_101);
    mark_spent(&conn, &pot2, &h64(0xfd), true, Some("winner-a"), Some(900_160));
    assert!(query_rows(&conn, &me).is_empty());
}

#[test]
fn mixed_set_keeps_only_the_live_pots() {
    let conn = production_schema_db();
    let me = h66(0xa1);
    // (a) unspent — live.
    let unspent = h64(0xd0);
    admit_pot(&conn, &unspent, 1_000, Some(GATE));
    file_party(&conn, &me, &unspent, GATE, "txA", 1_001);
    // (b) unconfirmed spend — live.
    let unconf = h64(0xd1);
    admit_pot(&conn, &unconf, 1_100, Some(GATE));
    file_party(&conn, &me, &unconf, GATE, "txB", 1_101);
    mark_spent(&conn, &unconf, &h64(0xf1), false, None, None);
    // (c) confirmed spend — EXCLUDED.
    let settled = h64(0xd2);
    admit_pot(&conn, &settled, 1_200, Some(GATE));
    file_party(&conn, &me, &settled, GATE, "txC", 1_201);
    mark_spent(&conn, &settled, &h64(0xf2), true, Some("winner-b"), Some(900_150));
    // (d) join miss (never indexed) — INCLUDED as unknown/possibly-live.
    let ghost = h64(0xd3);
    file_party(&conn, &me, &ghost, GATE, "txD", 1_301);

    let rows = query_rows(&conn, &me);
    let pots: Vec<&str> = rows.iter().map(|r| r.pot_txid.as_str()).collect();
    assert_eq!(rows.len(), 3);
    assert!(pots.contains(&unspent.as_str()));
    assert!(pots.contains(&unconf.as_str()));
    assert!(pots.contains(&ghost.as_str()));
    assert!(!pots.contains(&settled.as_str()), "settled pot must not appear");
    // The ghost arrives with the fail-safe unknown shape — included but
    // never asserted unspent.
    let g = rows.iter().find(|r| r.pot_txid == ghost).unwrap();
    assert_eq!(g.spent, None);
    assert_eq!(g.spent_confirmed, None);
    assert_eq!(g.cov_recovery_height, None);
}

// ── recovery-height sourcing (shared /refund-view semantics) ────────────────

#[test]
fn covenant_committed_height_beats_the_marker_hint() {
    let conn = production_schema_db();
    let me = h66(0xa1);
    let pot = h64(0xaa);
    admit_pot(&conn, &pot, 1_000, Some(GATE));
    // The caller's marker (byte-format-admitted) claims a WRONG height.
    file_party(&conn, &me, &pot, 111, "txPARTY", 1_001);

    let e = &assemble(&conn, &me, Some(900_078))[0];
    assert_eq!(e.recovery_height, Some(GATE as u64), "chain truth wins");
    assert_eq!(e.blocks_to_gate, Some(45));

    // Bare/legacy pot: the marker hint is all there is.
    let bare = h64(0xac);
    admit_pot(&conn, &bare, 2_000, None);
    file_party(&conn, &me, &bare, GATE + 7, "txBARE", 2_001);
    let entries = assemble(&conn, &me, Some(900_078));
    let b = entries.iter().find(|e| e.pot_txid == bare).unwrap();
    assert_eq!(b.recovery_height, Some((GATE + 7) as u64));

    // No tip: gate fields degrade to null/false, never a guess.
    let entries = assemble(&conn, &me, None);
    for e in &entries {
        assert_eq!(e.blocks_to_gate, None);
        assert!(!e.gate_passed);
    }
}

// ── fail-safe empty + window bounds ─────────────────────────────────────────

#[test]
fn unknown_identity_is_a_well_formed_empty_answer() {
    let conn = production_schema_db();
    seed_live_pot(&conn);

    let stranger = h66(0xee);
    let rows = query_rows(&conn, &stranger);
    assert!(rows.is_empty());
    let v: serde_json::Value =
        serde_json::from_str(&live_view_body(&stranger, None, &assemble(&conn, &stranger, None)))
            .unwrap();
    assert_eq!(v["live"], serde_json::json!([]));
    assert!(v["tip"].is_null());

    // The route's invalid-identity guard (fail-safe-empty 200, never an
    // error) keys on the same `valid_identity` every identity surface uses.
    assert!(!valid_identity(""));
    assert!(!valid_identity("zz"));
    assert!(!valid_identity(&h64(0xaa))); // 64 hex — a txid, not an identity
    assert!(valid_identity(&h66(0xee)));
}

#[test]
fn dust_replays_collapse_to_one_row_per_pot_oldest_representative() {
    let conn = production_schema_db();
    let (me, pot) = seed_live_pot(&conn);
    // Every replay files a DIFFERENT height so the assertion can tell WHICH
    // row won the partition (the refund-view delta-round-2 lesson: identical
    // heights let an ASC→DESC ordering drift pass unseen).
    for i in 0..40 {
        file_party(&conn, &me, &pot, GATE + 1_000 + i, &format!("txREPLAY{i:03}"), 2_000 + i);
    }
    let rows = query_rows(&conn, &me);
    assert_eq!(rows.len(), 1, "one pot ⇒ one row, whatever the replay count");
    // The representative is the OLDEST marker (the honest funding-time one) —
    // the only order an attacker cannot win by simply publishing later.
    assert_eq!(rows[0].marker_recovery_height, GATE as u32);
}

/// The #281 window rules exercised BEHAVIORALLY against this route's OWN SQL
/// (`live_view_sql` can drift independently of its siblings): 60 dust
/// replays of the victim's real live pot + 120 markers naming INVENTED pots,
/// every attacker row NEWER than the honest marker. All rows here are LIVE
/// (nothing spent), so the window math is identical to the refund view's:
/// the real pot survives at exactly quota depth; ghost promotion is bounded
/// to the newest [`LIVE_VIEW_UNKNOWN_POT_QUOTA`]; the demoted tier fills the
/// rest newest-first.
#[test]
fn unknown_pot_quota_bounds_ghost_promotion_and_the_real_pot_survives() {
    let conn = production_schema_db();
    let (me, honest_pot) = seed_live_pot(&conn); // indexed, createdAt 1_000
    const REPLAYS: i64 = 60;
    const GHOSTS: u64 = 120;
    for i in 0..REPLAYS {
        file_party(&conn, &me, &honest_pot, GATE, &format!("txREPLAY{i:03}"), 2_000 + i);
    }
    let ghost_txid = |i: u64| format!("{:064x}", 0xdead_0000_u64 + i);
    for i in 0..GHOSTS {
        file_party(&conn, &me, &ghost_txid(i), GATE, &format!("txGHOST{i:03}"), 3_000 + i as i64);
    }

    let rows = query_rows(&conn, &me);
    assert_eq!(rows.len(), LIVE_VIEW_MAX_ROWS, "page full");
    let pots: Vec<&String> = rows.iter().map(|r| &r.pot_txid).collect();
    assert_eq!(
        pots.iter().filter(|t| ***t == honest_pot).count(),
        1,
        "the real pot survives exactly once (replays collapsed)"
    );
    let pos = pots.iter().position(|t| **t == honest_pot).unwrap();
    assert_eq!(pos, LIVE_VIEW_UNKNOWN_POT_QUOTA, "quota-many promoted ghosts, no more");
    // The promoted slice is the NEWEST ghosts, newest first.
    let promoted: Vec<String> = (0..LIVE_VIEW_UNKNOWN_POT_QUOTA as u64)
        .map(|k| ghost_txid(GHOSTS - 1 - k))
        .collect();
    assert_eq!(pots[..pos].to_vec(), promoted.iter().collect::<Vec<_>>());
    // Ghost rows arrive with the fail-safe unknown shape (spent null).
    assert!(rows[0].spent.is_none() && rows[0].pot_txid != honest_pot);
}

#[test]
fn row_cap_bounds_the_page_and_confirmed_pots_free_slots() {
    let conn = production_schema_db();
    let me = h66(0xa1);
    let n = LIVE_VIEW_MAX_ROWS + 10;
    for i in 0..n {
        let pot = format!("{:064x}", 0x1000_u64 + i as u64);
        admit_pot(&conn, &pot, 1_000 + i as i64, Some(GATE));
        file_party(&conn, &me, &pot, GATE, &format!("txM{i:03}"), 1_000 + i as i64);
    }
    let rows = query_rows(&conn, &me);
    assert_eq!(rows.len(), LIVE_VIEW_MAX_ROWS, "hard cap");
    let unique: std::collections::HashSet<&String> = rows.iter().map(|r| &r.pot_txid).collect();
    assert_eq!(unique.len(), LIVE_VIEW_MAX_ROWS, "one row per pot");
    // Newest pots first — the 10 oldest fell off, not the newest.
    assert_eq!(rows[0].pot_txid, format!("{:064x}", 0x1000_u64 + (n as u64 - 1)));

    // The filter runs BEFORE the window: confirming spends on the newest 10
    // pots frees their slots and the previously-evicted oldest pots return.
    for i in (n - 10)..n {
        let pot = format!("{:064x}", 0x1000_u64 + i as u64);
        mark_spent(&conn, &pot, &h64(0xfe), true, Some("winner-a"), Some(900_150));
    }
    let rows = query_rows(&conn, &me);
    assert_eq!(rows.len(), LIVE_VIEW_MAX_ROWS, "page refills from live pots only");
    assert_eq!(
        rows[0].pot_txid,
        format!("{:064x}", 0x1000_u64 + (n as u64 - 11)),
        "newest LIVE pot leads; settled pots consume no window slot"
    );
}

// ── the adversarial-gate cells (2026-08 review) ─────────────────────────────

/// **HIGH-A, the shape production ACTUALLY produces** — the cell whose
/// absence hid the defect: bsv-low publishes the v1 marker and THEN the v2
/// seat-binding one for the same pot (`overlay.ts`: "ALONGSIDE v1, never
/// instead of it"; `potPartyRepublish.ts` awaits the v1 half first), so the
/// window's OLDEST-representative rule always yields the V1 row.
/// Representative-only corroboration was therefore dead on arrival: no case
/// was ever fetched and every honest row read `marker-unverified`. Here the
/// FULL shipped read path (window SQL → candidate queries → corroboration)
/// must corroborate the pot from its OWN v2 marker and fan out on it.
#[test]
fn production_v1_then_v2_for_one_pot_still_corroborates() {
    let conn = production_schema_db();
    let w = wallet_of(0x42);
    let me = identity_of(&w);
    let game = h64(0x21);
    let pot = h64(0xaa);

    // v1 lands FIRST (older createdAt) — it is what the window collapses to.
    file_party_game(&conn, &me, &game, &pot, GATE, "txV1", 1_001);
    // v2 lands SECOND, fully genuine; commit its settle key in the pot lock
    // so the membership pre-filter is exercised on the real path.
    let settle_pub =
        file_party_v2_real(&conn, &w, &game, &pot, GATE as u32, "txV2", 1_002, 0x51);
    admit_pot_keys(&conn, &pot, 1_000, Some(GATE), Some(settle_pub.clone()));

    let rows = query_rows(&conn, &me);
    assert_eq!(rows.len(), 1, "the two markers collapse to ONE representative row");
    assert_eq!(
        rows[0].seat_settle_pubkey, None,
        "the representative IS the v1 row (this is what made the old gate inoperative)"
    );
    assert_eq!(rows[0].cov_pub_a.as_deref(), Some(settle_pub.as_str()));

    // The candidate query finds the pot's genuine v2 marker…
    let candidates = fetch_candidates(&conn, &me, &rows);
    assert_eq!(candidates.get(&(pot.clone(), 0)).map(Vec::len), Some(1));
    // …and corroboration succeeds, supplying the gameId as a join key.
    let corr = corroborate_rows(&me, &rows, &candidates);
    let claim = corr.claims[0].as_ref().expect("the pot corroborates in the PRODUCTION shape");
    assert_eq!(claim.game_id, game);
    assert_eq!(corr.attempts, 1, "one honest candidate ⇒ one attempt");
    assert_eq!(fanout_targets(&rows, &corr.claims), vec![game.clone()]);

    // The served entry says so, and the case half is alive.
    let e = &assemble_live_view(rows, &corr, Some(900_000))[0];
    assert!(e.marker_verified);
    assert_eq!(e.marker_source, "seat-signed");
    assert_eq!(e.opponent_identity_source, Some("seat-signed"));
    assert_eq!(e.case_source, CaseProvenance::NotFetched, "eligible, pre-fan-out");
}

/// The same-second twin of the cell above: `createdAt` is server-side with
/// SECOND granularity, so v1 and v2 routinely share a timestamp and the
/// `pp.rowid ASC` tiebreak decides — and it also picks v1 (inserted first).
/// Corroboration must not depend on which one won.
#[test]
fn production_v1_and_v2_at_the_same_second_still_corroborates() {
    let conn = production_schema_db();
    let w = wallet_of(0x42);
    let me = identity_of(&w);
    let game = h64(0x22);
    let pot = h64(0xab);
    file_party_game(&conn, &me, &game, &pot, GATE, "txV1same", 1_001);
    let pk = file_party_v2_real(&conn, &w, &game, &pot, GATE as u32, "txV2same", 1_001, 0x51);
    admit_pot_keys(&conn, &pot, 1_000, Some(GATE), Some(pk));

    let (rows, corr) = corroborate(&conn, &me);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].seat_settle_pubkey, None,
        "the rowid tiebreak also picks the v1 row"
    );
    assert_eq!(
        corr.claims[0].as_ref().map(|c| c.game_id.clone()),
        Some(game),
        "and the pot still corroborates from its candidate"
    );
}

/// MEDIUM-2, through the REAL producer path: the victim has ONE real, KNOWN
/// (indexed) live pot with a genuine v2 marker; the attacker files
/// quota-many byte-format-only dust markers (victim identity, invented pots,
/// DISTINCT invented gameIds), all NEWER, which own the window head as
/// promoted ghosts. The SHIPPED selection must still fetch the victim's real
/// gameId — dust can no longer starve the case slots (it did when selection
/// was positional: cap 8 < quota 10) NOR the verify budget (quality order).
#[test]
fn dust_window_cannot_starve_the_corroborated_pots_case_fetch() {
    let conn = production_schema_db();
    let w = wallet_of(0x42);
    let me = identity_of(&w);
    let real_pot = h64(0xaa);
    let real_game = h64(0x21);
    // The victim's pot in the production shape: v1 then v2.
    file_party_game(&conn, &me, &real_game, &real_pot, GATE, "txREALv1", 1_001);
    let settle_pub =
        file_party_v2_real(&conn, &w, &real_game, &real_pot, GATE as u32, "txREALv2", 1_002, 0x51);
    admit_pot_keys(&conn, &real_pot, 1_000, Some(GATE), Some(settle_pub));

    // Exactly QUOTA fresh ghost markers, distinct invented gameIds, all
    // newer than the honest marker. Cost to the attacker: 10 OP_RETURNs.
    let ghost_pot = |i: u64| format!("{:064x}", 0xdead_0000_u64 + i);
    let ghost_game = |i: u64| format!("{:064x}", 0xbeef_0000_u64 + i);
    for i in 0..LIVE_VIEW_UNKNOWN_POT_QUOTA as u64 {
        file_party_game(
            &conn,
            &me,
            &ghost_game(i),
            &ghost_pot(i),
            GATE,
            &format!("txGHOST{i:03}"),
            2_000 + i as i64,
        );
    }

    let (rows, corr) = corroborate(&conn, &me);
    // The window shape that used to starve: 10 promoted ghosts AHEAD of the
    // real pot.
    let pos = rows.iter().position(|r| r.pot_txid == real_pot).expect("real pot present");
    assert_eq!(pos, LIVE_VIEW_UNKNOWN_POT_QUOTA, "ghosts occupy the whole window head");
    assert!(corr.claims[pos].is_some(), "the real pot corroborates through the shipped path");
    assert_eq!(
        corr.claims.iter().filter(|c| c.is_some()).count(),
        1,
        "dust never corroborates"
    );

    let targets = fanout_targets(&rows, &corr.claims);
    assert!(
        targets.contains(&real_game),
        "the victim's real gameId IS fetched despite quota-many newer dust markers"
    );
    assert_eq!(
        targets,
        vec![real_game],
        "and ONLY corroborated gameIds are ever used as join keys"
    );
}

/// HIGH-1a, through the REAL producer path: two DISTINCT live pots carrying
/// the SAME gameId (the funding-retry / double-fund shape, 2026-07-12), both
/// with genuine v2 markers. The tower's public case view is per-GAME (its
/// primary case), so both rows receive the SAME answer — the body must let a
/// client tell WHAT was actually vouched: the served `caseGameId` is the join
/// key fetched, and the source tag is the NON-VOUCHING
/// `"tower-by-gameid-unverified"` (never the retired `"tower"`).
#[test]
fn two_pots_one_gameid_serve_the_join_key_and_never_vouch_the_binding() {
    let conn = production_schema_db();
    let w = wallet_of(0x42);
    let me = identity_of(&w);
    let game = h64(0x21);
    let pot1 = h64(0xaa);
    let pot2 = h64(0xab); // the funding retry — same game, second pot
    let pk1 = file_party_v2_real(&conn, &w, &game, &pot1, GATE as u32, "txP1", 1_001, 0x51);
    let pk2 = file_party_v2_real(&conn, &w, &game, &pot2, GATE as u32, "txP2", 1_101, 0x52);
    admit_pot_keys(&conn, &pot1, 1_000, Some(GATE), Some(pk1));
    admit_pot_keys(&conn, &pot2, 1_100, Some(GATE), Some(pk2));

    let (rows, corr) = corroborate(&conn, &me);
    assert_eq!(rows.len(), 2, "two distinct live pots");
    assert!(corr.claims.iter().all(|c| c.is_some()));
    let targets = fanout_targets(&rows, &corr.claims);
    assert_eq!(targets, vec![game.clone()], "one shared fetch for the shared gameId");

    // The tower answers with the game's PRIMARY case — a terminal one here
    // (per `primary_case`, a terminal record wins over a pending one). At
    // most ONE of the two pots can be the one it belongs to.
    let terminal = shape_case(&serde_json::json!({
        "status": "finalized_concede",
        "epoch": 3,
        "deadline": 0.0f64,
        "accused": "B",
    }))
    .expect("terminal case shapes");
    let fetched: std::collections::HashMap<String, CaseView> = [(game.clone(), terminal)].into();
    let mut entries = assemble_live_view(rows, &corr, Some(900_000));
    apply_cases(&mut entries, &targets, &fetched);

    let body: serde_json::Value =
        serde_json::from_str(&live_view_body(&me, Some(900_000), &entries)).unwrap();
    let live = body["live"].as_array().unwrap();
    assert_eq!(live.len(), 2);
    for e in live {
        // Both rows carry the same per-game answer — but each SERVES THE
        // JOIN KEY and a tag that vouches only "the tower's answer for this
        // gameId": a client that knows its pot's real gameId can discard a
        // substituted key, and no client may read the pair as a verified
        // case↔pot binding.
        assert_eq!(e["case"]["status"], serde_json::json!("finalized_concede"));
        assert_eq!(e["caseGameId"], serde_json::json!(game.clone()), "the join key is served");
        assert_eq!(e["caseSource"], serde_json::json!("tower-by-gameid-unverified"));
        assert_ne!(e["caseSource"], serde_json::json!("tower"), "the vouching tag is retired");
        assert_eq!(e["markerSource"], serde_json::json!("seat-signed"));
    }
}

/// LOW-D, through the REAL producer path: a hostile HOST reuses an invite
/// code (the host mints the gameId — bsv-low `Table.tsx`), so the victim's
/// honest client signs a marker naming a gameId whose EARLIER game already
/// has a TERMINAL tower case. The row corroborates (it genuinely is the
/// victim's own signed claim) and `/case/:gameId` returns the earlier game's
/// terminal record — `caseGameId` equals the row's own gameId, so it cannot
/// expose the reuse. What must hold: the tag NEVER vouches for the binding,
/// so no consumer may read "this pot's hand is finalized" out of it.
#[test]
fn a_reused_gameid_returns_a_foreign_terminal_case_and_the_tag_never_vouches() {
    let conn = production_schema_db();
    let w = wallet_of(0x42);
    let me = identity_of(&w);
    let reused_game = h64(0x21); // minted by the hostile host, used before
    let pot = h64(0xaa);
    file_party_game(&conn, &me, &reused_game, &pot, GATE, "txV1", 1_001);
    let pk = file_party_v2_real(&conn, &w, &reused_game, &pot, GATE as u32, "txV2", 1_002, 0x51);
    admit_pot_keys(&conn, &pot, 1_000, Some(GATE), Some(pk));

    let (rows, corr) = corroborate(&conn, &me);
    assert!(corr.claims[0].is_some(), "the victim's own marker DOES corroborate");
    let targets = fanout_targets(&rows, &corr.claims);
    assert_eq!(targets, vec![reused_game.clone()]);

    // The tower's answer belongs to the PREVIOUS game — nothing in the shaped
    // subset can reveal that (no caseId, no outpoint).
    let foreign = shape_case(&serde_json::json!({
        "status": "finalized_refuse",
        "epoch": 9,
        "deadline": 0.0f64,
        "accused": "A",
    }))
    .expect("terminal case shapes");
    let mut entries = assemble_live_view(rows, &corr, Some(900_000));
    apply_cases(&mut entries, &targets, &[(reused_game.clone(), foreign)].into());
    let body: serde_json::Value =
        serde_json::from_str(&live_view_body(&me, Some(900_000), &entries)).unwrap();
    let e = &body["live"][0];
    assert_eq!(
        e["caseSource"],
        serde_json::json!("tower-by-gameid-unverified"),
        "the ONLY honesty carrier for a reused gameId — never a vouch"
    );
    assert_eq!(
        e["caseGameId"], e["gameId"],
        "caseGameId equals the row's own gameId, so it cannot expose the reuse (docs)"
    );
    // The pot is STILL served as live with its own facts — the foreign
    // terminal case never turns into a spend/settled assertion here.
    assert_eq!(e["spent"], serde_json::json!(false));
    assert_eq!(e["spentConfirmed"], serde_json::json!(false));
}

/// MEDIUM-B on the real path: an attacker crowds the victim's KNOWN pot's
/// candidate window with v2-SHAPED junk under a FOREIGN settle key. The
/// keyed candidate query filters those out IN SQL (the committed keys are
/// bound), so the honest marker is fetched and no curve time is spent on
/// junk at all.
#[test]
fn foreign_key_junk_never_reaches_verification_on_a_known_pot() {
    let conn = production_schema_db();
    let w = wallet_of(0x42);
    let me = identity_of(&w);
    let game = h64(0x21);
    let pot = h64(0xaa);
    file_party_game(&conn, &me, &game, &pot, GATE, "txV1", 1_001);
    let honest_pk =
        file_party_v2_real(&conn, &w, &game, &pot, GATE as u32, "txV2", 1_005, 0x51);
    admit_pot_keys(&conn, &pot, 1_000, Some(GATE), Some(honest_pk.clone()));
    // 20 junk v2-shaped rows under a key the lock never committed, all
    // stamped BEFORE the honest marker.
    for i in 0..20i64 {
        conn.execute(
            "INSERT OR IGNORE INTO potparty_records \
             (identity, opponentIdentity, gameId, potTxid, potVout, recoveryHeight, \
              sigHex, seatSettlePubkey, seatSigHex, txid, outputIndex, createdAt) \
             VALUES (?1, ?2, ?3, ?4, 0, ?5, '3045junk', ?6, '3045junk', ?7, 0, ?8)",
            params![
                me,
                h66(0xbb),
                h64(0x66),
                pot,
                GATE,
                h66(0x99), // foreign settle key
                format!("txJUNK{i:03}"),
                1_002 + i
            ],
        )
        .unwrap();
    }

    let rows = query_rows(&conn, &me);
    let candidates = fetch_candidates(&conn, &me, &rows);
    let fetched = candidates.get(&(pot.clone(), 0)).expect("candidates for the pot");
    assert!(
        fetched.iter().all(|m| m.seat_settle_pubkey == honest_pk),
        "the keyed query filtered every foreign-key row IN SQL"
    );
    let corr = corroborate_rows(&me, &rows, &candidates);
    assert_eq!(corr.claims[0].as_ref().map(|c| c.game_id.clone()), Some(game));
    assert_eq!(corr.attempts, 1, "zero curve time spent on 20 junk rows");
}

/// File a v2-SHAPED JUNK marker whose SEAT signature genuinely verifies (the
/// attacker's own key signing a preimage that embeds the VICTIM's identity —
/// the #230 F1 shape) under `settle_pub`, but whose IDENTITY signature does
/// not. It passes every cheap rejection, so it COSTS a verification attempt:
/// this is the crowding primitive.
#[allow(clippy::too_many_arguments)]
fn file_junk_v2(
    conn: &Connection,
    victim: &str,
    game_id: &str,
    pot_txid: &str,
    settle_key_seed: u8,
    settle_pub: &str,
    marker_txid: &str,
    at: i64,
) {
    let k = bsv_rs::primitives::ec::PrivateKey::from_bytes(&{
        let mut b = [0u8; 32];
        b[31] = settle_key_seed;
        b
    })
    .unwrap();
    let pre = seatsig_preimage(game_id, pot_txid, 0, victim).expect("preimage");
    let sig = hex::encode(k.sign(&bsv_rs::primitives::hash::sha256(&pre)).unwrap().to_der());
    conn.execute(
        "INSERT OR IGNORE INTO potparty_records \
         (identity, opponentIdentity, gameId, potTxid, potVout, recoveryHeight, \
          sigHex, seatSettlePubkey, seatSigHex, txid, outputIndex, createdAt) \
         VALUES (?1, ?2, ?3, ?4, 0, ?5, ?6, ?7, ?8, ?9, 0, ?10)",
        params![victim, h66(0xbb), game_id, pot_txid, GATE, sig, settle_pub, sig, marker_txid, at],
    )
    .expect("insert junk v2");
}

/// **R2-1, the cell whose absence hid the cross-pot starvation** — through the
/// shipped SQL with real crypto. The attacker funds BUDGET/PER_POT of their
/// OWN pots (so they are KNOWN and NEWER than the victim's older real pot),
/// names the victim as a party, and crowds each to the per-pot attempt cap
/// with junk that passes every cheap rejection. Under the previous
/// DEPTH-FIRST allotment that consumed the whole budget before the victim's
/// pot got its single attempt (no race against the victim's marker at all).
/// Under ROUND-ROBIN by depth, the victim's depth-0 honest marker is verified
/// in pass 0 and its case IS fetched.
#[test]
fn attacker_funded_known_pots_cannot_starve_the_victims_verify_attempt() {
    let conn = production_schema_db();
    let w = wallet_of(0x42);
    let me = identity_of(&w);
    let victim_pot = h64(0xaa);
    let victim_game = h64(0x21);
    // The victim's pot in the production shape (v1 then v2), OLDEST.
    file_party_game(&conn, &me, &victim_game, &victim_pot, GATE, "txV1", 1_001);
    let my_pub =
        file_party_v2_real(&conn, &w, &victim_game, &victim_pot, GATE as u32, "txV2", 1_002, 0x51);
    admit_pot_keys(&conn, &victim_pot, 1_000, Some(GATE), Some(my_pub));

    let n_pots = LIVE_VIEW_VERIFY_BUDGET / LIVE_VIEW_CANDIDATE_ATTEMPTS_PER_POT;
    for p in 0..n_pots {
        let pot = format!("{:064x}", 0xdead_0000_u64 + p as u64);
        let seed = 0xa0 + p as u8;
        let atk_pub = bsv_rs::primitives::ec::PrivateKey::from_bytes(&{
            let mut b = [0u8; 32];
            b[31] = seed;
            b
        })
        .unwrap()
        .public_key()
        .to_hex()
        .to_ascii_lowercase();
        // KNOWN (admitted) and NEWER than the victim's pot, committing the
        // attacker's own settle key so its junk passes the membership filter.
        admit_pot_keys(&conn, &pot, 5_000 + p as i64, Some(GATE), Some(atk_pub.clone()));
        file_party_game(
            &conn,
            &me,
            &format!("{:064x}", 0x9000_u64 + p as u64),
            &pot,
            GATE,
            &format!("txA{p}"),
            5_001 + p as i64,
        );
        for j in 0..LIVE_VIEW_CANDIDATE_ATTEMPTS_PER_POT {
            file_junk_v2(
                &conn,
                &me,
                &format!("{:064x}", 0xbad0_0000_u64 + (p * 10 + j) as u64),
                &pot,
                seed,
                &atk_pub,
                &format!("txJ{p}_{j}"),
                5_100 + (p * 10 + j) as i64,
            );
        }
    }

    let (rows, corr) = corroborate(&conn, &me);
    assert_eq!(rows.len(), 1 + n_pots, "every live pot is served");
    // The starvation SHAPE is present: the victim's pot sorts LAST among the
    // known class, behind every crowded attacker pot.
    let victim_idx = rows.iter().position(|r| r.pot_txid == victim_pot).unwrap();
    let rank = quality_order(&rows).iter().position(|i| *i == victim_idx).unwrap();
    assert_eq!(rank, n_pots, "victim is last in quality order");
    assert!(corr.attempts <= LIVE_VIEW_VERIFY_BUDGET, "the CPU ceiling still holds");
    // …and round-robin still serves it.
    assert_eq!(
        corr.claims[victim_idx].as_ref().map(|c| c.game_id.clone()),
        Some(victim_game.clone()),
        "the victim's depth-0 honest marker is verified in pass 0"
    );
    let targets = fanout_targets(&rows, &corr.claims);
    assert!(targets.contains(&victim_game), "and its case IS fetched");
    let entries = assemble_live_view(rows, &corr, Some(900_000));
    let victim = entries.iter().find(|e| e.pot_txid == victim_pot).unwrap();
    assert_eq!(victim.marker_source, "seat-signed");
    // The attacker's own pots stay honestly uncorroborated (their junk never
    // verifies) — never hidden, never vouched.
    assert_eq!(
        entries.iter().filter(|e| e.marker_source == "seat-signed").count(),
        1
    );
}
