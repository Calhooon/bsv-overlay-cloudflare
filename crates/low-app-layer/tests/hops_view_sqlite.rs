//! `/hops-view` proofs — REAL SQLite, PRODUCTION schema (bsv-low #315,
//! #252 stage 2b).
//!
//! These tests EXECUTE the exact shipped `hops_view_sql()` against real
//! SQLite carrying the overlay's PRODUCTION migration list verbatim, with
//! rows produced by the REAL producer chain (Rule 6b: never a hand-fed
//! shape):
//!
//!  - marker BYTES are built with the real digest builders + real BRC-42 /
//!    RFC6979 crypto, then driven through the topic manager's per-output
//!    admission gate (`HoppartyTopicManager::validate_hopparty_output` —
//!    the exact predicate `identify_admissible_outputs` applies), parsed
//!    by the same `parse_hopparty_marker` the lookup service uses, and
//!    written with the SHIPPED `hopparty_store_sql()` string (the
//!    `D1HoppartyStorage::store_record` write, exact bind order);
//!  - hop outpoints are admitted via the shipped `store_record_sql()`
//!    (`tm_lowfund` → `pot_records`) and spends via `mark_spent_sql()`;
//!  - the ADMITTED hop lock rides the engine's `outputs` insert shape.
//!
//! Cells: the verified positive control, the junk-sig / F1 / mismatched-
//! lock refusal labels (served, never dropped, never verified), the
//! unknown-first-class statuses, the superset-vs-forged-eviction window in
//! BOTH orderings, the honest truncation bit, and the route's fail-safe /
//! 503 cores.

use bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS;
use bsv_overlay_cloudflare::d1_discovery::{hopparty_store_sql, mark_spent_sql, store_record_sql};
use bsv_rs::wallet::{
    Counterparty, CreateSignatureArgs, GetPublicKeyArgs, Protocol, ProtoWallet, SecurityLevel,
};
use low_app_layer::hops_view::{
    assemble_hops_view, expected_hop_lock_hex, hops_view_body, hops_view_sql, HopStatus,
    HopsViewRow, MarkerVerification, HOPS_VIEW_MAX_OUTPOINTS,
};
use low_app_layer::logic::valid_identity;
use overlay_discovery::hopparty::topic_manager::HoppartyTopicManager;
use overlay_discovery::hopparty::{
    hopparty_identity_challenge, hopparty_seatsig_preimage, parse_hopparty_marker, HOPPARTY_TAG,
};
use rusqlite::{params, Connection};

/// A fresh in-memory SQLite carrying the REAL production schema (same
/// tolerance discipline as the sibling `_sqlite` suites).
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

// ── the CLIENT side: real marker bytes ──────────────────────────────────────

/// Minimal pushdata (the client builder's encoder).
fn push_data(blob: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let len = blob.len();
    if len < 0x4c {
        out.push(len as u8);
    } else if len <= 0xff {
        out.push(0x4c);
        out.push(len as u8);
    } else {
        out.push(0x4d);
        out.push((len & 0xff) as u8);
        out.push(((len >> 8) & 0xff) as u8);
    }
    out.extend_from_slice(blob);
    out
}

struct BuiltMarker {
    script: Vec<u8>,
    identity_hex: String,
    settle_pub_hex: String,
}

fn wallet_of(seed: u8) -> ProtoWallet {
    ProtoWallet::new(Some(
        bsv_rs::primitives::ec::PrivateKey::from_hex(&format!("{seed:064x}")).unwrap(),
    ))
}

/// Build a hopparty marker SCRIPT the way the client will: real BRC-42
/// settle derivation + real RFC6979 signatures over the shared digest
/// builders. `real_sigs: false` produces a byte-shape-valid marker whose
/// signatures are deterministic garbage (what an attacker can file for
/// dust — admission cannot tell the difference BY DESIGN).
fn build_marker(
    identity_seed: u8,
    game_id: [u8; 32],
    hop_txid: [u8; 32],
    hop_vout: u32,
    hop_sats: u64,
    real_sigs: bool,
) -> BuiltMarker {
    let wallet = wallet_of(identity_seed);
    let identity_hex = wallet.identity_key_hex().to_ascii_lowercase();
    let identity = hex::decode(&identity_hex).unwrap();
    let opponent_key =
        bsv_rs::primitives::ec::PrivateKey::from_hex(&format!("{:064x}", 0xb0u8)).unwrap();
    let opponent_pub = opponent_key.public_key();
    let opponent = hex::decode(opponent_pub.to_hex()).unwrap();

    let settle_protocol = Protocol::new(SecurityLevel::Counterparty, "low settle");
    let settle_pub_hex = wallet
        .get_public_key(GetPublicKeyArgs {
            identity_key: false,
            protocol_id: Some(settle_protocol.clone()),
            key_id: Some(hex::encode(game_id)),
            counterparty: Some(Counterparty::Other(opponent_pub.clone())),
            for_self: Some(true),
        })
        .unwrap()
        .public_key
        .to_ascii_lowercase();
    let settle_pk = hex::decode(&settle_pub_hex).unwrap();

    let (seat_sig, identity_sig) = if real_sigs {
        let preimage =
            hopparty_seatsig_preimage(&game_id, &hop_txid, hop_vout, &identity).unwrap();
        let seat_sig = wallet
            .create_signature(CreateSignatureArgs {
                data: Some(preimage),
                hash_to_directly_sign: None,
                protocol_id: settle_protocol,
                key_id: hex::encode(game_id),
                counterparty: Some(Counterparty::Other(opponent_pub)),
            })
            .unwrap()
            .signature;
        let challenge = hopparty_identity_challenge(
            &identity, &opponent, &game_id, &hop_txid, hop_vout, hop_sats, &settle_pk,
        )
        .unwrap();
        let identity_sig = wallet
            .create_signature(CreateSignatureArgs {
                data: Some(challenge),
                hash_to_directly_sign: None,
                protocol_id: Protocol::new(SecurityLevel::App, "low potparty"),
                key_id: hex::encode(game_id),
                counterparty: Some(Counterparty::Anyone),
            })
            .unwrap()
            .signature;
        (seat_sig, identity_sig)
    } else {
        // Byte-shape-valid garbage DER (71 bytes) — passes admission,
        // must NEVER pass the read-time filter.
        let mut junk = vec![0x30u8, 0x45];
        junk.extend_from_slice(&[0xabu8; 69]);
        (junk.clone(), junk)
    };

    let mut script = vec![0x00, 0x6a];
    script.extend(push_data(HOPPARTY_TAG));
    script.extend(push_data(&identity));
    script.extend(push_data(&opponent));
    script.extend(push_data(&game_id));
    script.extend(push_data(&hop_txid));
    script.extend(push_data(&hop_vout.to_le_bytes()));
    script.extend(push_data(&hop_sats.to_le_bytes()));
    script.extend(push_data(&settle_pk));
    script.extend(push_data(&seat_sig));
    script.extend(push_data(&identity_sig));

    BuiltMarker {
        script,
        identity_hex,
        settle_pub_hex,
    }
}

// ── the PRODUCTION admission chain into SQLite ──────────────────────────────

/// Drive marker BYTES through the real admission chain: the topic
/// manager's per-output gate, the lookup service's parser, and the SHIPPED
/// `hopparty_store_sql()` write with exactly the fields the parser
/// recovered (hex-encoded as `D1HoppartyStorage::store_record` receives
/// them). `created_at` is test-controlled in the storage-layer slot.
fn admit_marker(conn: &Connection, script: &[u8], marker_txid: &str, marker_vout: u32, at: i64) {
    // The TM gate production applies per output.
    let out = bsv_rs::transaction::TransactionOutput {
        satoshis: Some(0),
        locking_script: bsv_rs::script::LockingScript::from_binary(script).unwrap(),
        change: false,
    };
    assert!(
        HoppartyTopicManager::validate_hopparty_output(&out),
        "the marker must clear the REAL admission gate before storage"
    );
    // The LS parse + record construction.
    let m = parse_hopparty_marker(script).expect("admitted marker parses");
    conn.execute(
        hopparty_store_sql(),
        params![
            hex::encode(&m.identity),
            hex::encode(&m.opponent),
            hex::encode(m.game_id),
            hex::encode(m.hop_txid),
            m.hop_vout,
            m.hop_sats as i64,
            hex::encode(&m.seat_settle_pubkey),
            hex::encode(&m.seat_sig),
            hex::encode(&m.identity_sig),
            marker_txid,
            marker_vout,
            at
        ],
    )
    .expect("hopparty_store_sql");
}

/// Admit a hop outpoint the way `tm_lowfund` does (a `pot_records` row via
/// the SHIPPED `store_record_sql()` upsert — `lockKind = 'p2pkh'`, no
/// covenant params).
fn admit_hop(conn: &Connection, hop_txid: &str, sats: i64, created_at: i64) {
    conn.execute(
        store_record_sql(),
        params![
            hop_txid,
            0i64,                   // outputIndex
            0i64,                   // spent
            Option::<String>::None, // spendingTxid
            0i64,                   // spentConfirmed
            created_at,             // createdAt
            Some("p2pkh"),          // lockKind (the tm_lowfund branch)
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
            Some(sats),             // potSats
            1i64,                   // paramsDecoded
        ],
    )
    .expect("store_record_sql");
}

/// The engine's `outputs` insert shape (`D1Storage::insert_output`) for
/// the ADMITTED hop lock under the `tm_lowfund` topic.
fn admit_hop_output_script(conn: &Connection, hop_txid: &str, vout: u32, script_hex: &str) {
    conn.execute(
        "INSERT OR IGNORE INTO outputs \
         (txid, outputIndex, outputScript, topic, satoshis, \
          outputsConsumed, consumedBy, spent, blockHeight, score) \
         VALUES (?1, ?2, ?3, 'tm_lowfund', ?4, '[]', '[]', 0, NULL, 0)",
        params![hop_txid, vout, hex::decode(script_hex).unwrap(), 80_800i64],
    )
    .expect("insert outputs row");
}

/// Record a hop spend via the REAL `mark_spent_sql()` (no verdict — hops
/// have none).
fn mark_hop_spent(conn: &Connection, hop_txid: &str, spender: &str, confirmed: bool) {
    let sql = mark_spent_sql(confirmed, false);
    if confirmed {
        conn.execute(
            sql,
            params![spender, spender, Option::<i64>::None, Option::<i64>::None, hop_txid, 0i64],
        )
    } else {
        conn.execute(sql, params![spender, hop_txid, 0i64])
    }
    .expect("mark_spent_sql");
}

/// Execute the SHIPPED `hops_view_sql()` and map rows exactly as the route
/// does (same columns, same Option-ness).
fn query_rows(conn: &Connection, identity: &str) -> Vec<HopsViewRow> {
    let sql = hops_view_sql();
    let mut stmt = conn.prepare(&sql).expect("prepare hops_view_sql");
    stmt.query_map(params![identity], |r| {
        Ok(HopsViewRow {
            identity: r.get("identity")?,
            game_id: r.get("gameId")?,
            hop_txid: r.get("hopTxid")?,
            hop_vout: r.get::<_, i64>("hopVout")? as u32,
            hop_sats: r.get::<_, i64>("hopSats")? as u64,
            opponent_identity: r.get("opponentIdentity")?,
            seat_settle_pubkey: r.get("seatSettlePubkey")?,
            seat_sig_hex: r.get("seatSigHex")?,
            identity_sig_hex: r.get("identitySigHex")?,
            marker_txid: r.get("markerTxid")?,
            spent: r.get::<_, Option<i64>>("spent")?.map(|v| v != 0),
            spending_txid: r.get("spendingTxid")?,
            spent_confirmed: r.get::<_, Option<i64>>("spentConfirmed")?.map(|v| v != 0),
            spender_proof_verified: r
                .get::<_, Option<i64>>("spenderProofVerified")?
                .map(|v| v != 0),
            // The route applies the same empty-is-absent belt.
            hop_lock_hex: r
                .get::<_, Option<String>>("hopLockHex")?
                .filter(|s| !s.is_empty()),
        })
    })
    .expect("query")
    .collect::<Result<Vec<_>, _>>()
    .expect("rows")
}

const GAME: [u8; 32] = [0x11u8; 32];

// ── the positive control: admission → insert → view → VERIFIED ─────────────

#[test]
fn real_marker_on_an_indexed_hop_is_verified_and_unspent() {
    let conn = production_schema_db();
    let hop = h64(0xaa);
    let m = build_marker(0xa1, GAME, [0xaau8; 32], 0, 80_800, true);
    admit_hop(&conn, &hop, 80_800, 1_000);
    admit_hop_output_script(&conn, &hop, 0, &expected_hop_lock_hex(&m.settle_pub_hex).unwrap());
    admit_marker(&conn, &m.script, &h64(0xc1), 1, 1_001);

    let rows = query_rows(&conn, &m.identity_hex);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].hop_sats, 80_800, "8-byte LE sats round-trip the schema");
    assert_eq!(
        rows[0].hop_lock_hex.as_deref().map(str::to_ascii_lowercase),
        Some(expected_hop_lock_hex(&m.settle_pub_hex).unwrap()),
        "the admitted hop lock is joined from outputs/tm_lowfund"
    );
    let (entries, truncated, exhausted) = assemble_hops_view(rows);
    assert!(!truncated && !exhausted);
    let e = &entries[0];
    assert_eq!(e.status, HopStatus::Unspent);
    assert_eq!(e.status_source, Some("chain"));
    assert_eq!(
        e.marker_verified,
        MarkerVerification::Verified,
        "positive control: real sigs + matching admitted lock reach and clear every bar"
    );
    assert_eq!(e.game_id, hex::encode(GAME), "the /live-view join key rides each entry");
}

// ── refusal labels: served, labeled, never dropped, never verified ─────────

#[test]
fn junk_sig_marker_is_served_labeled_unverified_never_verified() {
    let conn = production_schema_db();
    let hop = h64(0xaa);
    let m = build_marker(0xa1, GAME, [0xaau8; 32], 0, 80_800, false); // garbage sigs
    admit_hop(&conn, &hop, 80_800, 1_000);
    admit_hop_output_script(&conn, &hop, 0, &expected_hop_lock_hex(&m.settle_pub_hex).unwrap());
    admit_marker(&conn, &m.script, &h64(0xc1), 1, 1_001);

    let (entries, _, _) = assemble_hops_view(query_rows(&conn, &m.identity_hex));
    assert_eq!(entries.len(), 1, "the failing row is SERVED, never dropped");
    assert_eq!(
        entries[0].marker_verified,
        MarkerVerification::Unverified,
        "junk sigs NEVER become markerVerified: verified — even with a matching lock"
    );
    // And the body labels it (the wire the client sees).
    let body = hops_view_body(&m.identity_hex, None, &entries, false, false);
    assert!(body.contains("\"markerVerified\":\"unverified\""));
    assert!(!body.contains("\"markerVerified\":\"verified\""));
}

/// The F1 bar through the full pipe: an OPPONENT's wallet can mint a valid
/// seatSig over the VICTIM's identity with its OWN settle key and file the
/// marker naming its own hop — only the missing identitySig separates it
/// from an honest row, so the filter must refuse it.
#[test]
fn opponent_minted_marker_with_victim_identity_is_unverified() {
    let conn = production_schema_db();
    let victim_wallet = wallet_of(0xa1);
    let victim_hex = victim_wallet.identity_key_hex().to_ascii_lowercase();
    let victim = hex::decode(&victim_hex).unwrap();

    // The ATTACKER (0xe7) seat-signs a preimage embedding the VICTIM's
    // identity with the attacker's own settle key over the attacker's hop.
    let attacker = wallet_of(0xe7);
    let attacker_id = hex::decode(attacker.identity_key_hex()).unwrap();
    let hop_txid = [0xeeu8; 32];
    let settle_protocol = Protocol::new(SecurityLevel::Counterparty, "low settle");
    let opp_pub = bsv_rs::primitives::ec::PrivateKey::from_hex(&format!("{:064x}", 0xb0u8))
        .unwrap()
        .public_key();
    let attacker_settle_hex = attacker
        .get_public_key(GetPublicKeyArgs {
            identity_key: false,
            protocol_id: Some(settle_protocol.clone()),
            key_id: Some(hex::encode(GAME)),
            counterparty: Some(Counterparty::Other(opp_pub.clone())),
            for_self: Some(true),
        })
        .unwrap()
        .public_key
        .to_ascii_lowercase();
    let attacker_settle = hex::decode(&attacker_settle_hex).unwrap();
    let preimage = hopparty_seatsig_preimage(&GAME, &hop_txid, 0, &victim).unwrap();
    let seat_sig = attacker
        .create_signature(CreateSignatureArgs {
            data: Some(preimage),
            hash_to_directly_sign: None,
            protocol_id: settle_protocol,
            key_id: hex::encode(GAME),
            counterparty: Some(Counterparty::Other(opp_pub)),
        })
        .unwrap()
        .signature;
    // The attacker CANNOT mint the victim's identitySig — it self-signs.
    let challenge = hopparty_identity_challenge(
        &victim, &attacker_id, &GAME, &hop_txid, 0, 80_800, &attacker_settle,
    )
    .unwrap();
    let fake_identity_sig = attacker
        .create_signature(CreateSignatureArgs {
            data: Some(challenge),
            hash_to_directly_sign: None,
            protocol_id: Protocol::new(SecurityLevel::App, "low potparty"),
            key_id: hex::encode(GAME),
            counterparty: Some(Counterparty::Anyone),
        })
        .unwrap()
        .signature;
    let mut script = vec![0x00, 0x6a];
    script.extend(push_data(HOPPARTY_TAG));
    script.extend(push_data(&victim));
    script.extend(push_data(&attacker_id));
    script.extend(push_data(&GAME));
    script.extend(push_data(&hop_txid));
    script.extend(push_data(&0u32.to_le_bytes()));
    script.extend(push_data(&80_800u64.to_le_bytes()));
    script.extend(push_data(&attacker_settle));
    script.extend(push_data(&seat_sig));
    script.extend(push_data(&fake_identity_sig));

    // The attacker's hop IS real and indexed, and its lock genuinely pays
    // the attacker's settle key — the script bar alone would pass!
    let hop_hex = hex::encode(hop_txid);
    admit_hop(&conn, &hop_hex, 80_800, 1_000);
    admit_hop_output_script(&conn, &hop_hex, 0, &expected_hop_lock_hex(&attacker_settle_hex).unwrap());
    admit_marker(&conn, &script, &h64(0xc9), 1, 1_001);

    let rows = query_rows(&conn, &victim_hex);
    assert_eq!(rows.len(), 1, "the row lands in the VICTIM's view (admission cannot stop it)");
    let (entries, _, _) = assemble_hops_view(rows);
    assert_eq!(
        entries[0].marker_verified,
        MarkerVerification::Unverified,
        "seatSig valid + lock match is NOT enough — the identity bar (F1) must refuse"
    );
}

#[test]
fn mismatched_admitted_lock_is_unverified() {
    let conn = production_schema_db();
    let hop = h64(0xaa);
    let m = build_marker(0xa1, GAME, [0xaau8; 32], 0, 80_800, true);
    admit_hop(&conn, &hop, 80_800, 1_000);
    // The chain's hop output pays a DIFFERENT key than the marker claims.
    admit_hop_output_script(&conn, &hop, 0, &format!("76a914{}88ac", "77".repeat(20)));
    admit_marker(&conn, &m.script, &h64(0xc1), 1, 1_001);

    let (entries, _, _) = assemble_hops_view(query_rows(&conn, &m.identity_hex));
    assert_eq!(entries[0].marker_verified, MarkerVerification::Unverified);
}

// ── unknown first-class ─────────────────────────────────────────────────────

#[test]
fn unindexed_hop_is_unknown_status_and_unknown_verification() {
    let conn = production_schema_db();
    // Real marker, but the hop was NEVER indexed (no pot_records row, no
    // outputs row) — the fresh-hop-in-flight / pre-admission case.
    let m = build_marker(0xa1, GAME, [0xaau8; 32], 0, 80_800, true);
    admit_marker(&conn, &m.script, &h64(0xc1), 1, 1_001);

    let rows = query_rows(&conn, &m.identity_hex);
    assert_eq!(rows.len(), 1, "an unindexed hop is still SERVED (promoted quota)");
    assert_eq!(rows[0].spent, None, "never asserted unspent");
    let (entries, _, _) = assemble_hops_view(rows);
    let e = &entries[0];
    assert_eq!(e.status, HopStatus::Unknown);
    assert_eq!(e.status_source, None);
    assert_eq!(
        e.marker_verified,
        MarkerVerification::Unknown,
        "valid sigs + no admitted script to check = unknown, not failed"
    );
}

#[test]
fn spend_statuses_confirmed_vs_parked() {
    let conn = production_schema_db();
    // Hop A: spend recorded + CONFIRMED — spent/chain.
    let ma = build_marker(0xa1, GAME, [0xaau8; 32], 0, 80_800, true);
    admit_hop(&conn, &h64(0xaa), 80_800, 1_000);
    admit_marker(&conn, &ma.script, &h64(0xc1), 1, 1_001);
    mark_hop_spent(&conn, &h64(0xaa), &h64(0xfe), true);
    // Hop B (same identity, different game bytes irrelevant): spend
    // recorded but UNCONFIRMED — a displaceable intent, unknown.
    let mb = build_marker(0xa1, GAME, [0xabu8; 32], 0, 70_000, true);
    admit_hop(&conn, &h64(0xab), 70_000, 1_100);
    admit_marker(&conn, &mb.script, &h64(0xc2), 1, 1_101);
    mark_hop_spent(&conn, &h64(0xab), &h64(0xfd), false);

    let (entries, _, _) = assemble_hops_view(query_rows(&conn, &ma.identity_hex));
    assert_eq!(entries.len(), 2);
    let a = entries.iter().find(|e| e.hop_txid == h64(0xaa)).unwrap();
    assert_eq!((a.status, a.status_source), (HopStatus::Spent, Some("chain")));
    assert_eq!(a.spending_txid.as_deref(), Some(h64(0xfe).as_str()));
    let b = entries.iter().find(|e| e.hop_txid == h64(0xab)).unwrap();
    assert_eq!((b.status, b.status_source), (HopStatus::Unknown, None));
    assert_eq!(b.spent, Some(true), "the raw facts still serve");
    assert_eq!(b.spent_confirmed, Some(false));
}

// ── the window: superset vs forged eviction, BOTH orderings ────────────────

#[test]
fn forged_markers_cannot_evict_the_honest_row_in_either_stamp_order() {
    for honest_first in [false, true] {
        let conn = production_schema_db();
        let hop = h64(0xaa);
        let honest = build_marker(0xa1, GAME, [0xaau8; 32], 0, 80_800, true);
        admit_hop(&conn, &hop, 80_800, 1_000);
        admit_hop_output_script(
            &conn,
            &hop,
            0,
            &expected_hop_lock_hex(&honest.settle_pub_hex).unwrap(),
        );
        let honest_at = if honest_first { 500 } else { 5_000 };
        admit_marker(&conn, &honest.script, &h64(0xc1), 1, honest_at);
        // Two forged (shape-valid, junk-sig) markers naming the SAME hop +
        // identity, stamped on the other side of the honest one.
        for i in 0..2u8 {
            let forged = build_marker(0xa1, GAME, [0xaau8; 32], 0, 1, false);
            admit_marker(&conn, &forged.script, &format!("{:064x}", 0xf0u64 + i as u64), 1, 1_000 + i as i64);
        }

        let rows = query_rows(&conn, &honest.identity_hex);
        assert_eq!(rows.len(), 3, "the SUPERSET serves all three (order {honest_first})");
        let (entries, _, _) = assemble_hops_view(rows);
        assert_eq!(
            entries
                .iter()
                .filter(|e| e.marker_verified == MarkerVerification::Verified)
                .count(),
            1,
            "exactly the honest row verifies (honest_first = {honest_first})"
        );
        assert_eq!(
            entries
                .iter()
                .find(|e| e.marker_verified == MarkerVerification::Verified)
                .unwrap()
                .marker_txid,
            h64(0xc1),
            "and it IS the honest marker — a forged stamp cannot evict it"
        );
    }
}

/// The honest `truncated` bit through the SHIPPED SQL: one hop past the
/// page flips it; exactly the page does not (both orderings of the bound).
#[test]
fn truncation_bit_via_the_shipped_sql() {
    let conn = production_schema_db();
    let wallet = wallet_of(0xa1);
    let identity_hex = wallet.identity_key_hex().to_ascii_lowercase();
    // MAX+1 indexed hops, one (junk-sig, cheap) marker each.
    for i in 0..=HOPS_VIEW_MAX_OUTPOINTS {
        let mut hop = [0u8; 32];
        hop[..8].copy_from_slice(&(i as u64).to_be_bytes());
        let hop_hex = hex::encode(hop);
        admit_hop(&conn, &hop_hex, 500, 1_000 + i as i64);
        let m = build_marker(0xa1, GAME, hop, 0, 500, false);
        admit_marker(&conn, &m.script, &format!("{:064x}", 0xa000u64 + i as u64), 1, 1_000 + i as i64);
    }
    let rows = query_rows(&conn, &identity_hex);
    let (entries, truncated, _) = assemble_hops_view(rows);
    assert!(truncated, "MAX+1 outpoints ⇒ the page is honestly incomplete");
    let distinct: std::collections::HashSet<&String> =
        entries.iter().map(|e| &e.hop_txid).collect();
    assert_eq!(distinct.len(), HOPS_VIEW_MAX_OUTPOINTS, "page bound in outpoints");

    // Remove one hop's marker → exactly the page → complete.
    conn.execute(
        "DELETE FROM hopparty_records WHERE hopTxid = ?1",
        params![hex::encode({
            let mut h = [0u8; 32];
            h[..8].copy_from_slice(&0u64.to_be_bytes());
            h
        })],
    )
    .unwrap();
    let (entries, truncated, _) = assemble_hops_view(query_rows(&conn, &identity_hex));
    assert!(!truncated, "exactly the page ⇒ complete");
    assert_eq!(entries.len(), HOPS_VIEW_MAX_OUTPOINTS);
}

// ── fail-safe / fault cores ─────────────────────────────────────────────────

#[test]
fn unknown_identity_is_a_well_formed_empty_answer() {
    let conn = production_schema_db();
    let m = build_marker(0xa1, GAME, [0xaau8; 32], 0, 80_800, true);
    admit_marker(&conn, &m.script, &h64(0xc1), 1, 1_001);

    // A valid identity with nothing indexed: zero rows, well-formed body.
    let stranger = h66(0xee);
    let rows = query_rows(&conn, &stranger);
    assert!(rows.is_empty());
    let (entries, truncated, exhausted) = assemble_hops_view(rows);
    let v: serde_json::Value =
        serde_json::from_str(&hops_view_body(&stranger, None, &entries, truncated, exhausted))
            .unwrap();
    assert_eq!(v["hops"], serde_json::json!([]));
    assert_eq!(v["truncated"], serde_json::json!(false));

    // The route's invalid-identity guard (fail-safe-empty 200, never an
    // error) keys on the same `valid_identity` every identity surface uses.
    assert!(!valid_identity(""));
    assert!(!valid_identity("zz"));
    assert!(!valid_identity(&h64(0xaa))); // 64 hex — a txid, not an identity
    assert!(valid_identity(&h66(0xee)));
}

/// The 503 core: against a PRE-MIGRATION schema (no `hopparty_records`)
/// the shipped SQL faults at prepare — which is exactly what the route
/// converts to a 503 (a fault is never shaped like an answer).
#[test]
fn pre_migration_schema_faults_the_query_the_routes_503_path() {
    let conn = Connection::open_in_memory().unwrap();
    assert!(
        conn.prepare(&hops_view_sql()).is_err(),
        "missing tables must FAULT the query (the route answers 503), \
         never serve an empty-but-complete-looking page"
    );
}
