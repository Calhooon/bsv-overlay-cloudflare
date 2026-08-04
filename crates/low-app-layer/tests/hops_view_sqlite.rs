//! `/hops-view` proofs — REAL SQLite, PRODUCTION schema (bsv-low #315,
//! #252 stage 2b).
//!
//! These tests EXECUTE the exact shipped `hops_view_sql(false)` against real
//! SQLite carrying the overlay's PRODUCTION migration list verbatim, with
//! rows produced by the REAL producer chain (Rule 6b: never a hand-fed
//! shape):
//!
//!  - marker BYTES are built with the real digest builders + real BRC-42 /
//!    RFC6979 crypto and placed as output 1 of a REAL CONTAINER
//!    transaction whose output 0 is the hop P2PKH (the production
//!    `createAction` layout that `randomizeOutputs: false` guarantees);
//!    that container is serialized to BEEF and driven through the topic
//!    manager's admission gate and then the REAL
//!    `HoppartyLookupService::output_admitted_by_topic` (whole-tx mode) —
//!    so the containing txid and the decoded container facts come from
//!    the real producer, never from a hand-written row;
//!  - the lookup service's record is written with the SHIPPED
//!    `hopparty_store_sql()` string (exact bind order);
//!  - hop outpoints are admitted via the shipped `store_record_sql()`
//!    (`tm_lowfund` → `pot_records`) and spends via `mark_spent_sql()`.
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
use bsv_rs::script::LockingScript;
use bsv_rs::transaction::{Transaction, TransactionInput, TransactionOutput};
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
        let preimage = hopparty_seatsig_preimage(&game_id, hop_vout, &identity).unwrap();
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
            &identity, &opponent, &game_id, hop_vout, hop_sats, &settle_pk,
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

/// Build the PRODUCTION container: a tx whose output 0 is the hop P2PKH
/// and whose output 1 is the marker OP_RETURN — the exact layout the hop
/// `createAction` emits under `randomizeOutputs: false`. `hop_lock_hex` /
/// `hop_sats` are what the CHAIN will say; the marker's own claims are
/// whatever `build_marker` put in it, so the two can be made to disagree.
fn container(hop_lock_hex: &str, hop_sats: u64, marker: &[u8], nonce: u8) -> (Vec<u8>, String) {
    let mut tx = Transaction::new();
    // The nonce varies the input so distinct containers get distinct txids.
    tx.add_input(TransactionInput::new(format!("{nonce:02x}").repeat(32), 0))
        .unwrap();
    tx.add_output(TransactionOutput {
        satoshis: Some(hop_sats),
        locking_script: LockingScript::from_hex(hop_lock_hex).unwrap(),
        change: false,
    })
    .unwrap();
    tx.add_output(TransactionOutput {
        satoshis: Some(0),
        locking_script: LockingScript::from_binary(marker).unwrap(),
        change: false,
    })
    .unwrap();
    let beef = tx.to_beef(true).expect("BEEF serialization");
    let txid = Transaction::from_beef(&beef, None)
        .expect("engine-side parse")
        .id();
    (beef, txid)
}

/// Drive a CONTAINER through the real admission chain and into SQLite: the
/// topic manager's per-output gate, then the decode the production lookup
/// service performs (container txid + its output at `hopVout`), written
/// with the SHIPPED `hopparty_store_sql()`. Returns the container txid.
/// `created_at` is test-controlled in the storage-layer slot.
fn admit_container(conn: &Connection, beef: &[u8], marker_vout: u32, at: i64) -> String {
    let tx = Transaction::from_beef(beef, None).expect("container parses");
    let marker_output = tx.outputs.get(marker_vout as usize).expect("marker output");
    // The TM gate production applies per output.
    assert!(
        HoppartyTopicManager::validate_hopparty_output(marker_output),
        "the marker must clear the REAL admission gate before storage"
    );
    let m = parse_hopparty_marker(&marker_output.locking_script.to_binary())
        .expect("admitted marker parses");
    let txid = tx.id();
    // The lookup service's #310 decode-at-write of the CONTAINER's own
    // output at hopVout.
    let container_outputs = tx.outputs.len() as u32;
    let hop_output = tx.outputs.get(m.hop_vout as usize);
    let hop_lock_hex = hop_output.map(|o| hex::encode(o.locking_script.to_binary()));
    let hop_sats_on_chain = hop_output.and_then(|o| o.satoshis);
    conn.execute(
        hopparty_store_sql(),
        params![
            hex::encode(&m.identity),
            hex::encode(&m.opponent),
            hex::encode(m.game_id),
            m.hop_vout,
            m.hop_sats as i64,
            hex::encode(&m.seat_settle_pubkey),
            hex::encode(&m.seat_sig),
            hex::encode(&m.identity_sig),
            hop_lock_hex,
            hop_sats_on_chain.map(|v| v as i64),
            container_outputs,
            &txid,
            marker_vout,
            at
        ],
    )
    .expect("hopparty_store_sql");
    txid
}

/// The common case: admit a marker inside a container that really pays
/// `settle_pub` the amount the marker claims.
fn admit_marker(conn: &Connection, m: &BuiltMarker, hop_sats: u64, nonce: u8, at: i64) -> String {
    let (beef, _) = container(
        &expected_hop_lock_hex(&m.settle_pub_hex).unwrap(),
        hop_sats,
        &m.script,
        nonce,
    );
    admit_container(conn, &beef, 1, at)
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

/// Execute the SHIPPED `hops_view_sql(false)` and map rows exactly as the route
/// does (same columns, same Option-ness).
fn query_rows(conn: &Connection, identity: &str) -> Vec<HopsViewRow> {
    query_rows_inner(conn, hops_view_sql(false), params![identity])
}

/// The `?gameId=`-scoped window — the escape hatch a truncated caller uses.
fn query_rows_scoped(conn: &Connection, identity: &str, game_id: &str) -> Vec<HopsViewRow> {
    query_rows_inner(conn, hops_view_sql(true), params![identity, game_id])
}

fn query_rows_inner<P: rusqlite::Params>(
    conn: &Connection,
    sql: String,
    binds: P,
) -> Vec<HopsViewRow> {
    let mut stmt = conn.prepare(&sql).expect("prepare hops_view_sql");
    stmt.query_map(binds, |r| {
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
            marker_vout: r.get::<_, i64>("markerVout")? as u32,
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
            hop_sats_on_chain: r.get::<_, Option<i64>>("hopSatsOnChain")?.map(|v| v as u64),
            container_outputs: r.get::<_, i64>("containerOutputs")? as u32,
        })
    })
    .expect("query")
    .collect::<Result<Vec<_>, _>>()
    .expect("rows")
}

const GAME: [u8; 32] = [0x11u8; 32];

// ── the positive control: admission → insert → view → VERIFIED ─────────────

#[test]
fn real_marker_in_its_production_container_is_verified_and_unspent() {
    let conn = production_schema_db();
    let m = build_marker(0xa1, GAME, 0, 80_800, true);
    // The container IS the hop tx: output 0 pays the settle key 80_800,
    // output 1 carries the marker.
    let txid = admit_marker(&conn, &m, 80_800, 0x01, 1_001);
    // tm_lowfund indexes that same outpoint.
    admit_hop(&conn, &txid, 80_800, 1_000);

    let rows = query_rows(&conn, &m.identity_hex);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].hop_txid, txid, "the container supplies the hop txid");
    assert_eq!(rows[0].marker_txid, txid, "…and it IS the marker's tx");
    assert_eq!(rows[0].marker_vout, 1);
    assert_eq!(rows[0].hop_sats, 80_800, "8-byte LE sats round-trip the schema");
    assert_eq!(rows[0].hop_sats_on_chain, Some(80_800), "the chain agrees");
    assert_eq!(rows[0].container_outputs, 2);
    assert_eq!(
        rows[0].hop_lock_hex.as_deref().map(str::to_ascii_lowercase),
        Some(expected_hop_lock_hex(&m.settle_pub_hex).unwrap()),
        "the container's own output at hopVout, decoded at admission"
    );
    let (entries, truncated, exhausted) = assemble_hops_view(rows);
    assert!(!truncated && !exhausted);
    let e = &entries[0];
    assert_eq!(e.status, HopStatus::Unspent);
    assert_eq!(e.status_source, Some("chain"));
    assert_eq!(
        e.marker_verified,
        MarkerVerification::Verified,
        "positive control: real sigs + a container that really pays the claimed \
         value to the claimed key clears every bar"
    );
    assert_eq!(e.game_id, hex::encode(GAME), "the /live-view join key rides each entry");
}

// ── refusal labels: served, labeled, never dropped, never verified ─────────

#[test]
fn junk_sig_marker_is_served_labeled_unverified_never_verified() {
    let conn = production_schema_db();
    let m = build_marker(0xa1, GAME, 0, 80_800, false); // garbage sigs
    let txid = admit_marker(&conn, &m, 80_800, 0x01, 1_001);
    admit_hop(&conn, &txid, 80_800, 1_000);

    let (entries, _, _) = assemble_hops_view(query_rows(&conn, &m.identity_hex));
    assert_eq!(entries.len(), 1, "the failing row is SERVED, never dropped");
    assert_eq!(
        entries[0].marker_verified,
        MarkerVerification::Unverified,
        "junk sigs NEVER become markerVerified: verified — even in a perfect container"
    );
    let body = hops_view_body(&m.identity_hex, None, &entries, false, false);
    assert!(body.contains("\"markerVerified\":\"unverified\""));
    assert!(!body.contains("\"markerVerified\":\"verified\""));
}

/// The VALUE bar end-to-end: a container that pays the right key the WRONG
/// amount is refused. This is the bar that prices the replay residual.
#[test]
fn a_container_paying_the_wrong_value_is_unverified() {
    let conn = production_schema_db();
    let m = build_marker(0xa1, GAME, 0, 80_800, true);
    // The marker claims 80_800; the container pays 1 sat to the same key.
    let txid = admit_marker(&conn, &m, 1, 0x01, 1_001);
    admit_hop(&conn, &txid, 1, 1_000);

    let rows = query_rows(&conn, &m.identity_hex);
    assert_eq!(rows[0].hop_sats, 80_800, "the CLAIM");
    assert_eq!(rows[0].hop_sats_on_chain, Some(1), "the CHAIN");
    let (entries, _, _) = assemble_hops_view(rows);
    assert_eq!(entries[0].marker_verified, MarkerVerification::Unverified);
}

/// A container that pays SOMEONE ELSE at hopVout is refused (the lock bar).
#[test]
fn a_container_paying_another_key_is_unverified() {
    let conn = production_schema_db();
    let m = build_marker(0xa1, GAME, 0, 80_800, true);
    let (beef, txid) = container(
        &format!("76a914{}88ac", "77".repeat(20)),
        80_800,
        &m.script,
        0x01,
    );
    admit_container(&conn, &beef, 1, 1_001);
    admit_hop(&conn, &txid, 80_800, 1_000);

    let (entries, _, _) = assemble_hops_view(query_rows(&conn, &m.identity_hex));
    assert_eq!(entries[0].marker_verified, MarkerVerification::Unverified);
}

/// A marker naming a vout its own container LACKS: the absence is proven
/// (`containerOutputs`), so the row is refuted — not left open.
#[test]
fn a_container_without_that_output_refutes_the_marker() {
    let conn = production_schema_db();
    // Claims hopVout 7; the container has 2 outputs.
    let m = build_marker(0xa1, GAME, 7, 80_800, true);
    let (beef, _) = container(
        &expected_hop_lock_hex(&m.settle_pub_hex).unwrap(),
        80_800,
        &m.script,
        0x01,
    );
    admit_container(&conn, &beef, 1, 1_001);

    let rows = query_rows(&conn, &m.identity_hex);
    assert_eq!(rows.len(), 1, "stored — admission never refuses");
    assert!(rows[0].hop_lock_hex.is_none() && rows[0].hop_sats_on_chain.is_none());
    assert_eq!(rows[0].container_outputs, 2, "absence is PROVEN");
    let (entries, _, _) = assemble_hops_view(rows);
    assert_eq!(entries[0].marker_verified, MarkerVerification::Unverified);
}

/// The F1 bar through the full pipe: an OPPONENT can mint a valid seatSig
/// over the VICTIM's identity with its OWN settle key and fund a container
/// that genuinely pays that key — every bar EXCEPT the identity signature
/// passes, so the identity bar is what refuses it.
#[test]
fn opponent_minted_marker_with_victim_identity_is_unverified() {
    let conn = production_schema_db();
    let victim_wallet = wallet_of(0xa1);
    let victim_hex = victim_wallet.identity_key_hex().to_ascii_lowercase();
    let victim = hex::decode(&victim_hex).unwrap();

    let attacker = wallet_of(0xe7);
    let attacker_id = hex::decode(attacker.identity_key_hex()).unwrap();
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
    // The attacker seat-signs a preimage embedding the VICTIM's identity.
    let preimage = hopparty_seatsig_preimage(&GAME, 0, &victim).unwrap();
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
    // It CANNOT mint the victim's identitySig — it self-signs instead.
    let challenge =
        hopparty_identity_challenge(&victim, &attacker_id, &GAME, 0, 80_800, &attacker_settle)
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
    script.extend(push_data(&0u32.to_le_bytes()));
    script.extend(push_data(&80_800u64.to_le_bytes()));
    script.extend(push_data(&attacker_settle));
    script.extend(push_data(&seat_sig));
    script.extend(push_data(&fake_identity_sig));

    // The attacker's container genuinely pays ITS OWN settle key the full
    // claimed amount — both container bars PASS.
    let (beef, txid) = container(
        &expected_hop_lock_hex(&attacker_settle_hex).unwrap(),
        80_800,
        &script,
        0x01,
    );
    admit_container(&conn, &beef, 1, 1_001);
    admit_hop(&conn, &txid, 80_800, 1_000);

    let rows = query_rows(&conn, &victim_hex);
    assert_eq!(rows.len(), 1, "the row lands in the VICTIM's view (admission cannot stop it)");
    assert_eq!(
        rows[0].hop_lock_hex.as_deref().map(str::to_ascii_lowercase),
        Some(expected_hop_lock_hex(&attacker_settle_hex).unwrap()),
        "the container bars would pass on their own"
    );
    let (entries, _, _) = assemble_hops_view(rows);
    assert_eq!(
        entries[0].marker_verified,
        MarkerVerification::Unverified,
        "seatSig valid + container match is NOT enough — the identity bar (F1) refuses"
    );
}

/// The PAID REPLAY residual, executed end-to-end through the producer
/// chain: an attacker copies the victim's admitted marker bytes verbatim
/// into their OWN container. It verifies — but ONLY because that container
/// pays `hopSats` to the VICTIM's settle key, i.e. the attacker gave the
/// victim the money. Every cheaper container refuses. Documented at
/// `derive_marker_verification`; pinned here so the boundary is executable
/// rather than prose.
#[test]
fn a_replay_container_verifies_only_by_paying_the_victim() {
    let conn = production_schema_db();
    let m = build_marker(0xa1, GAME, 0, 80_800, true);
    let honest_txid = admit_marker(&conn, &m, 80_800, 0x01, 1_001);
    admit_hop(&conn, &honest_txid, 80_800, 1_000);

    // The attacker's replay: same marker bytes, different container, and it
    // really pays the victim's settle key the full amount.
    let paid_txid = admit_marker(&conn, &m, 80_800, 0x02, 2_000);
    assert_ne!(paid_txid, honest_txid, "a distinct container ⇒ a distinct outpoint");
    // A cheaper replay: same bytes, 1 sat.
    let (beef, _) = container(
        &expected_hop_lock_hex(&m.settle_pub_hex).unwrap(),
        1,
        &m.script,
        0x03,
    );
    admit_container(&conn, &beef, 1, 2_100);

    let (entries, _, _) = assemble_hops_view(query_rows(&conn, &m.identity_hex));
    assert_eq!(entries.len(), 3, "all three rows are served");
    let paid = entries.iter().find(|e| e.hop_txid == paid_txid).unwrap();
    assert_eq!(
        paid.marker_verified,
        MarkerVerification::Verified,
        "the residual is REAL: a replay that pays the victim 80_800 sats verifies"
    );
    // …and the cheap one does not.
    assert_eq!(
        entries
            .iter()
            .filter(|e| e.marker_verified == MarkerVerification::Verified)
            .count(),
        2,
        "only the honest row and the FULLY PAID replay verify"
    );
}

// ── unknown first-class ─────────────────────────────────────────────────────

#[test]
fn unindexed_hop_is_unknown_status_but_still_verifiable() {
    let conn = production_schema_db();
    // Real marker in a real container, but the hop outpoint was never
    // indexed under tm_lowfund (no pot_records row) — the fresh
    // hop-in-flight case, which is exactly what this surface exists for.
    let m = build_marker(0xa1, GAME, 0, 80_800, true);
    admit_marker(&conn, &m, 80_800, 0x01, 1_001);

    let rows = query_rows(&conn, &m.identity_hex);
    assert_eq!(rows.len(), 1, "an unindexed hop is still SERVED (promoted quota)");
    assert_eq!(rows[0].spent, None, "never asserted unspent");
    let (entries, _, _) = assemble_hops_view(rows);
    let e = &entries[0];
    assert_eq!(e.status, HopStatus::Unknown);
    assert_eq!(e.status_source, None);
    assert_eq!(
        e.marker_verified,
        MarkerVerification::Verified,
        "verification rides the CONTAINER, so it does not wait on tm_lowfund \
         admission — this is what the outputs-join draft could not do"
    );
}

#[test]
fn spend_statuses_confirmed_vs_parked() {
    let conn = production_schema_db();
    // Hop A: spend recorded + CONFIRMED — spent/chain.
    let ma = build_marker(0xa1, GAME, 0, 80_800, true);
    let a_txid = admit_marker(&conn, &ma, 80_800, 0x01, 1_001);
    admit_hop(&conn, &a_txid, 80_800, 1_000);
    mark_hop_spent(&conn, &a_txid, &h64(0xfe), true);
    // Hop B: spend recorded but UNCONFIRMED — a displaceable intent.
    let mb = build_marker(0xa1, GAME, 0, 70_000, true);
    let b_txid = admit_marker(&conn, &mb, 70_000, 0x02, 1_101);
    admit_hop(&conn, &b_txid, 70_000, 1_100);
    mark_hop_spent(&conn, &b_txid, &h64(0xfd), false);

    let (entries, _, _) = assemble_hops_view(query_rows(&conn, &ma.identity_hex));
    assert_eq!(entries.len(), 2);
    let a = entries.iter().find(|e| e.hop_txid == a_txid).unwrap();
    assert_eq!((a.status, a.status_source), (HopStatus::Spent, Some("chain")));
    assert_eq!(a.spending_txid.as_deref(), Some(h64(0xfe).as_str()));
    let b = entries.iter().find(|e| e.hop_txid == b_txid).unwrap();
    assert_eq!((b.status, b.status_source), (HopStatus::Unknown, None));
    assert_eq!(b.spent, Some(true), "the raw facts still serve");
    assert_eq!(b.spent_confirmed, Some(false));
}

// ── the window: superset vs forged eviction, BOTH orderings ────────────────

#[test]
fn forged_markers_cannot_evict_the_honest_row_in_either_stamp_order() {
    for honest_first in [false, true] {
        let conn = production_schema_db();
        let m = build_marker(0xa1, GAME, 0, 80_800, true);
        // The honest container, and TWO junk markers riding that SAME
        // container at other vouts (the cheapest way to crowd one hop
        // outpoint — no extra funding needed).
        let honest_at = if honest_first { 500 } else { 5_000 };
        let junk = build_marker(0xa1, GAME, 0, 80_800, false);
        let mut tx = Transaction::new();
        tx.add_input(TransactionInput::new("aa".repeat(32), 0)).unwrap();
        tx.add_output(TransactionOutput {
            satoshis: Some(80_800),
            locking_script: LockingScript::from_hex(
                &expected_hop_lock_hex(&m.settle_pub_hex).unwrap(),
            )
            .unwrap(),
            change: false,
        })
        .unwrap();
        for script in [&m.script, &junk.script, &junk.script] {
            tx.add_output(TransactionOutput {
                satoshis: Some(0),
                locking_script: LockingScript::from_binary(script).unwrap(),
                change: false,
            })
            .unwrap();
        }
        let beef = tx.to_beef(true).unwrap();
        let txid = admit_container(&conn, &beef, 1, honest_at);
        admit_container(&conn, &beef, 2, 1_000);
        admit_container(&conn, &beef, 3, 1_001);
        admit_hop(&conn, &txid, 80_800, 900);

        let rows = query_rows(&conn, &m.identity_hex);
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
                .marker_vout,
            1,
            "and it IS the honest marker — a forged stamp cannot evict it"
        );
    }
}

/// The honest `truncated` bit through the SHIPPED SQL: one hop past the
/// page flips it; exactly the page does not.
#[test]
fn truncation_bit_via_the_shipped_sql() {
    let conn = production_schema_db();
    let wallet = wallet_of(0xa1);
    let identity_hex = wallet.identity_key_hex().to_ascii_lowercase();
    // MAX+1 containers, one (junk-sig, cheap) marker each.
    let mut first_txid = String::new();
    for i in 0..=HOPS_VIEW_MAX_OUTPOINTS {
        let m = build_marker(0xa1, GAME, 0, 500, false);
        let (beef, txid) = container(
            &expected_hop_lock_hex(&m.settle_pub_hex).unwrap(),
            500,
            &m.script,
            (i % 251) as u8,
        );
        // Distinct containers need distinct inputs; the nonce cycles, so
        // vary the input index too via a second tx field when it repeats.
        let txid = if i == 0 {
            first_txid = txid.clone();
            admit_container(&conn, &beef, 1, 1_000 + i as i64)
        } else if txid == first_txid {
            continue;
        } else {
            admit_container(&conn, &beef, 1, 1_000 + i as i64)
        };
        admit_hop(&conn, &txid, 500, 1_000 + i as i64);
    }
    let distinct_hops: i64 = conn
        .query_row(
            "SELECT COUNT(DISTINCT txid || ':' || hopVout) FROM hopparty_records",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(
        distinct_hops > HOPS_VIEW_MAX_OUTPOINTS as i64,
        "the fixture must exceed the page to test truncation (got {distinct_hops})"
    );

    let (entries, truncated, _) = assemble_hops_view(query_rows(&conn, &identity_hex));
    assert!(truncated, "more outpoints than the page ⇒ honestly incomplete");
    let distinct: std::collections::HashSet<&String> =
        entries.iter().map(|e| &e.hop_txid).collect();
    assert_eq!(distinct.len(), HOPS_VIEW_MAX_OUTPOINTS, "page bound in outpoints");

    // Drop the surplus hops → exactly the page → complete.
    conn.execute(
        "DELETE FROM hopparty_records WHERE txid IN \
         (SELECT txid FROM hopparty_records ORDER BY createdAt DESC LIMIT ?1)",
        params![distinct_hops - HOPS_VIEW_MAX_OUTPOINTS as i64],
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
    let m = build_marker(0xa1, GAME, 0, 80_800, true);
    admit_marker(&conn, &m, 80_800, 0x01, 1_001);

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
        conn.prepare(&hops_view_sql(false)).is_err(),
        "missing tables must FAULT the query (the route answers 503), \
         never serve an empty-but-complete-looking page"
    );
}

// ── gate HIGH-1: the flood threshold, measured through the real chain ──────

/// Mint `k` attacker hop outpoints in ONE container: k dust P2PKH outputs
/// (each its own hop outpoint) plus k markers naming `hopVout = 0..k-1`,
/// all naming the VICTIM's identity. Returns the container txid.
fn flood_container(conn: &Connection, victim_seed: u8, k: u32, dust: u64, at: i64) -> String {
    let mut tx = Transaction::new();
    tx.add_input(TransactionInput::new("f1".repeat(32), 0)).unwrap();
    let mut markers = Vec::new();
    for v in 0..k {
        let m = build_marker(victim_seed, GAME, v, dust, false);
        tx.add_output(TransactionOutput {
            satoshis: Some(dust),
            locking_script: LockingScript::from_hex(
                &expected_hop_lock_hex(&m.settle_pub_hex).unwrap(),
            )
            .unwrap(),
            change: false,
        })
        .unwrap();
        markers.push(m);
    }
    for m in &markers {
        tx.add_output(TransactionOutput {
            satoshis: Some(0),
            locking_script: LockingScript::from_binary(&m.script).unwrap(),
            change: false,
        })
        .unwrap();
    }
    let beef = tx.to_beef(true).expect("BEEF");
    let txid = Transaction::from_beef(&beef, None).unwrap().id();
    for v in 0..k {
        admit_container(conn, &beef, k + v, at + v as i64);
        // tm_lowfund admits EVERY P2PKH output of an explicitly-submitted
        // tx, so each dust output becomes a tier-0 pot_records row.
        conn.execute(
            store_record_sql(),
            params![
                &txid, v as i64, 0i64, Option::<String>::None, 0i64, at + v as i64,
                Some("p2pkh"), Option::<String>::None, Option::<String>::None,
                Option::<String>::None, Option::<String>::None, Option::<String>::None,
                Option::<String>::None, Option::<i64>::None, Option::<i64>::None,
                Option::<i64>::None, Option::<i64>::None, Some(dust as i64), 1i64,
            ],
        )
        .expect("store_record_sql");
    }
    txid
}

/// gate HIGH-1 — a DUST flood can no longer evict the honest verified row.
///
/// Pre-fix measurement (recorded so the regression is legible): one
/// transaction with 100 dust P2PKH outputs + 100 markers naming
/// `hopVout = 0..99` — ~3,600 sats all-in — erased an 80,800-sat hop
/// permanently (k=99 honest survived, **k=100 honestPresent=false,
/// verifiedRows=0**). `tm_lowfund` admits every P2PKH output of an
/// explicitly-submitted tx, so all 100 landed in the existence tier and
/// outranked the honest row on pure recency.
///
/// Post-fix: the ranking leads on `paidTier` (the container really paid the
/// claimed value) then `hopSatsOnChain DESC`, so a 1-sat ghost can never
/// outrank an 80,800-sat hop however many of them there are — measured
/// clean at k=400.
#[test]
fn a_dust_flood_cannot_evict_the_honest_verified_row() {
    for k in [100u32, 120, 400] {
        let conn = production_schema_db();
        let honest = build_marker(0xa1, GAME, 0, 80_800, true);
        let honest_txid = admit_marker(&conn, &honest, 80_800, 0x01, 1_000);
        admit_hop(&conn, &honest_txid, 80_800, 1_000);
        flood_container(&conn, 0xa1, k, 1, 5_000);

        let (entries, truncated, _) =
            assemble_hops_view(query_rows(&conn, &honest.identity_hex));
        let honest_row = entries
            .iter()
            .find(|e| e.hop_txid == honest_txid)
            .unwrap_or_else(|| panic!("k={k}: the honest hop must survive a dust flood"));
        assert_eq!(
            honest_row.marker_verified,
            MarkerVerification::Verified,
            "k={k}: and must still verify"
        );
        assert_eq!(
            entries.iter().position(|e| e.hop_txid == honest_txid),
            Some(0),
            "k={k}: verify-then-page puts the only verifiable row FIRST"
        );
        assert!(truncated || k < 100, "k={k}: an over-page flood is flagged");
    }
}

/// gate HIGH-1 — a REACTIVE flood that actually pays the honest hop's value
/// per outpoint (100 × 80,800 = 8.08M sats locked in one transaction) also
/// fails, because ties on value break OLDEST-first and a reactive flood is
/// by definition newer than the hop it targets (#283a: `createdAt` is
/// server-stamped, so an attacker can be newer but never backdate).
#[test]
fn a_value_matched_reactive_flood_cannot_evict_either() {
    for k in [100u32, 120] {
        let conn = production_schema_db();
        let honest = build_marker(0xa1, GAME, 0, 80_800, true);
        let honest_txid = admit_marker(&conn, &honest, 80_800, 0x01, 1_000);
        admit_hop(&conn, &honest_txid, 80_800, 1_000);
        flood_container(&conn, 0xa1, k, 80_800, 5_000);

        let (entries, _, _) = assemble_hops_view(query_rows(&conn, &honest.identity_hex));
        let honest_row = entries
            .iter()
            .find(|e| e.hop_txid == honest_txid)
            .unwrap_or_else(|| panic!("k={k}: the honest hop must survive"));
        assert_eq!(honest_row.marker_verified, MarkerVerification::Verified);
        assert_eq!(entries.iter().position(|e| e.hop_txid == honest_txid), Some(0));
    }
}

/// The DOCUMENTED RESIDUAL, pinned from the UNSAFE side so it cannot drift
/// into a silent regression or a silent fix nobody noticed.
///
/// An attacker who (a) targets a specific identity IN ADVANCE, (b) locks
/// ≥ the honest hop's value per outpoint — 100 × 80,800 = **8,080,000
/// sats** in one transaction — and (c) lands it BEFORE the victim ever
/// funds that hop, still displaces the honest row from the default page.
/// Measured: honestPresent=false, truncated=true.
///
/// Versus the pre-fix attack (~3,600 sats, REACTIVE) that is a ~2,244×
/// capital increase plus a change of kind: predictive rather than
/// reactive, with the capital held for the whole waiting period. It is the
/// same residual class `partyFor` documents for `potparty_records` (#281:
/// discovery cannot bind verified key material, so superset-plus-verify is
/// the fallback), and it has the same two real closures — priced admission,
/// or per-identity auth + quota (#318). `truncated: true` is set honestly
/// throughout, so the caller is never told a crowded page is complete.
#[test]
fn a_predated_value_matched_flood_is_the_documented_residual() {
    let conn = production_schema_db();
    let honest = build_marker(0xa1, GAME, 0, 80_800, true);
    // The flood lands FIRST — the attacker knew the identity in advance.
    flood_container(&conn, 0xa1, 100, 80_800, 10);
    let honest_txid = admit_marker(&conn, &honest, 80_800, 0x01, 5_000);
    admit_hop(&conn, &honest_txid, 80_800, 5_000);

    let (entries, truncated, _) = assemble_hops_view(query_rows(&conn, &honest.identity_hex));
    assert!(
        !entries.iter().any(|e| e.hop_txid == honest_txid),
        "this cell pins the RESIDUAL: if the honest row now survives a \
         pre-dated value-matched flood, the residual is CLOSED and this \
         cell (and the docs quoting it) must be rewritten — do not delete \
         it silently"
    );
    assert!(truncated, "and the caller is told the page is incomplete");
}

/// The `?gameId=` escape hatch reaches a row a flood crowded off the
/// default page — for floods that do NOT name the same game. Measured
/// honestly: a flood naming the victim's own gameId is NOT escaped this
/// way, which is why the residual above stands.
#[test]
fn the_gameid_scope_reaches_a_crowded_out_row() {
    let conn = production_schema_db();
    let honest = build_marker(0xa1, GAME, 0, 80_800, true);
    let honest_txid = admit_marker(&conn, &honest, 80_800, 0x01, 1_000);
    admit_hop(&conn, &honest_txid, 80_800, 1_000);
    // A pre-dated, value-matched flood naming a DIFFERENT game.
    let other_game = [0x77u8; 32];
    let mut tx = Transaction::new();
    tx.add_input(TransactionInput::new("f2".repeat(32), 0)).unwrap();
    let mut markers = Vec::new();
    for v in 0..120u32 {
        let m = build_marker(0xa1, other_game, v, 80_800, false);
        tx.add_output(TransactionOutput {
            satoshis: Some(80_800),
            locking_script: LockingScript::from_hex(
                &expected_hop_lock_hex(&m.settle_pub_hex).unwrap(),
            )
            .unwrap(),
            change: false,
        })
        .unwrap();
        markers.push(m);
    }
    for m in &markers {
        tx.add_output(TransactionOutput {
            satoshis: Some(0),
            locking_script: LockingScript::from_binary(&m.script).unwrap(),
            change: false,
        })
        .unwrap();
    }
    let beef = tx.to_beef(true).unwrap();
    for v in 0..120u32 {
        admit_container(&conn, &beef, 120 + v, 10 + v as i64);
    }

    // The default page is crowded and says so…
    let (_, truncated, _) = assemble_hops_view(query_rows(&conn, &honest.identity_hex));
    assert!(truncated, "the caller is told the page is incomplete");
    // …and scoping to the caller's OWN game reaches the row.
    let (scoped, scoped_truncated, _) = assemble_hops_view(query_rows_scoped(
        &conn,
        &honest.identity_hex,
        &hex::encode(GAME),
    ));
    let row = scoped
        .iter()
        .find(|e| e.hop_txid == honest_txid)
        .expect("the gameId scope must reach the caller's own row");
    assert_eq!(row.marker_verified, MarkerVerification::Verified);
    assert!(!scoped_truncated, "and the scoped page is complete");
}


/// gate MEDIUM-5 — the two windows over `hopparty_records` must not
/// disagree about unknown-hop promotion. The runtime dependency does not
/// exist (low-app-layer does not depend on overlay-cloudflare outside
/// tests), so the PIN carries the agreement across the boundary (Rule 16:
/// share the constant, and where you cannot, pin the equality).
#[test]
fn the_freshness_window_matches_the_overlay_sibling_windows() {
    assert_eq!(
        low_app_layer::hops_view::HOPS_VIEW_UNKNOWN_HOP_MAX_AGE_SECS,
        bsv_overlay_cloudflare::d1_discovery::UNKNOWN_POT_PROMOTION_MAX_AGE_SECS,
        "/hops-view must promote unknown hops on the same #283a freshness \
         window as ls_potparty/ls_potrefund/ls_hopparty"
    );
    // …and the semantics, not just the number: freshness-gated, and slots
    // allocated OLDEST-first (a newest-first quota is attacker-jumpable).
    let sql = hops_view_sql(false);
    assert!(sql.contains("freshUnknown = 1"));
    assert!(sql.contains("ORDER BY COALESCE(firstMarkerAt, 0) ASC"));
}

/// gate MEDIUM-5, behavioural: when the honest hop's `tm_lowfund`
/// admission has NOT landed (so the honest row is itself an unknown), a
/// burst of just-published ghosts must not take every promoted slot.
/// Pre-fix the quota was newest-first and 100 pure ghosts — no dust
/// outputs, no second topic — erased the honest row entirely.
#[test]
fn fresh_ghosts_cannot_take_the_promotion_quota_from_an_older_unknown_hop() {
    for ghosts in [10u32, 11, 20, 100] {
        let conn = production_schema_db();
        // The honest hop: real marker, paid container, but NO tm_lowfund
        // row yet (admission still in flight) ⇒ an unknown hop.
        let honest = build_marker(0xa1, GAME, 0, 80_800, true);
        let honest_txid = admit_marker(&conn, &honest, 80_800, 0x01, 1_000);
        // Pure ghosts: markers only, published later, never indexed.
        for i in 0..ghosts {
            let g = build_marker(0xa1, GAME, 0, 1, false);
            let (beef, _) = container(
                &expected_hop_lock_hex(&g.settle_pub_hex).unwrap(),
                1,
                &g.script,
                (i % 251) as u8 + 1,
            );
            admit_container(&conn, &beef, 1, 5_000 + i as i64);
        }
        let (entries, _, _) = assemble_hops_view(query_rows(&conn, &honest.identity_hex));
        let row = entries
            .iter()
            .find(|e| e.hop_txid == honest_txid)
            .unwrap_or_else(|| panic!("ghosts={ghosts}: the honest unknown hop must survive"));
        assert_eq!(
            row.marker_verified,
            MarkerVerification::Verified,
            "ghosts={ghosts}"
        );
        assert_eq!(
            entries.iter().position(|e| e.hop_txid == honest_txid),
            Some(0),
            "ghosts={ghosts}: paid + oldest + verified leads the page"
        );
    }
}


/// gate LOW — a `hopSats >= 2^63` marker wraps at the D1 bind. Pinned so
/// the SAFE fail direction is a property, not an accident: the wrapped
/// value cannot rebuild the identity challenge, so the row is served as
/// `unverified` junk and can never be laundered into a verified claim.
#[test]
fn an_out_of_range_hop_sats_row_is_served_unverified_never_verified() {
    let conn = production_schema_db();
    let m = build_marker(0xa1, GAME, 0, u64::MAX, true);
    let (beef, _) = container(
        &expected_hop_lock_hex(&m.settle_pub_hex).unwrap(),
        // A container cannot really pay u64::MAX either; pay what it can.
        21_000_000_00_000_000,
        &m.script,
        0x01,
    );
    admit_container(&conn, &beef, 1, 1_000);

    let (entries, _, _) = assemble_hops_view(query_rows(&conn, &m.identity_hex));
    assert_eq!(entries.len(), 1, "the junk row is SERVED, never dropped");
    assert_eq!(
        entries[0].marker_verified,
        MarkerVerification::Unverified,
        "an out-of-range hopSats can never become verified"
    );
}


/// `paidTier`'s ACTUAL job (see the ordering doc): a row whose container
/// has no output at `hopVout` is demoted deterministically, rather than
/// relying on SQLite's NULLs-sort-last-under-DESC convention. Pinned
/// because the injection round showed the tier does NOT carry the flood
/// defence, so its real contribution has to be stated and tested or
/// deleted.
#[test]
fn a_refuted_row_sorts_below_a_paid_one_regardless_of_null_ordering() {
    let conn = production_schema_db();
    // A row whose container LACKS the named output (hopSatsOnChain NULL).
    let refuted = build_marker(0xa1, GAME, 7, 900_000, true);
    let (beef, _) = container(
        &expected_hop_lock_hex(&refuted.settle_pub_hex).unwrap(),
        900_000,
        &refuted.script,
        0x02,
    );
    let refuted_txid = admit_container(&conn, &beef, 1, 10);
    // The honest, paid, LOWER-value row.
    let honest = build_marker(0xa1, GAME, 0, 80_800, true);
    let honest_txid = admit_marker(&conn, &honest, 80_800, 0x01, 1_000);

    let rows = query_rows(&conn, &honest.identity_hex);
    let pos = |t: &str| rows.iter().position(|r| r.hop_txid == t).unwrap();
    assert!(
        pos(&honest_txid) < pos(&refuted_txid),
        "a paid row must outrank one whose container lacks the output, \
         even though the refuted row claims the larger value"
    );
}

/// The case where the VALUE key does the work (found by injection: with
/// the value key neutered the reactive floods were still repelled by the
/// oldest-first tie-break alone, so the two keys defend DIFFERENT attacks
/// and each needs its own cell).
///
/// A PRE-DATED flood beats the tie-break by construction — it is older.
/// What stops it is that it did not pay: `hopSatsOnChain DESC` ranks the
/// honest 80,800-sat hop above any number of pre-dated 1-sat ghosts.
#[test]
fn a_predated_dust_flood_is_repelled_by_the_value_key() {
    for k in [100u32, 120, 400] {
        let conn = production_schema_db();
        // The flood lands FIRST and is therefore older than the honest hop.
        flood_container(&conn, 0xa1, k, 1, 10);
        let honest = build_marker(0xa1, GAME, 0, 80_800, true);
        let honest_txid = admit_marker(&conn, &honest, 80_800, 0x01, 5_000);
        admit_hop(&conn, &honest_txid, 80_800, 5_000);

        let (entries, _, _) = assemble_hops_view(query_rows(&conn, &honest.identity_hex));
        let row = entries
            .iter()
            .find(|e| e.hop_txid == honest_txid)
            .unwrap_or_else(|| panic!("k={k}: a pre-dated DUST flood must not evict"));
        assert_eq!(row.marker_verified, MarkerVerification::Verified, "k={k}");
        assert_eq!(
            entries.iter().position(|e| e.hop_txid == honest_txid),
            Some(0),
            "k={k}: the paid hop outranks unpaid ghosts however old they are"
        );
    }
}
