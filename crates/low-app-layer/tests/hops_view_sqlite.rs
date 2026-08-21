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
//!  - the lookup service's record is written through the REAL admission
//!    WRITER, `hopparty_insert_query` — the exact value production's
//!    `store_record` executes, so the `markerValid` latch these cells read
//!    back is the one production computes (bsv-low #362), never a
//!    transcription of it;
//!  - hop outpoints are admitted via the shipped `store_record_sql()`
//!    (`tm_lowfund` → `pot_records`) and spends via `mark_spent_sql()`.
//!
//! Cells: the verified positive control, the junk-sig / F1 / mismatched-
//! lock refusal labels (served, never dropped, never verified), the
//! unknown-first-class statuses, the superset-vs-forged-eviction window in
//! BOTH orderings, the honest truncation bit, and the route's fail-safe /
//! 503 cores.

use bsv_overlay_cloudflare::d1::QVal;
use bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS;
use bsv_overlay_cloudflare::d1_discovery::{
    hopparty_insert_query, mark_spent_sql, store_record_sql,
};
use bsv_rs::script::LockingScript;
use bsv_rs::transaction::{Transaction, TransactionInput, TransactionOutput};
use bsv_rs::wallet::{
    Counterparty, CreateSignatureArgs, GetPublicKeyArgs, ProtoWallet, Protocol, SecurityLevel,
};
use low_app_layer::hops_view::{
    assemble_hops_view, expected_hop_lock_hex, hops_view_body, hops_view_sql, HopStatus,
    HopsViewRow, MarkerVerification, HOPS_VIEW_MAX_OUTPOINTS,
};
use low_app_layer::logic::valid_identity;
use overlay_discovery::hopparty::storage::HoppartyRecord;
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
/// through the REAL writer `hopparty_insert_query` (which is where the
/// #362 `markerValid` latch is decided). Returns the container txid.
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
    let insert = hopparty_insert_query(
        &HoppartyRecord {
            identity: hex::encode(&m.identity),
            opponent_identity: hex::encode(&m.opponent),
            game_id: hex::encode(m.game_id),
            hop_vout: m.hop_vout,
            hop_sats: m.hop_sats,
            seat_settle_pubkey: hex::encode(&m.seat_settle_pubkey),
            seat_sig_hex: hex::encode(&m.seat_sig),
            identity_sig_hex: hex::encode(&m.identity_sig),
            hop_lock_hex,
            hop_sats_on_chain,
            container_outputs,
            txid: txid.clone(),
            output_index: marker_vout,
            created_at: 0, // ignored by the writer — the stamp below wins
        },
        at,
    );
    exec_query(conn, insert.query());
    txid
}

/// Replay a production-built [`bsv_overlay_cloudflare::d1::Query`] against
/// real SQLite: its OWN sql, its OWN bind list, in order.
fn exec_query(conn: &Connection, q: &bsv_overlay_cloudflare::d1::Query) {
    let vals: Vec<rusqlite::types::Value> = q
        .params()
        .iter()
        .map(|p| match p {
            QVal::Null => rusqlite::types::Value::Null,
            QVal::Int(i) => rusqlite::types::Value::Integer(*i),
            QVal::Text(s) => rusqlite::types::Value::Text(s.clone()),
            QVal::Bool(b) => rusqlite::types::Value::Integer(i64::from(*b)),
            QVal::Blob(b) => rusqlite::types::Value::Blob(b.clone()),
            QVal::Float(f) => rusqlite::types::Value::Real(*f),
        })
        .collect();
    conn.execute(q.sql(), rusqlite::params_from_iter(vals.iter()))
        .expect("the production query must execute against the production schema");
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
/// have none). The #371 finality CASE binds ride every variant
/// (probe, value, value); this legacy helper writes NULL finality — the
/// pre-migration shape.
fn mark_hop_spent(conn: &Connection, hop_txid: &str, spender: &str, confirmed: bool) {
    let sql = mark_spent_sql(confirmed, false);
    if confirmed {
        conn.execute(
            sql,
            params![
                spender,
                spender,
                Option::<i64>::None,
                Option::<i64>::None,
                spender,
                Option::<i64>::None,
                Option::<i64>::None,
                hop_txid,
                0i64
            ],
        )
    } else {
        conn.execute(
            sql,
            params![
                spender,
                spender,
                Option::<i64>::None,
                Option::<i64>::None,
                hop_txid,
                0i64
            ],
        )
    }
    .expect("mark_spent_sql");
}

/// Execute the SHIPPED `hops_view_sql(false)` and map rows exactly as the route
/// does (same columns, same Option-ness).
fn query_rows(conn: &Connection, identity: &str) -> Vec<HopsViewRow> {
    query_rows_inner(conn, hops_view_sql(false, None, 0), params![identity])
}

/// The `?gameId=`-scoped window — the escape hatch a truncated caller uses.
fn query_rows_scoped(conn: &Connection, identity: &str, game_id: &str) -> Vec<HopsViewRow> {
    query_rows_inner(
        conn,
        hops_view_sql(true, None, 0),
        params![identity, game_id],
    )
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
            // #371 — the third-arm inputs, read through the REAL SQL.
            spender_seen: r.get::<_, Option<i64>>("spenderSeen")?.map(|v| v != 0),
            spender_final: r.get::<_, Option<i64>>("spenderFinal")?.map(|v| v != 0),
            // The route applies the same empty-is-absent belt.
            hop_lock_hex: r
                .get::<_, Option<String>>("hopLockHex")?
                .filter(|s| !s.is_empty()),
            hop_sats_on_chain: r.get::<_, Option<i64>>("hopSatsOnChain")?.map(|v| v as u64),
            container_outputs: r.get::<_, i64>("containerOutputs")? as u32,
            // The #362 latch, exactly as the route maps it: NULL stays NULL.
            marker_valid: r.get::<_, Option<i64>>("markerValid")?.map(|v| v != 0),
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
    assert_eq!(
        rows[0].hop_txid, txid,
        "the container supplies the hop txid"
    );
    assert_eq!(rows[0].marker_txid, txid, "…and it IS the marker's tx");
    assert_eq!(rows[0].marker_vout, 1);
    assert_eq!(
        rows[0].hop_sats, 80_800,
        "8-byte LE sats round-trip the schema"
    );
    assert_eq!(rows[0].hop_sats_on_chain, Some(80_800), "the chain agrees");
    assert_eq!(rows[0].container_outputs, 2);
    assert_eq!(
        rows[0].hop_lock_hex.as_deref().map(str::to_ascii_lowercase),
        Some(expected_hop_lock_hex(&m.settle_pub_hex).unwrap()),
        "the container's own output at hopVout, decoded at admission"
    );
    let (entries, truncated) = assemble_hops_view(rows);
    assert!(!truncated);
    let e = &entries[0];
    assert_eq!(e.status, HopStatus::Unspent);
    assert_eq!(e.status_source, Some("chain"));
    assert_eq!(
        e.marker_verified,
        MarkerVerification::Verified,
        "positive control: real sigs + a container that really pays the claimed \
         value to the claimed key clears every bar"
    );
    assert_eq!(
        e.game_id,
        hex::encode(GAME),
        "the /live-view join key rides each entry"
    );
}

// ── refusal labels: served, labeled, never dropped, never verified ─────────

#[test]
fn junk_sig_marker_is_served_labeled_unverified_never_verified() {
    let conn = production_schema_db();
    let m = build_marker(0xa1, GAME, 0, 80_800, false); // garbage sigs
    let txid = admit_marker(&conn, &m, 80_800, 0x01, 1_001);
    admit_hop(&conn, &txid, 80_800, 1_000);

    let (entries, _) = assemble_hops_view(query_rows(&conn, &m.identity_hex));
    assert_eq!(entries.len(), 1, "the failing row is SERVED, never dropped");
    assert_eq!(
        entries[0].marker_verified,
        MarkerVerification::Unverified,
        "junk sigs NEVER become markerVerified: verified — even in a perfect container"
    );
    let body = hops_view_body(&m.identity_hex, None, &entries, false, 0);
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
    let (entries, _) = assemble_hops_view(rows);
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

    let (entries, _) = assemble_hops_view(query_rows(&conn, &m.identity_hex));
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
    let (entries, _) = assemble_hops_view(rows);
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
    assert_eq!(
        rows.len(),
        1,
        "the row lands in the VICTIM's view (admission cannot stop it)"
    );
    assert_eq!(
        rows[0].hop_lock_hex.as_deref().map(str::to_ascii_lowercase),
        Some(expected_hop_lock_hex(&attacker_settle_hex).unwrap()),
        "the container bars would pass on their own"
    );
    let (entries, _) = assemble_hops_view(rows);
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
    assert_ne!(
        paid_txid, honest_txid,
        "a distinct container ⇒ a distinct outpoint"
    );
    // A cheaper replay: same bytes, 1 sat.
    let (beef, _) = container(
        &expected_hop_lock_hex(&m.settle_pub_hex).unwrap(),
        1,
        &m.script,
        0x03,
    );
    admit_container(&conn, &beef, 1, 2_100);

    let (entries, _) = assemble_hops_view(query_rows(&conn, &m.identity_hex));
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
    assert_eq!(
        rows.len(),
        1,
        "an unindexed hop is still SERVED (promoted quota)"
    );
    assert_eq!(rows[0].spent, None, "never asserted unspent");
    let (entries, _) = assemble_hops_view(rows);
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

    let (entries, _) = assemble_hops_view(query_rows(&conn, &ma.identity_hex));
    assert_eq!(entries.len(), 2);
    let a = entries.iter().find(|e| e.hop_txid == a_txid).unwrap();
    assert_eq!(
        (a.status, a.status_source),
        (HopStatus::Spent, Some("chain"))
    );
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
        tx.add_input(TransactionInput::new("aa".repeat(32), 0))
            .unwrap();
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
        assert_eq!(
            rows.len(),
            3,
            "the SUPERSET serves all three (order {honest_first})"
        );
        let (entries, _) = assemble_hops_view(rows);
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

    let (entries, truncated) = assemble_hops_view(query_rows(&conn, &identity_hex));
    assert!(
        truncated,
        "more outpoints than the page ⇒ honestly incomplete"
    );
    let distinct: std::collections::HashSet<&String> =
        entries.iter().map(|e| &e.hop_txid).collect();
    assert_eq!(
        distinct.len(),
        HOPS_VIEW_MAX_OUTPOINTS,
        "page bound in outpoints"
    );

    // Drop the surplus hops → exactly the page → complete.
    conn.execute(
        "DELETE FROM hopparty_records WHERE txid IN \
         (SELECT txid FROM hopparty_records ORDER BY createdAt DESC LIMIT ?1)",
        params![distinct_hops - HOPS_VIEW_MAX_OUTPOINTS as i64],
    )
    .unwrap();
    let (entries, truncated) = assemble_hops_view(query_rows(&conn, &identity_hex));
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
    let (entries, truncated) = assemble_hops_view(rows);
    let v: serde_json::Value =
        serde_json::from_str(&hops_view_body(&stranger, None, &entries, truncated, 0)).unwrap();
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
        conn.prepare(&hops_view_sql(false, None, 0)).is_err(),
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
    tx.add_input(TransactionInput::new("f1".repeat(32), 0))
        .unwrap();
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
                &txid,
                v as i64,
                0i64,
                Option::<String>::None,
                0i64,
                at + v as i64,
                Some("p2pkh"),
                Option::<String>::None,
                Option::<String>::None,
                Option::<String>::None,
                Option::<String>::None,
                Option::<String>::None,
                Option::<String>::None,
                Option::<i64>::None,
                Option::<i64>::None,
                Option::<i64>::None,
                Option::<i64>::None,
                Some(dust as i64),
                1i64,
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

        let (entries, truncated) = assemble_hops_view(query_rows(&conn, &honest.identity_hex));
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

        let (entries, _) = assemble_hops_view(query_rows(&conn, &honest.identity_hex));
        let honest_row = entries
            .iter()
            .find(|e| e.hop_txid == honest_txid)
            .unwrap_or_else(|| panic!("k={k}: the honest hop must survive"));
        assert_eq!(honest_row.marker_verified, MarkerVerification::Verified);
        assert_eq!(
            entries.iter().position(|e| e.hop_txid == honest_txid),
            Some(0)
        );
    }
}

/// bsv-low #362 — THE REPRICING, MEASURED. Was: the documented residual.
///
/// **REACTIVE eviction at hop value + 1.** Before the latch, verification
/// happened AFTER paging, so the honest row had to survive on chain-fact
/// keys alone — and one satoshi above the hop value skipped the
/// oldest-first tie-break and evicted it reactively. Measured then at
/// exactly k=100 (k=99 present, k=100 ABSENT, `truncated: true`), for
/// ~3.5k sats burned.
///
/// Now the leading key is the latched verdict, which those markers cannot
/// reach: they carry byte-shape-valid GARBAGE signatures, so every one of
/// them latches `markerValid = 0` at admission and sorts below the honest
/// row however richly funded it is. Measured at k = 100, 120 and 400.
///
/// The rows are STILL SERVED and labelled — this is a sort key, never a
/// `WHERE`.
#[test]
fn a_value_plus_one_reactive_flood_no_longer_evicts_the_honest_row() {
    for k in [100u32, 120, 400] {
        let conn = production_schema_db();
        let honest = build_marker(0xa1, GAME, 0, 80_800, true);
        let honest_txid = admit_marker(&conn, &honest, 80_800, 0x01, 1_000);
        admit_hop(&conn, &honest_txid, 80_800, 1_000);
        // REACTIVE: strictly later than the honest hop. Paid at value + 1.
        flood_container(&conn, 0xa1, k, 80_801, 5_000);

        let (entries, _) = assemble_hops_view(query_rows(&conn, &honest.identity_hex));
        assert_eq!(
            entries.iter().position(|e| e.hop_txid == honest_txid),
            Some(0),
            "k={k}: the honest row leads the page — outranking a VERIFIED \
             row now requires a verified row, and paying more does not \
             produce one"
        );
        assert_eq!(
            entries[0].marker_verified,
            MarkerVerification::Verified,
            "k={k}"
        );
        // Serve-and-label: the flood is NOT hidden, just outranked.
        assert!(
            entries
                .iter()
                .skip(1)
                .all(|e| e.marker_verified == MarkerVerification::Unverified),
            "k={k}: every refuted row that fits on the page is SERVED and \
             labelled — a WHERE here would be the invisible-money class"
        );
    }
}

/// bsv-low #362 — the second half of the repricing, measured.
///
/// The chained-single-coin shape was the cheap one: `hopSatsOnChain` is
/// decoded once at admission and never re-read, so an attacker chained ONE
/// coin through k transactions, each spending the previous output into the
/// next minus fee, satisfying the value key k times at ~86,000 sats of peak
/// — ~1.06x the victim's hop — and FULLY RECOVERABLE.
///
/// It no longer evicts, and the reason is structural rather than a price:
/// the chain works because the attacker pays THEMSELVES and can spend the
/// output again. To reach the verified tier they would have to pay the
/// VICTIM'S settle key, which they cannot re-spend, so the coin cannot be
/// chained at all. Cheap-and-recoverable and reaches-the-top-tier are now
/// mutually exclusive.
#[test]
fn a_chained_single_coin_flood_no_longer_evicts_the_honest_row() {
    let conn = production_schema_db();
    let honest = build_marker(0xa1, GAME, 0, 80_800, true);
    let honest_txid = admit_marker(&conn, &honest, 80_800, 0x01, 1_000);
    admit_hop(&conn, &honest_txid, 80_800, 1_000);

    let peak = 86_000u64;
    for i in 0..100u32 {
        let value = peak - 50 * i as u64; // fee decay along the chain
        assert!(value > 80_800, "every link must still outrank on value");
        let m = build_marker(0xa1, GAME, 0, value, false);
        let (beef, _) = container(
            &expected_hop_lock_hex(&m.settle_pub_hex).unwrap(),
            value,
            &m.script,
            (i % 251) as u8 + 3,
        );
        let txid = admit_container(&conn, &beef, 1, 5_000 + i as i64);
        // Submitted under tm_lowfund too — free, and what buys the
        // existence tier. (Omitting this made an earlier pass of this
        // measurement read falsely clean.)
        admit_hop(&conn, &txid, value as i64, 5_000 + i as i64);
    }

    let (entries, _) = assemble_hops_view(query_rows(&conn, &honest.identity_hex));
    assert_eq!(
        entries.iter().position(|e| e.hop_txid == honest_txid),
        Some(0),
        "the chained flood outranks on VALUE and loses on VERDICT"
    );
    assert_eq!(entries[0].marker_verified, MarkerVerification::Verified);
}

/// THE REMAINING RESIDUAL, pinned from the UNSAFE side (bsv-low #362), and
/// it is NARROWER than expected — which is why it is measured in both
/// directions here rather than asserted in one.
///
/// A marker's bytes are portable (no digest can bind the containing txid),
/// so an attacker can REPLAY the victim's marker verbatim into containers of
/// their own. Those replays DO latch `markerValid = 1` and reach the top
/// tier. What they cannot do is outrank on the keys below it:
///
///  - **value**: the latch requires the container to pay EXACTLY `hopSats`,
///    so a replay can only ever TIE on `hopSatsOnChain DESC` — paying more
///    would refute it. The value key is unavailable to it by construction.
///  - **age**: the tie therefore falls to
///    `COALESCE(potCreatedAt, firstMarkerAt) ASC`, oldest-first, and a
///    replay is by definition REACTIVE — the attacker must see the marker
///    before they can copy it.
///
/// So the natural shape does NOT evict, at any k (leg 1, measured at 120).
/// What remains is a SUBMIT RACE: the attacker watches the hop transaction
/// in the mempool and gets their replay containers ADMITTED to the overlay
/// before the victim's own marker is submitted, so the server-stamped
/// `createdAt` ordering favours them (leg 2). That is real, and it costs
/// ~`hopSats` per outpoint PAID TO THE VICTIM'S OWN SETTLE KEY — a gift the
/// victim can spend and the attacker cannot, which is also why it cannot be
/// chained. Compare the pre-#362 price for the same effect: ~5.2k sats
/// burned and ~86k of RECOVERABLE peak capital.
///
/// If leg 2 ever fails, the residual is CLOSED and the pricing in
/// `hops_view::assemble_hops_view` must be rewritten — do not delete it
/// silently (epoch Rule 10).
#[test]
fn a_paid_replay_flood_is_the_remaining_residual() {
    /// `k` replays of the victim's own marker bytes, each in the attacker's
    /// own container that really pays the victim's settle key `hopSats`.
    /// `at` controls the server-stamped admission time of every replay.
    fn replay_flood(conn: &Connection, honest: &BuiltMarker, k: u32, at: i64) {
        for i in 0..k {
            let (beef, _) = container(
                &expected_hop_lock_hex(&honest.settle_pub_hex).unwrap(),
                80_800,
                &honest.script,
                (i % 251) as u8 + 3,
            );
            let txid = admit_container(conn, &beef, 1, at + i as i64);
            admit_hop(conn, &txid, 80_800, at + i as i64);
        }
    }

    // ── LEG 1: the natural, REACTIVE replay. It verifies and it LOSES.
    let conn = production_schema_db();
    let honest = build_marker(0xa1, GAME, 0, 80_800, true);
    let honest_txid = admit_marker(&conn, &honest, 80_800, 0x01, 1_000);
    admit_hop(&conn, &honest_txid, 80_800, 1_000);
    replay_flood(&conn, &honest, 120, 5_000);

    let (entries, truncated) = assemble_hops_view(query_rows(&conn, &honest.identity_hex));
    assert!(
        entries
            .iter()
            .all(|e| e.marker_verified == MarkerVerification::Verified),
        "positive control: a replay that really pays the victim DOES verify \
         — this leg is not passing because the replays were refused"
    );
    assert_eq!(
        entries.iter().position(|e| e.hop_txid == honest_txid),
        Some(0),
        "a REACTIVE paid replay can only TIE on value (paying more refutes \
         it), so it loses the oldest-first tie-break"
    );
    assert!(
        truncated,
        "the caller is always told the page is incomplete"
    );

    // ── LEG 2: the SUBMIT RACE — the replays are admitted FIRST. This is
    // the residual, and it is pinned from the unsafe side.
    let conn = production_schema_db();
    let honest_txid = admit_marker(&conn, &honest, 80_800, 0x01, 1_000);
    replay_flood(&conn, &honest, 120, 100);
    admit_hop(&conn, &honest_txid, 80_800, 1_000);

    let (entries, truncated) = assemble_hops_view(query_rows(&conn, &honest.identity_hex));
    assert!(
        !entries.iter().any(|e| e.hop_txid == honest_txid),
        "the RESIDUAL: replays admitted ahead of the victim's own marker \
         crowd it off the page — if this ever stops being true the residual \
         is closed and the docs must change"
    );
    assert!(truncated, "and the caller is told the page is incomplete");

    // The `?gameId=` scope is NOT an escape from it (the replays carry the
    // victim's own gameId — it is one of the marker's nine pushes).
    let (scoped, _) = assemble_hops_view(query_rows_scoped(
        &conn,
        &honest.identity_hex,
        &hex::encode(GAME),
    ));
    assert!(
        !scoped.iter().any(|e| e.hop_txid == honest_txid),
        "the gameId scope is NOT an escape from a same-game paid replay — \
         the closure direction is #318's per-identity auth + quota"
    );
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
    tx.add_input(TransactionInput::new("f2".repeat(32), 0))
        .unwrap();
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
    let (_, truncated) = assemble_hops_view(query_rows(&conn, &honest.identity_hex));
    assert!(truncated, "the caller is told the page is incomplete");
    // …and scoping to the caller's OWN game reaches the row.
    let (scoped, scoped_truncated) = assemble_hops_view(query_rows_scoped(
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

/// …and the OTHER half of that measurement. `routes.rs` used to claim the
/// gameId scope "cannot hide a row the caller asks for by game"; it could,
/// because the gameId is one of the nine pushes in the victim's own on-chain
/// marker, so naming it is free and a SAME-gameId flood defeated the scope
/// exactly as it defeated the default page.
///
/// Since #362 an UNVERIFIABLE same-game flood defeats neither — the leading
/// key is the latched verdict, and garbage signatures cannot reach it. The
/// scope is therefore no longer load-bearing against this shape.
///
/// The shape it still does NOT escape is the PAID REPLAY, measured in
/// `a_paid_replay_flood_is_the_remaining_residual` — which is where the
/// honest statement about the escape hatch now lives.
#[test]
fn a_same_game_flood_no_longer_crowds_out_the_scoped_row() {
    let conn = production_schema_db();
    let honest = build_marker(0xa1, GAME, 0, 80_800, true);
    let honest_txid = admit_marker(&conn, &honest, 80_800, 0x01, 1_000);
    admit_hop(&conn, &honest_txid, 80_800, 1_000);
    // The flood names the victim's OWN gameId, reactively, at value + 1.
    flood_container(&conn, 0xa1, 100, 80_801, 5_000);

    for (label, rows) in [
        ("default", query_rows(&conn, &honest.identity_hex)),
        (
            "scoped",
            query_rows_scoped(&conn, &honest.identity_hex, &hex::encode(GAME)),
        ),
    ] {
        let (entries, _) = assemble_hops_view(rows);
        assert_eq!(
            entries.iter().position(|e| e.hop_txid == honest_txid),
            Some(0),
            "{label}: the honest row leads — the verdict outranks the flood \
             in BOTH windows, so the scope is no longer load-bearing here"
        );
        assert_eq!(entries[0].marker_verified, MarkerVerification::Verified);
    }
}

// ── bsv-low #362: the LEGACY tier (rows admitted before the latch) ─────────

/// Simulate a PRE-MIGRATION row: admitted through the real chain, then its
/// `markerValid` set back to NULL. This is the only honest way to produce
/// one — the production writer always latches — and it is what every row in
/// the table looks like on deploy day.
fn unlatch(conn: &Connection, txid: &str) {
    let n = conn
        .execute(
            "UPDATE hopparty_records SET markerValid = NULL WHERE txid = ?1",
            params![txid],
        )
        .expect("unlatch");
    assert_eq!(n, 1, "the row must exist to be un-latched");
}

/// A legacy row is SERVED, labelled `unknown`, and ordered BETWEEN verified
/// and refuted — never hidden, never silently relabelled.
///
/// Three tiers rather than two is load-bearing: if `NULL` were treated as
/// valid, a legacy junk row would tie with a latched honest row at the top
/// and the tie-break would hand the slot back to whoever stamped earlier.
#[test]
fn a_legacy_row_is_served_labelled_unknown_and_ranks_between_the_two_verdicts() {
    let conn = production_schema_db();

    // One verified, one refuted, one legacy — three distinct hop outpoints.
    let good = build_marker(0xa1, GAME, 0, 80_800, true);
    let good_txid = admit_marker(&conn, &good, 80_800, 0x01, 1_000);
    admit_hop(&conn, &good_txid, 80_800, 1_000);

    let junk = build_marker(0xa1, GAME, 0, 80_800, false);
    let junk_txid = admit_marker(&conn, &junk, 80_800, 0x02, 1_001);
    admit_hop(&conn, &junk_txid, 80_800, 1_001);

    let legacy = build_marker(0xa1, GAME, 0, 80_800, true);
    let legacy_txid = admit_marker(&conn, &legacy, 80_800, 0x03, 999);
    admit_hop(&conn, &legacy_txid, 80_800, 999);
    unlatch(&conn, &legacy_txid);

    let (entries, _) = assemble_hops_view(query_rows(&conn, &good.identity_hex));
    assert_eq!(entries.len(), 3, "every tier is SERVED — never a WHERE");
    let labelled: Vec<(&str, MarkerVerification)> = entries
        .iter()
        .map(|e| {
            let which = if e.hop_txid == good_txid {
                "verified"
            } else if e.hop_txid == junk_txid {
                "refuted"
            } else {
                "legacy"
            };
            (which, e.marker_verified)
        })
        .collect();
    assert_eq!(
        labelled,
        vec![
            ("verified", MarkerVerification::Verified),
            ("legacy", MarkerVerification::Unknown),
            ("refuted", MarkerVerification::Unverified),
        ],
        "rank 2 > rank 1 (NULL) > rank 0 — and the legacy row is the OLDEST \
         of the three, so it would lead on every other key: the verdict is \
         genuinely the leading one"
    );

    // …and the wire says exactly that.
    let v: serde_json::Value = serde_json::from_str(&hops_view_body(
        &good.identity_hex,
        None,
        &entries,
        false,
        0,
    ))
    .unwrap();
    assert_eq!(v["hops"][1]["markerVerified"], serde_json::json!("unknown"));
    assert_eq!(v["hops"][1]["hopTxid"], serde_json::json!(legacy_txid));
    assert_eq!(v["verifyBudgetExhausted"], serde_json::json!(false));
}

/// DEPLOY DAY, measured: when EVERY row is legacy the ordering is exactly
/// what it was before the migration — the rank is constant, so every key
/// below it decides, unchanged. The latch cannot make a pre-existing page
/// worse; it can only fail to make it better (which is what the #367
/// re-latch sweep, `bsv_overlay_cloudflare::relatch`, retires row by row).
#[test]
fn an_all_legacy_table_orders_exactly_as_it_did_before_the_latch() {
    let conn = production_schema_db();
    let honest = build_marker(0xa1, GAME, 0, 80_800, true);
    let honest_txid = admit_marker(&conn, &honest, 80_800, 0x01, 1_000);
    admit_hop(&conn, &honest_txid, 80_800, 1_000);
    let flood_txid = flood_container(&conn, 0xa1, 100, 80_801, 5_000);

    // Every row in the table predates the latch.
    conn.execute("UPDATE hopparty_records SET markerValid = NULL", [])
        .unwrap();

    let (entries, truncated) = assemble_hops_view(query_rows(&conn, &honest.identity_hex));
    assert!(
        !entries.iter().any(|e| e.hop_txid == honest_txid),
        "PRE-#362 BEHAVIOUR, unchanged: a value+1 reactive flood evicts when \
         nothing is latched. This is the state the #367 re-latch sweep \
         exists to retire, and NOTHING ELSE can — no republish can re-latch \
         a marker whose transaction is already on chain. The sweep is bounded \
         per tick, so this remains the behaviour for any row it has not \
         reached yet."
    );
    assert!(truncated);
    assert!(
        entries
            .iter()
            .all(|e| e.marker_verified == MarkerVerification::Unknown),
        "and every served row says so: unknown, not a fabricated verdict"
    );
    assert!(entries.iter().all(|e| e.hop_txid == flood_txid));
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
    let sql = hops_view_sql(false, None, 0);
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
        let (entries, _) = assemble_hops_view(query_rows(&conn, &honest.identity_hex));
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
        // A container cannot really pay u64::MAX either; pay the whole
        // 21M-BTC supply in sats.
        2_100_000_000_000_000,
        &m.script,
        0x01,
    );
    admit_container(&conn, &beef, 1, 1_000);

    let (entries, _) = assemble_hops_view(query_rows(&conn, &m.identity_hex));
    assert_eq!(entries.len(), 1, "the junk row is SERVED, never dropped");
    assert_eq!(
        entries[0].marker_verified,
        MarkerVerification::Unverified,
        "an out-of-range hopSats can never become verified"
    );
}

/// `paidTier`'s ACTUAL job — and this cell had to be RE-FIXTURED, because
/// the first version of it was blind to the key it names (delta gate
/// NEW-MEDIUM-C: the fix for a decorative pin produced a second decorative
/// pin).
///
/// The first fixture used a GHOST refuted row (absent from `pot_records`),
/// so the EXISTENCE tier demoted it whether or not `paidTier` worked —
/// neutering `paidTier` left the cell green. The shape that actually needs
/// `paidTier` is an **INDEXED** row whose container output at `hopVout` is
/// the attacker's own large change output while the marker claims a
/// different (tiny) `hopSats`: `hopSatsOnChain DESC` would otherwise
/// promote it on a value the attacker never committed to the claim, for
/// free.
#[test]
fn an_indexed_refuted_row_sorts_below_a_paid_one() {
    let conn = production_schema_db();
    // The attacker: a container whose output 0 is a large change output,
    // with a marker claiming a 1-sat hop at that vout. hopSatsOnChain is
    // large; the CLAIM does not match it ⇒ paidTier = 1.
    let attacker = build_marker(0xa1, GAME, 0, 1, false);
    let (beef, attacker_txid) = container(
        &expected_hop_lock_hex(&attacker.settle_pub_hex).unwrap(),
        9_000_000, // the attacker's own change, not a payment for the claim
        &attacker.script,
        0x02,
    );
    admit_container(&conn, &beef, 1, 500);
    // INDEXED under tm_lowfund — so the existence tier canNOT be what
    // demotes it. This is the sensitivity the first fixture lacked.
    admit_hop(&conn, &attacker_txid, 9_000_000, 500);

    // The honest, paid, far LOWER-value row.
    let honest = build_marker(0xa1, GAME, 0, 80_800, true);
    let honest_txid = admit_marker(&conn, &honest, 80_800, 0x01, 1_000);
    admit_hop(&conn, &honest_txid, 80_800, 1_000);

    let rows = query_rows(&conn, &honest.identity_hex);
    let pos = |t: &str| rows.iter().position(|r| r.hop_txid == t).unwrap();
    assert!(
        pos(&honest_txid) < pos(&attacker_txid),
        "a PAID row must outrank an INDEXED row whose big on-chain value was \
         never committed to its claim — this is paidTier's whole job, and \
         the cell must go red when paidTier is neutered"
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

        let (entries, _) = assemble_hops_view(query_rows(&conn, &honest.identity_hex));
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

// ── #375 — the pre-launch era write-off ─────────────────────────────────────

/// File a hop marker row DIRECTLY (the topic manager's column shape, stub
/// sigs). MODELLING BOUNDARY (epoch Rule 17): the subject here is the era
/// WINDOW arithmetic, which reads only the server-stamped `createdAt`
/// columns — marker validity is orthogonal and pinned by this file's
/// real-producer cells above.
fn file_hop_marker(conn: &Connection, identity: &str, hop_txid: &str, marker_txid: &str, at: i64) {
    conn.execute(
        "INSERT OR IGNORE INTO hopparty_records \
         (identity, opponentIdentity, gameId, hopVout, hopSats, \
          seatSettlePubkey, seatSigHex, identitySigHex, containerOutputs, \
          txid, outputIndex, createdAt) \
         VALUES (?1, ?2, ?3, 0, 900, ?4, '3045ab', '3045cd', 2, ?5, 1, ?6)",
        params![
            identity,
            h66(0xbb),
            hex::encode(GAME),
            h66(0x0a),
            marker_txid,
            at
        ],
    )
    .expect("insert hopparty_records");
    // The window joins pot_records on (hp.txid, hp.hopVout) — hp.txid IS
    // the container/hop txid in production; this stub writes marker rows
    // whose txid is the hop txid, matching that join.
    assert_eq!(marker_txid, hop_txid, "era cells key the marker by its hop");
}

/// The hop-txid page served by the SHIPPED SQL under an era cutoff, both
/// arities (the cutoff is `?2` unscoped / `?3` scoped — always LAST).
fn hops_txids(
    conn: &Connection,
    identity: &str,
    scoped: Option<&str>,
    era: Option<i64>,
) -> Vec<String> {
    let sql = hops_view_sql(scoped.is_some(), era, 0);
    let mut stmt = conn.prepare(&sql).unwrap_or_else(|e| {
        panic!(
            "hops_view_sql({:?}, {era:?}) did not PREPARE: {e}\n{sql}",
            scoped.is_some()
        )
    });
    let map = |r: &rusqlite::Row| r.get::<_, String>("hopTxid");
    let rows = match (scoped, era) {
        (None, None) => stmt.query_map(params![identity], map),
        (None, Some(ms)) => stmt.query_map(params![identity, ms], map),
        (Some(g), None) => stmt.query_map(params![identity, g], map),
        (Some(g), Some(ms)) => stmt.query_map(params![identity, g, ms], map),
    }
    .expect("query");
    rows.collect::<Result<Vec<_>, _>>().expect("rows")
}

/// #375 through the SHIPPED `/hops-view` SQL, BOTH arities: an indexed hop
/// container admitted one second before the cutoff is DROPPED (its
/// post-cutoff marker cannot resurrect it), the at-cutoff hop is KEPT (the
/// `>=` boundary + the seconds→ms unit pin — `hopparty_records.createdAt`
/// is INTEGER unix seconds like every marker table here), an un-indexed hop
/// anchors on its marker stamp, and `None` serves the full page.
#[test]
fn the_written_off_era_is_dropped_and_the_unset_cutoff_is_inert() {
    let conn = production_schema_db();
    let me = h66(0xd1);
    const CUT_MS: i64 = 1_754_500_000_000;
    const CUT_SECS: i64 = CUT_MS / 1000;

    let pre = h64(0xd2);
    let post = h64(0xd3);
    admit_hop(&conn, &pre, 900, CUT_SECS - 1);
    admit_hop(&conn, &post, 900, CUT_SECS);
    file_hop_marker(&conn, &me, &pre, &pre, CUT_SECS + 5);
    file_hop_marker(&conn, &me, &post, &post, CUT_SECS + 6);
    let unknown_pre = h64(0xd4);
    let unknown_post = h64(0xd5);
    file_hop_marker(&conn, &me, &unknown_pre, &unknown_pre, CUT_SECS - 10);
    file_hop_marker(&conn, &me, &unknown_post, &unknown_post, CUT_SECS + 10);

    let game = hex::encode(GAME);
    for scope in [None, Some(game.as_str())] {
        let served = hops_txids(&conn, &me, scope, Some(CUT_MS));
        assert!(
            served.contains(&post),
            "the at-cutoff hop is KEPT (>= bar; scoped={})",
            scope.is_some()
        );
        assert!(
            served.contains(&unknown_post),
            "a fresh un-indexed hop with a post-cutoff marker is KEPT (scoped={})",
            scope.is_some()
        );
        assert_eq!(
            served.len(),
            2,
            "the pre-cutoff hop and pre-cutoff unknown marker are DROPPED \
             (scoped={}): {served:?}",
            scope.is_some()
        );

        let all = hops_txids(&conn, &me, scope, None);
        assert_eq!(
            all.len(),
            4,
            "None serves the full page (scoped={}): {all:?}",
            scope.is_some()
        );
    }
}
