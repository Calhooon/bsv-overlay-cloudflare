//! Generate live-proof vectors for the `tm_low` / `ls_low` deployment.
//!
//! Prints two BEEF hex blobs built from a random test key:
//!
//! 1. `TABLE_OPEN tx` — output 0 is a properly signed `LOW.table.v1`
//!    PushDrop (same construction as the unit-test vectors).
//! 2. `SPEND tx` — spends that output (closes the table).
//!
//! Usage (from the repo root):
//! ```sh
//! cargo run -p bsv-overlay-discovery --example low_live_proof
//! # then POST the BEEFs to /submit with x-topics: ["tm_low"] and
//! # x-submit-mode: historical-tx-no-spv, and query /lookup ls_low.
//! ```

use bsv_rs::primitives::ec::{PrivateKey, PublicKey};
use bsv_rs::script::templates::PushDrop;
use bsv_rs::transaction::{Transaction, TransactionInput, TransactionOutput};
use bsv_rs::wallet::{
    Counterparty, CreateSignatureArgs, GetPublicKeyArgs, ProtoWallet, Protocol, SecurityLevel,
};

fn make_signed_low_output(signer_key: &PrivateKey, data_fields: Vec<Vec<u8>>) -> TransactionOutput {
    let signer_wallet = ProtoWallet::new(Some(signer_key.clone()));
    let data: Vec<u8> = data_fields.iter().flat_map(|f| f.iter().copied()).collect();
    let protocol_id = Protocol::new(SecurityLevel::Counterparty, "low poker lobby");

    let sig_result = signer_wallet
        .create_signature(CreateSignatureArgs {
            data: Some(data),
            hash_to_directly_sign: None,
            protocol_id: protocol_id.clone(),
            key_id: "1".to_string(),
            counterparty: Some(Counterparty::Anyone),
        })
        .unwrap();

    let locking_key_hex = signer_wallet
        .get_public_key(GetPublicKeyArgs {
            identity_key: false,
            protocol_id: Some(protocol_id),
            key_id: Some("1".to_string()),
            counterparty: Some(Counterparty::Anyone),
            for_self: Some(true),
        })
        .unwrap()
        .public_key;
    let locking_key = PublicKey::from_hex(&locking_key_hex).unwrap();

    let mut all_fields = data_fields;
    all_fields.push(sig_result.signature);

    let pushdrop = PushDrop::new(locking_key, all_fields);
    TransactionOutput {
        satoshis: Some(1),
        locking_script: pushdrop.lock(),
        change: false,
    }
}

fn main() {
    let signer = PrivateKey::random();
    let identity_hex = ProtoWallet::new(Some(signer.clone())).identity_key_hex();
    let game_id = [0x4Cu8; 32]; // arbitrary test game

    // ── tx1: TABLE_OPEN announcement ────────────────────────────────────
    let table_fields = vec![
        b"LOW.table.v1".to_vec(),
        hex::decode(&identity_hex).unwrap(),
        game_id.to_vec(),
        1000u64.to_le_bytes().to_vec(), // stake: 1000 sats
        [0xAAu8; 32].to_vec(),          // rules hash
        b"https://low-relay.dev-a3e.workers.dev".to_vec(),
        950_000u32.to_le_bytes().to_vec(), // expiry height
    ];
    let table_output = make_signed_low_output(&signer, table_fields);

    let mut tx1 = Transaction::new();
    tx1.add_input(TransactionInput::new("01".repeat(32), 0))
        .unwrap();
    tx1.add_output(table_output).unwrap();
    let tx1_id = tx1.id();
    let beef1 = tx1.to_beef(true).unwrap();

    // ── tx2: spend the TABLE_OPEN (table closed) ────────────────────────
    let mut tx2 = Transaction::new();
    tx2.add_input(TransactionInput::with_source_transaction(tx1.clone(), 0))
        .unwrap();
    tx2.add_output(TransactionOutput {
        satoshis: Some(0),
        locking_script: bsv_rs::script::Script::from_hex("006a").unwrap().into(), // OP_FALSE OP_RETURN
        change: false,
    })
    .unwrap();
    let tx2_id = tx2.id();
    let beef2 = tx2.to_beef(true).unwrap();

    // ── tx3: GAME_UTXO pointer (live pot escrow outpoint for the game) ──
    let pointer_fields = vec![
        b"LOW.gameutxo.v1".to_vec(),
        hex::decode(&identity_hex).unwrap(),
        game_id.to_vec(),
        [0xBBu8; 32].to_vec(),          // pot txid
        0u32.to_le_bytes().to_vec(),    // pot vout
    ];
    let pointer_output = make_signed_low_output(&signer, pointer_fields);

    let mut tx3 = Transaction::new();
    tx3.add_input(TransactionInput::new("02".repeat(32), 0))
        .unwrap();
    tx3.add_output(pointer_output).unwrap();
    let tx3_id = tx3.id();
    let beef3 = tx3.to_beef(true).unwrap();

    println!("IDENTITY_KEY={identity_hex}");
    println!("GAME_ID={}", hex::encode(game_id));
    println!("TABLE_TXID={tx1_id}");
    println!("TABLE_BEEF={}", hex::encode(&beef1));
    println!("SPEND_TXID={tx2_id}");
    println!("SPEND_BEEF={}", hex::encode(&beef2));
    println!("POINTER_TXID={tx3_id}");
    println!("POINTER_BEEF={}", hex::encode(&beef3));
}
