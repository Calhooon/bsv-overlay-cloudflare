//! Cross-SDK test vectors — verify bsv-rs produces identical results to TS @bsv/sdk.
//!
//! The BEEF hex below is taken directly from:
//! ~/bsv/overlay-services/src/__tests/Engine.test.ts (line 22)
//!
//! The expected values (txid, previousTXID, satoshis) are from the same TS test file.

use bsv_rs::transaction::Transaction;

/// BEEF hex from the TS overlay-services Engine test suite (BRC-62 format).
const BRC62_BEEF_HEX: &str = "0100beef01fe636d0c0007021400fe507c0c7aa754cef1f7889d5fd395cf1f785dd7de98eed895dbedfe4e5bc70d1502ac4e164f5bc16746bb0868404292ac8318bbac3800e4aad13a014da427adce3e010b00bc4ff395efd11719b277694cface5aa50d085a0bb81f613f70313acd28cf4557010400574b2d9142b8d28b61d88e3b2c3f44d858411356b49a28a4643b6d1a6a092a5201030051a05fc84d531b5d250c23f4f886f6812f9fe3f402d61607f977b4ecd2701c19010000fd781529d58fc2523cf396a7f25440b409857e7e221766c57214b1d38c7b481f01010062f542f45ea3660f86c013ced80534cb5fd4c19d66c56e7e8c5d4bf2d40acc5e010100b121e91836fd7cd5102b654e9f72f3cf6fdbfd0b161c53a9c54b12c841126331020100000001cd4e4cac3c7b56920d1e7655e7e260d31f29d9a388d04910f1bbd72304a79029010000006b483045022100e75279a205a547c445719420aa3138bf14743e3f42618e5f86a19bde14bb95f7022064777d34776b05d816daf1699493fcdf2ef5a5ab1ad710d9c97bfb5b8f7cef3641210263e2dee22b1ddc5e11f6fab8bcd2378bdd19580d640501ea956ec0e786f93e76ffffffff013e660000000000001976a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac0000000001000100000001ac4e164f5bc16746bb0868404292ac8318bbac3800e4aad13a014da427adce3e000000006a47304402203a61a2e931612b4bda08d541cfb980885173b8dcf64a3471238ae7abcd368d6402204cbf24f04b9aa2256d8901f0ed97866603d2be8324c2bfb7a37bf8fc90edd5b441210263e2dee22b1ddc5e11f6fab8bcd2378bdd19580d640501ea956ec0e786f93e76ffffffff013c660000000000001976a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac0000000000";

/// Expected TXID from TS: Transaction.fromHexBEEF(BRC62Hex).id('hex')
const EXPECTED_TXID: &str = "157428aee67d11123203735e4c540fa1bdab3b36d5882c6f8c5ff79f07d20d1c";

/// Expected previous TXID from TS tests (the input's source transaction).
const EXPECTED_PREVIOUS_TXID: &str =
    "3ecead27a44d013ad1aae40038acbb1883ac9242406808bb4667c15b4f164eac";

/// Expected output[0] satoshis from TS: tx.outputs[0].satoshis
const EXPECTED_SATOSHIS: u64 = 26172;

/// Expected output[0] locking script hex from TS: tx.outputs[0].lockingScript.toHex()
const EXPECTED_SCRIPT_HEX: &str = "76a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac";

fn decode_hex(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect()
}

#[test]
fn test_beef_parses_without_error() {
    let beef_bytes = decode_hex(BRC62_BEEF_HEX);
    let tx = Transaction::from_beef(&beef_bytes, None);
    assert!(tx.is_ok(), "BEEF parsing failed: {:?}", tx.err());
}

#[test]
fn test_beef_txid_matches_ts_exactly() {
    let beef_bytes = decode_hex(BRC62_BEEF_HEX);
    let tx = Transaction::from_beef(&beef_bytes, None).unwrap();
    let txid = tx.id();
    assert_eq!(
        txid, EXPECTED_TXID,
        "Rust txid must match TS txid byte-for-byte"
    );
}

#[test]
fn test_beef_has_one_output() {
    let beef_bytes = decode_hex(BRC62_BEEF_HEX);
    let tx = Transaction::from_beef(&beef_bytes, None).unwrap();
    assert_eq!(tx.outputs.len(), 1, "Should have exactly 1 output");
}

#[test]
fn test_beef_has_one_input() {
    let beef_bytes = decode_hex(BRC62_BEEF_HEX);
    let tx = Transaction::from_beef(&beef_bytes, None).unwrap();
    assert_eq!(tx.inputs.len(), 1, "Should have exactly 1 input");
}

#[test]
fn test_beef_output_satoshis_matches_ts() {
    let beef_bytes = decode_hex(BRC62_BEEF_HEX);
    let tx = Transaction::from_beef(&beef_bytes, None).unwrap();
    let satoshis = tx.outputs[0].get_satoshis();
    assert_eq!(
        satoshis, EXPECTED_SATOSHIS,
        "Output satoshis must match TS value"
    );
}

#[test]
fn test_beef_output_locking_script_matches_ts() {
    let beef_bytes = decode_hex(BRC62_BEEF_HEX);
    let tx = Transaction::from_beef(&beef_bytes, None).unwrap();
    let script_hex = tx.outputs[0].locking_script.to_hex();
    assert_eq!(
        script_hex, EXPECTED_SCRIPT_HEX,
        "Locking script must match TS value byte-for-byte"
    );
}

#[test]
fn test_beef_input_source_txid_matches_ts() {
    let beef_bytes = decode_hex(BRC62_BEEF_HEX);
    let tx = Transaction::from_beef(&beef_bytes, None).unwrap();

    let source_txid = tx.inputs[0].get_source_txid().unwrap();
    println!("Input source txid: {source_txid}");
    println!("Expected previous: {EXPECTED_PREVIOUS_TXID}");

    assert_eq!(
        source_txid, EXPECTED_PREVIOUS_TXID,
        "Input source TXID must match TS examplePreviousTXID"
    );
}

#[test]
fn test_beef_input_source_output_index() {
    let beef_bytes = decode_hex(BRC62_BEEF_HEX);
    let tx = Transaction::from_beef(&beef_bytes, None).unwrap();
    assert_eq!(
        tx.inputs[0].source_output_index, 0,
        "Input source output index should be 0"
    );
}
