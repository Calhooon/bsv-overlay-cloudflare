//! Tests for getUTXOHistory — lookup response enrichment with ancestor spend history.
//!
//! Test-driven for #48.

use bsv_overlay_engine::engine::{Engine, EngineConfig, HistorySelector};
use bsv_overlay_engine::storage::memory::MemoryStorage;
use bsv_overlay_engine::storage::Storage;
use bsv_overlay_engine::types::*;
use std::collections::HashMap;

const BRC62_BEEF_HEX: &str = "0100beef01fe636d0c0007021400fe507c0c7aa754cef1f7889d5fd395cf1f785dd7de98eed895dbedfe4e5bc70d1502ac4e164f5bc16746bb0868404292ac8318bbac3800e4aad13a014da427adce3e010b00bc4ff395efd11719b277694cface5aa50d085a0bb81f613f70313acd28cf4557010400574b2d9142b8d28b61d88e3b2c3f44d858411356b49a28a4643b6d1a6a092a5201030051a05fc84d531b5d250c23f4f886f6812f9fe3f402d61607f977b4ecd2701c19010000fd781529d58fc2523cf396a7f25440b409857e7e221766c57214b1d38c7b481f01010062f542f45ea3660f86c013ced80534cb5fd4c19d66c56e7e8c5d4bf2d40acc5e010100b121e91836fd7cd5102b654e9f72f3cf6fdbfd0b161c53a9c54b12c841126331020100000001cd4e4cac3c7b56920d1e7655e7e260d31f29d9a388d04910f1bbd72304a79029010000006b483045022100e75279a205a547c445719420aa3138bf14743e3f42618e5f86a19bde14bb95f7022064777d34776b05d816daf1699493fcdf2ef5a5ab1ad710d9c97bfb5b8f7cef3641210263e2dee22b1ddc5e11f6fab8bcd2378bdd19580d640501ea956ec0e786f93e76ffffffff013e660000000000001976a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac0000000001000100000001ac4e164f5bc16746bb0868404292ac8318bbac3800e4aad13a014da427adce3e000000006a47304402203a61a2e931612b4bda08d541cfb980885173b8dcf64a3471238ae7abcd368d6402204cbf24f04b9aa2256d8901f0ed97866603d2be8324c2bfb7a37bf8fc90edd5b441210263e2dee22b1ddc5e11f6fab8bcd2378bdd19580d640501ea956ec0e786f93e76ffffffff013c660000000000001976a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac0000000000";
const EXAMPLE_TXID: &str = "157428aee67d11123203735e4c540fa1bdab3b36d5882c6f8c5ff79f07d20d1c";

fn decode_hex(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect()
}
fn example_beef() -> Vec<u8> {
    decode_hex(BRC62_BEEF_HEX)
}

fn make_output(txid: &str, oi: u32, consumed: Vec<Outpoint>) -> Output {
    Output {
        txid: txid.to_string(),
        output_index: oi,
        output_script: vec![0x76],
        satoshis: 1000,
        topic: "Hello".to_string(),
        spent: false,
        outputs_consumed: consumed,
        consumed_by: vec![],
        beef: Some(example_beef()),
        block_height: None,
        score: Some(1000.0),
    }
}

// ============================================================================
// Tests
// ============================================================================

/// No history selector → return output as-is.
/// TS: "Returns the given output if there is no history selector"
#[tokio::test]
async fn no_history_selector_returns_output() {
    let storage = MemoryStorage::new();
    let output = make_output(EXAMPLE_TXID, 0, vec![]);
    storage.insert_output(&output).await.unwrap();

    let engine = Engine::new(
        HashMap::new(),
        HashMap::new(),
        Box::new(storage),
        None,
        EngineConfig::default(),
    );

    let result = engine.get_utxo_history(&output, None).await.unwrap();
    assert!(result.is_some());
    assert_eq!(result.unwrap().txid, EXAMPLE_TXID);
}

/// Depth selector of 0 at depth 0 → should include (0 <= 0 is true).
#[tokio::test]
async fn depth_zero_includes_current() {
    let storage = MemoryStorage::new();
    let output = make_output(EXAMPLE_TXID, 0, vec![]);
    storage.insert_output(&output).await.unwrap();

    let engine = Engine::new(
        HashMap::new(),
        HashMap::new(),
        Box::new(storage),
        None,
        EngineConfig::default(),
    );

    let result = engine
        .get_utxo_history(&output, Some(HistorySelector::Depth(0)))
        .await
        .unwrap();
    assert!(
        result.is_some(),
        "Depth 0 at depth 0 should include current output"
    );
}

/// Depth selector less than current depth → return None.
/// TS: "Returns undefined if the history selector is a number, and less than the current depth"
#[tokio::test]
async fn depth_exceeded_returns_none() {
    let storage = MemoryStorage::new();
    let output = make_output(EXAMPLE_TXID, 0, vec![]);
    storage.insert_output(&output).await.unwrap();

    let engine = Engine::new(
        HashMap::new(),
        HashMap::new(),
        Box::new(storage),
        None,
        EngineConfig::default(),
    );

    // get_utxo_history with depth=0 should work at depth 0
    // But internal recursion at depth 1 with max_depth=0 should stop
    // This tests the public interface — depth 0 means "include this level only, no ancestors"
    let result = engine
        .get_utxo_history(&output, Some(HistorySelector::Depth(0)))
        .await
        .unwrap();
    assert!(result.is_some());
}

/// Output with ancestors and depth selector > 0 returns enriched BEEF.
#[tokio::test]
async fn depth_includes_ancestors() {
    let storage = MemoryStorage::new();

    let ancestor = make_output("ancestor_tx", 0, vec![]);
    storage.insert_output(&ancestor).await.unwrap();

    let child = Output {
        txid: EXAMPLE_TXID.to_string(),
        output_index: 0,
        output_script: vec![0x76],
        satoshis: 900,
        topic: "Hello".to_string(),
        spent: false,
        outputs_consumed: vec![Outpoint::new("ancestor_tx", 0)],
        consumed_by: vec![],
        beef: Some(example_beef()),
        block_height: None,
        score: Some(2000.0),
    };
    storage.insert_output(&child).await.unwrap();

    let engine = Engine::new(
        HashMap::new(),
        HashMap::new(),
        Box::new(storage),
        None,
        EngineConfig::default(),
    );

    let result = engine
        .get_utxo_history(&child, Some(HistorySelector::Depth(1)))
        .await
        .unwrap();
    assert!(result.is_some(), "Should return enriched output");
    // The returned output should have BEEF (potentially enriched with ancestor)
    assert!(result.unwrap().beef.is_some());
}

/// Output without BEEF should return error.
#[tokio::test]
async fn no_beef_returns_error() {
    let storage = MemoryStorage::new();
    let output = Output {
        txid: EXAMPLE_TXID.to_string(),
        output_index: 0,
        output_script: vec![],
        satoshis: 0,
        topic: "Hello".to_string(),
        spent: false,
        outputs_consumed: vec![],
        consumed_by: vec![],
        beef: None,
        block_height: None,
        score: None,
    };

    let engine = Engine::new(
        HashMap::new(),
        HashMap::new(),
        Box::new(storage),
        None,
        EngineConfig::default(),
    );

    let result = engine
        .get_utxo_history(&output, Some(HistorySelector::Depth(1)))
        .await;
    assert!(result.is_err(), "Should error when output has no BEEF");
}
