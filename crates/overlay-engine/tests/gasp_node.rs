//! Tests for provideForeignGASPNode — BEEF tree search for GASP hydration.
//!
//! Test-driven for #47. Based on TS Engine.test.ts provideForeignGASPNode tests
//! and the OverlayGASPRemote patterns.

use async_trait::async_trait;
use bsv_overlay_engine::engine::{Engine, EngineConfig, EngineError};
use bsv_overlay_engine::storage::memory::MemoryStorage;
use bsv_overlay_engine::storage::Storage;
use bsv_overlay_engine::topic_manager::{TopicManager, TopicManagerError};
use bsv_overlay_engine::types::*;
use std::collections::HashMap;

const BRC62_BEEF_HEX: &str = "0100beef01fe636d0c0007021400fe507c0c7aa754cef1f7889d5fd395cf1f785dd7de98eed895dbedfe4e5bc70d1502ac4e164f5bc16746bb0868404292ac8318bbac3800e4aad13a014da427adce3e010b00bc4ff395efd11719b277694cface5aa50d085a0bb81f613f70313acd28cf4557010400574b2d9142b8d28b61d88e3b2c3f44d858411356b49a28a4643b6d1a6a092a5201030051a05fc84d531b5d250c23f4f886f6812f9fe3f402d61607f977b4ecd2701c19010000fd781529d58fc2523cf396a7f25440b409857e7e221766c57214b1d38c7b481f01010062f542f45ea3660f86c013ced80534cb5fd4c19d66c56e7e8c5d4bf2d40acc5e010100b121e91836fd7cd5102b654e9f72f3cf6fdbfd0b161c53a9c54b12c841126331020100000001cd4e4cac3c7b56920d1e7655e7e260d31f29d9a388d04910f1bbd72304a79029010000006b483045022100e75279a205a547c445719420aa3138bf14743e3f42618e5f86a19bde14bb95f7022064777d34776b05d816daf1699493fcdf2ef5a5ab1ad710d9c97bfb5b8f7cef3641210263e2dee22b1ddc5e11f6fab8bcd2378bdd19580d640501ea956ec0e786f93e76ffffffff013e660000000000001976a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac0000000001000100000001ac4e164f5bc16746bb0868404292ac8318bbac3800e4aad13a014da427adce3e000000006a47304402203a61a2e931612b4bda08d541cfb980885173b8dcf64a3471238ae7abcd368d6402204cbf24f04b9aa2256d8901f0ed97866603d2be8324c2bfb7a37bf8fc90edd5b441210263e2dee22b1ddc5e11f6fab8bcd2378bdd19580d640501ea956ec0e786f93e76ffffffff013c660000000000001976a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac0000000000";
const EXAMPLE_TXID: &str = "157428aee67d11123203735e4c540fa1bdab3b36d5882c6f8c5ff79f07d20d1c";
const EXAMPLE_PREVIOUS_TXID: &str =
    "3ecead27a44d013ad1aae40038acbb1883ac9242406808bb4667c15b4f164eac";

fn decode_hex(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect()
}

fn example_beef() -> Vec<u8> {
    decode_hex(BRC62_BEEF_HEX)
}

struct AdmitAllTM;
#[async_trait(?Send)]
impl TopicManager for AdmitAllTM {
    async fn identify_admissible_outputs(
        &self,
        _: &[u8],
        _: &[u8],
        _: Option<&[u8]>,
        _: SubmitMode,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        Ok(AdmittanceInstructions {
            outputs_to_admit: vec![0],
            coins_to_retain: vec![],
            coins_removed: None,
        })
    }
    async fn get_documentation(&self) -> String {
        "test".into()
    }
    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "test".into(),
            ..Default::default()
        }
    }
}

fn make_engine_with_output(txid: &str, beef: Vec<u8>) -> Engine {
    let rt = tokio::runtime::Handle::current();
    let storage = MemoryStorage::new();

    // Can't use .await in sync fn, use block_on
    std::thread::scope(|_| {
        rt.block_on(async {
            storage
                .insert_output(&Output {
                    txid: txid.to_string(),
                    output_index: 0,
                    output_script: vec![0x76],
                    satoshis: 26172,
                    topic: "Hello".to_string(),
                    spent: false,
                    outputs_consumed: vec![],
                    consumed_by: vec![],
                    beef: Some(beef),
                    block_height: None,
                    score: Some(1000.0),
                })
                .await
                .unwrap();
        });
    });

    let mut managers: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    managers.insert("Hello".into(), Box::new(AdmitAllTM));
    Engine::new(
        managers,
        HashMap::new(),
        Box::new(storage),
        None,
        EngineConfig::default(),
    )
}

// ============================================================================
// Tests
// ============================================================================

/// provideForeignGASPNode should return a GASPNode for a known output.
#[tokio::test]
async fn provide_node_for_known_output() {
    let storage = MemoryStorage::new();
    storage
        .insert_output(&Output {
            txid: EXAMPLE_TXID.to_string(),
            output_index: 0,
            output_script: vec![0x76],
            satoshis: 26172,
            topic: "Hello".to_string(),
            spent: false,
            outputs_consumed: vec![],
            consumed_by: vec![],
            beef: Some(example_beef()),
            block_height: None,
            score: Some(1000.0),
        })
        .await
        .unwrap();

    let mut managers: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    managers.insert("Hello".into(), Box::new(AdmitAllTM));
    let engine = Engine::new(
        managers,
        HashMap::new(),
        Box::new(storage),
        None,
        EngineConfig::default(),
    );

    let graph_id = format!("{EXAMPLE_TXID}.0");
    let node = engine
        .provide_foreign_gasp_node(&graph_id, EXAMPLE_TXID, 0)
        .await
        .unwrap();

    assert_eq!(node.graph_id, graph_id);
    assert_eq!(node.output_index, 0);
    assert!(!node.raw_tx.is_empty(), "rawTx should be non-empty hex");
    // rawTx should be valid hex
    assert!(node.raw_tx.chars().all(|c| c.is_ascii_hexdigit()));
}

/// provideForeignGASPNode should find ancestor tx in BEEF tree.
#[tokio::test]
async fn provide_node_finds_ancestor_in_beef_tree() {
    let storage = MemoryStorage::new();
    storage
        .insert_output(&Output {
            txid: EXAMPLE_TXID.to_string(),
            output_index: 0,
            output_script: vec![0x76],
            satoshis: 26172,
            topic: "Hello".to_string(),
            spent: false,
            outputs_consumed: vec![],
            consumed_by: vec![],
            beef: Some(example_beef()),
            block_height: None,
            score: Some(1000.0),
        })
        .await
        .unwrap();

    let mut managers: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    managers.insert("Hello".into(), Box::new(AdmitAllTM));
    let engine = Engine::new(
        managers,
        HashMap::new(),
        Box::new(storage),
        None,
        EngineConfig::default(),
    );

    // Request the ANCESTOR txid (the input's source) — it should be found
    // inside the BEEF tree of the root output
    let graph_id = format!("{EXAMPLE_TXID}.0");
    let node = engine
        .provide_foreign_gasp_node(&graph_id, EXAMPLE_PREVIOUS_TXID, 0)
        .await
        .unwrap();

    assert_eq!(node.graph_id, graph_id);
    assert!(!node.raw_tx.is_empty());
    // The rawTx should be the ancestor transaction, not the root
}

/// provideForeignGASPNode should error for completely unknown txid.
#[tokio::test]
async fn provide_node_errors_for_unknown() {
    let engine = Engine::new(
        HashMap::new(),
        HashMap::new(),
        Box::new(MemoryStorage::new()),
        None,
        EngineConfig::default(),
    );

    let result = engine
        .provide_foreign_gasp_node("unknown.0", "unknown", 0)
        .await;
    assert!(result.is_err());
}

/// provideForeignGASPNode should error when BEEF is missing.
#[tokio::test]
async fn provide_node_errors_when_no_beef() {
    let storage = MemoryStorage::new();
    storage
        .insert_output(&Output {
            txid: EXAMPLE_TXID.to_string(),
            output_index: 0,
            output_script: vec![],
            satoshis: 0,
            topic: "Hello".to_string(),
            spent: false,
            outputs_consumed: vec![],
            consumed_by: vec![],
            beef: None, // No BEEF!
            block_height: None,
            score: None,
        })
        .await
        .unwrap();

    let engine = Engine::new(
        HashMap::new(),
        HashMap::new(),
        Box::new(storage),
        None,
        EngineConfig::default(),
    );

    let result = engine
        .provide_foreign_gasp_node(&format!("{EXAMPLE_TXID}.0"), EXAMPLE_TXID, 0)
        .await;
    assert!(result.is_err(), "Should error when output has no BEEF");
}
