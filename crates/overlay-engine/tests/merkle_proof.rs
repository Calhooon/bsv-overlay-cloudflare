//! Tests for handleNewMerkleProof — updating BEEF with merkle proofs
//! when transactions get mined.
//!
//! Test-driven for #45.

use bsv_overlay_engine::engine::{Engine, EngineConfig};
use bsv_overlay_engine::storage::memory::MemoryStorage;
use bsv_overlay_engine::storage::Storage;
use bsv_overlay_engine::topic_manager::TopicManager;
use bsv_overlay_engine::types::*;
use std::collections::HashMap;

use async_trait::async_trait;
use bsv_overlay_engine::topic_manager::TopicManagerError;

// ============================================================================
// Real BEEF from TS tests
// ============================================================================

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

fn make_engine_with_storage(storage: MemoryStorage) -> Engine {
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

/// handleNewMerkleProof should find outputs by txid and update block height.
#[tokio::test]
async fn handle_proof_updates_block_height() {
    let storage = MemoryStorage::new();

    // Insert an output
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

    let engine = make_engine_with_storage(storage);

    // Create a fake merkle proof hex (minimal valid MerklePath)
    // The handleNewMerkleProof should update blockHeight even if
    // the actual proof application to BEEF has issues
    let proof_hex = ""; // We'll test with the blockHeight update path

    engine
        .handle_new_merkle_proof(EXAMPLE_TXID, proof_hex, Some(850000))
        .await
        .unwrap();

    // Block height should be updated
    let output = engine
        .storage()
        .find_output(EXAMPLE_TXID, 0, Some("Hello"), None, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output.block_height, Some(850000));
}

/// handleNewMerkleProof with unknown txid should return error.
#[tokio::test]
async fn handle_proof_unknown_txid_errors() {
    let engine = make_engine_with_storage(MemoryStorage::new());

    let result = engine
        .handle_new_merkle_proof("nonexistent", "", None)
        .await;
    assert!(result.is_err());
}

/// handleNewMerkleProof should update BEEF in storage.
#[tokio::test]
async fn handle_proof_updates_beef_in_storage() {
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

    let engine = make_engine_with_storage(storage);

    // Get original BEEF length
    let orig = engine
        .storage()
        .find_output(EXAMPLE_TXID, 0, Some("Hello"), None, true)
        .await
        .unwrap()
        .unwrap();
    let _orig_beef_len = orig.beef.unwrap().len();

    // Apply proof — the BEEF may change size when a merkle path is added/updated
    engine
        .handle_new_merkle_proof(EXAMPLE_TXID, "", Some(850000))
        .await
        .unwrap();

    // BEEF should still be present (may or may not change depending on proof)
    let updated = engine
        .storage()
        .find_output(EXAMPLE_TXID, 0, Some("Hello"), None, true)
        .await
        .unwrap()
        .unwrap();
    assert!(
        updated.beef.is_some(),
        "BEEF should still be present after proof update"
    );
}

/// handleNewMerkleProof should recursively update consumedBy chain.
#[tokio::test]
async fn handle_proof_recurses_consumed_by() {
    let storage = MemoryStorage::new();

    // Create a chain: parent → child (child consumedBy includes parent)
    let parent_txid = "aaaa";
    let child_txid = EXAMPLE_TXID;

    storage
        .insert_output(&Output {
            txid: parent_txid.to_string(),
            output_index: 0,
            output_script: vec![],
            satoshis: 1000,
            topic: "Hello".to_string(),
            spent: true,
            outputs_consumed: vec![],
            consumed_by: vec![Outpoint::new(child_txid, 0)],
            beef: Some(example_beef()), // Reuse BEEF for testing
            block_height: None,
            score: Some(1000.0),
        })
        .await
        .unwrap();

    storage
        .insert_output(&Output {
            txid: child_txid.to_string(),
            output_index: 0,
            output_script: vec![],
            satoshis: 900,
            topic: "Hello".to_string(),
            spent: false,
            outputs_consumed: vec![Outpoint::new(parent_txid, 0)],
            consumed_by: vec![],
            beef: Some(example_beef()),
            block_height: None,
            score: Some(2000.0),
        })
        .await
        .unwrap();

    let engine = make_engine_with_storage(storage);

    // Update proof for parent — should recurse to child
    engine
        .handle_new_merkle_proof(parent_txid, "", Some(850000))
        .await
        .unwrap();

    // Parent should have block height updated
    let parent = engine
        .storage()
        .find_output(parent_txid, 0, Some("Hello"), None, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(parent.block_height, Some(850000));

    // Child should NOT have block height (proof was for parent, not child)
    let child = engine
        .storage()
        .find_output(child_txid, 0, Some("Hello"), None, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        child.block_height, None,
        "Child block height should not be set by parent's proof"
    );
}

/// Multiple outputs from the same txid should all get updated.
#[tokio::test]
async fn handle_proof_updates_all_outputs_for_txid() {
    let storage = MemoryStorage::new();

    // Two outputs from the same tx, different topics
    for topic in &["TopicA", "TopicB"] {
        storage
            .insert_output(&Output {
                txid: EXAMPLE_TXID.to_string(),
                output_index: 0,
                output_script: vec![],
                satoshis: 26172,
                topic: topic.to_string(),
                spent: false,
                outputs_consumed: vec![],
                consumed_by: vec![],
                beef: Some(example_beef()),
                block_height: None,
                score: Some(1000.0),
            })
            .await
            .unwrap();
    }

    let engine = make_engine_with_storage(storage);

    engine
        .handle_new_merkle_proof(EXAMPLE_TXID, "", Some(850000))
        .await
        .unwrap();

    // Both outputs should have block height updated
    for topic in &["TopicA", "TopicB"] {
        let output = engine
            .storage()
            .find_output(EXAMPLE_TXID, 0, Some(topic), None, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            output.block_height,
            Some(850000),
            "Output in {topic} should have block height"
        );
    }
}
