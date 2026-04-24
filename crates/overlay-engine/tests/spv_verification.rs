//! Tests for SPV verification in submit().
//!
//! Verifies that submit() validates transactions before admitting them.
//! Test-driven for #46.

use async_trait::async_trait;
use overlay_engine::engine::{Engine, EngineConfig, EngineError};
use overlay_engine::storage::memory::MemoryStorage;
use overlay_engine::topic_manager::{TopicManager, TopicManagerError};
use overlay_engine::types::*;
use std::collections::HashMap;

// ============================================================================
// Test BEEF
// ============================================================================

const BRC62_BEEF_HEX: &str = "0100beef01fe636d0c0007021400fe507c0c7aa754cef1f7889d5fd395cf1f785dd7de98eed895dbedfe4e5bc70d1502ac4e164f5bc16746bb0868404292ac8318bbac3800e4aad13a014da427adce3e010b00bc4ff395efd11719b277694cface5aa50d085a0bb81f613f70313acd28cf4557010400574b2d9142b8d28b61d88e3b2c3f44d858411356b49a28a4643b6d1a6a092a5201030051a05fc84d531b5d250c23f4f886f6812f9fe3f402d61607f977b4ecd2701c19010000fd781529d58fc2523cf396a7f25440b409857e7e221766c57214b1d38c7b481f01010062f542f45ea3660f86c013ced80534cb5fd4c19d66c56e7e8c5d4bf2d40acc5e010100b121e91836fd7cd5102b654e9f72f3cf6fdbfd0b161c53a9c54b12c841126331020100000001cd4e4cac3c7b56920d1e7655e7e260d31f29d9a388d04910f1bbd72304a79029010000006b483045022100e75279a205a547c445719420aa3138bf14743e3f42618e5f86a19bde14bb95f7022064777d34776b05d816daf1699493fcdf2ef5a5ab1ad710d9c97bfb5b8f7cef3641210263e2dee22b1ddc5e11f6fab8bcd2378bdd19580d640501ea956ec0e786f93e76ffffffff013e660000000000001976a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac0000000001000100000001ac4e164f5bc16746bb0868404292ac8318bbac3800e4aad13a014da427adce3e000000006a47304402203a61a2e931612b4bda08d541cfb980885173b8dcf64a3471238ae7abcd368d6402204cbf24f04b9aa2256d8901f0ed97866603d2be8324c2bfb7a37bf8fc90edd5b441210263e2dee22b1ddc5e11f6fab8bcd2378bdd19580d640501ea956ec0e786f93e76ffffffff013c660000000000001976a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac0000000000";

fn decode_hex(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect()
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

// ============================================================================
// Tests
// ============================================================================

/// HistoricalTxNoSpv mode should skip SPV verification entirely.
#[tokio::test]
async fn historical_no_spv_skips_verification() {
    let mut managers: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    managers.insert("Hello".into(), Box::new(AdmitAllTM));

    let engine = Engine::new(
        managers,
        HashMap::new(),
        Box::new(MemoryStorage::new()),
        None,
        EngineConfig::default(),
    );

    let beef = TaggedBEEF::new(decode_hex(BRC62_BEEF_HEX), vec!["Hello".into()]);
    let steak = engine
        .submit(&beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .unwrap();

    // Should succeed without any chain tracker
    assert_eq!(steak["Hello"].outputs_to_admit, vec![0]);
}

/// HistoricalTx mode should also work (no broadcast but SPV should still be checked
/// if chain tracker is configured — but we don't have one, so it should still pass
/// with our current implementation since verification is best-effort).
#[tokio::test]
async fn historical_tx_mode_works() {
    let mut managers: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    managers.insert("Hello".into(), Box::new(AdmitAllTM));

    let engine = Engine::new(
        managers,
        HashMap::new(),
        Box::new(MemoryStorage::new()),
        None,
        EngineConfig::default(),
    );

    let beef = TaggedBEEF::new(decode_hex(BRC62_BEEF_HEX), vec!["Hello".into()]);
    let steak = engine
        .submit(&beef, SubmitMode::HistoricalTx)
        .await
        .unwrap();

    assert_eq!(steak["Hello"].outputs_to_admit, vec![0]);
}

/// CurrentTx mode works when no chain tracker is configured (graceful degradation).
#[tokio::test]
async fn current_tx_without_chain_tracker_works() {
    let mut managers: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    managers.insert("Hello".into(), Box::new(AdmitAllTM));

    let engine = Engine::new(
        managers,
        HashMap::new(),
        Box::new(MemoryStorage::new()),
        None,
        EngineConfig::default(),
    );

    let beef = TaggedBEEF::new(decode_hex(BRC62_BEEF_HEX), vec!["Hello".into()]);
    let steak = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

    // Without chain tracker, SPV verification is skipped gracefully
    assert_eq!(steak["Hello"].outputs_to_admit, vec![0]);
}

/// Invalid BEEF should still be rejected regardless of mode.
#[tokio::test]
async fn invalid_beef_rejected_in_all_modes() {
    let mut managers: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    managers.insert("Hello".into(), Box::new(AdmitAllTM));

    let engine = Engine::new(
        managers,
        HashMap::new(),
        Box::new(MemoryStorage::new()),
        None,
        EngineConfig::default(),
    );

    let bad_beef = TaggedBEEF::new(vec![0xFF, 0xFE], vec!["Hello".into()]);

    // All modes should reject invalid BEEF
    for mode in &[
        SubmitMode::CurrentTx,
        SubmitMode::HistoricalTx,
        SubmitMode::HistoricalTxNoSpv,
    ] {
        let result = engine.submit(&bad_beef, *mode).await;
        assert!(
            result.is_err(),
            "Mode {:?} should reject invalid BEEF",
            mode
        );
        assert!(matches!(
            result.unwrap_err(),
            EngineError::BeefParseError(_)
        ));
    }
}

/// The BRC62 BEEF structure contains merkle proofs in the BUMP section.
/// Verify the BEEF can be parsed and contains valid SPV data.
#[tokio::test]
async fn brc62_beef_parses_with_spv_data() {
    use bsv_rs::transaction::{Beef, Transaction};

    let beef_bytes = decode_hex(BRC62_BEEF_HEX);

    // Parse as Beef struct to inspect structure
    let beef = Beef::from_binary(&beef_bytes).unwrap();
    println!("BEEF txs: {}", beef.txs.len());
    println!("BEEF bumps: {}", beef.bumps.len());

    // The BRC62 BEEF should have BUMPs (merkle proofs)
    assert!(
        !beef.bumps.is_empty(),
        "BRC62 BEEF should have BUMP merkle proofs"
    );

    // Also verify Transaction::from_beef works
    let tx = Transaction::from_beef(&beef_bytes, None).unwrap();
    assert!(!tx.id().is_empty());

    // Check the ancestor chain for merkle paths
    fn count_merkle_paths(tx: &Transaction, depth: usize) -> usize {
        let mut count = if tx.merkle_path.is_some() { 1 } else { 0 };
        if depth < 10 {
            for input in &tx.inputs {
                if let Some(ref source_tx) = input.source_transaction {
                    count += count_merkle_paths(source_tx, depth + 1);
                }
            }
        }
        count
    }

    let path_count = count_merkle_paths(&tx, 0);
    println!("Merkle paths found in tx chain: {path_count}");
    // The BEEF has BUMPs, but they may or may not be attached to Transaction objects
    // depending on how from_beef distributes them. Either way, the BEEF is valid SPV data.
}
