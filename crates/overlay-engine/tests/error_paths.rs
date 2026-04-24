//! Error-path tests — verify every failure mode is handled correctly.
//!
//! Tests that the engine logs and continues (or returns proper errors)
//! when individual components fail.

use async_trait::async_trait;
use overlay_engine::engine::*;
use overlay_engine::lookup_service::*;
use overlay_engine::storage::memory::MemoryStorage;
use overlay_engine::storage::*;
use overlay_engine::topic_manager::*;
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

fn valid_tagged_beef(topics: &[&str]) -> TaggedBEEF {
    TaggedBEEF::new(
        decode_hex(BRC62_BEEF_HEX),
        topics.iter().map(|s| s.to_string()).collect(),
    )
}

// ============================================================================
// Failing TopicManager
// ============================================================================

struct FailingTopicManager;

#[async_trait(?Send)]
impl TopicManager for FailingTopicManager {
    async fn identify_admissible_outputs(
        &self,
        _: &[u8],
        _: &[u8],
        _: Option<&[u8]>,
        _: SubmitMode,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        Err(TopicManagerError::Other("intentional failure".into()))
    }
    async fn get_documentation(&self) -> String {
        "Fails".into()
    }
    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "failing".into(),
            ..Default::default()
        }
    }
}

// ============================================================================
// Failing LookupService
// ============================================================================

struct FailingLookupService;

#[async_trait(?Send)]
impl LookupService for FailingLookupService {
    fn admission_mode(&self) -> AdmissionMode {
        AdmissionMode::LockingScript
    }
    fn spend_notification_mode(&self) -> SpendNotificationMode {
        SpendNotificationMode::None
    }
    async fn output_admitted_by_topic(
        &self,
        _: &OutputAdmittedByTopic,
    ) -> Result<(), LookupServiceError> {
        Err(LookupServiceError::Other(
            "intentional admission failure".into(),
        ))
    }
    async fn output_evicted(&self, _: &str, _: u32) -> Result<(), LookupServiceError> {
        Ok(())
    }
    async fn lookup(&self, _: &LookupQuestion) -> Result<Vec<UTXOReference>, LookupServiceError> {
        Err(LookupServiceError::Other(
            "intentional lookup failure".into(),
        ))
    }
    async fn get_documentation(&self) -> String {
        "Fails".into()
    }
    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "failing-ls".into(),
            ..Default::default()
        }
    }
}

// ============================================================================
// OK TopicManager (for when we need one that works alongside failing components)
// ============================================================================

struct OkTopicManager;

#[async_trait(?Send)]
impl TopicManager for OkTopicManager {
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
        "OK".into()
    }
    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "ok-tm".into(),
            ..Default::default()
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

/// Topic manager failure should not prevent other topics from succeeding.
#[tokio::test]
async fn topic_manager_error_doesnt_block_other_topics() {
    let mut mgrs: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    mgrs.insert("GoodTopic".into(), Box::new(OkTopicManager));
    mgrs.insert("BadTopic".into(), Box::new(FailingTopicManager));

    let engine = Engine::new(
        mgrs,
        HashMap::new(),
        Box::new(MemoryStorage::new()),
        None,
        EngineConfig::default(),
    );
    let beef = valid_tagged_beef(&["GoodTopic", "BadTopic"]);

    let steak = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

    // Good topic should succeed
    assert_eq!(steak["GoodTopic"].outputs_to_admit, vec![0]);
    // Bad topic should have empty admittance (failed gracefully)
    assert!(steak["BadTopic"].outputs_to_admit.is_empty());
}

/// Lookup service admission failure should be logged but not prevent output storage.
#[tokio::test]
async fn lookup_service_admission_failure_doesnt_block_submit() {
    let mut mgrs: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    mgrs.insert("Hello".into(), Box::new(OkTopicManager));

    let mut lss: HashMap<String, Box<dyn LookupService>> = HashMap::new();
    lss.insert("ls_failing".into(), Box::new(FailingLookupService));

    let engine = Engine::new(
        mgrs,
        lss,
        Box::new(MemoryStorage::new()),
        None,
        EngineConfig::default(),
    );
    let beef = valid_tagged_beef(&["Hello"]);

    // Submit should succeed even though lookup service fails on admission
    let steak = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();
    assert_eq!(steak["Hello"].outputs_to_admit, vec![0]);

    // Output should still be in storage
    let output = engine
        .storage()
        .find_output(
            "157428aee67d11123203735e4c540fa1bdab3b36d5882c6f8c5ff79f07d20d1c",
            0,
            Some("Hello"),
            None,
            false,
        )
        .await
        .unwrap();
    assert!(output.is_some());
}

/// Lookup service query failure returns EngineError::LookupFailed.
#[tokio::test]
async fn lookup_service_query_failure_returns_error() {
    let mut lss: HashMap<String, Box<dyn LookupService>> = HashMap::new();
    lss.insert("ls_failing".into(), Box::new(FailingLookupService));

    let engine = Engine::new(
        HashMap::new(),
        lss,
        Box::new(MemoryStorage::new()),
        None,
        EngineConfig::default(),
    );

    let result = engine
        .lookup(
            &LookupQuestion::new("ls_failing", serde_json::json!({})),
            None,
        )
        .await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), EngineError::LookupFailed(_)));
}

/// Invalid BEEF bytes return BeefParseError.
#[tokio::test]
async fn invalid_beef_returns_parse_error() {
    let mut mgrs: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    mgrs.insert("Hello".into(), Box::new(OkTopicManager));

    let engine = Engine::new(
        mgrs,
        HashMap::new(),
        Box::new(MemoryStorage::new()),
        None,
        EngineConfig::default(),
    );

    // Empty BEEF
    let result = engine
        .submit(
            &TaggedBEEF::new(vec![], vec!["Hello".into()]),
            SubmitMode::CurrentTx,
        )
        .await;
    assert!(matches!(
        result.unwrap_err(),
        EngineError::BeefParseError(_)
    ));

    // Garbage BEEF
    let result = engine
        .submit(
            &TaggedBEEF::new(vec![0xFF, 0xFE, 0xFD], vec!["Hello".into()]),
            SubmitMode::CurrentTx,
        )
        .await;
    assert!(matches!(
        result.unwrap_err(),
        EngineError::BeefParseError(_)
    ));

    // Truncated BEEF (valid header but cut off)
    let mut truncated = decode_hex(BRC62_BEEF_HEX);
    truncated.truncate(20);
    let result = engine
        .submit(
            &TaggedBEEF::new(truncated, vec!["Hello".into()]),
            SubmitMode::CurrentTx,
        )
        .await;
    assert!(matches!(
        result.unwrap_err(),
        EngineError::BeefParseError(_)
    ));
}

/// Empty topics list — no topics to process, should still parse BEEF.
#[tokio::test]
async fn submit_with_empty_topics_returns_empty_steak() {
    let engine = Engine::new(
        HashMap::new(),
        HashMap::new(),
        Box::new(MemoryStorage::new()),
        None,
        EngineConfig::default(),
    );
    let beef = valid_tagged_beef(&[]);
    let steak = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();
    assert!(steak.is_empty());
}

/// Storage find_output returns None for nonexistent outputs (not an error).
#[tokio::test]
async fn storage_find_nonexistent_returns_none() {
    let store = MemoryStorage::new();
    let result = store
        .find_output("nonexistent", 0, None, None, false)
        .await
        .unwrap();
    assert!(result.is_none());
}

/// Storage delete on nonexistent output is a no-op.
#[tokio::test]
async fn storage_delete_nonexistent_is_noop() {
    let store = MemoryStorage::new();
    let result = store.delete_output("nonexistent", 0, "tm_test").await;
    assert!(result.is_ok());
}

/// Storage find_utxos_for_topic with no data returns empty vec.
#[tokio::test]
async fn storage_find_utxos_empty_topic_returns_empty() {
    let store = MemoryStorage::new();
    let results = store
        .find_utxos_for_topic("tm_empty", None, None, false)
        .await
        .unwrap();
    assert!(results.is_empty());
}

/// Every EngineError variant can be constructed and displayed.
#[test]
fn all_engine_error_variants_display() {
    let errors = vec![
        EngineError::UnsupportedTopic("tm_x".into()),
        EngineError::LookupServiceNotFound("ls_x".into()),
        EngineError::LookupFailed("fail".into()),
        EngineError::StorageError("db".into()),
        EngineError::BroadcastError("net".into()),
        EngineError::SpvError("invalid".into()),
        EngineError::BeefParseError("bad".into()),
        EngineError::Other("misc".into()),
    ];
    for e in &errors {
        assert!(!e.to_string().is_empty());
    }
}

/// Every StorageError variant can be constructed and displayed.
#[test]
fn all_storage_error_variants_display() {
    let errors = vec![
        StorageError::NotFound("x".into()),
        StorageError::Duplicate("x".into()),
        StorageError::Database("x".into()),
        StorageError::Serialization("x".into()),
        StorageError::Other("x".into()),
    ];
    for e in &errors {
        assert!(!e.to_string().is_empty());
    }
}

/// Every TopicManagerError variant can be constructed and displayed.
#[test]
fn all_topic_manager_error_variants_display() {
    let errors = vec![
        TopicManagerError::InvalidBeef("x".into()),
        TopicManagerError::NoAdmissibleOutputs("x".into()),
        TopicManagerError::InvalidScript("x".into()),
        TopicManagerError::SignatureError("x".into()),
        TopicManagerError::Other("x".into()),
    ];
    for e in &errors {
        assert!(!e.to_string().is_empty());
    }
}

/// Every LookupServiceError variant can be constructed and displayed.
#[test]
fn all_lookup_service_error_variants_display() {
    let errors = vec![
        LookupServiceError::InvalidQuery("x".into()),
        LookupServiceError::StorageError("x".into()),
        LookupServiceError::Unsupported("x".into()),
        LookupServiceError::Other("x".into()),
    ];
    for e in &errors {
        assert!(!e.to_string().is_empty());
    }
}

/// Error conversions work correctly.
#[test]
fn error_conversions() {
    let se: EngineError = StorageError::Database("db fail".into()).into();
    assert!(matches!(se, EngineError::StorageError(_)));

    let te: EngineError = TopicManagerError::Other("tm fail".into()).into();
    assert!(matches!(te, EngineError::Other(_)));

    let le: EngineError = LookupServiceError::Other("ls fail".into()).into();
    assert!(matches!(le, EngineError::LookupFailed(_)));
}

/// Outpoint::from_graph_id edge cases.
#[test]
fn outpoint_edge_cases() {
    assert!(Outpoint::from_graph_id("").is_none());
    assert!(Outpoint::from_graph_id(".").is_none());
    assert!(Outpoint::from_graph_id("abc.").is_none());
    assert!(Outpoint::from_graph_id(".0").is_some()); // empty txid but valid parse
    assert!(Outpoint::from_graph_id("abc.-1").is_none()); // negative
    assert!(Outpoint::from_graph_id("abc.999999999999").is_none()); // overflow u32

    let op = Outpoint::from_graph_id("abc.0").unwrap();
    assert_eq!(op.txid, "abc");
    assert_eq!(op.output_index, 0);
    assert_eq!(op.to_string(), "abc.0");
}
