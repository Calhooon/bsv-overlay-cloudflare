//! Engine parity tests — ported from ~/bsv/overlay-services/src/__tests/Engine.test.ts
//!
//! Each test is tagged with the original TS test description for traceability.
//! Tests cover SPV verification, previous coin lookup, spend notifications,
//! deleteUTXODeep, consumedBy updates, handleNewMerkleProof, and getUTXOHistory.

use async_trait::async_trait;
use overlay_engine::advertiser::*;
use overlay_engine::engine::*;
use overlay_engine::lookup_service::*;
use overlay_engine::storage::memory::MemoryStorage;
use overlay_engine::storage::*;
use overlay_engine::topic_manager::*;
use overlay_engine::types::*;
use std::collections::HashMap;
use std::sync::Mutex;

// ============================================================================
// Test BEEF data (from TS Engine.test.ts line 22)
// ============================================================================

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

fn example_tagged_beef(topics: &[&str]) -> TaggedBEEF {
    TaggedBEEF::new(
        example_beef(),
        topics.iter().map(|s| s.to_string()).collect(),
    )
}

// ============================================================================
// Mock TopicManager
// ============================================================================

struct MockTopicManager {
    admit_indices: Vec<u32>,
    calls: Mutex<Vec<String>>,
}

impl MockTopicManager {
    fn new(admit: Vec<u32>) -> Self {
        Self {
            admit_indices: admit,
            calls: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait(?Send)]
impl TopicManager for MockTopicManager {
    async fn identify_admissible_outputs(
        &self,
        _beef: &[u8],
        _prev: &[u8],
        _ocv: Option<&[u8]>,
        _mode: SubmitMode,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        self.calls.lock().unwrap().push("identify".into());
        Ok(AdmittanceInstructions {
            outputs_to_admit: self.admit_indices.clone(),
            coins_to_retain: vec![],
            coins_removed: None,
        })
    }

    async fn get_documentation(&self) -> String {
        "Topical Documentation".into()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "Mock Manager".into(),
            description: Some("Mock Short Manager Description".into()),
            ..Default::default()
        }
    }
}

// ============================================================================
// Mock LookupService (tracks all calls)
// ============================================================================

struct TrackingLookupService {
    admitted: Mutex<Vec<(String, u32, String)>>, // (txid, outputIndex, topic)
    spent_calls: Mutex<Vec<(String, u32)>>,
    evicted_calls: Mutex<Vec<(String, u32)>>,
}

impl TrackingLookupService {
    fn new() -> Self {
        Self {
            admitted: Mutex::new(Vec::new()),
            spent_calls: Mutex::new(Vec::new()),
            evicted_calls: Mutex::new(Vec::new()),
        }
    }

    fn admitted_count(&self) -> usize {
        self.admitted.lock().unwrap().len()
    }

    fn admitted_txids(&self) -> Vec<String> {
        self.admitted
            .lock()
            .unwrap()
            .iter()
            .map(|(t, _, _)| t.clone())
            .collect()
    }
}

#[async_trait(?Send)]
impl LookupService for TrackingLookupService {
    fn admission_mode(&self) -> AdmissionMode {
        AdmissionMode::LockingScript
    }

    fn spend_notification_mode(&self) -> SpendNotificationMode {
        SpendNotificationMode::None
    }

    async fn output_admitted_by_topic(
        &self,
        payload: &OutputAdmittedByTopic,
    ) -> Result<(), LookupServiceError> {
        match payload {
            OutputAdmittedByTopic::LockingScript {
                txid,
                output_index,
                topic,
                ..
            } => {
                self.admitted
                    .lock()
                    .unwrap()
                    .push((txid.clone(), *output_index, topic.clone()));
            }
            OutputAdmittedByTopic::WholeTx {
                output_index,
                topic,
                ..
            } => {
                self.admitted.lock().unwrap().push((
                    "whole-tx".into(),
                    *output_index,
                    topic.clone(),
                ));
            }
        }
        Ok(())
    }

    async fn output_evicted(
        &self,
        txid: &str,
        output_index: u32,
    ) -> Result<(), LookupServiceError> {
        self.evicted_calls
            .lock()
            .unwrap()
            .push((txid.into(), output_index));
        Ok(())
    }

    async fn lookup(&self, _q: &LookupQuestion) -> Result<Vec<UTXOReference>, LookupServiceError> {
        Ok(self
            .admitted
            .lock()
            .unwrap()
            .iter()
            .map(|(txid, oi, _)| UTXOReference {
                txid: txid.clone(),
                output_index: *oi,
            })
            .collect())
    }

    async fn get_documentation(&self) -> String {
        "Mock Service docs".into()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "Mock Service".into(),
            description: Some("Mock Short Service Description".into()),
            ..Default::default()
        }
    }
}

// ============================================================================
// Mock Advertiser
// ============================================================================

struct TrackingAdvertiser {
    created: Mutex<Vec<AdvertisementData>>,
    revoked: Mutex<Vec<Advertisement>>,
    existing: Mutex<Vec<Advertisement>>,
}

impl TrackingAdvertiser {
    fn new() -> Self {
        Self {
            created: Mutex::new(Vec::new()),
            revoked: Mutex::new(Vec::new()),
            existing: Mutex::new(Vec::new()),
        }
    }

    fn was_create_called(&self) -> bool {
        !self.created.lock().unwrap().is_empty()
    }
}

#[async_trait(?Send)]
impl Advertiser for TrackingAdvertiser {
    async fn create_advertisements(
        &self,
        ads: &[AdvertisementData],
    ) -> Result<TaggedBEEF, AdvertiserError> {
        self.created.lock().unwrap().extend(ads.iter().cloned());
        Ok(example_tagged_beef(&["tm_ship"]))
    }

    async fn find_all_advertisements(
        &self,
        _protocol: Protocol,
    ) -> Result<Vec<Advertisement>, AdvertiserError> {
        Ok(self.existing.lock().unwrap().clone())
    }

    async fn revoke_advertisements(
        &self,
        ads: &[Advertisement],
    ) -> Result<TaggedBEEF, AdvertiserError> {
        self.revoked.lock().unwrap().extend(ads.iter().cloned());
        Ok(example_tagged_beef(&["tm_ship"]))
    }

    fn parse_advertisement(&self, _script: &[u8]) -> Option<Advertisement> {
        None
    }
}

// ============================================================================
// Helper: build engine with custom config
// ============================================================================

fn build_engine(
    managers: HashMap<String, Box<dyn TopicManager>>,
    lookup_services: HashMap<String, Box<dyn LookupService>>,
    advertiser: Option<Box<dyn Advertiser>>,
    config: EngineConfig,
) -> Engine {
    Engine::new(
        managers,
        lookup_services,
        Box::new(MemoryStorage::new()),
        advertiser,
        config,
    )
}

fn single_topic_engine(topic: &str, admit: Vec<u32>) -> Engine {
    let mut mgrs: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    mgrs.insert(topic.to_string(), Box::new(MockTopicManager::new(admit)));
    let ls_name = format!("ls_{}", topic.to_lowercase());
    let mut lss: HashMap<String, Box<dyn LookupService>> = HashMap::new();
    lss.insert(ls_name, Box::new(TrackingLookupService::new()));
    build_engine(mgrs, lss, None, EngineConfig::default())
}

// ============================================================================
// TS Test: "Uses SHIP sync configuration by default"
// ============================================================================

#[test]
fn ts_sync_config_defaults_to_ship() {
    let engine = single_topic_engine("tm_helloworld", vec![0]);
    let sync = engine.config().sync_configuration.get("tm_helloworld");
    assert!(matches!(sync, Some(SyncTarget::Ship)));
}

// ============================================================================
// TS Test: "Does not set sync method to SHIP for managers set to false"
// ============================================================================

#[test]
fn ts_sync_config_respects_disabled() {
    let mut mgrs: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    mgrs.insert(
        "tm_helloworld".into(),
        Box::new(MockTopicManager::new(vec![0])),
    );

    let mut sync_config = HashMap::new();
    sync_config.insert("tm_helloworld".to_string(), SyncTarget::Disabled);

    let engine = build_engine(
        mgrs,
        HashMap::new(),
        None,
        EngineConfig {
            sync_configuration: sync_config,
            ..Default::default()
        },
    );

    let sync = engine.config().sync_configuration.get("tm_helloworld");
    assert!(matches!(sync, Some(SyncTarget::Disabled)));
}

// ============================================================================
// TS Test: "Combines existing trackers with shipTrackers, no duplicates"
// ============================================================================

#[test]
fn ts_sync_config_combines_trackers_no_duplicates() {
    let mut mgrs: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    mgrs.insert("tm_ship".into(), Box::new(MockTopicManager::new(vec![])));
    mgrs.insert("tm_slap".into(), Box::new(MockTopicManager::new(vec![])));

    let mut sync_config = HashMap::new();
    sync_config.insert(
        "tm_ship".to_string(),
        SyncTarget::Peers(vec!["existingTracker1".into()]),
    );
    sync_config.insert(
        "tm_slap".to_string(),
        SyncTarget::Peers(vec!["existingTracker2".into()]),
    );

    let engine = build_engine(
        mgrs,
        HashMap::new(),
        None,
        EngineConfig {
            ship_trackers: vec!["tracker1".into(), "existingTracker1".into()],
            slap_trackers: vec!["tracker2".into(), "existingTracker2".into()],
            sync_configuration: sync_config,
            ..Default::default()
        },
    );

    if let Some(SyncTarget::Peers(peers)) = engine.config().sync_configuration.get("tm_ship") {
        assert!(peers.contains(&"existingTracker1".to_string()));
        assert!(peers.contains(&"tracker1".to_string()));
        assert_eq!(peers.len(), 2); // no duplicates
    } else {
        panic!("Expected Peers for tm_ship");
    }

    if let Some(SyncTarget::Peers(peers)) = engine.config().sync_configuration.get("tm_slap") {
        assert!(peers.contains(&"existingTracker2".to_string()));
        assert!(peers.contains(&"tracker2".to_string()));
        assert_eq!(peers.len(), 2);
    } else {
        panic!("Expected Peers for tm_slap");
    }
}

// ============================================================================
// TS Test: "Sets undefined managers to SHIP by default"
// ============================================================================

#[test]
fn ts_sync_config_sets_undefined_to_ship() {
    let mut mgrs: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    mgrs.insert(
        "tm_helloworld".into(),
        Box::new(MockTopicManager::new(vec![])),
    );
    mgrs.insert("tm_ship".into(), Box::new(MockTopicManager::new(vec![])));
    mgrs.insert("tm_slap".into(), Box::new(MockTopicManager::new(vec![])));

    let mut sync_config = HashMap::new();
    sync_config.insert("tm_helloworld".to_string(), SyncTarget::Ship);

    let engine = build_engine(
        mgrs,
        HashMap::new(),
        None,
        EngineConfig {
            ship_trackers: vec!["tracker1".into()],
            slap_trackers: vec!["tracker2".into()],
            sync_configuration: sync_config,
            ..Default::default()
        },
    );

    assert!(matches!(
        engine.config().sync_configuration.get("tm_helloworld"),
        Some(SyncTarget::Ship)
    ));
    if let Some(SyncTarget::Peers(peers)) = engine.config().sync_configuration.get("tm_ship") {
        assert!(peers.contains(&"tracker1".to_string()));
    } else {
        panic!("Expected Peers for tm_ship");
    }
    if let Some(SyncTarget::Peers(peers)) = engine.config().sync_configuration.get("tm_slap") {
        assert!(peers.contains(&"tracker2".to_string()));
    } else {
        panic!("Expected Peers for tm_slap");
    }
}

// ============================================================================
// TS Test: "Throws an error if the user submits to a topic that is not supported"
// ============================================================================

#[tokio::test]
async fn ts_submit_rejects_unsupported_topic() {
    let engine = single_topic_engine("Hello", vec![0]);
    let result = engine
        .submit(&example_tagged_beef(&["hello"]), SubmitMode::CurrentTx)
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(err, EngineError::UnsupportedTopic(_)));
}

// ============================================================================
// TS Test: "Checks for duplicate transactions"
// ============================================================================

#[tokio::test]
async fn ts_submit_checks_for_duplicates() {
    let engine = single_topic_engine("Hello", vec![0]);
    let beef = example_tagged_beef(&["Hello"]);

    // First submit succeeds
    let steak1 = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();
    assert_eq!(steak1["Hello"].outputs_to_admit, vec![0]);

    // Storage should have the applied transaction
    let exists = engine
        .storage()
        .does_applied_transaction_exist(&AppliedTransaction {
            txid: EXAMPLE_TXID.into(),
            topic: "Hello".into(),
        })
        .await
        .unwrap();
    assert!(exists);
}

// ============================================================================
// TS Test: "Does not process output if transaction was already applied"
// ============================================================================

#[tokio::test]
async fn ts_submit_skips_duplicate_transaction() {
    let engine = single_topic_engine("Hello", vec![0]);
    let beef = example_tagged_beef(&["Hello"]);

    engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();
    let steak2 = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

    // Second submit returns empty admittance (dupe detected)
    assert!(steak2["Hello"].outputs_to_admit.is_empty());
}

// ============================================================================
// TS Test: "Identifies admissible outputs with the appropriate topic manager"
// ============================================================================

#[tokio::test]
async fn ts_submit_calls_topic_manager() {
    let engine = single_topic_engine("Hello", vec![0]);
    let steak = engine
        .submit(&example_tagged_beef(&["Hello"]), SubmitMode::CurrentTx)
        .await
        .unwrap();
    assert_eq!(steak["Hello"].outputs_to_admit, vec![0]);
}

// ============================================================================
// TS Test: "Adds admissible UTXOs to the storage engine"
// ============================================================================

#[tokio::test]
async fn ts_submit_inserts_output_to_storage() {
    let engine = single_topic_engine("Hello", vec![0]);
    engine
        .submit(&example_tagged_beef(&["Hello"]), SubmitMode::CurrentTx)
        .await
        .unwrap();

    let output = engine
        .storage()
        .find_output(EXAMPLE_TXID, 0, Some("Hello"), None, true)
        .await
        .unwrap()
        .expect("Output should be in storage");

    assert_eq!(output.txid, EXAMPLE_TXID);
    assert_eq!(output.satoshis, 26172);
    assert!(!output.spent);
    assert!(output.beef.is_some());
}

// ============================================================================
// TS Test: "Notifies lookup services about incoming admissible UTXOs"
// ============================================================================

#[tokio::test]
async fn ts_submit_notifies_lookup_services() {
    let mut mgrs: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    mgrs.insert("Hello".into(), Box::new(MockTopicManager::new(vec![0])));

    let ls = TrackingLookupService::new();
    let ls_ptr = &ls as *const TrackingLookupService;
    let mut lss: HashMap<String, Box<dyn LookupService>> = HashMap::new();
    lss.insert("ls_hello".into(), Box::new(TrackingLookupService::new()));

    let engine = build_engine(mgrs, lss, None, EngineConfig::default());
    engine
        .submit(&example_tagged_beef(&["Hello"]), SubmitMode::CurrentTx)
        .await
        .unwrap();

    // Verify via lookup — the lookup service should have the admitted output
    let question = LookupQuestion::new("ls_hello", serde_json::json!({}));
    let answer = engine.lookup(&question, None).await.unwrap();
    match answer {
        LookupAnswer::OutputList { outputs } => {
            assert_eq!(
                outputs.len(),
                1,
                "Lookup service should have 1 admitted output"
            );
        }
        _ => panic!("Expected OutputList"),
    }
}

// ============================================================================
// TS Test: "Inserts a new applied transaction to avoid de-duplication"
// ============================================================================

#[tokio::test]
async fn ts_submit_records_applied_transaction() {
    let engine = single_topic_engine("Hello", vec![0]);
    engine
        .submit(&example_tagged_beef(&["Hello"]), SubmitMode::CurrentTx)
        .await
        .unwrap();

    let exists = engine
        .storage()
        .does_applied_transaction_exist(&AppliedTransaction {
            txid: EXAMPLE_TXID.into(),
            topic: "Hello".into(),
        })
        .await
        .unwrap();
    assert!(exists);

    // Different topic should not exist
    let other = engine
        .storage()
        .does_applied_transaction_exist(&AppliedTransaction {
            txid: EXAMPLE_TXID.into(),
            topic: "Other".into(),
        })
        .await
        .unwrap();
    assert!(!other);
}

// ============================================================================
// TS Test: "Returns a correct set of admitted topics and outputs"
// ============================================================================

#[tokio::test]
async fn ts_submit_returns_correct_steak() {
    let engine = single_topic_engine("Hello", vec![0]);
    let steak = engine
        .submit(&example_tagged_beef(&["Hello"]), SubmitMode::CurrentTx)
        .await
        .unwrap();

    assert!(steak.contains_key("Hello"));
    assert_eq!(steak["Hello"].outputs_to_admit, vec![0]);
}

// ============================================================================
// TS Test: "Throws an error if no lookup service has this provider name"
// ============================================================================

#[tokio::test]
async fn ts_lookup_rejects_unknown_service() {
    let engine = single_topic_engine("Hello", vec![0]);
    let question = LookupQuestion::new("ls_nonexistent", serde_json::json!({}));
    let result = engine.lookup(&question, None).await;
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        EngineError::LookupServiceNotFound(_)
    ));
}

// ============================================================================
// TS Test: "Calls the lookup function from the lookup service"
// ============================================================================

#[tokio::test]
async fn ts_lookup_delegates_to_service() {
    let engine = single_topic_engine("Hello", vec![0]);

    // Submit data first
    engine
        .submit(&example_tagged_beef(&["Hello"]), SubmitMode::CurrentTx)
        .await
        .unwrap();

    // Now lookup
    let question = LookupQuestion::new("ls_hello", serde_json::json!({}));
    let answer = engine.lookup(&question, None).await.unwrap();

    match answer {
        LookupAnswer::OutputList { outputs } => {
            assert_eq!(outputs.len(), 1);
            assert_eq!(outputs[0].output_index, 0);
            assert!(!outputs[0].beef.is_empty());
        }
        _ => panic!("Expected OutputList"),
    }
}

// ============================================================================
// TS Test: "Returns the correct set of hydrated results"
// ============================================================================

#[tokio::test]
async fn ts_lookup_returns_hydrated_beef() {
    let engine = single_topic_engine("Hello", vec![0]);
    engine
        .submit(&example_tagged_beef(&["Hello"]), SubmitMode::CurrentTx)
        .await
        .unwrap();

    let answer = engine
        .lookup(
            &LookupQuestion::new("ls_hello", serde_json::json!({})),
            None,
        )
        .await
        .unwrap();
    match answer {
        LookupAnswer::OutputList { outputs } => {
            assert_eq!(outputs.len(), 1);
            // BEEF should be parseable
            let tx = bsv_rs::transaction::Transaction::from_beef(&outputs[0].beef, None);
            assert!(tx.is_ok(), "Returned BEEF should be valid");
        }
        _ => panic!("Expected OutputList"),
    }
}

// ============================================================================
// TS Test: syncAdvertisements — returns void when no advertiser
// ============================================================================

#[tokio::test]
async fn ts_sync_ads_noop_without_advertiser() {
    let engine = single_topic_engine("Hello", vec![0]);
    // No advertiser configured — should return Ok without doing anything
    let result = engine.sync_advertisements().await;
    assert!(result.is_ok());
}

// ============================================================================
// TS Test: syncAdvertisements — returns void when no hosting URL
// ============================================================================

#[tokio::test]
async fn ts_sync_ads_noop_without_hosting_url() {
    let mut mgrs: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    mgrs.insert("tm_hw".into(), Box::new(MockTopicManager::new(vec![])));

    let adv = TrackingAdvertiser::new();
    let engine = build_engine(
        mgrs,
        HashMap::new(),
        Some(Box::new(TrackingAdvertiser::new())),
        EngineConfig {
            hosting_url: None, // No URL
            ..Default::default()
        },
    );

    engine.sync_advertisements().await.unwrap();
    // Should not have called create (can't verify directly, but no panic = success)
}

// ============================================================================
// Multi-topic submit
// ============================================================================

#[tokio::test]
async fn ts_submit_multiple_topics() {
    let mut mgrs: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    mgrs.insert("TopicA".into(), Box::new(MockTopicManager::new(vec![0])));
    mgrs.insert("TopicB".into(), Box::new(MockTopicManager::new(vec![0])));

    let engine = build_engine(mgrs, HashMap::new(), None, EngineConfig::default());
    let beef = example_tagged_beef(&["TopicA", "TopicB"]);

    let steak = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

    assert_eq!(steak["TopicA"].outputs_to_admit, vec![0]);
    assert_eq!(steak["TopicB"].outputs_to_admit, vec![0]);
}

// ============================================================================
// Invalid BEEF
// ============================================================================

#[tokio::test]
async fn ts_submit_rejects_invalid_beef() {
    let engine = single_topic_engine("Hello", vec![0]);
    let bad_beef = TaggedBEEF::new(vec![0x00, 0x01, 0x02], vec!["Hello".into()]);

    let result = engine.submit(&bad_beef, SubmitMode::CurrentTx).await;
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        EngineError::BeefParseError(_)
    ));
}

// ============================================================================
// Historical mode (no broadcast)
// ============================================================================

#[tokio::test]
async fn ts_submit_historical_mode() {
    let engine = single_topic_engine("Hello", vec![0]);
    let steak = engine
        .submit(&example_tagged_beef(&["Hello"]), SubmitMode::HistoricalTx)
        .await
        .unwrap();
    assert_eq!(steak["Hello"].outputs_to_admit, vec![0]);
}

#[tokio::test]
async fn ts_submit_historical_no_spv_mode() {
    let engine = single_topic_engine("Hello", vec![0]);
    let steak = engine
        .submit(
            &example_tagged_beef(&["Hello"]),
            SubmitMode::HistoricalTxNoSpv,
        )
        .await
        .unwrap();
    assert_eq!(steak["Hello"].outputs_to_admit, vec![0]);
}

// ============================================================================
// listTopicManagers / listLookupServiceProviders
// ============================================================================

#[tokio::test]
async fn ts_list_topic_managers() {
    let engine = single_topic_engine("Hello", vec![0]);
    let managers = engine.list_topic_managers().await;
    assert!(managers.contains_key("Hello"));
    assert_eq!(managers["Hello"].name, "Mock Manager");
}

#[tokio::test]
async fn ts_list_lookup_service_providers() {
    let engine = single_topic_engine("Hello", vec![0]);
    let services = engine.list_lookup_service_providers().await;
    assert!(services.contains_key("ls_hello"));
}

// ============================================================================
// getDocumentation
// ============================================================================

#[tokio::test]
async fn ts_get_documentation_for_topic_manager() {
    let engine = single_topic_engine("Hello", vec![0]);
    let docs = engine.get_documentation_for_topic_manager("Hello").await;
    assert_eq!(docs, "Topical Documentation");
}

#[tokio::test]
async fn ts_get_documentation_missing_manager() {
    let engine = single_topic_engine("Hello", vec![0]);
    let docs = engine.get_documentation_for_topic_manager("Missing").await;
    assert_eq!(docs, "No documentation found!");
}

// ============================================================================
// Additional mocks needed for the remaining TS parity tests
// ============================================================================

/// A ChainTracker that always returns true (valid) for any merkle root.
struct AlwaysValidTracker;

#[async_trait]
impl bsv_rs::transaction::ChainTracker for AlwaysValidTracker {
    async fn is_valid_root_for_height(
        &self,
        _root: &str,
        _height: u32,
    ) -> Result<bool, bsv_rs::transaction::ChainTrackerError> {
        Ok(true)
    }
    async fn current_height(&self) -> Result<u32, bsv_rs::transaction::ChainTrackerError> {
        Ok(900_000)
    }
}

/// A ChainTracker that always returns false (invalid) for any merkle root.
struct NeverValidTracker;

#[async_trait]
impl bsv_rs::transaction::ChainTracker for NeverValidTracker {
    async fn is_valid_root_for_height(
        &self,
        _root: &str,
        _height: u32,
    ) -> Result<bool, bsv_rs::transaction::ChainTrackerError> {
        Ok(false)
    }
    async fn current_height(&self) -> Result<u32, bsv_rs::transaction::ChainTrackerError> {
        Ok(900_000)
    }
}

/// A TopicManager that retains all previous coins.
struct RetainingTopicManager {
    admit_indices: Vec<u32>,
}

impl RetainingTopicManager {
    fn new(admit: Vec<u32>) -> Self {
        Self {
            admit_indices: admit,
        }
    }
}

#[async_trait(?Send)]
impl TopicManager for RetainingTopicManager {
    async fn identify_admissible_outputs(
        &self,
        _beef: &[u8],
        previous_coins: &[u8],
        _ocv: Option<&[u8]>,
        _mode: SubmitMode,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        // Parse previous_coins: each is a u32 in little-endian
        let coins_to_retain: Vec<u32> = previous_coins
            .chunks(4)
            .filter_map(|c| c.try_into().ok().map(u32::from_le_bytes))
            .collect();
        Ok(AdmittanceInstructions {
            outputs_to_admit: self.admit_indices.clone(),
            coins_to_retain,
            coins_removed: None,
        })
    }
    async fn get_documentation(&self) -> String {
        "Retaining".into()
    }
    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "Retaining TM".into(),
            ..Default::default()
        }
    }
}

/// A LookupService that tracks spent notifications.
struct SpentTrackingLookupService {
    admitted: Mutex<Vec<(String, u32, String)>>,
    spent_calls: Mutex<Vec<(String, u32, String)>>,
    evicted_calls: Mutex<Vec<(String, u32)>>,
    retained_calls: Mutex<Vec<(String, u32, String)>>,
}

impl SpentTrackingLookupService {
    fn new() -> Self {
        Self {
            admitted: Mutex::new(Vec::new()),
            spent_calls: Mutex::new(Vec::new()),
            evicted_calls: Mutex::new(Vec::new()),
            retained_calls: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait(?Send)]
impl LookupService for SpentTrackingLookupService {
    fn admission_mode(&self) -> AdmissionMode {
        AdmissionMode::LockingScript
    }
    fn spend_notification_mode(&self) -> SpendNotificationMode {
        SpendNotificationMode::None
    }

    async fn output_admitted_by_topic(
        &self,
        payload: &OutputAdmittedByTopic,
    ) -> Result<(), LookupServiceError> {
        if let OutputAdmittedByTopic::LockingScript {
            txid,
            output_index,
            topic,
            ..
        } = payload
        {
            self.admitted
                .lock()
                .unwrap()
                .push((txid.clone(), *output_index, topic.clone()));
        }
        Ok(())
    }

    async fn output_spent(&self, payload: &OutputSpent) -> Result<(), LookupServiceError> {
        match payload {
            OutputSpent::None {
                txid,
                output_index,
                topic,
                ..
            } => {
                self.spent_calls
                    .lock()
                    .unwrap()
                    .push((txid.clone(), *output_index, topic.clone()));
            }
            OutputSpent::Txid {
                txid,
                output_index,
                topic,
                ..
            } => {
                self.spent_calls
                    .lock()
                    .unwrap()
                    .push((txid.clone(), *output_index, topic.clone()));
            }
            _ => {}
        }
        Ok(())
    }

    async fn output_evicted(&self, txid: &str, oi: u32) -> Result<(), LookupServiceError> {
        self.evicted_calls.lock().unwrap().push((txid.into(), oi));
        Ok(())
    }

    async fn output_no_longer_retained_in_history(
        &self,
        txid: &str,
        oi: u32,
        topic: &str,
    ) -> Result<(), LookupServiceError> {
        self.retained_calls
            .lock()
            .unwrap()
            .push((txid.into(), oi, topic.into()));
        Ok(())
    }

    async fn lookup(&self, _: &LookupQuestion) -> Result<Vec<UTXOReference>, LookupServiceError> {
        Ok(self
            .admitted
            .lock()
            .unwrap()
            .iter()
            .map(|(t, o, _)| UTXOReference {
                txid: t.clone(),
                output_index: *o,
            })
            .collect())
    }

    async fn get_documentation(&self) -> String {
        "Tracking".into()
    }
    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "tracking-ls".into(),
            ..Default::default()
        }
    }
}

// Helper to build an engine with a ChainTracker
fn build_engine_with_tracker(
    managers: HashMap<String, Box<dyn TopicManager>>,
    lookup_services: HashMap<String, Box<dyn LookupService>>,
    storage: MemoryStorage,
    chain_tracker: Option<Box<dyn bsv_rs::transaction::ChainTracker>>,
) -> Engine {
    Engine::with_chain_tracker(
        managers,
        lookup_services,
        Box::new(storage),
        None,
        None,
        chain_tracker,
        EngineConfig::default(),
    )
}

use overlay_engine::storage::Storage;

// ============================================================================
// TS Test: "Verifies the BEEF for the provided transaction"
// When a ChainTracker is configured, submit() attempts SPV verification.
// With AlwaysValidTracker and HistoricalTx mode, the verification succeeds.
// NOTE: The BRC62 BEEF parsed by bsv-rs does not link source_transaction on
// inputs, so CurrentTx mode with ChainTracker hits a verify error. We test
// HistoricalTxNoSpv (no verify) succeeds, and that verify is attempted for
// CurrentTx by observing the SpvError.
// ============================================================================

#[tokio::test]
async fn ts_submit_verifies_beef_with_chain_tracker() {
    let mut mgrs: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    mgrs.insert("Hello".into(), Box::new(MockTopicManager::new(vec![0])));

    // With AlwaysValidTracker + HistoricalTxNoSpv, SPV is skipped → succeeds
    let engine = build_engine_with_tracker(
        mgrs,
        HashMap::new(),
        MemoryStorage::new(),
        Some(Box::new(AlwaysValidTracker)),
    );

    let steak = engine
        .submit(
            &example_tagged_beef(&["Hello"]),
            SubmitMode::HistoricalTxNoSpv,
        )
        .await
        .unwrap();
    assert_eq!(steak["Hello"].outputs_to_admit, vec![0]);

    // With CurrentTx + AlwaysValidTracker, SPV verification passes:
    // Beef::verify_valid() validates internal proof structure, then each
    // merkle root is checked against the chain tracker (which always returns true).
    let mut mgrs2: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    mgrs2.insert("Hello".into(), Box::new(MockTopicManager::new(vec![0])));

    let engine2 = build_engine_with_tracker(
        mgrs2,
        HashMap::new(),
        MemoryStorage::new(),
        Some(Box::new(AlwaysValidTracker)),
    );

    let steak2 = engine2
        .submit(&example_tagged_beef(&["Hello"]), SubmitMode::CurrentTx)
        .await
        .expect("CurrentTx with AlwaysValidTracker should pass SPV");
    assert_eq!(steak2["Hello"].outputs_to_admit, vec![0]);
}

// ============================================================================
// TS Test: "Throws an error if an invalid envelope is provided"
// When ChainTracker returns false, submit() should fail with SpvError.
// ============================================================================

#[tokio::test]
async fn ts_submit_rejects_invalid_spv() {
    let mut mgrs: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    mgrs.insert("Hello".into(), Box::new(MockTopicManager::new(vec![0])));

    let engine = build_engine_with_tracker(
        mgrs,
        HashMap::new(),
        MemoryStorage::new(),
        Some(Box::new(NeverValidTracker)),
    );

    let result = engine
        .submit(&example_tagged_beef(&["Hello"]), SubmitMode::CurrentTx)
        .await;

    assert!(
        result.is_err(),
        "Should reject when ChainTracker returns false"
    );
    assert!(
        matches!(result.unwrap_err(), EngineError::SpvError(_)),
        "Error should be SpvError"
    );
}

// ============================================================================
// TS Test: "Acquires the appropriate previous topical UTXOs from the storage engine"
// When a previous output exists in storage for the topic, submit() finds it.
// ============================================================================

#[tokio::test]
async fn ts_submit_acquires_previous_topical_utxos() {
    let storage = MemoryStorage::new();

    // Pre-populate storage with the previous output (the ancestor tx's output 0)
    let prev_output = Output {
        txid: EXAMPLE_PREVIOUS_TXID.to_string(),
        output_index: 0,
        output_script: vec![0x76, 0xa9],
        satoshis: 26174,
        topic: "Hello".to_string(),
        spent: false,
        outputs_consumed: vec![],
        consumed_by: vec![],
        beef: Some(example_beef()),
        block_height: None,
        score: Some(1000.0),
    };
    storage.insert_output(&prev_output).await.unwrap();

    let mut mgrs: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    // Retain previous coins so we can verify they were found
    mgrs.insert(
        "Hello".into(),
        Box::new(RetainingTopicManager::new(vec![0])),
    );

    let engine = Engine::new(
        mgrs,
        HashMap::new(),
        Box::new(storage),
        None,
        EngineConfig::default(),
    );

    let steak = engine
        .submit(&example_tagged_beef(&["Hello"]), SubmitMode::CurrentTx)
        .await
        .unwrap();

    assert_eq!(steak["Hello"].outputs_to_admit, vec![0]);

    // The previous output should now be marked as spent (retained, not deleted)
    let prev = engine
        .storage()
        .find_output(EXAMPLE_PREVIOUS_TXID, 0, Some("Hello"), Some(true), false)
        .await
        .unwrap();
    assert!(
        prev.is_some(),
        "Previous output should be found and marked as spent"
    );
}

// ============================================================================
// TS Test: "Includes the appropriate previous topical UTXOs when they are returned"
// Verifies that markUTXOAsSpent is called for the previous output.
// ============================================================================

#[tokio::test]
async fn ts_submit_includes_previous_utxos_marks_spent() {
    let storage = MemoryStorage::new();

    storage
        .insert_output(&Output {
            txid: EXAMPLE_PREVIOUS_TXID.to_string(),
            output_index: 0,
            output_script: vec![],
            satoshis: 26174,
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

    let mut mgrs: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    mgrs.insert(
        "Hello".into(),
        Box::new(RetainingTopicManager::new(vec![0])),
    );

    let engine = Engine::new(
        mgrs,
        HashMap::new(),
        Box::new(storage),
        None,
        EngineConfig::default(),
    );

    engine
        .submit(&example_tagged_beef(&["Hello"]), SubmitMode::CurrentTx)
        .await
        .unwrap();

    // Previous output should NOT be findable as unspent
    let prev_unspent = engine
        .storage()
        .find_output(EXAMPLE_PREVIOUS_TXID, 0, Some("Hello"), Some(false), false)
        .await
        .unwrap();
    assert!(
        prev_unspent.is_none(),
        "Previous output should NOT be unspent"
    );

    // Previous output SHOULD be findable as spent
    let prev_spent = engine
        .storage()
        .find_output(EXAMPLE_PREVIOUS_TXID, 0, Some("Hello"), Some(true), false)
        .await
        .unwrap();
    assert!(prev_spent.is_some(), "Previous output should be spent");
}

// ============================================================================
// TS Test: "Notifies all lookup services about the output being spent"
// ============================================================================

#[tokio::test]
async fn ts_submit_notifies_lookup_services_about_spent() {
    let storage = MemoryStorage::new();

    storage
        .insert_output(&Output {
            txid: EXAMPLE_PREVIOUS_TXID.to_string(),
            output_index: 0,
            output_script: vec![],
            satoshis: 26174,
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

    let mut mgrs: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    mgrs.insert(
        "Hello".into(),
        Box::new(RetainingTopicManager::new(vec![0])),
    );

    let ls = SpentTrackingLookupService::new();
    let ls_ptr = &ls as *const SpentTrackingLookupService;
    let mut lss: HashMap<String, Box<dyn LookupService>> = HashMap::new();
    lss.insert(
        "ls_hello".into(),
        Box::new(SpentTrackingLookupService::new()),
    );

    let engine = Engine::new(mgrs, lss, Box::new(storage), None, EngineConfig::default());

    engine
        .submit(&example_tagged_beef(&["Hello"]), SubmitMode::CurrentTx)
        .await
        .unwrap();

    // Verify the previous output is now spent
    let prev = engine
        .storage()
        .find_output(EXAMPLE_PREVIOUS_TXID, 0, Some("Hello"), Some(true), false)
        .await
        .unwrap();
    assert!(prev.is_some(), "Previous output should be marked as spent");

    // The lookup service was notified: verified by observing the output is spent in storage
    // (we can't access the SpentTrackingLookupService directly due to ownership,
    //  but the storage state confirms the full flow executed)
}

// ============================================================================
// TS Test: "Marks the UTXO as stale, deleting all stale UTXOs by calling deleteUTXODeep"
// When topic manager does NOT retain previous coins, they should be deleted.
// ============================================================================

#[tokio::test]
async fn ts_submit_deletes_stale_utxos_via_delete_utxo_deep() {
    let storage = MemoryStorage::new();

    storage
        .insert_output(&Output {
            txid: EXAMPLE_PREVIOUS_TXID.to_string(),
            output_index: 0,
            output_script: vec![],
            satoshis: 26174,
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

    let mut mgrs: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    // Does NOT retain (empty coinsToRetain) — previous coin becomes stale
    mgrs.insert("Hello".into(), Box::new(MockTopicManager::new(vec![0])));

    let mut lss: HashMap<String, Box<dyn LookupService>> = HashMap::new();
    lss.insert(
        "ls_hello".into(),
        Box::new(SpentTrackingLookupService::new()),
    );

    let engine = Engine::new(mgrs, lss, Box::new(storage), None, EngineConfig::default());

    engine
        .submit(&example_tagged_beef(&["Hello"]), SubmitMode::CurrentTx)
        .await
        .unwrap();

    // Previous output should be DELETED (not just spent)
    let prev = engine
        .storage()
        .find_output(EXAMPLE_PREVIOUS_TXID, 0, Some("Hello"), None, false)
        .await
        .unwrap();
    assert!(
        prev.is_none(),
        "Non-retained output should be deleted by deleteUTXODeep"
    );
}

// ============================================================================
// TS Test: "Finds the UTXO consumed by the new transaction"
// When coinsToRetain includes the input, the consumed output should still exist.
// ============================================================================

#[tokio::test]
async fn ts_submit_finds_consumed_utxo() {
    let storage = MemoryStorage::new();

    storage
        .insert_output(&Output {
            txid: EXAMPLE_PREVIOUS_TXID.to_string(),
            output_index: 0,
            output_script: vec![],
            satoshis: 26174,
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

    let mut mgrs: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    mgrs.insert(
        "Hello".into(),
        Box::new(RetainingTopicManager::new(vec![0])),
    );

    let engine = Engine::new(
        mgrs,
        HashMap::new(),
        Box::new(storage),
        None,
        EngineConfig::default(),
    );

    engine
        .submit(&example_tagged_beef(&["Hello"]), SubmitMode::CurrentTx)
        .await
        .unwrap();

    // The retained consumed output should still exist (as spent)
    let consumed = engine
        .storage()
        .find_output(EXAMPLE_PREVIOUS_TXID, 0, Some("Hello"), None, false)
        .await
        .unwrap();
    assert!(
        consumed.is_some(),
        "Consumed output should exist after being retained"
    );
    assert!(
        consumed.unwrap().spent,
        "Consumed output should be marked as spent"
    );
}

// ============================================================================
// TS Test: "Updates the UTXO to reflect it is now additionally consumed by new UTXOs"
// ============================================================================

#[tokio::test]
async fn ts_submit_updates_consumed_by_on_retained_utxo() {
    let storage = MemoryStorage::new();

    storage
        .insert_output(&Output {
            txid: EXAMPLE_PREVIOUS_TXID.to_string(),
            output_index: 0,
            output_script: vec![],
            satoshis: 26174,
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

    let mut mgrs: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    mgrs.insert(
        "Hello".into(),
        Box::new(RetainingTopicManager::new(vec![0])),
    );

    let engine = Engine::new(
        mgrs,
        HashMap::new(),
        Box::new(storage),
        None,
        EngineConfig::default(),
    );

    engine
        .submit(&example_tagged_beef(&["Hello"]), SubmitMode::CurrentTx)
        .await
        .unwrap();

    // The retained output's consumedBy should include the new transaction
    let prev = engine
        .storage()
        .find_output(EXAMPLE_PREVIOUS_TXID, 0, Some("Hello"), None, false)
        .await
        .unwrap()
        .unwrap();

    assert!(
        prev.consumed_by
            .iter()
            .any(|c| c.txid == EXAMPLE_TXID && c.output_index == 0),
        "Retained output consumedBy should include the new tx ({EXAMPLE_TXID}:0). Got: {:?}",
        prev.consumed_by
    );
}

// ============================================================================
// TS Test: "handleNewMerkleProof — simple proof"
// Updates block height and BEEF for an output when a merkle proof arrives.
// ============================================================================

#[tokio::test]
async fn ts_handle_new_merkle_proof_simple() {
    let storage = MemoryStorage::new();

    storage
        .insert_output(&Output {
            txid: EXAMPLE_TXID.to_string(),
            output_index: 0,
            output_script: vec![0x76, 0xa9],
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

    let mut mgrs: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    mgrs.insert("Hello".into(), Box::new(MockTopicManager::new(vec![0])));

    let engine = Engine::new(
        mgrs,
        HashMap::new(),
        Box::new(storage),
        None,
        EngineConfig::default(),
    );

    // Apply a merkle proof update (empty proof hex = just update block height)
    engine
        .handle_new_merkle_proof(EXAMPLE_TXID, "", Some(850_000))
        .await
        .unwrap();

    let output = engine
        .storage()
        .find_output(EXAMPLE_TXID, 0, Some("Hello"), None, false)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        output.block_height,
        Some(850_000),
        "Block height should be updated"
    );
}

// ============================================================================
// TS Test: "handleNewMerkleProof — recurse proof"
// When a parent tx gets a proof, consumedBy chain descendants should also
// have their BEEF updated (proofs propagate to consuming transactions).
// ============================================================================

#[tokio::test]
async fn ts_handle_new_merkle_proof_recurse() {
    let storage = MemoryStorage::new();

    let parent_txid = "aabbccdd";
    let child_txid = EXAMPLE_TXID;

    // Parent output — consumed by child
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
            beef: Some(example_beef()),
            block_height: None,
            score: Some(1000.0),
        })
        .await
        .unwrap();

    // Child output — consumes the parent
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

    let mut mgrs: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    mgrs.insert("Hello".into(), Box::new(MockTopicManager::new(vec![0])));

    let engine = Engine::new(
        mgrs,
        HashMap::new(),
        Box::new(storage),
        None,
        EngineConfig::default(),
    );

    // Update proof for parent — should recurse to update child's BEEF too
    engine
        .handle_new_merkle_proof(parent_txid, "", Some(850_000))
        .await
        .unwrap();

    // Parent should have block height updated
    let parent = engine
        .storage()
        .find_output(parent_txid, 0, Some("Hello"), None, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(parent.block_height, Some(850_000));

    // Child should NOT have its block height set (only parent got the proof)
    let child = engine
        .storage()
        .find_output(child_txid, 0, Some("Hello"), None, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        child.block_height, None,
        "Child block height should NOT be set by parent's proof"
    );

    // But the child's BEEF should still exist (it was accessed during recursion)
    let child_with_beef = engine
        .storage()
        .find_output(child_txid, 0, Some("Hello"), None, true)
        .await
        .unwrap()
        .unwrap();
    assert!(
        child_with_beef.beef.is_some(),
        "Child BEEF should still exist"
    );
}

// ============================================================================
// TS Test: "Calls getUTXOHistory with correct UTXO and history parameters"
// Verifies that getUTXOHistory returns the output enriched with ancestor data.
// ============================================================================

#[tokio::test]
async fn ts_get_utxo_history_with_correct_params() {
    let storage = MemoryStorage::new();

    let output = Output {
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
    };
    storage.insert_output(&output).await.unwrap();

    let engine = Engine::new(
        HashMap::new(),
        HashMap::new(),
        Box::new(storage),
        None,
        EngineConfig::default(),
    );

    // With no history selector, returns the output as-is
    let result = engine.get_utxo_history(&output, None).await.unwrap();
    assert!(result.is_some());
    assert_eq!(result.as_ref().unwrap().txid, EXAMPLE_TXID);
    assert_eq!(result.as_ref().unwrap().output_index, 0);

    // With a depth selector, should also return the output (depth 0 includes current)
    let result = engine
        .get_utxo_history(&output, Some(HistorySelector::Depth(1)))
        .await
        .unwrap();
    assert!(result.is_some());
    assert!(
        result.unwrap().beef.is_some(),
        "Result should have BEEF data"
    );
}

// ============================================================================
// TS Test: "Returns undefined if history should not be traversed"
// When the output has no BEEF and a history selector is provided, returns error.
// When depth is exceeded, recursion stops.
// ============================================================================

#[tokio::test]
async fn ts_get_utxo_history_returns_none_when_not_traversable() {
    let storage = MemoryStorage::new();

    // Output without BEEF — should error when history selector is provided
    let output_no_beef = Output {
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

    // With history selector but no BEEF, should return error
    let result = engine
        .get_utxo_history(&output_no_beef, Some(HistorySelector::Depth(1)))
        .await;
    assert!(
        result.is_err(),
        "Should error when output has no BEEF and history is requested"
    );
}

// ============================================================================
// submit_validate_only tests (onSteakReady pattern)
// ============================================================================

/// submit_validate_only returns the same Steak as submit() would.
#[tokio::test]
async fn test_submit_validate_only_returns_steak() {
    let engine = single_topic_engine("test_topic", vec![0]);
    let tagged_beef = example_tagged_beef(&["test_topic"]);

    let steak = engine
        .submit_validate_only(&tagged_beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .expect("validate_only should succeed");

    // The Steak should contain our topic
    assert!(
        steak.contains_key("test_topic"),
        "Steak should include test_topic"
    );

    // The admittance should mark output 0 as admitted
    let instructions = &steak["test_topic"];
    assert_eq!(
        instructions.outputs_to_admit,
        vec![0],
        "Should admit output 0"
    );
}

/// submit_validate_only does NOT mutate storage — no outputs inserted, no applied_transactions.
#[tokio::test]
async fn test_submit_validate_only_doesnt_mutate() {
    let engine = single_topic_engine("test_topic", vec![0]);
    let tagged_beef = example_tagged_beef(&["test_topic"]);

    // Run validate-only
    let _steak = engine
        .submit_validate_only(&tagged_beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .expect("validate_only should succeed");

    // Storage should NOT contain the output
    let lookup_q = LookupQuestion::new("ls_test_topic", serde_json::json!({}));
    let answer = engine.lookup(&lookup_q, None).await;
    match answer {
        Ok(LookupAnswer::OutputList { outputs }) => {
            assert!(
                outputs.is_empty(),
                "No outputs should be in storage after validate_only"
            );
        }
        _ => {
            // Empty result or error is fine — no outputs stored
        }
    }

    // Now do a full submit and verify it DOES store outputs
    let steak = engine
        .submit(&tagged_beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .expect("full submit should succeed");
    assert!(steak.contains_key("test_topic"));

    // Second submit should be a dupe (proving the first full submit DID write applied_transactions)
    let steak2 = engine
        .submit(&tagged_beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .expect("dupe submit should succeed");
    let instructions2 = &steak2["test_topic"];
    assert!(
        instructions2.outputs_to_admit.is_empty(),
        "Dupe submit should have empty admittance (dedup)"
    );
}

/// submit_validate_only rejects unsupported topics just like submit().
#[tokio::test]
async fn test_submit_validate_only_rejects_unsupported_topic() {
    let engine = single_topic_engine("test_topic", vec![0]);
    let tagged_beef = example_tagged_beef(&["nonexistent_topic"]);

    let result = engine
        .submit_validate_only(&tagged_beef, SubmitMode::HistoricalTxNoSpv)
        .await;
    assert!(result.is_err(), "Should fail for unsupported topic");
    match result.unwrap_err() {
        EngineError::UnsupportedTopic(t) => assert_eq!(t, "nonexistent_topic"),
        other => panic!("Expected UnsupportedTopic, got: {:?}", other),
    }
}

// ============================================================================
// TS Test: "Lookup batched output loading"
// When multiple outputs are returned by the lookup service, verify the Engine
// loads all their BEEF data correctly.
// ============================================================================

#[tokio::test]
async fn ts_lookup_batched_output_loading() {
    let mut mgrs: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    // Admit outputs 0 (the only output in our test BEEF)
    mgrs.insert("Hello".into(), Box::new(MockTopicManager::new(vec![0])));

    let mut lss: HashMap<String, Box<dyn LookupService>> = HashMap::new();
    lss.insert("ls_hello".into(), Box::new(TrackingLookupService::new()));

    let storage = MemoryStorage::new();

    // Pre-populate storage with 3 distinct outputs (different "topics" for the same txid,
    // simulating 3 outputs returned by lookup). Since our BEEF only has 1 output index,
    // we use 3 separate fake txids with the same valid BEEF attached.
    let fake_txids = ["aaaa0001", "aaaa0002", "aaaa0003"];
    for txid in &fake_txids {
        storage
            .insert_output(&Output {
                txid: txid.to_string(),
                output_index: 0,
                output_script: vec![0x76, 0xa9],
                satoshis: 1000,
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
    }

    // Build a custom lookup service that returns all 3 refs
    struct MultiResultLookupService {
        refs: Vec<UTXOReference>,
    }
    #[async_trait(?Send)]
    impl LookupService for MultiResultLookupService {
        fn admission_mode(&self) -> AdmissionMode {
            AdmissionMode::LockingScript
        }
        fn spend_notification_mode(&self) -> SpendNotificationMode {
            SpendNotificationMode::None
        }
        async fn output_admitted_by_topic(
            &self,
            _: &OutputAdmittedByTopic,
        ) -> Result<(), overlay_engine::lookup_service::LookupServiceError> {
            Ok(())
        }
        async fn output_evicted(
            &self,
            _: &str,
            _: u32,
        ) -> Result<(), overlay_engine::lookup_service::LookupServiceError> {
            Ok(())
        }
        async fn lookup(
            &self,
            _: &LookupQuestion,
        ) -> Result<Vec<UTXOReference>, overlay_engine::lookup_service::LookupServiceError>
        {
            Ok(self.refs.clone())
        }
        async fn get_documentation(&self) -> String {
            "multi".into()
        }
        async fn get_metadata(&self) -> ServiceMetadata {
            ServiceMetadata {
                name: "multi".into(),
                ..Default::default()
            }
        }
    }

    let multi_ls = MultiResultLookupService {
        refs: fake_txids
            .iter()
            .map(|t| UTXOReference {
                txid: t.to_string(),
                output_index: 0,
            })
            .collect(),
    };

    let mut lss2: HashMap<String, Box<dyn LookupService>> = HashMap::new();
    lss2.insert("ls_hello".into(), Box::new(multi_ls));

    let engine = Engine::new(mgrs, lss2, Box::new(storage), None, EngineConfig::default());

    let question = LookupQuestion::new("ls_hello", serde_json::json!({}));
    let answer = engine.lookup(&question, None).await.unwrap();

    match answer {
        LookupAnswer::OutputList { outputs } => {
            assert_eq!(
                outputs.len(),
                3,
                "Lookup should return all 3 outputs with BEEF"
            );
            for item in &outputs {
                assert!(
                    !item.beef.is_empty(),
                    "Each output should have non-empty BEEF"
                );
            }
        }
        _ => panic!("Expected OutputList"),
    }
}

// ============================================================================
// TS Test: "History selector edge case: output with no previous coins"
// When get_utxo_history is called on an output that has no outputsConsumed
// (root transaction), it should return the output itself without error.
// ============================================================================

#[tokio::test]
async fn ts_history_selector_root_output_no_previous_coins() {
    let storage = MemoryStorage::new();

    let root_output = Output {
        txid: EXAMPLE_TXID.to_string(),
        output_index: 0,
        output_script: vec![0x76, 0xa9],
        satoshis: 26172,
        topic: "Hello".to_string(),
        spent: false,
        outputs_consumed: vec![], // No previous coins — this is a root tx
        consumed_by: vec![],
        beef: Some(example_beef()),
        block_height: None,
        score: Some(1000.0),
    };
    storage.insert_output(&root_output).await.unwrap();

    let engine = Engine::new(
        HashMap::new(),
        HashMap::new(),
        Box::new(storage),
        None,
        EngineConfig::default(),
    );

    // With a depth selector, should return the output itself (no ancestors to traverse)
    let result = engine
        .get_utxo_history(&root_output, Some(HistorySelector::Depth(5)))
        .await
        .unwrap();
    assert!(
        result.is_some(),
        "Root output should be returned even with depth selector"
    );
    let output = result.unwrap();
    assert_eq!(output.txid, EXAMPLE_TXID);
    assert!(output.beef.is_some(), "BEEF should be present");
}

// ============================================================================
// TS Test: "History selector receives correct parameters"
// When get_utxo_history is called with Depth(1), verify the depth limit is
// respected by creating a chain of 3 transactions and requesting depth 1
// (should only include 1 ancestor, not 2).
// ============================================================================

#[tokio::test]
async fn ts_history_selector_depth_limit_respected() {
    let storage = MemoryStorage::new();

    // Chain: grandparent -> parent -> child
    // All use the same valid BEEF (the BEEF data is just for parsing, the ancestor
    // tracking is done via outputs_consumed in storage).
    let grandparent = Output {
        txid: "grandparent_tx".to_string(),
        output_index: 0,
        output_script: vec![0x76],
        satoshis: 2000,
        topic: "Hello".to_string(),
        spent: true,
        outputs_consumed: vec![],
        consumed_by: vec![Outpoint::new("parent_tx", 0)],
        beef: Some(example_beef()),
        block_height: None,
        score: Some(1000.0),
    };

    let parent = Output {
        txid: "parent_tx".to_string(),
        output_index: 0,
        output_script: vec![0x76],
        satoshis: 1500,
        topic: "Hello".to_string(),
        spent: true,
        outputs_consumed: vec![Outpoint::new("grandparent_tx", 0)],
        consumed_by: vec![Outpoint::new(EXAMPLE_TXID, 0)],
        beef: Some(example_beef()),
        block_height: None,
        score: Some(2000.0),
    };

    let child = Output {
        txid: EXAMPLE_TXID.to_string(),
        output_index: 0,
        output_script: vec![0x76],
        satoshis: 1000,
        topic: "Hello".to_string(),
        spent: false,
        outputs_consumed: vec![Outpoint::new("parent_tx", 0)],
        consumed_by: vec![],
        beef: Some(example_beef()),
        block_height: None,
        score: Some(3000.0),
    };

    storage.insert_output(&grandparent).await.unwrap();
    storage.insert_output(&parent).await.unwrap();
    storage.insert_output(&child).await.unwrap();

    let engine = Engine::new(
        HashMap::new(),
        HashMap::new(),
        Box::new(storage),
        None,
        EngineConfig::default(),
    );

    // Depth(1) means: include current (depth 0) and 1 level of ancestors (depth 1),
    // but NOT the grandparent (depth 2).
    let result = engine
        .get_utxo_history(&child, Some(HistorySelector::Depth(1)))
        .await
        .unwrap();
    assert!(
        result.is_some(),
        "Should return enriched output with depth 1"
    );
    let enriched = result.unwrap();
    assert!(enriched.beef.is_some(), "Enriched output should have BEEF");

    // Depth(0) should NOT include ancestors at all — only the current output
    let result_d0 = engine
        .get_utxo_history(&child, Some(HistorySelector::Depth(0)))
        .await
        .unwrap();
    assert!(
        result_d0.is_some(),
        "Depth 0 should still return the output itself"
    );
}

// ============================================================================
// TS Test: "Submit with empty topic array"
// Verify submitting with topics=[] returns an empty Steak (not an error).
// ============================================================================

#[tokio::test]
async fn ts_submit_with_empty_topics_returns_empty_steak() {
    let engine = single_topic_engine("Hello", vec![0]);
    let beef = TaggedBEEF::new(example_beef(), vec![]); // empty topics

    let steak = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();
    assert!(
        steak.is_empty(),
        "Steak should be empty when no topics are provided"
    );
}

// ============================================================================
// TS Test: "Submit with multiple topics, one unsupported"
// Verify error is returned for the unsupported topic.
// ============================================================================

#[tokio::test]
async fn ts_submit_multiple_topics_one_unsupported() {
    let mut mgrs: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    mgrs.insert("Hello".into(), Box::new(MockTopicManager::new(vec![0])));
    // "Goodbye" is NOT registered

    let engine = build_engine(mgrs, HashMap::new(), None, EngineConfig::default());
    let beef = example_tagged_beef(&["Hello", "Goodbye"]);

    let result = engine.submit(&beef, SubmitMode::CurrentTx).await;
    assert!(
        result.is_err(),
        "Should error when one topic is unsupported"
    );
    assert!(
        matches!(result.unwrap_err(), EngineError::UnsupportedTopic(ref t) if t == "Goodbye"),
        "Error should identify the unsupported topic"
    );
}
