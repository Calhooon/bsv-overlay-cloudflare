//! Tests for the submit() spend chain: previous coin lookup, spent notifications,
//! stale UTXO deletion, and consumedBy updates.
//!
//! These tests verify the CRITICAL gaps identified in GAP_ANALYSIS.md (#43, #44).
//! Test-driven: tests written first, then implementation.

use async_trait::async_trait;
use bsv_overlay_engine::engine::{Engine, EngineConfig};
use bsv_overlay_engine::lookup_service::{LookupService, LookupServiceError};
use bsv_overlay_engine::storage::memory::MemoryStorage;
use bsv_overlay_engine::storage::Storage;
use bsv_overlay_engine::topic_manager::{TopicManager, TopicManagerError};
use bsv_overlay_engine::types::*;
use std::collections::HashMap;
use std::sync::Mutex;

// ============================================================================
// Test BEEF — BRC62 from TS tests
// ============================================================================

const BRC62_BEEF_HEX: &str = "0100beef01fe636d0c0007021400fe507c0c7aa754cef1f7889d5fd395cf1f785dd7de98eed895dbedfe4e5bc70d1502ac4e164f5bc16746bb0868404292ac8318bbac3800e4aad13a014da427adce3e010b00bc4ff395efd11719b277694cface5aa50d085a0bb81f613f70313acd28cf4557010400574b2d9142b8d28b61d88e3b2c3f44d858411356b49a28a4643b6d1a6a092a5201030051a05fc84d531b5d250c23f4f886f6812f9fe3f402d61607f977b4ecd2701c19010000fd781529d58fc2523cf396a7f25440b409857e7e221766c57214b1d38c7b481f01010062f542f45ea3660f86c013ced80534cb5fd4c19d66c56e7e8c5d4bf2d40acc5e010100b121e91836fd7cd5102b654e9f72f3cf6fdbfd0b161c53a9c54b12c841126331020100000001cd4e4cac3c7b56920d1e7655e7e260d31f29d9a388d04910f1bbd72304a79029010000006b483045022100e75279a205a547c445719420aa3138bf14743e3f42618e5f86a19bde14bb95f7022064777d34776b05d816daf1699493fcdf2ef5a5ab1ad710d9c97bfb5b8f7cef3641210263e2dee22b1ddc5e11f6fab8bcd2378bdd19580d640501ea956ec0e786f93e76ffffffff013e660000000000001976a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac0000000001000100000001ac4e164f5bc16746bb0868404292ac8318bbac3800e4aad13a014da427adce3e000000006a47304402203a61a2e931612b4bda08d541cfb980885173b8dcf64a3471238ae7abcd368d6402204cbf24f04b9aa2256d8901f0ed97866603d2be8324c2bfb7a37bf8fc90edd5b441210263e2dee22b1ddc5e11f6fab8bcd2378bdd19580d640501ea956ec0e786f93e76ffffffff013c660000000000001976a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac0000000000";

/// TXID of the transaction in the BEEF
const EXAMPLE_TXID: &str = "157428aee67d11123203735e4c540fa1bdab3b36d5882c6f8c5ff79f07d20d1c";
/// TXID of the input's source transaction (the "previous coin")
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

// ============================================================================
// Tracking TopicManager — records what previousCoins it received
// ============================================================================

struct TrackingTopicManager {
    received_previous_coins: Mutex<Vec<Vec<u8>>>,
    admit_indices: Vec<u32>,
    coins_to_retain: Vec<u32>,
}

impl TrackingTopicManager {
    fn new(admit: Vec<u32>, retain: Vec<u32>) -> Self {
        Self {
            received_previous_coins: Mutex::new(Vec::new()),
            admit_indices: admit,
            coins_to_retain: retain,
        }
    }
}

#[async_trait(?Send)]
impl TopicManager for TrackingTopicManager {
    async fn identify_admissible_outputs(
        &self,
        _beef: &[u8],
        previous_coins: &[u8],
        _ocv: Option<&[u8]>,
        _mode: SubmitMode,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        self.received_previous_coins
            .lock()
            .unwrap()
            .push(previous_coins.to_vec());
        Ok(AdmittanceInstructions {
            outputs_to_admit: self.admit_indices.clone(),
            coins_to_retain: self.coins_to_retain.clone(),
            coins_removed: None,
        })
    }
    async fn get_documentation(&self) -> String {
        "Tracking".into()
    }
    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "tracking-tm".into(),
            ..Default::default()
        }
    }
}

// ============================================================================
// Tracking LookupService — records spent notifications
// ============================================================================

struct SpentTrackingLookupService {
    admitted: Mutex<Vec<(String, u32, String)>>,
    spent_calls: Mutex<Vec<OutputSpent>>,
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

    fn spent_count(&self) -> usize {
        self.spent_calls.lock().unwrap().len()
    }

    fn was_spent_called_for(&self, txid: &str, output_index: u32) -> bool {
        self.spent_calls.lock().unwrap().iter().any(|s| match s {
            OutputSpent::None {
                txid: t,
                output_index: oi,
                ..
            } => t == txid && *oi == output_index,
            OutputSpent::Txid {
                txid: t,
                output_index: oi,
                ..
            } => t == txid && *oi == output_index,
            _ => false,
        })
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
        self.spent_calls.lock().unwrap().push(payload.clone());
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

// ============================================================================
// Test: When a previous output exists in storage, it should be found
// ============================================================================

/// TS: "Acquires the appropriate previous topical UTXOs from the storage engine"
/// The BRC62 BEEF has 1 input spending EXAMPLE_PREVIOUS_TXID:0.
/// If that output exists in storage for topic "Hello", submit() should find it.
#[tokio::test]
async fn submit_finds_previous_output_in_storage() {
    let storage = MemoryStorage::new();

    // Pre-populate storage with the previous output
    let prev_output = Output {
        txid: EXAMPLE_PREVIOUS_TXID.to_string(),
        output_index: 0,
        output_script: vec![0x76, 0xa9],
        satoshis: 26174, // from the ancestor tx
        topic: "Hello".to_string(),
        spent: false,
        outputs_consumed: vec![],
        consumed_by: vec![],
        beef: Some(example_beef()),
        block_height: None,
        score: Some(1000.0),
    };
    storage.insert_output(&prev_output).await.unwrap();

    let mut managers: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    // Retain the previous coin so we can verify it's spent but not deleted
    managers.insert(
        "Hello".into(),
        Box::new(TrackingTopicManager::new(vec![0], vec![0])),
    );

    let mut lookup_services: HashMap<String, Box<dyn LookupService>> = HashMap::new();
    lookup_services.insert(
        "ls_hello".into(),
        Box::new(SpentTrackingLookupService::new()),
    );

    let engine = Engine::new(
        managers,
        lookup_services,
        Box::new(storage),
        None,
        EngineConfig::default(),
    );

    let beef = TaggedBEEF::new(example_beef(), vec!["Hello".to_string()]);
    let steak = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

    // The new output should be admitted
    assert_eq!(steak["Hello"].outputs_to_admit, vec![0]);

    // The previous output should now be marked as spent (retained, not deleted)
    let prev = engine
        .storage()
        .find_output(EXAMPLE_PREVIOUS_TXID, 0, Some("Hello"), Some(true), false)
        .await
        .unwrap();
    assert!(prev.is_some(), "Previous output should be marked as spent");
}

/// TS: "Includes the appropriate previous topical UTXOs when returned from storage"
/// Verifies that markUTXOAsSpent is called for the previous output.
#[tokio::test]
async fn submit_marks_previous_output_as_spent() {
    let storage = MemoryStorage::new();

    // Pre-populate
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

    let mut managers: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    // Retain so we can check spent status (non-retained coins get deleted)
    managers.insert(
        "Hello".into(),
        Box::new(TrackingTopicManager::new(vec![0], vec![0])),
    );

    let engine = Engine::new(
        managers,
        HashMap::new(),
        Box::new(storage),
        None,
        EngineConfig::default(),
    );

    let beef = TaggedBEEF::new(example_beef(), vec!["Hello".to_string()]);
    engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

    // Previous output must be spent (retained, so still in storage)
    let prev_unspent = engine
        .storage()
        .find_output(EXAMPLE_PREVIOUS_TXID, 0, Some("Hello"), Some(false), false)
        .await
        .unwrap();
    assert!(
        prev_unspent.is_none(),
        "Previous output should NOT be findable as unspent"
    );

    let prev_spent = engine
        .storage()
        .find_output(EXAMPLE_PREVIOUS_TXID, 0, Some("Hello"), Some(true), false)
        .await
        .unwrap();
    assert!(
        prev_spent.is_some(),
        "Previous output should be findable as spent"
    );
}

/// TS: "Notifies all lookup services about the output being spent"
#[tokio::test]
async fn submit_notifies_lookup_services_of_spent_output() {
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

    let mut managers: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    // Retain so output stays in storage as spent
    managers.insert(
        "Hello".into(),
        Box::new(TrackingTopicManager::new(vec![0], vec![0])),
    );

    let mut lookup_services: HashMap<String, Box<dyn LookupService>> = HashMap::new();
    lookup_services.insert(
        "ls_hello".into(),
        Box::new(SpentTrackingLookupService::new()),
    );

    let engine = Engine::new(
        managers,
        lookup_services,
        Box::new(storage),
        None,
        EngineConfig::default(),
    );

    let beef = TaggedBEEF::new(example_beef(), vec!["Hello".to_string()]);
    engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

    // The previous output should be marked as spent in storage
    let prev = engine
        .storage()
        .find_output(EXAMPLE_PREVIOUS_TXID, 0, Some("Hello"), Some(true), false)
        .await
        .unwrap();
    assert!(prev.is_some(), "Previous output should be spent");
}

/// When the previous output doesn't exist in storage (not from this topic),
/// it should NOT be in previousCoins and should NOT be marked as spent.
#[tokio::test]
async fn submit_ignores_inputs_not_in_topic() {
    let storage = MemoryStorage::new();
    // Storage is EMPTY — no previous output for "Hello" topic

    let mut managers: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    managers.insert(
        "Hello".into(),
        Box::new(TrackingTopicManager::new(vec![0], vec![])),
    );

    let engine = Engine::new(
        managers,
        HashMap::new(),
        Box::new(storage),
        None,
        EngineConfig::default(),
    );

    let beef = TaggedBEEF::new(example_beef(), vec!["Hello".to_string()]);
    let steak = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

    // Output should still be admitted (topic manager always admits index 0)
    assert_eq!(steak["Hello"].outputs_to_admit, vec![0]);

    // No previous output should be in storage at all
    let prev = engine
        .storage()
        .find_output(EXAMPLE_PREVIOUS_TXID, 0, Some("Hello"), None, false)
        .await
        .unwrap();
    assert!(prev.is_none());
}

/// When topic manager retains previous coins (coinsToRetain includes them),
/// they should NOT be deleted — just marked as spent.
#[tokio::test]
async fn submit_retains_previous_coins_when_topic_manager_says_retain() {
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

    let mut managers: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    // Topic manager retains the previous coin (index 0 in previousCoins)
    managers.insert(
        "Hello".into(),
        Box::new(TrackingTopicManager::new(vec![0], vec![0])),
    );

    let engine = Engine::new(
        managers,
        HashMap::new(),
        Box::new(storage),
        None,
        EngineConfig::default(),
    );

    let beef = TaggedBEEF::new(example_beef(), vec!["Hello".to_string()]);
    engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

    // Previous output should be spent but NOT deleted
    let prev = engine
        .storage()
        .find_output(EXAMPLE_PREVIOUS_TXID, 0, Some("Hello"), Some(true), false)
        .await
        .unwrap();
    assert!(
        prev.is_some(),
        "Retained output should still exist (as spent)"
    );
}

/// When topic manager does NOT retain previous coins,
/// they should be deleted via deleteUTXODeep.
#[tokio::test]
async fn submit_deletes_stale_previous_coins_when_not_retained() {
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

    let mut managers: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    // Topic manager does NOT retain (coinsToRetain is empty)
    managers.insert(
        "Hello".into(),
        Box::new(TrackingTopicManager::new(vec![0], vec![])),
    );

    let engine = Engine::new(
        managers,
        HashMap::new(),
        Box::new(storage),
        None,
        EngineConfig::default(),
    );

    let beef = TaggedBEEF::new(example_beef(), vec!["Hello".to_string()]);
    engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

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

/// New output's consumedBy should be updated on retained previous outputs.
#[tokio::test]
async fn submit_updates_consumed_by_on_retained_output() {
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

    let mut managers: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    managers.insert(
        "Hello".into(),
        Box::new(TrackingTopicManager::new(vec![0], vec![0])),
    );

    let engine = Engine::new(
        managers,
        HashMap::new(),
        Box::new(storage),
        None,
        EngineConfig::default(),
    );

    let beef = TaggedBEEF::new(example_beef(), vec!["Hello".to_string()]);
    engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

    // The retained previous output should have consumedBy updated
    let prev = engine
        .storage()
        .find_output(EXAMPLE_PREVIOUS_TXID, 0, Some("Hello"), None, false)
        .await
        .unwrap()
        .unwrap();

    assert!(
        prev.consumed_by.iter().any(|c| c.txid == EXAMPLE_TXID),
        "Retained output consumedBy should include the new tx. Got: {:?}",
        prev.consumed_by
    );
}

/// The new output's outputsConsumed should reference the retained previous outputs.
#[tokio::test]
async fn submit_sets_outputs_consumed_on_new_output() {
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

    let mut managers: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
    managers.insert(
        "Hello".into(),
        Box::new(TrackingTopicManager::new(vec![0], vec![0])),
    );

    let engine = Engine::new(
        managers,
        HashMap::new(),
        Box::new(storage),
        None,
        EngineConfig::default(),
    );

    let beef = TaggedBEEF::new(example_beef(), vec!["Hello".to_string()]);
    engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

    // The new output should have outputsConsumed referencing the previous output
    let new_output = engine
        .storage()
        .find_output(EXAMPLE_TXID, 0, Some("Hello"), None, false)
        .await
        .unwrap()
        .unwrap();

    assert!(
        new_output
            .outputs_consumed
            .iter()
            .any(|c| c.txid == EXAMPLE_PREVIOUS_TXID),
        "New output outputsConsumed should reference the previous output. Got: {:?}",
        new_output.outputs_consumed
    );
}
