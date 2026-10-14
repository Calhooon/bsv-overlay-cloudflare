// bsv-low #657: acceptance witnesses, intentionally RED until atomic admission.
// Included as a child of memory so snapshots see every table without a public API.
use super::MemoryStorage;
use crate::engine::{Engine, EngineConfig, MutationReport};
use crate::lookup_service::{LookupService, LookupServiceError};
use crate::storage::Storage;
use crate::topic_manager::{TopicManager, TopicManagerError};
use crate::types::*;
use async_trait::async_trait;
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{
    MerklePath, MerklePathLeaf, Transaction, TransactionInput, TransactionOutput,
};
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::sync::Mutex;

const REGISTRY: &str = "tm_registry_657";
const PROOF: &str = "tm_proof_657";
const REFUSAL: &str = "E657 deterministic refusal of registry output 2";
type Index = Rc<Mutex<BTreeMap<(String, u32, String), u64>>>;

struct RegistryManager;
#[async_trait(?Send)]
impl TopicManager for RegistryManager {
    async fn identify_admissible_outputs(
        &self,
        _: &Transaction,
        previous: &[u8],
        _: Option<&[u8]>,
        _: SubmitMode,
        _: &TopicAdmittanceContext,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        // TYPE 2 over the held coin; TYPE 1 if a refused attempt deleted it.
        Ok(AdmittanceInstructions {
            outputs_to_admit: if previous.is_empty() {
                vec![0]
            } else {
                vec![0, 2]
            },
            coins_to_retain: vec![],
            coins_removed: None,
        })
    }
    async fn get_documentation(&self) -> String {
        String::new()
    }
    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata::default()
    }
}
struct ProofManager;
#[async_trait(?Send)]
impl TopicManager for ProofManager {
    async fn identify_admissible_outputs(
        &self,
        _: &Transaction,
        _: &[u8],
        _: Option<&[u8]>,
        _: SubmitMode,
        _: &TopicAdmittanceContext,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        Ok(AdmittanceInstructions {
            outputs_to_admit: vec![2],
            coins_to_retain: vec![],
            coins_removed: None,
        })
    }
    async fn get_documentation(&self) -> String {
        String::new()
    }
    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata::default()
    }
}
struct RefusingLookup(Index);
#[async_trait(?Send)]
impl LookupService for RefusingLookup {
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
        let OutputAdmittedByTopic::LockingScript {
            txid,
            output_index,
            topic,
            ..
        } = payload
        else {
            unreachable!()
        };
        // Refuse by payload, never by call count: the same output always refuses.
        if topic == REGISTRY && *output_index == 2 {
            return Err(LookupServiceError::Other(REFUSAL.into()));
        }
        *self
            .0
            .lock()
            .unwrap()
            .entry((txid.clone(), *output_index, topic.clone()))
            .or_default() += 1;
        Ok(())
    }
    async fn output_evicted(&self, _: &str, _: u32) -> Result<(), LookupServiceError> {
        Ok(())
    }
    async fn lookup(&self, _: &LookupQuestion) -> Result<LookupResult, LookupServiceError> {
        Ok(LookupResult::OutputList(vec![]))
    }
    async fn get_documentation(&self) -> String {
        String::new()
    }
    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata::default()
    }
}

// Canonical table bytes, including keys, sorted independently of HashMap order.
// This compares stored values, not counts or only unspent outputs. It includes
// transactions, applied rows, metadata, sync/deferred tables and the LS index.
fn snapshot(store: &MemoryStorage, index: &Index) -> Vec<u8> {
    let mut tables = BTreeMap::new();
    macro_rules! rows {
        ($name:expr, $value:expr) => {{
            let held = $value.lock().unwrap();
            let mut rows: Vec<_> = held
                .iter()
                .map(|row| serde_json::to_vec(&row).unwrap())
                .collect();
            rows.sort();
            tables.insert($name, serde_json::to_vec(&rows).unwrap());
        }};
    }
    rows!("outputs", store.outputs);
    rows!("transactions", store.transactions);
    rows!("proven", store.proven);
    rows!("applied", store.applied);
    rows!("sync_state", store.sync_state);
    rows!("created_at", store.created_at);
    rows!("peer_health", store.peer_health);
    rows!("proven_at", store.proven_at);
    rows!("deferred_graphs", store.deferred_graphs);
    rows!("peer_yieldless", store.peer_yieldless);
    rows!("lookup_index", index);
    tables.insert(
        "clock_secs",
        serde_json::to_vec(&*store.clock_secs.lock().unwrap()).unwrap(),
    );
    // Fault switches and read/write instrumentation are not persisted tables.
    serde_json::to_vec(&tables).unwrap()
}

async fn fixture() -> (Engine, Rc<MemoryStorage>, Index, TaggedBEEF, String) {
    let store = Rc::new(MemoryStorage::new());
    let index = Rc::new(Mutex::new(BTreeMap::new()));
    let mut previous = Transaction::new();
    let mut funding = TransactionInput::new("00".repeat(32), u32::MAX);
    funding.unlocking_script = Some(UnlockingScript::from_binary(&[0x51]).unwrap());
    previous.inputs.push(funding);
    previous.outputs.push(TransactionOutput::new(
        3,
        LockingScript::from_binary(&[0x51]).unwrap(),
    ));
    let parent = previous.id();
    previous.merkle_path =
        Some(MerklePath::new(99, vec![vec![MerklePathLeaf::new_txid(0, parent.clone())]]).unwrap());
    let previous_beef = previous.to_beef(false).unwrap();
    let mut tx = Transaction::new();
    let mut input = TransactionInput::new(parent.clone(), 0);
    input.unlocking_script = Some(UnlockingScript::from_binary(&[0x51]).unwrap());
    tx.inputs.push(input);
    for _ in 0..3 {
        tx.outputs.push(TransactionOutput::new(
            1,
            LockingScript::from_binary(&[0x51]).unwrap(),
        ));
    }
    // Proven fixture, like existing engine harness pins. No network or tracker.
    tx.merkle_path =
        Some(MerklePath::new(100, vec![vec![MerklePathLeaf::new_txid(0, tx.id())]]).unwrap());
    let beef = TaggedBEEF::new(
        tx.to_beef(false).unwrap(),
        vec![REGISTRY.into(), PROOF.into()],
    );
    store
        .insert_output(&Output {
            txid: parent.clone(),
            output_index: 0,
            output_script: vec![0x51],
            satoshis: 3,
            topic: REGISTRY.into(),
            spent: false,
            outputs_consumed: vec![],
            consumed_by: vec![],
            beef: Some(previous_beef),
            block_height: Some(99),
            score: Some(1.0),
        })
        .await
        .unwrap();
    let managers: HashMap<String, Box<dyn TopicManager>> = HashMap::from([
        (
            REGISTRY.into(),
            Box::new(RegistryManager) as Box<dyn TopicManager>,
        ),
        (
            PROOF.into(),
            Box::new(ProofManager) as Box<dyn TopicManager>,
        ),
    ]);
    let services: HashMap<String, Box<dyn LookupService>> = HashMap::from([(
        "ls_refuse_657".into(),
        Box::new(RefusingLookup(index.clone())) as Box<dyn LookupService>,
    )]);
    let engine = Engine::new(
        managers,
        services,
        Box::new(store.clone()),
        None,
        EngineConfig::default(),
    );
    (engine, store, index, beef, parent)
}

fn refused(report: &MutationReport) {
    assert_eq!(
        report.faults.len(),
        1,
        "fixture must reach exactly the intended refusal"
    );
    assert_eq!(report.faults[0].topic, REGISTRY);
    assert_eq!(
        report.faults[0].site,
        "lookup_service.output_admitted_by_topic"
    );
    assert_eq!(report.faults[0].error, REFUSAL);
}

#[tokio::test]
async fn e657_refused_submit_leaves_store_byte_equal() {
    let (engine, store, index, beef, parent) = fixture().await;
    let before = snapshot(&store, &index);
    let (steak, report) = engine
        .submit_with_report(&beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .unwrap();
    refused(&report);
    assert_eq!(steak[REGISTRY].outputs_to_admit, vec![0, 2]);
    let after = snapshot(&store, &index);
    // A compact diagnostic keeps the RED log readable while equality checks every byte.
    assert!(
        before == after,
        "bsv-low #657: refused submit changed stored bytes (before {}, after {})",
        before.len(),
        after.len()
    );
    let old = store
        .find_output(&parent, 0, Some(REGISTRY), None, true)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !old.spent,
        "the refused submit must leave its previous coin unspent"
    );
}

#[tokio::test]
async fn e657_same_bytes_same_word_after_refusal() {
    let (engine, store, index, beef, _) = fixture().await;
    let before = snapshot(&store, &index);
    let (first_steak, first_report) = engine
        .submit_with_report(&beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .unwrap();
    refused(&first_report);
    let (second_steak, second_report) = engine
        .submit_with_report(&beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .unwrap();
    // Report and STEAK together are the engine's word, no HTTP mapping invented.
    assert_eq!(
        second_report, first_report,
        "bsv-low #657: identical bytes changed the word"
    );
    assert_eq!(
        serde_json::to_value(second_steak).unwrap(),
        serde_json::to_value(first_steak).unwrap()
    );
    assert!(
        before == snapshot(&store, &index),
        "both refusals must preserve every stored byte"
    );
}

// Test 3/11: exercise real engine writes, not a mock commit implementation.
#[tokio::test]
async fn a1_03_each_write_fault_rolls_back() {
    for at in 1..=5 {
        let (engine, store, index, beef, _) = fixture().await;
        let before = snapshot(&store, &index);
        store.fault_write(Some(at));
        let _ = engine.submit_with_report(&beef, SubmitMode::HistoricalTxNoSpv).await;
        assert!(before == snapshot(&store, &index), "a1_03 write {at} left rows");
    }
}

#[tokio::test]
async fn a1_11_applied_row_fault_rolls_back() {
    let (engine, store, index, mut beef, _) = fixture().await;
    // The proof topic writes its output then its applied row.
    beef.topics = vec![PROOF.into()];
    let before = snapshot(&store, &index);
    store.fault_write(Some(2));
    let _ = engine.submit_with_report(&beef, SubmitMode::HistoricalTxNoSpv).await;
    assert!(before == snapshot(&store, &index), "a1_11 applied fault left output and index");
}

#[tokio::test]
async fn a1_07_predecessor_commits_but_refused_successor_does_not() {
    let (engine, store, index, mut beef, parent) = fixture().await;
    // The fixture's proven previous coin is a separately landed unit.
    let predecessor = store.find_output(&parent, 0, Some(REGISTRY), None, true).await.unwrap().unwrap();
    let first = TaggedBEEF::new(predecessor.beef.clone().unwrap(), vec![REGISTRY.into()]);
    engine.submit_with_report(&first, SubmitMode::HistoricalTxNoSpv).await.unwrap();
    let before = snapshot(&store, &index);
    beef.topics = vec![REGISTRY.into()];
    let _ = engine.submit_with_report(&beef, SubmitMode::HistoricalTxNoSpv).await;
    assert!(before == snapshot(&store, &index), "a1_07 refused successor changed predecessor unit");
}

struct AwaitingLookup;
#[async_trait(?Send)]
impl LookupService for AwaitingLookup {
    fn admission_mode(&self) -> AdmissionMode { AdmissionMode::LockingScript }
    fn spend_notification_mode(&self) -> SpendNotificationMode { SpendNotificationMode::None }
    async fn output_admitted_by_topic(&self, _: &OutputAdmittedByTopic) -> Result<(), LookupServiceError> {
        // A real timer promise, not an already-ready async function.
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        Ok(())
    }
    async fn output_evicted(&self, _: &str, _: u32) -> Result<(), LookupServiceError> { Ok(()) }
    async fn lookup(&self, _: &LookupQuestion) -> Result<LookupResult, LookupServiceError> { Ok(LookupResult::OutputList(vec![])) }
    async fn get_documentation(&self) -> String { String::new() }
    async fn get_metadata(&self) -> ServiceMetadata { ServiceMetadata::default() }
}

#[tokio::test]
async fn a1_27_real_promise_is_loud_and_rolls_back() {
    let (_, store, index, mut beef, _) = fixture().await;
    beef.topics = vec![PROOF.into()];
    let engine = Engine::new(
        HashMap::from([(PROOF.into(), Box::new(ProofManager) as Box<dyn TopicManager>)]),
        HashMap::from([("awaiting".into(), Box::new(AwaitingLookup) as Box<dyn LookupService>)]),
        Box::new(store.clone()), None, EngineConfig::default());
    let before = snapshot(&store, &index);
    let result = engine.submit_with_report(&beef, SubmitMode::HistoricalTxNoSpv).await;
    assert!(before == snapshot(&store, &index), "a1_27 awaited promise committed rows: {result:?}");
}

struct RetainManager;
#[async_trait(?Send)]
impl TopicManager for RetainManager {
    async fn identify_admissible_outputs(&self, _: &Transaction, previous: &[u8], _: Option<&[u8]>, _: SubmitMode, _: &TopicAdmittanceContext) -> Result<AdmittanceInstructions, TopicManagerError> {
        if previous.is_empty() { return Err(TopicManagerError::NoAdmissibleOutputs("coin already spent".into())); }
        Ok(AdmittanceInstructions { outputs_to_admit: vec![0], coins_to_retain: vec![0], coins_removed: None })
    }
    async fn get_documentation(&self) -> String { String::new() }
    async fn get_metadata(&self) -> ServiceMetadata { ServiceMetadata::default() }
}

#[tokio::test]
async fn a1_05_two_spenders_judge_the_serial_store() {
    let (_, store, _, mut first, _) = fixture().await;
    first.topics = vec![REGISTRY.into()];
    let mut tx = Transaction::from_beef(&first.beef, None).unwrap();
    tx.lock_time += 1;
    tx.invalidate_caches();
    tx.merkle_path = Some(MerklePath::new(100, vec![vec![MerklePathLeaf::new_txid(0, tx.id())]]).unwrap());
    let second = TaggedBEEF::new(tx.to_beef(false).unwrap(), first.topics.clone());
    let engine = Engine::new(
        HashMap::from([(REGISTRY.into(), Box::new(RetainManager) as Box<dyn TopicManager>)]),
        HashMap::new(), Box::new(store.clone()), None, EngineConfig::default());
    let (a, b) = tokio::join!(
        engine.submit_with_report(&first, SubmitMode::HistoricalTxNoSpv),
        engine.submit_with_report(&second, SubmitMode::HistoricalTxNoSpv));
    let landed = [a, b].into_iter().filter(|r| r.as_ref().is_ok_and(|(_, report)| !report.applied_topics.is_empty())).count();
    assert_eq!(landed, 1, "a1_05 both spends admitted against one retained coin");
}

struct ReadEmitLookup { store: Rc<MemoryStorage>, sent: Index }
#[async_trait(?Send)]
impl LookupService for ReadEmitLookup {
    fn admission_mode(&self) -> AdmissionMode { AdmissionMode::LockingScript }
    fn spend_notification_mode(&self) -> SpendNotificationMode { SpendNotificationMode::None }
    async fn output_admitted_by_topic(&self, payload: &OutputAdmittedByTopic) -> Result<(), LookupServiceError> {
        let OutputAdmittedByTopic::LockingScript { txid, output_index, topic, .. } = payload else { unreachable!() };
        assert!(self.store.find_output(txid, *output_index, Some(topic), None, false).await.unwrap().is_some(), "service must read its unit's inserted row");
        self.sent.lock().unwrap().insert((txid.clone(), *output_index, topic.clone()), 1);
        Ok(())
    }
    async fn output_evicted(&self, _: &str, _: u32) -> Result<(), LookupServiceError> { Ok(()) }
    async fn lookup(&self, _: &LookupQuestion) -> Result<LookupResult, LookupServiceError> { Ok(LookupResult::OutputList(vec![])) }
    async fn get_documentation(&self) -> String { String::new() }
    async fn get_metadata(&self) -> ServiceMetadata { ServiceMetadata::default() }
}

#[tokio::test]
async fn a1_13_read_own_write_stages_event_until_drain() {
    let (_, store, sent, mut beef, _) = fixture().await;
    beef.topics = vec![PROOF.into()];
    let engine = Engine::new(
        HashMap::from([(PROOF.into(), Box::new(ProofManager) as Box<dyn TopicManager>)]),
        HashMap::from([("read_emit".into(), Box::new(ReadEmitLookup { store: store.clone(), sent: sent.clone() }) as Box<dyn LookupService>)]),
        Box::new(store), None, EngineConfig::default());
    engine.submit_with_report(&beef, SubmitMode::HistoricalTxNoSpv).await.unwrap();
    assert!(sent.lock().unwrap().is_empty(), "a1_13 event escaped before outbox drain");
}
