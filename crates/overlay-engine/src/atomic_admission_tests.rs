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
        _: &TopicAdmittanceContext<'_>,
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
        _: &TopicAdmittanceContext<'_>,
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
    fn is_staged(&self) -> bool { true }
    async fn stage_admitted(&self, payload: &OutputAdmittedByTopic, unit: &mut dyn crate::storage::AdmissionUnit) -> Result<(), crate::lookup_service::LookupRefusal> {
        let OutputAdmittedByTopic::LockingScript { txid, output_index, topic, .. } = payload else { unreachable!() };
        if topic == REGISTRY && *output_index == 2 {
            return Err(crate::lookup_service::LookupRefusal::Deterministic(REFUSAL.into()));
        }
        unit.stage(crate::storage::StagedWrite::new(super::MemoryWrite::Increment {
            table: "lookup_index".into(), key: format!("{txid}:{output_index}:{topic}")
        }, (txid.len() + topic.len() + 4) as u64));
        Ok(())
    }
    async fn after_commit(&self, _: &[OutputAdmittedByTopic], _: &[OutputSpent]) -> Result<(), LookupServiceError> { Ok(()) }
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
    tables.insert("admission", store.admission_snapshot());
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
    assert!(report.faults.is_empty());
    let refusal = report.refused.as_ref().expect("deterministic refusal");
    assert_eq!(refusal.topic, REGISTRY);
    assert_eq!(refusal.site, "lookup_service.stage_admitted");
    assert_eq!(refusal.class, "deterministic");
    assert_eq!(refusal.reason, REFUSAL);
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
