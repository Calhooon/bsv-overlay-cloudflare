// bsv-low #657: validate-before-apply acceptance witnesses adapted from the spike.
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
    fn check_admitted(&self, payload: &OutputAdmittedByTopic) -> Result<(), LookupServiceError> {
        if matches!(payload, OutputAdmittedByTopic::LockingScript { topic, output_index: 2, .. } if topic == REGISTRY)
        {
            return Err(LookupServiceError::Other(REFUSAL.into()));
        }
        Ok(())
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
    fixture_with_services(None).await
}

async fn fixture_with_services(
    services: Option<HashMap<String, Box<dyn LookupService>>>,
) -> (Engine, Rc<MemoryStorage>, Index, TaggedBEEF, String) {
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
    let beef = TaggedBEEF::new(tx.to_beef(false).unwrap(), vec![REGISTRY.into()]);
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
    let services = services.unwrap_or_else(|| {
        HashMap::from([(
            "ls_refuse_657".into(),
            Box::new(RefusingLookup(index.clone())) as Box<dyn LookupService>,
        )])
    });
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
    assert_eq!(
        report.refused.len(),
        1,
        "fixture reaches exactly the intended refusal"
    );
    assert_eq!(report.refused[0].topic, REGISTRY);
    assert_eq!(report.refused[0].service, "ls_refuse_657");
    assert_eq!(report.refused[0].site, "lookup_service.check_admitted");
    assert!(
        matches!(&report.refused[0].error, LookupServiceError::Other(error) if error == REFUSAL)
    );
}

#[tokio::test]
async fn e657_refused_submit_leaves_store_byte_equal() {
    let (engine, store, index, beef, parent) = fixture().await;
    let before = snapshot(&store, &index);
    let (steak, report) = engine
        .submit_with_report(&beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .unwrap();
    assert_eq!(steak[REGISTRY].outputs_to_admit, vec![0, 2]);
    let after = snapshot(&store, &index);
    // A compact diagnostic keeps the RED log readable while equality checks every byte.
    assert!(
        before == after,
        "bsv-low #657: refused submit changed stored bytes (before {}, after {})",
        before.len(),
        after.len()
    );
    refused(&report);
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
    let (second_steak, second_report) = engine
        .submit_with_report(&beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .unwrap();
    // Report and STEAK together are the engine's word, no HTTP mapping invented.
    assert_eq!(
        second_report, first_report,
        "bsv-low #657: identical bytes changed the word"
    );
    refused(&first_report);
    assert_eq!(
        serde_json::to_value(second_steak).unwrap(),
        serde_json::to_value(first_steak).unwrap()
    );
    assert!(
        before == snapshot(&store, &index),
        "both refusals must preserve every stored byte"
    );
}

#[derive(Default)]
struct Calls {
    checked_admitted: Vec<String>,
    checked_spent: Vec<String>,
    admitted: Vec<String>,
    spent: Vec<String>,
}
struct Probe {
    calls: Rc<Mutex<Calls>>,
    admission: AdmissionMode,
    spend: SpendNotificationMode,
    refuse_spent: bool,
}
#[async_trait(?Send)]
impl LookupService for Probe {
    fn admission_mode(&self) -> AdmissionMode {
        self.admission
    }
    fn spend_notification_mode(&self) -> SpendNotificationMode {
        self.spend
    }
    fn check_admitted(&self, payload: &OutputAdmittedByTopic) -> Result<(), LookupServiceError> {
        self.calls
            .lock()
            .unwrap()
            .checked_admitted
            .push(format!("{payload:?}"));
        Ok(())
    }
    fn check_spent(&self, payload: &OutputSpent) -> Result<(), LookupServiceError> {
        self.calls
            .lock()
            .unwrap()
            .checked_spent
            .push(format!("{payload:?}"));
        if self.refuse_spent {
            return Err(LookupServiceError::Other("spent refusal".into()));
        }
        Ok(())
    }
    async fn output_admitted_by_topic(
        &self,
        payload: &OutputAdmittedByTopic,
    ) -> Result<(), LookupServiceError> {
        self.calls
            .lock()
            .unwrap()
            .admitted
            .push(format!("{payload:?}"));
        Ok(())
    }
    async fn output_spent(&self, payload: &OutputSpent) -> Result<(), LookupServiceError> {
        self.calls
            .lock()
            .unwrap()
            .spent
            .push(format!("{payload:?}"));
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
fn probes(
    refuse_spent: bool,
) -> (
    HashMap<String, Box<dyn LookupService>>,
    Vec<Rc<Mutex<Calls>>>,
) {
    let mut services: HashMap<String, Box<dyn LookupService>> = HashMap::new();
    let mut captures = vec![];
    for admission in [AdmissionMode::LockingScript, AdmissionMode::WholeTx] {
        for spend in [
            SpendNotificationMode::None,
            SpendNotificationMode::Txid,
            SpendNotificationMode::Script,
            SpendNotificationMode::WholeTx,
        ] {
            let calls = Rc::new(Mutex::new(Calls::default()));
            services.insert(
                format!("probe_{}", captures.len()),
                Box::new(Probe {
                    calls: calls.clone(),
                    admission,
                    spend,
                    refuse_spent,
                }),
            );
            captures.push(calls);
        }
    }
    (services, captures)
}

#[tokio::test]
async fn e657_checks_once_per_payload_and_service() {
    let (services, captures) = probes(false);
    let (engine, _, _, mut beef, _) = fixture_with_services(Some(services)).await;
    beef.off_chain_values = Some(vec![1, 2, 3]);
    let (_, report) = engine
        .submit_with_report(&beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .unwrap();
    assert!(report.is_durable());
    for capture in &captures {
        let calls = capture.lock().unwrap();
        assert_eq!(
            calls.checked_admitted.len(),
            2,
            "one admit check per output and service"
        );
        assert_eq!(
            calls.checked_spent.len(),
            1,
            "one spent check per coin and service"
        );
        assert_eq!(
            calls.checked_admitted, calls.admitted,
            "checked payload equals notified payload"
        );
        assert_eq!(
            calls.checked_spent, calls.spent,
            "checked payload equals notified payload"
        );
    }
    // A deduplicated submit applies nothing and checks nothing again.
    engine
        .submit_with_report(&beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .unwrap();
    for capture in &captures {
        let calls = capture.lock().unwrap();
        assert_eq!(calls.checked_admitted.len(), 2);
        assert_eq!(calls.checked_spent.len(), 1);
    }
}

#[tokio::test]
async fn e657_checks_never_on_dry_run() {
    let (services, captures) = probes(false);
    let (engine, store, index, beef, _) = fixture_with_services(Some(services)).await;
    let before = snapshot(&store, &index);
    engine
        .submit_validate_only(&beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .unwrap();
    assert_eq!(before, snapshot(&store, &index));
    for capture in &captures {
        let calls = capture.lock().unwrap();
        assert!(calls.checked_admitted.is_empty());
        assert!(calls.checked_spent.is_empty());
    }
    // The real apply after a dry run must still perform its checks.
    engine
        .submit_with_report(&beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .unwrap();
    for capture in &captures {
        let calls = capture.lock().unwrap();
        assert_eq!(
            calls.checked_admitted.len(),
            2,
            "real apply after dry run must check"
        );
        assert_eq!(calls.checked_spent.len(), 1);
    }
}

#[tokio::test]
async fn e657_spent_refusal_leaves_store_byte_equal() {
    let (services, captures) = probes(true);
    let (engine, store, index, beef, _) = fixture_with_services(Some(services)).await;
    let before = snapshot(&store, &index);
    let (_, report) = engine
        .submit_with_report(&beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .unwrap();
    assert!(
        before == snapshot(&store, &index),
        "spent refusal changed stored bytes"
    );
    assert!(!report.is_durable());
    for capture in captures {
        let calls = capture.lock().unwrap();
        assert_eq!(calls.checked_admitted.len(), 2);
        assert_eq!(calls.checked_spent.len(), 1);
        assert!(calls.admitted.is_empty());
        assert!(calls.spent.is_empty());
    }
}

#[tokio::test]
async fn e657_report_types_both_refusal_sites_and_service_errors() {
    let (engine, _, _, beef, _) = fixture().await;
    let (_, report) = engine
        .submit_with_report(&beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .unwrap();
    assert!(
        report.faults.is_empty(),
        "a pure refusal is not a mutation fault"
    );
    assert!(!report.is_durable());
    assert!(report.applied_topics.is_empty());
    assert_eq!(report.refused.len(), 1);
    let refusal = &report.refused[0];
    assert_eq!(refusal.topic, REGISTRY);
    assert_eq!(refusal.service, "ls_refuse_657");
    assert_eq!(refusal.site, "lookup_service.check_admitted");
    assert!(matches!(&refusal.error, LookupServiceError::Other(error) if error == REFUSAL));
    assert!(report.summary().contains("lookup_service.check_admitted"));

    let (services, _) = probes(true);
    let (engine, _, _, beef, _) = fixture_with_services(Some(services)).await;
    let (_, report) = engine
        .submit_with_report(&beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .unwrap();
    assert!(report.faults.is_empty());
    assert!(!report.is_durable());
    assert_eq!(report.refused.len(), 8);
    for (i, refusal) in report.refused.iter().enumerate() {
        assert_eq!(refusal.topic, REGISTRY);
        assert_eq!(refusal.service, format!("probe_{i}"));
        assert_eq!(refusal.site, "lookup_service.check_spent");
        assert!(
            matches!(&refusal.error, LookupServiceError::Other(error) if error == "spent refusal")
        );
    }
}
