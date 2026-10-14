// bsv-low #657: acceptance witnesses, intentionally RED until atomic admission.
// Included as a child of memory so snapshots see every table without a public API.
use super::MemoryStorage;
use crate::admission::*;
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
struct RefusingLookup {
    store: Rc<MemoryStorage>,
    refuse: bool,
}
#[async_trait(?Send)]
impl LookupService for RefusingLookup {
    fn reads_off_chain_values(&self, _: &str) -> bool {
        false
    }
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
        if self.refuse && topic == REGISTRY && *output_index == 2 {
            return Err(LookupRefusal::Deterministic(REFUSAL.into()).into());
        }
        let key = serde_json::to_vec(&(txid, output_index, topic)).unwrap();
        let count = self
            .store
            .admission_row("lookup_index", &key)
            .map_or(0, |v| serde_json::from_slice::<u64>(&v).unwrap())
            + 1;
        self.store
            .put_admission_row("lookup_index", &key, &serde_json::to_vec(&count).unwrap())
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;
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
    let log = store.log.lock().unwrap();
    tables.insert(
        "log_rows",
        serde_json::to_vec(&log.rows.iter().collect::<Vec<_>>()).unwrap(),
    );
    tables.insert("log_units", serde_json::to_vec(&log.units).unwrap());
    tables.insert("log_outbox", serde_json::to_vec(&log.outbox).unwrap());
    tables.insert("log_head", serde_json::to_vec(&log.head).unwrap());
    // Fault switches and read/write instrumentation are not persisted tables.
    serde_json::to_vec(&tables).unwrap()
}

async fn fixture() -> (Engine, Rc<MemoryStorage>, Index, TaggedBEEF, String) {
    fixture_refusal(true).await
}

async fn fixture_refusal(refuse: bool) -> (Engine, Rc<MemoryStorage>, Index, TaggedBEEF, String) {
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
        Box::new(RefusingLookup {
            store: store.clone(),
            refuse,
        }) as Box<dyn LookupService>,
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

async fn submit_log(
    engine: &Engine,
    store: &MemoryStorage,
    beef: &TaggedBEEF,
) -> Result<(Steak, MutationReport), LogError> {
    let walked = engine
        .walk(
            beef,
            SubmitMode::HistoricalTxNoSpv,
            crate::engine::WalkBreachPolicy::NotNow,
            &NoEvidence,
        )
        .await
        .unwrap();
    store.admit(engine, &walked).map(|a| (a.steak, a.report))
}

fn refused(error: &LogError) {
    assert!(
        matches!(error, LogError::Refused(AdmitRefusal::Deterministic { topic, site, reason, .. })
        if topic == REGISTRY && site == "lookup_service.output_admitted_by_topic" && reason == REFUSAL),
        "{error:?}"
    );
}

#[tokio::test]
async fn e657_refused_submit_leaves_store_byte_equal() {
    let (engine, store, index, beef, parent) = fixture().await;
    let before = snapshot(&store, &index);
    let error = submit_log(&engine, &store, &beef).await.unwrap_err();
    refused(&error);
    assert!(
        before == snapshot(&store, &index),
        "bsv-low #657: refused submit changed stored bytes"
    );
    assert!(
        !store
            .find_output(&parent, 0, Some(REGISTRY), None, true)
            .await
            .unwrap()
            .unwrap()
            .spent
    );
}

#[tokio::test]
async fn e657_same_bytes_same_word_after_refusal() {
    let (engine, store, index, beef, _) = fixture().await;
    let before = snapshot(&store, &index);
    let first = submit_log(&engine, &store, &beef).await.unwrap_err();
    let second = submit_log(&engine, &store, &beef).await.unwrap_err();
    refused(&first);
    refused(&second);
    assert_eq!(
        first.to_string(),
        second.to_string(),
        "bsv-low #657: identical bytes changed the word"
    );
    assert!(
        before == snapshot(&store, &index),
        "both refusals must preserve every stored byte"
    );
    let accepting = accepting_engine(&store);
    let (third, _) = submit_log(&accepting, &store, &beef).await.unwrap();
    assert_eq!(third[REGISTRY].outputs_to_admit, vec![0, 2]);
    assert_eq!(third[REGISTRY].coins_removed, Some(vec![0]));
    let (clean_engine, clean_store, clean_index, clean_beef, _) = fixture_refusal(false).await;
    let (clean, _) = submit_log(&clean_engine, &clean_store, &clean_beef)
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(third).unwrap(),
        serde_json::to_value(clean).unwrap()
    );
    assert!(snapshot(&store, &index) == snapshot(&clean_store, &clean_index));
}

fn accepting_engine(store: &Rc<MemoryStorage>) -> Engine {
    Engine::new(
        HashMap::from([
            (
                REGISTRY.into(),
                Box::new(RegistryManager) as Box<dyn TopicManager>,
            ),
            (
                PROOF.into(),
                Box::new(ProofManager) as Box<dyn TopicManager>,
            ),
        ]),
        HashMap::from([(
            "index".into(),
            Box::new(RefusingLookup {
                store: store.clone(),
                refuse: false,
            }) as Box<dyn LookupService>,
        )]),
        Box::new(store.clone()),
        None,
        EngineConfig::default(),
    )
}

// Test 3/11: exercise real engine writes, not a mock commit implementation.
#[tokio::test]
async fn a1_03_each_write_fault_rolls_back() {
    let (clean_engine, clean_store, clean_index, beef, _) = fixture_refusal(false).await;
    clean_store.fault_write(None);
    submit_log(&clean_engine, &clean_store, &beef)
        .await
        .unwrap();
    let writes = clean_store.admission_fault.lock().unwrap().1;
    assert!(
        writes >= 8,
        "fixture covers marks, inserts, index, deletes and applied rows"
    );
    let clean = snapshot(&clean_store, &clean_index);
    for at in 1..=writes {
        let (engine, store, index, beef, _) = fixture_refusal(false).await;
        let before = snapshot(&store, &index);
        store.fault_write(Some(at));
        assert!(matches!(
            submit_log(&engine, &store, &beef).await,
            Err(LogError::Refused(AdmitRefusal::Transient { .. }))
        ));
        assert!(
            before == snapshot(&store, &index),
            "a1_03 write {at} left rows"
        );
        store.fault_write(None);
        submit_log(&engine, &store, &beef).await.unwrap();
        assert!(
            clean == snapshot(&store, &index),
            "write {at} replay differs from clean admission"
        );
    }
}

#[tokio::test]
async fn a1_11_applied_row_fault_rolls_back() {
    let (engine, store, index, mut beef, _) = fixture().await;
    // The proof topic writes its output then its applied row.
    beef.topics = vec![PROOF.into()];
    let before = snapshot(&store, &index);
    store.fault_write(Some(3));
    let _ = submit_log(&engine, &store, &beef).await;
    assert!(
        before == snapshot(&store, &index),
        "a1_11 applied fault left output and index"
    );
}

#[tokio::test]
async fn a1_07_predecessor_commits_but_refused_successor_does_not() {
    let (engine, store, index, mut beef, parent) = fixture().await;
    // The fixture's proven previous coin is a separately landed unit.
    let predecessor = store
        .find_output(&parent, 0, Some(REGISTRY), None, true)
        .await
        .unwrap()
        .unwrap();
    let first = TaggedBEEF::new(predecessor.beef.clone().unwrap(), vec![REGISTRY.into()]);
    submit_log(&engine, &store, &first).await.unwrap();
    let before = snapshot(&store, &index);
    beef.topics = vec![REGISTRY.into()];
    let _ = submit_log(&engine, &store, &beef).await;
    assert!(
        before == snapshot(&store, &index),
        "a1_07 refused successor changed predecessor unit"
    );
}

struct AwaitingLookup;
#[async_trait(?Send)]
impl LookupService for AwaitingLookup {
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
        // A real timer promise, not an already-ready async function.
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
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

#[tokio::test]
async fn a1_27_real_promise_is_loud_and_rolls_back() {
    let (_, store, index, mut beef, _) = fixture().await;
    beef.topics = vec![PROOF.into()];
    let engine = Engine::new(
        HashMap::from([(
            PROOF.into(),
            Box::new(ProofManager) as Box<dyn TopicManager>,
        )]),
        HashMap::from([(
            "awaiting".into(),
            Box::new(AwaitingLookup) as Box<dyn LookupService>,
        )]),
        Box::new(store.clone()),
        None,
        EngineConfig::default(),
    );
    let before = snapshot(&store, &index);
    let result = submit_log(&engine, &store, &beef).await;
    assert!(
        before == snapshot(&store, &index),
        "a1_27 awaited promise committed rows: {result:?}"
    );
    assert!(
        matches!(result, Err(LogError::Refused(AdmitRefusal::Transient { site, reason, .. })) if site == "admit_awaited_io" && reason.contains("awaiting.output_admitted_by_topic"))
    );
    assert_eq!(store.awaited_io(), 1);
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    assert!(
        before == snapshot(&store, &index),
        "dropped continuation must never resume"
    );
}

struct RetainManager;
#[async_trait(?Send)]
impl TopicManager for RetainManager {
    async fn identify_admissible_outputs(
        &self,
        _: &Transaction,
        previous: &[u8],
        _: Option<&[u8]>,
        _: SubmitMode,
        _: &TopicAdmittanceContext,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        if previous.is_empty() {
            return Err(TopicManagerError::NoAdmissibleOutputs(
                "coin already spent".into(),
            ));
        }
        Ok(AdmittanceInstructions {
            outputs_to_admit: vec![0],
            coins_to_retain: vec![0],
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

#[tokio::test]
async fn a1_05_two_spenders_judge_the_serial_store() {
    let (_, store, _, mut first, _) = fixture().await;
    first.topics = vec![REGISTRY.into()];
    let mut tx = Transaction::from_beef(&first.beef, None).unwrap();
    tx.lock_time += 1;
    tx.invalidate_caches();
    tx.merkle_path =
        Some(MerklePath::new(100, vec![vec![MerklePathLeaf::new_txid(0, tx.id())]]).unwrap());
    let second = TaggedBEEF::new(tx.to_beef(false).unwrap(), first.topics.clone());
    let engine = Engine::new(
        HashMap::from([(
            REGISTRY.into(),
            Box::new(RetainManager) as Box<dyn TopicManager>,
        )]),
        HashMap::new(),
        Box::new(store.clone()),
        None,
        EngineConfig::default(),
    );
    let (a, b) = tokio::join!(
        submit_log(&engine, &store, &first),
        submit_log(&engine, &store, &second)
    );
    let landed = [a, b]
        .into_iter()
        .filter(|r| {
            r.as_ref()
                .is_ok_and(|(_, report)| !report.applied_topics.is_empty())
        })
        .count();
    assert_eq!(
        landed, 1,
        "a1_05 both spends admitted against one retained coin"
    );
}

struct ReadEmitLookup {
    store: Rc<MemoryStorage>,
}
#[async_trait(?Send)]
impl LookupService for ReadEmitLookup {
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
        assert!(
            self.store
                .find_output(txid, *output_index, Some(topic), None, false)
                .await
                .unwrap()
                .is_some(),
            "service must read its unit's inserted row"
        );
        self.store
            .emit(OutboxEvent {
                txid: txid.clone(),
                topic: topic.clone(),
                output_index: *output_index,
                event_type: "admitted".into(),
                envelope: b"frozen-envelope".to_vec(),
            })
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;
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

#[tokio::test]
async fn a1_13_read_own_write_stages_event_until_drain() {
    let (_, store, sent, mut beef, _) = fixture().await;
    beef.topics = vec![PROOF.into()];
    let engine = Engine::new(
        HashMap::from([(
            PROOF.into(),
            Box::new(ProofManager) as Box<dyn TopicManager>,
        )]),
        HashMap::from([(
            "read_emit".into(),
            Box::new(ReadEmitLookup {
                store: store.clone(),
            }) as Box<dyn LookupService>,
        )]),
        Box::new(store.clone()),
        None,
        EngineConfig::default(),
    );
    submit_log(&engine, &store, &beef).await.unwrap();
    assert!(
        sent.lock().unwrap().is_empty(),
        "a1_13 event escaped before outbox drain"
    );
    let rows = store.outbox();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].event.envelope, b"frozen-envelope");
    assert_eq!(
        store
            .drain_outbox(|_| async { Err("queue down".into()) })
            .await,
        Err("queue down".into())
    );
    assert_eq!(store.outbox()[0].attempts, 1);
    let count = store
        .drain_outbox(|event| {
            sent.lock()
                .unwrap()
                .insert((event.txid, event.output_index, event.topic), 1);
            async { Ok(()) }
        })
        .await
        .unwrap();
    assert_eq!(count, 1);
    assert!(store.outbox().is_empty());
    assert_eq!(sent.lock().unwrap().len(), 1);
    assert_eq!(
        store
            .drain_outbox(|_| async { panic!("already sent") })
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn a1_statement_guard_catches_swallowed_manager_write() {
    let (_, store, index, _, _) = fixture().await;
    let before = snapshot(&store, &index);
    store.fault_write(Some(2));
    let result = store.transaction("manager", &AdmitEvidence::default(), || async {
        store.put_admission_row("heads", b"one", b"first").unwrap();
        let _ = store.put_admission_row("heads", b"two", b"second");
        Ok(())
    });
    assert!(
        matches!(result, Err(LogError::Refused(AdmitRefusal::Transient { site, .. })) if site == "admit_statement_failed")
    );
    assert!(before == snapshot(&store, &index));
}

#[tokio::test]
async fn a1_lost_answer_is_ambiguous_and_replay_deduplicates() {
    for point in [
        super::CrashPoint::BeforeUnit,
        super::CrashPoint::AfterCommit,
    ] {
        let (engine, store, index, beef, _) = fixture_refusal(false).await;
        let before = snapshot(&store, &index);
        let walked = engine
            .walk(
                &beef,
                SubmitMode::HistoricalTxNoSpv,
                crate::engine::WalkBreachPolicy::NotNow,
                &NoEvidence,
            )
            .await
            .unwrap();
        store.crash_at(point);
        assert!(matches!(
            store.admit(&engine, &walked),
            Err(LogError::AnswerLost)
        ));
        if point == super::CrashPoint::BeforeUnit {
            assert!(before == snapshot(&store, &index));
        } else {
            assert_eq!(store.log_head(), 1);
        }
        store.admit(&engine, &walked).unwrap();
        let committed = snapshot(&store, &index);
        let replay = store.admit(&engine, &walked).unwrap();
        assert_eq!(replay.report.deduped_topics.len(), 2);
        assert_eq!(store.log_units().len(), 1);
        assert!(committed == snapshot(&store, &index));
    }
}

#[tokio::test]
async fn a1_missing_evidence_and_notes_are_per_unit() {
    let (engine, store, index, beef, _) = fixture_refusal(false).await;
    let before = snapshot(&store, &index);
    let mut walked = engine
        .walk(
            &beef,
            SubmitMode::HistoricalTxNoSpv,
            crate::engine::WalkBreachPolicy::NotNow,
            &NoEvidence,
        )
        .await
        .unwrap();
    walked.evidence.tx.clear();
    assert!(
        matches!(store.admit(&engine, &walked), Err(LogError::Refused(AdmitRefusal::NotNow { site, .. })) if site == "evidence_missing")
    );
    assert!(before == snapshot(&store, &index));
    let result: Result<(), _> = store.transaction("notes", &AdmitEvidence::default(), || async {
        store.note(
            REGISTRY,
            RefusalNote {
                kind: "head_race".into(),
                reason: "terminal".into(),
            },
        );
        Err(AdmitRefusal::transient("test", "failure"))
    });
    assert!(matches!(result, Err(LogError::Refused(error)) if error.notes().len() == 1));
    store
        .transaction("next", &AdmitEvidence::default(), || async {
            assert!(store.notes().is_empty());
            Ok(())
        })
        .unwrap();
}

#[test]
fn a1_evidence_roundtrips_broadcaster_proof_and_sidecar() {
    let mut evidence = AdmitEvidence::default();
    evidence.tx.insert(
        "subject".into(),
        TxEvidence {
            walk: WalkOutcome::Spv,
            network: Some(NetworkVerdict::Accepted {
                by: NetworkSource::Broadcaster {
                    name: "Arcade".into(),
                    at_ms: 42,
                },
                status: "SEEN".into(),
            }),
            proof: Some(ProofVerdict::Holds {
                height: 100,
                merkle_root: [7; 32],
            }),
        },
    );
    evidence.sidecars.insert(
        [9; 32],
        SidecarVerdict::Stored {
            key: "content-hash".into(),
        },
    );
    let bytes = serde_json::to_vec(&evidence).unwrap();
    assert_eq!(
        serde_json::from_slice::<AdmitEvidence>(&bytes).unwrap(),
        evidence
    );
}

#[tokio::test]
async fn a1_dry_executor_rejects_even_swallowed_writes() {
    let (_, store, index, _, _) = fixture().await;
    let before = snapshot(&store, &index);
    let scope = store.admission_dry_run(&AdmitEvidence::default()).unwrap();
    let _ = store.put_admission_row("head", b"key", b"value");
    assert!(scope.failure().is_some());
    drop(scope);
    assert!(before == snapshot(&store, &index));
}

struct ChainManager;
#[async_trait(?Send)]
impl TopicManager for ChainManager {
    fn reads_off_chain_values(&self) -> bool {
        false
    }
    async fn identify_admissible_outputs(
        &self,
        tx: &Transaction,
        previous: &[u8],
        _: Option<&[u8]>,
        _: SubmitMode,
        _: &TopicAdmittanceContext,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        Ok(AdmittanceInstructions {
            outputs_to_admit: if previous.is_empty() {
                vec![]
            } else if tx.outputs.len() == 3 {
                vec![0, 2]
            } else {
                vec![0]
            },
            coins_to_retain: vec![],
            coins_removed: None,
        })
    }
    async fn identify_needed_inputs(
        &self,
        beef: &[u8],
        _: Option<&[u8]>,
    ) -> Result<Vec<Outpoint>, TopicManagerError> {
        let tx = Transaction::from_beef(beef, None).unwrap();
        Ok(tx
            .inputs
            .iter()
            .map(|i| Outpoint::new(i.get_source_txid().unwrap(), i.source_output_index))
            .collect())
    }
    async fn get_documentation(&self) -> String {
        String::new()
    }
    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata::default()
    }
}

#[tokio::test]
async fn a1_07_carried_body_has_its_own_transaction() {
    let (_, store, _, _, parent) = fixture().await;
    let held = store
        .find_output(&parent, 0, Some(REGISTRY), None, true)
        .await
        .unwrap()
        .unwrap();
    let funding = Transaction::from_beef(&held.beef.unwrap(), None).unwrap();
    let mut middle = Transaction::new();
    let mut input = TransactionInput::new(parent.clone(), 0);
    input.source_transaction = Some(Box::new(funding));
    input.unlocking_script = Some(UnlockingScript::from_binary(&[0x51]).unwrap());
    middle.inputs.push(input);
    middle.outputs.push(TransactionOutput::new(
        3,
        LockingScript::from_binary(&[0x51]).unwrap(),
    ));
    let middle_id = middle.id();
    let mut subject = Transaction::new();
    let mut input = TransactionInput::new(middle_id.clone(), 0);
    input.source_transaction = Some(Box::new(middle));
    input.unlocking_script = Some(UnlockingScript::from_binary(&[0x51]).unwrap());
    subject.inputs.push(input);
    for _ in 0..3 {
        subject.outputs.push(TransactionOutput::new(
            1,
            LockingScript::from_binary(&[0x51]).unwrap(),
        ));
    }
    let subject_id = subject.id();
    let beef = TaggedBEEF::new(subject.to_beef(false).unwrap(), vec![REGISTRY.into()]);
    let engine = Engine::new(
        HashMap::from([(
            REGISTRY.into(),
            Box::new(ChainManager) as Box<dyn TopicManager>,
        )]),
        HashMap::from([(
            "index".into(),
            Box::new(RefusingLookup {
                store: store.clone(),
                refuse: true,
            }) as Box<dyn LookupService>,
        )]),
        Box::new(store.clone()),
        None,
        EngineConfig::default(),
    );
    let walked = engine
        .walk(
            &beef,
            SubmitMode::HistoricalTxNoSpv,
            crate::engine::WalkBreachPolicy::NotNow,
            &NoEvidence,
        )
        .await
        .unwrap();
    assert!(walked.evidence.tx.contains_key(&middle_id));
    let error = store.admit(&engine, &walked).unwrap_err();
    refused(&error);
    assert_eq!(store.log_head(), 1);
    assert_eq!(store.log_units()[0].txid, middle_id);
    assert!(
        !store
            .find_output(&middle_id, 0, Some(REGISTRY), None, false)
            .await
            .unwrap()
            .unwrap()
            .spent
    );
    assert!(store
        .find_outputs_for_transaction(&subject_id, false)
        .await
        .unwrap()
        .is_empty());
    assert!(store
        .find_output(&parent, 0, Some(REGISTRY), None, false)
        .await
        .unwrap()
        .is_none());
}
