#![cfg(feature = "memory-storage")]
use async_trait::async_trait;
use bsv_overlay_engine::engine::{Engine, EngineConfig};
use bsv_overlay_engine::lookup_service::{LookupRefusal, LookupService, LookupServiceError};
use bsv_overlay_engine::storage::{
    memory::{MemoryStorage, MemoryWrite},
    AdmissionUnit, OutboxEvent, StagedWrite, Storage,
};
use bsv_overlay_engine::topic_manager::{TopicManager, TopicManagerError};
use bsv_overlay_engine::types::*;
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{
    MerklePath, MerklePathLeaf, Transaction, TransactionInput, TransactionOutput,
};
use std::{cell::Cell, collections::HashMap, rc::Rc};
const TOPIC: &str = "tm_atomic";

struct Manager;
#[async_trait(?Send)]
impl TopicManager for Manager {
    fn reads_off_chain_values(&self) -> bool {
        false
    }
    async fn identify_admissible_outputs(
        &self,
        tx: &Transaction,
        _: &[u8],
        _: Option<&[u8]>,
        _: SubmitMode,
        context: &TopicAdmittanceContext<'_>,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        if let Some(unit) = &context.unit {
            let mut unit = unit.borrow_mut();
            let key = format!("{}:0", tx.id());
            unit.stage_record(&key, key.clone(), key.len() as u64);
            unit.stage(StagedWrite::new(
                MemoryWrite::Increment {
                    table: "judgements".into(),
                    key: tx.id(),
                },
                8,
            ));
            unit.stage(StagedWrite::new(
                MemoryWrite::Put {
                    table: "admit".into(),
                    key: key.clone(),
                    value: key.as_bytes().to_vec(),
                },
                2 * key.len() as u64,
            ));
        }
        Ok(AdmittanceInstructions {
            outputs_to_admit: vec![0],
            coins_to_retain: vec![],
            coins_removed: None,
        })
    }
    async fn identify_needed_inputs(
        &self,
        beef: &[u8],
        _: Option<&[u8]>,
    ) -> Result<Vec<Outpoint>, TopicManagerError> {
        let tx = Transaction::from_beef(beef, None)
            .map_err(|e| TopicManagerError::Other(e.to_string()))?;
        Ok(tx
            .inputs
            .iter()
            .filter_map(|i| {
                i.get_source_txid()
                    .ok()
                    .map(|t| Outpoint::new(t, i.source_output_index))
            })
            .collect())
    }
    async fn get_documentation(&self) -> String {
        String::new()
    }
    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata::default()
    }
}
struct StagedLookup {
    store: Rc<MemoryStorage>,
    refuse: Rc<Cell<bool>>,
    effects: Rc<Cell<usize>>,
}
#[async_trait(?Send)]
impl LookupService for StagedLookup {
    fn is_staged(&self) -> bool {
        true
    }
    fn reads_off_chain_values(&self, _: &str) -> bool {
        false
    }
    fn admission_mode(&self) -> AdmissionMode {
        AdmissionMode::LockingScript
    }
    fn spend_notification_mode(&self) -> SpendNotificationMode {
        SpendNotificationMode::None
    }
    async fn stage_admitted(
        &self,
        payload: &OutputAdmittedByTopic,
        unit: &mut dyn AdmissionUnit,
    ) -> Result<(), LookupRefusal> {
        if self.refuse.get() {
            return Err(LookupRefusal::Deterministic("fixture refusal".into()));
        }
        let OutputAdmittedByTopic::LockingScript {
            txid, output_index, ..
        } = payload
        else {
            unreachable!()
        };
        let key = format!("{txid}:{output_index}");
        let envelope = unit
            .read_staged::<String>(&key)
            .map(|s| s.as_bytes().to_vec())
            .or_else(|| self.store.admission_index("admit", &key))
            .ok_or_else(|| LookupRefusal::Deterministic("missing manager record".into()))?;
        unit.emit(OutboxEvent {
            event_type: "admitted".into(),
            txid: txid.clone(),
            envelope,
        });
        Ok(())
    }
    async fn after_commit(
        &self,
        admitted: &[OutputAdmittedByTopic],
        _: &[OutputSpent],
    ) -> Result<(), LookupServiceError> {
        self.effects.set(self.effects.get() + admitted.len());
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
struct Bridge(Rc<Cell<usize>>);
#[async_trait(?Send)]
impl LookupService for Bridge {
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
        _: &OutputAdmittedByTopic,
    ) -> Result<(), LookupServiceError> {
        self.0.set(self.0.get() + 1);
        Err(LookupServiceError::Other(
            "bridge failed after commit".into(),
        ))
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
struct Fixture {
    engine: Engine,
    store: Rc<MemoryStorage>,
    beef: TaggedBEEF,
    txid: String,
    parent: String,
    refuse: Rc<Cell<bool>>,
    effects: Rc<Cell<usize>>,
    bridge: Rc<Cell<usize>>,
    root: Transaction,
}
fn child(parent: &Transaction) -> Transaction {
    let mut tx = Transaction::new();
    let mut input = TransactionInput::new(parent.id(), 0);
    input.unlocking_script = Some(UnlockingScript::from_binary(&[0x51]).unwrap());
    input.source_transaction = Some(Box::new(parent.clone()));
    tx.inputs.push(input);
    tx.outputs.push(TransactionOutput::new(
        1,
        LockingScript::from_binary(&[0x51]).unwrap(),
    ));
    tx
}
async fn fixture(with_bridge: bool) -> Fixture {
    let store = Rc::new(MemoryStorage::new());
    let mut root = Transaction::new();
    let mut input = TransactionInput::new("00".repeat(32), u32::MAX);
    input.unlocking_script = Some(UnlockingScript::from_binary(&[0x51]).unwrap());
    root.inputs.push(input);
    root.outputs.push(TransactionOutput::new(
        1,
        LockingScript::from_binary(&[0x51]).unwrap(),
    ));
    root.merkle_path =
        Some(MerklePath::new(99, vec![vec![MerklePathLeaf::new_txid(0, root.id())]]).unwrap());
    let parent = root.id();
    let tx = child(&root);
    let txid = tx.id();
    store
        .insert_output(&Output {
            txid: parent.clone(),
            output_index: 0,
            topic: TOPIC.into(),
            output_script: vec![0x51],
            satoshis: 1,
            spent: false,
            outputs_consumed: vec![],
            consumed_by: vec![],
            beef: Some(root.to_beef(false).unwrap()),
            block_height: Some(99),
            score: None,
        })
        .await
        .unwrap();
    let refuse = Rc::new(Cell::new(false));
    let effects = Rc::new(Cell::new(0));
    let bridge = Rc::new(Cell::new(0));
    let mut services: HashMap<String, Box<dyn LookupService>> = HashMap::from([(
        "ls_staged".into(),
        Box::new(StagedLookup {
            store: store.clone(),
            refuse: refuse.clone(),
            effects: effects.clone(),
        }) as Box<dyn LookupService>,
    )]);
    if with_bridge {
        services.insert("ls_bridge".into(), Box::new(Bridge(bridge.clone())));
    }
    let engine = Engine::new(
        HashMap::from([(TOPIC.into(), Box::new(Manager) as Box<dyn TopicManager>)]),
        services,
        Box::new(store.clone()),
        None,
        EngineConfig::default(),
    );
    Fixture {
        engine,
        store,
        beef: TaggedBEEF::new(tx.to_beef(false).unwrap(), vec![TOPIC.into()]),
        txid,
        parent,
        refuse,
        effects,
        bridge,
        root,
    }
}

#[tokio::test]
async fn a02_removed_refusal_matches_clean_first_submit() {
    let f = fixture(false).await;
    f.refuse.set(true);
    let before = f.store.admission_snapshot();
    let first = f
        .engine
        .submit_with_report(&f.beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .unwrap();
    let second = f
        .engine
        .submit_with_report(&f.beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .unwrap();
    assert_eq!(first.1, second.1);
    assert_eq!(before, f.store.admission_snapshot());
    f.refuse.set(false);
    let third = f
        .engine
        .submit_with_report(&f.beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .unwrap();
    let clean = fixture(false).await;
    let direct = clean
        .engine
        .submit_with_report(&clean.beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .unwrap();
    assert_eq!(third.1, direct.1);
    assert_eq!(
        serde_json::to_value(third.0).unwrap(),
        serde_json::to_value(direct.0).unwrap()
    );
    assert!(f
        .store
        .find_output(&f.parent, 0, Some(TOPIC), None, false)
        .await
        .unwrap()
        .is_none());
}
#[tokio::test]
async fn a04_midchain_fault_keeps_head_and_replay_lands() {
    let f = fixture(false).await;
    let before = f.store.admission_snapshot();
    f.store.set_admission_fault(Some(2));
    let (_, report) = f
        .engine
        .submit_with_report(&f.beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .unwrap();
    assert!(!report.is_durable());
    assert_eq!(before, f.store.admission_snapshot());
    f.store.set_admission_fault(None);
    let (_, report) = f
        .engine
        .submit_with_report(&f.beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .unwrap();
    assert!(report.is_durable());
    assert_eq!(f.store.output_count(), 1);
}
#[tokio::test]
async fn a06_external_effects_only_follow_commit() {
    let f = fixture(false).await;
    f.store.set_admission_fault(Some(2));
    f.engine
        .submit_with_report(&f.beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .unwrap();
    assert_eq!(f.effects.get(), 0);
    assert!(f.store.admission_outbox().is_empty());
    f.store.set_admission_fault(None);
    f.engine
        .submit_with_report(&f.beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .unwrap();
    assert_eq!(f.effects.get(), 1);
    assert_eq!(f.store.admission_outbox().len(), 1);
}
#[tokio::test]
async fn a07_carried_predecessor_has_its_own_unit() {
    let f = fixture(false).await;
    let intermediate = child(&f.root);
    let successor = child(&intermediate);
    let beef = TaggedBEEF::new(successor.to_beef(false).unwrap(), vec![TOPIC.into()]);
    f.store.set_admission_fault(Some(2));
    let before = f.store.admission_snapshot();
    let (_, report) = f
        .engine
        .submit_with_report(&beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .unwrap();
    assert!(!report.is_durable());
    assert_eq!(before, f.store.admission_snapshot());
    f.store.set_admission_fault(None);
    let (_, report) = f
        .engine
        .submit_with_report(&beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .unwrap();
    assert!(report.is_durable(), "{report:?}");
    assert_eq!(
        report.landed_predecessors,
        vec![(intermediate.id(), TOPIC.into())]
    );
    assert_eq!(
        f.store.admission_index("judgements", &successor.id()),
        Some(1_u64.to_le_bytes().to_vec())
    );
    assert_eq!(
        f.store.admission_index("judgements", &intermediate.id()),
        Some(1_u64.to_le_bytes().to_vec())
    );
    assert_eq!(f.store.output_count(), 1);
}
#[tokio::test]
async fn a13_first_admit_reads_manager_record_and_emits() {
    for role in ["pf_name", "pf_picture", "pf_content", "registry"] {
        let f = fixture(false).await;
        let (_, report) = f
            .engine
            .submit_with_report(&f.beef, SubmitMode::HistoricalTxNoSpv)
            .await
            .unwrap();
        assert!(report.is_durable(), "{role}: {report:?}");
        let rows = f.store.admission_outbox();
        assert_eq!(rows.len(), 1, "{role}");
        assert_eq!(
            rows[0].1.event.envelope,
            format!("{}:0", f.txid).into_bytes()
        );
    }
}
#[tokio::test]
async fn a15_bridge_runs_after_commit_and_fault_does_not_unapply() {
    let f = fixture(true).await;
    f.store.set_admission_fault(Some(2));
    f.engine
        .submit_with_report(&f.beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .unwrap();
    assert_eq!(f.bridge.get(), 0);
    assert_eq!(f.effects.get(), 0);
    f.store.set_admission_fault(None);
    let (_, report) = f
        .engine
        .submit_with_report(&f.beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .unwrap();
    assert_eq!(f.bridge.get(), 1);
    assert_eq!(f.effects.get(), 1);
    assert!(report.is_durable());
    assert_eq!(report.after_commit_faults.len(), 1);
    assert_eq!(report.after_commit_faults[0].site, "bridged_after_commit");
    assert!(f
        .store
        .does_applied_transaction_exist(&AppliedTransaction {
            txid: f.txid,
            topic: TOPIC.into()
        })
        .await
        .unwrap());
}
#[tokio::test]
async fn a16_renotify_stages_from_committed_record() {
    let f = fixture(false).await;
    f.engine
        .submit_with_report(&f.beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .unwrap();
    f.store.drain_admission_outbox(|_| Ok(()));
    let report = f.engine.renotify_admitted(&f.txid, TOPIC).await.unwrap();
    assert!(report.faults.is_empty());
    assert_eq!(report.notified, 1);
    assert_eq!(f.store.admission_outbox().len(), 1);
    f.store.drain_admission_outbox(|_| Ok(()));
    f.store.set_admission_fault(Some(0));
    let report = f.engine.renotify_admitted(&f.txid, TOPIC).await.unwrap();
    assert!(!report.faults.is_empty());
    assert!(f.store.admission_outbox().is_empty());
}
#[tokio::test]
async fn a20_runtime_shape_counts_every_service() {
    let f = fixture(true).await;
    let shape = f.engine.admission_shape();
    assert!(shape.atomic);
    assert_eq!(shape.staged, vec!["ls_staged"]);
    assert_eq!(shape.bridged, vec!["ls_bridge"]);
}

#[tokio::test]
async fn a12_engine_reports_unanswered_and_replay_dedups_whole_admission() {
    let f = fixture(false).await;
    f.store.set_admission_unanswered_after_commit(true);
    let (_, report) = f
        .engine
        .submit_with_report(&f.beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .unwrap();
    assert!(report.unanswered);
    assert!(!report.is_durable());
    assert_eq!(f.effects.get(), 0);
    assert_eq!(f.store.output_count(), 1);
    f.store.set_admission_unanswered_after_commit(false);
    let (_, report) = f
        .engine
        .submit_with_report(&f.beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .unwrap();
    assert!(report.is_durable());
    assert_eq!(report.deduped_topics, vec![TOPIC]);
    assert_eq!(f.store.output_count(), 1);
    assert_eq!(f.store.admission_outbox().len(), 1);
}

#[tokio::test]
async fn a17_dry_judgement_has_no_unit_and_sees_no_staged_record() {
    let f = fixture(false).await;
    let before = f.store.admission_snapshot();
    let mut pending = f.store.begin_admission();
    pending.stage_record(&format!("{}:0", f.txid), String::from("uncommitted"), 11);
    assert!(TopicAdmittanceContext::DRY_RUN.unit.is_none());
    let steak = f
        .engine
        .submit_validate_only(&f.beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .unwrap();
    assert_eq!(steak[TOPIC].outputs_to_admit, vec![0]);
    assert_eq!(before, f.store.admission_snapshot());
    assert_eq!(f.effects.get(), 0);
}

struct GuardedManager {
    verdict: bsv_overlay_engine::storage::PreconditionVerdict,
    classified: Rc<Cell<usize>>,
}
#[async_trait(?Send)]
impl TopicManager for GuardedManager {
    async fn identify_admissible_outputs(
        &self,
        tx: &Transaction,
        previous: &[u8],
        values: Option<&[u8]>,
        mode: SubmitMode,
        context: &TopicAdmittanceContext<'_>,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        let answer = Manager
            .identify_admissible_outputs(tx, previous, values, mode, context)
            .await?;
        if let Some(unit) = &context.unit {
            unit.borrow_mut()
                .require(bsv_overlay_engine::storage::Guard::Backend {
                    owner: TOPIC.into(),
                    check: Rc::new(bsv_overlay_engine::storage::memory::MemoryGuard {
                        table: "heads".into(),
                        key: "head".into(),
                        expected: Some(vec![1]),
                        lost: bsv_overlay_engine::storage::PreconditionVerdict::NotNow {
                            reason: "classify through the owner".into(),
                        },
                    }),
                    bytes: 9,
                });
        }
        Ok(answer)
    }
    async fn guard_lost(
        &self,
        check: &dyn std::any::Any,
    ) -> bsv_overlay_engine::storage::PreconditionVerdict {
        assert!(check.is::<bsv_overlay_engine::storage::memory::MemoryGuard>());
        self.classified.set(self.classified.get() + 1);
        self.verdict.clone()
    }
    async fn get_documentation(&self) -> String {
        String::new()
    }
    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata::default()
    }
}
#[tokio::test]
async fn a14_engine_asks_the_guard_owner_once_for_the_fresh_verdict() {
    use bsv_overlay_engine::storage::PreconditionVerdict;
    for verdict in [
        PreconditionVerdict::Replay,
        PreconditionVerdict::Contest {
            kind: "contest".into(),
            reason: "head lost".into(),
        },
        PreconditionVerdict::NotNow {
            reason: "predecessor missing".into(),
        },
    ] {
        let f = fixture(false).await;
        let before = f.store.admission_snapshot();
        let classified = Rc::new(Cell::new(0));
        let engine = Engine::new(
            HashMap::from([(
                TOPIC.into(),
                Box::new(GuardedManager {
                    verdict: verdict.clone(),
                    classified: classified.clone(),
                }) as Box<dyn TopicManager>,
            )]),
            HashMap::new(),
            Box::new(f.store.clone()),
            None,
            EngineConfig::default(),
        );
        let (steak, report) = engine
            .submit_with_report(&f.beef, SubmitMode::HistoricalTxNoSpv)
            .await
            .unwrap();
        assert_eq!(classified.get(), 1);
        assert_eq!(before, f.store.admission_snapshot());
        match verdict {
            PreconditionVerdict::Replay => {
                assert!(report.is_durable());
                assert!(steak[TOPIC].outputs_to_admit.is_empty());
                assert_eq!(report.deduped_topics, vec![TOPIC]);
            }
            PreconditionVerdict::Contest { .. } => {
                let refusal = report.refused.unwrap();
                assert_eq!(refusal.class, "deterministic");
                assert_eq!(refusal.kind.as_deref(), Some("contest"));
            }
            PreconditionVerdict::NotNow { .. } => {
                assert!(report.refused.is_none());
                assert_eq!(report.faults[0].site, "predecessor_not_landed");
            }
        }
    }
}

#[tokio::test]
async fn a16_renotify_does_not_bridge_an_open_journal() {
    let f = fixture(false).await;
    f.engine.submit_with_report(&f.beef, SubmitMode::HistoricalTxNoSpv).await.unwrap();
    let effects = f.effects.get();
    f.store.set_admission_statement_bound(Some(0));
    f.store.set_admission_decision_only(true);
    let report = f.engine.renotify_admitted(&f.txid, TOPIC).await.unwrap();
    assert_eq!(f.effects.get(), effects, "open renotify journal must precede the bridge");
    assert_eq!(report.notified, 0);
    assert_eq!(report.faults, ["renotify journal still open"]);
    f.store.set_admission_decision_only(false);
    f.store.drain_admission_journals(1).unwrap();
}
