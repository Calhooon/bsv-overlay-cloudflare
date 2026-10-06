//! bsv-low #530 (E1): topic history can continue behind a proven GASP node.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_trait::async_trait;
use bsv_overlay_engine::engine::{Engine, EngineConfig};
use bsv_overlay_engine::gasp::{
    AncestorFetcher, FetchedAncestor, GASPError, GASPRemote, GASPRemoteFactory, GASPStorage,
    GASPSync,
};
use bsv_overlay_engine::gasp_overlay::{
    new_finalized_graph_sink, FinalizedGraph, OverlayGASPStorage,
};
use bsv_overlay_engine::storage::memory::MemoryStorage;
use bsv_overlay_engine::storage::Storage;
use bsv_overlay_engine::topic_manager::{TopicManager, TopicManagerError};
use bsv_overlay_engine::types::*;
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{
    MerklePath, MerklePathLeaf, Transaction, TransactionInput, TransactionOutput,
};

const TOPIC: &str = "tm_head_chain";

fn chain(length: usize) -> Vec<GASPNode> {
    let mut nodes = Vec::new();
    let mut previous = None;
    for height in 0..length {
        let mut tx = Transaction::new();
        if let Some(txid) = previous {
            tx.inputs.push(TransactionInput::new(txid, 0));
        }
        tx.outputs.push(TransactionOutput::new(
            1000,
            LockingScript::from_hex("76a914000000000000000000000000000000000000000088ac").unwrap(),
        ));
        let txid = tx.id();
        let proof = MerklePath::new(
            100 + height as u32,
            vec![vec![MerklePathLeaf::new_txid(0, txid.clone())]],
        )
        .unwrap();
        nodes.push(GASPNode {
            graph_id: String::new(),
            raw_tx: tx.to_hex(),
            output_index: 0,
            proof: Some(proof.to_hex()),
            tx_metadata: None,
            output_metadata: None,
            inputs: None,
        });
        previous = Some(txid);
    }
    nodes
}

fn node_txid(node: &GASPNode) -> String {
    Transaction::from_hex(&node.raw_tx).unwrap().id()
}

type Request = (String, u32, bool);
type Requests = Rc<RefCell<Vec<Request>>>;

#[derive(Clone)]
struct RecordingRemote {
    nodes: Rc<HashMap<String, GASPNode>>,
    utxos: Vec<GASPOutput>,
    requests: Requests,
    // Txids the peer cannot answer for RIGHT NOW (a timeout, a 500): a
    // `RemoteError`, never the definite `NodeNotFound`. Shared, so a test can
    // heal the peer between two syncs.
    faults: Rc<RefCell<HashSet<String>>>,
}

impl RecordingRemote {
    fn new(nodes: &[GASPNode], tips: &[usize]) -> Self {
        Self {
            nodes: Rc::new(nodes.iter().map(|n| (node_txid(n), n.clone())).collect()),
            utxos: tips
                .iter()
                .enumerate()
                .map(|(score, &i)| GASPOutput {
                    txid: node_txid(&nodes[i]),
                    output_index: nodes[i].output_index,
                    score: (score + 1) as f64,
                })
                .collect(),
            requests: Rc::new(RefCell::new(Vec::new())),
            faults: Rc::new(RefCell::new(HashSet::new())),
        }
    }
}

impl GASPRemoteFactory for RecordingRemote {
    fn create_remote(&self, _peer_url: &str, topic: &str) -> Box<dyn GASPRemote> {
        assert_eq!(topic, TOPIC);
        Box::new(self.clone())
    }
}

#[async_trait(?Send)]
impl GASPRemote for RecordingRemote {
    async fn get_initial_response(
        &self,
        request: &GASPInitialRequest,
    ) -> Result<GASPInitialResponse, GASPError> {
        Ok(GASPInitialResponse {
            utxo_list: self
                .utxos
                .iter()
                .filter(|u| u.score as u64 >= request.since)
                .cloned()
                .collect(),
            since: request.since,
        })
    }

    async fn get_initial_reply(
        &self,
        _response: &GASPInitialResponse,
    ) -> Result<GASPInitialReply, GASPError> {
        Ok(GASPInitialReply { utxo_list: vec![] })
    }

    async fn request_node(
        &self,
        graph_id: &str,
        txid: &str,
        output_index: u32,
        metadata: bool,
    ) -> Result<GASPNode, GASPError> {
        self.requests
            .borrow_mut()
            .push((txid.to_string(), output_index, metadata));
        if self.faults.borrow().contains(txid) {
            return Err(GASPError::RemoteError(format!(
                "Peer returned HTTP 500 for {txid}"
            )));
        }
        let mut node = self
            .nodes
            .get(txid)
            .cloned()
            .ok_or_else(|| GASPError::NodeNotFound(txid.to_string()))?;
        node.graph_id = graph_id.to_string();
        node.output_index = output_index;
        Ok(node)
    }

    async fn submit_node(&self, _node: &GASPNode) -> Result<Option<GASPNodeResponse>, GASPError> {
        panic!("pull-only sync must never submit to a remote")
    }
}

#[derive(Default)]
struct HeadState {
    admitted: Vec<String>,
}

struct HeadChainManager(Rc<RefCell<HeadState>>);

#[async_trait(?Send)]
impl TopicManager for HeadChainManager {
    async fn identify_admissible_outputs(
        &self,
        tx: &Transaction,
        previous_coins: &[u8],
        _off_chain_values: Option<&[u8]>,
        mode: SubmitMode,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        if mode == SubmitMode::HistoricalTx {
            assert!(previous_coins.is_empty(), "GASP admission is a dry run");
            assert!(tx.merkle_path.is_some(), "the dry run carries the proof");
        }
        // The engine encodes the reference's previousCoins: number[] as
        // little-endian u32 input indices. Finalize must supply the first input.
        let extends_head = mode == SubmitMode::HistoricalTxNoSpv
            && previous_coins
                .chunks_exact(4)
                .any(|index| u32::from_le_bytes(index.try_into().unwrap()) == 0);
        if !tx.inputs.is_empty() && !extends_head {
            return Ok(AdmittanceInstructions::default());
        }
        if mode == SubmitMode::HistoricalTxNoSpv {
            self.0.borrow_mut().admitted.push(tx.id());
        }
        Ok(AdmittanceInstructions {
            outputs_to_admit: vec![0],
            ..Default::default()
        })
    }

    async fn identify_needed_inputs(
        &self,
        beef: &[u8],
        off_chain_values: Option<&[u8]>,
    ) -> Result<Vec<Outpoint>, TopicManagerError> {
        assert!(off_chain_values.is_none(), "the reference passes only BEEF");
        let tx = Transaction::from_beef(beef, None).unwrap();
        assert!(tx.merkle_path.is_some());
        Ok(tx
            .inputs
            .first()
            .map(|i| Outpoint::new(i.get_source_txid().unwrap(), i.source_output_index))
            .into_iter()
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
async fn a_engine_bootstraps_five_proven_head_transactions_oldest_first() {
    let started = Instant::now();
    let nodes = chain(5);
    let remote = RecordingRemote::new(&nodes, &[4]);
    let requests = remote.requests.clone();
    let state = Rc::new(RefCell::new(HeadState::default()));
    let store = Rc::new(MemoryStorage::new());
    let mut engine = Engine::new(
        HashMap::from([(
            TOPIC.to_string(),
            Box::new(HeadChainManager(state.clone())) as Box<dyn TopicManager>,
        )]),
        HashMap::new(),
        Box::new(store.clone()),
        None,
        EngineConfig {
            sync_configuration: HashMap::from([(
                TOPIC.to_string(),
                SyncTarget::Peers(vec!["mock://head-chain".to_string()]),
            )]),
            ..Default::default()
        },
    );
    engine.set_gasp_remote_factory(Box::new(remote));
    engine.start_gasp_sync().await.unwrap();

    let expected: Vec<_> = nodes.iter().map(node_txid).collect();
    let expected_requests: Vec<_> = expected
        .iter()
        .rev()
        .enumerate()
        .map(|(i, id)| (id.clone(), 0, i == 0))
        .collect();
    assert_eq!(*requests.borrow(), expected_requests, "walk tip to genesis");
    assert_eq!(state.borrow().admitted, expected, "admit oldest first");
    let utxos = store
        .find_utxos_for_topic(TOPIC, None, None, false)
        .await
        .unwrap();
    assert_eq!(utxos.len(), 1);
    assert_eq!(utxos[0].txid, expected[4]);
    println!(
        "PIN A engine: 5 transactions, 5 ordered requests, oldest-first admission; elapsed {:?}",
        started.elapsed()
    );
}

async fn synchronize(
    nodes: &[GASPNode],
    tips: &[usize],
    manager: Option<&dyn TopicManager>,
    store: &dyn Storage,
    fetcher: Option<Rc<dyn AncestorFetcher>>,
) -> (Vec<Request>, Vec<FinalizedGraph>, u64) {
    let (requests, graphs, cursor, _) =
        synchronize_counting(nodes, tips, manager, store, fetcher).await;
    (requests, graphs, cursor)
}

// The same sync, also returning `GASPSync::pruned_inputs`.
async fn synchronize_counting(
    nodes: &[GASPNode],
    tips: &[usize],
    manager: Option<&dyn TopicManager>,
    store: &dyn Storage,
    fetcher: Option<Rc<dyn AncestorFetcher>>,
) -> (Vec<Request>, Vec<FinalizedGraph>, u64, u64) {
    synchronize_remote(RecordingRemote::new(nodes, tips), manager, store, fetcher).await
}

// The same sync against a remote the test built itself.
async fn synchronize_remote(
    remote: RecordingRemote,
    manager: Option<&dyn TopicManager>,
    store: &dyn Storage,
    fetcher: Option<Rc<dyn AncestorFetcher>>,
) -> (Vec<Request>, Vec<FinalizedGraph>, u64, u64) {
    let requests = remote.requests.clone();
    let sink = new_finalized_graph_sink();
    let mut adapter =
        OverlayGASPStorage::new(store, TOPIC, sink.clone()).with_strict_beef(fetcher.is_some());
    if let Some(manager) = manager {
        adapter = adapter.with_topic_manager(manager);
    }
    let mut sync = GASPSync::new(Box::new(adapter), Box::new(remote), 0, "[E1]", true)
        .with_ancestor_fetcher(fetcher);
    sync.sync(None).await.unwrap();
    let graphs = sink.lock().unwrap().clone();
    let requests = requests.borrow().clone();
    (
        requests,
        graphs,
        sync.last_interaction,
        sync.pruned_inputs(),
    )
}

fn graph_txids(graph: &FinalizedGraph) -> Vec<String> {
    graph
        .beefs
        .iter()
        .map(|beef| Transaction::from_beef(beef, None).unwrap().id())
        .collect()
}

#[tokio::test]
async fn a_sync_finalizes_one_graph_with_all_five_transactions() {
    let nodes = chain(5);
    let manager = HeadChainManager(Rc::new(RefCell::new(HeadState::default())));
    let (requests, graphs, _) =
        synchronize(&nodes, &[4], Some(&manager), &MemoryStorage::new(), None).await;
    assert_eq!(requests.len(), 5);
    assert_eq!(graphs.len(), 1);
    assert_eq!(graphs[0].topic, TOPIC);
    assert_eq!(
        graph_txids(&graphs[0]),
        nodes.iter().map(node_txid).collect::<Vec<_>>()
    );
    assert!(
        manager.0.borrow().admitted.is_empty(),
        "dry runs do not record admissions"
    );
}

// Uses the trait's own default identify_needed_inputs, including rejection.
struct DefaultInputsManager;

#[async_trait(?Send)]
impl TopicManager for DefaultInputsManager {
    async fn identify_admissible_outputs(
        &self,
        _tx: &Transaction,
        _previous_coins: &[u8],
        _off_chain_values: Option<&[u8]>,
        _mode: SubmitMode,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        Ok(AdmittanceInstructions::default())
    }
    async fn get_documentation(&self) -> String {
        String::new()
    }
    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata::default()
    }
}

// Every manager of the workspace that names no inputs, plus the trait default.
fn workspace_managers() -> Vec<(&'static str, Box<dyn TopicManager>)> {
    use bsv_overlay_discovery::{
        agent::topic_manager::AgentTopicManager,
        collected::topic_manager::CollectedTopicManager,
        dm_delegation::topic_manager::DmDelegationTopicManager,
        hand::topic_manager::HandTopicManager,
        hopparty::topic_manager::HoppartyTopicManager,
        low::topic_manager::LowTopicManager,
        pot::{lowfund_topic_manager::LowFundTopicManager, topic_manager::PotTopicManager},
        potparty::topic_manager::PotpartyTopicManager,
        potrefund::topic_manager::PotrefundTopicManager,
        proof::topic_manager::ProofTopicManager,
        result::topic_manager::ResultTopicManager,
        reveal::topic_manager::RevealTopicManager,
        ship::topic_manager::SHIPTopicManager,
        slap::topic_manager::SLAPTopicManager,
        uhrp::topic_manager::UHRPTopicManager,
    };
    vec![
        ("default", Box::new(DefaultInputsManager)),
        ("SHIP", Box::new(SHIPTopicManager::new())),
        ("SLAP", Box::new(SLAPTopicManager::new())),
        ("UHRP", Box::new(UHRPTopicManager::new())),
        ("Agent", Box::new(AgentTopicManager::new())),
        ("DmDelegation", Box::new(DmDelegationTopicManager::new())),
        ("LOW", Box::new(LowTopicManager::new())),
        ("LowFund", Box::new(LowFundTopicManager::new())),
        ("Reveal", Box::new(RevealTopicManager::new())),
        ("Pot", Box::new(PotTopicManager::new())),
        ("Collected", Box::new(CollectedTopicManager::new())),
        ("Hand", Box::new(HandTopicManager::new())),
        ("Result", Box::new(ResultTopicManager::new())),
        ("Proof", Box::new(ProofTopicManager::new())),
        ("Potparty", Box::new(PotpartyTopicManager::new())),
        ("Potrefund", Box::new(PotrefundTopicManager::new())),
        ("Hopparty", Box::new(HoppartyTopicManager::new())),
    ]
}

#[tokio::test]
async fn b_empty_input_managers_preserve_requests_and_finalized_beef_bytes() {
    let managers = workspace_managers();
    // A proven tip and an unproven two-hop tip ending at a proven ancestor.
    for unproven_tip in [false, true] {
        let mut nodes = chain(3);
        if unproven_tip {
            nodes[1].proof = None;
            nodes[2].proof = None;
        }
        let store = MemoryStorage::new();
        let (baseline_requests, baseline_graphs, baseline_cursor) =
            synchronize(&nodes, &[2], None, &store, None).await;
        assert_eq!(baseline_requests.len(), if unproven_tip { 3 } else { 1 });
        assert_eq!(baseline_graphs.len(), 1);
        for (name, manager) in &managers {
            let (requests, graphs, cursor) =
                synchronize(&nodes, &[2], Some(manager.as_ref()), &store, None).await;
            assert_eq!(
                requests, baseline_requests,
                "{name}: requested outpoints and metadata"
            );
            assert_eq!(cursor, baseline_cursor, "{name}: cursor");
            assert_eq!(graphs.len(), baseline_graphs.len(), "{name}: graph count");
            assert_eq!(
                graphs[0].beefs, baseline_graphs[0].beefs,
                "{name}: BEEF bytes"
            );
        }
    }
    println!("PIN B: 17 managers x 2 sync shapes = 34 byte-identical comparisons");
}

#[derive(Debug)]
struct AdmissionCall {
    txid: String,
    previous_coins: Vec<u8>,
    off_chain_values: Option<Vec<u8>>,
    mode: SubmitMode,
    proof: Option<String>,
}

#[derive(Default)]
struct ProbeManager {
    outputs: Vec<u32>,
    named: Vec<Outpoint>,
    named_by_txid: HashMap<String, Vec<Outpoint>>,
    admission_error_txid: Option<String>,
    needed_error_txid: Option<String>,
    admissions: RefCell<Vec<AdmissionCall>>,
    needed: RefCell<Vec<Vec<u8>>>,
}

#[async_trait(?Send)]
impl TopicManager for ProbeManager {
    async fn identify_admissible_outputs(
        &self,
        tx: &Transaction,
        previous_coins: &[u8],
        off_chain_values: Option<&[u8]>,
        mode: SubmitMode,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        self.admissions.borrow_mut().push(AdmissionCall {
            txid: tx.id(),
            previous_coins: previous_coins.to_vec(),
            off_chain_values: off_chain_values.map(<[u8]>::to_vec),
            mode,
            proof: tx.merkle_path.as_ref().map(MerklePath::to_hex),
        });
        if self.admission_error_txid.as_deref() == Some(tx.id().as_str()) {
            return Err(TopicManagerError::Other("admission failed".to_string()));
        }
        Ok(AdmittanceInstructions {
            outputs_to_admit: self.outputs.clone(),
            ..Default::default()
        })
    }

    async fn identify_needed_inputs(
        &self,
        beef: &[u8],
        off_chain_values: Option<&[u8]>,
    ) -> Result<Vec<Outpoint>, TopicManagerError> {
        assert!(off_chain_values.is_none());
        self.needed.borrow_mut().push(beef.to_vec());
        let tx = Transaction::from_beef(beef, None).unwrap();
        if self.needed_error_txid.as_deref() == Some(tx.id().as_str()) {
            return Err(TopicManagerError::Other("needed inputs failed".to_string()));
        }
        Ok(self
            .named_by_txid
            .get(&tx.id())
            .unwrap_or(&self.named)
            .clone())
    }

    async fn get_documentation(&self) -> String {
        String::new()
    }
    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata::default()
    }
}

// Capture the actual tracing event without adding a logging dependency.
struct CaptureLogs(Arc<Mutex<Vec<String>>>);

impl tracing::Subscriber for CaptureLogs {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        struct Visitor(String);
        impl tracing::field::Visit for Visitor {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                use std::fmt::Write;
                write!(self.0, "{}={value:?} ", field.name()).unwrap();
            }
        }
        let mut visitor = Visitor(String::new());
        event.record(&mut visitor);
        self.0.lock().unwrap().push(visitor.0);
    }
    fn enter(&self, _span: &tracing::span::Id) {}
    fn exit(&self, _span: &tracing::span::Id) {}
}

#[tokio::test]
async fn c_needed_input_error_logs_outpoint_and_only_cuts_off_that_node() {
    let nodes = chain(3);
    let manager = ProbeManager {
        needed_error_txid: Some(node_txid(&nodes[1])),
        named_by_txid: HashMap::from([(
            node_txid(&nodes[2]),
            vec![Outpoint::new(node_txid(&nodes[1]), 0)],
        )]),
        ..Default::default()
    };
    let logs = Arc::new(Mutex::new(Vec::new()));
    let _guard = tracing::subscriber::set_default(CaptureLogs(logs.clone()));
    let (requests, graphs, cursor) =
        synchronize(&nodes, &[2, 0], Some(&manager), &MemoryStorage::new(), None).await;
    assert_eq!(
        requests,
        vec![
            (node_txid(&nodes[2]), 0, true),
            (node_txid(&nodes[1]), 0, false),
            (node_txid(&nodes[0]), 0, true)
        ]
    );
    assert_eq!(
        graphs.len(),
        2,
        "both the cutoff and independent graph finalize"
    );
    assert_eq!(
        graph_txids(&graphs[0]),
        vec![node_txid(&nodes[1]), node_txid(&nodes[2])]
    );
    assert_eq!(graph_txids(&graphs[1]), vec![node_txid(&nodes[0])]);
    assert_eq!(cursor, 2);
    assert_eq!(manager.needed.borrow().len(), 3);
    let outpoint = format!("{}.0", node_txid(&nodes[1]));
    assert!(
        logs.lock()
            .unwrap()
            .iter()
            .any(|log| { log.contains(&outpoint) && log.contains("needed inputs failed") }),
        "the error log must identify the node and the failure"
    );
}

#[tokio::test]
async fn d_only_named_inputs_are_requested_and_already_known_ones_are_stripped() {
    let mut nodes = chain(3);
    let known = Outpoint::new(node_txid(&nodes[0]), 0);
    let unknown = Outpoint::new(node_txid(&nodes[1]), 0);
    let mut tx = Transaction::from_hex(&nodes[2].raw_tx).unwrap();
    tx.add_input(TransactionInput::new(known.txid.clone(), 0))
        .unwrap();
    tx.add_input(TransactionInput::new("11".repeat(32), 2))
        .unwrap();
    nodes[2].raw_tx = tx.to_hex();
    nodes[2].proof = Some(
        MerklePath::new(200, vec![vec![MerklePathLeaf::new_txid(0, tx.id())]])
            .unwrap()
            .to_hex(),
    );
    // The manager names two history inputs, excluding the unrelated fee input.
    let manager = ProbeManager {
        named: vec![known.clone(), unknown.clone()],
        ..Default::default()
    };
    let store = MemoryStorage::new();
    store
        .insert_output(&Output {
            txid: known.txid,
            output_index: known.output_index,
            output_script: vec![],
            satoshis: 1000,
            topic: TOPIC.to_string(),
            spent: true,
            outputs_consumed: vec![],
            consumed_by: vec![],
            beef: None,
            block_height: None,
            score: None,
        })
        .await
        .unwrap();
    let adapter = OverlayGASPStorage::new(&store, TOPIC, new_finalized_graph_sink())
        .with_topic_manager(&manager);
    let response = adapter
        .find_needed_inputs(&nodes[2])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.requested_inputs.len(), 1);
    assert!(!response.requested_inputs[&unknown.to_graph_id()].metadata);
    let passed = Transaction::from_beef(&manager.needed.borrow()[0], None).unwrap();
    assert_eq!(passed.id(), node_txid(&nodes[2]));
    assert_eq!(
        passed.merkle_path.unwrap().to_hex(),
        nodes[2].proof.clone().unwrap()
    );

    // When every named outpoint is known, the existing strip ends the walk.
    let known_only = ProbeManager {
        named: vec![Outpoint::new(node_txid(&nodes[0]), 0)],
        ..Default::default()
    };
    let adapter = OverlayGASPStorage::new(&store, TOPIC, new_finalized_graph_sink())
        .with_topic_manager(&known_only);
    assert!(adapter
        .find_needed_inputs(&nodes[2])
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn e_admitted_output_stops_without_asking_for_inputs_and_uses_reference_context() {
    for metadata in [None, Some("café 00ff".to_string())] {
        let mut nodes = chain(2);
        nodes[1].tx_metadata = metadata.clone();
        let manager = ProbeManager {
            outputs: vec![0],
            needed_error_txid: Some(node_txid(&nodes[1])),
            ..Default::default()
        };
        let store = MemoryStorage::new();
        let adapter = OverlayGASPStorage::new(&store, TOPIC, new_finalized_graph_sink())
            .with_topic_manager(&manager);
        assert!(adapter
            .find_needed_inputs(&nodes[1])
            .await
            .unwrap()
            .is_none());
        assert!(manager.needed.borrow().is_empty());
        let calls = manager.admissions.borrow();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].txid, node_txid(&nodes[1]));
        assert!(calls[0].previous_coins.is_empty());
        assert_eq!(calls[0].off_chain_values, metadata.map(String::into_bytes));
        assert_eq!(calls[0].mode, SubmitMode::HistoricalTx);
        assert_eq!(calls[0].proof, nodes[1].proof);
    }
}

#[tokio::test]
async fn e_admitting_another_output_does_not_stop_the_named_input_walk() {
    let mut nodes = chain(2);
    let mut tx = Transaction::from_hex(&nodes[1].raw_tx).unwrap();
    tx.add_output(TransactionOutput::new(
        1,
        LockingScript::from_hex("51").unwrap(),
    ))
    .unwrap();
    nodes[1].raw_tx = tx.to_hex();
    nodes[1].output_index = 1;
    nodes[1].proof = Some(
        MerklePath::new(200, vec![vec![MerklePathLeaf::new_txid(0, tx.id())]])
            .unwrap()
            .to_hex(),
    );
    let named = Outpoint::new(node_txid(&nodes[0]), 0);
    let manager = ProbeManager {
        outputs: vec![0],
        named: vec![named.clone()],
        ..Default::default()
    };
    let store = MemoryStorage::new();
    let adapter = OverlayGASPStorage::new(&store, TOPIC, new_finalized_graph_sink())
        .with_topic_manager(&manager);
    let response = adapter
        .find_needed_inputs(&nodes[1])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.requested_inputs.len(), 1);
    assert!(!response.requested_inputs[&named.to_graph_id()].metadata);
    assert_eq!(manager.needed.borrow().len(), 1);
}

#[tokio::test]
async fn admission_error_propagates_from_storage_and_skips_only_the_failed_graph() {
    let nodes = chain(3);
    let manager = ProbeManager {
        admission_error_txid: Some(node_txid(&nodes[2])),
        ..Default::default()
    };
    let store = MemoryStorage::new();
    let adapter = OverlayGASPStorage::new(&store, TOPIC, new_finalized_graph_sink())
        .with_topic_manager(&manager);
    let error = adapter.find_needed_inputs(&nodes[2]).await.unwrap_err();
    assert!(error.to_string().contains("admission failed"));
    assert!(manager.needed.borrow().is_empty());

    // @bsv/gasp catches this at the incoming UTXO boundary, logs, and skips
    // completion. It does not call discardGraph. Rust also retains its cursor
    // gap guard so this failed graph can be retried on the next sync.
    let logs = Arc::new(Mutex::new(Vec::new()));
    let _guard = tracing::subscriber::set_default(CaptureLogs(logs.clone()));
    let (requests, graphs, cursor) =
        synchronize(&nodes, &[2, 0], Some(&manager), &store, None).await;
    assert_eq!(
        requests.len(),
        2,
        "only the two advertised roots are requested"
    );
    assert!(requests.iter().all(|r| r.0 != node_txid(&nodes[1])));
    assert_eq!(graphs.len(), 1);
    assert_eq!(graph_txids(&graphs[0]), vec![node_txid(&nodes[0])]);
    assert_eq!(
        cursor, 0,
        "existing Rust gap guard keeps the failed tip retryable"
    );
    assert!(logs
        .lock()
        .unwrap()
        .iter()
        .any(|log| log.contains("admission failed")));
}

#[tokio::test]
async fn unproven_node_requests_all_inputs_without_consulting_manager() {
    let mut nodes = chain(3);
    let mut tx = Transaction::from_hex(&nodes[2].raw_tx).unwrap();
    tx.add_input(TransactionInput::new(node_txid(&nodes[0]), 3))
        .unwrap();
    nodes[2].raw_tx = tx.to_hex();
    nodes[2].proof = None;
    let manager = ProbeManager::default();
    let store = MemoryStorage::new();
    let adapter = OverlayGASPStorage::new(&store, TOPIC, new_finalized_graph_sink())
        .with_topic_manager(&manager);
    let response = adapter
        .find_needed_inputs(&nodes[2])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.requested_inputs.len(), 2);
    assert!(!response.requested_inputs[&format!("{}.0", node_txid(&nodes[1]))].metadata);
    assert!(!response.requested_inputs[&format!("{}.3", node_txid(&nodes[0]))].metadata);
    assert!(manager.admissions.borrow().is_empty());
    assert!(manager.needed.borrow().is_empty());
}

struct RecordingFetcher {
    nodes: HashMap<String, GASPNode>,
    requested: RefCell<Vec<String>>,
}

#[async_trait(?Send)]
impl AncestorFetcher for RecordingFetcher {
    async fn fetch_ancestor(&self, txid: &str) -> Result<FetchedAncestor, GASPError> {
        self.requested.borrow_mut().push(txid.to_string());
        let node = &self.nodes[txid];
        Ok(FetchedAncestor {
            raw_tx: node.raw_tx.clone(),
            proof: node.proof.clone(),
        })
    }
}

#[tokio::test]
async fn ancestor_fetcher_honors_named_history_and_empty_managers_still_stop() {
    for needs_history in [true, false] {
        let mut nodes = chain(5);
        let fetcher = Rc::new(RecordingFetcher {
            nodes: nodes.iter().map(|n| (node_txid(n), n.clone())).collect(),
            requested: RefCell::new(Vec::new()),
        });
        // Exercise #126 root proof hydration before the topic manager runs.
        nodes[4].proof = None;
        let manager: Box<dyn TopicManager> = if needs_history {
            Box::new(HeadChainManager(Rc::new(
                RefCell::new(HeadState::default()),
            )))
        } else {
            Box::new(DefaultInputsManager)
        };
        let (requests, graphs, _) = synchronize(
            &nodes,
            &[4],
            Some(manager.as_ref()),
            &MemoryStorage::new(),
            Some(fetcher.clone()),
        )
        .await;
        assert_eq!(
            requests,
            vec![(node_txid(&nodes[4]), 0, true)],
            "peer serves only root"
        );
        let expected: Vec<_> = nodes.iter().map(node_txid).collect();
        assert_eq!(graphs.len(), 1);
        if needs_history {
            assert_eq!(
                *fetcher.requested.borrow(),
                expected.iter().rev().cloned().collect::<Vec<_>>()
            );
            assert_eq!(graph_txids(&graphs[0]), expected);
        } else {
            assert_eq!(*fetcher.requested.borrow(), vec![expected[4].clone()]);
            assert_eq!(graph_txids(&graphs[0]), vec![expected[4].clone()]);
        }
    }
}

// ============================================================================
// bsv-low #530 (D8 engine half): the decoy rule (zanaadu-v2 #314).
//
// A head covenant that signs under ANYONECANPAY lets a spender place a decoy
// witness-shaped input ahead of the real head input. The manager cannot tell
// them apart from the proven node alone, so it names EVERY witness-shaped
// input, and the engine prunes the branch of one the PEER answers it does not
// hold (`GASPError::NodeNotFound`). Any other error, and every error of a
// chain fetcher, is a fault of the moment: the UTXO fails and is retried.
// ============================================================================

const WITNESS_CAP: usize = 4;

// The fixture's whole notion of "witness-shaped": exactly two data pushes.
fn witness_shaped(input: &TransactionInput) -> bool {
    input.unlocking_script.as_ref().is_some_and(|script| {
        let chunks = script.chunks();
        chunks.len() == 2 && chunks.iter().all(|chunk| chunk.data.is_some())
    })
}

fn witness_input(txid: String, output_index: u32) -> TransactionInput {
    let mut input = TransactionInput::new(txid, output_index);
    input.set_unlocking_script(UnlockingScript::from_hex("01aa01bb").unwrap());
    input
}

// An outpoint no peer and no fetcher of these tests holds: the mock peer
// answers the definite `NodeNotFound` for it.
fn decoy_outpoint(tag: u8) -> Outpoint {
    Outpoint::new(format!("{tag:02x}").repeat(32), 7)
}

// A proven head chain. Every spend carries its real head input (witness-shaped)
// and a plain fee input (not witness-shaped, never named). A height listed in
// `decoys` carries that decoy witness-shaped input at index 0, AHEAD of the
// real head input at index 1.
fn decoy_chain(length: usize, decoys: &[(usize, Outpoint)]) -> Vec<GASPNode> {
    decoy_chain_with_tip_outputs(length, decoys, 1)
}

// The same chain whose TIP has `tip_outputs` outputs (each its own UTXO).
fn decoy_chain_with_tip_outputs(
    length: usize,
    decoys: &[(usize, Outpoint)],
    tip_outputs: usize,
) -> Vec<GASPNode> {
    let mut nodes = Vec::new();
    let mut previous: Option<String> = None;
    for height in 0..length {
        let mut tx = Transaction::new();
        if let Some(txid) = previous {
            for (_, decoy) in decoys.iter().filter(|(at, _)| *at == height) {
                tx.inputs
                    .push(witness_input(decoy.txid.clone(), decoy.output_index));
            }
            tx.inputs.push(witness_input(txid, 0));
            tx.inputs
                .push(TransactionInput::new("fe".repeat(32), height as u32));
        }
        let outputs = if height + 1 == length { tip_outputs } else { 1 };
        for _ in 0..outputs {
            tx.outputs.push(TransactionOutput::new(
                1000,
                LockingScript::from_hex("76a914000000000000000000000000000000000000000088ac")
                    .unwrap(),
            ));
        }
        let txid = tx.id();
        let proof = MerklePath::new(
            100 + height as u32,
            vec![vec![MerklePathLeaf::new_txid(0, txid.clone())]],
        )
        .unwrap();
        nodes.push(GASPNode {
            graph_id: String::new(),
            raw_tx: tx.to_hex(),
            output_index: 0,
            proof: Some(proof.to_hex()),
            tx_metadata: None,
            output_metadata: None,
            inputs: None,
        });
        previous = Some(txid);
    }
    nodes
}

// Zanaadu's side of the D8 pairing, in miniature: name EVERY witness-shaped
// input, capped. Admission extends the head when ANY previous coin (an input
// index the engine found in its own storage) is witness-shaped: the real head
// is input 1 behind a decoy, so "input 0" would be the wrong question.
struct DecoyHeadManager(Rc<RefCell<HeadState>>);

impl DecoyHeadManager {
    fn new() -> Self {
        Self(Rc::new(RefCell::new(HeadState::default())))
    }
}

#[async_trait(?Send)]
impl TopicManager for DecoyHeadManager {
    async fn identify_admissible_outputs(
        &self,
        tx: &Transaction,
        previous_coins: &[u8],
        _off_chain_values: Option<&[u8]>,
        mode: SubmitMode,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        let extends_head = mode == SubmitMode::HistoricalTxNoSpv
            && previous_coins.chunks_exact(4).any(|index| {
                let index = u32::from_le_bytes(index.try_into().unwrap()) as usize;
                tx.inputs.get(index).is_some_and(witness_shaped)
            });
        if !tx.inputs.is_empty() && !extends_head {
            return Ok(AdmittanceInstructions::default());
        }
        if mode == SubmitMode::HistoricalTxNoSpv {
            self.0.borrow_mut().admitted.push(tx.id());
        }
        Ok(AdmittanceInstructions {
            outputs_to_admit: vec![0],
            ..Default::default()
        })
    }

    async fn identify_needed_inputs(
        &self,
        beef: &[u8],
        off_chain_values: Option<&[u8]>,
    ) -> Result<Vec<Outpoint>, TopicManagerError> {
        assert!(off_chain_values.is_none(), "the reference passes only BEEF");
        let tx = Transaction::from_beef(beef, None).unwrap();
        assert!(tx.merkle_path.is_some());
        Ok(tx
            .inputs
            .iter()
            .filter(|input| witness_shaped(input))
            .take(WITNESS_CAP)
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

// The requests for `outpoint`, and the others in the order they were made.
// A node's requested inputs are a map, so where a decoy falls among its
// parent's requests is not an order worth pinning.
fn split_requests(requests: &[Request], outpoint: &Outpoint) -> (usize, Vec<Request>) {
    let (decoy, chain): (Vec<_>, Vec<_>) = requests
        .iter()
        .cloned()
        .partition(|r| r.0 == outpoint.txid && r.1 == outpoint.output_index);
    (decoy.len(), chain)
}

// tracing caches a callsite's interest from the thread that reaches it first,
// so EVERY test that reaches the prune warn captures logs, not only the tests
// that read them: a test without a subscriber could otherwise switch the warn
// off for the tests that assert on it.
fn capture_logs() -> (Arc<Mutex<Vec<String>>>, tracing::subscriber::DefaultGuard) {
    let logs = Arc::new(Mutex::new(Vec::new()));
    let guard = tracing::subscriber::set_default(CaptureLogs(logs.clone()));
    (logs, guard)
}

fn tip_to_genesis(nodes: &[GASPNode]) -> Vec<Request> {
    nodes
        .iter()
        .rev()
        .enumerate()
        .map(|(i, node)| (node_txid(node), 0, i == 0))
        .collect()
}

#[tokio::test]
async fn d8_a_engine_walks_a_decoy_tip_to_genesis_with_one_failed_round_trip() {
    let (_logs, _guard) = capture_logs();
    let decoy = decoy_outpoint(0xd0);
    let nodes = decoy_chain(5, &[(4, decoy.clone())]);
    let tip = Transaction::from_hex(&nodes[4].raw_tx).unwrap();
    assert!(witness_shaped(&tip.inputs[0]) && witness_shaped(&tip.inputs[1]));
    assert_eq!(tip.inputs[0].get_source_txid().unwrap(), decoy.txid);
    assert_eq!(
        tip.inputs[1].get_source_txid().unwrap(),
        node_txid(&nodes[3])
    );

    let remote = RecordingRemote::new(&nodes, &[4]);
    let requests = remote.requests.clone();
    let manager = DecoyHeadManager::new();
    let state = manager.0.clone();
    let store = Rc::new(MemoryStorage::new());
    let mut engine = Engine::new(
        HashMap::from([(
            TOPIC.to_string(),
            Box::new(manager) as Box<dyn TopicManager>,
        )]),
        HashMap::new(),
        Box::new(store.clone()),
        None,
        EngineConfig {
            sync_configuration: HashMap::from([(
                TOPIC.to_string(),
                SyncTarget::Peers(vec!["mock://head-chain".to_string()]),
            )]),
            ..Default::default()
        },
    );
    engine.set_gasp_remote_factory(Box::new(remote));
    let result = engine.start_gasp_sync().await.unwrap();

    let (decoy_requests, chain_requests) = split_requests(&requests.borrow(), &decoy);
    assert_eq!(decoy_requests, 1, "one failed round trip for the decoy");
    assert_eq!(
        chain_requests,
        tip_to_genesis(&nodes),
        "walk tip to genesis"
    );
    let expected: Vec<_> = nodes.iter().map(node_txid).collect();
    assert_eq!(state.borrow().admitted, expected, "admit oldest first");
    let utxos = store
        .find_utxos_for_topic(TOPIC, None, None, false)
        .await
        .unwrap();
    assert_eq!(utxos.len(), 1);
    assert_eq!(utxos[0].txid, expected[4]);
    let topic = &result.topics_synced[TOPIC];
    assert_eq!(topic.pruned_inputs, 1, "the prune is counted");
    assert!(topic.errors.is_empty(), "a pruned branch is not an error");
    assert_eq!(
        store
            .get_last_interaction("mock://head-chain", TOPIC)
            .await
            .unwrap(),
        1,
        "a pruned branch is not a failed UTXO: the cursor advances"
    );
    println!(
        "D8 PIN A engine: 5 transactions, {} requests, 1 failed round trip, pruned_inputs=1",
        requests.borrow().len()
    );
}

#[tokio::test]
async fn d8_a_sync_finalizes_one_graph_of_five_and_logs_the_prune() {
    let decoy = decoy_outpoint(0xd0);
    let nodes = decoy_chain(5, &[(4, decoy.clone())]);
    let manager = DecoyHeadManager::new();
    let (logs, _guard) = capture_logs();
    let (requests, graphs, cursor, pruned) =
        synchronize_counting(&nodes, &[4], Some(&manager), &MemoryStorage::new(), None).await;
    let (decoy_requests, chain_requests) = split_requests(&requests, &decoy);
    assert_eq!(decoy_requests, 1);
    assert_eq!(chain_requests, tip_to_genesis(&nodes));
    assert_eq!(graphs.len(), 1, "ONE finalized graph");
    assert_eq!(
        graph_txids(&graphs[0]),
        nodes.iter().map(node_txid).collect::<Vec<_>>()
    );
    assert_eq!(cursor, 1);
    assert_eq!(pruned, 1);
    let parent = format!("{}.0", node_txid(&nodes[4]));
    let requested = decoy.to_graph_id();
    assert!(
        logs.lock().unwrap().iter().any(|log| log.contains(&parent)
            && log.contains(&requested)
            && log.contains("node not found")),
        "the warn names the parent, the requested outpoint and the source's error"
    );
}

#[tokio::test]
async fn d8_b_mid_chain_decoy_prunes_and_the_walk_continues_below_it() {
    let (_logs, _guard) = capture_logs();
    let decoy = decoy_outpoint(0xd1);
    let nodes = decoy_chain(5, &[(2, decoy.clone())]);
    let manager = DecoyHeadManager::new();
    let (requests, graphs, cursor, pruned) =
        synchronize_counting(&nodes, &[4], Some(&manager), &MemoryStorage::new(), None).await;
    let (decoy_requests, chain_requests) = split_requests(&requests, &decoy);
    assert_eq!(decoy_requests, 1);
    assert_eq!(
        chain_requests,
        tip_to_genesis(&nodes),
        "nodes 1 and 0 below it"
    );
    assert_eq!(graphs.len(), 1);
    assert_eq!(
        graph_txids(&graphs[0]),
        nodes.iter().map(node_txid).collect::<Vec<_>>()
    );
    assert_eq!((cursor, pruned), (1, 1));
}

#[tokio::test]
async fn d8_c_unproven_parent_with_a_missing_spv_input_still_fails_that_utxo() {
    // The same decoy shape, but the tip is UNPROVEN: its inputs are SPV
    // necessities, not manager-named history, so the reference's rule holds.
    let mut nodes = decoy_chain(3, &[(2, decoy_outpoint(0xd2))]);
    nodes[2].proof = None;
    let manager = DecoyHeadManager::new();
    let (logs, _guard) = capture_logs();
    // Tip 2 at score 1 fails; the independent genesis at score 2 still syncs.
    let (requests, graphs, cursor, pruned) =
        synchronize_counting(&nodes, &[2, 0], Some(&manager), &MemoryStorage::new(), None).await;
    // The unproven tip needs ALL its inputs (decoy, head, fee) and the peer
    // holds neither the decoy nor the fee outpoint. Whichever of the two is
    // asked for first fails the UTXO there: one failed round trip, no more.
    let held: Vec<_> = nodes.iter().map(node_txid).collect();
    let unserved = requests.iter().filter(|r| !held.contains(&r.0)).count();
    assert_eq!(unserved, 1, "the first missing SPV input ends the walk");
    assert_eq!(graphs.len(), 1, "the failed tip finalizes nothing");
    assert_eq!(graph_txids(&graphs[0]), vec![node_txid(&nodes[0])]);
    assert_eq!(cursor, 0, "the gap guard keeps the failed tip retryable");
    assert_eq!(pruned, 0, "an unproven parent never prunes");
    assert!(logs
        .lock()
        .unwrap()
        .iter()
        .any(|log| log.contains("Error ingesting UTXO") && log.contains("node not found")));
}

// SHA-256 over everything pin B compares, for every manager and both shapes.
// Captured on the base (4965995, before the prune existed) and frozen here:
// managers that name nothing sync byte-for-byte as they did.
const D8_D_BASE_DIGEST: &str = "2b24dda6f8dfe6aee74e1211c6128680f3d6657376fbbe3c9a444bd156a76ba9";

#[tokio::test]
async fn d8_d_managers_naming_nothing_sync_byte_for_byte_as_on_the_base() {
    let mut transcript = Vec::new();
    let mut comparisons = 0;
    for unproven_tip in [false, true] {
        let mut nodes = chain(3);
        if unproven_tip {
            nodes[1].proof = None;
            nodes[2].proof = None;
        }
        let store = MemoryStorage::new();
        for (name, manager) in &workspace_managers() {
            let (requests, graphs, cursor, pruned) =
                synchronize_counting(&nodes, &[2], Some(manager.as_ref()), &store, None).await;
            assert_eq!(pruned, 0, "{name}: nothing named, nothing pruned");
            transcript.extend_from_slice(
                format!(
                    "{name}|{unproven_tip}|{requests:?}|{cursor}|{}|",
                    graphs.len()
                )
                .as_bytes(),
            );
            for graph in &graphs {
                for beef in &graph.beefs {
                    transcript.extend_from_slice(&(beef.len() as u64).to_le_bytes());
                    transcript.extend_from_slice(beef);
                }
            }
            comparisons += 1;
        }
    }
    let digest = hex::encode(bsv_rs::primitives::hash::sha256(&transcript));
    println!("D8 PIN D: {comparisons} syncs, transcript sha256 {digest}");
    assert_eq!(digest, D8_D_BASE_DIGEST);
}

// Serves the chain's own transactions and fails for every other txid, with
// the class the worker's chain fetcher gives "all providers failed"
// (`NodeNotFound`): on the fetcher arm even that class must not prune.
struct ChainOnlyFetcher {
    nodes: HashMap<String, GASPNode>,
    requested: RefCell<Vec<String>>,
}

#[async_trait(?Send)]
impl AncestorFetcher for ChainOnlyFetcher {
    async fn fetch_ancestor(&self, txid: &str) -> Result<FetchedAncestor, GASPError> {
        self.requested.borrow_mut().push(txid.to_string());
        let node = self
            .nodes
            .get(txid)
            .ok_or_else(|| GASPError::NodeNotFound(format!("chain has no {txid}")))?;
        Ok(FetchedAncestor {
            raw_tx: node.raw_tx.clone(),
            proof: node.proof.clone(),
        })
    }
}

// PIN E, inverted by the lens (HIGH 2). A chain fetcher has no definite
// "cannot serve": every input of a mined transaction is on chain, so each of
// its errors is a fault of the moment (its budget, a provider outage). A
// fetch failure on a named input fails the UTXO and is retried; it never
// prunes. With a real chain fetcher a decoy is SERVED from chain.
#[tokio::test]
async fn d8_e_fetcher_arm_never_prunes_a_fetch_failure_fails_the_utxo() {
    let decoy = decoy_outpoint(0xd3);
    let nodes = decoy_chain(5, &[(4, decoy.clone())]);
    let fetcher = Rc::new(ChainOnlyFetcher {
        nodes: nodes.iter().map(|n| (node_txid(n), n.clone())).collect(),
        requested: RefCell::new(Vec::new()),
    });
    let manager = DecoyHeadManager::new();
    let (logs, _guard) = capture_logs();
    let (requests, graphs, cursor, pruned) = synchronize_counting(
        &nodes,
        &[4],
        Some(&manager),
        &MemoryStorage::new(),
        Some(fetcher.clone()),
    )
    .await;
    assert!(
        requests
            .iter()
            .all(|r| *r == (node_txid(&nodes[4]), 0, true)),
        "the peer is asked for the root only: {requests:?}"
    );
    assert!(
        fetcher.requested.borrow().contains(&decoy.txid),
        "the fetcher was asked for the named input and failed"
    );
    assert!(graphs.is_empty(), "the failed UTXO finalizes nothing");
    assert_eq!(pruned, 0, "the fetcher arm never prunes");
    assert_eq!(cursor, 0, "the gap guard keeps the failed tip retryable");
    let logs = logs.lock().unwrap();
    assert!(logs
        .iter()
        .any(|log| log.contains("Error ingesting UTXO") && log.contains("chain has no")));
    assert!(logs.iter().any(|log| log.contains("Capping cursor")));
    assert!(!logs.iter().any(|log| log.contains("Pruned input")));
}

// PIN F, reshaped by the lens (LOW 2). Two mined transactions cannot spend
// one outpoint, so two PARENTS never share a decoy. What does occur: two
// UTXOs of the one decoy-bearing tip are two graphs of one sync, and each
// graph's walk names the decoy. `seen` is per graph; the pruned set is per
// sync, so the decoy costs one failed round trip, not one per graph.
#[tokio::test]
async fn d8_f_a_decoy_shared_by_two_graphs_of_one_sync_is_requested_once() {
    let (_logs, _guard) = capture_logs();
    let decoy = decoy_outpoint(0xd4);
    let nodes = decoy_chain_with_tip_outputs(5, &[(4, decoy.clone())], 2);
    let tip = node_txid(&nodes[4]);
    let mut remote = RecordingRemote::new(&nodes, &[]);
    remote.utxos = (0..2)
        .map(|output_index| GASPOutput {
            txid: tip.clone(),
            output_index,
            score: (output_index + 1) as f64,
        })
        .collect();
    let manager = DecoyHeadManager::new();
    let (requests, graphs, cursor, pruned) =
        synchronize_remote(remote, Some(&manager), &MemoryStorage::new(), None).await;
    let (decoy_requests, chain_requests) = split_requests(&requests, &decoy);
    assert_eq!(decoy_requests, 1, "asked once, not once per graph");
    let roots: Vec<_> = chain_requests.iter().filter(|r| r.0 == tip).collect();
    assert_eq!(
        roots,
        vec![&(tip.clone(), 0, true), &(tip.clone(), 1, true)],
        "both UTXOs of the tip were walked as their own graph"
    );
    assert_eq!(graphs.len(), 2, "two graphs, both complete");
    for graph in &graphs {
        assert_eq!(
            graph_txids(graph),
            nodes.iter().map(node_txid).collect::<Vec<_>>()
        );
    }
    assert_eq!(cursor, 2);
    assert_eq!(pruned, 1, "one outpoint, one failed round trip, one count");
    println!(
        "D8 PIN F: 2 graphs of one sync, {} requests, {decoy_requests} for the shared decoy, pruned_inputs={pruned}",
        requests.len()
    );
}

// PIN G (the lens's MEDIUM 1). The peer holds the whole chain and there is NO
// decoy, but it cannot answer for the REAL head input right now (a timeout, a
// 500: `RemoteError`). That is not a definite "not held": nothing is pruned,
// the UTXO fails, nothing of the graph is admitted and the cursor does not
// pass it. The failure is carried by the per-UTXO warn and the cursor cap,
// not by the topic's `errors`: that list holds PEER failures, and four
// engine tests pin that a failed UTXO does not enter it. The next tick, with
// the peer healthy, admits the chain. Pruning here would finalize the tip
// alone, advance the cursor and never ask for the history again.
#[tokio::test]
async fn d8_g_a_transient_fault_on_the_real_head_input_fails_the_utxo_and_the_next_tick_recovers() {
    let (logs, _guard) = capture_logs();
    let nodes = decoy_chain(5, &[]);
    let expected: Vec<_> = nodes.iter().map(node_txid).collect();
    let remote = RecordingRemote::new(&nodes, &[4]);
    let requests = remote.requests.clone();
    let faults = remote.faults.clone();
    // The real head input of the proven tip: node 3.
    faults.borrow_mut().insert(expected[3].clone());
    let manager = DecoyHeadManager::new();
    let state = manager.0.clone();
    let store = Rc::new(MemoryStorage::new());
    let mut engine = Engine::new(
        HashMap::from([(
            TOPIC.to_string(),
            Box::new(manager) as Box<dyn TopicManager>,
        )]),
        HashMap::new(),
        Box::new(store.clone()),
        None,
        EngineConfig {
            sync_configuration: HashMap::from([(
                TOPIC.to_string(),
                SyncTarget::Peers(vec!["mock://head-chain".to_string()]),
            )]),
            ..Default::default()
        },
    );
    engine.set_gasp_remote_factory(Box::new(remote));

    // Tick 1: the peer blips on the real head input.
    let result = engine.start_gasp_sync().await.unwrap();
    let topic = &result.topics_synced[TOPIC];
    assert_eq!(topic.pruned_inputs, 0, "a transient fault is never pruned");
    {
        let logs = logs.lock().unwrap();
        assert!(
            logs.iter().any(|log| log.contains("Error ingesting UTXO")
                && log.contains(&format!("{}.0", expected[4]))
                && log.contains("HTTP 500")),
            "the failed UTXO is warned with the peer's error"
        );
        assert!(logs.iter().any(|log| log.contains("Capping cursor")));
        assert!(!logs.iter().any(|log| log.contains("Pruned input")));
    }
    assert!(state.borrow().admitted.is_empty(), "nothing admitted");
    assert!(store
        .find_utxos_for_topic(TOPIC, None, None, false)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        store
            .get_last_interaction("mock://head-chain", TOPIC)
            .await
            .unwrap(),
        0,
        "the cursor does not pass the failed UTXO"
    );
    let tick_one = requests.borrow().len();
    assert!(requests.borrow().iter().any(|r| r.0 == expected[3]));

    // Tick 2: the peer is healthy; the same chain is asked for again.
    faults.borrow_mut().clear();
    let result = engine.start_gasp_sync().await.unwrap();
    let topic = &result.topics_synced[TOPIC];
    assert_eq!(topic.pruned_inputs, 0);
    assert!(topic.errors.is_empty(), "{:?}", topic.errors);
    assert_eq!(
        requests.borrow()[tick_one..],
        tip_to_genesis(&nodes),
        "tick 2 walks tip to genesis"
    );
    assert_eq!(state.borrow().admitted, expected, "admit oldest first");
    let utxos = store
        .find_utxos_for_topic(TOPIC, None, None, false)
        .await
        .unwrap();
    assert_eq!(utxos.len(), 1);
    assert_eq!(utxos[0].txid, expected[4]);
    assert_eq!(
        store
            .get_last_interaction("mock://head-chain", TOPIC)
            .await
            .unwrap(),
        1
    );
    println!(
        "D8 PIN G: tick 1 {tick_one} requests, 0 admitted, pruned_inputs=0, cursor 0; tick 2 {} requests, 5 admitted, cursor 1",
        requests.borrow().len() - tick_one
    );
}
