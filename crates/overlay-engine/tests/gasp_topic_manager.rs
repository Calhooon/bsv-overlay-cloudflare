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
            assert!(tx.merkle_path.is_some(), "the dry run carries the proof");
        }
        // The engine encodes the reference's previousCoins: number[] as
        // little-endian u32 input indices. The rule is the same in every
        // mode (bsv-low #551): the needed-input dry run passes no coins, the
        // anchor replay (`historical-tx`) passes the coins of its own set,
        // and finalize (`historical-tx-no-spv`) the coins of the storage.
        let extends_head = previous_coins
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
    let synced = synchronize_tracked(remote, manager, store, fetcher, None).await;
    (synced.requests, synced.graphs, synced.cursor, synced.pruned)
}

struct Synced {
    requests: Vec<Request>,
    graphs: Vec<FinalizedGraph>,
    cursor: u64,
    pruned: u64,
    // `GASPSync::discarded_graphs`: graphs the anchor check refused (#551).
    discarded: u64,
}

// The same sync, with the anchor check's chain tracker (`None`: the
// reference's 'scripts only') and the count of graphs it refused.
async fn synchronize_tracked(
    remote: RecordingRemote,
    manager: Option<&dyn TopicManager>,
    store: &dyn Storage,
    fetcher: Option<Rc<dyn AncestorFetcher>>,
    tracker: Option<&dyn bsv_rs::transaction::ChainTracker>,
) -> Synced {
    let requests = remote.requests.clone();
    let sink = new_finalized_graph_sink();
    let mut adapter =
        OverlayGASPStorage::new(store, TOPIC, sink.clone()).with_strict_beef(fetcher.is_some());
    if let Some(manager) = manager {
        adapter = adapter.with_topic_manager(manager);
    }
    if let Some(tracker) = tracker {
        adapter = adapter.with_chain_tracker(tracker);
    }
    let mut sync = GASPSync::new(Box::new(adapter), Box::new(remote), 0, "[E1]", true)
        .with_ancestor_fetcher(fetcher);
    sync.sync(None).await.unwrap();
    let graphs = sink.lock().unwrap().clone();
    let requests = requests.borrow().clone();
    Synced {
        requests,
        graphs,
        cursor: sync.last_interaction,
        pruned: sync.pruned_inputs(),
        discarded: sync.discarded_graphs(),
    }
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

// E1 pin B, as it stands since bsv-low #551. A manager that names nothing
// leaves the WALK exactly as it was (the requests and the cursor of the
// manager-less adapter). What changed is the end of it: none of these managers
// admits the fixture's plain output, so the anchor check refuses the graph and
// NOTHING is finalized, where the base pushed BEEFs the engine then admitted
// nothing from. The bytes of what IS finalized are frozen by `i551_e`.
#[tokio::test]
async fn b_empty_input_managers_preserve_requests_and_finalize_nothing_they_refuse() {
    let (_logs, _guard) = capture_logs();
    let managers = workspace_managers();
    // A proven tip and an unproven two-hop tip ending at a proven ancestor.
    for unproven_from in [3, 1] {
        let nodes = scripted_chain(3, "51", unproven_from);
        let store = MemoryStorage::new();
        let (baseline_requests, baseline_graphs, baseline_cursor) =
            synchronize(&nodes, &[2], None, &store, None).await;
        assert_eq!(
            baseline_requests.len(),
            if unproven_from == 1 { 3 } else { 1 }
        );
        assert_eq!(baseline_graphs.len(), 1);
        for (name, manager) in &managers {
            let synced = synchronize_tracked(
                RecordingRemote::new(&nodes, &[2]),
                Some(manager.as_ref()),
                &store,
                None,
                None,
            )
            .await;
            assert_eq!(
                synced.requests, baseline_requests,
                "{name}: requested outpoints and metadata"
            );
            assert_eq!(synced.cursor, baseline_cursor, "{name}: cursor");
            assert!(
                synced.graphs.is_empty(),
                "{name}: a refused root finalizes nothing"
            );
            assert_eq!(synced.discarded, 1, "{name}: the discard is counted");
        }
    }
    println!("PIN B: 17 managers x 2 sync shapes = 34 walks identical, 34 refused roots discarded");
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
    // Outputs admitted for one transaction only (the rest get `outputs`).
    outputs_by_txid: HashMap<String, Vec<u32>>,
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
            outputs_to_admit: self
                .outputs_by_txid
                .get(&tx.id())
                .unwrap_or(&self.outputs)
                .clone(),
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
        // The independent genesis is admitted on sight.
        outputs_by_txid: HashMap::from([(node_txid(&nodes[0]), vec![0])]),
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
    // The error cuts off node 1 only: the walk goes on to the independent
    // graph, which finalizes. The cut-off graph itself is refused by the
    // anchor replay (bsv-low #551): a manager that would not admit node 1 or
    // node 2 without their history does not admit them without it at the end
    // either, so the root is not a coin and nothing of that graph is
    // finalized. (Before #551 its two BEEFs reached the sink, and the engine
    // then admitted nothing from them.)
    assert_eq!(graphs.len(), 1, "only the independent graph finalizes");
    assert_eq!(graph_txids(&graphs[0]), vec![node_txid(&nodes[0])]);
    assert_eq!(cursor, 2);
    assert_eq!(manager.needed.borrow().len(), 2);
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
        // The independent genesis is admitted on sight (its graph must pass
        // the anchor replay to finalize, bsv-low #551).
        outputs_by_txid: HashMap::from([(node_txid(&nodes[0]), vec![0])]),
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
            // Names nothing (the trait default) and admits what it is shown:
            // a manager that refused the tip would have its graph discarded
            // by the anchor replay (bsv-low #551).
            Box::new(AdmitsOutputZero(Rc::new(
                RefCell::new(HeadState::default()),
            )))
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
        // The same rule in every mode (bsv-low #551): the anchor replay runs
        // it under `historical-tx` with the coins of its own set.
        let extends_head = previous_coins.chunks_exact(4).any(|index| {
            let index = u32::from_le_bytes(index.try_into().unwrap()) as usize;
            tx.inputs.get(index).is_some_and(witness_shaped)
        });
        if !tx.inputs.is_empty() && !extends_head {
            return Ok(AdmittanceInstructions::default());
        }
        if mode == SubmitMode::HistoricalTxNoSpv {
            self.0.borrow_mut().admitted.push(tx.id());
        }
        // Every output of a head transaction is a coin of the topic (pin F
        // syncs two UTXOs of one tip; a root the manager does not admit is
        // discarded by the anchor replay, bsv-low #551).
        Ok(AdmittanceInstructions {
            outputs_to_admit: (0..tx.outputs.len() as u32).collect(),
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

// D8 pin D (managers that name nothing sync byte for byte as on the base)
// froze, per manager, the BEEFs pushed to the sink. Since bsv-low #551 a root
// its manager refuses pushes none, so that digest cannot hold as it was; the
// same claim is frozen by `i551_e_managers_naming_nothing_walk_and_finalize_
// byte_for_byte`, captured on 39081dd: every workspace manager's requests and
// cursor on both shapes, nothing pruned, and the finalized bytes wherever a
// graph is finalized.

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

// ============================================================================
// bsv-low #551: the anchor check. `validate_graph_anchor` verifies the ROOT
// node's BEEF like the reference (`spvTx.verify(this.engine.chainTracker)`),
// then replays the ordered BEEFs through the topic manager over a set of
// coins; a graph whose root is not a coin at the end is discarded whole.
// ============================================================================

use bsv_rs::transaction::{ChainTracker, ChainTrackerError};
use std::sync::atomic::{AtomicUsize, Ordering};

// A chain tracker that knows exactly the (height, root) pairs it was given,
// and counts what it is asked.
#[derive(Clone, Default)]
struct KnownRoots {
    roots: Arc<Mutex<HashSet<(u32, String)>>>,
    asked: Arc<AtomicUsize>,
    // While set, every lookup FAILS (an outage), it never answers.
    down: Arc<Mutex<bool>>,
}

impl KnownRoots {
    // The root of every proven node's OWN merkle path.
    fn of(nodes: &[GASPNode]) -> Self {
        let tracker = Self::default();
        for node in nodes {
            if let Some(proof) = &node.proof {
                let path = MerklePath::from_hex(proof).unwrap();
                let root = path.compute_root(Some(&node_txid(node))).unwrap();
                tracker
                    .roots
                    .lock()
                    .unwrap()
                    .insert((path.block_height, root));
            }
        }
        tracker
    }
}

#[async_trait]
impl ChainTracker for KnownRoots {
    async fn is_valid_root_for_height(
        &self,
        root: &str,
        height: u32,
    ) -> Result<bool, ChainTrackerError> {
        self.asked.fetch_add(1, Ordering::SeqCst);
        if *self.down.lock().unwrap() {
            return Err(ChainTrackerError::NetworkError(
                "chaintracks unreachable".into(),
            ));
        }
        Ok(self
            .roots
            .lock()
            .unwrap()
            .contains(&(height, root.to_string())))
    }
    async fn current_height(&self) -> Result<u32, ChainTrackerError> {
        Ok(1_000)
    }
}

// A merkle path that PARSES and names the transaction, in a two-leaf block
// whose other leaf nobody mined: its root is one no chain tracker knows.
fn fabricated_proof(txid: &str, height: u32) -> String {
    MerklePath::new(
        height,
        vec![vec![
            MerklePathLeaf::new_txid(0, txid.to_string()),
            MerklePathLeaf::new(1, "ab".repeat(32)),
        ]],
    )
    .unwrap()
    .to_hex()
}

fn honest_proof(txid: &str, height: u32) -> String {
    MerklePath::new(
        height,
        vec![vec![MerklePathLeaf::new_txid(0, txid.to_string())]],
    )
    .unwrap()
    .to_hex()
}

fn node_of(tx: &Transaction, output_index: u32, proof: Option<String>) -> GASPNode {
    GASPNode {
        graph_id: String::new(),
        raw_tx: tx.to_hex(),
        output_index,
        proof,
        tx_metadata: None,
        output_metadata: None,
        inputs: None,
    }
}

// An input that REALLY spends an output locked by `OP_1` (an empty unlocking
// script), so an unproven spend of it passes the interpreter.
fn spending(txid: String, output_index: u32) -> TransactionInput {
    let mut input = TransactionInput::new(txid, output_index);
    input.set_unlocking_script(UnlockingScript::from_hex("").unwrap());
    input
}

// A chain like `chain`, but every output is locked by `locking` (hex) and
// every spend carries an empty unlocking script; nodes from `unproven_from`
// up carry no proof. With `51` (OP_1) the unproven spends are valid; with
// `00` (OP_0) the interpreter refuses them.
fn scripted_chain(length: usize, locking: &str, unproven_from: usize) -> Vec<GASPNode> {
    let mut nodes = Vec::new();
    let mut previous = None;
    for height in 0..length {
        let mut tx = Transaction::new();
        if let Some(txid) = previous {
            tx.inputs.push(spending(txid, 0));
        }
        tx.outputs.push(TransactionOutput::new(
            1000,
            LockingScript::from_hex(locking).unwrap(),
        ));
        let txid = tx.id();
        let proof = (height < unproven_from).then(|| honest_proof(&txid, 100 + height as u32));
        nodes.push(node_of(&tx, 0, proof));
        previous = Some(txid);
    }
    nodes
}

// Admits output 0 of everything and names no inputs (the trait default).
struct AdmitsOutputZero(Rc<RefCell<HeadState>>);

#[async_trait(?Send)]
impl TopicManager for AdmitsOutputZero {
    async fn identify_admissible_outputs(
        &self,
        tx: &Transaction,
        _previous_coins: &[u8],
        _off_chain_values: Option<&[u8]>,
        mode: SubmitMode,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        if mode == SubmitMode::HistoricalTxNoSpv {
            self.0.borrow_mut().admitted.push(tx.id());
        }
        Ok(AdmittanceInstructions {
            outputs_to_admit: vec![0],
            ..Default::default()
        })
    }
    async fn get_documentation(&self) -> String {
        String::new()
    }
    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata::default()
    }
}

struct EngineSync {
    result: bsv_overlay_engine::engine::GASPSyncResult,
    requests: Vec<Request>,
    store: Rc<MemoryStorage>,
    // SHA-256 over the topic's UTXOs (outpoint, script, sats, stored BEEF),
    // sorted by outpoint: what the sync ADMITTED, byte for byte.
    digest: String,
}

async fn admitted_digest(store: &MemoryStorage) -> String {
    let mut utxos = store
        .find_utxos_for_topic(TOPIC, None, None, true)
        .await
        .unwrap();
    utxos.sort_by(|a, b| (&a.txid, a.output_index).cmp(&(&b.txid, b.output_index)));
    let mut transcript = Vec::new();
    for utxo in &utxos {
        transcript.extend_from_slice(
            format!("{}.{}|{}|", utxo.txid, utxo.output_index, utxo.satoshis).as_bytes(),
        );
        transcript.extend_from_slice(&utxo.output_script);
        let beef = utxo.beef.clone().unwrap_or_default();
        transcript.extend_from_slice(&(beef.len() as u64).to_le_bytes());
        transcript.extend_from_slice(&beef);
    }
    hex::encode(bsv_rs::primitives::hash::sha256(&transcript))
}

// One engine sync of `tips` against a peer that serves `nodes`.
async fn engine_sync(
    remote: RecordingRemote,
    manager: Box<dyn TopicManager>,
    store: Rc<MemoryStorage>,
    tracker: Option<Box<dyn ChainTracker>>,
) -> EngineSync {
    let requests = remote.requests.clone();
    let mut engine = Engine::with_chain_tracker(
        HashMap::from([(TOPIC.to_string(), manager)]),
        HashMap::new(),
        Box::new(store.clone()),
        None,
        None,
        tracker,
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
    let digest = admitted_digest(&store).await;
    let requests = requests.borrow().clone();
    EngineSync {
        result,
        requests,
        store,
        digest,
    }
}

async fn utxo_txids(store: &MemoryStorage) -> Vec<String> {
    let mut txids: Vec<_> = store
        .find_utxos_for_topic(TOPIC, None, None, false)
        .await
        .unwrap()
        .into_iter()
        .map(|o| o.txid)
        .collect();
    txids.sort();
    txids
}

// PIN A. Five proven head transactions; the tracker knows the HONEST root of
// every one of them, and the peer serves the TIP with a fabricated BUMP.
#[tokio::test]
async fn i551_a_a_fabricated_bump_on_the_tip_discards_the_whole_chain() {
    let (_logs, _guard) = capture_logs();
    let mut nodes = chain(5);
    let tracker = KnownRoots::of(&nodes);
    nodes[4].proof = Some(fabricated_proof(&node_txid(&nodes[4]), 104));
    let state = Rc::new(RefCell::new(HeadState::default()));
    let synced = engine_sync(
        RecordingRemote::new(&nodes, &[4]),
        Box::new(HeadChainManager(state.clone())),
        Rc::new(MemoryStorage::new()),
        Some(Box::new(tracker.clone())),
    )
    .await;
    assert_eq!(synced.requests.len(), 5, "the walk still reaches genesis");
    assert!(
        state.borrow().admitted.is_empty(),
        "nothing of the chain is admitted: {:?}",
        state.borrow().admitted
    );
    assert!(utxo_txids(&synced.store).await.is_empty());
    let topic = &synced.result.topics_synced[TOPIC];
    assert_eq!(topic.discarded_graphs, 1, "the discard is counted");
    assert!(
        topic.errors.is_empty(),
        "a refused graph is not a peer error"
    );
    assert_eq!(
        tracker.asked.load(Ordering::SeqCst),
        1,
        "a proven root is one question: its own root"
    );

    // The same sync at the adapter: the sink stays EMPTY, and the warn names
    // the root and the reason.
    let (logs, _guard) = capture_logs();
    let manager = HeadChainManager(Rc::new(RefCell::new(HeadState::default())));
    let synced = synchronize_tracked(
        RecordingRemote::new(&nodes, &[4]),
        Some(&manager),
        &MemoryStorage::new(),
        None,
        Some(&tracker),
    )
    .await;
    assert!(synced.graphs.is_empty(), "the sink is empty");
    assert_eq!((synced.discarded, synced.cursor), (1, 1));
    let root = format!("{}.0", node_txid(&nodes[4]));
    assert!(
        logs.lock().unwrap().iter().any(|log| log.contains(&root)
            && log.contains("not well-anchored according to the rules of Bitcoin")
            && log.contains("is not valid for block height 104")),
        "the warn names the root and the reason"
    );
    println!("#551 PIN A: 5 requests, 1 tracker question, 0 admitted, discarded_graphs=1");
}

// The head chain of `chain(4)` whose last head also carries a second, plain
// output, and a proven TIP that spends THAT output: it does not extend the head.
fn chain_with_a_tip_off_the_head() -> Vec<GASPNode> {
    let mut nodes = chain(3);
    let mut head = Transaction::new();
    head.inputs
        .push(TransactionInput::new(node_txid(&nodes[2]), 0));
    for _ in 0..2 {
        head.outputs.push(TransactionOutput::new(
            1000,
            LockingScript::from_hex("76a914000000000000000000000000000000000000000088ac").unwrap(),
        ));
    }
    nodes.push(node_of(&head, 0, Some(honest_proof(&head.id(), 103))));
    let mut tip = Transaction::new();
    tip.inputs.push(TransactionInput::new(head.id(), 1));
    tip.outputs.push(TransactionOutput::new(
        1000,
        LockingScript::from_hex("76a914000000000000000000000000000000000000000088ac").unwrap(),
    ));
    nodes.push(node_of(&tip, 0, Some(honest_proof(&tip.id(), 104))));
    nodes
}

// PIN B. Every BUMP is honest and known; the manager admits the four heads
// behind the tip and REFUSES the tip at the end of the replay.
#[tokio::test]
async fn i551_b_a_root_the_manager_refuses_at_the_end_admits_nothing() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain_with_a_tip_off_the_head();
    let state = Rc::new(RefCell::new(HeadState::default()));
    let synced = engine_sync(
        RecordingRemote::new(&nodes, &[4]),
        Box::new(HeadChainManager(state.clone())),
        Rc::new(MemoryStorage::new()),
        Some(Box::new(KnownRoots::of(&nodes))),
    )
    .await;
    assert_eq!(synced.requests.len(), 5, "the walk reaches genesis");
    assert!(
        state.borrow().admitted.is_empty(),
        "the four heads behind a refused root are not admitted: {:?}",
        state.borrow().admitted
    );
    assert!(utxo_txids(&synced.store).await.is_empty());
    assert_eq!(synced.result.topics_synced[TOPIC].discarded_graphs, 1);

    let (logs, _guard) = capture_logs();
    let manager = HeadChainManager(Rc::new(RefCell::new(HeadState::default())));
    let tracker = KnownRoots::of(&nodes);
    let synced = synchronize_tracked(
        RecordingRemote::new(&nodes, &[4]),
        Some(&manager),
        &MemoryStorage::new(),
        None,
        Some(&tracker),
    )
    .await;
    assert!(synced.graphs.is_empty(), "the sink is empty");
    assert_eq!(synced.discarded, 1);
    let root = format!("{}.0", node_txid(&nodes[4]));
    assert!(
        logs.lock().unwrap().iter().any(|log| log.contains(&root)
            && log.contains("did not result in topical admittance of the root node")),
        "the warn names the root and the reason"
    );
    println!("#551 PIN B: 5 requests, 4 heads replayed as coins, root refused, 0 admitted");
}

// PIN C. The E1 head chain and the D8 decoy chain under a tracker that knows
// the fixture roots: the same walk, the same admission order, and the admitted
// set byte for byte what the base (39081dd, no anchor check) admitted.
const I551_C_HEAD_BASE_DIGEST: &str =
    "8615f5ec72dcbf835876a5065b6a507ac220b30fb230738b8fbaa0a4d896e864";
const I551_C_DECOY_BASE_DIGEST: &str =
    "47e0ff752b9c7220194f6448673bc13995fb3ada7104923b0e9ea7b02d9bb7a8";

#[tokio::test]
async fn i551_c_head_chain_and_decoy_chain_admit_byte_for_byte_under_a_tracker() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(5);
    let tracker = KnownRoots::of(&nodes);
    let state = Rc::new(RefCell::new(HeadState::default()));
    let head = engine_sync(
        RecordingRemote::new(&nodes, &[4]),
        Box::new(HeadChainManager(state.clone())),
        Rc::new(MemoryStorage::new()),
        Some(Box::new(tracker.clone())),
    )
    .await;
    let expected: Vec<_> = nodes.iter().map(node_txid).collect();
    assert_eq!(head.requests, tip_to_genesis(&nodes));
    assert_eq!(state.borrow().admitted, expected, "admit oldest first");
    assert_eq!(utxo_txids(&head.store).await, vec![expected[4].clone()]);
    println!("#551 PIN C head digest {}", head.digest);
    assert_eq!(head.digest, I551_C_HEAD_BASE_DIGEST);

    let decoy = decoy_outpoint(0xd0);
    let nodes = decoy_chain(5, &[(4, decoy.clone())]);
    let manager = DecoyHeadManager::new();
    let state = manager.0.clone();
    let decoyed = engine_sync(
        RecordingRemote::new(&nodes, &[4]),
        Box::new(manager),
        Rc::new(MemoryStorage::new()),
        Some(Box::new(KnownRoots::of(&nodes))),
    )
    .await;
    let expected: Vec<_> = nodes.iter().map(node_txid).collect();
    let (decoy_requests, chain_requests) = split_requests(&decoyed.requests, &decoy);
    assert_eq!(decoy_requests, 1);
    assert_eq!(chain_requests, tip_to_genesis(&nodes));
    assert_eq!(state.borrow().admitted, expected, "admit oldest first");
    assert_eq!(decoyed.result.topics_synced[TOPIC].pruned_inputs, 1);
    println!("#551 PIN C decoy digest {}", decoyed.digest);
    assert_eq!(decoyed.digest, I551_C_DECOY_BASE_DIGEST);
}

// PIN D. No chain tracker: the reference's `'scripts only'` walk. An unproven
// node whose unlocking script the interpreter refuses discards its graph; the
// same graph with a valid spend is admitted; a proven node is accepted on its
// BUMP unchecked (the fabricated BUMP of pin A, with nobody to ask).
#[tokio::test]
async fn i551_d_no_tracker_runs_the_scripts_only_walk() {
    let (_logs, _guard) = capture_logs();
    for (locking, admitted) in [("00", 0), ("51", 2)] {
        let nodes = scripted_chain(2, locking, 1);
        let state = Rc::new(RefCell::new(HeadState::default()));
        let synced = engine_sync(
            RecordingRemote::new(&nodes, &[1]),
            Box::new(AdmitsOutputZero(state.clone())),
            Rc::new(MemoryStorage::new()),
            None,
        )
        .await;
        assert_eq!(synced.requests.len(), 2);
        assert_eq!(
            state.borrow().admitted.len(),
            admitted,
            "an unproven spend of a `{locking}` output"
        );
        assert_eq!(utxo_txids(&synced.store).await.len(), admitted.min(1));
    }

    let mut nodes = chain(5);
    nodes[4].proof = Some(fabricated_proof(&node_txid(&nodes[4]), 104));
    let state = Rc::new(RefCell::new(HeadState::default()));
    engine_sync(
        RecordingRemote::new(&nodes, &[4]),
        Box::new(HeadChainManager(state.clone())),
        Rc::new(MemoryStorage::new()),
        None,
    )
    .await;
    assert_eq!(
        state.borrow().admitted.len(),
        5,
        "'scripts only': a BUMP is accepted unchecked"
    );
}

// PIN E. Managers that name nothing, the E1 pin B shapes (a proven tip, and an
// unproven two-hop tip over a proven ancestor, here with spends the
// interpreter accepts). Frozen on the base (39081dd): the requests and the
// cursor of every workspace manager, and the finalized BEEF bytes of the
// manager-less adapter and of a manager that admits what it is shown.
const I551_E_BASE_DIGEST: &str = "500553a385745ca37de407de5d03e6f89d2ce262531b3542108fda86423fac74";

#[tokio::test]
async fn i551_e_managers_naming_nothing_walk_and_finalize_byte_for_byte() {
    let (_logs, _guard) = capture_logs();
    let mut transcript = Vec::new();
    for unproven_from in [3, 1] {
        let nodes = scripted_chain(3, "51", unproven_from);
        let tracker = KnownRoots::of(&nodes);
        let store = MemoryStorage::new();
        let admits = AdmitsOutputZero(Rc::new(RefCell::new(HeadState::default())));
        let finalizing: [(&str, Option<&dyn TopicManager>); 2] =
            [("none", None), ("admits", Some(&admits))];
        for (name, manager) in finalizing {
            let synced = synchronize_tracked(
                RecordingRemote::new(&nodes, &[2]),
                manager,
                &store,
                None,
                Some(&tracker),
            )
            .await;
            assert_eq!(synced.graphs.len(), 1, "{name}: one finalized graph");
            assert_eq!((synced.pruned, synced.discarded), (0, 0), "{name}");
            transcript.extend_from_slice(
                format!(
                    "{name}|{unproven_from}|{:?}|{}|",
                    synced.requests, synced.cursor
                )
                .as_bytes(),
            );
            for beef in &synced.graphs[0].beefs {
                transcript.extend_from_slice(&(beef.len() as u64).to_le_bytes());
                transcript.extend_from_slice(beef);
            }
        }
        for (name, manager) in &workspace_managers() {
            let synced = synchronize_tracked(
                RecordingRemote::new(&nodes, &[2]),
                Some(manager.as_ref()),
                &store,
                None,
                Some(&tracker),
            )
            .await;
            assert_eq!(synced.pruned, 0, "{name}: nothing named, nothing pruned");
            // None of them admits the fixture's `OP_1` output: on the base
            // the sink carried BEEFs the engine admitted nothing from, now
            // the refused root is discarded and the sink stays empty.
            assert!(synced.graphs.is_empty(), "{name}: a refused root");
            assert_eq!(synced.discarded, 1, "{name}: the discard is counted");
            transcript.extend_from_slice(
                format!(
                    "{name}|{unproven_from}|{:?}|{}|",
                    synced.requests, synced.cursor
                )
                .as_bytes(),
            );
        }
        // One question per merkle path of each ROOT BEEF: a proven tip asks
        // once per sync, the unproven two-hop tip asks for its one proven
        // ancestor.
        assert_eq!(
            tracker.asked.load(Ordering::SeqCst),
            19,
            "tracker questions"
        );
    }
    let digest = hex::encode(bsv_rs::primitives::hash::sha256(&transcript));
    println!("#551 PIN E: transcript sha256 {digest}");
    assert_eq!(digest, I551_E_BASE_DIGEST);
}

// A head manager that RETAINS (or not) the head coin it spends, and records
// what the anchor replay showed it: (txid, previous coins) per call that
// carried coins under `historical-tx`.
type Replay = Rc<RefCell<Vec<(String, Vec<u8>)>>>;

struct RetainingHeadManager {
    retain: bool,
    state: Rc<RefCell<HeadState>>,
    replay: Replay,
}

#[async_trait(?Send)]
impl TopicManager for RetainingHeadManager {
    async fn identify_admissible_outputs(
        &self,
        tx: &Transaction,
        previous_coins: &[u8],
        _off_chain_values: Option<&[u8]>,
        mode: SubmitMode,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        if mode == SubmitMode::HistoricalTx && !previous_coins.is_empty() {
            self.replay
                .borrow_mut()
                .push((tx.id(), previous_coins.to_vec()));
        }
        if !tx.inputs.is_empty() && previous_coins.is_empty() {
            return Ok(AdmittanceInstructions::default());
        }
        if mode == SubmitMode::HistoricalTxNoSpv {
            self.state.borrow_mut().admitted.push(tx.id());
        }
        Ok(AdmittanceInstructions {
            outputs_to_admit: vec![0],
            coins_to_retain: if self.retain && !tx.inputs.is_empty() {
                vec![0]
            } else {
                vec![]
            },
            ..Default::default()
        })
    }

    async fn identify_needed_inputs(
        &self,
        beef: &[u8],
        _off_chain_values: Option<&[u8]>,
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

// PIN F. `coins_to_retain` in the replay, as the reference at f999e0c1a has
// it: `admitHistoricalBEEF` reads `outputsToAdmit` alone and its set only
// grows, so whether a manager RETAINS the coin it spends changes nothing the
// replay shows a later transaction or decides about the root. It changes what
// FINALIZE leaves behind: the engine keeps a retained coin (spent) and
// deletes one that is not retained. And where a coin WOULD be shown twice (two
// transactions of one graph spending it) the graph is refused, retained or
// not: finalize could admit only one of them.
#[tokio::test]
async fn i551_f_coins_to_retain_does_not_move_the_replay_and_a_double_spend_is_refused() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(4);
    let txids: Vec<_> = nodes.iter().map(node_txid).collect();
    let first_input = 0u32.to_le_bytes().to_vec();
    let mut replays = Vec::new();
    for retain in [false, true] {
        let state = Rc::new(RefCell::new(HeadState::default()));
        let replay = Rc::new(RefCell::new(Vec::new()));
        let synced = engine_sync(
            RecordingRemote::new(&nodes, &[3]),
            Box::new(RetainingHeadManager {
                retain,
                state: state.clone(),
                replay: replay.clone(),
            }),
            Rc::new(MemoryStorage::new()),
            Some(Box::new(KnownRoots::of(&nodes))),
        )
        .await;
        assert_eq!(state.borrow().admitted, txids, "retain={retain}: all four");
        assert_eq!(
            *replay.borrow(),
            txids[1..]
                .iter()
                .map(|txid| (txid.clone(), first_input.clone()))
                .collect::<Vec<_>>(),
            "retain={retain}: each spend is shown the head coin before it"
        );
        for spent_head in &txids[..3] {
            let kept = synced
                .store
                .find_output(spent_head, 0, Some(TOPIC), None, false)
                .await
                .unwrap();
            assert_eq!(
                kept.is_some(),
                retain,
                "retain={retain}: finalize keeps a spent head only when it is retained"
            );
        }
        replays.push(replay.borrow().clone());
    }
    assert_eq!(replays[0], replays[1], "the replay is the same either way");

    // A (genesis) is spent by BOTH X and Y, and the root R spends them both.
    let coin = |sats: u64| {
        TransactionOutput::new(
            sats,
            LockingScript::from_hex("76a914000000000000000000000000000000000000000088ac").unwrap(),
        )
    };
    let mut a = Transaction::new();
    a.outputs.push(coin(1000));
    let mut x = Transaction::new();
    x.inputs.push(TransactionInput::new(a.id(), 0));
    x.outputs.push(coin(900));
    let mut y = Transaction::new();
    y.inputs.push(TransactionInput::new(a.id(), 0));
    y.outputs.push(coin(800));
    let mut r = Transaction::new();
    r.inputs.push(TransactionInput::new(x.id(), 0));
    r.inputs.push(TransactionInput::new(y.id(), 0));
    r.outputs.push(coin(700));
    let nodes: Vec<_> = [&a, &x, &y, &r]
        .iter()
        .enumerate()
        .map(|(i, tx)| node_of(tx, 0, Some(honest_proof(&tx.id(), 100 + i as u32))))
        .collect();
    for retain in [false, true] {
        let state = Rc::new(RefCell::new(HeadState::default()));
        let synced = engine_sync(
            RecordingRemote::new(&nodes, &[3]),
            Box::new(RetainingHeadManager {
                retain,
                state: state.clone(),
                replay: Rc::new(RefCell::new(Vec::new())),
            }),
            Rc::new(MemoryStorage::new()),
            Some(Box::new(KnownRoots::of(&nodes))),
        )
        .await;
        assert!(
            state.borrow().admitted.is_empty(),
            "retain={retain}: nothing of a graph that spends {}.0 twice: {:?}",
            a.id(),
            state.borrow().admitted
        );
        assert_eq!(synced.result.topics_synced[TOPIC].discarded_graphs, 1);
    }
    println!("#551 PIN F: retain or not, one replay; a double spend in the graph admits nothing");
}

// PIN G (an addition to the reference, see `validate_graph_anchor`). The next
// head, synced one tick after the last: its head input is already HELD, so the
// walk strips it and the graph is the tip alone. The replay counts the coin
// the storage holds, exactly as the finalize submit will; the reference's own
// set would be empty and the tip refused on every tick, forever.
#[tokio::test]
async fn i551_g_the_next_head_over_a_held_head_is_admitted() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(5);
    let tracker = KnownRoots::of(&nodes);
    let state = Rc::new(RefCell::new(HeadState::default()));
    let store = Rc::new(MemoryStorage::new());
    engine_sync(
        RecordingRemote::new(&nodes[..4], &[3]),
        Box::new(HeadChainManager(state.clone())),
        store.clone(),
        Some(Box::new(tracker.clone())),
    )
    .await;
    assert_eq!(state.borrow().admitted.len(), 4);
    let tick_two = engine_sync(
        RecordingRemote::new(&nodes, &[4]),
        Box::new(HeadChainManager(state.clone())),
        store.clone(),
        Some(Box::new(tracker.clone())),
    )
    .await;
    assert_eq!(
        tick_two.requests,
        vec![(node_txid(&nodes[4]), 0, true)],
        "the held head is stripped: the graph is the tip alone"
    );
    assert_eq!(
        state.borrow().admitted,
        nodes.iter().map(node_txid).collect::<Vec<_>>()
    );
    assert_eq!(utxo_txids(&store).await, vec![node_txid(&nodes[4])]);
    assert_eq!(tick_two.result.topics_synced[TOPIC].discarded_graphs, 0);
}

// PIN H (an addition). The root R spends two history inputs its manager names:
// X (real) and W. The peer answers the request for W with ANOTHER transaction,
// a forged Z under a BUMP nobody mined. Z is not in the root's BEEF, so the
// Bitcoin check never sees it; the rule that every node is an input of its
// parent refuses the graph. Without it Z would be finalized and admitted.
#[tokio::test]
async fn i551_h_a_transaction_served_in_place_of_a_named_input_refuses_the_graph() {
    let (_logs, _guard) = capture_logs();
    let coin = |sats: u64| {
        TransactionOutput::new(
            sats,
            LockingScript::from_hex("76a914000000000000000000000000000000000000000088ac").unwrap(),
        )
    };
    let mut x = Transaction::new();
    x.outputs.push(coin(1000));
    let mut w = Transaction::new();
    w.outputs.push(coin(999));
    let mut forged = Transaction::new();
    forged.outputs.push(coin(777));
    let mut r = Transaction::new();
    r.inputs.push(TransactionInput::new(x.id(), 0));
    r.inputs.push(TransactionInput::new(w.id(), 0));
    r.outputs.push(coin(500));
    let honest: Vec<_> = [&x, &w, &r]
        .iter()
        .enumerate()
        .map(|(i, tx)| node_of(tx, 0, Some(honest_proof(&tx.id(), 100 + i as u32))))
        .collect();
    let tracker = KnownRoots::of(&honest);
    for forge in [false, true] {
        let mut remote = RecordingRemote::new(&honest, &[2]);
        if forge {
            let mut served = (*remote.nodes).clone();
            served.insert(
                w.id(),
                node_of(&forged, 0, Some(fabricated_proof(&forged.id(), 101))),
            );
            remote.nodes = Rc::new(served);
        }
        let state = Rc::new(RefCell::new(HeadState::default()));
        let synced = engine_sync(
            remote,
            Box::new(RetainingHeadManager {
                retain: false,
                state: state.clone(),
                replay: Rc::new(RefCell::new(Vec::new())),
            }),
            Rc::new(MemoryStorage::new()),
            Some(Box::new(tracker.clone())),
        )
        .await;
        let mut admitted = state.borrow().admitted.clone();
        admitted.sort();
        if forge {
            assert!(admitted.is_empty(), "nothing, the forged Z least of all");
            assert_eq!(synced.result.topics_synced[TOPIC].discarded_graphs, 1);
        } else {
            let mut all = vec![x.id(), w.id(), r.id()];
            all.sort();
            assert_eq!(admitted, all, "the honest graph is admitted whole");
        }
    }
}

// PIN I (an addition). A chain tracker that cannot ANSWER is not a verdict:
// the graph is discarded, nothing is admitted, but the UTXO FAILS, so the
// cursor does not pass it, and the next tick, with the tracker back, admits
// the chain. The reference would discard and advance, losing it for good.
#[tokio::test]
async fn i551_i_a_tracker_outage_fails_the_utxo_and_the_next_tick_recovers() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(5);
    let tracker = KnownRoots::of(&nodes);
    let state = Rc::new(RefCell::new(HeadState::default()));
    let store = Rc::new(MemoryStorage::new());
    *tracker.down.lock().unwrap() = true;
    let tick_one = engine_sync(
        RecordingRemote::new(&nodes, &[4]),
        Box::new(HeadChainManager(state.clone())),
        store.clone(),
        Some(Box::new(tracker.clone())),
    )
    .await;
    assert!(state.borrow().admitted.is_empty());
    let topic = &tick_one.result.topics_synced[TOPIC];
    assert_eq!(topic.discarded_graphs, 0, "no verdict, so not a refusal");
    assert_eq!(
        store
            .get_last_interaction("mock://head-chain", TOPIC)
            .await
            .unwrap(),
        0,
        "the cursor does not pass a graph nobody judged"
    );

    *tracker.down.lock().unwrap() = false;
    engine_sync(
        RecordingRemote::new(&nodes, &[4]),
        Box::new(HeadChainManager(state.clone())),
        store.clone(),
        Some(Box::new(tracker.clone())),
    )
    .await;
    assert_eq!(state.borrow().admitted.len(), 5, "tick 2 admits the chain");
    assert_eq!(
        store
            .get_last_interaction("mock://head-chain", TOPIC)
            .await
            .unwrap(),
        1
    );
}

// PIN J (an addition). An UNPROVEN tip over a coin the storage already holds:
// the walk strips the held input, so the graph does not carry its source. The
// stored BEEF of that coin is merged into the CHECKED copy and the spend is
// executed against its real source (the reference throws in `getBEEFForNode`
// and discards). `51` (OP_1) is a spend the interpreter accepts; `00` (OP_0)
// one it refuses, which shows the script really ran.
#[tokio::test]
async fn i551_j_an_unproven_tip_over_a_held_coin_is_executed_against_the_stored_source() {
    let (_logs, _guard) = capture_logs();
    for (locking, admitted) in [("51", 2), ("00", 1)] {
        let nodes = scripted_chain(3, locking, 2);
        let tracker = KnownRoots::of(&nodes);
        let state = Rc::new(RefCell::new(HeadState::default()));
        let store = Rc::new(MemoryStorage::new());
        engine_sync(
            RecordingRemote::new(&nodes[..2], &[1]),
            Box::new(AdmitsOutputZero(state.clone())),
            store.clone(),
            Some(Box::new(tracker.clone())),
        )
        .await;
        // A manager that names nothing stops at the proven tip: tick 1 admits
        // node 1 alone, with its proof.
        assert_eq!(state.borrow().admitted.len(), 1, "`{locking}`: tick 1");
        let asked = tracker.asked.load(Ordering::SeqCst);
        let tick_two = engine_sync(
            RecordingRemote::new(&nodes, &[2]),
            Box::new(AdmitsOutputZero(state.clone())),
            store.clone(),
            Some(Box::new(tracker.clone())),
        )
        .await;
        assert_eq!(tick_two.requests, vec![(node_txid(&nodes[2]), 0, true)]);
        assert_eq!(
            state.borrow().admitted.len(),
            admitted,
            "`{locking}`: tick 2"
        );
        assert_eq!(
            tick_two.result.topics_synced[TOPIC].discarded_graphs,
            (2 - admitted) as u64
        );
        // The walk runs a spend before it descends into its source: an
        // accepted spend goes on to check the stored source's own BUMP, a
        // refused one ends there.
        assert_eq!(
            tracker.asked.load(Ordering::SeqCst) > asked,
            locking == "51",
            "`{locking}`: the stored source's own BUMP"
        );
    }
}

// ============================================================================
// bsv-low #552: progress under the per-peer sync budget. With a budget set the
// engine submits each graph AS IT FINALIZES, inside the raced future, so the
// deadline cannot take back what was finalized before it; at the deadline the
// cursor moves past the COMPLETED UTXOs only.
// ============================================================================

use std::cell::Cell;

const PEER: &str = "mock://head-chain";

// A clock that counts REQUESTS: every request to the peer costs one unit and
// a tick may spend `allowance` of them. The request that overspends never
// answers and the deadline is due from that moment, so `race_or_deadline`
// drops the sync exactly there, on every run (a wall-clock sleep per request
// would pin the same thing with a race in it).
#[derive(Clone)]
struct RequestClock {
    spent: Rc<Cell<u64>>,
    allowance: Rc<Cell<u64>>,
}

impl RequestClock {
    fn allowing(allowance: u64) -> Self {
        Self {
            spent: Rc::new(Cell::new(0)),
            allowance: Rc::new(Cell::new(allowance)),
        }
    }

    async fn spend(&self) {
        self.spent.set(self.spent.get() + 1);
        if self.spent.get() > self.allowance.get() {
            std::future::pending::<()>().await;
        }
    }

    // The engine asks for one deadline per peer sync: a new tick's budget.
    fn budget(&self) -> bsv_overlay_engine::engine::SleepFactory {
        let clock = self.clone();
        Rc::new(move |_ms| {
            clock.spent.set(0);
            let clock = clock.clone();
            Box::pin(std::future::poll_fn(move |_| {
                if clock.spent.get() > clock.allowance.get() {
                    std::task::Poll::Ready(())
                } else {
                    std::task::Poll::Pending
                }
            }))
        })
    }
}

// The recording peer behind the clock. A request is recorded when it is SENT,
// so the one in flight at the deadline is in the list.
#[derive(Clone)]
struct MeteredRemote {
    inner: RecordingRemote,
    clock: RequestClock,
}

impl GASPRemoteFactory for MeteredRemote {
    fn create_remote(&self, _peer_url: &str, topic: &str) -> Box<dyn GASPRemote> {
        assert_eq!(topic, TOPIC);
        Box::new(self.clone())
    }
}

#[async_trait(?Send)]
impl GASPRemote for MeteredRemote {
    async fn get_initial_response(
        &self,
        request: &GASPInitialRequest,
    ) -> Result<GASPInitialResponse, GASPError> {
        let answer = self.inner.get_initial_response(request).await;
        self.clock.spend().await;
        answer
    }

    async fn get_initial_reply(
        &self,
        response: &GASPInitialResponse,
    ) -> Result<GASPInitialReply, GASPError> {
        self.inner.get_initial_reply(response).await
    }

    async fn request_node(
        &self,
        graph_id: &str,
        txid: &str,
        output_index: u32,
        metadata: bool,
    ) -> Result<GASPNode, GASPError> {
        let answer = self
            .inner
            .request_node(graph_id, txid, output_index, metadata)
            .await;
        self.clock.spend().await;
        answer
    }

    async fn submit_node(&self, node: &GASPNode) -> Result<Option<GASPNodeResponse>, GASPError> {
        self.inner.submit_node(node).await
    }
}

fn plain_output() -> TransactionOutput {
    TransactionOutput::new(
        1000,
        LockingScript::from_hex("76a914000000000000000000000000000000000000000088ac").unwrap(),
    )
}

// A proven head chain (output 0 is the head, spent by the next link) whose
// links listed in `records` also carry a RECORD at output 1 that nobody
// spends. An honest peer lists every record and the tip's head as UTXOs, so
// the chain reaches the syncing node as several graphs, oldest first.
fn recorded_chain(length: usize, records: &[usize]) -> Vec<GASPNode> {
    let mut nodes = Vec::new();
    let mut previous = None;
    for height in 0..length {
        let mut tx = Transaction::new();
        if let Some(txid) = previous {
            tx.inputs.push(TransactionInput::new(txid, 0));
        }
        tx.outputs.push(plain_output());
        if records.contains(&height) {
            tx.outputs.push(plain_output());
        }
        let txid = tx.id();
        nodes.push(node_of(
            &tx,
            0,
            Some(honest_proof(&txid, 100 + height as u32)),
        ));
        previous = Some(txid);
    }
    nodes
}

// The peer's UTXO list, in score order: (index into `nodes`, output index).
fn listing(nodes: &[GASPNode], utxos: &[(usize, u32)]) -> RecordingRemote {
    let mut remote = RecordingRemote::new(nodes, &[]);
    remote.utxos = utxos
        .iter()
        .enumerate()
        .map(|(score, &(i, output_index))| GASPOutput {
            txid: node_txid(&nodes[i]),
            output_index,
            score: (score + 1) as f64,
        })
        .collect();
    remote
}

// Admits every output of a genesis, and of a transaction whose input 0 spends
// a HEAD (output 0) that is a previous coin; names input 0 as history. A
// transaction that spends a record does not extend the head.
struct RecordedHeadManager(Rc<RefCell<HeadState>>);

#[async_trait(?Send)]
impl TopicManager for RecordedHeadManager {
    async fn identify_admissible_outputs(
        &self,
        tx: &Transaction,
        previous_coins: &[u8],
        _off_chain_values: Option<&[u8]>,
        mode: SubmitMode,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        let extends_head = previous_coins
            .chunks_exact(4)
            .any(|index| u32::from_le_bytes(index.try_into().unwrap()) == 0)
            && tx.inputs[0].source_output_index == 0;
        if !tx.inputs.is_empty() && !extends_head {
            return Ok(AdmittanceInstructions::default());
        }
        if mode == SubmitMode::HistoricalTxNoSpv {
            self.0.borrow_mut().admitted.push(tx.id());
        }
        Ok(AdmittanceInstructions {
            outputs_to_admit: (0..tx.outputs.len() as u32).collect(),
            ..Default::default()
        })
    }

    async fn identify_needed_inputs(
        &self,
        beef: &[u8],
        _off_chain_values: Option<&[u8]>,
    ) -> Result<Vec<Outpoint>, TopicManagerError> {
        let tx = Transaction::from_beef(beef, None).unwrap();
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

// One engine that syncs the same peer tick after tick under a request budget.
struct Budgeted {
    engine: Engine,
    store: Rc<MemoryStorage>,
    requests: Requests,
    clock: RequestClock,
}

impl Budgeted {
    fn new(remote: RecordingRemote, manager: Box<dyn TopicManager>, allowance: u64) -> Self {
        let store = Rc::new(MemoryStorage::new());
        let requests = remote.requests.clone();
        let clock = RequestClock::allowing(allowance);
        let mut engine = Engine::new(
            HashMap::from([(TOPIC.to_string(), manager)]),
            HashMap::new(),
            Box::new(store.clone()),
            None,
            EngineConfig {
                sync_configuration: HashMap::from([(
                    TOPIC.to_string(),
                    SyncTarget::Peers(vec![PEER.to_string()]),
                )]),
                ..Default::default()
            },
        );
        engine.set_gasp_remote_factory(Box::new(MeteredRemote {
            inner: remote,
            clock: clock.clone(),
        }));
        engine.set_peer_sync_budget(clock.budget(), 1);
        Self {
            engine,
            store,
            requests,
            clock,
        }
    }

    // One tick: the topic's result and the node requests this tick SENT.
    async fn tick(&self) -> (bsv_overlay_engine::engine::TopicSyncResult, Vec<String>) {
        self.requests.borrow_mut().clear();
        let result = self.engine.start_gasp_sync().await.unwrap();
        let sent = self.requests.borrow().iter().map(|r| r.0.clone()).collect();
        (result.topics_synced[TOPIC].clone(), sent)
    }

    async fn cursor(&self) -> u64 {
        self.store.get_last_interaction(PEER, TOPIC).await.unwrap()
    }

    async fn failures(&self) -> u64 {
        self.store
            .get_peer_sync_health(PEER, TOPIC)
            .await
            .unwrap()
            .consecutive_failures
    }
}

fn moved(from: u64, to: u64) -> Vec<bsv_overlay_engine::engine::CursorMove> {
    vec![bsv_overlay_engine::engine::CursorMove {
        peer: PEER.to_string(),
        from,
        to,
    }]
}

fn txids(nodes: &[GASPNode], indices: &[usize]) -> Vec<String> {
    indices.iter().map(|&i| node_txid(&nodes[i])).collect()
}

// PINS A, B and E. Nine head transactions with a record at heights 2 and 5:
// three graphs of three requests each (ten requests to sync the chain, with
// the list). The tick may spend FIVE. On the base the deadline dropped every
// tick whole and nothing was ever admitted.
#[tokio::test]
async fn i552_a_b_e_a_chain_deeper_than_the_budget_bootstraps_over_three_ticks() {
    let (_logs, _guard) = capture_logs();
    let nodes = recorded_chain(9, &[2, 5]);
    let state = Rc::new(RefCell::new(HeadState::default()));
    let node = Budgeted::new(
        listing(&nodes, &[(2, 1), (5, 1), (8, 0)]),
        Box::new(RecordedHeadManager(state.clone())),
        5,
    );

    // TICK 1: the list, the first graph (2, 1, 0), then the second graph is
    // dropped with one node fetched and the next request in flight.
    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&nodes, &[2, 1, 0, 5, 4]));
    assert_eq!(
        state.borrow().admitted,
        txids(&nodes, &[0, 1, 2]),
        "PIN A: the finalized graph survives the deadline, oldest first"
    );
    assert_eq!(
        utxo_txids(&node.store).await.len(),
        2,
        "PIN B: head 2 and record 2, nothing of the dropped graph (5, 4)"
    );
    // PIN E: the counts of a dropped tick.
    assert_eq!(
        (topic.finalized_graphs, topic.deadline_dropped_graphs),
        (1, 1)
    );
    assert_eq!(topic.cursor_moves, moved(0, 1));
    assert_eq!(topic.errors.len(), 1, "the deadline is still an error");
    assert_eq!(
        node.cursor().await,
        1,
        "past the completed UTXO, not the one in flight"
    );
    assert_eq!(
        node.failures().await,
        0,
        "a tick with progress is not a failed attempt"
    );

    // TICK 2 resumes from the admitted frontier: the first record is known
    // (not asked for), and the second graph's walk stops at head 2, which the
    // storage holds. Nodes 0, 1, 2 are never requested again.
    let (topic, sent) = node.tick().await;
    assert_eq!(
        sent,
        txids(&nodes, &[5, 4, 3, 8, 7]),
        "PIN B: no admitted ancestor is fetched again; 5 and 4 are (the dropped graph)"
    );
    assert_eq!(state.borrow().admitted, txids(&nodes, &[0, 1, 2, 3, 4, 5]));
    assert_eq!(
        (topic.finalized_graphs, topic.deadline_dropped_graphs),
        (1, 1)
    );
    assert_eq!(topic.cursor_moves, moved(1, 2));
    assert_eq!(node.cursor().await, 2);

    // TICK 3 completes: the third graph, then the page that does not advance.
    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&nodes, &[8, 7, 6]));
    assert_eq!(
        state.borrow().admitted,
        txids(&nodes, &(0..9).collect::<Vec<_>>()),
        "PIN A: the whole chain, oldest first, each transaction once"
    );
    assert_eq!(
        (topic.finalized_graphs, topic.deadline_dropped_graphs),
        (1, 0)
    );
    assert_eq!(topic.cursor_moves, moved(2, 3));
    assert!(topic.errors.is_empty());
    assert_eq!(node.cursor().await, 3);
    // Head 8 and the records of 2 and 5.
    let mut expected = txids(&nodes, &[2, 5, 8]);
    expected.sort();
    assert_eq!(utxo_txids(&node.store).await, expected);

    // TICK 4: nothing left to do, nothing asked for, nothing moves.
    let (topic, sent) = node.tick().await;
    assert!(sent.is_empty());
    assert_eq!(
        (topic.finalized_graphs, topic.deadline_dropped_graphs),
        (0, 0)
    );
    assert!(topic.cursor_moves.is_empty() && topic.errors.is_empty());
    assert_eq!(state.borrow().admitted.len(), 9);
    println!(
        "#552 PINS A/B/E: 9 transactions, budget 5 requests: admitted 3, 6, 9 over 3 ticks; \
         cursor 0->1->2->3; requests 5+5+3=13 against 9 unbudgeted (2 nodes fetched twice per dropped graph)"
    );
}

// PIN C. With NO budget the engine runs as it did: the E1 head chain and the
// D8 decoy chain make the same requests, leave the same cursor and admit the
// byte-for-byte set frozen on 39081dd (`I551_C_*_BASE_DIGEST`); the managers
// that name nothing walk as the manager-less adapter does and admit nothing.
// A budget that is never reached changes none of it.
#[tokio::test]
async fn i552_c_without_a_budget_nothing_changes_and_an_unreached_budget_changes_nothing() {
    let (_logs, _guard) = capture_logs();
    let decoy = decoy_outpoint(0xd0);
    let head_nodes = chain(5);
    let decoy_nodes = decoy_chain(5, &[(4, decoy.clone())]);
    let shapes: [(&str, &[GASPNode], &str, u64); 2] = [
        ("head", &head_nodes, I551_C_HEAD_BASE_DIGEST, 0),
        ("decoy", &decoy_nodes, I551_C_DECOY_BASE_DIGEST, 1),
    ];
    for (name, nodes, digest, pruned) in shapes {
        let manager = |state: &Rc<RefCell<HeadState>>| -> Box<dyn TopicManager> {
            if name == "head" {
                Box::new(HeadChainManager(state.clone()))
            } else {
                Box::new(DecoyHeadManager(state.clone()))
            }
        };
        let state = Rc::new(RefCell::new(HeadState::default()));
        let plain = engine_sync(
            RecordingRemote::new(nodes, &[4]),
            manager(&state),
            Rc::new(MemoryStorage::new()),
            None,
        )
        .await;
        let (_, walked) = split_requests(&plain.requests, &decoy);
        assert_eq!(walked, tip_to_genesis(nodes), "{name}: requests");
        assert_eq!(plain.requests.len() as u64, 5 + pruned, "{name}");
        assert_eq!(
            plain.digest, digest,
            "{name}: the admitted set, byte for byte"
        );
        assert_eq!(
            state.borrow().admitted,
            nodes.iter().map(node_txid).collect::<Vec<_>>()
        );
        let topic = &plain.result.topics_synced[TOPIC];
        assert_eq!(
            plain.store.get_last_interaction(PEER, TOPIC).await.unwrap(),
            1
        );
        assert_eq!(topic.cursor_moves, moved(0, 1), "{name}");
        assert_eq!(
            (
                topic.finalized_graphs,
                topic.deadline_dropped_graphs,
                topic.pruned_inputs
            ),
            (1, 0, pruned),
            "{name}"
        );

        let state = Rc::new(RefCell::new(HeadState::default()));
        let budgeted = Budgeted::new(RecordingRemote::new(nodes, &[4]), manager(&state), u64::MAX);
        let (topic, _) = budgeted.tick().await;
        // Where the decoy falls among its parent's requests is a map order.
        let (decoys, walked) = split_requests(&budgeted.requests.borrow(), &decoy);
        assert_eq!(walked, tip_to_genesis(nodes), "{name}: budgeted requests");
        assert_eq!(decoys as u64, pruned, "{name}");
        assert_eq!(admitted_digest(&budgeted.store).await, digest, "{name}");
        assert_eq!(budgeted.cursor().await, 1, "{name}");
        assert_eq!(topic.cursor_moves, moved(0, 1), "{name}");
        assert_eq!(
            (topic.finalized_graphs, topic.deadline_dropped_graphs),
            (1, 0)
        );
        assert!(topic.errors.is_empty());
    }

    let mut walks = 0;
    for unproven_from in [3, 1] {
        let nodes = scripted_chain(3, "51", unproven_from);
        let (baseline, _, _) = synchronize(&nodes, &[2], None, &MemoryStorage::new(), None).await;
        for index in 0..workspace_managers().len() {
            let mut managers = workspace_managers();
            let (name, manager) = managers.swap_remove(index);
            let plain = engine_sync(
                RecordingRemote::new(&nodes, &[2]),
                manager,
                Rc::new(MemoryStorage::new()),
                None,
            )
            .await;
            assert_eq!(plain.requests, baseline, "{name}: requests");
            assert!(utxo_txids(&plain.store).await.is_empty(), "{name}");
            assert_eq!(
                plain.store.get_last_interaction(PEER, TOPIC).await.unwrap(),
                1
            );
            let topic = &plain.result.topics_synced[TOPIC];
            assert_eq!(
                (
                    topic.discarded_graphs,
                    topic.finalized_graphs,
                    topic.deadline_dropped_graphs
                ),
                (1, 0, 0),
                "{name}"
            );

            let mut managers = workspace_managers();
            let (_, manager) = managers.swap_remove(index);
            let budgeted = Budgeted::new(RecordingRemote::new(&nodes, &[2]), manager, u64::MAX);
            let (topic, _) = budgeted.tick().await;
            assert_eq!(*budgeted.requests.borrow(), baseline, "{name}: budgeted");
            assert!(utxo_txids(&budgeted.store).await.is_empty(), "{name}");
            assert_eq!(budgeted.cursor().await, 1, "{name}");
            assert_eq!((topic.discarded_graphs, topic.finalized_graphs), (1, 0));
            walks += 2;
        }
    }
    println!(
        "#552 PIN C: head and decoy digests equal the base's with and without a budget; \
         {walks} walks of managers naming nothing identical"
    );
}

// PIN D. The anchor check runs for each graph as it finalizes. The first UTXO
// the peer lists is X, which spends the RECORD of head 2: its graph (X, 2, 1,
// 0) replays three good heads and then a root the manager refuses. It is
// discarded whole BEFORE the tip's graph is asked for: the three good heads
// behind it are not admitted through it.
#[tokio::test]
async fn i552_d_a_refused_ancestor_graph_admits_nothing_under_the_incremental_submit() {
    let (_logs, _guard) = capture_logs();
    let mut nodes = recorded_chain(6, &[2]);
    let mut x = Transaction::new();
    x.inputs
        .push(TransactionInput::new(node_txid(&nodes[2]), 1));
    x.outputs.push(plain_output());
    nodes.push(node_of(&x, 0, Some(honest_proof(&x.id(), 200))));
    let state = Rc::new(RefCell::new(HeadState::default()));
    let node = Budgeted::new(
        listing(&nodes, &[(6, 0), (5, 0)]),
        Box::new(RecordedHeadManager(state.clone())),
        5,
    );

    let (topic, sent) = node.tick().await;
    assert_eq!(
        sent,
        txids(&nodes, &[6, 2, 1, 0, 5]),
        "X's graph is walked and judged before the tip is asked for"
    );
    assert!(
        state.borrow().admitted.is_empty(),
        "nothing of it is admitted"
    );
    assert!(utxo_txids(&node.store).await.is_empty());
    assert_eq!(
        (
            topic.discarded_graphs,
            topic.finalized_graphs,
            topic.deadline_dropped_graphs
        ),
        (1, 0, 1)
    );
    assert_eq!(
        topic.cursor_moves,
        moved(0, 1),
        "a refused graph is completed work, as in the reference"
    );

    // With room, the next tick admits the six heads through the TIP's graph.
    // X is served again at the cursor's own score and refused again.
    node.clock.allowance.set(u64::MAX);
    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&nodes, &[6, 2, 1, 0, 5, 4, 3, 2, 1, 0]));
    assert_eq!(
        state.borrow().admitted,
        txids(&nodes, &[0, 1, 2, 3, 4, 5]),
        "X is never admitted"
    );
    assert_eq!((topic.discarded_graphs, topic.finalized_graphs), (1, 1));
    assert_eq!(topic.cursor_moves, moved(1, 2));
    println!("#552 PIN D: refused graph of 4 discarded before the tip, 0 admitted; then 6 admitted, X never");
}

// PIN F, the limit that remains. ONE graph whose own walk outlasts the budget
// (a head chain with no record: the tip is the only UTXO) is dropped whole on
// every tick: nothing is admitted, the cursor does not move, each tick is a
// failed attempt and it is counted. Nothing bounds a graph (parity), and
// nothing of a dropped graph may be admitted, so only a larger budget ends it.
#[tokio::test]
async fn i552_f_one_graph_deeper_than_the_budget_is_dropped_whole_every_tick() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(8);
    let state = Rc::new(RefCell::new(HeadState::default()));
    let node = Budgeted::new(
        RecordingRemote::new(&nodes, &[7]),
        Box::new(HeadChainManager(state.clone())),
        5,
    );
    for tick in 1..=3u64 {
        let (topic, sent) = node.tick().await;
        assert_eq!(sent, txids(&nodes, &[7, 6, 5, 4, 3]), "the same five again");
        assert_eq!(
            (topic.finalized_graphs, topic.deadline_dropped_graphs),
            (0, 1)
        );
        assert!(topic.cursor_moves.is_empty());
        assert!(state.borrow().admitted.is_empty());
        assert!(utxo_txids(&node.store).await.is_empty());
        assert_eq!(node.cursor().await, 0);
        assert_eq!(node.failures().await, tick, "no progress: a failed attempt");
    }
    // The list, eight nodes, and the page that does not advance.
    node.clock.allowance.set(10);
    let (topic, _) = node.tick().await;
    assert_eq!(state.borrow().admitted.len(), 8);
    assert_eq!(
        (topic.finalized_graphs, topic.deadline_dropped_graphs),
        (1, 0)
    );
    assert!(topic.errors.is_empty());
    assert_eq!(node.failures().await, 0);
    println!("#552 PIN F: one graph of 8 under a budget of 5: 3 ticks, 15 requests, 0 admitted; budget 10 admits 8");
}
