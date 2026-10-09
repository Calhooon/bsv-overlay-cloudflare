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
    Beef, MerklePath, MerklePathLeaf, Transaction, TransactionInput, TransactionOutput,
};

const TOPIC: &str = "tm_head_chain";

/// A genesis transaction's one input: a coinbase's null outpoint. A
/// transaction with no input is not a transaction (bsv-rs 0.4.1 `NoInputs`,
/// NL-8 W6), so the first transaction of a chain spends this.
fn coinbase_input() -> TransactionInput {
    genesis_input(0)
}

/// The input of the `n`th genesis of one graph: an outpoint of the all-zero
/// txid, which is no coin, at an index of its own, so two geneses of one
/// graph do not spend one outpoint.
fn genesis_input(n: u32) -> TransactionInput {
    let mut input = TransactionInput::new("00".repeat(32), u32::MAX - n);
    input.unlocking_script = Some(UnlockingScript::from_binary(&[0x51]).unwrap());
    input
}

/// Whether an input spends a coin: an outpoint of the all-zero txid does
/// not, and no manager asks a peer for it.
fn spends_a_coin(input: &TransactionInput) -> bool {
    input.get_source_txid().ok() != Some("00".repeat(32))
}

/// A chain's first transaction: it spends no coin.
fn is_genesis(tx: &Transaction) -> bool {
    !tx.inputs.iter().any(spends_a_coin)
}

fn chain(length: usize) -> Vec<GASPNode> {
    let mut nodes = Vec::new();
    let mut previous = None;
    for height in 0..length {
        let mut tx = Transaction::new();
        if let Some(txid) = previous {
            tx.inputs.push(TransactionInput::new(txid, 0));
        } else {
            tx.inputs.push(coinbase_input());
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
    fn reads_off_chain_values(&self) -> bool {
        false
    }

    async fn identify_admissible_outputs(
        &self,
        tx: &Transaction,
        previous_coins: &[u8],
        _off_chain_values: Option<&[u8]>,
        mode: SubmitMode,
        _context: &TopicAdmittanceContext,
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
        if !is_genesis(tx) && !extends_head {
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
        // The GASP walk asks it of a proven node; the door's predecessor
        // question asks it of an unproven subject or body too (the E1D lens
        // fold, H1: a dry run asks only of a named output).
        let tx = Transaction::from_beef(beef, None).unwrap();
        Ok(tx
            .inputs
            .first()
            .filter(|i| spends_a_coin(i))
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
    fn reads_off_chain_values(&self) -> bool {
        false
    }

    async fn identify_admissible_outputs(
        &self,
        _tx: &Transaction,
        _previous_coins: &[u8],
        _off_chain_values: Option<&[u8]>,
        _mode: SubmitMode,
        _context: &TopicAdmittanceContext,
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
    fn reads_off_chain_values(&self) -> bool {
        false
    }

    async fn identify_admissible_outputs(
        &self,
        tx: &Transaction,
        previous_coins: &[u8],
        off_chain_values: Option<&[u8]>,
        mode: SubmitMode,
        _context: &TopicAdmittanceContext,
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
    // Nodes 2, 1 and 0: the genesis spends an outpoint of the all-zero txid,
    // an input to name, so its manager is asked too (a node with no input to
    // name is not; no transaction has none since bsv-rs 0.4.1, NL-8 W6).
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
        passed.merkle_path.as_ref().unwrap().to_hex(),
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

// D13's dry run, with lane E1D's delta (bsv-low #575): an admitted proven
// node is ALSO asked for its named inputs (the reference is not), so a
// predecessor whose landing is unknown is walked first. Here the manager's
// naming fails: the node is cut off as in the not-admitted branch, nothing
// is requested, and the dry run itself is the reference's call.
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
        assert_eq!(manager.needed.borrow().len(), 1, "E1D: the names are asked");
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
        } else {
            tx.inputs.push(coinbase_input());
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
    fn reads_off_chain_values(&self) -> bool {
        false
    }

    async fn identify_admissible_outputs(
        &self,
        tx: &Transaction,
        previous_coins: &[u8],
        _off_chain_values: Option<&[u8]>,
        mode: SubmitMode,
        _context: &TopicAdmittanceContext,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        // The same rule in every mode (bsv-low #551): the anchor replay runs
        // it under `historical-tx` with the coins of its own set.
        let extends_head = previous_coins.chunks_exact(4).any(|index| {
            let index = u32::from_le_bytes(index.try_into().unwrap()) as usize;
            tx.inputs.get(index).is_some_and(witness_shaped)
        });
        if !is_genesis(tx) && !extends_head {
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
        } else {
            tx.inputs.push(coinbase_input());
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
    fn reads_off_chain_values(&self) -> bool {
        false
    }

    async fn identify_admissible_outputs(
        &self,
        tx: &Transaction,
        _previous_coins: &[u8],
        _off_chain_values: Option<&[u8]>,
        mode: SubmitMode,
        _context: &TopicAdmittanceContext,
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
// Re-frozen by NL-6e (bsv-rs 0.4.2): each genesis now spends an outpoint of
// the all-zero txid, a transaction with no input being refused since 0.4.1
// (NL-8 W6), so every txid moved. These digests are this test file's on
// main 1119225 (bsv-rs 0.4.0) and on the bump alike, the sets of 39081dd
// having been equal to main's on the old fixtures.
const I551_C_HEAD_BASE_DIGEST: &str =
    "6569e2d2015430cf0dad8b0ce2a76834be24af32baba0d3064bff1ae5cf9fb0c";
const I551_C_DECOY_BASE_DIGEST: &str =
    "927cfb8bfdf6c011c022eb91e048fbcfdab2c0e00595aee18b038605bbbc7b9e";

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
// Re-frozen by NL-6e as PIN C was (the genesis spends an outpoint of the
// all-zero txid): the same digest on main 1119225 (bsv-rs 0.4.0) and on the
// bump.
const I551_E_BASE_DIGEST: &str = "587851a68a649fa096eef50dc8a4b20a8a3b9f4a82213e91f931ae088c91711f";

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
    fn reads_off_chain_values(&self) -> bool {
        false
    }

    async fn identify_admissible_outputs(
        &self,
        tx: &Transaction,
        previous_coins: &[u8],
        _off_chain_values: Option<&[u8]>,
        mode: SubmitMode,
        _context: &TopicAdmittanceContext,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        if mode == SubmitMode::HistoricalTx && !previous_coins.is_empty() {
            self.replay
                .borrow_mut()
                .push((tx.id(), previous_coins.to_vec()));
        }
        if !is_genesis(tx) && previous_coins.is_empty() {
            return Ok(AdmittanceInstructions::default());
        }
        if mode == SubmitMode::HistoricalTxNoSpv {
            self.state.borrow_mut().admitted.push(tx.id());
        }
        Ok(AdmittanceInstructions {
            outputs_to_admit: vec![0],
            coins_to_retain: if self.retain && !is_genesis(tx) {
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
            .filter(|i| spends_a_coin(i))
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
    a.inputs.push(coinbase_input());
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
    x.inputs.push(coinbase_input());
    x.outputs.push(coin(1000));
    let mut w = Transaction::new();
    w.inputs.push(genesis_input(1));
    w.outputs.push(coin(999));
    let mut forged = Transaction::new();
    forged.inputs.push(genesis_input(2));
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
        } else {
            tx.inputs.push(coinbase_input());
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
    fn reads_off_chain_values(&self) -> bool {
        false
    }

    async fn identify_admissible_outputs(
        &self,
        tx: &Transaction,
        previous_coins: &[u8],
        _off_chain_values: Option<&[u8]>,
        mode: SubmitMode,
        _context: &TopicAdmittanceContext,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        let extends_head = previous_coins
            .chunks_exact(4)
            .any(|index| u32::from_le_bytes(index.try_into().unwrap()) == 0)
            && tx.inputs[0].source_output_index == 0;
        if !is_genesis(tx) && !extends_head {
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
            .filter(|i| spends_a_coin(i))
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
        Self::over(
            remote,
            manager,
            RequestClock::allowing(allowance),
            store.clone(),
            Box::new(store),
            true,
        )
    }

    // The same node over `storage`, a wrapper of `store` (the lens fold's
    // `ScriptedStore`), with the per-peer budget of `clock` or with none.
    fn over(
        remote: RecordingRemote,
        manager: Box<dyn TopicManager>,
        clock: RequestClock,
        store: Rc<MemoryStorage>,
        storage: Box<dyn Storage>,
        budgeted: bool,
    ) -> Self {
        let requests = remote.requests.clone();
        let mut engine = Engine::new(
            HashMap::from([(TOPIC.to_string(), manager)]),
            HashMap::new(),
            storage,
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
        if budgeted {
            engine.set_peer_sync_budget(clock.budget(), 1);
        }
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
            .get_peer_sync_health(&peer_origin(PEER), TOPIC)
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

// ============================================================================
// bsv-low #530 (E1, the dry-run option): `identify_admissible_outputs` carries
// the reference's fifth argument. The GASP walk asks every proven node of the
// peer what the manager WOULD admit, and the #551 anchor check replays the
// peer's whole graph the same way, both before anything is admitted: both
// pass `dry_run: true`, the finalize submit passes `false`. A manager that
// writes on admission reads the flag and writes nothing on a dry run.
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
struct ContextCall {
    txid: String,
    mode: SubmitMode,
    dry_run: bool,
}

#[derive(Default)]
struct HeadLedger {
    // Every admission call, in order, with the context it carried.
    calls: Vec<ContextCall>,
    // The manager's own durable state: the head, and every advance of it.
    head: Option<String>,
    advances: Vec<ContextCall>,
}

// A STATEFUL manager over the rule of `M`: it records every call with its
// context, and when the rule admits it ADVANCES ITS HEAD, unless the call is
// a dry run.
struct StatefulHead<M> {
    rule: M,
    ledger: Rc<RefCell<HeadLedger>>,
}

#[async_trait(?Send)]
impl<M: TopicManager> TopicManager for StatefulHead<M> {
    fn reads_off_chain_values(&self) -> bool {
        self.rule.reads_off_chain_values()
    }

    async fn identify_admissible_outputs(
        &self,
        tx: &Transaction,
        previous_coins: &[u8],
        off_chain_values: Option<&[u8]>,
        mode: SubmitMode,
        context: &TopicAdmittanceContext,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        let call = ContextCall {
            txid: tx.id(),
            mode,
            dry_run: context.dry_run,
        };
        self.ledger.borrow_mut().calls.push(call.clone());
        let admittance = self
            .rule
            .identify_admissible_outputs(tx, previous_coins, off_chain_values, mode, context)
            .await?;
        if !admittance.outputs_to_admit.is_empty() && !context.dry_run {
            let mut ledger = self.ledger.borrow_mut();
            ledger.head = Some(call.txid.clone());
            ledger.advances.push(call);
        }
        Ok(admittance)
    }

    async fn identify_needed_inputs(
        &self,
        beef: &[u8],
        off_chain_values: Option<&[u8]>,
    ) -> Result<Vec<Outpoint>, TopicManagerError> {
        self.rule
            .identify_needed_inputs(beef, off_chain_values)
            .await
    }

    async fn get_documentation(&self) -> String {
        String::new()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata::default()
    }
}

// What one sync of a chain must ask, in order: the walk's dry run of every
// proven node (tip to genesis), the anchor replay's dry run of every ordered
// BEEF (genesis to tip), then one real admission per finalize submit.
fn expected_calls(nodes: &[GASPNode]) -> (Vec<ContextCall>, Vec<ContextCall>) {
    let call = |node: &GASPNode, mode, dry_run| ContextCall {
        txid: node_txid(node),
        mode,
        dry_run,
    };
    let walk = nodes
        .iter()
        .rev()
        .map(|node| call(node, SubmitMode::HistoricalTx, true));
    let replay = nodes
        .iter()
        .map(|node| call(node, SubmitMode::HistoricalTx, true));
    let submits: Vec<_> = nodes
        .iter()
        .map(|node| call(node, SubmitMode::HistoricalTxNoSpv, false))
        .collect();
    let all = walk.chain(replay).chain(submits.clone()).collect();
    (all, submits)
}

// PIN A. The E1 head chain and the D8 decoy chain through the engine, under a
// tracker: every walk and replay call carries `dry_run: true`, every finalize
// submit `false`, and the stateful manager advances exactly once per submit
// and never on a dry run. What is admitted is byte for byte what #551 pinned.
#[tokio::test]
async fn dryrun_a_the_walk_and_the_replay_are_dry_runs_and_only_a_submit_advances_the_head() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(5);
    let ledger = Rc::new(RefCell::new(HeadLedger::default()));
    let head = engine_sync(
        RecordingRemote::new(&nodes, &[4]),
        Box::new(StatefulHead {
            rule: HeadChainManager(Rc::new(RefCell::new(HeadState::default()))),
            ledger: ledger.clone(),
        }),
        Rc::new(MemoryStorage::new()),
        Some(Box::new(KnownRoots::of(&nodes))),
    )
    .await;
    let (calls, submits) = expected_calls(&nodes);
    assert_eq!(ledger.borrow().calls, calls, "5 walk, 5 replay, 5 submits");
    assert_eq!(ledger.borrow().advances, submits, "one advance per submit");
    assert_eq!(ledger.borrow().head, Some(node_txid(&nodes[4])));
    assert_eq!(head.digest, I551_C_HEAD_BASE_DIGEST);

    let decoy = decoy_outpoint(0xd0);
    let nodes = decoy_chain(5, &[(4, decoy)]);
    let ledger = Rc::new(RefCell::new(HeadLedger::default()));
    let decoyed = engine_sync(
        RecordingRemote::new(&nodes, &[4]),
        Box::new(StatefulHead {
            rule: DecoyHeadManager::new(),
            ledger: ledger.clone(),
        }),
        Rc::new(MemoryStorage::new()),
        Some(Box::new(KnownRoots::of(&nodes))),
    )
    .await;
    let (calls, submits) = expected_calls(&nodes);
    assert_eq!(ledger.borrow().calls, calls, "5 walk, 5 replay, 5 submits");
    assert_eq!(ledger.borrow().advances, submits, "one advance per submit");
    assert_eq!(ledger.borrow().head, Some(node_txid(&nodes[4])));
    assert_eq!(decoyed.digest, I551_C_DECOY_BASE_DIGEST);
    println!("dry-run PIN A: head and decoy chain: 10 dry runs, 5 submits, 5 advances each");
}

// PIN B. The case of zanaadu-v2 #314: the peer serves the tip with a
// fabricated BUMP, so the anchor check discards the graph and nothing is
// submitted. The walk still asked the manager about all five nodes on the
// peer's word: every one of those calls is a dry run and the head never moves.
#[tokio::test]
async fn dryrun_b_a_graph_the_anchor_check_refuses_never_moves_the_head() {
    let (_logs, _guard) = capture_logs();
    let mut nodes = chain(5);
    let tracker = KnownRoots::of(&nodes);
    nodes[4].proof = Some(fabricated_proof(&node_txid(&nodes[4]), 104));
    let ledger = Rc::new(RefCell::new(HeadLedger::default()));
    let synced = engine_sync(
        RecordingRemote::new(&nodes, &[4]),
        Box::new(StatefulHead {
            rule: HeadChainManager(Rc::new(RefCell::new(HeadState::default()))),
            ledger: ledger.clone(),
        }),
        Rc::new(MemoryStorage::new()),
        Some(Box::new(tracker)),
    )
    .await;
    assert_eq!(synced.result.topics_synced[TOPIC].discarded_graphs, 1);
    assert!(utxo_txids(&synced.store).await.is_empty());
    let ledger = ledger.borrow();
    assert_eq!(ledger.calls.len(), 5, "the walk asked about every node");
    assert!(ledger.calls.iter().all(|call| call.dry_run));
    assert!(ledger.advances.is_empty(), "{:?}", ledger.advances);
    assert_eq!(ledger.head, None);
    println!("dry-run PIN B: 5 dry runs on a fabricated BUMP, 0 advances, 0 admitted");
}

// PIN C. The context itself: a real admission by default, and the one
// constant of a dry run.
#[test]
fn dryrun_c_the_default_is_a_real_admission() {
    let dry_runs = [
        TopicAdmittanceContext::default(),
        TopicAdmittanceContext::DRY_RUN,
    ]
    .map(|o| o.dry_run);
    assert_eq!(dry_runs, [false, true]);
}

// ============================================================================
// The lens fold of 2026-10-06 (bsv-low #551, #552, #530). HIGH-1: the deadline
// never drops a finalize submit between its writes. MEDIUM-1: a finalize
// submit that did not land stops its graph and holds the cursor. MEDIUM-2: a
// manager error in the anchor replay is "not now", never a refusal. LOW-1:
// `submit_validate_only` is a dry run.
// ============================================================================

use bsv_overlay_engine::storage::{PeerSyncHealth, StorageError};

// What a `ScriptedStore` does at the ONE insert it is armed for.
enum InsertEvent {
    // The write is a round trip (every D1 call is one) and the per-peer
    // deadline falls due while it is in flight.
    DeadlineFallsDue(RequestClock),
    // The write faults (a D1 outage) until the test disarms the store.
    Faults,
    // The write never answers (a hung D1 call) until the test disarms the
    // store (bsv-low #559, DELTA-3).
    Hangs,
}

type Armed = Rc<RefCell<Option<(String, u32, InsertEvent)>>>;

// A storage call other than `insert_output` that a `ScriptedStore` can
// script (the lens fold of 2026-10-07, bsv-low #559), keyed by the txid the
// call names.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Call {
    MarkSpent,
    Delete,
    // The reads and the applied-row write the delta fold of 2026-10-07
    // scripts (bsv-low #559): `does_applied_transaction_exist`,
    // `insert_applied_transaction`, `find_output` and
    // `find_outputs_for_transaction`.
    Applied,
    RecordApplied,
    FindOutput,
    OutputsOf,
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum CallEvent {
    // The call faults once and answers from then on (a transient D1 fault).
    FaultsOnce,
    // The call does its write and THEN faults, once (D1's `delete_output` is
    // a delete and a cleanup: the second can fault after the first landed).
    LandsThenFaultsOnce,
    // The call never answers until the test clears the script.
    Hangs,
}

type Calls = Rc<RefCell<Vec<(Call, String, CallEvent)>>>;

// `MemoryStorage` with one scripted `insert_output` and any number of
// scripted `mark_utxo_as_spent` and `delete_output` calls.
// Every other call delegates.
struct ScriptedStore {
    inner: Rc<MemoryStorage>,
    armed: Armed,
    calls: Calls,
    // Every read the engine made of an applied row or of an output.
    reads: Rc<Cell<usize>>,
    // The txid of every applied row the engine read, in order.
    applied_reads: Rc<RefCell<Vec<String>>>,
}

impl ScriptedStore {
    fn armed(
        inner: &Rc<MemoryStorage>,
        txid: String,
        output_index: u32,
        event: InsertEvent,
    ) -> Self {
        Self {
            inner: inner.clone(),
            armed: Rc::new(RefCell::new(Some((txid, output_index, event)))),
            calls: Calls::default(),
            reads: Rc::default(),
            applied_reads: Rc::default(),
        }
    }

    // No insert armed: only what the test pushes on `calls`.
    fn plain(inner: &Rc<MemoryStorage>) -> Self {
        Self {
            inner: inner.clone(),
            armed: Rc::new(RefCell::new(None)),
            calls: Calls::default(),
            reads: Rc::default(),
            applied_reads: Rc::default(),
        }
    }

    // What the script says of this call: `Ok(false)` to delegate,
    // `Ok(true)` to delegate and fault after, `Err` to fault before.
    async fn scripted(&self, call: Call, txid: &str) -> Result<bool, StorageError> {
        let event = {
            let mut calls = self.calls.borrow_mut();
            let hit = calls.iter().position(|(c, t, _)| *c == call && t == txid);
            match hit {
                Some(i) if calls[i].2 == CallEvent::Hangs => Some(calls[i].2),
                Some(i) => Some(calls.remove(i).2),
                None => None,
            }
        };
        match event {
            Some(CallEvent::Hangs) => std::future::pending().await,
            Some(CallEvent::FaultsOnce) => Err(Self::overloaded(call)),
            Some(CallEvent::LandsThenFaultsOnce) => Ok(true),
            None => Ok(false),
        }
    }

    fn overloaded(call: Call) -> StorageError {
        StorageError::Database(format!("D1_ERROR: storage overloaded ({call:?})"))
    }
}

#[async_trait(?Send)]
impl Storage for ScriptedStore {
    async fn insert_output(&self, output: &Output) -> Result<(), StorageError> {
        let hangs = matches!(
            self.armed.borrow().as_ref(),
            Some((txid, output_index, InsertEvent::Hangs))
                if *txid == output.txid && *output_index == output.output_index
        );
        if hangs {
            return std::future::pending().await;
        }
        let hit = {
            let mut armed = self.armed.borrow_mut();
            let is_hit = armed.as_ref().is_some_and(|(txid, output_index, _)| {
                *txid == output.txid && *output_index == output.output_index
            });
            match (is_hit, armed.as_ref()) {
                (true, Some((_, _, InsertEvent::Faults))) => {
                    return Err(StorageError::Database(
                        "D1_ERROR: storage overloaded".into(),
                    ));
                }
                (true, _) => armed.take(),
                _ => None,
            }
        };
        if let Some((_, _, InsertEvent::DeadlineFallsDue(clock))) = hit {
            clock.spent.set(clock.allowance.get() + 1);
            tokio::task::yield_now().await;
        }
        self.inner.insert_output(output).await
    }
    async fn delete_output(
        &self,
        txid: &str,
        output_index: u32,
        topic: &str,
    ) -> Result<(), StorageError> {
        let faults_after = self.scripted(Call::Delete, txid).await?;
        self.inner.delete_output(txid, output_index, topic).await?;
        if faults_after {
            return Err(Self::overloaded(Call::Delete));
        }
        Ok(())
    }
    async fn mark_utxo_as_spent(
        &self,
        txid: &str,
        output_index: u32,
        topic: &str,
    ) -> Result<(), StorageError> {
        if self.scripted(Call::MarkSpent, txid).await? {
            unreachable!("only a delete lands and then faults");
        }
        self.inner
            .mark_utxo_as_spent(txid, output_index, topic)
            .await
    }
    async fn update_consumed_by(
        &self,
        txid: &str,
        output_index: u32,
        topic: &str,
        consumed_by: &[Outpoint],
    ) -> Result<(), StorageError> {
        self.inner
            .update_consumed_by(txid, output_index, topic, consumed_by)
            .await
    }
    async fn update_transaction_beef(&self, txid: &str, beef: &[u8]) -> Result<(), StorageError> {
        self.inner.update_transaction_beef(txid, beef).await
    }
    async fn insert_applied_transaction(
        &self,
        tx: &AppliedTransaction,
    ) -> Result<(), StorageError> {
        self.scripted(Call::RecordApplied, &tx.txid).await?;
        self.inner.insert_applied_transaction(tx).await
    }
    async fn does_applied_transaction_exist(
        &self,
        tx: &AppliedTransaction,
    ) -> Result<bool, StorageError> {
        self.reads.set(self.reads.get() + 1);
        self.applied_reads.borrow_mut().push(tx.txid.clone());
        self.scripted(Call::Applied, &tx.txid).await?;
        self.inner.does_applied_transaction_exist(tx).await
    }
    async fn delete_applied_transaction(
        &self,
        tx: &AppliedTransaction,
    ) -> Result<(), StorageError> {
        self.inner.delete_applied_transaction(tx).await
    }
    async fn find_output(
        &self,
        txid: &str,
        output_index: u32,
        topic: Option<&str>,
        spent: Option<bool>,
        include_beef: bool,
    ) -> Result<Option<Output>, StorageError> {
        self.reads.set(self.reads.get() + 1);
        self.scripted(Call::FindOutput, txid).await?;
        self.inner
            .find_output(txid, output_index, topic, spent, include_beef)
            .await
    }
    async fn find_outputs_for_transaction(
        &self,
        txid: &str,
        include_beef: bool,
    ) -> Result<Vec<Output>, StorageError> {
        self.reads.set(self.reads.get() + 1);
        self.scripted(Call::OutputsOf, txid).await?;
        self.inner
            .find_outputs_for_transaction(txid, include_beef)
            .await
    }
    async fn find_utxos_for_topic(
        &self,
        topic: &str,
        since: Option<f64>,
        limit: Option<u64>,
        include_beef: bool,
    ) -> Result<Vec<Output>, StorageError> {
        self.inner
            .find_utxos_for_topic(topic, since, limit, include_beef)
            .await
    }
    async fn update_last_interaction(
        &self,
        host: &str,
        topic: &str,
        since: u64,
    ) -> Result<(), StorageError> {
        self.inner.update_last_interaction(host, topic, since).await
    }
    async fn get_last_interaction(&self, host: &str, topic: &str) -> Result<u64, StorageError> {
        self.inner.get_last_interaction(host, topic).await
    }
    async fn record_peer_sync_outcome(
        &self,
        host: &str,
        topic: &str,
        success: bool,
    ) -> Result<(), StorageError> {
        self.inner
            .record_peer_sync_outcome(host, topic, success)
            .await
    }
    async fn get_peer_sync_health(
        &self,
        host: &str,
        topic: &str,
    ) -> Result<PeerSyncHealth, StorageError> {
        self.inner.get_peer_sync_health(host, topic).await
    }
    async fn record_peer_sync_yield(
        &self,
        host: &str,
        topic: &str,
        yielded: bool,
    ) -> Result<bsv_overlay_engine::storage::PeerYieldStreak, StorageError> {
        self.inner
            .record_peer_sync_yield(host, topic, yielded)
            .await
    }
    async fn put_deferred_graph(
        &self,
        record: &bsv_overlay_engine::gasp::DeferredGraph,
    ) -> Result<bsv_overlay_engine::gasp::DeferredGraphSave, StorageError> {
        self.inner.put_deferred_graph(record).await
    }
    async fn find_deferred_graphs(
        &self,
        host: &str,
        topic: &str,
    ) -> Result<Vec<bsv_overlay_engine::gasp::DeferredGraphKey>, StorageError> {
        self.inner.find_deferred_graphs(host, topic).await
    }
    async fn get_deferred_graph(
        &self,
        host: &str,
        topic: &str,
        outpoint: &str,
    ) -> Result<Option<bsv_overlay_engine::gasp::DeferredGraph>, StorageError> {
        self.inner.get_deferred_graph(host, topic, outpoint).await
    }
    async fn delete_deferred_graph(
        &self,
        host: &str,
        topic: &str,
        outpoint: &str,
    ) -> Result<(), StorageError> {
        self.inner
            .delete_deferred_graph(host, topic, outpoint)
            .await
    }
}

// The topic's UTXOs as sorted `(index into nodes, output index)`.
async fn held(store: &MemoryStorage, nodes: &[GASPNode]) -> Vec<(usize, u32)> {
    let mut held: Vec<_> = store
        .find_utxos_for_topic(TOPIC, None, None, false)
        .await
        .unwrap()
        .into_iter()
        .map(|o| {
            let index = nodes.iter().position(|n| node_txid(n) == o.txid).unwrap();
            (index, o.output_index)
        })
        .collect();
    held.sort_unstable();
    held
}

async fn applied(store: &MemoryStorage, node: &GASPNode) -> bool {
    store
        .does_applied_transaction_exist(&AppliedTransaction {
            txid: node_txid(node),
            topic: TOPIC.to_string(),
        })
        .await
        .unwrap()
}

// HIGH-1, the lens's recipe. Six heads with a record at height 2, the peer
// lists the record and the tip; the manager does not retain a spent head. The
// deadline falls due INSIDE a finalize submit, at the insert named by `slow`.
// On a1cf98d the race dropped the submit right there, between its writes.
struct DroppedInASubmit {
    nodes: Vec<GASPNode>,
    state: Rc<RefCell<HeadState>>,
    node: Budgeted,
    // The first tick: its result, its requests, what the store held after it.
    topic: bsv_overlay_engine::engine::TopicSyncResult,
    sent: Vec<String>,
    held: Vec<(usize, u32)>,
    applied: Vec<bool>,
    cursor: u64,
    failures: u64,
}

async fn deadline_inside_a_finalize_submit(slow: (usize, u32)) -> DroppedInASubmit {
    let nodes = recorded_chain(6, &[2]);
    let state = Rc::new(RefCell::new(HeadState::default()));
    let store = Rc::new(MemoryStorage::new());
    let clock = RequestClock::allowing(1000);
    let storage = ScriptedStore::armed(
        &store,
        node_txid(&nodes[slow.0]),
        slow.1,
        InsertEvent::DeadlineFallsDue(clock.clone()),
    );
    let node = Budgeted::over(
        listing(&nodes, &[(2, 1), (5, 0)]),
        Box::new(RecordedHeadManager(state.clone())),
        clock,
        store,
        Box::new(storage),
        true,
    );
    let (topic, sent) = node.tick().await;
    let held = held(&node.store, &nodes).await;
    println!(
        "#552 HIGH-1: deadline inside the submit of {slow:?}: held after the drop {held:?}, \
         finalized_graphs={} deadline_dropped_graphs={} cursor_moves={:?}",
        topic.finalized_graphs, topic.deadline_dropped_graphs, topic.cursor_moves
    );
    assert_eq!(topic.errors.len(), 1, "the deadline dropped the tick");
    let mut rows = Vec::new();
    for n in &nodes {
        rows.push(applied(&node.store, n).await);
    }
    let (cursor, failures) = (node.cursor().await, node.failures().await);

    // The next ticks resume (asserted by `resumed`, after the first tick's
    // own asserts).
    for _ in 0..3 {
        node.tick().await;
    }
    println!(
        "#552 HIGH-1: deadline inside the submit of {slow:?}: held three ticks later {:?}",
        self::held(&node.store, &nodes).await
    );
    DroppedInASubmit {
        nodes,
        state,
        node,
        topic,
        sent,
        held,
        applied: rows,
        cursor,
        failures,
    }
}

impl DroppedInASubmit {
    // The next ticks resumed: the chain is complete, each transaction
    // admitted once.
    async fn resumed(&self) {
        assert_eq!(
            held(&self.node.store, &self.nodes).await,
            vec![(2, 1), (5, 0)],
            "the record of 2 and head 5 are held at the end"
        );
        assert_eq!(self.node.cursor().await, 2);
        assert_eq!(
            self.state.borrow().admitted,
            txids(&self.nodes, &[0, 1, 2, 3, 4, 5]),
            "each transaction admitted once, oldest first"
        );
    }
}

#[tokio::test]
async fn fold_high1_a_deadline_inside_a_finalize_submit_leaves_the_old_head_or_the_new_never_neither(
) {
    let (_logs, _guard) = capture_logs();
    // The deadline lands between the delete of head 0 and the insert of head 1.
    let dropped = deadline_inside_a_finalize_submit((1, 0)).await;
    assert_eq!(
        dropped.held,
        vec![(1, 0)],
        "the transaction in flight was written whole (the new head) and the sync stopped there"
    );
    assert_eq!(
        dropped.applied[..3],
        [true, true, false],
        "a prefix of the graph, ancestors first, each transaction whole"
    );
    assert_eq!(dropped.sent, txids(&dropped.nodes, &[2, 1, 0]));
    assert_eq!(
        (
            dropped.topic.finalized_graphs,
            dropped.topic.deadline_dropped_graphs
        ),
        (1, 0)
    );
    assert!(
        dropped.topic.cursor_moves.is_empty() && dropped.cursor == 0,
        "the graph was not submitted whole: its UTXO is asked for again"
    );
    assert_eq!(dropped.failures, 0, "a transaction landed: progress");
    dropped.resumed().await;
}

#[tokio::test]
async fn fold_high1_a_deadline_between_two_outputs_of_one_submit_loses_neither() {
    let (_logs, _guard) = capture_logs();
    // One step later: head 2 is inserted, its record (output 1) is not yet.
    // The transaction is the LAST of its graph, so the graph completes: the
    // cursor moves past it and the sync is dropped on its next request.
    let dropped = deadline_inside_a_finalize_submit((2, 1)).await;
    assert_eq!(dropped.held, vec![(2, 0), (2, 1)]);
    assert_eq!(dropped.sent, txids(&dropped.nodes, &[2, 1, 0, 5]));
    assert_eq!(
        (
            dropped.topic.finalized_graphs,
            dropped.topic.deadline_dropped_graphs
        ),
        (1, 1)
    );
    assert_eq!(dropped.topic.cursor_moves, moved(0, 1));
    dropped.resumed().await;
}

// Fails where the wrapped rule would answer, for the transactions in
// `not_now`: "I cannot place this yet". `in_replay` picks the call: the anchor
// replay (a dry run that is shown coins) or the finalize submit.
struct NotNow<M> {
    rule: M,
    not_now: Rc<RefCell<HashSet<String>>>,
    in_replay: bool,
}

#[async_trait(?Send)]
impl<M: TopicManager> TopicManager for NotNow<M> {
    fn reads_off_chain_values(&self) -> bool {
        self.rule.reads_off_chain_values()
    }

    async fn identify_admissible_outputs(
        &self,
        tx: &Transaction,
        previous_coins: &[u8],
        off_chain_values: Option<&[u8]>,
        mode: SubmitMode,
        context: &TopicAdmittanceContext,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        let this_call = if self.in_replay {
            context.dry_run && !previous_coins.is_empty()
        } else {
            mode == SubmitMode::HistoricalTxNoSpv
        };
        if this_call && self.not_now.borrow().contains(&tx.id()) {
            return Err(TopicManagerError::Other("the head lags: not now".into()));
        }
        self.rule
            .identify_admissible_outputs(tx, previous_coins, off_chain_values, mode, context)
            .await
    }

    async fn identify_needed_inputs(
        &self,
        beef: &[u8],
        off_chain_values: Option<&[u8]>,
    ) -> Result<Vec<Outpoint>, TopicManagerError> {
        self.rule
            .identify_needed_inputs(beef, off_chain_values)
            .await
    }

    async fn get_documentation(&self) -> String {
        String::new()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata::default()
    }
}

// MEDIUM-1. Three heads, the tip listed. The finalize submit of the GENESIS
// does not land (its insert faults, or the manager fails on it).
// The sequence stops there: no child is submitted (on a1cf98d each child was
// judged without the coin its ancestor failed to leave, admitted nothing and
// was recorded as applied, a dupe forever), and the cursor stays, so the next
// tick submits the graph whole. With a budget (the hook) and without.
#[tokio::test]
async fn fold_medium1_a_finalize_submit_that_did_not_land_stops_its_graph_and_holds_the_cursor() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(3);
    let genesis = node_txid(&nodes[0]);
    for budgeted in [true, false] {
        for storage_fault in [true, false] {
            let state = Rc::new(RefCell::new(HeadState::default()));
            let store = Rc::new(MemoryStorage::new());
            let not_now = Rc::new(RefCell::new(HashSet::new()));
            let mut outage: Option<Armed> = None;
            let storage: Box<dyn Storage> = if storage_fault {
                let scripted =
                    ScriptedStore::armed(&store, genesis.clone(), 0, InsertEvent::Faults);
                outage = Some(scripted.armed.clone());
                Box::new(scripted)
            } else {
                not_now.borrow_mut().insert(genesis.clone());
                Box::new(store.clone())
            };
            let node = Budgeted::over(
                RecordingRemote::new(&nodes, &[2]),
                Box::new(NotNow {
                    rule: HeadChainManager(state.clone()),
                    not_now: not_now.clone(),
                    in_replay: false,
                }),
                RequestClock::allowing(u64::MAX - 1),
                store,
                storage,
                budgeted,
            );
            let case = format!("budgeted={budgeted} storage_fault={storage_fault}");

            let (topic, _) = node.tick().await;
            let rows = [
                applied(&node.store, &nodes[0]).await,
                applied(&node.store, &nodes[1]).await,
                applied(&node.store, &nodes[2]).await,
            ];
            println!(
                "#551 MEDIUM-1 {case}: applied rows after the faulted genesis {rows:?}, cursor {}",
                node.cursor().await
            );
            assert_eq!(
                rows,
                [false, false, false],
                "{case}: the genesis did not land and no child was submitted behind it"
            );
            assert!(held(&node.store, &nodes).await.is_empty(), "{case}");
            assert_eq!(node.cursor().await, 0, "{case}: the cursor stays");
            assert!(topic.cursor_moves.is_empty(), "{case}");
            // Handed to submit once: after the sync, or under the hook as it
            // finalized. (Before bsv-low #554 the hook's count was 2: the
            // boundary row was served again and the failed UTXO was tried a
            // second time inside the same sync.)
            assert_eq!(topic.finalized_graphs, 1, "{case}");

            // The outage ends (the manager catches up): the next tick submits
            // the graph whole.
            not_now.borrow_mut().clear();
            if let Some(armed) = outage {
                armed.borrow_mut().take();
            }
            let (topic, sent) = node.tick().await;
            assert_eq!(sent, txids(&nodes, &[2, 1, 0]), "{case}: walked again");
            assert_eq!(held(&node.store, &nodes).await, vec![(2, 0)], "{case}");
            assert_eq!(topic.cursor_moves, moved(0, 1), "{case}");
            assert_eq!(node.cursor().await, 1, "{case}");
        }
    }
}

// MEDIUM-2. Two chains, both tips listed; the manager cannot place a
// transaction of the FIRST yet and says so with an error in the anchor
// replay. That is "not now", not a refusal: the UTXO fails
// (`AnchorUnavailable`), nothing is counted as discarded, the cursor waits
// below it and the next tick admits it. On a1cf98d it was discarded as
// refused, the cursor moved past it and it was never asked for again.
#[tokio::test]
async fn fold_medium2_a_manager_error_in_the_replay_fails_the_utxo_and_the_cursor_waits() {
    let (_logs, _guard) = capture_logs();
    let mut nodes = chain(3);
    nodes.extend(recorded_chain(3, &[0]));
    let state = Rc::new(RefCell::new(HeadState::default()));
    let not_now = Rc::new(RefCell::new(HashSet::from([node_txid(&nodes[1])])));
    let store = Rc::new(MemoryStorage::new());
    let node = Budgeted::over(
        listing(&nodes, &[(2, 0), (5, 0)]),
        Box::new(NotNow {
            rule: RecordedHeadManager(state.clone()),
            not_now: not_now.clone(),
            in_replay: true,
        }),
        RequestClock::allowing(u64::MAX - 1),
        store.clone(),
        Box::new(store),
        false,
    );

    let (topic, _) = node.tick().await;
    println!(
        "#551 MEDIUM-2: a manager Err in the replay: discarded_graphs={} cursor {} held {:?}",
        topic.discarded_graphs,
        node.cursor().await,
        held(&node.store, &nodes).await
    );
    assert_eq!(
        held(&node.store, &nodes).await,
        vec![(3, 1), (5, 0)],
        "the second chain is admitted, nothing of the first"
    );
    assert_eq!(topic.discarded_graphs, 0, "an error is not a refusal");
    assert_eq!(
        node.cursor().await,
        0,
        "the cursor waits below the failed UTXO"
    );

    not_now.borrow_mut().clear();
    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&nodes, &[2, 1, 0]), "asked for again");
    assert_eq!(
        held(&node.store, &nodes).await,
        vec![(2, 0), (3, 1), (5, 0)],
        "and admitted once the manager can place it"
    );
    assert_eq!(
        topic.cursor_moves,
        moved(0, 2),
        "Ok with nothing stays the final refusal and moves the cursor: i552_d"
    );
}

// LOW-1. `submit_validate_only` asks what WOULD be admitted and writes
// nothing: the manager is called with `dry_run: true` and the stateful
// fixture does not advance, so the real submit that follows finds its head
// where it was.
#[tokio::test]
async fn fold_low1_submit_validate_only_is_a_dry_run() {
    let nodes = chain(1);
    let mut tx = Transaction::from_hex(&nodes[0].raw_tx).unwrap();
    tx.merkle_path = Some(MerklePath::from_hex(nodes[0].proof.as_ref().unwrap()).unwrap());
    let beef = TaggedBEEF::new(tx.to_beef(false).unwrap(), vec![TOPIC.to_string()]);
    let ledger = Rc::new(RefCell::new(HeadLedger::default()));
    let store = Rc::new(MemoryStorage::new());
    let engine = Engine::new(
        HashMap::from([(
            TOPIC.to_string(),
            Box::new(StatefulHead {
                rule: HeadChainManager(Rc::new(RefCell::new(HeadState::default()))),
                ledger: ledger.clone(),
            }) as Box<dyn TopicManager>,
        )]),
        HashMap::new(),
        Box::new(store.clone()),
        None,
        EngineConfig::default(),
    );

    let steak = engine
        .submit_validate_only(&beef, SubmitMode::HistoricalTx)
        .await
        .unwrap();
    assert_eq!(
        steak[TOPIC].outputs_to_admit,
        vec![0],
        "the answer of a real call"
    );
    assert_eq!(
        ledger
            .borrow()
            .calls
            .iter()
            .map(|c| c.dry_run)
            .collect::<Vec<_>>(),
        vec![true]
    );
    assert!(
        ledger.borrow().advances.is_empty(),
        "no write on a validate-only call"
    );
    assert_eq!(ledger.borrow().head, None);
    assert!(utxo_txids(&store).await.is_empty());

    engine
        .submit(&beef, SubmitMode::HistoricalTx)
        .await
        .unwrap();
    assert_eq!(
        ledger.borrow().advances.len(),
        1,
        "the submit is the admission"
    );
    assert!(!ledger.borrow().advances[0].dry_run);
    assert_eq!(utxo_txids(&store).await, vec![node_txid(&nodes[0])]);
}

// ============================================================================
// bsv-low #559: a storage fault inside `Engine::submit` loses no head. The
// new outputs are inserted BEFORE the stale coin is deleted, and the delete
// waits for a submit with no fault, so a faulted topic leaves its previous
// coins where the replay finds them. The delta lens's executed recipe
// (`docs/audit/ingest-delta-2026-10-06.md`, DELTA-1).
// ============================================================================

async fn row_exists(store: &MemoryStorage, node: &GASPNode) -> bool {
    store
        .find_output(&node_txid(node), 0, Some(TOPIC), None, false)
        .await
        .unwrap()
        .is_some()
}

async fn applied_rows(store: &MemoryStorage, nodes: &[GASPNode]) -> Vec<bool> {
    let mut rows = Vec::new();
    for n in nodes {
        rows.push(applied(store, n).await);
    }
    rows
}

// The recipe. Four heads, the tip listed, a manager that does not retain a
// spent head; the insert of head 1 FAULTS (a D1 outage that lasts the tick).
// On 1821f73 head 0 was deleted before that insert: the store held nothing,
// every later head was judged with no coin, admitted nothing and was recorded
// as applied, and no later tick placed the chain. With a budget and without.
#[tokio::test]
async fn i559_a_a_storage_fault_on_a_mid_chain_insert_keeps_the_old_head_and_the_next_tick_resumes()
{
    let (_logs, _guard) = capture_logs();
    let nodes = chain(4);
    for budgeted in [true, false] {
        let state = Rc::new(RefCell::new(HeadState::default()));
        let store = Rc::new(MemoryStorage::new());
        let scripted = ScriptedStore::armed(&store, node_txid(&nodes[1]), 0, InsertEvent::Faults);
        let outage = scripted.armed.clone();
        let node = Budgeted::over(
            RecordingRemote::new(&nodes, &[3]),
            Box::new(HeadChainManager(state.clone())),
            RequestClock::allowing(u64::MAX - 1),
            store,
            Box::new(scripted),
            budgeted,
        );
        let case = format!("budgeted={budgeted}");

        // Tick 1, the outage on.
        node.tick().await;
        let old_head = row_exists(&node.store, &nodes[0]).await;
        let rows = applied_rows(&node.store, &nodes).await;
        println!(
            "#559 {case}: after the faulted insert of head 1: old head row held {old_head}, \
             utxos {:?}, applied {rows:?}, cursor {}",
            held(&node.store, &nodes).await,
            node.cursor().await
        );
        assert!(old_head, "{case}: the old head stays held on the fault");
        assert_eq!(
            rows,
            [true, false, false, false],
            "{case}: no applied row for a head that was not placed"
        );
        assert!(!row_exists(&node.store, &nodes[1]).await, "{case}");
        assert_eq!(node.cursor().await, 0, "{case}: the cursor stays");

        // The outage ends: the next tick resumes from the held head.
        outage.borrow_mut().take();
        let (topic, sent) = node.tick().await;
        assert_eq!(
            sent,
            txids(&nodes, &[3, 2, 1]),
            "{case}: the walk stops at the held head"
        );
        assert_eq!(held(&node.store, &nodes).await, vec![(3, 0)], "{case}");
        assert!(
            !row_exists(&node.store, &nodes[0]).await,
            "{case}: the old head is deleted once the new one is in"
        );
        assert_eq!(applied_rows(&node.store, &nodes).await, [true; 4], "{case}");
        assert_eq!(topic.cursor_moves, moved(0, 1), "{case}");

        // And it stays so.
        node.tick().await;
        assert_eq!(held(&node.store, &nodes).await, vec![(3, 0)], "{case}");
    }
}

fn proven_beef(node: &GASPNode) -> TaggedBEEF {
    let mut tx = Transaction::from_hex(&node.raw_tx).unwrap();
    tx.merkle_path = Some(MerklePath::from_hex(node.proof.as_ref().unwrap()).unwrap());
    TaggedBEEF::new(tx.to_beef(false).unwrap(), vec![TOPIC.to_string()])
}

// The same fault at the `/submit` door (the queue's shape: one engine, each
// transaction its own submit). Head 2 arrives while head 1 has not landed: it
// finds no coin and the manager admits nothing. On 1821f73 that wrote its
// applied row, so its replay was a dupe and the chain ended at head 1 for
// good. Now it is reported as not durable and recorded nowhere, and the
// replays place both.
#[tokio::test]
async fn i559_b_a_successor_of_a_faulted_submit_is_not_recorded_as_applied() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(3);
    let state = Rc::new(RefCell::new(HeadState::default()));
    let store = Rc::new(MemoryStorage::new());
    let scripted = ScriptedStore::armed(&store, node_txid(&nodes[1]), 0, InsertEvent::Faults);
    let outage = scripted.armed.clone();
    let engine = Engine::new(
        HashMap::from([(
            TOPIC.to_string(),
            Box::new(HeadChainManager(state.clone())) as Box<dyn TopicManager>,
        )]),
        HashMap::new(),
        Box::new(scripted),
        None,
        EngineConfig::default(),
    );
    let submit = |i: usize| {
        let beef = proven_beef(&nodes[i]);
        let engine = &engine;
        async move {
            engine
                .submit_with_report(&beef, SubmitMode::HistoricalTxNoSpv)
                .await
                .unwrap()
                .1
        }
    };

    assert!(submit(0).await.is_durable());
    let faulted = submit(1).await;
    assert!(!faulted.is_durable(), "the insert of head 1 faulted");
    assert!(
        row_exists(&store, &nodes[0]).await,
        "the old head stays held on the fault"
    );
    let successor = submit(2).await;
    println!(
        "#559 /submit: head 2 after the faulted head 1: durable {}, applied {:?}, rows {:?}",
        successor.is_durable(),
        successor.applied_topics,
        applied_rows(&store, &nodes).await
    );
    assert!(
        !successor.is_durable() && successor.applied_topics.is_empty(),
        "head 2 found no coin because head 1 did not land: not now, never applied"
    );
    assert_eq!(applied_rows(&store, &nodes).await, [true, false, false]);

    // The outage ends and the queue replays both, in order.
    outage.borrow_mut().take();
    assert!(submit(1).await.is_durable());
    let replayed = submit(2).await;
    assert!(replayed.is_durable());
    assert_eq!(replayed.applied_topics, vec![TOPIC.to_string()]);
    assert_eq!(held(&store, &nodes).await, vec![(2, 0)]);
    assert_eq!(applied_rows(&store, &nodes).await, [true; 3]);
}

// DELTA-3. The insert of head 1 never ANSWERS (a hung D1 call) inside a
// finalize write section, which no deadline drops. On 1821f73 that held the
// per-peer budget and the worker's belt for as long as the call hung: the
// tick never returned (RED there with this pin less its one call to
// `set_finalize_submit_budget`, which the base does not have). Now each
// storage call of a finalize submit is bounded by the submit's budget; the
// call that does not answer is that call's fault (the submit itself is never
// dropped: the lens fold of 2026-10-07, F2), the UTXO fails and the cursor
// stays.
#[tokio::test]
async fn i559_c_a_hung_storage_call_inside_a_finalize_submit_is_timed_out_and_fails_the_utxo() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(4);
    let state = Rc::new(RefCell::new(HeadState::default()));
    let store = Rc::new(MemoryStorage::new());
    let scripted = ScriptedStore::armed(&store, node_txid(&nodes[1]), 0, InsertEvent::Hangs);
    let hang = scripted.armed.clone();
    let mut node = Budgeted::over(
        RecordingRemote::new(&nodes, &[3]),
        Box::new(HeadChainManager(state.clone())),
        RequestClock::allowing(u64::MAX - 1),
        store,
        Box::new(scripted),
        true,
    );
    // The submit's own deadline falls due after a few polls, as a timer
    // does; the peer's deadline is never due here.
    node.engine.set_finalize_submit_budget(
        Rc::new(|_ms| {
            let mut polls = 0;
            Box::pin(std::future::poll_fn(move |cx| {
                polls += 1;
                if polls > 3 {
                    std::task::Poll::Ready(())
                } else {
                    cx.waker().wake_by_ref();
                    std::task::Poll::Pending
                }
            }))
        }),
        1,
    );

    let ticked = tokio::time::timeout(std::time::Duration::from_secs(10), node.tick()).await;
    assert!(
        ticked.is_ok(),
        "the hung write held the tick: nothing bounds a storage call inside a write section"
    );
    let rows = applied_rows(&node.store, &nodes).await;
    println!(
        "#559 DELTA-3: after the hung insert of head 1: old head row held {}, applied {rows:?}, cursor {}",
        row_exists(&node.store, &nodes[0]).await,
        node.cursor().await
    );
    assert!(row_exists(&node.store, &nodes[0]).await);
    assert_eq!(rows, [true, false, false, false]);
    assert_eq!(node.cursor().await, 0, "the UTXO failed: the cursor stays");

    // The call answers again: the next tick places the chain.
    hang.borrow_mut().take();
    let (topic, _) = node.tick().await;
    assert_eq!(held(&node.store, &nodes).await, vec![(3, 0)]);
    assert_eq!(applied_rows(&node.store, &nodes).await, [true; 4]);
    assert_eq!(topic.cursor_moves, moved(0, 1));
}

// ============================================================================
// bsv-low #554: a UTXO is ingested at most once per sync. The cursor moves to
// a UTXO's score before its ingest and the responder serves `score >= since`,
// so the row at the page boundary is served again on the next page; one whose
// ingest FAILED was in no set and was ingested a second time.
// ============================================================================

#[tokio::test]
async fn i554_a_failing_utxo_at_the_page_boundary_is_requested_once_per_sync() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(3);
    let tip = node_txid(&nodes[2]);
    let remote = RecordingRemote::new(&nodes, &[2]);
    // The peer cannot serve the listed tip right now (a timeout, a 500).
    remote.faults.borrow_mut().insert(tip.clone());
    let faults = remote.faults.clone();
    let state = Rc::new(RefCell::new(HeadState::default()));
    let store = Rc::new(MemoryStorage::new());
    let node = Budgeted::over(
        remote,
        Box::new(HeadChainManager(state.clone())),
        RequestClock::allowing(u64::MAX - 1),
        store.clone(),
        Box::new(store),
        false,
    );

    for tick in 1..=2 {
        let (_, sent) = node.tick().await;
        println!(
            "#554: tick {tick}: the failing tip was requested {} time(s)",
            sent.len()
        );
        assert_eq!(
            sent,
            vec![tip.clone()],
            "tick {tick}: the failing UTXO is requested once per sync"
        );
        assert_eq!(node.cursor().await, 0, "the cursor does not pass it");
    }

    // The peer heals: the gap guard kept the cursor below it, so the next
    // tick is served it and admits the chain.
    faults.borrow_mut().clear();
    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&nodes, &[2, 1, 0]));
    assert_eq!(held(&node.store, &nodes).await, vec![(2, 0)]);
    assert_eq!(topic.cursor_moves, moved(0, 1));
}

// ============================================================================
// The lens fold of 2026-10-07 (bsv-low #559, `docs/audit/E559-lens-2026-10-07.md`
// in bsv-low). F1: a fault AFTER the insert never leaves two unspent heads.
// F2: no finalize submit is dropped between its writes, each storage call is
// bounded instead. F3: a successor that arrives in a later invocation, before
// its predecessor's replay, is not recorded as applied.
// ============================================================================

// Every row of the chain's output 0 the store holds: "index:spent".
async fn rows(store: &MemoryStorage, nodes: &[GASPNode]) -> Vec<String> {
    let mut rows = Vec::new();
    for (i, n) in nodes.iter().enumerate() {
        if let Some(o) = store
            .find_output(&node_txid(n), 0, Some(TOPIC), None, false)
            .await
            .unwrap()
        {
            rows.push(format!("{i}:spent={}", o.spent));
        }
    }
    rows
}

// One engine of the `/submit` door over `store` (a worker invocation).
fn door<M: TopicManager + 'static>(manager: M, storage: ScriptedStore) -> Engine {
    Engine::new(
        HashMap::from([(
            TOPIC.to_string(),
            Box::new(manager) as Box<dyn TopicManager>,
        )]),
        HashMap::new(),
        Box::new(storage),
        None,
        EngineConfig::default(),
    )
}

fn head_door(store: &Rc<MemoryStorage>, calls: &Calls) -> Engine {
    let mut storage = ScriptedStore::plain(store);
    storage.calls = calls.clone();
    door(
        HeadChainManager(Rc::new(RefCell::new(HeadState::default()))),
        storage,
    )
}

async fn submitted(
    engine: &Engine,
    beef: &TaggedBEEF,
) -> bsv_overlay_engine::engine::MutationReport {
    engine
        .submit_with_report(beef, SubmitMode::HistoricalTxNoSpv)
        .await
        .unwrap()
        .1
}

// F1, the lens's X2 (GASP, one transient D1 fault, no other condition). Two
// heads, the tip listed; the mark-spent of the genesis faults once inside the
// finalize submit of head 1. On c9921ee the insert of head 1 landed and the
// delete of the genesis was skipped because a fault had been reported: both
// stayed, both unspent, and no later tick changed it (the new head is held,
// so the peer's listing of it is known and the cursor passes).
#[tokio::test]
async fn fold559_f1_x2_a_mark_fault_at_the_tip_never_leaves_two_unspent_heads() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(2);
    for budgeted in [true, false] {
        let store = Rc::new(MemoryStorage::new());
        let scripted = ScriptedStore::plain(&store);
        scripted.calls.borrow_mut().push((
            Call::MarkSpent,
            node_txid(&nodes[0]),
            CallEvent::FaultsOnce,
        ));
        let node = Budgeted::over(
            RecordingRemote::new(&nodes, &[1]),
            Box::new(HeadChainManager(Rc::new(
                RefCell::new(HeadState::default()),
            ))),
            RequestClock::allowing(u64::MAX - 1),
            store,
            Box::new(scripted),
            budgeted,
        );
        for tick in 1..=4 {
            node.tick().await;
            let utxos = held(&node.store, &nodes).await;
            println!(
                "#559 F1 X2 budgeted={budgeted} tick {tick}: rows {:?} utxos {utxos:?} applied {:?} cursor {}",
                rows(&node.store, &nodes).await,
                applied_rows(&node.store, &nodes).await,
                node.cursor().await
            );
            assert_eq!(
                utxos.len(),
                1,
                "budgeted={budgeted} tick {tick}: exactly one unspent head, never two, never none"
            );
        }
        assert_eq!(held(&node.store, &nodes).await, vec![(1, 0)]);
        assert_eq!(
            rows(&node.store, &nodes).await,
            ["1:spent=false"],
            "budgeted={budgeted}: the genesis row is gone"
        );
        assert_eq!(node.cursor().await, 1, "budgeted={budgeted}");
    }
}

// F1, the lens's X1 (the `/submit` door, the queue's order). Head 1's submit
// faults after its insert (the same mark fault) and its replay is queued;
// head 2 lands first in another invocation; the queue then replays head 1.
// On c9921ee the genesis was still held (the delete had been skipped), the
// manager admitted head 1 again and it was RE-INSERTED unspent beside head 2,
// every row applied: permanent. Three engines over one store, then one.
#[tokio::test]
async fn fold559_f1_x1_a_replay_after_the_successor_does_not_resurrect_a_spent_head() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(3);
    for one_engine in [false, true] {
        let store = Rc::new(MemoryStorage::new());
        let calls = Calls::default();
        let shared = head_door(&store, &calls);
        let invocation = || head_door(&store, &calls);
        let case = format!("one_engine={one_engine}");

        assert!(submitted(&shared, &proven_beef(&nodes[0]))
            .await
            .is_durable());
        calls
            .borrow_mut()
            .push((Call::MarkSpent, node_txid(&nodes[0]), CallEvent::FaultsOnce));
        let faulted = submitted(&shared, &proven_beef(&nodes[1])).await;
        assert!(!faulted.is_durable(), "{case}: the mark fault is reported");
        println!(
            "#559 F1 X1 {case}: after the faulted head 1: rows {:?} applied {:?}",
            rows(&store, &nodes).await,
            applied_rows(&store, &nodes).await
        );
        assert_eq!(held(&store, &nodes).await.len(), 1, "{case}: one head");

        // Head 2 arrives before the replay of head 1.
        let later = invocation();
        let successor = submitted(
            if one_engine { &shared } else { &later },
            &proven_beef(&nodes[2]),
        )
        .await;
        println!(
            "#559 F1 X1 {case}: after head 2: durable {} rows {:?} applied {:?}",
            successor.is_durable(),
            rows(&store, &nodes).await,
            applied_rows(&store, &nodes).await
        );
        assert_eq!(held(&store, &nodes).await.len(), 1, "{case}: one head");

        // The queue replays head 1, then anything not durable so far.
        let queue = invocation();
        let queue = if one_engine { &shared } else { &queue };
        submitted(queue, &proven_beef(&nodes[1])).await;
        if !successor.is_durable() {
            assert!(submitted(queue, &proven_beef(&nodes[2])).await.is_durable());
        }
        println!(
            "#559 F1 X1 {case}: after the replay of head 1: rows {:?} applied {:?}",
            rows(&store, &nodes).await,
            applied_rows(&store, &nodes).await
        );
        assert_eq!(
            rows(&store, &nodes).await,
            ["2:spent=false"],
            "{case}: head 2 is the only row, head 1 is not resurrected"
        );
        assert_eq!(applied_rows(&store, &nodes).await, [true; 3], "{case}");
    }
}

// A lookup service that records what it is told and faults ONCE on the
// admission of `faults_on`, and ONCE on the spend of `spend_faults_on`.
struct ToldLookup {
    admitted: Rc<RefCell<Vec<String>>>,
    spent: Rc<RefCell<Vec<String>>>,
    faults_on: RefCell<Option<String>>,
    spend_faults_on: RefCell<Option<String>>,
}

#[async_trait(?Send)]
impl bsv_overlay_engine::lookup_service::LookupService for ToldLookup {
    fn reads_off_chain_values(&self, _topic: &str) -> bool {
        false
    }

    fn admission_mode(&self) -> AdmissionMode {
        AdmissionMode::LockingScript
    }
    fn spend_notification_mode(&self) -> SpendNotificationMode {
        SpendNotificationMode::Txid
    }
    async fn output_admitted_by_topic(
        &self,
        payload: &OutputAdmittedByTopic,
    ) -> Result<(), bsv_overlay_engine::lookup_service::LookupServiceError> {
        let OutputAdmittedByTopic::LockingScript { txid, .. } = payload else {
            unreachable!("the mode is the locking script");
        };
        if self.faults_on.borrow().as_deref() == Some(txid) {
            self.faults_on.borrow_mut().take();
            return Err(
                bsv_overlay_engine::lookup_service::LookupServiceError::Other(
                    "D1_ERROR: the index is overloaded".into(),
                ),
            );
        }
        self.admitted.borrow_mut().push(txid.clone());
        Ok(())
    }
    async fn output_spent(
        &self,
        payload: &OutputSpent,
    ) -> Result<(), bsv_overlay_engine::lookup_service::LookupServiceError> {
        let OutputSpent::Txid { txid, .. } = payload else {
            unreachable!("the mode is the txid");
        };
        if self.spend_faults_on.borrow().as_deref() == Some(txid) {
            self.spend_faults_on.borrow_mut().take();
            return Err(
                bsv_overlay_engine::lookup_service::LookupServiceError::Other(
                    "D1_ERROR: the index is overloaded".into(),
                ),
            );
        }
        self.spent.borrow_mut().push(txid.clone());
        Ok(())
    }
    async fn output_evicted(
        &self,
        _txid: &str,
        _output_index: u32,
    ) -> Result<(), bsv_overlay_engine::lookup_service::LookupServiceError> {
        Ok(())
    }
    async fn lookup(
        &self,
        _question: &LookupQuestion,
    ) -> Result<LookupResult, bsv_overlay_engine::lookup_service::LookupServiceError> {
        Ok(LookupResult::OutputList(Vec::new()))
    }
    async fn get_documentation(&self) -> String {
        String::new()
    }
    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata::default()
    }
}

// F1, the notification variant the lens read but did not run (its NIT: "F1
// lives in exactly that gap"). The lookup hook faults on the admission of
// head 1, after its insert. On c9921ee that kept the genesis beside head 1
// and the replay after head 2 put head 1 back. Now the store is complete at
// the fault (the genesis deleted, the spent mark told), and the replay finds
// no coin. The limit this pins too: a manager whose rule needs the previous
// coin admits nothing on that replay, so the hook that faulted is NOT told of
// head 1 again (the reference tells nobody twice either).
#[tokio::test]
async fn fold559_f1_a_notification_fault_after_the_insert_leaves_one_head() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(3);
    let store = Rc::new(MemoryStorage::new());
    let (admitted, spent) = (
        Rc::new(RefCell::new(Vec::new())),
        Rc::new(RefCell::new(Vec::new())),
    );
    let invocation = |faults_on: Option<String>| {
        Engine::new(
            HashMap::from([(
                TOPIC.to_string(),
                Box::new(HeadChainManager(Rc::new(
                    RefCell::new(HeadState::default()),
                ))) as Box<dyn TopicManager>,
            )]),
            HashMap::from([(
                "ls_told".to_string(),
                Box::new(ToldLookup {
                    admitted: admitted.clone(),
                    spent: spent.clone(),
                    faults_on: RefCell::new(faults_on),
                    spend_faults_on: RefCell::new(None),
                }) as Box<dyn bsv_overlay_engine::lookup_service::LookupService>,
            )]),
            Box::new(ScriptedStore::plain(&store)),
            None,
            EngineConfig::default(),
        )
    };

    let first = invocation(Some(node_txid(&nodes[1])));
    assert!(submitted(&first, &proven_beef(&nodes[0]))
        .await
        .is_durable());
    let faulted = submitted(&first, &proven_beef(&nodes[1])).await;
    assert!(
        faulted
            .faults
            .iter()
            .any(|f| f.site == "lookup_service.output_admitted_by_topic"),
        "{}",
        faulted.summary()
    );
    assert_eq!(
        rows(&store, &nodes).await,
        ["1:spent=false"],
        "the store is complete at the fault: head 1 in, the genesis deleted"
    );
    assert_eq!(applied_rows(&store, &nodes).await, [true, false, false]);

    // Head 2 lands in a later invocation, then the queue replays head 1.
    assert!(submitted(&invocation(None), &proven_beef(&nodes[2]))
        .await
        .is_durable());
    let replay = submitted(&invocation(None), &proven_beef(&nodes[1])).await;
    assert!(replay.is_durable(), "{}", replay.summary());
    println!(
        "#559 F1 notify: rows {:?} applied {:?}; the hook was told of {} admission(s) and {} spend(s)",
        rows(&store, &nodes).await,
        applied_rows(&store, &nodes).await,
        admitted.borrow().len(),
        spent.borrow().len()
    );
    assert_eq!(rows(&store, &nodes).await, ["2:spent=false"]);
    assert_eq!(applied_rows(&store, &nodes).await, [true; 3]);
    assert_eq!(
        *admitted.borrow(),
        txids(&nodes, &[0, 2]),
        "the limit: the admission that faulted is not told again"
    );
    assert_eq!(*spent.borrow(), txids(&nodes, &[0, 1]));
}

// F1: an insert that faults after another output of the same transaction
// landed. On c9921ee the output that landed stayed beside the kept previous
// coin; a successor could spend and delete it before the replay, and the
// replay (the previous coin still held) inserted it again. Now nothing of a
// transaction that did not land is held.
#[tokio::test]
async fn fold559_f1_an_insert_fault_after_a_landed_output_holds_nothing_of_the_transaction() {
    let (_logs, _guard) = capture_logs();
    // Head 1 carries a record at output 1; its insert faults.
    let nodes = recorded_chain(3, &[1]);
    let store = Rc::new(MemoryStorage::new());
    let scripted = ScriptedStore::armed(&store, node_txid(&nodes[1]), 1, InsertEvent::Faults);
    let outage = scripted.armed.clone();
    let engine = door(
        RecordedHeadManager(Rc::new(RefCell::new(HeadState::default()))),
        scripted,
    );

    assert!(submitted(&engine, &proven_beef(&nodes[0]))
        .await
        .is_durable());
    let faulted = submitted(&engine, &proven_beef(&nodes[1])).await;
    assert!(!faulted.is_durable());
    println!(
        "#559 F1 partial: after the faulted insert of the record: rows {:?} utxos {:?}",
        rows(&store, &nodes).await,
        held(&store, &nodes).await
    );
    assert_eq!(
        rows(&store, &nodes).await,
        ["0:spent=true"],
        "the previous coin is kept and the output that landed is taken out again"
    );
    assert!(held(&store, &nodes).await.is_empty());
    assert_eq!(applied_rows(&store, &nodes).await, [true, false, false]);

    outage.borrow_mut().take();
    assert!(submitted(&engine, &proven_beef(&nodes[1]))
        .await
        .is_durable());
    assert_eq!(held(&store, &nodes).await, vec![(1, 0), (1, 1)]);
    assert!(!row_exists(&store, &nodes[0]).await);
}

// A transaction that does not carry a proof, in a BEEF with the bodies of the
// ancestors it spends from down to `proven`, which carries its proof: what a
// client submits for a spend whose parents are not all mined yet.
fn unproven_beef(nodes: &[GASPNode], subject: usize, proven: usize) -> TaggedBEEF {
    let mut tx = Transaction::from_hex(&nodes[proven].raw_tx).unwrap();
    tx.merkle_path = Some(MerklePath::from_hex(nodes[proven].proof.as_ref().unwrap()).unwrap());
    for node in &nodes[proven + 1..=subject] {
        let mut child = Transaction::from_hex(&node.raw_tx).unwrap();
        child.inputs[0].source_transaction = Some(Box::new(tx));
        tx = child;
    }
    TaggedBEEF::new(tx.to_beef(false).unwrap(), vec![TOPIC.to_string()])
}

// F1: the DELETE of the stale coin answers an error, after the insert. The
// first fold read the old head back held and took the new one out again; the
// second delta fold of 2026-10-07 (M1) undoes nothing once a delete was
// started, since the statement may land yet (the pin of that is
// `delta2_559_m1_y1`). So the new head stays beside the old one, which is
// marked spent: one UTXO, no applied row. Head 2, arriving before the replay,
// spends the new head and records head 1 as applied (H2), so the replay is a
// dupe and the old head's spent row is what stays behind.
#[tokio::test]
async fn fold559_f1_a_delete_fault_keeps_the_new_head_beside_the_spent_old_one() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(3);
    let store = Rc::new(MemoryStorage::new());
    let calls = Calls::default();
    let first = head_door(&store, &calls);
    assert!(submitted(&first, &proven_beef(&nodes[0]))
        .await
        .is_durable());
    calls
        .borrow_mut()
        .push((Call::Delete, node_txid(&nodes[0]), CallEvent::FaultsOnce));
    let faulted = submitted(&first, &proven_beef(&nodes[1])).await;
    assert!(
        faulted.faults.iter().any(|f| f.site == "delete_utxo_deep"),
        "{}",
        faulted.summary()
    );
    assert!(
        !faulted
            .faults
            .iter()
            .any(|f| f.site == "undo_insert_output"),
        "{}",
        faulted.summary()
    );
    println!(
        "#559 F1 delete: after the faulted delete of the genesis: rows {:?} applied {:?}",
        rows(&store, &nodes).await,
        applied_rows(&store, &nodes).await
    );
    assert_eq!(
        rows(&store, &nodes).await,
        ["0:spent=true", "1:spent=false"],
        "nothing is undone: the new head stays, the old one is marked spent"
    );
    assert_eq!(held(&store, &nodes).await, vec![(1, 0)], "one UTXO");
    assert_eq!(applied_rows(&store, &nodes).await, [true, false, false]);

    // Head 2 arrives in a later invocation, before the replay: it finds
    // head 1 and lands.
    let successor = submitted(&head_door(&store, &calls), &unproven_beef(&nodes, 2, 1)).await;
    assert!(successor.is_durable(), "{}", successor.summary());
    // The queue replays head 1: a dupe.
    let replay = submitted(&head_door(&store, &calls), &proven_beef(&nodes[1])).await;
    assert!(replay.is_durable(), "{}", replay.summary());
    assert_eq!(replay.deduped_topics, vec![TOPIC.to_string()]);
    assert_eq!(held(&store, &nodes).await, vec![(2, 0)], "one head");
    assert_eq!(
        rows(&store, &nodes).await,
        ["0:spent=true", "2:spent=false"],
        "the old head's spent row stays behind, no UTXO"
    );
    assert_eq!(applied_rows(&store, &nodes).await, [true; 3]);
}

// F1, the other end of a delete fault: the delete LANDED and its cleanup
// faulted (D1's shape). The old head is read back gone, so the new one stays
// (taking it out would leave no head at all); there is no applied row, and
// the replay, which finds no coin and admits nothing, writes it.
#[tokio::test]
async fn fold559_f1_a_delete_that_landed_and_then_faulted_keeps_the_new_head() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(2);
    let store = Rc::new(MemoryStorage::new());
    let calls = Calls::default();
    let engine = head_door(&store, &calls);
    assert!(submitted(&engine, &proven_beef(&nodes[0]))
        .await
        .is_durable());
    calls.borrow_mut().push((
        Call::Delete,
        node_txid(&nodes[0]),
        CallEvent::LandsThenFaultsOnce,
    ));
    let faulted = submitted(&engine, &proven_beef(&nodes[1])).await;
    assert!(!faulted.is_durable());
    assert_eq!(
        rows(&store, &nodes).await,
        ["1:spent=false"],
        "never no head: the old one is gone, so the new one stays"
    );
    assert_eq!(applied_rows(&store, &nodes).await, [true, false]);
    assert!(submitted(&engine, &proven_beef(&nodes[1]))
        .await
        .is_durable());
    assert_eq!(rows(&store, &nodes).await, ["1:spent=false"]);
    assert_eq!(applied_rows(&store, &nodes).await, [true; 2]);
}

// The submit's own deadline of the F2 pins: due after a few polls, as a timer
// is; a fresh one for every call of the factory (the undo has its own).
fn due_after_a_few_polls() -> bsv_overlay_engine::engine::SleepFactory {
    Rc::new(|_ms| {
        let mut polls = 0;
        Box::pin(std::future::poll_fn(move |cx| {
            polls += 1;
            if polls > 3 {
                std::task::Poll::Ready(())
            } else {
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        }))
    })
}

// F2, the lens's X3. Inside a finalize submit the delete of the old head
// never ANSWERS, after the new head's insert landed. On c9921ee the budget
// dropped the whole submit there, between two writes: the old head stayed
// (spent) beside the new one, with no applied row, and no later tick replayed
// it (the new head is held). Now the hung CALL is that call's fault and the
// submit runs on to its end: the UTXO fails within the budget and the cursor
// stays; the next tick places the chain. Since the delta fold of 2026-10-07
// (H1) nothing is undone on a delete that did not answer, it may land yet:
// the new head stays beside the old one, which is marked spent. One UNSPENT
// head at every step, never none, never two (the late landing itself is
// `delta559_h1_x4`).
#[tokio::test]
async fn fold559_f2_a_hung_delete_inside_a_finalize_submit_is_a_fault_of_that_call_not_a_drop() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(4);
    let store = Rc::new(MemoryStorage::new());
    let scripted = ScriptedStore::plain(&store);
    let calls = scripted.calls.clone();
    calls
        .borrow_mut()
        .push((Call::Delete, node_txid(&nodes[0]), CallEvent::Hangs));
    let mut node = Budgeted::over(
        RecordingRemote::new(&nodes, &[3]),
        Box::new(HeadChainManager(Rc::new(
            RefCell::new(HeadState::default()),
        ))),
        RequestClock::allowing(u64::MAX - 1),
        store,
        Box::new(scripted),
        true,
    );
    node.engine
        .set_finalize_submit_budget(due_after_a_few_polls(), 1);

    let ticked = tokio::time::timeout(std::time::Duration::from_secs(10), node.tick()).await;
    let (topic, _) = ticked.expect("the hung delete held the tick");
    println!(
        "#559 F2: after the hung delete of the genesis: rows {:?} applied {:?} cursor {} errors {:?}",
        rows(&node.store, &nodes).await,
        applied_rows(&node.store, &nodes).await,
        node.cursor().await,
        topic.errors
    );
    assert_eq!(
        rows(&node.store, &nodes).await,
        ["0:spent=true", "1:spent=false"],
        "one unspent head: the submit ran on past the hung call and undid nothing"
    );
    assert_eq!(
        applied_rows(&node.store, &nodes).await,
        [true, false, false, false]
    );
    assert_eq!(node.cursor().await, 0, "the UTXO failed: the cursor stays");
    assert!(topic.cursor_moves.is_empty());

    // The call answers again: the next tick resumes from the held head.
    calls.borrow_mut().clear();
    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&nodes, &[3, 2]));
    assert_eq!(
        rows(&node.store, &nodes).await,
        ["0:spent=true", "3:spent=false"]
    );
    assert_eq!(applied_rows(&node.store, &nodes).await, [true; 4]);
    assert_eq!(topic.cursor_moves, moved(0, 1));
}

// F2: EVERY storage call hangs (the store is gone for the tick). The first
// read of the finalize submit takes the budget and no call is started after
// it: the tick returns, nothing is written, the cursor stays.
#[tokio::test]
async fn fold559_f2_a_store_that_never_answers_costs_one_budget_and_writes_nothing() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(3);
    let store = Rc::new(MemoryStorage::new());
    let scripted = ScriptedStore::plain(&store);
    let calls = scripted.calls.clone();
    let mut node = Budgeted::over(
        RecordingRemote::new(&nodes, &[2]),
        Box::new(HeadChainManager(Rc::new(
            RefCell::new(HeadState::default()),
        ))),
        RequestClock::allowing(u64::MAX - 1),
        store,
        Box::new(scripted),
        true,
    );
    node.engine
        .set_finalize_submit_budget(due_after_a_few_polls(), 1);
    // The genesis is placed by a first sync of a one-link peer listing.
    assert!(submitted(&node.engine, &proven_beef(&nodes[0]))
        .await
        .is_durable());
    // From here the mark of the genesis hangs, and so does every later call
    // that names it or head 1.
    for call in [Call::MarkSpent, Call::Delete] {
        calls
            .borrow_mut()
            .push((call, node_txid(&nodes[0]), CallEvent::Hangs));
    }

    let ticked = tokio::time::timeout(std::time::Duration::from_secs(10), node.tick()).await;
    assert!(ticked.is_ok(), "the hung calls held the tick");
    println!(
        "#559 F2 all hung: rows {:?} applied {:?} cursor {}",
        rows(&node.store, &nodes).await,
        applied_rows(&node.store, &nodes).await,
        node.cursor().await
    );
    assert_eq!(rows(&node.store, &nodes).await.len(), 1, "one head row");
    assert_eq!(node.cursor().await, 0);

    calls.borrow_mut().clear();
    node.tick().await;
    assert_eq!(rows(&node.store, &nodes).await, ["2:spent=false"]);
    assert_eq!(node.cursor().await, 1);
}

// F3. Head 1's insert faults in one invocation (the old head is kept); head 2
// arrives in ANOTHER invocation, a new engine over the same store, before the
// replay. On c9921ee the memory of the fault had died with the first engine:
// head 2 found no coin, admitted nothing and was recorded as applied, and the
// chain then stopped at head 1 for good. Now the store itself answers: head
// 1, whose body head 2's BEEF carries, has no applied row, holds no output
// and spends a coin the topic holds. And head 3, over an unlanded head 2
// over an unlanded head 1, is "not now" the same way.
#[tokio::test]
async fn fold559_f3_a_successor_in_a_later_invocation_is_not_recorded_before_its_predecessor_lands()
{
    let (_logs, _guard) = capture_logs();
    let nodes = chain(4);
    let store = Rc::new(MemoryStorage::new());
    let calls = Calls::default();

    // Invocation A: the genesis lands, the insert of head 1 faults.
    let scripted = ScriptedStore::armed(&store, node_txid(&nodes[1]), 0, InsertEvent::Faults);
    let a = door(
        HeadChainManager(Rc::new(RefCell::new(HeadState::default()))),
        scripted,
    );
    assert!(submitted(&a, &proven_beef(&nodes[0])).await.is_durable());
    assert!(!submitted(&a, &proven_beef(&nodes[1])).await.is_durable());
    drop(a);
    assert_eq!(rows(&store, &nodes).await, ["0:spent=true"]);

    // Invocation B: head 2, its parent's body in the BEEF. The insert of
    // head 1 is still refused (since the E1D lens fold the door submits a
    // carried predecessor first: here it does not land, so head 2 waits).
    let b = door(
        HeadChainManager(Rc::new(RefCell::new(HeadState::default()))),
        ScriptedStore::armed(&store, node_txid(&nodes[1]), 0, InsertEvent::Faults),
    );
    let successor = submitted(&b, &unproven_beef(&nodes, 2, 1)).await;
    println!(
        "#559 F3: head 2 in a later invocation: durable {}, faults {}, applied {:?}",
        successor.is_durable(),
        successor.summary(),
        applied_rows(&store, &nodes).await
    );
    assert!(
        successor
            .faults
            .iter()
            .any(|f| f.site == "predecessor_not_landed"),
        "{}",
        successor.summary()
    );
    assert_eq!(
        applied_rows(&store, &nodes).await,
        [true, false, false, false]
    );
    drop(b);

    // Invocation C: head 3, over head 2 (not landed, holds no coin) over
    // head 1 (not landed, spends the held genesis; its insert refused).
    let c = door(
        HeadChainManager(Rc::new(RefCell::new(HeadState::default()))),
        ScriptedStore::armed(&store, node_txid(&nodes[1]), 0, InsertEvent::Faults),
    );
    let third = submitted(&c, &unproven_beef(&nodes, 3, 1)).await;
    assert!(
        third
            .faults
            .iter()
            .any(|f| f.site == "predecessor_not_landed"),
        "{}",
        third.summary()
    );
    assert_eq!(
        applied_rows(&store, &nodes).await,
        [true, false, false, false]
    );
    drop(c);

    // The queue replays the three, each in an invocation of its own.
    assert!(
        submitted(&head_door(&store, &calls), &proven_beef(&nodes[1]))
            .await
            .is_durable()
    );
    assert!(
        submitted(&head_door(&store, &calls), &unproven_beef(&nodes, 2, 1))
            .await
            .is_durable()
    );
    assert!(
        submitted(&head_door(&store, &calls), &unproven_beef(&nodes, 3, 1))
            .await
            .is_durable()
    );
    assert_eq!(rows(&store, &nodes).await, ["3:spent=false"]);
    assert_eq!(applied_rows(&store, &nodes).await, [true; 4]);

    // A transaction that admits nothing over a LANDED parent is recorded, as
    // before and as in the reference: the rule asks only about a parent the
    // store has not taken.
    let again = head_door(&store, &calls);
    let stranger = {
        let mut tx = Transaction::new();
        tx.inputs
            .push(TransactionInput::new(node_txid(&nodes[2]), 0));
        tx.inputs[0].source_transaction = Some(Box::new({
            let mut parent = Transaction::from_hex(&nodes[2].raw_tx).unwrap();
            parent.merkle_path =
                Some(MerklePath::from_hex(nodes[2].proof.as_ref().unwrap()).unwrap());
            parent
        }));
        // Two outputs: with one it would BE head 3.
        tx.outputs.push(plain_output());
        tx.outputs.push(plain_output());
        TaggedBEEF::new(tx.to_beef(false).unwrap(), vec![TOPIC.to_string()])
    };
    let recorded = submitted(&again, &stranger).await;
    assert!(recorded.is_durable(), "{}", recorded.summary());
    assert_eq!(recorded.applied_topics, vec![TOPIC.to_string()]);
}

// ============================================================================
// The delta fold of 2026-10-07 (bsv-low #559, `docs/audit/E559-delta-2026-10-07.md`
// in bsv-low): the one-head invariant made true for the six recipes the delta
// lens executed (X4 to X9) and for the first fold's two-fault residual. H1: a
// delete that did not answer never undoes the inserts. H2: the spender of a
// coin records that coin's transaction as applied. H3: a faulted dedup read
// is the topic's read fault. M1, M2: a read fault or the read bound inside
// the predecessor question answers "not now". Each pin is RED on 33e78fb.
// ============================================================================

// Head 1's finalize submit of a four-head chain whose tip is listed, with the
// delete of the genesis hung: the node and the script, after that tick.
async fn after_a_hung_delete_of_the_genesis(nodes: &[GASPNode]) -> (Budgeted, Calls) {
    let store = Rc::new(MemoryStorage::new());
    let scripted = ScriptedStore::plain(&store);
    let calls = scripted.calls.clone();
    calls
        .borrow_mut()
        .push((Call::Delete, node_txid(&nodes[0]), CallEvent::Hangs));
    let mut node = Budgeted::over(
        RecordingRemote::new(nodes, &[3]),
        Box::new(HeadChainManager(Rc::new(
            RefCell::new(HeadState::default()),
        ))),
        RequestClock::allowing(u64::MAX - 1),
        store,
        Box::new(scripted),
        true,
    );
    node.engine
        .set_finalize_submit_budget(due_after_a_few_polls(), 1);
    let ticked = tokio::time::timeout(std::time::Duration::from_secs(10), node.tick()).await;
    assert!(ticked.is_ok(), "the hung delete held the tick");
    (node, calls)
}

// H1, the delta lens's X4. A call dropped at its timeout is not cancelled: the
// delete of the genesis that did not answer inside head 1's finalize submit
// may land later. On 33e78fb the genesis was read back "held" and head 1 was
// taken out again; when the delete then landed the chain had NO head, every
// later head found no coin, admitted nothing and was recorded, for good. Now
// an unanswered delete leaves the inserts standing. Both orders, under GASP
// (which does not replay a held head) and under the queue's replay of head 1:
// the delete lands late, the delete never lands.
#[tokio::test]
async fn delta559_h1_x4_a_delete_that_lands_after_its_timeout_never_leaves_the_chain_headless() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(4);
    for lands_late in [true, false] {
        for queue_replays in [false, true] {
            let (node, calls) = after_a_hung_delete_of_the_genesis(&nodes).await;
            println!(
                "#559 H1 (lands late {lands_late}, queue {queue_replays}): after the hung delete: rows {:?} applied {:?} cursor {}",
                rows(&node.store, &nodes).await,
                applied_rows(&node.store, &nodes).await,
                node.cursor().await
            );
            assert_eq!(
                held(&node.store, &nodes).await,
                vec![(1, 0)],
                "one unspent head, the new one: nothing is undone on a delete that did not answer"
            );
            assert!(
                !applied(&node.store, &nodes[1]).await,
                "no applied row for the faulted submit"
            );
            assert_eq!(node.cursor().await, 0, "the UTXO failed: the cursor stays");

            if lands_late {
                node.store
                    .delete_output(&node_txid(&nodes[0]), 0, TOPIC)
                    .await
                    .unwrap();
            }
            calls.borrow_mut().clear();
            assert_eq!(held(&node.store, &nodes).await, vec![(1, 0)]);

            if queue_replays {
                // The replay re-judges head 1. Landed late: no coin, nothing
                // admitted, recorded. Never landed: the genesis is found,
                // head 1's insert is a no-op and the delete is finished.
                let replay =
                    submitted(&head_door(&node.store, &calls), &proven_beef(&nodes[1])).await;
                assert!(replay.is_durable(), "{}", replay.summary());
                assert_eq!(rows(&node.store, &nodes).await, ["1:spent=false"]);
                assert_eq!(
                    applied_rows(&node.store, &nodes).await,
                    [true, true, false, false]
                );
            }
            let (_, sent) = node.tick().await;
            assert_eq!(sent, txids(&nodes, &[3, 2]), "resumed from the held head");
            println!(
                "#559 H1 (lands late {lands_late}, queue {queue_replays}): after the next tick: rows {:?} applied {:?}",
                rows(&node.store, &nodes).await,
                applied_rows(&node.store, &nodes).await
            );
            assert_eq!(held(&node.store, &nodes).await, vec![(3, 0)], "one head");
            // The genesis row outlives a delete that never landed only where
            // nothing replays head 1 (GASP): spent, so no UTXO.
            let expected: &[&str] = if lands_late || queue_replays {
                &["3:spent=false"]
            } else {
                &["0:spent=true", "3:spent=false"]
            };
            assert_eq!(rows(&node.store, &nodes).await, expected);
            assert_eq!(applied_rows(&node.store, &nodes).await, [true; 4]);
            assert_eq!(node.cursor().await, 1);
        }
    }
}

// What a `ToldLookup` was told: the admitted outputs, the spent ones.
type Told = (Rc<RefCell<Vec<String>>>, Rc<RefCell<Vec<String>>>);

// One engine of the `/submit` door with a lookup service that faults once on
// the admission of `faults_on`.
fn told_door(store: &Rc<MemoryStorage>, told: &Told, faults_on: Option<String>) -> Engine {
    Engine::new(
        HashMap::from([(
            TOPIC.to_string(),
            Box::new(HeadChainManager(Rc::new(
                RefCell::new(HeadState::default()),
            ))) as Box<dyn TopicManager>,
        )]),
        HashMap::from([(
            "ls_told".to_string(),
            Box::new(ToldLookup {
                admitted: told.0.clone(),
                spent: told.1.clone(),
                faults_on: RefCell::new(faults_on),
                spend_faults_on: RefCell::new(None),
            }) as Box<dyn bsv_overlay_engine::lookup_service::LookupService>,
        )]),
        Box::new(ScriptedStore::plain(store)),
        None,
        EngineConfig::default(),
    )
}

// H2, the delta lens's X6: one fault, no model. The admission hook of the
// chain-OPENING transaction (the manager admits it with no previous coin)
// faults once after its insert: no applied row, the replay is queued. Head 1
// lands in another invocation and deletes the genesis. On 33e78fb the replay
// of the genesis then admitted it AGAIN, unspent, beside the real tip, for
// good. Now head 1's submit recorded the genesis as applied before deleting
// its coin, so the replay is a dupe.
#[tokio::test]
async fn delta559_h2_x6_a_replayed_opener_is_not_inserted_again_after_its_successor() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(3);
    let store = Rc::new(MemoryStorage::new());
    let told = Told::default();

    let faulted = submitted(
        &told_door(&store, &told, Some(node_txid(&nodes[0]))),
        &proven_beef(&nodes[0]),
    )
    .await;
    assert!(!faulted.is_durable());
    assert_eq!(rows(&store, &nodes).await, ["0:spent=false"]);
    assert_eq!(applied_rows(&store, &nodes).await, [false; 3]);

    let successor = submitted(&told_door(&store, &told, None), &proven_beef(&nodes[1])).await;
    assert!(successor.is_durable(), "{}", successor.summary());
    assert_eq!(rows(&store, &nodes).await, ["1:spent=false"]);

    let replay = submitted(&told_door(&store, &told, None), &proven_beef(&nodes[0])).await;
    println!(
        "#559 H2 X6: after the replay of the genesis: durable {} deduped {:?} rows {:?} applied {:?}",
        replay.is_durable(),
        replay.deduped_topics,
        rows(&store, &nodes).await,
        applied_rows(&store, &nodes).await
    );
    assert_eq!(
        rows(&store, &nodes).await,
        ["1:spent=false"],
        "the spent opener is not back beside the tip"
    );
    assert!(replay.is_durable());
    assert_eq!(replay.deduped_topics, vec![TOPIC.to_string()]);

    assert!(
        submitted(&told_door(&store, &told, None), &proven_beef(&nodes[2]))
            .await
            .is_durable()
    );
    assert_eq!(rows(&store, &nodes).await, ["2:spent=false"]);
    assert_eq!(applied_rows(&store, &nodes).await, [true; 3]);
}

// H2, row 13 of the delta lens's table (by reading there, executed here): the
// APPLIED-ROW write of the opener faults once. Same road as X6.
#[tokio::test]
async fn delta559_h2_a_faulted_applied_row_of_an_opener_does_not_bring_it_back() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(2);
    let store = Rc::new(MemoryStorage::new());
    let calls = Calls::default();
    calls.borrow_mut().push((
        Call::RecordApplied,
        node_txid(&nodes[0]),
        CallEvent::FaultsOnce,
    ));
    let faulted = submitted(&head_door(&store, &calls), &proven_beef(&nodes[0])).await;
    assert!(
        faulted
            .faults
            .iter()
            .any(|f| f.site == "insert_applied_transaction"),
        "{}",
        faulted.summary()
    );
    assert!(
        submitted(&head_door(&store, &calls), &proven_beef(&nodes[1]))
            .await
            .is_durable()
    );
    let replay = submitted(&head_door(&store, &calls), &proven_beef(&nodes[0])).await;
    assert!(replay.is_durable(), "{}", replay.summary());
    assert_eq!(rows(&store, &nodes).await, ["1:spent=false"]);
    assert_eq!(applied_rows(&store, &nodes).await, [true; 2]);
}

// H2, the delta lens's X5 (on the model: a statement lands after its caller
// stopped waiting). The insert of head 1 does not answer inside its finalize
// submit, the undo finds no row, and then the insert LANDS: the genesis
// (spent) beside head 1 (unspent), no applied row, and GASP never replays a
// held head. The chain moves on; then anyone submits head 1 at the door. On
// 33e78fb that submit found the genesis and inserted head 1 again beside the
// tip. Now head 2's submit recorded head 1 as applied.
#[tokio::test]
async fn delta559_h2_x5_an_insert_that_lands_after_its_timeout_is_not_replayed_beside_the_tip() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(4);
    let store = Rc::new(MemoryStorage::new());
    let scripted = ScriptedStore::armed(&store, node_txid(&nodes[1]), 0, InsertEvent::Hangs);
    let outage = scripted.armed.clone();
    let mut node = Budgeted::over(
        RecordingRemote::new(&nodes, &[3]),
        Box::new(HeadChainManager(Rc::new(
            RefCell::new(HeadState::default()),
        ))),
        RequestClock::allowing(u64::MAX - 1),
        store.clone(),
        Box::new(scripted),
        true,
    );
    node.engine
        .set_finalize_submit_budget(due_after_a_few_polls(), 1);
    let ticked = tokio::time::timeout(std::time::Duration::from_secs(10), node.tick()).await;
    assert!(ticked.is_ok(), "the hung insert held the tick");
    assert_eq!(rows(&store, &nodes).await, ["0:spent=true"]);

    // The statement lands late.
    outage.borrow_mut().take();
    let tx = Transaction::from_hex(&nodes[1].raw_tx).unwrap();
    store
        .insert_output(&Output {
            txid: node_txid(&nodes[1]),
            output_index: 0,
            output_script: tx.outputs[0].locking_script.to_binary(),
            satoshis: tx.outputs[0].get_satoshis(),
            topic: TOPIC.to_string(),
            spent: false,
            outputs_consumed: Vec::new(),
            consumed_by: Vec::new(),
            beef: Some(proven_beef(&nodes[1]).beef),
            block_height: None,
            score: Some(1.0),
        })
        .await
        .unwrap();
    assert_eq!(held(&store, &nodes).await, vec![(1, 0)], "one unspent head");

    node.tick().await;
    assert_eq!(held(&store, &nodes).await, vec![(3, 0)]);

    // Anyone submits head 1 again at the door.
    let calls = Calls::default();
    let replay = submitted(&head_door(&store, &calls), &proven_beef(&nodes[1])).await;
    println!(
        "#559 H2 X5: after a /submit of head 1: durable {} deduped {:?} rows {:?} applied {:?}",
        replay.is_durable(),
        replay.deduped_topics,
        rows(&store, &nodes).await,
        applied_rows(&store, &nodes).await
    );
    assert_eq!(held(&store, &nodes).await, vec![(3, 0)], "one head");
    assert_eq!(replay.deduped_topics, vec![TOPIC.to_string()]);
    assert_eq!(applied_rows(&store, &nodes).await, [true; 4]);
}

// H2, the first fold's own two-fault residual, by the road that still undoes
// (since the second delta fold a faulted DELETE undoes nothing): the record
// of the genesis as applied faults, so its coin is not deleted and the insert
// of head 1 is undone, AND that undo faults, so head 1 stays beside the kept
// genesis with no applied row. Head 2 spends it before the replay. On 33e78fb
// the replay of head 1 then found the genesis and inserted head 1 a second
// time. Now it is a dupe.
#[tokio::test]
async fn delta559_h2_the_undo_faulting_too_does_not_bring_a_spent_head_back() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(3);
    let store = Rc::new(MemoryStorage::new());
    let calls = Calls::default();
    assert!(
        submitted(&head_door(&store, &calls), &proven_beef(&nodes[0]))
            .await
            .is_durable()
    );
    calls.borrow_mut().push((
        Call::RecordApplied,
        node_txid(&nodes[0]),
        CallEvent::FaultsOnce,
    ));
    calls
        .borrow_mut()
        .push((Call::Delete, node_txid(&nodes[1]), CallEvent::FaultsOnce));
    let faulted = submitted(&head_door(&store, &calls), &proven_beef(&nodes[1])).await;
    assert!(
        faulted
            .faults
            .iter()
            .any(|f| f.site == "undo_insert_output"),
        "{}",
        faulted.summary()
    );
    assert_eq!(
        rows(&store, &nodes).await,
        ["0:spent=true", "1:spent=false"]
    );
    assert!(
        submitted(&head_door(&store, &calls), &proven_beef(&nodes[2]))
            .await
            .is_durable()
    );
    let replay = submitted(&head_door(&store, &calls), &proven_beef(&nodes[1])).await;
    assert!(replay.is_durable(), "{}", replay.summary());
    assert_eq!(held(&store, &nodes).await, vec![(2, 0)], "one head");
    assert_eq!(replay.deduped_topics, vec![TOPIC.to_string()]);
}

// H2's own write: recording the spent coin's transaction faults. The coin is
// then NOT deleted (its transaction is not yet a dupe for a replay), it is
// read back held and the inserts are undone: the old head, and the replay
// does the whole transaction.
#[tokio::test]
async fn delta559_h2_a_fault_of_the_spent_coins_record_keeps_the_old_head() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(2);
    let store = Rc::new(MemoryStorage::new());
    let calls = Calls::default();
    assert!(
        submitted(&head_door(&store, &calls), &proven_beef(&nodes[0]))
            .await
            .is_durable()
    );
    calls.borrow_mut().push((
        Call::RecordApplied,
        node_txid(&nodes[0]),
        CallEvent::FaultsOnce,
    ));
    let faulted = submitted(&head_door(&store, &calls), &proven_beef(&nodes[1])).await;
    assert!(
        faulted
            .faults
            .iter()
            .any(|f| f.site == "record_spent_coin_applied"),
        "{}",
        faulted.summary()
    );
    assert_eq!(rows(&store, &nodes).await, ["0:spent=true"]);
    assert_eq!(applied_rows(&store, &nodes).await, [true, false]);
    assert!(
        submitted(&head_door(&store, &calls), &proven_beef(&nodes[1]))
            .await
            .is_durable()
    );
    assert_eq!(rows(&store, &nodes).await, ["1:spent=false"]);
    assert_eq!(applied_rows(&store, &nodes).await, [true; 2]);
}

// H3, the delta lens's X8. The genesis and head 1 are applied; the genesis is
// presented again (a client retry, the crawler) and its dedup read faults
// once. On 33e78fb the fault was read as "not a dupe": the opener came back
// unspent beside head 1 with a DURABLE report. Now it is the topic's read
// fault, as the reference fails the topic: nothing written, reported, and
// the replay is a dupe.
#[tokio::test]
async fn delta559_h3_x8_a_faulted_dedup_read_is_a_read_fault_not_a_first_sight() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(3);
    let store = Rc::new(MemoryStorage::new());
    let calls = Calls::default();
    for node in &nodes[0..=1] {
        assert!(submitted(&head_door(&store, &calls), &proven_beef(node))
            .await
            .is_durable());
    }
    calls
        .borrow_mut()
        .push((Call::Applied, node_txid(&nodes[0]), CallEvent::FaultsOnce));
    let again = submitted(&head_door(&store, &calls), &proven_beef(&nodes[0])).await;
    println!(
        "#559 H3 X8: the re-presented genesis: durable {} ({}) rows {:?}",
        again.is_durable(),
        again.summary(),
        rows(&store, &nodes).await
    );
    assert_eq!(rows(&store, &nodes).await, ["1:spent=false"], "one head");
    assert!(
        again
            .faults
            .iter()
            .any(|f| f.site == "does_applied_transaction_exist"),
        "{}",
        again.summary()
    );
    assert!(again.applied_topics.is_empty() && again.deduped_topics.is_empty());

    let replay = submitted(&head_door(&store, &calls), &proven_beef(&nodes[0])).await;
    assert!(replay.is_durable());
    assert_eq!(replay.deduped_topics, vec![TOPIC.to_string()]);
    assert_eq!(rows(&store, &nodes).await, ["1:spent=false"]);
}

// M1, the delta lens's X9. Head 1's insert faults; head 2 arrives in a later
// invocation and ONE read of the predecessor question faults: the applied row
// of head 1, its held outputs, or the coin it spends. On 33e78fb each was
// read as "landed": head 2 was recorded, its replay was a dupe, and the chain
// stopped at head 1 for good. Now a question the store cannot answer is "not
// now".
#[tokio::test]
async fn delta559_m1_x9_a_read_fault_inside_the_predecessor_question_is_not_now() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(4);
    for (read, of) in [
        (Call::Applied, 1),
        (Call::OutputsOf, 1),
        (Call::FindOutput, 0),
    ] {
        let store = Rc::new(MemoryStorage::new());
        let calls = Calls::default();
        let scripted = ScriptedStore::armed(&store, node_txid(&nodes[1]), 0, InsertEvent::Faults);
        let a = door(
            HeadChainManager(Rc::new(RefCell::new(HeadState::default()))),
            scripted,
        );
        assert!(submitted(&a, &proven_beef(&nodes[0])).await.is_durable());
        assert!(!submitted(&a, &proven_beef(&nodes[1])).await.is_durable());
        drop(a);

        calls
            .borrow_mut()
            .push((read, node_txid(&nodes[of]), CallEvent::FaultsOnce));
        let successor = submitted(&head_door(&store, &calls), &unproven_beef(&nodes, 2, 1)).await;
        println!(
            "#559 M1 X9 ({read:?}): head 2: durable {} ({}) applied {:?}",
            successor.is_durable(),
            successor.summary(),
            applied_rows(&store, &nodes).await
        );
        assert!(calls.borrow().is_empty(), "{read:?}: the fault was hit");
        assert!(
            successor
                .faults
                .iter()
                .any(|f| f.site == "predecessor_not_landed"),
            "{read:?}: {}",
            successor.summary()
        );
        assert_eq!(
            applied_rows(&store, &nodes).await,
            [true, false, false, false],
            "{read:?}"
        );

        // The queue replays head 1 and head 2; head 3 follows.
        assert!(
            submitted(&head_door(&store, &calls), &proven_beef(&nodes[1]))
                .await
                .is_durable()
        );
        for k in 2..=3 {
            assert!(
                submitted(&head_door(&store, &calls), &unproven_beef(&nodes, k, 1))
                    .await
                    .is_durable()
            );
        }
        assert_eq!(rows(&store, &nodes).await, ["3:spent=false"], "{read:?}");
        assert_eq!(applied_rows(&store, &nodes).await, [true; 4]);
    }
}

// M2, the delta lens's X7: the read bound of the predecessor question, at its
// boundary. Head 1's insert faults; heads 2 to 8 arrive unproven before its
// replay, each in its own invocation. A single-input ancestor costs three
// reads. On 33e78fb the question stopped at 15, answered "landed" and the
// head past the bound was recorded: after every replay the chain stopped
// behind it for good. Now out of reads is "not now". Since the second delta
// fold of 2026-10-07 the bound counts the reads of bodies the BEEF does not
// prove (head 1 carries its proof here, so its three reads are free): head 7
// spends 15 counted reads on heads 6 to 2 and is answered by the store on
// head 1, head 8 runs out on its 16th counted read.
#[tokio::test]
async fn delta559_m2_x7_the_read_bound_of_the_predecessor_question_is_sixteen_and_not_now() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(10);
    let store = Rc::new(MemoryStorage::new());
    let calls = Calls::default();
    let scripted = ScriptedStore::armed(&store, node_txid(&nodes[1]), 0, InsertEvent::Faults);
    let a = door(
        HeadChainManager(Rc::new(RefCell::new(HeadState::default()))),
        scripted,
    );
    assert!(submitted(&a, &proven_beef(&nodes[0])).await.is_durable());
    assert!(!submitted(&a, &proven_beef(&nodes[1])).await.is_durable());
    drop(a);

    for k in 2..=8 {
        // Head 1's insert stays refused: the landing the door tries first
        // (the E1D lens fold, M1) does not land, so the bound is what is
        // measured.
        let storage = ScriptedStore::armed(&store, node_txid(&nodes[1]), 0, InsertEvent::Faults);
        let reads = storage.reads.clone();
        let engine = door(
            HeadChainManager(Rc::new(RefCell::new(HeadState::default()))),
            storage,
        );
        let r = submitted(&engine, &unproven_beef(&nodes, k, 1)).await;
        // Two reads are the submit's own: the dedup read and the previous
        // coin of its one input. The rest are the question's.
        let asked = reads.get() - 2;
        println!(
            "#559 M2 X7: head {k} before the replay: {asked} reads, durable {} ({})",
            r.is_durable(),
            r.summary()
        );
        assert!(
            r.faults.iter().any(|f| f.site == "predecessor_not_landed"),
            "head {k}: {}",
            r.summary()
        );
        // Up to head 7 the store answers (head 1 has not landed) and the
        // door then submits head 1 first, three reads of that submit's own
        // (its applied row, its coin, the read-back of its refused insert);
        // head 8 runs out of its 16 counted reads on head 2. Each then reads
        // its own outputs once before it answers "not now" (the E1D lens
        // fold, L4).
        assert_eq!(
            asked,
            if k <= 7 { 3 * (k - 1) + 3 + 1 } else { 16 + 1 },
            "head {k}"
        );
        assert_eq!(
            r.summary().contains("is not known to have landed"),
            k == 8,
            "head {k}: {}",
            r.summary()
        );
        assert_eq!(
            applied_rows(&store, &nodes).await[1..],
            [false; 9],
            "head {k} is not recorded"
        );
    }

    // The queue replays everything not durable, in order, then head 9 comes.
    assert!(
        submitted(&head_door(&store, &calls), &proven_beef(&nodes[1]))
            .await
            .is_durable()
    );
    for k in 2..=9 {
        let r = submitted(&head_door(&store, &calls), &unproven_beef(&nodes, k, 1)).await;
        assert!(r.is_durable(), "head {k}: {}", r.summary());
    }
    assert_eq!(rows(&store, &nodes).await, ["9:spent=false"], "the tip");
    assert_eq!(applied_rows(&store, &nodes).await, [true; 10]);
}

// ============================================================================
// The second delta fold of 2026-10-07 (bsv-low #559,
// `docs/audit/E559-delta-2-2026-10-07.md` in bsv-low). M1: nothing is undone
// once a delete of a stale coin was started, answered or not. M2: a GASP
// finalize submit does not ask the store's predecessor question, and at the
// door its read bound counts only bodies neither proven nor landed. L1: the
// limit of that question at a chain's OPENER, pinned as it is. And the pins
// of rows 2 and 6 of the lens's one-head table. The M1 and M2 pins are RED on
// e24f962.
// ============================================================================

// M1, the delta-2 lens's Y1, at the UNBOUNDED door (`/submit`, the queue). The
// delete of the genesis ANSWERS an error inside head 1's submit and the
// statement lands afterwards (modelled: the same delete applied to the store).
// On e24f962 the genesis was read back held and head 1 was taken out again;
// the late landing then left NO head, the queue's replay of head 1 found no
// coin, admitted nothing and was recorded, and so was every later head, for
// good. Now nothing is undone once a delete was started. Each order converges
// to one head: the delete lands late or never, the replay comes before the
// successor or after it.
#[tokio::test]
async fn delta2_559_m1_y1_an_answered_delete_error_that_lands_afterwards_never_leaves_the_chain_headless(
) {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(4);
    for lands_late in [true, false] {
        for replay_first in [true, false] {
            let case = format!("lands late {lands_late}, replay first {replay_first}");
            let store = Rc::new(MemoryStorage::new());
            let calls = Calls::default();
            assert!(
                submitted(&head_door(&store, &calls), &proven_beef(&nodes[0]))
                    .await
                    .is_durable()
            );
            calls
                .borrow_mut()
                .push((Call::Delete, node_txid(&nodes[0]), CallEvent::FaultsOnce));
            let faulted = submitted(&head_door(&store, &calls), &proven_beef(&nodes[1])).await;
            println!(
                "#559 M1 Y1 ({case}): the faulted submit: {} rows {:?} applied {:?}",
                faulted.summary(),
                rows(&store, &nodes).await,
                applied_rows(&store, &nodes).await
            );
            assert!(
                faulted.faults.iter().any(|f| f.site == "delete_utxo_deep"),
                "{case}: {}",
                faulted.summary()
            );
            assert_eq!(
                rows(&store, &nodes).await,
                ["0:spent=true", "1:spent=false"],
                "{case}: nothing is undone on a delete that answered an error"
            );
            assert_eq!(held(&store, &nodes).await, vec![(1, 0)], "{case}");
            assert_eq!(
                applied_rows(&store, &nodes).await,
                [true, false, false, false],
                "{case}: no applied row for the faulted submit"
            );

            if lands_late {
                store
                    .delete_output(&node_txid(&nodes[0]), 0, TOPIC)
                    .await
                    .unwrap();
                assert_eq!(
                    rows(&store, &nodes).await,
                    ["1:spent=false"],
                    "{case}: the late landing leaves the new head"
                );
            }

            if replay_first {
                // Landed late: no coin, nothing admitted, recorded with its
                // output held. Never landed: the genesis is found, the insert
                // is a no-op and the delete is finished.
                let replay = submitted(&head_door(&store, &calls), &proven_beef(&nodes[1])).await;
                assert!(replay.is_durable(), "{case}: {}", replay.summary());
                assert_eq!(rows(&store, &nodes).await, ["1:spent=false"], "{case}");
            }
            let second = submitted(&head_door(&store, &calls), &proven_beef(&nodes[2])).await;
            assert!(second.is_durable(), "{case}: {}", second.summary());
            if !replay_first {
                // The successor recorded head 1 as applied (H2): a dupe.
                let replay = submitted(&head_door(&store, &calls), &proven_beef(&nodes[1])).await;
                assert!(replay.is_durable(), "{case}: {}", replay.summary());
                assert_eq!(replay.deduped_topics, vec![TOPIC.to_string()], "{case}");
            }
            let third = submitted(&head_door(&store, &calls), &proven_beef(&nodes[3])).await;
            assert!(third.is_durable(), "{case}: {}", third.summary());
            println!(
                "#559 M1 Y1 ({case}): END rows {:?} applied {:?}",
                rows(&store, &nodes).await,
                applied_rows(&store, &nodes).await
            );
            assert_eq!(held(&store, &nodes).await, vec![(3, 0)], "{case}: one head");
            // The genesis row outlives a delete that never landed only where
            // the successor came before the replay: spent, so no UTXO.
            let expected: &[&str] = if lands_late || replay_first {
                &["3:spent=false"]
            } else {
                &["0:spent=true", "3:spent=false"]
            };
            assert_eq!(rows(&store, &nodes).await, expected, "{case}");
            assert_eq!(applied_rows(&store, &nodes).await, [true; 4], "{case}");
        }
    }
}

fn proven(tx: &Transaction, height: u32) -> Transaction {
    let mut tx = tx.clone();
    tx.merkle_path = Some(MerklePath::from_hex(&honest_proof(&tx.id(), height)).unwrap());
    tx
}

fn beef_of(tx: &Transaction) -> TaggedBEEF {
    TaggedBEEF::new(tx.to_beef(false).unwrap(), vec![TOPIC.to_string()])
}

// A transaction the head manager admits nothing of (two outputs, no coin of
// the topic), over `parents`, whose bodies and proofs its BEEF carries.
fn noop_over(parents: &[Transaction]) -> Transaction {
    let mut tx = Transaction::new();
    for parent in parents {
        let mut input = TransactionInput::new(parent.id(), 0);
        input.source_transaction = Some(Box::new(parent.clone()));
        tx.inputs.push(input);
    }
    tx.outputs.push(plain_output());
    tx.outputs.push(plain_output());
    tx
}

// `width` proven transactions, each spending `inputs` outpoints nobody holds.
fn strangers(width: usize, inputs: u32) -> Vec<Transaction> {
    (0..width)
        .map(|i| {
            let mut tx = Transaction::new();
            for vout in 0..inputs {
                tx.inputs.push(TransactionInput::new(
                    "11".repeat(32),
                    i as u32 * 100 + vout,
                ));
            }
            tx.outputs.push(TransactionOutput::new(
                2000 + i as u64,
                plain_output().locking_script,
            ));
            proven(&tx, 500 + i as u32)
        })
        .collect()
}

// M2 at the door, the delta-2 lens's Y2. The read bound of the predecessor
// question counted every body the BEEF carries. So a transaction that admits
// nothing and found no coin was "not now" on every submit, with NO unlanded
// predecessor anywhere, in three shapes: over 17 parents that had each
// landed, over six proven single-input parents the topic never saw, and over
// one proven parent with 15 inputs. The same bytes answer the same on every
// retry, so each was three retries and a dead letter, where the reference
// records it. Now a proven body and a landed one cost nothing against the
// bound, and each shape is recorded, on the first submit.
#[tokio::test]
async fn delta2_559_m2_y2_proven_and_landed_bodies_do_not_count_against_the_read_bound() {
    let (_logs, _guard) = capture_logs();
    let mut not_recorded = Vec::new();
    for (shape, parents, landed) in [
        ("17 landed parents", strangers(17, 1), true),
        ("six proven single-input parents", strangers(6, 1), false),
        ("one proven parent with 15 inputs", strangers(1, 15), false),
        // Wider than the 16: these reads are not counted against it. They
        // are 40 of the submit's allowance of 256 (`delta3_559_m1_*`).
        ("40 landed parents", strangers(40, 1), true),
    ] {
        let store = Rc::new(MemoryStorage::new());
        let calls = Calls::default();
        if landed {
            for parent in &parents {
                // Landed under a manager that names nothing: under
                // `HeadChainManager`, which names input 0 of everything, a
                // parent over an outpoint nobody holds is itself "not now"
                // since lane E1D (bsv-low #575).
                let nameless = door(
                    Nameless(HeadChainManager(Rc::new(
                        RefCell::new(HeadState::default()),
                    ))),
                    ScriptedStore::plain(&store),
                );
                let r = submitted(&nameless, &beef_of(parent)).await;
                assert!(r.is_durable(), "{shape}: {}", r.summary());
                assert_eq!(r.applied_topics, vec![TOPIC.to_string()], "{shape}");
            }
        }
        let subject = noop_over(&parents);
        let r = submitted(&head_door(&store, &calls), &beef_of(&subject)).await;
        println!(
            "#559 M2 Y2: {shape}: durable {} applied {:?} ({})",
            r.is_durable(),
            r.applied_topics,
            r.summary()
        );
        if !r.is_durable() || r.applied_topics != vec![TOPIC.to_string()] {
            not_recorded.push(shape);
        }
        assert!(held(&store, &[]).await.is_empty(), "{shape}");
    }
    assert!(
        not_recorded.is_empty(),
        "recorded, as in the reference, every shape but: {not_recorded:?}"
    );
}

// ============================================================================
// The third delta fold of 2026-10-07 (bsv-low #559,
// `docs/audit/E559-delta-3-2026-10-07.md` in bsv-low), M1: the store's
// predecessor question has a hard allowance of 256 reads per SUBMIT, over
// every read it makes (those of proven and landed bodies, which the 16 do
// not count, included) and over every topic of the submit; past it the
// answer is "not now", never "landed". Pins (a) and (c) are RED on 5ecf49c.
// ============================================================================

// One submit of `subject` under `topics`, each hosted by a head manager:
// the report and the store reads the engine made.
async fn counted_submit(
    store: &Rc<MemoryStorage>,
    topics: &[&str],
    subject: &Transaction,
) -> (bsv_overlay_engine::engine::MutationReport, usize) {
    let storage = ScriptedStore::plain(store);
    let reads = storage.reads.clone();
    let engine = Engine::new(
        topics
            .iter()
            .map(|topic| {
                (
                    topic.to_string(),
                    Box::new(HeadChainManager(Rc::new(
                        RefCell::new(HeadState::default()),
                    ))) as Box<dyn TopicManager>,
                )
            })
            .collect(),
        HashMap::new(),
        Box::new(storage),
        None,
        EngineConfig::default(),
    );
    let beef = TaggedBEEF::new(
        subject.to_beef(false).unwrap(),
        topics.iter().map(|topic| topic.to_string()).collect(),
    );
    let report = submitted(&engine, &beef).await;
    (report, reads.get())
}

// M1 (a), the delta-3 lens's measured shape: a transaction that admits
// nothing over 256 PROVEN single-input parents nobody holds. A proven,
// unlanded body costs the question three reads (its applied row, its outputs,
// its one input) and none of them is counted against the 16, so on 5ecf49c
// the question read all 768 (1,025 reads with the submit's own 257) and the
// transaction was recorded: one read per 41 bytes of a stranger's BEEF, with
// no bound of ours. Now the question stops at its 256th read and answers
// "not now": the submit costs `1 + 256 + 256` reads (the dedup read, the
// previous coin of each input, the allowance) and records nothing. The
// boundary, to the read: 85 such parents cost 255 and are recorded, the 86th
// is refused at its second read.
#[tokio::test]
async fn delta3_559_m1_a_the_question_stops_at_256_reads_of_one_submit_and_answers_not_now() {
    let (_logs, _guard) = capture_logs();
    for (width, recorded) in [(85usize, true), (86, false), (256, false)] {
        let store = Rc::new(MemoryStorage::new());
        let subject = noop_over(&strangers(width, 1));
        let (r, reads) = counted_submit(&store, &[TOPIC], &subject).await;
        // The submit's own reads: the dedup read and one previous coin per
        // input. The rest are the question's.
        let asked = reads - 1 - width;
        println!(
            "#559 delta-3 M1 (a): {width} proven parents: {reads} reads, {asked} the question's, durable {} ({})",
            r.is_durable(),
            r.summary()
        );
        assert_eq!(asked, (3 * width).min(256), "{width} parents");
        assert!(reads <= 1 + 256 + 256, "{width} parents: {reads} reads");
        assert_eq!(r.is_durable(), recorded, "{width} parents: {}", r.summary());
        let subject_row = AppliedTransaction {
            txid: subject.id(),
            topic: TOPIC.to_string(),
        };
        assert_eq!(
            store
                .does_applied_transaction_exist(&subject_row)
                .await
                .unwrap(),
            recorded,
            "{width} parents: recorded only when the store answered"
        );
        if recorded {
            assert_eq!(r.applied_topics, vec![TOPIC.to_string()], "{width} parents");
        } else {
            assert!(r.applied_topics.is_empty(), "{width} parents");
            assert_eq!(r.faults.len(), 1, "{width} parents: {}", r.summary());
            assert_eq!(r.faults[0].site, "predecessor_not_landed");
            assert!(
                r.faults[0]
                    .error
                    .contains("the question ran out of the submit's 256 reads"),
                "{width} parents: {}",
                r.summary()
            );
        }
    }
}

// M1 (c): the allowance is the SUBMIT's, not a topic's. A transaction that
// admits nothing over 50 proven single-input parents costs the question 150
// reads in one topic. Under one topic it is recorded. Under two, the first
// topic's question spends 150 and is recorded, the second starts from the
// remaining 106 and runs out: "not now" for that topic alone, the submit
// faults and the replay asks again. On 5ecf49c each topic asked its own 150
// and both were recorded: the cost was per named topic.
#[tokio::test]
async fn delta3_559_m1_c_two_topics_of_one_submit_share_the_allowance() {
    let (_logs, _guard) = capture_logs();
    const SECOND: &str = "tm_head_chain_second";
    let subject = noop_over(&strangers(50, 1));

    let store = Rc::new(MemoryStorage::new());
    let (r, reads) = counted_submit(&store, &[TOPIC], &subject).await;
    assert!(r.is_durable(), "one topic: {}", r.summary());
    assert_eq!(reads, 1 + 50 + 150, "one topic");

    let store = Rc::new(MemoryStorage::new());
    let (r, reads) = counted_submit(&store, &[TOPIC, SECOND], &subject).await;
    println!(
        "#559 delta-3 M1 (c): two topics: {reads} reads, applied {:?} ({})",
        r.applied_topics,
        r.summary()
    );
    // Each topic's own reads (the dedup read, 50 previous coins), then the
    // question: 150 in the first topic, the remaining 106 in the second.
    assert_eq!(reads, 2 * (1 + 50) + 256, "two topics");
    assert_eq!(r.applied_topics, vec![TOPIC.to_string()]);
    assert_eq!(r.faults.len(), 1, "{}", r.summary());
    assert_eq!(r.faults[0].topic, SECOND);
    assert_eq!(r.faults[0].site, "predecessor_not_landed");
    assert!(
        r.faults[0]
            .error
            .contains("the question ran out of the submit's 256 reads"),
        "{}",
        r.summary()
    );
    for (topic, recorded) in [(TOPIC, true), (SECOND, false)] {
        let row = AppliedTransaction {
            txid: subject.id(),
            topic: topic.to_string(),
        };
        assert_eq!(
            store.does_applied_transaction_exist(&row).await.unwrap(),
            recorded,
            "{topic}"
        );
    }
}

// Admits output 0 of a transaction whose output 0 carries 777 satoshis, on
// that shape alone, and names no input: every other transaction of a graph
// is one it admits nothing of.
struct TipShapeManager;

#[async_trait(?Send)]
impl TopicManager for TipShapeManager {
    fn reads_off_chain_values(&self) -> bool {
        false
    }

    async fn identify_admissible_outputs(
        &self,
        tx: &Transaction,
        _previous_coins: &[u8],
        _off_chain_values: Option<&[u8]>,
        _mode: SubmitMode,
        _context: &TopicAdmittanceContext,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        let is_tip = tx.outputs.first().is_some_and(|o| o.get_satoshis() == 777);
        Ok(AdmittanceInstructions {
            outputs_to_admit: if is_tip { vec![0] } else { Vec::new() },
            ..Default::default()
        })
    }
    async fn identify_needed_inputs(
        &self,
        _beef: &[u8],
        _off_chain_values: Option<&[u8]>,
    ) -> Result<Vec<Outpoint>, TopicManagerError> {
        Ok(Vec::new())
    }
    async fn get_documentation(&self) -> String {
        String::new()
    }
    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata::default()
    }
}

fn op_1_output(satoshis: u64) -> TransactionOutput {
    TransactionOutput::new(satoshis, LockingScript::from_hex("51").unwrap())
}

// One tick of a node over `nodes`, the last of them listed as the peer's only
// UTXO: the topic's result, the store, the applied rows the engine read.
async fn tip_shape_tick(
    nodes: &[GASPNode],
    budgeted: bool,
) -> (
    bsv_overlay_engine::engine::TopicSyncResult,
    Budgeted,
    Vec<String>,
) {
    let store = Rc::new(MemoryStorage::new());
    let scripted = ScriptedStore::plain(&store);
    let applied_reads = scripted.applied_reads.clone();
    let node = Budgeted::over(
        RecordingRemote::new(nodes, &[nodes.len() - 1]),
        Box::new(TipShapeManager),
        RequestClock::allowing(u64::MAX - 1),
        store,
        Box::new(scripted),
        budgeted,
    );
    let (topic, _) = node.tick().await;
    let read = applied_reads.borrow().clone();
    (topic, node, read)
}

// M2 under GASP, the stall the delta-2 lens read. A finalize submits EVERY
// node of a graph, ancestors first, an unproven node with the bodies of its
// parents, and a node that does not land ends its graph and holds the cursor.
// The graph: a listed tip over an unproven transaction W the manager admits
// nothing of, over 17 proven parents it admits nothing of either. On e24f962
// W found no coin, the store's question read the applied rows of its 17
// parents (each recorded a moment before), ran out at the 17th and answered
// "not now": the tip was never submitted and the cursor stayed, on every
// tick, until the peer served W proven. Now a finalize submit does not ask
// the store at all: each applied row is read once, by its own dedup read.
#[tokio::test]
async fn delta2_559_m2_a_finalize_over_a_node_with_seventeen_landed_parents_completes() {
    let (_logs, _guard) = capture_logs();
    let parents: Vec<Transaction> = (0..17u32)
        .map(|i| {
            let mut tx = Transaction::new();
            tx.inputs.push(spending("11".repeat(32), i));
            tx.outputs.push(op_1_output(1000));
            tx
        })
        .collect();
    let mut wide = Transaction::new();
    for parent in &parents {
        wide.inputs.push(spending(parent.id(), 0));
    }
    wide.outputs.push(op_1_output(5000));
    let mut tip = Transaction::new();
    tip.inputs.push(spending(wide.id(), 0));
    tip.outputs.push(op_1_output(777));

    let mut nodes: Vec<GASPNode> = parents
        .iter()
        .enumerate()
        .map(|(i, p)| node_of(p, 0, Some(honest_proof(&p.id(), 500 + i as u32))))
        .collect();
    nodes.push(node_of(&wide, 0, None));
    nodes.push(node_of(&tip, 0, None));

    for budgeted in [true, false] {
        let (topic, node, read) = tip_shape_tick(&nodes, budgeted).await;
        println!(
            "#559 M2 GASP wide (budgeted {budgeted}): finalized {} discarded {} utxos {:?} cursor {} applied reads {}",
            topic.finalized_graphs,
            topic.discarded_graphs,
            held(&node.store, &nodes).await,
            node.cursor().await,
            read.len()
        );
        assert_eq!(
            held(&node.store, &nodes).await,
            vec![(18, 0)],
            "budgeted {budgeted}: the tip is admitted"
        );
        assert_eq!(
            applied_rows(&node.store, &nodes).await,
            [true; 19],
            "budgeted {budgeted}: every node of the graph landed"
        );
        assert_eq!(
            node.cursor().await,
            1,
            "budgeted {budgeted}: the cursor moved"
        );
        for n in &nodes {
            let txid = node_txid(n);
            assert_eq!(
                read.iter().filter(|t| **t == txid).count(),
                1,
                "budgeted {budgeted}: one read of an applied row per node, its own dedup read"
            );
        }
    }
}

// M2 under GASP, depth: a finalize over a 20-deep unproven chain of
// transactions the manager admits nothing of, below a listed tip, completes
// in one tick, and no node's applied row is read by a question (each is read
// once, by its own dedup read; the pin counts applied-row reads ONLY, which
// is enough: the question's first read of a body is always that row).
// "Completes" held on e24f962 too (a
// single-input chain cost the question one read per node there, its parent
// having landed a moment before); what is new is that the question is not
// asked.
#[tokio::test]
async fn delta2_559_m2_a_finalize_over_a_twenty_deep_chain_completes_and_asks_the_store_nothing() {
    let (_logs, _guard) = capture_logs();
    let mut nodes = Vec::new();
    let mut previous: Option<String> = None;
    for height in 0..=21 {
        let mut tx = Transaction::new();
        if let Some(txid) = previous {
            tx.inputs.push(spending(txid, 0));
        } else {
            tx.inputs.push(coinbase_input());
        }
        tx.outputs
            .push(op_1_output(if height == 21 { 777 } else { 1000 }));
        let txid = tx.id();
        let proof = (height == 0).then(|| honest_proof(&txid, 100));
        nodes.push(node_of(&tx, 0, proof));
        previous = Some(txid);
    }

    for budgeted in [true, false] {
        let (topic, node, read) = tip_shape_tick(&nodes, budgeted).await;
        println!(
            "#559 M2 GASP deep (budgeted {budgeted}): finalized {} utxos {:?} cursor {} applied reads {}",
            topic.finalized_graphs,
            held(&node.store, &nodes).await,
            node.cursor().await,
            read.len()
        );
        assert_eq!(held(&node.store, &nodes).await, vec![(21, 0)]);
        assert_eq!(applied_rows(&node.store, &nodes).await, [true; 22]);
        assert_eq!(node.cursor().await, 1);
        for n in &nodes {
            let txid = node_txid(n);
            assert_eq!(
                read.iter().filter(|t| **t == txid).count(),
                1,
                "budgeted {budgeted}: no question reads an applied row inside a finalize"
            );
        }
    }
}

// Row 2 of the one-head table: the validation read of the previous coin
// faults. The manager judged without a coin the store holds, so NOTHING is
// written on that judgement: the old head stays, UNSPENT (the mark was never
// reached), there is no applied row, and the replay lands the new head.
#[tokio::test]
async fn delta2_559_row2_a_previous_coin_read_fault_writes_nothing_and_keeps_the_old_head_unspent()
{
    let (_logs, _guard) = capture_logs();
    let nodes = chain(2);
    let store = Rc::new(MemoryStorage::new());
    let calls = Calls::default();
    assert!(
        submitted(&head_door(&store, &calls), &proven_beef(&nodes[0]))
            .await
            .is_durable()
    );
    calls.borrow_mut().push((
        Call::FindOutput,
        node_txid(&nodes[0]),
        CallEvent::FaultsOnce,
    ));
    let faulted = submitted(&head_door(&store, &calls), &proven_beef(&nodes[1])).await;
    assert!(calls.borrow().is_empty(), "the fault was hit");
    assert_eq!(
        faulted.faults.iter().map(|f| f.site).collect::<Vec<_>>(),
        ["find_output"],
        "{}",
        faulted.summary()
    );
    assert!(faulted.applied_topics.is_empty());
    assert_eq!(
        rows(&store, &nodes).await,
        ["0:spent=false"],
        "nothing written: the old head is not even marked spent"
    );
    assert_eq!(applied_rows(&store, &nodes).await, [true, false]);

    let replay = submitted(&head_door(&store, &calls), &proven_beef(&nodes[1])).await;
    assert!(replay.is_durable(), "{}", replay.summary());
    assert_eq!(rows(&store, &nodes).await, ["1:spent=false"]);
    assert_eq!(applied_rows(&store, &nodes).await, [true; 2]);
}

// Row 6 of the one-head table: the lookup service's `output_spent` hook
// faults on the spend of the old head. The fault is reported and the submit
// carries on: the new head in, the old one deleted, no applied row. The
// replay finds no coin, admits nothing and is recorded; or, when a successor
// came first, it is a dupe (H2). One head either way. The limit this pins
// too: the hook that faulted is not told of that spend again.
#[tokio::test]
async fn delta2_559_row6_a_spend_notification_fault_leaves_the_new_head_and_no_applied_row() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(3);
    for successor_first in [false, true] {
        let store = Rc::new(MemoryStorage::new());
        let told = Told::default();
        let invocation = |spend_faults_on: Option<String>| {
            Engine::new(
                HashMap::from([(
                    TOPIC.to_string(),
                    Box::new(HeadChainManager(Rc::new(
                        RefCell::new(HeadState::default()),
                    ))) as Box<dyn TopicManager>,
                )]),
                HashMap::from([(
                    "ls_told".to_string(),
                    Box::new(ToldLookup {
                        admitted: told.0.clone(),
                        spent: told.1.clone(),
                        faults_on: RefCell::new(None),
                        spend_faults_on: RefCell::new(spend_faults_on),
                    })
                        as Box<dyn bsv_overlay_engine::lookup_service::LookupService>,
                )]),
                Box::new(ScriptedStore::plain(&store)),
                None,
                EngineConfig::default(),
            )
        };
        assert!(submitted(&invocation(None), &proven_beef(&nodes[0]))
            .await
            .is_durable());
        let faulted = submitted(
            &invocation(Some(node_txid(&nodes[0]))),
            &proven_beef(&nodes[1]),
        )
        .await;
        assert_eq!(
            faulted.faults.iter().map(|f| f.site).collect::<Vec<_>>(),
            ["lookup_service.output_spent"],
            "{}",
            faulted.summary()
        );
        assert_eq!(
            rows(&store, &nodes).await,
            ["1:spent=false"],
            "successor first {successor_first}: the new head in, the old one deleted"
        );
        assert_eq!(applied_rows(&store, &nodes).await, [true, false, false]);

        if successor_first {
            assert!(submitted(&invocation(None), &proven_beef(&nodes[2]))
                .await
                .is_durable());
        }
        let replay = submitted(&invocation(None), &proven_beef(&nodes[1])).await;
        assert!(replay.is_durable(), "{}", replay.summary());
        if successor_first {
            assert_eq!(replay.deduped_topics, vec![TOPIC.to_string()]);
        } else {
            assert_eq!(replay.applied_topics, vec![TOPIC.to_string()]);
            assert!(submitted(&invocation(None), &proven_beef(&nodes[2]))
                .await
                .is_durable());
        }
        assert_eq!(rows(&store, &nodes).await, ["2:spent=false"], "one head");
        assert_eq!(applied_rows(&store, &nodes).await, [true; 3]);
        assert_eq!(
            *told.1.borrow(),
            txids(&nodes, &[1]),
            "the limit: the spend that faulted is not told again"
        );
    }
}

// ============================================================================
// Lane E1D (bsv-low #575, zanaadu-v2 #365): a successor is never recorded
// over a predecessor whose landing is UNKNOWN, and the GASP walk re-asks the
// predecessor first. The Zanaadu captain's lens on e1c: after a refused D1
// batch, a PROVEN successor was recorded over the predecessor that never
// landed (admitted with no coin, or admitting nothing), and when the
// predecessor landed later its head stood unspent beside the tip for good.
// Each pin below is RED on `8d147d7`.
// ============================================================================

// A head manager that admits on SHAPE (a transaction of exactly one output,
// whatever it spends: the pf head manager's behaviour over its own head
// state), retains no coin, and NAMES its witness input (input 0) as overlay
// history (D13). It needs no coin to admit, so a successor whose
// predecessor has not landed is ADMITTED with no coin.
struct ShapeHeadManager {
    names: bool,
}

#[async_trait(?Send)]
impl TopicManager for ShapeHeadManager {
    fn reads_off_chain_values(&self) -> bool {
        false
    }

    async fn identify_admissible_outputs(
        &self,
        tx: &Transaction,
        _previous_coins: &[u8],
        _off_chain_values: Option<&[u8]>,
        _mode: SubmitMode,
        _context: &TopicAdmittanceContext,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        Ok(AdmittanceInstructions {
            outputs_to_admit: if tx.outputs.len() == 1 {
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
        if !self.names {
            return Ok(Vec::new());
        }
        let tx = Transaction::from_beef(beef, None).unwrap();
        Ok(tx
            .inputs
            .first()
            .filter(|i| spends_a_coin(i))
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

fn shape_door(store: &Rc<MemoryStorage>, names: bool) -> Engine {
    door(ShapeHeadManager { names }, ScriptedStore::plain(store))
}

// Zanaadu's run, on the GASP path. Four proven heads, the peer lists heads
// 1, 2 and 3 (each its own UTXO, as in the lens's run); the genesis is held.
// Tick 1: every insert of head 1 is REFUSED (a D1 batch refused for the
// tick). On the base the walk stopped at head 2 (its output is admitted by
// the no-coin dry run), the graph was head 2 alone and its finalize submit
// admitted it with no coin and recorded it, head 3 over it the same; tick 2
// then landed head 1 over the genesis, unspent beside the tip, for good.
// Now the walk asks the manager's named inputs of an admitted node too and
// requests the one whose landing is unknown: head 1 joins the graphs of
// heads 2 and 3, its submit faults, each graph stops before its successor
// and its UTXO waits. Tick 2 lands head 1 and then heads 2 and 3: one head,
// every applied row true.
#[tokio::test]
async fn e1d_a_gasp_a_refused_predecessor_is_walked_again_before_its_proven_successor() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(4);
    for budgeted in [true, false] {
        let store = Rc::new(MemoryStorage::new());
        let scripted = ScriptedStore::armed(&store, node_txid(&nodes[1]), 0, InsertEvent::Faults);
        let outage = scripted.armed.clone();
        let node = Budgeted::over(
            RecordingRemote::new(&nodes, &[1, 2, 3]),
            Box::new(ShapeHeadManager { names: true }),
            RequestClock::allowing(u64::MAX - 1),
            store,
            Box::new(scripted),
            budgeted,
        );
        let case = format!("budgeted={budgeted}");
        let genesis = node
            .engine
            .submit_with_report(&proven_beef(&nodes[0]), SubmitMode::HistoricalTxNoSpv)
            .await
            .unwrap()
            .1;
        assert!(genesis.is_durable(), "{case}");

        // Tick 1, the refusal on.
        let (_, sent) = node.tick().await;
        println!(
            "E1D GASP {case}: tick 1 sent {sent:?}; rows {:?} applied {:?}",
            rows(&node.store, &nodes).await,
            applied_rows(&node.store, &nodes).await
        );
        assert_eq!(
            applied_rows(&node.store, &nodes).await,
            [true, false, false, false],
            "{case}: no successor is recorded over head 1, which did not land"
        );
        assert_eq!(rows(&node.store, &nodes).await, ["0:spent=true"], "{case}");
        assert_eq!(node.cursor().await, 0, "{case}: the cursor waits");

        // The refusal ends: the next tick lands head 1, then 2, then 3.
        outage.borrow_mut().take();
        node.tick().await;
        println!(
            "E1D GASP {case}: tick 2 rows {:?} applied {:?}",
            rows(&node.store, &nodes).await,
            applied_rows(&node.store, &nodes).await
        );
        assert_eq!(
            rows(&node.store, &nodes).await,
            ["3:spent=false"],
            "{case}: one head, nothing doubled"
        );
        assert_eq!(applied_rows(&node.store, &nodes).await, [true; 4], "{case}");
        assert_eq!(held(&node.store, &nodes).await, vec![(3, 0)], "{case}");

        // And it stays so.
        node.tick().await;
        assert_eq!(rows(&node.store, &nodes).await, ["3:spent=false"], "{case}");
    }
}

// The same refusal at the `/submit` door (the queue's replay), each
// transaction in an invocation of its own, the successor PROVEN so its BEEF
// does not carry the predecessor's body. Class A, a manager that needs the
// coin (it admits nothing without it): on the base head 2 was recorded and
// its replay was a dupe, the chain stopping at head 1. Class B, a manager
// that admits on shape: on the base head 2 was admitted with no coin and
// recorded, and head 1's replay stood unspent beside it. Now the manager's
// named input answers: head 1 has no applied row and holds no output, so
// head 2 is "not now" (nothing written) until head 1 lands, and the replays
// converge to one head.
#[tokio::test]
async fn e1d_b_door_a_proven_successor_waits_for_a_predecessor_whose_landing_is_unknown() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(3);
    for class in ["A: admits nothing without the coin", "B: admits on shape"] {
        let shape = class.starts_with('B');
        let store = Rc::new(MemoryStorage::new());
        let calls = Calls::default();
        let invocation = |faulting: Option<String>| {
            let storage = match faulting {
                Some(txid) => ScriptedStore::armed(&store, txid, 0, InsertEvent::Faults),
                None => {
                    let mut s = ScriptedStore::plain(&store);
                    s.calls = calls.clone();
                    s
                }
            };
            if shape {
                door(ShapeHeadManager { names: true }, storage)
            } else {
                door(
                    HeadChainManager(Rc::new(RefCell::new(HeadState::default()))),
                    storage,
                )
            }
        };
        let a = invocation(Some(node_txid(&nodes[1])));
        assert!(submitted(&a, &proven_beef(&nodes[0])).await.is_durable());
        assert!(!submitted(&a, &proven_beef(&nodes[1])).await.is_durable());
        drop(a);

        let successor = submitted(&invocation(None), &proven_beef(&nodes[2])).await;
        println!(
            "E1D door {class}: head 2 before head 1's replay: durable {} applied {:?} ({}); rows {:?}",
            successor.is_durable(),
            successor.applied_topics,
            successor.summary(),
            rows(&store, &nodes).await
        );
        assert!(
            successor
                .faults
                .iter()
                .any(|f| f.site == "predecessor_not_landed"),
            "{class}: {}",
            successor.summary()
        );
        assert_eq!(
            applied_rows(&store, &nodes).await,
            [true, false, false],
            "{class}"
        );
        assert_eq!(
            rows(&store, &nodes).await,
            ["0:spent=true"],
            "{class}: nothing written"
        );

        // The queue replays head 1, then head 2.
        assert!(submitted(&invocation(None), &proven_beef(&nodes[1]))
            .await
            .is_durable());
        let replayed = submitted(&invocation(None), &proven_beef(&nodes[2])).await;
        assert!(replayed.is_durable(), "{class}: {}", replayed.summary());
        assert_eq!(
            rows(&store, &nodes).await,
            ["2:spent=false"],
            "{class}: one head"
        );
        assert_eq!(applied_rows(&store, &nodes).await, [true; 3], "{class}");
    }
}

// The opener (D17's stated limit, `limit_opener`, now cured). The genesis
// spends no coin of the topic, so "unlanded" never saw it. Now the manager is
// asked of the candidate body itself, a dry run with no coins over the output
// it NAMES (`HeadChainManager` names input 0, the genesis output): it would
// admit it, so the genesis is a predecessor that has not landed. With its
// body in the BEEF the door lands it FIRST (the E1D lens fold, M1) and head 1
// is then recorded over its coin, one submit; without it head 1 is "not now"
// (the manager's named input) until the replay lands the genesis.
#[tokio::test]
async fn e1d_c_a_successor_of_an_unlanded_opener_waits_with_or_without_its_body() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(3);
    for carried in [true, false] {
        let store = Rc::new(MemoryStorage::new());
        let calls = Calls::default();
        let scripted = ScriptedStore::armed(&store, node_txid(&nodes[0]), 0, InsertEvent::Faults);
        let a = door(
            HeadChainManager(Rc::new(RefCell::new(HeadState::default()))),
            scripted,
        );
        assert!(!submitted(&a, &proven_beef(&nodes[0])).await.is_durable());
        drop(a);
        assert!(rows(&store, &nodes).await.is_empty());

        let beef = |i: usize| {
            if carried {
                unproven_beef(&nodes, i, 0)
            } else {
                proven_beef(&nodes[i])
            }
        };
        let successor = submitted(&head_door(&store, &calls), &beef(1)).await;
        println!(
            "E1D opener carried={carried}: head 1 over an unlanded opener: durable {} applied {:?} ({})",
            successor.is_durable(),
            successor.applied_topics,
            successor.summary()
        );
        if carried {
            assert!(successor.is_durable(), "{}", successor.summary());
            assert_eq!(successor.applied_topics, vec![TOPIC.to_string()]);
            assert_eq!(rows(&store, &nodes).await, ["1:spent=false"]);
            assert_eq!(applied_rows(&store, &nodes).await, [true, true, false]);
        } else {
            assert!(
                successor
                    .faults
                    .iter()
                    .any(|f| f.site == "predecessor_not_landed"),
                "{}",
                successor.summary()
            );
            assert_eq!(applied_rows(&store, &nodes).await, [false; 3]);
            assert!(
                submitted(&head_door(&store, &calls), &proven_beef(&nodes[0]))
                    .await
                    .is_durable()
            );
            assert!(submitted(&head_door(&store, &calls), &beef(1))
                .await
                .is_durable());
        }
        assert!(submitted(&head_door(&store, &calls), &beef(2))
            .await
            .is_durable());
        assert_eq!(
            rows(&store, &nodes).await,
            ["2:spent=false"],
            "carried={carried}"
        );
        assert_eq!(
            applied_rows(&store, &nodes).await,
            [true; 3],
            "carried={carried}"
        );
    }
}

// What did NOT change, stated. A manager that admits on shape and names
// NOTHING (every one of the workspace's 16) has said no input is its
// history: its successor admitted with no coin over an unlanded predecessor
// is recorded at the door as in the reference (and as on the base). A
// transaction that admits nothing over parents the manager would not admit
// with no coin (a funding ancestry the topic never saw) is recorded too.
#[tokio::test]
async fn e1d_d_a_manager_that_names_nothing_is_answered_as_in_the_reference() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(3);
    let store = Rc::new(MemoryStorage::new());
    let scripted = ScriptedStore::armed(&store, node_txid(&nodes[1]), 0, InsertEvent::Faults);
    let a = door(ShapeHeadManager { names: false }, scripted);
    assert!(submitted(&a, &proven_beef(&nodes[0])).await.is_durable());
    assert!(!submitted(&a, &proven_beef(&nodes[1])).await.is_durable());
    drop(a);
    let successor = submitted(&shape_door(&store, false), &proven_beef(&nodes[2])).await;
    assert!(successor.is_durable(), "{}", successor.summary());
    assert_eq!(successor.applied_topics, vec![TOPIC.to_string()]);

    // A spend that admits nothing of a SHAPE-admissible parent nobody
    // submitted here (the E1D lens fold, H1: a revocation of an ad this node
    // never held; a stranger's own few-sat output), the parent unproven over
    // a proven funding, or two hops up: recorded, as in the reference. On
    // `683dffd` the no-coin dry run of the parent made it "not now" for good.
    let funding = strangers(1, 1).remove(0);
    let one_output_over = |parent: &Transaction| {
        let mut tx = Transaction::new();
        let mut input = TransactionInput::new(parent.id(), 0);
        input.source_transaction = Some(Box::new(parent.clone()));
        tx.inputs.push(input);
        tx.outputs.push(plain_output());
        tx
    };
    let ad = one_output_over(&funding);
    let hop = one_output_over(&ad);
    for (case, parent) in [("unproven parent", &ad), ("two hops", &hop)] {
        let store = Rc::new(MemoryStorage::new());
        let revoke = noop_over(std::slice::from_ref(parent));
        for attempt in 0..2 {
            let r = submitted(&shape_door(&store, false), &beef_of(&revoke)).await;
            assert!(r.is_durable(), "{case} attempt {attempt}: {}", r.summary());
        }
        let r = submitted(&shape_door(&store, false), &beef_of(&revoke)).await;
        assert_eq!(r.deduped_topics, vec![TOPIC.to_string()], "{case}");
    }

    let stranger_store = Rc::new(MemoryStorage::new());
    let subject = noop_over(&strangers(3, 2));
    let r = submitted(
        &head_door(&stranger_store, &Calls::default()),
        &beef_of(&subject),
    )
    .await;
    assert!(r.is_durable(), "{}", r.summary());
    assert_eq!(r.applied_topics, vec![TOPIC.to_string()]);
}

// A manager that names nothing, `M` otherwise.
struct Nameless<M>(M);

#[async_trait(?Send)]
impl<M: TopicManager> TopicManager for Nameless<M> {
    fn reads_off_chain_values(&self) -> bool {
        self.0.reads_off_chain_values()
    }

    async fn identify_admissible_outputs(
        &self,
        tx: &Transaction,
        previous_coins: &[u8],
        off_chain_values: Option<&[u8]>,
        mode: SubmitMode,
        context: &TopicAdmittanceContext,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        self.0
            .identify_admissible_outputs(tx, previous_coins, off_chain_values, mode, context)
            .await
    }

    async fn get_documentation(&self) -> String {
        String::new()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata::default()
    }
}

// The walk's rule for an ADMITTED proven node, one storage state at a time:
// its named input is requested only while its landing is unknown. Held (a
// coin of the topic), or landed (an applied row, or any output held in the
// topic), the walk stops as the reference's.
#[tokio::test]
async fn e1d_e_an_admitted_node_requests_only_a_named_input_whose_landing_is_unknown() {
    let nodes = chain(2);
    let named = Outpoint::new(node_txid(&nodes[0]), 0);
    let manager = ProbeManager {
        outputs: vec![0],
        named: vec![named.clone()],
        ..Default::default()
    };
    let parent = Transaction::from_hex(&nodes[0].raw_tx).unwrap();
    let parent_output = |output_index: u32, topic: &str| Output {
        txid: parent.id(),
        output_index,
        output_script: plain_output().locking_script.to_binary(),
        satoshis: 1000,
        topic: topic.to_string(),
        spent: true,
        outputs_consumed: vec![],
        consumed_by: vec![],
        beef: None,
        block_height: None,
        score: Some(1.0),
    };
    for (state, requested) in [
        ("unknown", true),
        ("held", false),
        ("applied", false),
        ("another output held", false),
        ("held in another topic only", true),
    ] {
        let store = MemoryStorage::new();
        match state {
            "held" => store.insert_output(&parent_output(0, TOPIC)).await.unwrap(),
            "applied" => store
                .insert_applied_transaction(&AppliedTransaction {
                    txid: parent.id(),
                    topic: TOPIC.to_string(),
                })
                .await
                .unwrap(),
            "another output held" => store.insert_output(&parent_output(1, TOPIC)).await.unwrap(),
            "held in another topic only" => store
                .insert_output(&parent_output(0, "tm_other"))
                .await
                .unwrap(),
            _ => {}
        }
        let adapter = OverlayGASPStorage::new(&store, TOPIC, new_finalized_graph_sink())
            .with_topic_manager(&manager);
        let response = adapter.find_needed_inputs(&nodes[1]).await.unwrap();
        assert_eq!(
            response.map(|r| r.requested_inputs.keys().cloned().collect::<Vec<_>>()),
            requested.then(|| vec![named.to_graph_id()]),
            "{state}"
        );
    }
}

// ============================================================================
// The E1D lens fold (lens `docs/audit/E1D-lens-2026-10-08.md` on `683dffd`):
// H1, M1, M2. Each pin below is RED on `683dffd`.
// ============================================================================

// H1, the lens's own scratch test. A revocation (it admits nothing) of an
// ad this node never held: the ad's output is shape-admissible, so on
// `683dffd` the opener dry run took the ad for an unlanded predecessor and
// the revocation was "not now" on every presentation, a queued 200, three
// replays and a dead letter, for a manager that names nothing (every one of
// the workspace's 16). Now a dry run asks only of an output the manager
// NAMES: the revocation is recorded at once, as in the reference, and its
// replays are dupes.
#[tokio::test]
async fn e1d_fold_h1_a_revocation_of_an_ad_this_node_never_held_is_recorded() {
    let (_logs, _guard) = capture_logs();
    let mut ad = Transaction::new();
    ad.inputs.push(TransactionInput::new("22".repeat(32), 0));
    ad.outputs
        .push(TransactionOutput::new(1000, plain_output().locking_script));
    let ad = proven(&ad, 900);
    let revoke = noop_over(&[ad]);
    let store = Rc::new(MemoryStorage::new());
    for attempt in 0..4 {
        let r = submitted(&shape_door(&store, false), &beef_of(&revoke)).await;
        println!(
            "E1D fold H1 attempt {attempt}: durable {} applied {:?} ({})",
            r.is_durable(),
            r.applied_topics,
            r.summary()
        );
        assert!(r.is_durable(), "attempt {attempt}: {}", r.summary());
        if attempt == 0 {
            assert_eq!(r.applied_topics, vec![TOPIC.to_string()]);
        } else {
            assert_eq!(
                r.deduped_topics,
                vec![TOPIC.to_string()],
                "attempt {attempt}"
            );
        }
    }
}

// M2. The manager names the SUBJECT's inputs even when the client's BEEF
// puts the subject before its parent in wire order (the subject is found by
// `subject.rs`, never by wire order). Head 1's insert is refused, then head
// 2 arrives unproven carrying head 1, subject FIRST on the wire. On
// `683dffd` the raw bytes went to `identify_needed_inputs`, whose
// `from_beef(_, None)` took the wire-last transaction, head 1, and named the
// genesis output: no input of head 2 was named, nothing was asked, and head
// 2 was admitted with no coin and recorded; head 1's replay then stood
// beside it. Now the subject-named BEEF goes to the manager: head 1 is named,
// found unlanded and landed first (M1), and head 2 lands over its coin.
#[tokio::test]
async fn e1d_fold_m2_an_out_of_order_beef_names_the_subjects_own_inputs() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(3);
    let store = Rc::new(MemoryStorage::new());
    let a = door(
        ShapeHeadManager { names: true },
        ScriptedStore::armed(&store, node_txid(&nodes[1]), 0, InsertEvent::Faults),
    );
    assert!(submitted(&a, &proven_beef(&nodes[0])).await.is_durable());
    assert!(!submitted(&a, &proven_beef(&nodes[1])).await.is_durable());
    drop(a);

    let mut beef = Beef::from_binary(&unproven_beef(&nodes, 2, 0).beef).unwrap();
    beef.txs.rotate_right(1);
    assert_eq!(
        beef.txs[0].txid(),
        node_txid(&nodes[2]),
        "the subject is wire-first"
    );
    // `to_writer`, not `to_binary`: the latter sorts the transactions back.
    let mut wire = bsv_rs::primitives::Writer::new();
    beef.to_writer(&mut wire);
    let out_of_order = TaggedBEEF::new(wire.into_bytes(), vec![TOPIC.to_string()]);
    assert_eq!(
        Transaction::from_beef(&out_of_order.beef, None)
            .unwrap()
            .id(),
        node_txid(&nodes[1]),
        "`from_beef(_, None)` over these bytes takes the parent, wire-last"
    );
    let r = submitted(&shape_door(&store, true), &out_of_order).await;
    println!(
        "E1D fold M2: head 2 subject-first: durable {} applied {:?} ({}); rows {:?}",
        r.is_durable(),
        r.applied_topics,
        r.summary(),
        rows(&store, &nodes).await
    );
    assert!(r.is_durable(), "{}", r.summary());
    assert_eq!(rows(&store, &nodes).await, ["2:spent=false"], "one head");
    assert_eq!(applied_rows(&store, &nodes).await, [true; 3]);

    // The replay of head 1 that comes later is a dupe: still one head.
    let replay = submitted(&shape_door(&store, true), &proven_beef(&nodes[1])).await;
    assert_eq!(replay.deduped_topics, vec![TOPIC.to_string()]);
    assert_eq!(rows(&store, &nodes).await, ["2:spent=false"]);
}

// M1, the body carried. An ADMITTING successor (the pf head manager's
// class) over a predecessor whose submit faulted and is never replayed (its
// dead letter, a single node, no GASP peer). On `683dffd` the successor was
// "not now" on every presentation (the S2 queued ack, three replays, the
// dead letter), and so was every later head: the chain froze at the door.
// Now the door submits the carried predecessor FIRST: one submit lands both,
// and the presentations after it are dupes. A chain of six unlanded heads
// carried by the seventh lands ancestors first, whole, in one submit.
#[tokio::test]
async fn e1d_fold_m1_a_the_door_lands_a_carried_predecessor_first() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(3);
    let store = Rc::new(MemoryStorage::new());
    let a = door(
        ShapeHeadManager { names: true },
        ScriptedStore::armed(&store, node_txid(&nodes[1]), 0, InsertEvent::Faults),
    );
    assert!(submitted(&a, &proven_beef(&nodes[0])).await.is_durable());
    assert!(!submitted(&a, &proven_beef(&nodes[1])).await.is_durable());
    drop(a);
    for attempt in 0..4 {
        let r = submitted(&shape_door(&store, true), &unproven_beef(&nodes, 2, 0)).await;
        println!(
            "E1D fold M1 attempt {attempt}: durable {} applied {:?} ({}); rows {:?}",
            r.is_durable(),
            r.applied_topics,
            r.summary(),
            rows(&store, &nodes).await
        );
        assert!(r.is_durable(), "attempt {attempt}: {}", r.summary());
        assert_eq!(
            rows(&store, &nodes).await,
            ["2:spent=false"],
            "attempt {attempt}"
        );
        assert_eq!(applied_rows(&store, &nodes).await, [true; 3]);
    }

    let nodes = chain(8);
    let store = Rc::new(MemoryStorage::new());
    assert!(
        submitted(&shape_door(&store, true), &proven_beef(&nodes[0]))
            .await
            .is_durable()
    );
    let r = submitted(&shape_door(&store, true), &unproven_beef(&nodes, 7, 0)).await;
    assert!(r.is_durable(), "{}", r.summary());
    assert_eq!(rows(&store, &nodes).await, ["7:spent=false"]);
    assert_eq!(applied_rows(&store, &nodes).await, [true; 8]);
}

// M1's bound. The landings of one submit share its 256 reads: a carried
// chain of unlanded heads too deep for them is "not now", nothing of it is
// written (a landing happens only once the chain below it is settled), and
// the reads stop near the allowance. The limit, stated: such a chain stays
// "not now" at the door until its heads land some other way.
#[tokio::test]
async fn e1d_fold_m1_b_the_landings_of_one_submit_stay_inside_its_allowance() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(70);
    let store = Rc::new(MemoryStorage::new());
    assert!(
        submitted(&shape_door(&store, true), &proven_beef(&nodes[0]))
            .await
            .is_durable()
    );
    let storage = ScriptedStore::plain(&store);
    let reads = storage.reads.clone();
    let engine = door(ShapeHeadManager { names: true }, storage);
    let r = submitted(&engine, &unproven_beef(&nodes, 69, 0)).await;
    println!(
        "E1D fold M1 bound: {} reads, durable {} ({})",
        reads.get(),
        r.is_durable(),
        r.summary()
    );
    assert!(
        r.faults.iter().any(|f| f.site == "predecessor_not_landed"),
        "{}",
        r.summary()
    );
    assert_eq!(
        rows(&store, &nodes).await,
        ["0:spent=false"],
        "nothing written"
    );
    assert_eq!(applied_rows(&store, &nodes).await[1..], [false; 69]);
    // The submit's own two reads (its applied row, its coin), the 256.
    assert!(reads.get() <= 2 + 256, "{} reads", reads.get());
}

// A head manager that admits on shape (one output) and NAMES EVERY input as
// its history: a decoy no peer serves included (D14's why).
struct NamesEveryInput;

#[async_trait(?Send)]
impl TopicManager for NamesEveryInput {
    fn reads_off_chain_values(&self) -> bool {
        false
    }

    async fn identify_admissible_outputs(
        &self,
        tx: &Transaction,
        previous_coins: &[u8],
        off_chain_values: Option<&[u8]>,
        mode: SubmitMode,
        context: &TopicAdmittanceContext,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        ShapeHeadManager { names: true }
            .identify_admissible_outputs(tx, previous_coins, off_chain_values, mode, context)
            .await
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
            .filter(|i| spends_a_coin(i))
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

// L4, the decoy at the door. A PROVEN head spend over the held genesis that
// also names a decoy nobody holds or serves; its delete of the genesis lands
// and then answers an error (D17 M1: its output stays, no applied row). Its
// replay finds no coin, and the named decoy has not landed: on `683dffd` the
// replay was "not now" on every presentation (three and a dead letter) and
// the applied row never came. Now a subject that already holds its output in
// the topic finishes: recorded, one head.
#[tokio::test]
async fn e1d_fold_l4_a_replay_whose_outputs_are_held_finishes_over_a_named_decoy() {
    let (_logs, _guard) = capture_logs();
    let mut genesis = Transaction::new();
    genesis.inputs.push(coinbase_input());
    genesis.outputs.push(plain_output());
    let genesis = proven(&genesis, 700);
    let mut head = Transaction::new();
    head.inputs.push(TransactionInput::new(genesis.id(), 0));
    head.inputs.push(TransactionInput::new("33".repeat(32), 0));
    head.outputs.push(plain_output());
    let head = proven(&head, 701);

    let store = Rc::new(MemoryStorage::new());
    let calls = Calls::default();
    let mut storage = ScriptedStore::plain(&store);
    storage.calls = calls.clone();
    let engine = door(NamesEveryInput, storage);
    assert!(submitted(&engine, &beef_of(&genesis)).await.is_durable());
    calls
        .borrow_mut()
        .push((Call::Delete, genesis.id(), CallEvent::LandsThenFaultsOnce));
    assert!(!submitted(&engine, &beef_of(&head)).await.is_durable());
    drop(engine);
    let ids = [genesis.id(), head.id()];
    let held_rows = || {
        let (store, ids) = (store.clone(), ids.clone());
        async move {
            let mut rows = Vec::new();
            for id in &ids {
                rows.push(
                    store
                        .find_output(id, 0, Some(TOPIC), None, false)
                        .await
                        .unwrap()
                        .map(|o| o.spent),
                );
            }
            rows
        }
    };
    assert_eq!(held_rows().await, [None, Some(false)]);

    for attempt in 0..2 {
        let r = submitted(
            &door(NamesEveryInput, ScriptedStore::plain(&store)),
            &beef_of(&head),
        )
        .await;
        println!(
            "E1D fold L4 replay {attempt}: durable {} applied {:?} ({})",
            r.is_durable(),
            r.applied_topics,
            r.summary()
        );
        assert!(r.is_durable(), "replay {attempt}: {}", r.summary());
    }
    assert_eq!(held_rows().await, [None, Some(false)], "one head");
    assert!(store
        .does_applied_transaction_exist(&AppliedTransaction {
            txid: ids[1].clone(),
            topic: TOPIC.to_string(),
        })
        .await
        .unwrap());
}

// L3. The GASP walk's "landed" reads for an admitted node's named inputs are
// capped per node (16; two per transaction at most): a manager that names
// twenty inputs of twenty transactions nobody holds (D14's decoys are
// uncapped) cost two reads each on `683dffd`, forty past the strip's own
// twenty. Past the cap an input is requested, as on a read fault.
#[tokio::test]
async fn e1d_fold_l3_the_walks_landed_reads_are_capped_per_node() {
    let nodes = chain(2);
    let named: Vec<Outpoint> = (0..20u8)
        .map(|i| Outpoint::new(format!("{:02x}", 0x50 + i).repeat(32), 0))
        .collect();
    let manager = ProbeManager {
        outputs: vec![0],
        named: named.clone(),
        ..Default::default()
    };
    let store = Rc::new(MemoryStorage::new());
    let scripted = ScriptedStore::plain(&store);
    let reads = scripted.reads.clone();
    let adapter = OverlayGASPStorage::new(&scripted, TOPIC, new_finalized_graph_sink())
        .with_topic_manager(&manager);
    let response = adapter.find_needed_inputs(&nodes[1]).await.unwrap();
    let mut requested = response
        .map(|r| r.requested_inputs.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    requested.sort();
    let mut expected: Vec<String> = named.iter().map(Outpoint::to_graph_id).collect();
    expected.sort();
    println!(
        "E1D fold L3: {} reads for {} named inputs",
        reads.get(),
        named.len()
    );
    assert_eq!(
        requested, expected,
        "every input whose landing is unknown is requested"
    );
    // The strip's one read per input (the reference's), then the 16.
    assert_eq!(reads.get(), 20 + 16);
}

// ============================================================================
// The E1D delta fold (delta lens `docs/audit/E1D-delta-2026-10-08.md` on
// `0da3a82`): M1, L1, L2, L4. Each pin below is RED on `0da3a82`.
// ============================================================================

// `k` held heads (proven, one output each: the shape manager admits them),
// `k` spends of them that admit nothing (`w` outputs each, never submitted
// here), and the subject: a spend of every output of those spends (two
// outputs, it admits nothing), whose BEEF carries them all. The delta lens's
// public-door shape (SHIP adverts, their revocations, one subject).
fn wide_over_carried_spends(k: usize, w: usize) -> (Vec<Transaction>, Transaction) {
    let heads: Vec<Transaction> = (0..k)
        .map(|j| {
            let mut head = Transaction::new();
            head.inputs
                .push(TransactionInput::new("44".repeat(32), j as u32));
            head.outputs
                .push(TransactionOutput::new(1000, plain_output().locking_script));
            proven(&head, 700 + j as u32)
        })
        .collect();
    let mut subject = Transaction::new();
    for head in &heads {
        let mut spend = Transaction::new();
        let mut input = TransactionInput::new(head.id(), 0);
        input.source_transaction = Some(Box::new(head.clone()));
        spend.inputs.push(input);
        for _ in 0..w {
            spend
                .outputs
                .push(TransactionOutput::new(10, plain_output().locking_script));
        }
        for vout in 0..w as u32 {
            let mut input = TransactionInput::new(spend.id(), vout);
            input.source_transaction = Some(Box::new(spend.clone()));
            subject.inputs.push(input);
        }
    }
    subject.outputs.push(plain_output());
    subject.outputs.push(plain_output());
    (heads, subject)
}

// M1, the delta lens's `delta_d1`. After each landing the door judged the
// subject again (its applied row, one coin per input), and none of those
// reads was charged to the submit's 256: one public submit of a 252-input
// subject over 18 carried spends read the store 5,081 times on `0da3a82`
// (3,283 for 12 x 20). Now the re-judgement is charged BEFORE its landing,
// and a landing whose re-judgement does not fit is not made: one submit
// reads its own validation (1 + inputs) plus at most 256, to the read.
#[tokio::test]
async fn e1d_delta_m1_one_submit_reads_its_validation_and_at_most_256_more() {
    let (_logs, _guard) = capture_logs();
    // (k, w, the exact reads of the one submit, landed)
    let rows = [(1, 2, 13, 1), (12, 20, 493, 1), (18, 14, 257, 0)];
    let mut seen = Vec::new();
    for (k, w, _, _) in rows {
        let (heads, subject) = wide_over_carried_spends(k, w);
        let store = Rc::new(MemoryStorage::new());
        for head in &heads {
            assert!(submitted(&shape_door(&store, false), &beef_of(head))
                .await
                .is_durable());
        }
        let storage = ScriptedStore::plain(&store);
        let reads = storage.reads.clone();
        let engine = door(ShapeHeadManager { names: false }, storage);
        let r = submitted(&engine, &beef_of(&subject)).await;
        println!(
            "E1D delta M1 k={k} w={w} ({} inputs): {} reads (bound {}), landed {}, durable {} ({})",
            k * w,
            reads.get(),
            1 + k * w + 256,
            r.landed_predecessors.len(),
            r.is_durable(),
            r.summary()
        );
        seen.push((reads.get(), r));
    }
    // The reads first, every row: the bound and the exact count.
    for ((k, w, exact, _), (reads, _)) in rows.iter().zip(&seen) {
        assert!(*reads <= 1 + k * w + 256, "k={k} w={w}: {reads} reads");
        assert_eq!(reads, exact, "k={k} w={w}");
    }
    for ((k, w, _, landed), (_, r)) in rows.iter().zip(&seen) {
        assert_eq!(r.landed_predecessors.len(), *landed, "k={k} w={w}");
        if *k == 1 {
            assert!(r.is_durable(), "{}", r.summary());
            assert_eq!(r.applied_topics, vec![TOPIC.to_string()]);
        } else {
            assert!(
                r.faults.iter().any(|f| f.site == "predecessor_not_landed"),
                "{}",
                r.summary()
            );
        }
    }
}

// A manager over the rule of `M` that records every admission call: the
// txid judged and whether it was a dry run.
struct Recorded<M> {
    rule: M,
    calls: Rc<RefCell<Vec<(String, bool)>>>,
}

#[async_trait(?Send)]
impl<M: TopicManager> TopicManager for Recorded<M> {
    fn reads_off_chain_values(&self) -> bool {
        self.rule.reads_off_chain_values()
    }

    async fn identify_admissible_outputs(
        &self,
        tx: &Transaction,
        previous_coins: &[u8],
        off_chain_values: Option<&[u8]>,
        mode: SubmitMode,
        context: &TopicAdmittanceContext,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        self.calls.borrow_mut().push((tx.id(), context.dry_run));
        self.rule
            .identify_admissible_outputs(tx, previous_coins, off_chain_values, mode, context)
            .await
    }

    async fn identify_needed_inputs(
        &self,
        beef: &[u8],
        off_chain_values: Option<&[u8]>,
    ) -> Result<Vec<Outpoint>, TopicManagerError> {
        self.rule
            .identify_needed_inputs(beef, off_chain_values)
            .await
    }

    async fn get_documentation(&self) -> String {
        String::new()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata::default()
    }
}

// A shape door that records its manager's calls and checks roots against
// `tracker`, over `store`.
fn recorded_door(
    store: &Rc<MemoryStorage>,
    calls: &Rc<RefCell<Vec<(String, bool)>>>,
    tracker: &KnownRoots,
) -> Engine {
    Engine::with_chain_tracker(
        HashMap::from([(
            TOPIC.to_string(),
            Box::new(Recorded {
                rule: ShapeHeadManager { names: true },
                calls: calls.clone(),
            }) as Box<dyn TopicManager>,
        )]),
        HashMap::new(),
        Box::new(ScriptedStore::plain(store)),
        None,
        None,
        Some(Box::new(tracker.clone())),
        EngineConfig::default(),
    )
}

// L4, the delta lens's `delta_d3`. A carried chain of unlanded heads, each
// blocked by the one under it: on `0da3a82` each link was submitted, found
// blocked, and submitted again once the one under it landed, so its manager
// was asked for REAL twice (`[1, 2, 2, ...]`), and in a walked mode each of
// those submits walked its whole atomic BEEF again (the ancestry, down to
// the proven genesis: the chain tracker asked once per submit). Now a
// blocked link is asked only a DRY RUN and admitted for real ONCE, ancestors
// first, and no body is walked twice in the submit: the successor's own walk
// covered the chain, the tracker is asked once for the whole submit. The
// chain is script-valid (`OP_1` outputs, empty unlocking scripts).
#[tokio::test]
async fn e1d_delta_l4_each_link_of_a_carried_chain_is_admitted_once_and_walked_once() {
    let (_logs, _guard) = capture_logs();
    let nodes = scripted_chain(8, "51", 1);
    let tracker = KnownRoots::of(&nodes);
    let store = Rc::new(MemoryStorage::new());
    let calls: Rc<RefCell<Vec<(String, bool)>>> = Rc::default();
    let genesis = recorded_door(&store, &calls, &tracker)
        .submit_with_report(&proven_beef(&nodes[0]), SubmitMode::HistoricalTx)
        .await
        .unwrap()
        .1;
    assert!(genesis.is_durable());
    calls.borrow_mut().clear();
    let asked_before = tracker.asked.load(Ordering::SeqCst);

    let r = recorded_door(&store, &calls, &tracker)
        .submit_with_report(&unproven_beef(&nodes, 7, 0), SubmitMode::HistoricalTx)
        .await
        .unwrap()
        .1;
    let real: Vec<usize> = nodes[1..7]
        .iter()
        .map(|n| {
            let txid = node_txid(n);
            calls
                .borrow()
                .iter()
                .filter(|(t, dry)| *t == txid && !dry)
                .count()
        })
        .collect();
    let asked = tracker.asked.load(Ordering::SeqCst) - asked_before;
    println!(
        "E1D delta L4: real calls per link {real:?}, tracker asked {asked}, landed {:?}, durable {} ({})",
        r.landed_predecessors.len(),
        r.is_durable(),
        r.summary()
    );
    assert!(r.is_durable(), "{}", r.summary());
    assert_eq!(rows(&store, &nodes).await, ["7:spent=false"]);
    assert_eq!(applied_rows(&store, &nodes).await, [true; 8]);
    assert_eq!(real, [1; 6], "each link admitted once");
    assert_eq!(asked, 1, "one walk reaches the proven genesis");
    // L2's record: the landed links, ancestors first.
    assert_eq!(
        r.landed_predecessors,
        nodes[1..7]
            .iter()
            .map(|n| (node_txid(n), TOPIC.to_string()))
            .collect::<Vec<_>>()
    );
}

// What a `ValuesLookup` was told: each txid and its off-chain values.
type ToldValues = Rc<RefCell<Vec<(String, Option<Vec<u8>>)>>>;

// A lookup service that records the off-chain values it is told with, and
// says whether it reads them.
struct ValuesLookup {
    told: ToldValues,
    reads: bool,
}

#[async_trait(?Send)]
impl bsv_overlay_engine::lookup_service::LookupService for ValuesLookup {
    fn admission_mode(&self) -> AdmissionMode {
        AdmissionMode::LockingScript
    }
    fn spend_notification_mode(&self) -> SpendNotificationMode {
        SpendNotificationMode::None
    }
    fn reads_off_chain_values(&self, _topic: &str) -> bool {
        self.reads
    }
    async fn output_admitted_by_topic(
        &self,
        payload: &OutputAdmittedByTopic,
    ) -> Result<(), bsv_overlay_engine::lookup_service::LookupServiceError> {
        if let OutputAdmittedByTopic::LockingScript {
            txid,
            off_chain_values,
            ..
        } = payload
        {
            self.told
                .borrow_mut()
                .push((txid.clone(), off_chain_values.clone()));
        }
        Ok(())
    }
    async fn output_evicted(
        &self,
        _txid: &str,
        _output_index: u32,
    ) -> Result<(), bsv_overlay_engine::lookup_service::LookupServiceError> {
        Ok(())
    }
    async fn lookup(
        &self,
        _question: &LookupQuestion,
    ) -> Result<LookupResult, bsv_overlay_engine::lookup_service::LookupServiceError> {
        Ok(LookupResult::OutputList(Vec::new()))
    }
    async fn get_documentation(&self) -> String {
        String::new()
    }
    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata::default()
    }
}

fn values_door(
    store: &Rc<MemoryStorage>,
    told: &ToldValues,
    reads: bool,
    storage: ScriptedStore,
) -> Engine {
    let _ = store;
    Engine::new(
        HashMap::from([(
            TOPIC.to_string(),
            Box::new(ShapeHeadManager { names: true }) as Box<dyn TopicManager>,
        )]),
        HashMap::from([(
            "ls_values".to_string(),
            Box::new(ValuesLookup {
                told: told.clone(),
                reads,
            }) as Box<dyn bsv_overlay_engine::lookup_service::LookupService>,
        )]),
        Box::new(storage),
        None,
        EngineConfig::default(),
    )
}

// L1, the delta lens's `delta_d2`. Head 1's own submit carries off-chain
// values and faults; head 2, carrying head 1, arrives before head 1's
// replay. On `0da3a82` the door landed head 1 with NO values (the lookup
// service was told `None`) and head 1's replay with its values was then a
// dupe: the values were never told. Now a topic whose lookup service reads
// off-chain values gets nothing landed first: head 2 waits, head 1's replay
// lands WITH its values, and head 2's replay then lands over its coin. A
// service that does not read them (the second half) lets the door land.
#[tokio::test]
async fn e1d_delta_l1_a_landing_never_takes_a_predecessors_off_chain_values() {
    let (_logs, _guard) = capture_logs();
    for reads in [true, false] {
        let nodes = chain(3);
        let store = Rc::new(MemoryStorage::new());
        let told: ToldValues = Rc::default();
        let with_values = |node: &GASPNode| {
            let mut beef = proven_beef(node);
            beef.off_chain_values = Some(b"sidecar".to_vec());
            beef
        };
        let a = values_door(
            &store,
            &told,
            reads,
            ScriptedStore::armed(&store, node_txid(&nodes[1]), 0, InsertEvent::Faults),
        );
        assert!(submitted(&a, &proven_beef(&nodes[0])).await.is_durable());
        assert!(!submitted(&a, &with_values(&nodes[1])).await.is_durable());
        drop(a);
        told.borrow_mut().clear();

        let door = || values_door(&store, &told, reads, ScriptedStore::plain(&store));
        let head2 = submitted(&door(), &unproven_beef(&nodes, 2, 0)).await;
        let replay1 = submitted(&door(), &with_values(&nodes[1])).await;
        let replay2 = submitted(&door(), &unproven_beef(&nodes, 2, 0)).await;
        let head1_told: Vec<Option<Vec<u8>>> = told
            .borrow()
            .iter()
            .filter(|(t, _)| *t == node_txid(&nodes[1]))
            .map(|(_, v)| v.clone())
            .collect();
        println!(
            "E1D delta L1 reads={reads}: head 2 durable {} ({}), head 1 replay applied {:?} deduped {:?}, head 1 told {head1_told:?}",
            head2.is_durable(),
            head2.summary(),
            replay1.applied_topics,
            replay1.deduped_topics
        );
        if reads {
            assert!(head2
                .faults
                .iter()
                .any(|f| f.site == "predecessor_not_landed"));
            assert!(head2.landed_predecessors.is_empty());
            assert_eq!(replay1.applied_topics, vec![TOPIC.to_string()]);
            assert_eq!(head1_told, [Some(b"sidecar".to_vec())]);
            assert_eq!(replay2.applied_topics, vec![TOPIC.to_string()]);
        } else {
            assert!(head2.is_durable(), "{}", head2.summary());
            assert_eq!(
                head2.landed_predecessors,
                [(node_txid(&nodes[1]), TOPIC.to_string())]
            );
            assert_eq!(replay1.deduped_topics, vec![TOPIC.to_string()]);
            assert_eq!(head1_told, [None]);
        }
        assert_eq!(rows(&store, &nodes).await, ["2:spent=false"]);
        assert_eq!(applied_rows(&store, &nodes).await, [true; 3]);
    }
}

// The E1D delta-2 fold, L2 (bsv-low #575). The door asks the caller's landing
// guard BEFORE it lands a carried predecessor: on `f057acc` the landing was
// guarded after the write only (the worker's eviction ledger moved an evicted
// predecessor out again after it was written and its lookup services told,
// and the successor stayed recorded over it). A refused body writes nothing,
// the successor's topic is "not now" (as on `683dffd`) and the guard is asked
// once per body, ancestors included; its own fault refuses too. RED on
// `f057acc` with an inert `set_landing_guard` grafted (both bodies landed).
#[tokio::test]
async fn e1d_fold3_l2_the_door_asks_the_landing_guard_before_it_lands() {
    let (_logs, _guard) = capture_logs();
    type Asked = Rc<RefCell<Vec<String>>>;
    let guarded = |store: &Rc<MemoryStorage>, refuse: String, fault: bool, asked: &Asked| {
        let mut engine = shape_door(store, true);
        let asked = asked.clone();
        engine.set_landing_guard(Rc::new(move |txid: &str, topic: &str| {
            asked.borrow_mut().push(txid.to_string());
            assert_eq!(topic, TOPIC);
            let answer = if txid != refuse {
                Ok(())
            } else if fault {
                Err("the ledger could not be read".to_string())
            } else {
                Err("under an OPEN eviction".to_string())
            };
            Box::pin(async move { answer })
        }));
        engine
    };
    // The carried predecessor itself refused, by a row and by a fault.
    for fault in [false, true] {
        let nodes = chain(3);
        let store = Rc::new(MemoryStorage::new());
        assert!(
            submitted(&shape_door(&store, true), &proven_beef(&nodes[0]))
                .await
                .is_durable()
        );
        let asked: Asked = Rc::default();
        let door = guarded(&store, node_txid(&nodes[1]), fault, &asked);
        let r = submitted(&door, &unproven_beef(&nodes, 2, 0)).await;
        let held = rows(&store, &nodes).await;
        println!(
            "E1D delta-2 L2 fault={fault}: durable {} ({}), landed {:?}, asked {}, rows {held:?}",
            r.is_durable(),
            r.summary(),
            r.landed_predecessors,
            asked.borrow().len(),
        );
        assert!(r.faults.iter().any(|f| f.site == "predecessor_not_landed"
            && f.error.contains("refused by the landing guard")));
        assert!(r.landed_predecessors.is_empty());
        assert_eq!(*asked.borrow(), [node_txid(&nodes[1])]);
        assert_eq!(rows(&store, &nodes).await, ["0:spent=false"]);
        assert_eq!(applied_rows(&store, &nodes).await, [true, false, false]);
    }
    // An ancestor refused under a link the guard lets through: the link was
    // only judged dry, nothing of either is written, each asked once.
    let nodes = chain(4);
    let store = Rc::new(MemoryStorage::new());
    assert!(
        submitted(&shape_door(&store, true), &proven_beef(&nodes[0]))
            .await
            .is_durable()
    );
    let asked: Asked = Rc::default();
    let door = guarded(&store, node_txid(&nodes[1]), false, &asked);
    let r = submitted(&door, &unproven_beef(&nodes, 3, 0)).await;
    assert!(!r.is_durable());
    assert!(r.landed_predecessors.is_empty());
    assert_eq!(
        *asked.borrow(),
        [node_txid(&nodes[2]), node_txid(&nodes[1])]
    );
    assert_eq!(rows(&store, &nodes).await, ["0:spent=false"]);
    assert_eq!(
        applied_rows(&store, &nodes).await,
        [true, false, false, false]
    );
    // A guard that lets every body through changes nothing: the chain lands.
    let asked: Asked = Rc::default();
    let door = guarded(&store, String::new(), false, &asked);
    let r = submitted(&door, &unproven_beef(&nodes, 3, 0)).await;
    assert!(r.is_durable(), "{}", r.summary());
    assert_eq!(
        *asked.borrow(),
        [node_txid(&nodes[2]), node_txid(&nodes[1])]
    );
    assert_eq!(rows(&store, &nodes).await, ["3:spent=false"]);
    assert_eq!(applied_rows(&store, &nodes).await, [true; 4]);
}

// ============================================================================
// bsv-low #555 (measured as #582): a graph deeper than its PER-GRAPH budget is
// DEFERRED, its partial walk persisted as ONE record, and RESUMED by the next
// pass from the record, never restarted. The other graphs of the pass go on.
// ============================================================================

use bsv_overlay_engine::gasp::{peer_origin, DeferredGraph, DEFERRED_GRAPH_MAX_PASSES};

// A per-graph deadline that never falls due: the call count binds.
fn never() -> bsv_overlay_engine::engine::SleepFactory {
    Rc::new(|_| Box::pin(std::future::pending::<()>()))
}

// `chain`, its every transaction made distinct by `salt` (two chains of one
// node would otherwise share their genesis).
fn salted_chain(length: usize, salt: u64) -> Vec<GASPNode> {
    let mut nodes = Vec::new();
    let mut previous = None;
    for height in 0..length {
        let mut tx = Transaction::new();
        if let Some(txid) = previous {
            tx.inputs.push(TransactionInput::new(txid, 0));
        } else {
            tx.inputs.push(coinbase_input());
        }
        tx.outputs.push(TransactionOutput::new(
            1000 + salt,
            LockingScript::from_hex("76a914000000000000000000000000000000000000000088ac").unwrap(),
        ));
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

fn outpoint_of(node: &GASPNode) -> String {
    format!("{}.0", node_txid(node))
}

fn record_shape(r: &DeferredGraph) -> (usize, Vec<String>, u64, u32, String) {
    (
        r.nodes.len(),
        r.pending.iter().map(|p| p.outpoint.clone()).collect(),
        r.calls,
        r.passes,
        r.reason.clone(),
    )
}

fn deferral(topic: &bsv_overlay_engine::engine::TopicSyncResult) -> (u64, u64, u64, Vec<String>) {
    (
        topic.deferred_graphs,
        topic.resumed_graphs,
        topic.converged_graphs,
        topic
            .dropped_graphs
            .iter()
            .map(|d| d.reason.clone())
            .collect(),
    )
}

// PIN A. One head chain of 8 proven links, the tip listed; the manager names
// each link's predecessor. The per-graph budget is THREE calls, the per-peer
// budget far above it. On cf933e8 nothing bounds a graph and nothing persists
// a walk (the API grafted inert: the walk runs to its end inside the per-peer
// budget, so this pin's per-tick request list is red at tick 1). Now the
// graph converges in three ticks, each node asked ONCE, through exactly one
// record, replaced (two writes) and deleted at convergence.
#[tokio::test]
async fn e555_a_a_graph_deeper_than_its_budget_converges_in_three_ticks_through_one_record() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(8);
    let state = Rc::new(RefCell::new(HeadState::default()));
    let mut node = Budgeted::new(
        RecordingRemote::new(&nodes, &[7]),
        Box::new(HeadChainManager(state.clone())),
        1000,
    );
    node.engine.set_graph_budget(never(), 3, 60_000);

    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&nodes, &[7, 6, 5]));
    assert_eq!(deferral(&topic), (1, 0, 0, vec![]));
    assert!(topic.errors.is_empty(), "a deferral is not an error");
    let records = node.store.deferred_graphs();
    assert_eq!(records.len(), 1);
    assert_eq!(
        (records[0].peer.as_str(), records[0].topic.as_str()),
        (PEER, TOPIC)
    );
    assert_eq!(records[0].outpoint, outpoint_of(&nodes[7]));
    assert_eq!(records[0].score, 1);
    assert_eq!(
        record_shape(&records[0]),
        (3, vec![outpoint_of(&nodes[4])], 3, 1, "calls".into())
    );
    assert!(
        records[0].nodes.iter().all(|w| w.node.proof.is_some()),
        "the nodes are kept with their proofs"
    );
    assert_eq!(node.store.deferred_graph_writes(), 1);
    assert_eq!(node.cursor().await, 0, "held below the deferred UTXO");
    assert!(state.borrow().admitted.is_empty());
    assert_eq!(node.failures().await, 0, "progress is not a failed attempt");

    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&nodes, &[4, 3, 2]), "only what was pending");
    assert_eq!(deferral(&topic), (1, 1, 0, vec![]));
    let records = node.store.deferred_graphs();
    assert_eq!(records.len(), 1, "replaced, never appended");
    assert_eq!(
        record_shape(&records[0]),
        (6, vec![outpoint_of(&nodes[1])], 6, 2, "calls".into())
    );
    assert_eq!(node.store.deferred_graph_writes(), 2);
    assert_eq!(node.cursor().await, 0);

    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&nodes, &[1, 0]));
    assert_eq!(deferral(&topic), (0, 1, 1, vec![]));
    assert_eq!(topic.finalized_graphs, 1);
    assert!(
        node.store.deferred_graphs().is_empty(),
        "deleted at convergence"
    );
    assert_eq!(node.store.deferred_graph_writes(), 2);
    assert_eq!(
        state.borrow().admitted,
        txids(&nodes, &[0, 1, 2, 3, 4, 5, 6, 7])
    );
    assert_eq!(held(&node.store, &nodes).await, vec![(7, 0)]);
    assert_eq!(applied_rows(&node.store, &nodes).await, [true; 8]);
    assert_eq!(node.cursor().await, 1);
    assert_eq!(topic.cursor_moves, moved(0, 1));

    let (topic, sent) = node.tick().await;
    assert!(sent.is_empty(), "held: nothing to walk");
    assert_eq!(deferral(&topic), (0, 0, 0, vec![]));
    println!(
        "#555 PIN A: 8 links under a 3-call graph budget: 3 ticks, 8 requests, 1 record, 2 writes"
    );
}

// A peer that never answers for the txids in `hangs` (a hung request) and
// raises `flag` when one is asked, so the per-graph deadline falls due.
#[derive(Clone)]
struct HangingRemote {
    inner: MeteredRemote,
    hangs: Rc<RefCell<HashSet<String>>>,
    flag: Rc<Cell<bool>>,
}

impl GASPRemoteFactory for HangingRemote {
    fn create_remote(&self, _peer_url: &str, _topic: &str) -> Box<dyn GASPRemote> {
        Box::new(self.clone())
    }
}

#[async_trait(?Send)]
impl GASPRemote for HangingRemote {
    async fn get_initial_response(
        &self,
        request: &GASPInitialRequest,
    ) -> Result<GASPInitialResponse, GASPError> {
        self.inner.get_initial_response(request).await
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
        if self.hangs.borrow().contains(txid) {
            self.inner
                .inner
                .requests
                .borrow_mut()
                .push((txid.to_string(), output_index, metadata));
            self.flag.set(true);
            std::future::pending::<()>().await;
        }
        self.inner
            .request_node(graph_id, txid, output_index, metadata)
            .await
    }
    async fn submit_node(&self, node: &GASPNode) -> Result<Option<GASPNodeResponse>, GASPError> {
        self.inner.submit_node(node).await
    }
}

// The per-graph deadline: due once a hung request raised the flag.
fn hang_deadline(flag: Rc<Cell<bool>>) -> bsv_overlay_engine::engine::SleepFactory {
    Rc::new(move |_| {
        flag.set(false);
        let flag = flag.clone();
        Box::pin(std::future::poll_fn(move |_| {
            if flag.get() {
                std::task::Poll::Ready(())
            } else {
                std::task::Poll::Pending
            }
        }))
    })
}

fn hanging(
    node: &mut Budgeted,
    remote: RecordingRemote,
    hangs: &[String],
) -> Rc<RefCell<HashSet<String>>> {
    let set = Rc::new(RefCell::new(hangs.iter().cloned().collect::<HashSet<_>>()));
    let flag = Rc::new(Cell::new(false));
    node.engine.set_gasp_remote_factory(Box::new(HangingRemote {
        inner: MeteredRemote {
            inner: remote,
            clock: node.clock.clone(),
        },
        hangs: set.clone(),
        flag: flag.clone(),
    }));
    node.engine
        .set_graph_budget(hang_deadline(flag), 100, 60_000);
    set
}

// PIN B. A stuck peer: it never answers for link 3 (a hung request). The
// per-graph TIME budget defers the graph at that request; each pass after it
// asks link 3 alone, once; after DEFERRED_GRAPH_MAX_PASSES passes the record
// is dropped (`max_passes`) and the UTXO fails as before #555 (the gap guard
// asks again: the next pass walks it from its root). When the peer heals, the
// graph converges from the record. On cf933e8 (the API grafted inert) the
// first tick hangs at link 3 forever: no per-graph deadline exists.
// Amended by the lens fold (H1): each pass after the first fetches NOTHING,
// a failed attempt for the quarantine (pinned apart, `e555f_h1`); the peer is
// re-admitted after each pass here so the record's own bound is reached.
#[tokio::test]
async fn e555_b_a_stuck_peer_is_deferred_each_pass_and_dropped_after_max_passes() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(8);
    let state = Rc::new(RefCell::new(HeadState::default()));
    let remote = RecordingRemote::new(&nodes, &[7]);
    let store = Rc::new(MemoryStorage::new());
    let mut node = Budgeted::over(
        remote.clone(),
        Box::new(HeadChainManager(state.clone())),
        RequestClock::allowing(1000),
        store.clone(),
        Box::new(store),
        false,
    );
    let hangs = hanging(&mut node, remote, &[node_txid(&nodes[3])]);

    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&nodes, &[7, 6, 5, 4, 3]));
    assert_eq!(deferral(&topic), (1, 0, 0, vec![]));
    assert_eq!(
        record_shape(&node.store.deferred_graphs()[0]),
        (4, vec![outpoint_of(&nodes[3])], 5, 1, "time".into())
    );
    for pass in 2..=DEFERRED_GRAPH_MAX_PASSES {
        let (topic, sent) = node.tick().await;
        assert_eq!(
            sent,
            txids(&nodes, &[3]),
            "pass {pass}: the stuck node alone"
        );
        assert_eq!(deferral(&topic), (1, 1, 0, vec![]));
        assert_eq!(node.store.deferred_graphs()[0].passes, pass);
        assert_eq!(node.failures().await, 1, "pass {pass}: nothing fetched");
        node.store
            .record_peer_sync_outcome(&peer_origin(PEER), TOPIC, true)
            .await
            .unwrap();
    }
    // The pass after the last: dropped with its reason, nothing asked, the
    // UTXO failed (held below the cursor), no record left.
    let (topic, sent) = node.tick().await;
    assert!(sent.is_empty());
    assert_eq!(deferral(&topic), (0, 0, 0, vec!["max_passes".to_string()]));
    assert_eq!(topic.dropped_graphs[0].outpoint, outpoint_of(&nodes[7]));
    assert!(node.store.deferred_graphs().is_empty());
    assert_eq!(node.cursor().await, 0);
    // The next pass walks it from its root, a new record.
    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&nodes, &[7, 6, 5, 4, 3]));
    assert_eq!(deferral(&topic), (1, 0, 0, vec![]));
    // The peer heals: the graph converges from the record.
    hangs.borrow_mut().clear();
    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&nodes, &[3, 2, 1, 0]));
    assert_eq!(deferral(&topic), (0, 1, 1, vec![]));
    assert_eq!(state.borrow().admitted.len(), 8);
    assert_eq!(held(&node.store, &nodes).await, vec![(7, 0)]);
    assert!(node.store.deferred_graphs().is_empty());
    println!("#555 PIN B: a hung node deferred {DEFERRED_GRAPH_MAX_PASSES} passes, dropped max_passes, then healed and converged");
}

// PIN B2, rewritten by bsv-low #585 (door 4). A record has NO byte bound: a
// walk the budget cut whose record is past 1 MiB is KEPT and resumed, as any
// other. Before #585 it was `too_big`: nothing written, the walk gone on
// under the per-peer budget alone (L4). RED on `e8ab762`: `too_big`, no
// record, no write.
#[tokio::test]
async fn e555_b2_a_record_past_a_megabyte_is_kept_and_resumed() {
    let (_logs, _guard) = capture_logs();
    let mut nodes = chain(4);
    // The tip carries a 655 KB second output (ten 64 KB pushes): its hex
    // alone is past 1 MiB.
    let mut tip = Transaction::new();
    tip.inputs
        .push(TransactionInput::new(node_txid(&nodes[2]), 0));
    tip.outputs.push(plain_output());
    tip.outputs.push(TransactionOutput::new(
        0,
        LockingScript::from_hex(&format!("4dffff{}", "00".repeat(65_535)).repeat(10)).unwrap(),
    ));
    let tip_txid = tip.id();
    nodes[3] = node_of(&tip, 0, Some(honest_proof(&tip_txid, 103)));
    let state = Rc::new(RefCell::new(HeadState::default()));
    let mut node = Budgeted::new(
        RecordingRemote::new(&nodes, &[3]),
        Box::new(HeadChainManager(state.clone())),
        1000,
    );
    node.engine.set_graph_budget(never(), 2, 60_000);
    // The tip alone is past the bytes limb: the first pass is cut on it.
    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&nodes, &[3]));
    assert!(nodes[3].raw_tx.len() > 1 << 20, "{}", nodes[3].raw_tx.len());
    assert_eq!(deferral(&topic), (1, 0, 0, vec![]), "deferred, no drop");
    let records = node.store.deferred_graphs();
    assert_eq!((records.len(), records[0].reason.as_str()), (1, "bytes"));
    assert!(
        records[0].byte_size() > 1 << 20,
        "{}",
        records[0].byte_size()
    );
    assert_eq!(node.store.deferred_graph_writes(), 1);
    assert!(state.borrow().admitted.is_empty());
    // Resumed from the record, the tip never asked again, until it lands.
    let mut asked = Vec::new();
    let mut converged = 0;
    for _ in 0..3 {
        let (topic, sent) = node.tick().await;
        assert!(
            topic.dropped_graphs.is_empty(),
            "{:?}",
            topic.dropped_graphs
        );
        asked.extend(sent);
        converged += topic.converged_graphs;
        if converged > 0 {
            break;
        }
    }
    assert_eq!(asked, txids(&nodes, &[2, 1, 0]), "only what was pending");
    assert_eq!(converged, 1);
    assert!(node.store.deferred_graphs().is_empty());
    assert_eq!(state.borrow().admitted.len(), 4, "admitted whole");
    assert_eq!(node.cursor().await, 1);
}

// PIN C. Two deferred graphs on one topic resume IN ORDER (by their score,
// the order the peer serves them), each from its own record. Amended by the
// lens fold (M1): the resumes of a pass share ONE per-graph budget, so a's
// two pending calls leave b one, and b converges a pass later.
#[tokio::test]
async fn e555_c_two_deferred_graphs_on_one_topic_resume_in_order() {
    let (_logs, _guard) = capture_logs();
    let a = salted_chain(5, 1);
    let b = salted_chain(5, 2);
    let nodes: Vec<GASPNode> = a.iter().chain(b.iter()).cloned().collect();
    let state = Rc::new(RefCell::new(HeadState::default()));
    let mut node = Budgeted::new(
        listing(&nodes, &[(4, 0), (9, 0)]),
        Box::new(HeadChainManager(state.clone())),
        1000,
    );
    node.engine.set_graph_budget(never(), 3, 60_000);

    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&nodes, &[4, 3, 2, 9, 8, 7]));
    assert_eq!(deferral(&topic), (2, 0, 0, vec![]));
    let records = node.store.deferred_graphs();
    assert_eq!(
        records
            .iter()
            .map(|r| (r.score, r.outpoint.clone()))
            .collect::<Vec<_>>(),
        vec![(1, outpoint_of(&nodes[4])), (2, outpoint_of(&nodes[9]))]
    );

    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&nodes, &[1, 0, 6]), "a's pending, then b's");
    assert_eq!(deferral(&topic), (1, 2, 1, vec![]));
    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&nodes, &[5]));
    assert_eq!(deferral(&topic), (0, 1, 1, vec![]));
    assert!(node.store.deferred_graphs().is_empty());
    assert_eq!(held(&node.store, &nodes).await, vec![(4, 0), (9, 0)]);
    assert_eq!(node.cursor().await, 2);
}

// PIN D. #554 across a resume: the deferred UTXO is served AGAIN by the next
// page of the same sync (the responder serves `score >= since`); it is
// resumed once and written once per sync, never walked a second time there.
#[tokio::test]
async fn e555_d_a_deferred_utxo_is_resumed_once_per_sync_though_served_twice() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(8);
    let state = Rc::new(RefCell::new(HeadState::default()));
    let mut node = Budgeted::new(
        RecordingRemote::new(&nodes, &[7]),
        Box::new(HeadChainManager(state.clone())),
        1000,
    );
    node.engine.set_graph_budget(never(), 3, 60_000);
    node.tick().await;
    let writes = node.store.deferred_graph_writes();
    let (topic, sent) = node.tick().await;
    // Two pages (the second re-serves the UTXO) and three node requests.
    assert_eq!(
        node.clock.spent.get(),
        2 + 3,
        "the UTXO was served by both pages"
    );
    assert_eq!(sent, txids(&nodes, &[4, 3, 2]));
    assert_eq!((topic.resumed_graphs, topic.deferred_graphs), (1, 1));
    assert_eq!(node.store.deferred_graph_writes(), writes + 1);
}

// PIN E. e1d's rule over a RESUMED graph: the finalize submit of link 1
// FAULTS (a D1 outage) on the tick that completes the resumed walk. Nothing
// after it is submitted (links 2 and 3 are never recorded over a predecessor
// that did not land), the old head stays, and the walk is KEPT (`not_landed`,
// nothing pending): the next tick completes it again without one request and
// lands the chain.
#[tokio::test]
async fn e555_e_a_resumed_graph_whose_finalize_does_not_land_records_no_successor() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(4);
    let state = Rc::new(RefCell::new(HeadState::default()));
    let store = Rc::new(MemoryStorage::new());
    let scripted = ScriptedStore::armed(&store, node_txid(&nodes[1]), 0, InsertEvent::Faults);
    let outage = scripted.armed.clone();
    let mut node = Budgeted::over(
        RecordingRemote::new(&nodes, &[3]),
        Box::new(HeadChainManager(state.clone())),
        RequestClock::allowing(1000),
        store,
        Box::new(scripted),
        true,
    );
    node.engine.set_graph_budget(never(), 2, 60_000);

    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&nodes, &[3, 2]));
    assert_eq!(deferral(&topic), (1, 0, 0, vec![]));

    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&nodes, &[1, 0]));
    assert_eq!(deferral(&topic), (1, 1, 0, vec![]));
    assert!(
        row_exists(&node.store, &nodes[0]).await,
        "the old head stays (marked spent until the replay, #559)"
    );
    assert!(!row_exists(&node.store, &nodes[2]).await && !row_exists(&node.store, &nodes[3]).await);
    assert_eq!(
        applied_rows(&node.store, &nodes).await,
        [true, false, false, false],
        "no successor recorded over the link that did not land"
    );
    let records = node.store.deferred_graphs();
    assert_eq!(
        record_shape(&records[0]),
        (4, vec![], 4, 2, "not_landed".into())
    );
    assert_eq!(node.cursor().await, 0);

    outage.borrow_mut().take();
    let (topic, sent) = node.tick().await;
    assert!(sent.is_empty(), "completed again from the record, no walk");
    assert_eq!(deferral(&topic), (0, 1, 1, vec![]));
    assert_eq!(held(&node.store, &nodes).await, vec![(3, 0)]);
    assert_eq!(applied_rows(&node.store, &nodes).await, [true; 4]);
    assert!(node.store.deferred_graphs().is_empty());
}

// PIN G. The other graphs of a pass complete while one is deferred: a deep
// graph listed FIRST no longer holds the pass, the one after it is admitted
// in the same tick, and the cursor stays below the deferred one.
#[tokio::test]
async fn e555_g_the_other_graphs_of_a_pass_complete_while_one_is_deferred() {
    let (_logs, _guard) = capture_logs();
    let deep = salted_chain(8, 1);
    let shallow = salted_chain(1, 2);
    let nodes: Vec<GASPNode> = deep.iter().chain(shallow.iter()).cloned().collect();
    let state = Rc::new(RefCell::new(HeadState::default()));
    let mut node = Budgeted::new(
        listing(&nodes, &[(7, 0), (8, 0)]),
        Box::new(HeadChainManager(state.clone())),
        1000,
    );
    node.engine.set_graph_budget(never(), 3, 60_000);

    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&nodes, &[7, 6, 5, 8]));
    assert_eq!(deferral(&topic), (1, 0, 0, vec![]));
    assert_eq!(topic.finalized_graphs, 1);
    assert_eq!(
        held(&node.store, &nodes).await,
        vec![(8, 0)],
        "the shallow graph is in"
    );
    assert_eq!(node.cursor().await, 0, "below the deferred graph");

    let (_, sent) = node.tick().await;
    assert_eq!(sent, txids(&nodes, &[4, 3, 2]), "the held one is not asked");
    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&nodes, &[1, 0]));
    assert_eq!(topic.converged_graphs, 1);
    assert_eq!(held(&node.store, &nodes).await, vec![(7, 0), (8, 0)]);
    assert_eq!(node.cursor().await, 2);
}

// PIN H. The per-PEER deadline (D16) that cuts a walk defers it too: pin F's
// graph (8 links, a tick of five calls), which on cf933e8 is dropped whole
// on every tick and never admitted, converges in two ticks.
#[tokio::test]
async fn e555_h_a_walk_cut_by_the_per_peer_deadline_is_deferred_and_converges() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(8);
    let state = Rc::new(RefCell::new(HeadState::default()));
    let mut node = Budgeted::new(
        RecordingRemote::new(&nodes, &[7]),
        Box::new(HeadChainManager(state.clone())),
        5,
    );
    node.engine.set_graph_budget(never(), 100, 60_000);

    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&nodes, &[7, 6, 5, 4, 3]));
    assert_eq!(topic.deadline_dropped_graphs, 1);
    assert_eq!(deferral(&topic), (1, 0, 0, vec![]));
    assert_eq!(
        record_shape(&node.store.deferred_graphs()[0]),
        (
            4,
            vec![outpoint_of(&nodes[3])],
            5,
            1,
            "peer_deadline".into()
        )
    );
    assert_eq!(node.failures().await, 0, "a deferral is progress");

    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&nodes, &[3, 2, 1, 0]), "only what was pending");
    assert_eq!(deferral(&topic), (0, 1, 1, vec![]));
    assert_eq!(state.borrow().admitted.len(), 8);
    assert_eq!(held(&node.store, &nodes).await, vec![(7, 0)]);
}

// PIN F. The two other ends of a record. (1) The measured case's way out:
// the root was served UNPROVEN (its every input an SPV necessity) and is
// re-asked at each resume; once its block lands the peer serves it PROVEN,
// the record is dropped (`root_proven`) and the walk restarts from the
// proven root. (2) A record whose UTXO a completed sync is no longer served
// (spent at the peer) is dropped (`not_served`).
#[tokio::test]
async fn e555_f_a_root_proven_since_restarts_and_an_unserved_record_is_dropped() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(6);
    let mut unproven = nodes.clone();
    unproven[5].proof = None;
    let state = Rc::new(RefCell::new(HeadState::default()));
    let mut node = Budgeted::new(
        RecordingRemote::new(&unproven, &[5]),
        Box::new(HeadChainManager(state.clone())),
        1000,
    );
    node.engine.set_graph_budget(never(), 3, 60_000);
    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&nodes, &[5, 4, 3]));
    assert_eq!(deferral(&topic), (1, 0, 0, vec![]));
    assert!(node.store.deferred_graphs()[0].nodes[0]
        .node
        .proof
        .is_none());

    // The block lands: the peer serves the root with its proof.
    let mut mined = RecordingRemote::new(&nodes, &[5]);
    mined.requests = node.requests.clone();
    node.engine.set_gasp_remote_factory(Box::new(MeteredRemote {
        inner: mined,
        clock: node.clock.clone(),
    }));
    let (topic, sent) = node.tick().await;
    assert_eq!(
        sent,
        txids(&nodes, &[5, 5, 4]),
        "the re-ask, then the walk from it"
    );
    assert_eq!(deferral(&topic), (1, 1, 0, vec!["root_proven".to_string()]));
    let record = &node.store.deferred_graphs()[0];
    assert!(record.nodes[0].node.proof.is_some());
    assert_eq!(record.passes, 1, "a new record");

    // The peer no longer lists the UTXO.
    let mut spent = RecordingRemote::new(&nodes, &[]);
    spent.requests = node.requests.clone();
    node.engine.set_gasp_remote_factory(Box::new(MeteredRemote {
        inner: spent,
        clock: node.clock.clone(),
    }));
    let (topic, sent) = node.tick().await;
    assert!(sent.is_empty());
    assert_eq!(deferral(&topic), (0, 0, 0, vec!["not_served".to_string()]));
    assert!(node.store.deferred_graphs().is_empty());
}

// ============================================================================
// bsv-low #555, the lens fold (docs/audit/E555-lens-2026-10-09.md): H1, M1,
// L3, L4, L6. Each pin is RED on 03e1e17 (the test knobs grafted inert).
// ============================================================================

// H1 (the lens's `lens1`). A peer that lists its UTXO and never answers a
// node request: under a per-graph budget every pass was deferred with ZERO
// nodes, the sync answered `Ok` and #302's quarantine never moved (10 passes,
// `consecutive_failures` 0). Now a walk that fetched nothing is no progress:
// no record is kept (nothing would be lost), each such pass is a FAILED
// attempt, and after PEER_QUARANTINE_THRESHOLD of them the peer is skipped.
#[tokio::test]
async fn e555f_h1_a_hung_peer_keeps_no_record_and_is_quarantined() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(4);
    let state = Rc::new(RefCell::new(HeadState::default()));
    let remote = RecordingRemote::new(&nodes, &[3]);
    let store = Rc::new(MemoryStorage::new());
    let mut node = Budgeted::over(
        remote.clone(),
        Box::new(HeadChainManager(state.clone())),
        RequestClock::allowing(1000),
        store.clone(),
        Box::new(store),
        true,
    );
    let all: Vec<String> = nodes.iter().map(node_txid).collect();
    let _h = hanging(&mut node, remote, &all);
    let threshold = bsv_overlay_engine::gasp::PEER_QUARANTINE_THRESHOLD;
    for pass in 1..=threshold {
        let (topic, sent) = node.tick().await;
        assert_eq!(sent, txids(&nodes, &[3]), "pass {pass}: the root, hung");
        assert_eq!(topic.deferred_graphs, 0, "pass {pass}: nothing kept");
        assert!(node.store.deferred_graphs().is_empty());
        assert_eq!(node.failures().await, pass, "pass {pass}: a failed attempt");
    }
    assert_eq!(node.store.deferred_graph_writes(), 0);
    for _ in 0..2 {
        let (_, sent) = node.tick().await;
        assert!(sent.is_empty(), "quarantined: skipped");
    }
    assert_eq!(node.failures().await, threshold);
    println!("#555 fold H1: a hung peer, {threshold} failed passes, then quarantined; 0 records");
}

// H1, the deadline arm. The per-PEER deadline cuts a walk that has fetched
// nothing (the root request is the one that overspends): no record, and the
// tick is a failed attempt. On 03e1e17 the 0-node record was saved and the
// tick counted a success (`deferred > 0`).
#[tokio::test]
async fn e555f_h1_the_peer_deadline_over_a_walk_that_fetched_nothing_is_a_failure() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(4);
    let state = Rc::new(RefCell::new(HeadState::default()));
    // One unit: the page request; the root request overspends.
    let mut node = Budgeted::new(
        RecordingRemote::new(&nodes, &[3]),
        Box::new(HeadChainManager(state.clone())),
        1,
    );
    node.engine.set_graph_budget(never(), 100, 60_000);
    for pass in 1..=3u64 {
        let (topic, sent) = node.tick().await;
        assert_eq!(sent, txids(&nodes, &[3]));
        assert_eq!(topic.deadline_dropped_graphs, 1);
        assert!(node.store.deferred_graphs().is_empty(), "nothing kept");
        assert_eq!(node.failures().await, pass);
    }
}

// M1 (the lens's `lens2`). Two deep graphs and a shallow one listed after
// them; per-graph budget 3 calls, per-peer budget 6 units (the worker's 15 s /
// 30 s). On 03e1e17 the two deferred graphs, served first each pass with a
// budget each, took the whole per-peer budget and the shallow graph was never
// reached. Now every resume of a pass shares ONE per-graph budget: the second
// record is HELD BACK untouched and the shallow graph is admitted on the
// first pass that resumes; the deep ones converge in turn.
#[tokio::test]
async fn e555f_m1_the_resumes_of_a_pass_share_one_budget_and_the_rest_is_reached() {
    let (_logs, _guard) = capture_logs();
    let a = salted_chain(12, 1);
    let b = salted_chain(12, 2);
    let c = salted_chain(1, 3);
    let nodes: Vec<GASPNode> = a.iter().chain(b.iter()).chain(c.iter()).cloned().collect();
    let state = Rc::new(RefCell::new(HeadState::default()));
    let mut node = Budgeted::new(
        listing(&nodes, &[(11, 0), (23, 0), (24, 0)]),
        Box::new(HeadChainManager(state.clone())),
        6,
    );
    node.engine.set_graph_budget(never(), 3, 60_000);
    // Pass 1: a walked (3), b cut by the per-peer deadline: both kept.
    node.tick().await;
    assert_eq!(node.store.deferred_graphs().len(), 2);
    // Pass 2: a resumes on the shared budget, b is held back, c is reached.
    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&nodes, &[8, 7, 6, 24]), "a's three, then c");
    assert_eq!(topic.held_back_graphs, 1);
    assert_eq!(topic.deadline_dropped_graphs, 0);
    assert!(
        held(&node.store, &nodes).await.contains(&(24, 0)),
        "c is in"
    );
    let mut ticks = 2;
    while held(&node.store, &nodes).await != vec![(11, 0), (23, 0), (24, 0)] {
        node.tick().await;
        ticks += 1;
        assert!(ticks < 20, "a and b converge");
    }
    assert!(node.store.deferred_graphs().is_empty());
    println!("#555 fold M1: c admitted on pass 2; a and b converged by pass {ticks}");
}

// L3. A sync reads the records' KEYS up front and a record only when its
// UTXO is served: two records held, the peer serves one, one record is read.
#[tokio::test]
async fn e555f_l3_a_record_is_read_only_when_its_utxo_is_served() {
    let (_logs, _guard) = capture_logs();
    let a = salted_chain(5, 1);
    let b = salted_chain(5, 2);
    let nodes: Vec<GASPNode> = a.iter().chain(b.iter()).cloned().collect();
    let state = Rc::new(RefCell::new(HeadState::default()));
    let mut node = Budgeted::new(
        listing(&nodes, &[(4, 0), (9, 0)]),
        Box::new(HeadChainManager(state.clone())),
        1000,
    );
    node.engine.set_graph_budget(never(), 3, 60_000);
    node.tick().await;
    assert_eq!(node.store.deferred_graphs().len(), 2);
    assert_eq!(node.store.deferred_graph_reads(), 0);
    // The peer now lists a's UTXO only, under a per-peer budget that ends
    // the sync inside a's walk (so b is not dropped `not_served`).
    let mut only_a = listing(&nodes, &[(4, 0)]);
    only_a.requests = node.requests.clone();
    node.engine.set_gasp_remote_factory(Box::new(MeteredRemote {
        inner: only_a,
        clock: node.clock.clone(),
    }));
    node.clock.allowance.set(2);
    node.tick().await;
    assert_eq!(node.store.deferred_graph_reads(), 1, "a's record alone");
}

// L4. A walk past its per-graph budget that cannot be KEPT (the storage's own
// ceiling, counted `too_many`) goes on under the per-peer budget alone, as
// before #555, and completes in the pass. On 03e1e17 its UTXO failed on every
// pass and the graph was never admitted.
#[tokio::test]
async fn e555f_l4_a_walk_that_cannot_be_kept_goes_on_and_completes() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(8);
    let state = Rc::new(RefCell::new(HeadState::default()));
    let node = Budgeted::new(
        RecordingRemote::new(&nodes, &[7]),
        Box::new(HeadChainManager(state.clone())),
        1000,
    );
    let mut node = node;
    node.engine.set_graph_budget(never(), 3, 60_000);
    node.store.set_deferred_graph_ceiling(Some(0));
    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&nodes, &[7, 6, 5, 4, 3, 2, 1, 0]));
    assert_eq!(deferral(&topic), (0, 0, 0, vec!["too_many".to_string()]));
    assert_eq!(state.borrow().admitted.len(), 8);
    assert_eq!(held(&node.store, &nodes).await, vec![(7, 0)]);
    assert_eq!(node.store.deferred_graph_writes(), 0);
    assert_eq!(node.failures().await, 0);
}

// L6. A key read that faults leaves the count of held records unknown: the
// (peer, topic) is taken as FULL for the sync, so no new record is saved over
// it (the walk goes on, L4). On 03e1e17 the count read 0 and a (peer, topic)
// could pass its 16 records by up to 16 more.
#[tokio::test]
async fn e555f_l6_a_key_read_fault_saves_no_new_record() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(8);
    let state = Rc::new(RefCell::new(HeadState::default()));
    let mut node = Budgeted::new(
        RecordingRemote::new(&nodes, &[7]),
        Box::new(HeadChainManager(state.clone())),
        1000,
    );
    node.engine.set_graph_budget(never(), 3, 60_000);
    node.store.set_deferred_graph_keys_fault(true);
    let (topic, _sent) = node.tick().await;
    assert_eq!(node.store.deferred_graph_writes(), 0, "no new record");
    assert_eq!(deferral(&topic), (0, 0, 0, vec!["too_many".to_string()]));
    assert_eq!(state.borrow().admitted.len(), 8);
}

// ============================================================================
// bsv-low #555, the delta fold (docs/audit/E555-delta-2026-10-09.md): D-M1,
// D-M2, D-L1, D-L2. Each pin is RED on 0974be5 (the test knobs grafted inert).
// ============================================================================

use bsv_overlay_engine::gasp::{PEER_QUARANTINE_THRESHOLD, PEER_YIELDLESS_SYNCS_ALLOWED};

const STRANGER: &str = "mock://stranger";

// Two peers of one topic, the stranger first: each its own remote.
#[derive(Clone)]
struct TwoPeers {
    stranger: RecordingRemote,
    honest: MeteredRemote,
}

impl GASPRemoteFactory for TwoPeers {
    fn create_remote(&self, peer_url: &str, _topic: &str) -> Box<dyn GASPRemote> {
        if peer_url == STRANGER {
            Box::new(self.stranger.clone())
        } else {
            Box::new(self.honest.clone())
        }
    }
}

// D-M1 (the lens's DELTA-6). A stranger's peer lists 20 fabricated UTXOs,
// serves each root and answers its input with a quick 5xx. On 0974be5 each
// fresh walk was a `fault` deferral with ONE node: 16 records in one tick,
// no time spent, rewritten every tick, and with the storage's ceiling full
// the honest peer's deep graph beside it was refused its record (`too_many`),
// walked from its root to the per-peer deadline on every tick and never
// admitted. Now a fresh walk's fault keeps no record (the UTXO fails, as
// before #555): the stranger holds none, and the honest graph is deferred
// and converges.
#[tokio::test]
async fn e555d_m1_a_5xx_peer_keeps_no_record_and_an_honest_deep_graph_keeps_its_own() {
    let (_logs, _guard) = capture_logs();
    let fabricated: Vec<GASPNode> = (0..20).flat_map(|s| salted_chain(2, 700 + s)).collect();
    let stranger = listing(
        &fabricated,
        &(0..20).map(|i| (2 * i + 1, 0)).collect::<Vec<_>>(),
    );
    stranger
        .faults
        .borrow_mut()
        .extend((0..20).map(|i| node_txid(&fabricated[2 * i])));
    let nodes = chain(8);
    let state = Rc::new(RefCell::new(HeadState::default()));
    let store = Rc::new(MemoryStorage::new());
    let clock = RequestClock::allowing(5);
    let honest = RecordingRemote::new(&nodes, &[7]);
    let requests = honest.requests.clone();
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
                SyncTarget::Peers(vec![STRANGER.to_string(), PEER.to_string()]),
            )]),
            ..Default::default()
        },
    );
    engine.set_gasp_remote_factory(Box::new(TwoPeers {
        stranger,
        honest: MeteredRemote {
            inner: honest,
            clock: clock.clone(),
        },
    }));
    engine.set_peer_sync_budget(clock.budget(), 1);
    engine.set_graph_budget(never(), 100, 60_000);
    // The storage's ceiling: one record over every peer.
    store.set_deferred_graph_ceiling(Some(1));

    let mut ticks = 0;
    while state.borrow().admitted.len() < 8 {
        requests.borrow_mut().clear();
        let result = engine.start_gasp_sync().await.unwrap();
        let topic = &result.topics_synced[TOPIC];
        ticks += 1;
        let stranger_records = store
            .deferred_graphs()
            .iter()
            .filter(|r| r.peer == STRANGER)
            .count();
        assert_eq!(stranger_records, 0, "tick {ticks}: a 5xx keeps no record");
        assert!(
            !topic.dropped_graphs.iter().any(|d| d.reason == "too_many"),
            "tick {ticks}: the honest graph is never refused its record: {:?}",
            topic.dropped_graphs
        );
        assert!(ticks <= 3, "the honest graph converges");
    }
    assert!(store.deferred_graphs().is_empty());
    assert_eq!(held(&store, &nodes).await, vec![(7, 0)]);
    println!("#555 delta D-M1: a 5xx stranger kept 0 records; the honest graph converged in {ticks} ticks");
}

// D-M2 (the lens's DELTA-1). A hostile peer lists ONE fresh fabricated UTXO a
// tick (only it), serves its root and hangs on its input: the fresh walk appends the
// root and is deferred `time`, a walk that "progressed", so on 0974be5 every
// sync was a success and the peer kept its slice for ever (failures 0 over
// any number of ticks). Now a sync that finalized no graph and moved no
// cursor is yieldless; past PEER_YIELDLESS_SYNCS_ALLOWED of them each is a
// failed attempt, and the peer is quarantined PEER_QUARANTINE_THRESHOLD
// syncs later. Amended by the delta-2 fold (D2-M1): the bound is in time
// too, so the storage's clock advances 15 min a tick (`*/15`): the 13th
// yieldless sync is also the first 3 h after the streak began.
#[tokio::test]
async fn e555d_m2_a_peer_serving_one_fresh_root_a_tick_is_quarantined() {
    let (_logs, _guard) = capture_logs();
    let state = Rc::new(RefCell::new(HeadState::default()));
    let store = Rc::new(MemoryStorage::new());
    let mut node = Budgeted::over(
        listing(&salted_chain(2, 0), &[(1, 0)]),
        Box::new(HeadChainManager(state.clone())),
        RequestClock::allowing(1000),
        store.clone(),
        Box::new(store),
        true,
    );
    let allowed = PEER_YIELDLESS_SYNCS_ALLOWED;
    let mut tick = 0u64;
    loop {
        tick += 1;
        if tick > 1 {
            node.store.advance_clock(15 * 60);
        }
        // Only the fresh one is listed: the last tick's record is dropped
        // `not_served` (16 held would send the 17th walk on, L4).
        let fresh = salted_chain(2, 100 + tick);
        let genesis = vec![node_txid(&fresh[0])];
        let mut remote = listing(&fresh, &[(1, 0)]);
        remote.requests = node.requests.clone();
        let _h = hanging(&mut node, remote, &genesis);
        let (topic, sent) = node.tick().await;
        let failures = node.failures().await;
        if tick <= allowed {
            assert!(!sent.is_empty(), "tick {tick}: attempted");
            assert_eq!(failures, 0, "tick {tick}: within the allowance");
            assert_eq!(node.store.peer_yieldless_syncs(PEER, TOPIC), tick);
            assert!(topic.errors.is_empty());
        } else if tick <= allowed + PEER_QUARANTINE_THRESHOLD {
            assert_eq!(failures, tick - allowed, "tick {tick}: a failed attempt");
            assert!(topic
                .errors
                .iter()
                .any(|e| e.contains("finalized no graph")));
        } else {
            assert!(sent.is_empty(), "tick {tick}: quarantined, skipped");
            assert_eq!(failures, PEER_QUARANTINE_THRESHOLD);
            break;
        }
    }
    assert!(state.borrow().admitted.is_empty());
    println!(
        "#555 delta D-M2: a peer serving one fresh root a tick: {allowed} yieldless syncs allowed, quarantined at sync {}",
        allowed + PEER_QUARANTINE_THRESHOLD
    );
}

// D-M2, the other side: an honest deep graph converges inside the allowance
// and its peer is never counted failed. (1) #582's shape: an UNPROVEN root
// over a chain deeper than the budget; its block lands on the fourth tick,
// the root comes back proven, the record is dropped `root_proven` and the
// walk restarts from it (this manager names every link's predecessor, so the
// restarted walk is not shorter here) and completes. (2) A chain walked to
// its end: 3 calls a pass, 33 links, 11 passes. The count resets when the
// graph lands.
#[tokio::test]
async fn e555d_m2_an_honest_deep_graph_converges_inside_the_allowance() {
    let (_logs, _guard) = capture_logs();
    // (1) The unproven root, mined on tick 4.
    let nodes = chain(12);
    let mut unproven = nodes.clone();
    unproven[11].proof = None;
    let state = Rc::new(RefCell::new(HeadState::default()));
    let mut node = Budgeted::new(
        RecordingRemote::new(&unproven, &[11]),
        Box::new(HeadChainManager(state.clone())),
        1000,
    );
    node.engine.set_graph_budget(never(), 3, 60_000);
    for tick in 1..=3u64 {
        node.tick().await;
        assert_eq!(node.store.peer_yieldless_syncs(PEER, TOPIC), tick);
        assert_eq!(node.failures().await, 0);
    }
    let mut mined = RecordingRemote::new(&nodes, &[11]);
    mined.requests = node.requests.clone();
    node.engine.set_gasp_remote_factory(Box::new(MeteredRemote {
        inner: mined,
        clock: node.clock.clone(),
    }));
    let mut ticks = 3;
    while held(&node.store, &nodes).await != vec![(11, 0)] {
        node.tick().await;
        ticks += 1;
        assert_eq!(node.failures().await, 0, "tick {ticks}");
        assert!(
            ticks < PEER_YIELDLESS_SYNCS_ALLOWED,
            "converged inside the allowance"
        );
    }
    assert_eq!(node.store.peer_yieldless_syncs(PEER, TOPIC), 0, "reset");
    let mined_at = ticks;

    // (2) Walked to its end.
    let nodes = chain(33);
    let state = Rc::new(RefCell::new(HeadState::default()));
    let mut node = Budgeted::new(
        RecordingRemote::new(&nodes, &[32]),
        Box::new(HeadChainManager(state.clone())),
        1000,
    );
    node.engine.set_graph_budget(never(), 3, 60_000);
    let mut ticks = 0;
    while state.borrow().admitted.len() < 33 {
        node.tick().await;
        ticks += 1;
        assert_eq!(node.failures().await, 0, "tick {ticks}");
    }
    assert_eq!(ticks, 11);
    assert!(ticks < PEER_YIELDLESS_SYNCS_ALLOWED);
    assert_eq!(node.store.peer_yieldless_syncs(PEER, TOPIC), 0);
    println!("#555 delta D-M2: the unproven root converged on tick {mined_at}, a 33-link walk in 11; no failure");
}

// D-L1 (the lens's DELTA-4). A walk that cannot be kept (the storage's
// ceiling) goes on under the per-peer budget alone; the per-peer deadline
// then cuts it. On 0974be5 its drop was counted twice (`too_many` x2) and the
// storage was asked a second time. Now once, one ask.
#[tokio::test]
async fn e555d_l1_a_walk_that_went_on_is_counted_and_saved_once() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(8);
    let state = Rc::new(RefCell::new(HeadState::default()));
    let mut node = Budgeted::new(
        RecordingRemote::new(&nodes, &[7]),
        Box::new(HeadChainManager(state.clone())),
        5,
    );
    node.engine.set_graph_budget(never(), 3, 60_000);
    node.store.set_deferred_graph_ceiling(Some(0));
    let (topic, _) = node.tick().await;
    assert_eq!(topic.deadline_dropped_graphs, 1);
    assert_eq!(
        deferral(&topic).3,
        vec!["too_many".to_string()],
        "one graph, one drop"
    );
    assert_eq!(node.store.deferred_graph_put_attempts(), 1, "one ask");
}

// D-L2. A store FAULT at a deferral: on 0974be5 the UTXO failed, a graph the
// per-peer budget would have completed in the pass. Now it goes on as the
// other bounds do (counted `store_fault`) and completes.
#[tokio::test]
async fn e555d_l2_a_store_fault_at_a_deferral_goes_on_and_completes() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(8);
    let state = Rc::new(RefCell::new(HeadState::default()));
    let mut node = Budgeted::new(
        RecordingRemote::new(&nodes, &[7]),
        Box::new(HeadChainManager(state.clone())),
        1000,
    );
    node.engine.set_graph_budget(never(), 3, 60_000);
    node.store.set_deferred_graph_put_fault(true);
    let (topic, sent) = node.tick().await;
    assert_eq!(
        sent,
        txids(&nodes, &[7, 6, 5, 4, 3, 2, 1, 0]),
        "the walk goes on"
    );
    assert_eq!(deferral(&topic), (0, 0, 0, vec!["store_fault".to_string()]));
    assert_eq!(state.borrow().admitted.len(), 8, "admitted in the pass");
    assert_eq!(held(&node.store, &nodes).await, vec![(7, 0)]);
    assert_eq!(node.cursor().await, 1);
    assert_eq!(node.store.deferred_graph_put_attempts(), 1);
}

// ============================================================================
// bsv-low #555, the delta-2 fold (the delta-2 lens on ef423da): D2-M1 the
// yieldless bound in time, D2-M2 the share and the health keyed on the
// normalized origin, configured peers reserved, a resumed idle fault bounded,
// D2-L1 a fresh walk that paid keeps its record.
// ============================================================================

use bsv_overlay_engine::gasp::{
    ship_peers_by_origin, DEFERRED_GRAPH_MAX_IDLE_FAULTS, PEER_YIELDLESS_DECAY_SECS,
    PEER_YIELDLESS_SECS_ALLOWED,
};
use bsv_overlay_engine::storage::PeerYieldStreak;

// D2-M1 (the lens's DELTA2-1, at one tick a MINUTE, LOW's beta cadence). An
// honest peer whose only new work is #582's shape: an UNPROVEN head over a
// chain deeper than the per-graph budget, which cannot complete before its
// block. Every sync is yieldless. On ef423da the 13th (13 minutes) was a
// failed attempt and the peer was quarantined at the 20th, then skipped the
// tick its block landed. Now no failure for 30 minutes of yieldless syncs;
// the block lands, the graph converges, the streak ends. And the bound still
// bites: a second node's streak, yieldless for 3 h, fails its next sync.
#[tokio::test]
async fn e555d2_m1_an_honest_unproven_head_at_one_tick_a_minute_is_not_failed_before_three_hours() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(100);
    let mut unproven = nodes.clone();
    unproven[99].proof = None;
    let state = Rc::new(RefCell::new(HeadState::default()));
    let mut node = Budgeted::new(
        RecordingRemote::new(&unproven, &[99]),
        Box::new(HeadChainManager(state.clone())),
        1000,
    );
    node.engine.set_graph_budget(never(), 3, 60_000);
    for tick in 1..=30u64 {
        if tick > 1 {
            node.store.advance_clock(60);
        }
        let (topic, sent) = node.tick().await;
        assert!(!sent.is_empty(), "tick {tick}: synced");
        assert_eq!(
            node.failures().await,
            0,
            "tick {tick}: no failure inside 3 h: {:?}",
            topic.errors
        );
        assert_eq!(node.store.peer_yieldless_syncs(PEER, TOPIC), tick);
    }
    // The block lands: the root is served proven.
    let mut mined = RecordingRemote::new(&nodes, &[99]);
    mined.requests = node.requests.clone();
    node.engine.set_gasp_remote_factory(Box::new(MeteredRemote {
        inner: mined,
        clock: node.clock.clone(),
    }));
    let mut ticks = 30;
    while held(&node.store, &nodes).await != vec![(99, 0)] {
        node.store.advance_clock(60);
        let (_, sent) = node.tick().await;
        ticks += 1;
        assert!(!sent.is_empty(), "tick {ticks}: never skipped");
        assert_eq!(node.failures().await, 0, "tick {ticks}");
        assert!(ticks < 80, "converges");
    }
    assert_eq!(
        node.store.peer_yieldless_syncs(PEER, TOPIC),
        0,
        "a yield ends the streak"
    );

    // The time bound bites: 14 yieldless syncs, the last 3 h after the first.
    let state = Rc::new(RefCell::new(HeadState::default()));
    let mut node = Budgeted::new(
        RecordingRemote::new(&unproven, &[99]),
        Box::new(HeadChainManager(state.clone())),
        1000,
    );
    node.engine.set_graph_budget(never(), 3, 60_000);
    for tick in 1..=13u64 {
        if tick > 1 {
            node.store.advance_clock(60);
        }
        node.tick().await;
        assert_eq!(node.failures().await, 0, "tick {tick}");
    }
    node.store.advance_clock(PEER_YIELDLESS_SECS_ALLOWED);
    let (topic, _) = node.tick().await;
    assert_eq!(node.failures().await, 1, "past both bounds");
    assert!(topic
        .errors
        .iter()
        .any(|e| e.contains("finalized no graph")));
    println!("#555 delta-2 D2-M1: 30 yieldless syncs at one a minute, no failure; converged on tick {ticks}; a 3 h streak fails");
}

// D2-M1, the streak's own rules over MemoryStorage (the model of the
// worker's `PEER_YIELD_UPSERT_SQL`): the age is counted from the FIRST
// yieldless sync; a yield ends the streak; a yieldless sync more than
// PEER_YIELDLESS_DECAY_SECS after the LAST one starts a new streak (quiet
// syncs between leave it).
#[tokio::test]
async fn e555d2_m1_the_streak_carries_its_age_and_decays_after_six_quiet_hours() {
    let store = MemoryStorage::new();
    let streak = |n: u64, age: u64| PeerYieldStreak {
        yieldless_syncs: n,
        secs_since_first: Some(age),
    };
    let y = |yielded: bool| store.record_peer_sync_yield("h", TOPIC, yielded);
    assert_eq!(y(false).await.unwrap(), streak(1, 0));
    store.advance_clock(600);
    assert_eq!(y(false).await.unwrap(), streak(2, 600));
    store.advance_clock(PEER_YIELDLESS_DECAY_SECS);
    assert_eq!(
        y(false).await.unwrap(),
        streak(3, 600 + PEER_YIELDLESS_DECAY_SECS),
        "6 h exactly: the same streak"
    );
    store.advance_clock(PEER_YIELDLESS_DECAY_SECS + 1);
    assert_eq!(
        y(false).await.unwrap(),
        streak(1, 0),
        "past 6 h quiet: a new streak"
    );
    assert_eq!(
        y(true).await.unwrap(),
        PeerYieldStreak::default(),
        "a yield ends it"
    );
    assert_eq!(y(false).await.unwrap(), streak(1, 0));
    // The pure rule: both bounds, and an unknown age never past.
    use bsv_overlay_engine::gasp::yieldless_sync_failed as failed;
    assert!(!failed(&streak(13, PEER_YIELDLESS_SECS_ALLOWED - 1)));
    assert!(!failed(&streak(12, PEER_YIELDLESS_SECS_ALLOWED)));
    assert!(failed(&streak(13, PEER_YIELDLESS_SECS_ALLOWED)));
    assert!(!failed(&PeerYieldStreak {
        yieldless_syncs: 1000,
        secs_since_first: None
    }));
}

// D2-M2 (the lens's DELTA2-3): seven spellings of one server passed
// `is_advertisable_uri` and were seven "hosts", each with its own share of the
// worker's ceiling, its own quarantine and a fresh yieldless streak. Now a
// peer's health and share are keyed on its normalized origin, and the SHIP
// peers of a topic are one per origin (the https, default-port spelling
// first). A subdomain is another host (stated). And the engine writes the
// health under the origin, never the URL.
#[tokio::test]
async fn e555d2_m2_spellings_of_one_server_are_one_origin_and_one_peer() {
    let spellings = [
        "https://evil.example",
        "https://evil.example/",
        "https://evil.example/?1",
        "https://evil.example/?2",
        "https://evil.example:8443",
        "https://EVIL.example/#x",
        "http://user@evil.example.:80/a?b#c",
    ];
    for s in spellings {
        assert_eq!(peer_origin(s), "evil.example", "{s}");
    }
    assert_eq!(peer_origin("https://a.evil.example"), "a.evil.example");
    assert_eq!(peer_origin("https://[::1]:8443/x"), "[::1]");
    assert_eq!(peer_origin(PEER), "head-chain");
    let mut adverts: Vec<String> = spellings.iter().map(|s| s.to_string()).collect();
    adverts.push("https://a.evil.example/?z".into());
    adverts.push("https://Overlay-US-1.bsvb.tech/".into());
    adverts.push("https://evil.example:443/?y".into());
    // One peer per ORIGIN (amended back by the delta-4 fold, D4-M1: the
    // delta-3 fold kept one per `scheme://host[:port]`, five peers here, on
    // one failure row per host): `https` first, then no explicit port.
    assert_eq!(
        ship_peers_by_origin(adverts),
        [
            "https://a.evil.example",
            "https://evil.example",
            "https://overlay-us-1.bsvb.tech"
        ]
    );
    // D3-L1, a stated LIMIT again: a stranger's default-port spelling
    // displaces an honest overlay that serves only on another port.
    assert_eq!(
        ship_peers_by_origin([
            "https://h.example:8443".to_string(),
            "https://h.example/?q".into()
        ]),
        ["https://h.example"],
        "the default port first"
    );
    assert_eq!(
        ship_peers_by_origin([
            "https://h.example:8443".to_string(),
            "http://h.example".into()
        ]),
        ["https://h.example:8443"],
        "https first"
    );
    assert_eq!(
        ship_peers_by_origin([
            "https://h.example:8443/".to_string(),
            "https://H.example:8443/?x".into(),
            "http://h.example:80".into(),
            "http://h.example/#y".into()
        ]),
        ["https://h.example:8443"]
    );
    // The scheme's default port is no explicit port (kept from the delta-3
    // fold): `:443` alone is the peer `https://h.example`, one cursor key.
    assert_eq!(
        ship_peers_by_origin(["https://h.example:443/?y".to_string()]),
        ["https://h.example"]
    );
    // D4-L1: the spellings of a port are spellings of one origin, one peer.
    assert_eq!(
        ship_peers_by_origin(
            [
                "https://h.example:0443",
                "https://h.example:00443/",
                "https://h.example:08443",
                "https+bsvauth://h.example:443",
                "wss://h.example",
                "https://h.example"
            ]
            .map(String::from)
        ),
        ["https://h.example"]
    );

    // The engine's writes: under the origin, nothing under the URL.
    let nodes = chain(4);
    let state = Rc::new(RefCell::new(HeadState::default()));
    let node = Budgeted::new(
        RecordingRemote::new(&nodes, &[3]),
        Box::new(HeadChainManager(state.clone())),
        1000,
    );
    node.tick().await;
    let by_url = node.store.get_peer_sync_health(PEER, TOPIC).await.unwrap();
    let by_origin = node
        .store
        .get_peer_sync_health(&peer_origin(PEER), TOPIC)
        .await
        .unwrap();
    assert_eq!(
        by_url.secs_since_last_attempt, None,
        "nothing under the URL"
    );
    assert_eq!(
        by_origin.secs_since_last_attempt,
        Some(0),
        "the attempt, under the origin"
    );
}

// D2-M2: a record says whether its peer is CONFIGURED (`SyncTarget::Peers`),
// so the worker's ceiling can reserve half of itself for those peers (the
// statement's pin is the worker's `e555d2_m2_*`). A record saved before the
// fold reads as a discovered peer's, with no idle faults.
#[tokio::test]
async fn e555d2_m2_a_configured_peers_record_says_so() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(8);
    let state = Rc::new(RefCell::new(HeadState::default()));
    let mut node = Budgeted::new(
        RecordingRemote::new(&nodes, &[7]),
        Box::new(HeadChainManager(state.clone())),
        1000,
    );
    node.engine.set_graph_budget(never(), 3, 60_000);
    node.tick().await;
    let records = node.store.deferred_graphs();
    assert_eq!(records.len(), 1);
    assert!(records[0].configured, "SyncTarget::Peers");
    let mut old = serde_json::to_value(&records[0]).unwrap();
    let fields = old.as_object_mut().unwrap();
    fields.remove("configured");
    fields.remove("idleFaults");
    let old: DeferredGraph = serde_json::from_value(old).unwrap();
    assert!(!old.configured);
    assert_eq!(old.idle_faults, 0);
}

// D2-M2: a RESUMED walk's quick fault was a free re-deferral (`gasp.rs:1588`
// exempted only fresh walks): a stranger's record was rewritten for one 503 a
// tick and held its place for 60 passes. Now a resumed pass that faults
// having appended nothing is IDLE; the record is kept (an honest transient
// 5xx) until DEFERRED_GRAPH_MAX_IDLE_FAULTS in a row, then dropped
// `idle_faults` and the UTXO fails. A pass that appends resets the count.
#[tokio::test]
async fn e555d2_m2_a_resumed_walk_that_faults_idle_is_dropped_after_three() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(8);
    let remote = RecordingRemote::new(&nodes, &[7]);
    let faults = remote.faults.clone();
    let state = Rc::new(RefCell::new(HeadState::default()));
    let mut node = Budgeted::new(remote, Box::new(HeadChainManager(state.clone())), 1000);
    node.engine.set_graph_budget(never(), 3, 60_000);
    node.tick().await;
    assert_eq!(record_shape(&node.store.deferred_graphs()[0]).4, "calls");
    // Node 4 is the next pending input: the peer 503s it.
    faults.borrow_mut().insert(node_txid(&nodes[4]));
    for idle in 1..DEFERRED_GRAPH_MAX_IDLE_FAULTS {
        let (topic, _) = node.tick().await;
        let records = node.store.deferred_graphs();
        assert_eq!(records.len(), 1, "idle {idle}: kept");
        assert_eq!(
            (records[0].idle_faults, records[0].reason.as_str()),
            (idle, "fault")
        );
        assert!(topic.dropped_graphs.is_empty());
    }
    let (topic, _) = node.tick().await;
    assert!(node.store.deferred_graphs().is_empty(), "dropped");
    assert_eq!(deferral(&topic).3, vec!["idle_faults".to_string()]);
    // A pass that appends resets the count: heal, fault one node deeper.
    faults.borrow_mut().clear();
    node.tick().await; // fresh: 7, 6, 5
    node.tick().await; // resumed: 4, 3, 2
    faults.borrow_mut().insert(node_txid(&nodes[1]));
    node.tick().await;
    assert_eq!(node.store.deferred_graphs()[0].idle_faults, 1);
    faults.borrow_mut().clear();
    node.tick().await;
    assert_eq!(held(&node.store, &nodes).await, vec![(7, 0)], "converged");
}

// D2-L1. A FRESH walk's fault kept no record (the delta fold's D-M1), which
// threw an honest flaky peer's whole pass away: a deep graph got its first
// record only on a clean pass, (1-p)^100. Now a fresh walk that faults after
// HALF its per-graph calls, or once half its time fell due, keeps its record
// (it paid, as a budget cut does); one that faults sooner keeps none (D-M1
// unchanged).
#[tokio::test]
async fn e555d2_l1_a_fresh_walk_that_paid_half_its_budget_keeps_its_record() {
    let (_logs, _guard) = capture_logs();
    let run = |calls: u32, fault_at: usize, sleep: bsv_overlay_engine::engine::SleepFactory| async move {
        let nodes = chain(8);
        let remote = RecordingRemote::new(&nodes, &[7]);
        remote
            .faults
            .borrow_mut()
            .insert(node_txid(&nodes[fault_at]));
        let state = Rc::new(RefCell::new(HeadState::default()));
        let mut node = Budgeted::new(remote, Box::new(HeadChainManager(state.clone())), 1000);
        node.engine.set_graph_budget(sleep, calls, 60_000);
        let (topic, _) = node.tick().await;
        (
            node.store
                .deferred_graphs()
                .iter()
                .map(record_shape)
                .collect::<Vec<_>>(),
            deferral(&topic),
        )
    };
    // 4 calls: 7 and 6 answered, the third (5) faults: half spent, kept.
    let (records, _) = run(4, 5, never()).await;
    assert_eq!(records.len(), 1, "paid by calls");
    assert_eq!((records[0].0, records[0].4.as_str()), (2, "fault"));
    // 6 calls: the second (6) faults, one answered: under half, none.
    let (records, _) = run(6, 6, never()).await;
    assert!(records.is_empty(), "under half: D-M1");
    // 100 calls, half the TIME due at once: the second call faults, kept.
    let half_due: bsv_overlay_engine::engine::SleepFactory = Rc::new(|ms| {
        if ms == 30_000 {
            Box::pin(std::future::ready(()))
        } else {
            Box::pin(std::future::pending::<()>())
        }
    });
    let (records, _) = run(100, 6, half_due).await;
    assert_eq!(records.len(), 1, "paid by time");
    assert_eq!(records[0].0, 1);
}

// Found by the delta-2 fold (not in the lens): the worker's deadlines are
// `async fn` futures (`broadcaster::sleep_ms`), and an `async fn` future
// polled again after it completed PANICS ("`async fn` resumed after
// completion"; in wasm the isolate traps). The shared resume deadline of the
// lens fold's M1 is kept after it fell due and polled again by the next
// record's resume: two records of one (peer, topic) and a spent resume
// budget crashed the sync, on every tick while both were held. The test
// factories were all re-pollable `poll_fn`s. Now every per-graph deadline is
// LATCHED (Ready for good once it fell due).
#[tokio::test]
async fn e555d2_x_a_deadline_that_fell_due_is_never_polled_again() {
    let (_logs, _guard) = capture_logs();
    let a = salted_chain(12, 1);
    let b = salted_chain(12, 2);
    let nodes: Vec<GASPNode> = a.iter().chain(b.iter()).cloned().collect();
    let state = Rc::new(RefCell::new(HeadState::default()));
    let mut node = Budgeted::new(
        listing(&nodes, &[(11, 0), (23, 0)]),
        Box::new(HeadChainManager(state.clone())),
        1000,
    );
    node.engine.set_graph_budget(never(), 3, 60_000);
    node.tick().await;
    assert_eq!(node.store.deferred_graphs().len(), 2);
    // Every deadline an `async` block that is due at once, as an `async fn`
    // sleep whose time has passed.
    let spent: bsv_overlay_engine::engine::SleepFactory = Rc::new(|_| Box::pin(async {}));
    node.engine.set_graph_budget(spent, 3, 60_000);
    let (topic, _) = node.tick().await;
    assert_eq!(topic.held_back_graphs, 2, "both held back, no panic");
    assert_eq!(node.store.deferred_graphs().len(), 2);
}

// ============================================================================
// bsv-low #555, the delta-3 fold (the delta-3 lens on 9280aed): D3-M1 the
// decay never fires for a failed peer; D3-L1 one SHIP peer per canonical
// `scheme://host[:port]`.
// ============================================================================

use bsv_overlay_engine::gasp::PEER_QUARANTINE_REPROBE_SECS;

// D3-M1 (the lens's DELTA3-1b). The hostile peer of e555d_m2_a (one fresh
// root a tick, its input hanging) carried past its quarantine for 200 ticks
// at `*/15`, the clock moving 901 s a tick (any sync of a second or cron
// drift): the probe lands more than PEER_YIELDLESS_DECAY_SECS after the
// peer's last yieldless sync. On 9280aed the probe started a fresh streak
// (count 1, not failed), "progressed", reset the failures to 0 and the peer
// was attended on 100 of the 200 ticks. Now a failed peer's streak does not
// decay: every probe fails and re-arms the quarantine, one probe per reprobe
// window (27 of 200 attended).
#[tokio::test]
async fn e555d3_m1_the_probe_of_a_quarantined_yieldless_peer_fails_and_rearms() {
    let (_logs, _guard) = capture_logs();
    let state = Rc::new(RefCell::new(HeadState::default()));
    let store = Rc::new(MemoryStorage::new());
    let mut node = Budgeted::over(
        listing(&salted_chain(2, 0), &[(1, 0)]),
        Box::new(HeadChainManager(state.clone())),
        RequestClock::allowing(1000),
        store.clone(),
        Box::new(store),
        true,
    );
    let quarantined_at = PEER_YIELDLESS_SYNCS_ALLOWED + PEER_QUARANTINE_THRESHOLD;
    let probe_every = PEER_QUARANTINE_REPROBE_SECS.div_ceil(901);
    let mut attended = 0u64;
    let mut probes = 0u64;
    for tick in 1..=200u64 {
        if tick > 1 {
            node.store.advance_clock(901);
        }
        let fresh = salted_chain(2, 100 + tick);
        let genesis = vec![node_txid(&fresh[0])];
        let mut remote = listing(&fresh, &[(1, 0)]);
        remote.requests = node.requests.clone();
        let _h = hanging(&mut node, remote, &genesis);
        let before = node.failures().await;
        let (_, sent) = node.tick().await;
        let failures = node.failures().await;
        if sent.is_empty() {
            continue;
        }
        attended += 1;
        if tick > quarantined_at {
            probes += 1;
            assert_eq!(
                (tick - quarantined_at) % probe_every,
                0,
                "tick {tick}: attended only at a probe"
            );
            assert_eq!(
                failures,
                before + 1,
                "tick {tick}: the probe continues the streak and fails"
            );
            assert!(
                node.store.peer_yieldless_syncs(&peer_origin(PEER), TOPIC) > quarantined_at,
                "tick {tick}: the streak did not decay"
            );
        }
    }
    let expected = quarantined_at + (200 - quarantined_at) / probe_every;
    assert_eq!(attended, expected, "one probe per reprobe window");
    assert!(probes >= 7);
    assert!(state.borrow().admitted.is_empty());
    println!(
        "#555 delta-3 D3-M1: 901 s a tick, 200 ticks: {attended} attended ({probes} probes, each failed)"
    );
}

// D3-M1, the model: a peer that is not failed still decays (D2-M1), a failed
// one does not, and a success makes it decay again.
#[tokio::test]
async fn e555d3_m1_the_decay_is_only_for_a_peer_that_is_not_failed() {
    let store = MemoryStorage::new();
    let y = || store.record_peer_sync_yield("h", TOPIC, false);
    assert_eq!(y().await.unwrap().yieldless_syncs, 1);
    store
        .record_peer_sync_outcome("h", TOPIC, false)
        .await
        .unwrap();
    store.advance_clock(PEER_YIELDLESS_DECAY_SECS + 1);
    assert_eq!(
        y().await.unwrap(),
        PeerYieldStreak {
            yieldless_syncs: 2,
            secs_since_first: Some(PEER_YIELDLESS_DECAY_SECS + 1)
        },
        "failed: the same streak past 6 h"
    );
    store
        .record_peer_sync_outcome("h", TOPIC, true)
        .await
        .unwrap();
    store.advance_clock(PEER_YIELDLESS_DECAY_SECS + 1);
    assert_eq!(
        y().await.unwrap(),
        PeerYieldStreak {
            yieldless_syncs: 1,
            secs_since_first: Some(0)
        },
        "not failed: a new streak"
    );
}

// ============================================================================
// bsv-low #555, the delta-4 fold (the delta-4 lens on 2c8ab50)
// ============================================================================

use bsv_overlay_engine::gasp::peer_sync_quarantined;

// A SHIP topic's remotes by URL: the honest peer answers on its default
// port (an empty listing, a successful sync), every other URL is a dead port
// (the listing request fails).
struct ByPort {
    honest: RecordingRemote,
    made: Rc<RefCell<Vec<String>>>,
}

struct DeadPort;

impl GASPRemoteFactory for ByPort {
    fn create_remote(&self, peer_url: &str, _topic: &str) -> Box<dyn GASPRemote> {
        self.made.borrow_mut().push(peer_url.to_string());
        if peer_url == "https://honest.example" {
            Box::new(self.honest.clone())
        } else {
            Box::new(DeadPort)
        }
    }
}

#[async_trait(?Send)]
impl GASPRemote for DeadPort {
    async fn get_initial_response(
        &self,
        _request: &GASPInitialRequest,
    ) -> Result<GASPInitialResponse, GASPError> {
        Err(GASPError::RemoteError("connection refused".into()))
    }
    async fn get_initial_reply(
        &self,
        _response: &GASPInitialResponse,
    ) -> Result<GASPInitialReply, GASPError> {
        Err(GASPError::RemoteError("connection refused".into()))
    }
    async fn request_node(
        &self,
        _graph_id: &str,
        _txid: &str,
        _output_index: u32,
        _metadata: bool,
    ) -> Result<GASPNode, GASPError> {
        Err(GASPError::RemoteError("connection refused".into()))
    }
    async fn submit_node(&self, _node: &GASPNode) -> Result<Option<GASPNodeResponse>, GASPError> {
        Err(GASPError::RemoteError("connection refused".into()))
    }
}

// (the honest peer's syncs, the dead ports' slices) over 96 ticks at `*/15`,
// the peers being what `ship_peers_by_origin` makes of the honest advert and
// `dead_ports` stranger adverts of dead ports of the same host.
async fn dead_port_adverts(dead_ports: u16) -> (usize, usize) {
    let mut adverts = vec!["https://honest.example".to_string()];
    adverts.extend((0..dead_ports).map(|p| format!("https://honest.example:{}", 9000 + p)));
    let peers = ship_peers_by_origin(adverts);
    let store = Rc::new(MemoryStorage::new());
    let made = Rc::new(RefCell::new(Vec::new()));
    let mut engine = Engine::new(
        HashMap::from([(
            TOPIC.to_string(),
            Box::new(HeadChainManager(Rc::new(
                RefCell::new(HeadState::default()),
            ))) as Box<dyn TopicManager>,
        )]),
        HashMap::new(),
        Box::new(store.clone()),
        None,
        EngineConfig {
            sync_configuration: HashMap::from([(TOPIC.to_string(), SyncTarget::Peers(peers))]),
            ..Default::default()
        },
    );
    engine.set_gasp_remote_factory(Box::new(ByPort {
        honest: RecordingRemote::new(&[], &[]),
        made: made.clone(),
    }));
    for tick in 0..96 {
        if tick > 0 {
            store.advance_clock(900);
        }
        engine.start_gasp_sync().await.unwrap();
    }
    let made = made.borrow();
    let honest = made
        .iter()
        .filter(|u| *u == "https://honest.example")
        .count();
    (honest, made.len() - honest)
}

// D4-M1 (the lens's DELTA4-1). A stranger advertises dead ports of an honest
// host's name on a SHIP topic. On 2c8ab50 each port was a peer and all of
// them wrote ONE failure row (the origin's): eight quarantined the honest
// peer (synced on 4 of 96 ticks), seven were never quarantined (672 dead-port
// slices in 96 ticks). One peer per origin: the honest advert is the peer,
// attended on every tick, and no dead port is ever asked.
#[tokio::test]
async fn e555d4_m1_dead_port_adverts_of_an_honest_host_do_not_silence_it() {
    let (_logs, _guard) = capture_logs();
    for dead_ports in [8, 12, 7, 1] {
        let (honest, dead) = dead_port_adverts(dead_ports).await;
        assert_eq!(
            (honest, dead),
            (96, 0),
            "{dead_ports} dead-port adverts: the honest peer on every tick, no dead-port slice"
        );
    }
}

// D4-L2 (the lens's DELTA4-2). The hostile peer of e555d3_m1 (one fresh root
// a tick, its input hanging; 901 s a tick, 200 ticks), which now answers its
// PROBE with an empty listing. On 2c8ab50 (and on 9280aed) that sync was a
// success: the failures went to 0, the next yieldless sync began a fresh
// streak, and the peer kept 100 of the 200 ticks. Now a peer at the
// quarantine threshold is re-admitted only by a yield: the empty probe is
// not recorded (the failures stay, the last attempt stays), the next sync
// that serves work continues the streak, fails and re-arms the quarantine.
#[tokio::test]
async fn e555d4_l2_an_empty_listing_at_the_probe_does_not_lift_the_quarantine() {
    let (_logs, _guard) = capture_logs();
    let state = Rc::new(RefCell::new(HeadState::default()));
    let store = Rc::new(MemoryStorage::new());
    let mut node = Budgeted::over(
        listing(&salted_chain(2, 0), &[(1, 0)]),
        Box::new(HeadChainManager(state.clone())),
        RequestClock::allowing(1000),
        store.clone(),
        Box::new(store),
        true,
    );
    let quarantined_at = PEER_YIELDLESS_SYNCS_ALLOWED + PEER_QUARANTINE_THRESHOLD;
    let probe_every = PEER_QUARANTINE_REPROBE_SECS.div_ceil(901);
    let origin = peer_origin(PEER);
    let (mut hostile_slices, mut empty_probes) = (0u64, 0u64);
    let mut was_skipped = false;
    for tick in 1..=200u64 {
        if tick > 1 {
            node.store.advance_clock(901);
        }
        let health = node
            .store
            .get_peer_sync_health(&origin, TOPIC)
            .await
            .unwrap();
        let skipped = peer_sync_quarantined(&health);
        let probe = was_skipped && !skipped;
        was_skipped = skipped;
        let fresh = salted_chain(2, 100 + tick);
        let genesis = vec![node_txid(&fresh[0])];
        // The peer's answer to the first listing after its silent hours: none.
        let mut remote = listing(&fresh, if probe { &[] } else { &[(1, 0)] });
        remote.requests = node.requests.clone();
        let _h = hanging(&mut node, remote, &genesis);
        let before = health.consecutive_failures;
        let (topic, sent) = node.tick().await;
        let after = node
            .store
            .get_peer_sync_health(&origin, TOPIC)
            .await
            .unwrap();
        if skipped {
            assert!(sent.is_empty(), "tick {tick}: quarantined, not asked");
            continue;
        }
        if probe {
            empty_probes += 1;
            assert!(sent.is_empty() && topic.errors.is_empty(), "tick {tick}");
            assert_eq!(
                after.consecutive_failures, before,
                "tick {tick}: an empty probe keeps the failures"
            );
            assert!(before >= PEER_QUARANTINE_THRESHOLD);
            assert!(
                after.secs_since_last_attempt >= Some(PEER_QUARANTINE_REPROBE_SECS),
                "tick {tick}: and is not recorded as an attempt"
            );
            continue;
        }
        hostile_slices += 1;
        if tick > quarantined_at {
            assert_eq!(
                after.consecutive_failures,
                before + 1,
                "tick {tick}: the sync after the empty probe continues the streak and fails"
            );
            assert!(peer_sync_quarantined(&after), "tick {tick}: re-armed");
            assert!(
                node.store.peer_yieldless_syncs(&origin, TOPIC) > quarantined_at,
                "tick {tick}: the streak did not decay"
            );
        }
    }
    assert!(empty_probes >= 7);
    assert!(
        hostile_slices <= quarantined_at + (200 - quarantined_at) / probe_every,
        "{hostile_slices} hostile slices: at most one per reprobe window"
    );
    assert!(state.borrow().admitted.is_empty());
    println!(
        "#555 delta-4 D4-L2: 901 s a tick, 200 ticks: {hostile_slices} hostile slices, {empty_probes} empty probes (none lifted the quarantine)"
    );
}

// D4-L2, the honest side. A quarantined peer that has gone QUIET is not
// stranded: its empty probe is not recorded, so the reprobe window stays
// open and it is attended on every tick after it; the first sync that yields
// lifts the quarantine and clears the streak. What did not change: below the
// threshold an empty listing is a success (it resets the failures), and with
// no per-graph budget #302's rule is the whole rule.
#[tokio::test]
async fn e555d4_l2_a_quarantined_peer_gone_quiet_is_attended_and_a_yield_lifts_it() {
    let (_logs, _guard) = capture_logs();
    let origin = peer_origin(PEER);
    let fail = |store: Rc<MemoryStorage>, n: u64| {
        let origin = origin.clone();
        async move {
            for _ in 0..n {
                store
                    .record_peer_sync_outcome(&origin, TOPIC, false)
                    .await
                    .unwrap();
            }
        }
    };
    let quiet = |node: &mut Budgeted| {
        let mut remote = listing(&salted_chain(2, 1), &[]);
        remote.requests = node.requests.clone();
        hanging(node, remote, &[])
    };
    let new_node = |budgeted_graphs: bool| {
        let state = Rc::new(RefCell::new(HeadState::default()));
        let store = Rc::new(MemoryStorage::new());
        let mut node = Budgeted::over(
            listing(&salted_chain(2, 1), &[]),
            Box::new(HeadChainManager(state.clone())),
            RequestClock::allowing(1000),
            store.clone(),
            Box::new(store),
            true,
        );
        if budgeted_graphs {
            let _ = quiet(&mut node);
        }
        (node, state)
    };

    // Quarantined (eight failed syncs), then quiet past the reprobe window.
    let (mut node, state) = new_node(true);
    fail(node.store.clone(), PEER_QUARANTINE_THRESHOLD).await;
    node.tick().await;
    assert_eq!(node.failures().await, 8, "skipped: quarantined");
    node.store.advance_clock(PEER_QUARANTINE_REPROBE_SECS);
    for tick in 1..=5 {
        let health = node
            .store
            .get_peer_sync_health(&origin, TOPIC)
            .await
            .unwrap();
        assert!(
            !peer_sync_quarantined(&health),
            "quiet tick {tick}: attended"
        );
        let (topic, _) = node.tick().await;
        assert!(topic.errors.is_empty());
        assert_eq!(node.failures().await, 8, "quiet tick {tick}: not lifted");
        node.store.advance_clock(60);
    }
    // Its first sync that yields (a graph lands, the cursor moves) lifts it.
    let fresh = salted_chain(2, 7);
    let mut remote = listing(&fresh, &[(1, 0)]);
    remote.requests = node.requests.clone();
    let _h = hanging(&mut node, remote, &[]);
    let (topic, _) = node.tick().await;
    assert_eq!((topic.finalized_graphs, topic.cursor_moves.len()), (1, 1));
    assert_eq!(state.borrow().admitted.len(), 2);
    assert_eq!(node.failures().await, 0, "a yield lifts the quarantine");
    assert_eq!(node.store.peer_yieldless_syncs(&origin, TOPIC), 0);

    // Below the threshold an empty listing is a success, as before.
    let (node, _) = new_node(true);
    fail(node.store.clone(), PEER_QUARANTINE_THRESHOLD - 1).await;
    node.tick().await;
    assert_eq!(
        node.failures().await,
        0,
        "seven failures, then a quiet sync"
    );

    // With no per-graph budget, #302's rule alone: the empty probe lifts it.
    let (node, _) = new_node(false);
    fail(node.store.clone(), PEER_QUARANTINE_THRESHOLD).await;
    node.store.advance_clock(PEER_QUARANTINE_REPROBE_SECS);
    node.tick().await;
    assert_eq!(node.failures().await, 0, "no graph budget: unchanged");
}

// ============================================================================
// bsv-low #586 (zanaadu-v2 #377): the per-graph budget's BYTES and NODES limbs.
// A graph whose walk is served `max_bytes_fetched` bytes in one pass, or
// appends `max_nodes` nodes, is DEFERRED exactly as at the call budget: the
// record saved, the cursor held, the walk resumed next pass. Never a drop,
// never a refusal. `Engine::set_graph_budget_limbs` does not exist on
// `fbb7fa8`: these pins do not compile there.
// ============================================================================

// The bytes the peer serves for a node, as the limb counts them: the hex of
// its raw transaction and of its proof.
fn served_bytes(node: &GASPNode) -> u64 {
    (node.raw_tx.len() + node.proof.as_ref().map_or(0, String::len)) as u64
}

// PIN B. The 8 proven links of #555's pin A, the call budget far above them
// (100): the BYTES limb is what five nodes weigh. Tick 1 is served five and
// defers with the reason `bytes`; tick 2 resumes from the record, asks only
// the three still pending, and the graph lands whole.
#[tokio::test]
async fn e586_b_a_graph_past_the_bytes_limb_defers_and_completes_over_two_passes() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(8);
    let state = Rc::new(RefCell::new(HeadState::default()));
    let mut node = Budgeted::new(
        RecordingRemote::new(&nodes, &[7]),
        Box::new(HeadChainManager(state.clone())),
        1000,
    );
    node.engine.set_graph_budget(never(), 100, 60_000);
    let five: u64 = nodes[3..].iter().map(served_bytes).sum();
    node.engine.set_graph_budget_limbs(five, 64);

    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&nodes, &[7, 6, 5, 4, 3]));
    assert_eq!(deferral(&topic), (1, 0, 0, vec![]));
    assert!(topic.errors.is_empty(), "a deferral is not an error");
    assert_eq!(topic.discarded_graphs, 0, "nothing is discarded");
    let records = node.store.deferred_graphs();
    assert_eq!(records.len(), 1);
    assert_eq!(
        record_shape(&records[0]),
        (5, vec![outpoint_of(&nodes[2])], 5, 1, "bytes".into())
    );
    assert_eq!(node.cursor().await, 0, "held below the deferred UTXO");
    assert!(state.borrow().admitted.is_empty());
    assert_eq!(node.failures().await, 0, "progress is not a failed attempt");

    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&nodes, &[2, 1, 0]), "only what was pending");
    assert_eq!(deferral(&topic), (0, 1, 1, vec![]));
    assert_eq!(topic.finalized_graphs, 1);
    assert!(node.store.deferred_graphs().is_empty());
    assert_eq!(
        state.borrow().admitted,
        txids(&nodes, &[0, 1, 2, 3, 4, 5, 6, 7])
    );
    assert_eq!(held(&node.store, &nodes).await, vec![(7, 0)]);
    assert_eq!(node.cursor().await, 1);
    println!("#586 PIN B: 8 links under a bytes limb of {five} (five nodes): 2 ticks, 8 requests");
}

// PIN C. The same under the NODES limb: five nodes appended, deferred with
// the reason `nodes`, completed by the next pass.
#[tokio::test]
async fn e586_c_a_graph_past_the_nodes_limb_defers_and_completes_over_two_passes() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(8);
    let state = Rc::new(RefCell::new(HeadState::default()));
    let mut node = Budgeted::new(
        RecordingRemote::new(&nodes, &[7]),
        Box::new(HeadChainManager(state.clone())),
        1000,
    );
    node.engine.set_graph_budget(never(), 100, 60_000);
    node.engine.set_graph_budget_limbs(u64::MAX, 5);

    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&nodes, &[7, 6, 5, 4, 3]));
    assert_eq!(deferral(&topic), (1, 0, 0, vec![]));
    assert!(topic.errors.is_empty());
    assert_eq!(topic.discarded_graphs, 0);
    let records = node.store.deferred_graphs();
    assert_eq!(
        record_shape(&records[0]),
        (5, vec![outpoint_of(&nodes[2])], 5, 1, "nodes".into())
    );
    assert_eq!(node.cursor().await, 0);
    assert!(state.borrow().admitted.is_empty());

    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&nodes, &[2, 1, 0]));
    assert_eq!(deferral(&topic), (0, 1, 1, vec![]));
    assert_eq!(topic.finalized_graphs, 1);
    assert!(node.store.deferred_graphs().is_empty());
    assert_eq!(
        state.borrow().admitted,
        txids(&nodes, &[0, 1, 2, 3, 4, 5, 6, 7])
    );
    assert_eq!(node.cursor().await, 1);
}

// PIN D. A limb is a budget per PASS, never a size limit: with a bytes limb
// of ONE byte every node is bigger than the whole budget, and each pass still
// walks one. The 8 links land on the eighth tick, one request a tick, through
// one record; no pass is an error, a drop or a discarded graph. And the
// resumes of one sync SHARE the limb (M1): two deferred graphs of one topic
// under a nodes limb of 2 resume as one budget, the second held back.
#[tokio::test]
async fn e586_d_a_node_bigger_than_the_limb_is_walked_and_resumes_share_the_limb() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(8);
    let state = Rc::new(RefCell::new(HeadState::default()));
    let mut node = Budgeted::new(
        RecordingRemote::new(&nodes, &[7]),
        Box::new(HeadChainManager(state.clone())),
        1000,
    );
    node.engine.set_graph_budget(never(), 100, 60_000);
    node.engine.set_graph_budget_limbs(1, 64);
    for tick in 0..7 {
        let (topic, sent) = node.tick().await;
        assert_eq!(sent, txids(&nodes, &[7 - tick]), "tick {tick}: one node");
        assert_eq!(deferral(&topic).0, 1, "tick {tick}: deferred");
        assert_eq!(deferral(&topic).3, Vec::<String>::new(), "tick {tick}");
        assert!(topic.errors.is_empty(), "tick {tick}");
        assert_eq!(topic.discarded_graphs, 0, "tick {tick}");
        let records = node.store.deferred_graphs();
        assert_eq!((records.len(), records[0].nodes.len()), (1, tick + 1));
        assert_eq!(records[0].reason, "bytes");
    }
    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&nodes, &[0]));
    assert_eq!(deferral(&topic), (0, 1, 1, vec![]));
    assert_eq!(state.borrow().admitted.len(), 8);
    assert_eq!(node.cursor().await, 1);

    // Two chains of 6, both tips listed, a nodes limb of 2.
    let (one, two) = (salted_chain(6, 1), salted_chain(6, 2));
    let all: Vec<GASPNode> = one.iter().chain(&two).cloned().collect();
    let state = Rc::new(RefCell::new(HeadState::default()));
    let mut node = Budgeted::new(
        RecordingRemote::new(&all, &[5, 11]),
        Box::new(HeadChainManager(state.clone())),
        1000,
    );
    node.engine.set_graph_budget(never(), 100, 60_000);
    node.engine.set_graph_budget_limbs(u64::MAX, 2);
    // Tick 1: each fresh walk has its own limb.
    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&all, &[5, 4, 11, 10]));
    assert_eq!(deferral(&topic), (2, 0, 0, vec![]));
    // Tick 2: the resumes share ONE limb of 2; the second record is held
    // back untouched.
    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&all, &[3, 2]));
    assert_eq!((topic.resumed_graphs, topic.held_back_graphs), (1, 1));
    let passes: Vec<u32> = node
        .store
        .deferred_graphs()
        .iter()
        .map(|r| r.passes)
        .collect();
    assert_eq!(
        passes.iter().sum::<u32>(),
        3,
        "one record resumed: {passes:?}"
    );
}

// A chain of UNPROVEN ~26 KB transactions over a proven genesis: each link
// really spends the `OP_1` output of the one before and carries a head
// transaction's worth of data.
fn fat_unmined_chain(length: usize) -> Vec<GASPNode> {
    fat_unmined(length, 1)
}

// The same, each link spending `each` `OP_1` outputs of the one before
// (`each` 2: the diamond chain of the #586 witness, a covenant output and the
// change, where a transaction is a node once per output index spent).
fn fat_unmined(length: usize, each: u32) -> Vec<GASPNode> {
    let mut nodes = Vec::new();
    let mut previous: Option<String> = None;
    for height in 0..length {
        let mut tx = Transaction::new();
        if let Some(txid) = &previous {
            for vout in 0..each {
                tx.inputs.push(spending(txid.clone(), vout));
            }
        } else {
            tx.inputs.push(coinbase_input());
        }
        for _ in 0..each {
            tx.outputs.push(TransactionOutput::new(
                1000,
                LockingScript::from_hex("51").unwrap(),
            ));
        }
        // OP_FALSE OP_RETURN <26,000 bytes>
        let mut data = String::from("006a4d9065");
        data.push_str(&format!("{height:02x}").repeat(26_000));
        tx.outputs.push(TransactionOutput::new(
            0,
            LockingScript::from_hex(&data).unwrap(),
        ));
        let txid = tx.id();
        let proof = (height == 0).then(|| honest_proof(&txid, 100));
        nodes.push(node_of(&tx, 0, proof));
        previous = Some(txid);
    }
    nodes
}

// THE RECORD PIN. What a deferral saves is the RAW nodes: each one's
// `rawTx` hex as the peer served it, its `proof` hex, who spends it, and the
// inputs still pending. No serialized BEEF, no hydrated transaction: nothing
// in the record's JSON is a BEEF, its keys are the ones named here, and its
// size is the bytes fetched (1.00x, plus its keys). Seven unmined ~26 KB
// links under a nodes limb of 4: four nodes saved, and the next pass
// completes the graph from them (the anchor check runs every script).
#[tokio::test]
async fn e586_e_a_deferred_record_holds_raw_nodes_and_weighs_what_was_fetched() {
    let (_logs, _guard) = capture_logs();
    let nodes = fat_unmined_chain(8);
    let state = Rc::new(RefCell::new(HeadState::default()));
    let mut node = Budgeted::new(
        RecordingRemote::new(&nodes, &[7]),
        Box::new(AdmitsOutputZero(state.clone())),
        1000,
    );
    node.engine.set_graph_budget(never(), 100, 60_000);
    node.engine.set_graph_budget_limbs(u64::MAX, 4);

    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&nodes, &[7, 6, 5, 4]));
    assert_eq!(deferral(&topic), (1, 0, 0, vec![]));
    let records = node.store.deferred_graphs();
    assert_eq!(
        record_shape(&records[0]),
        (4, vec![outpoint_of(&nodes[3])], 4, 1, "nodes".into())
    );
    let fetched: u64 = nodes[4..].iter().map(served_bytes).sum();

    let json = serde_json::to_value(&records[0]).unwrap();
    let keys = |v: &serde_json::Value| -> Vec<String> {
        let mut keys: Vec<String> = v.as_object().unwrap().keys().cloned().collect();
        keys.sort();
        keys
    };
    assert_eq!(
        keys(&json),
        [
            "calls",
            "configured",
            "idleFaults",
            "nodes",
            "outpoint",
            "passes",
            "peer",
            "pending",
            "reason",
            "score",
            "topic"
        ]
    );
    for (walked, served) in json["nodes"].as_array().unwrap().iter().zip([7, 6, 5, 4]) {
        assert_eq!(keys(walked), ["node", "spentBy"]);
        // An unproven node: its graph, its output index, its raw bytes.
        assert_eq!(keys(&walked["node"]), ["graphID", "outputIndex", "rawTx"]);
        assert_eq!(
            walked["node"]["rawTx"], nodes[served].raw_tx,
            "raw, as served"
        );
        assert_eq!(walked["node"]["graphID"], outpoint_of(&nodes[7]));
    }
    assert_eq!(json["nodes"][0]["spentBy"], serde_json::Value::Null);
    assert_eq!(json["nodes"][1]["spentBy"], outpoint_of(&nodes[7]));
    assert_eq!(
        keys(&json["pending"][0]),
        ["graphId", "metadata", "outpoint", "parentProven", "spentBy"]
    );
    // Nothing in it is a BEEF (BRC-62 `0100beef`, BRC-96 `0200beef`, the
    // atomic `01010101`): every long string is a raw transaction.
    fn strings<'v>(v: &'v serde_json::Value, out: &mut Vec<&'v str>) {
        match v {
            serde_json::Value::String(s) => out.push(s),
            serde_json::Value::Array(a) => a.iter().for_each(|x| strings(x, out)),
            serde_json::Value::Object(o) => o.values().for_each(|x| strings(x, out)),
            _ => {}
        }
    }
    let mut all = Vec::new();
    strings(&json, &mut all);
    let long: Vec<&str> = all.into_iter().filter(|s| s.len() > 200).collect();
    assert_eq!(long.len(), 4, "the four raw transactions and nothing else");
    for s in &long {
        assert!(s.starts_with("01000000"), "a raw transaction");
        assert!(!s.contains("0100beef") && !s.contains("0200beef"));
        Transaction::from_hex(s).unwrap();
    }
    let bytes = records[0].byte_size() as u64;
    let ratio = bytes as f64 / fetched as f64;
    println!(
        "#586 PIN E: a record of 4 unmined nodes is {bytes} bytes for {fetched} fetched ({ratio:.4}x)"
    );
    assert!(bytes >= fetched && ratio < 1.01, "{ratio}");

    // The next pass asks the unproven root again (one request), then what
    // is pending, and completes the graph from the record's raw nodes.
    let (topic, sent) = node.tick().await;
    assert_eq!(sent, txids(&nodes, &[7, 3, 2, 1, 0]));
    assert_eq!(deferral(&topic), (0, 1, 1, vec![]));
    assert_eq!(topic.finalized_graphs, 1);
    assert!(topic.errors.is_empty());
    assert_eq!(topic.discarded_graphs, 0);
    assert!(node.store.deferred_graphs().is_empty());
    assert_eq!(state.borrow().admitted.len(), 8);
    assert_eq!(node.cursor().await, 1);
}

// ============================================================================
// bsv-low #586, the lens fold (E586-L1): the bytes limb's DEFAULT sits UNDER
// the record cap. At 4 MiB (and 64 nodes) a pass of 26 KB heads was cut only
// once its record was past `DEFERRED_GRAPH_MAX_BYTES` (1 MiB, about 19 such
// nodes): `too_big`, the limbs off, the walk gone on under the per-peer budget
// alone. The default is now seven eighths of the cap, so the pass a limb cuts
// leaves a record that fits.
// ============================================================================

use bsv_overlay_engine::gasp::{DEFAULT_GRAPH_BUDGET_BYTES, DEFAULT_GRAPH_BUDGET_NODES};

// The record cap these pins were written under (1 MiB, `too_big`), removed by
// bsv-low #585: a record has no byte bound. Kept here as the figure the
// measurements below are stated against, and nothing else.
const DEFERRED_GRAPH_MAX_BYTES: usize = 1 << 20;

// What one tick was SERVED, as the bytes limb counts it (every answer).
fn served_this_tick(nodes: &[GASPNode], sent: &[String]) -> u64 {
    let by_txid: HashMap<String, u64> = nodes
        .iter()
        .map(|n| (node_txid(n), served_bytes(n)))
        .collect();
    sent.iter().map(|txid| by_txid[txid]).sum()
}

// The hex a record holds (each node's raw transaction and proof) and what
// the rest of its JSON weighs: (record bytes, node hex, the rest).
fn record_weight(record: &DeferredGraph) -> (u64, u64, u64) {
    let bytes = record.byte_size() as u64;
    let hex: u64 = record.nodes.iter().map(|w| served_bytes(&w.node)).sum();
    (bytes, hex, bytes - hex)
}

// A node under the ENGINE's defaults: a per-graph budget whose calls and time
// are far away, and the two limbs nobody set.
fn under_default_limbs(nodes: &[GASPNode], tip: usize) -> (Budgeted, Rc<RefCell<HeadState>>) {
    let state = Rc::new(RefCell::new(HeadState::default()));
    let mut node = Budgeted::new(
        RecordingRemote::new(nodes, &[tip]),
        Box::new(AdmitsOutputZero(state.clone())),
        100_000,
    );
    node.engine.set_graph_budget(never(), 100, 60_000);
    (node, state)
}

// PIN L1. 35 links of ~26 KB (34 unmined heads over a proven genesis) under
// the DEFAULT limbs, set by nobody. The bytes limb is reached FIRST (18 heads,
// far under the 64 nodes and the 100 calls): tick 1 defers with the reason
// `bytes` and a record UNDER the cap; tick 2 resumes from it (the unproven
// root asked again, then only what is pending: never a fresh walk twice) and
// the graph lands whole. Nothing is `too_big`, no walk goes on.
#[tokio::test]
async fn e586f_l1_a_graph_of_26kb_heads_fills_the_bytes_limb_first_and_its_record_fits() {
    let (_logs, _guard) = capture_logs();
    let nodes = fat_unmined_chain(35);
    let head = served_bytes(&nodes[34]);
    let (node, state) = under_default_limbs(&nodes, 34);

    let (topic, sent) = node.tick().await;
    let per_pass = sent.len();
    assert_eq!(per_pass, 18, "26 KB heads a fresh pass is served");
    assert_eq!(deferral(&topic), (1, 0, 0, vec![]));
    assert!(topic.errors.is_empty());
    let records = node.store.deferred_graphs();
    assert_eq!(
        records[0].reason, "bytes",
        "the bytes limb is reached first"
    );
    assert_eq!(records[0].nodes.len(), 18);
    let (bytes, hex, rest) = record_weight(&records[0]);
    assert!(
        bytes as usize <= DEFERRED_GRAPH_MAX_BYTES,
        "the record fits: {bytes}"
    );
    // The limb is read before a step: the pass ends on the node that
    // crosses it, at most one node past the limb.
    let served = served_this_tick(&nodes, &sent);
    assert!(served >= DEFAULT_GRAPH_BUDGET_BYTES && served < DEFAULT_GRAPH_BUDGET_BYTES + head);
    assert_eq!(node.cursor().await, 0, "held below the deferred UTXO");
    assert!(state.borrow().admitted.is_empty());
    assert_eq!(
        DEFAULT_GRAPH_BUDGET_BYTES, 917_504,
        "a budget per pass; no longer derived from a record cap (bsv-low #585)"
    );
    assert_eq!(DEFAULT_GRAPH_BUDGET_NODES, 64);
    println!(
        "#586 FOLD L1 (chain): limb {DEFAULT_GRAPH_BUDGET_BYTES}, {per_pass} heads of {head} hex served ({served}); record {bytes} = {hex} hex + {rest} ({:.4}x the hex, {:.4}x the served), {} bytes under the cap",
        bytes as f64 / hex as f64,
        bytes as f64 / served as f64,
        DEFERRED_GRAPH_MAX_BYTES as u64 - bytes
    );

    // Tick 2: the root again (it is unproven), then the 17 still unwalked.
    let (topic, sent) = node.tick().await;
    let mut expected = vec![34];
    expected.extend((0..17).rev());
    assert_eq!(sent, txids(&nodes, &expected), "only what was pending");
    assert_eq!(deferral(&topic), (0, 1, 1, vec![]), "converged, no drop");
    assert_eq!(topic.finalized_graphs, 1);
    assert!(topic.errors.is_empty());
    assert_eq!(topic.discarded_graphs, 0);
    assert!(node.store.deferred_graphs().is_empty());
    assert_eq!(state.borrow().admitted.len(), 35);
    assert_eq!(node.cursor().await, 1);
}

// The overhead of a record over the bytes it was served, MEASURED on the
// shapes the default's margin (an eighth of the cap, 131,072 bytes) must
// cover. (1) The #586 witness's diamond chain (7 transactions of ~26 KB, each
// spending two outputs of the one before: 13 nodes, 23 requests): a record
// holds a transaction once per OUTPOINT and the limb counts every answer, a
// repeat too, so the record is lighter than what was served. (2) Small proven
// nodes, where the JSON around a node outweighs its hex: the NODES limb (64)
// is what bounds that.
#[tokio::test]
async fn e586f_l1_the_margin_under_the_cap_covers_the_measured_overhead() {
    let (_logs, _guard) = capture_logs();
    let margin = (DEFERRED_GRAPH_MAX_BYTES as u64) - DEFAULT_GRAPH_BUDGET_BYTES;
    assert_eq!(margin, 131_072);

    // (1) The diamond chain: deferred by the bytes limb, converged by the
    // next pass, every record under the cap.
    let nodes = fat_unmined(7, 2);
    let (node, state) = under_default_limbs(&nodes, 6);
    let (topic, sent) = node.tick().await;
    assert_eq!(deferral(&topic), (1, 0, 0, vec![]));
    let records = node.store.deferred_graphs();
    assert_eq!(records[0].reason, "bytes");
    let (bytes, hex, rest) = record_weight(&records[0]);
    let served = served_this_tick(&nodes, &sent);
    assert!(bytes as usize <= DEFERRED_GRAPH_MAX_BYTES);
    assert!(bytes < served, "repeats are served and not kept");
    println!(
        "#586 FOLD L1 (diamond): {} requests served {served}; record of {} nodes, {} pending: {bytes} = {hex} hex + {rest} ({:.4}x the hex, {:.4}x the served)",
        sent.len(),
        records[0].nodes.len(),
        records[0].pending.len(),
        bytes as f64 / hex as f64,
        bytes as f64 / served as f64
    );
    let per_node = rest / records[0].nodes.len() as u64;
    let (topic, _) = node.tick().await;
    assert_eq!(deferral(&topic), (0, 1, 1, vec![]));
    assert_eq!(topic.finalized_graphs, 1);
    assert_eq!(state.borrow().admitted.len(), 7, "the seven transactions");

    // (2) 200 small proven links: the nodes limb cuts each pass at 64, and
    // what is not hex is most of the record.
    let small = chain(200);
    let state = Rc::new(RefCell::new(HeadState::default()));
    let mut node = Budgeted::new(
        RecordingRemote::new(&small, &[199]),
        Box::new(HeadChainManager(state.clone())),
        100_000,
    );
    node.engine.set_graph_budget(never(), 100, 60_000);
    let (topic, _) = node.tick().await;
    assert_eq!(deferral(&topic), (1, 0, 0, vec![]));
    let records = node.store.deferred_graphs();
    assert_eq!(
        (records[0].reason.as_str(), records[0].nodes.len()),
        ("nodes", 64)
    );
    let (bytes, hex, rest) = record_weight(&records[0]);
    let small_per_node = rest / 64;
    println!(
        "#586 FOLD L1 (small nodes): record of 64 nodes: {bytes} = {hex} hex + {rest} ({:.4}x the hex), {small_per_node} bytes of JSON a node (the diamond: {per_node})",
        bytes as f64 / hex as f64
    );
    // The margin: the JSON of the 64 nodes one pass may append, and what is
    // left for the node that crosses the limb and the pending inputs.
    let json_of_a_pass = u64::from(DEFAULT_GRAPH_BUDGET_NODES) * small_per_node.max(per_node);
    assert!(json_of_a_pass * 4 < margin, "{json_of_a_pass} of {margin}");
    println!(
        "#586 FOLD L1 (margin): {margin} bytes; 64 nodes of JSON take {json_of_a_pass}; {} are left for the crossing node and the pending inputs",
        margin - json_of_a_pass
    );
}

// bsv-low #585, DOOR 4: the limit `e586f_l1_limit` stated is gone. A record is
// the walk of EVERY pass and has no byte bound: a 46-link unmined graph of
// ~26 KB heads (about 2.4 MB of hex, 1.2 MB of raw transactions) is deferred
// by the bytes limb at each pass (18 heads, then 17 under the re-asked root),
// its record past 1 MiB from the second pass on, and resumed to completion at
// the third. No drop, no walk "gone on": every pass runs under its budget.
// RED on `e8ab762`: the second pass is `too_big`, its record deleted and the
// walk gone on under the per-peer budget alone (the old pin's 36 links).
#[tokio::test]
async fn e585_d4_a_46_node_unmined_graph_of_26kb_heads_is_deferred_and_resumed_whole() {
    let (_logs, _guard) = capture_logs();
    let nodes = fat_unmined_chain(46);
    let hex: u64 = nodes.iter().map(served_bytes).sum();
    let (node, state) = under_default_limbs(&nodes, 45);

    let (topic, sent) = node.tick().await;
    assert_eq!((sent.len(), deferral(&topic)), (18, (1, 0, 0, vec![])));
    let first = node.store.deferred_graphs()[0].byte_size();
    assert!(first <= DEFERRED_GRAPH_MAX_BYTES, "{first}");

    let (topic, sent) = node.tick().await;
    assert_eq!(sent.len(), 18, "the root again, then 17 under the limb");
    assert_eq!(
        deferral(&topic),
        (1, 1, 0, vec![]),
        "deferred again, no drop"
    );
    assert!(topic.errors.is_empty(), "{:?}", topic.errors);
    assert_eq!(topic.finalized_graphs, 0, "no walk went on past its budget");
    let records = node.store.deferred_graphs();
    assert_eq!((records.len(), records[0].nodes.len()), (1, 35));
    assert_eq!(records[0].reason, "bytes");
    let second = records[0].byte_size();
    assert!(
        second > DEFERRED_GRAPH_MAX_BYTES,
        "past the old cap: {second}"
    );
    assert!(state.borrow().admitted.is_empty());
    assert_eq!(node.cursor().await, 0, "held below the deferred UTXO");

    let (topic, sent) = node.tick().await;
    let mut expected = vec![45];
    expected.extend((0..11).rev());
    assert_eq!(sent, txids(&nodes, &expected), "only what was pending");
    assert_eq!(deferral(&topic), (0, 1, 1, vec![]), "converged, no drop");
    assert_eq!(topic.finalized_graphs, 1);
    assert!(topic.errors.is_empty());
    assert!(node.store.deferred_graphs().is_empty());
    assert_eq!(state.borrow().admitted.len(), 46);
    assert_eq!(node.cursor().await, 1);
    println!(
        "#585 DOOR 4: 46 links, {hex} hex served once each; records of {first} then {second} bytes ({:.2}x the old 1 MiB cap); 18 + 18 + 12 requests over three passes, converged",
        second as f64 / DEFERRED_GRAPH_MAX_BYTES as f64
    );
}

// bsv-low #585, DOOR 4: the storage's ceiling is a BUDGET of room, never the
// loss of a walk. A RESUMED walk whose bigger record finds no room (the
// worker's byte bounds; `AtCeiling`) KEEPS the record it holds: the pass is
// counted `too_many` and goes on under the per-peer budget alone (L4), and a
// pass that does not finish resumes from the held record. Here the peer's
// deadline cuts the walk that went on, and the next pass resumes from the
// record, never from the root. RED on `e8ab762` (the knob grafted):
// the held record is DELETED at the refusal.
#[tokio::test]
async fn e585_d4_a_resumed_walk_refused_room_keeps_its_record() {
    let (_logs, _guard) = capture_logs();
    let nodes = fat_unmined_chain(46);
    let (node, state) = under_default_limbs(&nodes, 45);
    let (topic, _) = node.tick().await;
    assert_eq!(deferral(&topic), (1, 0, 0, vec![]));
    let held = node.store.deferred_graphs()[0].clone();
    // Room for the record held, none for the second pass's.
    node.store
        .set_deferred_graph_byte_ceiling(Some(held.byte_size() + 1));
    // The tick may spend the list, the 18 requests of a pass and three more:
    // the walk that goes on at the refusal is cut by the per-peer deadline.
    node.clock.allowance.set(1 + 18 + 3);
    let (topic, _) = node.tick().await;
    assert_eq!(
        topic.dropped_graphs.len(),
        1,
        "the pass that found no room is counted"
    );
    assert_eq!(topic.dropped_graphs[0].reason, "too_many");
    let records = node.store.deferred_graphs();
    assert_eq!(records.len(), 1, "the held record is KEPT");
    assert_eq!(
        (records[0].nodes.len(), records[0].passes),
        (held.nodes.len(), held.passes),
        "as it was"
    );
    assert!(state.borrow().admitted.is_empty());
    assert_eq!(node.cursor().await, 0);
    // Room again: the next pass RESUMES from the held record (18 nodes in
    // hand: the root's re-ask and the pending inputs, never the 18 again).
    node.store.set_deferred_graph_byte_ceiling(None);
    node.clock.allowance.set(100_000);
    let (topic, sent) = node.tick().await;
    assert_eq!(sent.len(), 18, "the root again, then 17 under the limb");
    assert_eq!(sent[1], node_txid(&nodes[27]), "from where the record ends");
    assert_eq!(deferral(&topic), (1, 1, 0, vec![]));
    let (topic, _) = node.tick().await;
    assert_eq!(deferral(&topic), (0, 1, 1, vec![]));
    assert_eq!(state.borrow().admitted.len(), 46);
}

// The NOTE of the lens (the `root_proven` restart). The walk that restarts
// from a root served PROVEN is fresh, but its PASS is not: what the pass
// already spent of each counted limb is carried to it. The re-ask of the root
// is a call and its bytes; it appends nothing, so the nodes it carries are 0
// (the record's own nodes were appended by earlier passes), and the restarted
// walk appends exactly the nodes limb.
#[tokio::test]
async fn e586f_n_a_root_proven_restart_carries_what_the_pass_spent_of_every_limb() {
    let (_logs, _guard) = capture_logs();
    let nodes = chain(6);
    let mut unproven = nodes.clone();
    unproven[5].proof = None;
    // (bytes limb, nodes limb) of the restarting pass, what it sends after
    // the block landed, and the reason it defers with.
    let two_and_a_root = 2 * served_bytes(&nodes[5]) + served_bytes(&nodes[4]);
    let cases = [
        // The re-ask (root), the root again, node 4: the limb, with the
        // re-ask's bytes. Not carried, node 3 would be served too.
        ((two_and_a_root, 64), vec![5, 5, 4], "bytes"),
        // Two nodes appended by the restarted walk: the root and node 4.
        ((u64::MAX, 2), vec![5, 5, 4], "nodes"),
    ];
    for ((bytes, max_nodes), sends, reason) in cases {
        let state = Rc::new(RefCell::new(HeadState::default()));
        let mut node = Budgeted::new(
            RecordingRemote::new(&unproven, &[5]),
            Box::new(HeadChainManager(state.clone())),
            1000,
        );
        node.engine.set_graph_budget(never(), 100, 60_000);
        node.engine.set_graph_budget_limbs(u64::MAX, 2);
        let (topic, sent) = node.tick().await;
        assert_eq!(sent, txids(&nodes, &[5, 4]));
        assert_eq!(deferral(&topic), (1, 0, 0, vec![]));

        // The block lands: the peer serves the root with its proof.
        let mut mined = RecordingRemote::new(&nodes, &[5]);
        mined.requests = node.requests.clone();
        node.engine.set_gasp_remote_factory(Box::new(MeteredRemote {
            inner: mined,
            clock: node.clock.clone(),
        }));
        node.engine.set_graph_budget_limbs(bytes, max_nodes);
        let (topic, sent) = node.tick().await;
        assert_eq!(sent, txids(&nodes, &sends), "{reason}");
        assert_eq!(
            deferral(&topic),
            (1, 1, 0, vec!["root_proven".to_string()]),
            "{reason}"
        );
        let record = &node.store.deferred_graphs()[0];
        assert_eq!(
            record_shape(record),
            (2, vec![outpoint_of(&nodes[3])], 3, 1, reason.to_string())
        );
    }
}
