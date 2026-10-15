//! The world of the GASP shadow's witnesses (bsv-low #657, R7.7 item 5):
//! proven transactions served by a scripted peer, a stateless manager, a
//! recording storage that yields once per call, and a counting executor.
//! Everything here is the engine's public API at A1's tip (`9d5a75a`), so the
//! `off` recording runs unchanged on that base.
#![allow(dead_code)]

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use async_trait::async_trait;
use bsv_overlay_engine::engine::{Engine, EngineConfig, SleepFactory};
use bsv_overlay_engine::gasp::{GASPError, GASPRemote, GASPRemoteFactory};
use bsv_overlay_engine::storage::memory::MemoryStorage;
use bsv_overlay_engine::storage::{
    PeerSyncHealth, PeerYieldStreak, Storage, StorageError, TransactionBeef,
};
use bsv_overlay_engine::topic_manager::{TopicManager, TopicManagerError};
use bsv_overlay_engine::types::*;
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{
    ChainTracker, ChainTrackerError, MerklePath, MerklePathLeaf, Transaction, TransactionInput,
    TransactionOutput,
};

pub const TOPIC: &str = "tm_shadow";
pub const PEER: &str = "mock://shadow-peer";

/// The `n`th genesis's one input: an outpoint of the all-zero txid (no coin).
fn genesis_input(n: u32) -> TransactionInput {
    let mut input = TransactionInput::new("00".repeat(32), u32::MAX - n);
    input.unlocking_script = Some(UnlockingScript::from_binary(&[0x51]).unwrap());
    input
}

fn spends_a_coin(input: &TransactionInput) -> bool {
    input.get_source_txid().ok() != Some("00".repeat(32))
}

fn outputs(tx: &mut Transaction, count: usize) {
    for _ in 0..count {
        tx.outputs.push(TransactionOutput::new(
            1000,
            LockingScript::from_hex("51").unwrap(),
        ));
    }
}

/// A genesis with `count` outputs.
pub fn genesis(tag: u32, count: usize) -> Transaction {
    let mut tx = Transaction::new();
    tx.inputs.push(genesis_input(tag));
    outputs(&mut tx, count);
    tx
}

/// A transaction of `count` outputs spending `parent`'s output `vout`.
pub fn child(parent: &Transaction, vout: u32, count: usize) -> Transaction {
    let mut tx = Transaction::new();
    let mut input = TransactionInput::new(parent.id(), vout);
    input.unlocking_script = Some(UnlockingScript::from_binary(&[]).unwrap());
    tx.inputs.push(input);
    outputs(&mut tx, count);
    tx
}

/// A transaction of `count` outputs spending each of `sources` (a fan-in).
pub fn spends(sources: &[(&Transaction, u32)], count: usize) -> Transaction {
    let mut tx = Transaction::new();
    for (parent, vout) in sources {
        let mut input = TransactionInput::new(parent.id(), *vout);
        input.unlocking_script = Some(UnlockingScript::from_binary(&[]).unwrap());
        tx.inputs.push(input);
    }
    outputs(&mut tx, count);
    tx
}

pub fn honest_proof(txid: &str, height: u32) -> String {
    MerklePath::new(
        height,
        vec![vec![MerklePathLeaf::new_txid(0, txid.to_string())]],
    )
    .unwrap()
    .to_hex()
}

/// A proof that parses and names the transaction, whose root no tracker knows.
pub fn fabricated_proof(txid: &str, height: u32) -> String {
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

pub fn node(tx: &Transaction, proof: String) -> GASPNode {
    GASPNode {
        graph_id: String::new(),
        raw_tx: tx.to_hex(),
        output_index: 0,
        proof: Some(proof),
        tx_metadata: None,
        output_metadata: None,
        inputs: None,
    }
}

/// Admits every output and retains no coin; names every coin it spends. No
/// state: the served and every shadow engine judge alike. `fails_at_finalize`
/// makes the manager err on that txid under `historical-tx-no-spv` only (the
/// finalize, not the anchor replay).
pub struct AdmitAll {
    pub fails_at_finalize: Option<String>,
}

#[async_trait(?Send)]
impl TopicManager for AdmitAll {
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
        if mode == SubmitMode::HistoricalTxNoSpv && self.fails_at_finalize == Some(tx.id()) {
            return Err(TopicManagerError::InvalidScript(
                "the finalize refuses this body".into(),
            ));
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

pub fn managers(fails_at_finalize: Option<String>) -> HashMap<String, Box<dyn TopicManager>> {
    HashMap::from([(
        TOPIC.to_string(),
        Box::new(AdmitAll { fails_at_finalize }) as Box<dyn TopicManager>,
    )])
}

pub type Events = Rc<RefCell<Vec<String>>>;

/// A peer serving `nodes`, listing `utxos`; down, withholding and faulting
/// on demand between ticks. Every call it answers is an event.
#[derive(Clone)]
pub struct ChaosRemote {
    pub nodes: Rc<HashMap<String, GASPNode>>,
    pub utxos: Rc<Vec<GASPOutput>>,
    pub down: Rc<Cell<bool>>,
    /// Scores at or above this are withheld from the listing (a partition).
    pub withhold_from: Rc<Cell<Option<f64>>>,
    /// Txids whose node request blips (an HTTP 500).
    pub faults: Rc<RefCell<HashSet<String>>>,
    pub events: Events,
}

impl ChaosRemote {
    /// `listed`: (transaction, vout, score).
    pub fn new(nodes: &[GASPNode], listed: &[(&Transaction, u32, f64)], events: &Events) -> Self {
        Self {
            nodes: Rc::new(
                nodes
                    .iter()
                    .map(|n| (Transaction::from_hex(&n.raw_tx).unwrap().id(), n.clone()))
                    .collect(),
            ),
            utxos: Rc::new(
                listed
                    .iter()
                    .map(|(tx, vout, score)| GASPOutput {
                        txid: tx.id(),
                        output_index: *vout,
                        score: *score,
                    })
                    .collect(),
            ),
            down: Rc::default(),
            withhold_from: Rc::default(),
            faults: Rc::default(),
            events: events.clone(),
        }
    }
}

impl GASPRemoteFactory for ChaosRemote {
    fn create_remote(&self, peer_url: &str, topic: &str) -> Box<dyn GASPRemote> {
        self.events
            .borrow_mut()
            .push(format!("remote.create {peer_url} {topic}"));
        Box::new(self.clone())
    }
}

#[async_trait(?Send)]
impl GASPRemote for ChaosRemote {
    async fn get_initial_response(
        &self,
        request: &GASPInitialRequest,
    ) -> Result<GASPInitialResponse, GASPError> {
        self.events
            .borrow_mut()
            .push(format!("remote.initial since={}", request.since));
        if self.down.get() {
            return Err(GASPError::RemoteError("peer down".into()));
        }
        let cap = self.withhold_from.get();
        Ok(GASPInitialResponse {
            utxo_list: self
                .utxos
                .iter()
                .filter(|u| u.score >= request.since as f64 && cap.is_none_or(|c| u.score < c))
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
        self.events.borrow_mut().push(format!(
            "remote.node {graph_id} {txid}.{output_index} {metadata}"
        ));
        if self.down.get() || self.faults.borrow().contains(txid) {
            return Err(GASPError::RemoteError(format!("HTTP 500 for {txid}")));
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
        panic!("pull-only sync never submits")
    }
}

/// A tracker of exactly the honest roots it was given.
#[derive(Clone, Default)]
pub struct KnownRoots(pub Arc<Mutex<HashSet<(u32, String)>>>);

impl KnownRoots {
    pub fn of(nodes: &[GASPNode]) -> Self {
        let tracker = Self::default();
        for node in nodes {
            let tx = Transaction::from_hex(&node.raw_tx).unwrap();
            let path = MerklePath::from_hex(node.proof.as_ref().unwrap()).unwrap();
            tracker.0.lock().unwrap().insert((
                path.block_height,
                path.compute_root(Some(&tx.id())).unwrap(),
            ));
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
        Ok(self.0.lock().unwrap().contains(&(height, root.to_string())))
    }
    async fn current_height(&self) -> Result<u32, ChainTrackerError> {
        Ok(1_000)
    }
}

/// A served engine over `store` syncing `TOPIC` from `PEER`.
pub fn served_engine(
    store: Box<dyn Storage>,
    remote: &ChaosRemote,
    tracker: Option<KnownRoots>,
    fails_at_finalize: Option<String>,
) -> Engine {
    let mut engine = Engine::with_chain_tracker(
        managers(fails_at_finalize),
        HashMap::new(),
        store,
        None,
        None,
        tracker.map(|t| Box::new(t) as Box<dyn ChainTracker>),
        EngineConfig {
            sync_configuration: HashMap::from([(
                TOPIC.to_string(),
                SyncTarget::Peers(vec![PEER.to_string()]),
            )]),
            ..Default::default()
        },
    );
    engine.set_gasp_remote_factory(Box::new(remote.clone()));
    engine
}

/// A per-peer budget whose deadline never falls due: the finalize hook's
/// path (each graph submitted as it finalizes), with nothing dropped.
pub fn never_due() -> SleepFactory {
    Rc::new(|_| Box::pin(std::future::pending()))
}

/// What the served store holds of `txs` under `TOPIC`: each applied row and
/// each output's row (spent, consumers, consumed, satoshis, script, BEEF),
/// never its score (a wall clock).
pub async fn served_rows(store: &dyn Storage, txs: &[&Transaction]) -> BTreeMap<String, String> {
    let mut rows = BTreeMap::new();
    for tx in txs {
        let txid = tx.id();
        let applied = store
            .does_applied_transaction_exist(&AppliedTransaction {
                txid: txid.clone(),
                topic: TOPIC.into(),
            })
            .await
            .unwrap();
        rows.insert(format!("applied {txid}"), applied.to_string());
        for vout in 0..tx.outputs.len() as u32 {
            let row = store
                .find_output(&txid, vout, Some(TOPIC), None, true)
                .await
                .unwrap()
                .map(|o| {
                    format!(
                        "spent={} sats={} script={} consumed={:?} by={:?} height={:?} beef={}",
                        o.spent,
                        o.satoshis,
                        hex::encode(&o.output_script),
                        o.outputs_consumed,
                        o.consumed_by,
                        o.block_height,
                        o.beef.map_or(0, |b| b.len())
                    )
                })
                .unwrap_or_else(|| "absent".into());
            rows.insert(format!("output {txid}.{vout}"), row);
        }
    }
    rows
}

/// A `MemoryStorage` whose every call is recorded and first YIELDS once, so a
/// counting executor sees every await the engine makes of its storage.
pub struct RecordingStore {
    pub inner: Rc<MemoryStorage>,
    pub events: Events,
}

/// Pending once, then ready: one await point the executor counts.
pub struct YieldOnce(bool);
impl Future for YieldOnce {
    type Output = ();
    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

impl RecordingStore {
    async fn note(&self, event: String) {
        self.events.borrow_mut().push(event);
        YieldOnce(false).await;
    }
}

fn outpoints(list: &[Outpoint]) -> String {
    list.iter()
        .map(|o| format!("{}.{}", o.txid, o.output_index))
        .collect::<Vec<_>>()
        .join(",")
}

#[async_trait(?Send)]
impl Storage for RecordingStore {
    async fn insert_output(&self, o: &Output) -> Result<(), StorageError> {
        self.note(format!(
            "store.insert_output {}.{} {} spent={} sats={} consumed=[{}] by=[{}] beef={}",
            o.txid,
            o.output_index,
            o.topic,
            o.spent,
            o.satoshis,
            outpoints(&o.outputs_consumed),
            outpoints(&o.consumed_by),
            o.beef.as_ref().map_or(0, Vec::len)
        ))
        .await;
        self.inner.insert_output(o).await
    }
    async fn delete_output(&self, txid: &str, vout: u32, topic: &str) -> Result<(), StorageError> {
        self.note(format!("store.delete_output {txid}.{vout} {topic}"))
            .await;
        self.inner.delete_output(txid, vout, topic).await
    }
    async fn mark_utxo_as_spent(
        &self,
        txid: &str,
        vout: u32,
        topic: &str,
    ) -> Result<(), StorageError> {
        self.note(format!("store.mark_spent {txid}.{vout} {topic}"))
            .await;
        self.inner.mark_utxo_as_spent(txid, vout, topic).await
    }
    async fn update_consumed_by(
        &self,
        txid: &str,
        vout: u32,
        topic: &str,
        by: &[Outpoint],
    ) -> Result<(), StorageError> {
        self.note(format!(
            "store.consumed_by {txid}.{vout} {topic} [{}]",
            outpoints(by)
        ))
        .await;
        self.inner.update_consumed_by(txid, vout, topic, by).await
    }
    async fn update_transaction_beef(&self, txid: &str, beef: &[u8]) -> Result<(), StorageError> {
        self.note(format!("store.update_beef {txid} {}", beef.len()))
            .await;
        self.inner.update_transaction_beef(txid, beef).await
    }
    async fn mark_transaction_proven(&self, txid: &str) -> Result<(), StorageError> {
        self.note(format!("store.mark_proven {txid}")).await;
        self.inner.mark_transaction_proven(txid).await
    }
    async fn mark_transaction_proven_at(
        &self,
        txid: &str,
        height: Option<u64>,
    ) -> Result<(), StorageError> {
        self.note(format!("store.mark_proven_at {txid} {height:?}"))
            .await;
        self.inner.mark_transaction_proven_at(txid, height).await
    }
    async fn update_output_block_height(
        &self,
        txid: &str,
        vout: u32,
        topic: &str,
        height: u32,
    ) -> Result<(), StorageError> {
        self.note(format!("store.block_height {txid}.{vout} {topic} {height}"))
            .await;
        self.inner
            .update_output_block_height(txid, vout, topic, height)
            .await
    }
    async fn insert_applied_transaction(
        &self,
        tx: &AppliedTransaction,
    ) -> Result<(), StorageError> {
        self.note(format!("store.insert_applied {} {}", tx.txid, tx.topic))
            .await;
        self.inner.insert_applied_transaction(tx).await
    }
    async fn does_applied_transaction_exist(
        &self,
        tx: &AppliedTransaction,
    ) -> Result<bool, StorageError> {
        self.note(format!("store.applied? {} {}", tx.txid, tx.topic))
            .await;
        self.inner.does_applied_transaction_exist(tx).await
    }
    async fn delete_applied_transaction(
        &self,
        tx: &AppliedTransaction,
    ) -> Result<(), StorageError> {
        self.note(format!("store.delete_applied {} {}", tx.txid, tx.topic))
            .await;
        self.inner.delete_applied_transaction(tx).await
    }
    async fn find_output(
        &self,
        txid: &str,
        vout: u32,
        topic: Option<&str>,
        spent: Option<bool>,
        include_beef: bool,
    ) -> Result<Option<Output>, StorageError> {
        self.note(format!(
            "store.find_output {txid}.{vout} {topic:?} {spent:?} {include_beef}"
        ))
        .await;
        self.inner
            .find_output(txid, vout, topic, spent, include_beef)
            .await
    }
    async fn find_served_outputs(&self, list: &[Outpoint]) -> Result<Vec<Output>, StorageError> {
        self.note(format!("store.find_served [{}]", outpoints(list)))
            .await;
        self.inner.find_served_outputs(list).await
    }
    async fn find_outputs_for_transaction(
        &self,
        txid: &str,
        include_beef: bool,
    ) -> Result<Vec<Output>, StorageError> {
        self.note(format!("store.outputs_of {txid} {include_beef}"))
            .await;
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
        self.note(format!(
            "store.utxos {topic} {since:?} {limit:?} {include_beef}"
        ))
        .await;
        self.inner
            .find_utxos_for_topic(topic, since, limit, include_beef)
            .await
    }
    async fn find_transactions_for_proof_check(
        &self,
        limit: u64,
        min_age_secs: u64,
    ) -> Result<Vec<TransactionBeef>, StorageError> {
        self.note(format!("store.proof_check {limit} {min_age_secs}"))
            .await;
        self.inner
            .find_transactions_for_proof_check(limit, min_age_secs)
            .await
    }
    async fn update_last_interaction(
        &self,
        host: &str,
        topic: &str,
        since: u64,
    ) -> Result<(), StorageError> {
        self.note(format!("store.set_cursor {host} {topic} {since}"))
            .await;
        self.inner.update_last_interaction(host, topic, since).await
    }
    async fn get_last_interaction(&self, host: &str, topic: &str) -> Result<u64, StorageError> {
        self.note(format!("store.get_cursor {host} {topic}")).await;
        self.inner.get_last_interaction(host, topic).await
    }
    async fn record_peer_sync_outcome(
        &self,
        host: &str,
        topic: &str,
        success: bool,
    ) -> Result<(), StorageError> {
        self.note(format!("store.peer_outcome {host} {topic} {success}"))
            .await;
        self.inner
            .record_peer_sync_outcome(host, topic, success)
            .await
    }
    async fn get_peer_sync_health(
        &self,
        host: &str,
        topic: &str,
    ) -> Result<PeerSyncHealth, StorageError> {
        self.note(format!("store.peer_health {host} {topic}")).await;
        self.inner.get_peer_sync_health(host, topic).await
    }
    async fn record_peer_sync_yield(
        &self,
        host: &str,
        topic: &str,
        yielded: bool,
    ) -> Result<PeerYieldStreak, StorageError> {
        self.note(format!("store.peer_yield {host} {topic} {yielded}"))
            .await;
        self.inner
            .record_peer_sync_yield(host, topic, yielded)
            .await
    }
    async fn put_deferred_graph(
        &self,
        record: &bsv_overlay_engine::gasp::DeferredGraph,
    ) -> Result<bsv_overlay_engine::gasp::DeferredGraphSave, StorageError> {
        self.note("store.put_deferred".into()).await;
        self.inner.put_deferred_graph(record).await
    }
    async fn find_deferred_graphs(
        &self,
        host: &str,
        topic: &str,
    ) -> Result<Vec<bsv_overlay_engine::gasp::DeferredGraphKey>, StorageError> {
        self.note(format!("store.deferred_keys {host} {topic}"))
            .await;
        self.inner.find_deferred_graphs(host, topic).await
    }
    async fn get_deferred_graph(
        &self,
        host: &str,
        topic: &str,
        outpoint: &str,
    ) -> Result<Option<bsv_overlay_engine::gasp::DeferredGraph>, StorageError> {
        self.note(format!("store.get_deferred {host} {topic} {outpoint}"))
            .await;
        self.inner.get_deferred_graph(host, topic, outpoint).await
    }
    async fn delete_deferred_graph(
        &self,
        host: &str,
        topic: &str,
        outpoint: &str,
    ) -> Result<(), StorageError> {
        self.note(format!("store.delete_deferred {host} {topic} {outpoint}"))
            .await;
        self.inner
            .delete_deferred_graph(host, topic, outpoint)
            .await
    }
}

/// Drive `future` to its end on this thread, counting its polls: with a
/// storage that yields once per call, the count is the engine's awaits.
pub fn run_counting<F: Future>(future: F) -> (F::Output, u64) {
    let mut future = std::pin::pin!(future);
    let mut polls = 0;
    loop {
        polls += 1;
        if let Poll::Ready(value) = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            return (value, polls);
        }
        assert!(polls < 1_000_000, "the sync never finished");
    }
}

/// The recorded scenario: two ticks over one peer. Tick 1: a shared parent
/// whose two children are both listed (the parent is a duplicate body in the
/// second graph), two geneses, one of which blips (the cursor skip), and a
/// two-link chain. Tick 2: the blip healed. Run without a budget, and again
/// under a per-peer budget whose deadline never falls due (the finalize
/// hook's path). Returns the whole transcript (every storage call in order,
/// each awaited, and every peer call) and the polls of each tick.
/// `configure` is handed the engine, the served store and the transcript's
/// event stream (a test host can write its own lines into it).
pub fn recording(
    configure: &dyn Fn(&mut Engine, &Rc<MemoryStorage>, &Events),
) -> (Vec<String>, Vec<u64>) {
    let parent = genesis(1, 2);
    let left = child(&parent, 0, 1);
    let right = child(&parent, 1, 1);
    let lone = genesis(2, 1);
    let blip = genesis(3, 1);
    let low = genesis(4, 1);
    let high = child(&low, 0, 1);
    let txs = [&parent, &left, &right, &lone, &blip, &low, &high];
    let nodes: Vec<_> = txs
        .iter()
        .enumerate()
        .map(|(i, tx)| node(tx, honest_proof(&tx.id(), 100 + i as u32)))
        .collect();
    let listed = [
        (&left, 0, 1.0),
        (&right, 0, 2.0),
        (&lone, 0, 3.0),
        (&blip, 0, 4.0),
        (&high, 0, 5.0),
    ];
    let mut transcript = Vec::new();
    let mut polls = Vec::new();
    for budgeted in [false, true] {
        let events = Events::default();
        let store = Rc::new(MemoryStorage::new());
        let remote = ChaosRemote::new(&nodes, &listed, &events);
        remote.faults.borrow_mut().insert(blip.id());
        let mut engine = served_engine(
            Box::new(RecordingStore {
                inner: store.clone(),
                events: events.clone(),
            }),
            &remote,
            None,
            None,
        );
        if budgeted {
            engine.set_peer_sync_budget(never_due(), 30_000);
        }
        configure(&mut engine, &store, &events);
        for tick in 0..2 {
            if tick == 1 {
                remote.faults.borrow_mut().clear();
            }
            events
                .borrow_mut()
                .push(format!("tick {tick} budgeted={budgeted}"));
            let (result, count) = run_counting(engine.start_gasp_sync());
            let result = result.unwrap();
            let mut topics: Vec<_> = result.topics_synced.iter().collect();
            topics.sort_by_key(|(topic, _)| topic.as_str());
            events.borrow_mut().push(format!(
                "result {}",
                serde_json::to_string(&topics).unwrap()
            ));
            polls.push(count);
        }
        let rows = run_counting(served_rows(store.as_ref(), &txs)).0;
        events
            .borrow_mut()
            .extend(rows.into_iter().map(|(k, v)| format!("row {k} {v}")));
        transcript.extend(events.borrow().iter().cloned());
    }
    (transcript, polls)
}

pub fn digest(lines: &[String]) -> String {
    hex::encode(bsv_rs::primitives::hash::sha256(
        lines.join("\n").as_bytes(),
    ))
}

/// The `off` recording on A1's tip `9d5a75a` (run there, before the shadow).
pub const OFF_A1_DIGEST: &str = "431baab4e305582981b14370ed9ee36007a0a922e25341e9ca7c772192d4d112";
pub const OFF_A1_POLLS: [u64; 4] = [57, 11, 54, 11];
