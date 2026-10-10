//! bsv-low #586 (zanaadu-v2 #377): THE WITNESS. The memory of a GASP graph's
//! anchor check and finalize follows its transactions, not its references.
//!
//! Two UNMINED graphs of ~26 KB transactions (a head transaction's size),
//! their bottom spending coins the node already holds, so nothing of either
//! is mined. An unproven node asks for every input, so a transaction is a
//! node once per output index spent.
//!
//! - THE DIAMOND CHAIN, the shape Zanaadu measured (13 nodes, 659 KB fetched,
//!   24.1 MB native): 7 transactions, each spending BOTH outputs of the one
//!   before (a covenant output and that transaction's own change, a wallet's
//!   ordinary second spend). 13 nodes, 23 requests.
//! - THE LAYERED FAN-IN: 13 transactions, a root over three layers of four,
//!   every transaction spending one output of EVERY transaction of the layer
//!   below. 37 nodes, 85 requests.
//!
//! THE MEASURE, exact for a build: the peak growth of the LIVE HEAP (a
//! counting global allocator; this binary runs one test) inside
//! `validate_graph_anchor` and inside `finalize_graph`, each over the level
//! at its entry, through the real walk (`GASPSync::sync`) and the real
//! adapter (`OverlayGASPStorage`). It is stated against the bytes FETCHED
//! (every node the peer served, the repeats included); the finalize's also
//! net of the bytes it hands on (the per-node BEEFs are its output).
//!
//! On `fbb7fa8` `get_beef_for_node` rebuilt a shared ancestor once per
//! REFERENCE (a cloned subtree per parent, each `Transaction` holding its
//! parse, its raw bytes and its hex) and serialized it at every level: the
//! cost followed the graph's PATHS, doubling with every link of the diamond
//! chain.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use bsv_overlay_engine::gasp::{
    DeferredGraph, DeferredGraphKey, DeferredGraphSave, GASPError, GASPRemote, GASPStorage,
    GASPSync,
};
use bsv_overlay_engine::gasp_overlay::{new_finalized_graph_sink, OverlayGASPStorage};
use bsv_overlay_engine::storage::memory::MemoryStorage;
use bsv_overlay_engine::storage::Storage;
use bsv_overlay_engine::topic_manager::{TopicManager, TopicManagerError};
use bsv_overlay_engine::types::*;
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{
    MerklePath, MerklePathLeaf, Transaction, TransactionInput, TransactionOutput,
};

const TOPIC: &str = "tm_fan_in";

// ── The counting allocator ──────────────────────────────────────────────

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc(layout);
        if !p.is_null() {
            let live = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        p
    }

    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        System.dealloc(p, layout);
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// The live heap now, and the peak restarted from it.
fn mark() -> usize {
    let live = LIVE.load(Ordering::Relaxed);
    PEAK.store(live, Ordering::Relaxed);
    live
}

/// The peak growth since `mark`.
fn peak_over(mark: usize) -> usize {
    PEAK.load(Ordering::Relaxed).saturating_sub(mark)
}

// ── The graph ───────────────────────────────────────────────────────────

/// The data carried by every transaction: a head transaction's ~26 KB.
const PAYLOAD_BYTES: usize = 26_000;

fn op_1() -> LockingScript {
    LockingScript::from_hex("51").unwrap()
}

/// `OP_FALSE OP_RETURN <PAYLOAD_BYTES of salt>`.
fn payload(salt: u8) -> TransactionOutput {
    let len = PAYLOAD_BYTES as u16;
    let mut script = format!("006a4d{:02x}{:02x}", len & 0xff, len >> 8);
    script.push_str(&format!("{salt:02x}").repeat(PAYLOAD_BYTES));
    TransactionOutput::new(0, LockingScript::from_hex(&script).unwrap())
}

/// An input that really spends an `OP_1` output.
fn spending(txid: &str, output_index: usize) -> TransactionInput {
    let mut input = TransactionInput::new(txid.to_string(), output_index as u32);
    input.set_unlocking_script(UnlockingScript::from_hex("").unwrap());
    input
}

fn node_of(tx: &Transaction) -> GASPNode {
    GASPNode {
        graph_id: String::new(),
        raw_tx: tx.to_hex(),
        output_index: 0,
        proof: None,
        tx_metadata: None,
        output_metadata: None,
        inputs: None,
    }
}

struct Graph {
    /// The unproven transactions, the root last.
    txs: Vec<Transaction>,
    /// The mined funding transaction whose outputs the node already holds.
    funding: Transaction,
}

/// Layers of `widths` transactions, the root's layer (`1`) first. Each
/// transaction spends `each` outputs of EVERY transaction of the layer
/// below; the bottom layer spends `each` outputs of a mined funding
/// transaction.
fn layered(widths: &[usize], each: usize) -> Graph {
    assert_eq!(widths[0], 1);
    let bottom = *widths.last().unwrap();
    let mut funding = Transaction::new();
    // A coinbase's null outpoint: a transaction with no input is refused
    // since bsv-rs 0.4.1 (NL-8 W6), the stored funding's BEEF with it.
    funding
        .inputs
        .push(spending(&"00".repeat(32), u32::MAX as usize));
    for _ in 0..bottom * each {
        funding
            .outputs
            .push(TransactionOutput::new(1_000_000, op_1()));
    }
    let funding_txid = funding.id();
    funding.merkle_path = Some(
        MerklePath::new(
            100,
            vec![vec![MerklePathLeaf::new_txid(0, funding_txid.clone())]],
        )
        .unwrap(),
    );

    let mut txs = Vec::new();
    let mut salt = 0u8;
    // The layer below the one being built: its txids.
    let mut below: Vec<String> = vec![funding_txid];
    for layer in (0..widths.len()).rev() {
        // How many outputs the layer above spends of each transaction here.
        let spent = if layer == 0 {
            1
        } else {
            widths[layer - 1] * each
        };
        let mut this = Vec::new();
        for j in 0..widths[layer] {
            let mut tx = Transaction::new();
            for source in &below {
                for k in 0..each {
                    tx.inputs.push(spending(source, j * each + k));
                }
            }
            for _ in 0..spent {
                tx.outputs.push(TransactionOutput::new(1000, op_1()));
            }
            salt += 1;
            tx.outputs.push(payload(salt));
            this.push(tx.id());
            txs.push(tx);
        }
        below = this;
    }
    Graph { txs, funding }
}

// ── The peer, the manager, the measuring adapter ────────────────────────

struct Peer {
    nodes: std::collections::HashMap<String, GASPNode>,
    root: String,
    /// Raw bytes served, the repeats included.
    fetched: Rc<RefCell<(usize, usize)>>,
}

#[async_trait(?Send)]
impl GASPRemote for Peer {
    async fn get_initial_response(
        &self,
        request: &GASPInitialRequest,
    ) -> Result<GASPInitialResponse, GASPError> {
        Ok(GASPInitialResponse {
            utxo_list: vec![GASPOutput {
                txid: self.root.clone(),
                output_index: 0,
                score: 1.0,
            }],
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
        _metadata: bool,
    ) -> Result<GASPNode, GASPError> {
        let mut node = self
            .nodes
            .get(txid)
            .cloned()
            .ok_or_else(|| GASPError::NodeNotFound(txid.to_string()))?;
        node.graph_id = graph_id.to_string();
        node.output_index = output_index;
        let mut fetched = self.fetched.borrow_mut();
        fetched.0 += 1;
        fetched.1 += node.raw_tx.len() / 2;
        Ok(node)
    }

    async fn submit_node(&self, _node: &GASPNode) -> Result<Option<GASPNodeResponse>, GASPError> {
        panic!("pull-only sync must never submit to a remote")
    }
}

/// Admits every spendable output (all but the payload) and names nothing.
struct AdmitsSpendable;

#[async_trait(?Send)]
impl TopicManager for AdmitsSpendable {
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
            outputs_to_admit: (0..tx.outputs.len() as u32 - 1).collect(),
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

#[derive(Default)]
struct Measured {
    nodes: usize,
    graph_bytes: usize,
    anchor_peak: usize,
    finalize_peak: usize,
    anchor: Option<Result<(), String>>,
}

/// The real adapter, with the heap measured around its two graph calls.
struct Measuring<'a> {
    inner: OverlayGASPStorage<'a>,
    measured: Rc<RefCell<Measured>>,
}

#[async_trait(?Send)]
impl GASPStorage for Measuring<'_> {
    async fn find_known_utxos(
        &self,
        since: u64,
        limit: Option<u64>,
    ) -> Result<Vec<GASPOutput>, GASPError> {
        self.inner.find_known_utxos(since, limit).await
    }
    async fn hydrate_gasp_node(
        &self,
        graph_id: &str,
        txid: &str,
        output_index: u32,
        metadata: bool,
    ) -> Result<GASPNode, GASPError> {
        self.inner
            .hydrate_gasp_node(graph_id, txid, output_index, metadata)
            .await
    }
    async fn find_needed_inputs(
        &self,
        node: &GASPNode,
    ) -> Result<Option<GASPNodeResponse>, GASPError> {
        self.inner.find_needed_inputs(node).await
    }
    async fn append_to_graph(
        &self,
        node: &GASPNode,
        spent_by: Option<&str>,
    ) -> Result<(), GASPError> {
        {
            let mut m = self.measured.borrow_mut();
            m.nodes += 1;
            m.graph_bytes += node.raw_tx.len() / 2;
        }
        self.inner.append_to_graph(node, spent_by).await
    }
    async fn validate_graph_anchor(&self, graph_id: &str) -> Result<(), GASPError> {
        let entry = mark();
        let verdict = self.inner.validate_graph_anchor(graph_id).await;
        let mut m = self.measured.borrow_mut();
        m.anchor_peak = peak_over(entry);
        m.anchor = Some(match &verdict {
            Ok(()) => Ok(()),
            Err(e) => Err(e.to_string()),
        });
        verdict
    }
    async fn finalize_graph(&self, graph_id: &str) -> Result<(), GASPError> {
        let entry = mark();
        let done = self.inner.finalize_graph(graph_id).await;
        self.measured.borrow_mut().finalize_peak = peak_over(entry);
        done
    }
    async fn discard_graph(&self, graph_id: &str) -> Result<(), GASPError> {
        self.inner.discard_graph(graph_id).await
    }
    async fn load_deferred_graphs(&self) -> Result<Vec<DeferredGraphKey>, GASPError> {
        self.inner.load_deferred_graphs().await
    }
    async fn get_deferred_graph(&self, outpoint: &str) -> Result<Option<DeferredGraph>, GASPError> {
        self.inner.get_deferred_graph(outpoint).await
    }
    async fn save_deferred_graph(
        &self,
        record: &DeferredGraph,
    ) -> Result<DeferredGraphSave, GASPError> {
        self.inner.save_deferred_graph(record).await
    }
    async fn delete_deferred_graph(&self, outpoint: &str) -> Result<(), GASPError> {
        self.inner.delete_deferred_graph(outpoint).await
    }
}

/// What one sync of a graph measured.
struct Witness {
    txs: usize,
    distinct_bytes: usize,
    nodes: usize,
    calls: usize,
    fetched_bytes: usize,
    beefs: usize,
    output_bytes: usize,
    anchor_peak: usize,
    finalize_peak: usize,
    /// SHA-256 over the finalized BEEFs, sorted, each length-prefixed: what
    /// the finalize hands the engine, byte for byte.
    digest: String,
}

impl Witness {
    /// The anchor check's peak over the bytes fetched.
    fn anchor_x(&self) -> f64 {
        self.anchor_peak as f64 / self.fetched_bytes as f64
    }

    /// The finalize's peak, net of the BEEFs it hands on, over the bytes
    /// fetched.
    fn finalize_x(&self) -> f64 {
        self.finalize_peak.saturating_sub(self.output_bytes) as f64 / self.fetched_bytes as f64
    }

    fn print(&self, name: &str) {
        println!(
            "#586 WITNESS {name}: {} unmined transactions ({} raw bytes), {} nodes, {} requests \
             ({} bytes fetched); {} BEEFs, {} bytes handed on; live-heap peak growth: anchor \
             check {} bytes ({:.2}x fetched), finalize {} bytes ({:.2}x fetched net of the BEEFs \
             it hands on); digest {}",
            self.txs,
            self.distinct_bytes,
            self.nodes,
            self.calls,
            self.fetched_bytes,
            self.beefs,
            self.output_bytes,
            self.anchor_peak,
            self.anchor_x(),
            self.finalize_peak,
            self.finalize_x(),
            self.digest,
        );
    }
}

/// One sync of `graph`'s root through the real walk and the real adapter.
async fn witness(graph: &Graph) -> Witness {
    let distinct_bytes: usize = graph.txs.iter().map(|tx| tx.to_binary().len()).sum();
    let root = graph.txs.last().unwrap().id();

    // The node already holds the funding outputs the bottom layer spends.
    let store = MemoryStorage::new();
    let funding_beef = graph.funding.to_beef(true).unwrap();
    for (index, output) in graph.funding.outputs.iter().enumerate() {
        store
            .insert_output(&Output {
                txid: graph.funding.id(),
                output_index: index as u32,
                output_script: output.locking_script.to_binary(),
                satoshis: output.satoshis.unwrap_or_default(),
                topic: TOPIC.to_string(),
                spent: false,
                outputs_consumed: vec![],
                consumed_by: vec![],
                beef: Some(funding_beef.clone()),
                block_height: Some(100),
                score: Some(0.0),
            })
            .await
            .unwrap();
    }

    let fetched = Rc::new(RefCell::new((0usize, 0usize)));
    let peer = Peer {
        nodes: graph.txs.iter().map(|tx| (tx.id(), node_of(tx))).collect(),
        root,
        fetched: fetched.clone(),
    };
    let manager = AdmitsSpendable;
    let sink = new_finalized_graph_sink();
    let measured = Rc::new(RefCell::new(Measured::default()));
    let adapter = Measuring {
        inner: OverlayGASPStorage::new(&store, TOPIC, sink.clone()).with_topic_manager(&manager),
        measured: measured.clone(),
    };
    let mut sync = GASPSync::new(Box::new(adapter), Box::new(peer), 0, "[e586]", true);
    sync.sync(None).await.unwrap();

    let m = measured.borrow();
    assert_eq!(m.anchor, Some(Ok(())), "the anchor check admits the graph");
    assert_eq!(sync.discarded_graphs(), 0);
    let graphs = sink.lock().unwrap();
    assert_eq!(graphs.len(), 1);
    let beefs = &graphs[0].beefs;
    assert_eq!(beefs.len(), m.nodes, "one BEEF per node, as before");

    // The walk asks a node's inputs in the order of a `HashMap`
    // (`GASPNodeResponse::requested_inputs`), so the ORDER of sibling BEEFs
    // differs run to run, here and on `fbb7fa8`. What is fixed: the BEEFs
    // themselves (hashed sorted), and that a transaction's BEEF comes after
    // the BEEF of every transaction it spends.
    let index: std::collections::HashMap<String, usize> = graph
        .txs
        .iter()
        .enumerate()
        .map(|(i, tx)| (tx.id(), i))
        .collect();
    let mut finalized: std::collections::HashSet<usize> = std::collections::HashSet::new();
    for beef in beefs {
        let subject = Transaction::from_beef(beef, None).unwrap();
        for input in &subject.inputs {
            if let Some(&source) = index.get(&input.get_source_txid().unwrap()) {
                assert!(finalized.contains(&source), "a source is finalized first");
            }
        }
        finalized.insert(index[&subject.id()]);
    }
    assert_eq!(finalized.len(), graph.txs.len());
    let mut sorted: Vec<&Vec<u8>> = beefs.iter().collect();
    sorted.sort();
    let mut transcript = Vec::new();
    for beef in sorted {
        transcript.extend_from_slice(&(beef.len() as u64).to_le_bytes());
        transcript.extend_from_slice(beef);
    }
    let (calls, fetched_bytes) = *fetched.borrow();
    Witness {
        txs: graph.txs.len(),
        distinct_bytes,
        nodes: m.nodes,
        calls,
        fetched_bytes,
        beefs: beefs.len(),
        output_bytes: beefs.iter().map(Vec::len).sum(),
        anchor_peak: m.anchor_peak,
        finalize_peak: m.finalize_peak,
        digest: hex::encode(bsv_rs::primitives::hash::sha256(&transcript)),
    }
}

/// The finalized bytes of the two graphs, frozen on `fbb7fa8`. Re-frozen by
/// NL-6e (bsv-rs 0.4.2): the funding spends a coinbase's null outpoint, a
/// transaction with no input being refused since 0.4.1 (NL-8 W6), so every
/// txid moved; the same digests on main 1119225 (bsv-rs 0.4.0) and on the bump.
const E586_DIAMOND_BEEFS_DIGEST: &str =
    "2494189d8d5ad5ec71592810c356541cb3437395a443c834a4803b055c031f2e";
const E586_FAN_IN_BEEFS_DIGEST: &str =
    "ac343008a2de9ffa5254bf2326dc53eaa65418f36942d03118cafdd538b1207b";

/// The anchor check may grow the live heap by at most this many times the
/// bytes fetched; the finalize by this many, net of the BEEFs it hands on.
/// Measured on `fbb7fa8` (the test's failure there prints them): the diamond
/// chain 49.10x (29,505,006 bytes over 600,934 fetched; Zanaadu measured 36x
/// on its own 13 nodes) and 24.32x, the fan-in 11.65x and 5.70x.
const E586_ANCHOR_MAX_X: f64 = 4.0;
const E586_FINALIZE_MAX_X: f64 = 1.5;

#[tokio::test]
async fn e586_a_an_unmined_graph_is_assembled_from_its_raw_bytes_once() {
    // What ONE parsed transaction costs the heap (its parse and the raw
    // bytes the SDK keeps beside it), for the reader of the figures.
    let one = layered(&[1], 2);
    let one_raw = one.txs[0].to_binary();
    let entry = mark();
    let one_parsed = Transaction::from_binary(&one_raw).unwrap();
    let parsed_cost = LIVE.load(Ordering::Relaxed) - entry;
    drop(one_parsed);
    println!(
        "#586 WITNESS: one parsed transaction of {} raw bytes holds {parsed_cost} heap bytes",
        one_raw.len()
    );

    // Zanaadu's shape: 7 transactions, each spending both outputs of the
    // one before. 1 + 2 x 6 nodes; the root, its two inputs, then four
    // requests a link (two nodes, two inputs each).
    let diamond = witness(&layered(&[1; 7], 2)).await;
    diamond.print("diamond chain");
    assert_eq!((diamond.txs, diamond.nodes, diamond.calls), (7, 13, 23));

    // 1 + 4 + 16 + 16 nodes, and every input of every node asked.
    let fan_in = witness(&layered(&[1, 4, 4, 4], 1)).await;
    fan_in.print("layered fan-in");
    assert_eq!((fan_in.txs, fan_in.nodes, fan_in.calls), (13, 37, 85));

    assert_eq!(diamond.digest, E586_DIAMOND_BEEFS_DIGEST, "diamond bytes");
    assert_eq!(fan_in.digest, E586_FAN_IN_BEEFS_DIGEST, "fan-in bytes");
    for (name, w) in [("diamond chain", &diamond), ("layered fan-in", &fan_in)] {
        assert!(
            w.anchor_x() <= E586_ANCHOR_MAX_X,
            "{name}: the anchor check grew the heap by {} bytes, {:.2}x the {} fetched",
            w.anchor_peak,
            w.anchor_x(),
            w.fetched_bytes
        );
        assert!(
            w.finalize_x() <= E586_FINALIZE_MAX_X,
            "{name}: the finalize grew the heap by {} bytes beyond the {} it hands on, {:.2}x \
             the {} fetched",
            w.finalize_peak.saturating_sub(w.output_bytes),
            w.output_bytes,
            w.finalize_x(),
            w.fetched_bytes
        );
    }

    // A further unproven link no longer MULTIPLIES. Six more links of the
    // diamond chain (13 transactions, 25 nodes): on `fbb7fa8` each doubled
    // the hydrated tree (64 times the 7-link figure, gigabytes: not run
    // there, the assertions above fail first). What is left grows with the
    // BEEFs themselves, a node's BEEF holding its whole unproven ancestry.
    let deep = witness(&layered(&[1; 13], 2)).await;
    deep.print("diamond chain, 13 links");
    assert_eq!((deep.txs, deep.nodes, deep.calls), (13, 25, 47));
    assert!(
        deep.anchor_x() <= E586_ANCHOR_MAX_X && deep.finalize_x() <= E586_FINALIZE_MAX_X,
        "13 links: {:.2}x and {:.2}x",
        deep.anchor_x(),
        deep.finalize_x()
    );
}
