//! OverlayGASPStorage — bridges the Engine's storage to the GASP protocol.
//!
//! Wraps a reference to the Engine's `Storage` trait plus a topic name, providing
//! the `GASPStorage` interface that `GASPSync` needs to synchronize UTXOs with
//! remote peers.
//!
//! ## finalize_graph
//!
//! When a graph is finalized, the nodes are converted into ordered BEEF byte
//! arrays (ancestors first, root last) and stored in a shared
//! `FinalizedGraphSink`. The caller (Engine) drains the sink and submits each
//! BEEF to `Engine::submit()` with `HistoricalTxNoSpv` mode: after
//! `GASPSync::sync()` returns, or, under a per-peer sync budget, after every
//! completed UTXO (`gasp::FinalizedGraphHook`, bsv-low #552).
//!
//! ## find_needed_inputs
//!
//! Parses the node's raw transaction hex to determine what inputs are needed:
//! - A proven node ends the walk unless its topic manager names inputs needed
//!   for overlay history: when its output is not yet admissible, those not
//!   held; when it is admissible, those neither held nor landed (lane E1D).
//! - If no proof, all transaction inputs are requested (minus any already
//!   known in local storage).
//!
//! ## validate_graph_anchor (bsv-low #551)
//!
//! Before a graph is finalized it is checked the way the reference checks it
//! (`OverlayGASPStorage.ts` `validateGraphAnchor`): the ROOT node's BEEF is
//! verified by the engine's own SPV check, then the ordered BEEFs are replayed
//! through the topic manager over a set of coins, and the graph is discarded
//! WHOLE unless its root is a coin at the end. That is what makes the
//! `HistoricalTxNoSpv` submits of `finalize_graph` rest on a true premise.
//!
//! Ported from `~/bsv/overlay-services/src/GASP/OverlayGASPStorage.ts` (388 lines).

use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use async_trait::async_trait;
use bsv_rs::transaction::{Beef, ChainTracker, ChainTrackerError, MerklePath, Transaction};
use tracing::{debug, error, warn};

use crate::gasp::{GASPError, GASPStorage};
use crate::storage::Storage;
use crate::topic_manager::TopicManager;
use crate::types::{
    GASPInputRequest, GASPNode, GASPNodeResponse, GASPOutput, SubmitMode, TopicAdmittanceContext,
};

/// The most store reads [`OverlayGASPStorage::find_needed_inputs`] spends
/// on asking whether an admitted node's named inputs have LANDED (E1D's
/// re-ask; the E1D lens fold, L3): two per transaction at most (its applied
/// row, then its held outputs). A transaction past it is requested from the
/// peer. The figure is the door's own per-topic bound (`PREDECESSOR_READS`).
const LANDED_READS_PER_NODE: usize = 16;

/// A GASP node stored during in-progress graph construction.
#[derive(Debug, Clone)]
struct PendingNode {
    node: GASPNode,
    #[allow(dead_code)]
    spent_by: Option<String>,
    /// Children of this node (nodes whose transactions are inputs to this one).
    children: Vec<String>, // keys into the pending_graphs map
}

/// A finalized graph ready for Engine submission.
///
/// Contains ordered BEEF byte arrays (ancestors first, root last) and
/// the topic they belong to.
#[derive(Debug, Clone)]
pub struct FinalizedGraph {
    /// Topic this graph belongs to.
    pub topic: String,
    /// Ordered BEEF byte arrays — ancestors first, root transaction last.
    pub beefs: Vec<Vec<u8>>,
}

/// Shared container for finalized graph data.
///
/// The Engine creates this before sync and passes it to `OverlayGASPStorage`.
/// After sync completes, the Engine drains the list and submits each BEEF.
pub type FinalizedGraphSink = Rc<Mutex<Vec<FinalizedGraph>>>;

/// Create a new empty finalized graph sink.
pub fn new_finalized_graph_sink() -> FinalizedGraphSink {
    Rc::new(Mutex::new(Vec::new()))
}

/// `GASPStorage` implementation that delegates to the Engine's `Storage` trait.
///
/// Tracks one topic at a time — create a new instance per (topic, peer) sync.
/// During sync, incoming graph nodes are accumulated in memory. On `finalize_graph`,
/// the accumulated nodes are converted to BEEF format and pushed to the shared
/// `FinalizedGraphSink` for later Engine submission.
pub struct OverlayGASPStorage<'a> {
    /// The Engine's storage backend.
    storage: &'a dyn Storage,
    /// Topic being synchronized.
    topic: String,
    /// Optional topic manager for overlay-specific history behind proven nodes.
    topic_manager: Option<&'a dyn TopicManager>,
    /// Temporary graphs being constructed during sync.
    /// Key: node identifier (graph_id for root, "txid.outputIndex" for children).
    /// Value: the pending node with its relationship data.
    pending_graphs: Mutex<HashMap<String, PendingNode>>,
    /// Shared sink for completed graphs ready for Engine submission.
    finalized_sink: FinalizedGraphSink,
    /// If true, finalize uses strict `to_beef(false)` so a missing ancestor
    /// fails loud rather than silently emitting a partial BEEF. Defaults to
    /// `false` (tolerant `to_beef(true)`) — byte-identical to today. Only set
    /// `true` when ancestor hydration is enabled, so post-hydration
    /// completeness is enforced.
    strict_beef: bool,
    /// The engine's chain tracker, for the anchor's Bitcoin check. `None` is
    /// the reference's `'scripts only'`: a merkle path is accepted unchecked,
    /// every unproven input's script still runs.
    chain_tracker: Option<&'a dyn ChainTracker>,
    /// [`crate::engine::Engine::set_script_verification`], handed over so the
    /// anchor check obeys the same switch as `Engine::submit`. Default `true`.
    verify_scripts: bool,
}

/// Lends the engine's tracker to the anchor check and notes whether it
/// FAULTED: a tracker that could not answer is a fault of the moment, not a
/// verdict on the graph (see `validate_graph_anchor`). Noted here, on the
/// call, so no error text is matched.
struct FaultNotingTracker<'a> {
    inner: &'a dyn ChainTracker,
    faulted: AtomicBool,
}

#[async_trait]
impl ChainTracker for FaultNotingTracker<'_> {
    async fn is_valid_root_for_height(
        &self,
        root: &str,
        height: u32,
    ) -> Result<bool, ChainTrackerError> {
        let answer = self.inner.is_valid_root_for_height(root, height).await;
        if answer.is_err() {
            self.faulted.store(true, Ordering::Relaxed);
        }
        answer
    }

    async fn current_height(&self) -> Result<u32, ChainTrackerError> {
        self.inner.current_height().await
    }
}

/// What the anchor check needs of a pending graph, assembled under the lock.
struct AnchorInput {
    /// The root node's transaction id.
    root_txid: String,
    /// The root node's BEEF (tolerant: a source the graph does not carry is
    /// absent, and merged from local storage or refused by the verifier).
    root_beef: Vec<u8>,
    /// Inputs of the root's UNPROVEN ancestry that the graph does not carry.
    absent_sources: Vec<(String, u32)>,
    /// The graph's BEEFs, ancestors first, root last.
    ordered_beefs: Vec<Vec<u8>>,
}

impl<'a> OverlayGASPStorage<'a> {
    /// Create a new `OverlayGASPStorage` for the given topic.
    ///
    /// The `sink` parameter is a shared container where finalized graphs are
    /// pushed. The Engine holds a clone and drains it after sync.
    pub fn new(
        storage: &'a dyn Storage,
        topic: impl Into<String>,
        sink: FinalizedGraphSink,
    ) -> Self {
        Self {
            storage,
            topic: topic.into(),
            topic_manager: None,
            pending_graphs: Mutex::new(HashMap::new()),
            finalized_sink: sink,
            strict_beef: false,
            chain_tracker: None,
            verify_scripts: true,
        }
    }

    /// Check merkle roots of the anchor against this chain tracker (the
    /// engine's: `Engine::start_gasp_sync` wires it). Without one the anchor
    /// check is the reference's `'scripts only'` walk.
    #[must_use]
    pub fn with_chain_tracker(mut self, tracker: &'a dyn ChainTracker) -> Self {
        self.chain_tracker = Some(tracker);
        self
    }

    /// Carry [`crate::engine::Engine::set_script_verification`] into the
    /// anchor check. `false` is that switch's escape hatch, not a mode: the
    /// pre-2026-09-08 structural check (roots only, and nothing without a
    /// tracker). Default `true`.
    #[must_use]
    pub fn with_script_verification(mut self, enabled: bool) -> Self {
        self.verify_scripts = enabled;
        self
    }

    /// Consult the topic manager when a proven node needs overlay history.
    /// Without a manager, proven nodes retain the legacy stopping rule.
    #[must_use]
    pub fn with_topic_manager(mut self, manager: &'a dyn TopicManager) -> Self {
        self.topic_manager = Some(manager);
        self
    }

    /// Enable strict-BEEF finalize (`to_beef(false)`), failing loud on a
    /// missing ancestor instead of silently emitting a partial BEEF.
    ///
    /// Defaults to OFF (tolerant). Only enable this together with ancestor
    /// hydration so that, after the chain fallback has spliced in ancestors,
    /// an incomplete graph is discarded + retried rather than stored partial.
    #[must_use]
    pub fn with_strict_beef(mut self, strict: bool) -> Self {
        self.strict_beef = strict;
        self
    }

    /// Take all finalized graphs from the shared sink, leaving it empty.
    ///
    /// Convenience method for callers who hold a reference to this storage
    /// rather than the raw sink.
    pub fn take_finalized_graphs(&self) -> Vec<FinalizedGraph> {
        self.finalized_sink
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .drain(..)
            .collect()
    }

    /// Get the number of pending graphs (for testing).
    #[cfg(test)]
    pub fn pending_graph_count(&self) -> usize {
        let refs = self.pending_graphs.lock().unwrap();
        let graph_ids: std::collections::HashSet<&str> =
            refs.values().map(|pn| pn.node.graph_id.as_str()).collect();
        graph_ids.len()
    }

    /// Get the number of finalized graphs (for testing).
    #[cfg(test)]
    pub fn finalized_graph_count(&self) -> usize {
        self.finalized_sink.lock().unwrap().len()
    }

    /// Build a BEEF for a single graph node.
    ///
    /// Recursively walks the node's children (input providers) to hydrate
    /// source transactions or attach merkle proofs. Returns the node's
    /// transaction serialized as BEEF bytes.
    fn get_beef_for_node(
        node_key: &str,
        refs: &HashMap<String, PendingNode>,
        strict_beef: bool,
    ) -> Result<(Transaction, Vec<u8>), GASPError> {
        let pending = refs
            .get(node_key)
            .ok_or_else(|| GASPError::Other(format!("Node {node_key} not found in graph refs")))?;

        let mut tx = Transaction::from_hex(&pending.node.raw_tx)
            .map_err(|e| GASPError::Other(format!("Failed to parse raw_tx for {node_key}: {e}")))?;

        // If this node has a proof, attach the merkle path — this is a leaf.
        if let Some(ref proof_hex) = pending.node.proof {
            tx.merkle_path = Some(MerklePath::from_hex(proof_hex).map_err(|e| {
                GASPError::Other(format!("Failed to parse proof for {node_key}: {e}"))
            })?);
        } else {
            // No proof — hydrate each input's source transaction from children.
            // Collect child keys first to avoid borrow conflicts.
            let child_info: Vec<(usize, String)> = tx
                .inputs
                .iter()
                .enumerate()
                .filter_map(|(idx, input)| {
                    let source_txid = input.get_source_txid().unwrap_or_default();
                    if source_txid.is_empty() {
                        return None;
                    }
                    let child_key = format!("{}.{}", source_txid, input.source_output_index);
                    if refs.contains_key(&child_key) {
                        Some((idx, child_key))
                    } else {
                        None
                    }
                })
                .collect();

            for (input_idx, child_key) in child_info {
                let (child_tx, _) = Self::get_beef_for_node(&child_key, refs, strict_beef)?;
                tx.inputs[input_idx].source_transaction = Some(Box::new(child_tx));
            }
        }

        // `allow_partial`: in tolerant mode (default, byte-identical to today)
        // we pass `true` so missing-ancestor inputs are dropped silently. When
        // strict mode is enabled (only alongside ancestor hydration), pass
        // `false` so a still-missing ancestor fails loud and the graph is
        // discarded + retried rather than stored as a partial BEEF.
        let allow_partial = !strict_beef;
        let beef = tx.to_beef(allow_partial).map_err(|e| {
            GASPError::Other(format!("Failed to serialize BEEF for {node_key}: {e}"))
        })?;

        Ok((tx, beef))
    }

    /// Compute ordered BEEFs for a graph (ancestors first, root last).
    ///
    /// Walks the graph depth-first from the root, collecting BEEFs from
    /// leaf nodes (with proofs) up to the root.
    fn compute_ordered_beefs(
        graph_id: &str,
        refs: &HashMap<String, PendingNode>,
        strict_beef: bool,
    ) -> Result<Vec<Vec<u8>>, GASPError> {
        let mut beefs: Vec<Vec<u8>> = Vec::new();
        let mut visited: std::collections::HashSet<String> = std::collections::HashSet::new();

        fn hydrate(
            node_key: &str,
            refs: &HashMap<String, PendingNode>,
            beefs: &mut Vec<Vec<u8>>,
            visited: &mut std::collections::HashSet<String>,
            strict_beef: bool,
        ) -> Result<(), GASPError> {
            if visited.contains(node_key) {
                return Ok(());
            }
            visited.insert(node_key.to_string());

            let Some(pending) = refs.get(node_key) else {
                return Ok(());
            };

            // First, recurse into children (they go before us in order)
            for child_key in &pending.children {
                hydrate(child_key, refs, beefs, visited, strict_beef)?;
            }

            // Then add our own BEEF
            let (_, beef) = OverlayGASPStorage::get_beef_for_node(node_key, refs, strict_beef)?;
            beefs.push(beef);
            Ok(())
        }

        hydrate(graph_id, refs, &mut beefs, &mut visited, strict_beef)?;
        Ok(beefs)
    }

    /// Everything `validate_graph_anchor` needs of the pending graph, read in
    /// one pass under the lock (nothing awaits while the guard is held).
    ///
    /// It also enforces the one structural rule the rest relies on: every
    /// node hangs off its parent by an input the parent REALLY spends. The
    /// walk asks the peer for an outpoint and files the answer under the
    /// answer's own txid, so a peer that answers with some other transaction
    /// would otherwise ride into the ordered BEEFs unlinked to anything the
    /// Bitcoin check covers. With the rule, every node of the graph is tied
    /// to the verified root by a chain of txids. (A divergence by addition
    /// from the reference at f999e0c1a, which has no such rule; upstream added
    /// it later in `appendToGraph`: "The GASP child node is not an input of
    /// its declared parent".)
    ///
    /// A graph that cannot be ASSEMBLED (a raw transaction or a proof that
    /// does not parse, or under `strict_beef` a missing ancestor) is
    /// `GASPError::AnchorUnavailable`, not a refusal: before #551 that error
    /// surfaced from `finalize_graph` and failed the UTXO, so the cursor gap
    /// guard asked for it again (#43), and it still does.
    fn read_anchor_input(
        graph_id: &str,
        refs: &HashMap<String, PendingNode>,
        strict_beef: bool,
    ) -> Result<AnchorInput, GASPError> {
        let unassembled = |e: GASPError| {
            GASPError::AnchorUnavailable(format!("graph {graph_id} cannot be assembled: {e}"))
        };
        if !refs.contains_key(graph_id) {
            return Err(GASPError::ValidationFailed(format!(
                "Graph node with ID {graph_id} not found"
            )));
        }

        let mut absent_sources: Vec<(String, u32)> = Vec::new();
        let mut visited: HashSet<&str> = HashSet::new();
        // (node key, whether a path of UNPROVEN nodes leads from the root to it)
        let mut stack: Vec<(&str, bool)> = vec![(graph_id, true)];
        while let Some((key, in_root_beef)) = stack.pop() {
            if !visited.insert(key) {
                continue;
            }
            let Some(pending) = refs.get(key) else {
                continue;
            };
            let tx = Transaction::from_hex(&pending.node.raw_tx).map_err(|e| {
                unassembled(GASPError::Other(format!(
                    "Failed to parse raw_tx for {key}: {e}"
                )))
            })?;
            let spends: HashSet<String> = tx
                .inputs
                .iter()
                .filter_map(|input| {
                    let source_txid = input.get_source_txid().ok()?;
                    Some(format!("{}.{}", source_txid, input.source_output_index))
                })
                .collect();
            for child in &pending.children {
                if !spends.contains(child) {
                    return Err(GASPError::ValidationFailed(format!(
                        "node {child} is not an input of its declared parent {key}"
                    )));
                }
            }
            let unproven = pending.node.proof.is_none();
            if in_root_beef && unproven {
                for outpoint in &spends {
                    if !refs.contains_key(outpoint) {
                        if let Some(source) = crate::gasp::parse_outpoint(outpoint) {
                            absent_sources.push(source);
                        }
                    }
                }
            }
            for child in &pending.children {
                stack.push((child.as_str(), in_root_beef && unproven));
            }
        }

        // The root's CHECKED copy is assembled tolerant whatever `strict_beef`
        // says: it is never stored, sources the storage holds are merged into
        // it, and the verifier refuses any source still missing. The ordered
        // BEEFs are assembled exactly as `finalize_graph` will assemble them.
        let (root_tx, root_beef) =
            Self::get_beef_for_node(graph_id, refs, false).map_err(unassembled)?;
        let ordered_beefs =
            Self::compute_ordered_beefs(graph_id, refs, strict_beef).map_err(unassembled)?;
        Ok(AnchorInput {
            root_txid: root_tx.id(),
            root_beef,
            absent_sources,
            ordered_beefs,
        })
    }
}

#[async_trait(?Send)]
impl GASPStorage for OverlayGASPStorage<'_> {
    /// Returns UTXOs known for this topic since the given score/timestamp.
    ///
    /// Delegates to `Storage::find_utxos_for_topic()`, converting `Output`
    /// records to `GASPOutput` with txid, output_index, and score.
    async fn find_known_utxos(
        &self,
        since: u64,
        limit: Option<u64>,
    ) -> Result<Vec<GASPOutput>, GASPError> {
        let outputs = self
            .storage
            .find_utxos_for_topic(
                &self.topic,
                Some(since as f64),
                limit,
                false, // don't need BEEF for listing
            )
            .await
            .map_err(|e| GASPError::StorageError(e.to_string()))?;

        Ok(outputs
            .iter()
            .map(|o| GASPOutput {
                txid: o.txid.clone(),
                output_index: o.output_index,
                score: o.score.unwrap_or(0.0),
            })
            .collect())
    }

    /// Hydrate a GASP node with transaction data from storage.
    ///
    /// Looks up the root output (from graph_id) and searches its BEEF tree
    /// for the requested txid. This is a simplified version that returns
    /// the raw transaction data and proof if available.
    ///
    /// For a full implementation, this would delegate to
    /// `Engine::provide_foreign_gasp_node()`, but since we only hold a
    /// `Storage` reference (not the full Engine), we do a simpler lookup.
    async fn hydrate_gasp_node(
        &self,
        graph_id: &str,
        txid: &str,
        output_index: u32,
        _metadata: bool,
    ) -> Result<GASPNode, GASPError> {
        // Try to find the output with BEEF data
        let output = self
            .storage
            .find_output(txid, output_index, Some(&self.topic), None, true)
            .await
            .map_err(|e| GASPError::StorageError(e.to_string()))?;

        if let Some(output) = output {
            if let Some(ref beef) = output.beef {
                // Parse the BEEF to extract raw transaction hex
                match bsv_rs::transaction::Transaction::from_beef(beef, None) {
                    Ok(tx) => {
                        let mut node = GASPNode {
                            graph_id: graph_id.to_string(),
                            raw_tx: tx.to_hex(),
                            output_index,
                            proof: None,
                            tx_metadata: None,
                            output_metadata: None,
                            inputs: None,
                        };
                        if let Some(ref merkle_path) = tx.merkle_path {
                            node.proof = Some(merkle_path.to_hex());
                        }
                        return Ok(node);
                    }
                    Err(e) => {
                        warn!("Failed to parse BEEF for {txid}: {e}");
                    }
                }
            }
        }

        // Fallback: look up without topic filter
        let output = self
            .storage
            .find_output(txid, output_index, None, None, true)
            .await
            .map_err(|e| GASPError::StorageError(e.to_string()))?
            .ok_or_else(|| {
                GASPError::NodeNotFound(format!("Output {txid}.{output_index} not found"))
            })?;

        let beef = output.beef.as_ref().ok_or_else(|| {
            GASPError::NodeNotFound(format!("No BEEF data for {txid}.{output_index}"))
        })?;

        let tx = bsv_rs::transaction::Transaction::from_beef(beef, None)
            .map_err(|e| GASPError::Other(format!("BEEF parse error: {e}")))?;

        let mut node = GASPNode {
            graph_id: graph_id.to_string(),
            raw_tx: tx.to_hex(),
            output_index,
            proof: None,
            tx_metadata: None,
            output_metadata: None,
            inputs: None,
        };
        if let Some(ref merkle_path) = tx.merkle_path {
            node.proof = Some(merkle_path.to_hex());
        }
        Ok(node)
    }

    /// Determine which inputs are needed to validate this node.
    ///
    /// - A proven node ends the walk if its topic manager names no inputs it
    ///   still needs: for a node whose output is NOT admissible, every named
    ///   input not held (D13); for one whose output IS admissible, every named
    ///   input neither held nor landed (lane E1D, an addition: the reference
    ///   stops at an admissible node). Without a manager, proven nodes always
    ///   stop.
    /// - If no proof, parses the raw transaction and requests all inputs,
    ///   filtering out any inputs already known in local storage.
    /// - Admission errors propagate to GASP's incoming UTXO handler. Errors
    ///   identifying needed inputs are logged and cut off this node, like TS.
    /// - Unproven raw_tx parse failures retain the legacy graceful fallback.
    async fn find_needed_inputs(
        &self,
        node: &GASPNode,
    ) -> Result<Option<GASPNodeResponse>, GASPError> {
        let mut requested_inputs: HashMap<String, GASPInputRequest> = HashMap::new();
        // A proven node whose own output the dry run admits (lane E1D).
        let mut admitted = false;

        if let Some(proof_hex) = &node.proof {
            let Some(manager) = self.topic_manager else {
                return Ok(None);
            };
            let mut tx = Transaction::from_hex(&node.raw_tx)
                .map_err(|e| GASPError::Other(format!("Failed to parse raw_tx: {e}")))?;
            tx.merkle_path = Some(
                MerklePath::from_hex(proof_hex)
                    .map_err(|e| GASPError::Other(format!("Failed to parse proof: {e}")))?,
            );

            // TS passes { dryRun: true }: the node is the peer's and nothing
            // is admitted here, so a manager that writes on admission must not.
            // Utils.toArray(string) defaults to UTF-8, even for hex-like text.
            // This call is outside the needed-input catch in the reference.
            let admittance = manager
                .identify_admissible_outputs(
                    &tx,
                    &[],
                    node.tx_metadata.as_deref().map(str::as_bytes),
                    SubmitMode::HistoricalTx,
                    &TopicAdmittanceContext::DRY_RUN,
                )
                .await
                .map_err(|e| GASPError::Other(e.to_string()))?;
            // The reference stops here (D13). Lane E1D (bsv-low #575,
            // zanaadu-v2 #365) asks the named inputs of an ADMITTED node too:
            // a manager that admits a successor with no coin (the pf head
            // manager over its own head state) ended the walk at the
            // successor while its predecessor had not landed, the graph was
            // the successor alone, and its finalize submit recorded it with
            // no coin; the predecessor, landed on a later tick, stood unspent
            // beside the tip for good. So the predecessor is asked FIRST: a
            // named input that is neither held nor landed (below) is
            // requested and lands before the node in the graph's order, a
            // finalize submit of it that does not land stops the graph, and
            // a named input the peer cannot serve prunes (D14). When every
            // named input is known the walk stops here, as the reference's.
            admitted = admittance.outputs_to_admit.contains(&node.output_index);
            // A node with no input to name stops here, the manager not
            // asked (the reference's call count for such a node).
            if admitted
                && !tx
                    .inputs
                    .iter()
                    .any(|input| input.get_source_txid().is_ok_and(|s| !s.is_empty()))
            {
                return Ok(None);
            }

            // A serialization RED is unreachable: bsv-rs stops collecting ancestors at a merkle path.
            let needed_inputs = async {
                let beef = tx
                    .to_beef(false)
                    .map_err(|e| format!("Failed to serialize proven BEEF: {e}"))?;
                manager
                    .identify_needed_inputs(&beef, None)
                    .await
                    .map_err(|e| e.to_string())
            }
            .await;
            match needed_inputs {
                Ok(inputs) => {
                    for input in inputs {
                        requested_inputs
                            .insert(input.to_graph_id(), GASPInputRequest { metadata: false });
                    }
                }
                Err(e) => {
                    error!(
                        "An error occurred when identifying needed inputs for transaction: {}.{}: {e}",
                        tx.id(), node.output_index
                    );
                    return Ok(None);
                }
            }
        } else {
            // Unproven nodes always need all inputs, independently of the manager.
            let tx = match Transaction::from_hex(&node.raw_tx) {
                Ok(tx) => tx,
                Err(e) => {
                    warn!(
                        "Cannot parse raw_tx for find_needed_inputs (graph_id={}): {e}",
                        node.graph_id
                    );
                    return Ok(None);
                }
            };
            for input in &tx.inputs {
                let source_txid = input.get_source_txid().unwrap_or_default();
                if source_txid.is_empty() {
                    continue;
                }
                let outpoint = format!("{}.{}", source_txid, input.source_output_index);
                requested_inputs.insert(outpoint, GASPInputRequest { metadata: false });
            }
        }

        if requested_inputs.is_empty() {
            return Ok(None);
        }

        // Strip inputs that are already known in local storage.
        let mut to_remove = Vec::new();
        for outpoint in requested_inputs.keys() {
            if let Some((txid, oi)) = crate::gasp::parse_outpoint(outpoint) {
                match self
                    .storage
                    .find_output(&txid, oi, Some(&self.topic), None, false)
                    .await
                {
                    Ok(Some(_)) => {
                        to_remove.push(outpoint.clone());
                    }
                    Ok(None) => {}
                    Err(e) => {
                        debug!("Storage lookup failed for {outpoint}: {e}");
                    }
                }
            }
        }
        for key in &to_remove {
            requested_inputs.remove(key);
        }

        // An admitted node's named inputs are requested only while their
        // landing is unknown (E1D): one whose transaction has an applied row
        // in the topic, or an output held there, has landed, and the walk
        // stops as the reference's. A read that faults keeps the input: one
        // round trip to the peer is the cheaper error. So does a transaction
        // past [`LANDED_READS_PER_NODE`] (the E1D lens fold, L3: a manager
        // names as many inputs as it likes, D14's decoys included, and each
        // costs up to two reads): it is requested, and a landed one the peer
        // serves is a dupe at its finalize submit.
        if admitted {
            let mut landed: HashSet<String> = HashSet::new();
            let mut unlanded: HashSet<String> = HashSet::new();
            let mut reads = 0usize;
            let mut outpoints: Vec<&String> = requested_inputs.keys().collect();
            outpoints.sort();
            for outpoint in outpoints {
                let Some((txid, _)) = crate::gasp::parse_outpoint(outpoint) else {
                    continue;
                };
                if landed.contains(&txid) || unlanded.contains(&txid) {
                    continue;
                }
                if reads + 2 > LANDED_READS_PER_NODE {
                    break;
                }
                reads += 1;
                let record = crate::types::AppliedTransaction {
                    txid: txid.clone(),
                    topic: self.topic.clone(),
                };
                let mut holds = matches!(
                    self.storage.does_applied_transaction_exist(&record).await,
                    Ok(true)
                );
                if !holds {
                    reads += 1;
                    holds = matches!(
                        self.storage.find_outputs_for_transaction(&txid, false).await,
                        Ok(outputs) if outputs.iter().any(|o| o.topic == self.topic)
                    );
                }
                if holds {
                    landed.insert(txid);
                } else {
                    unlanded.insert(txid);
                }
            }
            requested_inputs.retain(|outpoint, _| {
                crate::gasp::parse_outpoint(outpoint)
                    .is_none_or(|(txid, _)| !landed.contains(&txid))
            });
        }

        if requested_inputs.is_empty() {
            return Ok(None);
        }

        Ok(Some(GASPNodeResponse { requested_inputs }))
    }

    /// Append a node to a temporary graph being constructed during sync.
    ///
    /// If `spent_by` is `None`, this is the root node and is keyed by `graph_id`.
    /// Otherwise, the node is keyed by its computed txid.outputIndex, and is
    /// registered as a child of the parent node identified by `spent_by`.
    async fn append_to_graph(
        &self,
        node: &GASPNode,
        spent_by: Option<&str>,
    ) -> Result<(), GASPError> {
        debug!(
            "Appending node to graph {}: tx={}..., oi={}, spent_by={:?}",
            node.graph_id,
            &node.raw_tx[..node.raw_tx.len().min(16)],
            node.output_index,
            spent_by,
        );

        // Compute the key for this node.
        let node_key = if spent_by.is_none() {
            // Root node — keyed by graph_id
            node.graph_id.clone()
        } else {
            // Child node — keyed by txid.outputIndex
            match Transaction::from_hex(&node.raw_tx) {
                Ok(tx) => format!("{}.{}", tx.id(), node.output_index),
                Err(_) => {
                    // Fallback: use raw_tx prefix as key
                    format!(
                        "{}.{}",
                        &node.raw_tx[..node.raw_tx.len().min(64)],
                        node.output_index
                    )
                }
            }
        };

        let mut refs = self
            .pending_graphs
            .lock()
            .map_err(|e| GASPError::Other(format!("Lock poisoned: {e}")))?;

        // Insert this node
        refs.insert(
            node_key.clone(),
            PendingNode {
                node: node.clone(),
                spent_by: spent_by.map(std::string::ToString::to_string),
                children: Vec::new(),
            },
        );

        // If spent_by is set, register this node as a child of the parent.
        if let Some(parent_key) = spent_by {
            if let Some(parent) = refs.get_mut(parent_key) {
                parent.children.push(node_key);
            }
        }

        Ok(())
    }

    /// Check a completed graph before anything of it is finalized (bsv-low
    /// #551). The reference's `validateGraphAnchor`
    /// (`OverlayGASPStorage.ts` at f999e0c1a, lines 261 to 297), in its two
    /// steps. An `Err` is a discard: `GASPSync::complete_graph` drops the
    /// graph and NOTHING of it reaches the sink.
    ///
    /// **1. Bitcoin.** The ROOT node's BEEF (`get_beef_for_node`) is verified
    /// by the engine's own check, `verify_spv_like_the_reference`: the
    /// reference's `spvTx.verify(this.engine.chainTracker)`. With a tracker
    /// every merkle root in that BEEF is checked against it and every unproven
    /// input's script is executed; with none it is `'scripts only'` (roots
    /// accepted unchecked, scripts still run). One verifier, the same switch
    /// (`with_script_verification`) as `Engine::submit`.
    ///
    /// **2. Overlay, all or nothing.** The ordered BEEFs (ancestors first, root
    /// last) are replayed through the topic manager as the reference's
    /// `admitHistoricalBEEF` does: mode `historical-tx`, no off-chain values,
    /// `previous_coins` the inputs whose source outpoint is a coin (the
    /// engine's little-endian u32 input indices), and every output the manager
    /// admits joins the coins. If the ROOT's outpoint is not a coin at the
    /// end, the graph is refused. The set only GROWS, as in the reference at
    /// f999e0c1a: it reads `outputsToAdmit` alone, never `coinsToRetain`, and
    /// removes nothing ("a Set of all historical coins to retain (no need to
    /// remove them)"). The reference passes `{ dryRun: true }` and so does
    /// this replay (`TopicAdmittanceContext::DRY_RUN`): a manager that writes
    /// on admission writes nothing here.
    ///
    /// WITHOUT a topic manager step 2 does not run: there is nothing to
    /// replay, and `Engine::submit` refuses such a topic (`UnsupportedTopic`),
    /// so nothing can be admitted. `Engine::start_gasp_sync` always wires the
    /// manager of a topic it can admit.
    ///
    /// **Divergences by addition** (each with its why):
    ///
    /// - *Coins the storage already holds count.* `find_needed_inputs` strips
    ///   an input the local storage already holds (as the reference does), so
    ///   its node is not in the graph and the replay's own set never sees it.
    ///   The reference then refuses a root that extends a coin it already
    ///   holds: the next head of a head chain, synced one tick after the
    ///   last, would be discarded forever. A previous coin is therefore an
    ///   input in the set OR in the storage for this topic, the exact lookup
    ///   `Engine::submit` makes when finalize submits the same BEEF.
    /// - *Sources the storage already holds are merged for step 1.* For the
    ///   same reason an UNPROVEN node may lack an input's source transaction
    ///   (the reference throws in `getBEEFForNode` and discards). The stored
    ///   BEEF of that output is merged into the checked copy, so the spend is
    ///   executed against its real source and that source's own ancestry is
    ///   verified too. If the stored BEEF is itself incomplete the verifier
    ///   refuses, and the graph is discarded as in the reference. Only the
    ///   CHECKED copy is merged; the finalized bytes are untouched.
    /// - *Every node is an input of its parent* (`read_anchor_input`).
    /// - *A conflicting spend refuses the graph.* Two transactions of one
    ///   graph that spend the same outpoint cannot both be admitted by the
    ///   finalize submits (the first one consumes the coin), so a replay that
    ///   showed the coin to both would promise an admission finalize cannot
    ///   keep. Upstream added the same rule after f999e0c1a (`spentOutpoints`,
    ///   "Historical GASP graph contains a conflicting spend").
    /// - *A fault of the moment is not a verdict.* A chain tracker that could
    ///   not answer, or a storage read that failed, says nothing about the
    ///   graph: the error is `GASPError::AnchorUnavailable`, the graph is
    ///   still discarded, but `complete_graph` FAILS the UTXO so the cursor
    ///   gap guard asks for it again. The reference discards and moves on,
    ///   which here would lose the UTXO for good behind an advanced cursor.
    /// - *A manager error in the replay is "not now", not a refusal.* The
    ///   replay is where a manager is shown the coins, so it is where one
    ///   whose own state lags can say it cannot place a transaction YET. An
    ///   `Err` from it is `GASPError::AnchorUnavailable` too: the UTXO fails
    ///   and is asked for again. `Ok` with nothing admitted stays the final
    ///   refusal, and there the cursor moves, as in the reference (which
    ///   moves it past a refused and a failed graph alike, `GASP.ts` 389 to
    ///   392, 419 to 423, 559 to 572, `Engine.ts` 1600 to 1604). The cost: a
    ///   manager that errors FOREVER on one UTXO holds that peer's cursor
    ///   below it, and what lies above and is not yet held is walked again
    ///   on every tick.
    ///
    /// **Cost.** Step 1 runs the script of every input of every unproven
    /// transaction in the root's BEEF and asks the tracker once per merkle
    /// path in it (a proven root: one question, no script). The ancestors a
    /// manager named BEHIND a proven node are not in that BEEF: their own
    /// merkle paths are not checked, here or in the reference; they are bound
    /// to the verified root by txid instead. Step 2 is one manager call per
    /// transaction and one storage read per input that is not already a coin
    /// of the set, which is what the finalize submits cost again. The walk is
    /// the unbudgeted reference walk, the one `Engine::submit` runs on a
    /// client's BEEF: `DoorBudget` is NOT applied. That budget belongs to the
    /// `broadcast-gated` door, whose breach means "inconclusive, the network
    /// judges"; there is no stronger bar behind this check, so a breach could
    /// only be a refusal, and a refusal here is final for that UTXO. What
    /// bounds a peer's graph is what bounded it before: the per-peer sync
    /// budget (which cannot interrupt a script already running). The
    /// reference serialises this method behind `acquireAnchorValidationSlot`
    /// (at most 4 at once); here graphs are completed one at a time by one
    /// sync on one thread, so there is nothing to serialise.
    async fn validate_graph_anchor(&self, graph_id: &str) -> Result<(), GASPError> {
        let anchor = {
            let refs = self
                .pending_graphs
                .lock()
                .map_err(|e| GASPError::Other(format!("Lock poisoned: {e}")))?;
            Self::read_anchor_input(graph_id, &refs, self.strict_beef)?
        };
        let unavailable =
            |what: String| GASPError::AnchorUnavailable(format!("graph {graph_id}: {what}"));
        let refused = |why: String| GASPError::ValidationFailed(format!("graph {graph_id}: {why}"));

        // ── 1. Bitcoin ──────────────────────────────────────────────────
        let mut root_beef = anchor.root_beef;
        if !anchor.absent_sources.is_empty() {
            let mut merged = Beef::from_binary(&root_beef)
                .map_err(|e| refused(format!("root BEEF does not parse: {e}")))?;
            for (txid, output_index) in &anchor.absent_sources {
                let held = self
                    .storage
                    .find_output(txid, *output_index, Some(&self.topic), None, true)
                    .await
                    .map_err(|e| unavailable(format!("reading {txid}.{output_index}: {e}")))?;
                if let Some(stored) = held.and_then(|output| output.beef) {
                    if let Ok(stored) = Beef::from_binary(&stored) {
                        merged.merge_beef(&stored);
                    }
                }
            }
            root_beef = merged.to_binary();
        }
        let tracker = self.chain_tracker.map(|inner| FaultNotingTracker {
            inner,
            faulted: AtomicBool::new(false),
        });
        let verdict = crate::engine::verify_spv_like_the_reference(
            tracker.as_ref().map(|t| t as &dyn ChainTracker),
            self.verify_scripts,
            &root_beef,
            &anchor.root_txid,
        )
        .await;
        if let Err(e) = verdict {
            if tracker
                .as_ref()
                .is_some_and(|t| t.faulted.load(Ordering::Relaxed))
            {
                return Err(unavailable(format!("the chain tracker faulted: {e}")));
            }
            return Err(refused(format!(
                "The graph is not well-anchored according to the rules of Bitcoin: {e}"
            )));
        }

        // ── 2. Overlay ──────────────────────────────────────────────────
        let Some(manager) = self.topic_manager else {
            return Ok(());
        };
        let mut coins: HashSet<String> = HashSet::new();
        let mut spent: HashSet<String> = HashSet::new();
        let mut replayed: HashSet<String> = HashSet::new();
        for beef in &anchor.ordered_beefs {
            // The parse `Engine::submit` makes of the same bytes at finalize.
            let subject = Beef::from_binary(beef)
                .map(|mut b| crate::subject::subject_txid_of(&mut b))
                .map_err(|e| refused(format!("a graph BEEF does not parse: {e}")))?;
            let tx = Transaction::from_beef(beef, subject.as_deref())
                .map_err(|e| refused(format!("a graph BEEF does not parse: {e}")))?;
            let txid = tx.id();
            // Two outputs of one transaction are two nodes with one BEEF.
            if !replayed.insert(txid.clone()) {
                continue;
            }
            let mut previous_coins: Vec<u8> = Vec::new();
            for (input_index, input) in tx.inputs.iter().enumerate() {
                let source_txid = input.get_source_txid().unwrap_or_default();
                if source_txid.is_empty() {
                    continue;
                }
                let outpoint = format!("{}.{}", source_txid, input.source_output_index);
                if !spent.insert(outpoint.clone()) {
                    return Err(refused(format!(
                        "the graph contains a conflicting spend of {outpoint}"
                    )));
                }
                let is_coin = coins.contains(&outpoint)
                    || self
                        .storage
                        .find_output(
                            &source_txid,
                            input.source_output_index,
                            Some(&self.topic),
                            None,
                            false,
                        )
                        .await
                        .map_err(|e| unavailable(format!("reading {outpoint}: {e}")))?
                        .is_some();
                if is_coin {
                    previous_coins.extend_from_slice(&(input_index as u32).to_le_bytes());
                }
            }
            // TS passes { dryRun: true }: the replay only asks, the finalize
            // submit is the admission.
            // An `Err` is the manager's "not now" (a fault of the moment, see
            // the divergences above); `Ok` with nothing is its refusal.
            let admittance = manager
                .identify_admissible_outputs(
                    &tx,
                    &previous_coins,
                    None,
                    SubmitMode::HistoricalTx,
                    &TopicAdmittanceContext::DRY_RUN,
                )
                .await
                .map_err(|e| unavailable(format!("the topic manager failed on {txid}: {e}")))?;
            for output_index in admittance.outputs_to_admit {
                coins.insert(format!("{txid}.{output_index}"));
            }
        }
        if !coins.contains(graph_id) {
            return Err(refused(
                "This graph did not result in topical admittance of the root node. Rejecting."
                    .to_string(),
            ));
        }
        Ok(())
    }

    /// Finalize a graph — convert accumulated nodes to ordered BEEFs and push
    /// them to the shared sink for later Engine submission.
    ///
    /// Computes ordered BEEF byte arrays (ancestors first, root last) from the
    /// temporary graph nodes and stores them in the `FinalizedGraphSink`. The
    /// Engine drains it (after the sync, or per completed UTXO under a
    /// per-peer budget) and submits each one with `HistoricalTxNoSpv` mode, which skips SPV because
    /// `validate_graph_anchor` has already done it (the reference's
    /// `finalizeGraph`, lines 317 to 333).
    async fn finalize_graph(&self, graph_id: &str) -> Result<(), GASPError> {
        let refs = self
            .pending_graphs
            .lock()
            .map_err(|e| GASPError::Other(format!("Lock poisoned: {e}")))?;

        if !refs.contains_key(graph_id) {
            return Err(GASPError::Other(format!("No pending graph for {graph_id}")));
        }

        let node_count = refs
            .values()
            .filter(|pn| pn.node.graph_id == graph_id)
            .count();

        debug!("Finalizing graph {graph_id} ({node_count} nodes). Computing ordered BEEFs.");

        // Compute ordered BEEFs for the graph.
        let beefs = Self::compute_ordered_beefs(graph_id, &refs, self.strict_beef)?;

        // Push to shared sink for later Engine submission.
        drop(refs);
        self.finalized_sink
            .lock()
            .map_err(|e| GASPError::Other(format!("Lock poisoned: {e}")))?
            .push(FinalizedGraph {
                topic: self.topic.clone(),
                beefs,
            });

        // Remove all nodes belonging to this graph from pending refs.
        let mut refs = self
            .pending_graphs
            .lock()
            .map_err(|e| GASPError::Other(format!("Lock poisoned: {e}")))?;
        let keys_to_remove: Vec<String> = refs
            .iter()
            .filter(|(_, pn)| pn.node.graph_id == graph_id)
            .map(|(k, _)| k.clone())
            .collect();
        for key in keys_to_remove {
            refs.remove(&key);
        }

        Ok(())
    }

    /// Discard a temporary graph that failed validation.
    async fn discard_graph(&self, graph_id: &str) -> Result<(), GASPError> {
        debug!("Discarding graph: {graph_id}");
        let mut refs = self
            .pending_graphs
            .lock()
            .map_err(|e| GASPError::Other(format!("Lock poisoned: {e}")))?;
        let keys_to_remove: Vec<String> = refs
            .iter()
            .filter(|(_, pn)| pn.node.graph_id == graph_id)
            .map(|(k, _)| k.clone())
            .collect();
        for key in keys_to_remove {
            refs.remove(&key);
        }
        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gasp::GASPStorage;
    use crate::storage::memory::MemoryStorage;
    use crate::storage::Storage;
    use crate::types::Output;

    fn make_sink() -> FinalizedGraphSink {
        new_finalized_graph_sink()
    }

    fn make_output(txid: &str, index: u32, topic: &str, score: f64) -> Output {
        Output {
            txid: txid.to_string(),
            output_index: index,
            output_script: vec![0x76, 0xa9],
            satoshis: 1000,
            topic: topic.to_string(),
            spent: false,
            outputs_consumed: vec![],
            consumed_by: vec![],
            beef: Some(vec![0xBE, 0xEF]),
            block_height: None,
            score: Some(score),
        }
    }

    /// Build a minimal valid transaction hex with one input and one output.
    fn make_valid_tx_hex(source_txid: &str, source_oi: u32) -> String {
        let mut tx = Transaction::new();
        let input = bsv_rs::transaction::TransactionInput::new(source_txid.to_string(), source_oi);
        tx.inputs.push(input);
        tx.outputs.push(bsv_rs::transaction::TransactionOutput::new(
            100,
            bsv_rs::script::LockingScript::from_hex(
                "76a914000000000000000000000000000000000000000088ac",
            )
            .unwrap(),
        ));
        tx.to_hex()
    }

    /// Build a coinbase-like tx (no meaningful inputs).
    fn make_coinbase_tx_hex() -> String {
        let mut tx = Transaction::new();
        tx.outputs.push(bsv_rs::transaction::TransactionOutput::new(
            5000,
            bsv_rs::script::LockingScript::from_hex(
                "76a914000000000000000000000000000000000000000088ac",
            )
            .unwrap(),
        ));
        tx.to_hex()
    }

    // ── find_known_utxos tests ────────────────────────────────────────

    #[tokio::test]
    async fn find_known_utxos_returns_correct_outputs() {
        let store = MemoryStorage::new();
        store
            .insert_output(&make_output("tx1", 0, "tm_test", 100.0))
            .await
            .unwrap();
        store
            .insert_output(&make_output("tx2", 0, "tm_test", 200.0))
            .await
            .unwrap();
        store
            .insert_output(&make_output("tx3", 0, "tm_other", 300.0))
            .await
            .unwrap();

        let gasp_storage = OverlayGASPStorage::new(&store, "tm_test", make_sink());

        let utxos = gasp_storage.find_known_utxos(0, None).await.unwrap();
        assert_eq!(utxos.len(), 2, "Should return only tm_test UTXOs");
        assert_eq!(utxos[0].txid, "tx1");
        assert_eq!(utxos[1].txid, "tx2");
    }

    #[tokio::test]
    async fn find_known_utxos_respects_since() {
        let store = MemoryStorage::new();
        store
            .insert_output(&make_output("tx1", 0, "tm_test", 100.0))
            .await
            .unwrap();
        store
            .insert_output(&make_output("tx2", 0, "tm_test", 200.0))
            .await
            .unwrap();
        store
            .insert_output(&make_output("tx3", 0, "tm_test", 300.0))
            .await
            .unwrap();

        let gasp_storage = OverlayGASPStorage::new(&store, "tm_test", make_sink());

        let utxos = gasp_storage.find_known_utxos(200, None).await.unwrap();
        assert_eq!(utxos.len(), 2, "Should return UTXOs with score >= 200");
        assert_eq!(utxos[0].txid, "tx2");
        assert_eq!(utxos[1].txid, "tx3");
    }

    #[tokio::test]
    async fn find_known_utxos_respects_limit() {
        let store = MemoryStorage::new();
        store
            .insert_output(&make_output("tx1", 0, "tm_test", 100.0))
            .await
            .unwrap();
        store
            .insert_output(&make_output("tx2", 0, "tm_test", 200.0))
            .await
            .unwrap();
        store
            .insert_output(&make_output("tx3", 0, "tm_test", 300.0))
            .await
            .unwrap();

        let gasp_storage = OverlayGASPStorage::new(&store, "tm_test", make_sink());

        let utxos = gasp_storage.find_known_utxos(0, Some(2)).await.unwrap();
        assert_eq!(utxos.len(), 2, "Should limit to 2 results");
    }

    #[tokio::test]
    async fn find_known_utxos_excludes_spent() {
        let store = MemoryStorage::new();
        store
            .insert_output(&make_output("tx1", 0, "tm_test", 100.0))
            .await
            .unwrap();
        store
            .insert_output(&make_output("tx2", 0, "tm_test", 200.0))
            .await
            .unwrap();
        store.mark_utxo_as_spent("tx1", 0, "tm_test").await.unwrap();

        let gasp_storage = OverlayGASPStorage::new(&store, "tm_test", make_sink());

        let utxos = gasp_storage.find_known_utxos(0, None).await.unwrap();
        assert_eq!(utxos.len(), 1, "Should exclude spent UTXOs");
        assert_eq!(utxos[0].txid, "tx2");
    }

    #[tokio::test]
    async fn find_known_utxos_empty_topic() {
        let store = MemoryStorage::new();
        let gasp_storage = OverlayGASPStorage::new(&store, "tm_empty", make_sink());

        let utxos = gasp_storage.find_known_utxos(0, None).await.unwrap();
        assert!(utxos.is_empty());
    }

    // ── find_needed_inputs tests ──────────────────────────────────────

    #[tokio::test]
    async fn find_needed_inputs_returns_none_for_unparsable_tx() {
        let store = MemoryStorage::new();
        let gasp_storage = OverlayGASPStorage::new(&store, "tm_test", make_sink());

        let node = GASPNode {
            graph_id: "abc.0".to_string(),
            raw_tx: "deadbeef".to_string(), // not valid tx hex
            output_index: 0,
            proof: None,
            tx_metadata: None,
            output_metadata: None,
            inputs: None,
        };

        let result = gasp_storage.find_needed_inputs(&node).await.unwrap();
        assert!(result.is_none(), "Unparsable tx: graceful fallback to None");
    }

    #[tokio::test]
    async fn find_needed_inputs_returns_none_for_proved_tx() {
        let store = MemoryStorage::new();
        let gasp_storage = OverlayGASPStorage::new(&store, "tm_test", make_sink());

        // A node with a proof (merkle path) should not need inputs.
        let raw_tx = make_valid_tx_hex(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            0,
        );
        let node = GASPNode {
            graph_id: "abc.0".to_string(),
            raw_tx,
            output_index: 0,
            proof: Some("some_proof_hex".to_string()),
            tx_metadata: None,
            output_metadata: None,
            inputs: None,
        };

        let result = gasp_storage.find_needed_inputs(&node).await.unwrap();
        assert!(result.is_none(), "Proved tx should not need inputs");
    }

    #[tokio::test]
    async fn find_needed_inputs_requests_inputs_for_unproved_tx() {
        let store = MemoryStorage::new();
        let gasp_storage = OverlayGASPStorage::new(&store, "tm_test", make_sink());

        let source_txid = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let raw_tx = make_valid_tx_hex(source_txid, 0);

        let node = GASPNode {
            graph_id: "abc.0".to_string(),
            raw_tx,
            output_index: 0,
            proof: None,
            tx_metadata: None,
            output_metadata: None,
            inputs: None,
        };

        let result = gasp_storage.find_needed_inputs(&node).await.unwrap();
        assert!(result.is_some(), "Unproved tx should request inputs");
        let response = result.unwrap();
        assert_eq!(response.requested_inputs.len(), 1);
        let key = format!("{source_txid}.0");
        assert!(
            response.requested_inputs.contains_key(&key),
            "Should request the source input"
        );
    }

    #[tokio::test]
    async fn find_needed_inputs_strips_known_inputs() {
        let store = MemoryStorage::new();
        let source_txid = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

        // Insert the source output so it's already known
        store
            .insert_output(&make_output(source_txid, 0, "tm_test", 50.0))
            .await
            .unwrap();

        let gasp_storage = OverlayGASPStorage::new(&store, "tm_test", make_sink());

        let raw_tx = make_valid_tx_hex(source_txid, 0);
        let node = GASPNode {
            graph_id: "abc.0".to_string(),
            raw_tx,
            output_index: 0,
            proof: None,
            tx_metadata: None,
            output_metadata: None,
            inputs: None,
        };

        let result = gasp_storage.find_needed_inputs(&node).await.unwrap();
        assert!(
            result.is_none(),
            "Should strip already-known inputs, resulting in None"
        );
    }

    #[tokio::test]
    async fn find_needed_inputs_returns_none_for_coinbase() {
        let store = MemoryStorage::new();
        let gasp_storage = OverlayGASPStorage::new(&store, "tm_test", make_sink());

        let raw_tx = make_coinbase_tx_hex();
        let node = GASPNode {
            graph_id: "abc.0".to_string(),
            raw_tx,
            output_index: 0,
            proof: None,
            tx_metadata: None,
            output_metadata: None,
            inputs: None,
        };

        let result = gasp_storage.find_needed_inputs(&node).await.unwrap();
        assert!(
            result.is_none(),
            "Coinbase-like tx (no inputs) should need nothing"
        );
    }

    // ── append / finalize / discard tests ─────────────────────────────

    #[tokio::test]
    async fn append_and_finalize_graph() {
        let store = MemoryStorage::new();
        let sink = make_sink();
        let gasp_storage = OverlayGASPStorage::new(&store, "tm_test", sink.clone());

        let raw_tx = make_coinbase_tx_hex();
        let node = GASPNode {
            graph_id: "abc.0".to_string(),
            raw_tx,
            output_index: 0,
            proof: None,
            tx_metadata: None,
            output_metadata: None,
            inputs: None,
        };

        gasp_storage.append_to_graph(&node, None).await.unwrap();
        assert_eq!(gasp_storage.pending_graph_count(), 1);

        gasp_storage.finalize_graph("abc.0").await.unwrap();
        assert_eq!(gasp_storage.pending_graph_count(), 0);

        // Check that finalized data was produced
        let finalized = sink.lock().unwrap();
        assert_eq!(finalized.len(), 1, "Should have one finalized graph");
        assert_eq!(finalized[0].topic, "tm_test");
        assert!(!finalized[0].beefs.is_empty(), "Should have BEEF data");
    }

    #[tokio::test]
    async fn finalize_graph_produces_submittable_beef() {
        let store = MemoryStorage::new();
        let sink = make_sink();
        let gasp_storage = OverlayGASPStorage::new(&store, "tm_test", sink.clone());

        let raw_tx = make_coinbase_tx_hex();
        let node = GASPNode {
            graph_id: "root.0".to_string(),
            raw_tx,
            output_index: 0,
            proof: None,
            tx_metadata: None,
            output_metadata: None,
            inputs: None,
        };

        gasp_storage.append_to_graph(&node, None).await.unwrap();
        gasp_storage.finalize_graph("root.0").await.unwrap();

        let finalized = sink.lock().unwrap();
        assert_eq!(finalized.len(), 1);

        // Each BEEF should be parseable
        for beef_bytes in &finalized[0].beefs {
            let tx = Transaction::from_beef(beef_bytes, None);
            assert!(
                tx.is_ok(),
                "Finalized BEEF should be parseable: {:?}",
                tx.err()
            );
        }
    }

    #[tokio::test]
    async fn append_multiple_nodes_same_graph() {
        let store = MemoryStorage::new();
        let gasp_storage = OverlayGASPStorage::new(&store, "tm_test", make_sink());

        let raw_tx1 = make_coinbase_tx_hex();
        let node1 = GASPNode {
            graph_id: "abc.0".to_string(),
            raw_tx: raw_tx1,
            output_index: 0,
            proof: None,
            tx_metadata: None,
            output_metadata: None,
            inputs: None,
        };
        let raw_tx2 = make_coinbase_tx_hex();
        let node2 = GASPNode {
            graph_id: "abc.0".to_string(),
            raw_tx: raw_tx2,
            output_index: 0,
            proof: None,
            tx_metadata: None,
            output_metadata: None,
            inputs: None,
        };

        gasp_storage.append_to_graph(&node1, None).await.unwrap();
        gasp_storage
            .append_to_graph(&node2, Some("abc.0"))
            .await
            .unwrap();
        assert_eq!(gasp_storage.pending_graph_count(), 1);
    }

    #[tokio::test]
    async fn discard_graph_removes_pending() {
        let store = MemoryStorage::new();
        let gasp_storage = OverlayGASPStorage::new(&store, "tm_test", make_sink());

        let raw_tx = make_coinbase_tx_hex();
        let node = GASPNode {
            graph_id: "abc.0".to_string(),
            raw_tx,
            output_index: 0,
            proof: None,
            tx_metadata: None,
            output_metadata: None,
            inputs: None,
        };

        gasp_storage.append_to_graph(&node, None).await.unwrap();
        assert_eq!(gasp_storage.pending_graph_count(), 1);

        gasp_storage.discard_graph("abc.0").await.unwrap();
        assert_eq!(gasp_storage.pending_graph_count(), 0);
    }

    #[tokio::test]
    async fn validate_graph_anchor_refuses_an_unknown_graph() {
        let store = MemoryStorage::new();
        let gasp_storage = OverlayGASPStorage::new(&store, "tm_test", make_sink());

        // The reference throws `Graph node with ID ... not found`.
        let error = gasp_storage
            .validate_graph_anchor("nonexistent.0")
            .await
            .unwrap_err();
        assert!(matches!(error, GASPError::ValidationFailed(_)), "{error}");
    }

    #[tokio::test]
    async fn finalize_empty_graph_errors() {
        let store = MemoryStorage::new();
        let gasp_storage = OverlayGASPStorage::new(&store, "tm_test", make_sink());

        let result = gasp_storage.finalize_graph("nonexistent.0").await;
        assert!(result.is_err(), "Should error on unknown graph");
    }

    #[tokio::test]
    async fn gasp_storage_is_object_safe() {
        let store = MemoryStorage::new();
        let gasp_storage = OverlayGASPStorage::new(&store, "tm_test", make_sink());

        // Verify it can be used as dyn GASPStorage
        let boxed: Box<dyn GASPStorage> = Box::new(gasp_storage);
        let utxos = boxed.find_known_utxos(0, None).await.unwrap();
        assert!(utxos.is_empty());
    }

    /// Integration test: OverlayGASPStorage works with GASPSync.
    #[tokio::test]
    async fn overlay_gasp_storage_with_gasp_sync() {
        use crate::gasp::{GASPRemote, GASPSync};
        use crate::types::{GASPInitialReply, GASPInitialRequest, GASPInitialResponse};

        // Set up local storage with one UTXO
        let store = MemoryStorage::new();
        store
            .insert_output(&make_output("local_tx", 0, "tm_test", 50.0))
            .await
            .unwrap();

        let sink = make_sink();
        let gasp_storage = OverlayGASPStorage::new(&store, "tm_test", sink.clone());

        // Mock remote with two UTXOs — provides unparsable raw_tx so
        // find_needed_inputs returns None (graceful fallback).
        struct TestRemote;

        #[async_trait(?Send)]
        impl GASPRemote for TestRemote {
            async fn get_initial_response(
                &self,
                request: &GASPInitialRequest,
            ) -> Result<GASPInitialResponse, GASPError> {
                Ok(GASPInitialResponse {
                    utxo_list: vec![
                        GASPOutput {
                            txid: "remote_tx1".to_string(),
                            output_index: 0,
                            score: 100.0,
                        },
                        GASPOutput {
                            txid: "remote_tx2".to_string(),
                            output_index: 0,
                            score: 200.0,
                        },
                    ],
                    since: request.since,
                })
            }
            async fn get_initial_reply(
                &self,
                _: &GASPInitialResponse,
            ) -> Result<GASPInitialReply, GASPError> {
                Ok(GASPInitialReply {
                    utxo_list: Vec::new(),
                })
            }
            async fn request_node(
                &self,
                graph_id: &str,
                txid: &str,
                output_index: u32,
                _: bool,
            ) -> Result<GASPNode, GASPError> {
                Ok(GASPNode {
                    graph_id: graph_id.to_string(),
                    raw_tx: format!("rawtx_{txid}"),
                    output_index,
                    proof: None,
                    tx_metadata: None,
                    output_metadata: None,
                    inputs: None,
                })
            }
            async fn submit_node(
                &self,
                _: &GASPNode,
            ) -> Result<Option<GASPNodeResponse>, GASPError> {
                Ok(None)
            }
        }

        let mut sync = GASPSync::new(
            Box::new(gasp_storage),
            Box::new(TestRemote),
            0,
            "[TEST]",
            true, // unidirectional
        );

        sync.sync(None).await.unwrap();
        // Capped below the lowest un-ingested score (the synthetic test graph
        // cannot assemble a BEEF, so ingest fails transiently) rather than
        // skipping to the seen tip@200. See the #43 gap-guard in `gasp.rs::sync`.
        assert_eq!(sync.last_interaction, 99);
    }

    /// Integration test: full sync with valid transactions produces submittable BEEFs.
    #[tokio::test]
    async fn gasp_sync_with_valid_tx_produces_finalized_beefs() {
        use crate::gasp::{GASPRemote, GASPSync};
        use crate::types::{GASPInitialReply, GASPInitialRequest, GASPInitialResponse};

        let store = MemoryStorage::new();
        let sink = make_sink();
        let gasp_storage = OverlayGASPStorage::new(&store, "tm_test", sink.clone());

        // Build a valid coinbase-style tx for the remote to provide.
        let valid_tx_hex = make_coinbase_tx_hex();

        struct ValidTxRemote {
            tx_hex: String,
        }

        #[async_trait(?Send)]
        impl GASPRemote for ValidTxRemote {
            async fn get_initial_response(
                &self,
                request: &GASPInitialRequest,
            ) -> Result<GASPInitialResponse, GASPError> {
                Ok(GASPInitialResponse {
                    utxo_list: vec![GASPOutput {
                        txid: "valid_tx".to_string(),
                        output_index: 0,
                        score: 100.0,
                    }],
                    since: request.since,
                })
            }
            async fn get_initial_reply(
                &self,
                _: &GASPInitialResponse,
            ) -> Result<GASPInitialReply, GASPError> {
                Ok(GASPInitialReply {
                    utxo_list: Vec::new(),
                })
            }
            async fn request_node(
                &self,
                graph_id: &str,
                _txid: &str,
                output_index: u32,
                _: bool,
            ) -> Result<GASPNode, GASPError> {
                // Served PROVEN: since bsv-low #551 the anchor check refuses
                // an unproven transaction that creates satoshis (this one has
                // no inputs). With no chain tracker a merkle path is accepted
                // unchecked, the reference's 'scripts only'.
                let txid = Transaction::from_hex(&self.tx_hex).unwrap().id();
                let proof = MerklePath::new(
                    100,
                    vec![vec![bsv_rs::transaction::MerklePathLeaf::new_txid(0, txid)]],
                )
                .unwrap();
                Ok(GASPNode {
                    graph_id: graph_id.to_string(),
                    raw_tx: self.tx_hex.clone(),
                    output_index,
                    proof: Some(proof.to_hex()),
                    tx_metadata: None,
                    output_metadata: None,
                    inputs: None,
                })
            }
            async fn submit_node(
                &self,
                _: &GASPNode,
            ) -> Result<Option<GASPNodeResponse>, GASPError> {
                Ok(None)
            }
        }

        let remote = ValidTxRemote {
            tx_hex: valid_tx_hex,
        };

        let mut sync = GASPSync::new(Box::new(gasp_storage), Box::new(remote), 0, "[TEST]", true);

        sync.sync(None).await.unwrap();

        let finalized = sink.lock().unwrap();
        assert_eq!(finalized.len(), 1, "Should have one finalized graph");
        assert!(!finalized[0].beefs.is_empty(), "Should have BEEF bytes");

        // Verify the BEEF is parseable
        for beef_bytes in &finalized[0].beefs {
            let parsed = Transaction::from_beef(beef_bytes, None);
            assert!(parsed.is_ok(), "BEEF should be parseable");
        }
    }

    #[tokio::test]
    async fn take_finalized_graphs_drains_list() {
        let store = MemoryStorage::new();
        let sink = make_sink();
        let gasp_storage = OverlayGASPStorage::new(&store, "tm_test", sink.clone());

        let raw_tx = make_coinbase_tx_hex();
        let node = GASPNode {
            graph_id: "abc.0".to_string(),
            raw_tx,
            output_index: 0,
            proof: None,
            tx_metadata: None,
            output_metadata: None,
            inputs: None,
        };

        gasp_storage.append_to_graph(&node, None).await.unwrap();
        gasp_storage.finalize_graph("abc.0").await.unwrap();

        let taken = gasp_storage.take_finalized_graphs();
        assert_eq!(taken.len(), 1);

        // Should be empty after draining
        let taken2 = gasp_storage.take_finalized_graphs();
        assert!(taken2.is_empty());
    }
}
