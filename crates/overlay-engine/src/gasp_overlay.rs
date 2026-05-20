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
//! `FinalizedGraphSink`. After `GASPSync::sync()` returns, the caller (Engine)
//! drains the sink and submits each BEEF to `Engine::submit()` with
//! `HistoricalTxNoSpv` mode.
//!
//! ## find_needed_inputs
//!
//! Parses the node's raw transaction hex to determine what inputs are needed:
//! - If the node has a merkle proof, no inputs are needed (it is mined).
//! - If no proof, all transaction inputs are requested (minus any already
//!   known in local storage).
//!
//! Ported from `~/bsv/overlay-services/src/GASP/OverlayGASPStorage.ts` (388 lines).

use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Mutex;

use async_trait::async_trait;
use bsv_rs::transaction::{MerklePath, Transaction};
use tracing::{debug, warn};

use crate::gasp::{GASPError, GASPStorage};
use crate::storage::Storage;
use crate::types::{GASPInputRequest, GASPNode, GASPNodeResponse, GASPOutput};

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
            pending_graphs: Mutex::new(HashMap::new()),
            finalized_sink: sink,
            strict_beef: false,
        }
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
    /// - If the node has a merkle proof, it is a mined transaction and
    ///   no further inputs are needed (returns `None`).
    /// - If no proof, parses the raw transaction and requests all inputs,
    ///   filtering out any inputs already known in local storage.
    /// - If the raw_tx cannot be parsed, returns `None` (graceful fallback).
    async fn find_needed_inputs(
        &self,
        node: &GASPNode,
    ) -> Result<Option<GASPNodeResponse>, GASPError> {
        // If there is a merkle proof, this transaction is mined — no inputs needed.
        if node.proof.is_some() {
            return Ok(None);
        }

        // Parse the raw transaction to enumerate its inputs.
        let tx = match Transaction::from_hex(&node.raw_tx) {
            Ok(tx) => tx,
            Err(e) => {
                warn!(
                    "Cannot parse raw_tx for find_needed_inputs (graph_id={}): {e}",
                    node.graph_id
                );
                // Graceful fallback: if we can't parse, don't request inputs.
                return Ok(None);
            }
        };

        let mut requested_inputs: HashMap<String, GASPInputRequest> = HashMap::new();

        for input in &tx.inputs {
            let source_txid = input.get_source_txid().unwrap_or_default();
            if source_txid.is_empty() {
                continue;
            }
            let outpoint = format!("{}.{}", source_txid, input.source_output_index);
            requested_inputs.insert(outpoint, GASPInputRequest { metadata: false });
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

    /// Validate that the graph's anchor references proven or known transactions.
    ///
    /// For the minimal implementation, we accept all anchors.
    /// A full implementation would verify merkle proofs against a chain tracker.
    async fn validate_graph_anchor(&self, _graph_id: &str) -> Result<(), GASPError> {
        // Accept all anchors for now. When SPV validation is needed per-graph,
        // this should verify the root node's proof against the chain tracker.
        Ok(())
    }

    /// Finalize a graph — convert accumulated nodes to ordered BEEFs and push
    /// them to the shared sink for later Engine submission.
    ///
    /// Computes ordered BEEF byte arrays (ancestors first, root last) from the
    /// temporary graph nodes and stores them in the `FinalizedGraphSink`. The
    /// Engine retrieves these after sync completes and submits each one with
    /// `HistoricalTxNoSpv` mode.
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
    async fn validate_graph_anchor_always_succeeds() {
        let store = MemoryStorage::new();
        let gasp_storage = OverlayGASPStorage::new(&store, "tm_test", make_sink());

        // Should not error even for unknown graph IDs
        gasp_storage
            .validate_graph_anchor("nonexistent.0")
            .await
            .unwrap();
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
        assert_eq!(sync.last_interaction, 200);
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
                Ok(GASPNode {
                    graph_id: graph_id.to_string(),
                    raw_tx: self.tx_hex.clone(),
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
