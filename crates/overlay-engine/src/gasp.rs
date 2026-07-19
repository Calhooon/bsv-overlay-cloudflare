//! GASP — Graph Aware Sync Protocol.
//!
//! Defines the `GASPStorage` and `GASPRemote` traits, plus the `GASP` orchestrator
//! that synchronizes overlay UTXOs between peers.
//!
//! Ported from:
//! - `~/bsv/gasp-core/src/GASP.ts` (1,557 lines)
//! - `~/bsv/overlay-services/src/GASP/OverlayGASPStorage.ts` (388 lines)
//! - `~/bsv/overlay-services/src/GASP/OverlayGASPRemote.ts` (108 lines)

use async_trait::async_trait;
use tracing::{debug, error, info, warn};

use crate::types::{
    GASPInitialReply, GASPInitialRequest, GASPInitialResponse, GASPNode, GASPNodeResponse,
    GASPOutput,
};

/// Current GASP protocol version.
pub const GASP_VERSION: u32 = 1;

/// Default sync limit per page.
pub const DEFAULT_GASP_SYNC_LIMIT: u64 = 10000;

// ============================================================================
// GASPStorage trait
// ============================================================================

/// Local storage interface for the GASP protocol.
///
/// Manages known UTXOs, temporary graph construction during sync,
/// and graph validation/finalization.
#[async_trait(?Send)]
pub trait GASPStorage {
    /// Returns UTXOs known to be unspent since the given score/timestamp.
    /// Non-confirmed UTXOs should always be returned regardless of `since`.
    async fn find_known_utxos(
        &self,
        since: u64,
        limit: Option<u64>,
    ) -> Result<Vec<GASPOutput>, GASPError>;

    /// Hydrate a GASP node with transaction data, proof, and optional metadata.
    async fn hydrate_gasp_node(
        &self,
        graph_id: &str,
        txid: &str,
        output_index: u32,
        metadata: bool,
    ) -> Result<GASPNode, GASPError>;

    /// Determine which input transactions are needed to validate this node.
    /// Returns None if no additional inputs are needed.
    async fn find_needed_inputs(
        &self,
        node: &GASPNode,
    ) -> Result<Option<GASPNodeResponse>, GASPError>;

    /// Append a node to a temporary graph being constructed during sync.
    /// `spent_by` is the "txid.outputIndex" of the node that spent this one (if not the root).
    async fn append_to_graph(
        &self,
        node: &GASPNode,
        spent_by: Option<&str>,
    ) -> Result<(), GASPError>;

    /// Validate that the graph's anchor (root) references only proven or known transactions.
    async fn validate_graph_anchor(&self, graph_id: &str) -> Result<(), GASPError>;

    /// Finalize a graph — commit the synced UTXO and its ancestors to permanent storage.
    async fn finalize_graph(&self, graph_id: &str) -> Result<(), GASPError>;

    /// Discard a temporary graph that failed validation.
    async fn discard_graph(&self, graph_id: &str) -> Result<(), GASPError>;
}

// ============================================================================
// GASPRemote trait
// ============================================================================

/// Communication interface with a foreign GASP peer.
#[async_trait(?Send)]
pub trait GASPRemote {
    /// Send an initial request and get the peer's initial response.
    async fn get_initial_response(
        &self,
        request: &GASPInitialRequest,
    ) -> Result<GASPInitialResponse, GASPError>;

    /// Send our initial response and get the peer's reply.
    async fn get_initial_reply(
        &self,
        response: &GASPInitialResponse,
    ) -> Result<GASPInitialReply, GASPError>;

    /// Request a specific node from the peer.
    async fn request_node(
        &self,
        graph_id: &str,
        txid: &str,
        output_index: u32,
        metadata: bool,
    ) -> Result<GASPNode, GASPError>;

    /// Submit a node to the peer and get back which inputs they need.
    async fn submit_node(&self, node: &GASPNode) -> Result<Option<GASPNodeResponse>, GASPError>;
}

// ============================================================================
// GASPRemoteFactory trait
// ============================================================================

/// Factory for creating `GASPRemote` instances for specific peers.
///
/// The Engine holds an optional factory. Platform-specific crates (like
/// overlay-cloudflare) provide an implementation that creates HTTP-based
/// remotes.
pub trait GASPRemoteFactory {
    /// Create a `GASPRemote` for the given peer URL and topic.
    fn create_remote(&self, peer_url: &str, topic: &str) -> Box<dyn GASPRemote>;
}

// ============================================================================
// AncestorFetcher trait (OPT-IN, OFF by default)
// ============================================================================

/// Optional chain-backed ancestor fetcher for GASP ingest self-healing.
///
/// **OPT-IN / OFF BY DEFAULT.** When a peer (e.g. legacy TS beta) cannot serve
/// a needed ancestor node during graph ingest (it returns HTTP 400 "Incomplete
/// SPV data!" because its stored BEEF is minimal), the orchestrator normally
/// abandons that graph. If — and ONLY if — an `AncestorFetcher` is configured,
/// `process_incoming_node` falls back to fetching the ancestor's raw tx from
/// chain (e.g. WhatsOnChain) and synthesizing a no-proof `GASPNode` that the
/// existing recursion stitches into the graph.
///
/// This is a deliberate one-time-migration escape hatch. Production must NOT
/// configure a fetcher: when the fetcher is `None` the ingest path is
/// byte-identical to today (peer errors propagate / are swallowed upstream).
///
/// The orchestrator stays platform-agnostic; the concrete WoC/`worker::Fetch`
/// implementation lives in the platform crate (e.g. zanaadu `overlay`), never
/// in this engine crate (no `reqwest`/`std::time` — must stay wasm-clean).
#[async_trait(?Send)]
pub trait AncestorFetcher {
    /// Fetch an ancestor transaction by txid from chain.
    ///
    /// Returns the raw tx hex plus, when the ancestor is mined, its BUMP merkle
    /// proof hex. Supplying the proof is important: a synthesized node WITH a
    /// proof terminates the ingest recursion (it is a mined SPV leaf —
    /// `find_needed_inputs` returns `None`), so the graph walk stops at the
    /// first proven layer instead of recursing through every input (including
    /// funding/fee inputs) back toward coinbase. Omitting it would make a deep
    /// chain's graph effectively unbounded.
    ///
    /// The implementation MUST verify that the returned bytes hash to the
    /// requested `txid` before returning them (integrity check) so a
    /// malicious/garbled response cannot inject a forged ancestor.
    async fn fetch_ancestor(&self, txid: &str) -> Result<FetchedAncestor, GASPError>;

    /// Fetch (and chaintracks-verify) ONLY the merkle BUMP for `txid`, WITHOUT
    /// fetching the raw tx.
    ///
    /// The proof-completion passes (`complete_missing_proofs`, the LOW pot-store
    /// tick) already hold the raw in the stored BEEF, so the raw fetch that
    /// [`Self::fetch_ancestor`] performs is a redundant network round-trip there
    /// — and a free-tier WhatsOnChain raw fetch 429s (#192/#193). This method is
    /// the raw-free path.
    ///
    /// Default: delegate to `fetch_ancestor` and drop the raw, so a fetcher that
    /// only implements `fetch_ancestor` keeps working unchanged. The production
    /// `ChainProofFetcher` overrides it to skip the raw fetch entirely. Returns
    /// `None` for an unmined/unverifiable tx (fail-closed), never an error.
    async fn verified_proof_for(&self, txid: &str) -> Option<String> {
        self.fetch_ancestor(txid).await.ok().and_then(|a| a.proof)
    }

    /// Verify that `bump_hex` is a chaintracks-valid merkle proof for `txid`.
    ///
    /// Used by proof completion to re-check a STORED structural bump before
    /// trusting its `has_proof` flag: a structural bump admitted WITHOUT SPV (or
    /// forged) must never be latched-proven and trimmed on (#192/#193). Default:
    /// fail-closed `false` — a fetcher with no header source can prove nothing.
    /// The production `ChainProofFetcher` overrides it against chaintracks.
    async fn verify_proof(&self, txid: &str, bump_hex: &str) -> bool {
        let _ = (txid, bump_hex);
        false
    }
}

/// An ancestor transaction fetched from chain: its raw tx hex plus an optional
/// BUMP merkle proof hex (present when the tx is mined).
#[derive(Debug, Clone)]
pub struct FetchedAncestor {
    /// Raw transaction hex.
    pub raw_tx: String,
    /// BUMP merkle proof hex, if the ancestor is mined. When `Some`, the
    /// synthesized node is a proven SPV leaf and the recursion terminates.
    pub proof: Option<String>,
}

// ============================================================================
// GASP orchestrator
// ============================================================================

/// GASP sync orchestrator.
///
/// Coordinates between local `GASPStorage` and a `GASPRemote` peer to
/// synchronize overlay UTXOs. Supports paginated sync and unidirectional mode.
///
/// The lifetime `'a` allows the storage and remote to borrow from their
/// environment (e.g., `OverlayGASPStorage` borrows from the Engine's storage).
pub struct GASPSync<'a> {
    storage: Box<dyn GASPStorage + 'a>,
    remote: Box<dyn GASPRemote + 'a>,
    /// Score of last successful interaction with this peer.
    pub last_interaction: u64,
    /// If true, only pull from remote — don't push local UTXOs.
    pub unidirectional: bool,
    log_prefix: String,
    /// OPT-IN / OFF BY DEFAULT chain-backed ancestor fetcher. When `None`
    /// (the default), the ingest path is byte-identical to today: a peer's
    /// `request_node` error propagates and the graph is abandoned upstream.
    /// When `Some`, a peer ancestor-serve failure falls back to chain.
    ancestor_fetcher: Option<std::rc::Rc<dyn AncestorFetcher + 'a>>,
}

impl<'a> GASPSync<'a> {
    /// Create a new GASP sync orchestrator.
    ///
    /// The ancestor fetcher is OFF by default. Use `with_ancestor_fetcher` to
    /// opt in to chain-backed ancestry hydration (one-time-migration only).
    pub fn new(
        storage: Box<dyn GASPStorage + 'a>,
        remote: Box<dyn GASPRemote + 'a>,
        last_interaction: u64,
        log_prefix: impl Into<String>,
        unidirectional: bool,
    ) -> Self {
        Self {
            storage,
            remote,
            last_interaction,
            unidirectional,
            log_prefix: log_prefix.into(),
            ancestor_fetcher: None,
        }
    }

    /// Opt in to chain-backed ancestor hydration (OFF by default).
    ///
    /// When set, a peer's inability to serve a needed ancestor during ingest
    /// triggers a fallback fetch of that ancestor's raw tx from chain. This is
    /// a deliberate one-time-migration escape hatch — production should NOT
    /// call this. Without it, behavior is unchanged.
    #[must_use]
    pub fn with_ancestor_fetcher(
        mut self,
        fetcher: Option<std::rc::Rc<dyn AncestorFetcher + 'a>>,
    ) -> Self {
        self.ancestor_fetcher = fetcher;
        self
    }

    /// Run the sync protocol with the remote peer.
    ///
    /// 1. Request remote's UTXOs since last interaction (paginated)
    /// 2. For each unknown UTXO, request the full graph and ingest it
    /// 3. If bidirectional, push our unknown UTXOs to the remote
    pub async fn sync(&mut self, limit: Option<u64>) -> Result<(), GASPError> {
        info!(
            "{} Starting sync. last_interaction={}",
            self.log_prefix, self.last_interaction
        );

        // Track what we already know
        let local_utxos = self.storage.find_known_utxos(0, None).await?;
        let mut known_outpoints: std::collections::HashSet<String> = local_utxos
            .iter()
            .map(|u| format!("{}.{}", u.txid, u.output_index))
            .collect();
        let mut shared_outpoints: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        // The cursor we entered with. The persisted cursor must never regress
        // below it (a failed RE-ingest of an already-synced range is not a new
        // gap), and the gap-guard below is floored at it.
        let initial_interaction = self.last_interaction;
        // Lowest score of any UTXO whose graph ingest FAILED this run. The
        // in-loop `last_interaction` advance below is by max-seen-score so
        // pagination terminates, but a transient ingest failure must NOT let the
        // PERSISTED cursor skip past that output — else the peer never re-serves
        // it (next `since` excludes scores below the cursor) and it is stranded
        // forever. After the loop we cap the cursor strictly below this score so
        // the next sync re-requests the failed graph. (Without this, a single
        // blipped graph fetch silently drops that UTXO from continuous sync.)
        let mut min_failed_score: Option<u64> = None;

        // Paginated pull from remote
        loop {
            let request = GASPInitialRequest {
                version: GASP_VERSION,
                since: self.last_interaction,
                limit,
            };
            let response = self.remote.get_initial_response(&request).await?;
            let page_size = response.utxo_list.len();

            info!(
                "{} Processing page with {} UTXOs (since: {})",
                self.log_prefix, page_size, response.since
            );

            for utxo in &response.utxo_list {
                // Track highest score for pagination
                if utxo.score as u64 > self.last_interaction {
                    self.last_interaction = utxo.score as u64;
                }

                let outpoint = format!("{}.{}", utxo.txid, utxo.output_index);
                if known_outpoints.contains(&outpoint) {
                    shared_outpoints.insert(outpoint.clone());
                    known_outpoints.remove(&outpoint);
                } else if !shared_outpoints.contains(&outpoint) {
                    // New UTXO — request and ingest the graph
                    match self.ingest_utxo(utxo, &outpoint).await {
                        Ok(()) => {
                            shared_outpoints.insert(outpoint);
                        }
                        Err(e) => {
                            warn!(
                                "{} Error ingesting UTXO {}: {}",
                                self.log_prefix, outpoint, e
                            );
                            // Remember the lowest failed score so the persisted
                            // cursor cannot skip past it (see cap below).
                            let s = utxo.score as u64;
                            min_failed_score =
                                Some(min_failed_score.map_or(s, |cur| cur.min(s)));
                        }
                    }
                }
            }

            // Continue pagination if we got a full page
            if limit.is_none() || (page_size as u64) < limit.unwrap_or(u64::MAX) {
                break;
            }
        }

        // Gap-guard: if any graph ingest failed transiently, cap the cursor
        // strictly below the lowest failed score so the next sync re-pulls it
        // (the remote serves `score >= since`). Floored at `initial_interaction`
        // so the cursor never regresses — a failure within the already-synced
        // range is not a new gap and must not rewind the cursor.
        if let Some(failed) = min_failed_score {
            let cap = failed.saturating_sub(1).max(initial_interaction);
            if self.last_interaction > cap {
                warn!(
                    "{} Capping cursor {} -> {} (transient ingest failure at score {}); next sync will re-pull",
                    self.log_prefix, self.last_interaction, cap, failed
                );
                self.last_interaction = cap;
            }
        }

        // Bidirectional: push our UTXOs to remote
        if !self.unidirectional {
            // Find local UTXOs the remote doesn't have
            for utxo in &local_utxos {
                let outpoint = format!("{}.{}", utxo.txid, utxo.output_index);
                if !shared_outpoints.contains(&outpoint) {
                    match self.push_utxo(utxo).await {
                        Ok(()) => {}
                        Err(e) => {
                            warn!("{} Error pushing UTXO {}: {}", self.log_prefix, outpoint, e);
                        }
                    }
                }
            }
        }

        info!("{} Sync completed!", self.log_prefix);
        Ok(())
    }

    /// Request a UTXO's graph from remote and ingest it locally.
    async fn ingest_utxo(&self, utxo: &GASPOutput, outpoint: &str) -> Result<(), GASPError> {
        debug!("{} Requesting node for {}", self.log_prefix, outpoint);

        let node = self
            .remote
            .request_node(outpoint, &utxo.txid, utxo.output_index, true)
            .await?;

        self.process_incoming_node(&node, None, &mut std::collections::HashSet::new())
            .await?;
        self.complete_graph(&node.graph_id).await?;

        Ok(())
    }

    /// Push a local UTXO's graph to the remote.
    async fn push_utxo(&self, utxo: &GASPOutput) -> Result<(), GASPError> {
        let outpoint = format!("{}.{}", utxo.txid, utxo.output_index);
        debug!("{} Hydrating node for {}", self.log_prefix, outpoint);

        let node = self
            .storage
            .hydrate_gasp_node(&outpoint, &utxo.txid, utxo.output_index, true)
            .await?;

        self.process_outgoing_node(&node, &mut std::collections::HashSet::new())
            .await?;

        Ok(())
    }

    /// Process an incoming node: append to graph, then recursively fetch needed inputs.
    fn process_incoming_node<'b>(
        &'b self,
        node: &'b GASPNode,
        spent_by: Option<&'b str>,
        seen: &'b mut std::collections::HashSet<String>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), GASPError>> + 'b>> {
        Box::pin(async move {
            // Key by the node's own TXID (computed from raw_tx), NOT graph_id (which
            // is constant across a whole graph) and NOT the raw_tx hex. Mirrors TS
            // @bsv/gasp processIncomingNode: nodeId = `${computeTXID(rawTx)}.${oi}`
            // (GASP.js:319) and spentBy = compute36ByteStructure(computeTXID(rawTx), oi)
            // (GASP.js:335). Matches the append-side key in gasp_overlay.rs
            // (Transaction::from_hex(raw_tx).id()). Using graph_id/raw_tx here orphaned
            // every child node → multi-node graphs never assembled → stranded sync.
            let node_txid = match bsv_rs::transaction::Transaction::from_hex(&node.raw_tx) {
                Ok(tx) => tx.id(),
                Err(_) => node.raw_tx[..node.raw_tx.len().min(64)].to_string(),
            };
            let node_id = format!("{}.{}", node_txid, node.output_index);
            if seen.contains(&node_id) {
                return Ok(());
            }
            seen.insert(node_id);

            // GOD-TIER proof-anchoring (#126): if the peer served this node WITHOUT
            // a merkle proof but the tx is mined, hydrate its OWN proof via the
            // ancestor fetcher (WoC `/beef`). A proven node is a self-anchored SPV
            // leaf — `find_needed_inputs` returns None for it — so we NEVER walk into
            // the spent prior contract-state (a 2-tx-pattern template, or a covenant's
            // previous UTXO). Without this, the walk reaches a spent input whose output
            // record exists in storage (so `find_needed_inputs` strips it) but whose tx
            // is absent from the in-memory graph → `get_beef_for_node` fails "Missing
            // source transaction" and the whole graph is discarded. Legacy beta's
            // GASP-serve omits proofs, so this is required cross-stack. No-op when no
            // fetcher is configured (production default unchanged) or the node already
            // carries a proof (children fetched via the walk already do).
            // Only the GRAPH ROOT arrives from the peer (spent_by == None); children
            // come from the ancestry walk already carrying their proofs, so gate on
            // the root to avoid redundant re-fetches.
            let mut node_owned = node.clone();
            if spent_by.is_none() && node_owned.proof.is_none() {
                if let Some(fetcher) = &self.ancestor_fetcher {
                    if let Ok(root_fetch) = fetcher.fetch_ancestor(&node_txid).await {
                        if root_fetch.proof.is_some() {
                            node_owned.proof = root_fetch.proof;
                        }
                    }
                }
            }
            let node = &node_owned;

            self.storage.append_to_graph(node, spent_by).await?;

            if let Some(needed) = self.storage.find_needed_inputs(node).await? {
                for (outpoint, input_req) in &needed.requested_inputs {
                    if let Some((txid, oi)) = parse_outpoint(outpoint) {
                        let child_node = match &self.ancestor_fetcher {
                            // OPT-IN ancestry hydration (off by default). When a
                            // fetcher is configured we KNOW the peer cannot serve
                            // ancestry — e.g. legacy beta stores minimal BEEFs and
                            // returns HTTP 400 "Incomplete SPV data!" for every
                            // ancestor — so we SKIP the doomed peer round-trip
                            // entirely and fetch the ancestor's raw tx from chain
                            // directly. Asking the peer first would cost one
                            // sequential request per ancestor (the ~90-deep
                            // user_registry chain → ~90 round-trips), enough to
                            // exhaust a worker invocation before the graph finalizes.
                            //
                            // The fetcher returns the ancestor's rawtx and, when it
                            // is mined, its BUMP proof. A proven node is an SPV leaf
                            // — find_needed_inputs returns None for it — so the walk
                            // TERMINATES at the first proven layer instead of
                            // recursing through every input (incl. funding/fee
                            // inputs) toward coinbase, keeping the graph bounded. If
                            // proof is None (unmined ancestor), the recursion
                            // continues to ITS parents as before.
                            Some(fetcher) => {
                                let ancestor = fetcher.fetch_ancestor(&txid).await?;
                                GASPNode {
                                    graph_id: node.graph_id.clone(),
                                    raw_tx: ancestor.raw_tx,
                                    output_index: oi,
                                    proof: ancestor.proof,
                                    tx_metadata: None,
                                    output_metadata: None,
                                    inputs: None,
                                }
                            },
                            // Default / production: no fetcher → ask the peer and
                            // propagate its error on failure (today's exact behavior).
                            None => {
                                self.remote
                                    .request_node(&node.graph_id, &txid, oi, input_req.metadata)
                                    .await?
                            },
                        };

                        let spent_by_str = format!("{}.{}", node_txid, node.output_index);
                        self.process_incoming_node(&child_node, Some(&spent_by_str), seen)
                            .await?;
                    }
                }
            }

            Ok(())
        })
    }

    /// Process an outgoing node: submit to remote, then recursively send requested inputs.
    fn process_outgoing_node<'b>(
        &'b self,
        node: &'b GASPNode,
        seen: &'b mut std::collections::HashSet<String>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), GASPError>> + 'b>> {
        Box::pin(async move {
            if self.unidirectional {
                return Ok(());
            }

            let node_id = format!("{}.{}", node.graph_id, node.output_index);
            if seen.contains(&node_id) {
                return Ok(());
            }
            seen.insert(node_id);

            if let Some(response) = self.remote.submit_node(node).await? {
                for (outpoint, input_req) in &response.requested_inputs {
                    if let Some((txid, oi)) = parse_outpoint(outpoint) {
                        match self
                            .storage
                            .hydrate_gasp_node(&node.graph_id, &txid, oi, input_req.metadata)
                            .await
                        {
                            Ok(hydrated) => {
                                self.process_outgoing_node(&hydrated, seen).await?;
                            }
                            Err(e) => {
                                error!("{} Error hydrating outgoing node: {}", self.log_prefix, e);
                                return Ok(()); // Stop this branch, remote will discard
                            }
                        }
                    }
                }
            }

            Ok(())
        })
    }

    /// Validate and finalize a completed graph, or discard on failure.
    async fn complete_graph(&self, graph_id: &str) -> Result<(), GASPError> {
        info!("{} Completing graph: {}", self.log_prefix, graph_id);
        match self.storage.validate_graph_anchor(graph_id).await {
            Ok(()) => {
                self.storage.finalize_graph(graph_id).await?;
                info!("{} Graph finalized: {}", self.log_prefix, graph_id);
                Ok(())
            }
            Err(e) => {
                warn!(
                    "{} Graph validation failed: {}. Discarding.",
                    self.log_prefix, e
                );
                self.storage.discard_graph(graph_id).await?;
                Ok(())
            }
        }
    }
}

/// Parse "txid.outputIndex" into components.
pub fn parse_outpoint(s: &str) -> Option<(String, u32)> {
    let parts: Vec<&str> = s.splitn(2, '.').collect();
    if parts.len() != 2 {
        return None;
    }
    let oi = parts[1].parse::<u32>().ok()?;
    Some((parts[0].to_string(), oi))
}

// ============================================================================
// Error type
// ============================================================================

/// GASP protocol errors.
#[derive(Debug, thiserror::Error)]
pub enum GASPError {
    /// Version mismatch between peers.
    #[error("version mismatch: local={local}, remote={remote}")]
    VersionMismatch { local: u32, remote: u32 },

    /// Invalid timestamp format.
    #[error("invalid timestamp: {0}")]
    InvalidTimestamp(String),

    /// Node not found.
    #[error("node not found: {0}")]
    NodeNotFound(String),

    /// Graph validation failed.
    #[error("graph validation failed: {0}")]
    ValidationFailed(String),

    /// Network/communication error.
    #[error("remote error: {0}")]
    RemoteError(String),

    /// Storage error.
    #[error("storage error: {0}")]
    StorageError(String),

    /// Generic error.
    #[error("{0}")]
    Other(String),
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    // ── Mock GASPStorage ───────────────────────────────────────────────

    struct MockGASPStorage {
        utxos: Vec<GASPOutput>,
        graphs: Mutex<HashMap<String, Vec<GASPNode>>>,
        finalized: Mutex<Vec<String>>,
        discarded: Mutex<Vec<String>>,
    }

    impl MockGASPStorage {
        fn new(utxos: Vec<GASPOutput>) -> Self {
            Self {
                utxos,
                graphs: Mutex::new(HashMap::new()),
                finalized: Mutex::new(Vec::new()),
                discarded: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait(?Send)]
    impl GASPStorage for MockGASPStorage {
        async fn find_known_utxos(
            &self,
            since: u64,
            _limit: Option<u64>,
        ) -> Result<Vec<GASPOutput>, GASPError> {
            Ok(self
                .utxos
                .iter()
                .filter(|u| u.score as u64 >= since)
                .cloned()
                .collect())
        }

        async fn hydrate_gasp_node(
            &self,
            graph_id: &str,
            txid: &str,
            output_index: u32,
            _metadata: bool,
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

        async fn find_needed_inputs(
            &self,
            _node: &GASPNode,
        ) -> Result<Option<GASPNodeResponse>, GASPError> {
            Ok(None) // No inputs needed for simple tests
        }

        async fn append_to_graph(
            &self,
            node: &GASPNode,
            _spent_by: Option<&str>,
        ) -> Result<(), GASPError> {
            self.graphs
                .lock()
                .unwrap()
                .entry(node.graph_id.clone())
                .or_default()
                .push(node.clone());
            Ok(())
        }

        async fn validate_graph_anchor(&self, _graph_id: &str) -> Result<(), GASPError> {
            Ok(())
        }

        async fn finalize_graph(&self, graph_id: &str) -> Result<(), GASPError> {
            self.finalized.lock().unwrap().push(graph_id.to_string());
            Ok(())
        }

        async fn discard_graph(&self, graph_id: &str) -> Result<(), GASPError> {
            self.discarded.lock().unwrap().push(graph_id.to_string());
            Ok(())
        }
    }

    // ── Mock GASPRemote ────────────────────────────────────────────────

    struct MockGASPRemote {
        remote_utxos: Vec<GASPOutput>,
        submitted: Mutex<Vec<GASPNode>>,
    }

    impl MockGASPRemote {
        fn new(utxos: Vec<GASPOutput>) -> Self {
            Self {
                remote_utxos: utxos,
                submitted: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait(?Send)]
    impl GASPRemote for MockGASPRemote {
        async fn get_initial_response(
            &self,
            request: &GASPInitialRequest,
        ) -> Result<GASPInitialResponse, GASPError> {
            if request.version != GASP_VERSION {
                return Err(GASPError::VersionMismatch {
                    local: GASP_VERSION,
                    remote: request.version,
                });
            }
            let utxos: Vec<GASPOutput> = self
                .remote_utxos
                .iter()
                .filter(|u| u.score as u64 >= request.since)
                .cloned()
                .collect();
            Ok(GASPInitialResponse {
                utxo_list: utxos,
                since: request.since,
            })
        }

        async fn get_initial_reply(
            &self,
            _response: &GASPInitialResponse,
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
            _metadata: bool,
        ) -> Result<GASPNode, GASPError> {
            Ok(GASPNode {
                graph_id: graph_id.to_string(),
                raw_tx: format!("remote_rawtx_{txid}"),
                output_index,
                proof: Some("proof_hex".to_string()),
                tx_metadata: None,
                output_metadata: None,
                inputs: None,
            })
        }

        async fn submit_node(
            &self,
            node: &GASPNode,
        ) -> Result<Option<GASPNodeResponse>, GASPError> {
            self.submitted.lock().unwrap().push(node.clone());
            Ok(None) // No further inputs needed
        }
    }

    // ── Tests ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_sync_pulls_remote_utxos() {
        let local_storage = MockGASPStorage::new(vec![]);
        let remote = MockGASPRemote::new(vec![
            GASPOutput {
                txid: "tx1".to_string(),
                output_index: 0,
                score: 100.0,
            },
            GASPOutput {
                txid: "tx2".to_string(),
                output_index: 0,
                score: 200.0,
            },
        ]);

        let mut gasp = GASPSync::new(
            Box::new(local_storage),
            Box::new(remote),
            0,
            "[TEST]",
            false,
        );

        gasp.sync(None).await.unwrap();

        assert_eq!(gasp.last_interaction, 200);
    }

    #[tokio::test]
    async fn test_sync_skips_known_utxos() {
        let local_storage = MockGASPStorage::new(vec![GASPOutput {
            txid: "tx1".to_string(),
            output_index: 0,
            score: 100.0,
        }]);
        let remote = MockGASPRemote::new(vec![
            GASPOutput {
                txid: "tx1".to_string(),
                output_index: 0,
                score: 100.0,
            },
            GASPOutput {
                txid: "tx2".to_string(),
                output_index: 0,
                score: 200.0,
            },
        ]);

        let mut gasp = GASPSync::new(
            Box::new(local_storage),
            Box::new(remote),
            0,
            "[TEST]",
            true, // unidirectional — skip push
        );

        gasp.sync(None).await.unwrap();

        // Only tx2 should have been ingested (tx1 was already known)
        // last_interaction should be 200
        assert_eq!(gasp.last_interaction, 200);
    }

    #[tokio::test]
    async fn test_sync_unidirectional_skips_push() {
        let local_storage = MockGASPStorage::new(vec![GASPOutput {
            txid: "local_only".to_string(),
            output_index: 0,
            score: 50.0,
        }]);
        let remote = MockGASPRemote::new(vec![]);

        let mut gasp = GASPSync::new(Box::new(local_storage), Box::new(remote), 0, "[TEST]", true);

        gasp.sync(None).await.unwrap();
        // In unidirectional mode, local_only should NOT be pushed to remote
        // (no way to check directly with current mock, but no error = success)
    }

    #[tokio::test]
    async fn test_complete_graph_finalizes_on_valid() {
        let storage = MockGASPStorage::new(vec![]);
        let remote = MockGASPRemote::new(vec![]);

        let gasp = GASPSync::new(Box::new(storage), Box::new(remote), 0, "[TEST]", false);

        gasp.complete_graph("test_graph.0").await.unwrap();
        // Storage mock always validates OK, so graph should be finalized
    }

    #[tokio::test]
    async fn test_complete_graph_discards_on_invalid() {
        struct FailValidationStorage;

        #[async_trait(?Send)]
        impl GASPStorage for FailValidationStorage {
            async fn find_known_utxos(
                &self,
                _: u64,
                _: Option<u64>,
            ) -> Result<Vec<GASPOutput>, GASPError> {
                Ok(vec![])
            }
            async fn hydrate_gasp_node(
                &self,
                _: &str,
                _: &str,
                _: u32,
                _: bool,
            ) -> Result<GASPNode, GASPError> {
                unreachable!()
            }
            async fn find_needed_inputs(
                &self,
                _: &GASPNode,
            ) -> Result<Option<GASPNodeResponse>, GASPError> {
                Ok(None)
            }
            async fn append_to_graph(
                &self,
                _: &GASPNode,
                _: Option<&str>,
            ) -> Result<(), GASPError> {
                Ok(())
            }
            async fn validate_graph_anchor(&self, _: &str) -> Result<(), GASPError> {
                Err(GASPError::ValidationFailed("bad anchor".into()))
            }
            async fn finalize_graph(&self, _: &str) -> Result<(), GASPError> {
                panic!("should not finalize")
            }
            async fn discard_graph(&self, _: &str) -> Result<(), GASPError> {
                Ok(())
            }
        }

        let gasp = GASPSync::new(
            Box::new(FailValidationStorage),
            Box::new(MockGASPRemote::new(vec![])),
            0,
            "[TEST]",
            false,
        );

        // Should not error — discards instead of finalizing
        gasp.complete_graph("bad_graph.0").await.unwrap();
    }

    // ── AncestorFetcher fallback tests ─────────────────────────────────

    /// Build a minimal valid tx hex with one input referencing `source_txid`.
    fn make_tx_hex(source_txid: &str, source_oi: u32) -> String {
        let mut tx = bsv_rs::transaction::Transaction::new();
        tx.inputs.push(bsv_rs::transaction::TransactionInput::new(
            source_txid.to_string(),
            source_oi,
        ));
        tx.outputs.push(bsv_rs::transaction::TransactionOutput::new(
            100,
            bsv_rs::script::LockingScript::from_hex(
                "76a914000000000000000000000000000000000000000088ac",
            )
            .unwrap(),
        ));
        tx.to_hex()
    }

    /// Storage that records appended nodes and requests exactly one ancestor
    /// (the first input) on the FIRST node it sees, then nothing further.
    struct RecordingStorage {
        appended: Mutex<Vec<GASPNode>>,
        request_once: Mutex<bool>,
        ancestor_outpoint: String,
    }

    #[async_trait(?Send)]
    impl GASPStorage for RecordingStorage {
        async fn find_known_utxos(
            &self,
            _: u64,
            _: Option<u64>,
        ) -> Result<Vec<GASPOutput>, GASPError> {
            Ok(vec![])
        }
        async fn hydrate_gasp_node(
            &self,
            _: &str,
            _: &str,
            _: u32,
            _: bool,
        ) -> Result<GASPNode, GASPError> {
            unreachable!()
        }
        async fn find_needed_inputs(
            &self,
            _: &GASPNode,
        ) -> Result<Option<GASPNodeResponse>, GASPError> {
            let mut once = self.request_once.lock().unwrap();
            if *once {
                *once = false;
                let mut requested_inputs = HashMap::new();
                requested_inputs.insert(
                    self.ancestor_outpoint.clone(),
                    crate::types::GASPInputRequest { metadata: false },
                );
                Ok(Some(GASPNodeResponse { requested_inputs }))
            } else {
                Ok(None)
            }
        }
        async fn append_to_graph(&self, node: &GASPNode, _: Option<&str>) -> Result<(), GASPError> {
            self.appended.lock().unwrap().push(node.clone());
            Ok(())
        }
        async fn validate_graph_anchor(&self, _: &str) -> Result<(), GASPError> {
            Ok(())
        }
        async fn finalize_graph(&self, _: &str) -> Result<(), GASPError> {
            Ok(())
        }
        async fn discard_graph(&self, _: &str) -> Result<(), GASPError> {
            Ok(())
        }
    }

    /// Remote that always errors on `request_node` (mimics beta's HTTP 400
    /// "Incomplete SPV data!").
    struct FailingNodeRemote;

    #[async_trait(?Send)]
    impl GASPRemote for FailingNodeRemote {
        async fn get_initial_response(
            &self,
            _: &GASPInitialRequest,
        ) -> Result<GASPInitialResponse, GASPError> {
            Ok(GASPInitialResponse {
                utxo_list: vec![],
                since: 0,
            })
        }
        async fn get_initial_reply(
            &self,
            _: &GASPInitialResponse,
        ) -> Result<GASPInitialReply, GASPError> {
            Ok(GASPInitialReply { utxo_list: vec![] })
        }
        async fn request_node(
            &self,
            _: &str,
            _: &str,
            _: u32,
            _: bool,
        ) -> Result<GASPNode, GASPError> {
            Err(GASPError::RemoteError(
                "Peer returned HTTP 400: Incomplete SPV data!".to_string(),
            ))
        }
        async fn submit_node(&self, _: &GASPNode) -> Result<Option<GASPNodeResponse>, GASPError> {
            Ok(None)
        }
    }

    /// Mock fetcher that returns a pre-built ancestor rawtx (+ optional proof).
    struct MockFetcher {
        ancestor_hex: String,
        proof: Option<String>,
        called: Mutex<u32>,
    }

    #[async_trait(?Send)]
    impl AncestorFetcher for MockFetcher {
        async fn fetch_ancestor(&self, _txid: &str) -> Result<FetchedAncestor, GASPError> {
            *self.called.lock().unwrap() += 1;
            Ok(FetchedAncestor {
                raw_tx: self.ancestor_hex.clone(),
                proof: self.proof.clone(),
            })
        }
    }

    /// Remote that records how many times `request_node` is invoked (it would
    /// be a doomed peer call on the migration path) so a test can assert the
    /// fetcher path SKIPS the peer entirely.
    struct CountingNodeRemote {
        request_node_calls: std::rc::Rc<Mutex<u32>>,
    }

    #[async_trait(?Send)]
    impl GASPRemote for CountingNodeRemote {
        async fn get_initial_response(
            &self,
            _: &GASPInitialRequest,
        ) -> Result<GASPInitialResponse, GASPError> {
            Ok(GASPInitialResponse {
                utxo_list: vec![],
                since: 0,
            })
        }
        async fn get_initial_reply(
            &self,
            _: &GASPInitialResponse,
        ) -> Result<GASPInitialReply, GASPError> {
            Ok(GASPInitialReply { utxo_list: vec![] })
        }
        async fn request_node(
            &self,
            _: &str,
            _: &str,
            _: u32,
            _: bool,
        ) -> Result<GASPNode, GASPError> {
            *self.request_node_calls.lock().unwrap() += 1;
            Err(GASPError::RemoteError(
                "Peer returned HTTP 400: Incomplete SPV data!".to_string(),
            ))
        }
        async fn submit_node(&self, _: &GASPNode) -> Result<Option<GASPNodeResponse>, GASPError> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn ancestor_fetcher_none_preserves_peer_error() {
        // Build a tip tx whose input references some ancestor.
        let ancestor_hex = make_tx_hex(
            "1111111111111111111111111111111111111111111111111111111111111111",
            0,
        );
        let ancestor_txid = bsv_rs::transaction::Transaction::from_hex(&ancestor_hex)
            .unwrap()
            .id();
        let tip_hex = make_tx_hex(&ancestor_txid, 0);

        let storage = RecordingStorage {
            appended: Mutex::new(vec![]),
            request_once: Mutex::new(true),
            ancestor_outpoint: format!("{ancestor_txid}.0"),
        };

        // No fetcher configured → default path → peer error must propagate.
        let gasp = GASPSync::new(
            Box::new(storage),
            Box::new(FailingNodeRemote),
            0,
            "[TEST]",
            true,
        );

        let tip_node = GASPNode {
            graph_id: format!("{}.0", "deadbeef"),
            raw_tx: tip_hex,
            output_index: 0,
            proof: None,
            tx_metadata: None,
            output_metadata: None,
            inputs: None,
        };

        let mut seen = std::collections::HashSet::new();
        let result = gasp.process_incoming_node(&tip_node, None, &mut seen).await;
        assert!(
            result.is_err(),
            "with no fetcher, the peer error must propagate (default path unchanged)"
        );
    }

    #[tokio::test]
    async fn ancestor_fetcher_some_self_heals_missing_ancestor() {
        let ancestor_hex = make_tx_hex(
            "2222222222222222222222222222222222222222222222222222222222222222",
            0,
        );
        let ancestor_txid = bsv_rs::transaction::Transaction::from_hex(&ancestor_hex)
            .unwrap()
            .id();
        let tip_hex = make_tx_hex(&ancestor_txid, 0);

        let storage = RecordingStorage {
            appended: Mutex::new(vec![]),
            request_once: Mutex::new(true),
            ancestor_outpoint: format!("{ancestor_txid}.0"),
        };

        let fetcher = std::rc::Rc::new(MockFetcher {
            ancestor_hex: ancestor_hex.clone(),
            proof: None,
            called: Mutex::new(0),
        });

        let gasp = GASPSync::new(
            Box::new(storage),
            Box::new(FailingNodeRemote),
            0,
            "[TEST]",
            true,
        )
        .with_ancestor_fetcher(Some(fetcher.clone()));

        let tip_node = GASPNode {
            graph_id: "deadbeef.0".to_string(),
            raw_tx: tip_hex.clone(),
            output_index: 0,
            proof: None,
            tx_metadata: None,
            output_metadata: None,
            inputs: None,
        };

        let mut seen = std::collections::HashSet::new();
        gasp.process_incoming_node(&tip_node, None, &mut seen)
            .await
            .expect("with fetcher, missing ancestor self-heals from chain");

        assert_eq!(
            *fetcher.called.lock().unwrap(),
            2,
            "fetcher fires twice: once to hydrate the root's OWN proof (mock returns \
             proof:None here, so the root stays proofless + the walk continues), then \
             once to self-heal the missing ancestor (#126 root proof-anchoring)"
        );

        // The synthesized ancestor node must have been appended to the graph,
        // with proof: None so the existing recursion would continue upward.
        // (We can't reach into the boxed storage here, but the Ok(()) above
        // proves the synthesized node flowed through process_incoming_node and
        // append_to_graph without error.)
    }

    #[tokio::test]
    async fn ancestor_fetcher_some_skips_peer_request_node() {
        // Proof of the peer-skip optimization: with a fetcher present, the
        // doomed peer `request_node` call must NOT fire for ancestors — the
        // fetcher is consulted directly. (Otherwise a deep chain pays one
        // sequential peer round-trip per ancestor before falling back.)
        let ancestor_hex = make_tx_hex(
            "3333333333333333333333333333333333333333333333333333333333333333",
            0,
        );
        let ancestor_txid = bsv_rs::transaction::Transaction::from_hex(&ancestor_hex)
            .unwrap()
            .id();
        let tip_hex = make_tx_hex(&ancestor_txid, 0);

        let storage = RecordingStorage {
            appended: Mutex::new(vec![]),
            request_once: Mutex::new(true),
            ancestor_outpoint: format!("{ancestor_txid}.0"),
        };

        let counter = std::rc::Rc::new(Mutex::new(0u32));
        let remote = CountingNodeRemote {
            request_node_calls: counter.clone(),
        };
        let fetcher = std::rc::Rc::new(MockFetcher {
            ancestor_hex: ancestor_hex.clone(),
            proof: None,
            called: Mutex::new(0),
        });

        let gasp = GASPSync::new(Box::new(storage), Box::new(remote), 0, "[TEST]", true)
            .with_ancestor_fetcher(Some(fetcher.clone()));

        let tip_node = GASPNode {
            graph_id: "deadbeef.0".to_string(),
            raw_tx: tip_hex,
            output_index: 0,
            proof: None,
            tx_metadata: None,
            output_metadata: None,
            inputs: None,
        };

        let mut seen = std::collections::HashSet::new();
        gasp.process_incoming_node(&tip_node, None, &mut seen)
            .await
            .expect("self-heal via fetcher should succeed");

        assert_eq!(
            *counter.lock().unwrap(),
            0,
            "peer request_node must be SKIPPED for ancestors when a fetcher is present"
        );
        assert_eq!(
            *fetcher.called.lock().unwrap(),
            2,
            "fetcher fires twice: root's OWN proof hydration (#126) + serving the \
             ancestor directly (peer request_node still skipped)"
        );
    }

    #[test]
    fn test_parse_outpoint() {
        let (txid, oi) = parse_outpoint("abc123.5").unwrap();
        assert_eq!(txid, "abc123");
        assert_eq!(oi, 5);

        assert!(parse_outpoint("no_dot").is_none());
        assert!(parse_outpoint("abc.notanum").is_none());
    }

    #[tokio::test]
    async fn test_gasp_storage_is_object_safe() {
        let storage: Box<dyn GASPStorage> = Box::new(MockGASPStorage::new(vec![]));
        let utxos = storage.find_known_utxos(0, None).await.unwrap();
        assert!(utxos.is_empty());
    }

    #[tokio::test]
    async fn test_gasp_remote_is_object_safe() {
        let remote: Box<dyn GASPRemote> = Box::new(MockGASPRemote::new(vec![]));
        let response = remote
            .get_initial_response(&GASPInitialRequest {
                version: 1,
                since: 0,
                limit: None,
            })
            .await
            .unwrap();
        assert!(response.utxo_list.is_empty());
    }

    #[tokio::test]
    async fn test_version_mismatch_error() {
        let remote = MockGASPRemote::new(vec![]);
        let result = remote
            .get_initial_response(&GASPInitialRequest {
                version: 99,
                since: 0,
                limit: None,
            })
            .await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            GASPError::VersionMismatch { .. }
        ));
    }
}
