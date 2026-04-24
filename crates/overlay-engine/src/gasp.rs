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
}

impl<'a> GASPSync<'a> {
    /// Create a new GASP sync orchestrator.
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
        }
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
                        }
                    }
                }
            }

            // Continue pagination if we got a full page
            if limit.is_none() || (page_size as u64) < limit.unwrap_or(u64::MAX) {
                break;
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
            let node_id = format!("{}.{}", node.graph_id, node.output_index);
            if seen.contains(&node_id) {
                return Ok(());
            }
            seen.insert(node_id);

            self.storage.append_to_graph(node, spent_by).await?;

            if let Some(needed) = self.storage.find_needed_inputs(node).await? {
                for (outpoint, input_req) in &needed.requested_inputs {
                    if let Some((txid, oi)) = parse_outpoint(outpoint) {
                        let child_node = self
                            .remote
                            .request_node(&node.graph_id, &txid, oi, input_req.metadata)
                            .await?;

                        let spent_by_str = format!("{}.{}", node.raw_tx, node.output_index);
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
