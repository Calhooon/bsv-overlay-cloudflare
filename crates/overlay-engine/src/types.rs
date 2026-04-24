//! Core types for the BSV Overlay Services Engine.
//!
//! Types that already exist in `bsv::overlay::types` (Protocol, LookupQuestion, LookupAnswer,
//! TaggedBEEF, AdmittanceInstructions, Steak, ServiceMetadata, etc.) are re-exported.
//! This module defines Engine-specific types not present in the SDK.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// Re-export SDK overlay types so consumers only need `overlay_engine::types::*`
pub use bsv_rs::overlay::types::{
    AdmittanceInstructions, HostResponse, LookupAnswer, LookupAnswerType, LookupFormula,
    LookupQuestion, NetworkPreset, OutputListItem, Protocol, ServiceMetadata, Steak, TaggedBEEF,
};

// ============================================================================
// Outpoint
// ============================================================================

/// A transaction outpoint — txid + output index.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Outpoint {
    pub txid: String,
    #[serde(rename = "outputIndex")]
    pub output_index: u32,
}

impl Outpoint {
    pub fn new(txid: impl Into<String>, output_index: u32) -> Self {
        Self {
            txid: txid.into(),
            output_index,
        }
    }

    /// Parse from "txid.outputIndex" format used by GASP graphIDs.
    pub fn from_graph_id(graph_id: &str) -> Option<Self> {
        let parts: Vec<&str> = graph_id.splitn(2, '.').collect();
        if parts.len() != 2 {
            return None;
        }
        let output_index = parts[1].parse::<u32>().ok()?;
        Some(Self {
            txid: parts[0].to_string(),
            output_index,
        })
    }

    /// Format as "txid.outputIndex" for GASP graphIDs.
    pub fn to_graph_id(&self) -> String {
        format!("{}.{}", self.txid, self.output_index)
    }
}

impl std::fmt::Display for Outpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.txid, self.output_index)
    }
}

// ============================================================================
// Output — tracked UTXO in the overlay
// ============================================================================

/// A UTXO tracked by the Overlay Services Engine.
///
/// Ported from `@bsv/overlay` Output interface.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Output {
    /// Transaction ID containing this output.
    pub txid: String,

    /// Index of this output within the transaction.
    #[serde(rename = "outputIndex")]
    pub output_index: u32,

    /// Locking script bytes.
    #[serde(rename = "outputScript")]
    pub output_script: Vec<u8>,

    /// Satoshi value.
    pub satoshis: u64,

    /// Topic this output belongs to.
    pub topic: String,

    /// Whether this output has been spent.
    pub spent: bool,

    /// Outpoints consumed by the transaction that created this output (its inputs).
    #[serde(rename = "outputsConsumed", default)]
    pub outputs_consumed: Vec<Outpoint>,

    /// Outpoints of transactions that later spent this output.
    #[serde(rename = "consumedBy", default)]
    pub consumed_by: Vec<Outpoint>,

    /// BEEF-encoded transaction data (optional, loaded on demand).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub beef: Option<Vec<u8>>,

    /// Block height if the transaction is mined.
    #[serde(rename = "blockHeight", skip_serializing_if = "Option::is_none")]
    pub block_height: Option<u32>,

    /// Score for GASP pagination (typically a timestamp or sequence number).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
}

impl Output {
    /// Get this output's outpoint.
    pub fn outpoint(&self) -> Outpoint {
        Outpoint::new(&self.txid, self.output_index)
    }
}

// ============================================================================
// Advertisement
// ============================================================================

/// A SHIP or SLAP overlay advertisement.
///
/// Represents a service node's announcement that it hosts a particular topic (SHIP)
/// or provides a particular lookup service (SLAP).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Advertisement {
    /// Protocol type (SHIP or SLAP).
    pub protocol: Protocol,

    /// Identity key of the advertising node (66-char hex compressed pubkey).
    #[serde(rename = "identityKey")]
    pub identity_key: String,

    /// Domain/URL where the service is hosted.
    pub domain: String,

    /// Topic name (for SHIP, e.g. "tm_mycoin") or service name (for SLAP, e.g. "ls_myservice").
    #[serde(rename = "topicOrService")]
    pub topic_or_service: String,

    /// BEEF transaction containing the advertisement output (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub beef: Option<Vec<u8>>,

    /// Output index within the advertisement transaction (optional).
    #[serde(rename = "outputIndex", skip_serializing_if = "Option::is_none")]
    pub output_index: Option<u32>,
}

/// Data needed to create a new advertisement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvertisementData {
    /// Protocol type (SHIP or SLAP).
    pub protocol: Protocol,

    /// Topic or service name to advertise.
    #[serde(rename = "topicOrServiceName")]
    pub topic_or_service_name: String,
}

// ============================================================================
// SyncConfiguration
// ============================================================================

/// Configuration for topic synchronization via GASP.
///
/// Maps topic names to their sync targets:
/// - `SyncTarget::Ship` — discover peers dynamically via SHIP protocol
/// - `SyncTarget::Peers(urls)` — sync with specific hardcoded peer URLs
/// - `SyncTarget::Disabled` — no sync for this topic
pub type SyncConfiguration = HashMap<String, SyncTarget>;

/// Sync target for a topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SyncTarget {
    /// Use SHIP protocol to discover peers dynamically.
    Ship,
    /// Sync with specific peer URLs.
    Peers(Vec<String>),
    /// Sync disabled for this topic.
    Disabled,
}

// ============================================================================
// Submit mode
// ============================================================================

/// Mode for transaction submission to the Engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum SubmitMode {
    /// Current transaction — broadcast to network + SPV verification.
    #[default]
    #[serde(rename = "current-tx")]
    CurrentTx,
    /// Historical transaction — no broadcast, SPV verified.
    #[serde(rename = "historical-tx")]
    HistoricalTx,
    /// Historical transaction — no broadcast, no SPV (for GASP sync).
    #[serde(rename = "historical-tx-no-spv")]
    HistoricalTxNoSpv,
}

// ============================================================================
// Lookup Service payload types
// ============================================================================

/// How the Engine delivers data to a LookupService on output admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdmissionMode {
    /// Provide only the locking script, txid, outputIndex, satoshis, topic.
    #[serde(rename = "locking-script")]
    LockingScript,
    /// Provide the entire Atomic BEEF transaction.
    #[serde(rename = "whole-tx")]
    WholeTx,
}

/// How the Engine notifies a LookupService when an output is spent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SpendNotificationMode {
    /// No spend notification.
    #[serde(rename = "none")]
    None,
    /// Provide only the spending txid.
    #[serde(rename = "txid")]
    Txid,
    /// Provide the spending txid + unlocking script + input details.
    #[serde(rename = "script")]
    Script,
    /// Provide the entire spending transaction as Atomic BEEF.
    #[serde(rename = "whole-tx")]
    WholeTx,
}

/// Payload delivered to LookupService::output_admitted_by_topic().
#[derive(Debug, Clone)]
pub enum OutputAdmittedByTopic {
    /// Locking-script mode — provides parsed output fields.
    LockingScript {
        txid: String,
        output_index: u32,
        topic: String,
        satoshis: u64,
        locking_script: Vec<u8>,
        off_chain_values: Option<Vec<u8>>,
    },
    /// Whole-tx mode — provides the entire Atomic BEEF.
    WholeTx {
        atomic_beef: Vec<u8>,
        output_index: u32,
        topic: String,
        off_chain_values: Option<Vec<u8>>,
    },
}

/// Payload delivered to LookupService::output_spent().
#[derive(Debug, Clone)]
pub enum OutputSpent {
    /// No details — just that it was spent.
    None {
        txid: String,
        output_index: u32,
        topic: String,
    },
    /// Spending txid provided.
    Txid {
        txid: String,
        output_index: u32,
        topic: String,
        spending_txid: String,
    },
    /// Spending txid + unlocking script + input details.
    Script {
        txid: String,
        output_index: u32,
        topic: String,
        spending_txid: String,
        input_index: u32,
        unlocking_script: Vec<u8>,
        sequence_number: u32,
        off_chain_values: Option<Vec<u8>>,
    },
    /// Full spending transaction as Atomic BEEF.
    WholeTx {
        txid: String,
        output_index: u32,
        topic: String,
        spending_atomic_beef: Vec<u8>,
        off_chain_values: Option<Vec<u8>>,
    },
}

// ============================================================================
// GASP types
// ============================================================================

/// Initial request in the GASP (Graph Aware Sync Protocol).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GASPInitialRequest {
    /// GASP protocol version. Currently 1.
    pub version: u32,
    /// Unix timestamp (seconds) of last sync between these two parties.
    pub since: u64,
    /// Maximum number of UTXOs to return.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
}

/// A single output in a GASP sync response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GASPOutput {
    /// Transaction ID.
    pub txid: String,
    /// Output index.
    #[serde(rename = "outputIndex")]
    pub output_index: u32,
    /// Score/timestamp for this output (for pagination).
    pub score: f64,
}

/// Initial response in the GASP protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GASPInitialResponse {
    /// List of UTXOs witnessed since the request's timestamp.
    #[serde(rename = "UTXOList")]
    pub utxo_list: Vec<GASPOutput>,
    /// Timestamp from which the responder wants UTXOs back.
    pub since: u64,
}

/// Reply to the initial response (UTXOs the requester sends back).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GASPInitialReply {
    /// UTXOs not already in the initial response, after the response's since timestamp.
    #[serde(rename = "UTXOList")]
    pub utxo_list: Vec<GASPOutput>,
}

/// A node in a GASP transaction graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GASPNode {
    /// Graph ID this node belongs to ("txid.outputIndex" format).
    #[serde(rename = "graphID")]
    pub graph_id: String,
    /// Raw transaction hex.
    #[serde(rename = "rawTx")]
    pub raw_tx: String,
    /// Output index in the transaction.
    #[serde(rename = "outputIndex")]
    pub output_index: u32,
    /// BUMP merkle proof hex (if transaction is mined).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proof: Option<String>,
    /// Transaction metadata hex (if requested).
    #[serde(rename = "txMetadata", skip_serializing_if = "Option::is_none")]
    pub tx_metadata: Option<String>,
    /// Output metadata hex (if requested).
    #[serde(rename = "outputMetadata", skip_serializing_if = "Option::is_none")]
    pub output_metadata: Option<String>,
    /// Input transaction references with metadata hashes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inputs: Option<HashMap<String, GASPInputRef>>,
}

/// Reference to an input transaction in a GASP graph node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GASPInputRef {
    /// Hash of the input's metadata.
    pub hash: String,
}

/// Response indicating which inputs are needed to complete a GASP graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GASPNodeResponse {
    /// Map of txid → whether metadata is requested for that input.
    #[serde(rename = "requestedInputs")]
    pub requested_inputs: HashMap<String, GASPInputRequest>,
}

/// Request for a specific input in GASP sync.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GASPInputRequest {
    /// Whether metadata should be provided for this input.
    pub metadata: bool,
}

/// Applied transaction record (deduplication tracking).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppliedTransaction {
    pub txid: String,
    pub topic: String,
}

// ============================================================================
// UTXO Reference (used by SHIP/SLAP lookup responses)
// ============================================================================

/// A minimal reference to a UTXO — just txid and outputIndex.
/// Returned by SHIP/SLAP lookup services.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UTXOReference {
    pub txid: String,
    #[serde(rename = "outputIndex")]
    pub output_index: u32,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_outpoint_graph_id_roundtrip() {
        let op = Outpoint::new("abc123", 2);
        let graph_id = op.to_graph_id();
        assert_eq!(graph_id, "abc123.2");
        let parsed = Outpoint::from_graph_id(&graph_id).unwrap();
        assert_eq!(parsed, op);
    }

    #[test]
    fn test_outpoint_from_graph_id_invalid() {
        assert!(Outpoint::from_graph_id("no_dot").is_none());
        assert!(Outpoint::from_graph_id("abc.notanum").is_none());
    }

    #[test]
    fn test_output_serde_roundtrip() {
        let output = Output {
            txid: "deadbeef".to_string(),
            output_index: 0,
            output_script: vec![0x76, 0xa9],
            satoshis: 1000,
            topic: "tm_test".to_string(),
            spent: false,
            outputs_consumed: vec![Outpoint::new("prev", 1)],
            consumed_by: vec![],
            beef: None,
            block_height: Some(800000),
            score: Some(1.0),
        };
        let json = serde_json::to_string(&output).unwrap();
        let back: Output = serde_json::from_str(&json).unwrap();
        assert_eq!(back.txid, "deadbeef");
        assert_eq!(back.output_index, 0);
        assert_eq!(back.satoshis, 1000);
        assert_eq!(back.topic, "tm_test");
        assert!(!back.spent);
        assert_eq!(back.outputs_consumed.len(), 1);
        assert_eq!(back.block_height, Some(800000));
    }

    #[test]
    fn test_advertisement_serde() {
        let ad = Advertisement {
            protocol: Protocol::Ship,
            identity_key: "02abc".to_string(),
            domain: "https://example.com".to_string(),
            topic_or_service: "tm_test".to_string(),
            beef: None,
            output_index: None,
        };
        let json = serde_json::to_string(&ad).unwrap();
        assert!(json.contains("SHIP"));
        let back: Advertisement = serde_json::from_str(&json).unwrap();
        assert_eq!(back.protocol, Protocol::Ship);
        assert_eq!(back.topic_or_service, "tm_test");
    }

    #[test]
    fn test_submit_mode_serde() {
        let json = serde_json::to_string(&SubmitMode::HistoricalTxNoSpv).unwrap();
        assert_eq!(json, "\"historical-tx-no-spv\"");
        let back: SubmitMode = serde_json::from_str(&json).unwrap();
        assert_eq!(back, SubmitMode::HistoricalTxNoSpv);
    }

    #[test]
    fn test_gasp_initial_request_serde() {
        let req = GASPInitialRequest {
            version: 1,
            since: 1700000000,
            limit: Some(10000),
        };
        let json = serde_json::to_string(&req).unwrap();
        let back: GASPInitialRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(back.version, 1);
        assert_eq!(back.since, 1700000000);
        assert_eq!(back.limit, Some(10000));
    }

    #[test]
    fn test_gasp_node_serde() {
        let node = GASPNode {
            graph_id: "abc.0".to_string(),
            raw_tx: "0100000001".to_string(),
            output_index: 0,
            proof: None,
            tx_metadata: None,
            output_metadata: None,
            inputs: None,
        };
        let json = serde_json::to_string(&node).unwrap();
        assert!(json.contains("graphID"));
        assert!(json.contains("rawTx"));
        let back: GASPNode = serde_json::from_str(&json).unwrap();
        assert_eq!(back.graph_id, "abc.0");
    }

    #[test]
    fn test_utxo_reference_serde() {
        let r = UTXOReference {
            txid: "dead".to_string(),
            output_index: 3,
        };
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("outputIndex"));
        let back: UTXOReference = serde_json::from_str(&json).unwrap();
        assert_eq!(back.output_index, 3);
    }

    #[test]
    fn test_admission_mode_serde() {
        let json = serde_json::to_string(&AdmissionMode::LockingScript).unwrap();
        assert_eq!(json, "\"locking-script\"");
    }

    #[test]
    fn test_spend_notification_mode_serde() {
        let json = serde_json::to_string(&SpendNotificationMode::WholeTx).unwrap();
        assert_eq!(json, "\"whole-tx\"");
    }
}
