//! A batched node request between peers that both answer it (engine 0.2.1,
//! bsv-overlay-cloudflare #58).
//!
//! `POST /requestForeignGASPNodes` takes up to [`NODE_BATCH_MAX_OUTPOINTS`]
//! outpoints and answers their nodes in one body of at most
//! [`NODE_BATCH_MAX_BYTES`]. Each entry names its outpoint and carries either
//! the node, as `/requestForeignGASPNode` answers it, or a refusal word. The
//! route is ours: a reference peer answers it 404 and is asked one node at a
//! time, as before (`overlay-services@480d77a src/GASP/OverlayGASPRemote.ts`
//! has the single request only).
//!
//! What the sync does with an answer is in `GASPSync::prefetched_root`:
//! every entry feeds the same code a single answer feeds.

use crate::types::GASPNode;
use serde::{Deserialize, Serialize};

/// The most outpoints one batch request may name; a request over it is
/// answered 400 with this number in the body.
///
/// 100: the engine's page ([`crate::gasp::ENGINE_GASP_SYNC_PAGE_LIMIT`]), so
/// one body answers a page. The answering Worker reads one D1 row per
/// outpoint (`find_output`, one statement), so a full request costs 100 of
/// the 1,000 D1 queries a paid invocation allows (`backfill.rs`, the
/// platform's limits as cited there).
pub const NODE_BATCH_MAX_OUTPOINTS: usize = 100;

/// The most bytes one batch answer may carry, its JSON envelope included.
///
/// 4 MiB (4,194,304). A Worker's answer has no size limit of its own short
/// of the isolate's 128 MiB of memory (`backfill.rs`, the platform's limits
/// as cited there); both ends hold the body, its parse and the nodes made
/// from it at once, about three copies, so 4 MiB keeps one answer under a
/// tenth of the isolate. A `tm_uhrp` node is a raw transaction (the one
/// whose size is written down, 483 bytes: the 0.1.9 lane's brief) and at most a peer
/// proof of [`crate::beef_limits::PEER_PROOF_MAX_BYTES`] (4,095 bytes), in
/// hex with its keys about 9.4 KB, so a full request of 100 such nodes is
/// about 0.94 MB and the count binds; the bytes bind only for nodes above
/// about 41 KB on average. A node larger than the whole budget is answered
/// [`refusal::OVER_BUDGET`] and asked alone on the single route.
pub const NODE_BATCH_MAX_BYTES: usize = 4 * 1024 * 1024;

/// The words of a per-entry refusal.
pub mod refusal {
    /// The peer does not hold the outpoint: its word, counted by the absence
    /// rule exactly as a single request's `NodeNotFound` (an HTTP 400).
    pub const NOT_FOUND: &str = "not_found";
    /// The node alone is larger than [`super::NODE_BATCH_MAX_BYTES`].
    pub const OVER_BUDGET: &str = "over_budget";
    /// Anything else the single route would answer outside 2xx.
    pub const ERROR: &str = "error";
}

/// One outpoint of a batch request.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BatchOutpoint {
    pub txid: String,
    pub vout: u32,
}

impl BatchOutpoint {
    /// The `txid.vout` form the sync keys outputs by.
    pub fn key(&self) -> String {
        format!("{}.{}", self.txid, self.vout)
    }
}

/// The body of `POST /requestForeignGASPNodes`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeBatchRequest {
    pub outpoints: Vec<BatchOutpoint>,
}

/// One entry of a batch answer: the node, or a refusal word.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeBatchEntry {
    pub txid: String,
    pub vout: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<GASPNode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refused: Option<String>,
}

impl NodeBatchEntry {
    pub fn key(&self) -> String {
        format!("{}.{}", self.txid, self.vout)
    }
}

/// The answer of `POST /requestForeignGASPNodes`. An outpoint the byte
/// budget left out has no entry; the asker names it again in its next body.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeBatchAnswer {
    pub nodes: Vec<NodeBatchEntry>,
}

/// What a remote received for one batch request: the answer and the bytes
/// of its body, which the sync holds to [`NODE_BATCH_MAX_BYTES`].
#[derive(Debug, Clone, Default)]
pub struct NodeBatch {
    pub answer: NodeBatchAnswer,
    pub bytes: usize,
}

/// The bytes of `{"nodes":[]}`, the envelope of an answer.
const ENVELOPE_BYTES: usize = 12;

/// The answer's packer: entries in the asked order while they fit in
/// `budget` bytes, the commas between them counted. An entry larger than the
/// whole budget on its own is replaced by its `over_budget` refusal; the
/// entry that does not fit ends the answer, and the outpoints from it
/// on are left out.
pub(crate) struct Packer {
    budget: usize,
    used: usize,
    pub(crate) answer: NodeBatchAnswer,
}

impl Packer {
    pub(crate) fn new(budget: usize) -> Self {
        Self {
            budget,
            used: ENVELOPE_BYTES,
            answer: NodeBatchAnswer::default(),
        }
    }

    fn size(entry: &NodeBatchEntry) -> usize {
        serde_json::to_string(entry).map_or(usize::MAX, |s| s.len())
    }

    /// Add `entry`; `false` when the answer is full and packing must stop.
    pub(crate) fn push(&mut self, entry: NodeBatchEntry) -> bool {
        let comma = usize::from(!self.answer.nodes.is_empty());
        let mut entry = entry;
        let mut size = Self::size(&entry);
        if ENVELOPE_BYTES.saturating_add(size) > self.budget {
            entry = NodeBatchEntry {
                txid: entry.txid,
                vout: entry.vout,
                node: None,
                refused: Some(refusal::OVER_BUDGET.into()),
            };
            size = Self::size(&entry);
        }
        if self.used.saturating_add(comma).saturating_add(size) > self.budget {
            return false;
        }
        self.used += comma + size;
        self.answer.nodes.push(entry);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(hex_len: usize) -> GASPNode {
        GASPNode {
            graph_id: "aa.0".into(),
            raw_tx: "0".repeat(hex_len),
            output_index: 0,
            proof: None,
            tx_metadata: None,
            output_metadata: None,
            inputs: None,
        }
    }

    fn entry(vout: u32, hex_len: usize) -> NodeBatchEntry {
        NodeBatchEntry {
            txid: "aa".into(),
            vout,
            node: Some(node(hex_len)),
            refused: None,
        }
    }

    #[test]
    fn batch_021_the_packer_never_crosses_its_budget() {
        let budget = 1_000;
        let mut packer = Packer::new(budget);
        let mut count = 0;
        while packer.push(entry(count, 200)) {
            count += 1;
        }
        let body = serde_json::to_string(&packer.answer).unwrap();
        assert_eq!(body.len(), packer.used);
        assert!(body.len() <= budget, "{}", body.len());
        assert_eq!(count as usize, packer.answer.nodes.len());
        assert!(count >= 3, "{count}");
    }

    #[test]
    fn batch_021_a_node_over_the_whole_budget_is_refused_by_name() {
        let mut packer = Packer::new(1_000);
        assert!(packer.push(entry(0, 5_000)));
        assert!(packer.push(entry(1, 10)));
        let alone = &packer.answer.nodes[0];
        assert!(alone.node.is_none());
        assert_eq!(alone.refused.as_deref(), Some(refusal::OVER_BUDGET));
        assert!(packer.answer.nodes[1].node.is_some());
        let body = serde_json::to_string(&packer.answer).unwrap();
        assert_eq!(body.len(), packer.used);
    }

    #[test]
    fn batch_021_the_wire_shape() {
        let request: NodeBatchRequest =
            serde_json::from_str(r#"{"outpoints":[{"txid":"aa","vout":1}]}"#).unwrap();
        assert_eq!(request.outpoints[0].key(), "aa.1");
        assert!(serde_json::from_str::<NodeBatchRequest>(r#"{"outpoints":[],"x":1}"#).is_err());
        let refused = NodeBatchEntry {
            txid: "aa".into(),
            vout: 1,
            node: None,
            refused: Some(refusal::NOT_FOUND.into()),
        };
        assert_eq!(
            serde_json::to_string(&refused).unwrap(),
            r#"{"txid":"aa","vout":1,"refused":"not_found"}"#
        );
        let served = serde_json::to_value(entry(0, 2)).unwrap();
        assert_eq!(served["node"]["graphID"], "aa.0");
        assert_eq!(served["node"]["rawTx"], "00");
    }

    /// The derivation's arithmetic, so a change to either bound is seen: a
    /// full request of the largest `tm_uhrp` node the derivation names fits.
    #[test]
    fn batch_021_a_full_request_of_proven_uhrp_nodes_fits_the_budget() {
        let mut node = node(483 * 2);
        node.proof = Some("0".repeat(crate::beef_limits::PEER_PROOF_MAX_BYTES * 2));
        let mut packer = Packer::new(NODE_BATCH_MAX_BYTES);
        for vout in 0..NODE_BATCH_MAX_OUTPOINTS as u32 {
            assert!(packer.push(NodeBatchEntry {
                txid: "ab".repeat(32),
                vout,
                node: Some(node.clone()),
                refused: None,
            }));
        }
        let body = serde_json::to_string(&packer.answer).unwrap().len();
        assert!(body < 1_000_000, "{body}");
    }
}
