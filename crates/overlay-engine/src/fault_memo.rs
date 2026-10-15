//! Compact fault facts, separate from the partial graphs they never resume.

use crate::gasp::{DeferredGraph, DEAD_REASON, FAULT_RETRY_REASON, UNANCHORABLE_REASON};
use serde::{Deserialize, Serialize};

/// Twenty thousand compact facts per topic, as for the expired-outpoint memo.
/// Evict the oldest observation at capacity; losing a fact permits another
/// bounded walk, never an unverified admission. Partial-graph slots and bytes
/// do not constrain this memo.
pub const FAULT_MEMO_MAX_ROWS: usize = 20_000;

/// A sync offers at most this many due facts to its existing resume allowance.
/// Page lookups are separate, so a large retry backlog cannot hide a page fact.
pub const FAULT_MEMO_DUE_LIMIT: usize = 128;

#[derive(Debug, Clone, Serialize, Deserialize)]
// Persist the existing lifecycle flags independently, including passability.
/// One root's compact fault fact: what a deferred-graph fault record says, without its graph.
#[allow(clippy::struct_excessive_bools)]
pub struct FaultMemo {
    /// The peer the root was walked from.
    pub peer: String,
    /// The topic.
    pub topic: String,
    /// The root UTXO, `txid.outputIndex`.
    pub outpoint: String,
    /// The fault class of the walk that faulted.
    pub class: String,
    /// Consecutive runs whose walk of this root faulted.
    pub faults: u32,
    /// Retry reservations made after the unanchorable verdict.
    pub retries: u32,
    /// Unix seconds when the fact was observed, kept while its score holds.
    pub first_at: u64,
    /// Unix seconds before which the root is not walked again.
    pub next_at: u64,
    /// The root UTXO's score at the peer.
    pub score: u64,
    /// Whether the root is recorded dead.
    pub dead: bool,
    /// Whether the root is recorded unanchorable.
    pub unanchorable: bool,
    /// Whether the cursor may pass this root.
    pub cursor_passable: bool,
    /// The advert expiry the topic manager read from the root, if any.
    pub expiry: Option<u64>,
    /// Whether the peer is a configured one, not one `ls_ship` discovered.
    pub configured: bool,
}

impl FaultMemo {
    /// The memo of a fault record observed at `observed_at`; `None` for a record that is not a fault.
    pub fn from_record(record: &DeferredGraph, observed_at: u64) -> Option<Self> {
        Some(Self {
            peer: record.peer.clone(),
            topic: record.topic.clone(),
            outpoint: record.outpoint.clone(),
            class: record.fault_class()?.to_string(),
            faults: record.faults,
            retries: record.unanchorable_retries,
            first_at: observed_at,
            next_at: record.retry_at,
            score: record.score,
            dead: record.is_dead(),
            unanchorable: record.is_unanchorable(),
            cursor_passable: record.cursor_passable,
            expiry: record.expiry,
            configured: record.configured,
        })
    }

    /// The walk's existing lifecycle representation. No graph bytes are stored.
    pub fn into_record(self) -> DeferredGraph {
        let prefix = if self.dead {
            DEAD_REASON
        } else if self.unanchorable {
            UNANCHORABLE_REASON
        } else {
            FAULT_RETRY_REASON
        };
        DeferredGraph {
            peer: self.peer,
            topic: self.topic,
            outpoint: self.outpoint,
            score: self.score,
            nodes: Vec::new(),
            pending: Vec::new(),
            calls: 0,
            passes: 0,
            reason: format!("{prefix}{}", self.class),
            configured: self.configured,
            idle_faults: 0,
            cursor_passable: self.cursor_passable,
            faults: self.faults,
            retry_at: self.next_at,
            idle_resumes: 0,
            unanchorable_retries: self.retries,
            expiry: self.expiry,
        }
    }
}

/// The fault memos a read returned, with what the read folded and swept.
#[derive(Debug, Default)]
pub struct FaultMemoBatch {
    /// The memos, one per root.
    pub records: Vec<FaultMemo>,
    /// Legacy fault records the read folded into the memo.
    pub folded: u64,
    /// Expired memos the read removed.
    pub swept: u64,
}
