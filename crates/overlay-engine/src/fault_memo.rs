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
#[allow(clippy::struct_excessive_bools)]
pub struct FaultMemo {
    pub peer: String,
    pub topic: String,
    pub outpoint: String,
    pub class: String,
    pub faults: u32,
    pub retries: u32,
    pub first_at: u64,
    pub next_at: u64,
    pub score: u64,
    pub dead: bool,
    pub unanchorable: bool,
    pub cursor_passable: bool,
    pub expiry: Option<u64>,
    pub configured: bool,
}

impl FaultMemo {
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

#[derive(Debug, Default)]
pub struct FaultMemoBatch {
    pub records: Vec<FaultMemo>,
    pub folded: u64,
    pub swept: u64,
}
