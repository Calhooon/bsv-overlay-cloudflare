//! Bounded admin backfills. Continuations select work; persisted cursors own progress.

use crate::{engine::GASPSyncResult, meter::SyncCost, sync_load::SyncFilter};
use serde::{Deserialize, Serialize};

/// A request stays within the existing 240 s sync belt with 60 s for finalization
/// and accounting. HTTP duration itself is unbounded while connected; paid Workers
/// default to 30 s CPU and 10,000 subrequests, with 128 MiB per isolate and six
/// simultaneous connections. Paid D1 allows 1,000 queries per invocation. Stop at
/// 600 measured SQL statements or 500 trait/network operations, leaving room for
/// schema startup, an in-flight guarded submit and bookkeeping.
/// These are conservative application bounds, not a CPU or SQL-row estimate.
/// Source: https://developers.cloudflare.com/workers/platform/limits/
/// Source: https://developers.cloudflare.com/d1/platform/limits/
pub const BACKFILL_REQUEST_MS: u64 = 180_000;
pub const BACKFILL_OPERATIONS: u64 = 500;
pub const BACKFILL_D1_QUERIES: u64 = 600;
pub const BACKFILL_MAX_SLICES: u64 = 64;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyncRequest {
    pub topics: Option<Vec<String>>,
    pub peers: Option<Vec<String>>,
    pub until: Option<Until>,
    pub budget_ms: Option<u64>,
    pub continuation: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Until {
    Tip,
}

impl SyncRequest {
    pub fn parse(body: &str) -> Result<Self, String> {
        let request = if body.trim().is_empty() {
            Self::default()
        } else {
            serde_json::from_str(body).map_err(|e| e.to_string())?
        };
        request.validate()?;
        Ok(request)
    }

    pub fn filter(&self) -> SyncFilter {
        SyncFilter {
            topics: self.topics.clone(),
            peers: self.peers.clone(),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.until.is_none() {
            if self.budget_ms.is_some() || self.continuation.is_some() {
                return Err("budget_ms and continuation require until: tip".into());
            }
            return Ok(());
        }
        let (topic, peer) = self.pair()?;
        if self.budget_ms.is_none_or(|n| n == 0) {
            return Err("until: tip requires a positive integer budget_ms".into());
        }
        if let Some(token) = &self.continuation {
            let token = Continuation::decode(token)?;
            if token.topic != topic || token.peer != peer {
                return Err("continuation belongs to another topic or peer".into());
            }
        }
        Ok(())
    }

    pub fn pair(&self) -> Result<(&str, &str), String> {
        match (self.topics.as_deref(), self.peers.as_deref()) {
            (Some([topic]), Some([peer]))
                if !topic.is_empty()
                    && topic.len() <= 256
                    && !peer.is_empty()
                    && peer.len() <= 1024 =>
            {
                Ok((topic, peer))
            }
            _ => Err(
                "until: tip requires one topic (1..256 bytes) and one peer (1..1024 bytes)".into(),
            ),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Continuation {
    version: u8,
    pub topic: String,
    pub peer: String,
    pub cursor: u64,
}

impl Continuation {
    pub fn new(topic: &str, peer: &str, cursor: u64) -> Self {
        Self {
            version: 1,
            topic: topic.into(),
            peer: peer.into(),
            cursor,
        }
    }

    pub fn encode(&self) -> String {
        serde_json::to_string(self).expect("continuation has only scalar fields")
    }

    pub fn decode(token: &str) -> Result<Self, String> {
        if token.len() > 16_384 {
            return Err("continuation exceeds 16384 bytes".into());
        }
        let token: Self = serde_json::from_str(token).map_err(|_| "invalid continuation")?;
        if token.version != 1 {
            return Err("unsupported continuation version".into());
        }
        Ok(token)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PeerSyncResult {
    #[serde(flatten)]
    pub cost: SyncCost,
    pub slices: u64,
    pub pages: u64,
    pub reached_tip: bool,
    pub cursor: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct BackfillResult {
    #[serde(flatten)]
    pub sync: GASPSyncResult,
    pub slices: u64,
    pub reached_tip: bool,
    pub stop_reason: String,
    pub continuation: Option<String>,
}

#[derive(Clone)]
pub(crate) struct Control {
    pub meter: crate::meter::LoadMeter,
    pub budget_ms: u64,
}

impl Control {
    pub fn remaining_ms(&self) -> u64 {
        self.budget_ms.saturating_sub(self.meter.snapshot().ms)
    }

    pub fn stop_reason(&self) -> Option<&'static str> {
        let cost = self.meter.snapshot();
        if cost.ms >= self.budget_ms {
            Some("wall_budget")
        } else if cost.d1_queries >= BACKFILL_D1_QUERIES
            || cost
                .calls_out
                .saturating_add(cost.storage_reads)
                .saturating_add(cost.storage_writes)
                >= BACKFILL_OPERATIONS
        {
            Some("request_allowance")
        } else {
            None
        }
    }
}

type ExpiredMemo = std::rc::Rc<std::cell::RefCell<std::collections::HashSet<String>>>;
pub(crate) struct Run {
    pub fetcher: Option<std::rc::Rc<dyn crate::gasp::AncestorFetcher>>,
    expired: std::cell::RefCell<std::collections::HashMap<String, ExpiredMemo>>,
}
impl Run {
    pub fn new(fetcher: Option<std::rc::Rc<dyn crate::gasp::AncestorFetcher>>) -> Self {
        Self {
            fetcher: fetcher
                .map(|f| std::rc::Rc::new(crate::sync_fetcher::SyncFetcher::new(f)) as _),
            expired: std::cell::RefCell::default(),
        }
    }
    pub fn expired(&self, topic: &str) -> ExpiredMemo {
        self.expired
            .borrow_mut()
            .entry(topic.to_string())
            .or_default()
            .clone()
    }
}

impl GASPSyncResult {
    pub(crate) fn append_slice(&mut self, slice: Self) {
        for topic in slice.topics_filtered {
            if !self.topics_filtered.contains(&topic) {
                self.topics_filtered.push(topic);
            }
        }
        for (name, mut incoming) in slice.topics_synced {
            let Some(topic) = self.topics_synced.get_mut(&name) else {
                self.topics_synced.insert(name, incoming);
                continue;
            };
            topic.cost.add(incoming.cost);
            macro_rules! sum {
                ($($field:ident),+ $(,)?) => { $(topic.$field = topic.$field.saturating_add(incoming.$field);)+ };
            }
            sum!(
                roots_proven,
                roots_unproven,
                expired_refused,
                pruned_inputs,
                discarded_graphs,
                anchor_walk_held_graphs,
                finalized_graphs,
                deadline_dropped_graphs,
                deferred_graphs,
                resumed_graphs,
                converged_graphs,
                stalled_graphs,
                held_back_graphs,
                backed_off_graphs,
                waiting_graphs,
                idle_counted,
                unanchorable_graphs,
                unanchorable_cleared,
                strikes_refused,
                dead_graphs,
                expired_swept,
                memo_folded
            );
            topic.root_bounds.append(&mut incoming.root_bounds);
            topic.peers_skipped.append(&mut incoming.peers_skipped);
            topic.errors.append(&mut incoming.errors);
            topic.cursor_moves.append(&mut incoming.cursor_moves);
            topic.dropped_graphs.append(&mut incoming.dropped_graphs);
            topic.cursor_held_by = incoming.cursor_held_by;
            for fault in incoming.faulted_roots {
                if !topic
                    .faulted_roots
                    .iter()
                    .any(|f| f.peer == fault.peer && f.outpoint == fault.outpoint)
                {
                    topic.faulted_roots.push(fault);
                }
            }
            for (peer, result) in incoming.peer_results {
                let held = topic.peer_results.entry(peer).or_default();
                held.cost.add(result.cost);
                held.slices += result.slices;
                held.pages += result.pages;
                held.reached_tip = result.reached_tip;
                held.cursor = result.cursor;
            }
        }
    }
}
