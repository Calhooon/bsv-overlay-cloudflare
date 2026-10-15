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
/// The most trait and network operations one backfill request runs ([`BACKFILL_REQUEST_MS`] gives the reasoning).
pub const BACKFILL_OPERATIONS: u64 = 500;
/// The most SQL statements one backfill request runs.
pub const BACKFILL_D1_QUERIES: u64 = 600;
/// The most sync slices one backfill request runs.
pub const BACKFILL_MAX_SLICES: u64 = 64;

/// The body of a sync or backfill request: the selection, and for a backfill the bound and where to resume.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyncRequest {
    /// The topics to sync; `None` selects every configured topic.
    pub topics: Option<Vec<String>>,
    /// The peers to sync with; `None` selects every peer.
    pub peers: Option<Vec<String>>,
    /// Walk to the peer's tip (a backfill); `None` is one ordinary sync.
    pub until: Option<Until>,
    /// The backfill's wall-clock budget in milliseconds, capped at [`BACKFILL_REQUEST_MS`].
    pub budget_ms: Option<u64>,
    /// The continuation token of an earlier backfill to resume.
    pub continuation: Option<String>,
}

/// How far a backfill walks.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Until {
    /// To the peer's tip.
    Tip,
}

impl SyncRequest {
    /// Parse and validate a request body; an empty body is the default request.
    pub fn parse(body: &str) -> Result<Self, String> {
        let request = if body.trim().is_empty() {
            Self::default()
        } else {
            serde_json::from_str(body).map_err(|e| e.to_string())?
        };
        request.validate()?;
        Ok(request)
    }

    /// The topics and peers the request selects.
    pub fn filter(&self) -> SyncFilter {
        SyncFilter {
            topics: self.topics.clone(),
            peers: self.peers.clone(),
        }
    }

    /// Refuse a request whose fields do not fit together: a budget or continuation without `until`, or a backfill that names other than one topic and one peer.
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

    /// The one topic and one peer a backfill names, each within its length bound.
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

/// Where a backfill stopped: its topic, its peer and the durable cursor, as an opaque token.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Continuation {
    version: u8,
    /// The topic the backfill walked.
    pub topic: String,
    /// The peer it walked.
    pub peer: String,
    /// The peer's durable cursor when the token was made.
    pub cursor: u64,
}

impl Continuation {
    /// The token for `topic` and `peer` at `cursor`.
    pub fn new(topic: &str, peer: &str, cursor: u64) -> Self {
        Self {
            version: 1,
            topic: topic.into(),
            peer: peer.into(),
            cursor,
        }
    }

    // bsv-stack-lean #134: four scalar fields cannot fail to serialize
    /// The token's JSON form.
    #[allow(clippy::expect_used)]
    pub fn encode(&self) -> String {
        serde_json::to_string(self).expect("continuation has only scalar fields")
    }

    /// Read a token, refusing one too long, malformed or of another version.
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

/// One peer's part of a sync: its cost, its slices and pages, and where its cursor ended.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PeerSyncResult {
    /// What the peer's sync cost.
    #[serde(flatten)]
    pub cost: SyncCost,
    /// Slices that moved the walk: a page passed or an output walked.
    pub slices: u64,
    /// Pages of the peer's UTXO list read.
    pub pages: u64,
    /// Whether the walk reached the peer's tip.
    pub reached_tip: bool,
    /// The peer's durable cursor after the sync.
    pub cursor: u64,
}

/// What a backfill request did: the merged sync result, how far it got, why it stopped and where to resume.
#[derive(Debug, Clone, Serialize)]
pub struct BackfillResult {
    /// The slices' merged sync result.
    #[serde(flatten)]
    pub sync: GASPSyncResult,
    /// Slices run.
    pub slices: u64,
    /// Whether the walk reached the peer's tip.
    pub reached_tip: bool,
    /// Why it stopped: `tip`, `wall_budget`, `request_allowance`, `no_progress`, `peer_wait` or `slice_allowance`.
    pub stop_reason: String,
    /// The token to resume from; `None` once the tip is reached.
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
    /// The peers asked one node at a time for the rest of this run (engine
    /// 0.2.1, #58), by peer: never persisted, so a peer that gains the batch
    /// route is asked for it on the next run.
    single_only: std::cell::RefCell<std::collections::HashMap<String, SingleOnly>>,
}
type SingleOnly = std::rc::Rc<std::cell::Cell<bool>>;
impl Run {
    pub fn new(fetcher: Option<std::rc::Rc<dyn crate::gasp::AncestorFetcher>>) -> Self {
        Self {
            fetcher: fetcher
                .map(|f| std::rc::Rc::new(crate::sync_fetcher::SyncFetcher::new(f)) as _),
            expired: std::cell::RefCell::default(),
            single_only: std::cell::RefCell::default(),
        }
    }
    pub fn single_only(&self, peer: &str) -> SingleOnly {
        self.single_only
            .borrow_mut()
            .entry(peer.to_string())
            .or_default()
            .clone()
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
                absent_struck,
                strikes_refused,
                dead_graphs,
                expired_swept,
                memo_folded
            );
            topic.root_bounds.append(&mut incoming.root_bounds);
            topic.peers_skipped.append(&mut incoming.peers_skipped);
            topic.errors.append(&mut incoming.errors);
            topic.route_faults.append(&mut incoming.route_faults);
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
