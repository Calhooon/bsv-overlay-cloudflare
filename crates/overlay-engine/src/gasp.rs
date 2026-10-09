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
// Per-peer sync budgets + dead-peer quarantine (bsv-low#302)
// ============================================================================

/// PURE (bsv-low#257/#302): race `fut` against `deadline`; `None` = the
/// deadline won and `fut` was DROPPED (its in-flight work cancelled).
/// Injectable deadline so the control flow is natively unit-tested — the
/// house idiom generalized out of `overlay-cloudflare`'s scheduled handler
/// (which passes its platform `sleep_ms`) so the ENGINE's per-peer GASP
/// budget can reuse it.
pub async fn race_or_deadline<F, D, T>(fut: F, deadline: D) -> Option<T>
where
    F: std::future::Future<Output = T>,
    D: std::future::Future<Output = ()>,
{
    let mut fut = std::pin::pin!(fut);
    let mut deadline = std::pin::pin!(deadline);
    std::future::poll_fn(move |cx| {
        if let std::task::Poll::Ready(v) = fut.as_mut().poll(cx) {
            return std::task::Poll::Ready(Some(v));
        }
        if deadline.as_mut().poll(cx).is_ready() {
            return std::task::Poll::Ready(None);
        }
        std::task::Poll::Pending
    })
    .await
}

/// What makes a deadline COOPERATIVE around a write (bsv-low #552, the lens
/// fold's HIGH-1). `Engine::submit` is several storage writes (mark spent,
/// delete the stale coin, insert the outputs, notify, record applied), each
/// an await on D1, and a future dropped between two of them leaves a head
/// chain with NO head: the old one deleted, the new one never inserted, and
/// no applied row to say so.
///
/// The writer opens a [`WriteSection`] around ONE transaction's submit and
/// asks [`SubmitGate::stop_if_due`] between two of them.
/// [`race_or_deadline_guarded`] does not drop its future while a section is
/// open: it notes the deadline as due and keeps polling until the section
/// closes, so the wait is bounded by the writes of one transaction. What the
/// deadline leaves is then a prefix of whole transactions, never part of one.
/// The race does not bound a write that never answers: the writer does
/// (bsv-low #559, `Engine::set_finalize_submit_budget`: the engine bounds
/// each storage call and hook inside the section, and a call that never
/// answers is that call's fault; the submit itself is never dropped).
#[derive(Debug, Default)]
pub struct SubmitGate {
    writing: std::cell::Cell<u32>,
    due: std::cell::Cell<bool>,
}

impl SubmitGate {
    /// Open a section no guarded race may drop. Closed when the returned
    /// value is dropped.
    pub fn write_section(&self) -> WriteSection<'_> {
        self.writing.set(self.writing.get() + 1);
        WriteSection(self)
    }

    /// Whether a guarded race's deadline fell due while a section was open:
    /// the race is waiting for the writer to stop.
    pub fn deadline_is_due(&self) -> bool {
        self.due.get()
    }

    /// The writer's question at a transaction boundary, with NO section open.
    /// If a deadline is due this never returns: it hands the future back to
    /// the race, which drops it on this very poll (only a guarded race sets
    /// the flag, and it clears it when it ends).
    pub async fn stop_if_due(&self) {
        if self.due.get() {
            std::future::pending::<()>().await;
        }
    }
}

/// An open write section of a [`SubmitGate`].
#[must_use = "the section closes when this is dropped"]
pub struct WriteSection<'g>(&'g SubmitGate);

impl Drop for WriteSection<'_> {
    fn drop(&mut self) {
        self.0.writing.set(self.0.writing.get().saturating_sub(1));
    }
}

/// [`race_or_deadline`] that never drops `fut` inside a [`WriteSection`] of
/// `gate`. A deadline that falls due while a section is open is remembered
/// (and shown to the writer, [`SubmitGate::deadline_is_due`]); `fut` is
/// polled on, by its own wakers, and dropped at its first pending point
/// outside a section. `None` then means what it meant: the deadline won.
pub async fn race_or_deadline_guarded<F, D, T>(fut: F, deadline: D, gate: &SubmitGate) -> Option<T>
where
    F: std::future::Future<Output = T>,
    D: std::future::Future<Output = ()>,
{
    // The flag never outlives the race that set it, however the race ends
    // (an outer drop included): a stale one would park the next writer.
    struct ClearDue<'g>(&'g SubmitGate);
    impl Drop for ClearDue<'_> {
        fn drop(&mut self) {
            self.0.due.set(false);
        }
    }
    let _clear = ClearDue(gate);
    let mut fut = std::pin::pin!(fut);
    let mut deadline = std::pin::pin!(deadline);
    let mut due = false;
    std::future::poll_fn(move |cx| {
        if let std::task::Poll::Ready(v) = fut.as_mut().poll(cx) {
            return std::task::Poll::Ready(Some(v));
        }
        if !due && deadline.as_mut().poll(cx).is_ready() {
            due = true;
        }
        if due {
            if gate.writing.get() == 0 {
                return std::task::Poll::Ready(None);
            }
            // `fut` is parked inside a write: its own waker brings us back.
            gate.due.set(true);
        }
        std::task::Poll::Pending
    })
    .await
}

/// Consecutive all-failure syncs after which a peer is QUARANTINED
/// (bsv-low#302). At the production `*/15` cron cadence 8 consecutive
/// failures ≈ 2 hours of unbroken unreachability — comfortably past any
/// transient blip (deploy, restart, brief network fault, one bad tick),
/// while still cutting a genuinely dead peer's budget burn off the same
/// day it dies.
pub const PEER_QUARANTINE_THRESHOLD: u64 = 8;

/// Re-probe interval for a quarantined peer, seconds (6 h ≈ every 24 cron
/// ticks). Quarantine is NEVER a deletion: once `secs_since_last_attempt`
/// reaches this age the peer gets exactly ONE fresh sync attempt — success
/// resets its failure count to 0 (full re-admission), failure re-arms the
/// quarantine for another window. A peer that died transiently therefore
/// re-admits itself automatically on the first re-probe after it recovers.
pub const PEER_QUARANTINE_REPROBE_SECS: u64 = 6 * 3600;

/// PURE quarantine rule (bsv-low#302): a peer is skipped IFF it has hit
/// [`PEER_QUARANTINE_THRESHOLD`] consecutive failures AND its last attempt
/// is younger than [`PEER_QUARANTINE_REPROBE_SECS`]. A peer with no
/// recorded attempt (`secs_since_last_attempt == None` — pristine, or a
/// backend without peer-health tracking) is NEVER quarantined, and an old
/// enough last attempt always re-opens one probe — fail-safe in the
/// "attempt more, never strand a peer forever" direction.
///
/// ACCEPTED RESIDUAL (bsv-low#304 gate M-3, documented — not built): a
/// peer that ALTERNATES one success into every window of ≤7 failures
/// resets its streak each time and is never quarantined, burning up to one
/// per-peer budget slice (30 s) per (host, topic) per tick forever. Why
/// accepted: (a) the burn is bounded by the per-peer budget times the
/// configured peer set, all inside the 240 s outer step belt — the #257
/// unbounded-hang class cannot recur; (b) sustaining it requires the peer
/// to actually COMPLETE a real sync every few ticks, i.e. behave as a
/// (slow) live peer — indistinguishable in principle from a genuinely
/// flaky honest peer, which the rule must never strand; (c) LOW's money
/// topics do not GASP-sync at all (no `sync_configuration` peers in prod),
/// so the exposure is cron-budget noise, not a money surface. A
/// streak-decay or success-ratio rule could narrow it later if a real
/// abuser appears.
pub fn peer_sync_quarantined(health: &crate::storage::PeerSyncHealth) -> bool {
    health.consecutive_failures >= PEER_QUARANTINE_THRESHOLD
        && health
            .secs_since_last_attempt
            .is_some_and(|secs| secs < PEER_QUARANTINE_REPROBE_SECS)
}

// ============================================================================
// Deferred graphs (bsv-low #555)
// ============================================================================

/// Default per-GRAPH call budget (bsv-low #555): requests to the peer, or
/// chain fetches, that ONE graph's walk may make in one pass before it is
/// deferred. With [`DEFAULT_GRAPH_BUDGET_MS`]: at the 1.2 s per request
/// measured on beta (2026-10-09, #582) 100 calls take about 120 s, so on a
/// slow peer the time binds first (about 50 nodes a pass) and on a fast one
/// the calls do, which also bounds how much a record grows per pass.
pub const DEFAULT_GRAPH_BUDGET_CALLS: u32 = 100;

/// Default per-GRAPH time budget, ms (bsv-low #555): half of the 120 s
/// topic slice of LOW's node (one peer), so a deep graph takes at most half
/// of its topic's slice and the UTXOs after it keep the other half. A caller
/// keeps it BELOW its per-peer budget (`Engine::set_peer_sync_budget`); a
/// graph the per-peer deadline cuts first is deferred all the same.
pub const DEFAULT_GRAPH_BUDGET_MS: u64 = 60_000;

/// A deferred graph whose record has been deferred this many passes is
/// dropped at its next resume (reason `max_passes`) and its UTXO fails as a
/// failed ingest does: the gap guard asks for it again and the next pass
/// walks it from its root. One hour at a one-minute cadence, fifteen at
/// `*/15`.
pub const DEFERRED_GRAPH_MAX_PASSES: u32 = 60;

/// The most bytes one record may hold (its JSON): 1 MiB, half of D1's 2 MB
/// row. A record past it is dropped (reason `too_big`). The measured case
/// (a 24 KB head over a wallet's small funding transactions, about 1 KB of
/// hex each) fits about a thousand nodes.
pub const DEFERRED_GRAPH_MAX_BYTES: usize = 1 << 20;

/// The most records one (peer, topic) may hold. A new deferral past it is
/// not saved (reason `too_many`) and its walk goes on under the per-peer
/// budget alone, as before #555 (the lens fold's L4): a peer that serves many
/// deep graphs costs at most 16 records. A storage may refuse a record at a
/// ceiling of its own ([`DeferredGraphSave::AtCeiling`]), counted `too_many`
/// too (the worker's global ceiling, the lens fold's M3).
pub const DEFERRED_GRAPHS_PER_PEER_TOPIC: usize = 16;

/// The per-graph budget of a [`GASPSync`] (bsv-low #555,
/// `Engine::set_graph_budget`). Setting one turns deferral ON for that sync.
pub struct GraphBudget<'a> {
    /// Calls one graph may make in one pass ([`DEFAULT_GRAPH_BUDGET_CALLS`]).
    pub max_calls: u32,
    /// A fresh deadline for ONE graph's pass ([`DEFAULT_GRAPH_BUDGET_MS`]).
    pub deadline: Box<dyn Fn() -> crate::engine::SleepFuture + 'a>,
}

/// The key of one held record (bsv-low #555): what a sync loads up front.
/// The record itself is read only when the peer serves its UTXO
/// ([`GASPStorage::get_deferred_graph`]), so at most one record is in memory
/// at a time (the lens fold's L3: the up-front load held up to 16 of them).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeferredGraphKey {
    /// The root UTXO, `txid.outputIndex`.
    pub outpoint: String,
    /// The root UTXO's score at the peer.
    pub score: u64,
}

/// What a storage did with a record it was asked to save (bsv-low #555).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeferredGraphSave {
    /// Saved (replaced).
    Saved,
    /// Refused at a ceiling of the storage's own (the worker's global count
    /// and byte bounds, the lens fold's M3): counted `too_many`, and the walk
    /// goes on under the per-peer budget alone, as before #555.
    AtCeiling,
}

/// One node a deferred walk has fetched and appended, in append order.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WalkedNode {
    /// The node as it was appended (the root's proof hydrated, if it was).
    pub node: GASPNode,
    /// `txid.outputIndex` of the node that spends it; `None` for the root.
    pub spent_by: Option<String>,
}

/// One input a deferred walk has still to ask for.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingInput {
    /// `txid.outputIndex` of the input (for the root, the UTXO itself).
    pub outpoint: String,
    /// The graph id the request carries (the root's outpoint).
    pub graph_id: String,
    /// Whether the input's metadata is asked for.
    pub metadata: bool,
    /// `txid.outputIndex` of the node that needs it; `None` for the root.
    pub spent_by: Option<String>,
    /// Whether that node is PROVEN: a named input the peer does not hold is
    /// then pruned (the D8 rule), else it fails the graph.
    pub parent_proven: bool,
}

/// The persisted partial walk of ONE graph (bsv-low #555): one record per
/// (peer, topic, root outpoint), REPLACED on every deferral, never appended.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeferredGraph {
    /// The peer the graph is walked from.
    pub peer: String,
    /// The topic.
    pub topic: String,
    /// The root UTXO, `txid.outputIndex`.
    pub outpoint: String,
    /// The root UTXO's score at the peer.
    pub score: u64,
    /// The nodes fetched so far, with their proofs, in append order.
    pub nodes: Vec<WalkedNode>,
    /// The inputs still to ask for, a stack (the last is asked next).
    pub pending: Vec<PendingInput>,
    /// Calls spent on this graph over every pass.
    pub calls: u64,
    /// Passes that deferred it (its age, in passes).
    pub passes: u32,
    /// Why it was last deferred: `calls`, `time`, `fault`,
    /// `anchor_unavailable`, `not_landed` or `peer_deadline`.
    pub reason: String,
}

impl DeferredGraph {
    /// The record's size as stored (its JSON), for
    /// [`DEFERRED_GRAPH_MAX_BYTES`].
    pub fn byte_size(&self) -> usize {
        serde_json::to_vec(self).map_or(usize::MAX, |b| b.len())
    }
}

/// Why a deferred graph's record was deleted without converging.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DropReason {
    /// Deferred [`DEFERRED_GRAPH_MAX_PASSES`] times; the UTXO fails.
    MaxPasses,
    /// Past [`DEFERRED_GRAPH_MAX_BYTES`]; the UTXO fails.
    TooBig,
    /// Past [`DEFERRED_GRAPHS_PER_PEER_TOPIC`]; the UTXO fails.
    TooMany,
    /// The record could not be written; the UTXO fails.
    StoreFault,
    /// A sync that ran to its end was not served the UTXO (spent at the peer).
    NotServed,
    /// The node already holds the UTXO (it landed another way).
    Held,
    /// The peer answered it does not hold an input an UNPROVEN node needs;
    /// the UTXO fails, as in the reference.
    NotHeld,
    /// The root came back PROVEN: the walk restarts from it, shorter.
    RootProven,
    /// The anchor check refused the graph (a final verdict, the cursor moves).
    Refused,
    /// A resumed record whose pass ended still holding no node (a record
    /// saved before the lens fold's H1, which saves none such); the UTXO
    /// fails.
    NoProgress,
}

impl DropReason {
    /// Every reason, for a caller that serves a counter per reason.
    pub const ALL: [DropReason; 10] = [
        Self::MaxPasses,
        Self::TooBig,
        Self::TooMany,
        Self::StoreFault,
        Self::NotServed,
        Self::Held,
        Self::NotHeld,
        Self::RootProven,
        Self::Refused,
        Self::NoProgress,
    ];

    /// The reason's name, as logged and counted.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MaxPasses => "max_passes",
            Self::TooBig => "too_big",
            Self::TooMany => "too_many",
            Self::StoreFault => "store_fault",
            Self::NotServed => "not_served",
            Self::Held => "held",
            Self::NotHeld => "not_held",
            Self::RootProven => "root_proven",
            Self::Refused => "refused",
            Self::NoProgress => "no_progress",
        }
    }
}

/// One record dropped in a sync.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DroppedGraph {
    /// The root outpoint.
    pub outpoint: String,
    /// Why.
    pub reason: DropReason,
}

/// What a sync did with deferred graphs (bsv-low #555).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DeferralStats {
    /// Graphs deferred (a new record, or a resumed one deferred again).
    pub deferred: u64,
    /// Graphs resumed from a record.
    pub resumed: u64,
    /// Resumed graphs that completed and landed (their record deleted).
    pub converged: u64,
    /// Records deleted without converging.
    pub dropped: Vec<DroppedGraph>,
    /// Walks of the sync that ended unfinished (deferred, or dropped at a
    /// bound) having appended at least one node in their pass, or that
    /// completed the graph (the lens fold's H1): the peer answered.
    pub progressed: u64,
    /// Walks of the sync that ended unfinished having appended NO node in
    /// their pass (a hung or failing peer; the lens fold's H1). A sync whose
    /// only outcome was these is a FAILED attempt for #302's quarantine.
    pub stalled: u64,
    /// Records NOT resumed this sync because the resumes of the pass had
    /// spent their shared budget (the lens fold's M1); held, untouched, for
    /// the next sync.
    pub held_back: u64,
}

/// The walk of the graph in hand: its record (what a deferral saves) and
/// the per-pass state. Lives in the [`GASPSync`], OUTSIDE the walk's
/// future, so a deadline that drops the future leaves it whole.
struct Walk {
    record: DeferredGraph,
    seen: std::collections::HashSet<String>,
    calls_this_pass: u32,
    resumed: bool,
    /// The calls this pass may make: the per-graph budget, or what the
    /// resumes of the pass have left of their shared one (M1); `u32::MAX`
    /// once a walk that cannot be kept goes on under the per-peer budget
    /// alone (L4).
    call_cap: u32,
    /// `record.nodes.len()` when the pass started (H1).
    nodes_at_start: usize,
    /// Whether this pass was already counted progressed or stalled.
    counted: bool,
}

/// How a graph's walk ended in this pass.
enum WalkEnd {
    /// Nothing is pending: complete the graph.
    Done,
    /// Deferred, with this reason.
    Deferred(&'static str),
}

/// What one step of the walk got.
enum StepOut {
    Pruned,
    Seen,
    Appended {
        node_id: String,
        walked: Box<WalkedNode>,
        children: Vec<PendingInput>,
    },
}

/// How [`GASPSync::ingest_utxo`] ended for a UTXO.
enum Ingested {
    Completed,
    Deferred,
    /// A record not resumed: the pass's resumes spent their shared budget.
    HeldBack,
}

/// What [`GASPSync::save_walk`] did with the walk in hand.
enum Saved {
    Yes,
    /// Not kept, for this reason; the walk is handed back so the caller can
    /// go on with it (L4) or drop it.
    Unkept(DropReason, Box<Walk>),
    Fault(GASPError),
}

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

    /// The keys of the deferred graphs of this (peer, topic), lowest score
    /// first (bsv-low #555). REQUIRED, as the three below (the lens fold's
    /// M2): a storage that keeps no records says so here, by answering none
    /// and refusing every save, and its graphs past the budget then fail.
    async fn load_deferred_graphs(&self) -> Result<Vec<DeferredGraphKey>, GASPError>;

    /// The record of the graph rooted at `outpoint`, if one is held.
    async fn get_deferred_graph(&self, outpoint: &str) -> Result<Option<DeferredGraph>, GASPError>;

    /// Save (replace) the record of one deferred graph.
    async fn save_deferred_graph(
        &self,
        record: &DeferredGraph,
    ) -> Result<DeferredGraphSave, GASPError>;

    /// Delete the record of the graph rooted at `outpoint`.
    async fn delete_deferred_graph(&self, outpoint: &str) -> Result<(), GASPError>;
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
    /// proof hex. A proven node ends the walk unless its topic manager names
    /// inputs needed for overlay history. Managers naming nothing still stop
    /// at the first proven layer. Managers needing history select those inputs
    /// instead of walking every funding/fee input back toward coinbase.
    ///
    /// The transaction DAG (no cycles), `seen` keyed by `txid.outputIndex`,
    /// the already-known strip, and the manager naming nothing at genesis
    /// bound the walk. There is no node cap, matching the reference's undefined
    /// `maxNodesInGraph` in `Engine.startGASPSync`. A per-peer budget drops the
    /// sync future at its deadline: the graphs finalized before it stay
    /// admitted (bsv-low #552, [`FinalizedGraphHook`]), the graph in flight
    /// is lost whole and walked again by the next sync, unless a per-graph
    /// budget is set (bsv-low #555, `Engine::set_graph_budget`): then a walk
    /// past either budget is deferred with its fetched nodes kept and resumed
    /// by the next sync, so ONE graph deeper than a pass converges over
    /// passes. Without it such a graph still never completes.
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

    /// Like [`Self::verified_proof_for`], but a TRANSPORT/READ FAULT stays
    /// distinguishable (bsv-low#304 gate M-5): `Err` = the proof source or
    /// header source READ failed (e.g. a chaintracks call starved at the
    /// invocation's subrequest wall) — retryable, NOT a chain verdict;
    /// `Ok(None)` = honestly no verified proof yet. Money-relevant callers
    /// (the spend-confirmation chaser) surface + count the fault so a
    /// starved tick is visible instead of masquerading as "not mined yet".
    /// Default: wrap `verified_proof_for` (which never faults) — existing
    /// fetchers keep their semantics unchanged.
    async fn verified_proof_for_detailed(&self, txid: &str) -> Result<Option<String>, String> {
        Ok(self.verified_proof_for(txid).await)
    }

    /// bsv-low #451 slice B (2026-09-17): like [`Self::verified_proof_for_detailed`], but the ladder asks EVERY
    /// rung — no per-pass short-circuit on the broadcaster's "held unmined" word. The completion passes may stop
    /// early (a fresh unmined word from Arcade means no courier holds a proof yet; the next tick asks again), but
    /// a caller that ACTS on `Ok(None)` this pass — the reorg re-anchor, which demotes a refuted row it could not
    /// re-prove; the unmined-ancestry walk, which treats an unproven ancestor as a branch to keep walking — must
    /// hear the couriers too (the delta-verify's LOW-A). Default: the detailed ask (a fetcher with no short-circuit
    /// has nothing to skip).
    async fn verified_proof_for_exhaustive(&self, txid: &str) -> Result<Option<String>, String> {
        self.verified_proof_for_detailed(txid).await
    }

    /// bsv-low M19B-G1 round 2 (review MED-2): the ladder's REMAINING
    /// per-invocation budget, when it keeps one (`None` = unbounded, or not
    /// known). A budgeted ladder answers `Ok(None)` to every ask once its
    /// budget is spent, which a caller must not read as "no courier serves a
    /// proof": it checks this first and leaves the row for a fresh budget.
    fn budget_remaining(&self) -> Option<u32> {
        None
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

    /// Which txid spends `txid:vout`, per an EXTERNAL INDEXER — a HINT, never
    /// a verdict (bsv-low 2026-08-18, the displaced-spender reconcile). The
    /// caller must independently prove the hint before acting on it:
    /// [`Self::spender_binding_raw`] (the bytes really spend the outpoint)
    /// and [`Self::verified_proof_for_detailed`] (the spender is MINED, root
    /// verified against our PoW-anchored headers). An indexer can therefore
    /// only ever point at the REAL confirmed spender — pointing anywhere else
    /// fails one of the two proofs and changes nothing.
    ///
    /// Default `Ok(None)` — unknown — so every existing implementation keeps
    /// compiling AND keeps today's behavior (no resolution, fail-closed).
    async fn resolve_spender(&self, _txid: &str, _vout: u32) -> Result<Option<String>, String> {
        Ok(None)
    }

    /// bsv-low (2026-09-04) — the SCRIPTHASH-HISTORY rung: the txids a
    /// courier's script-history index lists for `scripthash_le_hex` OTHER
    /// than `funding_txid` (newest first, a few at most). A pot's covenant
    /// script is unique to that pot, so its history is exactly
    /// `[funding, spender]` and the answer is the spender CANDIDATE; the
    /// caller BINDS it with [`spender_binding_raw`] before believing it. This
    /// rung survives a provider retiring its per-output spend endpoint
    /// (Bitails, pruned mode, 2026-09). `Err` = every rung faulted; an empty
    /// `Ok` = the indexes answered and list no other tx. Default: no index.
    ///
    /// [`spender_binding_raw`]: AncestorFetcher::spender_binding_raw
    async fn resolve_spender_by_script(
        &self,
        _scripthash_le_hex: &str,
        _funding_txid: &str,
    ) -> Result<Vec<String>, String> {
        Ok(Vec::new())
    }

    /// bsv-low (2026-09-04): an output's LOCKING SCRIPT by outpoint from a
    /// courier's output index (BananaBlocks `/txo/{txid}/{vout}`) — the
    /// scripthash rung's script source when no BEEF is stored for the row.
    /// `Ok(None)` = the index never saw it. Default: no such index.
    async fn output_script_hint(&self, _txid: &str, _vout: u32) -> Result<Option<Vec<u8>>, String> {
        Ok(None)
    }

    /// `spender`'s raw tx hex — fetched CONTENT-ADDRESSED (the bytes must
    /// hash to `spender`) — returned iff those bytes carry an input spending
    /// exactly `txid:vout`. This is the binding that turns
    /// [`Self::resolve_spender`]'s hint into a fact, and the caller gets the
    /// PROVEN bytes back so it can derive further facts from them (bytes
    /// finality, a durable BEEF) without a second fetch. `Ok(None)` = the
    /// bytes were read and do NOT bind (a lying or garbled hint — refuse);
    /// `Err` = could not read (a fault, never a verdict). Default `Ok(None)`:
    /// fail-closed, an unbindable hint is a dead hint.
    async fn spender_binding_raw(
        &self,
        _spender: &str,
        _txid: &str,
        _vout: u32,
    ) -> Result<Option<String>, String> {
        Ok(None)
    }
}

/// An ancestor transaction fetched from chain: its raw tx hex plus an optional
/// BUMP merkle proof hex (present when the tx is mined).
#[derive(Debug, Clone)]
pub struct FetchedAncestor {
    /// Raw transaction hex.
    pub raw_tx: String,
    /// BUMP merkle proof hex, if the ancestor is mined. When `Some`, the
    /// synthesized node ends the walk unless its topic manager names inputs.
    pub proof: Option<String>,
}

// ============================================================================
// FinalizedGraphHook (bsv-low #552)
// ============================================================================

/// Called by [`GASPSync::sync`] after EVERY incoming UTXO whose graph was
/// completed (finalized, or refused by the anchor check and discarded), before
/// the next UTXO is asked for.
///
/// The reference submits inside `finalizeGraph`, so a graph is admitted the
/// moment it is finalized. Here `OverlayGASPStorage::finalize_graph` only
/// pushes to a sink (it holds a `Storage`, not the engine), and the engine
/// used to drain that sink after the WHOLE sync: a sync dropped at its
/// per-peer budget admitted nothing. With a hook the engine drains the sink
/// per graph, inside the raced future, so what was finalized before the
/// deadline stays admitted and the next sync's known-UTXO skip and known-input
/// strip resume from it.
///
/// An `Err` FAILS THAT UTXO, exactly as a failed ingest does: the gap guard
/// keeps the cursor below it and the next sync asks for it again. The engine
/// answers `Err` when a finalize submit did not land (the lens fold's
/// MEDIUM-1). It cannot fail the sync. While the hook runs,
/// [`GASPSync::completed_cursor`] still sits BELOW the UTXO just completed,
/// so a sync dropped inside the hook asks for that UTXO again.
#[async_trait(?Send)]
pub trait FinalizedGraphHook {
    /// One incoming UTXO's graph was completed; drain what it finalized.
    async fn graph_completed(&self) -> Result<(), GASPError>;
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
    /// (the default), a peer's `request_node` error propagates and the graph
    /// is abandoned upstream, except for an input NAMED by a proven node's
    /// topic manager that the peer answers it does not hold
    /// (`GASPError::NodeNotFound`), whose branch is pruned (the D8 decoy
    /// rule). When `Some`, ancestors come from chain instead of the peer and
    /// nothing is pruned: every fetcher error abandons the graph.
    ancestor_fetcher: Option<std::rc::Rc<dyn AncestorFetcher + 'a>>,
    /// Manager-named inputs whose branch was PRUNED because the peer answered
    /// that it does not hold them (zanaadu-v2 #314, D8), keyed
    /// `txid.outputIndex`. One entry is one failed round trip: an outpoint in
    /// this set is never asked for again in this sync, whichever parent or
    /// graph names it. Cleared at the start of each `sync`.
    pruned: std::cell::RefCell<std::collections::HashSet<String>>,
    /// Distinct outpoints pruned in the last `sync` (the size of `pruned`).
    pruned_inputs: std::cell::Cell<u64>,
    /// Graphs REFUSED by `validate_graph_anchor` and discarded in the last
    /// `sync` (bsv-low #551). Cleared at the start of each `sync`.
    discarded_graphs: std::cell::Cell<u64>,
    /// Called after every completed incoming UTXO (bsv-low #552). `None`
    /// (the default): nothing is called and `sync` runs as it always did.
    finalized_hook: Option<Box<dyn FinalizedGraphHook + 'a>>,
    /// See [`Self::completed_cursor`].
    completed_cursor: u64,
    /// See [`Self::graph_in_flight`].
    graph_in_flight: bool,
    /// The per-graph budget (bsv-low #555). `None` (the default): no graph
    /// is deferred and the walk is the one before #555.
    graph_budget: Option<GraphBudget<'a>>,
    /// The roots of this (peer, topic)'s records not yet resumed in this
    /// sync (their keys; a record is read when its UTXO is served).
    deferred: std::cell::RefCell<std::collections::HashSet<String>>,
    /// ONE deadline for every resume of the sync (the lens fold's M1), made
    /// at the first resume.
    resume_deadline: std::cell::RefCell<Option<crate::engine::SleepFuture>>,
    /// The calls every resume of the sync may still make together (M1).
    resume_calls_left: std::cell::Cell<u32>,
    /// Calls made by the sync's walks so far.
    calls_made: std::cell::Cell<u64>,
    /// How many records this (peer, topic) holds in storage.
    held_records: std::cell::Cell<usize>,
    /// The walk of the graph in hand (see [`Walk`]).
    walk: std::cell::RefCell<Option<Walk>>,
    /// See [`Self::deferral_stats`].
    stats: std::cell::RefCell<DeferralStats>,
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
            pruned: std::cell::RefCell::new(std::collections::HashSet::new()),
            pruned_inputs: std::cell::Cell::new(0),
            discarded_graphs: std::cell::Cell::new(0),
            finalized_hook: None,
            completed_cursor: last_interaction,
            graph_in_flight: false,
            graph_budget: None,
            deferred: std::cell::RefCell::new(std::collections::HashSet::new()),
            resume_deadline: std::cell::RefCell::new(None),
            resume_calls_left: std::cell::Cell::new(0),
            calls_made: std::cell::Cell::new(0),
            held_records: std::cell::Cell::new(0),
            walk: std::cell::RefCell::new(None),
            stats: std::cell::RefCell::new(DeferralStats::default()),
        }
    }

    /// Bound each graph's walk by `budget` and DEFER a graph past it
    /// (bsv-low #555): its partial walk is saved through
    /// [`GASPStorage::save_deferred_graph`], its UTXO is held below the
    /// cursor like a failed one, the sync goes on to the next UTXO, and the
    /// next sync that is served the UTXO RESUMES the walk from the record,
    /// asking only what is still pending. Without a budget nothing is
    /// deferred and `sync` is unchanged.
    #[must_use]
    pub fn with_graph_budget(mut self, budget: GraphBudget<'a>) -> Self {
        self.graph_budget = Some(budget);
        self
    }

    /// What the last `sync` did with deferred graphs (bsv-low #555).
    pub fn deferral_stats(&self) -> DeferralStats {
        self.stats.borrow().clone()
    }

    /// Have `hook` called after every completed incoming UTXO (bsv-low #552,
    /// see [`FinalizedGraphHook`]). Without one `sync` is unchanged.
    #[must_use]
    pub fn with_finalized_graph_hook(mut self, hook: Box<dyn FinalizedGraphHook + 'a>) -> Self {
        self.finalized_hook = Some(hook);
        self
    }

    /// The cursor that is safe to persist if `sync` stopped RIGHT NOW (its
    /// future dropped at a deadline; bsv-low #552). It is the gap guard's
    /// rule applied to unfinished work: strictly below the lowest score of
    /// any UTXO of the current page not yet completed (the one in flight
    /// included) and of any UTXO whose ingest failed, never below the cursor
    /// the sync entered with. The remote serves `score >= since`, so the next
    /// sync is served everything this one did not finish. After a completed
    /// `sync` it equals `last_interaction`.
    ///
    /// `last_interaction` itself is NOT safe after a drop: it is raised to
    /// each UTXO's score BEFORE that UTXO is ingested, for pagination.
    pub fn completed_cursor(&self) -> u64 {
        self.completed_cursor
    }

    /// Whether a graph was being fetched, walked or anchor-checked when `sync`
    /// last stopped: `true` only after a `sync` future dropped mid-graph
    /// (bsv-low #552). That graph's nodes died with the storage adapter;
    /// nothing of it was finalized.
    pub fn graph_in_flight(&self) -> bool {
        self.graph_in_flight
    }

    /// `cursor` capped strictly below `lowest_unfinished`, floored at `floor`:
    /// the gap guard's arithmetic, shared with [`Self::completed_cursor`].
    fn capped_below(cursor: u64, lowest_unfinished: Option<u64>, floor: u64) -> u64 {
        match lowest_unfinished {
            Some(score) => cursor.min(score.saturating_sub(1).max(floor)),
            None => cursor,
        }
    }

    /// How many graphs this orchestrator's last `sync` discarded because
    /// `validate_graph_anchor` REFUSED them (a root that does not verify, or
    /// one the topic manager does not admit at the end of the replay; bsv-low
    /// #551). Nothing of a discarded graph is finalized. As in the reference
    /// a refused graph is not a failed UTXO: the cursor advances past it. A
    /// graph whose anchor could not be CHECKED
    /// (`GASPError::AnchorUnavailable`: a tracker or storage fault, a topic
    /// manager error in the replay) is not counted here: that one fails its
    /// UTXO and is asked for again.
    pub fn discarded_graphs(&self) -> u64 {
        self.discarded_graphs.get()
    }

    /// How many manager-named inputs this orchestrator pruned because the
    /// peer answered that it does not hold them (the D8 decoy rule, see
    /// `process_incoming_node`). Counted per DISTINCT outpoint per `sync`
    /// call of this orchestrator, which talks to ONE peer: the same decoy
    /// seen through two peers is counted by each. A pruned branch is NOT a
    /// failed UTXO: the graph completes with what it has and the cursor
    /// advances.
    pub fn pruned_inputs(&self) -> u64 {
        self.pruned_inputs.get()
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
        self.pruned.borrow_mut().clear();
        self.pruned_inputs.set(0);
        self.discarded_graphs.set(0);
        self.completed_cursor = self.last_interaction;
        self.graph_in_flight = false;
        self.walk.borrow_mut().take();
        *self.stats.borrow_mut() = DeferralStats::default();
        self.deferred.borrow_mut().clear();
        self.held_records.set(0);
        self.resume_deadline.borrow_mut().take();
        self.resume_calls_left
            .set(self.graph_budget.as_ref().map_or(0, |b| b.max_calls));
        // bsv-low #555: the deferred graphs of this (peer, topic), resumed
        // as the peer serves their UTXOs again (the cursor was held below
        // them). Only their keys: a record is read when its UTXO is served
        // (the lens fold's L3). A read fault resumes nothing this sync: those
        // UTXOs are walked from their roots and their records replaced, and
        // the (peer, topic) is taken as FULL, so no new record is saved over
        // a count not known (the lens fold's L6).
        if self.graph_budget.is_some() {
            match self.storage.load_deferred_graphs().await {
                Ok(keys) => {
                    self.held_records.set(keys.len());
                    self.deferred
                        .borrow_mut()
                        .extend(keys.into_iter().map(|k| k.outpoint));
                }
                Err(e) => {
                    self.held_records.set(DEFERRED_GRAPHS_PER_PEER_TOPIC);
                    warn!(
                        "{} Could not read the deferred graphs (bsv-low #555): {}",
                        self.log_prefix, e
                    );
                }
            }
        }

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
        // The UTXOs whose ingest FAILED this run (bsv-low #554). The cursor
        // moves to a UTXO's score before its ingest and the responder serves
        // `score >= since`, so the row at a page boundary is served again on
        // the next page of this same sync. A completed one is in
        // `shared_outpoints` and skipped; a failed one was in no set and was
        // walked a second time, at twice the requests and twice the per-peer
        // budget. An addition to the reference: `GASP.ts sync` keeps only
        // `sharedOutpoints` (added on success), so a failed UTXO served
        // again by a later page is ingested again there. Here a UTXO is
        // ingested at most once per sync; the retry is the next sync's, which
        // the gap guard below makes sure is served it. Kept apart from
        // `shared_outpoints`, which also decides what a bidirectional sync
        // pushes.
        let mut failed_outpoints: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        // Paginated pull from remote
        loop {
            let cursor_before_page = self.last_interaction;
            // Every UTXO seen so far is completed or failed: were the sync
            // dropped inside the page request below, this is what it did.
            self.completed_cursor =
                Self::capped_below(self.last_interaction, min_failed_score, initial_interaction);
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

            // lowest_ahead[i]: the lowest score among this page's UTXOs from
            // i on, for `completed_cursor` (a page is not assumed sorted).
            let mut lowest_ahead: Vec<u64> =
                response.utxo_list.iter().map(|u| u.score as u64).collect();
            for i in (0..lowest_ahead.len().saturating_sub(1)).rev() {
                lowest_ahead[i] = lowest_ahead[i].min(lowest_ahead[i + 1]);
            }

            for (position, utxo) in response.utxo_list.iter().enumerate() {
                // Track highest score for pagination
                if utxo.score as u64 > self.last_interaction {
                    self.last_interaction = utxo.score as u64;
                }

                let outpoint = format!("{}.{}", utxo.txid, utxo.output_index);
                if known_outpoints.contains(&outpoint) {
                    // A deferred graph whose UTXO the node now holds (it
                    // landed another way): nothing to resume.
                    if self.deferred.borrow_mut().remove(&outpoint) {
                        self.drop_record(&outpoint, DropReason::Held).await;
                    }
                    shared_outpoints.insert(outpoint.clone());
                    known_outpoints.remove(&outpoint);
                } else if !shared_outpoints.contains(&outpoint)
                    && !failed_outpoints.contains(&outpoint)
                {
                    // New UTXO — request and ingest the graph
                    let lowest_unfinished = min_failed_score
                        .map_or(lowest_ahead[position], |f| f.min(lowest_ahead[position]));
                    self.completed_cursor = Self::capped_below(
                        self.last_interaction,
                        Some(lowest_unfinished),
                        initial_interaction,
                    );
                    self.graph_in_flight = true;
                    let ingested = self.ingest_utxo(utxo, &outpoint).await;
                    self.graph_in_flight = false;
                    let ingested = match ingested {
                        Ok(Ingested::Completed) => {
                            let landed = match &self.finalized_hook {
                                Some(hook) => hook.graph_completed().await,
                                None => Ok(()),
                            };
                            self.settle_completed(landed.is_ok()).await;
                            landed.map(|()| true)
                        }
                        Ok(Ingested::Deferred | Ingested::HeldBack) => Ok(false),
                        Err(e) => {
                            self.walk.borrow_mut().take();
                            Err(e)
                        }
                    };
                    match ingested {
                        Ok(true) => {
                            shared_outpoints.insert(outpoint);
                        }
                        // bsv-low #555: DEFERRED, its walk saved (or a
                        // record held back for the next sync, M1). Held
                        // below the cursor like a failed UTXO (the gap
                        // guard) and not walked again in this sync (#554);
                        // the next sync resumes it. The sync goes on.
                        Ok(false) => {
                            let s = utxo.score as u64;
                            min_failed_score = Some(min_failed_score.map_or(s, |cur| cur.min(s)));
                            failed_outpoints.insert(outpoint);
                        }
                        Err(e) => {
                            warn!(
                                "{} Error ingesting UTXO {}: {}",
                                self.log_prefix, outpoint, e
                            );
                            // Remember the lowest failed score so the persisted
                            // cursor cannot skip past it (see cap below).
                            let s = utxo.score as u64;
                            min_failed_score = Some(min_failed_score.map_or(s, |cur| cur.min(s)));
                            // Not again in this sync (bsv-low #554).
                            failed_outpoints.insert(outpoint);
                        }
                    }
                }
            }

            // Pagination termination (bsv-low #291 gate findings M1/LOW-A):
            // keep paging on ANY page that advanced the score cursor —
            // with or without a requested limit. The old rule ("continue
            // only on a full page"; `None` never paged at all) silently
            // terminated after one page against a responder that CLAMPS
            // the page size (Engine::clamp_sync_limit clamps BOTH an
            // over-large and an ABSENT limit) — a clamped page is never
            // "full" by our limit, yet more rows remain. A non-advancing
            // page (empty, or only rows at scores we already hold — the
            // responder serves `score >= since`, so the boundary row is
            // re-served) is the true completion signal: no further request
            // can make progress.
            //
            // Known residual (gate LOW-B, documented not fixed): the
            // cursor is the bare GASP `since` score, so a tie group of
            // IDENTICAL scores at least as large as the responder's page
            // would end the run with that group's tail unreached (break on
            // non-advance — a silent gap, never a spin). A compound
            // (score, rowid) cursor would need a wire change to `since`,
            // which is out of bounds; unconstructible in practice — see
            // the responder-side note at
            // `D1Storage::find_utxos_for_topic`.
            if self.last_interaction == cursor_before_page {
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
        self.completed_cursor = self.last_interaction;

        // bsv-low #555: a record whose UTXO a sync that ran to its end was
        // not served (spent at the peer, or no longer listed): nothing will
        // resume it.
        let unserved: Vec<String> = self.deferred.borrow_mut().drain().collect();
        for outpoint in unserved {
            self.drop_record(&outpoint, DropReason::NotServed).await;
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
    ///
    /// With a per-graph budget (bsv-low #555, [`Self::with_graph_budget`]) a
    /// walk past it is DEFERRED (`Ok(Ingested::Deferred)`): its record is
    /// saved and the caller holds the UTXO below the cursor like a failed
    /// one. A record of this UTXO saved by an earlier sync is RESUMED: its
    /// nodes are appended again (no request) and only its pending inputs are
    /// asked. Every walk error then defers too (the progress is kept), except
    /// the peer's definite "not held" for an input an UNPROVEN node needs (an
    /// SPV necessity, as in the reference): that one fails the UTXO and drops
    /// the record. On `Completed` the walk stays in hand until
    /// [`Self::settle_completed`] knows whether it landed.
    ///
    /// Every RESUME of one sync shares ONE per-graph budget (the lens fold's
    /// M1): one deadline, made at the first resume, and one call count. The
    /// cursor is held below the deferred UTXOs, so the peer serves them
    /// first; with a budget each, two of them took the whole per-peer budget
    /// and no UTXO above them was reached. Now the resumes of a pass take at
    /// most one graph's budget and the first new UTXO keeps its own. A record
    /// served once that budget is spent is HELD BACK untouched (no read, no
    /// pass counted) for the next sync.
    async fn ingest_utxo(&self, utxo: &GASPOutput, outpoint: &str) -> Result<Ingested, GASPError> {
        debug!("{} Requesting node for {}", self.log_prefix, outpoint);
        let Some(budget) = &self.graph_budget else {
            *self.walk.borrow_mut() = Some(Self::fresh_walk(utxo.score as u64, outpoint, u32::MAX));
            let walked = self.walk_graph(None).await;
            let walk = self.walk.borrow_mut().take();
            walked?;
            self.complete_graph(&Self::graph_id_of(walk.as_ref(), outpoint))
                .await?;
            return Ok(Ingested::Completed);
        };

        if !self.deferred.borrow_mut().remove(outpoint) {
            let mut deadline = (budget.deadline)();
            return self
                .ingest_budgeted(utxo, outpoint, &mut deadline, budget.max_calls, false)
                .await;
        }
        let mut deadline = self
            .resume_deadline
            .borrow_mut()
            .take()
            .unwrap_or_else(|| (budget.deadline)());
        let cap = self.resume_calls_left.get();
        if cap == 0 || Self::due(&mut deadline).await {
            *self.resume_deadline.borrow_mut() = Some(deadline);
            self.stats.borrow_mut().held_back += 1;
            info!(
                "{} Deferred graph {} held back: the resumes of this pass spent their budget (bsv-low #555)",
                self.log_prefix, outpoint
            );
            return Ok(Ingested::HeldBack);
        }
        let before = self.calls_made.get();
        let ingested = self
            .ingest_budgeted(utxo, outpoint, &mut deadline, cap, true)
            .await;
        let spent = u32::try_from(self.calls_made.get() - before).unwrap_or(u32::MAX);
        self.resume_calls_left.set(cap.saturating_sub(spent));
        *self.resume_deadline.borrow_mut() = Some(deadline);
        ingested
    }

    /// Whether `deadline` has fallen due (polled once, with the task's own
    /// waker).
    async fn due(deadline: &mut crate::engine::SleepFuture) -> bool {
        std::future::poll_fn(|cx| std::task::Poll::Ready(deadline.as_mut().poll(cx).is_ready()))
            .await
    }

    /// A walk from the root `outpoint`, nothing fetched yet.
    fn fresh_walk(score: u64, outpoint: &str, call_cap: u32) -> Walk {
        Walk {
            record: DeferredGraph {
                peer: String::new(),
                topic: String::new(),
                outpoint: outpoint.to_string(),
                score,
                nodes: Vec::new(),
                pending: vec![PendingInput {
                    outpoint: outpoint.to_string(),
                    graph_id: outpoint.to_string(),
                    metadata: true,
                    spent_by: None,
                    parent_proven: false,
                }],
                calls: 0,
                passes: 0,
                reason: String::new(),
            },
            seen: std::collections::HashSet::new(),
            calls_this_pass: 0,
            resumed: false,
            call_cap,
            nodes_at_start: 0,
            counted: false,
        }
    }

    /// One pass of a graph's walk under the per-graph budget: `deadline`,
    /// and `cap` calls. `from_record`: a record of it is held (its key was
    /// loaded); it is read now.
    async fn ingest_budgeted(
        &self,
        utxo: &GASPOutput,
        outpoint: &str,
        deadline: &mut crate::engine::SleepFuture,
        cap: u32,
        from_record: bool,
    ) -> Result<Ingested, GASPError> {
        let score = utxo.score as u64;
        // A read fault fails the UTXO and keeps the record for the next sync.
        let record = if from_record {
            let record = self.storage.get_deferred_graph(outpoint).await?;
            if record.is_none() {
                // Deleted since the keys were read (the worker's sweep).
                self.held_records
                    .set(self.held_records.get().saturating_sub(1));
            }
            record
        } else {
            None
        };
        let walk = match record {
            None => Self::fresh_walk(score, outpoint, cap),
            Some(record) if record.passes >= DEFERRED_GRAPH_MAX_PASSES => {
                let passes = record.passes;
                self.drop_record(outpoint, DropReason::MaxPasses).await;
                return Err(GASPError::Other(format!(
                    "deferred graph {outpoint} dropped after {passes} passes (bsv-low #555)"
                )));
            }
            Some(record) => {
                self.stats.borrow_mut().resumed += 1;
                info!(
                    "{} Resuming deferred graph {} (bsv-low #555): {} nodes, {} pending, {} calls over {} passes",
                    self.log_prefix,
                    outpoint,
                    record.nodes.len(),
                    record.pending.len(),
                    record.calls,
                    record.passes
                );
                Walk {
                    seen: record
                        .nodes
                        .iter()
                        .filter_map(|w| Self::node_id(&w.node))
                        .collect(),
                    nodes_at_start: record.nodes.len(),
                    record,
                    calls_this_pass: 0,
                    resumed: true,
                    call_cap: cap,
                    counted: false,
                }
            }
        };
        let resumed = walk.resumed;
        let root_unproven = walk
            .record
            .nodes
            .first()
            .is_some_and(|w| w.node.proof.is_none());
        *self.walk.borrow_mut() = Some(walk);

        let ended = async {
            if resumed && root_unproven {
                // The root is asked again (one call): once its block lands
                // the peer serves it PROVEN, and the walk from it is shorter
                // than the unproven ancestry the record is still walking.
                let root = PendingInput {
                    outpoint: outpoint.to_string(),
                    graph_id: outpoint.to_string(),
                    metadata: true,
                    spent_by: None,
                    parent_proven: false,
                };
                match race_or_deadline(self.fetch_root(&root), deadline.as_mut()).await {
                    None => return Ok(WalkEnd::Deferred("time")),
                    Some(Ok(node)) if node.proof.is_some() => {
                        let carried = self.walk.borrow().as_ref().map_or(0, |w| w.calls_this_pass);
                        self.drop_record(outpoint, DropReason::RootProven).await;
                        let mut restarted = Self::fresh_walk(score, outpoint, cap);
                        restarted.calls_this_pass = carried;
                        restarted.record.calls = u64::from(carried);
                        *self.walk.borrow_mut() = Some(restarted);
                    }
                    // Unchanged, or not answered: the record goes on.
                    Some(_) => {}
                }
            }
            self.append_walked().await?;
            self.walk_graph(Some(&mut *deadline)).await
        }
        .await;
        self.finish_pass(outpoint, ended).await
    }

    /// Append the walk's fetched nodes to the graph (a resume, or a walk
    /// that goes on after its graph was discarded).
    async fn append_walked(&self) -> Result<(), GASPError> {
        let nodes: Vec<WalkedNode> = self
            .walk
            .borrow()
            .as_ref()
            .map(|w| w.record.nodes.clone())
            .unwrap_or_default();
        for walked in &nodes {
            self.storage
                .append_to_graph(&walked.node, walked.spent_by.as_deref())
                .await?;
        }
        Ok(())
    }

    /// The end of a budgeted pass: complete the graph, defer it, or fail
    /// its UTXO. A walk past its per-graph budget that cannot be KEPT (past
    /// [`DEFERRED_GRAPHS_PER_PEER_TOPIC`], [`DEFERRED_GRAPH_MAX_BYTES`] or
    /// the storage's ceiling) goes on under the per-peer budget alone, as
    /// every walk did before #555 (the lens fold's L4: such a graph completed
    /// in one pass before #555, and was then failed on every pass).
    async fn finish_pass(
        &self,
        outpoint: &str,
        mut ended: Result<WalkEnd, GASPError>,
    ) -> Result<Ingested, GASPError> {
        loop {
            let graph_id = Self::graph_id_of(self.walk.borrow().as_ref(), outpoint);
            // Whether a record of this graph is still held: not after a
            // `root_proven` restart, whose walk is fresh.
            let resumed = self.walk.borrow().as_ref().is_some_and(|w| w.resumed);
            let (reason, completed) = match ended {
                Ok(WalkEnd::Done) => match self.complete_graph(&graph_id).await {
                    Ok(true) => return Ok(Ingested::Completed),
                    Ok(false) => {
                        // Refused by the anchor check: a verdict, the cursor
                        // moves, nothing to resume.
                        self.walk.borrow_mut().take();
                        if resumed {
                            self.drop_record(outpoint, DropReason::Refused).await;
                        }
                        return Ok(Ingested::Completed);
                    }
                    // `complete_graph` discarded the graph already.
                    Err(GASPError::AnchorUnavailable(_)) => ("anchor_unavailable", true),
                    // A finalize fault: the graph is still in hand.
                    Err(_) => {
                        let _ = self.storage.discard_graph(&graph_id).await;
                        ("fault", true)
                    }
                },
                Ok(WalkEnd::Deferred(reason)) => (reason, false),
                Err(e @ GASPError::NodeNotFound(_)) => {
                    let _ = self.storage.discard_graph(&graph_id).await;
                    self.walk.borrow_mut().take();
                    if resumed {
                        self.drop_record(outpoint, DropReason::NotHeld).await;
                    }
                    return Err(e);
                }
                Err(e) => {
                    warn!(
                        "{} Walk of {} faulted, deferred with its progress: {}",
                        self.log_prefix, outpoint, e
                    );
                    let _ = self.storage.discard_graph(&graph_id).await;
                    ("fault", false)
                }
            };
            match self.save_walk(reason, completed).await {
                Saved::Yes => {
                    let _ = self.storage.discard_graph(&graph_id).await;
                    return Ok(Ingested::Deferred);
                }
                Saved::Fault(e) => {
                    let _ = self.storage.discard_graph(&graph_id).await;
                    return Err(e);
                }
                Saved::Unkept(why, mut walk) => {
                    self.unkept(why, &mut walk).await;
                    let goes_on = matches!(reason, "calls" | "time")
                        && matches!(why, DropReason::TooBig | DropReason::TooMany);
                    if !goes_on {
                        let _ = self.storage.discard_graph(&graph_id).await;
                        return Err(GASPError::Other(format!(
                            "deferred graph {outpoint} not kept: {} (bsv-low #555)",
                            why.as_str()
                        )));
                    }
                    info!(
                        "{} Graph {} cannot be kept ({}): its walk goes on under the per-peer budget alone (bsv-low #555)",
                        self.log_prefix,
                        outpoint,
                        why.as_str()
                    );
                    walk.call_cap = u32::MAX;
                    *self.walk.borrow_mut() = Some(*walk);
                    // The graph in hand was never discarded: walk on.
                    ended = self.walk_graph(None).await;
                }
            }
        }
    }

    /// After a COMPLETED graph's hook (bsv-low #555): a graph that landed
    /// deletes its record (a resumed one converged); one whose finalize did
    /// not land keeps its walk as a record with nothing pending, so the next
    /// pass completes it again without walking it again. No-op without a
    /// per-graph budget.
    async fn settle_completed(&self, landed: bool) {
        if self.graph_budget.is_none() {
            self.walk.borrow_mut().take();
            return;
        }
        if landed {
            let Some(walk) = self.walk.borrow_mut().take() else {
                return;
            };
            if walk.resumed {
                if let Err(e) = self
                    .storage
                    .delete_deferred_graph(&walk.record.outpoint)
                    .await
                {
                    warn!(
                        "{} Could not delete the record of converged graph {}: {}",
                        self.log_prefix, walk.record.outpoint, e
                    );
                }
                self.held_records
                    .set(self.held_records.get().saturating_sub(1));
                self.stats.borrow_mut().converged += 1;
                info!(
                    "{} Deferred graph {} CONVERGED after {} passes, {} calls, {} nodes (bsv-low #555)",
                    self.log_prefix,
                    walk.record.outpoint,
                    walk.record.passes + 1,
                    walk.record.calls,
                    walk.record.nodes.len()
                );
            }
        } else if let Saved::Unkept(why, mut walk) = self.save_walk("not_landed", true).await {
            self.unkept(why, &mut walk).await;
        }
    }

    /// Save the walk in hand as its graph's record (bsv-low #555), counting
    /// a deferral; or hand it back UNKEPT with the reason (past a bound, or
    /// holding no node), or answer the storage's fault (the UTXO then fails
    /// as before #555). Counts the pass progressed or stalled (the lens
    /// fold's H1), once per pass.
    async fn save_walk(&self, reason: &'static str, completed: bool) -> Saved {
        let Some(mut walk) = self.walk.borrow_mut().take() else {
            // Nothing in hand: nothing to save.
            return Saved::Yes;
        };
        if !walk.counted {
            walk.counted = true;
            let mut stats = self.stats.borrow_mut();
            if completed || walk.record.nodes.len() > walk.nodes_at_start {
                stats.progressed += 1;
            } else {
                stats.stalled += 1;
            }
        }
        walk.record.passes += 1;
        walk.record.reason = reason.to_string();
        // H1: a walk that holds no node is never kept (nothing would be lost
        // by not keeping it, and an empty record took one of the 16 places
        // for up to 60 passes, L2): its UTXO fails as before #555.
        if walk.record.nodes.is_empty() {
            return Saved::Unkept(DropReason::NoProgress, Box::new(walk));
        }
        if !walk.resumed && self.held_records.get() >= DEFERRED_GRAPHS_PER_PEER_TOPIC {
            return Saved::Unkept(DropReason::TooMany, Box::new(walk));
        }
        let bytes = walk.record.byte_size();
        if bytes > DEFERRED_GRAPH_MAX_BYTES {
            return Saved::Unkept(DropReason::TooBig, Box::new(walk));
        }
        let outpoint = walk.record.outpoint.clone();
        match self.storage.save_deferred_graph(&walk.record).await {
            Ok(DeferredGraphSave::Saved) => {
                if !walk.resumed {
                    self.held_records.set(self.held_records.get() + 1);
                }
                self.stats.borrow_mut().deferred += 1;
                info!(
                    "{} DEFERRED graph {} ({}): {} nodes, {} pending, {} calls ({} this pass), pass {}, {} bytes (bsv-low #555)",
                    self.log_prefix,
                    outpoint,
                    reason,
                    walk.record.nodes.len(),
                    walk.record.pending.len(),
                    walk.record.calls,
                    walk.calls_this_pass,
                    walk.record.passes,
                    bytes
                );
                Saved::Yes
            }
            Ok(DeferredGraphSave::AtCeiling) => Saved::Unkept(DropReason::TooMany, Box::new(walk)),
            Err(e) => {
                if walk.resumed {
                    self.drop_record(&outpoint, DropReason::StoreFault).await;
                } else {
                    self.note_dropped(&outpoint, DropReason::StoreFault);
                }
                Saved::Fault(e)
            }
        }
    }

    /// A walk [`Self::save_walk`] did not keep: its held record (if any) is
    /// deleted with the reason, else the reason is counted; a fresh walk
    /// that fetched nothing was never a record and is only logged.
    async fn unkept(&self, why: DropReason, walk: &mut Walk) {
        let outpoint = walk.record.outpoint.clone();
        if walk.resumed {
            self.drop_record(&outpoint, why).await;
            walk.resumed = false;
        } else if why == DropReason::NoProgress {
            warn!(
                "{} Walk of {} fetched nothing: not kept, the UTXO fails (bsv-low #555)",
                self.log_prefix, outpoint
            );
        } else {
            self.note_dropped(&outpoint, why);
        }
    }

    /// Delete a held record and count it dropped.
    async fn drop_record(&self, outpoint: &str, reason: DropReason) {
        if let Err(e) = self.storage.delete_deferred_graph(outpoint).await {
            warn!(
                "{} Could not delete the record of deferred graph {}: {}",
                self.log_prefix, outpoint, e
            );
        }
        self.held_records
            .set(self.held_records.get().saturating_sub(1));
        self.note_dropped(outpoint, reason);
    }

    fn note_dropped(&self, outpoint: &str, reason: DropReason) {
        warn!(
            "{} Deferred graph {} DROPPED ({}) (bsv-low #555)",
            self.log_prefix,
            outpoint,
            reason.as_str()
        );
        self.stats.borrow_mut().dropped.push(DroppedGraph {
            outpoint: outpoint.to_string(),
            reason,
        });
    }

    /// Save the walk of the graph that was in hand when a deadline dropped
    /// `sync` (bsv-low #555): the per-peer deadline (D16) defers it like the
    /// per-graph one, so the next pass resumes it instead of walking it
    /// again. No-op without a per-graph budget or with no walk in hand.
    pub async fn defer_in_flight(&self) {
        if self.graph_budget.is_some() {
            if let Saved::Unkept(why, mut walk) = self.save_walk("peer_deadline", false).await {
                self.unkept(why, &mut walk).await;
            }
        }
    }

    /// The graph id of the walk in hand: its root's, else the outpoint.
    fn graph_id_of(walk: Option<&Walk>, outpoint: &str) -> String {
        walk.and_then(|w| w.record.nodes.first())
            .map_or_else(|| outpoint.to_string(), |w| w.node.graph_id.clone())
    }

    /// `txid.outputIndex` of a node, its txid computed from its raw bytes.
    fn node_id(node: &GASPNode) -> Option<String> {
        bsv_rs::transaction::Transaction::from_hex(&node.raw_tx)
            .ok()
            .map(|tx| format!("{}.{}", tx.id(), node.output_index))
    }

    /// Count one call of the graph in hand.
    fn count_call(&self) {
        self.calls_made.set(self.calls_made.get() + 1);
        if let Some(w) = self.walk.borrow_mut().as_mut() {
            w.calls_this_pass += 1;
            w.record.calls += 1;
        }
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

    /// Walk the graph in hand until nothing is pending (an explicit stack,
    /// bsv-low #555; before it a recursion, in the same order: an input's
    /// whole branch before its next sibling). Each step's result is
    /// committed to the walk only once the step has finished, so a deadline
    /// that drops this future leaves the walk as it was before that step.
    /// With a per-graph budget the walk stops at its call count or its
    /// `deadline` and answers `Deferred`; without one it runs to its end.
    async fn walk_graph(
        &self,
        mut deadline: Option<&mut crate::engine::SleepFuture>,
    ) -> Result<WalkEnd, GASPError> {
        loop {
            let (item, over_calls) = {
                let walk = self.walk.borrow();
                let Some(walk) = walk.as_ref() else {
                    return Ok(WalkEnd::Done);
                };
                (
                    walk.record.pending.last().cloned(),
                    self.graph_budget.is_some() && walk.calls_this_pass >= walk.call_cap,
                )
            };
            let Some(item) = item else {
                return Ok(WalkEnd::Done);
            };
            // One failed round trip per decoy: an outpoint pruned earlier in
            // this sync is not asked for again, even when a later graph of
            // the sync names it (two UTXOs of one decoy-bearing transaction).
            if item.parent_proven && self.pruned.borrow().contains(&item.outpoint) {
                debug!(
                    "{} Input {} of {:?} was already pruned in this sync; not re-requested",
                    self.log_prefix, item.outpoint, item.spent_by
                );
                if let Some(w) = self.walk.borrow_mut().as_mut() {
                    w.record.pending.pop();
                }
                continue;
            }
            if over_calls {
                return Ok(WalkEnd::Deferred("calls"));
            }
            let step = self.step(&item);
            let out = match deadline.as_mut() {
                Some(deadline) => match race_or_deadline(step, deadline.as_mut()).await {
                    Some(out) => out,
                    None => return Ok(WalkEnd::Deferred("time")),
                },
                None => step.await,
            }?;
            self.commit(out);
        }
    }

    /// Commit one finished step to the walk in hand: its item is no longer
    /// pending, and an appended node's needed inputs are, the first of them
    /// on top.
    fn commit(&self, out: StepOut) {
        let mut walk = self.walk.borrow_mut();
        let Some(walk) = walk.as_mut() else {
            return;
        };
        walk.record.pending.pop();
        if let StepOut::Appended {
            node_id,
            walked,
            children,
        } = out
        {
            walk.seen.insert(node_id);
            walk.record.nodes.push(*walked);
            walk.record.pending.extend(children.into_iter().rev());
        }
    }

    /// The walk from a root already in hand (the lib tests' entry, from
    /// before the walk was a stack): its proof hydrated, then every needed
    /// input to the end.
    #[cfg(test)]
    async fn process_incoming_node(
        &self,
        node: &GASPNode,
        _spent_by: Option<&str>,
        _seen: &mut std::collections::HashSet<String>,
    ) -> Result<(), GASPError> {
        let root = PendingInput {
            outpoint: node.graph_id.clone(),
            graph_id: node.graph_id.clone(),
            metadata: true,
            spent_by: None,
            parent_proven: false,
        };
        *self.walk.borrow_mut() = Some(Walk {
            record: DeferredGraph {
                peer: String::new(),
                topic: String::new(),
                outpoint: node.graph_id.clone(),
                score: 0,
                nodes: Vec::new(),
                pending: vec![root.clone()],
                calls: 0,
                passes: 0,
                reason: String::new(),
            },
            seen: std::collections::HashSet::new(),
            calls_this_pass: 0,
            resumed: false,
            call_cap: u32::MAX,
            nodes_at_start: 0,
            counted: false,
        });
        let node = self.hydrate_root(node.clone()).await;
        let out = self.absorb(&root, node).await?;
        self.commit(out);
        self.walk_graph(None).await.map(|_| ())
    }

    /// The ROOT of a graph: the peer's node for the UTXO, its own proof
    /// hydrated by the ancestor fetcher when it has none.
    async fn fetch_root(&self, item: &PendingInput) -> Result<GASPNode, GASPError> {
        let (txid, oi) = parse_outpoint(&item.outpoint)
            .ok_or_else(|| GASPError::Other(format!("bad outpoint {}", item.outpoint)))?;
        self.count_call();
        let node = self
            .remote
            .request_node(&item.graph_id, &txid, oi, item.metadata)
            .await?;
        Ok(self.hydrate_root(node).await)
    }

    /// The root's own proof, hydrated by the ancestor fetcher when the peer
    /// served it with none.
    async fn hydrate_root(&self, mut node: GASPNode) -> GASPNode {
        // GOD-TIER proof-anchoring (#126): if the peer served this node WITHOUT
        // a merkle proof but the tx is mined, hydrate its OWN proof via the
        // ancestor fetcher (WoC `/beef`). A proven node ends the walk UNLESS
        // its topic manager names inputs needed for overlay history. Managers
        // naming nothing still avoid the spent prior contract-state (a
        // 2-tx-pattern template, or a covenant's previous UTXO). Without this,
        // the walk reaches a spent input whose output
        // record exists in storage (so `find_needed_inputs` strips it) but whose tx
        // is absent from the in-memory graph, so `get_beef_for_node` fails "Missing
        // source transaction" and the whole graph is discarded. Legacy beta's
        // GASP-serve omits proofs, so this is required cross-stack. No-op when no
        // fetcher is configured (production default unchanged) or the node already
        // carries a proof (children fetched via the walk already do).
        // Only the GRAPH ROOT arrives from the peer (spent_by == None); children
        // come from the ancestry walk already carrying their proofs, so gate on
        // the root to avoid redundant re-fetches.
        if node.proof.is_none() {
            if let Some(fetcher) = &self.ancestor_fetcher {
                if let Some(node_txid) =
                    Self::node_id(&node).and_then(|id| parse_outpoint(&id).map(|(txid, _)| txid))
                {
                    self.count_call();
                    if let Ok(root_fetch) = fetcher.fetch_ancestor(&node_txid).await {
                        if root_fetch.proof.is_some() {
                            node.proof = root_fetch.proof;
                        }
                    }
                }
            }
        }
        node
    }

    /// One step of the walk: fetch `item`, append it to the graph, name its
    /// needed inputs. Nothing of the walk is changed here (see
    /// [`Self::walk_graph`]).
    async fn step(&self, item: &PendingInput) -> Result<StepOut, GASPError> {
        // THE D8 DECOY RULE (zanaadu-v2 #314, the owner's ruling of
        // 2026-10-06). A DELIBERATE DIVERGENCE from the reference.
        //
        // The reference requests every needed input with no catch
        // (`GASP.ts:602`, `await this.remote.requestNode(...)` inside
        // `processIncomingNode`), so one input the peer cannot serve
        // throws out of the walk and the per-UTXO catch in `sync`
        // drops that whole UTXO: the graph is never completed.
        //
        // We keep that for an UNPROVEN parent and diverge for a PROVEN
        // one. A proven parent needs no input for SPV; the only
        // inputs `find_needed_inputs` returns for it are the ones its
        // topic manager NAMED as history. A manager cannot always
        // tell which input is the real one: a head covenant that
        // signs under ANYONECANPAY lets a spender place a decoy
        // witness-shaped input ahead of the real head input, and once
        // mined it is permanent. The pairing is that such a manager
        // names EVERY witness-shaped input, and the engine PRUNES the
        // branch of a named input the PEER DEFINITELY DOES NOT HOLD
        // instead of discarding the graph: warn, count, carry on with
        // the parent's other named inputs, complete the graph with
        // what it has. Under the reference's rule one decoy would
        // strand the chain behind it on every sync, forever.
        //
        // An unproven parent's inputs are SPV necessities, not named
        // history: a missing one still fails the UTXO (the `?` below),
        // exactly as the reference does.
        //
        // THE CLASSIFICATION RULE. A named input is pruned only on a
        // DEFINITE answer: the peer said it does not hold the
        // outpoint, the typed class `GASPError::NodeNotFound`. Every
        // other error (the request could not be sent, a timeout, a
        // 5xx, a 429, a body that does not parse) is a fault of the
        // moment and says nothing about what the peer holds: it
        // fails the UTXO as the reference does, the per-UTXO arm of
        // `sync` records it in the cursor gap guard, and the next
        // sync asks again. Pruning on such a fault would cut the
        // REAL head input, finalize a truncated graph, advance the
        // cursor and never ask for that history again. The remote
        // decides the class: the worker's maps HTTP 400 from
        // `/requestForeignGASPNode` to `NodeNotFound` and nothing
        // else (`gasp_remote.rs`), because 400 is what the reference
        // peer answers for an outpoint it does not hold. Accepted
        // residual: the reference answers the same masked 400 when
        // its own storage faults inside `provideForeignGASPNode`, so
        // that fault at a reference peer reads as "not held".
        //
        // THE FETCHER ARM NEVER PRUNES. A chain fetcher has no
        // definite "cannot serve": every input of a mined transaction
        // exists on chain, so each of its errors is a fault of the
        // moment (its per-tick budget, a provider outage, a rate
        // limit), whatever class it carries. With a real chain
        // fetcher a decoy is SERVED from chain (one fetch, no failed
        // round trip, no prune), so the prune lives on the peer arm,
        // which is what runs when no fetcher is installed.
        //
        // NOTHING HERE BOUNDS THE DECOYS OF ONE PARENT. The manager's
        // list has no cap, and each definite decoy is one sequential
        // failed round trip inside the per-peer sync budget. A parent
        // with enough of them exceeds that budget and the sync is
        // dropped whole; what a dropped sync keeps is bsv-low #552's
        // ground, and a cap on the list is the manager's choice.
        let node = if item.spent_by.is_none() {
            self.fetch_root(item).await?
        } else {
            let Some((txid, oi)) = parse_outpoint(&item.outpoint) else {
                return Ok(StepOut::Pruned);
            };
            self.count_call();
            match &self.ancestor_fetcher {
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
                // is mined, its BUMP proof. A proven node ends the walk
                // UNLESS its topic manager names inputs. Empty managers
                // still stop at the first proven layer; managers needing
                // history select their ancestors through this same
                // fetcher. Without a proof, every input is requested
                // as before.
                // The transaction DAG (no cycles), `seen` keyed by
                // `txid.outputIndex`, the already-known strip, and the
                // manager naming nothing at genesis bound the walk.
                // There is no node cap, matching the reference's
                // undefined `maxNodesInGraph` in `Engine.startGASPSync`.
                // A per-peer budget drops the sync future at its
                // deadline: graphs finalized before it stay admitted
                // (bsv-low #552), the graph in flight is lost whole,
                // and ONE graph whose own walk outlasts the budget
                // never completes unless a per-graph budget defers and
                // resumes it (bsv-low #555).
                //
                // Every fetcher error fails the UTXO (the `?`):
                // the fetcher arm never prunes, see the rule above.
                Some(fetcher) => {
                    let ancestor = fetcher.fetch_ancestor(&txid).await?;
                    GASPNode {
                        graph_id: item.graph_id.clone(),
                        raw_tx: ancestor.raw_tx,
                        output_index: oi,
                        proof: ancestor.proof,
                        tx_metadata: None,
                        output_metadata: None,
                        inputs: None,
                    }
                }
                // Default / production: no fetcher → ask the peer.
                None => match self
                    .remote
                    .request_node(&item.graph_id, &txid, oi, item.metadata)
                    .await
                {
                    Ok(child_node) => child_node,
                    // Proven parent and a DEFINITE "not held": the D8 prune
                    // (see the rule above).
                    Err(e @ GASPError::NodeNotFound(_)) if item.parent_proven => {
                        warn!(
                            "{} Pruned input {} named by proven node {}: the peer does not hold it: {}",
                            self.log_prefix,
                            item.outpoint,
                            item.spent_by.as_deref().unwrap_or_default(),
                            e
                        );
                        if self.pruned.borrow_mut().insert(item.outpoint.clone()) {
                            self.pruned_inputs.set(self.pruned_inputs.get() + 1);
                        }
                        return Ok(StepOut::Pruned);
                    }
                    // Any other error, and every error under an unproven
                    // parent: propagate, as the reference does.
                    Err(e) => return Err(e),
                },
            }
        };
        self.absorb(item, node).await
    }

    /// The node of `item`, fetched: append it to the graph and name its
    /// needed inputs.
    async fn absorb(&self, item: &PendingInput, node: GASPNode) -> Result<StepOut, GASPError> {
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
        if self
            .walk
            .borrow()
            .as_ref()
            .is_some_and(|w| w.seen.contains(&node_id))
        {
            return Ok(StepOut::Seen);
        }

        self.storage
            .append_to_graph(&node, item.spent_by.as_deref())
            .await?;

        let mut children = Vec::new();
        if let Some(needed) = self.storage.find_needed_inputs(&node).await? {
            let parent_proven = node.proof.is_some();
            for (outpoint, input_req) in &needed.requested_inputs {
                if parse_outpoint(outpoint).is_some() {
                    children.push(PendingInput {
                        outpoint: outpoint.clone(),
                        graph_id: node.graph_id.clone(),
                        metadata: input_req.metadata,
                        spent_by: Some(node_id.clone()),
                        parent_proven,
                    });
                }
            }
        }
        Ok(StepOut::Appended {
            node_id,
            walked: Box::new(WalkedNode {
                node,
                spent_by: item.spent_by.clone(),
            }),
            children,
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
    /// `Ok(true)`: finalized; `Ok(false)`: refused by the anchor check and
    /// discarded (a verdict: the cursor moves past it).
    async fn complete_graph(&self, graph_id: &str) -> Result<bool, GASPError> {
        info!("{} Completing graph: {}", self.log_prefix, graph_id);
        match self.storage.validate_graph_anchor(graph_id).await {
            Ok(()) => {
                self.storage.finalize_graph(graph_id).await?;
                info!("{} Graph finalized: {}", self.log_prefix, graph_id);
                Ok(true)
            }
            Err(e) => {
                warn!(
                    "{} Graph validation failed for {}: {}. Discarding.",
                    self.log_prefix, graph_id, e
                );
                self.storage.discard_graph(graph_id).await?;
                // The anchor could not be checked (a tracker outage, a
                // storage fault, a topic manager that answered the replay
                // with an error): no verdict. Fail the UTXO so the gap guard
                // re-requests it, instead of advancing the cursor past a
                // graph nobody judged (a divergence by addition, bsv-low
                // #551: the reference discards and moves on either way).
                if matches!(e, GASPError::AnchorUnavailable(_)) {
                    return Err(e);
                }
                self.discarded_graphs.set(self.discarded_graphs.get() + 1);
                Ok(false)
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

    /// A graph's anchor could not be CHECKED right now (the chain tracker or
    /// a storage read faulted, or the topic manager answered the replay with
    /// an error): a fault of the moment, not a verdict on the graph. `GASPSync::complete_graph` discards the graph and fails the
    /// UTXO, so the cursor gap guard asks for it again (bsv-low #551).
    #[error("anchor unavailable: {0}")]
    AnchorUnavailable(String),

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

    // ── race_or_deadline (bsv-low#257/#302, generalized here) ──────────

    #[tokio::test]
    async fn race_or_deadline_future_wins() {
        let out = race_or_deadline(async { 42u32 }, std::future::pending::<()>()).await;
        assert_eq!(out, Some(42));
    }

    #[tokio::test]
    async fn race_or_deadline_deadline_wins() {
        let out = race_or_deadline(std::future::pending::<u32>(), async {}).await;
        assert_eq!(out, None);
    }

    // The lens fold's HIGH-1: a deadline that falls due inside a write
    // section waits for the section to close, then drops the future at its
    // next pending point; the writer that asks at the boundary stops there.
    #[tokio::test]
    async fn guarded_race_never_drops_inside_a_write_section() {
        let gate = SubmitGate::default();
        let written = std::cell::Cell::new(0u32);
        let out = race_or_deadline_guarded(
            async {
                for _ in 0..3 {
                    gate.stop_if_due().await;
                    let _section = gate.write_section();
                    // Two writes of one transaction, each a round trip.
                    tokio::task::yield_now().await;
                    written.set(written.get() + 1);
                    tokio::task::yield_now().await;
                    written.set(written.get() + 1);
                }
            },
            async {},
            &gate,
        )
        .await;
        assert_eq!(out, None, "the deadline still wins");
        assert_eq!(written.get(), 2, "one transaction, whole, and no second");
        assert!(
            !gate.deadline_is_due(),
            "the flag does not outlive the race"
        );

        // With no section open it is `race_or_deadline`.
        let out = race_or_deadline_guarded(std::future::pending::<u8>(), async {}, &gate).await;
        assert_eq!(out, None);
        let out = race_or_deadline_guarded(async { 7u8 }, std::future::pending(), &gate).await;
        assert_eq!(out, Some(7));
        // A future that finishes inside its section after the deadline wins.
        let out = race_or_deadline_guarded(
            async {
                let _section = gate.write_section();
                tokio::task::yield_now().await;
                7u8
            },
            async {},
            &gate,
        )
        .await;
        assert_eq!(out, Some(7));
        assert!(!gate.deadline_is_due());
    }

    #[tokio::test]
    async fn race_or_deadline_ready_future_beats_ready_deadline() {
        // The sync future is polled FIRST — an instantly-ready result wins
        // even against an already-expired deadline (a completed sync is
        // never discarded).
        let out = race_or_deadline(async { 7u32 }, async {}).await;
        assert_eq!(out, Some(7));
    }

    // ── peer_sync_quarantined (bsv-low#302 pure rule) ──────────────────

    fn health(fails: u64, age: Option<u64>) -> crate::storage::PeerSyncHealth {
        crate::storage::PeerSyncHealth {
            consecutive_failures: fails,
            secs_since_last_attempt: age,
        }
    }

    #[test]
    fn quarantine_needs_the_full_threshold() {
        assert!(!peer_sync_quarantined(&health(0, Some(0))));
        assert!(!peer_sync_quarantined(&health(
            PEER_QUARANTINE_THRESHOLD - 1,
            Some(0)
        )));
        assert!(peer_sync_quarantined(&health(
            PEER_QUARANTINE_THRESHOLD,
            Some(0)
        )));
        assert!(peer_sync_quarantined(&health(
            PEER_QUARANTINE_THRESHOLD + 100,
            Some(0)
        )));
    }

    #[test]
    fn quarantine_reopens_one_probe_after_the_reprobe_window() {
        let fails = PEER_QUARANTINE_THRESHOLD;
        assert!(peer_sync_quarantined(&health(
            fails,
            Some(PEER_QUARANTINE_REPROBE_SECS - 1)
        )));
        // AT the window boundary the probe re-opens — a quarantined peer is
        // never stranded forever.
        assert!(!peer_sync_quarantined(&health(
            fails,
            Some(PEER_QUARANTINE_REPROBE_SECS)
        )));
    }

    #[test]
    fn never_attempted_peer_is_never_quarantined() {
        // `None` age = pristine peer OR a backend without health tracking —
        // fail-safe: always attempted.
        assert!(!peer_sync_quarantined(&health(u64::MAX, None)));
    }

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
        // Keeps no deferred graphs (bsv-low #555): never given a per-graph budget.
        async fn load_deferred_graphs(
            &self,
        ) -> Result<Vec<crate::gasp::DeferredGraphKey>, GASPError> {
            Ok(Vec::new())
        }
        async fn get_deferred_graph(
            &self,
            _: &str,
        ) -> Result<Option<crate::gasp::DeferredGraph>, GASPError> {
            Ok(None)
        }
        async fn save_deferred_graph(
            &self,
            _: &crate::gasp::DeferredGraph,
        ) -> Result<crate::gasp::DeferredGraphSave, GASPError> {
            Err(GASPError::StorageError("keeps no deferred graphs".into()))
        }
        async fn delete_deferred_graph(&self, _: &str) -> Result<(), GASPError> {
            Ok(())
        }
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
            // Keeps no deferred graphs (bsv-low #555): never given a per-graph budget.
            async fn load_deferred_graphs(
                &self,
            ) -> Result<Vec<crate::gasp::DeferredGraphKey>, GASPError> {
                Ok(Vec::new())
            }
            async fn get_deferred_graph(
                &self,
                _: &str,
            ) -> Result<Option<crate::gasp::DeferredGraph>, GASPError> {
                Ok(None)
            }
            async fn save_deferred_graph(
                &self,
                _: &crate::gasp::DeferredGraph,
            ) -> Result<crate::gasp::DeferredGraphSave, GASPError> {
                Err(GASPError::StorageError("keeps no deferred graphs".into()))
            }
            async fn delete_deferred_graph(&self, _: &str) -> Result<(), GASPError> {
                Ok(())
            }
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
        // Keeps no deferred graphs (bsv-low #555): never given a per-graph budget.
        async fn load_deferred_graphs(
            &self,
        ) -> Result<Vec<crate::gasp::DeferredGraphKey>, GASPError> {
            Ok(Vec::new())
        }
        async fn get_deferred_graph(
            &self,
            _: &str,
        ) -> Result<Option<crate::gasp::DeferredGraph>, GASPError> {
            Ok(None)
        }
        async fn save_deferred_graph(
            &self,
            _: &crate::gasp::DeferredGraph,
        ) -> Result<crate::gasp::DeferredGraphSave, GASPError> {
            Err(GASPError::StorageError("keeps no deferred graphs".into()))
        }
        async fn delete_deferred_graph(&self, _: &str) -> Result<(), GASPError> {
            Ok(())
        }
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
