//! Overlay Services Engine — the core orchestrator.
//!
//! Receives transactions (submit), answers queries (lookup), manages advertisements,
//! and coordinates GASP sync. All storage, topic management, and lookup service
//! operations go through trait interfaces.
//!
//! Ported from `~/bsv/overlay-services/src/Engine.ts` (1,337 lines).

use bsv_rs::transaction::{Beef, Transaction};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tracing::{error, info, warn};

use crate::advertiser::{Advertiser, AdvertiserError};
use crate::broadcaster::{ArcBroadcaster, Broadcaster};
use crate::lookup_service::{LookupService, LookupServiceError};
use crate::storage::{Storage, StorageError};
use crate::topic_manager::{TopicManager, TopicManagerError};
use crate::types::*;

/// Controls how much spend history to include in lookup responses.
///
/// Maps to the TS `historySelector` parameter which can be a number
/// (depth limit) or an async function (per-output decider).
#[derive(Debug, Clone)]
pub enum HistorySelector {
    /// Include up to N levels of ancestor spend history.
    Depth(u32),
}

/// Configuration for constructing the Engine.
pub struct EngineConfig {
    /// URL where this engine is hosted. Required for advertisement sync.
    pub hosting_url: Option<String>,
    /// Known SHIP tracker URLs for bootstrapping.
    pub ship_trackers: Vec<String>,
    /// Known SLAP tracker URLs for bootstrapping.
    pub slap_trackers: Vec<String>,
    /// Configuration for GASP topic synchronization.
    pub sync_configuration: SyncConfiguration,
    /// Whether to suppress default SHIP/SLAP sync advertisements.
    pub suppress_default_sync_advertisements: bool,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            hosting_url: None,
            ship_trackers: Vec::new(),
            slap_trackers: Vec::new(),
            sync_configuration: HashMap::new(),
            suppress_default_sync_advertisements: true,
        }
    }
}

/// The Overlay Services Engine.
///
/// Orchestrates topic managers, lookup services, storage, and advertisements.
/// Does NOT include an HTTP server — that's the job of overlay-cloudflare.
pub struct Engine {
    managers: HashMap<String, Box<dyn TopicManager>>,
    lookup_services: HashMap<String, Box<dyn LookupService>>,
    storage: Box<dyn Storage>,
    advertiser: Option<Box<dyn Advertiser>>,
    broadcaster: Option<Box<dyn Broadcaster>>,
    arc_broadcaster: Option<Box<dyn ArcBroadcaster>>,
    chain_tracker: Option<Box<dyn bsv_rs::transaction::ChainTracker>>,
    gasp_remote_factory: Option<Box<dyn crate::gasp::GASPRemoteFactory>>,
    /// OPT-IN / OFF BY DEFAULT chain-backed ancestor fetcher for GASP ingest.
    /// When `None` (default / production), GASP ingest is byte-identical to
    /// today. A platform crate may set `Some` (one-time-migration only) to let
    /// ingest self-heal missing ancestry by fetching it from chain.
    ancestor_fetcher: Option<std::rc::Rc<dyn crate::gasp::AncestorFetcher>>,
    /// bsv-low#302: per-peer GASP sync wall-clock budget —
    /// `(sleep factory, budget ms)`. `None` (default) = unbounded per-peer
    /// sync, byte-identical to the pre-#302 behavior. When set, each peer's
    /// `GASPSync::sync` is raced against `sleep(budget_ms)`; a peer that
    /// exceeds the budget is DROPPED (loud log, failure recorded, cursor NOT
    /// advanced) and the loop continues with the next peer.
    peer_sync_budget: Option<(SleepFactory, u64)>,
    /// The per-GRAPH GASP budget (bsv-low #555): the sleep, the calls, the
    /// ms. `None`: no graph is deferred.
    graph_budget: Option<(SleepFactory, u32, u64)>,
    /// The bound of one transaction's finalize submit (bsv-low #559), see
    /// [`Engine::set_finalize_submit_budget`].
    finalize_submit_budget: Option<(SleepFactory, u64)>,
    /// Open around each GASP finalize submit, so no deadline that races the
    /// sync drops a transaction between its writes (bsv-low #552, the lens
    /// fold's HIGH-1). See [`Engine::finalize_submit_gate`].
    finalize_gate: crate::gasp::SubmitGate,
    /// (txid, topic) pairs whose Phase-3 writes FAULTED in this engine and
    /// have not landed since (bsv-low #559): a successor that finds no coin
    /// while its predecessor is in here is not recorded as applied. One
    /// invocation's memory; across invocations the same question is put to
    /// the store ([`Engine::unlanded_predecessor`]).
    not_landed: std::cell::RefCell<HashSet<(String, String)>>,
    /// Asked before the door lands a carried predecessor (bsv-low #575, the
    /// E1D delta-2 fold, L2). See [`Engine::set_landing_guard`].
    landing_guard: Option<LandingGuard>,
    /// Reference-parity spend verification on submit (2026-09-08). `true`
    /// (DEFAULT): every submit outside `HistoricalTxNoSpv` runs the
    /// reference's `Transaction.verify` walk: merkle paths against the chain
    /// tracker AND every input's unlocking script EXECUTED, recursively over
    /// the unproven ancestry in the BEEF; with no chain tracker the walk is
    /// the ts-sdk's `'scripts only'` (roots accepted unchecked, scripts still
    /// run). `false` is an ESCAPE HATCH, not a mode: it restores the
    /// pre-2026-09-08 structural check (`Beef::verify_valid` + every root
    /// against the tracker; NOTHING without a tracker), which admitted an
    /// invalid spend on structure alone. See
    /// [`Engine::set_script_verification`].
    verify_scripts: bool,
    config: EngineConfig,
}

/// Boxed platform sleep future (bsv-low#302). Not `Send` — the engine runs
/// single-threaded on wasm (Workers) and under `#[tokio::test]` locally.
pub type SleepFuture = std::pin::Pin<Box<dyn std::future::Future<Output = ()>>>;

/// Platform sleep factory (bsv-low#302): milliseconds → a future that
/// resolves after that delay. The Cloudflare crate passes its `sleep_ms`;
/// tests pass `ready(())` (instant deadline) or `pending()` (no deadline)
/// to drive the race deterministically.
pub type SleepFactory = std::rc::Rc<dyn Fn(u64) -> SleepFuture>;

/// The answer of a [`LandingGuard`]: `Ok` lets the body land, `Err` says why
/// it may not (the topic waits).
pub type LandingGuardFuture =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>>>>;

/// The caller's admission predicate over a carried predecessor the door is
/// about to land: `(predecessor txid, topic)`. See
/// [`Engine::set_landing_guard`].
pub type LandingGuard = std::rc::Rc<dyn Fn(&str, &str) -> LandingGuardFuture>;

/// The bound of ONE GASP finalize submit's storage calls and hooks (bsv-low
/// #559, the lens fold of 2026-10-07, F2). The submit itself is never
/// dropped: it is the write section no deadline drops, and c9921ee, which
/// raced the whole submit against its budget inside the section, dropped it
/// between two writes all the same. Each storage call, lookup hook and topic
/// manager call inside it is raced against the section's ONE deadline
/// instead, and a call that has not answered when the deadline falls due is
/// dropped ALONE and reported as that call's fault: the submit runs on to
/// its end as for any storage fault. Once the deadline is due no further
/// call is started (each answers "no answer" at once), so a transaction
/// costs at most its budget, and once more for the undo
/// ([`CallBound::renew`]).
struct CallBound {
    sleep: SleepFactory,
    budget_ms: u64,
    deadline: std::cell::RefCell<SleepFuture>,
    due: std::cell::Cell<bool>,
}

impl CallBound {
    fn new(sleep: &SleepFactory, budget_ms: u64) -> Self {
        Self {
            deadline: std::cell::RefCell::new(sleep(budget_ms)),
            sleep: sleep.clone(),
            budget_ms,
            due: std::cell::Cell::new(false),
        }
    }

    /// A fresh allowance of the same length, for the undo of a faulted
    /// submit: the calls that put the store back as it was found must not be
    /// refused because the deadline the fault answered to has passed.
    fn renew(&self) {
        *self.deadline.borrow_mut() = (self.sleep)(self.budget_ms);
        self.due.set(false);
    }

    /// Whether a call under this bound went unanswered (or was not started
    /// because one had) since the last [`CallBound::renew`].
    fn is_due(&self) -> bool {
        self.due.get()
    }

    /// `call`, or `None` when the deadline fell due first.
    async fn call<T>(&self, call: impl std::future::Future<Output = T>) -> Option<T> {
        if self.due.get() {
            return None;
        }
        let mut call = std::pin::pin!(call);
        std::future::poll_fn(|cx| {
            if let std::task::Poll::Ready(v) = call.as_mut().poll(cx) {
                return std::task::Poll::Ready(Some(v));
            }
            if self.deadline.borrow_mut().as_mut().poll(cx).is_ready() {
                self.due.set(true);
                return std::task::Poll::Ready(None);
            }
            std::task::Poll::Pending
        })
        .await
    }
}

/// `call` under `bound` (or unbounded with none); `Err` is "no answer".
async fn bounded<T>(
    bound: Option<&CallBound>,
    call: impl std::future::Future<Output = T>,
) -> Result<T, String> {
    match bound {
        Some(bound) => bound
            .call(call)
            .await
            .ok_or_else(|| format!("no answer within {} ms", bound.budget_ms)),
        None => Ok(call.await),
    }
}

/// A storage call under `bound`: no answer in time is that call's fault.
async fn stored<T>(
    bound: Option<&CallBound>,
    call: impl std::future::Future<Output = Result<T, StorageError>>,
) -> Result<T, StorageError> {
    match bounded(bound, call).await {
        Ok(answer) => answer,
        Err(no_answer) => Err(StorageError::Database(no_answer)),
    }
}

/// A lookup hook under `bound`, its error as the report carries it.
async fn hooked(
    bound: Option<&CallBound>,
    call: impl std::future::Future<Output = Result<(), LookupServiceError>>,
) -> Result<(), String> {
    bounded(bound, call).await?.map_err(|e| e.to_string())
}

/// The most store reads [`Engine::unlanded_predecessor`] makes for one
/// topic of one submit (bsv-low #559, F3): the question is asked of every
/// transaction that admits nothing and found no coin, so it is kept cheap.
/// Counted are the reads spent on a body the BEEF does not prove and the
/// store does not hold as landed (the second delta fold of 2026-10-07, M2);
/// the 17th of those is never made: out of reads is "not now" (the delta
/// fold of 2026-10-07, M2). An unproven body still needs room under it for
/// the reads that find it landed (one for an applied row, two for a held
/// output): each is tested before it is made.
const PREDECESSOR_READS: usize = 16;

/// The most store reads [`Engine::unlanded_predecessor`] makes for one
/// SUBMIT (the third delta fold of 2026-10-07, M1): every read of the
/// question, counted against [`PREDECESSOR_READS`] or not (those of proven
/// and landed bodies included), in every topic of the submit together. The
/// 257th is never made: past the allowance the answer is "not now", never
/// "landed". Without it the question read as far as the BEEF reached (one
/// read per 41 bytes: 66,305 reads measured from one 2.7 MB BEEF), on a
/// public route. Per submit and not per topic because what it protects, the
/// invocation's D1 allowance, is per invocation; 256 is the door's own unit
/// ([`DoorBudget`]'s inputs per transaction), no derived figure.
const PREDECESSOR_READS_PER_SUBMIT: usize = 256;

/// Why a successor must wait ([`Engine::unlanded_predecessor`]), and the
/// unlanded predecessor whose body the BEEF carries, when the answer names
/// one: the door lands it first ([`Engine::land_carried`]).
/// A submit's STEAK and report, and the carried predecessor that blocks each
/// topic of a carried predecessor's own submit ([`Engine::submit_counted`]).
type Counted = (Steak, MutationReport, HashMap<String, String>);

struct NotNow {
    why: String,
    carried: Option<String>,
}

/// Summary of an [`Engine::complete_missing_proofs`] pass.
///
/// All counts are over the single bounded page scanned this tick. A no-fetcher
/// (production) build returns the all-zero default.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ProofCompletionSummary {
    /// Stored transactions inspected this tick (the bounded page size).
    pub scanned: usize,
    /// Of `scanned`, how many still lacked a merkle proof for their own tx.
    pub proofless: usize,
    /// Proofless txs whose BUMP was fetched + stitched into the stored BEEF.
    pub completed: usize,
    /// Proofless txs the fetcher could serve but that are not yet mined
    /// (no BUMP yet) — retried on a later tick.
    pub still_unconfirmed: usize,
    /// Proofless txs the fetcher errored on (e.g. budget exhausted, 429) —
    /// retried on a later tick.
    pub fetch_failed: usize,
    /// Proofless txs whose proof came back but failed to stitch into storage.
    pub stitch_failed: usize,
    /// Scanned rows that ALREADY carried a valid proof (stale `has_proof = 0`
    /// flag) and were marked proven this tick so they drop out of the candidate
    /// window — clearing the window-clog (#130).
    pub already_proven: usize,
}

/// Outcome of one [`Engine::sync_advertisements`] run (bsv-low #320 defect
/// 3a).
///
/// The engine used to swallow create/submit failures into `error!` and
/// return `Ok(())`, so the admin route reported `success` while zero
/// advertisements were admitted locally; a caller could not tell a
/// converged no-op (`to_create == 0`) from a silent failure. Every field
/// here is observational — the sync's behavior is unchanged, its outcome is
/// no longer hidden.
#[derive(Debug, Default, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncAdvertisementsReport {
    /// Advertisements the diff found missing (attempted this run). A
    /// converged node reports 0 here.
    pub to_create: usize,
    /// Stale advertisements the diff said to revoke.
    pub to_revoke: usize,
    /// `advertiser.create_advertisements` failure, verbatim.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create_error: Option<String>,
    /// Local `Engine::submit` failure for the created ads, verbatim — the
    /// path that silently hid the self-admission failure live (our own
    /// ls_ship/ls_slap never gained our ads, so every cycle re-created all
    /// of them).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub submit_error: Option<String>,
    /// Outputs admitted per topic by the local submit (from the STEAK).
    /// A successful submit that admitted NOTHING (topic-manager refusal)
    /// shows up here as explicit zeros.
    pub admitted: std::collections::BTreeMap<String, usize>,
    /// `advertiser.find_all_advertisements` failure, verbatim (#320 M2).
    /// When set, creation was REFUSED this run: a blind read must never
    /// become "current ads = none" and re-create (re-pay) the entire set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lookup_error: Option<String>,
    /// Stale ads the diff wanted revoked but the advertiser declined to
    /// build a revocation for (the CF advertiser's documented v1 no-op —
    /// #320 L2). Distinguishes "revoke happened" from "revoke skipped".
    pub revoke_skipped: usize,
    /// `advertiser.revoke_advertisements` failure, verbatim.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revoke_error: Option<String>,
    /// Local `Engine::submit` failure for the revocation, verbatim.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revoke_submit_error: Option<String>,
}

impl SyncAdvertisementsReport {
    /// True when nothing errored. NOTE: a submit that admitted zero outputs
    /// is not an error at this layer — that refusal is [`Self::effective`]'s
    /// job.
    #[must_use]
    pub fn ok(&self) -> bool {
        self.create_error.is_none()
            && self.submit_error.is_none()
            && self.lookup_error.is_none()
            && self.revoke_error.is_none()
            && self.revoke_submit_error.is_none()
    }

    /// Total outputs admitted by the local submit across topics.
    #[must_use]
    pub fn admitted_total(&self) -> usize {
        self.admitted.values().sum()
    }

    /// True when the run errored nowhere AND the local submit admitted
    /// EVERY created ad (#320 M1 + delta D1): each advertisement is exactly
    /// one output and change is never a SHIP/SLAP token, so full admission
    /// means `admitted_total() >= to_create` (defensive `>=`). A weaker
    /// `> 0` bar let a PARTIAL admit pass — the live set is 9 SHIP + 8 SLAP
    /// in ONE tx with independent per-topic admission, so tm_ship admitting
    /// its 9 while tm_slap refused all 8 read as success while the diff
    /// re-created (re-paid for) the refused 8 every cycle. Subsumes the
    /// converged no-op (`0 >= 0`).
    #[must_use]
    pub fn effective(&self) -> bool {
        self.ok() && self.admitted_total() >= self.to_create
    }
}

/// Internal result from Phase 1 + 2 validation.
///
/// Carries per-topic admittance decisions, previous coins, and the parsed
/// transaction so that Phase 3 (mutations) can proceed without re-parsing.
struct TopicValidation {
    topic: String,
    is_dupe: bool,
    /// Input indices that spend previously-admitted outputs from this topic.
    previous_coins: Vec<u32>,
    /// The previous outputs found in storage (parallel to previous_coins).
    previous_outputs: Vec<Output>,
    admittance: AdmittanceInstructions,
    failed: bool,
    /// The FIRST storage read fault hit while scanning this topic's
    /// previous outputs (S2, bsv-low 2026-08-29). A faulted read makes a
    /// spend look like it consumes nothing — the settle's spend pointer is
    /// then never written — so it is carried into the [`MutationReport`]
    /// and blocks the `applied_transactions` record for this topic: a
    /// replay re-reads instead of being deduplicated away. With the read
    /// that faulted: `find_output` (a previous coin) or
    /// `does_applied_transaction_exist` (the dedup read, the delta fold of
    /// 2026-10-07, H3).
    read_fault: Option<(&'static str, String)>,
}

/// One Phase-3 write — or the validation read it depends on — that FAILED
/// during a submit (S2 queue-durable admission, bsv-low 2026-08-29).
///
/// Before this existed every such failure was `error!`-logged and swallowed
/// while `submit` returned `Ok(steak)`: under a D1-overload storm the
/// overlay ACKED admissions whose writes never landed (the 2026-08-26
/// phantom class — `c4d2ed06…`/`62c2a9b3…` absent from the engine store,
/// beta D1 queried directly). A fault is now a VALUE the caller can act on
/// (re-queue for an idempotent replay) instead of a log line nobody reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationFault {
    /// The topic whose mutation faulted.
    pub topic: String,
    /// The write (or read) that failed: `insert_output`,
    /// `mark_utxo_as_spent`, `update_consumed_by`, `delete_utxo_deep`,
    /// `insert_applied_transaction`, `find_output` (validation read),
    /// `does_applied_transaction_exist` (the dedup read),
    /// `lookup_service.output_spent`, `lookup_service.output_admitted_by_topic`,
    /// `undo_insert_output` (the undo of a faulted submit's inserts),
    /// `record_spent_coin_applied` (the applied row a spender writes for the
    /// transaction of a coin it deletes), `predecessor_not_landed`.
    pub site: &'static str,
    /// The backend's own error text.
    pub error: String,
}

/// What Phase 3 actually landed for a submit — the durability report.
///
/// `applied_topics` are the topics whose EVERY write (and lookup-service
/// notification) succeeded and were therefore recorded in
/// `applied_transactions`. A topic with any fault is deliberately NOT
/// recorded: the dedup in Phase 1 would otherwise turn a replay into a
/// no-op and make the loss permanent (the ordering was already
/// write-then-record; the record is now conditional on the writes).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MutationReport {
    pub faults: Vec<MutationFault>,
    pub applied_topics: Vec<String>,
    /// Topics this (txid, topic) pair had ALREADY been applied under — the
    /// Phase-1 dedup skipped them, so their STEAK entry is an empty default
    /// that means "nothing new", never "judged another tx" (loop-3 client
    /// belt re-presents read the empty STEAK as index-pending).
    pub deduped_topics: Vec<String>,
    /// The predecessors the door LANDED first from this submit's BEEF, as
    /// `(txid, topic)`, ancestors first ([`Engine::land_carried`]; the E1D
    /// delta fold, L2). Each is an admission write of its own that no
    /// caller asked for: a caller that guards its admission writes (the
    /// worker's eviction ledger, bsv-low #513) guards these too.
    pub landed_predecessors: Vec<(String, String)>,
}

/// bsv-low PLAN-PRE-LOOP4 §H4 (2026-09-06): what [`Engine::renotify_admitted`] did.
#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize)]
pub struct RenotifyReport {
    /// stored outputs of (txid, topic) the engine had ALREADY admitted
    pub outputs: u32,
    /// lookup-service notifications that returned Ok (outputs × services)
    pub notified: u32,
    /// notifications that failed — `<service>: <error>`; the others still ran
    pub faults: Vec<String>,
    /// the output indexes re-notified (the caller's spend discovery keys on them)
    pub vouts: Vec<u32>,
}

impl MutationReport {
    fn fault(&mut self, topic: &str, site: &'static str, error: String) {
        self.faults.push(MutationFault {
            topic: topic.to_string(),
            site,
            error,
        });
    }

    /// True when every mutation this submit needed landed (nothing to
    /// replay). A dupe-only submit is durable (there was nothing to write).
    #[must_use]
    pub fn is_durable(&self) -> bool {
        self.faults.is_empty()
    }

    /// One log line naming every fault (`topic/site: error`), each error
    /// clipped so a D1 stack dump cannot flood the log stream.
    #[must_use]
    pub fn summary(&self) -> String {
        self.faults
            .iter()
            .map(|f| {
                let err: String = f.error.chars().take(120).collect();
                format!("{}/{}: {}", f.topic, f.site, err)
            })
            .collect::<Vec<_>>()
            .join("; ")
    }
}

/// What a merkle path proves during the linear submit walk
/// ([`Engine::verify_beef_linear`] and [`Engine::verify_scripts_only`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RootPolicy {
    /// The reference's `tx.verify(chainTracker)`: a proven transaction's root
    /// is checked against the chain tracker when one is configured.
    AgainstTracker,
    /// The reference's `tx.verify('scripts only')`: a proven transaction is
    /// trusted as-is; no root is computed and no tracker is consulted.
    AcceptUnchecked,
}

/// The DOOR's static work bound (bsv-low W-A gate MED-1, 2026-09-09). Bitcoin
/// script has no loops: every opcode runs at most once, so an input's work is
/// bounded BEFORE execution by its opcode census times the interpreter's
/// element limit (the stack memory limit bounds every element, and every
/// CAT / NUM2BIN / SPLIT result with it) plus its signature checks times the
/// transaction's own size (a BIP-143 preimage hashes the prevouts and the
/// outputs). The estimate is computed from the bytes; nothing executes past
/// the budget, and a budget breach is the DOOR's verdict (inconclusive: the
/// network judges), never the interpreter's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DoorBudget {
    /// Unproven transactions the walk may execute (the subject + its ancestry).
    pub max_unproven_txs: usize,
    /// Inputs per unproven transaction (the walk clones the other inputs per
    /// input: O(n²) in a transaction's inputs).
    pub max_inputs_per_tx: usize,
    /// Serialized bytes of one unproven transaction (bounds every preimage).
    pub max_tx_bytes: usize,
    /// The interpreter's stack memory limit: bounds every element.
    pub memory_limit: usize,
    /// The whole walk's estimated work, in bytes hashed or pushed.
    pub max_work_bytes: u64,
}

impl DoorBudget {
    /// Sized for LOW's real shapes with a wide margin: a 30-deep P2PKH hop
    /// ancestry plus one 3.5 KB OP_PUSH_TX covenant spend estimates under
    /// 8 MB. What the static census BOUNDS: hashing (hash-class ops × the
    /// element limit) and signature checks (× the transaction's size; a
    /// CHECKMULTISIG weighted by its static key count when the count is a
    /// small-int push, else by the most keys the element limit admits).
    ///
    /// THE OPEN RESIDUAL (the W-A gate's delta-verify, 2026-09-09, measured):
    /// the interpreter itself meters nothing, so three classes stay charged
    /// only by their script bytes — bignum arithmetic (`OP_MUL`/`OP_DIV`/
    /// `OP_MOD` on operands up to the element limit: ~1.3 ms per 30 KB×30 KB
    /// round, linear in rounds), a CHECKMULTISIG whose key count is a computed
    /// value (~80 µs per EC verify per key tried), and `OP_NUM2BIN`, which
    /// allocates its size operand BEFORE the memory limit is consulted (up to
    /// bsv-rs's 1 GB element size: an isolate kill, not a refusal). None of
    /// them can refuse a spend (a breach and a trip are the door's bound and
    /// the request proceeds); all of them cost the beta overlay CPU until
    /// bsv-rs meters work dynamically (a `work_limit` charged per op with the
    /// real operand sizes, and a pre-allocation check in NUM2BIN) — OWED
    /// before any PROD flip of `SCRIPT_VERIFY_NETWORK_GATED`.
    pub const DEFAULT: DoorBudget = DoorBudget {
        max_unproven_txs: 64,
        max_inputs_per_tx: 256,
        max_tx_bytes: 512 * 1024,
        memory_limit: 128 * 1024,
        max_work_bytes: 64 * 1024 * 1024,
    };
}

/// What the walk did (the door's cost instrument, since `Date.now()` is
/// frozen during synchronous work on Workers and cannot time it).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WalkStats {
    /// Unproven transactions whose inputs were executed.
    pub unproven_txs: usize,
    /// Inputs executed (scripts run).
    pub inputs_executed: usize,
    /// Unlocking + locking script bytes executed.
    pub script_bytes: usize,
    /// Hash-class opcodes seen (SHA1/SHA256/HASH160/HASH256/RIPEMD160).
    pub hash_ops: usize,
    /// Signature checks seen (CHECKSIG counts 1, CHECKMULTISIG counts its
    /// consensus maximum of 20).
    pub sig_ops: usize,
    /// The static work estimate charged against the budget.
    pub work_bytes: u64,
    /// Whether the SUBJECT's own inputs were all executed (an ancestor may
    /// still have faulted afterwards).
    pub subject_judged: bool,
}

/// How the linear walk is run: the reference's walk (no budget; structural
/// faults are `SpvError`) or the door's (`'scripts only'`, a budget, and the
/// door's own error classes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WalkPolicy {
    roots: RootPolicy,
    budget: Option<DoorBudget>,
}

/// Static opcode census of one input's scripts: (bytes, hash ops, sig ops).
///
/// A CHECKMULTISIG is weighted by its KEY COUNT: the small-int push that
/// precedes it when the script states the count (the Poc5 covenant's
/// `OP_3`), else the most 33-byte keys the element limit admits (the count is
/// a computed value the census cannot see; bsv-rs allows up to `i32::MAX`).
fn script_census(
    unlocking: &[u8],
    locking: &[u8],
    memory_limit: usize,
) -> Result<(usize, usize, usize), String> {
    use bsv_rs::script::op::*;
    let most_keys = (memory_limit / 33).max(1);
    let mut hash_ops = 0usize;
    let mut sig_ops = 0usize;
    for (label, bytes) in [("unlocking", unlocking), ("locking", locking)] {
        let script = bsv_rs::script::Script::from_binary(bytes)
            .map_err(|e| format!("{label} script does not parse: {e}"))?;
        let chunks = script.chunks();
        for (i, chunk) in chunks.iter().enumerate() {
            match chunk.op {
                OP_RIPEMD160 | OP_SHA1 | OP_SHA256 | OP_HASH160 | OP_HASH256 => hash_ops += 1,
                OP_CHECKSIG | OP_CHECKSIGVERIFY => sig_ops += 1,
                OP_CHECKMULTISIG | OP_CHECKMULTISIGVERIFY => {
                    let stated = i
                        .checked_sub(1)
                        .map(|j| chunks[j].op)
                        .and_then(|op| match op {
                            OP_1..=OP_16 => Some((op - OP_1 + 1) as usize),
                            _ => None,
                        });
                    sig_ops += stated.unwrap_or(most_keys);
                }
                _ => {}
            }
        }
    }
    Ok((unlocking.len() + locking.len(), hash_ops, sig_ops))
}

impl Engine {
    /// Page size used by `/requestSyncResponse` when the (public) caller
    /// omits `limit`. A bounded page is not lossy for a conforming
    /// initiator: ours ([`crate::gasp::GASPSync`]) keeps requesting pages
    /// for as long as the `since` score cursor advances (gate finding M1 —
    /// it must NOT require a "full" page to continue, because this clamp
    /// makes a full-by-the-initiator's-limit page impossible).
    pub const SYNC_RESPONSE_DEFAULT_LIMIT: u64 = 500;
    /// Hard cap on a `/requestSyncResponse` page. Before bsv-low #291 an
    /// absent/huge caller-supplied limit meant an unbounded scan of the
    /// outputs table on a public route.
    pub const SYNC_RESPONSE_MAX_LIMIT: u64 = 1000;

    /// Clamp the public `/requestSyncResponse` page size: absent → the
    /// default page; anything larger than the cap → the cap. The result is
    /// ALWAYS `Some`-worthy — a sync response is never unbounded (bsv-low
    /// #291). Completeness relies on the INITIATOR paging while its `since`
    /// cursor advances (see `SYNC_RESPONSE_DEFAULT_LIMIT`'s doc) — the
    /// responder alone cannot page for it.
    pub fn clamp_sync_limit(requested: Option<u64>) -> u64 {
        requested
            .unwrap_or(Self::SYNC_RESPONSE_DEFAULT_LIMIT)
            .min(Self::SYNC_RESPONSE_MAX_LIMIT)
    }

    /// Create a new Overlay Services Engine.
    pub fn new(
        managers: HashMap<String, Box<dyn TopicManager>>,
        lookup_services: HashMap<String, Box<dyn LookupService>>,
        storage: Box<dyn Storage>,
        advertiser: Option<Box<dyn Advertiser>>,
        config: EngineConfig,
    ) -> Self {
        Self::with_chain_tracker(
            managers,
            lookup_services,
            storage,
            advertiser,
            None,
            None,
            config,
        )
    }

    /// Create a new Engine with an optional ChainTracker for SPV verification.
    pub fn with_chain_tracker(
        managers: HashMap<String, Box<dyn TopicManager>>,
        lookup_services: HashMap<String, Box<dyn LookupService>>,
        storage: Box<dyn Storage>,
        advertiser: Option<Box<dyn Advertiser>>,
        broadcaster: Option<Box<dyn Broadcaster>>,
        chain_tracker: Option<Box<dyn bsv_rs::transaction::ChainTracker>>,
        config: EngineConfig,
    ) -> Self {
        Self::with_all(
            managers,
            lookup_services,
            storage,
            advertiser,
            broadcaster,
            None,
            chain_tracker,
            config,
        )
    }

    /// Create a new Engine with all optional components.
    #[allow(clippy::too_many_arguments)]
    pub fn with_all(
        managers: HashMap<String, Box<dyn TopicManager>>,
        lookup_services: HashMap<String, Box<dyn LookupService>>,
        storage: Box<dyn Storage>,
        advertiser: Option<Box<dyn Advertiser>>,
        broadcaster: Option<Box<dyn Broadcaster>>,
        arc_broadcaster: Option<Box<dyn ArcBroadcaster>>,
        chain_tracker: Option<Box<dyn bsv_rs::transaction::ChainTracker>>,
        mut config: EngineConfig,
    ) -> Self {
        // Build default sync configuration: SHIP for all topics except tm_ship/tm_slap
        // which get their trackers merged with provided shipTrackers/slapTrackers.
        for manager_name in managers.keys() {
            if manager_name == "tm_ship" {
                if matches!(
                    config.sync_configuration.get(manager_name),
                    Some(SyncTarget::Disabled)
                ) {
                    continue;
                }
                let mut combined: HashSet<String> = HashSet::new();
                if let Some(SyncTarget::Peers(peers)) = config.sync_configuration.get(manager_name)
                {
                    combined.extend(peers.iter().cloned());
                }
                combined.extend(config.ship_trackers.iter().cloned());
                if !combined.is_empty() {
                    config.sync_configuration.insert(
                        manager_name.clone(),
                        SyncTarget::Peers(combined.into_iter().collect()),
                    );
                }
            } else if manager_name == "tm_slap" {
                if matches!(
                    config.sync_configuration.get(manager_name),
                    Some(SyncTarget::Disabled)
                ) {
                    continue;
                }
                let mut combined: HashSet<String> = HashSet::new();
                if let Some(SyncTarget::Peers(peers)) = config.sync_configuration.get(manager_name)
                {
                    combined.extend(peers.iter().cloned());
                }
                combined.extend(config.slap_trackers.iter().cloned());
                if !combined.is_empty() {
                    config.sync_configuration.insert(
                        manager_name.clone(),
                        SyncTarget::Peers(combined.into_iter().collect()),
                    );
                }
            } else if !config.sync_configuration.contains_key(manager_name) {
                config
                    .sync_configuration
                    .insert(manager_name.clone(), SyncTarget::Ship);
            }
        }

        Self {
            managers,
            lookup_services,
            storage,
            advertiser,
            broadcaster,
            arc_broadcaster,
            chain_tracker,
            gasp_remote_factory: None,
            ancestor_fetcher: None,
            peer_sync_budget: None,
            graph_budget: None,
            finalize_submit_budget: None,
            finalize_gate: crate::gasp::SubmitGate::default(),
            not_landed: std::cell::RefCell::new(HashSet::new()),
            landing_guard: None,
            verify_scripts: true,
            config,
        }
    }

    /// Set the ARC broadcaster for network broadcast to miners.
    ///
    /// When set, the Engine broadcasts transactions to ARC during Phase 2
    /// of `submit()` for `CurrentTx` submissions.
    pub fn set_arc_broadcaster(&mut self, arc: Box<dyn ArcBroadcaster>) {
        self.arc_broadcaster = Some(arc);
    }

    /// Set the GASP remote factory for peer communication during sync.
    ///
    /// Platform-specific crates (like overlay-cloudflare) provide an implementation
    /// that creates HTTP-based remotes using their native fetch API.
    pub fn set_gasp_remote_factory(&mut self, factory: Box<dyn crate::gasp::GASPRemoteFactory>) {
        self.gasp_remote_factory = Some(factory);
    }

    /// Set the OPT-IN chain-backed ancestor fetcher for GASP ingest.
    ///
    /// **OFF BY DEFAULT.** When set, a peer's failure to serve a needed
    /// ancestor during GASP ingest falls back to fetching that ancestor's raw
    /// tx from chain (via the supplied fetcher), self-healing the graph and
    /// also enabling strict-BEEF finalize so missing ancestry fails loud.
    ///
    /// This is a deliberate one-time-migration escape hatch. Production must
    /// NOT call this — when unset, GASP ingest behavior is unchanged.
    pub fn set_ancestor_fetcher(&mut self, fetcher: std::rc::Rc<dyn crate::gasp::AncestorFetcher>) {
        self.ancestor_fetcher = Some(fetcher);
    }

    /// Set the admission predicate asked BEFORE the door lands a carried
    /// predecessor (bsv-low #575, the E1D delta-2 fold, L2;
    /// [`Engine::land_carried`]). It is asked once per body the landing
    /// would submit, before that body's submit (a dry one included) and
    /// before its reads are charged; an `Err` (the caller's refusal, or its
    /// own read fault) ends the landing with nothing of that body written
    /// and the successor's topic answers "not now", as on `683dffd`. The
    /// worker installs its eviction ledger here: an OPEN eviction of the
    /// body refuses it. Its read is not one of the submit's 256 (it is the
    /// caller's store, at most one per landed body). Unset (default): every
    /// carried predecessor may land. What the predicate cannot see (a row
    /// opened while the landing writes) stays the caller's to guard after
    /// the write ([`MutationReport::landed_predecessors`]).
    pub fn set_landing_guard(&mut self, guard: LandingGuard) {
        self.landing_guard = Some(guard);
    }

    /// Set the per-peer GASP sync budget (bsv-low#302).
    ///
    /// `sleep` is the platform sleep factory (ms → future); `budget_ms` is
    /// the wall-clock slice ONE peer's sync may consume. A peer exceeding it
    /// is dropped (loudly logged, listed in the topic's `errors`) and the
    /// loop moves on. Unset (default) = unbounded, the pre-#302 behavior.
    ///
    /// **Progress survives the deadline (bsv-low #552).** With a budget set,
    /// every graph is submitted AS IT FINALIZES, inside the raced future (the
    /// reference submits inside `finalizeGraph` too), so what was finalized
    /// before the deadline stays admitted. At the deadline the cursor is
    /// advanced to `GASPSync::completed_cursor`: past the UTXOs whose graphs
    /// were completed, never past the one in flight. That graph is lost
    /// whole (nothing of it was finalized) and the next tick walks it again,
    /// down to whatever is now in storage.
    ///
    /// **The deadline never lands inside a transaction's writes** (the lens
    /// fold's HIGH-1). It is cooperative around a finalize submit
    /// ([`Engine::finalize_submit_gate`]): a deadline that falls due while
    /// one transaction is being written waits for that transaction, and the
    /// sync is dropped at the boundary. What a dropped tick leaves of the
    /// graph being submitted is an ancestors-first prefix of WHOLE
    /// transactions, with the cursor below its UTXO. Everywhere else (a
    /// request to the peer, the walk, the anchor check) the deadline drops
    /// the sync at once, as a budget against a dead peer must. A dropped tick that finalized a
    /// graph or moved the cursor is recorded as a SUCCESSFUL attempt for the
    /// quarantine count: a peer serving a long bootstrap is not a dead peer.
    /// One with no progress is a failed attempt, as before.
    ///
    /// The budget bounds the work of a TICK. Without a per-graph budget
    /// nothing bounds a GRAPH (parity: the reference has no node cap): one
    /// graph whose own walk outlasts the budget is dropped on every tick
    /// (`deadline_dropped_graphs`) and never admitted. With one
    /// ([`Engine::set_graph_budget`], bsv-low #555) that walk is deferred
    /// with its progress kept, here too, and resumed next tick.
    pub fn set_peer_sync_budget(&mut self, sleep: SleepFactory, budget_ms: u64) {
        self.peer_sync_budget = Some((sleep, budget_ms));
    }

    /// Set the per-GRAPH GASP budget and turn DEFERRAL on (bsv-low #555).
    ///
    /// A graph whose walk makes `max_calls` requests (to the peer, or chain
    /// fetches) or runs `budget_ms` in one pass is DEFERRED: its partial walk
    /// (the nodes fetched with their proofs, the inputs still pending, the
    /// calls spent, its age in passes) is saved as ONE record of the storage
    /// ([`Storage::put_deferred_graph`], replaced on every deferral), its
    /// UTXO is held below the cursor as a failed one is (the gap guard) and
    /// not walked again in the sync (#554), and the sync goes on to the next
    /// UTXO and topic. The next sync that is served that UTXO RESUMES the
    /// walk from the record, asking only what is pending, under the same
    /// budget; a graph so converges over passes and is then completed as
    /// any graph is (#551's anchor check, the finalize submits, e1d's
    /// rules). A walk cut by the per-peer deadline is deferred the same way.
    ///
    /// Bounds: a record deferred [`crate::gasp::DEFERRED_GRAPH_MAX_PASSES`]
    /// times, or bigger than [`crate::gasp::DEFERRED_GRAPH_MAX_BYTES`], is
    /// dropped with its reason and the UTXO fails as before (the gap guard
    /// asks again, from the root); at most
    /// [`crate::gasp::DEFERRED_GRAPHS_PER_PEER_TOPIC`] records per (peer,
    /// topic). Keep `budget_ms` below the per-peer budget so that a deep
    /// graph leaves the pass time for the UTXOs after it. Defaults offered:
    /// [`crate::gasp::DEFAULT_GRAPH_BUDGET_CALLS`],
    /// [`crate::gasp::DEFAULT_GRAPH_BUDGET_MS`].
    ///
    /// Unset (the default) nothing is deferred and the walk is the one
    /// before #555 (parity: the reference has no budget and no deferral).
    /// A storage that keeps no records (the trait's default) defers nothing
    /// either: a graph past the budget then fails its UTXO.
    pub fn set_graph_budget(&mut self, sleep: SleepFactory, max_calls: u32, budget_ms: u64) {
        self.graph_budget = Some((sleep, max_calls, budget_ms));
    }

    /// Bound ONE transaction's GASP finalize submit (bsv-low #559, the delta
    /// lens's DELTA-3). That submit is the write section no deadline drops
    /// ([`Engine::finalize_submit_gate`]), so a storage call or lookup hook
    /// that never answers inside it held the per-peer budget and any outer
    /// race for as long as it hung. With this set EACH storage call, lookup
    /// hook and topic manager call of that submit is raced against one
    /// `sleep(budget_ms)` of the section (the lens fold of 2026-10-07, F2):
    /// a call still unanswered when it falls due is dropped alone and is
    /// that call's FAULT, and no call is started after it. The SUBMIT is
    /// never dropped: it runs to its end and leaves what a fault leaves
    /// ([`Engine::submit_with_report`]: nothing of the transaction, or all
    /// of it), the transaction did not land, the rest of its graph is not
    /// submitted, its UTXO fails and the cursor stays below it. The undo of
    /// a faulted submit has one more `sleep(budget_ms)` of its own, so a
    /// peer costs at most its budget plus twice this one. What the backend
    /// does with a statement whose caller stopped waiting is its own: a
    /// write that lands after its timeout is not seen by this submit.
    ///
    /// DEFAULT: none (a native engine has no clock). A caller that sets a
    /// per-peer budget should set this too.
    pub fn set_finalize_submit_budget(&mut self, sleep: SleepFactory, budget_ms: u64) {
        self.finalize_submit_budget = Some((sleep, budget_ms));
    }

    /// The gate around every GASP finalize submit (bsv-low #552, the lens
    /// fold's HIGH-1). `start_gasp_sync` races each peer against its budget
    /// with [`crate::gasp::race_or_deadline_guarded`] over this gate. A
    /// caller that races `start_gasp_sync` ITSELF against a deadline (the
    /// worker's scheduled step) must use the same function over the same
    /// gate: a plain `race_or_deadline` there can still drop a finalize
    /// submit between its writes.
    pub fn finalize_submit_gate(&self) -> &crate::gasp::SubmitGate {
        &self.finalize_gate
    }

    /// Turn reference-parity script verification on submit on or off.
    /// **DEFAULT ON** (set in every constructor).
    ///
    /// On, every submit outside `HistoricalTxNoSpv` executes every unproven
    /// input's unlocking script against its source output (and checks every
    /// merkle path against the chain tracker), exactly as the reference's
    /// `Engine.submit` → `tx.verify(this.chainTracker)` does. Off is an
    /// ESCAPE HATCH for an operator who must admit a body the interpreter
    /// refuses (a bsv-rs interpreter defect, say) while it is fixed. It is
    /// NOT a mode: it restores the pre-2026-09-08 structural check, under
    /// which an invalid spend that no broadcaster had yet refused was
    /// admitted on BEEF structure alone. Prefer
    /// [`crate::builder::EngineBuilder::with_script_verification`].
    pub fn set_script_verification(&mut self, enabled: bool) {
        self.verify_scripts = enabled;
    }

    /// Whether submits execute input scripts, see
    /// [`Engine::set_script_verification`]. `true` by default.
    pub fn script_verification(&self) -> bool {
        self.verify_scripts
    }

    /// The reference's `tx.verify('scripts only')` over a BEEF: every unproven
    /// transaction from `subject_txid` down executes every input's unlocking
    /// script against its source output and obeys the value rule, while a
    /// transaction that carries a merkle path is TRUSTED AS-IS — no root is
    /// computed and no chain tracker is consulted (the ts-sdk's own shape when
    /// `chainTracker === 'scripts only'`: a proven tx is added to the verified
    /// set and the walk stops there).
    ///
    /// Built for an admission path whose bar is the NETWORK, not a proof
    /// (bsv-low's `broadcast-gated`, register row D1): the overlay broadcasts
    /// and admits on network evidence, so a lagging tracker must never read as
    /// a refused transaction — but a spend the interpreter refuses is one the
    /// network will refuse too, and executing it at the door saves the
    /// broadcast and names the fault. Independent of
    /// [`Engine::set_script_verification`] (that switch governs `submit`; this
    /// is an explicit ask) and of [`Engine::submit`]'s mode (`HistoricalTxNoSpv`
    /// still skips, as the reference does).
    ///
    /// Runs under [`DoorBudget::DEFAULT`]: the work is bounded statically
    /// before anything executes, and the interpreter's stack memory limit is
    /// the budget's element limit.
    ///
    /// Errors: [`EngineError::ScriptVerificationFailed`] is the INTERPRETER's
    /// verdict and the only refusal; [`EngineError::ScriptWalkInconclusive`]
    /// is a structural fault (a transaction or source missing from the BEEF,
    /// a parse fault, the value rule) and [`EngineError::ScriptWalkOverBudget`]
    /// the door's own bound (the static budget or the memory limit) — both
    /// name whether the SUBJECT was judged before the fault, and a caller with
    /// a stronger bar behind it does not refuse on either.
    pub async fn verify_scripts_only(
        &self,
        beef_bytes: &[u8],
        subject_txid: &str,
    ) -> Result<WalkStats, EngineError> {
        Self::verify_beef_linear_with(
            self.chain_tracker.as_deref(),
            beef_bytes,
            subject_txid,
            WalkPolicy {
                roots: RootPolicy::AcceptUnchecked,
                budget: Some(DoorBudget::DEFAULT),
            },
            &HashSet::new(),
        )
        .await
    }

    // ========================================================================
    // Submit — 3-phase pipeline
    // ========================================================================

    /// Validate a tagged BEEF without applying mutations.
    ///
    /// Runs Phase 1 (topic validation) and Phase 2 (broadcast) but NOT Phase 3
    /// (storage mutations). Returns the Steak (admittance decisions) that
    /// `submit()` would return.
    ///
    /// Used by the onSteakReady pattern to return results to clients before
    /// mutations are applied via a queue consumer or `ctx.wait_until()`.
    ///
    /// The topic managers are called with `dry_run: true`
    /// (`TopicAdmittanceContext::DRY_RUN`): nothing is admitted on this call, so a
    /// manager that writes on admission leaves no trace of it.
    pub async fn submit_validate_only(
        &self,
        tagged_beef: &TaggedBEEF,
        mode: SubmitMode,
    ) -> Result<Steak, EngineError> {
        // A validate-only call admits nothing: a dry run by its own name. A
        // manager that writes on admission must not write here, or the real
        // submit that follows meets a head this call already advanced.
        let (_validations, steak, _tx, _txid) = self
            .run_validation(
                tagged_beef,
                mode,
                &TopicAdmittanceContext::DRY_RUN,
                None,
                None,
            )
            .await?;
        Ok(steak)
    }

    /// Submit a transaction for processing by the overlay.
    ///
    /// Three phases:
    /// 1. **VALIDATE** — check topics, dedup, find previous coins, call topic managers
    /// 2. **BROADCAST** — broadcast to network (if mode is CurrentTx)
    /// 3. **MUTATE** — mark spent, delete stale, insert new outputs, notify lookup services
    ///
    /// Returns a STEAK mapping each topic to its admittance instructions.
    ///
    /// Phase-3 faults are reported through [`Engine::submit_with_report`];
    /// this convenience keeps the historical signature and only LOGS the
    /// report's summary. A caller that must not ack an admission whose
    /// writes did not land (the `/submit` route — S2) uses the report form.
    pub async fn submit(
        &self,
        tagged_beef: &TaggedBEEF,
        mode: SubmitMode,
    ) -> Result<Steak, EngineError> {
        let (steak, report) = self.submit_with_report(tagged_beef, mode).await?;
        if !report.is_durable() {
            warn!(
                "submit: {} mutation fault(s) — not durable: {}",
                report.faults.len(),
                report.summary()
            );
        }
        Ok(steak)
    }

    /// [`Engine::submit`] plus the Phase-3 durability report (S2
    /// queue-durable admission, bsv-low 2026-08-29).
    ///
    /// The STEAK is the admission DECISION; the report says whether the
    /// decision was DURABLY WRITTEN. They are returned together because
    /// they are answered by different things: Phase 1+2 decide, Phase 3
    /// lands, and only the caller knows whether an undurable landing may
    /// be acked (never, on the money path — re-queue it instead).
    ///
    /// Every write failure is still logged AND still non-fatal to the
    /// other topics (a faulted topic does not un-admit its siblings); what
    /// changed is that the failure is also VISIBLE, and that the faulted
    /// topic is not recorded as applied — so a replay of the same bytes is
    /// re-validated and re-written (every backend write is idempotent:
    /// `INSERT OR IGNORE` / `OR REPLACE` / `UPDATE`).
    ///
    /// What a fault leaves (bsv-low #559 and its lens fold of 2026-10-07):
    /// a topic's previous coins are deleted once EVERY admitted output of
    /// the transaction is inserted, and only then. A fault before that
    /// (a validation read, the spent mark of a coin the manager retains, an
    /// insert) leaves the previous coins held and nothing of the
    /// transaction: what did get inserted is taken out again, so the replay
    /// is the same judgement and no successor can spend half of it. A fault
    /// after it (a lookup notification, the consumed-by update, the applied
    /// row) leaves the transaction's outputs and no previous coin. A fault
    /// before any delete was started (the record of the spent coin's
    /// transaction, the read of the coin) is the first case if the coins
    /// are read back still held; once a delete was started its fault never
    /// undoes, answered or not (it may land yet): the outputs stay beside
    /// the stale coin, marked spent. A submit that deletes a stale coin
    /// first records that coin's transaction as applied, so a replay of it
    /// after its successor is a dupe. So a non-retaining chain never has
    /// two unspent heads and never none, whatever single call faults. The
    /// applied row is withheld on ANY fault. And a successor that was
    /// judged without the coin an unlanded predecessor has yet to leave is
    /// reported as a fault (`predecessor_not_landed`), not recorded as
    /// applied.
    pub async fn submit_with_report(
        &self,
        tagged_beef: &TaggedBEEF,
        mode: SubmitMode,
    ) -> Result<(Steak, MutationReport), EngineError> {
        self.submit_bounded(tagged_beef, mode, None, false).await
    }

    /// [`Engine::submit_with_report`] with every storage call, lookup hook
    /// and topic manager call under `bound` (the GASP finalize submit,
    /// [`CallBound`]); `None` is the unbounded submit of every other door.
    /// `finalize` is `true` for the GASP finalize submit alone, bounded or
    /// not: it does not ask the store's predecessor question.
    async fn submit_bounded(
        &self,
        tagged_beef: &TaggedBEEF,
        mode: SubmitMode,
        bound: Option<&CallBound>,
        finalize: bool,
    ) -> Result<(Steak, MutationReport), EngineError> {
        // The reads the store's predecessor question has made in this
        // submit, every topic together ([`PREDECESSOR_READS_PER_SUBMIT`]),
        // the predecessors the door lands first from the BEEF included.
        let mut question_reads = 0usize;
        self.submit_counted(
            tagged_beef,
            mode,
            bound,
            finalize,
            None,
            &mut question_reads,
        )
        .await
        .map(|(steak, report, _)| (steak, report))
    }

    /// [`Engine::submit_bounded`] over the submit's read allowance
    /// (`question_reads`), which a predecessor the door lands first from the
    /// BEEF shares with the successor that carried it (lane E1D's lens fold,
    /// M1). `carried` is `Some` for such a predecessor's own submit
    /// ([`Engine::land_carried`]), holding the bodies whose SPV walk already
    /// passed in this submit (none is walked twice; the delta fold, L4): it
    /// lands nothing first itself, and it names instead the carried
    /// predecessor that blocks each topic (the third value), so the landing
    /// is an explicit stack and never a recursion of submits (nested polls
    /// grow the stack, and a Worker's is small). Its manager is asked as a
    /// DRY RUN first, and for real only once its topic is not blocked: a
    /// link blocked below is judged twice and admitted once. Boxed: the
    /// landing is a submit of its own.
    fn submit_counted<'a>(
        &'a self,
        tagged_beef: &'a TaggedBEEF,
        mode: SubmitMode,
        bound: Option<&'a CallBound>,
        finalize: bool,
        carried: Option<&'a HashSet<String>>,
        question_reads: &'a mut usize,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Counted, EngineError>> + 'a>>
    {
        Box::pin(async move {
            self.submit_counted_inner(tagged_beef, mode, bound, finalize, carried, question_reads)
                .await
        })
    }

    async fn submit_counted_inner(
        &self,
        tagged_beef: &TaggedBEEF,
        mode: SubmitMode,
        bound: Option<&CallBound>,
        finalize: bool,
        walked: Option<&HashSet<String>>,
        question_reads: &mut usize,
    ) -> Result<Counted, EngineError> {
        // A submit is a real admission, never a dry run; a carried
        // predecessor's is judged dry until its topic is known not to wait.
        let carried = walked.is_some();
        let context = if carried {
            TopicAdmittanceContext::DRY_RUN
        } else {
            TopicAdmittanceContext::default()
        };
        let (mut validations, mut steak, tx, txid) = self
            .run_validation(tagged_beef, mode, &context, bound, walked)
            .await?;
        let mut report = MutationReport::default();
        // The body every LOOKUP SERVICE receives NAMES the subject (BRC-95
        // atomic prefix over the WHOLE submitted body — bsv-rs's
        // `to_binary_atomic` sorts, names, never prunes). Lookup services
        // re-parse it with `from_beef(_, None)`; on a plain incomplete body
        // that pick is the wire-last ANCESTOR, so the pot index's admit hook
        // shape-checked a hop and wrote no record while the engine had
        // admitted the pot (loop-2 fleet, 2026-09-05; the live proof after
        // the engine's own subject fix). A body that cannot be re-serialized
        // (never, for a parsed subject) is handed through as submitted.
        let subject_named_beef: Vec<u8> = bsv_rs::transaction::Beef::from_binary(&tagged_beef.beef)
            .ok()
            .and_then(|mut b| b.to_binary_atomic(&txid).ok())
            .unwrap_or_else(|| tagged_beef.beef.clone());

        // The successor rule, asked of every topic that found no coin before
        // anything of this submit is written ([`Engine::successors_waiting`]):
        // the topics that must wait, each with why.
        let mut blocked: HashMap<String, String> = HashMap::new();
        let waits = self
            .successors_waiting(
                &tx,
                &txid,
                &subject_named_beef,
                &mut validations,
                &mut steak,
                tagged_beef.off_chain_values.as_deref(),
                mode,
                bound,
                finalize,
                !carried,
                &mut blocked,
                question_reads,
                &mut report.landed_predecessors,
            )
            .await;
        // A carried predecessor's topic that does not wait is judged again,
        // for real: what it admits is this call's word, over the same coins.
        if carried {
            for v in &mut validations {
                if v.is_dupe || v.failed || v.read_fault.is_some() || waits.contains_key(&v.topic) {
                    continue;
                }
                match self
                    .judge(
                        &tx,
                        &v.topic,
                        &v.previous_coins,
                        tagged_beef.off_chain_values.as_deref(),
                        mode,
                        &TopicAdmittanceContext::default(),
                        bound,
                    )
                    .await
                {
                    Ok(admittance) => v.admittance = admittance,
                    Err(e) => {
                        error!("Error validating topic {} during submit: {e}", v.topic);
                        v.failed = true;
                        v.previous_coins.clear();
                        v.previous_outputs.clear();
                        v.admittance = AdmittanceInstructions::default();
                    }
                }
                steak.insert(v.topic.clone(), v.admittance.clone());
            }
        }

        // =================================================================
        // PHASE 3: MUTATE STORAGE
        // =================================================================
        for v in &validations {
            if v.is_dupe {
                report.deduped_topics.push(v.topic.clone());
                continue;
            }
            // A failed topic comes first, a read fault of it included: a
            // previous-coin read that faulted and a manager that then ERRED
            // is a failed topic with a durable report, no fault and no
            // replay. Parity: the reference fails the topic and never
            // replays. Nothing was written, so the store is as it was.
            if v.failed {
                continue;
            }

            let topic = &v.topic;
            let admittance = &v.admittance;
            let faults_before = report.faults.len();

            // A validation read that faulted: the manager judged without a
            // coin the store may hold, so NOTHING is written on that
            // judgement (the lens fold of 2026-10-07). Writing its outputs
            // would leave them beside the unseen coin, for a successor to
            // spend before the replay deletes it.
            if let Some((read, err)) = &v.read_fault {
                report.fault(topic, read, err.clone());
                self.hold_unapplied(&txid, topic, "a validation read faulted, nothing written");
                continue;
            }

            // bsv-low #559 and lane E1D: a topic that found no coin while a
            // predecessor's landing is unknown is "not now", recorded
            // nowhere ([`Engine::successors_waiting`]).
            if let Some(not_now) = waits.get(topic) {
                report.fault(topic, "predecessor_not_landed", not_now.clone());
                self.hold_unapplied(
                    &txid,
                    topic,
                    &format!("found no coin and its predecessor {not_now}"),
                );
                continue;
            }

            // ── Handle stale vs retained previous coins ──
            let mut outputs_consumed: Vec<Outpoint> = Vec::new();
            let mut stale_coins: Vec<&Output> = Vec::new();

            for (coin_idx, prev_output) in v.previous_outputs.iter().enumerate() {
                let input_index = v.previous_coins[coin_idx];
                if admittance.coins_to_retain.contains(&input_index) {
                    // Retained: track as consumed (will update consumedBy later)
                    outputs_consumed
                        .push(Outpoint::new(&prev_output.txid, prev_output.output_index));
                } else {
                    // Not retained: mark as stale for deletion
                    stale_coins.push(prev_output);
                }
            }

            // Update STEAK with removed coins
            if let Some(steak_entry) = steak.get_mut(topic) {
                steak_entry.coins_removed = Some(
                    stale_coins
                        .iter()
                        .enumerate()
                        .filter_map(|(i, _)| {
                            // Map back to input indices
                            v.previous_coins.get(i).copied()
                        })
                        .collect(),
                );
            }

            // ── Mark previous outputs as spent + notify lookup services ──
            // A mark that faults does not skip the notification (the
            // reference skips it): the coin is spent whatever the row says,
            // a stale one is deleted below and its lookup services must
            // know. The mark of a RETAINED coin is part of what landed: that
            // row stays, and left unspent it would be listed as a UTXO
            // beside the output that consumed it.
            let mut retained_mark_faulted = false;
            for (prev_idx, prev_output) in v.previous_outputs.iter().enumerate() {
                if let Err(e) = stored(
                    bound,
                    self.storage.mark_utxo_as_spent(
                        &prev_output.txid,
                        prev_output.output_index,
                        topic,
                    ),
                )
                .await
                {
                    error!("Error marking UTXO as spent: {e}");
                    report.fault(topic, "mark_utxo_as_spent", e.to_string());
                    retained_mark_faulted |= admittance
                        .coins_to_retain
                        .contains(&v.previous_coins[prev_idx]);
                }

                // The input index within tx.inputs that spends this previous output.
                let spending_input_idx = v.previous_coins[prev_idx] as usize;

                // Notify all lookup services about the spent output
                for ls in self.lookup_services.values() {
                    let payload = match ls.spend_notification_mode() {
                        SpendNotificationMode::None => OutputSpent::None {
                            txid: prev_output.txid.clone(),
                            output_index: prev_output.output_index,
                            topic: topic.clone(),
                        },
                        SpendNotificationMode::Txid => OutputSpent::Txid {
                            txid: prev_output.txid.clone(),
                            output_index: prev_output.output_index,
                            topic: topic.clone(),
                            spending_txid: txid.clone(),
                        },
                        SpendNotificationMode::Script => {
                            // Extract unlocking script and sequence from the spending input
                            let input = &tx.inputs[spending_input_idx];
                            let unlocking_script = input
                                .unlocking_script
                                .as_ref()
                                .map(bsv_rs::UnlockingScript::to_binary)
                                .unwrap_or_default();
                            OutputSpent::Script {
                                txid: prev_output.txid.clone(),
                                output_index: prev_output.output_index,
                                topic: topic.clone(),
                                spending_txid: txid.clone(),
                                input_index: spending_input_idx as u32,
                                unlocking_script,
                                sequence_number: input.sequence,
                                off_chain_values: tagged_beef.off_chain_values.clone(),
                            }
                        }
                        SpendNotificationMode::WholeTx => OutputSpent::WholeTx {
                            txid: prev_output.txid.clone(),
                            output_index: prev_output.output_index,
                            topic: topic.clone(),
                            spending_atomic_beef: subject_named_beef.clone(),
                            off_chain_values: tagged_beef.off_chain_values.clone(),
                        },
                    };
                    if let Err(e) = hooked(bound, ls.output_spent(&payload)).await {
                        error!("Error notifying lookup service of spent output: {e}");
                        report.fault(topic, "lookup_service.output_spent", e);
                    }
                }
            }
            if retained_mark_faulted {
                self.hold_unapplied(
                    &txid,
                    topic,
                    "the spent mark of a retained coin faulted, nothing inserted",
                );
                continue;
            }

            // ── Insert admitted outputs ──
            // The inserts stop at the first one that faults: what follows is
            // the undo, not the rest of the transaction.
            let mut new_utxos: Vec<Outpoint> = Vec::new();
            let mut attempted: Vec<u32> = Vec::new();
            let mut insert_faulted = false;

            for &output_index in &admittance.outputs_to_admit {
                let (script_bytes, sats) =
                    if let Some(tx_output) = tx.outputs.get(output_index as usize) {
                        (
                            tx_output.locking_script.to_binary(),
                            tx_output.get_satoshis(),
                        )
                    } else {
                        (Vec::new(), 0)
                    };

                let output = Output {
                    txid: txid.clone(),
                    output_index,
                    output_script: script_bytes,
                    satoshis: sats,
                    topic: topic.clone(),
                    spent: false,
                    outputs_consumed: outputs_consumed.clone(),
                    consumed_by: Vec::new(),
                    beef: Some(tagged_beef.beef.clone()),
                    block_height: None,
                    score: Some(current_timestamp_ms()),
                };

                attempted.push(output_index);
                if let Err(e) = stored(bound, self.storage.insert_output(&output)).await {
                    error!("Error inserting output for topic {topic}: {e}");
                    report.fault(topic, "insert_output", e.to_string());
                    insert_faulted = true;
                    break;
                }

                new_utxos.push(Outpoint::new(&txid, output_index));

                // Notify lookup services
                for ls in self.lookup_services.values() {
                    let (ls_script, ls_sats) =
                        if let Some(tx_out) = tx.outputs.get(output_index as usize) {
                            (tx_out.locking_script.to_binary(), tx_out.get_satoshis())
                        } else {
                            (Vec::new(), 0)
                        };

                    let payload = match ls.admission_mode() {
                        AdmissionMode::LockingScript => OutputAdmittedByTopic::LockingScript {
                            txid: txid.clone(),
                            output_index,
                            topic: topic.clone(),
                            satoshis: ls_sats,
                            locking_script: ls_script,
                            off_chain_values: tagged_beef.off_chain_values.clone(),
                        },
                        AdmissionMode::WholeTx => OutputAdmittedByTopic::WholeTx {
                            atomic_beef: subject_named_beef.clone(),
                            output_index,
                            topic: topic.clone(),
                            off_chain_values: tagged_beef.off_chain_values.clone(),
                        },
                    };

                    if let Err(e) = hooked(bound, ls.output_admitted_by_topic(&payload)).await {
                        error!("Error notifying lookup service: {e}");
                        report.fault(topic, "lookup_service.output_admitted_by_topic", e);
                    }
                }
            }

            // An insert faulted: the transaction did not land, so NOTHING of
            // it stays (the lens fold of 2026-10-07, F1). The previous coins
            // are kept (#559: with the delete first, one faulted insert left
            // a non-retaining head chain with NO head) and the outputs that
            // were inserted are taken out again, the faulted one included (a
            // backend's insert can fault after its row landed). Left in,
            // they could be spent and deleted by a successor before the
            // replay, and the replay, which still finds the previous coin,
            // would insert them a second time: a spent head back among the
            // UTXOs for good.
            if insert_faulted {
                if let Some(bound) = bound {
                    bound.renew();
                }
                self.undo_inserts(&txid, topic, &attempted, bound, &mut report)
                    .await;
                self.hold_unapplied(
                    &txid,
                    topic,
                    &format!(
                        "an insert faulted: {} previous coin(s) kept, nothing of it held",
                        stale_coins.len()
                    ),
                );
                continue;
            }

            // ── Delete stale outputs recursively ──
            // AFTER the inserts, and once every one of them landed (bsv-low
            // #559 and the lens fold of 2026-10-07, F1). The reference
            // deletes first (`applyTopicStorageMutation`: `removeStaleOutputs`,
            // then `admitOutput`), catches a throw per topic and never
            // replays; here a fault is survived and replayed, so the order
            // is ours, an addition. The delete does NOT wait for a clean
            // report (c9921ee did, and one faulted mark or notification
            // after the insert then left the stale coin beside the new
            // output for good: two unspent heads): a fault of the mark, of
            // a notification, deletes as the reference does.
            //
            // Before a stale coin is deleted its transaction is recorded as
            // applied in the topic (the delta fold of 2026-10-07, H2; an
            // addition, one idempotent write per deleted coin). A held coin
            // proves the inserts of its transaction landed, but that
            // transaction may have no applied row (a fault after its
            // inserts withheld it, or its insert landed after its timeout),
            // and then its replay is judged again: a manager that admits it
            // with NO previous coin (the opener of a chain) had it inserted
            // a second time, unspent, beside the output that spent it, for
            // good. With the row its replay is a dupe. A record that faults
            // leaves its coin undeleted: that is a delete fault, below.
            let due_before_the_delete = bound.is_some_and(CallBound::is_due);
            let mut delete_faulted = false;
            let mut delete_started = false;
            for stale in &stale_coins {
                let spent_coin_tx = AppliedTransaction {
                    txid: stale.txid.clone(),
                    topic: topic.clone(),
                };
                if let Err(e) = stored(
                    bound,
                    self.storage.insert_applied_transaction(&spent_coin_tx),
                )
                .await
                {
                    error!(
                        "Error recording {} as applied before its coin is deleted from topic {topic}: {e}",
                        stale.txid
                    );
                    report.fault(topic, "record_spent_coin_applied", e.to_string());
                    delete_faulted = true;
                    continue;
                }
                self.not_landed
                    .borrow_mut()
                    .remove(&(stale.txid.clone(), topic.clone()));
                match stored(
                    bound,
                    self.storage.find_output(
                        &stale.txid,
                        stale.output_index,
                        Some(topic),
                        None,
                        false,
                    ),
                )
                .await
                {
                    Ok(Some(stale_output)) => {
                        delete_started = true;
                        if let Err(e) = self.delete_utxo_deep(&stale_output, bound).await {
                            error!("Error deleting stale output for topic {topic}: {e}");
                            report.fault(topic, "delete_utxo_deep", e.to_string());
                            delete_faulted = true;
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        error!("Error reading stale output for topic {topic}: {e}");
                        report.fault(topic, "find_output", e.to_string());
                        delete_faulted = true;
                    }
                }
            }

            // A fault here, and what it leaves. The store is put back as it
            // was found (the inserts undone, the replay does the whole
            // transaction) only when NO delete was started: the fault is of
            // H2's record or of the read before the delete, the call
            // answered, and EVERY stale coin is read back still held.
            //
            // Once a delete was STARTED nothing is undone, whether it
            // answered or not (the delta fold of 2026-10-07, H1, for a call
            // that did not answer; the second delta fold, M1, for one that
            // answered an error). A statement can land after its caller was
            // told it failed, as after its caller stopped waiting: the coin
            // is read back held now and is deleted a moment later, and the
            // undo then left the chain with no head, for good, for a manager
            // that needs the previous coin. So the outputs stay, the stale
            // coin stays (marked spent) until its delete lands or is done
            // again, and there is no applied row. Since H2 every order from
            // there converges. The replay first, the delete never landed:
            // the coin is found, the manager admits again, the inserts are
            // no-ops, the delete is done and the row written. The replay
            // first, the delete landed late: no previous coin, so a manager
            // that needs one admits nothing and the transaction is recorded
            // with its outputs held. A successor first: it recorded this
            // transaction as applied (H2) and the replay is a dupe; the
            // stale coin's spent row is then left behind, no UTXO.
            //
            // A call that did not ANSWER before any delete (the record, the
            // read) undoes nothing either: it is not known what it did. A
            // deadline that was already due BEFORE the first of these calls
            // started nothing here, so the coins are certainly held and the
            // store is put back.
            if delete_faulted {
                let unanswered = !due_before_the_delete && bound.is_some_and(CallBound::is_due);
                if let Some(bound) = bound {
                    bound.renew();
                }
                let mut all_held = !unanswered && !delete_started;
                for stale in &stale_coins {
                    if !all_held {
                        break;
                    }
                    let read = stored(
                        bound,
                        self.storage.find_output(
                            &stale.txid,
                            stale.output_index,
                            Some(topic),
                            None,
                            false,
                        ),
                    )
                    .await;
                    if !matches!(read, Ok(Some(_))) {
                        all_held = false;
                        break;
                    }
                }
                if all_held {
                    self.undo_inserts(&txid, topic, &attempted, bound, &mut report)
                        .await;
                }
                self.hold_unapplied(
                    &txid,
                    topic,
                    if all_held {
                        "no delete of a stale coin was started: the coins are held, its outputs taken out"
                    } else if unanswered {
                        "the delete of a stale coin did not answer: nothing undone, its outputs stay"
                    } else if delete_started {
                        "the delete of a stale coin faulted: nothing undone, its outputs stay"
                    } else {
                        "no delete was started and a stale coin was not read back held: nothing undone, its outputs stay"
                    },
                );
                continue;
            }

            // ── Update consumedBy on retained previous outputs ──
            for consumed_outpoint in &outputs_consumed {
                let consumed_output = match stored(
                    bound,
                    self.storage.find_output(
                        &consumed_outpoint.txid,
                        consumed_outpoint.output_index,
                        Some(topic),
                        None,
                        false,
                    ),
                )
                .await
                {
                    Ok(Some(o)) => o,
                    Ok(None) => continue,
                    Err(e) => {
                        error!("Error reading consumed output for topic {topic}: {e}");
                        report.fault(topic, "find_output", e.to_string());
                        continue;
                    }
                };
                {
                    let mut new_consumed_by = consumed_output.consumed_by.clone();
                    for new_utxo in &new_utxos {
                        if !new_consumed_by.iter().any(|c| {
                            c.txid == new_utxo.txid && c.output_index == new_utxo.output_index
                        }) {
                            new_consumed_by.push(new_utxo.clone());
                        }
                    }
                    if let Err(e) = stored(
                        bound,
                        self.storage.update_consumed_by(
                            &consumed_outpoint.txid,
                            consumed_outpoint.output_index,
                            topic,
                            &new_consumed_by,
                        ),
                    )
                    .await
                    {
                        error!("Error updating consumedBy: {e}");
                        report.fault(topic, "update_consumed_by", e.to_string());
                    }
                }
            }

            // Record applied transaction — ONLY when every write above
            // landed. A faulted topic stays unrecorded so the Phase-1 dedup
            // cannot turn its replay into a no-op (S2).
            if report.faults.len() > faults_before {
                self.hold_unapplied(
                    &txid,
                    topic,
                    &format!(
                        "{} mutation fault(s); a replay of these bytes re-applies",
                        report.faults.len() - faults_before
                    ),
                );
                continue;
            }
            let tx_record = AppliedTransaction {
                txid: txid.clone(),
                topic: topic.clone(),
            };
            match stored(bound, self.storage.insert_applied_transaction(&tx_record)).await {
                Ok(()) => {
                    report.applied_topics.push(topic.clone());
                    self.not_landed
                        .borrow_mut()
                        .remove(&(txid.clone(), topic.clone()));
                }
                Err(e) => {
                    error!("Error inserting applied transaction for topic {topic}: {e}");
                    report.fault(topic, "insert_applied_transaction", e.to_string());
                    self.not_landed
                        .borrow_mut()
                        .insert((txid.clone(), topic.clone()));
                }
            }
        }

        Ok((steak, report, blocked))
    }

    /// The successor rule (bsv-low #559; lane E1D, bsv-low #575; and its
    /// lens fold of 2026-10-08), asked of every topic of a submit that found
    /// NO previous coin while the transaction spends something, before
    /// anything of the submit is written. Returns the topics that must wait,
    /// each with why: those are reported as a fault (`predecessor_not_landed`)
    /// and recorded nowhere, "not now". An addition to the reference, which
    /// records every non-failed topic.
    ///
    /// Such a topic was judged without the coin a predecessor has yet to
    /// leave. Recorded as applied, its replay was a dupe for good: a
    /// transaction that admits nothing ended the chain at the predecessor
    /// (#559), and one the manager admits WITHOUT its coin (the pf head
    /// manager over its own head state, E1D) was inserted, and the
    /// predecessor, landing later, stood unspent beside the tip.
    ///
    /// "Has not landed" is known two ways: this engine saw it fault
    /// (`not_landed`, one invocation, every door), or the store says so
    /// ([`Engine::unlanded_predecessor`], any invocation, never at a GASP
    /// finalize submit: its graph passed the anchor check, every parent
    /// inside the graph was submitted just before it and the sequence stops
    /// at the first that does not land; the second delta fold of 2026-10-07,
    /// M2). For a successor that admits something, both look only at the
    /// inputs the manager NAMES as its history (`identify_needed_inputs`
    /// over the subject-named BEEF; the lens fold, L1): a manager that
    /// names nothing (all 16 of this workspace) asks nothing there, as in
    /// the reference. A successor that admits NOTHING is asked of every
    /// input whatever its manager names (D17 (a)), so under such a manager
    /// it still waits for, or lands first, a carried body that spends a
    /// coin the topic holds: there the store differs from the reference's
    /// (the E1D delta fold, L3).
    ///
    /// The door lands a carried predecessor FIRST (the lens fold, M1; the
    /// GASP walk's rule, at the door): when the question names an unlanded
    /// predecessor whose body the BEEF carries, that body is submitted on
    /// its own to this topic ([`Engine::land_carried`]), under the submit's
    /// read allowance, and once it lands the topic is judged again
    /// ([`Engine::validate_topic`]) and asked again. The re-judgement's
    /// reads (the applied row, one coin per input) are charged to that
    /// allowance BEFORE the landing (the E1D delta fold, M1): a landing whose
    /// re-judgement would not fit is not made, and the topic waits. So one
    /// submit reads at most its own validation and writes plus
    /// [`PREDECESSOR_READS_PER_SUBMIT`], landings (on their clean path) and
    /// re-judgements included; a faulted landing's read-backs and undo, and
    /// a deep delete's walk of retained history, are not charged.
    /// Nothing is landed into a topic whose manager or a lookup
    /// service reads off-chain values (the delta fold, L1: the landing has
    /// none to give, and the predecessor's own submit would be a dupe): the
    /// topic waits for that submit. The first predecessor that does not land
    /// ends it: the topic waits. A predecessor whose body the BEEF does not
    /// carry is never landed here: its successor converges only when it
    /// lands (its own replay, a GASP peer, a resubmit).
    #[allow(clippy::too_many_arguments)]
    async fn successors_waiting(
        &self,
        tx: &Transaction,
        txid: &str,
        subject_named_beef: &[u8],
        validations: &mut [TopicValidation],
        steak: &mut Steak,
        off_chain_values: Option<&[u8]>,
        mode: SubmitMode,
        bound: Option<&CallBound>,
        finalize: bool,
        lands: bool,
        blocked: &mut HashMap<String, String>,
        question_reads: &mut usize,
        landed: &mut Vec<(String, String)>,
    ) -> HashMap<String, String> {
        let mut waits = HashMap::new();
        let spent: Vec<(String, u32)> = tx
            .inputs
            .iter()
            .filter_map(|input| {
                let source = input.get_source_txid().ok()?;
                (!source.is_empty()).then_some((source, input.source_output_index))
            })
            .collect();
        if spent.is_empty() {
            return waits;
        }
        let found_no_coin = |v: &TopicValidation| {
            !v.is_dupe && !v.failed && v.read_fault.is_none() && v.previous_outputs.is_empty()
        };
        for v in validations.iter_mut() {
            if !found_no_coin(v) {
                continue;
            }
            let Some(manager) = self.managers.get(&v.topic) else {
                continue;
            };
            let topic = v.topic.clone();
            // The manager's word on the subject's history, asked at most
            // once per topic: up front for a successor that admits, by the
            // question when it needs it for one that does not.
            let mut subject_names: Option<HashSet<String>> = None;
            loop {
                let admits = !v.admittance.outputs_to_admit.is_empty();
                if admits && subject_names.is_none() {
                    subject_names = Some(
                        self.named_history(manager.as_ref(), subject_named_beef, bound)
                            .await,
                    );
                }
                let remembered = spent.iter().find(|(source, vout)| {
                    (!admits
                        || subject_names
                            .as_ref()
                            .is_some_and(|named| named.contains(&format!("{source}.{vout}"))))
                        && self
                            .not_landed
                            .borrow()
                            .contains(&(source.clone(), topic.clone()))
                });
                if let Some((predecessor, _)) = remembered {
                    waits.insert(topic.clone(), format!("{predecessor} has not landed"));
                    break;
                }
                if finalize {
                    break;
                }
                let Some(not_now) = self
                    .unlanded_predecessor(
                        tx,
                        txid,
                        subject_named_beef,
                        &topic,
                        mode,
                        admits,
                        &mut subject_names,
                        question_reads,
                        bound,
                    )
                    .await
                else {
                    break;
                };
                let Some(carried) = not_now.carried else {
                    waits.insert(topic.clone(), not_now.why);
                    break;
                };
                if !lands {
                    blocked.insert(topic.clone(), carried);
                    waits.insert(topic.clone(), not_now.why);
                    break;
                }
                if !self.lands_without_values(&topic) {
                    waits.insert(
                        topic.clone(),
                        format!(
                            "{}; not landed first from the BEEF: the topic's manager or a lookup service reads off-chain values, and only the predecessor's own submit carries them",
                            not_now.why
                        ),
                    );
                    break;
                }
                // The re-judgement after the landing, charged before it.
                let rejudge = 1 + spent.len();
                if *question_reads + rejudge > PREDECESSOR_READS_PER_SUBMIT {
                    waits.insert(
                        topic.clone(),
                        format!(
                            "{}; its landing and the re-judgement after it do not fit in the submit's {PREDECESSOR_READS_PER_SUBMIT} reads",
                            not_now.why
                        ),
                    );
                    break;
                }
                *question_reads += rejudge;
                match self
                    .land_carried(
                        subject_named_beef,
                        txid,
                        &carried,
                        &topic,
                        mode,
                        bound,
                        question_reads,
                        landed,
                    )
                    .await
                {
                    Ok(()) => {
                        *v = self
                            .validate_topic(
                                tx,
                                txid,
                                &topic,
                                off_chain_values,
                                mode,
                                &TopicAdmittanceContext::default(),
                                bound,
                            )
                            .await;
                        steak.insert(topic.clone(), v.admittance.clone());
                        if !found_no_coin(v) {
                            break;
                        }
                    }
                    Err(did_not_land) => {
                        // No re-judgement follows: its reads were not made.
                        *question_reads -= rejudge;
                        waits.insert(
                            topic.clone(),
                            format!(
                                "{}; submitted first from the BEEF, it did not land ({did_not_land})",
                                not_now.why
                            ),
                        );
                        break;
                    }
                }
            }
            // A subject that already HOLDS an output in the topic landed
            // there before (the leftover of its own earlier submit whose
            // delete was started and faulted, D17 M1: its outputs held, no
            // applied row), and that admission was judged with its coin.
            // Its replay finishes it and does not wait (the E1D lens fold,
            // L4: a proven head spend that names a decoy no peer serves was
            // "not now" on every replay, three and a dead letter). One read,
            // only on the way to "not now", inside the allowance.
            if !finalize
                && waits.contains_key(&topic)
                && *question_reads < PREDECESSOR_READS_PER_SUBMIT
            {
                *question_reads += 1;
                if let Ok(outputs) = stored(
                    bound,
                    self.storage.find_outputs_for_transaction(txid, false),
                )
                .await
                {
                    if outputs.iter().any(|o| o.topic == topic) {
                        info!("topic {topic}: {txid} holds its outputs already; its replay finishes it");
                        waits.remove(&topic);
                        blocked.remove(&topic);
                    }
                }
            }
        }
        waits
    }

    /// Land an unlanded predecessor whose body the successor's BEEF carries
    /// (lane E1D's lens fold, M1): submit it on its own to `topic`, its
    /// atomic BEEF out of the successor's, no off-chain values (the caller
    /// lands nothing into a topic that reads them; the delta fold, L1),
    /// never a broadcast (`current-tx` is submitted as `historical-tx`: the
    /// successor's BEEF carries it to the network).
    ///
    /// Each body is WALKED once in this submit (the delta fold, L4): the
    /// successor's own SPV walk (`subject`, the same mode, tracker and
    /// switch) already passed over every body it reaches without crossing a
    /// proven one, so those are not walked again; a body reached only
    /// through a proven one (which the successor's walk does not descend)
    /// gets its own walk, trusting what this submit already walked, and so
    /// does nothing twice. A walk that refuses a body ends the landing.
    ///
    /// A predecessor whose own submit waits on a carried predecessor of ITS
    /// own goes on a stack under it: ancestors first, the walk's rule at
    /// the door. Its manager was asked only a DRY RUN then and nothing was
    /// written; once the one under it landed it is submitted again and
    /// admitted for real, once ([`Engine::submit_counted`]). Each submit's
    /// reads on its clean path (its applied row, one coin per input, and
    /// its writes' one read of each coin it spends) are charged to the
    /// submit's allowance before it starts, and its predecessor
    /// question shares that allowance, so the landings of one submit stop
    /// at [`PREDECESSOR_READS_PER_SUBMIT`] reads, however deep the chain.
    /// Each body that lands is pushed on `landed` (the report's
    /// `landed_predecessors`, for the caller's admission guard; the delta
    /// fold, L2). Each body is asked of the caller's landing guard first,
    /// once ([`Engine::set_landing_guard`]; the delta-2 fold, L2): a refusal
    /// ends the landing before that body is submitted.
    /// `Ok` once `predecessor` LANDED in `topic` (recorded there,
    /// now or before); otherwise why not, at the first that did not.
    #[allow(clippy::too_many_arguments)]
    async fn land_carried(
        &self,
        beef_bytes: &[u8],
        subject: &str,
        predecessor: &str,
        topic: &str,
        mode: SubmitMode,
        bound: Option<&CallBound>,
        question_reads: &mut usize,
        landed: &mut Vec<(String, String)>,
    ) -> Result<(), String> {
        let beef = Beef::from_binary(beef_bytes)
            .map_err(|e| format!("the BEEF could not be read again ({e})"))?;
        let mode = match mode {
            SubmitMode::CurrentTx => SubmitMode::HistoricalTx,
            other => other,
        };
        let walks = mode != SubmitMode::HistoricalTxNoSpv;
        let mut walked: HashSet<String> = if walks {
            walk_cover(&beef, subject)
        } else {
            HashSet::new()
        };
        let mut stack = vec![predecessor.to_string()];
        let mut guarded: HashSet<String> = HashSet::new();
        while let Some(next) = stack.last().cloned() {
            let body = beef
                .find_atomic_transaction(&next)
                .ok_or_else(|| format!("{next}: its body is not in the BEEF"))?;
            // The caller's admission predicate, before anything of this body
            // is submitted (the delta-2 fold, L2).
            if let Some(guard) = &self.landing_guard {
                if guarded.insert(next.clone()) {
                    bounded(bound, guard(&next, topic))
                        .await
                        .and_then(|answer| answer)
                        .map_err(|why| format!("{next}: refused by the landing guard ({why})"))?;
                }
            }
            // Its validation (the applied row, one coin per input) and its
            // writes' one read of each coin it spends.
            let cost = 1 + 2 * body.inputs.len();
            if *question_reads + cost > PREDECESSOR_READS_PER_SUBMIT {
                return Err(format!(
                    "{next}: its submit does not fit in the submit's {PREDECESSOR_READS_PER_SUBMIT} reads"
                ));
            }
            *question_reads += cost;
            let atomic = body
                .to_atomic_beef(true)
                .map_err(|e| format!("{next}: its BEEF could not be built ({e})"))?;
            let tagged = TaggedBEEF::new(atomic, vec![topic.to_string()]);
            let (_, report, blocked) = self
                .submit_counted(&tagged, mode, bound, false, Some(&walked), question_reads)
                .await
                .map_err(|e| format!("{next}: {e}"))?;
            // Its walk passed, so did that of every body it reaches.
            if walks && !walked.contains(&next) {
                walked.extend(walk_cover(&beef, &next));
            }
            let landed_now = report.applied_topics.iter().any(|t| t == topic);
            if report.is_durable()
                && (landed_now || report.deduped_topics.iter().any(|t| t == topic))
            {
                info!("topic {topic}: {next} landed first from its successor's BEEF");
                if landed_now {
                    landed.push((next.clone(), topic.to_string()));
                }
                stack.pop();
                continue;
            }
            match blocked.get(topic) {
                Some(under) if !stack.contains(under) => stack.push(under.clone()),
                _ if report.is_durable() => return Err(format!("{next}: the manager failed it")),
                _ => return Err(format!("{next}: {}", report.summary())),
            }
        }
        Ok(())
    }

    /// A topic of a submit that is NOT recorded as applied: logged, and
    /// remembered for the successor rule (`not_landed`).
    fn hold_unapplied(&self, txid: &str, topic: &str, why: &str) {
        warn!("topic {topic}: {txid}: {why}; applied_transactions NOT recorded (bsv-low #559)");
        self.not_landed
            .borrow_mut()
            .insert((txid.to_string(), topic.to_string()));
    }

    /// Take out again the outputs a faulted submit inserted (bsv-low #559,
    /// the lens fold of 2026-10-07, F1), so that nothing of a transaction
    /// that did not land is held. Only a row nobody has touched since is
    /// deleted: unspent and consumed by nothing. A plain delete, not the
    /// deep one: the row's retained inputs were not pointed at it yet. A
    /// fault here is reported (`undo_insert_output`) and the row stays: the
    /// limit of an undo made of separate calls.
    async fn undo_inserts(
        &self,
        txid: &str,
        topic: &str,
        output_indices: &[u32],
        bound: Option<&CallBound>,
        report: &mut MutationReport,
    ) {
        for &output_index in output_indices {
            let row = stored(
                bound,
                self.storage
                    .find_output(txid, output_index, Some(topic), None, false),
            )
            .await;
            let undone = match row {
                Ok(Some(row)) if !row.spent && row.consumed_by.is_empty() => {
                    stored(bound, self.storage.delete_output(txid, output_index, topic)).await
                }
                Ok(_) => Ok(()),
                Err(e) => Err(e),
            };
            if let Err(e) = undone {
                error!("Error taking {txid}.{output_index} out of topic {topic} again: {e}");
                report.fault(topic, "undo_insert_output", e.to_string());
            }
        }
    }

    /// The store's own answer to "has a predecessor of `tx` not landed?"
    /// (bsv-low #559, the lens fold of 2026-10-07, F3; lane E1D, bsv-low
    /// #575, and its lens fold of 2026-10-08), for a transaction that found
    /// no previous coin. The engine's memory of a faulted submit
    /// (`not_landed`) dies with the invocation; this does not. A transaction
    /// HAS LANDED in `topic` when it has an applied row there or an output
    /// held there; anything else is not known to have landed, and it is a
    /// PREDECESSOR of `tx` when one of these says so:
    ///
    /// - its body is in the BEEF and it spends a coin the topic HOLDS (or,
    ///   one step up, a coin of another such transaction): a spend of this
    ///   topic the store has not taken yet (#559);
    /// - its body is in the BEEF, it spends no coin the topic holds, the
    ///   manager NAMES the outpoint the walk spends from it as overlay
    ///   history (`identify_needed_inputs` over the BEEF of the transaction
    ///   that spends it, `tx` or a body the walk reached), and the manager,
    ///   asked of that body in a dry run with no coins
    ///   (`TopicAdmittanceContext::DRY_RUN`, the submit's mode, no off-chain
    ///   values), would admit that named output: an OPENER that has not
    ///   landed (E1D, the cure D17 named). Only a NAMED output is dry-run
    ///   (the lens fold, H1): with every unheld body dry-run, a manager that
    ///   admits on output shape (all 16 of this workspace) made a spend of a
    ///   shape-admissible output this node never held (a revocation of an
    ///   ad it never saw; a stranger's own few-sat SHIP output) "not now" on
    ///   every presentation, three replays and a dead letter each, where the
    ///   reference records it. A manager `Err` in the dry run is "not now"
    ///   (the manager's contract at the anchor replay, D15), except its
    ///   typed refusal `NoAdmissibleOutputs`, which is "not admitted";
    /// - its body is NOT in the BEEF (a PROVEN successor carries none) and
    ///   the manager names the outpoint `tx` spends from it as overlay
    ///   history (D13's word; an `Err` there names nothing, as in the walk).
    ///
    /// The manager is asked over `beef_bytes`, the SUBJECT-NAMED BEEF of the
    /// submit (BRC-95 atomic, the body the lookup services get; the lens
    /// fold, M2): over the submitted bytes a manager that parses
    /// `from_beef(_, None)` took the wire-LAST transaction of a non-atomic
    /// BEEF, named an ancestor's inputs, and the question asked nothing.
    ///
    /// `admits` says which inputs of `tx` start the walk. A transaction that
    /// admits nothing (D17's class) starts from every input. One that admits
    /// WITHOUT its coin (E1D) starts only from the inputs the manager names:
    /// a manager that admits a successor with no coin has said the coin does
    /// not decide the admission, so only its own word makes an input a
    /// predecessor. Every manager of this workspace names nothing: an
    /// admitting successor is then asked nothing, as in the reference, and
    /// so is a spend of an unheld output nobody names (the lens fold, H1);
    /// but a successor that admits nothing still finds case (a), a carried
    /// body that spends a coin the topic holds, and waits for it or has it
    /// landed first, where the reference records the successor and never
    /// admits that body (the E1D delta fold, L3). A named input whose body
    /// the BEEF carries is still walked and dry-run, so a named decoy the
    /// manager would not admit is no predecessor.
    ///
    /// The answer carries the predecessor when its body is in the BEEF (the
    /// first two cases): the door lands it first ([`Engine::successors_waiting`]).
    ///
    /// The limits, stated. It cannot tell a faulted predecessor from one
    /// nobody submitted yet: a successor is "not now" in both until the
    /// predecessor lands (the door lands a carried one itself). A named
    /// input whose body is absent and which never lands (a decoy no one
    /// submits, beside a real predecessor that landed and holds no coin)
    /// keeps its successor "not now" at the door; the GASP walk prunes such
    /// an input instead (D14). Deeper than the successor's own inputs, an
    /// absent body ends the walk as before.
    ///
    /// "Landed" needs a clean answer (the delta fold of 2026-10-07, M1 and
    /// M2): a read that faults, or the question running out of its reads
    /// (the 16 of a topic or the 256 of the submit), answers "not now" too,
    /// naming the transaction it could not settle. Recording the successor
    /// there made its replay a dupe and stopped the chain behind it for
    /// good; a retried no-op is the lesser cost.
    ///
    /// What the bound counts (the second delta fold of 2026-10-07, M2): the
    /// reads spent on a body that is NEITHER proven in the BEEF NOR found
    /// landed, [`PREDECESSOR_READS`] of them per topic. A body the BEEF
    /// proves, and one whose applied row or held output answers "landed", is
    /// read and costs nothing against that bound: counted, they made a
    /// transaction that admits nothing over 17 landed parents, over six
    /// proven parents the topic never saw, or over one proven parent with 15
    /// inputs "not now" on every submit, with no unlanded predecessor
    /// anywhere. A landed body still needs ROOM under it: each read of an
    /// unproven body is tested before it is made, so with 15 counted reads
    /// spent one that landed by a held output (no applied row) is refused at
    /// its second read, and with 16 spent any unproven body at its first.
    ///
    /// "Proven" is the BEEF's word here: a body that carries a merkle path
    /// (a bump index) is counted as proven for the read bound ONLY. No proof
    /// is checked at this question; the SPV walk of the submit and the GASP
    /// anchor check are where proofs are checked, and under
    /// `historical-tx-no-spv` or the scripts-only walk nothing checked this
    /// one, so the 16 are the caller's to switch off. Nothing is recorded on
    /// that word (a proven body is walked like any other), and the allowance
    /// below does not ask it.
    ///
    /// The hard allowance (the third delta fold of 2026-10-07, M1):
    /// `question_reads`, owned by the submit, counts EVERY read of the
    /// question, those of proven and landed bodies included, over every
    /// topic of the submit, and stops it at
    /// [`PREDECESSOR_READS_PER_SUBMIT`]; the 16 stay inside it. A later
    /// topic's question starts from what the earlier ones left.
    ///
    /// The costs, stated. A transaction that admits nothing, found no coin
    /// and carries more UNPROVEN, UNLANDED bodies than 16 reads settle (five
    /// single-input ancestors, fewer with more inputs), or a BEEF whose
    /// question needs more than 256 reads over the submit's topics (a
    /// landed body costs one read by its applied row or two by a held
    /// output, a proven, unlanded one two and one per input), is never
    /// recorded, where the reference records it: each submit of it is a
    /// fault and a replay. The manager CPU it adds: one names call per
    /// spender a dry run needs (the subject's, and one per walked body with
    /// an unheld source), and at most one dry run per body the reads
    /// reached; each candidate is built out of the BEEF parsed once (the
    /// lens fold, L2).
    ///
    /// Returns why the submit must wait, for the report.
    #[allow(clippy::too_many_arguments)]
    async fn unlanded_predecessor(
        &self,
        tx: &Transaction,
        txid: &str,
        beef_bytes: &[u8],
        topic: &str,
        mode: SubmitMode,
        admits: bool,
        subject_names: &mut Option<HashSet<String>>,
        question_reads: &mut usize,
        bound: Option<&CallBound>,
    ) -> Option<NotNow> {
        let manager = self.managers.get(topic)?;
        let wait = |why: String| Some(NotNow { why, carried: None });
        // The outpoints `tx` spends, as `txid.vout`.
        let starts: Vec<(String, u32)> = tx
            .inputs
            .iter()
            .filter_map(|input| {
                let source = input.get_source_txid().ok()?;
                (!source.is_empty()).then_some((source, input.source_output_index))
            })
            .collect();
        // The outputs of each candidate that the walk spends, and who spends
        // each: a dry run asks only of an output its spender names.
        let mut spent_of: HashMap<String, HashSet<u32>> = HashMap::new();
        let mut spenders: HashMap<String, Vec<(String, u32)>> = HashMap::new();
        let mut ask: Vec<String> = Vec::new();
        for (source, vout) in &starts {
            if admits
                && subject_names
                    .as_ref()
                    .is_some_and(|named| !named.contains(&format!("{source}.{vout}")))
            {
                continue;
            }
            spent_of.entry(source.clone()).or_default().insert(*vout);
            spenders
                .entry(source.clone())
                .or_default()
                .push((txid.to_string(), *vout));
            ask.push(source.clone());
        }
        // Nothing to ask (a transaction that admits and names nothing, every
        // manager of this workspace): the BEEF is not even parsed.
        if ask.is_empty() {
            return None;
        }
        let direct: HashSet<String> = ask.iter().cloned().collect();
        let beef = Beef::from_binary(beef_bytes).ok()?;
        // Each body the BEEF carries, and whether the BEEF says it is
        // proven (a bump index: its word, no proof is checked here).
        let bodies: HashMap<String, (&Transaction, bool)> = beef
            .txs
            .iter()
            .filter_map(|btx| {
                btx.tx()
                    .map(|body| (btx.txid(), (body, btx.bump_index().is_some())))
            })
            .collect();
        // The manager's names over each walked body other than `tx`.
        let mut names_over: HashMap<String, HashSet<String>> = HashMap::new();
        // The reads spent on bodies neither proven nor found landed.
        let mut reads = 0usize;
        let mut asked: HashSet<String> = HashSet::new();
        while let Some(candidate) = ask.pop() {
            if candidate.is_empty() || !asked.insert(candidate.clone()) {
                continue;
            }
            let spent_outputs = spent_of.get(&candidate).cloned().unwrap_or_default();
            // An absent body is asked of the store only when it is one of
            // `tx`'s own inputs and the manager names it (E1D).
            let body = bodies.get(&candidate).copied();
            if body.is_none() {
                if !direct.contains(&candidate) {
                    continue;
                }
                if subject_names.is_none() {
                    *subject_names = Some(
                        self.named_history(manager.as_ref(), beef_bytes, bound)
                            .await,
                    );
                }
                let is_named = subject_names.as_ref().is_some_and(|named| {
                    spent_outputs
                        .iter()
                        .any(|vout| named.contains(&format!("{candidate}.{vout}")))
                });
                if !is_named {
                    continue;
                }
            }
            let proven = body.is_some_and(|(_, proven)| proven);
            // This candidate's own reads: they join `reads` once it is
            // found not landed, and never if the BEEF proves it.
            let mut spent = 0usize;
            let out_of = |spent: usize| !proven && reads + spent >= PREDECESSOR_READS;
            let out_of_reads = || {
                wait(format!(
                    "{candidate} is not known to have landed: the question ran out of its {PREDECESSOR_READS} reads"
                ))
            };
            // The submit's allowance, over every read: proven or landed,
            // this topic or an earlier one.
            let out_of_allowance = || {
                wait(format!(
                    "{candidate} is not known to have landed: the question ran out of the submit's {PREDECESSOR_READS_PER_SUBMIT} reads"
                ))
            };
            let unread = |e: StorageError| {
                wait(format!(
                    "{candidate} is not known to have landed: the store could not say ({e})"
                ))
            };
            let carried = |why: String| {
                Some(NotNow {
                    why,
                    carried: Some(candidate.clone()),
                })
            };
            let record = AppliedTransaction {
                txid: candidate.clone(),
                topic: topic.to_string(),
            };
            // An applied row, or an output held, ends the question for this
            // transaction: it landed.
            if out_of(spent) {
                return out_of_reads();
            }
            if *question_reads >= PREDECESSOR_READS_PER_SUBMIT {
                return out_of_allowance();
            }
            spent += 1;
            *question_reads += 1;
            match stored(bound, self.storage.does_applied_transaction_exist(&record)).await {
                Ok(false) => {}
                Ok(true) => continue,
                Err(e) => return unread(e),
            }
            if out_of(spent) {
                return out_of_reads();
            }
            if *question_reads >= PREDECESSOR_READS_PER_SUBMIT {
                return out_of_allowance();
            }
            spent += 1;
            *question_reads += 1;
            match stored(
                bound,
                self.storage.find_outputs_for_transaction(&candidate, false),
            )
            .await
            {
                Ok(outputs) if outputs.iter().all(|o| o.topic != topic) => {}
                Ok(_) => continue,
                Err(e) => return unread(e),
            }
            // Not landed. With no body to walk, the manager's word is the
            // whole answer (E1D).
            let Some((body, _)) = body else {
                return wait(format!(
                    "{candidate} has not landed (the manager names it as history; the BEEF does not carry it)"
                ));
            };
            for input in &body.inputs {
                let source = input.get_source_txid().unwrap_or_default();
                if source.is_empty() {
                    continue;
                }
                if out_of(spent) {
                    return out_of_reads();
                }
                if *question_reads >= PREDECESSOR_READS_PER_SUBMIT {
                    return out_of_allowance();
                }
                spent += 1;
                *question_reads += 1;
                let coin = stored(
                    bound,
                    self.storage.find_output(
                        &source,
                        input.source_output_index,
                        Some(topic),
                        None,
                        false,
                    ),
                )
                .await;
                match coin {
                    Ok(Some(_)) => return carried(format!("{candidate} has not landed")),
                    // Not held: its own transaction may be the one that has
                    // not landed.
                    Ok(None) => {
                        spent_of
                            .entry(source.clone())
                            .or_default()
                            .insert(input.source_output_index);
                        spenders
                            .entry(source.clone())
                            .or_default()
                            .push((candidate.clone(), input.source_output_index));
                        ask.push(source);
                    }
                    Err(e) => return unread(e),
                }
            }
            // It spends no coin the topic holds. Does a spender NAME an
            // output the walk spends of it (the lens fold, H1)? A names call
            // per spender, no store read.
            let mut named_outputs: HashSet<u32> = HashSet::new();
            for (spender, vout) in spenders.get(&candidate).cloned().unwrap_or_default() {
                let names = if spender == txid {
                    if subject_names.is_none() {
                        *subject_names = Some(
                            self.named_history(manager.as_ref(), beef_bytes, bound)
                                .await,
                        );
                    }
                    subject_names.as_ref()
                } else {
                    if !names_over.contains_key(&spender) {
                        let names = match beef
                            .find_atomic_transaction(&spender)
                            .and_then(|spender| spender.to_atomic_beef(true).ok())
                        {
                            Some(spender_beef) => {
                                self.named_history(manager.as_ref(), &spender_beef, bound)
                                    .await
                            }
                            None => HashSet::new(),
                        };
                        names_over.insert(spender.clone(), names);
                    }
                    names_over.get(&spender)
                };
                if names.is_some_and(|named| named.contains(&format!("{candidate}.{vout}"))) {
                    named_outputs.insert(vout);
                }
            }
            if !named_outputs.is_empty() {
                // Would the manager admit a named output of it with no coins
                // (E1D, the opener)? A dry run over the candidate built out
                // of the BEEF parsed above (its proof and, unproven, its
                // sources linked): no store read, nothing counted.
                let judged = beef
                    .find_atomic_transaction(&candidate)
                    .unwrap_or_else(|| body.clone());
                let verdict = bounded(
                    bound,
                    manager.identify_admissible_outputs(
                        &judged,
                        &[],
                        None,
                        mode,
                        &TopicAdmittanceContext::DRY_RUN,
                    ),
                )
                .await;
                match verdict {
                    Ok(Ok(admittance)) => {
                        if admittance
                            .outputs_to_admit
                            .iter()
                            .any(|vout| named_outputs.contains(vout))
                        {
                            return carried(format!(
                                "{candidate} has not landed (the manager names the output spent from it and would admit it with no coin: an opener)"
                            ));
                        }
                    }
                    Ok(Err(TopicManagerError::NoAdmissibleOutputs(_))) => {}
                    Ok(Err(e)) => {
                        return wait(format!(
                            "{candidate} is not known to have landed: the manager could not judge it ({e})"
                        ))
                    }
                    Err(no_answer) => {
                        return wait(format!(
                            "{candidate} is not known to have landed: the manager did not answer ({no_answer})"
                        ))
                    }
                }
            }
            if !proven {
                reads += spent;
            }
        }
        None
    }

    /// The outpoints (`txid.vout`) the topic manager NAMES as overlay
    /// history of the transaction `beef_bytes` names (`identify_needed_inputs`,
    /// D13's word; an atomic BEEF, so a manager that parses
    /// `from_beef(_, None)` reads that transaction). An `Err` or no answer
    /// names nothing, as the GASP walk treats it (logged).
    async fn named_history(
        &self,
        manager: &dyn TopicManager,
        beef_bytes: &[u8],
        bound: Option<&CallBound>,
    ) -> HashSet<String> {
        match bounded(bound, manager.identify_needed_inputs(beef_bytes, None)).await {
            Ok(Ok(named)) => named.iter().map(Outpoint::to_graph_id).collect(),
            Ok(Err(e)) => {
                warn!("the predecessor question: the manager could not name its inputs: {e}");
                HashSet::new()
            }
            Err(no_answer) => {
                warn!("the predecessor question: the manager did not name its inputs: {no_answer}");
                HashSet::new()
            }
        }
    }

    /// The reference's `tx.verify(chainTracker)` on submit, walked LINEARLY
    /// over the BEEF's own transaction map: every txid once, a transaction the
    /// BEEF proves (a BUMP) checked by its root against the chain tracker and
    /// not descended, an unproven one script-checked on every input with its
    /// source looked up by txid, plus the reference's value rule (outputs
    /// never exceed inputs). No source objects are cloned or linked.
    ///
    /// Why not bsv-rs `Transaction::verify`: it walks `source_transaction`
    /// links that `from_beef` builds by CLONING, once per input. Two inputs
    /// sourcing the same unproven parent (a covenant head output plus that
    /// spend's own change, the shape of every second head spend of a wallet)
    /// leave the second clone bare ("Input 0 has no source transaction"), and
    /// re-linking every clone materializes the ancestry EXPONENTIALLY along a
    /// diamond chain: a 12-deep unmined chain of head spends took a Worker past
    /// its memory on beta (zanaadu, 2026-09-08). A map keyed by txid is what the
    /// TypeScript SDK effectively has (its inputs share one object), in O(n).
    async fn verify_beef_linear(
        chain_tracker: Option<&dyn bsv_rs::transaction::ChainTracker>,
        beef_bytes: &[u8],
        subject_txid: &str,
        trusted: &HashSet<String>,
    ) -> Result<(), EngineError> {
        Self::verify_beef_linear_with(
            chain_tracker,
            beef_bytes,
            subject_txid,
            WalkPolicy {
                roots: RootPolicy::AgainstTracker,
                budget: None,
            },
            trusted,
        )
        .await
        .map(|_| ())
    }

    /// The linear walk of [`Engine::verify_beef_linear`], parameterised by what
    /// a merkle path means (checked against the chain tracker — the reference's
    /// `tx.verify(chainTracker)` — or trusted unchecked, its `'scripts only'`)
    /// and by an optional door budget (see [`DoorBudget`]). With a budget the
    /// structural faults become [`EngineError::ScriptWalkInconclusive`] and a
    /// bound breach [`EngineError::ScriptWalkOverBudget`], each naming whether
    /// the subject was judged; without one they are `SpvError`, as before.
    ///
    /// The tracker is an ARGUMENT, not `self`'s: the GASP anchor check
    /// ([`verify_spv_like_the_reference`]) runs this same walk from
    /// `OverlayGASPStorage`, which borrows the engine's tracker.
    async fn verify_beef_linear_with(
        chain_tracker: Option<&dyn bsv_rs::transaction::ChainTracker>,
        beef_bytes: &[u8],
        subject_txid: &str,
        policy: WalkPolicy,
        trusted: &HashSet<String>,
    ) -> Result<WalkStats, EngineError> {
        use bsv_rs::primitives::bsv::sighash::{TxInput, TxOutput};
        use bsv_rs::script::{LockingScript, Script, Spend, SpendParams, UnlockingScript};

        let roots = policy.roots;
        let budget = policy.budget;
        let mut stats = WalkStats::default();

        let beef = Beef::from_binary(beef_bytes)
            .map_err(|e| EngineError::BeefParseError(e.to_string()))?;
        let mut by_txid: HashMap<String, &Transaction> = HashMap::new();
        for btx in &beef.txs {
            if let Some(tx) = btx.tx() {
                by_txid.insert(btx.txid(), tx);
            }
        }
        // A structural fault: the reference walk's `SpvError`; the door's
        // `ScriptWalkInconclusive` naming the transaction and whether the
        // subject had already been judged.
        let fault = |at_txid: &str, subject_judged: bool, msg: String| -> EngineError {
            if budget.is_some() {
                EngineError::ScriptWalkInconclusive {
                    at_txid: at_txid.to_string(),
                    subject_judged,
                    reason: msg,
                }
            } else {
                EngineError::SpvError(format!("Unable to verify SPV information: {msg}"))
            }
        };
        let spv =
            |msg: String| EngineError::SpvError(format!("Unable to verify SPV information: {msg}"));
        let over = |at_txid: &str, subject_judged: bool, what: String| -> EngineError {
            EngineError::ScriptWalkOverBudget {
                at_txid: at_txid.to_string(),
                subject_judged,
                what,
            }
        };

        // A trusted transaction (one an earlier walk of the same submit
        // passed over) is neither checked nor descended again.
        let mut seen: HashSet<String> = trusted.clone();
        let mut queue: Vec<String> = vec![subject_txid.to_string()];
        while let Some(txid) = queue.pop() {
            if !seen.insert(txid.clone()) {
                continue;
            }
            let judged = stats.subject_judged;
            let Some(tx) = by_txid.get(&txid).copied() else {
                return Err(fault(
                    &txid,
                    judged,
                    format!("transaction {txid} is not in the BEEF"),
                ));
            };
            if let Some(mp) = beef.find_bump(&txid) {
                if roots == RootPolicy::AcceptUnchecked {
                    // 'scripts only': a proven transaction is trusted as-is —
                    // no root computed, no tracker asked (the reference adds it
                    // to the verified set and stops). The caller's bar is the
                    // network's acceptance, never this proof.
                    continue;
                }
                let root = mp
                    .compute_root(Some(&txid))
                    .map_err(|e| spv(format!("invalid merkle path for transaction {txid}: {e}")))?;
                if let Some(tracker) = chain_tracker {
                    match tracker
                        .is_valid_root_for_height(&root, mp.block_height)
                        .await
                    {
                        Ok(true) => {}
                        Ok(false) => {
                            return Err(EngineError::SpvError(format!(
                                "Invalid merkle path for transaction {txid}: root {root} is not valid for block height {}",
                                mp.block_height
                            )));
                        }
                        Err(e) => {
                            return Err(EngineError::SpvError(format!(
                                "Chain tracker error at height {}: {e}",
                                mp.block_height
                            )));
                        }
                    }
                }
                continue; // proven: trusted, no ancestry needed (the reference stops here too)
            }

            // Unproven: the value rule and every input's script, sources by txid.
            // The door's bounds, judged from the BYTES before anything runs.
            let tx_bytes = tx.to_binary().len();
            if let Some(b) = budget {
                stats.unproven_txs += 1;
                if stats.unproven_txs > b.max_unproven_txs {
                    return Err(over(
                        &txid,
                        judged,
                        format!(
                            "more than {} unproven transactions to execute",
                            b.max_unproven_txs
                        ),
                    ));
                }
                if tx.inputs.len() > b.max_inputs_per_tx {
                    return Err(over(
                        &txid,
                        judged,
                        format!(
                            "transaction {txid} has {} inputs (limit {})",
                            tx.inputs.len(),
                            b.max_inputs_per_tx
                        ),
                    ));
                }
                if tx_bytes > b.max_tx_bytes {
                    return Err(over(
                        &txid,
                        judged,
                        format!(
                            "transaction {txid} is {tx_bytes} bytes (limit {})",
                            b.max_tx_bytes
                        ),
                    ));
                }
            } else {
                stats.unproven_txs += 1;
            }
            let outputs: Vec<TxOutput> = tx
                .outputs
                .iter()
                .map(|o| TxOutput {
                    satoshis: o.satoshis.unwrap_or(0),
                    script: o.locking_script.to_binary(),
                })
                .collect();
            let mut input_total: u64 = 0;
            for (vin, input) in tx.inputs.iter().enumerate() {
                let Some(src_txid) = input.source_txid.clone() else {
                    return Err(fault(
                        &txid,
                        judged,
                        format!("input {vin} of transaction {txid} names no source"),
                    ));
                };
                let Some(source) = by_txid.get(&src_txid).copied() else {
                    return Err(fault(
                        &txid,
                        judged,
                        format!("input {vin} of transaction {txid} has no source transaction"),
                    ));
                };
                let Some(source_output) = source.outputs.get(input.source_output_index as usize)
                else {
                    return Err(fault(
                        &txid,
                        judged,
                        format!(
                            "input {vin} of transaction {txid}: source output index out of bounds"
                        ),
                    ));
                };
                let source_sats = source_output.satoshis.unwrap_or(0);
                input_total = input_total.checked_add(source_sats).ok_or_else(|| {
                    fault(
                        &txid,
                        judged,
                        format!("satoshi total overflows in transaction {txid}"),
                    )
                })?;
                let Some(unlocking) = input.unlocking_script.as_ref() else {
                    return Err(fault(
                        &txid,
                        judged,
                        format!(
                            "input {vin} of transaction {txid} is missing its unlocking script"
                        ),
                    ));
                };
                // The door's static charge for this input (nothing has run yet).
                let unlocking_bytes = unlocking.to_binary();
                let locking_bytes = source_output.locking_script.to_binary();
                if let Some(b) = budget {
                    let (bytes, hash_ops, sig_ops) =
                        script_census(&unlocking_bytes, &locking_bytes, b.memory_limit).map_err(
                            |e| {
                                fault(
                                    &txid,
                                    judged,
                                    format!("input {vin} of transaction {txid}: {e}"),
                                )
                            },
                        )?;
                    stats.script_bytes += bytes;
                    stats.hash_ops += hash_ops;
                    stats.sig_ops += sig_ops;
                    stats.work_bytes += bytes as u64
                        + (hash_ops as u64) * (b.memory_limit as u64)
                        + (sig_ops as u64) * (tx_bytes as u64);
                    if stats.work_bytes > b.max_work_bytes {
                        return Err(over(
                            &txid,
                            judged,
                            format!(
                                "estimated work {} bytes exceeds the door budget of {} (input {vin} of {txid})",
                                stats.work_bytes, b.max_work_bytes
                            ),
                        ));
                    }
                } else {
                    stats.script_bytes += unlocking_bytes.len() + locking_bytes.len();
                }
                let other_inputs: Vec<TxInput> = tx
                    .inputs
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| *i != vin)
                    .map(|(_, inp)| TxInput {
                        txid: inp.get_source_txid_bytes().unwrap_or([0u8; 32]),
                        output_index: inp.source_output_index,
                        script: inp
                            .unlocking_script
                            .as_ref()
                            .map(bsv_rs::UnlockingScript::to_binary)
                            .unwrap_or_default(),
                        sequence: inp.sequence,
                    })
                    .collect();
                let source_txid_bytes = input
                    .get_source_txid_bytes()
                    .map_err(|e| spv(format!("input {vin} of transaction {txid}: {e}")))?;
                let locking_script =
                    LockingScript::from_script(Script::from_binary(&locking_bytes).map_err(
                        |e| fault(&txid, judged, format!("locking script of {src_txid}: {e}")),
                    )?);
                let unlocking_script =
                    UnlockingScript::from_script(Script::from_binary(&unlocking_bytes).map_err(
                        |e| fault(&txid, judged, format!("unlocking script of {txid}: {e}")),
                    )?);
                let mut spend = Spend::new(SpendParams {
                    source_txid: source_txid_bytes,
                    source_output_index: input.source_output_index,
                    source_satoshis: source_sats,
                    locking_script,
                    transaction_version: tx.version.cast_signed(),
                    other_inputs,
                    outputs: outputs.clone(),
                    input_index: vin,
                    unlocking_script,
                    input_sequence: input.sequence,
                    lock_time: tx.lock_time,
                    memory_limit: budget.map(|b| b.memory_limit),
                });
                stats.inputs_executed += 1;
                match spend.validate() {
                    Ok(true) => {}
                    Ok(false) => {
                        return Err(EngineError::ScriptVerificationFailed {
                            subject_txid: txid.clone(),
                            input_index: vin as u32,
                            reason: "script evaluated to false".into(),
                        });
                    }
                    // The door's OWN limit tripping inside the interpreter (the
                    // stack memory limit is the budget's element bound) is the
                    // door's verdict, never the network's: over budget, not
                    // refused. bsv-rs 0.3.23 reports it as its own class
                    // (`resource_limit`, the ts-sdk's `ScriptResourceLimitError`:
                    // the stack budget, the alt stack, NUM2BIN's element-size
                    // pre-check), so no wording is matched.
                    Err(e) if budget.is_some() && e.is_resource_limit() => {
                        return Err(over(
                            &txid,
                            judged,
                            format!("input {vin} of transaction {txid}: {}", e.message),
                        ));
                    }
                    Err(e) => {
                        return Err(EngineError::ScriptVerificationFailed {
                            subject_txid: txid.clone(),
                            input_index: vin as u32,
                            reason: e.message.clone(),
                        });
                    }
                }
                queue.push(src_txid);
            }
            if txid == subject_txid {
                stats.subject_judged = true;
            }
            let mut output_total: u64 = 0;
            for output in &tx.outputs {
                output_total = output_total
                    .checked_add(output.satoshis.unwrap_or(0))
                    .ok_or_else(|| {
                        fault(
                            &txid,
                            stats.subject_judged,
                            format!("satoshi total overflows in transaction {txid}"),
                        )
                    })?;
            }
            if output_total > input_total {
                return Err(fault(
                    &txid,
                    stats.subject_judged,
                    format!(
                        "transaction {txid} creates {output_total} sats from {input_total} sats of inputs"
                    ),
                ));
            }
        }
        Ok(stats)
    }

    /// The pre-2026-09-08 SPV block, kept verbatim as the
    /// [`Engine::set_script_verification`] `false` escape hatch: BEEF
    /// structural validity plus every root against the chain tracker, and
    /// NOTHING when no tracker is configured. It never executes a script.
    async fn verify_spv_structurally(
        chain_tracker: Option<&dyn bsv_rs::transaction::ChainTracker>,
        beef_bytes: &[u8],
    ) -> Result<(), EngineError> {
        let Some(chain_tracker) = chain_tracker else {
            return Ok(());
        };
        let mut beef = Beef::from_binary(beef_bytes)
            .map_err(|e| EngineError::SpvError(format!("BEEF parse error: {e}")))?;
        let validation = beef.verify_valid(false);
        if !validation.valid {
            return Err(EngineError::SpvError(
                "BEEF internal proof validation failed".into(),
            ));
        }
        for (height, root) in &validation.roots {
            match chain_tracker.is_valid_root_for_height(root, *height).await {
                Ok(true) => {}
                Ok(false) => {
                    return Err(EngineError::SpvError(format!(
                        "Merkle root {root} invalid for block height {height}"
                    )));
                }
                Err(e) => {
                    return Err(EngineError::SpvError(format!(
                        "Chain tracker error at height {height}: {e}"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Phase 1 of ONE topic: the dedup read, the previous coins, the
    /// manager's judgement. Read-only. [`Engine::run_validation`] asks it of
    /// every topic; the door asks it again of a topic whose predecessor it
    /// landed first from the BEEF (lane E1D's lens fold, M1).
    #[allow(clippy::too_many_arguments)]
    async fn validate_topic(
        &self,
        tx: &Transaction,
        txid: &str,
        topic: &str,
        off_chain_values: Option<&[u8]>,
        mode: SubmitMode,
        context: &TopicAdmittanceContext,
        bound: Option<&CallBound>,
    ) -> TopicValidation {
        let tx_record = AppliedTransaction {
            txid: txid.to_string(),
            topic: topic.to_string(),
        };
        // A dedup read that FAULTS is the topic's read fault (the delta
        // fold of 2026-10-07, H3): nothing is judged and nothing is
        // written for the topic, the fault rides the report and the
        // replay asks again. The reference fails the topic there
        // (`Engine.ts` `submit`: the throw lands in the per-topic catch,
        // `failedTopics.add`). Read as "not a dupe", as it was, a
        // re-presented transaction the manager admits with no previous
        // coin was inserted a second time, unspent, beside the output
        // that had spent it, under a durable report.
        let is_dupe = match stored(
            bound,
            self.storage.does_applied_transaction_exist(&tx_record),
        )
        .await
        {
            Ok(is_dupe) => is_dupe,
            Err(e) => {
                error!("Error reading the applied row of {txid} for topic {topic}: {e}");
                return TopicValidation {
                    topic: topic.to_string(),
                    is_dupe: false,
                    previous_coins: vec![],
                    previous_outputs: vec![],
                    admittance: AdmittanceInstructions::default(),
                    failed: false,
                    read_fault: Some(("does_applied_transaction_exist", e.to_string())),
                };
            }
        };

        if is_dupe {
            return TopicValidation {
                topic: topic.to_string(),
                is_dupe: true,
                previous_coins: vec![],
                previous_outputs: vec![],
                admittance: AdmittanceInstructions::default(),
                failed: false,
                read_fault: None,
            };
        }

        let mut previous_coins: Vec<u32> = Vec::new();
        let mut previous_outputs: Vec<Output> = Vec::new();
        let mut read_fault: Option<(&'static str, String)> = None;

        for (input_idx, input) in tx.inputs.iter().enumerate() {
            let source_txid = input.get_source_txid().unwrap_or_default();
            if source_txid.is_empty() {
                continue;
            }
            match stored(
                bound,
                self.storage.find_output(
                    &source_txid,
                    input.source_output_index,
                    Some(topic),
                    None,
                    false,
                ),
            )
            .await
            {
                Ok(Some(prev_output)) => {
                    previous_coins.push(input_idx as u32);
                    previous_outputs.push(prev_output);
                }
                Ok(None) => {}
                Err(e) => {
                    // A faulted read is NOT "no previous coin": the
                    // spend may consume an admitted output we simply
                    // could not read. Validation proceeds (the topic
                    // manager decides on what it can see) but the fault
                    // rides the report and blocks the applied record.
                    error!(
                        "Error reading previous output {source_txid}:{} for topic {topic}: {e}",
                        input.source_output_index
                    );
                    if read_fault.is_none() {
                        read_fault = Some(("find_output", e.to_string()));
                    }
                }
            }
        }

        let judged = self
            .judge(
                tx,
                topic,
                &previous_coins,
                off_chain_values,
                mode,
                context,
                bound,
            )
            .await;
        match judged {
            Ok(admittance) => TopicValidation {
                topic: topic.to_string(),
                is_dupe: false,
                previous_coins,
                previous_outputs,
                admittance,
                failed: false,
                read_fault: read_fault.clone(),
            },
            Err(e) => {
                error!("Error validating topic {topic} during submit: {e}");
                TopicValidation {
                    topic: topic.to_string(),
                    is_dupe: false,
                    previous_coins: vec![],
                    previous_outputs: vec![],
                    admittance: AdmittanceInstructions::default(),
                    failed: true,
                    read_fault,
                }
            }
        }
    }

    /// The topic manager's judgement of `tx` over the previous coins found
    /// (input indices), as [`Engine::validate_topic`] asks it; a carried
    /// predecessor's submit asks it again for real once its topic does not
    /// wait (the E1D delta fold, L4).
    #[allow(clippy::too_many_arguments)]
    async fn judge(
        &self,
        tx: &Transaction,
        topic: &str,
        previous_coins: &[u32],
        off_chain_values: Option<&[u8]>,
        mode: SubmitMode,
        context: &TopicAdmittanceContext,
        bound: Option<&CallBound>,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        let manager = &self.managers[topic];
        let previous_coin_bytes = previous_coins
            .iter()
            .flat_map(|i| i.to_le_bytes())
            .collect::<Vec<u8>>();
        // Under a finalize submit's bound a manager that does not answer
        // is a manager that failed: nothing is written for the topic.
        bounded(
            bound,
            manager.identify_admissible_outputs(
                // The ONE engine parse of the submitted BEEF, shared by
                // every topic on this submit (bsv-low #289 — managers
                // used to re-parse the same bytes independently).
                tx,
                &previous_coin_bytes,
                off_chain_values,
                mode,
                // `dry_run` is `false` on a submit, `true` on a
                // validate-only call.
                context,
            ),
        )
        .await
        .map_err(TopicManagerError::Other)
        .and_then(|judged| judged)
    }

    /// Whether the door may land a carried predecessor into `topic` (the
    /// E1D delta fold, L1): neither its manager nor any lookup service
    /// reads off-chain values there, which the landing does not have.
    fn lands_without_values(&self, topic: &str) -> bool {
        self.managers
            .get(topic)
            .is_some_and(|manager| !manager.reads_off_chain_values())
            && self
                .lookup_services
                .values()
                .all(|ls| !ls.reads_off_chain_values(topic))
    }

    /// Run Phase 1 (topic validation) and Phase 2 (broadcast) without mutating storage.
    ///
    /// Returns (validations, steak, parsed_tx, txid) so the caller can either
    /// stop (validate-only) or proceed with Phase 3 (mutations).
    async fn run_validation(
        &self,
        tagged_beef: &TaggedBEEF,
        mode: SubmitMode,
        context: &TopicAdmittanceContext,
        bound: Option<&CallBound>,
        walked: Option<&HashSet<String>>,
    ) -> Result<(Vec<TopicValidation>, Steak, Transaction, String), EngineError> {
        // Validate all topics are supported
        for topic in &tagged_beef.topics {
            if !self.managers.contains_key(topic) {
                return Err(EngineError::UnsupportedTopic(topic.clone()));
            }
        }

        // Parse the SUBJECT from the BEEF — order-independent (`subject.rs`,
        // loop-2 hardening 2026-09-05): `from_beef(_, None)` takes the last
        // tx in wire order, which an SDK-sorted INCOMPLETE ancestry fills
        // with a fully-sourced ancestor; the engine then judged a hop, admitted
        // nothing and the pot never entered the index while its JOIN mined.
        let subject = {
            let mut b = bsv_rs::transaction::Beef::from_binary(&tagged_beef.beef)
                .map_err(|e| EngineError::BeefParseError(e.to_string()))?;
            crate::subject::subject_txid_of(&mut b)
        };
        let tx = Transaction::from_beef(&tagged_beef.beef, subject.as_deref())
            .map_err(|e| EngineError::BeefParseError(e.to_string()))?;
        let txid = tx.id();

        // SPV verification, skipped ONLY for HistoricalTxNoSpv, exactly the
        // reference's `if (mode !== 'historical-tx-no-spv') tx.verify(...)`.
        // That mode exists for GASP `finalizeGraph`, whose graphs
        // `validateGraphAnchor` already verified, and since bsv-low #551 that
        // is TRUE here: `OverlayGASPStorage::validate_graph_anchor` runs this
        // same check ([`verify_spv_like_the_reference`], the same tracker and
        // the same switch) over the ROOT node's BEEF and replays the graph
        // through the topic manager before a single BEEF of it reaches this
        // door. Before #551 the anchor check was a no-op and this skip
        // admitted a peer's graph on structure alone.
        //
        // A predecessor the door lands first (`walked`, the E1D delta fold,
        // L4) is not walked again when this submit's walks already passed
        // over it, and its walk trusts what they passed over.
        if mode != SubmitMode::HistoricalTxNoSpv {
            match walked {
                Some(walked) if walked.contains(&txid) => {}
                trusted => {
                    verify_spv_trusting(
                        self.chain_tracker.as_deref(),
                        self.verify_scripts,
                        &tagged_beef.beef,
                        &txid,
                        trusted.unwrap_or(&HashSet::new()),
                    )
                    .await?;
                }
            }
        }

        let mut steak = Steak::new();

        // =============================================================
        // PHASE 1: VALIDATE (read-only)
        // =============================================================
        let mut validations = Vec::new();

        for topic in &tagged_beef.topics {
            validations.push(
                self.validate_topic(
                    &tx,
                    &txid,
                    topic,
                    tagged_beef.off_chain_values.as_deref(),
                    mode,
                    context,
                    bound,
                )
                .await,
            );
        }

        // Build preliminary STEAK
        for v in &validations {
            steak.insert(v.topic.clone(), v.admittance.clone());
        }

        // =============================================================
        // PHASE 2: BROADCAST / SHIP propagation (before mutations)
        // =============================================================
        if mode == SubmitMode::CurrentTx {
            // ── ARC network broadcast ──────────────────────────────────
            // Broadcast to miners via ARC, matching the TS Engine pattern.
            // Skip if the tx already has a merkle path (already mined).
            //
            // INCIDENT D1-CALLBACK-FLOOD 2026-09-01: also skip when EVERY
            // topic classified the tx as a dupe (already applied) — the
            // exact `!v.is_dupe` filter the SHIP arm below has always had,
            // ten lines away. Without it, every RE-PRESENT of an
            // already-applied tx (cron ad-sync, peer crawl, a client
            // retrying a default-mode /submit) re-POSTed to Arcade and
            // re-registered the status callback, and registrations
            // accumulate per POST on Arcade's side — the flood's fuel. A
            // genuinely new tx (any topic non-dupe) broadcasts exactly as
            // before.
            let all_topics_dupe = !validations.is_empty() && validations.iter().all(|v| v.is_dupe);
            if let Some(ref arc) = self.arc_broadcaster {
                if all_topics_dupe {
                    info!(
                        "Skipping ARC broadcast — tx already applied to every topic (re-present)"
                    );
                } else if tx.merkle_path.is_none() {
                    let raw_tx_hex = tx.to_hex();
                    match arc.broadcast(&raw_tx_hex).await {
                        Ok(arc_txid) => {
                            info!("ARC broadcast succeeded: txid={arc_txid}");
                        }
                        Err(e) => {
                            error!("ARC broadcast failed (non-fatal): {e}");
                        }
                    }
                } else {
                    info!("Skipping ARC broadcast — tx already has merkle proof");
                }
            }

            // ── SHIP peer propagation ──────────────────────────────────
            if let Some(ref broadcaster) = self.broadcaster {
                let relevant_topics: Vec<String> = validations
                    .iter()
                    .filter(|v| {
                        !v.is_dupe && !v.failed && !v.admittance.outputs_to_admit.is_empty()
                    })
                    .map(|v| v.topic.clone())
                    .collect();

                if !relevant_topics.is_empty() {
                    if let Some(ship_ls) = self.lookup_services.get("ls_ship") {
                        for topic in &relevant_topics {
                            let question = LookupQuestion::new(
                                "ls_ship",
                                serde_json::json!({ "topics": [topic] }),
                            );
                            match ship_ls.lookup(&question).await {
                                Ok(LookupResult::OutputList(refs)) => {
                                    for reference in &refs {
                                        if let Ok(Some(output)) = self
                                            .storage
                                            .find_output(
                                                &reference.txid,
                                                reference.output_index,
                                                None,
                                                None,
                                                false,
                                            )
                                            .await
                                        {
                                            if let Some(domain) =
                                                parse_ship_domain_from_script(&output.output_script)
                                            {
                                                if let Some(ref our_url) = self.config.hosting_url {
                                                    if domain.trim_end_matches('/')
                                                        == our_url.trim_end_matches('/')
                                                    {
                                                        continue;
                                                    }
                                                }
                                                if let Err(e) = broadcaster
                                                    .broadcast_to_host(&domain, tagged_beef)
                                                    .await
                                                {
                                                    error!(
                                                        "SHIP propagation to {domain} failed for topic {topic}: {e}"
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                                Ok(LookupResult::Answer(_)) => {
                                    // SHIP only ever returns OutputList; an Answer here means
                                    // a misconfigured replacement service. Skip rather than
                                    // attempt to extract refs from a freeform/formula payload.
                                    error!(
                                        "SHIP lookup for topic {topic} returned a pre-formed \
                                         LookupAnswer; expected OutputList. Skipping fanout."
                                    );
                                }
                                Err(e) => {
                                    error!("SHIP lookup for topic {topic} failed: {e}");
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok((validations, steak, tx, txid))
    }

    // ========================================================================
    // Lookup
    // ========================================================================

    /// Answer a lookup query.
    ///
    /// Delegates to the appropriate LookupService, then hydrates results with BEEF.
    ///
    /// When `history_selector` is provided, each output is hydrated with its
    /// ancestor spend chain (via `get_utxo_history`) before building the response.
    /// This matches the TS Engine behavior when a history selector is configured.
    pub async fn lookup(
        &self,
        question: &LookupQuestion,
        history_selector: Option<HistorySelector>,
    ) -> Result<LookupAnswer, EngineError> {
        Ok(self.lookup_with_txids(question, history_selector).await?.0)
    }

    /// Same as [`Engine::lookup`], additionally returning each hydrated
    /// output's txid, aligned index-for-index with the `OutputList` items
    /// (empty for pre-formed `Answer`s).
    ///
    /// The txid IS the storage primary key the row was just fetched by —
    /// handing it to the caller lets the aggregated wire serializer write it
    /// directly instead of re-deriving it with a full BEEF parse plus a
    /// double-SHA256 per output (bsv-low #289).
    pub async fn lookup_with_txids(
        &self,
        question: &LookupQuestion,
        history_selector: Option<HistorySelector>,
    ) -> Result<(LookupAnswer, Vec<String>), EngineError> {
        let service = self
            .lookup_services
            .get(&question.service)
            .ok_or_else(|| EngineError::LookupServiceNotFound(question.service.clone()))?;

        // Preserve WHOSE fault it was. A lookup service reports a malformed
        // query as `InvalidQuery` / `Unsupported`; stringifying both into
        // `LookupFailed` (as this did) threw that away and reported every
        // caller mistake as a 500.
        let result = service.lookup(question).await.map_err(|e| match e {
            LookupServiceError::InvalidQuery(m) | LookupServiceError::Unsupported(m) => {
                EngineError::InvalidQuery(m)
            }
            other => EngineError::LookupFailed(other.to_string()),
        })?;

        // Two paths per LookupResult:
        // - OutputList(refs): the LS yields outpoints; we hydrate each with
        //   BEEF (and optional ancestor chain via history_selector) and
        //   assemble LookupAnswer::OutputList.
        // - Answer(answer): the LS already produced the full answer
        //   (Freeform/Formula); pass through verbatim. The Engine does NOT
        //   apply history-selector hydration to a pre-formed Answer — the
        //   LS is presumed to have embedded whatever ancestry it wants.
        let refs = match result {
            LookupResult::OutputList(refs) => refs,
            LookupResult::Answer(answer) => return Ok((answer, Vec::new())),
        };

        // ONE batched hydration query for the whole result set instead of a
        // D1 round-trip per row (the #289 N+1). Order is preserved: the
        // batch contract returns outputs in input-outpoint order, skipping
        // outpoints with no stored row. A storage error is a REAL error now
        // (it used to be swallowed per-row into a silently-empty list — the
        // "vanishing table" failure mode): an uncertain read must never
        // masquerade as a confidently-empty lobby.
        let outpoints: Vec<Outpoint> = refs
            .iter()
            .map(|r| Outpoint::new(&r.txid, r.output_index))
            .collect();
        let found = self
            .storage
            .find_outputs_by_outpoints(&outpoints, true)
            .await
            .map_err(|e| EngineError::StorageError(e.to_string()))?;

        let mut outputs = Vec::new();
        let mut txids = Vec::new();
        for output in found {
            // If history selector provided, hydrate ancestor chain
            let final_output = if history_selector.is_some() {
                match self
                    .get_utxo_history(&output, history_selector.clone())
                    .await
                {
                    Ok(Some(hydrated)) => hydrated,
                    _ => output,
                }
            } else {
                output
            };

            // Skip an output whose BEEF is missing OR empty: a lookup item
            // without decodable BEEF is useless to the client (it can only
            // drop it), and returning an empty-BEEF row is what made a fresh
            // LOW table show up in a query yet be undecodable/invisible to
            // opponents (the "vanishing table" — 2026-07-11). Only ever
            // return fully-hydrated, decodable outputs.
            if let Some(beef) = final_output.beef {
                if !beef.is_empty() {
                    txids.push(final_output.txid.clone());
                    outputs.push(OutputListItem {
                        beef,
                        output_index: final_output.output_index,
                        context: None,
                    });
                }
            }
        }

        Ok((LookupAnswer::OutputList { outputs }, txids))
    }

    // ========================================================================
    // Metadata
    // ========================================================================

    // ========================================================================
    // UTXO History
    // ========================================================================

    /// Traverse and return the history of a UTXO.
    ///
    /// If no `history_selector` is provided, returns the output as-is.
    /// If a depth selector is provided, includes up to N levels of ancestor
    /// transactions by embedding them as source_transactions in the BEEF.
    pub async fn get_utxo_history(
        &self,
        output: &Output,
        history_selector: Option<HistorySelector>,
    ) -> Result<Option<Output>, EngineError> {
        let Some(selector) = history_selector else {
            return Ok(Some(output.clone()));
        };

        // Verify BEEF exists before attempting hydration
        if output.beef.is_none() {
            return Err(EngineError::Other(
                "Output must have associated transaction BEEF!".into(),
            ));
        }

        match self.hydrate_utxo_history(output, &selector, 0).await {
            Ok(Some(hydrated_output)) => Ok(Some(hydrated_output)),
            Ok(None) => Ok(None),
            Err(e) => Err(EngineError::Other(format!(
                "Error retrieving UTXO history: {e}"
            ))),
        }
    }

    /// Recursive UTXO history hydration.
    fn hydrate_utxo_history<'a>(
        &'a self,
        output: &'a Output,
        selector: &'a HistorySelector,
        current_depth: u32,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Option<Output>, EngineError>> + 'a>,
    > {
        Box::pin(async move {
            let beef_data = output.beef.as_ref().ok_or_else(|| {
                EngineError::Other("Output must have associated transaction BEEF!".into())
            })?;

            // Check if we should traverse at this depth
            let should_traverse = match selector {
                HistorySelector::Depth(max_depth) => current_depth <= *max_depth,
            };

            if !should_traverse {
                return Ok(None);
            }

            // Start from the STORED BEEF, not from a re-parsed subject.
            //
            // #4: this used to be `Transaction::from_beef(beef_data, None)` followed
            // by `tx.to_beef(true)`, which keeps only the subject transaction and
            // therefore DESTROYS the stored BUMPs. Measured on prod before the fix:
            // `ls_low` byGameId `73de6e48…` returned 1,172 B / 1 tx / 1 bump (PROVEN)
            // with no header, and 676 B / 1 tx / 0 bumps with `x-history-depth: 3` —
            // a caller asking for MORE provenance got back a BEEF no wallet can
            // verify, silently, behind a 200.
            //
            // Accumulating into the stored BEEF fixes both halves: its own bumps
            // survive, and so does any ancestry that arrived inside the submitted
            // bytes but was never admitted as an output of its own (a foreign
            // parent), which the `outputs_consumed` walk below cannot see.
            let mut acc = Beef::from_binary(beef_data)
                .map_err(|e| EngineError::BeefParseError(e.to_string()))?;

            // For each consumed output, recursively hydrate and embed as source transaction
            for consumed in &output.outputs_consumed {
                if let Ok(Some(child_output)) = self
                    .storage
                    .find_output(&consumed.txid, consumed.output_index, None, None, true)
                    .await
                {
                    if let Ok(Some(hydrated_child)) = self
                        .hydrate_utxo_history(&child_output, selector, current_depth + 1)
                        .await
                    {
                        // MERGE the hydrated ancestor in, rather than embedding it
                        // as a `source_transaction` on a re-parsed subject: a merge
                        // carries the child's own BUMPs across, which is the whole
                        // point of asking for history. An unparseable child is
                        // skipped — never allowed to poison the accumulator.
                        if let Some(ref child_beef) = hydrated_child.beef {
                            if let Ok(child) = Beef::from_binary(child_beef) {
                                acc.merge_beef(&child);
                            }
                        }
                    }
                }
            }

            Ok(Some(Output {
                beef: Some(acc.to_binary()),
                ..output.clone()
            }))
        })
    }

    // ========================================================================
    // Metadata
    // ========================================================================

    pub async fn list_topic_managers(&self) -> HashMap<String, ServiceMetadata> {
        let mut result = HashMap::new();
        for (name, manager) in &self.managers {
            let meta = manager.get_metadata().await;
            result.insert(name.clone(), meta);
        }
        result
    }

    /// List all registered lookup services with their metadata.
    pub async fn list_lookup_service_providers(&self) -> HashMap<String, ServiceMetadata> {
        let mut result = HashMap::new();
        for (name, service) in &self.lookup_services {
            let meta = service.get_metadata().await;
            result.insert(name.clone(), meta);
        }
        result
    }

    /// Get documentation for a specific topic manager.
    pub async fn get_documentation_for_topic_manager(&self, name: &str) -> String {
        match self.managers.get(name) {
            Some(manager) => manager.get_documentation().await,
            None => "No documentation found!".to_string(),
        }
    }

    /// Get documentation for a specific lookup service.
    pub async fn get_documentation_for_lookup_service(&self, name: &str) -> String {
        match self.lookup_services.get(name) {
            Some(service) => service.get_documentation().await,
            None => "No documentation found!".to_string(),
        }
    }

    // ========================================================================
    // GASP endpoints
    // ========================================================================

    /// Respond to a GASP initial sync request.
    ///
    /// Returns UTXOs for the given topic since the requested score.
    ///
    /// The caller-supplied `limit` is CLAMPED to
    /// [`Engine::SYNC_RESPONSE_MAX_LIMIT`] (and defaulted when absent):
    /// `/requestSyncResponse` is a PUBLIC route, and an absent limit used to
    /// mean an unbounded scan of the outputs table (bsv-low #291). Bounding
    /// is lossless for sync correctness because the initiator
    /// ([`crate::gasp::GASPSync`]) pages for as long as its `since` score
    /// cursor advances — a truncated page is fetched by its NEXT request
    /// within the same run (gate finding M1: pagination must not demand a
    /// full page, which this clamp can make impossible).
    pub async fn provide_foreign_sync_response(
        &self,
        request: &GASPInitialRequest,
        topic: &str,
    ) -> Result<GASPInitialResponse, EngineError> {
        let limit = Self::clamp_sync_limit(request.limit);
        let outputs = self
            .storage
            .find_utxos_for_topic(topic, Some(request.since as f64), Some(limit), false)
            .await
            .map_err(|e| EngineError::StorageError(e.to_string()))?;

        Ok(GASPInitialResponse {
            utxo_list: outputs
                .iter()
                .map(|o| GASPOutput {
                    txid: o.txid.clone(),
                    output_index: o.output_index,
                    score: o.score.unwrap_or(0.0),
                })
                .collect(),
            since: request.since,
        })
    }

    /// Provide a GASPNode for a specific transaction within a graph.
    ///
    /// Searches the BEEF tree of the root output (identified by graphID) for
    /// the requested txid. If not found in the BEEF tree, recurses through
    /// outputsConsumed in storage.
    pub async fn provide_foreign_gasp_node(
        &self,
        graph_id: &str,
        txid: &str,
        output_index: u32,
    ) -> Result<GASPNode, EngineError> {
        let root_outpoint = Outpoint::from_graph_id(graph_id)
            .ok_or_else(|| EngineError::Other(format!("Invalid graphID: {graph_id}")))?;

        let root_output = self
            .storage
            .find_output(
                &root_outpoint.txid,
                root_outpoint.output_index,
                None,
                None,
                true,
            )
            .await
            .map_err(|e| EngineError::StorageError(e.to_string()))?
            .ok_or(EngineError::NodeNotFound)?;

        self.hydrate_gasp_node(&root_output, graph_id, txid, output_index)
            .await
    }

    /// Recursively search a BEEF tree for a specific txid and return a GASPNode.
    fn hydrate_gasp_node<'a>(
        &'a self,
        output: &'a Output,
        graph_id: &'a str,
        txid: &'a str,
        output_index: u32,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<GASPNode, EngineError>> + 'a>>
    {
        Box::pin(async move {
            let beef_data = output.beef.as_ref().ok_or(EngineError::NodeNotFound)?;

            // The stored body's SUBJECT by the same order-independent rule
            // (`subject.rs`) — a plain stored body's last tx can be an ancestor.
            let root = bsv_rs::transaction::Beef::from_binary(beef_data)
                .ok()
                .and_then(|mut b| crate::subject::subject_txid_of(&mut b));
            let root_tx = Transaction::from_beef(beef_data, root.as_deref())
                .map_err(|e| EngineError::BeefParseError(e.to_string()))?;

            // Search the transaction tree for the requested txid
            if let Some(node) = Self::search_tx_tree(&root_tx, graph_id, txid, output_index) {
                return Ok(node);
            }

            // Fallback: try parsing BEEF with the target txid directly
            // (some BEEF structures have the tx in the list but not linked as source_transaction)
            if let Some(node) = Self::search_beef_for_txid(beef_data, graph_id, txid, output_index)
            {
                return Ok(node);
            }

            // Not found in BEEF tree — recurse through outputsConsumed in storage
            for consumed in &output.outputs_consumed {
                if let Ok(Some(consumed_output)) = self
                    .storage
                    .find_output(&consumed.txid, consumed.output_index, None, None, true)
                    .await
                {
                    if let Ok(node) = self
                        .hydrate_gasp_node(&consumed_output, graph_id, txid, output_index)
                        .await
                    {
                        return Ok(node);
                    }
                }
            }

            // The graph root is known but neither its BEEF nor anything it
            // consumed holds the requested transaction. That is a definite
            // "not held", the same answer as an unknown root: the reference
            // throws `Unable to find output associated with your request!`
            // here and its route answers 400. A syncing peer reads the 400 as
            // `GASPError::NodeNotFound` and prunes a decoy input on it (the D8
            // rule, `GASPSync::process_incoming_node`); a 500 would read as a
            // fault of the moment and strand the chain behind the decoy.
            Err(EngineError::NodeNotFound)
        })
    }

    /// Search a Transaction tree (via sourceTransaction links) for a specific txid.
    fn search_tx_tree(
        tx: &Transaction,
        graph_id: &str,
        target_txid: &str,
        output_index: u32,
    ) -> Option<GASPNode> {
        let current_txid = tx.id();

        if current_txid == target_txid {
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
            return Some(node);
        }

        // Recurse into inputs via source_transaction
        for input in &tx.inputs {
            if let Some(ref source_tx) = input.source_transaction {
                if let Some(node) =
                    Self::search_tx_tree(source_tx, graph_id, target_txid, output_index)
                {
                    return Some(node);
                }
            }
        }

        None
    }

    /// Alternative search: try parsing the BEEF with the target txid directly.
    /// Some BEEF structures may not link source_transaction on inputs but
    /// still contain the transaction in the BEEF's transaction list.
    fn search_beef_for_txid(
        beef_data: &[u8],
        graph_id: &str,
        target_txid: &str,
        output_index: u32,
    ) -> Option<GASPNode> {
        // Try parsing BEEF with the specific txid
        if let Ok(tx) = Transaction::from_beef(beef_data, Some(target_txid)) {
            if tx.id() == target_txid {
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
                return Some(node);
            }
        }
        None
    }

    // ========================================================================
    // Merkle proof handling
    // ========================================================================

    /// Handle a new merkle proof for a transaction.
    ///
    /// When a transaction gets mined, ARC calls back with the merkle proof.
    /// This method:
    /// 1. Finds all outputs for the txid in storage
    /// 2. Parses the BEEF, updates the merkle path in the transaction tree
    /// 3. Serializes updated BEEF back to storage
    /// 4. Recursively updates the consumedBy chain (outputs that spent this one)
    /// 5. Updates blockHeight on the outputs
    pub async fn handle_new_merkle_proof(
        &self,
        txid: &str,
        proof_hex: &str,
        block_height: Option<u32>,
    ) -> Result<(), EngineError> {
        let outputs = self
            .storage
            .find_outputs_for_transaction(txid, true)
            .await
            .map_err(|e| EngineError::StorageError(e.to_string()))?;

        if outputs.is_empty() {
            return Err(EngineError::Other(
                "Could not find matching transaction outputs for proof ingest!".into(),
            ));
        }

        // Parse merkle proof if provided
        let proof = if proof_hex.is_empty() {
            None
        } else {
            Some(
                bsv_rs::transaction::MerklePath::from_hex(proof_hex)
                    .map_err(|e| EngineError::Other(format!("Invalid merkle proof hex: {e}")))?,
            )
        };

        for output in &outputs {
            // Stitch the proof into the STORED BEEF (#284) — never rebuild it
            // around a re-picked subject; see stitch_proof_into_stored_beef.
            if let (Some(ref beef_data), Some(ref proof)) = (&output.beef, &proof) {
                if let Some(new_beef) = Self::stitch_proof_into_stored_beef(beef_data, txid, proof)
                {
                    let _ = self
                        .storage
                        .update_transaction_beef(&output.txid, &new_beef)
                        .await;
                }
            }

            // Update block height
            if let Some(height) = block_height {
                let _ = self
                    .storage
                    .update_output_block_height(
                        &output.txid,
                        output.output_index,
                        &output.topic,
                        height,
                    )
                    .await;
            }

            // Recursively update consumedBy chain
            for consuming in &output.consumed_by {
                if let Ok(consumed_outputs) = self
                    .storage
                    .find_outputs_for_transaction(&consuming.txid, true)
                    .await
                {
                    for consumed_output in &consumed_outputs {
                        // The consuming row's BEEF carries this tx as an
                        // ancestor — stitch the same bump in there too, again
                        // without rebuilding the tx set (#284: this branch is
                        // the one that was observed replacing a head-spend's
                        // stored BEEF with parent ancestry only).
                        if let (Some(ref beef_data), Some(ref proof)) =
                            (&consumed_output.beef, &proof)
                        {
                            if let Some(new_beef) =
                                Self::stitch_proof_into_stored_beef(beef_data, txid, proof)
                            {
                                let _ = self
                                    .storage
                                    .update_transaction_beef(&consumed_output.txid, &new_beef)
                                    .await;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Complete missing merkle proofs for stored, confirmed transactions.
    ///
    /// The `/submit`-admitted ingest path stores a proofless BEEF for each
    /// transaction (the submitter does not yet have a merkle proof at submit
    /// time). When such a transaction later mines, nothing in the overlay
    /// re-fetches its proof, so its stored BEEF stays proofless forever — and a
    /// frontend that trims `inputBEEF` to `{source tx + its BUMP}` for a P2PKH
    /// payment-import spend has no BUMP to trim to, hanging the import (#130).
    ///
    /// This pass closes that gap from chain (the one blessed server-side WoC
    /// use). It:
    /// 1. Pulls a bounded page (`limit`) of stored `(txid, beef)` from storage.
    /// 2. Parses each BEEF and keeps only those whose target tx still lacks a
    ///    merkle proof (`Beef::find_txid(txid).has_proof() == false`).
    /// 3. For each proofless candidate, fetches its BUMP via the configured
    ///    [`AncestorFetcher`](crate::gasp::AncestorFetcher). If a proof comes
    ///    back, it calls [`Self::handle_new_merkle_proof`], which stitches the
    ///    BUMP into the stored BEEF (D1 + R2 mirror), updates output block
    ///    height, and recurses the `consumedBy` chain.
    /// 4. Skips (does NOT error) txs the fetcher can't prove yet (still
    ///    unconfirmed) — those are simply retried on a later tick.
    ///
    /// **No-op when no [`AncestorFetcher`] is configured** — production-safe
    /// default (the same opt-in switch as GASP ancestor hydration). Bounded by
    /// `limit` (a per-tick budget, like the platform fetcher's own budget).
    ///
    /// `min_age_secs` is the PUSH-PRIMARY BACKSTOP gate (bsv-low #228 /
    /// arcade#259): since the Arcade MINED webhook (`/arc-ingest`) pushes a
    /// verified proof ~150 ms after a tx mines, this poll pass is a BACKSTOP,
    /// not the primary source — rows stored less than `min_age_secs` ago are
    /// skipped entirely (their push is still expected). Pass `0` to disable
    /// the gate (poll everything — the pre-#228 behaviour, and the degradation
    /// mode if pushes stop: an old-enough row is ALWAYS polled, so webhook
    /// loss degrades to polling, never to nothing). Unknown-age rows are
    /// always eligible (fail-safe — see the storage-trait doc).
    pub async fn complete_missing_proofs(
        &self,
        limit: u64,
        min_age_secs: u64,
    ) -> Result<ProofCompletionSummary, EngineError> {
        let mut summary = ProofCompletionSummary::default();

        // Production-safe: with no fetcher there is no chain source to complete
        // from, so this is a pure no-op (matches GASP-hydration opt-in).
        let Some(fetcher) = self.ancestor_fetcher.clone() else {
            return Ok(summary);
        };

        let candidates = self
            .storage
            .find_transactions_for_proof_check(limit, min_age_secs)
            .await
            .map_err(|e| EngineError::StorageError(e.to_string()))?;
        summary.scanned = candidates.len();

        for cand in candidates {
            // Parse the stored BEEF; skip anything unparseable (don't error the
            // whole pass on one bad row).
            let beef = match bsv_rs::transaction::Beef::from_binary(&cand.beef) {
                Ok(b) => b,
                Err(e) => {
                    warn!(txid = %cand.txid, error = %e, "[PROOF COMPLETION] unparseable BEEF, skipping");
                    continue;
                }
            };

            // Already structurally proven? The stored BEEF carries a merkle bump
            // for the target tx, but its backend `has_proof` flag may still be 0
            // — EITHER a legitimate GASP-synced proof written before overlay
            // migration 0010 (which defaulted every existing row to 0), OR an
            // admit-time bump that was NEVER SPV-verified (`/submit` skips SPV on
            // historical topics) or is outright forged. Serve-time BEEF trimming
            // trusts the `has_proof` flag, so latching one on STRUCTURE ALONE
            // would let a forged bump be trimmed on (#192/#193 MEDIUM). Re-verify
            // the STORED bump against chaintracks (via the fetcher — its header
            // source is the only arbiter of a merkle root). Genuine → latch
            // (fast, no re-fetch). Otherwise DON'T trust it: fall through to the
            // fetch+stitch path below, which overwrites the bump with a
            // chaintracks-verified one (or leaves the row proofless for retry).
            // Fail-closed throughout.
            if beef
                .find_txid(&cand.txid)
                .is_some_and(bsv_rs::transaction::BeefTx::has_proof)
            {
                // the tx's OWN bump (its `bump_index`), never `find_bump`
                // (the FIRST bump containing the txid — the stale one after a
                // same-height reorg; review HIGH-1).
                let stored_bump = beef
                    .find_txid(&cand.txid)
                    .and_then(bsv_rs::transaction::BeefTx::bump_index)
                    .and_then(|bi| beef.bumps.get(bi))
                    .map(bsv_rs::transaction::MerklePath::to_hex);
                if let Some(bump_hex) = stored_bump {
                    if fetcher.verify_proof(&cand.txid, &bump_hex).await {
                        // Idempotent + best-effort: a failure here is logged, not
                        // fatal — the row simply lingers one more tick.
                        // bsv-low M19 R2 round 3 (review MED-2): latch WITH the
                        // anchor height (from the tx's OWN verified bump) so the
                        // revalidation sweep's transactions leg can window this
                        // row — `mark_transaction_proven` alone left every
                        // fast-path row anchorless, in no window forever.
                        let height = bsv_rs::transaction::MerklePath::from_hex(&bump_hex)
                            .ok()
                            .map(|mp| u64::from(mp.block_height));
                        if let Err(e) = self
                            .storage
                            .mark_transaction_proven_at(&cand.txid, height)
                            .await
                        {
                            warn!(txid = %cand.txid, error = %e, "[PROOF COMPLETION] failed to mark verified-proven row");
                        } else {
                            summary.already_proven += 1;
                        }
                        continue;
                    }
                    warn!(txid = %cand.txid, "[PROOF COMPLETION] stored structural bump FAILED chaintracks re-verify — not trusting it, refetching");
                }
                // Unverifiable structural bump → treat as proofless (fall through).
            }
            summary.proofless += 1;

            // Fetch this tx's own VERIFIED bump from chain — PROOF ONLY (the raw
            // is already in the stored BEEF, so no redundant raw fetch; #192/#193
            // FIX 2). A tx with no verified proof yet (still unconfirmed, or an
            // unverifiable/forged courier response) is skipped, not errored.
            let Some(proof_hex) = fetcher.verified_proof_for(&cand.txid).await else {
                summary.still_unconfirmed += 1;
                continue;
            };

            // Pull the block height out of the BUMP so the output rows update too.
            let block_height = bsv_rs::transaction::MerklePath::from_hex(&proof_hex)
                .ok()
                .map(|mp| mp.block_height);

            match self
                .handle_new_merkle_proof(&cand.txid, &proof_hex, block_height)
                .await
            {
                Ok(()) => summary.completed += 1,
                Err(e) => {
                    warn!(txid = %cand.txid, error = %e, "[PROOF COMPLETION] stitch failed");
                    summary.stitch_failed += 1;
                }
            }
        }

        info!(
            scanned = summary.scanned,
            proofless = summary.proofless,
            completed = summary.completed,
            still_unconfirmed = summary.still_unconfirmed,
            fetch_failed = summary.fetch_failed,
            stitch_failed = summary.stitch_failed,
            already_proven = summary.already_proven,
            "[PROOF COMPLETION] pass complete"
        );

        Ok(summary)
    }

    /// Recursively update merkle paths in a transaction's input tree.
    ///
    /// If the transaction's id matches txid, set its merkle_path.
    /// Otherwise, recurse into sourceTransactions of each input.
    /// Stitch a verified BUMP for `txid` into a STORED BEEF, preserving every
    /// transaction and every other proof in it (#284).
    ///
    /// The old path round-tripped the stored bytes through
    /// `Transaction::from_beef(bytes, None)` -> `to_beef(true)`, and
    /// `from_beef(_, None)` picks **`txs.last()`** — it ignores the atomic
    /// subject pointer entirely. Whenever the subject was not the last tx in
    /// wire order, a PARENT was picked, `to_beef(true)` then serialized that
    /// parent's world, and the rewrite dropped the subject transaction from its
    /// own stored BEEF — observed live on every pf head-spend (list / reprice /
    /// delist / buy) the moment it mined, breaking the NEXT spend on that name
    /// (zanaadu#284). `update_input_proofs`' "already has a proof — update it"
    /// arm could even stamp the child's bump onto that mis-picked parent.
    /// This is the same destroy-the-stored-BEEF class the #4 fix note above
    /// (`hydrate_utxo_history`) already documents for lookup hydration.
    ///
    /// Operating on [`Beef`] directly has none of those failure modes:
    /// `merge_bump` dedupes against existing bumps by (height, root) and
    /// assigns the bump ONLY to transactions that appear as flagged txid
    /// leaves, so stitching tx X's proof can never mislabel tx Y, and the tx
    /// set is never rebuilt at all.
    ///
    /// Returns `None` when the bytes do not parse or `txid` is not in this
    /// BEEF (nothing to stitch — e.g. a consuming row whose ancestry got
    /// trimmed), so callers skip the write instead of writing garbage.
    ///
    /// bsv-low M19 R2 round 3 (review HIGH-1): a same-height DIFFERENT-root
    /// proof (the 2026-09-07 reorg shape) does NOT combine in `merge_bump`
    /// (the roots differ) — it is pushed as a SECOND bump, and
    /// `update_bump_indices` assigns it only to txs whose `bump_index` is
    /// `None`, so the reorged subject keeps its index on the STALE bump.
    /// Left alone, `has_proof`/`proofHeight` and every `find_txid(txid)
    /// .bump_index()` reader then keep pointing at the orphan bump, and the
    /// row refutes again next sweep pass forever. So after the merge, FORCE
    /// the subject onto the bump that actually proves it, then drop any bump
    /// no transaction references (the orphan's, now unreferenced).
    pub fn stitch_proof_into_stored_beef(
        stored: &[u8],
        txid: &str,
        proof: &bsv_rs::transaction::MerklePath,
    ) -> Option<Vec<u8>> {
        let mut beef = bsv_rs::transaction::Beef::from_binary(stored).ok()?;
        beef.find_txid(txid)?;
        let i = beef.merge_bump(proof.clone());
        // Does the merged bump actually prove `txid` (a flagged txid leaf)?
        // (It always does for a completion proof; the guard keeps a stray
        // caller from mis-anchoring a subject onto an ancestor's bump.)
        let proves_subject = beef.bumps.get(i).is_some_and(|b| {
            b.path.first().is_some_and(|leaves| {
                leaves
                    .iter()
                    .any(|l| l.txid && l.hash.as_deref() == Some(txid))
            })
        });
        if proves_subject {
            if let Some(tx) = beef.find_txid_mut(txid) {
                tx.set_bump_index(Some(i));
            }
        }
        Self::gc_unreferenced_bumps(&mut beef);
        Some(beef.to_binary())
    }

    /// Drop every BUMP no transaction references and re-index the survivors
    /// (bsv-low M19 R2 round 3, review HIGH-1): after the subject is moved
    /// off an orphan bump, that bump is usually dead weight the next reader
    /// would still `find_bump`. A bump still referenced by ANOTHER tx (a
    /// sibling mined in the same block) is kept.
    fn gc_unreferenced_bumps(beef: &mut bsv_rs::transaction::Beef) {
        let referenced: std::collections::HashSet<usize> = beef
            .txs
            .iter()
            .filter_map(bsv_rs::transaction::BeefTx::bump_index)
            .collect();
        if referenced.len() == beef.bumps.len() {
            return;
        }
        let mut remap: Vec<Option<usize>> = vec![None; beef.bumps.len()];
        let mut kept = Vec::with_capacity(referenced.len());
        for (old, bump) in std::mem::take(&mut beef.bumps).into_iter().enumerate() {
            if referenced.contains(&old) {
                remap[old] = Some(kept.len());
                kept.push(bump);
            }
        }
        beef.bumps = kept;
        for tx in &mut beef.txs {
            if let Some(old) = tx.bump_index() {
                tx.set_bump_index(remap[old]);
            }
        }
    }

    // ========================================================================
    // Advertisement sync
    // ========================================================================

    /// Sync SHIP/SLAP advertisements with configured managers and services.
    ///
    /// Creates missing advertisements and revokes stale ones. Returns a
    /// [`SyncAdvertisementsReport`] so a create/submit failure is VISIBLE to
    /// the caller (bsv-low #320 defect 3a): this method used to swallow both
    /// into `error!` and return `Ok(())`, so `/admin/syncAdvertisements`
    /// reported `success` while zero advertisements were admitted locally —
    /// a caller could not tell a converged no-op from a silent failure, and
    /// every cycle re-created the full ad set.
    pub async fn sync_advertisements(&self) -> Result<SyncAdvertisementsReport, EngineError> {
        let mut report = SyncAdvertisementsReport::default();

        let Some(advertiser) = &self.advertiser else {
            return Ok(report); // No advertiser configured
        };

        let hosting_url = match &self.config.hosting_url {
            Some(url) if !url.is_empty() => url.clone(),
            _ => return Ok(report), // No hosting URL
        };

        // Get configured topics and services
        let mut configured_topics: Vec<String> = self.managers.keys().cloned().collect();
        let mut configured_services: Vec<String> = self.lookup_services.keys().cloned().collect();

        if self.config.suppress_default_sync_advertisements {
            configured_topics.retain(|t| t != "tm_ship" && t != "tm_slap");
            configured_services.retain(|s| s != "ls_ship" && s != "ls_slap");
        }

        // Fetch current advertisements. A read failure REFUSES creation
        // (#320 M2): swallowing it into "current ads = none" made a
        // transient D1/storage fault re-create — and re-pay for — the
        // entire advertisement set, reported as a legitimate `to_create`.
        let current_ship = match advertiser.find_all_advertisements(Protocol::Ship).await {
            Ok(ads) => ads,
            Err(e) => {
                error!("Failed to read current SHIP advertisements — refusing to create: {e}");
                report.lookup_error = Some(format!("ship: {e}"));
                return Ok(report);
            }
        };
        let current_slap = match advertiser.find_all_advertisements(Protocol::Slap).await {
            Ok(ads) => ads,
            Err(e) => {
                error!("Failed to read current SLAP advertisements — refusing to create: {e}");
                report.lookup_error = Some(format!("slap: {e}"));
                return Ok(report);
            }
        };

        // Determine what to create
        let ships_to_create: Vec<AdvertisementData> = configured_topics
            .iter()
            .filter(|topic| {
                !current_ship
                    .iter()
                    .any(|a| a.topic_or_service == **topic && a.domain == hosting_url)
            })
            .map(|topic| AdvertisementData {
                protocol: Protocol::Ship,
                topic_or_service_name: topic.clone(),
            })
            .collect();

        let slaps_to_create: Vec<AdvertisementData> = configured_services
            .iter()
            .filter(|service| {
                !current_slap
                    .iter()
                    .any(|a| a.topic_or_service == **service && a.domain == hosting_url)
            })
            .map(|service| AdvertisementData {
                protocol: Protocol::Slap,
                topic_or_service_name: service.clone(),
            })
            .collect();

        // Determine what to revoke
        let ships_to_revoke: Vec<Advertisement> = current_ship
            .into_iter()
            .filter(|a| !configured_topics.contains(&a.topic_or_service))
            .collect();

        let slaps_to_revoke: Vec<Advertisement> = current_slap
            .into_iter()
            .filter(|a| !configured_services.contains(&a.topic_or_service))
            .collect();

        // Create new advertisements
        let mut all_to_create = ships_to_create;
        all_to_create.extend(slaps_to_create);
        report.to_create = all_to_create.len();
        if !all_to_create.is_empty() {
            match advertiser.create_advertisements(&all_to_create).await {
                Ok(tagged_beef) => match self.submit(&tagged_beef, SubmitMode::CurrentTx).await {
                    Ok(steak) => {
                        for (topic, instructions) in &steak {
                            report
                                .admitted
                                .insert(topic.clone(), instructions.outputs_to_admit.len());
                        }
                    }
                    Err(e) => {
                        error!("Failed to submit new advertisements: {e}");
                        report.submit_error = Some(e.to_string());
                    }
                },
                Err(e) => {
                    error!("Failed to create advertisements: {e}");
                    report.create_error = Some(e.to_string());
                }
            }
        }

        // Revoke stale advertisements
        let mut all_to_revoke = ships_to_revoke;
        all_to_revoke.extend(slaps_to_revoke);
        report.to_revoke = all_to_revoke.len();
        if !all_to_revoke.is_empty() {
            match advertiser.revoke_advertisements(&all_to_revoke).await {
                // An advertiser that declines to build a revocation (e.g. the
                // CF advertiser's documented v1 no-op) returns an EMPTY
                // TaggedBEEF — submitting it would only manufacture a parse
                // error. Skip, but say so in the report (#320 L2): the stale
                // ads stay on-chain until they age out.
                Ok(tagged_beef) if tagged_beef.topics.is_empty() => {
                    report.revoke_skipped = all_to_revoke.len();
                }
                Ok(tagged_beef) => {
                    if let Err(e) = self.submit(&tagged_beef, SubmitMode::CurrentTx).await {
                        error!("Failed to submit revocation: {e}");
                        report.revoke_submit_error = Some(e.to_string());
                    }
                }
                Err(e) => {
                    error!("Failed to revoke advertisements: {e}");
                    report.revoke_error = Some(e.to_string());
                }
            }
        }

        Ok(report)
    }

    // ========================================================================
    // Deep deletion
    // ========================================================================

    /// Recursively delete a UTXO and all stale consumed inputs.
    ///
    /// Only deletes if the output has no remaining consumers (consumedBy is empty).
    /// Then recurses into outputsConsumed, removing the deleted output from their
    /// consumedBy lists and deleting them if they become unreferenced.
    #[allow(dead_code)] // Used by submit() once BEEF parsing is wired up
    ///
    /// `bound` is a finalize submit's [`CallBound`] over each call here
    /// (bsv-low #559); `None` everywhere else.
    fn delete_utxo_deep<'a>(
        &'a self,
        output: &'a Output,
        bound: Option<&'a CallBound>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), EngineError>> + 'a>> {
        Box::pin(async move {
            // Only delete if nothing consumes this output
            if output.consumed_by.is_empty() {
                stored(
                    bound,
                    self.storage
                        .delete_output(&output.txid, output.output_index, &output.topic),
                )
                .await
                .map_err(|e| EngineError::StorageError(e.to_string()))?;

                // Notify lookup services. A hook error is dropped, not
                // reported (parity: the reference catches it, best effort).
                for ls in self.lookup_services.values() {
                    let _ = hooked(
                        bound,
                        ls.output_no_longer_retained_in_history(
                            &output.txid,
                            output.output_index,
                            &output.topic,
                        ),
                    )
                    .await;
                }
            }

            // Recurse into consumed outputs
            for consumed in &output.outputs_consumed {
                if let Ok(Some(mut stale_output)) = stored(
                    bound,
                    self.storage.find_output(
                        &consumed.txid,
                        consumed.output_index,
                        Some(&output.topic),
                        None,
                        false,
                    ),
                )
                .await
                {
                    // Remove the deleted output from consumedBy
                    stale_output.consumed_by.retain(|c| {
                        !(c.txid == output.txid && c.output_index == output.output_index)
                    });

                    stored(
                        bound,
                        self.storage.update_consumed_by(
                            &consumed.txid,
                            consumed.output_index,
                            &output.topic,
                            &stale_output.consumed_by,
                        ),
                    )
                    .await
                    .map_err(|e| EngineError::StorageError(e.to_string()))?;

                    // Recurse
                    let _ = self.delete_utxo_deep(&stale_output, bound).await;
                }
            }

            Ok(())
        }) // Box::pin
    }

    // ========================================================================
    // Eviction
    // ========================================================================

    /// Evict a specific outpoint from the overlay.
    ///
    /// 1. Deletes the output from storage for the given topic.
    /// 2. Notifies all lookup services via `output_evicted()`.
    ///
    /// If no topic is specified, finds all outputs for the txid and evicts the
    /// matching outputIndex across every topic.
    ///
    /// Matches TS OverlayExpress `/admin/evictOutpoint` (lines 974-1004).
    /// bsv-low PLAN-PRE-LOOP4 §H4 (2026-09-06): RE-NOTIFY every lookup service
    /// of the outputs the engine ALREADY admitted for (`txid`, `topic`).
    ///
    /// Phase-1 dedup means a (txid, topic) once recorded in
    /// `applied_transactions` never re-enters through `/submit`, so a lookup
    /// service that missed — or mis-parsed — its admit notification could
    /// never be told again (the loop-2 subject trap: the pot index's admit
    /// hook shape-checked a wire-last hop and wrote no record while the engine
    /// had admitted the pot). This replays `output_admitted_by_topic` from the
    /// engine's OWN stored outputs (with their BEEF) and the subject-NAMED body
    /// exactly as `submit_with_report` sends it. It validates nothing anew
    /// (the topic manager judged these outputs at admit), writes none of the
    /// engine's tables, and is idempotent for every lookup service whose
    /// writes are (the pot index's `store_record` is insert-if-absent for the
    /// spend fields — re-admission never regresses spend state).
    /// `outputs == 0` means the engine never admitted this txid on this topic:
    /// nothing to re-notify — the caller re-SUBMITS instead.
    pub async fn renotify_admitted(
        &self,
        txid: &str,
        topic: &str,
    ) -> Result<RenotifyReport, EngineError> {
        let txid = txid.to_ascii_lowercase();
        let outputs = self
            .storage
            .find_outputs_for_transaction(&txid, true)
            .await
            .map_err(|e| EngineError::StorageError(e.to_string()))?;
        let mut report = RenotifyReport::default();
        for output in outputs.iter().filter(|o| o.topic == topic) {
            report.outputs += 1;
            report.vouts.push(output.output_index);
            // the body every WholeTx service receives NAMES the subject (BRC-95
            // atomic prefix) — the same rule as the live submit path
            let subject_named: Option<Vec<u8>> = output.beef.as_ref().map(|beef| {
                bsv_rs::transaction::Beef::from_binary(beef)
                    .ok()
                    .and_then(|mut b| b.to_binary_atomic(&txid).ok())
                    .unwrap_or_else(|| beef.clone())
            });
            for (name, ls) in &self.lookup_services {
                let payload = match ls.admission_mode() {
                    AdmissionMode::LockingScript => OutputAdmittedByTopic::LockingScript {
                        txid: txid.clone(),
                        output_index: output.output_index,
                        topic: topic.to_string(),
                        satoshis: output.satoshis,
                        locking_script: output.output_script.clone(),
                        off_chain_values: None,
                    },
                    AdmissionMode::WholeTx => {
                        let Some(atomic_beef) = subject_named.as_ref() else {
                            report.faults.push(format!(
                                "{name}: {txid}:{} is stored without its BEEF — a WholeTx service cannot be re-notified",
                                output.output_index
                            ));
                            continue;
                        };
                        OutputAdmittedByTopic::WholeTx {
                            atomic_beef: atomic_beef.clone(),
                            output_index: output.output_index,
                            topic: topic.to_string(),
                            off_chain_values: None,
                        }
                    }
                };
                match ls.output_admitted_by_topic(&payload).await {
                    Ok(()) => report.notified += 1,
                    Err(e) => report.faults.push(format!("{name}: {e}")),
                }
            }
        }
        Ok(report)
    }

    /// bsv-low PLAN-PRE-LOOP4 §H4 (2026-09-06): FORGET a PHANTOM applied row —
    /// (txid, topic) recorded in `applied_transactions` while the topic holds
    /// NO output of that txid (the engine judged the tx and admitted nothing:
    /// the loop-2 JOIN `f9e85aab…` under the pre-D5 subject rule). Such a row
    /// does nothing but block a correct re-submit through the Phase-1 dedup
    /// (`/admin/readmit` on beta answered `deduped: ["tm_pot_beta"]` and the
    /// pot stayed `known:false`). A row with ANY stored output on the topic is
    /// a real admission and is never touched (returns false, as does a missing
    /// row). The forgotten bytes are then re-VALIDATED by the topic manager
    /// like any first submit — nothing is admitted here.
    pub async fn forget_phantom_applied(
        &self,
        txid: &str,
        topic: &str,
    ) -> Result<bool, EngineError> {
        let txid = txid.to_ascii_lowercase();
        let outputs = self
            .storage
            .find_outputs_for_transaction(&txid, false)
            .await
            .map_err(|e| EngineError::StorageError(e.to_string()))?;
        if outputs.iter().any(|o| o.topic == topic) {
            return Ok(false);
        }
        let rec = AppliedTransaction {
            txid: txid.clone(),
            topic: topic.to_string(),
        };
        let exists = self
            .storage
            .does_applied_transaction_exist(&rec)
            .await
            .map_err(|e| EngineError::StorageError(e.to_string()))?;
        if !exists {
            return Ok(false);
        }
        self.storage
            .delete_applied_transaction(&rec)
            .await
            .map_err(|e| EngineError::StorageError(e.to_string()))?;
        Ok(true)
    }

    pub async fn evict_output(
        &self,
        txid: &str,
        output_index: u32,
        topic: Option<&str>,
    ) -> Result<(), EngineError> {
        if let Some(topic) = topic {
            // Delete from storage
            self.storage
                .delete_output(txid, output_index, topic)
                .await
                .map_err(|e| EngineError::StorageError(e.to_string()))?;

            // Notify all lookup services
            for ls in self.lookup_services.values() {
                let _ = ls.output_evicted(txid, output_index).await;
            }
        } else {
            // No topic specified — find all outputs for this txid and evict matching ones
            let outputs = self
                .storage
                .find_outputs_for_transaction(txid, false)
                .await
                .map_err(|e| EngineError::StorageError(e.to_string()))?;

            for output in &outputs {
                if output.output_index == output_index {
                    self.storage
                        .delete_output(txid, output_index, &output.topic)
                        .await
                        .map_err(|e| EngineError::StorageError(e.to_string()))?;
                }
            }

            // Notify all lookup services once
            for ls in self.lookup_services.values() {
                let _ = ls.output_evicted(txid, output_index).await;
            }
        }

        Ok(())
    }

    // ========================================================================
    // GASP sync
    // ========================================================================

    /// Start GASP synchronization with peers for all configured topics.
    ///
    /// For each topic in `sync_configuration`:
    /// - `SyncTarget::Ship` — discovers peers via local `ls_ship` lookup, parses
    ///   SHIP advertisement scripts to extract domain URLs.
    /// - `SyncTarget::Peers(urls)` — uses the provided peer URLs directly.
    /// - `SyncTarget::Disabled` — skips the topic.
    ///
    /// Filters out our own `hosting_url` to avoid self-sync.
    ///
    /// Returns a `GASPSyncResult` summarizing discovered peers per topic.
    ///
    /// Discovers peers for each configured topic and runs GASP sync with each.
    ///
    /// When a `GASPRemoteFactory` is set (via `set_gasp_remote_factory`), this
    /// method creates `OverlayGASPStorage` + `GASPRemote` instances per
    /// (topic, peer) pair and runs `GASPSync::sync()` to exchange UTXOs.
    ///
    /// Without a factory, peer discovery still runs but no actual sync occurs
    /// (backwards-compatible with the previous discovery-only behavior).
    pub async fn start_gasp_sync(&self) -> Result<GASPSyncResult, EngineError> {
        use crate::gasp::{GASPSync, DEFAULT_GASP_SYNC_LIMIT};
        use crate::gasp_overlay::OverlayGASPStorage;

        if self.config.sync_configuration.is_empty() {
            info!("[GASP SYNC] No sync configuration — nothing to sync");
            return Ok(GASPSyncResult {
                topics_synced: HashMap::new(),
            });
        }

        let mut topics_synced: HashMap<String, TopicSyncResult> = HashMap::new();

        for (topic, target) in &self.config.sync_configuration {
            let (peers, sync_type) = match target {
                SyncTarget::Disabled => {
                    info!("[GASP SYNC] Topic {topic} is disabled — skipping");
                    continue;
                }
                SyncTarget::Ship => {
                    info!("[GASP SYNC] Topic {topic} configured for SHIP discovery");
                    let peers = self.discover_ship_peers(topic).await;
                    info!(
                        "[GASP SYNC] Discovered {} peer(s) for topic {topic}",
                        peers.len()
                    );
                    (peers, "ship".to_string())
                }
                SyncTarget::Peers(peer_urls) => {
                    info!(
                        "[GASP SYNC] Topic {topic} configured with {} hardcoded peer(s)",
                        peer_urls.len()
                    );
                    let peers: Vec<String> = peer_urls
                        .iter()
                        .filter(|url| {
                            if let Some(ref our_url) = self.config.hosting_url {
                                url.trim_end_matches('/') != our_url.trim_end_matches('/')
                            } else {
                                true
                            }
                        })
                        .cloned()
                        .collect();

                    info!(
                        "[GASP SYNC] {} peer(s) for topic {topic} after self-filtering",
                        peers.len()
                    );
                    (peers, "peers".to_string())
                }
            };

            let mut errors = Vec::new();
            let mut pruned_inputs: u64 = 0;
            let mut discarded_graphs: u64 = 0;
            let mut finalized_graphs: u64 = 0;
            let mut deadline_dropped_graphs: u64 = 0;
            let mut cursor_moves: Vec<CursorMove> = Vec::new();
            let mut deferred_graphs: u64 = 0;
            let mut resumed_graphs: u64 = 0;
            let mut converged_graphs: u64 = 0;
            let mut dropped_graphs: Vec<DroppedDeferral> = Vec::new();
            let mut stalled_graphs: u64 = 0;
            let mut held_back_graphs: u64 = 0;

            // If we have a remote factory, actually run GASP sync
            if let Some(ref factory) = self.gasp_remote_factory {
                for peer_url in &peers {
                    // bsv-low#302 quarantine gate: a peer at
                    // PEER_QUARANTINE_THRESHOLD consecutive failures is
                    // SKIPPED (no attempt recorded — its last-attempt age
                    // keeps growing) until the re-probe window opens. A
                    // health-read FAULT treats the peer as healthy: broken
                    // bookkeeping must never silence a live peer.
                    let health = self
                        .storage
                        .get_peer_sync_health(peer_url, topic)
                        .await
                        .unwrap_or_default();
                    if crate::gasp::peer_sync_quarantined(&health) {
                        warn!(
                            "[GASP SYNC] Peer {peer_url} for {topic} QUARANTINED ({} consecutive failed syncs, last attempt {:?}s ago) — skipped until the {}s re-probe window (bsv-low#302)",
                            health.consecutive_failures,
                            health.secs_since_last_attempt,
                            crate::gasp::PEER_QUARANTINE_REPROBE_SECS
                        );
                        continue;
                    }

                    info!("[GASP SYNC] Syncing topic {topic} with peer {peer_url}");

                    // Get last interaction score for this (peer, topic) pair
                    let last_interaction = self
                        .storage
                        .get_last_interaction(peer_url, topic)
                        .await
                        .unwrap_or(0);

                    // Create shared sink for finalized graphs
                    let sink = crate::gasp_overlay::new_finalized_graph_sink();

                    // OPT-IN ancestor hydration: only when a fetcher is
                    // configured do we enable chain-backed ancestor fallback +
                    // strict-BEEF finalize. Default (None) = byte-identical to
                    // today (tolerant BEEF, peer errors abandon the graph;
                    // the one exception is the D8 prune of a manager-named
                    // input the peer answers it does not hold, which exists
                    // only WITHOUT a fetcher, see
                    // `GASPSync::process_incoming_node`).
                    let hydration_on = self.ancestor_fetcher.is_some();

                    // Create storage adapter and remote
                    let mut gasp_storage =
                        OverlayGASPStorage::new(self.storage.as_ref(), topic, sink.clone())
                            .with_peer(peer_url.as_str())
                            .with_strict_beef(hydration_on)
                            .with_script_verification(self.verify_scripts);
                    if let Some(manager) = self.managers.get(topic) {
                        gasp_storage = gasp_storage.with_topic_manager(manager.as_ref());
                    }
                    // bsv-low #551: the anchor check verifies each graph's
                    // root against THIS engine's tracker, with this engine's
                    // script switch, before finalize. No tracker is the
                    // reference's 'scripts only'.
                    if let Some(tracker) = self.chain_tracker.as_deref() {
                        gasp_storage = gasp_storage.with_chain_tracker(tracker);
                    }
                    let gasp_remote = factory.create_remote(peer_url, topic);

                    let log_prefix = format!("[GASP {topic} <-> {peer_url}]");
                    // Graphs of this peer handed to submit (bsv-low #552).
                    let peer_finalized = std::cell::Cell::new(0u64);
                    let mut sync = GASPSync::new(
                        Box::new(gasp_storage),
                        gasp_remote,
                        last_interaction,
                        &log_prefix,
                        true, // unidirectional — overlay GASP is pull-only (submitNode throws); matches TS Engine.startGASPSync
                    )
                    .with_ancestor_fetcher(self.ancestor_fetcher.clone());
                    // bsv-low #555: a graph past its own budget is deferred,
                    // not dropped with the sync.
                    if let Some((sleep, max_calls, budget_ms)) = &self.graph_budget {
                        let (sleep, budget_ms) = (sleep.clone(), *budget_ms);
                        sync = sync.with_graph_budget(crate::gasp::GraphBudget {
                            max_calls: *max_calls,
                            deadline: Box::new(move || sleep(budget_ms)),
                        });
                    }
                    // bsv-low #552: under a budget each graph is submitted as
                    // it finalizes, so the deadline cannot take it back. With
                    // no budget nothing can drop the sync, and the graphs are
                    // submitted after it as they always were.
                    if self.peer_sync_budget.is_some() {
                        sync = sync.with_finalized_graph_hook(Box::new(SubmitAsFinalized {
                            engine: self,
                            sink: sink.clone(),
                            peer_url,
                            topic,
                            submitted: &peer_finalized,
                        }));
                    }

                    // bsv-low#302: bound THIS peer's sync to the configured
                    // budget. `None` from the race = the budget won and the
                    // sync future was dropped mid-flight. What it finalized
                    // before that is already submitted (the hook above); the
                    // cursor then advances to `completed_cursor` only, see
                    // the deadline arm below. The race is GUARDED: it never
                    // drops the sync inside one transaction's finalize
                    // submit (the lens fold's HIGH-1), it waits for that
                    // transaction and the hook stops at the boundary.
                    let sync_outcome = match &self.peer_sync_budget {
                        Some((sleep, budget_ms)) => {
                            crate::gasp::race_or_deadline_guarded(
                                sync.sync(Some(DEFAULT_GASP_SYNC_LIMIT)),
                                sleep(*budget_ms),
                                &self.finalize_gate,
                            )
                            .await
                        }
                        None => Some(sync.sync(Some(DEFAULT_GASP_SYNC_LIMIT)).await),
                    };

                    let mut outcome_success = matches!(sync_outcome, Some(Ok(())));
                    // D8 decoy rule: branches pruned in this peer's walk. Not
                    // an error and not a failed UTXO, so it is counted apart
                    // from `errors` and never touches the outcome or cursor.
                    // Summed over peers: one decoy seen through two peers
                    // counts twice.
                    pruned_inputs += sync.pruned_inputs();
                    // bsv-low #551: graphs the anchor check refused. Counted
                    // apart from `errors` for the same reason as a prune: a
                    // refused graph is not a failed sync (the reference
                    // discards it and carries on).
                    discarded_graphs += sync.discarded_graphs();
                    match sync_outcome {
                        None => {
                            let budget_ms = self.peer_sync_budget.as_ref().map_or(0, |(_, ms)| *ms);
                            let msg = format!(
                                "{peer_url}: per-peer sync budget of {budget_ms} ms EXCEEDED — dropped (bsv-low#302)"
                            );
                            warn!("[GASP SYNC] {msg}");
                            errors.push(msg);

                            // bsv-low #552: the work COMPLETED before the
                            // deadline is kept. Its graphs are already
                            // submitted (the hook); the cursor moves past the
                            // UTXOs that were completed and stops below the
                            // one in flight, whose graph is lost whole and
                            // walked again next tick. A graph the deadline
                            // met while it was being SUBMITTED is not "in
                            // flight" here: it left a prefix of whole
                            // transactions, is counted finalized, and its
                            // UTXO is below the cursor too.
                            let in_flight = u64::from(sync.graph_in_flight());
                            deadline_dropped_graphs += in_flight;
                            // bsv-low #555: the walk in hand is saved, so the
                            // next tick resumes it instead of walking it again.
                            sync.defer_in_flight().await;
                            let completed = sync.completed_cursor();
                            if completed > last_interaction {
                                match self
                                    .storage
                                    .update_last_interaction(peer_url, topic, completed)
                                    .await
                                {
                                    Ok(()) => cursor_moves.push(CursorMove {
                                        peer: peer_url.clone(),
                                        from: last_interaction,
                                        to: completed,
                                    }),
                                    Err(e) => warn!(
                                        "[GASP SYNC] Failed to update last_interaction for {peer_url}/{topic}: {e}"
                                    ),
                                }
                            }
                            // A tick that finalized a graph or moved the
                            // cursor reached a live peer: not a failed
                            // attempt for the quarantine count. So did one
                            // whose walk PROGRESSED (bsv-low #555: it
                            // appended a node, or completed its graph). A
                            // deferral that fetched nothing is no progress
                            // (the lens fold's H1: a hung peer deferred with
                            // 0 nodes on every tick and was never
                            // quarantined).
                            outcome_success = peer_finalized.get() > 0
                                || completed > last_interaction
                                || sync.deferral_stats().progressed > 0;
                            warn!(
                                "[GASP SYNC] {peer_url} for {topic} at the deadline: finalized_graphs={} deadline_dropped_graphs={in_flight} cursor {last_interaction} -> {} (bsv-low #552)",
                                peer_finalized.get(),
                                completed.max(last_interaction)
                            );
                        }
                        Some(Ok(())) => {
                            // Submit finalized graphs to the Engine (under a
                            // budget the hook already did, graph by graph,
                            // and the sink is empty here).
                            let landed = self
                                .submit_finalized_graphs(&sink, peer_url, topic, &peer_finalized)
                                .await;
                            // bsv-low #555, the lens fold's H1: a sync that
                            // ran to its end with walks the per-graph budget
                            // cut having FETCHED NOTHING, and nothing else
                            // got done (no walk progressed, no graph
                            // finalized, the cursor did not move), reached
                            // a peer that does not answer: a FAILED attempt.
                            // Before #555 such a peer held the sync to the
                            // per-peer deadline, a failure; the per-graph
                            // budget had made it an `Ok` sync. A sync whose
                            // UTXOs failed in any other way is unchanged.
                            let stats = sync.deferral_stats();
                            if stats.stalled > 0
                                && stats.progressed == 0
                                && peer_finalized.get() == 0
                                && !(landed && sync.last_interaction > last_interaction)
                            {
                                outcome_success = false;
                                let msg = format!(
                                    "{peer_url}: {} graph walk(s) cut by the per-graph budget with nothing fetched (bsv-low #555)",
                                    stats.stalled
                                );
                                warn!("[GASP SYNC] {msg}");
                                errors.push(msg);
                            }

                            // Advance the persisted cursor when it moved forward — i.e.
                            // the peer reported UTXOs at a higher score than our last
                            // `since` (the in-memory cursor only grows on SEEN scores,
                            // so `>` already excludes the empty-bootstrap case where no
                            // UTXOs were seen). Mirrors TS `if (gasp.lastInteraction >
                            // lastInteraction)` — NOT gated on a new submission: a sync
                            // that re-sees already-known UTXOs must still advance, else
                            // every cron re-scans the same range forever (the cursor
                            // stayed pinned at 0 in prod).
                            //
                            // NOT advanced when a finalize submit above did
                            // not land (the lens fold's MEDIUM-1): the sink
                            // does not say which UTXO that graph was, so the
                            // whole cursor stays and the next sync is served
                            // the range again (what did land is known and
                            // skipped). Under a budget the hook fails that
                            // one UTXO instead and the gap guard has already
                            // capped `sync.last_interaction` below it.
                            if !landed {
                                warn!(
                                    "[GASP SYNC] a finalize submit for {topic} from {peer_url} did not land: the cursor stays at {last_interaction}"
                                );
                            } else if sync.last_interaction > last_interaction {
                                match self
                                    .storage
                                    .update_last_interaction(peer_url, topic, sync.last_interaction)
                                    .await
                                {
                                    Ok(()) => cursor_moves.push(CursorMove {
                                        peer: peer_url.clone(),
                                        from: last_interaction,
                                        to: sync.last_interaction,
                                    }),
                                    Err(e) => warn!(
                                        "[GASP SYNC] Failed to update last_interaction for {peer_url}/{topic}: {e}"
                                    ),
                                }
                            }
                            info!(
                                "[GASP SYNC] Sync with {peer_url} for {topic} completed (last_interaction={})",
                                sync.last_interaction
                            );
                        }
                        Some(Err(e)) => {
                            let msg = format!("{peer_url}: {e}");
                            warn!("[GASP SYNC] Sync failed: {msg}");
                            errors.push(msg);
                        }
                    }
                    finalized_graphs += peer_finalized.get();
                    let deferral = sync.deferral_stats();
                    deferred_graphs += deferral.deferred;
                    resumed_graphs += deferral.resumed;
                    converged_graphs += deferral.converged;
                    stalled_graphs += deferral.stalled;
                    held_back_graphs += deferral.held_back;
                    dropped_graphs.extend(deferral.dropped.into_iter().map(|d| DroppedDeferral {
                        peer: peer_url.clone(),
                        outpoint: d.outpoint,
                        reason: d.reason.as_str().to_string(),
                    }));

                    // bsv-low#302: record the attempt outcome (success resets
                    // the consecutive-failure count; timeout/error increments
                    // it). Best-effort — a bookkeeping fault must never fail
                    // the sync pass itself.
                    if let Err(e) = self
                        .storage
                        .record_peer_sync_outcome(peer_url, topic, outcome_success)
                        .await
                    {
                        warn!(
                            "[GASP SYNC] failed to record peer sync outcome for {peer_url}/{topic}: {e}"
                        );
                    }
                }
            }

            topics_synced.insert(
                topic.clone(),
                TopicSyncResult {
                    peers,
                    sync_type,
                    errors,
                    pruned_inputs,
                    discarded_graphs,
                    finalized_graphs,
                    deadline_dropped_graphs,
                    cursor_moves,
                    deferred_graphs,
                    resumed_graphs,
                    converged_graphs,
                    dropped_graphs,
                    stalled_graphs,
                    held_back_graphs,
                },
            );
        }

        info!(
            "[GASP SYNC] Peer discovery complete for {} topic(s)",
            topics_synced.len()
        );

        Ok(GASPSyncResult { topics_synced })
    }

    /// Drain `sink` and submit every finalized graph in it, ancestors first
    /// (`historical-tx-no-spv`: the anchor check already verified them).
    /// Each graph is added to `submitted` as its first submit starts. Returns
    /// whether EVERY transaction of every graph landed.
    ///
    /// Each graph here passed the anchor check: its root verified and the
    /// replay admitted it, so these submits rest on a checked premise and are
    /// not expected to fail. If one does not LAND, the REST OF THAT GRAPH is
    /// not submitted: the reference's `finalizeGraph` awaits each submit in
    /// order and its throw ends the loop (the BEEFs already submitted stay,
    /// it has no rollback either). A later BEEF would otherwise be judged
    /// without the coin its ancestor failed to leave, admit nothing and be
    /// recorded as applied, a dupe for good. "Landed" is read from the
    /// durability report, not from `Ok`: `submit` answers `Ok` when a storage
    /// write faulted and when the topic manager failed, and in both it
    /// records no applied row. A transaction landed when its topic is applied
    /// or was already applied (a dupe). Other graphs carry on, and the caller
    /// keeps the cursor from passing a graph that did not land.
    ///
    /// Each transaction's submit is one write section of the engine's gate
    /// ([`Engine::finalize_submit_gate`]) and the gate is asked between two:
    /// a deadline that is due stops the sequence there (this future is
    /// dropped at that await, between two whole transactions).
    async fn submit_finalized_graphs(
        &self,
        sink: &crate::gasp_overlay::FinalizedGraphSink,
        peer_url: &str,
        topic: &str,
        submitted: &std::cell::Cell<u64>,
    ) -> bool {
        let finalized: Vec<_> = sink
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .drain(..)
            .collect();

        let mut all_landed = true;
        for graph in &finalized {
            for (position, beef_bytes) in graph.beefs.iter().enumerate() {
                self.finalize_gate.stop_if_due().await;
                if position == 0 {
                    submitted.set(submitted.get() + 1);
                }
                let tagged = TaggedBEEF::new(beef_bytes.clone(), vec![graph.topic.clone()]);
                // No deadline drops a write section, and the section does
                // not drop ITSELF either (bsv-low #559, the delta lens's
                // DELTA-3 and the lens fold of 2026-10-07, F2,
                // `set_finalize_submit_budget`): the submit always runs to
                // its end. What is bounded is each storage call and hook
                // inside it ([`CallBound`]): one that never answers is that
                // call's FAULT, the submit handles it as any fault (nothing
                // of the transaction held, or all of it), the transaction
                // did not land, its UTXO fails and the cursor stays.
                let outcome = {
                    let _writing = self.finalize_gate.write_section();
                    let bound = self
                        .finalize_submit_budget
                        .as_ref()
                        .map(|(sleep, budget_ms)| CallBound::new(sleep, *budget_ms));
                    self.submit_bounded(
                        &tagged,
                        SubmitMode::HistoricalTxNoSpv,
                        bound.as_ref(),
                        true,
                    )
                    .await
                };
                let not_landed = match outcome {
                    Ok((_, report)) => {
                        let landed = report.is_durable()
                            && (report.applied_topics.contains(&graph.topic)
                                || report.deduped_topics.contains(&graph.topic));
                        (!landed).then(|| {
                            if report.is_durable() {
                                "the topic manager failed on it".to_string()
                            } else {
                                format!("not durable: {}", report.summary())
                            }
                        })
                    }
                    Err(e) => Some(e.to_string()),
                };
                if let Some(why) = not_landed {
                    warn!(
                        "[GASP SYNC] A finalize submit for topic {} did not land ({why}); the rest of its graph is not submitted",
                        graph.topic
                    );
                    all_landed = false;
                    break;
                }
            }
        }

        if !finalized.is_empty() {
            info!(
                "[GASP SYNC] Submitted {} finalized graph(s) for {topic} from {peer_url}",
                finalized.len()
            );
        }
        all_landed
    }

    /// Discover peer overlay nodes for a topic via SHIP lookup.
    ///
    /// Queries the local `ls_ship` lookup service for SHIP advertisement records
    /// matching the given topic, then parses each record's PushDrop locking script
    /// to extract the advertised domain URL. Filters out our own hosting URL.
    async fn discover_ship_peers(&self, topic: &str) -> Vec<String> {
        let Some(ship_ls) = self.lookup_services.get("ls_ship") else {
            warn!("[GASP SYNC] No ls_ship lookup service registered — cannot discover peers");
            return Vec::new();
        };

        let question = LookupQuestion::new("ls_ship", serde_json::json!({ "topics": [topic] }));

        let refs = match ship_ls.lookup(&question).await {
            Ok(LookupResult::OutputList(refs)) => refs,
            Ok(LookupResult::Answer(_)) => {
                error!(
                    "[GASP SYNC] SHIP lookup for topic {topic} returned a pre-formed \
                     LookupAnswer; expected OutputList. Skipping."
                );
                return Vec::new();
            }
            Err(e) => {
                error!("[GASP SYNC] SHIP lookup for topic {topic} failed: {e}");
                return Vec::new();
            }
        };

        let mut domains = HashSet::new();
        for reference in &refs {
            if let Ok(Some(output)) = self
                .storage
                .find_output(&reference.txid, reference.output_index, None, None, false)
                .await
            {
                if let Some(domain) = parse_ship_domain_from_script(&output.output_script) {
                    if let Some(ref our_url) = self.config.hosting_url {
                        if domain.trim_end_matches('/') == our_url.trim_end_matches('/') {
                            continue;
                        }
                    }
                    domains.insert(domain);
                }
            }
        }

        let mut peers: Vec<String> = domains.into_iter().collect();
        peers.sort();
        peers
    }

    // ========================================================================
    // Helpers
    // ========================================================================

    /// Get a reference to the storage backend.
    pub fn storage(&self) -> &dyn Storage {
        self.storage.as_ref()
    }

    /// Get the engine configuration.
    pub fn config(&self) -> &EngineConfig {
        &self.config
    }
}

/// The SPV check of [`Engine::submit`], as one function over a BORROWED
/// tracker: the reference's `tx.verify(chainTracker)` (roots against the
/// tracker, every unproven input's script executed, the value rule), its
/// `tx.verify('scripts only')` when `chain_tracker` is `None` (roots accepted
/// unchecked, scripts still run), and the pre-2026-09-08 structural check when
/// `verify_scripts` is `false` ([`Engine::set_script_verification`]'s escape
/// hatch).
///
/// There is ONE verifier. `Engine::submit` calls this with its own tracker
/// and switch; the GASP anchor check
/// (`OverlayGASPStorage::validate_graph_anchor`, bsv-low #551) calls it with
/// the same two, handed over by `Engine::start_gasp_sync`.
pub(crate) async fn verify_spv_like_the_reference(
    chain_tracker: Option<&dyn bsv_rs::transaction::ChainTracker>,
    verify_scripts: bool,
    beef_bytes: &[u8],
    subject_txid: &str,
) -> Result<(), EngineError> {
    verify_spv_trusting(
        chain_tracker,
        verify_scripts,
        beef_bytes,
        subject_txid,
        &HashSet::new(),
    )
    .await
}

/// [`verify_spv_like_the_reference`] trusting the transactions an earlier
/// walk of the same submit passed over (a predecessor the door lands first,
/// the E1D delta fold, L4): the script walk neither checks nor descends
/// them. The structural escape hatch checks the whole BEEF it is given.
async fn verify_spv_trusting(
    chain_tracker: Option<&dyn bsv_rs::transaction::ChainTracker>,
    verify_scripts: bool,
    beef_bytes: &[u8],
    subject_txid: &str,
    trusted: &HashSet<String>,
) -> Result<(), EngineError> {
    if verify_scripts {
        Engine::verify_beef_linear(chain_tracker, beef_bytes, subject_txid, trusted).await
    } else {
        Engine::verify_spv_structurally(chain_tracker, beef_bytes).await
    }
}

/// The transactions the SPV walk of `from` passes over in `beef`
/// ([`Engine::verify_beef_linear_with`]'s traversal, nothing checked): `from`
/// and every source the BEEF carries, a proven transaction included and not
/// descended. The E1D delta fold, L4: what a landing need not walk again.
fn walk_cover(beef: &Beef, from: &str) -> HashSet<String> {
    let bodies: HashMap<String, &Transaction> = beef
        .txs
        .iter()
        .filter_map(|btx| btx.tx().map(|tx| (btx.txid(), tx)))
        .collect();
    let mut cover = HashSet::new();
    let mut queue = vec![from.to_string()];
    while let Some(txid) = queue.pop() {
        if !cover.insert(txid.clone()) || beef.find_bump(&txid).is_some() {
            continue;
        }
        if let Some(tx) = bodies.get(&txid) {
            queue.extend(tx.inputs.iter().filter_map(|i| i.source_txid.clone()));
        }
    }
    cover
}

/// The engine's [`crate::gasp::FinalizedGraphHook`] (bsv-low #552): submit
/// what a peer's sync just finalized, before its next UTXO is asked for.
struct SubmitAsFinalized<'e> {
    engine: &'e Engine,
    sink: crate::gasp_overlay::FinalizedGraphSink,
    peer_url: &'e str,
    topic: &'e str,
    submitted: &'e std::cell::Cell<u64>,
}

#[async_trait::async_trait(?Send)]
impl crate::gasp::FinalizedGraphHook for SubmitAsFinalized<'_> {
    async fn graph_completed(&self) -> Result<(), crate::gasp::GASPError> {
        // A graph is counted as its first submit starts, and the deadline
        // cannot land inside a submit: a sync dropped in here has written at
        // least that transaction whole (an ancestors-first prefix).
        let landed = self
            .engine
            .submit_finalized_graphs(&self.sink, self.peer_url, self.topic, self.submitted)
            .await;
        if landed {
            Ok(())
        } else {
            // Fails this UTXO: the cursor stays below it and the next sync
            // asks for it again (the lens fold's MEDIUM-1).
            Err(crate::gasp::GASPError::StorageError(
                "a finalize submit did not land".to_string(),
            ))
        }
    }
}

/// Result of a GASP sync operation.
///
/// Returned by `Engine::start_gasp_sync()` to summarize what happened.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GASPSyncResult {
    /// Per-topic sync results.
    pub topics_synced: HashMap<String, TopicSyncResult>,
}

/// Sync result for a single topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicSyncResult {
    /// Peer URLs discovered or configured for this topic.
    pub peers: Vec<String>,
    /// How peers were determined: "ship" or "peers".
    pub sync_type: String,
    /// Any errors encountered during sync (peer URL -> error message): a
    /// peer whose sync failed or ran over its budget. A single UTXO whose
    /// graph ingest failed is NOT listed here: it is warned (`Error ingesting
    /// UTXO`), held back by the cursor gap guard and asked for again by the
    /// next sync.
    pub errors: Vec<String>,
    /// Manager-named inputs pruned because a peer answered that it does not
    /// hold them (the D8 decoy rule, `GASPSync::pruned_inputs`). Counted per
    /// distinct outpoint PER PEER per sync and summed over this topic's
    /// peers: one decoy seen through two peers reads 2. Not errors: the
    /// graphs completed without those branches.
    #[serde(default)]
    pub pruned_inputs: u64,
    /// Graphs the anchor check REFUSED and discarded whole (bsv-low #551,
    /// `GASPSync::discarded_graphs`): a root whose BEEF does not verify, or
    /// one the topic manager does not admit at the end of the replay. Summed
    /// over this topic's peers. Not errors, and nothing of them was admitted.
    #[serde(default)]
    pub discarded_graphs: u64,
    /// Graphs that passed the anchor check and were handed to submit, summed
    /// over this topic's peers (bsv-low #552). Under a per-peer budget a
    /// graph is counted, and submitted, the moment it finalizes, so the
    /// count includes those of a peer the deadline then dropped. A counted
    /// graph had its first transaction submitted whole (the deadline never
    /// lands inside a submit); one whose later submit did not land is still
    /// counted, and its UTXO stays below the cursor.
    /// A graph is counted as its first submit STARTS; a first submit that FAULTS
    /// still counts here, so this is not a delivered count (bsv-low #559).
    #[serde(default)]
    pub finalized_graphs: u64,
    /// Graphs that were mid-walk when a peer's sync was dropped at its
    /// budget (bsv-low #552): at most one per peer per sync. NOTHING of such
    /// a graph was admitted; the next sync walks it again. The same root
    /// counted tick after tick with no `finalized_graphs` and no
    /// `cursor_moves` is ONE graph whose walk outlasts the budget (with a
    /// per-graph budget it is also counted in `deferred_graphs` and resumed,
    /// bsv-low #555).
    #[serde(default)]
    pub deadline_dropped_graphs: u64,
    /// Every persisted cursor that moved in this sync (bsv-low #552), one
    /// entry per peer. After a completed sync `to` is the peer's highest
    /// score seen (less the gap guard); after a deadline it is
    /// `GASPSync::completed_cursor`: past completed UTXOs only.
    #[serde(default)]
    pub cursor_moves: Vec<CursorMove>,
    /// Graphs DEFERRED in this sync, summed over peers (bsv-low #555,
    /// `Engine::set_graph_budget`): a new record, or a resumed graph
    /// deferred again. Their UTXOs are held below the cursor.
    #[serde(default)]
    pub deferred_graphs: u64,
    /// Graphs RESUMED from a record in this sync.
    #[serde(default)]
    pub resumed_graphs: u64,
    /// Resumed graphs that completed and landed in this sync (their record
    /// deleted).
    #[serde(default)]
    pub converged_graphs: u64,
    /// Records deleted without converging, with their reason.
    #[serde(default)]
    pub dropped_graphs: Vec<DroppedDeferral>,
    /// Walks the per-graph budget (or the per-peer deadline) cut having
    /// fetched NOTHING in their pass (bsv-low #555, the lens fold's H1). A
    /// peer's sync whose only outcome was these is a failed attempt for the
    /// quarantine count.
    #[serde(default)]
    pub stalled_graphs: u64,
    /// Records NOT resumed in this sync: the resumes of a peer's pass share
    /// ONE per-graph budget and had spent it (the lens fold's M1). Held for
    /// the next sync, untouched.
    #[serde(default)]
    pub held_back_graphs: u64,
}

/// A deferred graph's record deleted without converging (bsv-low #555,
/// [`TopicSyncResult::dropped_graphs`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DroppedDeferral {
    /// The peer URL.
    pub peer: String,
    /// The graph's root outpoint.
    pub outpoint: String,
    /// [`crate::gasp::DropReason::as_str`].
    pub reason: String,
}

/// One peer's persisted `last_interaction` cursor moving `from` -> `to` in a
/// sync ([`TopicSyncResult::cursor_moves`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorMove {
    /// The peer URL.
    pub peer: String,
    /// The cursor the sync entered with.
    pub from: u64,
    /// The cursor it persisted.
    pub to: u64,
}

/// Get current time in milliseconds (for output scores).
///
/// Uses `js_sys::Date::now()` on wasm32 (Cloudflare Workers) since
/// `std::time::SystemTime` is not available on that platform.
fn current_timestamp_ms() -> f64 {
    #[cfg(target_arch = "wasm32")]
    {
        js_sys::Date::now()
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0.0, |d| d.as_millis() as f64)
    }
}

/// Extract the domain field from a SHIP PushDrop locking script.
///
/// SHIP PushDrop format: field[0] = "SHIP", field[1] = identity_key,
/// field[2] = domain (UTF-8), field[3] = topic.
/// Returns `None` if the script cannot be parsed or is not a SHIP advertisement.
fn parse_ship_domain_from_script(output_script: &[u8]) -> Option<String> {
    use bsv_rs::script::templates::PushDrop;

    let script = bsv_rs::script::Script::from_binary(output_script).ok()?;
    let pushdrop = PushDrop::decode(&script.into()).ok()?;

    if pushdrop.fields.len() < 3 {
        return None;
    }

    let protocol = String::from_utf8_lossy(&pushdrop.fields[0]);
    if protocol != "SHIP" {
        return None;
    }

    let domain = String::from_utf8_lossy(&pushdrop.fields[2]).to_string();
    if domain.is_empty() {
        return None;
    }

    Some(domain)
}

// ============================================================================
// Submit-time spend verification helpers (reference parity, 2026-09-08)
// ============================================================================

// ============================================================================
// Error type
// ============================================================================

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("This server does not support this topic: {0}")]
    UnsupportedTopic(String),

    #[error("Lookup service not found for provider: {0}")]
    LookupServiceNotFound(String),

    /// GASP: `/requestForeignGASPNode` asked for a graph/txid we don't
    /// have locally. Matches mainline's 400-class "not found" response.
    #[error("No matching output found!")]
    NodeNotFound,

    #[error("lookup failed: {0}")]
    LookupFailed(String),

    /// The CALLER's query was malformed — a bad identity key, a gameId that is
    /// not 32 bytes of hex, a missing required field. Distinct from
    /// `LookupFailed`, which means WE failed: without the distinction every
    /// caller mistake was reported as a server fault (HTTP 500), so a malformed
    /// query and a genuine outage were indistinguishable in logs and alerts on
    /// an unauthenticated endpoint anyone can hit.
    #[error("invalid query: {0}")]
    InvalidQuery(String),

    #[error("storage error: {0}")]
    StorageError(String),

    #[error("broadcast error: {0}")]
    BroadcastError(String),

    #[error("SPV verification failed: {0}")]
    SpvError(String),

    #[error("BEEF parsing failed: {0}")]
    BeefParseError(String),

    /// A spend in the submitted BEEF does not satisfy its source output's
    /// locking script: the input's unlocking script was EXECUTED (reference
    /// parity, 2026-09-08) and the interpreter refused it. Distinct from
    /// [`EngineError::SpvError`] (a bad or unverifiable PROOF, a missing
    /// source, a chain-tracker fault) so an operator can tell a bad spend from
    /// a bad proof. `input_index` is the failing input's position in the
    /// transaction whose script failed (the subject, or an unproven
    /// ancestor of it in the same BEEF); `reason` is the interpreter's own
    /// message.
    #[error("script verification failed (subject {subject_txid}): input {input_index}: {reason}")]
    ScriptVerificationFailed {
        subject_txid: String,
        input_index: u32,
        reason: String,
    },

    /// The DOOR walk ([`Engine::verify_scripts_only`]) hit a STRUCTURAL fault
    /// before reaching a verdict on `at_txid` (a source the BEEF lacks, a
    /// parse fault, the value rule): not the interpreter's verdict, so not a
    /// refusal for a caller whose bar is the network. `subject_judged` says
    /// whether the subject's own inputs had all executed before the fault
    /// (an ancestor faulted) or not (the subject itself is unjudged).
    #[error("script walk inconclusive at {at_txid} (subject judged: {subject_judged}): {reason}")]
    ScriptWalkInconclusive {
        at_txid: String,
        subject_judged: bool,
        reason: String,
    },

    /// The DOOR walk exceeded its own bound ([`DoorBudget`]: the static work
    /// estimate, or the interpreter's memory limit tripping): the door's
    /// verdict, never the network's — the request proceeds to the network.
    #[error("script walk over budget at {at_txid} (subject judged: {subject_judged}): {what}")]
    ScriptWalkOverBudget {
        at_txid: String,
        subject_judged: bool,
        what: String,
    },

    #[error("{0}")]
    Other(String),
}

impl From<StorageError> for EngineError {
    fn from(e: StorageError) -> Self {
        EngineError::StorageError(e.to_string())
    }
}

impl From<TopicManagerError> for EngineError {
    fn from(e: TopicManagerError) -> Self {
        EngineError::Other(e.to_string())
    }
}

impl From<LookupServiceError> for EngineError {
    fn from(e: LookupServiceError) -> Self {
        EngineError::LookupFailed(e.to_string())
    }
}

impl From<AdvertiserError> for EngineError {
    fn from(e: AdvertiserError) -> Self {
        EngineError::Other(e.to_string())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lookup_service::LookupService as LookupServiceTrait;
    use crate::storage::memory::MemoryStorage;
    use crate::topic_manager::TopicManager as TopicManagerTrait;
    use async_trait::async_trait;
    use std::sync::Mutex;

    // ── Mock TopicManager ──────────────────────────────────────────────

    struct MockTopicManager {
        admit_indices: Vec<u32>,
    }

    impl MockTopicManager {
        fn admitting(indices: Vec<u32>) -> Self {
            Self {
                admit_indices: indices,
            }
        }
    }

    #[async_trait(?Send)]
    impl TopicManagerTrait for MockTopicManager {
        async fn identify_admissible_outputs(
            &self,
            _tx: &Transaction,
            _previous_coins: &[u8],
            _off_chain_values: Option<&[u8]>,
            _mode: SubmitMode,
            _context: &TopicAdmittanceContext,
        ) -> Result<AdmittanceInstructions, TopicManagerError> {
            Ok(AdmittanceInstructions {
                outputs_to_admit: self.admit_indices.clone(),
                coins_to_retain: vec![],
                coins_removed: None,
            })
        }

        async fn get_documentation(&self) -> String {
            "Mock topic manager".to_string()
        }

        async fn get_metadata(&self) -> ServiceMetadata {
            ServiceMetadata {
                name: "mock-tm".to_string(),
                description: Some("Mock for testing".to_string()),
                ..Default::default()
            }
        }
    }

    // ── Mock LookupService ─────────────────────────────────────────────

    struct MockLookupService {
        records: Mutex<Vec<UTXOReference>>,
    }

    impl MockLookupService {
        fn new() -> Self {
            Self {
                records: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait(?Send)]
    impl LookupServiceTrait for MockLookupService {
        fn admission_mode(&self) -> AdmissionMode {
            AdmissionMode::LockingScript
        }

        fn spend_notification_mode(&self) -> SpendNotificationMode {
            SpendNotificationMode::None
        }

        async fn output_admitted_by_topic(
            &self,
            payload: &OutputAdmittedByTopic,
        ) -> Result<(), LookupServiceError> {
            let (txid, oi) = match payload {
                OutputAdmittedByTopic::LockingScript {
                    txid, output_index, ..
                } => (txid.clone(), *output_index),
                OutputAdmittedByTopic::WholeTx { output_index, .. } => {
                    ("whole".into(), *output_index)
                }
            };
            self.records.lock().unwrap().push(UTXOReference {
                txid,
                output_index: oi,
            });
            Ok(())
        }

        async fn output_evicted(
            &self,
            txid: &str,
            output_index: u32,
        ) -> Result<(), LookupServiceError> {
            self.records
                .lock()
                .unwrap()
                .retain(|r| !(r.txid == txid && r.output_index == output_index));
            Ok(())
        }

        async fn lookup(
            &self,
            _question: &LookupQuestion,
        ) -> Result<LookupResult, LookupServiceError> {
            Ok(LookupResult::OutputList(
                self.records.lock().unwrap().clone(),
            ))
        }

        async fn get_documentation(&self) -> String {
            "Mock lookup service".to_string()
        }

        async fn get_metadata(&self) -> ServiceMetadata {
            ServiceMetadata {
                name: "mock-ls".to_string(),
                ..Default::default()
            }
        }
    }

    // ── Test BEEF data ──────────────────────────────────────────────────

    /// Real BRC-62 BEEF from TS overlay-services test suite.
    const TEST_BEEF_HEX: &str = "0100beef01fe636d0c0007021400fe507c0c7aa754cef1f7889d5fd395cf1f785dd7de98eed895dbedfe4e5bc70d1502ac4e164f5bc16746bb0868404292ac8318bbac3800e4aad13a014da427adce3e010b00bc4ff395efd11719b277694cface5aa50d085a0bb81f613f70313acd28cf4557010400574b2d9142b8d28b61d88e3b2c3f44d858411356b49a28a4643b6d1a6a092a5201030051a05fc84d531b5d250c23f4f886f6812f9fe3f402d61607f977b4ecd2701c19010000fd781529d58fc2523cf396a7f25440b409857e7e221766c57214b1d38c7b481f01010062f542f45ea3660f86c013ced80534cb5fd4c19d66c56e7e8c5d4bf2d40acc5e010100b121e91836fd7cd5102b654e9f72f3cf6fdbfd0b161c53a9c54b12c841126331020100000001cd4e4cac3c7b56920d1e7655e7e260d31f29d9a388d04910f1bbd72304a79029010000006b483045022100e75279a205a547c445719420aa3138bf14743e3f42618e5f86a19bde14bb95f7022064777d34776b05d816daf1699493fcdf2ef5a5ab1ad710d9c97bfb5b8f7cef3641210263e2dee22b1ddc5e11f6fab8bcd2378bdd19580d640501ea956ec0e786f93e76ffffffff013e660000000000001976a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac0000000001000100000001ac4e164f5bc16746bb0868404292ac8318bbac3800e4aad13a014da427adce3e000000006a47304402203a61a2e931612b4bda08d541cfb980885173b8dcf64a3471238ae7abcd368d6402204cbf24f04b9aa2256d8901f0ed97866603d2be8324c2bfb7a37bf8fc90edd5b441210263e2dee22b1ddc5e11f6fab8bcd2378bdd19580d640501ea956ec0e786f93e76ffffffff013c660000000000001976a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac0000000000";
    const TEST_TXID: &str = "157428aee67d11123203735e4c540fa1bdab3b36d5882c6f8c5ff79f07d20d1c";

    fn decode_hex(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect()
    }

    fn test_beef() -> Vec<u8> {
        decode_hex(TEST_BEEF_HEX)
    }

    fn test_tagged_beef(topics: Vec<&str>) -> TaggedBEEF {
        TaggedBEEF::new(
            test_beef(),
            topics.into_iter().map(str::to_string).collect(),
        )
    }

    // ── Helpers ────────────────────────────────────────────────────────

    fn make_engine(admit_indices: Vec<u32>) -> Engine {
        let mut managers: HashMap<String, Box<dyn TopicManagerTrait>> = HashMap::new();
        managers.insert(
            "tm_test".to_string(),
            Box::new(MockTopicManager::admitting(admit_indices)),
        );

        let mut lookup_services: HashMap<String, Box<dyn LookupServiceTrait>> = HashMap::new();
        lookup_services.insert("ls_test".to_string(), Box::new(MockLookupService::new()));

        let storage = Box::new(MemoryStorage::new());

        Engine::new(
            managers,
            lookup_services,
            storage,
            None,
            EngineConfig::default(),
        )
    }

    // ── Tests ──────────────────────────────────────────────────────────

    /// bsv-low#289 parse-once + zanaadu#284 subject discipline: the tx handed
    /// to every topic manager must be the ATOMIC SUBJECT of the submitted
    /// BEEF, not whatever sits last in wire order. bsv-rs < 0.3.20's
    /// `from_beef(_, None)` picked `txs.last()`; wallet serializers do not all
    /// place the subject last, so a TM could be handed a PARENT to validate.
    /// This pins the engine on the fixed rule (`txid ?? atomicTxid ?? last`).
    #[tokio::test]
    async fn parse_once_hands_tms_the_atomic_subject_not_wire_order() {
        use bsv_rs::transaction::beef_tx::ATOMIC_BEEF;
        use std::cell::RefCell;
        use std::rc::Rc;

        // Rebuild the fixture chain SUBJECT-FIRST via to_writer (to_binary
        // re-sorts parents-first and would hide the trigger).
        let parsed = Beef::from_binary(&test_beef()).unwrap();
        let parent = parsed.txs[0].tx().unwrap().clone();
        let child = parsed.txs[1].tx().unwrap().clone();
        let child_id = child.id();
        let parent_id = parent.id();
        let mut beef = Beef::new();
        // The parent's BUMP rides along: the reference-parity walk
        // (2026-09-08) executes the subject's script against the parent and
        // then trusts the parent on its proof; without the BUMP it would
        // demand the parent's own ancestry ("Input 0 has no source
        // transaction"), which this fixture does not carry.
        for bump in &parsed.bumps {
            beef.merge_bump(bump.clone());
        }
        beef.merge_transaction(child);
        beef.merge_transaction(parent);
        let mut w = bsv_rs::primitives::encoding::Writer::new();
        w.write_u32_le(ATOMIC_BEEF);
        let mut le = hex::decode(&child_id).unwrap();
        le.reverse();
        w.write_bytes(&le);
        beef.to_writer(&mut w);
        let atomic_wire_order = w.into_bytes();

        // A TM that records the txid it is handed.
        struct RecordingTm(Rc<RefCell<Option<String>>>);
        #[async_trait(?Send)]
        impl TopicManagerTrait for RecordingTm {
            async fn identify_admissible_outputs(
                &self,
                tx: &Transaction,
                _previous_coins: &[u8],
                _off_chain_values: Option<&[u8]>,
                _mode: SubmitMode,
                _context: &TopicAdmittanceContext,
            ) -> Result<AdmittanceInstructions, TopicManagerError> {
                *self.0.borrow_mut() = Some(tx.id());
                Ok(AdmittanceInstructions {
                    outputs_to_admit: vec![0],
                    coins_to_retain: vec![],
                    coins_removed: None,
                })
            }
            async fn get_documentation(&self) -> String {
                String::new()
            }
            async fn get_metadata(&self) -> ServiceMetadata {
                ServiceMetadata {
                    name: "tm_test".to_string(),
                    ..Default::default()
                }
            }
        }

        let seen: Rc<RefCell<Option<String>>> = Rc::new(RefCell::new(None));
        let mut managers: HashMap<String, Box<dyn TopicManagerTrait>> = HashMap::new();
        managers.insert(
            "tm_test".to_string(),
            Box::new(RecordingTm(Rc::clone(&seen))),
        );
        let engine = Engine::new(
            managers,
            HashMap::new(),
            Box::new(MemoryStorage::new()),
            None,
            EngineConfig::default(),
        );

        let tagged = TaggedBEEF::new(atomic_wire_order, vec!["tm_test".to_string()]);
        let _ = engine.submit(&tagged, SubmitMode::CurrentTx).await.unwrap();

        let handed = seen.borrow().clone().expect("TM was not invoked");
        assert_eq!(
            handed, child_id,
            "TM must be handed the atomic subject, got the wire-order pick"
        );
        assert_ne!(handed, parent_id);
    }

    /// #4 REGRESSION: history hydration must never DOWNGRADE provenance.
    ///
    /// `get_utxo_history` used to `Transaction::from_beef(beef, None)` and then
    /// `tx.to_beef(true)`, which keeps only the subject and DROPS the stored
    /// BUMPs. Measured on prod before the fix: `ls_low` byGameId `73de6e48…`
    /// returned 1,172 B / 1 tx / 1 bump (PROVEN) with no header, and 676 B /
    /// 1 tx / 0 bumps with `x-history-depth: 3`. A caller asking for MORE
    /// provenance got a BEEF no wallet can verify — silently, behind a 200.
    ///
    /// There was NO test on this function at all, which is exactly how that
    /// shipped. This one asserts the invariant that matters: whatever else
    /// hydration does, a proof that went in comes back out.
    #[tokio::test]
    async fn history_hydration_preserves_the_stored_proof() {
        use bsv_rs::transaction::Beef;

        let engine = make_engine(vec![0]);
        let stored = test_beef();
        let bumps_before = Beef::from_binary(&stored)
            .expect("fixture BEEF parses")
            .bumps
            .len();
        assert!(
            bumps_before > 0,
            "fixture must actually carry a proof, or this cell proves nothing"
        );

        let output = Output {
            txid: TEST_TXID.to_string(),
            output_index: 0,
            output_script: vec![0x76, 0xa9],
            satoshis: 1000,
            topic: "tm_test".to_string(),
            spent: false,
            // No admitted parents: hydration has nothing to add, so anything
            // it REMOVES is pure loss — the cleanest form of the bug.
            outputs_consumed: vec![],
            consumed_by: vec![],
            beef: Some(stored.clone()),
            block_height: None,
            score: Some(1000.0),
        };

        let hydrated = engine
            .get_utxo_history(&output, Some(HistorySelector::Depth(3)))
            .await
            .expect("hydration must not error")
            .expect("hydration must return the output");
        let beef = Beef::from_binary(&hydrated.beef.expect("hydrated BEEF present"))
            .expect("hydrated BEEF parses");

        assert_eq!(
            beef.bumps.len(),
            bumps_before,
            "asking for history must not strip the merkle proof (issue #4)"
        );
    }

    #[tokio::test]
    async fn test_submit_unsupported_topic_errors() {
        let engine = make_engine(vec![0]);
        let beef = test_tagged_beef(vec!["tm_nonexistent"]);

        let result = engine.submit(&beef, SubmitMode::CurrentTx).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            EngineError::UnsupportedTopic(_)
        ));
    }

    #[tokio::test]
    async fn test_submit_admits_outputs() {
        let engine = make_engine(vec![0, 1]);
        let beef = test_tagged_beef(vec!["tm_test"]);

        let steak = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        // STEAK should have tm_test with outputs 0 and 1 admitted
        let instructions = steak.get("tm_test").unwrap();
        assert_eq!(instructions.outputs_to_admit, vec![0, 1]);

        // Outputs should be in storage with real data
        let txid = TEST_TXID;
        let found = engine
            .storage()
            .find_output(txid, 0, Some("tm_test"), None, true)
            .await
            .unwrap()
            .expect("Output 0 should be in storage");

        assert_eq!(found.txid, TEST_TXID);
        assert_eq!(found.satoshis, 26172); // Real value from BRC62 BEEF
        assert!(
            !found.output_script.is_empty(),
            "Script should be populated"
        );
        assert!(found.beef.is_some(), "BEEF should be stored");
        assert!(!found.spent);
    }

    #[tokio::test]
    async fn test_submit_duplicate_is_skipped() {
        let engine = make_engine(vec![0]);
        let beef = test_tagged_beef(vec!["tm_test"]);

        // First submit
        let steak1 = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();
        assert_eq!(steak1["tm_test"].outputs_to_admit, vec![0]);

        // Second submit — should be a dupe, no new outputs admitted
        let steak2 = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();
        assert!(steak2["tm_test"].outputs_to_admit.is_empty());
    }

    #[tokio::test]
    async fn test_submit_notifies_lookup_service() {
        let engine = make_engine(vec![0]);
        let beef = test_tagged_beef(vec!["tm_test"]);

        engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        // Lookup service should have the output
        let question = LookupQuestion::new("ls_test", serde_json::json!({}));
        let answer = engine.lookup(&question, None).await.unwrap();

        match answer {
            LookupAnswer::OutputList { outputs } => {
                assert_eq!(outputs.len(), 1);
            }
            _ => panic!("Expected OutputList"),
        }
    }

    /// #289: `lookup_with_txids` returns the storage txid per hydrated
    /// output, aligned with the OutputList — the aggregated serializer
    /// writes it instead of re-hashing the BEEF.
    #[tokio::test]
    async fn test_lookup_with_txids_aligns_txids_with_outputs() {
        let engine = make_engine(vec![0]);
        let beef = test_tagged_beef(vec!["tm_test"]);
        engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        let question = LookupQuestion::new("ls_test", serde_json::json!({}));
        let (answer, txids) = engine.lookup_with_txids(&question, None).await.unwrap();
        match answer {
            LookupAnswer::OutputList { outputs } => {
                assert_eq!(outputs.len(), 1);
                assert_eq!(txids.len(), outputs.len(), "one txid per output");
                assert_eq!(txids[0], TEST_TXID, "the storage primary key, verbatim");
            }
            _ => panic!("Expected OutputList"),
        }
    }

    /// #291: the public sync-response page size is always bounded.
    #[test]
    fn sync_limit_clamp() {
        assert_eq!(
            Engine::clamp_sync_limit(None),
            Engine::SYNC_RESPONSE_DEFAULT_LIMIT,
            "absent limit gets the default page, never unbounded"
        );
        assert_eq!(Engine::clamp_sync_limit(Some(7)), 7);
        assert_eq!(
            Engine::clamp_sync_limit(Some(u64::MAX)),
            Engine::SYNC_RESPONSE_MAX_LIMIT,
            "a huge caller-supplied limit is capped"
        );
    }

    #[tokio::test]
    async fn test_lookup_unknown_service_errors() {
        let engine = make_engine(vec![]);
        let question = LookupQuestion::new("ls_nonexistent", serde_json::json!({}));

        let result = engine.lookup(&question, None).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            EngineError::LookupServiceNotFound(_)
        ));
    }

    #[tokio::test]
    async fn test_list_topic_managers() {
        let engine = make_engine(vec![]);
        let managers = engine.list_topic_managers().await;

        assert_eq!(managers.len(), 1);
        assert!(managers.contains_key("tm_test"));
        assert_eq!(managers["tm_test"].name, "mock-tm");
    }

    #[tokio::test]
    async fn test_list_lookup_service_providers() {
        let engine = make_engine(vec![]);
        let services = engine.list_lookup_service_providers().await;

        assert_eq!(services.len(), 1);
        assert!(services.contains_key("ls_test"));
        assert_eq!(services["ls_test"].name, "mock-ls");
    }

    #[tokio::test]
    async fn test_get_documentation() {
        let engine = make_engine(vec![]);

        let tm_docs = engine.get_documentation_for_topic_manager("tm_test").await;
        assert_eq!(tm_docs, "Mock topic manager");

        let ls_docs = engine.get_documentation_for_lookup_service("ls_test").await;
        assert_eq!(ls_docs, "Mock lookup service");

        let missing = engine
            .get_documentation_for_topic_manager("tm_missing")
            .await;
        assert_eq!(missing, "No documentation found!");
    }

    #[tokio::test]
    async fn test_provide_foreign_sync_response() {
        let engine = make_engine(vec![0]);

        // Submit data
        let beef = test_tagged_beef(vec!["tm_test"]);
        engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        // Query GASP sync
        let request = GASPInitialRequest {
            version: 1,
            since: 0,
            limit: Some(100),
        };
        let response = engine
            .provide_foreign_sync_response(&request, "tm_test")
            .await
            .unwrap();

        // One tx admitted with output index 0
        assert_eq!(response.utxo_list.len(), 1);
        assert_eq!(response.utxo_list[0].txid, TEST_TXID);
        assert_eq!(response.since, 0);
    }

    #[tokio::test]
    async fn test_delete_utxo_deep_simple() {
        let engine = make_engine(vec![0]);
        let beef = test_tagged_beef(vec!["tm_test"]);

        engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        let txid = TEST_TXID;
        let output = engine
            .storage()
            .find_output(txid, 0, Some("tm_test"), None, false)
            .await
            .unwrap()
            .unwrap();

        // Delete it
        engine.delete_utxo_deep(&output, None).await.unwrap();

        // Should be gone
        let found = engine
            .storage()
            .find_output(txid, 0, Some("tm_test"), None, false)
            .await
            .unwrap();
        assert!(found.is_none());
    }

    #[tokio::test]
    async fn test_sync_config_defaults_to_ship() {
        let engine = make_engine(vec![]);
        let sync = &engine.config.sync_configuration;

        // tm_test should default to SHIP sync
        assert!(sync.contains_key("tm_test"));
        assert!(matches!(sync["tm_test"], SyncTarget::Ship));
    }

    #[tokio::test]
    async fn test_multiple_topics_in_single_submit() {
        let mut managers: HashMap<String, Box<dyn TopicManagerTrait>> = HashMap::new();
        managers.insert(
            "tm_alpha".to_string(),
            Box::new(MockTopicManager::admitting(vec![0])),
        );
        managers.insert(
            "tm_beta".to_string(),
            Box::new(MockTopicManager::admitting(vec![1])),
        );

        let engine = Engine::new(
            managers,
            HashMap::new(),
            Box::new(MemoryStorage::new()),
            None,
            EngineConfig::default(),
        );

        let beef = test_tagged_beef(vec!["tm_alpha", "tm_beta"]);

        let steak = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        assert_eq!(steak["tm_alpha"].outputs_to_admit, vec![0]);
        assert_eq!(steak["tm_beta"].outputs_to_admit, vec![1]);
    }

    #[tokio::test]
    async fn test_evict_output_with_topic() {
        let engine = make_engine(vec![0]);
        let beef = test_tagged_beef(vec!["tm_test"]);
        engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        // Output should exist
        let found = engine
            .storage()
            .find_output(TEST_TXID, 0, Some("tm_test"), None, false)
            .await
            .unwrap();
        assert!(found.is_some(), "Output should exist before eviction");

        // Evict with topic
        engine
            .evict_output(TEST_TXID, 0, Some("tm_test"))
            .await
            .unwrap();

        // Output should be gone from storage
        let found = engine
            .storage()
            .find_output(TEST_TXID, 0, Some("tm_test"), None, false)
            .await
            .unwrap();
        assert!(found.is_none(), "Output should be gone after eviction");

        // Lookup service should also have been notified (output evicted)
        let question = LookupQuestion::new("ls_test", serde_json::json!({}));
        let answer = engine.lookup(&question, None).await.unwrap();
        match answer {
            LookupAnswer::OutputList { outputs } => {
                assert!(
                    outputs.is_empty(),
                    "Lookup should return empty after eviction"
                );
            }
            _ => panic!("Expected OutputList"),
        }
    }

    #[tokio::test]
    async fn test_evict_output_without_topic() {
        let engine = make_engine(vec![0]);
        let beef = test_tagged_beef(vec!["tm_test"]);
        engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        // Evict without topic — should find and remove across all topics
        engine.evict_output(TEST_TXID, 0, None).await.unwrap();

        let found = engine
            .storage()
            .find_output(TEST_TXID, 0, Some("tm_test"), None, false)
            .await
            .unwrap();
        assert!(
            found.is_none(),
            "Output should be gone after topic-less eviction"
        );
    }

    #[tokio::test]
    async fn test_evict_nonexistent_output_is_ok() {
        let engine = make_engine(vec![]);

        // Evicting something that doesn't exist should not error
        let result = engine
            .evict_output("nonexistent_txid", 99, Some("tm_test"))
            .await;
        assert!(result.is_ok(), "Evicting nonexistent output should succeed");
    }

    // ── Mock Broadcaster ──────────────────────────────────────────────

    use crate::broadcaster::Broadcaster;
    use std::sync::Arc;

    type BroadcastCallLog = Arc<Mutex<Vec<(String, Vec<String>)>>>;

    struct MockBroadcaster {
        calls: BroadcastCallLog,
    }

    impl MockBroadcaster {
        fn new() -> Self {
            Self {
                calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        #[allow(dead_code, reason = "kept for future test extension")]
        fn call_count(&self) -> usize {
            self.calls.lock().unwrap().len()
        }

        #[allow(dead_code, reason = "kept for future test extension")]
        fn calls(&self) -> Vec<(String, Vec<String>)> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait(?Send)]
    impl Broadcaster for MockBroadcaster {
        async fn broadcast_to_host(
            &self,
            host_url: &str,
            tagged_beef: &TaggedBEEF,
        ) -> Result<(), String> {
            self.calls
                .lock()
                .unwrap()
                .push((host_url.to_string(), tagged_beef.topics.clone()));
            Ok(())
        }
    }

    // ── Mock SHIP LookupService ─────────────────────────────────────

    /// A mock "ls_ship" lookup service that returns pre-configured UTXOReferences
    /// when queried. Used to test SHIP propagation without depending on
    /// overlay-discovery in the engine crate's unit tests.
    struct MockSHIPLookupService {
        /// Maps topic -> list of (txid, output_index) references
        records: Mutex<HashMap<String, Vec<UTXOReference>>>,
    }

    impl MockSHIPLookupService {
        fn with_records(records: HashMap<String, Vec<UTXOReference>>) -> Self {
            Self {
                records: Mutex::new(records),
            }
        }
    }

    #[async_trait(?Send)]
    impl LookupServiceTrait for MockSHIPLookupService {
        fn admission_mode(&self) -> AdmissionMode {
            AdmissionMode::LockingScript
        }

        fn spend_notification_mode(&self) -> SpendNotificationMode {
            SpendNotificationMode::None
        }

        async fn output_admitted_by_topic(
            &self,
            _payload: &OutputAdmittedByTopic,
        ) -> Result<(), LookupServiceError> {
            Ok(())
        }

        async fn output_evicted(
            &self,
            _txid: &str,
            _output_index: u32,
        ) -> Result<(), LookupServiceError> {
            Ok(())
        }

        async fn lookup(
            &self,
            question: &LookupQuestion,
        ) -> Result<LookupResult, LookupServiceError> {
            // Parse the topics from the query
            if let Some(topics) = question.query.get("topics").and_then(|v| v.as_array()) {
                let records = self.records.lock().unwrap();
                let mut results = Vec::new();
                for topic_val in topics {
                    if let Some(topic) = topic_val.as_str() {
                        if let Some(refs) = records.get(topic) {
                            results.extend(refs.iter().cloned());
                        }
                    }
                }
                return Ok(LookupResult::OutputList(results));
            }
            Ok(LookupResult::OutputList(vec![]))
        }

        async fn get_documentation(&self) -> String {
            "Mock SHIP lookup service".to_string()
        }

        async fn get_metadata(&self) -> ServiceMetadata {
            ServiceMetadata {
                name: "mock-ls-ship".to_string(),
                ..Default::default()
            }
        }
    }

    /// Build a minimal PushDrop locking script for a SHIP advertisement.
    fn build_ship_pushdrop_script(domain: &str, topic: &str) -> Vec<u8> {
        use bsv_rs::script::templates::PushDrop;
        use bsv_rs::PublicKey;

        let fields = vec![
            b"SHIP".to_vec(),
            vec![0x02; 33], // fake compressed pubkey bytes for identity_key field
            domain.as_bytes().to_vec(),
            topic.as_bytes().to_vec(),
        ];

        // PushDrop requires a real compressed public key for the locking script
        // Use a well-known test key (generator point G)
        let pubkey = PublicKey::from_hex(
            "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798",
        )
        .expect("valid test pubkey");

        let pd = PushDrop::new(pubkey, fields);
        pd.lock().to_binary()
    }

    /// Create engine with a broadcaster and a mock SHIP lookup service.
    /// The `ship_domains` parameter provides (domain, topic) pairs to pre-populate.
    async fn make_engine_with_broadcaster(
        admit_indices: Vec<u32>,
        broadcaster: MockBroadcaster,
        ship_domains: Vec<(&str, &str)>,
        hosting_url: Option<&str>,
    ) -> (Engine, Arc<Mutex<Vec<(String, Vec<String>)>>>) {
        let calls = broadcaster.calls.clone();

        let mut managers: HashMap<String, Box<dyn TopicManagerTrait>> = HashMap::new();
        managers.insert(
            "tm_test".to_string(),
            Box::new(MockTopicManager::admitting(admit_indices)),
        );

        let storage = Box::new(MemoryStorage::new());

        // Build SHIP records: map each topic to its UTXO references and
        // store outputs in storage with PushDrop scripts so the engine can
        // parse the domain when broadcasting.
        let mut ship_records: HashMap<String, Vec<UTXOReference>> = HashMap::new();

        for (domain, topic) in &ship_domains {
            // Deterministic fake txid per domain+topic
            let fake_txid = format!("{:064x}", {
                let mut h: u64 = 0;
                for b in domain.bytes().chain(topic.bytes()) {
                    h = h.wrapping_mul(31).wrapping_add(u64::from(b));
                }
                h
            });

            // Add to SHIP records for the mock lookup service
            ship_records
                .entry(topic.to_string())
                .or_default()
                .push(UTXOReference {
                    txid: fake_txid.clone(),
                    output_index: 0,
                });

            // Build PushDrop SHIP script and store as an output in main storage
            let script = build_ship_pushdrop_script(domain, topic);

            let output = Output {
                txid: fake_txid,
                output_index: 0,
                output_script: script,
                satoshis: 1,
                topic: "tm_ship".to_string(),
                spent: false,
                outputs_consumed: vec![],
                consumed_by: vec![],
                beef: None,
                block_height: None,
                score: None,
            };

            storage.insert_output(&output).await.unwrap();
        }

        let mut lookup_services: HashMap<String, Box<dyn LookupServiceTrait>> = HashMap::new();
        lookup_services.insert("ls_test".to_string(), Box::new(MockLookupService::new()));
        lookup_services.insert(
            "ls_ship".to_string(),
            Box::new(MockSHIPLookupService::with_records(ship_records)),
        );

        let config = EngineConfig {
            hosting_url: hosting_url.map(str::to_string),
            ..Default::default()
        };

        let engine = Engine::with_chain_tracker(
            managers,
            lookup_services,
            storage,
            None,
            Some(Box::new(broadcaster)),
            None,
            config,
        );

        (engine, calls)
    }

    // ── Broadcaster Tests ─────────────────────────────────────────────

    #[tokio::test]
    async fn test_broadcaster_called_on_current_tx_with_ship_peers() {
        let broadcaster = MockBroadcaster::new();
        let (engine, calls) = make_engine_with_broadcaster(
            vec![0],
            broadcaster,
            vec![("https://peer1.example.com", "tm_test")],
            Some("https://self.example.com"),
        )
        .await;

        let beef = test_tagged_beef(vec!["tm_test"]);
        let steak = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        // Submit should succeed
        assert!(!steak["tm_test"].outputs_to_admit.is_empty());

        // Broadcaster should have been called for peer1
        let recorded = calls.lock().unwrap();
        assert_eq!(
            recorded.len(),
            1,
            "Broadcaster should be called once for the one SHIP peer"
        );
        assert_eq!(recorded[0].0, "https://peer1.example.com");
    }

    #[tokio::test]
    async fn test_broadcaster_not_called_on_historical_tx() {
        let broadcaster = MockBroadcaster::new();
        let (engine, calls) = make_engine_with_broadcaster(
            vec![0],
            broadcaster,
            vec![("https://peer1.example.com", "tm_test")],
            None,
        )
        .await;

        let beef = test_tagged_beef(vec!["tm_test"]);
        let steak = engine
            .submit(&beef, SubmitMode::HistoricalTx)
            .await
            .unwrap();

        assert!(!steak["tm_test"].outputs_to_admit.is_empty());

        // Broadcaster should NOT have been called
        assert_eq!(
            calls.lock().unwrap().len(),
            0,
            "Broadcaster should not be called for historical TX"
        );
    }

    #[tokio::test]
    async fn test_broadcaster_not_called_on_historical_tx_no_spv() {
        let broadcaster = MockBroadcaster::new();
        let (engine, calls) = make_engine_with_broadcaster(
            vec![0],
            broadcaster,
            vec![("https://peer1.example.com", "tm_test")],
            None,
        )
        .await;

        let beef = test_tagged_beef(vec!["tm_test"]);
        let steak = engine
            .submit(&beef, SubmitMode::HistoricalTxNoSpv)
            .await
            .unwrap();

        assert!(!steak["tm_test"].outputs_to_admit.is_empty());

        assert_eq!(
            calls.lock().unwrap().len(),
            0,
            "Broadcaster should not be called for historical-tx-no-spv"
        );
    }

    #[tokio::test]
    async fn test_broadcaster_skips_self_hosting_url() {
        let broadcaster = MockBroadcaster::new();
        let (engine, calls) = make_engine_with_broadcaster(
            vec![0],
            broadcaster,
            vec![
                ("https://self.example.com", "tm_test"),
                ("https://peer2.example.com", "tm_test"),
            ],
            Some("https://self.example.com"),
        )
        .await;

        let beef = test_tagged_beef(vec!["tm_test"]);
        engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        // Should only broadcast to peer2, not to self
        let recorded = calls.lock().unwrap();
        assert_eq!(
            recorded.len(),
            1,
            "Should broadcast to peer2 only, skipping self"
        );
        assert_eq!(recorded[0].0, "https://peer2.example.com");
    }

    #[tokio::test]
    async fn test_broadcaster_not_called_when_no_outputs_admitted() {
        let broadcaster = MockBroadcaster::new();
        // Topic manager admits nothing
        let (engine, calls) = make_engine_with_broadcaster(
            vec![],
            broadcaster,
            vec![("https://peer1.example.com", "tm_test")],
            None,
        )
        .await;

        let beef = test_tagged_beef(vec!["tm_test"]);
        let steak = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        assert!(steak["tm_test"].outputs_to_admit.is_empty());

        // No outputs admitted -> no broadcast
        assert_eq!(
            calls.lock().unwrap().len(),
            0,
            "Broadcaster should not be called when no outputs are admitted"
        );
    }

    #[tokio::test]
    async fn test_broadcaster_not_called_when_no_ship_peers() {
        let broadcaster = MockBroadcaster::new();
        // No SHIP peers registered
        let (engine, calls) = make_engine_with_broadcaster(
            vec![0],
            broadcaster,
            vec![], // no SHIP peers
            None,
        )
        .await;

        let beef = test_tagged_beef(vec!["tm_test"]);
        engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        assert_eq!(
            calls.lock().unwrap().len(),
            0,
            "Broadcaster should not be called when no SHIP peers exist"
        );
    }

    // ── Configurable SpendNotification mock ─────────────────────────────

    /// A mock lookup service whose SpendNotificationMode can be configured.
    /// Uses a shared Arc<Mutex<Vec<OutputSpent>>> so the caller can inspect
    /// captured payloads after submit().
    struct SpendModeLookupService {
        mode: SpendNotificationMode,
        records: Mutex<Vec<UTXOReference>>,
        spent_payloads: Arc<Mutex<Vec<OutputSpent>>>,
    }

    impl SpendModeLookupService {
        fn with_mode(mode: SpendNotificationMode, capture: Arc<Mutex<Vec<OutputSpent>>>) -> Self {
            Self {
                mode,
                records: Mutex::new(Vec::new()),
                spent_payloads: capture,
            }
        }
    }

    #[async_trait(?Send)]
    impl LookupServiceTrait for SpendModeLookupService {
        fn admission_mode(&self) -> AdmissionMode {
            AdmissionMode::LockingScript
        }

        fn spend_notification_mode(&self) -> SpendNotificationMode {
            self.mode
        }

        async fn output_admitted_by_topic(
            &self,
            payload: &OutputAdmittedByTopic,
        ) -> Result<(), LookupServiceError> {
            let (txid, oi) = match payload {
                OutputAdmittedByTopic::LockingScript {
                    txid, output_index, ..
                } => (txid.clone(), *output_index),
                OutputAdmittedByTopic::WholeTx { output_index, .. } => {
                    ("whole".into(), *output_index)
                }
            };
            self.records.lock().unwrap().push(UTXOReference {
                txid,
                output_index: oi,
            });
            Ok(())
        }

        async fn output_spent(&self, payload: &OutputSpent) -> Result<(), LookupServiceError> {
            self.spent_payloads.lock().unwrap().push(payload.clone());
            Ok(())
        }

        async fn output_evicted(
            &self,
            txid: &str,
            output_index: u32,
        ) -> Result<(), LookupServiceError> {
            self.records
                .lock()
                .unwrap()
                .retain(|r| !(r.txid == txid && r.output_index == output_index));
            Ok(())
        }

        async fn lookup(
            &self,
            _question: &LookupQuestion,
        ) -> Result<LookupResult, LookupServiceError> {
            Ok(LookupResult::OutputList(
                self.records.lock().unwrap().clone(),
            ))
        }

        async fn get_documentation(&self) -> String {
            "SpendMode mock lookup service".to_string()
        }

        async fn get_metadata(&self) -> ServiceMetadata {
            ServiceMetadata {
                name: "spend-mode-ls".to_string(),
                ..Default::default()
            }
        }
    }

    /// TXID of the input's source transaction in TEST_BEEF_HEX (the "previous coin").
    const PREVIOUS_TXID: &str = "3ecead27a44d013ad1aae40038acbb1883ac9242406808bb4667c15b4f164eac";

    /// Build an engine with a pre-populated previous output and a SpendModeLookupService.
    /// Returns the engine and the shared capture vec for inspecting OutputSpent payloads.
    async fn make_spend_mode_engine(
        mode: SpendNotificationMode,
    ) -> (Engine, Arc<Mutex<Vec<OutputSpent>>>) {
        let storage = MemoryStorage::new();

        // Pre-populate storage with the previous output that TEST_BEEF_HEX's input spends.
        let prev_output = Output {
            txid: PREVIOUS_TXID.to_string(),
            output_index: 0,
            output_script: vec![0x76, 0xa9],
            satoshis: 26174,
            topic: "tm_test".to_string(),
            spent: false,
            outputs_consumed: vec![],
            consumed_by: vec![],
            beef: Some(test_beef()),
            block_height: None,
            score: Some(1000.0),
        };
        storage.insert_output(&prev_output).await.unwrap();

        let mut managers: HashMap<String, Box<dyn TopicManagerTrait>> = HashMap::new();
        managers.insert(
            "tm_test".to_string(),
            Box::new(MockTopicManager::admitting(vec![0])),
        );

        let capture: Arc<Mutex<Vec<OutputSpent>>> = Arc::new(Mutex::new(Vec::new()));
        let ls = SpendModeLookupService::with_mode(mode, capture.clone());
        let mut lookup_services: HashMap<String, Box<dyn LookupServiceTrait>> = HashMap::new();
        lookup_services.insert("ls_test".to_string(), Box::new(ls));

        let engine = Engine::new(
            managers,
            lookup_services,
            Box::new(storage),
            None,
            EngineConfig::default(),
        );

        (engine, capture)
    }

    #[tokio::test]
    async fn test_spend_notification_mode_script() {
        let (engine, capture) = make_spend_mode_engine(SpendNotificationMode::Script).await;

        let beef = test_tagged_beef(vec!["tm_test"]);
        let steak = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        // The new output should be admitted
        assert_eq!(steak["tm_test"].outputs_to_admit, vec![0]);

        // The lookup service should have received a Script spend notification
        let payloads = capture.lock().unwrap().clone();
        assert_eq!(payloads.len(), 1, "Expected exactly one spend notification");

        match &payloads[0] {
            OutputSpent::Script {
                txid,
                output_index,
                topic,
                spending_txid,
                input_index,
                unlocking_script,
                sequence_number,
                ..
            } => {
                assert_eq!(txid, PREVIOUS_TXID);
                assert_eq!(*output_index, 0);
                assert_eq!(topic, "tm_test");
                assert_eq!(spending_txid, TEST_TXID);
                // The BEEF has a single input at index 0 that spends PREVIOUS_TXID:0
                assert_eq!(*input_index, 0);
                // The unlocking script should be non-empty (it's a P2PKH scriptSig)
                assert!(
                    !unlocking_script.is_empty(),
                    "Unlocking script should be non-empty"
                );
                // Standard final sequence number
                assert_eq!(*sequence_number, 0xffff_ffff);
            }
            other => panic!("Expected OutputSpent::Script, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_spend_notification_mode_whole_tx() {
        let (engine, capture) = make_spend_mode_engine(SpendNotificationMode::WholeTx).await;

        let beef = test_tagged_beef(vec!["tm_test"]);
        let steak = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        assert_eq!(steak["tm_test"].outputs_to_admit, vec![0]);

        let payloads = capture.lock().unwrap().clone();
        assert_eq!(payloads.len(), 1, "Expected exactly one spend notification");

        match &payloads[0] {
            OutputSpent::WholeTx {
                txid,
                output_index,
                topic,
                spending_atomic_beef,
                ..
            } => {
                assert_eq!(txid, PREVIOUS_TXID);
                assert_eq!(*output_index, 0);
                assert_eq!(topic, "tm_test");
                // The spending BEEF is the entire BEEF we submitted, NAMED
                // (atomic prefix) for the lookup service's own re-parse.
                let mut named =
                    bsv_rs::transaction::Beef::from_binary(spending_atomic_beef).unwrap();
                assert!(named.is_atomic());
                let submitted = bsv_rs::transaction::Beef::from_binary(&test_beef()).unwrap();
                assert_eq!(
                    named.txs.len(),
                    submitted.txs.len(),
                    "the whole body, never pruned"
                );
                let tip = crate::subject::subject_txid_of(&mut named).unwrap();
                assert_eq!(named.atomic_txid.as_deref(), Some(tip.as_str()));
            }
            other => panic!("Expected OutputSpent::WholeTx, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_spend_notification_mode_txid() {
        let (engine, capture) = make_spend_mode_engine(SpendNotificationMode::Txid).await;

        let beef = test_tagged_beef(vec!["tm_test"]);
        let steak = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        assert_eq!(steak["tm_test"].outputs_to_admit, vec![0]);

        let payloads = capture.lock().unwrap().clone();
        assert_eq!(payloads.len(), 1, "Expected exactly one spend notification");

        match &payloads[0] {
            OutputSpent::Txid {
                txid,
                output_index,
                topic,
                spending_txid,
            } => {
                assert_eq!(txid, PREVIOUS_TXID);
                assert_eq!(*output_index, 0);
                assert_eq!(topic, "tm_test");
                assert_eq!(spending_txid, TEST_TXID);
            }
            other => panic!("Expected OutputSpent::Txid, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_spend_notification_mode_none() {
        let (engine, capture) = make_spend_mode_engine(SpendNotificationMode::None).await;

        let beef = test_tagged_beef(vec!["tm_test"]);
        let steak = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        assert_eq!(steak["tm_test"].outputs_to_admit, vec![0]);

        let payloads = capture.lock().unwrap().clone();
        assert_eq!(payloads.len(), 1, "Expected exactly one spend notification");

        match &payloads[0] {
            OutputSpent::None {
                txid,
                output_index,
                topic,
            } => {
                assert_eq!(txid, PREVIOUS_TXID);
                assert_eq!(*output_index, 0);
                assert_eq!(topic, "tm_test");
            }
            other => panic!("Expected OutputSpent::None, got: {other:?}"),
        }
    }

    // ── GASP Sync Tests ───────────────────────────────────────────────

    #[tokio::test]
    async fn test_start_gasp_sync_empty_config() {
        let engine = Engine::new(
            HashMap::new(),
            HashMap::new(),
            Box::new(MemoryStorage::new()),
            None,
            EngineConfig {
                sync_configuration: HashMap::new(),
                ..Default::default()
            },
        );

        let result = engine.start_gasp_sync().await.unwrap();
        assert!(
            result.topics_synced.is_empty(),
            "No topics should be synced with empty config"
        );
    }

    #[tokio::test]
    async fn test_start_gasp_sync_disabled_topic_skipped() {
        let mut sync_config: SyncConfiguration = HashMap::new();
        sync_config.insert("tm_test".to_string(), SyncTarget::Disabled);

        let engine = Engine::new(
            HashMap::new(),
            HashMap::new(),
            Box::new(MemoryStorage::new()),
            None,
            EngineConfig {
                sync_configuration: sync_config,
                ..Default::default()
            },
        );

        let result = engine.start_gasp_sync().await.unwrap();
        assert!(
            result.topics_synced.is_empty(),
            "Disabled topics should not appear in results"
        );
    }

    #[tokio::test]
    async fn test_start_gasp_sync_peers_config() {
        let mut sync_config: SyncConfiguration = HashMap::new();
        sync_config.insert(
            "tm_test".to_string(),
            SyncTarget::Peers(vec![
                "https://peer1.example.com".to_string(),
                "https://peer2.example.com".to_string(),
            ]),
        );

        let engine = Engine::new(
            HashMap::new(),
            HashMap::new(),
            Box::new(MemoryStorage::new()),
            None,
            EngineConfig {
                sync_configuration: sync_config,
                ..Default::default()
            },
        );

        let result = engine.start_gasp_sync().await.unwrap();
        assert_eq!(result.topics_synced.len(), 1);

        let topic_result = &result.topics_synced["tm_test"];
        assert_eq!(topic_result.peers.len(), 2);
        assert_eq!(topic_result.sync_type, "peers");
        assert!(topic_result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_start_gasp_sync_peers_filters_self() {
        let mut sync_config: SyncConfiguration = HashMap::new();
        sync_config.insert(
            "tm_test".to_string(),
            SyncTarget::Peers(vec![
                "https://self.example.com".to_string(),
                "https://peer2.example.com".to_string(),
            ]),
        );

        let engine = Engine::new(
            HashMap::new(),
            HashMap::new(),
            Box::new(MemoryStorage::new()),
            None,
            EngineConfig {
                hosting_url: Some("https://self.example.com".to_string()),
                sync_configuration: sync_config,
                ..Default::default()
            },
        );

        let result = engine.start_gasp_sync().await.unwrap();
        let topic_result = &result.topics_synced["tm_test"];
        assert_eq!(
            topic_result.peers.len(),
            1,
            "Self URL should be filtered out"
        );
        assert_eq!(topic_result.peers[0], "https://peer2.example.com");
    }

    #[tokio::test]
    async fn test_start_gasp_sync_ship_discovers_peers() {
        let broadcaster = MockBroadcaster::new();
        let (engine, _calls) = make_engine_with_broadcaster(
            vec![0],
            broadcaster,
            vec![
                ("https://peer1.example.com", "tm_test"),
                ("https://peer2.example.com", "tm_test"),
            ],
            Some("https://self.example.com"),
        )
        .await;

        let result = engine.start_gasp_sync().await.unwrap();
        assert!(
            result.topics_synced.contains_key("tm_test"),
            "tm_test should be in sync results"
        );

        let topic_result = &result.topics_synced["tm_test"];
        assert_eq!(topic_result.sync_type, "ship");
        assert_eq!(
            topic_result.peers.len(),
            2,
            "Should discover both SHIP peers"
        );
        assert!(topic_result
            .peers
            .contains(&"https://peer1.example.com".to_string()));
        assert!(topic_result
            .peers
            .contains(&"https://peer2.example.com".to_string()));
    }

    #[tokio::test]
    async fn test_start_gasp_sync_ship_filters_self() {
        let broadcaster = MockBroadcaster::new();
        let (engine, _calls) = make_engine_with_broadcaster(
            vec![0],
            broadcaster,
            vec![
                ("https://self.example.com", "tm_test"),
                ("https://peer2.example.com", "tm_test"),
            ],
            Some("https://self.example.com"),
        )
        .await;

        let result = engine.start_gasp_sync().await.unwrap();
        let topic_result = &result.topics_synced["tm_test"];
        assert_eq!(
            topic_result.peers.len(),
            1,
            "Self URL should be filtered out from SHIP discovery"
        );
        assert_eq!(topic_result.peers[0], "https://peer2.example.com");
    }

    #[tokio::test]
    async fn test_start_gasp_sync_ship_no_ls_ship_returns_empty() {
        let mut managers: HashMap<String, Box<dyn TopicManagerTrait>> = HashMap::new();
        managers.insert(
            "tm_custom".to_string(),
            Box::new(MockTopicManager::admitting(vec![])),
        );

        let mut sync_config: SyncConfiguration = HashMap::new();
        sync_config.insert("tm_custom".to_string(), SyncTarget::Ship);

        let engine = Engine::new(
            managers,
            HashMap::new(),
            Box::new(MemoryStorage::new()),
            None,
            EngineConfig {
                sync_configuration: sync_config,
                ..Default::default()
            },
        );

        let result = engine.start_gasp_sync().await.unwrap();
        let topic_result = &result.topics_synced["tm_custom"];
        assert!(
            topic_result.peers.is_empty(),
            "Without ls_ship, SHIP discovery should return no peers"
        );
    }

    #[tokio::test]
    async fn test_start_gasp_sync_mixed_config() {
        let mut managers: HashMap<String, Box<dyn TopicManagerTrait>> = HashMap::new();
        managers.insert(
            "tm_peers".to_string(),
            Box::new(MockTopicManager::admitting(vec![])),
        );
        managers.insert(
            "tm_disabled".to_string(),
            Box::new(MockTopicManager::admitting(vec![])),
        );
        managers.insert(
            "tm_ship_topic".to_string(),
            Box::new(MockTopicManager::admitting(vec![])),
        );

        let mut sync_config: SyncConfiguration = HashMap::new();
        sync_config.insert(
            "tm_peers".to_string(),
            SyncTarget::Peers(vec!["https://peer.com".to_string()]),
        );
        sync_config.insert("tm_disabled".to_string(), SyncTarget::Disabled);
        sync_config.insert("tm_ship_topic".to_string(), SyncTarget::Ship);

        let engine = Engine::new(
            managers,
            HashMap::new(),
            Box::new(MemoryStorage::new()),
            None,
            EngineConfig {
                sync_configuration: sync_config,
                ..Default::default()
            },
        );

        let result = engine.start_gasp_sync().await.unwrap();
        assert_eq!(result.topics_synced.len(), 2);
        assert!(result.topics_synced.contains_key("tm_peers"));
        assert!(result.topics_synced.contains_key("tm_ship_topic"));
        assert!(!result.topics_synced.contains_key("tm_disabled"));

        assert_eq!(result.topics_synced["tm_peers"].peers.len(), 1);
        assert_eq!(result.topics_synced["tm_peers"].sync_type, "peers");
        assert_eq!(result.topics_synced["tm_ship_topic"].sync_type, "ship");
    }

    #[tokio::test]
    async fn test_gasp_sync_result_serialization() {
        let mut topics_synced = HashMap::new();
        topics_synced.insert(
            "tm_test".to_string(),
            super::TopicSyncResult {
                peers: vec!["https://peer.com".to_string()],
                sync_type: "ship".to_string(),
                errors: vec![],
                pruned_inputs: 0,
                discarded_graphs: 0,
                finalized_graphs: 0,
                deadline_dropped_graphs: 0,
                cursor_moves: Vec::new(),
                deferred_graphs: 0,
                resumed_graphs: 0,
                converged_graphs: 0,
                dropped_graphs: Vec::new(),
                stalled_graphs: 0,
                held_back_graphs: 0,
            },
        );

        let result = super::GASPSyncResult { topics_synced };
        let json = serde_json::to_string(&result).unwrap();
        let back: super::GASPSyncResult = serde_json::from_str(&json).unwrap();

        assert_eq!(back.topics_synced.len(), 1);
        assert_eq!(back.topics_synced["tm_test"].peers.len(), 1);
        assert_eq!(back.topics_synced["tm_test"].sync_type, "ship");
    }

    // ── GASP Sync With Factory Tests ─────────────────────────────────

    /// Mock GASPRemote for factory tests
    struct MockSyncRemote {
        utxos: Vec<crate::types::GASPOutput>,
    }

    #[async_trait(?Send)]
    impl crate::gasp::GASPRemote for MockSyncRemote {
        async fn get_initial_response(
            &self,
            request: &crate::types::GASPInitialRequest,
        ) -> Result<crate::types::GASPInitialResponse, crate::gasp::GASPError> {
            let utxos: Vec<crate::types::GASPOutput> = self
                .utxos
                .iter()
                .filter(|u| u.score as u64 >= request.since)
                .cloned()
                .collect();
            Ok(crate::types::GASPInitialResponse {
                utxo_list: utxos,
                since: request.since,
            })
        }
        async fn get_initial_reply(
            &self,
            _: &crate::types::GASPInitialResponse,
        ) -> Result<crate::types::GASPInitialReply, crate::gasp::GASPError> {
            Ok(crate::types::GASPInitialReply {
                utxo_list: Vec::new(),
            })
        }
        async fn request_node(
            &self,
            graph_id: &str,
            txid: &str,
            output_index: u32,
            _: bool,
        ) -> Result<crate::types::GASPNode, crate::gasp::GASPError> {
            Ok(crate::types::GASPNode {
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
            _: &crate::types::GASPNode,
        ) -> Result<Option<crate::types::GASPNodeResponse>, crate::gasp::GASPError> {
            Ok(None)
        }
    }

    /// Mock factory that creates MockSyncRemote instances
    struct MockGASPRemoteFactory;

    impl crate::gasp::GASPRemoteFactory for MockGASPRemoteFactory {
        fn create_remote(&self, _peer_url: &str, _topic: &str) -> Box<dyn crate::gasp::GASPRemote> {
            Box::new(MockSyncRemote {
                utxos: vec![
                    crate::types::GASPOutput {
                        txid: "remote_tx1".to_string(),
                        output_index: 0,
                        score: 100.0,
                    },
                    crate::types::GASPOutput {
                        txid: "remote_tx2".to_string(),
                        output_index: 0,
                        score: 200.0,
                    },
                ],
            })
        }
    }

    #[tokio::test]
    async fn test_start_gasp_sync_with_factory_runs_sync() {
        let mut sync_config: SyncConfiguration = HashMap::new();
        sync_config.insert(
            "tm_test".to_string(),
            SyncTarget::Peers(vec!["https://peer1.example.com".to_string()]),
        );

        let mut engine = Engine::new(
            HashMap::new(),
            HashMap::new(),
            Box::new(MemoryStorage::new()),
            None,
            EngineConfig {
                sync_configuration: sync_config,
                ..Default::default()
            },
        );

        engine.set_gasp_remote_factory(Box::new(MockGASPRemoteFactory));

        let result = engine.start_gasp_sync().await.unwrap();
        assert_eq!(result.topics_synced.len(), 1);

        let topic_result = &result.topics_synced["tm_test"];
        assert_eq!(topic_result.peers.len(), 1);
        assert!(
            topic_result.errors.is_empty(),
            "Sync should succeed with mock remote"
        );

        // Verify last_interaction was persisted
        let last = engine
            .storage()
            .get_last_interaction("https://peer1.example.com", "tm_test")
            .await
            .unwrap();
        // The mock remote serves synthetic `rawtx_*` nodes that cannot assemble
        // into a real BEEF, so both graphs fail to INGEST (a transient-class
        // error). The cursor is therefore capped at 99 — strictly below the
        // lowest un-ingested score (remote_tx1@100) — so the next sync re-pulls
        // them rather than skipping past. (Pre-#43 this advanced to 200, the
        // highest *seen* score, silently stranding both graphs.)
        assert_eq!(
            last, 99,
            "cursor must cap below the lowest un-ingested score, not skip to the seen tip"
        );
    }

    #[tokio::test]
    async fn test_start_gasp_sync_with_factory_handles_error() {
        struct FailingFactory;

        impl crate::gasp::GASPRemoteFactory for FailingFactory {
            fn create_remote(
                &self,
                _peer_url: &str,
                _topic: &str,
            ) -> Box<dyn crate::gasp::GASPRemote> {
                struct FailingRemote;
                #[async_trait(?Send)]
                impl crate::gasp::GASPRemote for FailingRemote {
                    async fn get_initial_response(
                        &self,
                        _: &crate::types::GASPInitialRequest,
                    ) -> Result<crate::types::GASPInitialResponse, crate::gasp::GASPError>
                    {
                        Err(crate::gasp::GASPError::RemoteError(
                            "connection refused".into(),
                        ))
                    }
                    async fn get_initial_reply(
                        &self,
                        _: &crate::types::GASPInitialResponse,
                    ) -> Result<crate::types::GASPInitialReply, crate::gasp::GASPError>
                    {
                        unreachable!()
                    }
                    async fn request_node(
                        &self,
                        _: &str,
                        _: &str,
                        _: u32,
                        _: bool,
                    ) -> Result<crate::types::GASPNode, crate::gasp::GASPError>
                    {
                        unreachable!()
                    }
                    async fn submit_node(
                        &self,
                        _: &crate::types::GASPNode,
                    ) -> Result<Option<crate::types::GASPNodeResponse>, crate::gasp::GASPError>
                    {
                        unreachable!()
                    }
                }
                Box::new(FailingRemote)
            }
        }

        let mut sync_config: SyncConfiguration = HashMap::new();
        sync_config.insert(
            "tm_test".to_string(),
            SyncTarget::Peers(vec!["https://bad-peer.example.com".to_string()]),
        );

        let mut engine = Engine::new(
            HashMap::new(),
            HashMap::new(),
            Box::new(MemoryStorage::new()),
            None,
            EngineConfig {
                sync_configuration: sync_config,
                ..Default::default()
            },
        );

        engine.set_gasp_remote_factory(Box::new(FailingFactory));

        let result = engine.start_gasp_sync().await.unwrap();
        assert_eq!(result.topics_synced.len(), 1);

        let topic_result = &result.topics_synced["tm_test"];
        assert_eq!(
            topic_result.errors.len(),
            1,
            "Should have one error for the failing peer"
        );
        assert!(topic_result.errors[0].contains("connection refused"));
    }

    #[tokio::test]
    async fn test_start_gasp_sync_without_factory_still_discovers_peers() {
        let mut sync_config: SyncConfiguration = HashMap::new();
        sync_config.insert(
            "tm_test".to_string(),
            SyncTarget::Peers(vec!["https://peer.example.com".to_string()]),
        );

        // No factory set — should still work (just peer discovery, no sync)
        let engine = Engine::new(
            HashMap::new(),
            HashMap::new(),
            Box::new(MemoryStorage::new()),
            None,
            EngineConfig {
                sync_configuration: sync_config,
                ..Default::default()
            },
        );

        let result = engine.start_gasp_sync().await.unwrap();
        assert_eq!(result.topics_synced.len(), 1);

        let topic_result = &result.topics_synced["tm_test"];
        assert_eq!(topic_result.peers.len(), 1);
        assert!(topic_result.errors.is_empty());
    }

    #[tokio::test]
    async fn test_start_gasp_sync_with_factory_multiple_peers() {
        let mut sync_config: SyncConfiguration = HashMap::new();
        sync_config.insert(
            "tm_test".to_string(),
            SyncTarget::Peers(vec![
                "https://peer1.example.com".to_string(),
                "https://peer2.example.com".to_string(),
            ]),
        );

        let mut engine = Engine::new(
            HashMap::new(),
            HashMap::new(),
            Box::new(MemoryStorage::new()),
            None,
            EngineConfig {
                sync_configuration: sync_config,
                ..Default::default()
            },
        );

        engine.set_gasp_remote_factory(Box::new(MockGASPRemoteFactory));

        let result = engine.start_gasp_sync().await.unwrap();
        let topic_result = &result.topics_synced["tm_test"];
        assert_eq!(topic_result.peers.len(), 2);
        assert!(topic_result.errors.is_empty());

        // Both peers should have their last_interaction updated
        let last1 = engine
            .storage()
            .get_last_interaction("https://peer1.example.com", "tm_test")
            .await
            .unwrap();
        let last2 = engine
            .storage()
            .get_last_interaction("https://peer2.example.com", "tm_test")
            .await
            .unwrap();
        // Capped at 99 (below the lowest un-ingested score@100), not 200 — the
        // synthetic mock graphs fail to ingest, so the cursor must not skip past
        // them. See `test_start_gasp_sync_with_factory_runs_sync` (#43 gap-guard).
        assert_eq!(last1, 99);
        assert_eq!(last2, 99);
    }

    // ── Per-peer budget + dead-peer quarantine (bsv-low#302) ──────────

    /// A remote whose initial request HANGS forever — models the dead
    /// ephemeral peers (ngrok tunnels) whose fetch never returns, the #257
    /// root cause the per-peer budget exists for.
    struct HangingRemote;

    #[async_trait(?Send)]
    impl crate::gasp::GASPRemote for HangingRemote {
        async fn get_initial_response(
            &self,
            _: &crate::types::GASPInitialRequest,
        ) -> Result<crate::types::GASPInitialResponse, crate::gasp::GASPError> {
            std::future::pending::<()>().await;
            unreachable!()
        }
        async fn get_initial_reply(
            &self,
            _: &crate::types::GASPInitialResponse,
        ) -> Result<crate::types::GASPInitialReply, crate::gasp::GASPError> {
            unreachable!()
        }
        async fn request_node(
            &self,
            _: &str,
            _: &str,
            _: u32,
            _: bool,
        ) -> Result<crate::types::GASPNode, crate::gasp::GASPError> {
            unreachable!()
        }
        async fn submit_node(
            &self,
            _: &crate::types::GASPNode,
        ) -> Result<Option<crate::types::GASPNodeResponse>, crate::gasp::GASPError> {
            unreachable!()
        }
    }

    /// Factory that HANGS for `hang.example.com` peers and serves the normal
    /// mock sync remote for everyone else, counting `create_remote` calls
    /// (a quarantine-skipped peer never reaches the factory).
    struct SelectiveHangFactory {
        created: std::rc::Rc<std::cell::RefCell<Vec<String>>>,
    }

    impl crate::gasp::GASPRemoteFactory for SelectiveHangFactory {
        fn create_remote(&self, peer_url: &str, _topic: &str) -> Box<dyn crate::gasp::GASPRemote> {
            self.created.borrow_mut().push(peer_url.to_string());
            if peer_url.contains("hang.example.com") {
                Box::new(HangingRemote)
            } else {
                Box::new(MockSyncRemote {
                    utxos: vec![crate::types::GASPOutput {
                        txid: "remote_tx1".to_string(),
                        output_index: 0,
                        score: 100.0,
                    }],
                })
            }
        }
    }

    /// The deterministic "instant deadline" sleep factory: `race_or_deadline`
    /// polls the sync future FIRST, so an in-memory sync that never yields
    /// still completes; only a future that actually returns Pending (the
    /// hanging remote) loses the race.
    fn instant_deadline() -> crate::engine::SleepFactory {
        std::rc::Rc::new(|_ms| Box::pin(std::future::ready(())))
    }

    fn two_peer_engine(store: std::rc::Rc<MemoryStorage>) -> Engine {
        let mut sync_config: SyncConfiguration = HashMap::new();
        sync_config.insert(
            "tm_test".to_string(),
            SyncTarget::Peers(vec![
                "https://hang.example.com".to_string(),
                "https://good.example.com".to_string(),
            ]),
        );
        Engine::new(
            HashMap::new(),
            HashMap::new(),
            Box::new(store),
            None,
            EngineConfig {
                sync_configuration: sync_config,
                ..Default::default()
            },
        )
    }

    #[tokio::test]
    async fn peer_budget_timeout_skips_dead_peer_continues_loop_and_never_advances_cursor() {
        let store = std::rc::Rc::new(MemoryStorage::new());
        let created = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        let mut engine = two_peer_engine(store.clone());
        engine.set_gasp_remote_factory(Box::new(SelectiveHangFactory {
            created: created.clone(),
        }));
        engine.set_peer_sync_budget(instant_deadline(), 1);

        let result = engine.start_gasp_sync().await.unwrap();
        let topic_result = &result.topics_synced["tm_test"];

        // The hanging peer was dropped LOUDLY (an error entry) and the loop
        // CONTINUED: the good peer was still attempted and completed.
        assert_eq!(topic_result.errors.len(), 1, "one budget-exceeded error");
        assert!(
            topic_result.errors[0].contains("budget"),
            "{:?}",
            topic_result.errors
        );
        assert_eq!(
            created.borrow().as_slice(),
            ["https://hang.example.com", "https://good.example.com"],
            "both peers attempted this tick"
        );

        // Cursor semantics (the #257 gate property): a timed-out peer's
        // cursor NEVER advances; the completed peer's does.
        let hang_cursor = store
            .get_last_interaction("https://hang.example.com", "tm_test")
            .await
            .unwrap();
        assert_eq!(hang_cursor, 0, "timeout must not advance the sync cursor");
        let good_cursor = store
            .get_last_interaction("https://good.example.com", "tm_test")
            .await
            .unwrap();
        assert!(good_cursor > 0, "completed peer's cursor advances");

        // Health bookkeeping: timeout = failure, completion = success.
        let hang_health = store
            .get_peer_sync_health("https://hang.example.com", "tm_test")
            .await
            .unwrap();
        assert_eq!(hang_health.consecutive_failures, 1);
        let good_health = store
            .get_peer_sync_health("https://good.example.com", "tm_test")
            .await
            .unwrap();
        assert_eq!(good_health.consecutive_failures, 0);
    }

    #[tokio::test]
    async fn peer_quarantined_after_threshold_reprobed_after_window_readmitted_on_success() {
        let store = std::rc::Rc::new(MemoryStorage::new());
        let created = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));

        let mut sync_config: SyncConfiguration = HashMap::new();
        sync_config.insert(
            "tm_test".to_string(),
            SyncTarget::Peers(vec!["https://hang.example.com".to_string()]),
        );
        let mut engine = Engine::new(
            HashMap::new(),
            HashMap::new(),
            Box::new(store.clone()),
            None,
            EngineConfig {
                sync_configuration: sync_config,
                ..Default::default()
            },
        );
        engine.set_gasp_remote_factory(Box::new(SelectiveHangFactory {
            created: created.clone(),
        }));
        engine.set_peer_sync_budget(instant_deadline(), 1);

        // Fail the peer up to (but not past) the quarantine threshold — every
        // tick attempts it (advance the clock between ticks so the attempts
        // are distinguishable in age).
        for i in 0..crate::gasp::PEER_QUARANTINE_THRESHOLD {
            engine.start_gasp_sync().await.unwrap();
            store.advance_clock(900);
            assert_eq!(
                created.borrow().len() as u64,
                i + 1,
                "peer attempted while below the threshold"
            );
        }
        let health = store
            .get_peer_sync_health("https://hang.example.com", "tm_test")
            .await
            .unwrap();
        assert_eq!(
            health.consecutive_failures,
            crate::gasp::PEER_QUARANTINE_THRESHOLD
        );

        // Next tick: QUARANTINED — the factory is never consulted, no error
        // is emitted, and the attempt count does not grow (a skip is not an
        // attempt, so the re-probe window keeps aging).
        let result = engine.start_gasp_sync().await.unwrap();
        assert!(result.topics_synced["tm_test"].errors.is_empty());
        assert_eq!(
            created.borrow().len() as u64,
            crate::gasp::PEER_QUARANTINE_THRESHOLD,
            "quarantined peer must be skipped"
        );

        // Age past the re-probe window: exactly one fresh probe is allowed.
        store.advance_clock(crate::gasp::PEER_QUARANTINE_REPROBE_SECS);
        engine.start_gasp_sync().await.unwrap();
        assert_eq!(
            created.borrow().len() as u64,
            crate::gasp::PEER_QUARANTINE_THRESHOLD + 1,
            "re-probe attempted after the window"
        );
        // The probe failed again → failure count kept growing → re-armed.
        let health = store
            .get_peer_sync_health("https://hang.example.com", "tm_test")
            .await
            .unwrap();
        assert_eq!(
            health.consecutive_failures,
            crate::gasp::PEER_QUARANTINE_THRESHOLD + 1
        );
        let result = engine.start_gasp_sync().await.unwrap();
        assert!(
            result.topics_synced["tm_test"].errors.is_empty(),
            "re-quarantined"
        );

        // The peer comes back to life: age to the next re-probe, then answer
        // like a healthy peer → SUCCESS resets the count to 0 (full
        // re-admission — quarantine is never a deletion).
        store.advance_clock(crate::gasp::PEER_QUARANTINE_REPROBE_SECS);
        // Swap the factory for one that serves the healthy mock for this URL.
        engine.set_gasp_remote_factory(Box::new(MockGASPRemoteFactory));
        let result = engine.start_gasp_sync().await.unwrap();
        assert!(result.topics_synced["tm_test"].errors.is_empty());
        let health = store
            .get_peer_sync_health("https://hang.example.com", "tm_test")
            .await
            .unwrap();
        assert_eq!(health.consecutive_failures, 0, "success fully re-admits");
    }

    // ── Mock ARC Broadcaster ─────────────────────────────────────────

    use crate::broadcaster::ArcBroadcaster;

    struct MockArcBroadcaster {
        calls: Arc<Mutex<Vec<String>>>,
        should_fail: bool,
    }

    impl MockArcBroadcaster {
        fn new() -> Self {
            Self {
                calls: Arc::new(Mutex::new(Vec::new())),
                should_fail: false,
            }
        }

        fn failing() -> Self {
            Self {
                calls: Arc::new(Mutex::new(Vec::new())),
                should_fail: true,
            }
        }
    }

    #[async_trait(?Send)]
    impl ArcBroadcaster for MockArcBroadcaster {
        async fn broadcast(&self, raw_tx_hex: &str) -> Result<String, String> {
            self.calls.lock().unwrap().push(raw_tx_hex.to_string());
            if self.should_fail {
                Err("mock ARC failure".to_string())
            } else {
                Ok("mock_txid_from_arc".to_string())
            }
        }
    }

    /// Create engine with an ARC broadcaster (and optionally a SHIP broadcaster).
    fn make_engine_with_arc(
        admit_indices: Vec<u32>,
        arc: MockArcBroadcaster,
    ) -> (Engine, Arc<Mutex<Vec<String>>>) {
        let arc_calls = arc.calls.clone();

        let mut managers: HashMap<String, Box<dyn TopicManagerTrait>> = HashMap::new();
        managers.insert(
            "tm_test".to_string(),
            Box::new(MockTopicManager::admitting(admit_indices)),
        );

        let mut lookup_services: HashMap<String, Box<dyn LookupServiceTrait>> = HashMap::new();
        lookup_services.insert("ls_test".to_string(), Box::new(MockLookupService::new()));

        let storage = Box::new(MemoryStorage::new());

        let engine = Engine::with_all(
            managers,
            lookup_services,
            storage,
            None,
            None,
            Some(Box::new(arc)),
            None,
            EngineConfig::default(),
        );

        (engine, arc_calls)
    }

    // ── ARC Broadcaster Tests ────────────────────────────────────────

    #[tokio::test]
    async fn test_arc_broadcaster_called_on_current_tx() {
        let arc = MockArcBroadcaster::new();
        let (engine, arc_calls) = make_engine_with_arc(vec![0], arc);

        let beef = test_tagged_beef(vec!["tm_test"]);
        let steak = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        assert!(!steak["tm_test"].outputs_to_admit.is_empty());

        let calls = arc_calls.lock().unwrap();
        assert_eq!(
            calls.len(),
            1,
            "ARC broadcaster should be called once for CurrentTx"
        );
        assert!(
            !calls[0].is_empty(),
            "ARC should receive non-empty raw tx hex"
        );
    }

    #[tokio::test]
    async fn test_arc_broadcaster_not_called_on_historical_tx() {
        let arc = MockArcBroadcaster::new();
        let (engine, arc_calls) = make_engine_with_arc(vec![0], arc);

        let beef = test_tagged_beef(vec!["tm_test"]);
        engine
            .submit(&beef, SubmitMode::HistoricalTx)
            .await
            .unwrap();

        assert_eq!(
            arc_calls.lock().unwrap().len(),
            0,
            "ARC broadcaster should NOT be called for HistoricalTx"
        );
    }

    #[tokio::test]
    async fn test_arc_broadcaster_not_called_on_historical_tx_no_spv() {
        let arc = MockArcBroadcaster::new();
        let (engine, arc_calls) = make_engine_with_arc(vec![0], arc);

        let beef = test_tagged_beef(vec!["tm_test"]);
        engine
            .submit(&beef, SubmitMode::HistoricalTxNoSpv)
            .await
            .unwrap();

        assert_eq!(
            arc_calls.lock().unwrap().len(),
            0,
            "ARC broadcaster should NOT be called for HistoricalTxNoSpv"
        );
    }

    #[tokio::test]
    async fn test_arc_broadcast_failure_does_not_fail_submit() {
        let arc = MockArcBroadcaster::failing();
        let (engine, arc_calls) = make_engine_with_arc(vec![0], arc);

        let beef = test_tagged_beef(vec!["tm_test"]);
        // submit should succeed even when ARC fails
        let steak = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        assert!(!steak["tm_test"].outputs_to_admit.is_empty());

        // ARC was called (and failed), but submit still succeeded
        assert_eq!(
            arc_calls.lock().unwrap().len(),
            1,
            "ARC broadcaster should be attempted even if it will fail"
        );
    }

    #[tokio::test]
    async fn test_arc_broadcaster_not_present_still_works() {
        // Engine without ARC broadcaster — should work fine
        let engine = make_engine(vec![0]);

        let beef = test_tagged_beef(vec!["tm_test"]);
        let steak = engine.submit(&beef, SubmitMode::CurrentTx).await.unwrap();

        assert!(!steak["tm_test"].outputs_to_admit.is_empty());
    }

    // ── Sync advertisements URL validation ─────────────────────────────

    /// Mock Advertiser that tracks created advertisements via shared state.
    struct TrackingAdvertiser {
        created: Arc<Mutex<Vec<AdvertisementData>>>,
        existing_ship: Vec<Advertisement>,
        existing_slap: Vec<Advertisement>,
        /// Topics the returned TaggedBEEF is tagged with — a topic with no
        /// registered manager forces the LOCAL submit to fail (#320 3a);
        /// several topics exercise independent per-topic admission (D1).
        tag_topics: Vec<String>,
        /// Simulate a create_advertisements failure (#320 3a).
        create_fails: bool,
        /// Simulate a find_all_advertisements failure (#320 M2).
        find_fails: bool,
        /// Return the v1 no-op EMPTY TaggedBEEF from revoke (#320 L2).
        revoke_empty: bool,
    }

    impl TrackingAdvertiser {
        fn new() -> (Self, Arc<Mutex<Vec<AdvertisementData>>>) {
            let created = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    created: created.clone(),
                    existing_ship: vec![],
                    existing_slap: vec![],
                    tag_topics: vec!["tm_test".to_string()],
                    create_fails: false,
                    find_fails: false,
                    revoke_empty: false,
                },
                created,
            )
        }
    }

    use crate::advertiser::{Advertiser, AdvertiserError};

    #[async_trait(?Send)]
    impl Advertiser for TrackingAdvertiser {
        async fn create_advertisements(
            &self,
            ads: &[AdvertisementData],
        ) -> Result<TaggedBEEF, AdvertiserError> {
            if self.create_fails {
                return Err(AdvertiserError::CreationFailed(
                    "mock create refused".into(),
                ));
            }
            self.created.lock().unwrap().extend(ads.iter().cloned());
            Ok(TaggedBEEF::new(test_beef(), self.tag_topics.clone()))
        }

        async fn find_all_advertisements(
            &self,
            protocol: Protocol,
        ) -> Result<Vec<Advertisement>, AdvertiserError> {
            if self.find_fails {
                return Err(AdvertiserError::LookupFailed("mock lookup refused".into()));
            }
            match protocol {
                Protocol::Ship => Ok(self.existing_ship.clone()),
                Protocol::Slap => Ok(self.existing_slap.clone()),
            }
        }

        async fn revoke_advertisements(
            &self,
            _ads: &[Advertisement],
        ) -> Result<TaggedBEEF, AdvertiserError> {
            if self.revoke_empty {
                return Ok(TaggedBEEF::new(vec![], vec![]));
            }
            Ok(TaggedBEEF::new(test_beef(), vec!["tm_test".to_string()]))
        }

        fn parse_advertisement(&self, _script: &[u8]) -> Option<Advertisement> {
            None
        }
    }

    /// TS: "Sync advertisements URL validation"
    /// sync_advertisements should be a no-op when hosting_url is empty string,
    /// and should skip suppressed topics (tm_ship/tm_slap) when configured.
    #[tokio::test]
    async fn test_sync_advertisements_url_validation() {
        // Case 1: Empty hosting URL — should return early without creating ads
        let (adv, created) = TrackingAdvertiser::new();

        let mut managers: HashMap<String, Box<dyn TopicManagerTrait>> = HashMap::new();
        managers.insert(
            "tm_test".to_string(),
            Box::new(MockTopicManager::admitting(vec![0])),
        );

        let engine = Engine::new(
            managers,
            HashMap::new(),
            Box::new(MemoryStorage::new()),
            Some(Box::new(adv)),
            EngineConfig {
                hosting_url: Some(String::new()), // empty string — invalid
                ..Default::default()
            },
        );

        engine.sync_advertisements().await.unwrap();
        assert!(
            created.lock().unwrap().is_empty(),
            "Empty hosting URL should skip advertisement creation"
        );

        // Case 2: Valid hosting URL — should create SHIP advertisements for non-suppressed topics
        let (adv2, created2) = TrackingAdvertiser::new();

        let mut managers2: HashMap<String, Box<dyn TopicManagerTrait>> = HashMap::new();
        managers2.insert(
            "tm_test".to_string(),
            Box::new(MockTopicManager::admitting(vec![0])),
        );
        // Also register tm_ship to verify it gets suppressed
        managers2.insert(
            "tm_ship".to_string(),
            Box::new(MockTopicManager::admitting(vec![])),
        );

        let engine2 = Engine::new(
            managers2,
            HashMap::new(),
            Box::new(MemoryStorage::new()),
            Some(Box::new(adv2)),
            EngineConfig {
                hosting_url: Some("https://valid.example.com".to_string()),
                suppress_default_sync_advertisements: true,
                ..Default::default()
            },
        );

        engine2.sync_advertisements().await.unwrap();
        let ads = created2.lock().unwrap();
        // tm_ship should be suppressed, only tm_test should get an advertisement
        assert!(
            ads.iter().all(|a| a.topic_or_service_name != "tm_ship"),
            "tm_ship should be suppressed when suppress_default_sync_advertisements is true"
        );
        assert!(
            ads.iter().any(|a| a.topic_or_service_name == "tm_test"),
            "tm_test should get a SHIP advertisement"
        );
    }

    // ── sync_advertisements report (bsv-low #320 defect 3a) ────────────
    //
    // Pre-#320 the engine returned `Ok(())` unconditionally: a failed
    // create or a failed LOCAL submit was `error!`-logged and invisible to
    // the caller — the admin route said `success` while the node's own
    // ls_ship/ls_slap never gained its ads, so every cycle re-created the
    // whole set. These cells pin the report contract.

    fn sync_test_engine(adv: TrackingAdvertiser) -> Engine {
        let mut managers: HashMap<String, Box<dyn TopicManagerTrait>> = HashMap::new();
        managers.insert(
            "tm_test".to_string(),
            Box::new(MockTopicManager::admitting(vec![0])),
        );
        Engine::new(
            managers,
            HashMap::new(),
            Box::new(MemoryStorage::new()),
            Some(Box::new(adv)),
            EngineConfig {
                hosting_url: Some("https://valid.example.com".to_string()),
                suppress_default_sync_advertisements: true,
                ..Default::default()
            },
        )
    }

    #[tokio::test]
    async fn sync_report_counts_admitted_outputs_on_success() {
        let (adv, _) = TrackingAdvertiser::new();
        let report = sync_test_engine(adv).sync_advertisements().await.unwrap();
        assert!(report.ok(), "clean run must report ok: {report:?}");
        assert_eq!(report.to_create, 1, "tm_test was missing");
        assert_eq!(
            report.admitted.get("tm_test"),
            Some(&1),
            "the STEAK's admitted count must be surfaced: {report:?}"
        );
    }

    #[tokio::test]
    async fn sync_report_surfaces_local_submit_failure() {
        // The TaggedBEEF comes back tagged for a topic with no registered
        // manager → the LOCAL submit fails. That failure must be in the
        // report, not swallowed (the live silent-failure shape).
        let (mut adv, _) = TrackingAdvertiser::new();
        adv.tag_topics = vec!["tm_unregistered".to_string()];
        let report = sync_test_engine(adv).sync_advertisements().await.unwrap();
        assert!(!report.ok(), "a failed local submit must not report ok");
        assert_eq!(report.to_create, 1);
        let submit_error = report.submit_error.as_deref().unwrap_or_default();
        assert!(
            submit_error.contains("tm_unregistered"),
            "verbatim engine error expected, got: {submit_error}"
        );
        assert!(report.admitted.is_empty());
    }

    #[tokio::test]
    async fn sync_report_surfaces_create_failure() {
        let (mut adv, _) = TrackingAdvertiser::new();
        adv.create_fails = true;
        let report = sync_test_engine(adv).sync_advertisements().await.unwrap();
        assert!(!report.ok());
        assert!(
            report
                .create_error
                .as_deref()
                .unwrap_or_default()
                .contains("mock create refused"),
            "{report:?}"
        );
        assert!(report.submit_error.is_none(), "create failed before submit");
    }

    #[tokio::test]
    async fn sync_report_converged_noop_is_ok_and_creates_nothing() {
        // A node whose own storage already lists its ads must attempt ZERO
        // creates — the convergence direction of #320 defect 3 (live, the
        // missing self-admission made every cycle re-create all 17 ads).
        let (mut adv, created) = TrackingAdvertiser::new();
        adv.existing_ship = vec![Advertisement {
            protocol: Protocol::Ship,
            identity_key: "02aa".to_string(),
            domain: "https://valid.example.com".to_string(),
            topic_or_service: "tm_test".to_string(),
            beef: None,
            output_index: None,
        }];
        let report = sync_test_engine(adv).sync_advertisements().await.unwrap();
        assert!(report.ok(), "{report:?}");
        assert_eq!(report.to_create, 0, "converged ⇒ nothing to create");
        assert!(created.lock().unwrap().is_empty());
        assert!(
            report.effective(),
            "converged-noop is the one zero-admit shape that IS success"
        );
    }

    #[tokio::test]
    async fn sync_report_zero_admit_is_ok_but_not_effective() {
        // #320 M1: the local submit SUCCEEDS but the topic manager admits
        // ZERO outputs — a non-error refusal. The ads never enter our own
        // ls_ship/ls_slap, so the diff re-creates (re-pays) the full set
        // next cycle. `ok()` (errors-only) stays true; `effective()` must
        // refuse, and the route keys success on `effective()`.
        let (adv, _) = TrackingAdvertiser::new();
        let mut managers: HashMap<String, Box<dyn TopicManagerTrait>> = HashMap::new();
        managers.insert(
            "tm_test".to_string(),
            Box::new(MockTopicManager::admitting(vec![])), // refuses everything
        );
        let engine = Engine::new(
            managers,
            HashMap::new(),
            Box::new(MemoryStorage::new()),
            Some(Box::new(adv)),
            EngineConfig {
                hosting_url: Some("https://valid.example.com".to_string()),
                suppress_default_sync_advertisements: true,
                ..Default::default()
            },
        );
        let report = engine.sync_advertisements().await.unwrap();
        assert!(report.ok(), "no ERROR occurred: {report:?}");
        assert_eq!(report.to_create, 1);
        assert_eq!(report.admitted_total(), 0, "{report:?}");
        assert!(
            !report.effective(),
            "zero admitted on a non-empty create must not read as success: {report:?}"
        );
    }

    #[tokio::test]
    async fn sync_report_partial_admit_is_not_effective() {
        // Delta D1: TWO topics in one submitted tx, per-topic admission
        // independent — tm_test admits its output, tm_refuse refuses all.
        // admitted_total (1) > 0 but < to_create (2): the refused ads
        // would be re-created (re-paid) every cycle, so this must NOT
        // read as success.
        let (mut adv, _) = TrackingAdvertiser::new();
        adv.tag_topics = vec!["tm_test".to_string(), "tm_refuse".to_string()];
        let mut managers: HashMap<String, Box<dyn TopicManagerTrait>> = HashMap::new();
        managers.insert(
            "tm_test".to_string(),
            Box::new(MockTopicManager::admitting(vec![0])),
        );
        managers.insert(
            "tm_refuse".to_string(),
            Box::new(MockTopicManager::admitting(vec![])), // refuses everything
        );
        let engine = Engine::new(
            managers,
            HashMap::new(),
            Box::new(MemoryStorage::new()),
            Some(Box::new(adv)),
            EngineConfig {
                hosting_url: Some("https://valid.example.com".to_string()),
                suppress_default_sync_advertisements: true,
                ..Default::default()
            },
        );
        let report = engine.sync_advertisements().await.unwrap();
        assert!(report.ok(), "no ERROR occurred: {report:?}");
        assert_eq!(report.to_create, 2, "{report:?}");
        assert_eq!(report.admitted_total(), 1, "{report:?}");
        assert!(
            !report.effective(),
            "a partial admit must not read as success: {report:?}"
        );
    }

    #[tokio::test]
    async fn sync_report_lookup_failure_refuses_creation() {
        // #320 M2: a transient read failure must never become "current ads
        // = none" and re-create (re-pay) the whole set — creation is
        // REFUSED and the failure lands in the report.
        let (mut adv, created) = TrackingAdvertiser::new();
        adv.find_fails = true;
        let report = sync_test_engine(adv).sync_advertisements().await.unwrap();
        assert!(!report.ok(), "{report:?}");
        assert!(!report.effective());
        assert!(
            report
                .lookup_error
                .as_deref()
                .unwrap_or_default()
                .contains("mock lookup refused"),
            "{report:?}"
        );
        assert_eq!(report.to_create, 0, "creation refused, not attempted");
        assert!(
            created.lock().unwrap().is_empty(),
            "no create call on a blind read"
        );
        assert!(report.submit_error.is_none());
    }

    #[tokio::test]
    async fn sync_report_counts_skipped_revocations() {
        // #320 L2: `to_revoke: N` with a v1 no-op advertiser must be
        // distinguishable from a revoke that actually ran.
        let (mut adv, _) = TrackingAdvertiser::new();
        adv.revoke_empty = true;
        adv.existing_ship = vec![
            Advertisement {
                protocol: Protocol::Ship,
                identity_key: "02aa".to_string(),
                domain: "https://valid.example.com".to_string(),
                topic_or_service: "tm_test".to_string(),
                beef: None,
                output_index: None,
            },
            Advertisement {
                protocol: Protocol::Ship,
                identity_key: "02aa".to_string(),
                domain: "https://valid.example.com".to_string(),
                topic_or_service: "tm_stale".to_string(), // no longer configured
                beef: None,
                output_index: None,
            },
        ];
        let report = sync_test_engine(adv).sync_advertisements().await.unwrap();
        assert!(report.ok(), "{report:?}");
        assert_eq!(report.to_revoke, 1, "tm_stale is stale");
        assert_eq!(
            report.revoke_skipped, 1,
            "the v1 no-op must be visible, not silent: {report:?}"
        );
        assert!(report.revoke_error.is_none());
        assert!(report.revoke_submit_error.is_none());
    }

    // ── complete_missing_proofs (#130) ─────────────────────────────────

    use crate::gasp::{AncestorFetcher, FetchedAncestor, GASPError};
    use std::cell::Cell;

    /// A single raw mainnet tx (no proof) — used to build a proofless BEEF.
    const PROOFLESS_RAW_TX: &str = "0100000001c997a5e56e104102fa209c6a852dd90660a20b2d9c352423edce25857fcd3704000000004847304402204e45e16932b8af514961a1d3a1a25fdf3f4f7732e9d624c6c61548ab5fb8cd410220181522ec8eca07de4860a4acdd12909d831cc56cbbac4622082221a8768d1d0901ffffffff0200ca9a3b00000000434104ae1a62fe09c5f51b13905f07f06b99a2f7159b2225f374cd378d71302fa28414e7aab37397f554a7df5f142c21c1b7303b8a0626f1baded5c72a704f7e6cd84cac00286bee0000000043410411db93e1dcdb8a016b49840f8c53bc1eb68a382e97b1482ecad7b148a6909a5cb2e0eaddfb84ccf9744464f82e160bfa9b8b64f9d4c03f999b8643f656b412a3ac00000000";

    /// Build a proofless single-tx BEEF + return `(beef_bytes, txid)`.
    fn proofless_beef() -> (Vec<u8>, String) {
        use bsv_rs::transaction::{Beef, Transaction};
        let tx = Transaction::from_hex(PROOFLESS_RAW_TX).unwrap();
        let txid = tx.id();
        let mut beef = Beef::new();
        beef.merge_transaction(tx);
        (beef.to_binary(), txid)
    }

    // ── #284: stitching a proof must never rebuild the stored BEEF ─────────

    /// The corruption shape observed live on rust-beta: a stored BEEF whose
    /// SUBJECT is not the last tx in wire order. The old rewrite went through
    /// `Transaction::from_beef(bytes, None)`, whose None arm picks
    /// `txs.last()` — the PARENT here — and `to_beef(true)` then serialized the
    /// parent's world, dropping the subject from its own row. TEST_BEEF_HEX is
    /// exactly a two-tx chain (parent + child spending it), so re-encoding it
    /// child-first reproduces the trigger byte-for-byte.
    fn subject_not_last_beef() -> (Vec<u8>, String, String) {
        use bsv_rs::transaction::Beef;
        let parsed = Beef::from_binary(&test_beef()).unwrap();
        let parent = parsed.txs[0].tx().unwrap().clone();
        let child = parsed.txs[1].tx().unwrap().clone();
        let (parent_id, child_id) = (parent.id(), child.id());
        assert_ne!(parent_id, child_id);

        // Serialize via to_writer, NOT to_binary: to_binary re-sorts
        // parents-first (subject last), which is precisely why bsv_rs's own
        // output never triggers the bug. The corrupt rows held WALLET
        // serialized bytes stored verbatim at admit time, whose wire order is
        // whatever the wallet emitted — model that by writing the child first.
        let mut beef = Beef::new();
        beef.merge_transaction(child); // subject FIRST — the trigger
        beef.merge_transaction(parent);
        let mut w = bsv_rs::primitives::encoding::Writer::new();
        beef.to_writer(&mut w);
        let bytes = w.into_bytes();

        // The premise the whole test rests on: wire order preserved, subject
        // genuinely not last.
        let reread = Beef::from_binary(&bytes).unwrap();
        assert_eq!(reread.txs.last().unwrap().txid(), parent_id);
        (bytes, child_id, parent_id)
    }

    #[test]
    fn stitch_preserves_the_subject_when_it_is_not_last() {
        use bsv_rs::transaction::{Beef, MerklePath};
        let (stored, child_id, parent_id) = subject_not_last_beef();
        let proof = MerklePath::from_hex(&single_leaf_bump_hex(&child_id, 900_000)).unwrap();

        let out = Engine::stitch_proof_into_stored_beef(&stored, &child_id, &proof)
            .expect("stitch must succeed on a beef that contains the txid");

        let after = Beef::from_binary(&out).unwrap();
        assert!(
            after.find_txid(&child_id).is_some(),
            "the SUBJECT must survive its own proof-completion"
        );
        assert!(
            after.find_txid(&parent_id).is_some(),
            "the ancestry must survive too"
        );
        assert!(
            after.find_bump(&child_id).is_some(),
            "the stitched bump must prove the subject"
        );
        assert!(
            after.find_bump(&parent_id).is_none(),
            "the parent must NOT be claimed by the child's bump"
        );
    }

    /// The old path, reproduced against the same input, to pin WHY the new one
    /// exists: it demonstrably drops the subject. If bsv_rs ever changes
    /// `from_beef(_, None)` to honor the subject, this starts failing and the
    /// comment trail can be revisited — until then it documents the hazard.
    #[test]
    fn the_old_round_trip_demonstrably_dropped_the_subject() {
        use bsv_rs::transaction::{Beef, Transaction};
        let (stored, child_id, _parent_id) = subject_not_last_beef();
        let tx = Transaction::from_beef(&stored, None).unwrap();
        assert_ne!(
            tx.id(),
            child_id,
            "from_beef(None) picks txs.last(), not the subject — the trigger"
        );
        let rebuilt = tx.to_beef(true).unwrap();
        let reparsed = Beef::from_binary(&rebuilt).unwrap();
        assert!(
            reparsed.find_txid(&child_id).is_none(),
            "the rebuild loses the subject — exactly the #284 artifact"
        );
    }

    /// bsv-low M19 R2 round 3 (review HIGH-1): a stored BEEF whose SUBJECT
    /// sits on a stale (orphan) bump at height H, re-stitched with a
    /// same-height DIFFERENT-root proof, must serialize with the subject on
    /// the NEW bump and the orphan bump dropped — otherwise the hop leg
    /// churns and re-latches the orphan proof forever.
    #[test]
    fn stitch_reanchors_a_subject_off_a_same_height_orphan_bump() {
        use bsv_rs::transaction::{Beef, BeefTx, MerklePath, MerklePathLeaf, Transaction};
        // subject spends a real parent; both are raw in the BEEF
        let parent = Transaction::from_hex(RAW_PARENT).unwrap();
        let parent_id = parent.id();
        let subject = child_of(&parent_id, 0);
        let subject_id = subject.id();
        // the ORPHAN bump: a two-leaf block at H so its root is NOT the txid,
        // and it differs from the canonical block's root
        let orphan = MerklePath::new_unchecked(
            965_771,
            vec![vec![
                MerklePathLeaf::new_txid(0, subject_id.clone()),
                MerklePathLeaf::new(1, "aa".repeat(32)),
            ]],
        )
        .unwrap();
        let mut beef = Beef::new();
        let bi = beef.merge_bump(orphan.clone());
        beef.merge_raw_tx(parent.to_binary(), None);
        beef.merge_raw_tx(subject.to_binary(), Some(bi));
        let orphan_root = orphan.compute_root(Some(&subject_id)).unwrap();
        let stored = beef.to_binary();
        assert_eq!(
            Beef::from_binary(&stored)
                .unwrap()
                .find_bump(&subject_id)
                .map(|b| b.compute_root(Some(&subject_id)).unwrap()),
            Some(orphan_root.clone())
        );
        // the canonical proof at the SAME height, a DIFFERENT root
        let canonical = MerklePath::new_unchecked(
            965_771,
            vec![vec![
                MerklePathLeaf::new_txid(0, subject_id.clone()),
                MerklePathLeaf::new(1, "bb".repeat(32)),
            ]],
        )
        .unwrap();
        let canonical_root = canonical.compute_root(Some(&subject_id)).unwrap();
        assert_ne!(
            canonical_root, orphan_root,
            "the reorg shape: same height, new root"
        );
        let out = Engine::stitch_proof_into_stored_beef(&stored, &subject_id, &canonical).unwrap();
        let after = Beef::from_binary(&out).unwrap();
        // the subject's OWN bump is the canonical one
        let own = after
            .find_txid(&subject_id)
            .and_then(BeefTx::bump_index)
            .and_then(|bi| after.bumps.get(bi))
            .unwrap();
        assert_eq!(
            own.compute_root(Some(&subject_id)).unwrap(),
            canonical_root,
            "the subject re-anchored to the new bump"
        );
        // the orphan bump is gone (only the canonical one remains)
        assert_eq!(
            after.bumps.len(),
            1,
            "the unreferenced orphan bump was dropped"
        );
        // and a reader that uses the tx's OWN bump sees canonical, never orphan
        assert!(after.find_txid(&parent_id).is_some(), "ancestry preserved");
    }

    #[test]
    fn stitch_refuses_a_beef_that_lacks_the_txid() {
        use bsv_rs::transaction::MerklePath;
        let (stored, _c, _p) = subject_not_last_beef();
        let foreign = "aa".repeat(32);
        let proof = MerklePath::from_hex(&single_leaf_bump_hex(&foreign, 900_000)).unwrap();
        assert!(
            Engine::stitch_proof_into_stored_beef(&stored, &foreign, &proof).is_none(),
            "no txid in the beef -> no write, never garbage"
        );
    }

    /// A real mainnet raw tx, the unproven parent for the re-anchor pin.
    const RAW_PARENT: &str = "0100000001c997a5e56e104102fa209c6a852dd90660a20b2d9c352423edce25857fcd3704000000004847304402204e45e16932b8af514961a1d3a1a25fdf3f4f7732e9d624c6c61548ab5fb8cd410220181522ec8eca07de4860a4acdd12909d831cc56cbbac4622082221a8768d1d0901ffffffff0200ca9a3b00000000434104ae1a62fe09c5f51b13905f07f06b99a2f7159b2225f374cd378d71302fa28414e7aab37397f554a7df5f142c21c1b7303b8a0626f1baded5c72a704f7e6cd84cac00286bee0000000043410411db93e1dcdb8a016b49840f8c53bc1eb68a382e97b1482ecad7b148a6909a5cb2e0eaddfb84ccf9744464f82e160bfa9b8b64f9d4c03f999b8643f656b412a3ac00000000";

    /// A minimal raw tx spending `source:vout` with one OP_TRUE output.
    fn child_of(source_txid: &str, vout: u32) -> bsv_rs::transaction::Transaction {
        let mut sx = String::from("0100000001");
        let mut prev = hex::decode(source_txid).unwrap();
        prev.reverse();
        sx.push_str(&hex::encode(prev));
        sx.push_str(&hex::encode(vout.to_le_bytes()));
        sx.push_str("00ffffffff01");
        sx.push_str(&hex::encode(1000u64.to_le_bytes()));
        sx.push_str("015100000000");
        bsv_rs::transaction::Transaction::from_hex(&sx).unwrap()
    }

    /// A minimal valid single-leaf BUMP hex proving `txid` at `height`.
    fn single_leaf_bump_hex(txid: &str, height: u32) -> String {
        use bsv_rs::transaction::{MerklePath, MerklePathLeaf};
        let leaf = MerklePathLeaf::new_txid(0, txid.to_string());
        MerklePath::new_unchecked(height, vec![vec![leaf]])
            .unwrap()
            .to_hex()
    }

    /// Mock fetcher: returns a configured proof (or none) for any txid, and
    /// counts how many times it was asked.
    struct StubFetcher {
        proof: Option<String>,
        calls: Cell<u32>,
    }

    #[async_trait(?Send)]
    impl AncestorFetcher for StubFetcher {
        async fn fetch_ancestor(&self, _txid: &str) -> Result<FetchedAncestor, GASPError> {
            self.calls.set(self.calls.get() + 1);
            Ok(FetchedAncestor {
                raw_tx: PROOFLESS_RAW_TX.to_string(),
                proof: self.proof.clone(),
            })
        }

        /// Models a fetcher whose header source CONFIRMS a stored bump — so the
        /// already-structurally-proven re-verify gate (the window-clog path)
        /// latches without a re-fetch, exactly as before the #192/#193 hardening.
        /// (`verified_proof_for` is left at its default: it delegates to
        /// `fetch_ancestor` above, so the `calls` counter still tracks fetches.)
        async fn verify_proof(&self, _txid: &str, _bump_hex: &str) -> bool {
            true
        }
    }

    /// Build an engine over a storage holding one proofless output, with an
    /// optional ancestor fetcher.
    async fn engine_with_proofless_output(
        fetcher: Option<std::rc::Rc<StubFetcher>>,
    ) -> (Engine, String) {
        let (beef, txid) = proofless_beef();
        let storage = MemoryStorage::new();
        storage
            .insert_output(&Output {
                txid: txid.clone(),
                output_index: 0,
                output_script: vec![0x76],
                satoshis: 1_000_000_000,
                topic: "Hello".to_string(),
                spent: false,
                outputs_consumed: vec![],
                consumed_by: vec![],
                beef: Some(beef),
                block_height: None,
                score: Some(1.0),
            })
            .await
            .unwrap();

        let mut managers: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
        managers.insert(
            "Hello".into(),
            Box::new(MockTopicManager::admitting(vec![0])),
        );
        let mut engine = Engine::new(
            managers,
            HashMap::new(),
            Box::new(storage),
            None,
            EngineConfig::default(),
        );
        if let Some(f) = fetcher {
            engine.set_ancestor_fetcher(f);
        }
        (engine, txid)
    }

    #[tokio::test]
    async fn complete_missing_proofs_noop_without_fetcher() {
        // No ancestor fetcher configured → pure no-op (production default).
        let (engine, _txid) = engine_with_proofless_output(None).await;
        let summary = engine.complete_missing_proofs(50, 0).await.unwrap();
        assert_eq!(summary, ProofCompletionSummary::default());
    }

    #[tokio::test]
    async fn complete_missing_proofs_stitches_when_fetcher_returns_proof() {
        let (_beef, txid) = proofless_beef();
        let proof_hex = single_leaf_bump_hex(&txid, 850_000);
        let fetcher = std::rc::Rc::new(StubFetcher {
            proof: Some(proof_hex),
            calls: Cell::new(0),
        });
        let (engine, txid) = engine_with_proofless_output(Some(fetcher.clone())).await;

        let summary = engine.complete_missing_proofs(50, 0).await.unwrap();
        assert_eq!(summary.scanned, 1);
        assert_eq!(summary.proofless, 1);
        assert_eq!(
            summary.completed, 1,
            "the proofless BEEF should be completed"
        );
        assert_eq!(fetcher.calls.get(), 1);

        // The stored BEEF now carries a proof for the target tx + the output
        // got its block height.
        let out = engine
            .storage()
            .find_output(&txid, 0, Some("Hello"), None, true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(out.block_height, Some(850_000));
        let beef = bsv_rs::transaction::Beef::from_binary(out.beef.as_ref().unwrap()).unwrap();
        assert!(
            beef.find_txid(&txid)
                .is_some_and(bsv_rs::transaction::BeefTx::has_proof),
            "completed BEEF must prove the target tx"
        );

        // A second pass is a no-op: the tx is now proven, so it is not counted
        // proofless and the fetcher is not asked again.
        let summary2 = engine.complete_missing_proofs(50, 0).await.unwrap();
        assert_eq!(summary2.proofless, 0);
        assert_eq!(summary2.completed, 0);
        assert_eq!(fetcher.calls.get(), 1, "no re-fetch once proven");
    }

    #[tokio::test]
    async fn complete_missing_proofs_skips_unconfirmed() {
        // Fetcher reachable but the tx is unmined (no BUMP) → skipped, not
        // errored, and counted still_unconfirmed for retry.
        let fetcher = std::rc::Rc::new(StubFetcher {
            proof: None,
            calls: Cell::new(0),
        });
        let (engine, _txid) = engine_with_proofless_output(Some(fetcher.clone())).await;
        let summary = engine.complete_missing_proofs(50, 0).await.unwrap();
        assert_eq!(summary.proofless, 1);
        assert_eq!(summary.completed, 0);
        assert_eq!(summary.still_unconfirmed, 1);
        assert_eq!(fetcher.calls.get(), 1);
    }

    /// Build an engine like [`engine_with_proofless_output`] but keep a shared
    /// handle to the `MemoryStorage` (via the `Rc<T: Storage>` delegation) so
    /// the test can advance its deterministic clock — the push-primary
    /// backstop age-gate tests (#228) need to age rows without sleeping.
    async fn engine_with_proofless_output_shared(
        fetcher: Option<std::rc::Rc<StubFetcher>>,
    ) -> (Engine, String, std::rc::Rc<MemoryStorage>) {
        let (beef, txid) = proofless_beef();
        let storage = std::rc::Rc::new(MemoryStorage::new());
        storage
            .insert_output(&Output {
                txid: txid.clone(),
                output_index: 0,
                output_script: vec![0x76],
                satoshis: 1_000_000_000,
                topic: "Hello".to_string(),
                spent: false,
                outputs_consumed: vec![],
                consumed_by: vec![],
                beef: Some(beef),
                block_height: None,
                score: Some(1.0),
            })
            .await
            .unwrap();

        let mut managers: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
        managers.insert(
            "Hello".into(),
            Box::new(MockTopicManager::admitting(vec![0])),
        );
        let mut engine = Engine::new(
            managers,
            HashMap::new(),
            Box::new(storage.clone()),
            None,
            EngineConfig::default(),
        );
        if let Some(f) = fetcher {
            engine.set_ancestor_fetcher(f);
        }
        (engine, txid, storage)
    }

    #[tokio::test]
    async fn complete_missing_proofs_age_gate_young_rows_wait_for_the_push() {
        // Push-primary backstop (#228): a FRESHLY stored proofless tx must NOT
        // be polled — its proof is expected via /arc-ingest at push speed. The
        // gated pass scans nothing and never asks the fetcher.
        let proof_hex = {
            let (_beef, txid) = proofless_beef();
            single_leaf_bump_hex(&txid, 850_000)
        };
        let fetcher = std::rc::Rc::new(StubFetcher {
            proof: Some(proof_hex),
            calls: Cell::new(0),
        });
        let (engine, _txid, storage) =
            engine_with_proofless_output_shared(Some(fetcher.clone())).await;

        let summary = engine.complete_missing_proofs(50, 1800).await.unwrap();
        assert_eq!(summary.scanned, 0, "a young row is skipped entirely");
        assert_eq!(fetcher.calls.get(), 0, "no courier fetch for a young row");

        // WEBHOOK-OUTAGE DEGRADATION: no push ever arrives. Once the row is
        // older than the gate, the SAME backstop pass polls and completes it
        // exactly as the pre-#228 behaviour — degradation is to polling,
        // never to nothing.
        storage.advance_clock(1800);
        let summary = engine.complete_missing_proofs(50, 1800).await.unwrap();
        assert_eq!(summary.scanned, 1, "an old-enough row is always polled");
        assert_eq!(summary.completed, 1, "the backstop completes the proof");
        assert_eq!(fetcher.calls.get(), 1);
    }

    #[tokio::test]
    async fn complete_missing_proofs_skips_tx_whose_proof_was_pushed() {
        // Pushed-proof-then-chaser-skips (#228): a proof arriving via the real
        // /arc-ingest producer path (`handle_new_merkle_proof` → the
        // `update_transaction_beef` stitch, which latches `has_proof`) drops
        // the tx out of the poll candidate set entirely — even after the age
        // gate opens, the chaser never asks the fetcher about it.
        let fetcher = std::rc::Rc::new(StubFetcher {
            proof: None,
            calls: Cell::new(0),
        });
        let (engine, txid, storage) =
            engine_with_proofless_output_shared(Some(fetcher.clone())).await;

        // The push lands (arc-ingest calls exactly this after its own
        // chaintracks verify).
        let proof_hex = single_leaf_bump_hex(&txid, 850_000);
        engine
            .handle_new_merkle_proof(&txid, &proof_hex, Some(850_000))
            .await
            .unwrap();

        // Age the row well past the backstop gate: still skipped — the pushed
        // proof, not the age, is what removes it from the chaser's world.
        storage.advance_clock(1_000_000);
        let summary = engine.complete_missing_proofs(50, 1800).await.unwrap();
        assert_eq!(summary.scanned, 0, "a pushed-proof tx is never re-polled");
        assert_eq!(summary.completed, 0);
        assert_eq!(fetcher.calls.get(), 0, "the chaser must skip it entirely");
    }

    #[tokio::test]
    async fn complete_missing_proofs_respects_limit() {
        // Two proofless txs, limit 1 → only one row is scanned this tick.
        let (beef1, txid1) = proofless_beef();
        // A second distinct proofless tx (different raw tx → different txid).
        let raw2 = "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff2803dc7e0e0499170e6a0003cf341b017e0000152f476f72696c6c61506f6f6c2e696f20f09fa68d2f0000000003000000000000000032006a0547504f4f4c08dc7e0e0000000000200158a2360a03939451e72c3a9302f5d48712bf54a5b2edf8f3c69aed35a668e312236000000000001976a914068a58835bb93b152c901ffb18f6578824f9d5b788ac6eb66612000000001976a91402fd5a91155231d5799e2d22c490d1664cde62cb88ac00000000";
        let (beef2, txid2) = {
            use bsv_rs::transaction::{Beef, Transaction};
            let tx = Transaction::from_hex(raw2).unwrap();
            let id = tx.id();
            let mut b = Beef::new();
            b.merge_transaction(tx);
            (b.to_binary(), id)
        };

        let storage = MemoryStorage::new();
        for (txid, beef) in [(&txid1, beef1), (&txid2, beef2)] {
            storage
                .insert_output(&Output {
                    txid: txid.clone(),
                    output_index: 0,
                    output_script: vec![0x76],
                    satoshis: 1000,
                    topic: "Hello".to_string(),
                    spent: false,
                    outputs_consumed: vec![],
                    consumed_by: vec![],
                    beef: Some(beef),
                    block_height: None,
                    score: Some(1.0),
                })
                .await
                .unwrap();
        }

        let fetcher = std::rc::Rc::new(StubFetcher {
            proof: None,
            calls: Cell::new(0),
        });
        let mut managers: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
        managers.insert(
            "Hello".into(),
            Box::new(MockTopicManager::admitting(vec![0])),
        );
        let mut engine = Engine::new(
            managers,
            HashMap::new(),
            Box::new(storage),
            None,
            EngineConfig::default(),
        );
        engine.set_ancestor_fetcher(fetcher.clone());

        let summary = engine.complete_missing_proofs(1, 0).await.unwrap();
        assert_eq!(summary.scanned, 1, "limit 1 → only one row scanned");
    }

    #[tokio::test]
    async fn complete_missing_proofs_marks_already_proven_rows_so_window_advances() {
        // Window-clog regression (#130): migration 0010 defaulted EVERY
        // existing `transactions` row to has_proof = 0, including rows whose
        // stored BEEF already carries a proof (e.g. GASP-synced txs). The
        // candidate query (`WHERE has_proof = 0 LIMIT n`) returns those rows
        // every tick; the engine skips them but — before this fix — never
        // flipped the flag, so they lingered in the bounded window forever and
        // starved genuinely-proofless rows behind them. The fix marks each
        // already-proven scanned row so it drops out of the NEXT pass.
        use bsv_rs::transaction::{Beef, MerklePath, Transaction};

        let storage = MemoryStorage::new();

        // Seed several ALREADY-PROVEN rows. They are inserted via
        // `insert_output`, which (modeling migration 0010's default) does NOT
        // set the proof flag — so despite carrying a valid proof they show up
        // as has_proof = 0 candidates: the exact clog.
        let proven_raws = [
            "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff2803dc7e0e0499170e6a0003cf341b017e0000152f476f72696c6c61506f6f6c2e696f20f09fa68d2f0000000003000000000000000032006a0547504f4f4c08dc7e0e0000000000200158a2360a03939451e72c3a9302f5d48712bf54a5b2edf8f3c69aed35a668e312236000000000001976a914068a58835bb93b152c901ffb18f6578824f9d5b788ac6eb66612000000001976a91402fd5a91155231d5799e2d22c490d1664cde62cb88ac00000000",
            "0100000001c997a5e56e104102fa209c6a852dd90660a20b2d9c352423edce25857fcd3704000000004847304402204e45e16932b8af514961a1d3a1a25fdf3f4f7732e9d624c6c61548ab5fb8cd410220181522ec8eca07de4860a4acdd12909d831cc56cbbac4622082221a8768d1d0901ffffffff0200ca9a3b00000000434104ae1a62fe09c5f51b13905f07f06b99a2f7159b2225f374cd378d71302fa28414e7aab37397f554a7df5f142c21c1b7303b8a0626f1baded5c72a704f7e6cd84cac00286bee0000000043410411db93e1dcdb8a016b49840f8c53bc1eb68a382e97b1482ecad7b148a6909a5cb2e0eaddfb84ccf9744464f82e160bfa9b8b64f9d4c03f999b8643f656b412a3ac00000000",
        ];
        let mut proven_ids = Vec::new();
        for raw in proven_raws {
            let mut tx = Transaction::from_hex(raw).unwrap();
            let id = tx.id();
            tx.merkle_path =
                Some(MerklePath::from_hex(&single_leaf_bump_hex(&id, 800_000)).unwrap());
            // to_beef(true) carries the merkle proof into the BEEF, so the row
            // genuinely IS proven — yet its flag stays 0 (insert path).
            let proven_beef = tx.to_beef(true).unwrap();
            assert_eq!(
                Beef::from_binary(&proven_beef)
                    .unwrap()
                    .find_txid(&id)
                    .map(bsv_rs::transaction::BeefTx::has_proof),
                Some(true),
                "proven fixture row must carry its proof"
            );
            proven_ids.push(id.clone());
            storage
                .insert_output(&Output {
                    txid: id,
                    output_index: 0,
                    output_script: vec![0x76],
                    satoshis: 1000,
                    topic: "Hello".to_string(),
                    spent: false,
                    outputs_consumed: vec![],
                    consumed_by: vec![],
                    beef: Some(proven_beef),
                    block_height: Some(800_000),
                    score: Some(1.0),
                })
                .await
                .unwrap();
        }

        let fetcher = std::rc::Rc::new(StubFetcher {
            proof: None,
            calls: Cell::new(0),
        });
        let mut managers: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
        managers.insert(
            "Hello".into(),
            Box::new(MockTopicManager::admitting(vec![0])),
        );
        let mut engine = Engine::new(
            managers,
            HashMap::new(),
            Box::new(storage),
            None,
            EngineConfig::default(),
        );
        engine.set_ancestor_fetcher(fetcher.clone());

        // First pass over a window covering both proven rows: each is detected
        // already-proven and marked. Nothing proofless, nothing fetched.
        let summary = engine.complete_missing_proofs(50, 0).await.unwrap();
        assert_eq!(summary.scanned, proven_ids.len());
        assert_eq!(summary.proofless, 0);
        assert_eq!(summary.completed, 0);
        assert_eq!(
            summary.already_proven,
            proven_ids.len(),
            "every already-proven row is marked this pass"
        );
        assert_eq!(fetcher.calls.get(), 0, "no fetch for already-proven rows");

        // Window has advanced: the NEXT candidate query no longer returns the
        // marked rows, so the pass scans nothing.
        let summary2 = engine.complete_missing_proofs(50, 0).await.unwrap();
        assert_eq!(summary2.scanned, 0, "marked rows dropped out of the window");
        assert_eq!(summary2.already_proven, 0);
    }

    #[tokio::test]
    async fn complete_missing_proofs_reaches_proofless_tx_behind_clogging_proven_rows() {
        // The window-clog in action with a small limit: a genuinely-proofless
        // backlog row sits behind already-proven (stale-flag) rows. Each tick
        // marks the proven rows it scans (dropping them out), so over a few
        // bounded ticks the window advances until the proofless tx is reached
        // + completed — instead of being starved forever.
        use bsv_rs::transaction::{MerklePath, Transaction};

        let storage = MemoryStorage::new();

        let proven_raws = [
            "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff2803dc7e0e0499170e6a0003cf341b017e0000152f476f72696c6c61506f6f6c2e696f20f09fa68d2f0000000003000000000000000032006a0547504f4f4c08dc7e0e0000000000200158a2360a03939451e72c3a9302f5d48712bf54a5b2edf8f3c69aed35a668e312236000000000001976a914068a58835bb93b152c901ffb18f6578824f9d5b788ac6eb66612000000001976a91402fd5a91155231d5799e2d22c490d1664cde62cb88ac00000000",
        ];
        for raw in proven_raws {
            let mut tx = Transaction::from_hex(raw).unwrap();
            let id = tx.id();
            tx.merkle_path =
                Some(MerklePath::from_hex(&single_leaf_bump_hex(&id, 800_000)).unwrap());
            let proven_beef = tx.to_beef(true).unwrap();
            storage
                .insert_output(&Output {
                    txid: id,
                    output_index: 0,
                    output_script: vec![0x76],
                    satoshis: 1000,
                    topic: "Hello".to_string(),
                    spent: false,
                    outputs_consumed: vec![],
                    consumed_by: vec![],
                    beef: Some(proven_beef),
                    block_height: Some(800_000),
                    score: Some(1.0),
                })
                .await
                .unwrap();
        }

        // The historical proofless tx (the f584f846-style backlog row).
        let (proofless, proofless_txid) = proofless_beef();
        storage
            .insert_output(&Output {
                txid: proofless_txid.clone(),
                output_index: 0,
                output_script: vec![0x76],
                satoshis: 1000,
                topic: "Hello".to_string(),
                spent: false,
                outputs_consumed: vec![],
                consumed_by: vec![],
                beef: Some(proofless),
                block_height: None,
                score: Some(1.0),
            })
            .await
            .unwrap();

        let proof_hex = single_leaf_bump_hex(&proofless_txid, 850_000);
        let fetcher = std::rc::Rc::new(StubFetcher {
            proof: Some(proof_hex),
            calls: Cell::new(0),
        });
        let mut managers: HashMap<String, Box<dyn TopicManager>> = HashMap::new();
        managers.insert(
            "Hello".into(),
            Box::new(MockTopicManager::admitting(vec![0])),
        );
        let mut engine = Engine::new(
            managers,
            HashMap::new(),
            Box::new(storage),
            None,
            EngineConfig::default(),
        );
        engine.set_ancestor_fetcher(fetcher.clone());

        // Run bounded ticks (limit 1) until the proofless tx is completed. With
        // 2 rows total a couple of ticks suffice; cap iterations defensively.
        let mut completed_total = 0;
        for _ in 0..5 {
            let s = engine.complete_missing_proofs(1, 0).await.unwrap();
            completed_total += s.completed;
            if completed_total > 0 {
                break;
            }
        }
        assert_eq!(
            completed_total, 1,
            "the proofless backlog tx is reached + completed once the proven rows are marked out of the window"
        );

        let out = engine
            .storage()
            .find_output(&proofless_txid, 0, Some("Hello"), None, true)
            .await
            .unwrap()
            .unwrap();
        let beef = bsv_rs::transaction::Beef::from_binary(out.beef.as_ref().unwrap()).unwrap();
        assert_eq!(
            beef.find_txid(&proofless_txid)
                .map(bsv_rs::transaction::BeefTx::has_proof),
            Some(true),
            "the old proofless tx now carries its proof"
        );
    }

    // ── S2 queue-durable admission (bsv-low 2026-08-29): Phase-3 faults ──
    //
    // The phantom class: under a D1 storm `submit` returned Ok(steak) while
    // its writes failed — the ack outlived the admission. These pins hold
    // the new contract: a fault is REPORTED, the faulted topic is NOT
    // recorded as applied, and a replay of the same bytes re-applies.

    use crate::storage::StorageError;
    use std::rc::Rc;

    /// `MemoryStorage` behind two fault switches. Every method delegates;
    /// the two the tests flip model a D1 write fault (`insert_output`) and
    /// a D1 read fault (`find_output` — the validation's previous-coin scan
    /// AND Phase 3's stale/consumed lookups).
    struct FaultingStorage {
        inner: MemoryStorage,
        fail_insert_output: Cell<bool>,
        fail_find_output: Cell<bool>,
    }

    impl FaultingStorage {
        fn new() -> Rc<Self> {
            Rc::new(Self {
                inner: MemoryStorage::new(),
                fail_insert_output: Cell::new(false),
                fail_find_output: Cell::new(false),
            })
        }
    }

    #[async_trait(?Send)]
    impl Storage for FaultingStorage {
        async fn put_deferred_graph(
            &self,
            record: &crate::gasp::DeferredGraph,
        ) -> Result<crate::gasp::DeferredGraphSave, StorageError> {
            self.inner.put_deferred_graph(record).await
        }
        async fn find_deferred_graphs(
            &self,
            host: &str,
            topic: &str,
        ) -> Result<Vec<crate::gasp::DeferredGraphKey>, StorageError> {
            self.inner.find_deferred_graphs(host, topic).await
        }
        async fn get_deferred_graph(
            &self,
            host: &str,
            topic: &str,
            outpoint: &str,
        ) -> Result<Option<crate::gasp::DeferredGraph>, StorageError> {
            self.inner.get_deferred_graph(host, topic, outpoint).await
        }
        async fn delete_deferred_graph(
            &self,
            host: &str,
            topic: &str,
            outpoint: &str,
        ) -> Result<(), StorageError> {
            self.inner
                .delete_deferred_graph(host, topic, outpoint)
                .await
        }
        async fn insert_output(&self, output: &Output) -> Result<(), StorageError> {
            if self.fail_insert_output.get() {
                return Err(StorageError::Database(
                    "D1_ERROR: storage overloaded".into(),
                ));
            }
            self.inner.insert_output(output).await
        }
        async fn delete_output(
            &self,
            txid: &str,
            output_index: u32,
            topic: &str,
        ) -> Result<(), StorageError> {
            self.inner.delete_output(txid, output_index, topic).await
        }
        async fn mark_utxo_as_spent(
            &self,
            txid: &str,
            output_index: u32,
            topic: &str,
        ) -> Result<(), StorageError> {
            self.inner
                .mark_utxo_as_spent(txid, output_index, topic)
                .await
        }
        async fn update_consumed_by(
            &self,
            txid: &str,
            output_index: u32,
            topic: &str,
            consumed_by: &[Outpoint],
        ) -> Result<(), StorageError> {
            self.inner
                .update_consumed_by(txid, output_index, topic, consumed_by)
                .await
        }
        async fn update_transaction_beef(
            &self,
            txid: &str,
            beef: &[u8],
        ) -> Result<(), StorageError> {
            self.inner.update_transaction_beef(txid, beef).await
        }
        async fn insert_applied_transaction(
            &self,
            tx: &AppliedTransaction,
        ) -> Result<(), StorageError> {
            self.inner.insert_applied_transaction(tx).await
        }
        async fn does_applied_transaction_exist(
            &self,
            tx: &AppliedTransaction,
        ) -> Result<bool, StorageError> {
            self.inner.does_applied_transaction_exist(tx).await
        }
        async fn delete_applied_transaction(
            &self,
            tx: &AppliedTransaction,
        ) -> Result<(), StorageError> {
            self.inner.delete_applied_transaction(tx).await
        }
        async fn find_output(
            &self,
            txid: &str,
            output_index: u32,
            topic: Option<&str>,
            spent: Option<bool>,
            include_beef: bool,
        ) -> Result<Option<Output>, StorageError> {
            if self.fail_find_output.get() {
                return Err(StorageError::Database("D1_ERROR: read timed out".into()));
            }
            self.inner
                .find_output(txid, output_index, topic, spent, include_beef)
                .await
        }
        async fn find_outputs_for_transaction(
            &self,
            txid: &str,
            include_beef: bool,
        ) -> Result<Vec<Output>, StorageError> {
            self.inner
                .find_outputs_for_transaction(txid, include_beef)
                .await
        }
        async fn find_utxos_for_topic(
            &self,
            topic: &str,
            since: Option<f64>,
            limit: Option<u64>,
            include_beef: bool,
        ) -> Result<Vec<Output>, StorageError> {
            self.inner
                .find_utxos_for_topic(topic, since, limit, include_beef)
                .await
        }
        async fn update_last_interaction(
            &self,
            host: &str,
            topic: &str,
            since: u64,
        ) -> Result<(), StorageError> {
            self.inner.update_last_interaction(host, topic, since).await
        }
        async fn get_last_interaction(&self, host: &str, topic: &str) -> Result<u64, StorageError> {
            self.inner.get_last_interaction(host, topic).await
        }
    }

    fn make_faulting_engine(admit_indices: Vec<u32>) -> (Engine, Rc<FaultingStorage>) {
        let storage = FaultingStorage::new();
        let mut managers: HashMap<String, Box<dyn TopicManagerTrait>> = HashMap::new();
        managers.insert(
            "tm_test".to_string(),
            Box::new(MockTopicManager::admitting(admit_indices)),
        );
        let mut lookup_services: HashMap<String, Box<dyn LookupServiceTrait>> = HashMap::new();
        lookup_services.insert("ls_test".to_string(), Box::new(MockLookupService::new()));
        let engine = Engine::new(
            managers,
            lookup_services,
            Box::new(Rc::clone(&storage)),
            None,
            EngineConfig::default(),
        );
        (engine, storage)
    }

    async fn applied(storage: &FaultingStorage) -> bool {
        storage
            .does_applied_transaction_exist(&AppliedTransaction {
                txid: TEST_TXID.to_string(),
                topic: "tm_test".to_string(),
            })
            .await
            .unwrap()
    }

    /// The old behaviour, pinned: a clean submit is durable, records the
    /// topic as applied, and the output is in storage.
    #[tokio::test]
    async fn clean_submit_is_durable_and_records_applied() {
        let (engine, storage) = make_faulting_engine(vec![0]);
        let (steak, report) = engine
            .submit_with_report(&test_tagged_beef(vec!["tm_test"]), SubmitMode::CurrentTx)
            .await
            .unwrap();
        assert_eq!(steak["tm_test"].outputs_to_admit, vec![0]);
        assert!(report.is_durable(), "{}", report.summary());
        assert_eq!(report.applied_topics, vec!["tm_test".to_string()]);
        assert!(applied(&storage).await);
        assert!(storage
            .find_output(TEST_TXID, 0, Some("tm_test"), None, false)
            .await
            .unwrap()
            .is_some());
    }

    /// THE PHANTOM, red→green: the insert fails, the steak still says
    /// "admitted" (the decision stands), but the report says NOT DURABLE and
    /// names the site — and the topic is NOT recorded as applied, so nothing
    /// downstream can treat the loss as final.
    #[tokio::test]
    async fn phase3_write_fault_is_reported_and_blocks_the_applied_record() {
        let (engine, storage) = make_faulting_engine(vec![0]);
        storage.fail_insert_output.set(true);
        let (steak, report) = engine
            .submit_with_report(&test_tagged_beef(vec!["tm_test"]), SubmitMode::CurrentTx)
            .await
            .unwrap();
        assert_eq!(
            steak["tm_test"].outputs_to_admit,
            vec![0],
            "the admission DECISION is unchanged by a write fault"
        );
        assert!(!report.is_durable());
        assert_eq!(report.faults.len(), 1, "{}", report.summary());
        assert_eq!(report.faults[0].topic, "tm_test");
        assert_eq!(report.faults[0].site, "insert_output");
        assert!(report.faults[0].error.contains("D1_ERROR"));
        assert!(report.applied_topics.is_empty());
        assert!(
            !applied(&storage).await,
            "a faulted topic must not be recorded as applied — the replay would be a dupe"
        );
        assert!(storage
            .find_output(TEST_TXID, 0, Some("tm_test"), None, false)
            .await
            .unwrap()
            .is_none());
    }

    /// The replay: same bytes, storage healthy again → re-validated (not a
    /// dupe), re-written, recorded. A THIRD submit is then the dupe it
    /// should be: nothing admitted, nothing to write, durable.
    #[tokio::test]
    async fn replay_after_a_fault_reapplies_and_records_applied() {
        let (engine, storage) = make_faulting_engine(vec![0]);
        storage.fail_insert_output.set(true);
        let (_, first) = engine
            .submit_with_report(&test_tagged_beef(vec!["tm_test"]), SubmitMode::CurrentTx)
            .await
            .unwrap();
        assert!(!first.is_durable());

        storage.fail_insert_output.set(false);
        let (steak, replay) = engine
            .submit_with_report(&test_tagged_beef(vec!["tm_test"]), SubmitMode::CurrentTx)
            .await
            .unwrap();
        assert_eq!(
            steak["tm_test"].outputs_to_admit,
            vec![0],
            "re-validated, not deduped"
        );
        assert!(replay.is_durable(), "{}", replay.summary());
        assert_eq!(replay.applied_topics, vec!["tm_test".to_string()]);
        assert!(applied(&storage).await);
        assert!(storage
            .find_output(TEST_TXID, 0, Some("tm_test"), None, false)
            .await
            .unwrap()
            .is_some());

        let (steak, third) = engine
            .submit_with_report(&test_tagged_beef(vec!["tm_test"]), SubmitMode::CurrentTx)
            .await
            .unwrap();
        assert!(
            steak["tm_test"].outputs_to_admit.is_empty(),
            "now a genuine dupe: nothing admitted"
        );
        assert!(third.is_durable() && third.applied_topics.is_empty());
    }

    /// The SPEND half of the phantom: a `find_output` fault during
    /// validation made a settle look like it consumed nothing, so the spend
    /// pointer was never written and the ack stood. Now the read fault is
    /// reported, the topic stays unrecorded, the previously-admitted coin
    /// is untouched — and the healthy replay consumes it.
    #[tokio::test]
    async fn validation_read_fault_is_reported_and_the_replay_consumes_the_coin() {
        let (engine, storage) = make_faulting_engine(vec![0]);
        // The subject's own input: seed the coin it spends as a previously
        // admitted tm_test output.
        let tx = Transaction::from_beef(&test_beef(), None).unwrap();
        let source_txid = tx.inputs[0].get_source_txid().unwrap();
        let source_vout = tx.inputs[0].source_output_index;
        storage
            .insert_output(&Output {
                txid: source_txid.clone(),
                output_index: source_vout,
                output_script: vec![0x51],
                satoshis: 1,
                topic: "tm_test".to_string(),
                spent: false,
                outputs_consumed: vec![],
                consumed_by: vec![],
                beef: None,
                block_height: None,
                score: Some(1.0),
            })
            .await
            .unwrap();

        storage.fail_find_output.set(true);
        let (_, faulted) = engine
            .submit_with_report(&test_tagged_beef(vec!["tm_test"]), SubmitMode::CurrentTx)
            .await
            .unwrap();
        assert!(!faulted.is_durable());
        assert!(
            faulted.faults.iter().any(|f| f.site == "find_output"),
            "{}",
            faulted.summary()
        );
        assert!(
            !applied(&storage).await,
            "unrecorded: the replay must re-read"
        );
        storage.fail_find_output.set(false);
        assert!(
            storage
                .find_output(&source_txid, source_vout, Some("tm_test"), None, false)
                .await
                .unwrap()
                .is_some(),
            "the coin the faulted pass could not see is still there, untouched"
        );

        let (_, replay) = engine
            .submit_with_report(&test_tagged_beef(vec!["tm_test"]), SubmitMode::CurrentTx)
            .await
            .unwrap();
        assert!(replay.is_durable(), "{}", replay.summary());
        assert!(applied(&storage).await);
        // The mock manager retains no coins, so the consumed coin is stale
        // and deleted — the observable proof the spend was PROCESSED.
        assert!(
            storage
                .find_output(&source_txid, source_vout, Some("tm_test"), None, false)
                .await
                .unwrap()
                .is_none(),
            "the healthy replay consumed the coin the faulted pass missed"
        );
    }

    /// bsv-low PLAN-PRE-LOOP4 §H4 (2026-09-06): a lookup service that missed
    /// its admit notification is told AGAIN from the engine's own stored
    /// outputs — one call per admitted output per service, the WholeTx body
    /// naming the subject — while `/submit` of the same bytes is deduped and
    /// tells nobody (the premise, RED-verified in the same test); a topic the
    /// engine never admitted this txid on yields 0 (the caller re-submits).
    #[tokio::test]
    async fn renotify_admitted_replays_the_admit_from_stored_outputs() {
        use std::cell::RefCell;
        use std::rc::Rc;

        struct CountingLs(Rc<RefCell<Vec<(String, u32, String)>>>, AdmissionMode);
        #[async_trait(?Send)]
        impl LookupServiceTrait for CountingLs {
            fn admission_mode(&self) -> AdmissionMode {
                self.1
            }
            fn spend_notification_mode(&self) -> SpendNotificationMode {
                SpendNotificationMode::None
            }
            async fn output_admitted_by_topic(
                &self,
                payload: &OutputAdmittedByTopic,
            ) -> Result<(), LookupServiceError> {
                let rec = match payload {
                    OutputAdmittedByTopic::LockingScript {
                        txid,
                        output_index,
                        topic,
                        ..
                    } => (txid.clone(), *output_index, topic.clone()),
                    OutputAdmittedByTopic::WholeTx {
                        atomic_beef,
                        output_index,
                        topic,
                        ..
                    } => {
                        let beef = Beef::from_binary(atomic_beef).expect("the WholeTx body parses");
                        (
                            beef.atomic_txid
                                .clone()
                                .expect("the WholeTx body NAMES its subject"),
                            *output_index,
                            topic.clone(),
                        )
                    }
                };
                self.0.borrow_mut().push(rec);
                Ok(())
            }
            async fn output_evicted(
                &self,
                _txid: &str,
                _output_index: u32,
            ) -> Result<(), LookupServiceError> {
                Ok(())
            }
            async fn lookup(
                &self,
                _question: &LookupQuestion,
            ) -> Result<LookupResult, LookupServiceError> {
                Ok(LookupResult::OutputList(Vec::new()))
            }
            async fn get_documentation(&self) -> String {
                "counting".to_string()
            }
            async fn get_metadata(&self) -> ServiceMetadata {
                ServiceMetadata {
                    name: "counting-ls".to_string(),
                    ..Default::default()
                }
            }
        }

        let mut managers: HashMap<String, Box<dyn TopicManagerTrait>> = HashMap::new();
        managers.insert(
            "tm_test".to_string(),
            Box::new(MockTopicManager::admitting(vec![0])),
        );
        let seen = Rc::new(RefCell::new(Vec::new()));
        let mut lookup_services: HashMap<String, Box<dyn LookupServiceTrait>> = HashMap::new();
        lookup_services.insert(
            "ls_whole".to_string(),
            Box::new(CountingLs(seen.clone(), AdmissionMode::WholeTx)),
        );
        lookup_services.insert(
            "ls_script".to_string(),
            Box::new(CountingLs(seen.clone(), AdmissionMode::LockingScript)),
        );
        let engine = Engine::new(
            managers,
            lookup_services,
            Box::new(MemoryStorage::new()),
            None,
            EngineConfig::default(),
        );

        let tagged = test_tagged_beef(vec!["tm_test"]);
        engine.submit(&tagged, SubmitMode::CurrentTx).await.unwrap();
        assert_eq!(seen.borrow().len(), 2, "one admit per service");
        let (txid, vout, _) = seen.borrow()[0].clone();
        assert_eq!(txid, TEST_TXID);

        // the premise, RED-verified: the same bytes again are DEDUPED — /submit can never re-notify
        let (_, report) = engine
            .submit_with_report(&tagged, SubmitMode::CurrentTx)
            .await
            .unwrap();
        assert_eq!(report.deduped_topics, vec!["tm_test".to_string()]);
        assert_eq!(seen.borrow().len(), 2, "dedup told nobody");

        // the verb: told again from the stored outputs, the WholeTx body naming the subject
        let r = engine.renotify_admitted(&txid, "tm_test").await.unwrap();
        assert_eq!((r.outputs, r.notified, r.faults.len()), (1, 2, 0));
        assert_eq!(r.vouts, vec![vout]);
        assert_eq!(seen.borrow().len(), 4);
        assert!(seen.borrow()[2..]
            .iter()
            .all(|(t, v, topic)| *t == txid && *v == vout && topic == "tm_test"));

        // a topic the engine never admitted this txid on: nothing to re-notify (re-submit instead)
        let r0 = engine.renotify_admitted(&txid, "tm_other").await.unwrap();
        assert_eq!((r0.outputs, r0.notified), (0, 0));
        assert_eq!(seen.borrow().len(), 4);

        // idempotent: a second re-notify tells everyone again, faults nothing
        let r2 = engine
            .renotify_admitted(&txid.to_ascii_uppercase(), "tm_test")
            .await
            .unwrap();
        assert_eq!((r2.outputs, r2.notified, r2.faults.len()), (1, 2, 0));
        assert_eq!(seen.borrow().len(), 6);
    }

    /// bsv-low PLAN-PRE-LOOP4 §H4: a PHANTOM applied row (the topic admitted
    /// NOTHING) dedups every re-submit of those bytes; forgetting it lets the
    /// same bytes be judged again — while a REAL admission's row is never
    /// forgotten and a missing row is nothing to forget.
    #[tokio::test]
    async fn forget_phantom_applied_unblocks_a_resubmit_but_never_a_real_admission() {
        // phantom: a TM that admits nothing still records the topic as applied
        let engine = make_engine(vec![]);
        let tagged = test_tagged_beef(vec!["tm_test"]);
        let (steak, r1) = engine
            .submit_with_report(&tagged, SubmitMode::CurrentTx)
            .await
            .unwrap();
        assert!(steak.get("tm_test").unwrap().outputs_to_admit.is_empty());
        assert_eq!(r1.applied_topics, vec!["tm_test".to_string()]);
        let (_, r2) = engine
            .submit_with_report(&tagged, SubmitMode::CurrentTx)
            .await
            .unwrap();
        assert_eq!(
            r2.deduped_topics,
            vec!["tm_test".to_string()],
            "the phantom row dedups the re-submit"
        );
        assert!(engine
            .forget_phantom_applied(TEST_TXID, "tm_test")
            .await
            .unwrap());
        let (_, r3) = engine
            .submit_with_report(&tagged, SubmitMode::CurrentTx)
            .await
            .unwrap();
        assert!(
            r3.deduped_topics.is_empty(),
            "judged again after the forget"
        );
        assert!(
            !engine
                .forget_phantom_applied(TEST_TXID, "tm_other")
                .await
                .unwrap(),
            "no row: nothing to forget"
        );

        // real: a TM that admits output 0 — its row is a real admission, never forgotten
        let real = make_engine(vec![0]);
        real.submit(&tagged, SubmitMode::CurrentTx).await.unwrap();
        assert!(!real
            .forget_phantom_applied(TEST_TXID, "tm_test")
            .await
            .unwrap());
        let (_, r4) = real
            .submit_with_report(&tagged, SubmitMode::CurrentTx)
            .await
            .unwrap();
        assert_eq!(
            r4.deduped_topics,
            vec!["tm_test".to_string()],
            "a real admission still dedups"
        );
    }
}
