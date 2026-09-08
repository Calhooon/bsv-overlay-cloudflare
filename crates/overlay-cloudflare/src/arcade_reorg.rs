//! bsv-low M19B-G1 (2026-09-08): the overlay CONSUMES Arcade's reorg events,
//! so an orphan demotion or a re-anchor arrives as an EVENT instead of
//! waiting for a third-party proof index to catch up. The I/O half; the
//! decisions are `overlay_discovery::pot::arcade_events`.
//!
//! What Arcade actually offers (read from `~/bsv/arcade` v0.13.3 and probed
//! on the deployed instance): its `ReorgEvent`s ride an SSE stream with no
//! event ids, no replay and nothing on connect, which a Worker holding no
//! connection between passes cannot consume. Its durable projection can be:
//! `GET /api/v1/blocks/processing-status` (REST, no auth, newest first, a
//! height keyset cursor) lists every block Arcade tracked, and a block a
//! `ReorgEvent` orphaned (or its tie-scan found orphaned) reads `status:
//! "orphaned"` with an `orphanedAt` stamp. One such row is one event, keyed
//! `(orphanedAt, height, hash)`; the consumer keeps a persisted cursor over
//! that order (`arcade_reorg_state`, one JSON row, versioned) and moves it
//! ONLY past an event it finished (applied in full, skipped as
//! uncorroborated, or released at the head-of-line ceiling); a fault of any
//! kind leaves it, so the event replays idempotently.
//!
//! An event is a HINT, never evidence (Arcade's own table holds the
//! CANONICAL 965773 block of 2026-09-07 as orphaned, see the build log):
//! - the orphaned hash is CORROBORATED against chaintracks first (the header
//!   at that height must differ; the same hash, or no header, deep below
//!   chaintracks' tip means the row cannot be corroborated by our header
//!   source: skipped, counted; near the tip chaintracks may lag: held; a
//!   tracker more than a few blocks behind Arcade's own listing holds
//!   everything and is counted apart);
//! - then every row anchored at that height is judged by ITS OWN stored
//!   proof exactly like the R2 sweep (`reorg_sweep::reverify_window_with`):
//!   a refuted spender proof is RE-ANCHORED IN PLACE when the courier ladder
//!   (Arcade first) serves a chaintracks-verified proof for another block
//!   (`apply_pushed_proof_to_pot_stores`, the `/arc-ingest` path), and
//!   demoted to SEEN only when no canonical proof is served, the demotion
//!   bound to the height it was judged at; a standing proof stands; a
//!   tracker fault changes nothing and counts; a spent ladder budget stops
//!   the page (nothing judged blind of the couriers); the pots' own proofs
//!   and the hop proofs are unlatched when refuted (the completion passes
//!   re-prove them).
//!
//! Bounded per pass: at most [`ARCADE_EVENTS_PAGES`] Arcade pages, the
//! limits' events finished, one page of at most `leg_limit` rows per leg
//! (three legs), the ladder's own budget, ONE shared root memo seeded from
//! the corroboration header read (round 2, review MED-5). A leg that is not
//! exhausted persists its cursor and the next pass continues it. Runs on
//! every block-event pass (`/internal/tip-changed`, before the routine
//! sweep so a demotion is re-chased in the same tick; the smaller
//! [`PassLimits::block_event`]), on the `*/15` cron (a drought fallback and
//! the first-run catch-up; [`PassLimits::cron`]) and on demand
//! (`POST /internal/arcade-reorg`, which can also RELEASE the pending event).

use bsv_rs::transaction::ChainTracker;
use overlay_discovery::pot::arcade_events::{
    classify_corroboration, events_after, next_page_before, parse_block_status_page,
    ConsumerState, Corroboration, EventKey, LegProgress, OrphanEvent, ReleaseReason,
    UnresolvedEvent,
};
use overlay_discovery::pot::storage::PotStorage;
use overlay_engine::gasp::AncestorFetcher;

use crate::proof_fetcher::push_log;
use crate::reorg_sweep::{
    reverify_pot_beefs_window_with, reverify_transactions_window_with, reverify_window_with,
    ProofLegSummary, ProvenTxStore, ReverifyMode, ReverifyPassSummary, RootMemo,
};

/// Rows per Arcade listing page (Arcade caps at 200).
pub const ARCADE_EVENTS_PAGE_LIMIT: u64 = 100;
/// Listing pages read per pass: the last ~200 blocks (~33 h) are the
/// event window; an orphan mark older than that is the routine sweep's.
pub const ARCADE_EVENTS_PAGES: u32 = 2;
/// Events FINISHED per cron pass at most (a corroborated event with more
/// rows than one page per leg spans passes; that is one event, not two).
pub const ARCADE_EVENTS_PER_PASS: u32 = 2;
/// Events finished per BLOCK-EVENT pass (round 2, review MED-5: that pass
/// also runs the R2 sweep and the confirmation chaser in the same
/// invocation, so the consumer's share is the smaller one).
pub const ARCADE_BLOCK_EVENT_EVENTS_PER_PASS: u32 = 1;
/// An uncorroborated hash this many blocks or more below chaintracks' tip
/// is skipped (chaintracks is not lagging: Arcade's row is wrong for our
/// header source); nearer the tip the event is held for the next pass.
pub const ARCADE_UNCORROBORATED_SKIP_DEPTH: u64 = 3;
/// Round 2 (review LOW-3): chaintracks' tip this many blocks or more BELOW
/// the newest height Arcade lists = our header source is behind the feed;
/// nothing is skipped as uncorroborated while it is.
pub const ARCADE_TRACKER_LAG_TOLERANCE: u64 = 3;
/// Rows per leg per cron pass (the R2 sweep's page; index-served).
pub const ARCADE_LEG_LIMIT: u64 = crate::tip_pass::REORG_SWEEP_LIMIT;
/// Rows per leg per block-event pass (round 2, review MED-5).
pub const ARCADE_BLOCK_EVENT_LEG_LIMIT: u64 = 100;
/// The courier ladder's budget for the re-anchor-first arm, per pass.
pub const ARCADE_LADDER_BUDGET: u32 = 20;
/// The Arcade listing GET's deadline (the pass must stay bounded in time).
pub const ARCADE_FEED_TIMEOUT_MS: u64 = 10_000;
/// The consumer's state row name.
pub const ARCADE_STATE_NAME: &str = "events";
/// Round 2 (review MED-3): the head-of-line ceilings. An event HELD this
/// many passes (chaintracks never got past it) or FAULTING this many passes
/// (a leg, header or tip read that never succeeds) is released: finished as
/// unresolved, counted and RECORDED (round 3, review MED: `unresolved` on
/// the state row and `/health/invariants.arcadeReorg.unresolved`), so the
/// queue behind it moves. A released event's rows are the OPERATOR's, not
/// the sweep's: the routine sweep walks forward and never revisits a height
/// older than its window, so the heal is `POST /internal/reorg {"fromHeight":
/// H, "toHeight": H}` for the recorded H.
pub const ARCADE_HELD_CEILING: u32 = 24;
pub const ARCADE_FAULT_CEILING: u32 = 12;

/// What one feed read handed the pass.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FeedRead {
    pub events: Vec<OrphanEvent>,
    pub rows: usize,
    pub malformed: usize,
    pub pages: u32,
    /// The newest height the listing carried: Arcade's view of the tip.
    pub newest_height: Option<u64>,
}

/// The orphan feed (Arcade's block status listing) as the pass sees it.
pub trait OrphanFeed {
    fn recent_orphans(&self) -> impl std::future::Future<Output = Result<FeedRead, String>>;
}

/// What chaintracks holds at a height: the block hash and, when the frame
/// carried it, the merkle root (the memo's seed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalHeader {
    pub hash: String,
    pub merkle_root: Option<String>,
}

/// The header source for corroboration: what chaintracks holds at a height
/// (`Ok(None)` = no header there) and its tip.
pub trait HeaderSource {
    fn hash_at(&self, height: u64) -> impl std::future::Future<Output = Result<Option<CanonicalHeader>, String>>;
    fn tip(&self) -> impl std::future::Future<Output = Result<u64, String>>;
}

/// The consumer's persisted state (one row) with its write version (round
/// 2, review LOW-2): `read` hands the version back, `write` succeeds only
/// against that version (`Ok(Some(new))`), `Ok(None)` = another isolate
/// wrote in between and this pass must stop.
pub trait ConsumerStateStore {
    fn read(&self) -> impl std::future::Future<Output = Result<(ConsumerState, Option<u64>), String>>;
    fn write(
        &self,
        state: &ConsumerState,
        expected_version: Option<u64>,
    ) -> impl std::future::Future<Output = Result<Option<u64>, String>>;
}

/// The per-pass bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PassLimits {
    pub events_per_pass: u32,
    pub leg_limit: u64,
    pub skip_depth: u64,
    pub held_ceiling: u32,
    pub fault_ceiling: u32,
    pub lag_tolerance: u64,
}

impl PassLimits {
    /// The cron's (and the operator route's) bounds.
    pub fn cron() -> Self {
        Self {
            events_per_pass: ARCADE_EVENTS_PER_PASS,
            leg_limit: ARCADE_LEG_LIMIT,
            skip_depth: ARCADE_UNCORROBORATED_SKIP_DEPTH,
            held_ceiling: ARCADE_HELD_CEILING,
            fault_ceiling: ARCADE_FAULT_CEILING,
            lag_tolerance: ARCADE_TRACKER_LAG_TOLERANCE,
        }
    }

    /// The block-event pass's bounds (round 2, review MED-5: one event, a
    /// half page per leg; the sweep and the chaser share the invocation).
    pub fn block_event() -> Self {
        Self {
            events_per_pass: ARCADE_BLOCK_EVENT_EVENTS_PER_PASS,
            leg_limit: ARCADE_BLOCK_EVENT_LEG_LIMIT,
            ..Self::cron()
        }
    }
}

impl Default for PassLimits {
    fn default() -> Self {
        Self::cron()
    }
}

/// What one pass did.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ArcadePassSummary {
    /// The feed was read this pass (once per pass, at most).
    pub feed_read: bool,
    pub feed_rows: usize,
    pub feed_malformed: usize,
    /// Events on the feed after the cursor (before the per-pass budget).
    pub feed_events: usize,
    /// Events finished by APPLYING them (every leg exhausted, no fault) and
    /// the cursor move PERSISTED (a lost or failed write counts nothing:
    /// the rows judged are idempotent to redo, the event is still pending).
    pub applied: usize,
    /// Events finished by SKIPPING them: chaintracks holds the "orphaned"
    /// hash as canonical (or serves no header there), deep below its tip.
    pub skipped_uncorroborated: usize,
    /// Events finished by RELEASING them at the head-of-line ceiling
    /// (held or faulting for too many passes): unresolved, counted and
    /// recorded; their rows are the operator's (`/internal/reorg`).
    pub skipped_unresolved: usize,
    /// The pending event released by the operator (`skipPending`).
    pub released_by_operator: usize,
    /// The pending event was HELD (chaintracks not past it yet).
    pub held: usize,
    /// The pending event was held because our header source is behind
    /// Arcade's own listing (counted apart from `held`).
    pub tracker_lagging: usize,
    /// The spenders leg, accumulated over the events this pass touched.
    pub spenders: ReverifyPassSummary,
    pub pot_beefs: ProofLegSummary,
    pub transactions: ProofLegSummary,
    /// The spenders leg stopped its page because the ladder's budget was
    /// spent (nothing judged blind of the couriers; resumed next pass).
    pub budget_stops: usize,
    /// Heights whose canonical root the corroboration read seeded into the
    /// shared memo (every row anchored there is judged without a read).
    pub memo_seeded: usize,
    /// Distinct (height, root) chaintracks reads the pass's memo made.
    pub memo_reads: usize,
    /// Read faults (feed, header source, tracker) that STOPPED the pass with
    /// the cursor where it was.
    pub faults: usize,
    /// Storage faults (state read/write, row writes).
    pub errors: usize,
    /// The state write lost its compare-and-set: another isolate ran this
    /// pass; nothing of this pass's progress was written.
    pub contended: usize,
    /// Nothing after the cursor: the feed is quiet.
    pub idle: bool,
    /// Why the pass ended before its budget, when it did.
    pub stopped: Option<String>,
    /// The cursor after the pass.
    pub cursor: Option<EventKey>,
    /// The event still pending after the pass, with its held count.
    pub pending: Option<(EventKey, u32)>,
    /// Round 3 (review MED): the released events the state row records
    /// (newest last, bounded): the heights the operator owes an
    /// `/internal/reorg` run.
    pub unresolved: Vec<UnresolvedEvent>,
}

impl ArcadePassSummary {
    /// Confirmed rows whose anchor moved without a demotion (the ladder's
    /// canonical proof replaced the refuted bump, or a standing bump's
    /// height was corrected).
    pub fn reanchored(&self) -> usize {
        self.spenders.reanchored_from_courier + self.spenders.reanchored
    }

    /// Confirmed rows demoted to SEEN (refuted, no canonical proof served).
    pub fn demoted(&self) -> usize {
        self.spenders.stale
    }

    /// Events finished (the cursor moved past them).
    pub fn events_finished(&self) -> usize {
        self.applied + self.skipped_uncorroborated + self.skipped_unresolved + self.released_by_operator
    }
}

fn add_spenders(acc: &mut ReverifyPassSummary, s: &ReverifyPassSummary) {
    acc.scanned += s.scanned;
    acc.standing += s.standing;
    acc.stale += s.stale;
    acc.demoted_blind += s.demoted_blind;
    acc.no_stored_proof += s.no_stored_proof;
    acc.reanchored += s.reanchored;
    acc.reanchored_from_courier += s.reanchored_from_courier;
    acc.stored_from_courier += s.stored_from_courier;
    acc.memo_reads += s.memo_reads;
    acc.demote_missed += s.demote_missed;
    acc.stood_on_recheck += s.stood_on_recheck;
    acc.faults += s.faults;
    acc.errors += s.errors;
    acc.budget_exhausted |= s.budget_exhausted;
}

fn add_leg(acc: &mut ProofLegSummary, s: &ProofLegSummary) {
    acc.scanned += s.scanned;
    acc.standing += s.standing;
    acc.stale += s.stale;
    acc.no_bump += s.no_bump;
    acc.faults += s.faults;
    acc.errors += s.errors;
    acc.memo_reads += s.memo_reads;
}

/// The first 16 hex of a hash for the log lines (panic-free on any input).
pub(crate) fn short(hash: &str) -> &str {
    hash.get(..16).unwrap_or(hash)
}

/// How a state write ended.
enum Persist {
    Written,
    Contended,
    Faulted,
}

/// Persist the state against the version this pass read; on success the
/// version moves with it. A lost compare-and-set (another isolate wrote in
/// between) or a write fault is counted and ends the pass; the in-memory
/// progress is dropped (every row write it carried is idempotent to redo).
async fn persist<C: ConsumerStateStore>(
    store: &C,
    state: &ConsumerState,
    version: &mut Option<u64>,
    out: &mut ArcadePassSummary,
) -> Persist {
    match store.write(state, *version).await {
        Ok(Some(new_version)) => {
            *version = Some(new_version);
            Persist::Written
        }
        Ok(None) => {
            out.contended += 1;
            out.stopped = Some("another isolate owns this pass (the state write lost its compare-and-set)".into());
            push_log("[arcade-reorg] the state write lost its compare-and-set: another isolate ran this pass; this pass's progress is redone by whoever holds the row");
            Persist::Contended
        }
        Err(e) => {
            out.errors += 1;
            out.stopped = Some(format!("state write: {e}"));
            push_log(&format!("[arcade-reorg] state write failed: this pass's progress is redone next pass: {e}"));
            Persist::Faulted
        }
    }
}

/// ONE bounded pass of the consumer. See the module doc for the contract.
/// `memo` is the pass's shared root memo (the caller hands the same one to
/// the sweep); `release_pending` finishes the pending event unresolved
/// before anything else (the operator's `skipPending`).
#[allow(clippy::too_many_arguments)] // the three sources, the two stores, the two chain sources, the bounds, the memo, the release
pub async fn consume_arcade_reorg_events<F, H, C, S>(
    feed: &F,
    headers: &H,
    state_store: &C,
    pot_storage: &dyn PotStorage,
    tx_store: Option<&S>,
    tracker: Option<&dyn ChainTracker>,
    fetcher: Option<&dyn AncestorFetcher>,
    limits: PassLimits,
    memo: &mut RootMemo,
    release_pending: bool,
) -> ArcadePassSummary
where
    F: OrphanFeed,
    H: HeaderSource,
    C: ConsumerStateStore,
    S: ProvenTxStore,
{
    let mut out = ArcadePassSummary::default();
    // No header source, no verdict of any kind: nothing is read, nothing moves.
    let Some(tracker) = tracker else {
        out.stopped = Some("no header source configured".into());
        push_log("[arcade-reorg] no header source configured: no event consumed");
        return out;
    };
    let (mut state, mut version) = match state_store.read().await {
        Ok(s) => s,
        Err(e) => {
            out.errors += 1;
            out.stopped = Some(format!("state read: {e}"));
            push_log(&format!("[arcade-reorg] state read failed: nothing consumed: {e}"));
            return out;
        }
    };
    out.cursor = state.cursor.clone();
    // ── the operator's release of the pending event ──
    if release_pending {
        if let Some(key) = state.release_pending(ReleaseReason::Operator) {
            push_log(&format!(
                "[arcade-reorg] RELEASED by the operator: {}@{} (orphanedAt {}) finished unresolved and recorded; its rows are the operator's: POST /internal/reorg {{\"fromHeight\": {}, \"toHeight\": {}}}",
                short(&key.hash),
                key.height,
                key.orphaned_at,
                key.height,
                key.height
            ));
            if !matches!(persist(state_store, &state, &mut version, &mut out).await, Persist::Written) {
                out.cursor = state.cursor.clone();
                return out;
            }
            out.released_by_operator += 1;
        }
    }
    let mut feed_events: Option<Vec<OrphanEvent>> = None;
    let mut feed_tip: Option<u64> = None;
    let mut finished = 0u32;
    while finished < limits.events_per_pass {
        // ── the head-of-line ceiling (round 2, review MED-3) ──
        if let Some(why) = state.pending_ceiling(limits.held_ceiling, limits.fault_ceiling) {
            if let Some(key) = state.release_pending(why) {
                push_log(&format!(
                    "[arcade-reorg] RELEASED at the ceiling ({why:?}): {}@{} (orphanedAt {}) finished unresolved, counted and recorded; its rows are NOT the sweep's (a height older than the sweep's window is never walked again): POST /internal/reorg {{\"fromHeight\": {}, \"toHeight\": {}}}",
                    short(&key.hash),
                    key.height,
                    key.orphaned_at,
                    key.height,
                    key.height
                ));
                if !matches!(persist(state_store, &state, &mut version, &mut out).await, Persist::Written) {
                    break;
                }
                out.skipped_unresolved += 1;
                finished += 1;
                continue;
            }
        }
        if state.pending.is_none() {
            if feed_events.is_none() {
                match feed.recent_orphans().await {
                    Ok(read) => {
                        out.feed_read = true;
                        out.feed_rows = read.rows;
                        out.feed_malformed = read.malformed;
                        feed_tip = read.newest_height;
                        let after = events_after(&read.events, state.cursor.as_ref());
                        out.feed_events = after.len();
                        feed_events = Some(after);
                    }
                    Err(e) => {
                        out.faults += 1;
                        out.stopped = Some(format!("feed read: {e}"));
                        push_log(&format!("[arcade-reorg] Arcade feed read failed: cursor unchanged: {e}"));
                        break;
                    }
                }
            }
            let next = feed_events
                .as_ref()
                .and_then(|evs| evs.iter().find(|e| state.cursor.as_ref().is_none_or(|c| e.key() > *c)))
                .cloned();
            match next {
                Some(ev) => state.start(ev),
                None => {
                    out.idle = true;
                    break;
                }
            }
        }
        let Some(pending) = state.pending.clone() else { break };
        let ev = pending.event.clone();
        // ── corroborate the hint against our header source ──
        let canonical = match headers.hash_at(ev.height).await {
            Ok(h) => h,
            Err(e) => {
                out.faults += 1;
                out.stopped = Some(format!("header read at {}: {e}", ev.height));
                push_log(&format!("[arcade-reorg] header read at {} failed: cursor unchanged: {e}", ev.height));
                state.note_fault_on_pending();
                let _ = persist(state_store, &state, &mut version, &mut out).await;
                break;
            }
        };
        let tip = match headers.tip().await {
            Ok(t) => t,
            Err(e) => {
                out.faults += 1;
                out.stopped = Some(format!("tip read: {e}"));
                push_log(&format!("[arcade-reorg] tip read failed: cursor unchanged: {e}"));
                state.note_fault_on_pending();
                let _ = persist(state_store, &state, &mut version, &mut out).await;
                break;
            }
        };
        let verdict = classify_corroboration(
            canonical.as_ref().map(|c| c.hash.as_str()),
            &ev.hash,
            tip,
            ev.height,
            limits.skip_depth,
            feed_tip,
            limits.lag_tolerance,
        );
        match verdict {
            Corroboration::TrackerLagging => {
                state.hold_pending();
                out.tracker_lagging += 1;
                push_log(&format!(
                    "[arcade-reorg] HELD {}@{}: our header source (tip {tip}) is behind Arcade's listing (newest {:?}); nothing judged while it lags",
                    short(&ev.hash),
                    ev.height,
                    feed_tip
                ));
                let _ = persist(state_store, &state, &mut version, &mut out).await;
                break;
            }
            Corroboration::Held => {
                state.hold_pending();
                out.held += 1;
                push_log(&format!(
                    "[arcade-reorg] HELD {}@{} (orphanedAt {}): chaintracks holds {} there, tip {tip}: may lag Arcade; next pass",
                    short(&ev.hash),
                    ev.height,
                    ev.orphaned_at,
                    canonical.as_ref().map_or("no header yet", |h| short(&h.hash))
                ));
                let _ = persist(state_store, &state, &mut version, &mut out).await;
                break;
            }
            Corroboration::Uncorroborated => {
                push_log(&format!(
                    "[arcade-reorg] UNCORROBORATED {}@{} (orphanedAt {}): chaintracks holds {} there, {} blocks below its tip {tip}: Arcade's row cannot be corroborated by our header source; skipped, nothing re-verified",
                    short(&ev.hash),
                    ev.height,
                    ev.orphaned_at,
                    canonical.as_ref().map_or("no header", |h| short(&h.hash)),
                    tip.saturating_sub(ev.height)
                ));
                state.advance_past_pending();
                // counted only once the cursor move PERSISTED (a lost write is
                // the other isolate's event to count)
                if !matches!(persist(state_store, &state, &mut version, &mut out).await, Persist::Written) {
                    break;
                }
                out.skipped_uncorroborated += 1;
                finished += 1;
            }
            Corroboration::Corroborated => {
                // round 2 (review MED-5): the header read already names the
                // canonical root at this height; every row anchored there
                // is judged against it without another chaintracks read
                if let Some(root) = canonical.as_ref().and_then(|c| c.merkle_root.as_deref()) {
                    if let Ok(h) = u32::try_from(ev.height) {
                        memo.seed_canonical(h, root);
                        out.memo_seeded += 1;
                    }
                }
                // ── the three legs at the orphaned height, each one bounded page ──
                let mut progress = pending;
                let h = ev.height;
                let mut faulted = false;
                let mut budget_stop = false;
                if !progress.spenders.exhausted {
                    let s = reverify_window_with(
                        pot_storage,
                        Some(tracker),
                        fetcher,
                        h,
                        h,
                        progress.spenders.after,
                        limits.leg_limit,
                        ReverifyMode { demote_proofless: false, reanchor_first: true },
                        memo,
                    )
                    .await;
                    add_spenders(&mut out.spenders, &s);
                    if s.faults > 0 || s.errors > 0 {
                        faulted = true;
                        out.stopped = Some(format!("spenders leg at {h}: faults={} errors={}", s.faults, s.errors));
                    } else {
                        progress.spenders = LegProgress { after: s.next_cursor, exhausted: s.exhausted };
                        if s.budget_exhausted {
                            budget_stop = true;
                        }
                    }
                }
                if !faulted && !budget_stop && !progress.pot_beefs.exhausted {
                    let s = reverify_pot_beefs_window_with(pot_storage, Some(tracker), h, h, progress.pot_beefs.after, limits.leg_limit, memo).await;
                    add_leg(&mut out.pot_beefs, &s);
                    if s.faults > 0 || s.errors > 0 {
                        faulted = true;
                        out.stopped = Some(format!("pot_beefs leg at {h}: faults={} errors={}", s.faults, s.errors));
                    } else {
                        progress.pot_beefs = LegProgress { after: s.next_cursor, exhausted: s.exhausted };
                    }
                }
                if !faulted && !budget_stop && !progress.transactions.exhausted {
                    match tx_store {
                        Some(store) => {
                            let s = reverify_transactions_window_with(store, Some(tracker), h, h, progress.transactions.after, limits.leg_limit, memo).await;
                            add_leg(&mut out.transactions, &s);
                            if s.faults > 0 || s.errors > 0 {
                                faulted = true;
                                out.stopped = Some(format!("transactions leg at {h}: faults={} errors={}", s.faults, s.errors));
                            } else {
                                progress.transactions = LegProgress { after: s.next_cursor, exhausted: s.exhausted };
                            }
                        }
                        // round 2 (review LOW-4): a missing transactions store is
                        // a fault, never "that leg is done"
                        None => {
                            faulted = true;
                            out.stopped = Some("no transactions store: the hop-proof leg cannot run".into());
                        }
                    }
                }
                // the legs' progress persists (a faulted leg keeps its previous cursor)
                state.pending = Some(progress.clone());
                if faulted {
                    out.faults += 1;
                    state.note_fault_on_pending();
                    push_log(&format!(
                        "[arcade-reorg] event {}@{} interrupted by a read fault: its cursor stays, replayed next pass ({})",
                        short(&ev.hash),
                        h,
                        out.stopped.as_deref().unwrap_or("?")
                    ));
                    let _ = persist(state_store, &state, &mut version, &mut out).await;
                    break;
                }
                if budget_stop {
                    out.budget_stops += 1;
                    out.stopped = Some(format!("event {}@{}: the ladder's budget is spent; the spenders leg resumes at its cursor next pass", short(&ev.hash), h));
                    push_log(&format!(
                        "[arcade-reorg] event {}@{}: the ladder's budget is spent; nothing judged blind of the couriers; the spenders leg resumes at its cursor next pass",
                        short(&ev.hash),
                        h
                    ));
                    let _ = persist(state_store, &state, &mut version, &mut out).await;
                    break;
                }
                if progress.all_exhausted() {
                    state.advance_past_pending();
                    push_log(&format!(
                        "[arcade-reorg] APPLIED {}@{} (orphanedAt {}): spenders scanned={} standing={} reanchored={} demoted={}; pot_beefs stale={}; transactions stale={}",
                        short(&ev.hash),
                        h,
                        ev.orphaned_at,
                        out.spenders.scanned,
                        out.spenders.standing,
                        out.reanchored(),
                        out.demoted(),
                        out.pot_beefs.stale,
                        out.transactions.stale
                    ));
                    // counted only once the cursor move PERSISTED
                    if !matches!(persist(state_store, &state, &mut version, &mut out).await, Persist::Written) {
                        break;
                    }
                    out.applied += 1;
                    finished += 1;
                } else {
                    // bounded: one page per leg this pass; the next pass continues
                    out.stopped = Some(format!("event {}@{} continues next pass (a leg is not exhausted)", short(&ev.hash), h));
                    let _ = persist(state_store, &state, &mut version, &mut out).await;
                    break;
                }
            }
        }
    }
    out.cursor = state.cursor.clone();
    out.pending = state.pending.as_ref().map(|p| (p.event.key(), p.held_passes));
    out.unresolved = state.unresolved.clone();
    out.memo_reads = memo.reads();
    out
}

// ── the production sources ──────────────────────────────────────────────

/// Arcade's block status listing over HTTP (`ARCADE_URL`, the same host the
/// broadcaster and the courier ladder use).
pub struct ArcadeBlockStatusFeed {
    base_url: String,
}

impl ArcadeBlockStatusFeed {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self { base_url: base_url.into().trim_end_matches('/').to_string() }
    }

    /// The page URL: the head, or the page below `before_height` (Arcade
    /// lists `block_height < before-height`; see `next_page_before`).
    pub fn page_url(&self, before_height: Option<u64>) -> String {
        match before_height {
            Some(h) => format!("{}/api/v1/blocks/processing-status?limit={ARCADE_EVENTS_PAGE_LIMIT}&before-height={h}", self.base_url),
            None => format!("{}/api/v1/blocks/processing-status?limit={ARCADE_EVENTS_PAGE_LIMIT}", self.base_url),
        }
    }
}

impl OrphanFeed for ArcadeBlockStatusFeed {
    async fn recent_orphans(&self) -> Result<FeedRead, String> {
        let mut read = FeedRead::default();
        let mut before: Option<u64> = None;
        while read.pages < ARCADE_EVENTS_PAGES {
            let url = self.page_url(before);
            let fetched = overlay_engine::gasp::race_or_deadline(
                crate::proof_fetcher::http_get(&url, None),
                crate::broadcaster::sleep_ms(ARCADE_FEED_TIMEOUT_MS),
            )
            .await;
            let (status, body) = match fetched {
                Some(Ok(r)) => r,
                Some(Err(e)) => return Err(format!("GET {url}: {e}")),
                None => return Err(format!("GET {url}: no answer within {ARCADE_FEED_TIMEOUT_MS} ms")),
            };
            if !(200..300).contains(&status) {
                let head: String = body.chars().take(160).collect();
                return Err(format!("GET {url}: HTTP {status}: {head}"));
            }
            let page = parse_block_status_page(&body).map_err(|e| format!("GET {url}: {e}"))?;
            read.pages += 1;
            read.rows += page.rows;
            read.malformed += page.malformed;
            if let Some(n) = page.newest_height {
                read.newest_height = Some(read.newest_height.map_or(n, |m| m.max(n)));
            }
            read.events.extend(page.orphans);
            // round 2 (review LOW-1): the next page re-lists the boundary
            // height (Arcade pages strictly below `before-height`; the same
            // height's other rows would be dropped otherwise); dedup by key
            match next_page_before(page.next_cursor) {
                Some(b) => before = Some(b),
                None => break,
            }
        }
        Ok(read)
    }
}

/// Chaintracks through the worker's binding for the corroborating header
/// read, and the tracker for the tip (its 30 s memo is fine here).
pub struct EnvHeaderSource<'a> {
    pub env: &'a worker::Env,
    pub tracker: &'a dyn ChainTracker,
}

impl HeaderSource for EnvHeaderSource<'_> {
    async fn hash_at(&self, height: u64) -> Result<Option<CanonicalHeader>, String> {
        crate::chain_tracker::chaintracks_block_hash_detailed(self.env, height)
            .await
            .map(|h| h.map(|(hash, merkle_root)| CanonicalHeader { hash, merkle_root }))
    }

    async fn tip(&self) -> Result<u64, String> {
        self.tracker
            .current_height()
            .await
            .map(u64::from)
            .map_err(|e| format!("chaintracks tip read: {e}"))
    }
}

/// The D1 `arcade_reorg_state` row (versioned compare-and-set writes).
pub struct D1ConsumerState<'a>(pub &'a worker::D1Database);

impl ConsumerStateStore for D1ConsumerState<'_> {
    async fn read(&self) -> Result<(ConsumerState, Option<u64>), String> {
        #[derive(serde::Deserialize)]
        struct Row {
            state: String,
            #[serde(default)]
            version: Option<f64>,
        }
        let row: Option<Row> = crate::d1::Query::new(crate::d1_discovery::arcade_reorg_state_read_sql())
            .bind(ARCADE_STATE_NAME)
            .fetch_optional(self.0)
            .await
            .map_err(|e| e.to_string())?;
        match row {
            Some(r) => Ok((ConsumerState::from_json(&r.state)?, Some(r.version.unwrap_or(0.0).max(0.0) as u64))),
            None => Ok((ConsumerState::default(), None)),
        }
    }

    async fn write(&self, state: &ConsumerState, expected_version: Option<u64>) -> Result<Option<u64>, String> {
        #[derive(serde::Deserialize)]
        struct Row {
            version: f64,
        }
        let row: Option<Row> = crate::d1::Query::new(crate::d1_discovery::arcade_reorg_state_upsert_sql())
            .bind(ARCADE_STATE_NAME)
            .bind(state.to_json())
            .bind(js_sys::Date::now())
            .bind(crate::d1_discovery::opt_f64(expected_version.map(|v| v as f64)))
            .fetch_optional(self.0)
            .await
            .map_err(|e| e.to_string())?;
        Ok(row.map(|r| r.version.max(0.0) as u64))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, reason = "test code")]
mod tests {
    use super::*;
    use crate::proof_fetcher::tests::{real_spender_raw, single_tx_bump};
    use crate::reorg_sweep::stored_bump_anchor;
    use bsv_rs::transaction::{ChainTrackerError, MockChainTracker, Transaction};
    use overlay_discovery::pot::reorg::{BumpAnchor, RowKey};
    use overlay_discovery::pot::storage::{MemoryPotStorage, PotRecord};
    use std::cell::{Cell, RefCell};
    use std::collections::HashMap;
    use std::sync::Mutex;

    // ── the 2026-09-07 facts (chaintracks + WoC agree; Arcade's rows differ) ──
    const ORPHAN_965771: &str = "0000000000000000153e10f465dba9697e4bde364fdf3a3224a736b019ffbfb1";
    const CANONICAL_965771: &str = "00000000000000001de5aa96baa3566ce66e4941f8295cc44cc85fc75949db4d";
    const CANONICAL_965772: &str = "000000000000000014d2556ff86def96cd4ce5a74eb0e26af56104f45168248e";
    const CANONICAL_965773: &str = "0000000000000000146bc084ec137a3c9608a07159128c66302051b6fe176e33";
    const EMPTY_SIBLING_965773: &str = "00000000000000000851a554167480b696b3dbd36ab1fd862f2520217988afa0";
    const CANONICAL_965774: &str = "0000000000000000167282824c6019608da4d6fc6e93eb573afc1b45bacb01cf";

    /// Arcade's listing page around the event, verbatim from the deployed
    /// instance (the same bytes `overlay_discovery` pins its parser on).
    const REAL_PAGE: &str = include_str!("../../overlay-discovery/src/pot/arcade_events_fixture_965769_965775.json");

    fn ev(stamp: &str, height: u64, hash: &str) -> OrphanEvent {
        OrphanEvent { orphaned_at: stamp.into(), height, hash: hash.into() }
    }

    /// The three real events, in Arcade's own order (the page parse).
    fn real_events() -> Vec<OrphanEvent> {
        parse_block_status_page(REAL_PAGE).unwrap().orphans
    }

    /// A feed stub: a fixed answer per call, or a fault; counts its reads;
    /// its newest height defaults to the events' max (Arcade at its tip).
    struct StubFeed {
        answer: RefCell<Result<Vec<OrphanEvent>, String>>,
        reads: Cell<usize>,
        newest: Cell<Option<u64>>,
    }
    impl StubFeed {
        fn of(events: Vec<OrphanEvent>) -> Self {
            let newest = events.iter().map(|e| e.height).max();
            Self { answer: RefCell::new(Ok(events)), reads: Cell::new(0), newest: Cell::new(newest) }
        }
        fn faulty() -> Self {
            Self { answer: RefCell::new(Err("arcade 503".into())), reads: Cell::new(0), newest: Cell::new(None) }
        }
    }
    impl OrphanFeed for StubFeed {
        async fn recent_orphans(&self) -> Result<FeedRead, String> {
            self.reads.set(self.reads.get() + 1);
            self.answer.borrow().clone().map(|events| FeedRead {
                rows: events.len() + 5,
                events,
                malformed: 0,
                pages: 1,
                newest_height: self.newest.get(),
            })
        }
    }

    /// A header source stub: canonical hash per height (missing = no
    /// header yet), optional roots, a tip, or a fault on either.
    struct StubHeaders {
        hashes: HashMap<u64, String>,
        roots: HashMap<u64, String>,
        tip: Result<u64, String>,
        hash_fault: bool,
    }
    impl StubHeaders {
        fn real(tip: u64) -> Self {
            Self {
                hashes: [
                    (965_771, CANONICAL_965771),
                    (965_772, CANONICAL_965772),
                    (965_773, CANONICAL_965773),
                    (965_774, CANONICAL_965774),
                ]
                .into_iter()
                .filter(|(h, _)| *h <= tip)
                .map(|(h, x)| (h, x.to_string()))
                .collect(),
                roots: HashMap::new(),
                tip: Ok(tip),
                hash_fault: false,
            }
        }
    }
    impl HeaderSource for StubHeaders {
        async fn hash_at(&self, height: u64) -> Result<Option<CanonicalHeader>, String> {
            if self.hash_fault {
                return Err("chaintracks 502".into());
            }
            Ok(self.hashes.get(&height).map(|hash| CanonicalHeader { hash: hash.clone(), merkle_root: self.roots.get(&height).cloned() }))
        }
        async fn tip(&self) -> Result<u64, String> {
            self.tip.clone()
        }
    }

    /// A state store stub: the document as a string (what D1 holds), its
    /// version, a write counter, optional read/write faults, and a
    /// `contend` switch that makes the next write lose its compare-and-set
    /// (another isolate wrote in between).
    #[derive(Default)]
    struct StubState {
        doc: RefCell<Option<String>>,
        version: Cell<Option<u64>>,
        writes: Cell<usize>,
        read_fault: Cell<bool>,
        write_fault: Cell<bool>,
        contend: Cell<bool>,
    }
    impl StubState {
        fn state(&self) -> ConsumerState {
            self.doc.borrow().as_deref().map_or_else(ConsumerState::default, |d| ConsumerState::from_json(d).unwrap())
        }
        fn with_doc(doc: &str) -> Self {
            let s = Self::default();
            *s.doc.borrow_mut() = Some(doc.to_string());
            s.version.set(Some(1));
            s
        }
    }
    impl ConsumerStateStore for StubState {
        async fn read(&self) -> Result<(ConsumerState, Option<u64>), String> {
            if self.read_fault.get() {
                return Err("d1 read".into());
            }
            let doc = self.doc.borrow().clone();
            let state = match doc.as_deref() {
                Some(d) => ConsumerState::from_json(d)?,
                None => ConsumerState::default(),
            };
            Ok((state, self.version.get()))
        }
        async fn write(&self, state: &ConsumerState, expected_version: Option<u64>) -> Result<Option<u64>, String> {
            if self.write_fault.get() {
                return Err("d1 write".into());
            }
            if self.contend.get() {
                // another isolate wrote first: its version moved on
                self.version.set(Some(self.version.get().unwrap_or(0) + 1));
                self.contend.set(false);
            }
            if expected_version != self.version.get() {
                return Ok(None);
            }
            self.writes.set(self.writes.get() + 1);
            *self.doc.borrow_mut() = Some(state.to_json());
            let new_version = self.version.get().unwrap_or(0) + 1;
            self.version.set(Some(new_version));
            Ok(Some(new_version))
        }
    }

    /// A tracker whose header source cannot be read.
    struct FaultyTracker;
    #[async_trait::async_trait]
    impl ChainTracker for FaultyTracker {
        async fn is_valid_root_for_height(&self, _root: &str, _height: u32) -> Result<bool, ChainTrackerError> {
            Err(ChainTrackerError::NetworkError("starved".into()))
        }
        async fn current_height(&self) -> Result<u32, ChainTrackerError> {
            Err(ChainTrackerError::NetworkError("starved".into()))
        }
    }

    /// A tracker holding a SET of (height, root) pairs that RECORDS every
    /// root asked (a single-leaf bump's root is its txid).
    struct RecordingTracker {
        valid: std::collections::HashSet<(u32, String)>,
        asked: Mutex<Vec<String>>,
    }
    #[async_trait::async_trait]
    impl ChainTracker for RecordingTracker {
        async fn is_valid_root_for_height(&self, root: &str, height: u32) -> Result<bool, ChainTrackerError> {
            self.asked.lock().unwrap().push(root.to_string());
            Ok(self.valid.contains(&(height, root.to_string())))
        }
        async fn current_height(&self) -> Result<u32, ChainTrackerError> {
            Ok(965_860)
        }
    }

    /// The courier ladder stub: a per-spender configured answer
    /// (`Ok(Some(hex))` = a chaintracks-verified proof, as the real ladder
    /// only ever answers); counts its calls; an optional budget it spends
    /// one unit per ask and reports through `budget_remaining`.
    struct CourierStub {
        answers: HashMap<String, Result<Option<String>, String>>,
        calls: Cell<usize>,
        budget: Cell<Option<u32>>,
    }
    impl CourierStub {
        fn none() -> Self {
            Self { answers: HashMap::new(), calls: Cell::new(0), budget: Cell::new(None) }
        }
        fn with(answers: HashMap<String, Result<Option<String>, String>>) -> Self {
            Self { answers, calls: Cell::new(0), budget: Cell::new(None) }
        }
    }
    #[async_trait::async_trait(?Send)]
    impl AncestorFetcher for CourierStub {
        async fn fetch_ancestor(&self, _txid: &str) -> Result<overlay_engine::gasp::FetchedAncestor, overlay_engine::gasp::GASPError> {
            Err(overlay_engine::gasp::GASPError::NodeNotFound("stub".into()))
        }
        async fn verified_proof_for_detailed(&self, txid: &str) -> Result<Option<String>, String> {
            self.calls.set(self.calls.get() + 1);
            if let Some(b) = self.budget.get() {
                if b == 0 {
                    return Ok(None); // the real ladder's budget refusal shape
                }
                self.budget.set(Some(b - 1));
            }
            self.answers.get(txid).cloned().unwrap_or(Ok(None))
        }
        fn budget_remaining(&self) -> Option<u32> {
            self.budget.get()
        }
    }

    /// Round 2 (review MED-1): a ladder that LANDS the canonical proof into
    /// the store itself before answering it (the `/arc-ingest` webhook or
    /// another isolate getting there between the pass's page read and its
    /// ask), or moves the row to a NEW height and then answers nothing.
    struct RacingLadder<'a> {
        store: &'a MemoryPotStorage,
        spender: String,
        proof: String,
        answer_after_landing: bool,
        calls: Cell<usize>,
    }
    #[async_trait::async_trait(?Send)]
    impl AncestorFetcher for RacingLadder<'_> {
        async fn fetch_ancestor(&self, _txid: &str) -> Result<overlay_engine::gasp::FetchedAncestor, overlay_engine::gasp::GASPError> {
            Err(overlay_engine::gasp::GASPError::NodeNotFound("stub".into()))
        }
        async fn verified_proof_for_detailed(&self, txid: &str) -> Result<Option<String>, String> {
            self.calls.set(self.calls.get() + 1);
            if txid == self.spender {
                // the concurrent writer lands the verified proof first
                let _ = crate::proof_fetcher::apply_pushed_proof_to_pot_stores(self.store, &self.spender, &self.proof).await;
                return Ok(self.answer_after_landing.then(|| self.proof.clone()));
            }
            Ok(None)
        }
    }

    /// (rowid, txid, beef, proofHeight, has_proof)
    type TxRow = (i64, String, Vec<u8>, Option<u64>, bool);

    /// A memory twin of the transactions leg store (the sweep's shape).
    #[derive(Default)]
    struct MemoryTxStore {
        rows: Mutex<Vec<TxRow>>,
    }
    impl MemoryTxStore {
        fn insert(&self, txid: &str, beef: Vec<u8>, height: Option<u64>, proven: bool) {
            let mut rows = self.rows.lock().unwrap();
            let rowid = rows.len() as i64 + 1;
            rows.push((rowid, txid.to_string(), beef, height, proven));
        }
        fn proven(&self, txid: &str) -> bool {
            self.rows.lock().unwrap().iter().any(|r| r.1 == txid && r.4)
        }
    }
    impl ProvenTxStore for MemoryTxStore {
        async fn proven_window_page(&self, lo: u64, hi: u64, after: Option<RowKey>, limit: u64) -> Result<Vec<(RowKey, String, Vec<u8>)>, String> {
            let mut page: Vec<(RowKey, String, Vec<u8>)> = self
                .rows
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r.4 && r.3.is_some_and(|h| h >= lo && h <= hi))
                .map(|r| (RowKey { height: r.3.unwrap(), rowid: r.0 }, r.1.clone(), r.2.clone()))
                .collect();
            page.sort_by_key(|(k, _, _)| std::cmp::Reverse((k.height, k.rowid)));
            if let Some(a) = after {
                page.retain(|(k, _, _)| k.height < a.height || (k.height == a.height && k.rowid < a.rowid));
            }
            page.truncate(limit as usize);
            Ok(page)
        }
        async fn unprove(&self, txid: &str) -> Result<(), String> {
            for r in self.rows.lock().unwrap().iter_mut() {
                if r.1 == txid {
                    r.4 = false;
                }
            }
            Ok(())
        }
    }

    fn pot(n: u32) -> String {
        format!("{n:064x}")
    }

    /// A pot CONFIRMED at `height` by a spender whose stored pot BEEF carries
    /// a single-leaf bump at that height (root = the spender txid), latched
    /// verified with its anchor. Returns the spender txid.
    async fn confirmed_pot_with_stored_proof(store: &MemoryPotStorage, pot: &str, height: u32) -> String {
        confirmed_pot_with_proof_at(store, pot, height, true).await
    }

    /// `anchored = false` latches the spender's proof WITHOUT its
    /// `proofHeight`: the pot_beefs leg never pages it, so a pin about the
    /// spenders leg alone can count the header source's questions (the
    /// R2 pins' device; a stored spender BEEF is a `pot_beefs` row too).
    async fn confirmed_pot_with_proof_at(store: &MemoryPotStorage, pot: &str, height: u32, anchored: bool) -> String {
        let raw = real_spender_raw(pot, 0);
        let spender = Transaction::from_hex(&raw).unwrap().id();
        store.store_record(&PotRecord { txid: pot.into(), output_index: 0, ..Default::default() }).await.unwrap();
        store.mark_spent(pot, 0, &spender, true, None, Some(u64::from(height)), Some(true)).await.unwrap();
        let bump_hex = single_tx_bump(&spender, height).to_hex();
        let beef = crate::proof_fetcher::assemble_spender_beef(&raw, &bump_hex, &spender).unwrap();
        store.store_beef(&spender, &beef).await.unwrap();
        store.mark_pot_beef_proven_at(&spender, anchored.then_some(u64::from(height))).await.unwrap();
        spender
    }

    fn limits(events: u32, leg: u64) -> PassLimits {
        PassLimits { events_per_pass: events, leg_limit: leg, ..PassLimits::cron() }
    }

    /// One pass with a fresh memo and no operator release (the common call).
    #[allow(clippy::too_many_arguments)]
    async fn run<F: OrphanFeed, H: HeaderSource, C: ConsumerStateStore, S: ProvenTxStore>(
        feed: &F,
        headers: &H,
        state: &C,
        store: &MemoryPotStorage,
        txs: Option<&S>,
        tracker: Option<&dyn ChainTracker>,
        fetcher: Option<&dyn AncestorFetcher>,
        lim: PassLimits,
    ) -> ArcadePassSummary {
        consume_arcade_reorg_events(feed, headers, state, store, txs, tracker, fetcher, lim, &mut RootMemo::default(), false).await
    }

    /// The 2026-09-07 event, consumed: the 965771 orphan (corroborated: our
    /// header source holds the canonical block there) re-verifies the rows
    /// confirmed at 965771: the one the ladder can re-prove is RE-ANCHORED
    /// IN PLACE (never demoted, its stored bump and height replaced), the
    /// one no courier can prove is DEMOTED and unlatched, the one whose
    /// stored bump still verifies STANDS, and a row confirmed at another
    /// height is never examined. The cursor moves only once every leg drained.
    #[tokio::test]
    async fn a_corroborated_orphan_event_reanchors_reproven_rows_in_place_and_demotes_the_rest() {
        let store = MemoryPotStorage::new();
        let mut tracker = MockChainTracker::new(965_860);
        let standing = confirmed_pot_with_stored_proof(&store, &pot(1), 965_771).await;
        tracker.add_root(965_771, standing.clone());
        let reproven = confirmed_pot_with_stored_proof(&store, &pot(2), 965_771).await;
        tracker.add_root(965_773, reproven.clone()); // re-mined in the canonical 965773
        let orphaned_only = confirmed_pot_with_stored_proof(&store, &pot(3), 965_771).await;
        let elsewhere = confirmed_pot_with_stored_proof(&store, &pot(4), 965_774).await; // refuted, but not at the event's height
        let ladder = CourierStub::with([(reproven.clone(), Ok(Some(single_tx_bump(&reproven, 965_773).to_hex())))].into_iter().collect());
        let feed = StubFeed::of(real_events());
        let headers = StubHeaders::real(965_860);
        let state = StubState::default();
        let txs = MemoryTxStore::default();
        let s = run(&feed, &headers, &state, &store, Some(&txs), Some(&tracker), Some(&ladder), limits(1, 50)).await;
        assert_eq!((s.applied, s.skipped_uncorroborated, s.held, s.faults, s.errors), (1, 0, 0, 0, 0), "{s:?}");
        assert_eq!((s.spenders.scanned, s.spenders.standing, s.reanchored(), s.demoted()), (3, 1, 1, 1), "{s:?}");
        assert_eq!(ladder.calls.get(), 2, "the ladder was asked once per REFUTED row, never for the standing one");
        // standing: untouched
        let r = store.get_spent_status(&pot(1), 0).await.unwrap().unwrap();
        assert!(r.spent_confirmed && r.spent_height == Some(965_771));
        // re-proven: still confirmed, now at 965773, the stored bump replaced by the canonical one, the latch kept
        let r = store.get_spent_status(&pot(2), 0).await.unwrap().unwrap();
        assert!(r.spent_confirmed, "a re-anchor never passes through SEEN");
        assert_eq!(r.spent_height, Some(965_773));
        assert_eq!(stored_bump_anchor(&store.get_beef(&reproven).await.unwrap().unwrap(), &reproven), Some(BumpAnchor { height: 965_773, root: reproven.clone() }));
        assert!(store.pot_beef_proof_verified(&reproven).await.unwrap());
        // orphaned only: SEEN again, the pointer and the witness kept, the proof unlatched, the bytes kept
        let r = store.get_spent_status(&pot(3), 0).await.unwrap().unwrap();
        assert!(r.spent && !r.spent_confirmed && r.spent_height.is_none());
        assert_eq!(r.spending_txid.as_deref(), Some(orphaned_only.as_str()));
        assert!(!store.pot_beef_proof_verified(&orphaned_only).await.unwrap());
        assert!(store.get_beef(&orphaned_only).await.unwrap().is_some());
        // another height: never examined by this event
        assert!(store.get_spent_status(&pot(4), 0).await.unwrap().unwrap().spent_confirmed);
        assert!(store.pot_beef_proof_verified(&elsewhere).await.unwrap());
        // the cursor is the applied event, nothing pending
        let key = real_events().iter().find(|e| e.hash == ORPHAN_965771).unwrap().key();
        assert_eq!(state.state().cursor, Some(key.clone()));
        assert_eq!(state.state().pending, None);
        assert_eq!(s.cursor, Some(key));
        assert_eq!(s.pending, None);
    }

    /// Round 2 (review MED-1): the demotion the consumer would have written
    /// on a row ANOTHER WRITER re-anchored between its page read and its
    /// ladder ask. The store already holds the ladder's verified anchor
    /// when the ask returns (nothing lands): the row STANDS, latch kept,
    /// counted as re-anchored. And a row moved to a NEW height under the
    /// pass with no proof served: the height-bound demotion MISSES.
    #[tokio::test]
    async fn a_concurrent_reanchor_between_the_read_and_the_ladder_ask_never_demotes() {
        // (a) the same-height competitor case: the concurrent writer lands a
        // two-leaf bump at 965771 (another root, the same height), the
        // ladder then answers that same proof
        let store = MemoryPotStorage::new();
        let tracker = MockChainTracker::new(965_860);
        let spender = confirmed_pot_with_stored_proof(&store, &pot(90), 965_771).await;
        let sibling = "cd".repeat(32);
        let two_leaf = bsv_rs::transaction::MerklePath::new(
            965_771,
            vec![vec![
                bsv_rs::transaction::MerklePathLeaf::new_txid(0, spender.clone()),
                bsv_rs::transaction::MerklePathLeaf::new(1, sibling),
            ]],
        )
        .unwrap();
        let canonical_root = two_leaf.compute_root(Some(&spender)).unwrap().to_ascii_lowercase();
        let mut tracker_two = MockChainTracker::new(965_860);
        tracker_two.add_root(965_771, canonical_root.clone());
        let _ = tracker;
        let ladder = RacingLadder { store: &store, spender: spender.clone(), proof: two_leaf.to_hex(), answer_after_landing: true, calls: Cell::new(0) };
        let only_965771 = vec![real_events().into_iter().find(|e| e.hash == ORPHAN_965771).unwrap()];
        let state = StubState::default();
        let txs = MemoryTxStore::default();
        let s = run(&StubFeed::of(only_965771.clone()), &StubHeaders::real(965_860), &state, &store, Some(&txs), Some(&tracker_two), Some(&ladder), limits(2, 50)).await;
        assert_eq!((s.applied, s.demoted(), s.reanchored(), s.spenders.demote_missed, s.faults), (1, 0, 1, 0, 0), "{s:?}");
        assert_eq!(ladder.calls.get(), 1);
        let r = store.get_spent_status(&pot(90), 0).await.unwrap().unwrap();
        assert!(r.spent_confirmed && r.spent_height == Some(965_771), "the row STANDS: {r:?}");
        assert!(store.pot_beef_proof_verified(&spender).await.unwrap(), "the latch on a CANONICAL BEEF is kept");
        assert_eq!(stored_bump_anchor(&store.get_beef(&spender).await.unwrap().unwrap(), &spender), Some(BumpAnchor { height: 965_771, root: canonical_root }));
        // (b) the new-height case with NO proof served after the landing (the
        // writer replaced the bump AND moved the row to 965773): the round-3
        // re-check sees the replaced, verifying bump and the row STANDS
        let store2 = MemoryPotStorage::new();
        let mut tracker_new = MockChainTracker::new(965_860);
        let spender2 = confirmed_pot_with_stored_proof(&store2, &pot(91), 965_771).await;
        tracker_new.add_root(965_773, spender2.clone());
        let ladder2 = RacingLadder { store: &store2, spender: spender2.clone(), proof: single_tx_bump(&spender2, 965_773).to_hex(), answer_after_landing: false, calls: Cell::new(0) };
        let state2 = StubState::default();
        let s2 = run(&StubFeed::of(only_965771.clone()), &StubHeaders::real(965_860), &state2, &store2, Some(&txs), Some(&tracker_new), Some(&ladder2), limits(2, 50)).await;
        assert_eq!((s2.applied, s2.demoted(), s2.spenders.stood_on_recheck, s2.spenders.demote_missed), (1, 0, 1, 0), "{s2:?}");
        let r = store2.get_spent_status(&pot(91), 0).await.unwrap().unwrap();
        assert!(r.spent_confirmed && r.spent_height == Some(965_773), "moved under the pass, never demoted: {r:?}");
        assert!(store2.pot_beef_proof_verified(&spender2).await.unwrap());
        // (c) the height moved under the pass WITHOUT the bump changing (a writer that
        // only re-anchored the row): the re-check holds the judged verdict and the
        // height-bound demotion MISSES; the row stays confirmed at its new height
        struct HeightMover<'a> {
            store: &'a MemoryPotStorage,
            pot: String,
            spender: String,
        }
        #[async_trait::async_trait(?Send)]
        impl AncestorFetcher for HeightMover<'_> {
            async fn fetch_ancestor(&self, _txid: &str) -> Result<overlay_engine::gasp::FetchedAncestor, overlay_engine::gasp::GASPError> {
                Err(overlay_engine::gasp::GASPError::NodeNotFound("stub".into()))
            }
            async fn verified_proof_for_detailed(&self, txid: &str) -> Result<Option<String>, String> {
                if txid == self.spender {
                    let _ = self.store.reanchor_confirmed_for_spender(&self.pot, 0, &self.spender, 965_773).await;
                }
                Ok(None)
            }
        }
        let store3 = MemoryPotStorage::new();
        let spender3 = confirmed_pot_with_stored_proof(&store3, &pot(92), 965_771).await;
        let mover = HeightMover { store: &store3, pot: pot(92), spender: spender3.clone() };
        let state3 = StubState::default();
        let s3 = run(&StubFeed::of(only_965771), &StubHeaders::real(965_860), &state3, &store3, Some(&txs), Some(&MockChainTracker::new(965_860)), Some(&mover), limits(2, 50)).await;
        assert_eq!((s3.applied, s3.demoted(), s3.spenders.stood_on_recheck, s3.spenders.demote_missed), (1, 0, 0, 1), "{s3:?}");
        let r = store3.get_spent_status(&pot(92), 0).await.unwrap().unwrap();
        assert!(r.spent_confirmed && r.spent_height == Some(965_773), "the height-bound guard missed, nothing written: {r:?}");
    }

    /// Round 2 (review MED-2): a spent ladder budget stops the spenders
    /// leg's page BEFORE the row that needs it; nothing is demoted blind of
    /// the couriers; the leg's cursor is the last row judged and the event
    /// stays pending; a fresh budget resumes at that row. Pinned through the
    /// REAL fetcher at budget 0 and the stub at budget 1.
    #[tokio::test]
    async fn a_spent_ladder_budget_stops_the_page_and_demotes_nothing_blind() {
        let store = MemoryPotStorage::new();
        let mut tracker = MockChainTracker::new(965_860);
        // walked newest rowid first: the refuted row is inserted LAST so the
        // standing one is judged before the budget stops the page
        let refuted_a = confirmed_pot_with_stored_proof(&store, &pot(95), 965_771).await;
        let standing = confirmed_pot_with_stored_proof(&store, &pot(96), 965_771).await;
        tracker.add_root(965_771, standing.clone());
        let refuted_b = confirmed_pot_with_stored_proof(&store, &pot(97), 965_771).await;
        let only_965771 = vec![real_events().into_iter().find(|e| e.hash == ORPHAN_965771).unwrap()];
        let txs = MemoryTxStore::default();
        // the REAL ladder at budget 0: no courier is asked, nothing is demoted
        let real = crate::proof_fetcher::ChainProofFetcher::new(Some(std::rc::Rc::new(MockChainTracker::new(965_860)))).with_budget(0);
        let state = StubState::default();
        let s = run(&StubFeed::of(only_965771.clone()), &StubHeaders::real(965_860), &state, &store, Some(&txs), Some(&tracker), Some(&real), limits(2, 50)).await;
        assert_eq!((s.budget_stops, s.demoted(), s.reanchored(), s.applied, s.faults), (1, 0, 0, 0, 0), "{s:?}");
        assert_eq!(s.spenders.scanned, 3, "the page was read; judged up to the budget stop");
        for p in [95, 96, 97] {
            assert!(store.get_spent_status(&pot(p), 0).await.unwrap().unwrap().spent_confirmed, "pot {p} untouched");
        }
        let pending = state.state().pending.unwrap();
        assert_eq!(pending.event.hash, ORPHAN_965771, "the event stays pending");
        assert!(!pending.spenders.exhausted);
        assert_eq!(pending.spenders.after, None, "the first row (refuted_b, newest) needed the ladder: nothing judged before it");
        // the stub at budget 1: refuted_b is asked (no proof: demoted), the
        // standing row is judged, refuted_a stops the page; the cursor is
        // the standing row (the last judged); the next pass (budget 1
        // again) finishes refuted_a and the event
        let store2 = MemoryPotStorage::new();
        let mut tracker2 = MockChainTracker::new(965_860);
        let a = confirmed_pot_with_stored_proof(&store2, &pot(95), 965_771).await;
        let st = confirmed_pot_with_stored_proof(&store2, &pot(96), 965_771).await;
        tracker2.add_root(965_771, st.clone());
        let b = confirmed_pot_with_stored_proof(&store2, &pot(97), 965_771).await;
        let stub = CourierStub::none();
        stub.budget.set(Some(1));
        let state2 = StubState::default();
        let s2 = run(&StubFeed::of(only_965771.clone()), &StubHeaders::real(965_860), &state2, &store2, Some(&txs), Some(&tracker2), Some(&stub), limits(2, 50)).await;
        assert_eq!((s2.budget_stops, s2.demoted(), s2.spenders.standing, s2.applied), (1, 1, 1, 0), "{s2:?}");
        assert_eq!(stub.calls.get(), 1, "one ask, then the budget is spent");
        assert!(!store2.get_spent_status(&pot(97), 0).await.unwrap().unwrap().spent_confirmed, "b asked and demoted");
        assert!(store2.get_spent_status(&pot(95), 0).await.unwrap().unwrap().spent_confirmed, "a NOT demoted blind");
        let pending = state2.state().pending.unwrap();
        assert!(pending.spenders.after.is_some() && !pending.spenders.exhausted, "the cursor is the last row judged: {pending:?}");
        stub.budget.set(Some(1));
        let s3 = run(&StubFeed::of(only_965771), &StubHeaders::real(965_860), &state2, &store2, Some(&txs), Some(&tracker2), Some(&stub), limits(2, 50)).await;
        assert_eq!((s3.applied, s3.demoted(), s3.spenders.scanned), (1, 1, 1), "resumed at the cursor: a alone: {s3:?}");
        assert!(!store2.get_spent_status(&pot(95), 0).await.unwrap().unwrap().spent_confirmed);
        let _ = (refuted_a, refuted_b, a, b);
    }

    /// Idempotent replay: the applied event fed again (the cursor reset to
    /// before it, as a fresh isolate or a lost write would) changes nothing
    /// more; and past the cursor, the same page yields no event (the feed
    /// is quiet: `idle`, no writes).
    #[tokio::test]
    async fn replaying_an_applied_event_changes_nothing_and_a_consumed_page_is_idle() {
        let store = MemoryPotStorage::new();
        let mut tracker = MockChainTracker::new(965_860);
        let standing = confirmed_pot_with_stored_proof(&store, &pot(10), 965_771).await;
        tracker.add_root(965_771, standing.clone());
        let _orphaned_only = confirmed_pot_with_stored_proof(&store, &pot(11), 965_771).await;
        let only_965771 = vec![real_events().into_iter().find(|e| e.hash == ORPHAN_965771).unwrap()];
        let feed = StubFeed::of(only_965771.clone());
        let headers = StubHeaders::real(965_860);
        let state = StubState::default();
        let txs = MemoryTxStore::default();
        let first = run(&feed, &headers, &state, &store, Some(&txs), Some(&tracker), Some(&CourierStub::none()), limits(2, 50)).await;
        assert_eq!((first.applied, first.demoted(), first.spenders.standing), (1, 1, 1), "{first:?}");
        assert!(!store.get_spent_status(&pot(11), 0).await.unwrap().unwrap().spent_confirmed);
        let writes_after_first = state.writes.get();
        // the same feed again: nothing after the cursor
        let second = run(&feed, &headers, &state, &store, Some(&txs), Some(&tracker), Some(&CourierStub::none()), limits(2, 50)).await;
        assert!(second.idle, "{second:?}");
        assert_eq!((second.applied, second.demoted(), second.spenders.scanned, second.faults), (0, 0, 0, 0));
        assert_eq!(state.writes.get(), writes_after_first, "an idle pass writes nothing");
        assert_eq!(second.cursor, first.cursor);
        // a replay from BEFORE the cursor (a fresh document): the event applies again as a no-op
        *state.doc.borrow_mut() = None;
        state.version.set(None);
        let replay = run(&feed, &headers, &state, &store, Some(&txs), Some(&tracker), Some(&CourierStub::none()), limits(2, 50)).await;
        assert_eq!((replay.applied, replay.demoted(), replay.reanchored(), replay.spenders.scanned, replay.spenders.standing), (1, 0, 0, 1, 1), "{replay:?}");
        assert!(store.get_spent_status(&pot(10), 0).await.unwrap().unwrap().spent_confirmed, "the standing row still stands");
        assert!(!store.get_spent_status(&pot(11), 0).await.unwrap().unwrap().spent_confirmed, "the demoted row is not touched again (it is no longer confirmed)");
        assert_eq!(state.state().cursor, first.cursor);
    }

    /// Arcade's rows for 965773 (the real ones): the canonical block it
    /// holds `orphaned` is UNCORROBORATED (chaintracks holds that hash deep
    /// below its tip): skipped, counted, the rows at 965773 never examined;
    /// its near-empty sibling is corroborated and applied (nothing there).
    /// With chaintracks near the tip the same row is HELD: the cursor stays,
    /// the next event is not started, nothing is examined.
    #[tokio::test]
    async fn an_uncorroborated_event_is_skipped_when_deep_and_held_when_near_the_tip_and_examines_nothing() {
        let store = MemoryPotStorage::new();
        let mut tracker = MockChainTracker::new(965_860);
        let mined_965773 = confirmed_pot_with_stored_proof(&store, &pot(20), 965_773).await;
        tracker.add_root(965_773, mined_965773.clone());
        let _refuted_965773 = confirmed_pot_with_stored_proof(&store, &pot(21), 965_773).await; // in no canonical block
        let two_965773: Vec<OrphanEvent> = real_events().into_iter().filter(|e| e.height == 965_773).collect();
        assert_eq!(two_965773.len(), 2);
        // DEEP: chaintracks at 965860 holds the canonical 965773 = Arcade's "orphaned" hash
        let feed = StubFeed::of(two_965773.clone());
        let headers = StubHeaders::real(965_860);
        let state = StubState::default();
        let txs = MemoryTxStore::default();
        let s = run(&feed, &headers, &state, &store, Some(&txs), Some(&tracker), Some(&CourierStub::none()), limits(2, 50)).await;
        assert_eq!((s.skipped_uncorroborated, s.applied, s.held, s.faults), (1, 1, 0, 0), "{s:?}");
        assert_eq!(s.events_finished(), 2);
        // the skipped event examined NOTHING: the refuted row at 965773 is still confirmed after the skip …
        // … but the corroborated sibling event (a real orphan at the same height) examined the height and demoted it
        assert_eq!((s.spenders.scanned, s.spenders.standing, s.demoted()), (2, 1, 1), "{s:?}");
        assert!(store.get_spent_status(&pot(20), 0).await.unwrap().unwrap().spent_confirmed);
        assert!(!store.get_spent_status(&pot(21), 0).await.unwrap().unwrap().spent_confirmed);
        assert_eq!(state.state().cursor, Some(two_965773.iter().max_by_key(|e| e.key()).unwrap().key()));
        // an ONLY-uncorroborated feed examines nothing at all
        let store2 = MemoryPotStorage::new();
        let planted = confirmed_pot_with_stored_proof(&store2, &pot(22), 965_773).await;
        let bogus = vec![two_965773.iter().find(|e| e.hash == CANONICAL_965773).unwrap().clone()];
        let feed2 = StubFeed::of(bogus.clone());
        let state2 = StubState::default();
        let s2 = run(&feed2, &headers, &state2, &store2, Some(&txs), Some(&tracker), Some(&CourierStub::none()), limits(2, 50)).await;
        assert_eq!((s2.skipped_uncorroborated, s2.spenders.scanned, s2.demoted()), (1, 0, 0), "{s2:?}");
        assert!(store2.get_spent_status(&pot(22), 0).await.unwrap().unwrap().spent_confirmed, "a wrong Arcade row demotes nothing");
        assert!(store2.pot_beef_proof_verified(&planted).await.unwrap());
        // NEAR THE TIP (965773..965775): held, the cursor stays, nothing examined, nothing started after it
        for tip in [965_773u64, 965_774, 965_775] {
            let state3 = StubState::default();
            let feed3 = StubFeed::of(two_965773.clone());
            feed3.newest.set(Some(tip)); // Arcade at the same tip: no lag
            let s3 = run(&feed3, &StubHeaders::real(tip), &state3, &store2, Some(&txs), Some(&tracker), Some(&CourierStub::none()), limits(2, 50)).await;
            assert_eq!((s3.held, s3.skipped_uncorroborated, s3.applied, s3.spenders.scanned), (1, 0, 0, 0), "tip {tip}: {s3:?}");
            assert_eq!(state3.state().cursor, None, "tip {tip}: the cursor stays");
            let pending = state3.state().pending.unwrap();
            assert_eq!((pending.event.hash.as_str(), pending.held_passes), (CANONICAL_965773, 1));
            assert!(store2.get_spent_status(&pot(22), 0).await.unwrap().unwrap().spent_confirmed);
        }
        // no header at that height yet, near the tip (chaintracks one behind): held too
        let mut lagging = StubHeaders::real(965_772);
        lagging.tip = Ok(965_772);
        let feed4 = StubFeed::of(bogus);
        feed4.newest.set(Some(965_773));
        let state4 = StubState::default();
        let s4 = run(&feed4, &lagging, &state4, &store2, Some(&txs), Some(&tracker), Some(&CourierStub::none()), limits(2, 50)).await;
        assert_eq!((s4.held, s4.spenders.scanned), (1, 0), "{s4:?}");
        assert_eq!(state4.state().cursor, None);
    }

    /// Round 2 (review LOW-3): chaintracks more than the tolerance behind
    /// Arcade's own listing holds EVERY event (even a corroborable one) and
    /// is counted apart from an uncorroborated event; nothing is examined.
    #[tokio::test]
    async fn a_tracker_behind_arcades_listing_holds_and_is_counted_apart() {
        let store = MemoryPotStorage::new();
        let mut tracker = MockChainTracker::new(965_850);
        let refuted = confirmed_pot_with_stored_proof(&store, &pot(23), 965_771).await;
        let _ = &mut tracker;
        let only_965771 = vec![real_events().into_iter().find(|e| e.hash == ORPHAN_965771).unwrap()];
        let feed = StubFeed::of(only_965771);
        feed.newest.set(Some(965_860)); // Arcade lists ten blocks our header source has not reached
        let mut headers = StubHeaders::real(965_850);
        headers.tip = Ok(965_850);
        let state = StubState::default();
        let txs = MemoryTxStore::default();
        let s = run(&feed, &headers, &state, &store, Some(&txs), Some(&tracker), Some(&CourierStub::none()), limits(2, 50)).await;
        assert_eq!((s.tracker_lagging, s.held, s.skipped_uncorroborated, s.applied, s.spenders.scanned), (1, 0, 0, 0, 0), "{s:?}");
        assert!(store.get_spent_status(&pot(23), 0).await.unwrap().unwrap().spent_confirmed);
        assert!(store.pot_beef_proof_verified(&refuted).await.unwrap());
        assert_eq!(state.state().cursor, None);
        assert_eq!(state.state().pending.unwrap().held_passes, 1, "a lag hold counts toward the ceiling too");
    }

    /// A fault of ANY kind leaves the cursor where it was and changes no
    /// row: the feed unreadable, the header source unreadable, the tip
    /// unreadable, the tracker faulting inside a leg (the leg's cursor
    /// stays too), the state row unreadable or unwritable, no header
    /// source, no transactions store (round 2, review LOW-4).
    #[tokio::test]
    async fn every_fault_leaves_the_cursor_and_changes_nothing_and_counts() {
        let store = MemoryPotStorage::new();
        let mut tracker = MockChainTracker::new(965_860);
        let standing = confirmed_pot_with_stored_proof(&store, &pot(30), 965_771).await;
        tracker.add_root(965_771, standing.clone());
        let refuted = confirmed_pot_with_stored_proof(&store, &pot(31), 965_771).await;
        let events = real_events();
        let txs = MemoryTxStore::default();
        let still_confirmed = || async {
            let a = store.get_spent_status(&pot(30), 0).await.unwrap().unwrap().spent_confirmed;
            let b = store.get_spent_status(&pot(31), 0).await.unwrap().unwrap().spent_confirmed;
            a && b && store.pot_beef_proof_verified(&refuted).await.unwrap()
        };
        // the feed
        let state = StubState::default();
        let s = run(&StubFeed::faulty(), &StubHeaders::real(965_860), &state, &store, Some(&txs), Some(&tracker), Some(&CourierStub::none()), limits(2, 50)).await;
        assert_eq!((s.faults, s.applied, s.spenders.scanned), (1, 0, 0), "{s:?}");
        assert!(s.stopped.as_deref().unwrap().starts_with("feed read"));
        assert_eq!(state.state(), ConsumerState::default());
        assert_eq!(state.writes.get(), 0);
        assert!(still_confirmed().await);
        // the header source: the event is pending, its fault counted toward the ceiling
        let mut faulty_headers = StubHeaders::real(965_860);
        faulty_headers.hash_fault = true;
        let s = run(&StubFeed::of(events.clone()), &faulty_headers, &state, &store, Some(&txs), Some(&tracker), Some(&CourierStub::none()), limits(2, 50)).await;
        assert_eq!((s.faults, s.applied, s.spenders.scanned), (1, 0, 0), "{s:?}");
        assert_eq!(state.state().pending.as_ref().map(|p| p.fault_passes), Some(1));
        assert_eq!(state.state().cursor, None);
        assert!(still_confirmed().await);
        // the tip
        let mut no_tip = StubHeaders::real(965_860);
        no_tip.tip = Err("tip 502".into());
        let s = run(&StubFeed::of(events.clone()), &no_tip, &state, &store, Some(&txs), Some(&tracker), Some(&CourierStub::none()), limits(2, 50)).await;
        assert_eq!((s.faults, s.applied), (1, 0), "{s:?}");
        assert_eq!(state.state().pending.as_ref().map(|p| p.fault_passes), Some(2));
        assert!(still_confirmed().await);
        // the tracker inside the spenders leg: examined, judged nothing, the leg's cursor not persisted
        let s = run(&StubFeed::of(events.clone()), &StubHeaders::real(965_860), &state, &store, Some(&txs), Some(&FaultyTracker), Some(&CourierStub::none()), limits(2, 50)).await;
        assert_eq!((s.faults, s.applied, s.spenders.scanned, s.spenders.faults, s.demoted()), (1, 0, 2, 2, 0), "{s:?}");
        assert!(still_confirmed().await);
        let pending = state.state().pending.unwrap();
        assert_eq!(pending.event.hash, ORPHAN_965771, "the event stays pending");
        assert_eq!(pending.spenders, LegProgress::default(), "the faulted leg's cursor did not move");
        assert_eq!(pending.fault_passes, 3);
        assert_eq!(state.state().cursor, None);
        // the state row unreadable: nothing read, nothing examined
        state.read_fault.set(true);
        let s = run(&StubFeed::of(events.clone()), &StubHeaders::real(965_860), &state, &store, Some(&txs), Some(&tracker), Some(&CourierStub::none()), limits(2, 50)).await;
        assert_eq!((s.errors, s.applied, s.spenders.scanned), (1, 0, 0), "{s:?}");
        state.read_fault.set(false);
        // the state row unwritable: the rows ARE judged (idempotent writes) but the cursor cannot
        // move, and an event whose cursor move did not persist is NOT counted as applied
        state.write_fault.set(true);
        let writes = state.writes.get();
        let s = run(&StubFeed::of(events.clone()), &StubHeaders::real(965_860), &state, &store, Some(&txs), Some(&tracker), Some(&CourierStub::none()), limits(2, 50)).await;
        assert_eq!((s.errors, s.applied, s.demoted()), (1, 0, 1), "{s:?}");
        assert_eq!(state.writes.get(), writes);
        assert_eq!(state.state().cursor, None, "the persisted cursor did not move");
        state.write_fault.set(false);
        // no header source at all: nothing consumed
        let s = consume_arcade_reorg_events::<_, _, _, MemoryTxStore>(&StubFeed::of(events.clone()), &StubHeaders::real(965_860), &state, &store, None, None, None, limits(2, 50), &mut RootMemo::default(), false).await;
        assert_eq!(s.stopped.as_deref(), Some("no header source configured"));
        assert_eq!((s.faults, s.applied, s.spenders.scanned), (0, 0, 0));
        // no transactions store (review LOW-4): a fault, the event pending with the other legs' progress kept
        let store_lo4 = MemoryPotStorage::new();
        let st = confirmed_pot_with_stored_proof(&store_lo4, &pot(35), 965_771).await;
        let mut tracker_lo4 = MockChainTracker::new(965_860);
        tracker_lo4.add_root(965_771, st);
        let state_lo4 = StubState::default();
        let s = consume_arcade_reorg_events::<_, _, _, MemoryTxStore>(&StubFeed::of(events), &StubHeaders::real(965_860), &state_lo4, &store_lo4, None, Some(&tracker_lo4), Some(&CourierStub::none()), limits(2, 50), &mut RootMemo::default(), false).await;
        assert_eq!((s.faults, s.applied), (1, 0), "{s:?}");
        assert!(s.stopped.as_deref().unwrap().starts_with("no transactions store"));
        let pending = state_lo4.state().pending.unwrap();
        assert!(pending.spenders.exhausted && pending.pot_beefs.exhausted && !pending.transactions.exhausted, "{pending:?}");
        assert_eq!(pending.fault_passes, 1);
        assert_eq!(state_lo4.state().cursor, None);
    }

    /// Round 2 (review MED-4): a persisted state row with a malformed hash
    /// is a COUNTED fault of the pass, never a panic (the pass runs ahead of
    /// the sweep and the confirmation chaser on both money paths); nothing
    /// is examined, nothing written.
    #[tokio::test]
    async fn a_malformed_state_row_is_a_counted_fault_not_a_panic() {
        let store = MemoryPotStorage::new();
        let mut tracker = MockChainTracker::new(965_860);
        let standing = confirmed_pot_with_stored_proof(&store, &pot(32), 965_771).await;
        tracker.add_root(965_771, standing);
        let txs = MemoryTxStore::default();
        for doc in [
            r#"{"v":1,"cursor":{"orphaned_at":"2026-09-07T22:45:22.316Z","height":965771,"hash":"abc"},"pending":null}"#,
            r#"{"v":1,"cursor":null,"pending":{"event":{"orphaned_at":"nope","height":965771,"hash":"0000000000000000153e10f465dba9697e4bde364fdf3a3224a736b019ffbfb1"},"spenders":{"after":null,"exhausted":false},"pot_beefs":{"after":null,"exhausted":false},"transactions":{"after":null,"exhausted":false},"held_passes":0}}"#,
            r#"{"v":9}"#,
            "not json",
        ] {
            let state = StubState::with_doc(doc);
            let s = run(&StubFeed::of(real_events()), &StubHeaders::real(965_860), &state, &store, Some(&txs), Some(&tracker), Some(&CourierStub::none()), limits(2, 50)).await;
            assert_eq!((s.errors, s.faults, s.applied, s.spenders.scanned), (1, 0, 0, 0), "{doc}: {s:?}");
            assert!(s.stopped.as_deref().unwrap().starts_with("state read"), "{s:?}");
            assert_eq!(state.writes.get(), 0, "{doc}: nothing written over the row");
        }
        assert!(store.get_spent_status(&pot(32), 0).await.unwrap().unwrap().spent_confirmed);
        // and the log helper never slices a short hash
        assert_eq!(short("abc"), "abc");
        assert_eq!(short(ORPHAN_965771), "0000000000000000");
    }

    /// Round 2 (review LOW-2): a state write that loses its compare-and-set
    /// (another isolate wrote in between) stops the pass; the loser's
    /// document is not written and its progress is not counted as the
    /// cursor's.
    #[tokio::test]
    async fn a_lost_state_write_stops_the_pass_and_writes_nothing() {
        let store = MemoryPotStorage::new();
        let mut tracker = MockChainTracker::new(965_860);
        let standing = confirmed_pot_with_stored_proof(&store, &pot(33), 965_771).await;
        tracker.add_root(965_771, standing);
        let txs = MemoryTxStore::default();
        let state = StubState::default();
        state.contend.set(true); // the other isolate's write lands before ours
        let s = run(&StubFeed::of(real_events()), &StubHeaders::real(965_860), &state, &store, Some(&txs), Some(&tracker), Some(&CourierStub::none()), limits(1, 50)).await;
        assert_eq!((s.contended, s.applied, s.errors, s.spenders.scanned), (1, 0, 0, 1), "the rows were judged (idempotent), the write lost, the event NOT counted: {s:?}");
        assert_eq!(state.writes.get(), 0, "the loser wrote nothing");
        assert!(s.stopped.as_deref().unwrap().contains("another isolate"));
        assert_eq!(state.state().cursor, None, "the row is the other isolate's");
        // the next pass reads the row afresh and proceeds
        let s2 = run(&StubFeed::of(real_events()), &StubHeaders::real(965_860), &state, &store, Some(&txs), Some(&tracker), Some(&CourierStub::none()), limits(1, 50)).await;
        assert_eq!((s2.contended, s2.applied), (0, 1), "{s2:?}");
        assert_eq!(state.writes.get(), 1);
    }

    /// Round 2 (review MED-3): the head-of-line queue has a ceiling and a
    /// release. An event faulting for `fault_ceiling` passes, or held for
    /// `held_ceiling` passes, is finished as unresolved (counted) so the
    /// events behind it move; the operator can release the pending event
    /// at once (`skipPending`).
    #[tokio::test]
    async fn the_ceiling_releases_a_stuck_event_and_so_can_the_operator() {
        let store = MemoryPotStorage::new();
        let mut tracker = MockChainTracker::new(965_860);
        let standing = confirmed_pot_with_stored_proof(&store, &pot(34), 965_771).await;
        tracker.add_root(965_771, standing);
        let txs = MemoryTxStore::default();
        let events = real_events();
        // FAULTS: the spenders leg faults every pass (a starved tracker); ceiling 2
        let lim = PassLimits { events_per_pass: 1, leg_limit: 50, held_ceiling: 2, fault_ceiling: 2, ..PassLimits::cron() };
        let state = StubState::default();
        for expected_faults in [1u32, 2] {
            let s = run(&StubFeed::of(events.clone()), &StubHeaders::real(965_860), &state, &store, Some(&txs), Some(&FaultyTracker), Some(&CourierStub::none()), lim).await;
            assert_eq!((s.faults, s.applied, s.skipped_unresolved), (1, 0, 0), "{s:?}");
            assert_eq!(state.state().pending.as_ref().map(|p| p.fault_passes), Some(expected_faults));
        }
        let s = run(&StubFeed::of(events.clone()), &StubHeaders::real(965_860), &state, &store, Some(&txs), Some(&FaultyTracker), Some(&CourierStub::none()), lim).await;
        assert_eq!((s.skipped_unresolved, s.events_finished()), (1, 1), "released at the ceiling: {s:?}");
        let released_key = events.iter().find(|e| e.hash == ORPHAN_965771).unwrap().key();
        assert_eq!(state.state().cursor, Some(released_key.clone()), "the cursor moved past the stuck event");
        assert!(store.get_spent_status(&pot(34), 0).await.unwrap().unwrap().spent_confirmed, "nothing was demoted by the release");
        // round 3 (review MED): the release is RECORDED with its reason, on the row and on the summary,
        // and the health surface names the height the operator owes an /internal/reorg run
        assert_eq!(state.state().unresolved, vec![UnresolvedEvent { key: released_key.clone(), why: ReleaseReason::FaultCeiling }]);
        assert_eq!(s.unresolved, state.state().unresolved);
        let surface = crate::ops::arcade_reorg_state_json(&state.state(), Some(1), Some(3));
        assert_eq!(surface["unresolved"][0]["height"], 965_771);
        assert_eq!(surface["unresolved"][0]["hash"], ORPHAN_965771);
        assert_eq!(surface["unresolved"][0]["why"], "fault-ceiling");
        assert_eq!(surface["unresolved"][0]["heal"], "POST /internal/reorg {\"fromHeight\": 965771, \"toHeight\": 965771}");
        // HELD: the same-hash event near the tip, held 2 passes, released on the third
        let two_965773: Vec<OrphanEvent> = events.iter().filter(|e| e.height == 965_773).cloned().collect();
        let state2 = StubState::default();
        for expected_held in [1u32, 2] {
            let feed = StubFeed::of(two_965773.clone());
            feed.newest.set(Some(965_774));
            let s = run(&feed, &StubHeaders::real(965_774), &state2, &store, Some(&txs), Some(&tracker), Some(&CourierStub::none()), lim).await;
            assert_eq!((s.held, s.events_finished()), (1, 0), "{s:?}");
            assert_eq!(state2.state().pending.as_ref().map(|p| p.held_passes), Some(expected_held));
        }
        let feed = StubFeed::of(two_965773.clone());
        feed.newest.set(Some(965_774));
        let s = run(&feed, &StubHeaders::real(965_774), &state2, &store, Some(&txs), Some(&tracker), Some(&CourierStub::none()), lim).await;
        assert_eq!((s.skipped_unresolved, s.applied), (1, 0), "released at the held ceiling; the budget of one event is spent by the release: {s:?}");
        assert_eq!(state2.state().cursor.as_ref().map(|c| c.hash.as_str()), Some(CANONICAL_965773));
        assert_eq!(state2.state().unresolved.iter().map(|u| (u.key.height, u.why)).collect::<Vec<_>>(), vec![(965_773, ReleaseReason::HeldCeiling)]);
        // the OPERATOR: a pending event released at once, then the pass proceeds to the next event
        let state3 = StubState::default();
        let s = run(&StubFeed::of(events.clone()), &StubHeaders::real(965_860), &state3, &store, Some(&txs), Some(&FaultyTracker), Some(&CourierStub::none()), lim).await;
        assert_eq!(s.faults, 1);
        assert!(state3.state().pending.is_some());
        let s = consume_arcade_reorg_events(&StubFeed::of(events.clone()), &StubHeaders::real(965_860), &state3, &store, Some(&txs), Some(&tracker), Some(&CourierStub::none()), limits(2, 50), &mut RootMemo::default(), true).await;
        assert_eq!(s.released_by_operator, 1, "{s:?}");
        assert!(s.events_finished() >= 2, "the release, then the next events: {s:?}");
        assert_eq!(state3.state().cursor.as_ref().map(|c| c.height), Some(965_773));
        assert_eq!(state3.state().unresolved.iter().map(|u| (u.key.hash.as_str(), u.why)).collect::<Vec<_>>(), vec![(ORPHAN_965771, ReleaseReason::Operator)], "the operator's release is recorded too");
        // nothing pending: the operator's release is a no-op
        let s = consume_arcade_reorg_events(&StubFeed::of(events), &StubHeaders::real(965_860), &state3, &store, Some(&txs), Some(&tracker), Some(&CourierStub::none()), limits(2, 50), &mut RootMemo::default(), true).await;
        assert_eq!(s.released_by_operator, 0);
        assert!(s.idle);
    }

    /// Gating: a re-anchor stores nothing unless the ladder's proof verified
    /// (the ladder answers `Ok(None)` for an unverifiable proof, and a fault
    /// as `Err`: neither writes a bump); an event demotes nothing whose
    /// stored bump still verifies; a refuted row with no courier configured
    /// is demoted (the plain arm).
    #[tokio::test]
    async fn a_reanchor_needs_a_verified_proof_and_a_demotion_needs_a_refuted_bump() {
        let store = MemoryPotStorage::new();
        let mut tracker = MockChainTracker::new(965_860);
        let standing = confirmed_pot_with_stored_proof(&store, &pot(40), 965_771).await;
        tracker.add_root(965_771, standing.clone());
        let unverifiable = confirmed_pot_with_stored_proof(&store, &pot(41), 965_771).await;
        let faulted = confirmed_pot_with_stored_proof(&store, &pot(42), 965_771).await;
        let only_965771 = vec![real_events().into_iter().find(|e| e.hash == ORPHAN_965771).unwrap()];
        // the ladder: the standing row is never asked; `unverifiable` gets the ladder's
        // "nothing verifies" (Ok(None)); `faulted` a chaintracks read fault (Err)
        let ladder = CourierStub::with([(faulted.clone(), Err("chaintracks starved".to_string()))].into_iter().collect());
        let txs = MemoryTxStore::default();
        let state = StubState::default();
        let s = run(&StubFeed::of(only_965771.clone()), &StubHeaders::real(965_860), &state, &store, Some(&txs), Some(&tracker), Some(&ladder), limits(2, 50)).await;
        assert_eq!((s.spenders.scanned, s.spenders.standing, s.reanchored(), s.demoted(), s.spenders.faults, s.faults), (3, 1, 0, 1, 1, 1), "{s:?}");
        assert_eq!(ladder.calls.get(), 2);
        assert!(store.get_spent_status(&pot(40), 0).await.unwrap().unwrap().spent_confirmed, "a verifying stored bump is never demoted");
        let r = store.get_spent_status(&pot(41), 0).await.unwrap().unwrap();
        assert!(!r.spent_confirmed, "no verified proof anywhere: demoted");
        assert_eq!(stored_bump_anchor(&store.get_beef(&unverifiable).await.unwrap().unwrap(), &unverifiable).map(|a| a.height), Some(965_771), "nothing was stored for it");
        assert!(store.get_spent_status(&pot(42), 0).await.unwrap().unwrap().spent_confirmed, "a ladder FAULT is not a verdict: the row is untouched");
        assert!(store.pot_beef_proof_verified(&faulted).await.unwrap());
        assert_eq!(state.state().cursor, None, "the leg faulted: the event stays pending");
        // no courier configured: the refuted rows take the plain demotion arm
        let store2 = MemoryPotStorage::new();
        let refuted = confirmed_pot_with_stored_proof(&store2, &pot(43), 965_771).await;
        let state2 = StubState::default();
        let s2 = run(&StubFeed::of(only_965771), &StubHeaders::real(965_860), &state2, &store2, Some(&txs), Some(&tracker), None, limits(2, 50)).await;
        assert_eq!((s2.applied, s2.demoted(), s2.reanchored()), (1, 1, 0), "{s2:?}");
        assert!(!store2.get_spent_status(&pot(43), 0).await.unwrap().unwrap().spent_confirmed);
        assert!(!store2.pot_beef_proof_verified(&refuted).await.unwrap());
    }

    /// Bounded: an event with more rows than one leg page spans passes with
    /// no row asked twice and none lost; the per-pass event budget stops a
    /// multi-event feed at the cap and the next pass resumes at the cursor.
    #[tokio::test]
    async fn a_pass_is_bounded_and_resumes_at_its_cursors_with_no_row_or_event_lost() {
        let store = MemoryPotStorage::new();
        let mut spenders = Vec::new();
        for i in 0..23u32 {
            // unanchored latches: this pin counts the SPENDERS leg's questions only
            spenders.push(confirmed_pot_with_proof_at(&store, &pot(100 + i), 965_771, false).await);
        }
        // every 5th one is refuted (in no canonical block)
        let valid: std::collections::HashSet<(u32, String)> =
            spenders.iter().enumerate().filter(|(i, _)| i % 5 != 3).map(|(_, s)| (965_771u32, s.clone())).collect();
        let tracker = RecordingTracker { valid, asked: Mutex::new(Vec::new()) };
        let events = real_events();
        let txs = MemoryTxStore::default();
        let state = StubState::default();
        let headers = StubHeaders::real(965_860);
        // 23 rows at 10 per pass: three passes drain the 965771 event (the
        // third also runs the other legs and finishes it)
        let mut passes = 0;
        loop {
            passes += 1;
            let feed = StubFeed::of(events.clone());
            let s = run(&feed, &headers, &state, &store, Some(&txs), Some(&tracker), Some(&CourierStub::none()), limits(1, 10)).await;
            assert!(s.spenders.scanned <= 10, "{s:?}");
            assert_eq!(s.faults, 0);
            if s.applied == 1 {
                break;
            }
            let pending = state.state().pending.unwrap();
            assert_eq!(pending.event.hash, ORPHAN_965771);
            assert!(pending.spenders.after.is_some() && !pending.spenders.exhausted, "the leg cursor persists: {pending:?}");
            assert!(passes < 6, "runaway");
        }
        assert_eq!(passes, 3);
        let asked = tracker.asked.lock().unwrap().clone();
        let mut unique = asked.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(asked.len(), 23, "every row asked exactly once across the passes");
        assert_eq!(unique.len(), 23);
        let mut all = spenders.clone();
        all.sort();
        assert_eq!(unique, all, "no row lost");
        let refuted = spenders.iter().enumerate().filter(|(i, _)| i % 5 == 3).count();
        let mut confirmed = 0;
        for i in 0..23u32 {
            if store.get_spent_status(&pot(100 + i), 0).await.unwrap().unwrap().spent_confirmed {
                confirmed += 1;
            }
        }
        assert_eq!(confirmed, 23 - refuted, "exactly the refuted rows were demoted");
        // the event budget: the two 965773 events remain; one per pass
        let s = run(&StubFeed::of(events.clone()), &headers, &state, &store, Some(&txs), Some(&tracker), Some(&CourierStub::none()), limits(1, 10)).await;
        assert_eq!(s.events_finished(), 1, "{s:?}");
        assert_eq!(s.feed_events, 2, "two were waiting; the budget took one");
        let s = run(&StubFeed::of(events.clone()), &headers, &state, &store, Some(&txs), Some(&tracker), Some(&CourierStub::none()), limits(1, 10)).await;
        assert_eq!(s.events_finished(), 1, "{s:?}");
        let s = run(&StubFeed::of(events.clone()), &headers, &state, &store, Some(&txs), Some(&tracker), Some(&CourierStub::none()), limits(1, 10)).await;
        assert!(s.idle, "{s:?}");
        assert_eq!(state.state().cursor, events.iter().map(OrphanEvent::key).max());
        // the block-event limits are the smaller ones
        assert_eq!((PassLimits::block_event().events_per_pass, PassLimits::block_event().leg_limit), (1, 100));
        assert_eq!((PassLimits::cron().events_per_pass, PassLimits::cron().leg_limit), (2, 200));
    }

    /// Round 2 (review MED-5): the corroboration header read seeds the
    /// pass's memo with the canonical root at the event's height, so the
    /// spenders leg judges every row anchored there WITHOUT a chaintracks
    /// read; the same memo handed to the routine sweep afterwards answers
    /// its rows at that height from the seed too (zero reads).
    #[tokio::test]
    async fn the_memo_is_seeded_from_the_corroboration_read_and_shared_with_the_sweep() {
        let store = MemoryPotStorage::new();
        // the canonical block's root at 965771 is the one standing spender's
        // txid (a single-leaf bump's root); the other rows are refuted
        let standing = confirmed_pot_with_proof_at(&store, &pot(200), 965_771, false).await;
        let mut refuted = Vec::new();
        for i in 1..6u32 {
            refuted.push(confirmed_pot_with_proof_at(&store, &pot(200 + i), 965_771, false).await);
        }
        let tracker = RecordingTracker { valid: [(965_771u32, standing.clone())].into_iter().collect(), asked: Mutex::new(Vec::new()) };
        let mut headers = StubHeaders::real(965_860);
        headers.roots.insert(965_771, standing.clone());
        let only_965771 = vec![real_events().into_iter().find(|e| e.hash == ORPHAN_965771).unwrap()];
        let txs = MemoryTxStore::default();
        let state = StubState::default();
        let mut memo = RootMemo::default();
        let s = consume_arcade_reorg_events(&StubFeed::of(only_965771), &headers, &state, &store, Some(&txs), Some(&tracker), Some(&CourierStub::none()), limits(2, 50), &mut memo, false).await;
        assert_eq!((s.applied, s.spenders.scanned, s.spenders.standing, s.demoted(), s.memo_seeded), (1, 6, 1, 5, 1), "{s:?}");
        assert_eq!(tracker.asked.lock().unwrap().len(), 0, "every row at the seeded height was judged from the header read");
        assert_eq!(s.memo_reads, 0);
        // the sweep over the same memo: a row confirmed at 965771 after the event (a late
        // confirmation) is judged from the seed too
        let late = confirmed_pot_with_proof_at(&store, &pot(210), 965_771, false).await;
        let sweep = crate::reorg_sweep::reorg_revalidation_sweep_with(&store, Some(&txs), Some(&tracker), None, 965_773, 3, 50, &mut memo).await;
        assert!(sweep.spenders.scanned >= 1, "{sweep:?}");
        assert_eq!(tracker.asked.lock().unwrap().len(), 0, "the sweep asked nothing for rows at the seeded height");
        assert!(!store.get_spent_status(&pot(210), 0).await.unwrap().unwrap().spent_confirmed, "the late row's root is not the canonical one: refuted from the seed");
        let _ = (refuted, late);
        // without a root on the header read, the memo is not seeded and the rows are read once per root
        let store2 = MemoryPotStorage::new();
        let standing2 = confirmed_pot_with_proof_at(&store2, &pot(220), 965_771, false).await;
        let tracker2 = RecordingTracker { valid: [(965_771u32, standing2)].into_iter().collect(), asked: Mutex::new(Vec::new()) };
        let state2 = StubState::default();
        let mut memo2 = RootMemo::default();
        let s2 = consume_arcade_reorg_events(&StubFeed::of(vec![real_events().into_iter().find(|e| e.hash == ORPHAN_965771).unwrap()]), &StubHeaders::real(965_860), &state2, &store2, Some(&txs), Some(&tracker2), Some(&CourierStub::none()), limits(2, 50), &mut memo2, false).await;
        assert_eq!((s2.memo_seeded, s2.memo_reads), (0, 1), "{s2:?}");
        assert_eq!(tracker2.asked.lock().unwrap().len(), 1);
    }

    /// The other legs and the empty case: an orphan event at a height where
    /// we hold nothing applies as a no-op (counted, the cursor moves); the
    /// pots' own proofs and the hop proofs at the event's height are
    /// unlatched / un-proved when refuted and kept when standing.
    #[tokio::test]
    async fn an_event_for_a_height_we_do_not_hold_is_inert_and_the_proof_legs_unlatch_refuted_bumps() {
        let store = MemoryPotStorage::new();
        let txs = MemoryTxStore::default();
        let tracker = MockChainTracker::new(965_860);
        let state = StubState::default();
        let only_965771 = vec![real_events().into_iter().find(|e| e.hash == ORPHAN_965771).unwrap()];
        let s = run(&StubFeed::of(only_965771.clone()), &StubHeaders::real(965_860), &state, &store, Some(&txs), Some(&tracker), Some(&CourierStub::none()), limits(2, 50)).await;
        assert_eq!((s.applied, s.spenders.scanned, s.pot_beefs.scanned, s.transactions.scanned, s.faults), (1, 0, 0, 0, 0), "{s:?}");
        assert_eq!(state.state().cursor, Some(only_965771[0].key()));
        // the proof legs (a SET tracker: two canonical roots at one height,
        // which the one-root-per-height mock cannot hold)
        let pot_beef = |n: u32, height: u32| async move {
            let raw = real_spender_raw(&pot(n), 0);
            let txid = Transaction::from_hex(&raw).unwrap().id();
            let beef = crate::proof_fetcher::assemble_spender_beef(&raw, &single_tx_bump(&txid, height).to_hex(), &txid).unwrap();
            (txid, beef)
        };
        let (join_ok, beef_ok) = pot_beef(50, 965_771).await;
        let (join_bad, beef_bad) = pot_beef(51, 965_771).await;
        for (txid, beef) in [(&join_ok, &beef_ok), (&join_bad, &beef_bad)] {
            store.store_beef(txid, beef).await.unwrap();
            store.mark_pot_beef_proven_at(txid, Some(965_771)).await.unwrap();
        }
        let (hop_ok, hbeef_ok) = pot_beef(52, 965_771).await;
        let (hop_bad, hbeef_bad) = pot_beef(53, 965_771).await;
        txs.insert(&hop_ok, hbeef_ok, Some(965_771), true);
        txs.insert(&hop_bad, hbeef_bad, Some(965_771), true);
        let tracker = RecordingTracker {
            valid: [(965_771u32, join_ok.clone()), (965_771u32, hop_ok.clone())].into_iter().collect(),
            asked: Mutex::new(Vec::new()),
        };
        let state2 = StubState::default();
        let s = run(&StubFeed::of(only_965771), &StubHeaders::real(965_860), &state2, &store, Some(&txs), Some(&tracker), Some(&CourierStub::none()), limits(2, 50)).await;
        assert_eq!((s.applied, s.pot_beefs.scanned, s.pot_beefs.stale, s.transactions.scanned, s.transactions.stale), (1, 2, 1, 2, 1), "{s:?}");
        assert!(store.pot_beef_proof_verified(&join_ok).await.unwrap());
        assert!(!store.pot_beef_proof_verified(&join_bad).await.unwrap(), "the orphan's JOIN lost its latch");
        assert!(txs.proven(&hop_ok));
        assert!(!txs.proven(&hop_bad), "un-proved: the engine's completion pass re-fetches it");
    }

    #[test]
    fn the_feed_pages_arcades_listing_by_its_keyset_cursor_one_above_the_boundary() {
        let feed = ArcadeBlockStatusFeed::new("https://arcade-v2-us-1.bsvblockchain.tech/");
        assert_eq!(feed.page_url(None), "https://arcade-v2-us-1.bsvblockchain.tech/api/v1/blocks/processing-status?limit=100");
        // round 2 (review LOW-1): the next page is asked ONE ABOVE the page's lowest
        // height, so Arcade's `block_height < before-height` re-lists the boundary
        // height's other rows (the same-height competitors a reorg produces)
        let page = parse_block_status_page(REAL_PAGE).unwrap();
        assert_eq!(page.next_cursor, Some(965_769));
        assert_eq!(next_page_before(page.next_cursor), Some(965_770));
        assert_eq!(feed.page_url(next_page_before(page.next_cursor)), "https://arcade-v2-us-1.bsvblockchain.tech/api/v1/blocks/processing-status?limit=100&before-height=965770");
        assert_eq!(page.orphans.len(), 3);
        assert_eq!(page.newest_height, Some(965_775));
        let _ = ev("2026-09-07T22:45:22.316Z", 965_771, ORPHAN_965771);
        assert_eq!(EMPTY_SIBLING_965773.len(), 64);
    }
}
