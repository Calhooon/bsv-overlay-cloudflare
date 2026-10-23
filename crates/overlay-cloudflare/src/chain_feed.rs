//! bsv-low #620 (the words map's L4): the overlay's reorg word is chaintracks' TYPED chain-event feed, the one the
//! app layer's intake reads (#771, `low_app_layer::tracker_host::chain_events`), consumed on the overlay's own durable
//! cursor ([`CREATE`], one versioned row). `GET /events?since=<cursor>` over the CHAINTRACKS binding (the stack's
//! `docs/CHAIN-EVENTS.md`); every envelope passes `bsv_tracker::decode_envelope`, the crate's gate, and one it refuses
//! (an unknown kind or version, a malformed known shape, the feed's own `unreadable`) STOPS the pull at its cursor:
//! nothing past it consumed, the cursor kept, recorded on the row, counted once, logged.
//!
//! The reaction, per kind. A `reorg` at `forkHeight`, a `fork` at `height` and a `split` at `forkHeight` select every
//! row whose stored proof is anchored from that height to the highest the event names (the deactivated headers, the
//! new tip, the competing or witness tips); an `invalidated` block names no height and its header is no longer served
//! as active (`findHeaderHexForBlockHash` reads the active chain only), so it selects the last
//! [`INVALIDATED_DEPTH`] heights below chaintracks' tip. `tip`, `tipAge`, `frozen` and `splitResolved` select nothing
//! (the crate's own `on_chain` set; a tip's recheck of SUSPECT words is the app layer's, the overlay holds no word).
//! The selected rows are judged by THE EXISTING JUDGEMENT, never a new one (`arcade_reorg::judge_window_legs`, which
//! Arcade's orphan events, register row D7, run over one height): each confirmed spender by ITS OWN stored proof
//! against the active header's root at its height (the root rule, never the hash), a refuted one re-anchored IN
//! PLACE from a chaintracks-verified courier proof before any demotion and demoted only when none is served, bound to
//! the judged height; the pots' own proofs and the hop proofs unlatched when refuted. The event IS the header source's
//! word, so no corroboration is asked (D7's corroboration exists because Arcade's listing is a hint).
//!
//! Bounded per pass: at most [`PAGES_PER_PASS`] feed pages, `PassLimits::events_per_pass` selecting events finished,
//! one page of `leg_limit` rows per leg for the event in hand (a window wider than one page is HELD as `pending` with
//! each leg's cursor and walked on across passes), the ladder's own budget. The cursor moves only past an event
//! FINISHED (every leg exhausted with no fault, or released at the fault ceiling) and only by a compare-and-set write
//! of the row ([`STATE_WRITE`]); a fault of any kind leaves it, so an event replays idempotently (every row write of
//! the judgement is idempotent to redo). An event faulting `PassLimits::fault_ceiling` passes is RELEASED unresolved
//! and RECORDED with its heal (`POST /internal/reorg {"fromHeight": lo, "toHeight": hi}`), as D7 releases one.
//!
//! The start: a fresh consumer begins at [`START_VAR`] (the journal's head; `0` replays it, and the production journal
//! holds bootstrap-era rows: a fork at 956433 of depth 13,906), never at the journal's start by default; unset, the
//! consumer reads nothing, writes nothing and counts `overlay_chain_events_unconfigured_total`. A present row ignores
//! the var. The operator's lever ([`lever`], `/internal/chain-events-cursor`) reads the row or sets the cursor. A 409
//! `cursorAhead` is recorded as a stop at the stored cursor (counted, logged; the lever is the way on), never a reset to
//! `0`: a replay of the journal re-judges the bootstrap fork's whole window.

use bsv_rs::transaction::ChainTracker;
use bsv_tracker::{decode_envelope, ChainEvent};
use overlay_discovery::pot::arcade_events::LegProgress;
use overlay_discovery::pot::storage::PotStorage;
use overlay_engine::gasp::AncestorFetcher;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, VecDeque};

use crate::arcade_reorg::{judge_window_legs, PassLimits, WindowLegs};
use crate::proof_fetcher::push_log;
use crate::reorg_sweep::{ProofLegSummary, ProvenTxStore, ReverifyPassSummary, RootMemo};

/// The consumer's row: the state document (JSON, [`FeedState`]), its compare-and-set version and its stamp (ms).
pub const CREATE: &str = "CREATE TABLE IF NOT EXISTS overlay_chain_events (id INTEGER PRIMARY KEY CHECK (id = 1), state TEXT NOT NULL, version INTEGER NOT NULL, updated_at INTEGER NOT NULL)";

/// The row, or none before the first pass.
pub const STATE_READ: &str =
    "SELECT state, version, updated_at FROM overlay_chain_events WHERE id = 1";

/// The state's COMPARE-AND-SET write: a first write inserts version 1 (`?3` NULL: the writer read no row); a later
/// write replaces the document only while the row still carries the version the writer READ, and bumps it. A row back
/// (the new version) = written; none = another isolate wrote in between (the pass stops, its progress redone).
pub const STATE_WRITE: &str = "INSERT INTO overlay_chain_events (id, state, version, updated_at) VALUES (1, ?1, 1, ?2) ON CONFLICT(id) DO UPDATE SET state = excluded.state, version = overlay_chain_events.version + 1, updated_at = excluded.updated_at WHERE overlay_chain_events.version IS ?3 RETURNING version";

/// Where a FRESH consumer begins: a decimal feed cursor (the journal's head). Unset or empty: nothing is read.
pub const START_VAR: &str = "CHAIN_EVENTS_START_CURSOR";

/// The feed pages one pass reads (a page is at most 100 events).
pub const PAGES_PER_PASS: usize = 2;

/// An `invalidated` block's window: this many heights below chaintracks' tip, up to the tip.
pub const INVALIDATED_DEPTH: u64 = 144;

/// The released windows the row keeps (newest last).
pub const UNRESOLVED_KEPT: usize = 20;

/// The feed GET's deadline.
pub const FEED_TIMEOUT_MS: u64 = 10_000;

/// The state document's version; a higher one is unreadable (a fault), never guessed.
pub const FEED_STATE_VERSION: u32 = 1;

/// The event the consumer is judging, with each leg's progress through its window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingWindow {
    /// The event's feed cursor (the row's cursor moves to it when the window is finished).
    pub cursor: String,
    /// The event's wire kind.
    pub kind: String,
    /// The lowest height of the window.
    pub lo: u64,
    /// The highest height of the window.
    pub hi: u64,
    /// The spenders leg's progress.
    pub spenders: LegProgress,
    /// The pot BEEFs leg's progress.
    pub pot_beefs: LegProgress,
    /// The transactions leg's progress.
    pub transactions: LegProgress,
    /// Passes this window ended in a read fault.
    #[serde(default)]
    pub fault_passes: u32,
}

impl PendingWindow {
    fn new(cursor: u64, kind: &str, lo: u64, hi: u64) -> Self {
        Self {
            cursor: cursor.to_string(),
            kind: kind.to_string(),
            lo,
            hi,
            spenders: LegProgress::default(),
            pot_beefs: LegProgress::default(),
            transactions: LegProgress::default(),
            fault_passes: 0,
        }
    }

    fn all_exhausted(&self) -> bool {
        self.spenders.exhausted && self.pot_beefs.exhausted && self.transactions.exhausted
    }
}

/// A window finished WITHOUT its rows judged (the fault ceiling, or the operator's lever moving the cursor past it):
/// its rows are the operator's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReleasedWindow {
    /// The event's feed cursor.
    pub cursor: String,
    /// The event's wire kind.
    pub kind: String,
    /// The lowest height of its window.
    pub lo: u64,
    /// The highest height of its window.
    pub hi: u64,
    /// `fault-ceiling` or `operator`.
    pub why: String,
    /// When it was released (ms).
    pub at_ms: u64,
}

/// The envelope the pull stopped at, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Stopped {
    /// The envelope's feed cursor (or the stored cursor the feed answered 409 at).
    pub cursor: String,
    /// The crate's refusal, or `cursorAhead`.
    pub fault: String,
}

/// The consumer's state document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FeedState {
    /// The document's version ([`FEED_STATE_VERSION`]).
    pub v: u32,
    /// The last feed cursor consumed.
    pub cursor: String,
    /// The selecting event in hand, when one is.
    #[serde(default)]
    pub pending: Option<PendingWindow>,
    /// The envelope the pull stopped at (an unreadable one, or the feed's 409).
    #[serde(default)]
    pub stop: Option<Stopped>,
    /// The last event finished: its kind.
    #[serde(default)]
    pub last_kind: Option<String>,
    /// The last event finished: its cursor.
    #[serde(default)]
    pub last_cursor: Option<String>,
    /// The last tip a `tip` or `tipAge` event named.
    #[serde(default)]
    pub tip_height: Option<u64>,
    /// Where the row was first written (the start var or the lever) and when (ms).
    #[serde(default)]
    pub seed: Option<String>,
    /// When the row was first written (ms).
    #[serde(default)]
    pub started_at_ms: Option<u64>,
    /// The windows released unresolved, newest last, at most [`UNRESOLVED_KEPT`].
    #[serde(default)]
    pub unresolved: Vec<ReleasedWindow>,
}

impl FeedState {
    /// A fresh row at `cursor`.
    pub fn seeded(cursor: &str, now_ms: u64) -> Self {
        Self {
            v: FEED_STATE_VERSION,
            cursor: cursor.to_string(),
            pending: None,
            stop: None,
            last_kind: None,
            last_cursor: None,
            tip_height: None,
            seed: Some(cursor.to_string()),
            started_at_ms: Some(now_ms),
            unresolved: Vec::new(),
        }
    }

    /// The document as the row keeps it.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    /// The row's document; a newer version or a cursor that is not a feed cursor is unreadable.
    pub fn from_json(text: &str) -> Result<Self, String> {
        let state: FeedState =
            serde_json::from_str(text).map_err(|e| format!("overlay_chain_events state: {e}"))?;
        if state.v > FEED_STATE_VERSION {
            return Err(format!(
                "overlay_chain_events state version {} is newer than {FEED_STATE_VERSION}",
                state.v
            ));
        }
        if parse_cursor(&state.cursor).is_none() {
            return Err(format!(
                "overlay_chain_events cursor {:?} is not a feed cursor",
                state.cursor
            ));
        }
        Ok(state)
    }

    fn position(&self) -> u64 {
        parse_cursor(&self.cursor).unwrap_or(0)
    }

    fn release(&mut self, pending: PendingWindow, why: &str, now_ms: u64) {
        self.cursor = pending.cursor.clone();
        self.last_kind = Some(pending.kind.clone());
        self.last_cursor = Some(pending.cursor.clone());
        self.unresolved.push(ReleasedWindow {
            cursor: pending.cursor,
            kind: pending.kind,
            lo: pending.lo,
            hi: pending.hi,
            why: why.to_string(),
            at_ms: now_ms,
        });
        let over = self.unresolved.len().saturating_sub(UNRESOLVED_KEPT);
        self.unresolved.drain(..over);
        self.pending = None;
    }
}

/// A feed cursor: an opaque decimal string inside D1's exact integer range (the app layer's rule).
pub fn parse_cursor(text: &str) -> Option<u64> {
    if text.is_empty() || text.len() > 16 || !text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

/// What [`START_VAR`] says.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Start {
    /// Unset or empty: nothing is read.
    Unset,
    /// Not a feed cursor: treated as unset, its text logged.
    Malformed(String),
    /// Seed an absent row here.
    At(String),
}

impl Start {
    /// The var's text read.
    pub fn of(raw: Option<&str>) -> Start {
        match raw.map(str::trim) {
            None | Some("") => Start::Unset,
            Some(t) => match parse_cursor(t) {
                Some(n) => Start::At(n.to_string()),
                None => Start::Malformed(t.to_string()),
            },
        }
    }
}

/// The event's wire kind, for the counters and the row (the app layer's names).
pub fn kind_name(event: &ChainEvent) -> &'static str {
    match event {
        ChainEvent::Tip { .. } => "tip",
        ChainEvent::Fork { .. } => "fork",
        ChainEvent::Reorg { .. } => "reorg",
        ChainEvent::Invalidated { .. } => "invalidated",
        ChainEvent::Frozen { .. } => "frozen",
        ChainEvent::TipAge { .. } => "tip_age",
        ChainEvent::Split { .. } => "split",
        ChainEvent::SplitResolved { .. } => "split_resolved",
    }
}

/// The heights an event's proofs can rest on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Selection {
    /// The rows anchored in `lo..=hi`.
    Heights(u64, u64),
    /// The last [`INVALIDATED_DEPTH`] heights below chaintracks' tip (an invalidated block names none).
    BelowTip,
    /// Nothing moves.
    None,
}

/// Which rows an event can move: the crate's own `on_chain` set (a fork's, a reorg's and a split's rows at or above
/// its height; an invalidation's by its block), each window closed at the highest height the event names.
pub fn selection(event: &ChainEvent) -> Selection {
    let window = |lo: u32, highs: &mut dyn Iterator<Item = u32>| {
        let hi = highs.fold(lo, u32::max);
        Selection::Heights(u64::from(lo), u64::from(hi))
    };
    match event {
        ChainEvent::Reorg {
            fork_height,
            deactivated_headers,
            new_tip,
            ..
        } => window(
            *fork_height,
            &mut deactivated_headers
                .iter()
                .map(|h| h.height)
                .chain([new_tip.height]),
        ),
        ChainEvent::Fork {
            height,
            competing_tips,
            ..
        } => window(*height, &mut competing_tips.iter().map(|h| h.height)),
        ChainEvent::Split {
            fork_height, tips, ..
        } => window(*fork_height, &mut tips.iter().map(|t| t.height)),
        ChainEvent::Invalidated { .. } => Selection::BelowTip,
        ChainEvent::Tip { .. }
        | ChainEvent::TipAge { .. }
        | ChainEvent::Frozen { .. }
        | ChainEvent::SplitResolved { .. } => Selection::None,
    }
}

/// The feed's transport: one page of `GET /events?since=<since>`, its status and body, or the transport's fault.
pub trait EventFeed {
    /// One page from `since`.
    fn page(
        &self,
        since: &str,
    ) -> impl std::future::Future<Output = Result<(u16, Vec<u8>), String>>;
}

/// The consumer's row: the document and its version, or none; a compare-and-set write.
pub trait FeedStateStore {
    /// The document and its version, or none before the first write.
    fn read(&self) -> impl std::future::Future<Output = Result<Option<(FeedState, u64)>, String>>;
    /// `Ok(Some(new_version))` written; `Ok(None)` another isolate wrote in between.
    fn write(
        &self,
        state: &FeedState,
        expected_version: Option<u64>,
        now_ms: u64,
    ) -> impl std::future::Future<Output = Result<Option<u64>, String>>;
}

/// What one pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FeedPassSummary {
    /// No row and no start cursor: nothing read, nothing written.
    pub unconfigured: bool,
    /// The row was seeded at this cursor this pass.
    pub seeded: Option<String>,
    /// Feed pages read.
    pub pages: usize,
    /// Events FINISHED this pass by kind (counted only once the cursor's move persisted).
    pub consumed: BTreeMap<&'static str, u64>,
    /// Selecting events finished with every leg exhausted.
    pub applied: usize,
    /// Selecting events released unresolved at the fault ceiling.
    pub released: usize,
    /// The spenders leg, accumulated over the windows this pass touched.
    pub spenders: ReverifyPassSummary,
    /// The pot BEEFs leg, accumulated.
    pub pot_beefs: ProofLegSummary,
    /// The transactions leg, accumulated.
    pub transactions: ProofLegSummary,
    /// The spenders leg stopped its page on the ladder's spent budget.
    pub budget_stops: usize,
    /// Read faults (feed, tracker, a leg) that stopped the pass with the cursor kept.
    pub faults: usize,
    /// Storage faults (the row's read or write).
    pub errors: usize,
    /// The row's write lost its compare-and-set.
    pub contended: usize,
    /// The pull stopped at an envelope the crate refuses that it had not stopped at before.
    pub unreadable: bool,
    /// The feed answered 409 `cursorAhead` at a cursor it had not stopped at before.
    pub cursor_ahead: bool,
    /// Nothing past the cursor.
    pub idle: bool,
    /// Why the pass ended before its budget, when it did.
    pub stopped: Option<String>,
    /// The state after the pass, as written (or as read, when nothing was).
    pub state: Option<FeedState>,
}

impl FeedPassSummary {
    /// bsv-low #620 (c): the feed is the reorg word on this deployment when THIS PASS was LIVE: its row read (or was
    /// seeded) AND the pass ended with no recorded stop on the row (an envelope the crate refuses, a 409 `cursorAhead`)
    /// and no fault of the pass (the feed's transport, a page that did not read, an HTTP status, a tip read for an
    /// invalidation, a leg's read fault). Arcade's orphan listing is then no INPUT of the scheduled and block-event
    /// passes ([`arcade_not_an_input`]). Unconfigured (no row, no start cursor), no header source, a row that did not
    /// read, a stop or a fault: false, and the caller runs D7's consumer as before this module, for that pass; the stop
    /// stays recorded and served, and a healthy pass (after the operator's lever clears a recorded stop) re-takes the
    /// word. The W15E-L4 lens fold, MEDIUM-3 (Rule 6: never a self-healing failure traded for a permanent one): as
    /// built at `40fec4ce` the word was the row's presence alone, so a recorded stop, permanent by the producer's
    /// contract, switched off the working fallback until the lever. The row's own write fault (`errors`) is not the
    /// feed's: the rows the pass judged were judged, the cursor is redone next pass, and D7's cursor row shares the
    /// store; a lost compare-and-set (`contended`) is another isolate running this very pass.
    pub fn is_reorg_word(&self) -> bool {
        !self.unconfigured
            && self.faults == 0
            && self.state.as_ref().is_some_and(|st| st.stop.is_none())
    }

    /// Confirmed rows whose anchor moved without a demotion.
    pub fn reanchored(&self) -> usize {
        self.spenders.reanchored_from_courier + self.spenders.reanchored
    }
    /// Confirmed rows demoted to SEEN (refuted, no canonical proof served).
    pub fn demoted(&self) -> usize {
        self.spenders.stale
    }

    /// The R2 lifetime totals, which count the same rows whatever the producer (the sweep, the announce, the
    /// operator, Arcade's events, and now the feed), as D7's pass bumps them.
    pub fn reorg_totals(&self) -> Vec<(&'static str, u64)> {
        [
            (crate::ops::COUNTER_REORG_DEMOTED, self.demoted() as u64),
            (
                crate::ops::COUNTER_REORG_STALE_PROOFS,
                self.demoted() as u64,
            ),
            (
                crate::ops::COUNTER_REORG_STALE_POT_PROOFS,
                self.pot_beefs.stale as u64,
            ),
            (
                crate::ops::COUNTER_REORG_STALE_TX_PROOFS,
                self.transactions.stale as u64,
            ),
            (
                crate::ops::COUNTER_REORG_REANCHORED,
                self.reanchored() as u64,
            ),
            (
                crate::ops::COUNTER_REORG_TRACKER_FAULTS,
                (self.spenders.faults + self.pot_beefs.faults + self.transactions.faults) as u64,
            ),
        ]
        .into_iter()
        .filter(|(_, n)| *n > 0)
        .collect()
    }

    /// The pass's durable counter deltas (`ops_counters`, nonzero only).
    pub fn counters(&self) -> Vec<(String, u64)> {
        let mut out: Vec<(String, u64)> = vec![("overlay_chain_events_pass_total".into(), 1)];
        let mut add = |name: &str, n: u64| {
            if n > 0 {
                out.push((name.to_string(), n));
            }
        };
        add("overlay_chain_events_total", self.consumed.values().sum());
        for (kind, n) in &self.consumed {
            add(&format!("overlay_chain_events_{kind}_total"), *n);
        }
        add("overlay_chain_events_applied_total", self.applied as u64);
        add("overlay_chain_events_released_total", self.released as u64);
        add(
            "overlay_chain_events_reanchored_total",
            self.reanchored() as u64,
        );
        add("overlay_chain_events_demoted_total", self.demoted() as u64);
        add(
            "overlay_chain_events_stale_pot_proofs_total",
            self.pot_beefs.stale as u64,
        );
        add(
            "overlay_chain_events_stale_tx_proofs_total",
            self.transactions.stale as u64,
        );
        add(
            "overlay_chain_events_budget_stops_total",
            self.budget_stops as u64,
        );
        add(
            "overlay_chain_events_faults_total",
            (self.faults + self.errors) as u64,
        );
        add(
            "overlay_chain_events_contended_total",
            self.contended as u64,
        );
        add(
            "overlay_chain_events_unreadable_total",
            u64::from(self.unreadable),
        );
        add(
            "overlay_chain_events_cursor_ahead_total",
            u64::from(self.cursor_ahead),
        );
        add(
            "overlay_chain_events_unconfigured_total",
            u64::from(self.unconfigured),
        );
        out
    }
}

/// bsv-low #620 (c), THE GATE both passes read (the cron's `tick.rs`, the block-event pass's `tip_pass.rs`; one
/// function, the W15E-L4 lens fold's MEDIUM-3): what to do with Arcade's orphan listing (register row D7) after the
/// feed's pass. `None`: the listing is an INPUT this pass and D7's consumer runs (the feed was not the reorg word this
/// pass, or did not run at all). `Some(why)`: not an input (the feed was the reorg word this pass).
pub fn arcade_not_an_input(feed: Option<&FeedPassSummary>) -> Option<&'static str> {
    feed.is_some_and(FeedPassSummary::is_reorg_word)
        .then_some("not an input: the chain-event feed is the reorg word this pass (bsv-low #620)")
}

struct Page {
    events: Vec<(u64, Value)>,
    has_more: bool,
}

/// One feed page (`{"v":1,"cursor":..,"events":[{"cursor":..,"event":{..}}],"hasMore":..}`), the app layer's reading.
fn parse_page(body: &[u8]) -> Result<Page, String> {
    let v: Value = serde_json::from_slice(body).map_err(|e| format!("events page: {e}"))?;
    if v["v"].as_u64() != Some(1) {
        return Err(format!("events page carrier version {}", v["v"]));
    }
    let has_more = v["hasMore"]
        .as_bool()
        .ok_or("events page: hasMore missing")?;
    let mut events = Vec::new();
    for item in v["events"]
        .as_array()
        .ok_or("events page: events missing")?
    {
        let c = match &item["cursor"] {
            Value::String(s) => parse_cursor(s),
            Value::Number(n) => n.as_u64(),
            _ => None,
        }
        .ok_or_else(|| format!("events page: malformed cursor {}", item["cursor"]))?;
        events.push((c, item["event"].clone()));
    }
    Ok(Page { events, has_more })
}

enum Persist {
    Written,
    Contended,
    Faulted,
}

async fn persist<C: FeedStateStore>(
    store: &C,
    state: &FeedState,
    version: &mut Option<u64>,
    now_ms: u64,
    out: &mut FeedPassSummary,
) -> Persist {
    match store.write(state, *version, now_ms).await {
        Ok(Some(v)) => {
            *version = Some(v);
            Persist::Written
        }
        Ok(None) => {
            out.contended += 1;
            out.stopped = Some("another isolate owns this pass (the row's compare-and-set)".into());
            push_log("[chain-feed] the state write lost its compare-and-set: another isolate ran this pass; its progress is redone by whoever holds the row");
            Persist::Contended
        }
        Err(e) => {
            out.errors += 1;
            out.stopped = Some(format!("state write: {e}"));
            push_log(&format!(
                "[chain-feed] state write failed: this pass's progress is redone next pass: {e}"
            ));
            Persist::Faulted
        }
    }
}

/// ONE bounded pass of the consumer. See the module doc for the contract. `memo` is the pass's shared root memo (the
/// caller hands the same one to the routine sweep).
#[allow(clippy::too_many_arguments)]
// the feed, the row, the two stores, the two chain sources, the bounds, the memo, the start, the clock
#[allow(unused_assignments)] // the last `write_now!` resets tallies nothing reads after it
pub async fn consume_chain_feed<F, C, S>(
    feed: &F,
    store: &C,
    pot_storage: &dyn PotStorage,
    tx_store: Option<&S>,
    tracker: Option<&dyn ChainTracker>,
    fetcher: Option<&dyn AncestorFetcher>,
    limits: PassLimits,
    memo: &mut RootMemo,
    start: &Start,
    now_ms: u64,
) -> FeedPassSummary
where
    F: EventFeed,
    C: FeedStateStore,
    S: ProvenTxStore,
{
    let mut out = FeedPassSummary::default();
    // No header source, no verdict of any kind: nothing is read, nothing moves.
    let Some(tracker) = tracker else {
        out.stopped = Some("no header source configured".into());
        push_log("[chain-feed] no header source configured: no event consumed");
        return out;
    };
    let (mut state, mut version) = match store.read().await {
        Ok(Some((state, version))) => (state, Some(version)),
        Ok(None) => match start {
            Start::At(seed) => {
                let state = FeedState::seeded(seed, now_ms);
                let mut version = None;
                if !matches!(
                    persist(store, &state, &mut version, now_ms, &mut out).await,
                    Persist::Written
                ) {
                    return out;
                }
                push_log(&format!(
                    "[chain-feed] SEEDED the cursor at {seed} from {START_VAR}"
                ));
                out.seeded = Some(seed.clone());
                (state, version)
            }
            Start::Unset | Start::Malformed(_) => {
                let said = match start {
                    Start::Malformed(t) => format!(" ({START_VAR}={t:?} is not a decimal cursor)"),
                    _ => String::new(),
                };
                push_log(&format!(
                    "[chain-feed] no start cursor is configured{said}: the reorg feed reads nothing (set {START_VAR} to the journal's head, or POST /internal/chain-events-cursor)"
                ));
                out.unconfigured = true;
                out.stopped = Some("unconfigured".into());
                return out;
            }
        },
        Err(e) => {
            out.errors += 1;
            out.stopped = Some(format!("state read: {e}"));
            push_log(&format!(
                "[chain-feed] state read failed: nothing consumed: {e}"
            ));
            return out;
        }
    };
    let mut written = state.clone();
    // the kinds finished since the last write, counted once that write lands
    let mut unwritten: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut applied_unwritten = 0usize;
    let mut released_unwritten = 0usize;
    let mut buffer: VecDeque<(u64, Value)> = VecDeque::new();
    let mut more = true;
    let mut finished = 0u32;
    macro_rules! write_now {
        () => {{
            let landed = state == written
                || matches!(
                    persist(store, &state, &mut version, now_ms, &mut out).await,
                    Persist::Written
                );
            if landed {
                written = state.clone();
                for (kind, n) in std::mem::take(&mut unwritten) {
                    *out.consumed.entry(kind).or_default() += n;
                }
                out.applied += std::mem::take(&mut applied_unwritten);
                out.released += std::mem::take(&mut released_unwritten);
            } else {
                unwritten.clear();
                applied_unwritten = 0;
                released_unwritten = 0;
            }
            landed
        }};
    }
    loop {
        // ── the selecting event in hand ──
        if let Some(mut pending) = state.pending.clone() {
            if pending.fault_passes >= limits.fault_ceiling {
                push_log(&format!(
                    "[chain-feed] RELEASED at the fault ceiling: the {} at cursor {} ({}..={}) finished unresolved and recorded; its rows are the operator's: POST /internal/reorg {{\"fromHeight\": {}, \"toHeight\": {}}}",
                    pending.kind, pending.cursor, pending.lo, pending.hi, pending.lo, pending.hi
                ));
                let kind = kind_of_name(&pending.kind);
                state.release(pending, "fault-ceiling", now_ms);
                *unwritten.entry(kind).or_default() += 1;
                released_unwritten += 1;
                if !write_now!() {
                    break;
                }
                finished += 1;
                continue;
            }
            if finished >= limits.events_per_pass {
                break;
            }
            let legs = judge_window_legs(
                pot_storage,
                tx_store,
                tracker,
                fetcher,
                pending.lo,
                pending.hi,
                [
                    &mut pending.spenders,
                    &mut pending.pot_beefs,
                    &mut pending.transactions,
                ],
                limits.leg_limit,
                memo,
                &mut out.spenders,
                &mut out.pot_beefs,
                &mut out.transactions,
            )
            .await;
            match legs {
                WindowLegs::Faulted(why) => {
                    pending.fault_passes += 1;
                    push_log(&format!(
                        "[chain-feed] the {} at cursor {} ({}..={}) interrupted by a read fault: its cursor stays, replayed next pass ({why})",
                        pending.kind, pending.cursor, pending.lo, pending.hi
                    ));
                    state.pending = Some(pending);
                    out.faults += 1;
                    out.stopped = Some(why);
                    write_now!();
                    break;
                }
                WindowLegs::BudgetStop => {
                    let at = format!(
                        "the {} at cursor {} ({}..={})",
                        pending.kind, pending.cursor, pending.lo, pending.hi
                    );
                    state.pending = Some(pending);
                    out.budget_stops += 1;
                    out.stopped = Some(format!("{at}: the ladder's budget is spent"));
                    push_log(&format!(
                        "[chain-feed] {at}: the ladder's budget is spent; nothing judged blind of the couriers; the spenders leg resumes at its cursor next pass"
                    ));
                    write_now!();
                    break;
                }
                WindowLegs::Ran if pending.all_exhausted() => {
                    push_log(&format!(
                        "[chain-feed] APPLIED the {} at cursor {} ({}..={}): spenders scanned={} standing={} reanchored={} demoted={}; pot_beefs stale={}; transactions stale={}",
                        pending.kind,
                        pending.cursor,
                        pending.lo,
                        pending.hi,
                        out.spenders.scanned,
                        out.spenders.standing,
                        out.reanchored(),
                        out.demoted(),
                        out.pot_beefs.stale,
                        out.transactions.stale
                    ));
                    *unwritten.entry(kind_of_name(&pending.kind)).or_default() += 1;
                    applied_unwritten += 1;
                    state.cursor = pending.cursor.clone();
                    state.last_kind = Some(pending.kind.clone());
                    state.last_cursor = Some(pending.cursor);
                    state.pending = None;
                    if !write_now!() {
                        break;
                    }
                    finished += 1;
                    continue;
                }
                WindowLegs::Ran => {
                    out.stopped = Some(format!(
                        "the {} at cursor {} continues next pass (a leg is not exhausted)",
                        pending.kind, pending.cursor
                    ));
                    state.pending = Some(pending);
                    write_now!();
                    break;
                }
            }
        }
        // ── the next envelope ──
        if buffer.is_empty() {
            if !more || out.pages >= PAGES_PER_PASS {
                break;
            }
            out.pages += 1;
            let since = state.cursor.clone();
            match feed.page(&since).await {
                Err(e) => {
                    out.faults += 1;
                    out.stopped = Some(format!("events feed: {e}"));
                    push_log(&format!(
                        "[chain-feed] the feed read at cursor {since} failed: cursor unchanged: {e}"
                    ));
                    break;
                }
                Ok((409, _)) => {
                    let fresh = state.stop.as_ref().map(|s| s.cursor.as_str()) != Some(&since);
                    out.cursor_ahead = fresh;
                    state.stop = Some(Stopped {
                        cursor: since.clone(),
                        fault: "cursorAhead".into(),
                    });
                    out.stopped = Some(format!("the feed answered 409 cursorAhead at {since}"));
                    push_log(&format!(
                        "[chain-feed] STOPPED: the feed answered 409 cursorAhead at cursor {since}; the cursor is kept (never reset to 0: a replay re-judges the journal's bootstrap fork); the operator's lever sets it"
                    ));
                    break;
                }
                Ok((200, body)) => match parse_page(&body) {
                    Ok(page) => {
                        more = page.has_more;
                        buffer.extend(page.events);
                    }
                    Err(e) => {
                        out.faults += 1;
                        out.stopped = Some(e.clone());
                        push_log(&format!(
                            "[chain-feed] the feed page at cursor {since} did not read: cursor unchanged: {e}"
                        ));
                        break;
                    }
                },
                Ok((status, _)) => {
                    out.faults += 1;
                    out.stopped = Some(format!("events HTTP {status}"));
                    push_log(&format!(
                        "[chain-feed] the feed answered HTTP {status} at cursor {since}: cursor unchanged"
                    ));
                    break;
                }
            }
            if buffer.is_empty() {
                out.idle = true;
                break;
            }
        }
        let Some((at, envelope)) = buffer.pop_front() else {
            continue;
        };
        // delivery is at least once: a cursor at or below the stored one is never consumed again
        if at <= state.position() {
            continue;
        }
        let bytes = serde_json::to_vec(&envelope).unwrap_or_default();
        let event = match decode_envelope(&bytes) {
            Ok(event) => event,
            Err(e) => {
                let at_s = at.to_string();
                out.unreadable = state.stop.as_ref().map(|s| &s.cursor) != Some(&at_s);
                state.stop = Some(Stopped {
                    cursor: at_s,
                    fault: e.to_string(),
                });
                out.stopped = Some(format!("unreadable envelope at {at}: {e}"));
                push_log(&format!(
                    "[chain-feed] STOPPED at cursor {at}: {e} (the cursor is not passed; the operator reads the stored row)"
                ));
                break;
            }
        };
        let kind = kind_name(&event);
        let (lo, hi) = match selection(&event) {
            Selection::None => {
                state.cursor = at.to_string();
                state.last_kind = Some(kind.to_string());
                state.last_cursor = Some(at.to_string());
                state.stop = None;
                match &event {
                    ChainEvent::Tip { height, .. } => state.tip_height = Some(u64::from(*height)),
                    ChainEvent::TipAge { tip, .. } => {
                        state.tip_height = Some(u64::from(tip.height))
                    }
                    _ => {}
                }
                *unwritten.entry(kind).or_default() += 1;
                continue;
            }
            // a selecting event past the pass's budget is the next pass's (its cursor not passed)
            _ if finished >= limits.events_per_pass => break,
            Selection::Heights(lo, hi) => (lo, hi),
            Selection::BelowTip => match tracker.current_height().await {
                Ok(tip) => {
                    let tip = u64::from(tip);
                    (tip.saturating_sub(INVALIDATED_DEPTH), tip)
                }
                Err(e) => {
                    out.faults += 1;
                    out.stopped = Some(format!("tip read for the invalidated at {at}: {e}"));
                    push_log(&format!(
                        "[chain-feed] the tip read for the invalidated at cursor {at} failed: cursor unchanged: {e}"
                    ));
                    break;
                }
            },
        };
        push_log(&format!(
            "[chain-feed] the {kind} at cursor {at} selects the rows anchored in {lo}..={hi}"
        ));
        state.stop = None;
        state.pending = Some(PendingWindow::new(at, kind, lo, hi));
    }
    write_now!();
    out.state = Some(written);
    out
}

/// The stored kind back to its static name (the counters' key); an unknown one is `unknown`.
fn kind_of_name(name: &str) -> &'static str {
    [
        "tip",
        "fork",
        "reorg",
        "invalidated",
        "frozen",
        "tip_age",
        "split",
        "split_resolved",
    ]
    .into_iter()
    .find(|k| *k == name)
    .unwrap_or("unknown")
}

/// PURE: the row's health shape (`/health/invariants.chainFeed`, the lever's read).
pub fn state_json(state: &FeedState, version: Option<u64>, updated_at_ms: Option<u64>) -> Value {
    json!({
        "readable": true,
        "everRan": true,
        "cursor": state.cursor,
        "lastKind": state.last_kind,
        "lastCursor": state.last_cursor,
        "tipHeight": state.tip_height,
        "seed": state.seed,
        "startedAtMs": state.started_at_ms,
        "pending": state.pending.as_ref().map(|p| json!({
            "cursor": p.cursor,
            "kind": p.kind,
            "fromHeight": p.lo,
            "toHeight": p.hi,
            "faultPasses": p.fault_passes,
            "legsExhausted": {
                "spenders": p.spenders.exhausted,
                "potBeefs": p.pot_beefs.exhausted,
                "transactions": p.transactions.exhausted,
            },
        })),
        "stop": state.stop.as_ref().map(|s| json!({"cursor": s.cursor, "fault": s.fault})),
        "unresolved": state.unresolved.iter().map(|u| json!({
            "cursor": u.cursor,
            "kind": u.kind,
            "fromHeight": u.lo,
            "toHeight": u.hi,
            "why": u.why,
            "atMs": u.at_ms,
            "heal": format!("POST /internal/reorg {{\"fromHeight\": {}, \"toHeight\": {}}}", u.lo, u.hi),
        })).collect::<Vec<_>>(),
        "version": version,
        "updatedAtMs": updated_at_ms,
    })
}

/// What the operator's lever answered: the status, the body, the counters to bump.
#[derive(Debug, Clone, PartialEq)]
pub struct LeverAnswer {
    /// The HTTP status.
    pub status: u16,
    /// The JSON body.
    pub body: Value,
    /// The counters to bump.
    pub counters: Vec<(String, u64)>,
}

fn refused(status: u16, error: impl Into<String>) -> LeverAnswer {
    LeverAnswer {
        status,
        body: json!({"error": error.into()}),
        counters: Vec::new(),
    }
}

/// THE OPERATOR'S LEVER, `/internal/chain-events-cursor` (bearer `INTERNAL_TOKEN`, checked by the caller before any
/// read). `GET` answers the row. `POST {"cursor": "<decimal>"}` sets the cursor, back or forward, by the row's
/// compare-and-set: the recorded stop is cleared, and an event in hand is RELEASED (recorded unresolved, `operator`,
/// with its heal) so its rows are never silently dropped; an absent row is created with that `seed`. Counted
/// `overlay_chain_events_cursor_set_total`. Touches no other table.
pub async fn lever<C: FeedStateStore>(
    store: &C,
    post: bool,
    body: &[u8],
    now_ms: u64,
) -> LeverAnswer {
    let read = match store.read().await {
        Ok(r) => r,
        Err(e) => return refused(503, format!("read: {e}")),
    };
    if !post {
        return LeverAnswer {
            status: 200,
            body: json!({"row": read.as_ref().map(|(s, v)| state_json(s, Some(*v), None))}),
            counters: Vec::new(),
        };
    }
    let Ok(Value::Object(posted)) = serde_json::from_slice::<Value>(body) else {
        return refused(400, "malformed body: expected {\"cursor\": \"<decimal>\"}");
    };
    let cursor = match posted.get("cursor") {
        None => return refused(400, "cursor is required"),
        Some(Value::String(c)) => c.clone(),
        Some(other) => {
            return refused(400, format!("cursor must be a decimal string, not {other}"))
        }
    };
    let Some(cursor) = parse_cursor(&cursor).map(|n| n.to_string()) else {
        return refused(
            400,
            format!("cursor is not a decimal feed cursor: {cursor:?}"),
        );
    };
    let (old, mut state, version) = match read {
        Some((state, version)) => (Some(state.cursor.clone()), state, Some(version)),
        None => (None, FeedState::seeded(&cursor, now_ms), None),
    };
    if let Some(pending) = state.pending.clone() {
        state.release(pending, "operator", now_ms);
    }
    state.cursor = cursor.clone();
    state.stop = None;
    match store.write(&state, version, now_ms).await {
        Ok(Some(v)) => {
            push_log(&format!(
                "[chain-feed] CURSOR SET by the operator: {} -> {cursor} (the stop cleared; an event in hand released and recorded)",
                old.as_deref().unwrap_or("none")
            ));
            LeverAnswer {
                status: 200,
                body: json!({"ok": true, "old": old, "new": cursor, "row": state_json(&state, Some(v), Some(now_ms))}),
                counters: vec![("overlay_chain_events_cursor_set_total".into(), 1)],
            }
        }
        Ok(None) => refused(
            409,
            "the row moved under the lever (a pass wrote it): read it and set again",
        ),
        Err(e) => refused(503, format!("write: {e}")),
    }
}

// ── the production sources ──────────────────────────────────────────────

/// The feed over the CHAINTRACKS binding (or `CHAIN_TRACKER_URL`), under [`FEED_TIMEOUT_MS`].
pub struct ChaintracksFeed<'a>(pub &'a worker::Env);

impl EventFeed for ChaintracksFeed<'_> {
    async fn page(&self, since: &str) -> Result<(u16, Vec<u8>), String> {
        overlay_engine::gasp::race_or_deadline(
            crate::chain_tracker::chaintracks_events_page(self.0, since),
            crate::broadcaster::sleep_ms(FEED_TIMEOUT_MS),
        )
        .await
        .unwrap_or_else(|| Err(format!("no answer within {FEED_TIMEOUT_MS} ms")))
    }
}

/// The D1 row.
pub struct D1FeedState<'a>(pub &'a worker::D1Database);

#[derive(serde::Deserialize)]
struct StateRow {
    state: String,
    #[serde(default)]
    version: Option<f64>,
    #[serde(default)]
    updated_at: Option<f64>,
}

impl FeedStateStore for D1FeedState<'_> {
    async fn read(&self) -> Result<Option<(FeedState, u64)>, String> {
        let row: Option<StateRow> = crate::d1::Query::new(STATE_READ)
            .fetch_optional(self.0)
            .await
            .map_err(|e| e.to_string())?;
        row.map(|r| {
            Ok((
                FeedState::from_json(&r.state)?,
                r.version.unwrap_or(0.0).max(0.0) as u64,
            ))
        })
        .transpose()
    }

    async fn write(
        &self,
        state: &FeedState,
        expected_version: Option<u64>,
        now_ms: u64,
    ) -> Result<Option<u64>, String> {
        #[derive(serde::Deserialize)]
        struct Row {
            version: f64,
        }
        let row: Option<Row> = crate::d1::Query::new(STATE_WRITE)
            .bind(state.to_json())
            .bind(now_ms as f64)
            .bind(crate::d1_discovery::opt_f64(
                expected_version.map(|v| v as f64),
            ))
            .fetch_optional(self.0)
            .await
            .map_err(|e| e.to_string())?;
        Ok(row.map(|r| r.version.max(0.0) as u64))
    }
}

/// `/health/invariants.chainFeed`: the row, `readable: false` when it does not read, `everRan: false` before it.
pub async fn health_json(db: &worker::D1Database) -> Value {
    let row: Result<Option<StateRow>, _> =
        crate::d1::Query::new(STATE_READ).fetch_optional(db).await;
    match row {
        Err(_) => json!({"readable": false, "everRan": false}),
        Ok(None) => json!({"readable": true, "everRan": false, "cursor": null, "pending": null}),
        Ok(Some(r)) => match FeedState::from_json(&r.state) {
            Ok(state) => state_json(
                &state,
                r.version.map(|v| v.max(0.0) as u64),
                r.updated_at.map(|v| v.max(0.0) as u64),
            ),
            Err(e) => json!({"readable": false, "everRan": true, "error": e}),
        },
    }
}

fn start_of(env: &worker::Env) -> Start {
    Start::of(env.var(START_VAR).ok().map(|v| v.to_string()).as_deref())
}

/// ONE pass on the Worker's sources (the feed over the binding, the D1 row, chaintracks as the header source, the
/// courier ladder on the D7 ladder's budget), its outcome bumped into `ops_counters` and logged on one line. Runs on
/// the `*/15` cron and the block-event pass (`/internal/tip-changed`), before the routine sweep, which takes `memo`.
pub async fn run_chain_feed_pass(
    env: &worker::Env,
    pot_storage: &dyn PotStorage,
    db: &worker::D1Database,
    origin: &str,
    limits: PassLimits,
    memo: &mut RootMemo,
) -> FeedPassSummary {
    let tracker = crate::lookup_service_chain_tracker(env);
    let fetcher = crate::courier_fetcher(env, tracker.clone())
        .with_budget(crate::arcade_reorg::ARCADE_LADDER_BUDGET);
    let tx_store = crate::reorg_sweep::D1ProvenTxStore(db);
    let s = consume_chain_feed(
        &ChaintracksFeed(env),
        &D1FeedState(db),
        pot_storage,
        Some(&tx_store),
        tracker.as_deref(),
        Some(&fetcher),
        limits,
        memo,
        &start_of(env),
        js_sys::Date::now() as u64,
    )
    .await;
    for (name, n) in s.counters() {
        crate::ops::bump_counter(db, &name, n).await;
    }
    for (name, n) in s.reorg_totals() {
        crate::ops::bump_counter(db, name, n).await;
    }
    // bsv-low #484, as D7's pass: reorg evidence clears the app layer's CONFIRMED probe memos so the next owed walk
    // re-probes them (`hop_probe_memos`)
    crate::hop_probe_memos::clear_on_reorg(
        db,
        origin,
        crate::hop_probe_memos::feed_pass_rejudged(&s),
    )
    .await;
    worker::console_log!(
        "[chain-feed] ({origin}) pages={} consumed={:?} applied={} released={} spenders scanned={} standing={} reanchored={} demoted={} budget_stops={}; pot_beefs stale={}; transactions stale={}; faults={} errors={} contended={} unreadable={} cursor_ahead={} unconfigured={} idle={} cursor={:?} pending={:?} stopped={:?}",
        s.pages,
        s.consumed,
        s.applied,
        s.released,
        s.spenders.scanned,
        s.spenders.standing,
        s.reanchored(),
        s.demoted(),
        s.budget_stops,
        s.pot_beefs.stale,
        s.transactions.stale,
        s.faults,
        s.errors,
        s.contended,
        s.unreadable,
        s.cursor_ahead,
        s.unconfigured,
        s.idle,
        s.state.as_ref().map(|st| st.cursor.clone()),
        s.state
            .as_ref()
            .and_then(|st| st.pending.as_ref())
            .map(|p| (p.cursor.clone(), p.kind.clone(), p.lo, p.hi)),
        s.stopped
    );
    s
}

/// `/internal/chain-events-cursor` (bearer `INTERNAL_TOKEN`, 401 before any read): [`lever`] over the D1 row.
pub async fn internal_cursor(
    mut req: worker::Request,
    env: &worker::Env,
    db: Option<&worker::D1Database>,
) -> worker::Result<worker::Response> {
    let authorization = req.headers().get("authorization").ok().flatten();
    let secret = env.secret("INTERNAL_TOKEN").ok().map(|s| s.to_string());
    if !crate::tip_pass::bearer_ok(authorization.as_deref(), secret.as_deref()) {
        return worker::Response::error("unauthorized", 401);
    }
    let post = match req.method() {
        worker::Method::Post => true,
        worker::Method::Get => false,
        _ => return worker::Response::error("method not allowed", 405),
    };
    let body = if post { req.bytes().await? } else { Vec::new() };
    let Some(db) = db else {
        return worker::Response::error("no D1 handle", 503);
    };
    let answer = lever(&D1FeedState(db), post, &body, js_sys::Date::now() as u64).await;
    for (name, n) in &answer.counters {
        crate::ops::bump_counter(db, name, *n).await;
    }
    Ok(worker::Response::from_json(&answer.body)?.with_status(answer.status))
}

#[cfg(test)]
#[path = "chain_feed_tests.rs"]
mod tests;
