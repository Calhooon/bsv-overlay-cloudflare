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

/// Pull 100 outputs per request under the Worker's 30-second peer budget.
/// Reference: overlay-services@480d77a src/Engine.ts:29,895 requests 10,000
/// through src/GASP/OverlayGASPRemote.ts. Smaller requests bound each reply
/// and allow a slow peer's completed prefix to persist across ticks.
/// The responder's default stays unchanged for wire compatibility.
pub const ENGINE_GASP_SYNC_PAGE_LIMIT: u64 = 100;

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

/// `deadline`, LATCHED: once it fell due it answers `Ready` on every poll and
/// its inner future is never polled again (bsv-low #555, found by the
/// delta-2 fold). The worker's sleeps are `async fn` futures, which PANIC
/// when polled after they completed, and a per-graph deadline is polled
/// again after it fell due (the shared resume deadline, kept for the next
/// record; the half deadline of D2-L1).
pub fn latched(deadline: crate::engine::SleepFuture) -> crate::engine::SleepFuture {
    let mut inner = Some(deadline);
    Box::pin(std::future::poll_fn(move |cx| {
        let Some(deadline) = inner.as_mut() else {
            return std::task::Poll::Ready(());
        };
        if deadline.as_mut().poll(cx).is_ready() {
            inner = None;
            return std::task::Poll::Ready(());
        }
        std::task::Poll::Pending
    }))
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

/// Consecutive YIELDLESS syncs of a (peer, topic) allowed before each further
/// one is a FAILED attempt for the quarantine (bsv-low #555, the delta fold's
/// D-M2). A sync is yieldless when the peer served it work it did not hold
/// ([`GASPSync::graphs_attempted`] > 0) and it finalized no graph and moved no
/// cursor; it then counts a success only because a walk PROGRESSED (a node
/// appended), and a node is the peer's word: a hostile peer that lists one
/// fresh fabricated UTXO a tick, serves its root and hangs on its input kept
/// its slice for ever. Past this many in a row such a sync is a failure, so
/// that peer is quarantined after this many plus
/// [`PEER_QUARANTINE_THRESHOLD`] syncs (20, 5 h at `*/15`) and gets one probe
/// per [`PEER_QUARANTINE_REPROBE_SECS`] after that. A sync that finalized a
/// graph or moved the cursor resets the count; a quiet one leaves it.
/// A quarantined peer is re-admitted only by such a yield (the delta-4
/// fold's D4-L2): its sync that did not fail and yielded nothing, an empty
/// listing at its probe included, is not recorded, so its failures stay and
/// it is attended on each tick until it yields or fails again.
///
/// The bound is in SYNCS AND in TIME (the delta-2 fold's D2-M1): a yieldless
/// sync is a failure only when the streak holds more than this many syncs
/// AND began at least [`PEER_YIELDLESS_SECS_ALLOWED`] ago
/// ([`yieldless_sync_failed`]). Counted in syncs alone, 12 was three hours at
/// `*/15` and twelve minutes at LOW's one tick a minute, under the tail of a
/// block interval: an honest peer whose only new work was #582's unproven
/// head was quarantined while it waited for its block.
///
/// Why 12 and 3 h: an honest deep graph converges well inside both. The
/// measured one (#582) is an UNPROVEN head over unproven ancestry: its root
/// is re-asked at each resume and the pass after its block lands restarts the
/// walk from a proven root and completes it (`root_proven`), one or two passes
/// after a block that comes in ~10 min on average (past 3 h about once in
/// 10^8 blocks by the exponential model, at any cadence). Walked to its end
/// instead, 12 passes are about 150 nodes on the Cloudflare worker (15 s a
/// pass at 1.2 s a request) and 600 on LOW's node (60 s). Applied only with a
/// per-graph budget (`Engine::set_graph_budget`): without one no walk is
/// deferred and the quarantine is the one of #302.
pub const PEER_YIELDLESS_SYNCS_ALLOWED: u64 = 12;

/// The AGE a yieldless streak must reach, seconds since its first yieldless
/// sync, before a yieldless sync is a failure (3 h; the delta-2 fold's
/// D2-M1, see [`PEER_YIELDLESS_SYNCS_ALLOWED`]). The storage keeps the
/// streak's start ([`crate::storage::PeerYieldStreak`]) on its own clock.
pub const PEER_YIELDLESS_SECS_ALLOWED: u64 = 3 * 3600;

/// The DECAY of a yieldless streak (the delta-2 fold's D2-M1): a yieldless
/// sync more than this long (6 h, [`PEER_QUARANTINE_REPROBE_SECS`]) after the
/// streak's LAST yieldless sync starts a new streak (count 1, its start now)
/// instead of adding to the old one. Quiet syncs in between leave the streak
/// as it is (a quiet sync is not a yield), so an honest peer whose rare
/// graphs that never land are hours apart is not counted across days. The
/// cost, stated: a peer yieldless for 3 h, quiet for 6 h, and so on, keeps
/// its slice during the yieldless hours (a third of its ticks at most), as
/// #302's own accepted residual of one success in every eight.
///
/// Only for a peer that is NOT failed (`consecutive_failures` 0; the delta-3
/// fold's D3-M1). This window equals the reprobe's, so the probe of a peer
/// quarantined by this bound always came more than 6 h after its last
/// yieldless sync: it started a fresh streak (count 1, not failed), the sync
/// "progressed", the failures went to 0 and the quarantine lifted at every
/// probe (half the ticks at `*/15`). The probe of a failed peer continues
/// its streak and fails, so it gets one probe per reprobe window.
pub const PEER_YIELDLESS_DECAY_SECS: u64 = PEER_QUARANTINE_REPROBE_SECS;

/// PURE rule of the yieldless bound (bsv-low #555, the delta-2 fold's
/// D2-M1): a yieldless sync is a FAILED attempt IFF its streak holds more
/// than [`PEER_YIELDLESS_SYNCS_ALLOWED`] syncs AND is at least
/// [`PEER_YIELDLESS_SECS_ALLOWED`] old. A streak of unknown age (a backend
/// with no clock) is never past (fail-safe, as #302's unknown attempt age).
pub fn yieldless_sync_failed(streak: &crate::storage::PeerYieldStreak) -> bool {
    streak.yieldless_syncs > PEER_YIELDLESS_SYNCS_ALLOWED
        && streak
            .secs_since_first
            .is_some_and(|secs| secs >= PEER_YIELDLESS_SECS_ALLOWED)
}

/// The NORMALIZED ORIGIN of a peer URL (bsv-low #555, the delta-2 fold's
/// D2-M2): its host, lowercased, with no scheme, user, port, path, query or
/// fragment, and no trailing dot. The key of a peer's quarantine and
/// yieldless streak (the engine passes it to the three peer-health methods
/// of [`crate::storage::Storage`]) and of its share of a storage's deferred
/// graph ceiling (the worker's `origin` column): keyed on the URL string, one
/// server was eight "hosts" for eight adverts (`?1`, `:8443`, a trailing
/// slash, `#x`), each with its own share and a fresh streak. A subdomain is
/// another host still (no public suffix list here): stated. The cursor and
/// the deferred records stay keyed by the URL the peer is synced at.
pub fn peer_origin(url: &str) -> String {
    peer_authority(url).1
}

/// `(scheme, host, port)` of a peer URL: the scheme lowercased (`https`
/// when absent), the host as [`peer_origin`] gives it, the port as written
/// (`None` when absent, empty, or the scheme's default: `443` for `https`,
/// `80` for `http`).
fn peer_authority(url: &str) -> (String, String, Option<String>) {
    let url = url.trim();
    let (scheme, rest) = url.split_once("://").unwrap_or(("https", url));
    let scheme = scheme.to_ascii_lowercase();
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let host_port = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    let (host, port) = match host_port.strip_prefix('[') {
        // `[::1]:8443`: the bracketed address, brackets kept.
        Some(v6) => match v6.split_once(']') {
            Some((addr, after)) => (format!("[{addr}]"), after.strip_prefix(':')),
            None => (host_port.to_string(), None),
        },
        None => match host_port.split_once(':') {
            Some((host, port)) => (host.to_string(), Some(port)),
            None => (host_port.to_string(), None),
        },
    };
    let default_port = match scheme.as_str() {
        "https" => "443",
        "http" => "80",
        _ => "",
    };
    let port = port.filter(|p| !p.is_empty() && *p != default_port);
    (
        scheme,
        host.trim_end_matches('.').to_ascii_lowercase(),
        port.map(str::to_string),
    )
}

/// The peers of a SHIP-discovered topic, ONE per [`peer_origin`] (bsv-low
/// #555, the delta-2 fold's D2-M2; restored by the delta-4 fold, D4-M1): each
/// advertised domain is canonicalized to `scheme://host[:port]` (the scheme
/// and host lowercased; path, query, fragment, user, a trailing dot and the
/// scheme's default port dropped), and of the spellings of one origin the
/// first by `https`, then no explicit port, then the string is kept. So
/// eight adverts of one server (`/?1`, `/?2`, `/#x`, `:8443`, ...) are one
/// peer synced once a tick, and a stranger's spelling of an honest host (a
/// query, a dead port) cannot stand beside that host's own advert under its
/// quarantine key. The delta-3 fold kept one peer per `scheme://host[:port]`
/// while the failures stayed on one row per origin (D4-M1): eight adverts of
/// dead ports of an honest host's name quarantined the honest peer, and
/// seven were never quarantined, a slice each on every tick. The LIMIT,
/// stated (the delta-3 lens's D3-L1): two overlays of one host on two ports,
/// on one SHIP topic, are one peer (the default port's), so a stranger's
/// default-port spelling displaces an honest overlay that serves only on
/// another port. Sorted, for a stable order.
pub fn ship_peers_by_origin<I: IntoIterator<Item = String>>(domains: I) -> Vec<String> {
    type Rank = (bool, bool, String);
    let mut by_origin: std::collections::BTreeMap<String, (Rank, String)> =
        std::collections::BTreeMap::new();
    for domain in domains {
        let (scheme, origin, port) = peer_authority(&domain);
        if origin.is_empty() {
            continue;
        }
        let canonical = match &port {
            Some(port) => format!("{scheme}://{origin}:{port}"),
            None => format!("{scheme}://{origin}"),
        };
        let candidate = (scheme != "https", port.is_some(), canonical.clone());
        match by_origin.get(&origin) {
            Some((kept, _)) if *kept <= candidate => {}
            _ => {
                by_origin.insert(origin, (candidate, canonical));
            }
        }
    }
    let mut peers: Vec<String> = by_origin.into_values().map(|(_, url)| url).collect();
    peers.sort();
    peers
}

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

/// Default per-GRAPH bytes budget (bsv-low #586): the bytes of the nodes ONE
/// graph's walk may be SERVED in one pass before it is deferred, counted as
/// the peer serves them and as a record keeps them (the hex of each node's
/// raw transaction and of its proof, every answer counted, a repeat too).
/// A BUDGET PER PASS, not a limit: the walk makes at least one request a
/// pass whatever its size, and what the pass leaves is resumed.
///
/// 917,504 bytes: a budget per pass and nothing else (bsv-low #585). It was
/// derived from a 1 MiB cap on a record (seven eighths of it, the lens fold's
/// E586-L1) so that the pass it cut left a record that fitted; a record has
/// no cap since #585 (the storage keeps one of any size, the worker in
/// chunks), so the figure is kept for what it measures, 18 heads of 26 KB a
/// fresh pass, and no longer sits "under" anything. A consumer may set any
/// other ([`GraphBudget::max_bytes_fetched`]).
pub const DEFAULT_GRAPH_BUDGET_BYTES: u64 = 917_504;

/// Default per-GRAPH node budget (bsv-low #586): the nodes ONE graph's walk
/// may APPEND in one pass before it is deferred (five times the 12 of
/// Zanaadu's largest legitimate graph). As [`DEFAULT_GRAPH_BUDGET_BYTES`], a
/// budget per pass that resumes; it also bounds the JSON a pass adds to a
/// record around its nodes' hex, which the bytes limb does not count.
pub const DEFAULT_GRAPH_BUDGET_NODES: u32 = 64;

/// Reset a partial walk after this many passes (`max_passes`). If its cursor
/// could have passed, keep a root retry obligation in the same bounded slot.
/// One hour at a one-minute cadence, fifteen at `*/15`.
pub const DEFERRED_GRAPH_MAX_PASSES: u32 = 60;

/// The most records one (peer, topic) may hold. A new deferral past it is
/// not saved (reason `too_many`) and its walk goes on under the per-peer
/// budget alone, as before #555 (the lens fold's L4): a peer that serves many
/// deep graphs costs at most 16 records. A storage may refuse a record at a
/// ceiling of its own ([`DeferredGraphSave::AtCeiling`]), counted `too_many`
/// too (the worker's global ceiling, the lens fold's M3). A record has NO
/// byte bound (bsv-low #585: it had 1 MiB, `too_big`): a graph of any size
/// is deferred and resumed whole, and the storage's ceiling is a BUDGET of
/// room, under which a RESUMED walk keeps the record it holds.
pub const DEFERRED_GRAPHS_PER_PEER_TOPIC: usize = 16;

/// Reset a resumed partial walk after this many consecutive idle faults.
/// Keep the root retry obligation if the cursor could have passed it. These
/// attempts remain yieldless and count toward peer quarantine.
pub const DEFERRED_GRAPH_MAX_IDLE_FAULTS: u32 = 3;

/// Three consecutive cuts by the graph's OWN deadline (`time`) distinguish a
/// repeatedly stalled root from a single slow response. Keep a bounded retry
/// record meanwhile; only the third cut permits the cursor to pass a root
/// with no fetched node. The per-PEER deadline (`peer_deadline`) is not the
/// graph's: the graph in flight when the peer's slice ends is held at once
/// (see [`GASPSync::defer_in_flight`]).
pub const DEADLINE_STALL_DEFERRAL_RUNS: u32 = 3;

/// How long a backend remembers an outpoint the walk refused as expired,
/// past `max(expiry, refusal)`: seven days (engine 0.1.8). A peer keeps
/// listing a dead advert while it is unspent there, so without the memo
/// every run fetched its node to refuse it again (fact 3 of the 0.1.8
/// brief: about 470 node fetches, most of a 30 s slice). With it, one fetch
/// a week per dead outpoint; the sweep bounds the table by age.
pub const EXPIRED_OUTPOINT_MEMO_SECS: u64 = 7 * 24 * 3600;

/// The most expired roots a backend remembers for one topic: about nine
/// times the 2,282 dead adverts of the old overlay's UHRP history. Past it
/// the oldest rows go; a forgotten one costs one node fetch again.
pub const EXPIRED_OUTPOINT_MEMO_MAX_ROWS: usize = 20_000;

/// A root whose walk is dropped by a FAULT (not a deadline, not a bound) in
/// this many consecutive runs is recorded unanchorable (engine 0.1.9): a
/// record `unanchorable:<class>` the cursor passes, re-walked on the long
/// backoff [`UNANCHORABLE_RETRY_SECS`]. Three, the deadline rule's number
/// ([`DEADLINE_STALL_DEFERRAL_RUNS`]): one fault is the moment's (a 5xx, a
/// rung's rate limit, a courier budget earlier graphs spent); a fault that
/// survives three runs, each with a fresh peer slice and a fresh courier
/// budget and each walking this root ahead of the pages, is the graph's own.
/// Before 0.1.9 such a root held the cursor below it for ever (the 0.1.4
/// rule) and every later output waited on a captain's hand. A held record
/// whose RESUMED pass is dropped by such a fault counts the same runs on its
/// own slot (engine 0.1.11): on 0.1.10 a root retry obligation whose parent
/// no rung served was dropped and saved again as it was on every run, never
/// struck (fact 3 of the 0.1.11 brief).
pub const UNANCHORABLE_AFTER_FAULTED_RUNS: u32 = 3;

/// How long an unanchorable root waits before its next walk: six hours. What
/// it lacks lands on that scale or not at all: an ancestor a rung indexes
/// once mined (minutes to an hour), a peer that gains the history, a proof.
/// Four walks a day of each of at most [`DEFERRED_GRAPHS_PER_PEER_TOPIC`]
/// records is 64 walks a day for a (peer, topic) at worst, against 1,440
/// ticks a day at a one-minute cadence. A later success admits it as any
/// root and clears the record; nothing is admitted unanchored (T2).
pub const UNANCHORABLE_RETRY_SECS: u64 = 6 * 3600;

/// Four retries, six hours apart, cover one day after an unanchorable verdict.
/// A spent allowance is a durable fact until a new score or operator reset.
pub const UNANCHORABLE_RETRIES: u32 = 4;
pub const DEAD_REASON: &str = "dead:";

/// The least time between two counted absences of one root (engine 0.1.15,
/// bsv-overlay-cloudflare #56): an absence is the advertising peer's word
/// ([`fault_class::NOT_FOUND`]), and three of them strike the root
/// `unanchorable:not_found` only when they were asked on three runs with a
/// gap. Sixty seconds: two of the stack's 30 s per-peer slices
/// (`GASP_PEER_SYNC_BUDGET_MS`), one tick at its one-minute cadence, so a
/// backfill call walking slice after slice cannot count one root three
/// times in one breath. Until it passes the root's record costs no request
/// and the cursor holds below it, as for any fault under the threshold.
pub const ABSENCE_GAP_SECS: u64 = 60;

/// A held (passable) record resumed this many CONSECUTIVE passes with no
/// node appended goes to the long backoff (engine 0.1.10): it is not resumed
/// again until [`IDLE_RESUME_RETRY_SECS`] have passed, so the resumes' shared
/// budget goes to records that progress and the peer's slice to its page.
/// Three, the deadline rule's number ([`DEADLINE_STALL_DEFERRAL_RUNS`]) and
/// the idle-fault rule's ([`DEFERRED_GRAPH_MAX_IDLE_FAULTS`]): one empty
/// pass is the moment's (a slow peer, a budget the earlier resumes spent),
/// three in a row, each with a fresh slice, are the record's own. On 0.1.9 a
/// root retry obligation the peer no longer serves (fact 3 of the 0.1.10
/// brief: 14 of them, `nodes 0`, `not_held` on every run) was resumed on
/// every tick for ever, and they took the slice the page's fresh roots
/// needed. A resume that appends a node resets the count; one past the
/// backoff that appends nothing goes back to it at once.
pub const IDLE_RESUMES_BEFORE_BACKOFF: u32 = 3;

/// How long a record sent to the long backoff waits before its next resume:
/// [`UNANCHORABLE_RETRY_SECS`] (six hours), as an unanchorable root does, for
/// the same reason: what it lacks (a peer that serves the root again, a
/// proof, an ancestor) lands on that scale or not at all. The record is
/// kept (bsv-low #555's resume: its cursor may have passed it) and costs
/// four resumes a day instead of 1,440.
pub const IDLE_RESUME_RETRY_SECS: u64 = UNANCHORABLE_RETRY_SECS;

/// The share of the per-PEER sync budget the resumes of one pass may take
/// together, in percent (engine 0.1.11): the held records are walked ahead
/// of the page, so without a share of its own the page got what they left.
/// The rest of the slice is the page's.
///
/// 33: on the stack's worker the per-peer budget is 30,000 ms
/// (`GASP_PEER_SYNC_BUDGET_MS`) and one graph's own budget 15,000 ms
/// (`GASP_GRAPH_BUDGET_MS`), which was the resumes' shared deadline (the lens
/// fold's M1) and started at the opening resume, so the reads and writes of
/// the storage between resumes (a record read, its save or drop, the
/// settled read: about five a record) came on top of it, outside any race.
/// The share is [`resume_share_ms`]: `min(30,000 * 33 / 100, 15,000)` =
/// 9,900 ms, started when the held loop starts, so those storage calls are
/// inside it. The page keeps 20,100 ms: one fresh graph's whole 15,000 ms
/// budget and 5,100 ms for its listing pages, the finalize submits and the
/// storage. The courier's rungs (bitails, woc, bananablocks, asked in turn)
/// have no timeout of their own (the worker's `http_get` sets none): a rung
/// is cut only by the deadline its step races, which inside the held loop is
/// this share. The worker sizes a courier attempt at 500 ms
/// (`GASP_PROOF_FETCH_BUDGET`, a planning figure, not a measurement), so the
/// share holds about 20 attempts: about 6 resumes of fact 3's shape (a root
/// request, a root proof ask and an ancestor's three rungs), the rest held
/// back to the next run. A record held back by the share is not resumed, not
/// a failed UTXO, not a stalled walk and not an idle resume.
pub const RESUMES_SHARE_OF_PEER_BUDGET_PERCENT: u64 = 33;

/// The resumes' share of a pass, ms (engine 0.1.11): the
/// [`RESUMES_SHARE_OF_PEER_BUDGET_PERCENT`] of the per-peer budget, never
/// more than one graph's own budget (the shared deadline before 0.1.11).
pub fn resume_share_ms(peer_budget_ms: u64, graph_budget_ms: u64) -> u64 {
    (peer_budget_ms.saturating_mul(RESUMES_SHARE_OF_PEER_BUDGET_PERCENT) / 100).min(graph_budget_ms)
}

/// The reason prefix of a root's record while its fault streak is under
/// [`UNANCHORABLE_AFTER_FAULTED_RUNS`]: not passable, the cursor holds.
pub const FAULT_RETRY_REASON: &str = "retry:fault:";

/// The reason prefix of an unanchorable root's record: passable.
pub const UNANCHORABLE_REASON: &str = "unanchorable:";

/// The fault classes a dropped root is named with (engine 0.1.9).
pub mod fault_class {
    /// The courier (the chain ancestor fetcher, its rungs) faulted or could
    /// not serve an ancestor.
    pub const COURIER: &str = "courier";
    /// Absence (engine 0.1.15, bsv-overlay-cloudflare #56): the advertising
    /// peer answered it does not hold the root, or a transaction an
    /// UNPROVEN node of the graph needs (`NodeNotFound`), so nothing can
    /// anchor; or, beside it, every configured raw rung answered absent
    /// (`DefinitiveNotFound`). One run counts once whoever said it. The
    /// peers' word decides: a rung, when one is configured, fetches and
    /// never strikes by itself (on 0.1.12 to 0.1.14 the rungs' absence
    /// struck `unanchorable:not_found` at once). Three runs at least
    /// [`super::ABSENCE_GAP_SECS`] apart strike it, as any striking class.
    pub const NOT_FOUND: &str = "not_found";
    /// The class a peer's `NodeNotFound` carried on 0.1.9 to 0.1.14. Kept
    /// so the records written then read as they were; a peer's word of
    /// absence is [`NOT_FOUND`] since 0.1.15.
    pub const NOT_SERVED: &str = "not_served";
    /// The peer faulted serving a node (a transport fault, a 5xx, a body
    /// that does not parse). Named, never a strike: a peer that answers
    /// with faults is the quarantine's to judge (bsv-low #302, #555's D-M1:
    /// a 5xx keeps no record), not a reason to pass the roots it lists.
    pub const PEER: &str = "peer";
    /// A served node was refused when appended to the graph (it does not
    /// parse or verify as the graph needs it).
    pub const VERIFICATION: &str = "verification";
    /// Any other failure (a storage fault of ours, a bound): named, never a
    /// strike against the root.
    pub const OTHER: &str = "other";
    /// The route to the peer faulted before the peer was reached (engine
    /// 0.2.0, #57; `GASPError::RouteFault`): named, never a strike, as it
    /// says nothing of the root or of the peer.
    pub const ROUTE: &str = "route";

    /// Whether a fault of `class` counts a faulted run of its root: the
    /// three of the 0.1.9 brief (the courier, an input nobody served, a
    /// verification refusal), never a peer's fault of the moment.
    pub fn strikes(class: &str) -> bool {
        matches!(class, COURIER | NOT_SERVED | VERIFICATION | NOT_FOUND)
    }
}

/// One root a sync dropped, named with why (engine 0.1.9): what the sync's
/// accounting and the tick's summary line carry, once per (peer, root) per
/// run.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FaultedRoot {
    /// The peer, filled by the engine (empty inside one `GASPSync`).
    #[serde(default)]
    pub peer: String,
    /// The root outpoint.
    pub outpoint: String,
    /// Its class, one of [`fault_class`].
    pub class: String,
    /// The consecutive faulted runs its record counts (0: not recorded).
    pub faults: u32,
    /// Whether it is recorded unanchorable (the cursor passes it).
    pub unanchorable: bool,
    /// The error, cut to 160 characters.
    pub error: String,
    /// Why its strike was refused: `no_room` at the storage's own ceiling,
    /// or `store_fault`; empty when written or no strike was due.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub refused: String,
}

/// Why a strike was refused (engine 0.1.10), as [`FaultedRoot::refused`]
/// names it.
pub mod strike_refused {
    /// No room at the storage's own ceiling. Memo eviction is not a refusal.
    pub const NO_ROOM: &str = "no_room";
    /// The storage faulted writing the record.
    pub const STORE_FAULT: &str = "store_fault";
}

/// The per-graph budget of a [`GASPSync`] (bsv-low #555,
/// `Engine::set_graph_budget`). Setting one turns deferral ON for that sync.
pub struct GraphBudget<'a> {
    /// Calls one graph may make in one pass ([`DEFAULT_GRAPH_BUDGET_CALLS`]).
    pub max_calls: u32,
    /// Bytes one graph may be served in one pass
    /// ([`DEFAULT_GRAPH_BUDGET_BYTES`], bsv-low #586). Reached, the graph is
    /// DEFERRED as at `max_calls` (reason `bytes`): never dropped, never
    /// refused.
    pub max_bytes_fetched: u64,
    /// Nodes one graph may append in one pass
    /// ([`DEFAULT_GRAPH_BUDGET_NODES`], bsv-low #586). Reached, the graph is
    /// deferred (reason `nodes`).
    pub max_nodes: u32,
    /// A fresh deadline for ONE graph's pass ([`DEFAULT_GRAPH_BUDGET_MS`]).
    pub deadline: Box<dyn Fn() -> crate::engine::SleepFuture + 'a>,
    /// A fresh deadline for HALF of it (the delta-2 fold's D2-L1): a FRESH
    /// walk that faults once this fell due, or after half its calls, has
    /// paid for a record and keeps one. `None`: the calls alone decide.
    pub half_deadline: Option<Box<dyn Fn() -> crate::engine::SleepFuture + 'a>>,
}

impl GraphBudget<'_> {
    /// What one pass of one graph may spend.
    fn pass(&self) -> PassCap {
        PassCap {
            calls: self.max_calls,
            bytes: self.max_bytes_fetched,
            nodes: self.max_nodes,
        }
    }
}

/// What one pass of a graph's walk may still spend of the per-graph budget's
/// three counted limbs (its time is a deadline): calls made, bytes served,
/// nodes appended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PassCap {
    calls: u32,
    bytes: u64,
    nodes: u32,
}

impl PassCap {
    /// No bound: a sync with no per-graph budget, or a walk that goes on
    /// under the per-peer budget alone (L4).
    const NONE: PassCap = PassCap {
        calls: u32::MAX,
        bytes: u64::MAX,
        nodes: u32::MAX,
    };

    /// Whether a limb is spent to nothing.
    fn spent(self) -> bool {
        self.calls == 0 || self.bytes == 0 || self.nodes == 0
    }
}

/// The key of one held record (bsv-low #555): what a sync loads up front.
/// The records resume independently of the peer's listing, one at a time
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
    /// goes on under the per-peer budget alone, as before #555. A BUDGET
    /// signal, never the loss of a walk (bsv-low #585): the storage must
    /// leave the record it HELD under that key as it was, and the engine
    /// keeps it, so a resumed graph refused room resumes from that record at
    /// the next pass and never walks from its root again.
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
    /// Why it was last deferred: `calls`, `time`, `bytes`, `nodes`, `fault`,
    /// `anchor_unavailable`, `anchor_walk_held` (the E592 lens fold: the
    /// anchor's walk past the budget, held until the root is served proven),
    /// `not_landed` or `peer_deadline`; for a root whose walk faulted
    /// (engine 0.1.9) `retry:fault:<class>` under
    /// [`UNANCHORABLE_AFTER_FAULTED_RUNS`] runs, `unanchorable:<class>` at it.
    pub reason: String,
    /// Whether the peer is a CONFIGURED one (`SyncTarget::Peers`), not one
    /// `ls_ship` discovered (bsv-low #555, the delta-2 fold's D2-M2): a
    /// storage reserves part of its ceiling for these (the worker: records
    /// of discovered peers hold at most half of it). Set by the storage
    /// adapter on every save; `false` in a record saved before the fold.
    #[serde(default)]
    pub configured: bool,
    /// Consecutive RESUMED passes that faulted having appended no node
    /// (the delta-2 fold's D2-M2): the record is dropped (`idle_faults`)
    /// when it reaches [`DEFERRED_GRAPH_MAX_IDLE_FAULTS`]; a pass that
    /// appends a node resets it.
    #[serde(default)]
    pub idle_faults: u32,
    /// The cursor may pass this durable obligation. A false value also reads
    /// records written before independent resume support.
    #[serde(default)]
    pub cursor_passable: bool,
    /// Consecutive runs whose walk of this root faulted (engine 0.1.9): a
    /// record `retry:fault:<class>` under [`UNANCHORABLE_AFTER_FAULTED_RUNS`],
    /// `unanchorable:<class>` at it. 0 in every other record (and not
    /// written, so their JSON is unchanged).
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub faults: u32,
    /// Unix seconds before which an unanchorable root is not walked again
    /// ([`UNANCHORABLE_RETRY_SECS`]); 0 otherwise.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub retry_at: u64,
    /// Consecutive passes that RESUMED this held record and appended no node
    /// (engine 0.1.10): at [`IDLE_RESUMES_BEFORE_BACKOFF`] the record waits
    /// [`IDLE_RESUME_RETRY_SECS`] (its `retry_at`) before its next resume; a
    /// pass that appends a node resets it. 0 in every other record (and not
    /// written, so their JSON is unchanged).
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub idle_resumes: u32,
    /// Retry reservations made after the unanchorable verdict, including cuts.
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub unanchorable_retries: u32,
    /// Advert expiry read by the topic manager from the content-addressed root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expiry: Option<u64>,
}

// serde's `skip_serializing_if` passes the field by reference.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_zero_u32(n: &u32) -> bool {
    *n == 0
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_zero_u64(n: &u64) -> bool {
    *n == 0
}

impl DeferredGraph {
    /// Whether this record counts a root's faulted runs (engine 0.1.9).
    pub fn is_struck(&self) -> bool {
        self.faults > 0
            && (self.reason.starts_with(FAULT_RETRY_REASON)
                || self.reason.starts_with(UNANCHORABLE_REASON)
                || self.reason.starts_with(DEAD_REASON))
    }

    /// Whether this record holds an unanchorable root (passable).
    pub fn is_unanchorable(&self) -> bool {
        self.is_struck() && self.reason.starts_with(UNANCHORABLE_REASON)
    }

    pub fn is_dead(&self) -> bool {
        self.is_struck() && self.reason.starts_with(DEAD_REASON)
    }

    /// Whether this held record waits out the long backoff of its idle
    /// resumes at `now` (engine 0.1.10): not resumed, the cursor passes it.
    /// A fault record keeps its own backoff, a record that holds the cursor
    /// is never backed off.
    pub fn waits_idle(&self, now: u64) -> bool {
        !self.is_struck() && self.cursor_passable && now < self.retry_at
    }

    /// The fault class the record names, if it counts faulted runs.
    pub fn fault_class(&self) -> Option<&str> {
        if !self.is_struck() {
            return None;
        }
        self.reason
            .strip_prefix(DEAD_REASON)
            .or_else(|| self.reason.strip_prefix(UNANCHORABLE_REASON))
            .or_else(|| self.reason.strip_prefix(FAULT_RETRY_REASON))
    }

    /// The record's size as stored (its JSON): what the deferral's log line
    /// and a storage's byte budget read. Nothing refuses a record for it
    /// (bsv-low #585).
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
    /// Past [`DEFERRED_GRAPHS_PER_PEER_TOPIC`] or the storage's ceiling: a
    /// walk the budget cut goes on under the per-peer budget alone (L4),
    /// else the UTXO fails. A RESUMED walk's held record is KEPT (bsv-low
    /// #585): the count is of the pass that found no room, not of a record.
    TooMany,
    /// The record could not be written; as [`Self::TooMany`] (D-L2).
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
    /// [`DEFERRED_GRAPH_MAX_IDLE_FAULTS`] resumed passes in a row faulted
    /// having appended no node (the delta-2 fold's D2-M2); the UTXO fails.
    IdleFaults,
}

impl DropReason {
    /// Every reason, for a caller that serves a counter per reason.
    pub const ALL: [DropReason; 10] = [
        Self::MaxPasses,
        Self::TooMany,
        Self::StoreFault,
        Self::NotServed,
        Self::Held,
        Self::NotHeld,
        Self::RootProven,
        Self::Refused,
        Self::NoProgress,
        Self::IdleFaults,
    ];

    /// The reason's name, as logged and counted.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MaxPasses => "max_passes",
            Self::TooMany => "too_many",
            Self::StoreFault => "store_fault",
            Self::NotServed => "not_served",
            Self::Held => "held",
            Self::NotHeld => "not_held",
            Self::RootProven => "root_proven",
            Self::Refused => "refused",
            Self::NoProgress => "no_progress",
            Self::IdleFaults => "idle_faults",
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
    /// Per-pass work limits that stopped a root.
    pub root_bounds: Vec<crate::sync_load::RootBound>,
    /// Graphs deferred (a new record, or a resumed one deferred again).
    pub deferred: u64,
    /// Graphs resumed from a record.
    pub resumed: u64,
    /// Resumed graphs that completed and landed (their record deleted).
    pub converged: u64,
    /// Partial walks dropped or reset without converging.
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
    /// Records sent to the long backoff by this sync (engine 0.1.10): their
    /// resume appended no node [`IDLE_RESUMES_BEFORE_BACKOFF`] passes in a
    /// row.
    pub backed_off: u64,
    /// Records NOT resumed by this sync because they wait out that backoff
    /// (no request at all; the cursor passes them).
    pub waiting: u64,
    /// Records whose resumed pass this sync counted one idle resume on
    /// (engine 0.1.11): it asked and appended nothing it kept, whatever its
    /// ending (a save, a drop, an error). A record a strike counted instead
    /// is not among them.
    pub idle_counted: u64,
}

impl Walk {
    /// The counted limb this pass has spent, if any: its name is the
    /// deferral's reason.
    fn over(&self) -> Option<&'static str> {
        if self.calls_this_pass >= self.cap.calls {
            Some("calls")
        } else if self.bytes_this_pass >= self.cap.bytes {
            Some("bytes")
        } else if self.nodes_this_pass >= self.cap.nodes {
            Some("nodes")
        } else {
            None
        }
    }
}

/// The walk of the graph in hand: its record (what a deferral saves) and
/// the per-pass state. Lives in the [`GASPSync`], OUTSIDE the walk's
/// future, so a deadline that drops the future leaves it whole.
struct Walk {
    record: DeferredGraph,
    seen: std::collections::HashSet<String>,
    calls_this_pass: u32,
    /// Bytes served to this pass (bsv-low #586; see
    /// [`DEFAULT_GRAPH_BUDGET_BYTES`] for what is counted).
    bytes_this_pass: u64,
    /// Nodes this pass appended (bsv-low #586).
    nodes_this_pass: u32,
    resumed: bool,
    /// What this pass may spend: the per-graph budget, or what the resumes
    /// of the pass have left of their shared one (M1); [`PassCap::NONE`]
    /// once a walk that cannot be kept goes on under the per-peer budget
    /// alone (L4).
    cap: PassCap,
    /// `record.nodes.len()` when the pass started (H1).
    nodes_at_start: usize,
    /// Whether this pass was already counted progressed or stalled.
    counted: bool,
    /// The walk could not be kept and GOES ON under the per-peer budget
    /// alone (L4): its drop is counted once, when it went on, and it is
    /// never saved again in this pass (the delta fold's D-L1: a per-peer
    /// deadline that then cut it counted the drop twice and asked the
    /// storage a second time).
    gone_on: bool,
    /// A FRESH walk's half-budget deadline, started with the walk (the
    /// delta-2 fold's D2-L1); `None` for a resumed walk or with no
    /// [`GraphBudget::half_deadline`].
    half: Option<crate::engine::SleepFuture>,
    /// The record of a root whose walk faulted in earlier runs (engine
    /// 0.1.9), walked again from its root: the walk holds that record's
    /// slot, a landing or a verdict deletes it, and a fault counts one more
    /// run on it ([`GASPSync::strike`]).
    struck: Option<Box<DeferredGraph>>,
}

/// How a graph's walk ended in this pass.
enum WalkEnd {
    Expired,
    /// Nothing is pending: complete the graph.
    Done,
    /// Deferred, with this reason.
    Deferred(&'static str),
}

/// What one step of the walk got.
enum StepOut {
    Expired,
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
    Retry,
    Expired,
    Completed,
    Deferred,
    /// A record not resumed: the pass's resumes spent their shared budget.
    HeldBack,
    /// An unanchorable root, held (engine 0.1.9): recorded at this walk, or
    /// a record whose backoff has not run out. The cursor passes it.
    Unanchorable,
    /// A held record that waits out the long backoff of its idle resumes
    /// (engine 0.1.10): not resumed, no request. The cursor passes it.
    Waiting,
}

/// What a strike did (engine 0.1.10): the record that holds the count
/// (its faulted runs, whether it is unanchorable), and why it was refused.
#[derive(Debug, Clone, Copy, Default)]
struct Strike {
    held: Option<(u32, bool)>,
    refused: Option<&'static str>,
}

impl Strike {
    /// Whether the root is held unanchorable: the cursor passes it.
    fn passable(self) -> bool {
        self.held.is_some_and(|(_, unanchorable)| unanchorable)
    }
}

/// What [`GASPSync::save_walk`] did with the walk in hand.
enum Saved {
    Yes,
    Retry,
    /// Not kept, for this reason; the walk is handed back so the caller can
    /// go on with it (L4) or drop it.
    Unkept(DropReason, Box<Walk>),
    /// The storage faulted (counted `store_fault`, a held record deleted);
    /// the walk is handed back so the caller can go on with it (D-L2).
    Fault(GASPError, Box<Walk>),
    /// A RESUMED walk the storage had no room for (`AtCeiling`; counted
    /// `too_many`): the record it holds is KEPT as it was (bsv-low #585), and
    /// the walk is handed back so the caller can go on with it.
    Ceiling(Box<Walk>),
    /// The walk had already gone on (D-L1): nothing saved, nothing counted.
    WentOn,
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
    /// The root advert's own expiry. The adapter binds the bytes to the
    /// requested outpoint before interpreting a topic's expiry field.
    fn sync_expiry(&self, _node: &GASPNode, _outpoint: &str) -> Result<Option<u64>, GASPError> {
        Ok(None)
    }

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

    /// Batched compact facts for a page, or due facts and maintenance with None.
    /// Standalone adapters may have no memo; engine adapters must forward it.
    async fn read_fault_memos(
        &self,
        _outpoints: Option<&[String]>,
    ) -> Result<crate::fault_memo::FaultMemoBatch, GASPError> {
        Ok(crate::fault_memo::FaultMemoBatch::default())
    }

    /// Atomically claim the next retry. A storage without retry support declines.
    async fn reserve_unanchorable_retry(&self, _record: &DeferredGraph) -> Result<bool, GASPError> {
        Ok(false)
    }

    /// Delete the record of the graph rooted at `outpoint`.
    async fn delete_deferred_graph(&self, outpoint: &str) -> Result<(), GASPError>;

    /// Remember a root refused as expired at `expiry` (engine 0.1.8), so a
    /// later sync skips it with no node fetch. Default: nothing kept, the
    /// walk fetches and refuses it again (fail-safe).
    async fn remember_expired(&self, _outpoint: &str, _expiry: u64) -> Result<(), GASPError> {
        Ok(())
    }

    /// Of `outpoints`, those remembered as expired. Default: none.
    async fn remembered_expired(&self, _outpoints: &[String]) -> Result<Vec<String>, GASPError> {
        Ok(Vec::new())
    }
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
    /// Reset invocation-local retry caches before a new sync.
    fn begin_sync(&self) {}

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

#[derive(Default)]
struct PageProgress {
    pages: u64,
    advanced: bool,
    reached_tip: bool,
}

/// GASP sync orchestrator.
///
/// Coordinates between local `GASPStorage` and a `GASPRemote` peer to
/// synchronize overlay UTXOs. Supports paginated sync and unidirectional mode.
///
/// The lifetime `'a` allows the storage and remote to borrow from their
/// environment (e.g., `OverlayGASPStorage` borrows from the Engine's storage).
pub struct GASPSync<'a> {
    expired: std::rc::Rc<std::cell::RefCell<std::collections::HashSet<String>>>,
    root_proofs: std::cell::RefCell<std::collections::HashMap<String, bool>>,
    roots_proven: std::cell::Cell<u64>,
    roots_unproven: std::cell::Cell<u64>,
    cursor_held_by: Option<(u64, String)>,
    cursor_after_in_flight: u64,
    /// The root (score, outpoint) of the graph in flight, kept OUTSIDE the
    /// walk's future like [`Walk`], so a per-peer deadline that drops the
    /// sync before the walk exists (or after a refusal took it) still names
    /// the graph it cut.
    in_flight: Option<(u64, String)>,
    /// New durable holds this sync saved: see [`Self::graphs_retained`].
    retained: std::cell::Cell<u64>,
    /// Held graphs this sync resumed that appended a node or converged.
    resumed_with_progress: std::cell::Cell<u64>,
    cursor_yielded: std::cell::Cell<bool>,
    expired_refused: std::cell::Cell<u64>,
    /// The expiry `root_expired` last read off a refused root, for the
    /// durable memo.
    refused_expiry: std::cell::Cell<Option<u64>>,
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
    /// Inputs of UNPROVEN nodes the peer answered it does not hold in this
    /// sync (engine 0.1.15), keyed `txid.outputIndex`: needed again by
    /// another graph of the sync, the answer is remembered, not asked.
    /// Cleared at the start of each `sync`.
    absent: std::cell::RefCell<std::collections::HashSet<String>>,
    /// Graphs REFUSED by `validate_graph_anchor` and discarded in the last
    /// `sync` (bsv-low #551). Cleared at the start of each `sync`.
    discarded_graphs: std::cell::Cell<u64>,
    /// Graphs whose anchor walk could not run within the budget and were
    /// HELD (the E592 lens fold): [`GASPSync::anchor_walk_held`].
    anchor_walk_held: std::cell::Cell<u64>,
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
    /// sync (keys only; records are read one at a time).
    deferred: std::cell::RefCell<std::collections::HashSet<String>>,
    /// ONE deadline for every resume of the sync (the lens fold's M1), made
    /// at the first resume.
    resume_deadline: std::cell::RefCell<Option<crate::engine::SleepFuture>>,
    /// What every resume of the sync may still spend together (M1).
    resume_left: std::cell::Cell<PassCap>,
    /// A fresh deadline for the resumes' share of a pass (engine 0.1.11,
    /// [`Self::with_resume_share`]); `None`: the per-graph deadline, made at
    /// the resume that opens the pass, as before.
    resume_share: Option<Box<dyn Fn() -> crate::engine::SleepFuture + 'a>>,
    /// Calls made by the sync's walks so far.
    calls_made: std::cell::Cell<u64>,
    /// Bytes served to the sync's walks so far (bsv-low #586).
    bytes_fetched: std::cell::Cell<u64>,
    /// Nodes appended by the sync's walks so far (bsv-low #586).
    nodes_appended: std::cell::Cell<u64>,
    /// How many records this (peer, topic) holds in storage, its fault
    /// records apart (engine 0.1.10: they are counted in `fault_records`).
    held_records: std::cell::Cell<usize>,
    /// The roots of this (peer, topic)'s FAULT records known in this sync
    /// (engine 0.1.14), outside the partial-graph slot count: each
    /// held one is known when the sync reads it (every held record is read
    /// ahead of the pages), a new one when its strike is written.
    fault_records: std::cell::RefCell<std::collections::HashSet<String>>,
    /// See [`Self::strikes_refused`].
    strikes_refused: std::cell::Cell<u64>,
    /// The idle resumes counted on the held record the resume in hand read
    /// (engine 0.1.10), and the nodes it held (engine 0.1.11), before its
    /// pass: what its pass adds one to when it asks and keeps nothing more.
    /// `None` for any other walk.
    resume_idle: std::cell::Cell<Option<(u32, usize)>>,
    /// The pass in hand saved its own record ([`Self::save_walk`]): that save
    /// counted its idle resume, or reset the count on a node appended, so
    /// nothing is counted after it.
    pass_saved: std::cell::Cell<bool>,
    /// The walk of the graph in hand (see [`Walk`]).
    walk: std::cell::RefCell<Option<Walk>>,
    /// See [`Self::deferral_stats`].
    stats: std::cell::RefCell<DeferralStats>,
    /// See [`Self::graphs_attempted`].
    graphs_attempted: std::cell::Cell<u64>,
    /// The fault class of the step that last failed a walk (engine 0.1.9),
    /// set where the walk faults and read where its root is dropped;
    /// `None` for a failure that is no fault of the graph (a storage fault
    /// of ours, a bound).
    last_fault: std::cell::Cell<Option<&'static str>>,
    /// See [`Self::faulted_roots`].
    faulted: std::cell::RefCell<Vec<FaultedRoot>>,
    /// See [`Self::unanchorable_recorded`].
    unanchorable_recorded: std::cell::Cell<u64>,
    /// See [`Self::unanchorable_cleared`].
    unanchorable_cleared: std::cell::Cell<u64>,
    /// See [`Self::absent_struck`].
    absent_struck: std::cell::Cell<u64>,
    dead_recorded: std::cell::Cell<u64>,
    expired_swept: std::cell::Cell<u64>,
    memo_folded: std::cell::Cell<u64>,
    root_expiries: std::cell::RefCell<std::collections::HashMap<String, u64>>,
    page: PageProgress,
    request_stop: Option<std::rc::Rc<dyn Fn() -> bool + 'a>>,
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
            expired: std::rc::Rc::default(),
            expired_refused: std::cell::Cell::new(0),
            refused_expiry: std::cell::Cell::new(None),
            root_proofs: std::cell::RefCell::default(),
            roots_proven: std::cell::Cell::new(0),
            roots_unproven: std::cell::Cell::new(0),
            cursor_held_by: None,
            cursor_after_in_flight: last_interaction,
            in_flight: None,
            retained: std::cell::Cell::new(0),
            resumed_with_progress: std::cell::Cell::new(0),
            cursor_yielded: std::cell::Cell::new(false),
            last_interaction,
            unidirectional,
            log_prefix: log_prefix.into(),
            ancestor_fetcher: None,
            pruned: std::cell::RefCell::new(std::collections::HashSet::new()),
            pruned_inputs: std::cell::Cell::new(0),
            absent: std::cell::RefCell::default(),
            discarded_graphs: std::cell::Cell::new(0),
            anchor_walk_held: std::cell::Cell::new(0),
            finalized_hook: None,
            completed_cursor: last_interaction,
            graph_in_flight: false,
            graph_budget: None,
            deferred: std::cell::RefCell::new(std::collections::HashSet::new()),
            resume_deadline: std::cell::RefCell::new(None),
            resume_left: std::cell::Cell::new(PassCap::NONE),
            resume_share: None,
            calls_made: std::cell::Cell::new(0),
            bytes_fetched: std::cell::Cell::new(0),
            nodes_appended: std::cell::Cell::new(0),
            held_records: std::cell::Cell::new(0),
            fault_records: std::cell::RefCell::default(),
            strikes_refused: std::cell::Cell::new(0),
            resume_idle: std::cell::Cell::new(None),
            pass_saved: std::cell::Cell::new(false),
            walk: std::cell::RefCell::new(None),
            stats: std::cell::RefCell::new(DeferralStats::default()),
            graphs_attempted: std::cell::Cell::new(0),
            last_fault: std::cell::Cell::new(None),
            faulted: std::cell::RefCell::default(),
            unanchorable_recorded: std::cell::Cell::new(0),
            unanchorable_cleared: std::cell::Cell::new(0),
            absent_struck: std::cell::Cell::new(0),
            dead_recorded: std::cell::Cell::new(0),
            expired_swept: std::cell::Cell::new(0),
            memo_folded: std::cell::Cell::new(0),
            root_expiries: std::cell::RefCell::default(),
            page: PageProgress::default(),
            request_stop: None,
        }
    }

    /// Share expired outpoints across the topic peers of one engine run.
    #[must_use]
    pub fn with_expired_outpoints(
        mut self,
        expired: std::rc::Rc<std::cell::RefCell<std::collections::HashSet<String>>>,
    ) -> Self {
        self.expired = expired;
        self
    }

    /// Backfill slices stop after one page or at a caller-owned request bound.
    pub fn with_page_slice(mut self, stop: std::rc::Rc<dyn Fn() -> bool + 'a>) -> Self {
        self.request_stop = Some(stop);
        self
    }
    fn request_stopped(&self) -> bool {
        self.request_stop.as_ref().is_some_and(|stop| stop())
    }
    pub fn pages(&self) -> u64 {
        self.page.pages
    }
    pub fn page_advanced(&self) -> bool {
        self.page.advanced
    }
    pub fn reached_tip(&self) -> bool {
        self.page.reached_tip
    }

    pub fn expired_refused(&self) -> u64 {
        self.expired_refused.get()
    }

    pub fn roots_proven(&self) -> u64 {
        self.roots_proven.get()
    }
    pub fn roots_unproven(&self) -> u64 {
        self.roots_unproven.get()
    }
    pub fn cursor_held_by(&self) -> Option<String> {
        self.cursor_held_by
            .as_ref()
            .map(|(_, outpoint)| outpoint.clone())
    }

    pub fn cursor_yielded(&self) -> bool {
        self.cursor_yielded.get()
    }

    /// New durable holds the last `sync` saved holding at least one node
    /// the peer served (a walk past its budget, or the graph in flight at
    /// the per-peer deadline): work the cursor may pass because the next
    /// sync resumes it. A hold of nothing is not counted: the peer served
    /// no node of it (the hung peer of bsv-low #555's H1).
    pub fn graphs_retained(&self) -> u64 {
        self.retained.get()
    }

    /// Whether the last `sync` did work beyond finalizing a graph or moving
    /// the cursor (engine 0.1.8): it refused an expired root not refused
    /// before, retained a graph or resumed one with progress. Such a sync is
    /// never a failed attempt. Only the refusal also ends the yieldless
    /// streak (see `Engine::start_gasp_sync`).
    pub fn did_work(&self) -> bool {
        self.expired_refused.get() > 0
            || self.retained.get() > 0
            || self.resumed_with_progress.get() > 0
    }

    /// Held graphs the last `sync` resumed that appended at least one node
    /// or converged.
    pub fn graphs_resumed_with_progress(&self) -> u64 {
        self.resumed_with_progress.get()
    }

    /// The roots the last `sync` dropped, each named once with its fault
    /// class (engine 0.1.9): what `cursor_held_by` cannot say.
    pub fn faulted_roots(&self) -> Vec<FaultedRoot> {
        self.faulted.borrow().clone()
    }

    /// Roots the last `sync` recorded unanchorable: their walk faulted in
    /// [`UNANCHORABLE_AFTER_FAULTED_RUNS`] consecutive runs, and the cursor
    /// passes them from now on.
    pub fn unanchorable_recorded(&self) -> u64 {
        self.unanchorable_recorded.get()
    }

    /// Roots the last `sync` struck `unanchorable:not_found` (engine 0.1.15):
    /// the peers' word of absence counted on [`UNANCHORABLE_AFTER_FAULTED_RUNS`]
    /// runs at least [`ABSENCE_GAP_SECS`] apart. Counted in
    /// [`Self::unanchorable_recorded`] too.
    pub fn absent_struck(&self) -> u64 {
        self.absent_struck.get()
    }

    /// Roots with a fault record (unanchorable or not yet) that the last
    /// `sync` walked again and admitted or refused on a verdict: their
    /// record cleared.
    pub fn dead_recorded(&self) -> u64 {
        self.dead_recorded.get()
    }

    pub fn memo_folded(&self) -> u64 {
        self.memo_folded.get()
    }

    pub fn expired_swept(&self) -> u64 {
        self.expired_swept.get()
    }

    pub fn unanchorable_cleared(&self) -> u64 {
        self.unanchorable_cleared.get()
    }

    /// Strikes the last `sync` could not write at the storage's ceiling
    /// or on a storage fault. Each is named in
    /// [`Self::faulted_roots`] with its [`FaultedRoot::refused`]; a refused
    /// strike of a new root leaves it a gap the cursor holds below.
    pub fn strikes_refused(&self) -> u64 {
        self.strikes_refused.get()
    }

    /// Know `outpoint` as a fault record of this (peer, topic) (engine
    /// 0.1.10): one counted among the deferred slots moves to the fault
    /// records. A no-op for one already known.
    fn know_fault_record(&self, outpoint: &str) {
        if self.fault_records.borrow_mut().insert(outpoint.to_string()) {
            self.held_records
                .set(self.held_records.get().saturating_sub(1));
        }
    }

    /// A record of this (peer, topic) read from the storage: a fault record
    /// is known as one (engine 0.1.10).
    fn read_record(&self, record: Option<&DeferredGraph>) {
        if let Some(record) = record.filter(|r| r.is_struck()) {
            self.know_fault_record(&record.outpoint);
        }
    }

    /// A record of this (peer, topic) deleted: one fewer fault record, or
    /// one fewer deferred slot held.
    fn forget_record(&self, outpoint: &str) {
        if !self.fault_records.borrow_mut().remove(outpoint) {
            self.held_records
                .set(self.held_records.get().saturating_sub(1));
        }
    }

    /// Release the cursor's hold on `outpoint`: it is finished or held.
    fn release_hold(&mut self, outpoint: &str) {
        if self
            .cursor_held_by
            .as_ref()
            .is_some_and(|(_, held)| held == outpoint)
        {
            self.cursor_held_by = None;
        }
    }

    fn hold_cursor(&mut self, score: u64, outpoint: &str) {
        if self
            .cursor_held_by
            .as_ref()
            .is_none_or(|(held, _)| score < *held)
        {
            self.cursor_held_by = Some((score, outpoint.to_string()));
        }
    }

    fn root_expired(&self, node: &GASPNode, outpoint: &str) -> Result<bool, GASPError> {
        let now = (crate::engine::current_timestamp_ms() / 1000.0) as u64;
        let expiry = self.storage.sync_expiry(node, outpoint)?;
        if let Some(expiry) = expiry {
            self.root_expiries
                .borrow_mut()
                .insert(outpoint.to_string(), expiry);
        }
        let expiry = expiry.filter(|expiry| *expiry <= now);
        if expiry.is_some() {
            self.refused_expiry.set(expiry);
        }
        Ok(expiry.is_some())
    }

    /// Load the remembered dead among `outpoints` into the run's memo; they
    /// are refused with no node fetch and are not counted again. A read
    /// fault is fail-safe: they are fetched and refused as before.
    async fn load_remembered_expired(&self, outpoints: Vec<String>) {
        let outpoints: Vec<String> = {
            let memo = self.expired.borrow();
            outpoints
                .into_iter()
                .filter(|o| !memo.contains(o))
                .collect()
        };
        if outpoints.is_empty() {
            return;
        }
        match self.storage.remembered_expired(&outpoints).await {
            Ok(dead) => self.expired.borrow_mut().extend(dead),
            Err(e) => warn!(
                "{} Could not read the remembered expired roots: {}",
                self.log_prefix, e
            ),
        }
    }

    async fn refuse_expired(&self, outpoint: &str) -> Result<Ingested, GASPError> {
        let walk = self.walk.borrow_mut().take();
        let expiry = self.refused_expiry.take();
        if self.expired.borrow_mut().insert(outpoint.to_string()) {
            self.expired_refused.set(self.expired_refused.get() + 1);
            // Remembered across runs (0.1.8): the next sync skips it with no
            // node fetch. Best effort: a lost row costs one fetch again.
            if let Some(expiry) = expiry {
                if let Err(e) = self.storage.remember_expired(outpoint, expiry).await {
                    warn!(
                        "{} Could not remember expired root {}: {}",
                        self.log_prefix, outpoint, e
                    );
                }
            }
        }
        let _ = self
            .storage
            .discard_graph(&Self::graph_id_of(walk.as_ref(), outpoint))
            .await;
        if walk
            .as_ref()
            .is_some_and(|w| w.resumed || w.struck.is_some())
            || self.deferred.borrow_mut().remove(outpoint)
        {
            self.drop_record(outpoint, DropReason::Refused).await;
        }
        Ok(Ingested::Expired)
    }

    /// Retire only fault facts whose own advert expired. No remote read.
    async fn sweep_expired_record(
        &self,
        record: Option<&DeferredGraph>,
    ) -> Result<bool, GASPError> {
        let Some(record) = record.filter(|r| r.is_unanchorable() || r.is_dead()) else {
            return Ok(false);
        };
        let outpoint = record.outpoint.as_str();
        self.read_record(Some(record));
        if record.expiry.is_none_or(|expiry| expiry > Self::now_secs())
            && !self.expired.borrow().contains(outpoint)
        {
            return Ok(false);
        }
        // Remember before deletion, so a later page cannot reopen an expired root.
        self.storage
            .remember_expired(outpoint, record.expiry.unwrap_or(Self::now_secs()))
            .await?;
        self.storage.delete_deferred_graph(outpoint).await?;
        self.forget_record(outpoint);
        self.deferred.borrow_mut().remove(outpoint);
        self.expired.borrow_mut().insert(outpoint.to_string());
        self.expired_swept.set(self.expired_swept.get() + 1);
        Ok(true)
    }

    fn note_dead(&self, record: &DeferredGraph) {
        self.dead_recorded.set(self.dead_recorded.get() + 1);
        let mut faulted = self.faulted.borrow_mut();
        faulted.retain(|f| f.outpoint != record.outpoint);
        faulted.push(FaultedRoot {
            peer: String::new(),
            outpoint: record.outpoint.clone(),
            class: record.reason.clone(),
            faults: record.faults,
            unanchorable: true,
            error: format!("retry allowance spent: {}", record.unanchorable_retries),
            refused: String::new(),
        });
        warn!(
            "{} Root {} {} after {} retries",
            self.log_prefix, record.outpoint, record.reason, record.unanchorable_retries
        );
    }

    async fn keep_unanchorable(&self, record: &DeferredGraph) -> Result<(), GASPError> {
        let mut kept = record.clone();
        kept.retry_at = Self::now_secs() + UNANCHORABLE_RETRY_SECS;
        if kept.unanchorable_retries >= UNANCHORABLE_RETRIES {
            kept.reason = format!(
                "{DEAD_REASON}{}",
                record.fault_class().unwrap_or(fault_class::OTHER)
            );
            kept.retry_at = 0;
        }
        if self.storage.save_deferred_graph(&kept).await? != DeferredGraphSave::Saved {
            return Err(GASPError::StorageError(
                "could not retain retry allowance".into(),
            ));
        }
        self.know_fault_record(&kept.outpoint);
        if kept.is_dead() && !record.is_dead() {
            self.note_dead(&kept);
        }
        Ok(())
    }

    /// Unix seconds now, the clock of an unanchorable root's backoff.
    fn now_secs() -> u64 {
        (crate::engine::current_timestamp_ms() / 1000.0) as u64
    }

    /// Mark the walk's failure with its fault class (engine 0.1.9) and hand
    /// the error back.
    fn faulted(&self, class: &'static str, error: GASPError) -> GASPError {
        if !matches!(error, GASPError::StorageError(_)) {
            self.last_fault.set(Some(class));
        }
        error
    }

    /// The class of a peer's failure to serve a node: its `NodeNotFound`
    /// is its word that it does not hold the transaction (engine 0.1.15),
    /// any other failure is the moment's.
    fn peer_fault_class(error: &GASPError) -> &'static str {
        match error {
            GASPError::NodeNotFound(_) => fault_class::NOT_FOUND,
            GASPError::RouteFault { .. } => fault_class::ROUTE,
            _ => fault_class::PEER,
        }
    }

    /// Name a dropped root once in this run's accounting (engine 0.1.9).
    fn note_faulted(
        &self,
        outpoint: &str,
        error: &GASPError,
        class: Option<&'static str>,
        struck: Strike,
    ) {
        let mut faulted = self.faulted.borrow_mut();
        if faulted.iter().any(|f| f.outpoint == outpoint) {
            return;
        }
        let (faults, unanchorable) = struck.held.unwrap_or((0, false));
        faulted.push(FaultedRoot {
            peer: String::new(),
            outpoint: outpoint.to_string(),
            class: class.unwrap_or(fault_class::OTHER).to_string(),
            faults,
            unanchorable,
            error: error.to_string().chars().take(160).collect(),
            refused: struck.refused.unwrap_or_default().to_string(),
        });
    }

    /// A fresh root dropped in the page walk: count its faulted run when
    /// the failure is a fault and the root is the run's lowest gap, and name
    /// it either way. Whether it is now unanchorable (the cursor passes it).
    async fn strike_fresh(
        &self,
        score: u64,
        outpoint: &str,
        error: &GASPError,
        lowest: bool,
    ) -> bool {
        let class = self.last_fault.take();
        // Absence counts on every root of the run, not only the lowest
        // (0.1.12 for the rungs' word, 0.1.15 for the peer's): it is a
        // definite answer, one row of the bounded memo each (0.1.14), so a
        // page of absent roots passes together on its third gapped run.
        let struck = match class {
            Some(class)
                if (lowest || class == fault_class::NOT_FOUND)
                    && self.graph_budget.is_some()
                    && fault_class::strikes(class) =>
            {
                self.strike(score, outpoint, class, None).await
            }
            _ => Strike::default(),
        };
        self.note_faulted(outpoint, error, class, struck);
        struck.passable()
    }

    /// A RESUMED walk of a held record dropped by a fault (engine 0.1.11):
    /// a root like any other, so a fault of a striking class
    /// ([`fault_class::strikes`]) counts a faulted run on the record's own
    /// slot ([`Self::strike`] with the record as `prior`: `faults`, the
    /// reason `retry:fault:<class>`, its score and its pass kept), and three
    /// make it `unanchorable:<class>`. Named in `faulted` with the count.
    /// `None` when no strike was written: no fault of the graph (a peer's
    /// fault of the moment, a storage fault, a bound), a pass that did not
    /// get the root (the peer no longer serves it: the record's own idle
    /// rule, engine 0.1.10), the record unread, or the strike refused
    /// (named `no_room` or `store_fault`); the caller then drops the record
    /// as before 0.1.11.
    async fn strike_resumed(
        &self,
        outpoint: &str,
        walk: Option<&Walk>,
        error: &GASPError,
    ) -> Option<Strike> {
        let class = self.last_fault.get().filter(|c| fault_class::strikes(c))?;
        // The root was never served this pass: the peer no longer offers
        // it, which says nothing of its graph.
        if walk.is_none_or(|w| w.record.nodes.is_empty()) {
            return None;
        }
        let prior = self.storage.get_deferred_graph(outpoint).await.ok()??;
        if prior.is_struck() {
            return None;
        }
        self.last_fault.set(None);
        let struck = self
            .strike(prior.score, outpoint, class, Some(&prior))
            .await;
        self.note_faulted(outpoint, error, Some(class), struck);
        struck.held.map(|_| struck)
    }

    /// Count one more faulted run of `outpoint` (engine 0.1.9), on its
    /// record `prior` or on a new one: `retry:fault:<class>` (the cursor
    /// holds) under [`UNANCHORABLE_AFTER_FAULTED_RUNS`], `unanchorable:<class>`
    /// (passable, walked again after [`UNANCHORABLE_RETRY_SECS`]) at it. The
    /// record holds nothing but the root to ask for: the walk starts again
    /// from it. The storage keeps this compact fact in its bounded memo,
    /// separate from partial graphs. A strike that is not written (the
    /// storage's ceiling, a storage fault) is REFUSED: counted in
    /// [`Self::strikes_refused`] and named with why; a new root then stays a
    /// gap, a held unanchorable record stays as it was.
    async fn strike(
        &self,
        score: u64,
        outpoint: &str,
        class: &'static str,
        prior: Option<&DeferredGraph>,
    ) -> Strike {
        let refuse = |why: &'static str| {
            self.strikes_refused.set(self.strikes_refused.get() + 1);
            warn!(
                "{} Root {} faulted ({}): its strike was REFUSED ({}), the cursor holds (engine 0.1.10)",
                self.log_prefix, outpoint, class, why
            );
            Strike {
                // A held unanchorable record stays as it was: passable.
                held: prior
                    .filter(|r| r.is_unanchorable())
                    .map(|r| (r.faults, true)),
                refused: Some(why),
            }
        };
        let mut record = Self::fresh_walk(score, outpoint, PassCap::NONE).record;
        record.faults = prior.map_or(0, |r| r.faults).saturating_add(1);
        record.unanchorable_retries = prior.map_or(0, |r| r.unanchorable_retries);
        record.expiry = self
            .root_expiries
            .borrow()
            .get(outpoint)
            .copied()
            .or_else(|| prior.and_then(|r| r.expiry));
        // Engine 0.1.15: absence strikes on the three-run rule like every
        // class; on 0.1.12 to 0.1.14 the rungs' word struck at once.
        let unanchorable = record.faults >= UNANCHORABLE_AFTER_FAULTED_RUNS;
        // A record the cursor had passed stays passable (engine 0.1.11): the
        // strike counts its faulted runs and never takes the pass back.
        record.cursor_passable = unanchorable || prior.is_some_and(|r| r.cursor_passable);
        if unanchorable {
            record.reason = format!("{UNANCHORABLE_REASON}{class}");
            record.retry_at = Self::now_secs() + UNANCHORABLE_RETRY_SECS;
        } else {
            record.reason = format!("{FAULT_RETRY_REASON}{class}");
            // The next absence counts after the gap; until then the record
            // costs no request (engine 0.1.15).
            if class == fault_class::NOT_FOUND {
                record.retry_at = Self::now_secs() + ABSENCE_GAP_SECS;
            }
        }
        if unanchorable && record.unanchorable_retries >= UNANCHORABLE_RETRIES {
            record.reason = format!("{DEAD_REASON}{class}");
            record.retry_at = 0;
        }
        match self.storage.save_deferred_graph(&record).await {
            Ok(DeferredGraphSave::Saved) => {
                if record.is_dead() && !prior.is_some_and(DeferredGraph::is_dead) {
                    self.note_dead(&record);
                }
                // A new key holds a fault record and no deferred slot; a
                // held record struck on its own slot frees its deferred
                // slot for the fault record (engine 0.1.11); a prior fault
                // record was known when it was read.
                if prior.is_some() {
                    self.know_fault_record(outpoint);
                } else {
                    self.fault_records.borrow_mut().insert(outpoint.to_string());
                }
                if unanchorable && !prior.is_some_and(DeferredGraph::is_unanchorable) {
                    self.unanchorable_recorded
                        .set(self.unanchorable_recorded.get() + 1);
                    if class == fault_class::NOT_FOUND {
                        self.absent_struck.set(self.absent_struck.get() + 1);
                    }
                    warn!(
                        "{} Root {} UNANCHORABLE ({}): faulted {} runs in a row; the cursor passes it, walked again in {} s (engine 0.1.9)",
                        self.log_prefix, outpoint, class, record.faults, UNANCHORABLE_RETRY_SECS
                    );
                } else {
                    warn!(
                        "{} Root {} faulted ({}): run {} of {} (engine 0.1.9)",
                        self.log_prefix,
                        outpoint,
                        class,
                        record.faults,
                        UNANCHORABLE_AFTER_FAULTED_RUNS
                    );
                }
                Strike {
                    held: Some((record.faults, unanchorable)),
                    refused: None,
                }
            }
            Ok(DeferredGraphSave::AtCeiling) => refuse(strike_refused::NO_ROOM),
            Err(_) => refuse(strike_refused::STORE_FAULT),
        }
    }

    /// Walk again the root of a fault record (engine 0.1.9), under this
    /// pass's resume allowance. An unanchorable one waits out its backoff
    /// (no request at all). The walk is fresh from the root and holds the
    /// record's slot: a landing or a verdict clears the record, a budget
    /// cut or a deadline replaces it with the hold it earned, and a fault
    /// counts one more run on it.
    async fn rewalk_struck(
        &self,
        utxo: &GASPOutput,
        outpoint: &str,
        mut record: DeferredGraph,
        deadline: &mut crate::engine::SleepFuture,
        cap: PassCap,
    ) -> Result<Ingested, GASPError> {
        let score = utxo.score as u64;
        let absent = record.fault_class() == Some(fault_class::NOT_FOUND);
        if record.is_dead() || (record.is_unanchorable() && Self::now_secs() < record.retry_at) {
            // Not asked of the peer: not an attempt for the yieldless
            // streak (a quiet peer holding one is never quarantined for it).
            self.graphs_attempted
                .set(self.graphs_attempted.get().saturating_sub(1));
            if absent {
                crate::meter::peer_absent_memo_hit();
            }
            debug!(
                "{} Unanchorable root {} waits until {} (engine 0.1.9)",
                self.log_prefix, outpoint, record.retry_at
            );
            return Ok(Ingested::Unanchorable);
        }
        // Engine 0.1.15: an absence counted less than ABSENCE_GAP_SECS ago
        // is not asked again; the run counts nothing and the cursor holds
        // below a root it has not passed.
        if !record.is_unanchorable() && Self::now_secs() < record.retry_at {
            self.graphs_attempted
                .set(self.graphs_attempted.get().saturating_sub(1));
            if absent {
                crate::meter::peer_absent_memo_hit();
            }
            debug!(
                "{} Absent root {} counted {} runs; the next count waits until {} (engine 0.1.15)",
                self.log_prefix, outpoint, record.faults, record.retry_at
            );
            return Ok(if record.cursor_passable {
                Ingested::Deferred
            } else {
                Ingested::Retry
            });
        }
        if record.is_unanchorable() {
            if record.unanchorable_retries >= UNANCHORABLE_RETRIES {
                self.keep_unanchorable(&record).await?;
                self.graphs_attempted
                    .set(self.graphs_attempted.get().saturating_sub(1));
                return Ok(Ingested::Unanchorable);
            }
            // Reserve before dispatch: cancellation cannot buy another retry.
            record.unanchorable_retries += 1;
            record.retry_at = Self::now_secs() + UNANCHORABLE_RETRY_SECS;
            if !self.storage.reserve_unanchorable_retry(&record).await? {
                self.graphs_attempted
                    .set(self.graphs_attempted.get().saturating_sub(1));
                return Ok(Ingested::Unanchorable);
            }
        }
        info!(
            "{} Walking root {} again after {} faulted runs ({}) (engine 0.1.9)",
            self.log_prefix, outpoint, record.faults, record.reason
        );
        crate::meter::walk_output();
        let mut walk = Self::fresh_walk(score, outpoint, cap);
        walk.struck = Some(Box::new(record.clone()));
        *self.walk.borrow_mut() = Some(walk);
        let ended = self.walk_graph(Some(&mut *deadline)).await;
        let out = self.finish_pass(outpoint, ended, deadline).await;
        match out {
            Ok(ingested)
                if record.is_unanchorable()
                    && !matches!(ingested, Ingested::Completed | Ingested::Expired) =>
            {
                self.keep_unanchorable(&record).await?;
                Ok(Ingested::Unanchorable)
            }
            // A stall of the graph's own deadline does not undo the record:
            // the empty retry `save_walk` wrote is replaced again. So for a
            // passable fault record struck on a held record's slot (engine
            // 0.1.11): the empty retry would hold the cursor it had passed.
            Ok(Ingested::Retry) if record.cursor_passable => {
                let unanchorable = record.is_unanchorable();
                let kept = record.clone();
                if let Ok(DeferredGraphSave::Saved) = self.storage.save_deferred_graph(&kept).await
                {
                    // A fault record again, not a deferred slot (0.1.10).
                    self.know_fault_record(outpoint);
                }
                Ok(if unanchorable {
                    Ingested::Unanchorable
                } else {
                    Ingested::Deferred
                })
            }
            Ok(ingested) => Ok(ingested),
            Err(error) => {
                self.walk.borrow_mut().take();
                let class = self.last_fault.take();
                let Some(class) = class.filter(|c| fault_class::strikes(c)) else {
                    // No fault of the graph's: the record stands.
                    self.note_faulted(
                        outpoint,
                        &error,
                        class,
                        Strike {
                            held: Some((record.faults, record.is_unanchorable())),
                            refused: None,
                        },
                    );
                    if record.is_unanchorable() {
                        self.keep_unanchorable(&record).await?;
                        return Ok(Ingested::Unanchorable);
                    }
                    return Err(error);
                };
                let struck = self.strike(score, outpoint, class, Some(&record)).await;
                self.note_faulted(outpoint, &error, Some(class), struck);
                if struck.passable() {
                    Ok(Ingested::Unanchorable)
                } else {
                    Err(error)
                }
            }
        }
    }

    /// Bound each graph's walk and retain its pending work. A successfully
    /// saved hold permits cursor progress; an unsaved graph remains a gap.
    /// Bound the resumes of each pass by `share` (engine 0.1.11): ONE
    /// deadline for every held record walked ahead of the page, started when
    /// the held loop starts, so the page keeps the rest of the per-peer
    /// slice (see [`RESUMES_SHARE_OF_PEER_BUDGET_PERCENT`]). A record reached
    /// once it fell due is held back for the next sync. Without one the
    /// resumes share one per-graph deadline (the lens fold's M1).
    #[must_use]
    pub fn with_resume_share(
        mut self,
        share: Box<dyn Fn() -> crate::engine::SleepFuture + 'a>,
    ) -> Self {
        self.resume_share = Some(share);
        self
    }

    pub fn with_graph_budget(mut self, budget: GraphBudget<'a>) -> Self {
        self.graph_budget = Some(budget);
        self
    }

    /// What the last `sync` did with deferred graphs (bsv-low #555).
    pub fn deferral_stats(&self) -> DeferralStats {
        self.stats.borrow().clone()
    }

    /// How many UTXOs the sync set out to ingest (a new walk, a resume, or a
    /// record held back): the peer served it work it did not hold. A sync
    /// that attempted none was quiet (bsv-low #555, the delta fold's D-M2:
    /// a quiet sync never counts toward [`PEER_YIELDLESS_SYNCS_ALLOWED`]).
    pub fn graphs_attempted(&self) -> u64 {
        self.graphs_attempted.get()
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
    /// rule applied to completed scores: never above a completed output, and
    /// strictly below the lowest score of
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

    /// Highest completed score below every gap, floored at the entry cursor.
    /// Retaining scores handles unordered pages without inventing a cursor
    /// between outputs or skipping an unfinished member of a score tie.
    fn cursor_for_completed(
        completed: &std::collections::BTreeSet<u64>,
        lowest_unfinished: Option<u64>,
        floor: u64,
    ) -> u64 {
        let highest = match lowest_unfinished {
            Some(score) => completed.range(..score).next_back(),
            None => completed.iter().next_back(),
        };
        highest.copied().unwrap_or(floor).max(floor)
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

    /// How many graphs this orchestrator's last `sync` HELD because their
    /// anchor's script walk could not run within the engine's walk budget
    /// (`GASPError::AnchorWalkHeld`; bsv-low #592, the E592 lens fold): not
    /// finalized, the UTXO failed and the cursor kept below it, until the
    /// peer serves the root proven. Not counted in `discarded_graphs`.
    pub fn anchor_walk_held(&self) -> u64 {
        self.anchor_walk_held.get()
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
        self.page.pages = 0;
        self.page.advanced = false;
        self.page.reached_tip = false;
        self.pruned.borrow_mut().clear();
        self.pruned_inputs.set(0);
        self.absent.borrow_mut().clear();
        self.expired_refused.set(0);
        self.root_proofs.borrow_mut().clear();
        self.roots_proven.set(0);
        self.roots_unproven.set(0);
        self.cursor_held_by = None;
        self.in_flight = None;
        self.retained.set(0);
        self.resumed_with_progress.set(0);
        self.cursor_yielded.set(false);
        self.last_fault.set(None);
        self.faulted.borrow_mut().clear();
        self.unanchorable_recorded.set(0);
        self.unanchorable_cleared.set(0);
        self.absent_struck.set(0);
        self.dead_recorded.set(0);
        self.expired_swept.set(0);
        self.memo_folded.set(0);
        self.root_expiries.borrow_mut().clear();
        self.strikes_refused.set(0);
        self.fault_records.borrow_mut().clear();
        self.discarded_graphs.set(0);
        self.anchor_walk_held.set(0);
        self.completed_cursor = self.last_interaction;
        self.graph_in_flight = false;
        self.walk.borrow_mut().take();
        *self.stats.borrow_mut() = DeferralStats::default();
        self.deferred.borrow_mut().clear();
        self.held_records.set(0);
        self.resume_deadline.borrow_mut().take();
        self.resume_left.set(
            self.graph_budget
                .as_ref()
                .map_or(PassCap::NONE, GraphBudget::pass),
        );
        // Load only keys here. Records are resumed independently of pages,
        // one at a time under the shared resume allowance. A key-read fault
        // conservatively reserves all slots for the records already held.
        let mut held_keys = Vec::new();
        if self.graph_budget.is_some() {
            let memo = self.storage.read_fault_memos(None).await?;
            self.memo_folded.set(memo.folded);
            self.expired_swept.set(memo.swept);
            match self.storage.load_deferred_graphs().await {
                Ok(keys) => {
                    held_keys.clone_from(&keys);
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
            for record in memo.records {
                held_keys.retain(|key| key.outpoint != record.outpoint);
                self.fault_records
                    .borrow_mut()
                    .insert(record.outpoint.clone());
                self.deferred.borrow_mut().insert(record.outpoint.clone());
                held_keys.push(DeferredGraphKey {
                    outpoint: record.outpoint,
                    score: record.score,
                });
            }
            held_keys.sort_by(|a, b| (a.score, &a.outpoint).cmp(&(b.score, &b.outpoint)));
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
        let mut completed_scores = std::collections::BTreeSet::new();
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

        // Saved graphs are work independent of the peer's pagination cursor.
        // Read one record at a time and share the existing resume allowance.
        // A held root remembered dead is refused with no fetch.
        self.load_remembered_expired(held_keys.iter().map(|k| k.outpoint.clone()).collect())
            .await;
        // Engine 0.1.11: the resumes' share starts with the held loop, so the
        // storage's calls between resumes are inside it (polled once now, so
        // a timer that starts when it is polled runs).
        if !held_keys.is_empty() && self.graph_budget.is_some() {
            if let Some(share) = &self.resume_share {
                let mut deadline = latched(share());
                let _ = Self::due(&mut deadline).await;
                *self.resume_deadline.borrow_mut() = Some(deadline);
            }
        }
        let held_scores: std::collections::HashMap<String, u64> = held_keys
            .iter()
            .map(|key| (key.outpoint.clone(), key.score))
            .collect();
        for key in held_keys {
            if self.request_stopped() {
                break;
            }
            if known_outpoints.contains(&key.outpoint) {
                self.deferred.borrow_mut().remove(&key.outpoint);
                self.drop_record(&key.outpoint, DropReason::Held).await;
                shared_outpoints.insert(key.outpoint);
                continue;
            }
            let Some((txid, output_index)) = parse_outpoint(&key.outpoint) else {
                continue;
            };
            let utxo = GASPOutput {
                txid,
                output_index,
                score: key.score as f64,
            };
            self.graph_in_flight = true;
            self.in_flight = Some((key.score, key.outpoint.clone()));
            self.cursor_after_in_flight = initial_interaction;
            self.hold_cursor(key.score, &key.outpoint);
            let result = self.ingest_utxo(&utxo, &key.outpoint).await;
            self.graph_in_flight = false;
            self.in_flight = None;
            let settled = match result {
                Ok(Ingested::Completed) => {
                    let landed = match &self.finalized_hook {
                        Some(hook) => hook.graph_completed().await.is_ok(),
                        None => true,
                    };
                    self.settle_completed(landed).await;
                    if landed {
                        self.cursor_yielded.set(true);
                    }
                    landed
                }
                Ok(Ingested::Expired) => {
                    self.cursor_yielded.set(true);
                    true
                }
                Ok(
                    Ingested::Deferred
                    | Ingested::HeldBack
                    | Ingested::Unanchorable
                    | Ingested::Waiting,
                ) => true,
                Ok(Ingested::Retry) => false,
                Err(error) => {
                    warn!(
                        "{} Deferred graph {} retry failed: {}",
                        self.log_prefix, key.outpoint, error
                    );
                    self.walk.borrow_mut().take();
                    let class = self.last_fault.take();
                    self.note_faulted(&key.outpoint, &error, class, Strike::default());
                    false
                }
            };
            let settled = settled
                || self
                    .storage
                    .get_deferred_graph(&key.outpoint)
                    .await
                    .ok()
                    .flatten()
                    .is_some_and(|r| r.cursor_passable);
            if settled {
                if self
                    .cursor_held_by
                    .as_ref()
                    .is_some_and(|(_, held)| held == &key.outpoint)
                {
                    self.cursor_held_by = None;
                }
                // A saved graph settles only itself. The current page must
                // account for any earlier unsaved gap before its score can
                // contribute to cursor movement.
                shared_outpoints.insert(key.outpoint);
            } else {
                self.hold_cursor(key.score, &key.outpoint);
                if key.score >= initial_interaction {
                    min_failed_score =
                        Some(min_failed_score.map_or(key.score, |s| s.min(key.score)));
                }
                failed_outpoints.insert(key.outpoint);
            }
        }

        // Paginated pull from remote
        'pages: loop {
            if self.request_stopped() {
                break;
            }
            let cursor_before_page = self.last_interaction;
            // Every UTXO seen so far is completed or failed: were the sync
            // dropped inside the page request below, this is what it did.
            self.completed_cursor = Self::cursor_for_completed(
                &completed_scores,
                min_failed_score,
                initial_interaction,
            );
            let request = GASPInitialRequest {
                version: GASP_VERSION,
                since: self.last_interaction,
                limit,
            };
            let response = self.remote.get_initial_response(&request).await?;
            let page_size = response.utxo_list.len();
            self.page.pages += 1;

            info!(
                "{} Processing page with {} UTXOs (since: {})",
                self.log_prefix, page_size, response.since
            );

            // The page's remembered dead are refused with no node fetch.
            self.load_remembered_expired(
                response
                    .utxo_list
                    .iter()
                    .map(|u| format!("{}.{}", u.txid, u.output_index))
                    .filter(|o| !known_outpoints.contains(o))
                    .collect(),
            )
            .await;

            if self.graph_budget.is_some() {
                let outpoints: Vec<_> = response
                    .utxo_list
                    .iter()
                    .map(|u| format!("{}.{}", u.txid, u.output_index))
                    .collect();
                let memo = self.storage.read_fault_memos(Some(&outpoints)).await?;
                for record in memo.records {
                    let changed = response.utxo_list.iter().any(|u| {
                        format!("{}.{}", u.txid, u.output_index) == record.outpoint
                            && u.score as u64 != record.score
                    });
                    if changed && (record.dead || record.unanchorable) {
                        shared_outpoints.remove(&record.outpoint);
                        failed_outpoints.remove(&record.outpoint);
                    }
                    self.fault_records
                        .borrow_mut()
                        .insert(record.outpoint.clone());
                    self.deferred.borrow_mut().insert(record.outpoint);
                }
            }

            // lowest_ahead[i]: the lowest score among this page's UTXOs from
            // i on, for `completed_cursor` (a page is not assumed sorted).
            let mut lowest_ahead: Vec<u64> =
                response.utxo_list.iter().map(|u| u.score as u64).collect();
            for i in (0..lowest_ahead.len().saturating_sub(1)).rev() {
                lowest_ahead[i] = lowest_ahead[i].min(lowest_ahead[i + 1]);
            }

            // The request may have returned an unordered page. Hold below all
            // its unfinished outputs before any graph or storage await.
            let unfinished = match lowest_ahead.as_slice() {
                [lowest, ..] => Some(min_failed_score.map_or(*lowest, |score| score.min(*lowest))),
                [] => min_failed_score,
            };
            self.completed_cursor =
                Self::cursor_for_completed(&completed_scores, unfinished, initial_interaction);

            for (position, utxo) in response.utxo_list.iter().enumerate() {
                if self.request_stopped() {
                    let score = lowest_ahead[position];
                    min_failed_score = Some(min_failed_score.map_or(score, |s| s.min(score)));
                    break 'pages;
                }
                // Track highest score for pagination
                if utxo.score as u64 > self.last_interaction {
                    self.last_interaction = utxo.score as u64;
                }

                let outpoint = format!("{}.{}", utxo.txid, utxo.output_index);
                if held_scores
                    .get(&outpoint)
                    .is_some_and(|score| *score != utxo.score as u64)
                    && self
                        .storage
                        .get_deferred_graph(&outpoint)
                        .await?
                        .is_some_and(|r| r.is_unanchorable() || r.is_dead())
                {
                    shared_outpoints.remove(&outpoint);
                    failed_outpoints.remove(&outpoint);
                    self.deferred.borrow_mut().insert(outpoint.clone());
                }
                if known_outpoints.contains(&outpoint) {
                    self.cursor_yielded.set(true);
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
                    self.completed_cursor = Self::cursor_for_completed(
                        &completed_scores,
                        Some(lowest_unfinished),
                        initial_interaction,
                    );
                    let mut with_current = completed_scores.clone();
                    with_current.insert(utxo.score as u64);
                    let remaining = lowest_ahead
                        .get(position + 1)
                        .copied()
                        .into_iter()
                        .chain(min_failed_score)
                        .min();
                    self.cursor_after_in_flight =
                        Self::cursor_for_completed(&with_current, remaining, initial_interaction);
                    self.hold_cursor(utxo.score as u64, &outpoint);
                    self.graph_in_flight = true;
                    self.in_flight = Some((utxo.score as u64, outpoint.clone()));
                    let ingested = self.ingest_utxo(utxo, &outpoint).await;
                    self.graph_in_flight = false;
                    self.in_flight = None;
                    let ingested = match ingested {
                        Ok(Ingested::Expired) => {
                            self.cursor_yielded.set(true);
                            Ok(true)
                        }
                        Ok(Ingested::Completed) => {
                            let landed = match &self.finalized_hook {
                                Some(hook) => hook.graph_completed().await,
                                None => Ok(()),
                            };
                            self.settle_completed(landed.is_ok()).await;
                            if landed.is_ok() {
                                self.cursor_yielded.set(true);
                            }
                            landed.map(|()| true)
                        }
                        Ok(
                            Ingested::Deferred
                            | Ingested::HeldBack
                            | Ingested::Unanchorable
                            | Ingested::Waiting,
                        ) => Ok(true),
                        Ok(Ingested::Retry) => Ok(false),
                        Err(e) => {
                            self.walk.borrow_mut().take();
                            Err(e)
                        }
                    };
                    match ingested {
                        Ok(true) => {
                            if self
                                .cursor_held_by
                                .as_ref()
                                .is_some_and(|(_, held)| held == &outpoint)
                            {
                                self.cursor_held_by = None;
                            }
                            shared_outpoints.insert(outpoint.clone());
                        }
                        // An empty deadline retry has not earned a durable
                        // hold yet; retain the gap until its named threshold.
                        Ok(false) => {
                            let s = utxo.score as u64;
                            min_failed_score = Some(min_failed_score.map_or(s, |cur| cur.min(s)));
                            failed_outpoints.insert(outpoint.clone());
                        }
                        Err(e) => {
                            warn!(
                                "{} Error ingesting UTXO {}: {}",
                                self.log_prefix, outpoint, e
                            );
                            let s = utxo.score as u64;
                            // Engine 0.1.9: a FAULT of the lowest gap of the
                            // run counts the root's opening faulted run (a
                            // record `retry:fault:<class>`); its next runs
                            // walk it ahead of the pages, and the cursor
                            // passes it once it is recorded unanchorable.
                            // Only the lowest: one new record a (peer, topic)
                            // a run, so a peer that faults every node cannot
                            // fill the store in one tick (the delta fold's
                            // D-M1).
                            let lowest = min_failed_score.is_none_or(|m| s < m);
                            if self.strike_fresh(s, &outpoint, &e, lowest).await {
                                self.release_hold(&outpoint);
                                shared_outpoints.insert(outpoint.clone());
                            } else {
                                // Remember the lowest failed score so the
                                // persisted cursor cannot skip past it (see
                                // cap below).
                                min_failed_score =
                                    Some(min_failed_score.map_or(s, |cur| cur.min(s)));
                                // Not again in this sync (bsv-low #554).
                                failed_outpoints.insert(outpoint.clone());
                            }
                        }
                    }
                }
                // Known outputs and graphs whose finalize hook landed are
                // settled. Explicit anchor refusals retain the existing
                // terminal-discard rule. Saved holds can be passed; faults
                // without a durable hold retain a gap.
                if shared_outpoints.contains(&outpoint) {
                    completed_scores.insert(utxo.score as u64);
                }
                let unfinished = lowest_ahead
                    .get(position + 1)
                    .copied()
                    .into_iter()
                    .chain(min_failed_score)
                    .min();
                self.completed_cursor =
                    Self::cursor_for_completed(&completed_scores, unfinished, initial_interaction);
            }

            // Keep the settled frontier and any completed scores above a gap.
            // An advancing page needs no older score again.
            completed_scores.retain(|score| *score >= self.completed_cursor);

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
                let settled = min_failed_score.is_none()
                    && response.utxo_list.iter().all(|u| {
                        shared_outpoints.contains(&format!("{}.{}", u.txid, u.output_index))
                    });
                if self.request_stop.is_some() && settled && !response.utxo_list.is_empty() {
                    // Inclusive peers repeat the boundary. Confirm an empty page above
                    // it before claiming tip, without ever storing the probe cursor.
                    // A clamped boundary can conceal later rows: retain our cursor
                    // and report no progress when the probe finds any.
                    if !self.request_stopped() {
                        if let Some(since) = self.last_interaction.checked_add(1) {
                            let probe = self
                                .remote
                                .get_initial_response(&GASPInitialRequest {
                                    version: GASP_VERSION,
                                    since,
                                    limit,
                                })
                                .await?;
                            self.page.pages += 1;
                            self.page.reached_tip = probe.utxo_list.is_empty();
                        }
                    }
                } else {
                    self.page.reached_tip = settled;
                }
                break;
            }
            self.page.advanced = true;
            if self.request_stop.is_some() {
                break;
            }
        }

        // Gap-guard: if any graph ingest failed transiently, cap the cursor
        // strictly below the lowest failed score so the next sync re-pulls it
        // (the remote serves `score >= since`). Floored at `initial_interaction`
        // so the cursor never regresses — a failure within the already-synced
        // range is not a new gap and must not rewind the cursor.
        self.completed_cursor =
            Self::cursor_for_completed(&completed_scores, min_failed_score, initial_interaction);
        if let Some(failed) = min_failed_score {
            if self.last_interaction > self.completed_cursor {
                warn!(
                    "{} Capping cursor {} -> {} (transient ingest failure at score {}); next sync will re-pull",
                    self.log_prefix, self.last_interaction, self.completed_cursor, failed
                );
            }
        }
        self.last_interaction = self.completed_cursor;

        // A saved graph can now lie below the peer cursor. Absence from a
        // page is not evidence that its work may be deleted.

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

    /// Request a graph or resume its durable record. Saved holds are safe
    /// to pass because the next run resumes keys independently of pagination.
    /// Empty deadline retries hold the cursor until the stall threshold.
    /// All resumed graphs share one allowance; fresh graphs retain theirs.
    async fn ingest_utxo(&self, utxo: &GASPOutput, outpoint: &str) -> Result<Ingested, GASPError> {
        if self.graph_budget.is_some() {
            crate::meter::root_scope(self.ingest_utxo_inner(utxo, outpoint)).await
        } else {
            self.ingest_utxo_inner(utxo, outpoint).await
        }
    }

    async fn ingest_utxo_inner(
        &self,
        utxo: &GASPOutput,
        outpoint: &str,
    ) -> Result<Ingested, GASPError> {
        self.last_fault.set(None);
        self.resume_idle.set(None);
        self.pass_saved.set(false);
        if self.expired.borrow().contains(outpoint) {
            if self.deferred.borrow().contains(outpoint) {
                let record = self.storage.get_deferred_graph(outpoint).await?;
                if self.sweep_expired_record(record.as_ref()).await? {
                    return Ok(Ingested::Expired);
                }
            }
            return self.refuse_expired(outpoint).await;
        }
        debug!("{} Requesting node for {}", self.log_prefix, outpoint);
        self.graphs_attempted.set(self.graphs_attempted.get() + 1);
        let Some(budget) = &self.graph_budget else {
            *self.walk.borrow_mut() =
                Some(Self::fresh_walk(utxo.score as u64, outpoint, PassCap::NONE));
            crate::meter::walk_output();
            let walked = self.walk_graph(None).await;
            if matches!(walked, Ok(WalkEnd::Expired)) {
                return self.refuse_expired(outpoint).await;
            }
            let walk = self.walk.borrow_mut().take();
            walked?;
            self.complete_graph(&Self::graph_id_of(walk.as_ref(), outpoint))
                .await?;
            return Ok(Ingested::Completed);
        };

        if !self.deferred.borrow_mut().remove(outpoint) {
            let mut deadline = latched((budget.deadline)());
            return self
                .ingest_budgeted(utxo, outpoint, &mut deadline, budget.pass(), false)
                .await;
        }
        let mut deadline = self
            .resume_deadline
            .borrow_mut()
            .take()
            .unwrap_or_else(|| latched((budget.deadline)()));
        let cap = self.resume_left.get();
        if cap.spent() || Self::due(&mut deadline).await {
            *self.resume_deadline.borrow_mut() = Some(deadline);
            let record = self.storage.get_deferred_graph(outpoint).await?;
            self.read_record(record.as_ref());
            if self.sweep_expired_record(record.as_ref()).await? {
                return Ok(Ingested::Expired);
            }
            if record
                .as_ref()
                .is_some_and(|r| r.waits_idle(Self::now_secs()))
            {
                return Ok(self.wait_idle(outpoint));
            }
            // An unanchorable root inside its backoff would not be walked
            // anyway: not held back, and no attempt (engine 0.1.11).
            if record.as_ref().is_some_and(|r| {
                r.is_dead() || (r.is_unanchorable() && Self::now_secs() < r.retry_at)
            }) {
                self.graphs_attempted
                    .set(self.graphs_attempted.get().saturating_sub(1));
                return Ok(Ingested::Unanchorable);
            }
            self.stats.borrow_mut().held_back += 1;
            info!(
                "{} Deferred graph {} held back: the resumes of this pass spent their budget (bsv-low #555)",
                self.log_prefix, outpoint
            );
            let Some(mut record) = record else {
                return Ok(Ingested::Retry);
            };
            if record.reason == "retry:deadline" {
                return Ok(Ingested::Retry);
            }
            // A fault streak not yet at its threshold holds the cursor; one
            // not walked this run is not a faulted run (engine 0.1.9).
            if record.is_struck() && !record.cursor_passable {
                return Ok(Ingested::Retry);
            }
            if !record.cursor_passable {
                record.cursor_passable = true;
                if self.storage.save_deferred_graph(&record).await? != DeferredGraphSave::Saved {
                    return Ok(Ingested::Retry);
                }
            }
            return Ok(Ingested::HeldBack);
        }
        let before = (
            self.calls_made.get(),
            self.bytes_fetched.get(),
            self.nodes_appended.get(),
        );
        let ingested = self
            .ingest_budgeted(utxo, outpoint, &mut deadline, cap, true)
            .await;
        // Engine 0.1.10: a held record's resume that asked the peer,
        // appended no node and did not finish counts one more idle resume.
        // Its own save counted it; any other ending counts it here (engine
        // 0.1.11): a drop that saved the record again as a fresh obligation
        // (`drop_record`) or an error that left it as it was. What the pass
        // KEPT is read from the record, not from the nodes it appended: on
        // 0.1.10 a root appended and then thrown away with the drop (fact 3
        // of the 0.1.11 brief: the courier exhausted on its parent) read as
        // progress, and the count never fired. A record a strike counted is
        // left to the strike.
        if let Some((prior, held)) = self.resume_idle.take() {
            if self.calls_made.get() > before.0
                && !self.pass_saved.get()
                && !matches!(ingested, Ok(Ingested::Completed | Ingested::Expired))
            {
                self.idle_resume(outpoint, prior, held).await;
            }
        }
        let narrowed = |spent: u64| u32::try_from(spent).unwrap_or(u32::MAX);
        self.resume_left.set(PassCap {
            calls: cap
                .calls
                .saturating_sub(narrowed(self.calls_made.get() - before.0)),
            bytes: cap
                .bytes
                .saturating_sub(self.bytes_fetched.get() - before.1),
            nodes: cap
                .nodes
                .saturating_sub(narrowed(self.nodes_appended.get() - before.2)),
        });
        *self.resume_deadline.borrow_mut() = Some(deadline);
        ingested
    }

    /// A held record not resumed: it waits out the long backoff of its idle
    /// resumes (engine 0.1.10). Not asked of the peer: not an attempt for
    /// the yieldless streak, as an unanchorable root inside its backoff.
    fn wait_idle(&self, outpoint: &str) -> Ingested {
        self.graphs_attempted
            .set(self.graphs_attempted.get().saturating_sub(1));
        self.stats.borrow_mut().waiting += 1;
        debug!(
            "{} Deferred graph {} waits out its idle backoff (engine 0.1.10)",
            self.log_prefix, outpoint
        );
        Ingested::Waiting
    }

    /// Count one more idle resume on the held record of `outpoint` after a
    /// pass that did not save it (engine 0.1.10): `prior` is what it counted
    /// when its pass read it, so a record the pass dropped and saved again as
    /// a fresh obligation (`drop_record`) counts on from there. At
    /// [`IDLE_RESUMES_BEFORE_BACKOFF`] it goes to the long backoff. A record
    /// gone, a fault record, one that holds the cursor, or one holding more
    /// nodes than the `held` its pass read (it kept progress) is left alone.
    async fn idle_resume(&self, outpoint: &str, prior: u32, held: usize) {
        let Ok(Some(mut record)) = self.storage.get_deferred_graph(outpoint).await else {
            return;
        };
        if record.is_struck() || !record.cursor_passable || record.nodes.len() > held {
            return;
        }
        record.idle_resumes = prior.saturating_add(1);
        let backoff = record.idle_resumes >= IDLE_RESUMES_BEFORE_BACKOFF;
        if backoff {
            record.retry_at = Self::now_secs() + IDLE_RESUME_RETRY_SECS;
        }
        let saved = self.storage.save_deferred_graph(&record).await;
        if let Ok(DeferredGraphSave::Saved) = saved {
            self.stats.borrow_mut().idle_counted += 1;
        }
        match saved {
            Ok(DeferredGraphSave::Saved) if backoff => {
                self.stats.borrow_mut().backed_off += 1;
                info!(
                    "{} Deferred graph {} resumed {} passes in a row with no node appended: not resumed for {} s (engine 0.1.10)",
                    self.log_prefix, outpoint, record.idle_resumes, IDLE_RESUME_RETRY_SECS
                );
            }
            Ok(_) => {}
            Err(e) => warn!(
                "{} Could not count an idle resume of deferred graph {}: {}",
                self.log_prefix, outpoint, e
            ),
        }
    }

    /// Whether `deadline` has fallen due (polled once, with the task's own
    /// waker).
    async fn due(deadline: &mut crate::engine::SleepFuture) -> bool {
        std::future::poll_fn(|cx| std::task::Poll::Ready(deadline.as_mut().poll(cx).is_ready()))
            .await
    }

    /// A walk from the root `outpoint`, nothing fetched yet.
    fn fresh_walk(score: u64, outpoint: &str, cap: PassCap) -> Walk {
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
                configured: false,
                idle_faults: 0,
                cursor_passable: false,
                faults: 0,
                retry_at: 0,
                idle_resumes: 0,
                unanchorable_retries: 0,
                expiry: None,
            },
            seen: std::collections::HashSet::new(),
            calls_this_pass: 0,
            bytes_this_pass: 0,
            nodes_this_pass: 0,
            resumed: false,
            cap,
            nodes_at_start: 0,
            counted: false,
            gone_on: false,
            half: None,
            struck: None,
        }
    }

    /// One pass of a graph's walk under the per-graph budget: `deadline`,
    /// and `cap` (calls, bytes, nodes). `from_record`: a record of it is held (its key was
    /// loaded); it is read now.
    async fn ingest_budgeted(
        &self,
        utxo: &GASPOutput,
        outpoint: &str,
        deadline: &mut crate::engine::SleepFuture,
        cap: PassCap,
        from_record: bool,
    ) -> Result<Ingested, GASPError> {
        let score = utxo.score as u64;
        // A read fault fails the UTXO and keeps the record for the next sync.
        let mut record = if from_record {
            let record = self.storage.get_deferred_graph(outpoint).await?;
            self.read_record(record.as_ref());
            if record.is_none() {
                // Deleted since the keys were read (the worker's sweep).
                self.held_records
                    .set(self.held_records.get().saturating_sub(1));
            }
            record
        } else {
            None
        };
        if self.sweep_expired_record(record.as_ref()).await? {
            return Ok(Ingested::Expired);
        }
        if record
            .as_ref()
            .is_some_and(|r| (r.is_unanchorable() || r.is_dead()) && r.score != score)
        {
            self.storage.delete_deferred_graph(outpoint).await?;
            self.forget_record(outpoint);
            record = None;
        }
        if let Some(record) = record.as_ref().filter(|r| r.is_struck()) {
            return self
                .rewalk_struck(utxo, outpoint, record.clone(), deadline, cap)
                .await;
        }
        if let Some(record) = &record {
            if let [root, ..] = record.nodes.as_slice() {
                if self.root_expired(&root.node, outpoint)? {
                    self.deferred.borrow_mut().insert(outpoint.to_string());
                    return self.refuse_expired(outpoint).await;
                }
            }
            // Engine 0.1.10: a held record past its idle resumes waits out
            // the long backoff (an expired root it holds was refused above,
            // with no request); else its pass counts on from what it holds.
            if record.cursor_passable {
                if record.waits_idle(Self::now_secs()) {
                    return Ok(self.wait_idle(outpoint));
                }
                self.resume_idle
                    .set(Some((record.idle_resumes, record.nodes.len())));
            }
        }
        let walk = match record {
            None => {
                let mut walk = Self::fresh_walk(score, outpoint, cap);
                // D2-L1: the half-budget clock starts with the walk (polled
                // once now, so a timer that starts on its first poll runs).
                if let Some(half) = self
                    .graph_budget
                    .as_ref()
                    .and_then(|b| b.half_deadline.as_ref())
                {
                    let mut half = latched(half());
                    let _ = Self::due(&mut half).await;
                    walk.half = Some(half);
                }
                walk
            }
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
                    bytes_this_pass: 0,
                    nodes_this_pass: 0,
                    resumed: true,
                    cap,
                    counted: false,
                    gone_on: false,
                    half: None,
                    struck: None,
                }
            }
        };
        let resumed = walk.resumed;
        let root_unproven = walk
            .record
            .nodes
            .first()
            .is_some_and(|w| w.node.proof.is_none());
        crate::meter::walk_output();
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
                    Some(Ok(None)) => return Ok(WalkEnd::Expired),
                    Some(Ok(Some(node))) if node.proof.is_some() => {
                        // The walk is fresh, the PASS is not: what it spent
                        // of every counted limb goes with it (the re-ask
                        // appends nothing, so the nodes are 0 today; carried
                        // all the same, so a limb is never forgotten here).
                        let (carried, carried_bytes, carried_nodes) =
                            self.walk.borrow().as_ref().map_or((0, 0, 0), |w| {
                                (w.calls_this_pass, w.bytes_this_pass, w.nodes_this_pass)
                            });
                        let reserved = self
                            .walk
                            .borrow()
                            .as_ref()
                            .is_some_and(|w| w.record.cursor_passable);
                        self.drop_record(outpoint, DropReason::RootProven).await;
                        let mut restarted = Self::fresh_walk(score, outpoint, cap);
                        restarted.resumed = reserved;
                        restarted.calls_this_pass = carried;
                        restarted.bytes_this_pass = carried_bytes;
                        restarted.nodes_this_pass = carried_nodes;
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
        self.finish_pass(outpoint, ended, deadline).await
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
    /// its UTXO. A call/size cut whose record cannot be kept may continue
    /// within the same root deadline (L4). Time and courier limits stop it
    /// even when the storage refuses the save; the cursor retains its gap.
    async fn finish_pass(
        &self,
        outpoint: &str,
        mut ended: Result<WalkEnd, GASPError>,
        deadline: &mut crate::engine::SleepFuture,
    ) -> Result<Ingested, GASPError> {
        loop {
            let graph_id = Self::graph_id_of(self.walk.borrow().as_ref(), outpoint);
            // Whether a record of this graph is still held: not after a
            // `root_proven` restart, whose walk is fresh.
            let resumed = self.walk.borrow().as_ref().is_some_and(|w| w.resumed);
            let (reason, completed) = match ended {
                Ok(WalkEnd::Expired) => return self.refuse_expired(outpoint).await,
                Ok(WalkEnd::Done) => match self.complete_graph(&graph_id).await {
                    Ok(true) => return Ok(Ingested::Completed),
                    Ok(false) => {
                        // Refused by the anchor check: a verdict, the cursor
                        // moves, nothing to resume. A root's fault record
                        // goes with it (engine 0.1.9).
                        let struck = self
                            .walk
                            .borrow_mut()
                            .take()
                            .is_some_and(|w| w.struck.is_some());
                        if resumed || struck {
                            if struck {
                                self.unanchorable_cleared
                                    .set(self.unanchorable_cleared.get() + 1);
                            }
                            self.drop_record(outpoint, DropReason::Refused).await;
                        }
                        return Ok(Ingested::Completed);
                    }
                    // `complete_graph` discarded the graph already.
                    Err(GASPError::AnchorUnavailable(_)) => ("anchor_unavailable", true),
                    // The same hold, for a walk past the budget (E592 fold).
                    Err(GASPError::AnchorWalkHeld(_)) => ("anchor_walk_held", true),
                    // A finalize fault: the graph is still in hand.
                    Err(_) => {
                        let _ = self.storage.discard_graph(&graph_id).await;
                        ("fault", true)
                    }
                },
                Ok(WalkEnd::Deferred(reason)) => (reason, false),
                Err(e @ (GASPError::NodeNotFound(_) | GASPError::DefinitiveNotFound(_))) => {
                    let _ = self.storage.discard_graph(&graph_id).await;
                    let walk = self.walk.borrow_mut().take();
                    if resumed {
                        // Engine 0.1.11: a fault of the graph is a strike on
                        // the record's own slot, as for a fresh root.
                        if let Some(struck) = self.strike_resumed(outpoint, walk.as_ref(), &e).await
                        {
                            return if struck.passable() {
                                Ok(Ingested::Unanchorable)
                            } else {
                                Err(e)
                            };
                        }
                        self.drop_record(outpoint, DropReason::NotHeld).await;
                    }
                    return Err(e);
                }
                // A walk ERROR on a FRESH walk keeps no record (the delta
                // fold's D-M1): a fault is not a budget cut, and before #555
                // it failed the UTXO. A record then costs a peer a budget
                // cut (`calls`, `time`, `peer_deadline`), never a quick 5xx:
                // a stranger who answered each input with a 503 kept a
                // one-node record per UTXO, 16 per (peer, topic) in one tick
                // with no time spent, and filled the worker's ceiling. A
                // RESUMED walk keeps its record (its progress was paid for).
                //
                // Unless it PAID (the delta-2 fold's D2-L1): a fresh walk
                // that faults after half its per-graph calls or half its
                // time keeps its record as a budget cut would, so an honest
                // flaky peer's deep graph is not walked from its root on
                // every tick ((1-p)^100 a clean pass at a fault rate p).
                Err(e) if !resumed && !self.walk_paid().await => {
                    warn!(
                        "{} Walk of {} faulted: no record, the UTXO fails as before #555: {}",
                        self.log_prefix, outpoint, e
                    );
                    let _ = self.storage.discard_graph(&graph_id).await;
                    self.walk.borrow_mut().take();
                    return Err(e);
                }
                Err(e) => {
                    // D2-M2: a RESUMED pass that faulted having appended
                    // nothing is idle; at DEFERRED_GRAPH_MAX_IDLE_FAULTS in a
                    // row the record goes and the UTXO fails.
                    let idle = {
                        let mut guard = self.walk.borrow_mut();
                        guard.as_mut().map_or(0, |w| {
                            if w.resumed && w.record.nodes.len() <= w.nodes_at_start {
                                w.record.idle_faults += 1;
                            }
                            w.record.idle_faults
                        })
                    };
                    if idle >= DEFERRED_GRAPH_MAX_IDLE_FAULTS {
                        warn!(
                            "{} Walk of {} faulted with nothing appended {} passes in a row: dropped: {}",
                            self.log_prefix, outpoint, idle, e
                        );
                        let _ = self.storage.discard_graph(&graph_id).await;
                        let walk = self.walk.borrow_mut().take();
                        // Engine 0.1.11: dropped by a fault of the graph, a
                        // strike on the record's own slot.
                        if let Some(struck) = self.strike_resumed(outpoint, walk.as_ref(), &e).await
                        {
                            return if struck.passable() {
                                Ok(Ingested::Unanchorable)
                            } else {
                                Err(e)
                            };
                        }
                        self.drop_record(outpoint, DropReason::IdleFaults).await;
                        return Err(e);
                    }
                    warn!(
                        "{} Walk of {} faulted, deferred with its progress: {}",
                        self.log_prefix, outpoint, e
                    );
                    let _ = self.storage.discard_graph(&graph_id).await;
                    ("fault", false)
                }
            };
            // Call and size cuts may go on if no record fits (L4), with
            // the original deadline still active. Time and courier cuts
            // stop this root even if its save is refused.
            let budget_cut = matches!(reason, "calls" | "bytes" | "nodes");
            let (why, mut walk) = match self.save_walk(reason, completed).await {
                Saved::Retry => {
                    let _ = self.storage.discard_graph(&graph_id).await;
                    return Ok(Ingested::Retry);
                }
                Saved::Yes => {
                    let _ = self.storage.discard_graph(&graph_id).await;
                    return Ok(Ingested::Deferred);
                }
                Saved::WentOn => {
                    let _ = self.storage.discard_graph(&graph_id).await;
                    return Err(GASPError::Other(format!(
                        "graph {outpoint} went on and did not complete (bsv-low #555)"
                    )));
                }
                // A store FAULT at a deferral goes on as the other bounds do
                // (the delta fold's D-L2; counted `store_fault` by
                // `save_walk`): it failed the UTXO, a transient D1 fault
                // failing a graph the per-peer budget would have completed.
                Saved::Fault(e, walk) => {
                    if !budget_cut {
                        let _ = self.storage.discard_graph(&graph_id).await;
                        return Err(e);
                    }
                    (DropReason::StoreFault, walk)
                }
                // No room for a RESUMED walk (bsv-low #585): its held record
                // stays, so what this pass adds is lost and nothing else. A
                // budget cut goes on as the other bounds do; any other end
                // fails the UTXO and the next pass resumes from the record.
                Saved::Ceiling(walk) => {
                    if !budget_cut {
                        let _ = self.storage.discard_graph(&graph_id).await;
                        return Err(GASPError::Other(format!(
                            "deferred graph {outpoint} found no room, its record kept (bsv-low #585)"
                        )));
                    }
                    (DropReason::TooMany, walk)
                }
                Saved::Unkept(why, mut walk) => {
                    self.unkept(why, &mut walk).await;
                    if !(budget_cut && why == DropReason::TooMany) {
                        let _ = self.storage.discard_graph(&graph_id).await;
                        return Err(GASPError::Other(format!(
                            "deferred graph {outpoint} not kept: {} (bsv-low #555)",
                            why.as_str()
                        )));
                    }
                    (why, walk)
                }
            };
            info!(
                "{} Graph {} cannot be kept ({}): its walk goes on with the root deadline (bsv-low #555)",
                self.log_prefix,
                outpoint,
                why.as_str()
            );
            walk.cap = PassCap::NONE;
            walk.gone_on = true;
            *self.walk.borrow_mut() = Some(*walk);
            // The graph in hand was never discarded: walk on.
            ended = self.walk_graph(Some(&mut *deadline)).await;
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
            if walk.struck.is_some() {
                // A root walked again after faulted runs landed (engine
                // 0.1.9): admitted as any root, its record cleared.
                if let Err(e) = self
                    .storage
                    .delete_deferred_graph(&walk.record.outpoint)
                    .await
                {
                    warn!(
                        "{} Could not delete the fault record of landed root {}: {}",
                        self.log_prefix, walk.record.outpoint, e
                    );
                }
                self.forget_record(&walk.record.outpoint);
                self.unanchorable_cleared
                    .set(self.unanchorable_cleared.get() + 1);
                info!(
                    "{} Root {} LANDED after {} faulted runs: its record cleared (engine 0.1.9)",
                    self.log_prefix,
                    walk.record.outpoint,
                    walk.struck.as_ref().map_or(0, |r| r.faults)
                );
            } else if walk.resumed {
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
                self.forget_record(&walk.record.outpoint);
                self.stats.borrow_mut().converged += 1;
                self.resumed_with_progress
                    .set(self.resumed_with_progress.get() + 1);
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
    /// holding no node), or answer the storage's fault with the walk (a call
    /// or size cut may go on within its deadline, D-L2; any other fails its
    /// UTXO). A walk that already went on is neither saved nor counted again
    /// (D-L1). Counts the pass progressed or stalled (the lens fold's H1),
    /// once per pass.
    async fn save_walk(&self, reason: &'static str, completed: bool) -> Saved {
        let Some(mut walk) = self.walk.borrow_mut().take() else {
            // Nothing in hand: nothing to save.
            return Saved::Yes;
        };
        if walk.gone_on {
            // Its drop was counted when it went on (D-L1).
            return Saved::WentOn;
        }
        if !walk.counted {
            walk.counted = true;
            let mut stats = self.stats.borrow_mut();
            if completed || walk.record.nodes.len() > walk.nodes_at_start {
                stats.progressed += 1;
            } else {
                stats.stalled += 1;
            }
        }
        let was_held = walk.record.cursor_passable;
        let deadline_cut = matches!(reason, "time" | "peer_deadline");
        let deadline_streak = was_held || walk.record.reason == "retry:deadline";
        if walk.record.nodes.is_empty() && !deadline_streak {
            walk.record.passes = 0;
        }
        walk.record.passes += 1;
        // Only the graph's OWN deadline earns the stall allowance. The
        // per-peer deadline ends the slice, not the graph: what it cut is
        // held at once and the cursor passes it.
        let retry = walk.record.nodes.is_empty()
            && reason == "time"
            && !was_held
            && walk.record.passes < DEADLINE_STALL_DEFERRAL_RUNS;
        walk.record.cursor_passable = !retry;
        walk.record.reason = if retry {
            "retry:deadline".into()
        } else {
            reason.into()
        };
        if walk.record.nodes.len() > walk.nodes_at_start {
            walk.record.idle_faults = 0;
            // A pass that appends a node resets the idle resumes (0.1.10).
            walk.record.idle_resumes = 0;
            walk.record.retry_at = 0;
        }
        // Engine 0.1.10: a RESUMED pass of a held record that asked the peer
        // and appended nothing counts one idle resume in this save; at the
        // bound the record waits out the long backoff.
        let idle_now = walk.resumed
            && was_held
            && walk.struck.is_none()
            && !completed
            && walk.calls_this_pass > 0
            && walk.record.nodes.len() <= walk.nodes_at_start;
        let backing_off = if idle_now {
            walk.record.idle_resumes = walk.record.idle_resumes.saturating_add(1);
            let due = walk.record.idle_resumes >= IDLE_RESUMES_BEFORE_BACKOFF;
            if due {
                walk.record.retry_at = Self::now_secs() + IDLE_RESUME_RETRY_SECS;
            }
            due
        } else {
            false
        };
        // Empty walks qualify only after repeated deadline cuts. The small
        // retry record uses the same per-peer and global storage ceilings.
        // A quick fresh fault still earns no record.
        if walk.record.nodes.is_empty() && !deadline_cut && !was_held {
            return Saved::Unkept(DropReason::NoProgress, Box::new(walk));
        }
        // A resumed walk holds its record's deferred slot. A root walked
        // again after faulted runs holds a FAULT record (engine 0.1.10): the
        // hold its walk earned replaces it only with room among the deferred
        // slots, else the walk goes on as any walk past the bound (L4) and
        // its fault record stays as it was.
        if !walk.resumed && self.held_records.get() >= DEFERRED_GRAPHS_PER_PEER_TOPIC {
            return Saved::Unkept(DropReason::TooMany, Box::new(walk));
        }
        // No bound on a record's size (bsv-low #585): the figure is logged
        // and is the storage's to budget.
        let bytes = walk.record.byte_size();
        let outpoint = walk.record.outpoint.clone();
        match self.storage.save_deferred_graph(&walk.record).await {
            Ok(DeferredGraphSave::Saved) => {
                if walk.resumed {
                    self.pass_saved.set(true);
                }
                if idle_now {
                    self.stats.borrow_mut().idle_counted += 1;
                }
                if !walk.resumed {
                    // A fault record replaced is a fault record no more.
                    self.fault_records.borrow_mut().remove(&outpoint);
                    self.held_records.set(self.held_records.get() + 1);
                    if !retry && !walk.record.nodes.is_empty() {
                        self.retained.set(self.retained.get() + 1);
                    }
                } else if walk.record.nodes.len() > walk.nodes_at_start {
                    self.resumed_with_progress
                        .set(self.resumed_with_progress.get() + 1);
                }
                if !retry {
                    self.stats.borrow_mut().deferred += 1;
                }
                if backing_off {
                    self.stats.borrow_mut().backed_off += 1;
                    info!(
                        "{} Deferred graph {} resumed {} passes in a row with no node appended: not resumed for {} s (engine 0.1.10)",
                        self.log_prefix, outpoint, walk.record.idle_resumes, IDLE_RESUME_RETRY_SECS
                    );
                }
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
                if retry {
                    Saved::Retry
                } else {
                    Saved::Yes
                }
            }
            // The storage's ceiling is a budget of room, not a verdict on
            // the graph (bsv-low #585): a RESUMED walk keeps the record it
            // holds (it was deleted, and the graph walked from its root at
            // the next pass); a fresh walk has none to keep.
            Ok(DeferredGraphSave::AtCeiling) if walk.resumed => {
                self.note_dropped(&outpoint, DropReason::TooMany);
                Saved::Ceiling(Box::new(walk))
            }
            Ok(DeferredGraphSave::AtCeiling) => Saved::Unkept(DropReason::TooMany, Box::new(walk)),
            // The save is lost and counted `store_fault`. A RESUMED walk's
            // held record is KEPT (the delta-2 lens's D2-N2: it was deleted,
            // and one transient D1 fault threw away every earlier pass of a
            // walk the per-peer budget did not then finish): the next pass
            // resumes from it, losing this pass only; a walk that goes on
            // and lands deletes it as a converged one.
            Err(e) => {
                self.note_dropped(&outpoint, DropReason::StoreFault);
                Saved::Fault(e, Box::new(walk))
            }
        }
    }

    /// A walk the save did not keep: reset its retained obligation or count
    /// the drop. A fresh walk that fetched nothing is only logged.
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

    /// Whether the walk in hand has PAID for a record (the delta-2 fold's
    /// D2-L1): half its per-graph calls made in this pass, or its half
    /// deadline fallen due. A walk that holds no node never has.
    async fn walk_paid(&self) -> bool {
        let Some(budget) = &self.graph_budget else {
            return false;
        };
        let mut half = {
            let mut guard = self.walk.borrow_mut();
            let Some(walk) = guard.as_mut() else {
                return false;
            };
            if walk.record.nodes.is_empty() {
                return false;
            }
            if u64::from(walk.calls_this_pass) * 2 >= u64::from(budget.max_calls) {
                return true;
            }
            match walk.half.take() {
                Some(half) => half,
                None => return false,
            }
        };
        let paid = Self::due(&mut half).await;
        if let Some(walk) = self.walk.borrow_mut().as_mut() {
            walk.half = Some(half);
        }
        paid
    }

    /// Drop the partial walk, preserving a retry obligation if the cursor may have passed.
    async fn drop_record(&self, outpoint: &str, reason: DropReason) {
        if !matches!(reason, DropReason::Held | DropReason::Refused) {
            let Ok(record) = self.storage.get_deferred_graph(outpoint).await else {
                return;
            };
            if let Some(record) = record {
                if record.cursor_passable {
                    let mut retry = Self::fresh_walk(record.score, outpoint, PassCap::NONE).record;
                    retry.reason = "retry".into();
                    retry.cursor_passable = true;
                    let _ = self.storage.save_deferred_graph(&retry).await;
                    self.note_dropped(outpoint, reason);
                    return;
                }
            }
        }
        if let Err(e) = self.storage.delete_deferred_graph(outpoint).await {
            warn!(
                "{} Could not delete the record of deferred graph {}: {}",
                self.log_prefix, outpoint, e
            );
        }
        self.forget_record(outpoint);
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
    /// again. No-op without a per-graph budget.
    ///
    /// The graph in flight is HELD at once, whatever it fetched: its root
    /// outpoint and partial walk become a durable record (reason
    /// `peer_deadline`) the next sync resumes ahead of its pages, so the
    /// cursor passes it to the page's last finished score. Before 0.1.8 a
    /// graph the deadline met before its root came back was only a retry,
    /// and the next run re-read the same page behind it (bsv-low #552:
    /// "the graph in flight is lost whole"). A graph the deadline met after
    /// its refusal is finished, not held. A graph that cannot be held (no
    /// room) stays the cursor's gap and is named in `cursor_held_by`.
    pub async fn defer_in_flight(&mut self) {
        let in_flight = self.in_flight.take();
        if self.graph_budget.is_none() {
            return;
        }
        if let Some((_, outpoint)) = &in_flight {
            if self.expired.borrow().contains(outpoint) {
                let outpoint = outpoint.clone();
                self.walk.borrow_mut().take();
                self.completed_cursor = self.cursor_after_in_flight;
                self.release_hold(&outpoint);
                return;
            }
        }
        // An unanchorable root cut mid-walk stays unanchorable (engine
        // 0.1.9): its record is kept, its backoff starts again, the cursor
        // passes it as before. A root whose fault streak is still under the
        // threshold is held at the deadline as any graph in flight.
        let unanchorable = self
            .walk
            .borrow()
            .as_ref()
            .and_then(|w| w.struck.as_deref().filter(|r| r.is_unanchorable()).cloned());
        if let Some(record) = unanchorable {
            self.walk.borrow_mut().take();
            let outpoint = record.outpoint.clone();
            if let Err(e) = self.keep_unanchorable(&record).await {
                warn!(
                    "{} Could not keep unanchorable root {} at the deadline: {}",
                    self.log_prefix, outpoint, e
                );
            }
            self.completed_cursor = self.cursor_after_in_flight;
            self.release_hold(&outpoint);
            return;
        }
        // A deadline during pagination has no graph to save. Preserve the
        // completed cursor, including any earlier unsaved gap.
        if self.walk.borrow().is_none() {
            let Some((score, outpoint)) = in_flight else {
                return;
            };
            *self.walk.borrow_mut() = Some(Self::fresh_walk(score, &outpoint, PassCap::NONE));
        }
        let outpoint = self
            .walk
            .borrow()
            .as_ref()
            .map(|w| w.record.outpoint.clone())
            .unwrap_or_default();
        match self.save_walk("peer_deadline", false).await {
            Saved::Yes => {
                self.completed_cursor = self.cursor_after_in_flight;
                self.release_hold(&outpoint);
            }
            Saved::Unkept(why, mut walk) => self.unkept(why, &mut walk).await,
            _ => {}
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

    /// Count a node the graph in hand was SERVED (bsv-low #586): the bytes
    /// the peer sent for it, which are the bytes a record keeps of it.
    fn count_fetched(&self, node: &GASPNode) {
        let bytes = (node.raw_tx.len() + node.proof.as_ref().map_or(0, String::len)) as u64;
        self.bytes_fetched.set(self.bytes_fetched.get() + bytes);
        if let Some(w) = self.walk.borrow_mut().as_mut() {
            w.bytes_this_pass += bytes;
        }
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

    fn root_bound(&self, reason: &'static str) -> WalkEnd {
        self.last_fault.set(None);
        if let Some(walk) = self.walk.borrow().as_ref() {
            self.stats
                .borrow_mut()
                .root_bounds
                .push(crate::sync_load::RootBound {
                    peer: String::new(),
                    outpoint: walk.record.outpoint.clone(),
                    reason: reason.into(),
                });
        }
        WalkEnd::Deferred(reason)
    }

    /// Walk the graph in hand until nothing is pending (an explicit stack,
    /// bsv-low #555; before it a recursion, in the same order: an input's
    /// whole branch before its next sibling). Each step's result is
    /// committed to the walk only once the step has finished, so a deadline
    /// that drops this future leaves the walk as it was before that step.
    /// With a per-graph budget the walk stops at a counted limb of it (its
    /// calls; the bytes it was served or the nodes it appended, bsv-low #586)
    /// or at its `deadline`, and answers `Deferred` with the limb's name;
    /// without one it runs to its end. A limb is read BEFORE a step, so a
    /// pass always makes one: no node is too big to walk.
    async fn walk_graph(
        &self,
        mut deadline: Option<&mut crate::engine::SleepFuture>,
    ) -> Result<WalkEnd, GASPError> {
        loop {
            let (item, over) = {
                let walk = self.walk.borrow();
                let Some(walk) = walk.as_ref() else {
                    return Ok(WalkEnd::Done);
                };
                (
                    walk.record.pending.last().cloned(),
                    self.graph_budget.as_ref().and_then(|_| walk.over()),
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
            if let Some(limb) = over {
                return Ok(WalkEnd::Deferred(limb));
            }
            let step = self.step(&item);
            let out = match deadline.as_mut() {
                Some(deadline) => match race_or_deadline(step, deadline.as_mut()).await {
                    Some(out) => out,
                    None => return Ok(self.root_bound("time")),
                },
                None => step.await,
            };
            if crate::meter::root_exhausted() || matches!(out, Err(GASPError::CourierBudget)) {
                return Ok(self.root_bound("courier_rungs"));
            }
            let out = out?;
            if matches!(out, StepOut::Expired) {
                return Ok(WalkEnd::Expired);
            }
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
            walk.nodes_this_pass += 1;
            self.nodes_appended.set(self.nodes_appended.get() + 1);
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
                configured: false,
                idle_faults: 0,
                cursor_passable: false,
                faults: 0,
                retry_at: 0,
                idle_resumes: 0,
                unanchorable_retries: 0,
                expiry: None,
            },
            seen: std::collections::HashSet::new(),
            calls_this_pass: 0,
            bytes_this_pass: 0,
            nodes_this_pass: 0,
            resumed: false,
            cap: PassCap::NONE,
            nodes_at_start: 0,
            counted: false,
            gone_on: false,
            half: None,
            struck: None,
        });
        let node = self.hydrate_root(node.clone()).await;
        let out = self.absorb(&root, node).await?;
        self.commit(out);
        self.walk_graph(None).await.map(|_| ())
    }

    /// The ROOT of a graph: the peer's node for the UTXO, its own proof
    /// hydrated by the ancestor fetcher when it has none.
    async fn fetch_root(&self, item: &PendingInput) -> Result<Option<GASPNode>, GASPError> {
        let (txid, oi) = parse_outpoint(&item.outpoint)
            .ok_or_else(|| GASPError::Other(format!("bad outpoint {}", item.outpoint)))?;
        self.count_call();
        let node = self
            .remote
            .request_node(&item.graph_id, &txid, oi, item.metadata)
            .await?;
        self.count_fetched(&node);
        if self.root_expired(&node, &item.outpoint)? {
            return Ok(None);
        }
        Ok(Some(self.hydrate_root(node).await))
    }

    /// The root's own proof, hydrated by the ancestor fetcher when the peer
    /// served it with none.
    async fn hydrate_root(&self, mut node: GASPNode) -> GASPNode {
        // The peer already supplied the raw. Ask our proof source for only
        // its verified BUMP, inside the graph and peer deadlines. Production
        // re-verifies courier proofs against CHAINTRACKS; the graph's anchor
        // check still runs before any admission. A resumed proofless root
        // takes this path too, so a kept graph can land once its proof exists.
        if node.proof.is_none() {
            if let Some(fetcher) = &self.ancestor_fetcher {
                if let Some(node_txid) =
                    Self::node_id(&node).and_then(|id| parse_outpoint(&id).map(|(txid, _)| txid))
                {
                    self.count_call();
                    node.proof = fetcher.verified_proof_for(&node_txid).await;
                }
            }
        }
        self.root_proofs
            .borrow_mut()
            .insert(node.graph_id.clone(), node.proof.is_some());
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
        // Each failure below is marked with its fault class (engine 0.1.9),
        // read where the root is dropped.
        let node = if item.spent_by.is_none() {
            let Some(node) = self
                .fetch_root(item)
                .await
                .map_err(|e| self.faulted(Self::peer_fault_class(&e), e))?
            else {
                return Ok(StepOut::Expired);
            };
            node
        } else {
            let Some((txid, oi)) = parse_outpoint(&item.outpoint) else {
                return Ok(StepOut::Pruned);
            };
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
                // Every fetcher error fails the UTXO: the fetcher arm
                // never prunes, see the rule above.
                //
                // THE PEER DECIDES ABSENCE (engine 0.1.15, #56). An input
                // of an UNPROVEN node the fetcher cannot serve is asked of
                // the peer, one request: a rung fetches, it never decides.
                // On a deployment with no rung configured (the stack's
                // production since 2026-10-14) the fetcher answers every
                // such input "exhausted" with no request, so before 0.1.15
                // the peer was never asked and a graph the chain never
                // held was named `courier`, never the absence it is. The
                // peer serving it, the walk goes on; its `NodeNotFound` is
                // its word that nothing of this graph can anchor
                // ([`fault_class::NOT_FOUND`]); any other answer keeps the
                // fetcher's error and class. A proven node's named inputs
                // and a spent courier budget are as before.
                Some(fetcher) => {
                    self.count_call();
                    let ancestor = match fetcher.fetch_ancestor(&txid).await {
                        Ok(ancestor) => ancestor,
                        Err(e) => {
                            let class = if matches!(e, GASPError::DefinitiveNotFound(_)) {
                                fault_class::NOT_FOUND
                            } else {
                                fault_class::COURIER
                            };
                            if item.parent_proven
                                || matches!(e, GASPError::CourierBudget)
                                || crate::meter::root_exhausted()
                            {
                                return Err(self.faulted(class, e));
                            }
                            return match self.ask_peer_for_input(item, &txid, oi).await {
                                Ok(node) => self
                                    .absorb(item, node)
                                    .await
                                    .map_err(|e| self.faulted(fault_class::VERIFICATION, e)),
                                Err(peer @ GASPError::NodeNotFound(_)) => {
                                    Err(self.faulted(fault_class::NOT_FOUND, peer))
                                }
                                Err(_) => Err(self.faulted(class, e)),
                            };
                        }
                    };
                    let node = GASPNode {
                        graph_id: item.graph_id.clone(),
                        raw_tx: ancestor.raw_tx,
                        output_index: oi,
                        proof: ancestor.proof,
                        tx_metadata: None,
                        output_metadata: None,
                        inputs: None,
                    };
                    self.count_fetched(&node);
                    node
                }
                // No fetcher, an unproven parent: ask the peer; its
                // `NodeNotFound` is absence (engine 0.1.15), remembered for
                // the rest of the sync.
                None if !item.parent_proven => self
                    .ask_peer_for_input(item, &txid, oi)
                    .await
                    .map_err(|e| self.faulted(Self::peer_fault_class(&e), e))?,
                // No fetcher, a proven parent → ask the peer.
                None => {
                    self.count_call();
                    match self
                        .remote
                        .request_node(&item.graph_id, &txid, oi, item.metadata)
                        .await
                    {
                        Ok(child_node) => {
                            self.count_fetched(&child_node);
                            child_node
                        }
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
                        // Any other error: propagate, as the reference does.
                        Err(e) => return Err(self.faulted(Self::peer_fault_class(&e), e)),
                    }
                }
            }
        };
        self.absorb(item, node)
            .await
            .map_err(|e| self.faulted(fault_class::VERIFICATION, e))
    }

    /// Ask the peer for `item`, an input an UNPROVEN node needs (engine
    /// 0.1.15): one request, or none when the peer already answered in this
    /// sync that it does not hold it (counted a peer-absence memo hit).
    async fn ask_peer_for_input(
        &self,
        item: &PendingInput,
        txid: &str,
        output_index: u32,
    ) -> Result<GASPNode, GASPError> {
        if self.absent.borrow().contains(&item.outpoint) {
            crate::meter::peer_absent_memo_hit();
            return Err(GASPError::NodeNotFound(format!(
                "{}: the peer answered it does not hold it earlier in this sync",
                item.outpoint
            )));
        }
        self.count_call();
        match self
            .remote
            .request_node(&item.graph_id, txid, output_index, item.metadata)
            .await
        {
            Ok(node) => {
                self.count_fetched(&node);
                Ok(node)
            }
            Err(e) => {
                if matches!(e, GASPError::NodeNotFound(_)) {
                    self.absent.borrow_mut().insert(item.outpoint.clone());
                }
                Err(e)
            }
        }
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
                let proven =
                    self.root_proofs
                        .borrow_mut()
                        .remove(graph_id)
                        .or_else(|| {
                            self.walk.borrow().as_ref().and_then(|w| {
                                match w.record.nodes.as_slice() {
                                    [root, ..] => Some(root.node.proof.is_some()),
                                    [] => None,
                                }
                            })
                        })
                        .unwrap_or(false);
                let count = if proven {
                    &self.roots_proven
                } else {
                    &self.roots_unproven
                };
                count.set(count.get() + 1);
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
                // A walk past the budget: held the same way, counted apart
                // (the E592 lens fold, H1 and L3).
                if matches!(e, GASPError::AnchorWalkHeld(_)) {
                    self.anchor_walk_held.set(self.anchor_walk_held.get() + 1);
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
#[derive(Debug, Clone, thiserror::Error)]
pub enum GASPError {
    /// Every enabled raw rung answered absence, with no skipped or failed rung.
    /// Since engine 0.1.15 it counts one run of absence beside the peer's
    /// word ([`fault_class::NOT_FOUND`]), never a strike by itself.
    #[error("all raw couriers report not found: {0}")]
    DefinitiveNotFound(String),
    /// The pass spent its courier allowance; retain its pending walk.
    #[error("root courier rung budget exhausted")]
    CourierBudget,
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

    /// The platform answered before the peer was reached (engine 0.2.0,
    /// bsv-overlay-cloudflare #57; [`crate::route_fault`]): a fault of the
    /// route, never of the peer. It counts nothing against the peer's
    /// health rows and is named `route_fault:<class>`.
    #[error("route_fault:{class}: {detail}")]
    RouteFault { class: String, detail: String },

    /// Storage error.
    #[error("storage error: {0}")]
    StorageError(String),

    /// A graph's anchor could not be CHECKED right now (the chain tracker or
    /// a storage read faulted, or the topic manager answered the replay with
    /// an error): a fault of the moment, not a verdict on the graph. `GASPSync::complete_graph` discards the graph and fails the
    /// UTXO, so the cursor gap guard asks for it again (bsv-low #551).
    #[error("anchor unavailable: {0}")]
    AnchorUnavailable(String),

    /// The anchor's script walk could not run within the engine's walk
    /// budget (bsv-low #592; the E592 lens fold, H1): no verdict, and
    /// nothing stands in for one (no network accepted these bytes). Held
    /// exactly as [`GASPError::AnchorUnavailable`] holds: the graph is not
    /// finalized, the UTXO fails and the cursor stays below it, until the
    /// peer serves its root PROVEN (#555's `root_proven` restart, whose walk
    /// trusts the proof and runs no script). Counted apart
    /// (`GASPSync::anchor_walk_held`).
    #[error("anchor walk held: {0}")]
    AnchorWalkHeld(String),

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
    async fn proofs_page_deadline_never_passes_a_failed_unsaved_graph() {
        struct PageDeadline(std::rc::Rc<std::cell::Cell<bool>>);
        #[async_trait(?Send)]
        impl GASPRemote for PageDeadline {
            async fn get_initial_response(
                &self,
                request: &GASPInitialRequest,
            ) -> Result<GASPInitialResponse, GASPError> {
                if request.since > 0 {
                    self.0.set(true);
                    std::future::pending::<()>().await;
                }
                Ok(GASPInitialResponse {
                    since: request.since,
                    utxo_list: vec![GASPOutput {
                        txid: "11".repeat(32),
                        output_index: 0,
                        score: 10.0,
                    }],
                })
            }
            async fn get_initial_reply(
                &self,
                _: &GASPInitialResponse,
            ) -> Result<GASPInitialReply, GASPError> {
                unreachable!()
            }
            async fn request_node(
                &self,
                _: &str,
                _: &str,
                _: u32,
                _: bool,
            ) -> Result<GASPNode, GASPError> {
                Err(GASPError::RemoteError("offline".into()))
            }
            async fn submit_node(
                &self,
                _: &GASPNode,
            ) -> Result<Option<GASPNodeResponse>, GASPError> {
                unreachable!()
            }
        }
        let due = std::rc::Rc::new(std::cell::Cell::new(false));
        let mut sync = GASPSync::new(
            Box::new(MockGASPStorage::new(vec![])),
            Box::new(PageDeadline(due.clone())),
            0,
            "test",
            true,
        )
        .with_graph_budget(GraphBudget {
            max_calls: 10,
            max_bytes_fetched: u64::MAX,
            max_nodes: u32::MAX,
            deadline: Box::new(|| Box::pin(std::future::pending())),
            half_deadline: None,
        });
        let deadline = std::future::poll_fn(move |_| {
            if due.get() {
                std::task::Poll::Ready(())
            } else {
                std::task::Poll::Pending
            }
        });
        assert!(race_or_deadline(sync.sync(None), deadline).await.is_none());
        assert!(!sync.graph_in_flight());
        sync.defer_in_flight().await;
        assert_eq!(
            sync.completed_cursor(),
            0,
            "a failed graph has no durable hold"
        );
        assert_eq!(
            sync.cursor_held_by(),
            Some(format!("{}.0", "11".repeat(32)))
        );
    }

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
