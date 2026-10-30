//! bsv-low #436 (lens fold, 2026-10-06): THE ONE FLUSH the three change webhooks ship through
//! (`pot_changes`, `hop_changes`, `lobby_changes`; `POST /internal/{pot,hop,lobby}-changed` on the app layer).
//!
//! Each module keeps its own per-isolate set and its own body shape; the rule for getting a drained set to the
//! app layer is one rule, written once and driven by the pins through a fake transport ([`ship_chunks`] takes
//! the POST as a closure, so the pin executes the very loop production runs):
//!
//! 1. **Chunked at the app layer's bound** ([`CHANGE_CHUNK`], pinned equal to its three caps from its tests,
//!    which link this crate): nothing a flush ships is over the receiver's cap.
//! 2. **Bounded per flush** ([`CHANGE_MAX_CHUNKS`] POSTs, and a wall budget, [`FLUSH_WALL_BUDGET_MS`]): the
//!    flush runs under `wait_until`, whose wall is finite, and each POST is a subrequest. What does not fit is
//!    DEFERRED (noted back for the next flush), never dropped.
//! 3. **A failed POST is retried once** ([`settle_failed`]): its entries are noted back for the next flush; an
//!    entry whose second POST fails too is counted undelivered and let go (the app layer re-reads the truth on
//!    its cadence).
//! 4. **Nothing is lost silently**: refused by the receiver (`dropped` in its answer), failed twice, retried,
//!    deferred: each is logged and counted.
//! 5. **Every note-back is accounted for twice** (the delta lens's D-L2, [`settle`]): once when it is made
//!    (`*_retried_total`, `*_deferred_total`) and once when a later flush POSTs it again (`*_resent_total`,
//!    whatever that POST answers). The difference is the note-backs NEVER RE-SENT, served by name on
//!    `/health/invariants` (`*_noted_back_unresent`); a retry that failed has its own name too
//!    (`*_retry_failed_total`, the part of `*_undelivered_total` that had its second POST).
//!
//! THE COST OF THE BOUND (lens L4, measured by `the_worst_case_flush_is_bounded_in_posts_bytes_and_wall`): the
//! worst case flush of one set is 16 sequential POSTs of 8 entries (128 entries; a pot body of 8 is at most 767
//! bytes, 12.3 kB over the flush). The request's detached task ships the hop set and then the pot set under ONE
//! deadline, so at most 32 POSTs there, and the lobby set's own task at most 16: 48 subrequests in the worst
//! request. Counters and parks commit in one D1 batch per flush. The time one POST takes is the app layer's handling of a
//! body of eight (one broadcast push per outpoint, the attribution reads, the per-seat filings), which no native
//! pin can measure: it is beta's figure. The wall budget makes the bound hold whatever that figure is: at a
//! modelled 2 s per POST a flush of 128 gets 10 POSTs out in its 20 s and DEFERS the other 48 entries, counted,
//! where it used to lose them with the task, uncounted (the pin runs 131 entries: 80 delivered, 51 deferred).
//! Each POST is raced against that same deadline. An unacknowledged chunk is deferred, counted and parked;
//! a background flush reserves three seconds for its one accounting and parking batch, with no later D1 await.
//!
//! THE NOTE-BACK IS NOT DURABLE (lens L3; the delta lens's D-L2 states what that costs the counters). A
//! deferred or retried entry is noted back into the isolate's set from inside the detached task, after the
//! request's own flush, so it waits for the NEXT flush on THIS isolate (any dispatched fetch since bsv-low
//! #523, a cron tick, a queue batch). That wait has NO time bound: Cloudflare routes the next request to any
//! isolate, and an isolate evicted first takes its notes with it. The population is every entry of a POST the
//! app layer did not accept (any 5xx, since the retry of L2) and every entry past a flush's bound.
//!
//! WHAT THE COUNTERS PROVE, stated truthfully. `*_undelivered_total = 0` does NOT prove delivery: it counts
//! only what an isolate lived to see refused or fail twice. Delivery is proven by `*_undelivered_total = 0`
//! AND `*_noted_back_unresent = 0` (`retried + deferred - resent`, floored at 0): every note-back made was
//! POSTed again, and none of those POSTs was refused. `*_noted_back_unresent` is above 0 for the moments
//! between a note-back and the next flush of its isolate; one that STAYS above 0 is that many entries lost
//! with an isolate (or with a POST still in flight when the task's wall ended), never re-sent. THE FIGURE IS
//! CUMULATIVE, NOT A GAUGE (the second delta lens's D2-L2): it is the difference of three totals summed over
//! every isolate since the counters began, so entries lost once stay in it for good and it does NOT return to
//! 0 after the first loss. Read it against a RECORDED BASELINE: it rises at a note-back and falls back to the
//! baseline at the next flush of that isolate; a rise that stays is that many more entries lost, and the new
//! value is the next baseline. No current figure is served beside it on purpose: what is waiting lives in each
//! isolate's own memory, and `/health/invariants` answers from whichever isolate takes the request. It errs toward
//! reading a loss: a note-back whose own counter write failed is not counted at all (the floor hides a resend
//! that was), and the per-isolate memory of note-backs is bounded ([`RETRY_MEMORY_MAX`],
//! [`DEFER_MEMORY_MAX`]): past it a failed or deferred entry is not noted back, it is let go and counted
//! undelivered at once. The cost of every one of these is the receiver's own cadence (the owed list's
//! read-aged recompute, the Lobby's next event), never money.
//!
//! THE OUTBOX (bsv-low #560, B-COST-1). Every NEW note-back ([`settle_noting`]'s `fresh`: a failed POST's entries
//! and the entries past the bound) is also PARKED in D1 (`change_parked`, under the set's kind, by
//! `FlushBatch`, in the table #649's skip parks into), counted `*_outboxed_total`; the cron's drain
//! (`change_park::deliver_parked`, the `*/15` tick and `/admin/complete-proofs`) delivers it, backs it off on a
//! refusal and counts it exhausted after its last attempt (`change_park_exhausted_total`). The in-memory
//! note-back stays the FAST path and the figures above still describe it; the outbox is the DURABLE one, so an
//! entry the memory lets go (an isolate's end, a retry that failed too, and since the lens's LOW-3 an entry the FULL
//! memory could not hold, [`RETRY_MEMORY_MAX`] / [`DEFER_MEMORY_MAX`], parked first under the same bound and still
//! counted undelivered by the memory's account) is still a row. An entry REFUSED by the receiver (`dropped` in its
//! answer) is not a note-back and is not parked. One that did not reach the
//! outbox is counted where it missed: `change_park_full_total` (the table's ceiling), `change_park_faults_total`
//! (the batch faulted), `change_outbox_over_bound_total` (past a flush's batch bound). The flush's account and
//! all hop, pot and word parks commit atomically in ONE `counted_batch` (`FlushBatch`, OVERLAY-D2 LOW-2).
//! The reserve therefore covers one round trip rather than a variable sequence of counter writes. A failed
//! batch leaves notes in memory, logs the fault and queues its counter deltas, including park faults, for the
//! next flush; no further D1 await spends the reserve. A runtime cancellation before commit can still leave
//! notes in memory only, with no partial account, and an isolate lost then loses those notes. The #649 refusal
//! facts are already durable before this flush. Delivery is proven by the outbox's own figures:
//! `/health/invariants.changeParked` (rows waiting, by set) and the counters `change_unparked_total` (delivered),
//! `change_park_exhausted_total` (abandoned).
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;

use worker::*;

/// The app layer's bound on one webhook body (`internal_events::POT_CHANGED_MAX`, `HOP_CHANGED_MAX`,
/// `LOBBY_CHANGED_MAX`: all eight, pinned equal from its tests).
pub const CHANGE_CHUNK: usize = 8;

/// The most POSTs one flush of one set makes (128 entries).
pub const CHANGE_MAX_CHUNKS: usize = 16;

/// One flush's delivery wall, including in-flight POSTs. Background callers also reserve time for
/// the note-back, durable parking and counters inside the platform window.
pub const FLUSH_WALL_BUDGET_MS: u64 = 20_000;

/// The most entries one isolate remembers as "retried once" per set. Past it a failed entry is not retried
/// (counted undelivered): the memory is bounded whatever the app layer does.
pub const RETRY_MEMORY_MAX: usize = 1024;

/// The most entries one isolate remembers as "deferred, not yet re-sent" per set. Past it an entry over a
/// flush's bound is not noted back (counted undelivered): the memory is bounded whatever the flood is.
pub const DEFER_MEMORY_MAX: usize = 4096;

/// What one flush did with its entries. Every entry is in exactly one of `delivered`, `failed`, `deferred`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Shipped<T> {
    /// POSTs started.
    pub posts: usize,
    /// Entries of the POSTs the app layer accepted (a 2xx), in order.
    pub delivered: Vec<T>,
    /// Entries the app layer answered it REFUSED (`dropped` in its 2xx answers, summed): 0 from our own chunking.
    pub refused: usize,
    /// Entries of the POSTs it did not accept.
    pub failed: Vec<T>,
    /// Entries past the chunk or wall bound, including a POST whose acknowledgement timed out.
    pub deferred: Vec<T>,
}

/// THE FLUSH LOOP, transport injected. `post` gets one chunk's body and answers `Ok(dropped)` on a 2xx (what
/// the receiver answered it refused) or `Err(())`; `now_ms` is the clock the wall is read on; no POST is
/// started at or past `deadline_ms`. An unacknowledged POST is deferred at that wall.
/// Sequential: one POST in flight at a time.
pub async fn ship_chunks<T, B, P, Fut, C>(
    items: Vec<T>,
    body: B,
    post: P,
    now_ms: C,
    deadline_ms: u64,
) -> Shipped<T>
where
    T: Clone,
    B: Fn(&[T]) -> String,
    P: FnMut(String) -> Fut,
    Fut: Future<Output = std::result::Result<usize, ()>>,
    C: Fn() -> u64,
{
    ship_chunks_with(
        items,
        body,
        post,
        now_ms,
        deadline_ms,
        crate::broadcaster::sleep_ms,
    )
    .await
}

pub(crate) async fn ship_chunks_with<T, B, P, Fut, C, S, Sleep>(
    items: Vec<T>,
    body: B,
    mut post: P,
    now_ms: C,
    deadline_ms: u64,
    sleep: S,
) -> Shipped<T>
where
    T: Clone,
    B: Fn(&[T]) -> String,
    P: FnMut(String) -> Fut,
    Fut: Future<Output = std::result::Result<usize, ()>>,
    C: Fn() -> u64,
    S: Fn(u64) -> Sleep,
    Sleep: Future<Output = ()>,
{
    let mut out = Shipped {
        posts: 0,
        delivered: Vec::new(),
        refused: 0,
        failed: Vec::new(),
        deferred: Vec::new(),
    };
    for (i, chunk) in items.chunks(CHANGE_CHUNK).enumerate() {
        if i >= CHANGE_MAX_CHUNKS || now_ms() >= deadline_ms {
            out.deferred.extend_from_slice(chunk);
            continue;
        }
        out.posts += 1;
        match crate::race_or_deadline(
            post(body(chunk)),
            sleep(deadline_ms.saturating_sub(now_ms())),
        )
        .await
        {
            Some(Ok(dropped)) => {
                out.refused += dropped;
                out.delivered.extend_from_slice(chunk);
            }
            Some(Err(())) => out.failed.extend_from_slice(chunk),
            None => out.deferred.extend_from_slice(chunk),
        }
    }
    out
}

/// PURE (lens L2): a failed POST's entries are retried ONCE. `retried` is the isolate's memory of the entries
/// already noted back once: a delivered entry leaves it; a failed entry not in it enters it and is returned to
/// be noted back; a failed entry already in it leaves it and is counted lost (so is one the full memory cannot
/// hold). Returns `(note back, lost)`.
pub fn settle_failed<T: Ord + Clone>(
    retried: &mut BTreeSet<T>,
    delivered: &[T],
    failed: Vec<T>,
) -> (Vec<T>, usize) {
    let (again, lost, let_go) = settle_failed_splitting(retried, delivered, failed);
    (again, lost + let_go.len())
}

/// [`settle_failed`], answering apart the FIRST failures the full memory could not hold (`let_go`: never noted back,
/// so never parked by an earlier flush; the lens's LOW-3 parks them). Returns `(note back, lost on the retry, let go)`.
fn settle_failed_splitting<T: Ord + Clone>(
    retried: &mut BTreeSet<T>,
    delivered: &[T],
    failed: Vec<T>,
) -> (Vec<T>, usize, Vec<T>) {
    for d in delivered {
        retried.remove(d);
    }
    let mut again = Vec::new();
    let mut lost = 0usize;
    let mut let_go = Vec::new();
    for f in failed {
        if retried.remove(&f) {
            lost += 1;
        } else if retried.len() >= RETRY_MEMORY_MAX {
            let_go.push(f);
        } else {
            retried.insert(f.clone());
            again.push(f);
        }
    }
    (again, lost, let_go)
}

/// One isolate's memory of the entries of one set it noted back and has not POSTed again since: the entries
/// awaiting their one retry, and the entries deferred past a flush's bound. Bounded ([`RETRY_MEMORY_MAX`],
/// [`DEFER_MEMORY_MAX`]). An entry is in at most one of the two.
#[derive(Debug)]
pub struct NotedBack<T> {
    retried: BTreeSet<T>,
    deferred: BTreeSet<T>,
}

impl<T> NotedBack<T> {
    /// An empty memory.
    pub const fn new() -> Self {
        Self {
            retried: BTreeSet::new(),
            deferred: BTreeSet::new(),
        }
    }

    /// Forget everything (the pins start from an empty memory).
    pub fn clear(&mut self) {
        self.retried.clear();
        self.deferred.clear();
    }
}

impl<T> Default for NotedBack<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// One flush's account, in entries. `retried + deferred` over every flush is the note-backs MADE; `resent` over
/// every flush is the note-backs POSTed again; the difference is what was never re-sent.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Tally {
    /// Let go: refused by the app layer, failed on the retry too, or past the note-back memory.
    pub undelivered: usize,
    /// The part of `undelivered` whose SECOND POST failed (it was retried and still failed).
    pub retry_failed: usize,
    /// Failed for the first time: noted back to be retried once.
    pub retried: usize,
    /// Past the flush's bound for the first time: noted back for the next flush.
    pub deferred: usize,
    /// Noted back by an earlier flush (retried or deferred) and POSTed by this one, whatever it answered.
    pub resent: usize,
}

/// PURE (the delta lens's D-L2): WHAT A FLUSH OWES AFTER ITS POSTS. Returns the entries to note back for the
/// next flush and the flush's account. A failed POST's entries are retried once ([`settle_failed`]); the entries
/// past the bound are noted back; and every entry this flush POSTed that an earlier flush had noted back is
/// counted `resent`, so the note-backs made (`retried`, `deferred`: counted ONCE each, an entry deferred again
/// while it waits is not a new note-back) and the note-backs re-sent meet when nothing was lost with an isolate.
pub fn settle<T: Ord + Clone>(memory: &mut NotedBack<T>, shipped: &Shipped<T>) -> (Vec<T>, Tally) {
    let (back, _, tally) = settle_splitting(memory, shipped);
    (back, tally)
}

/// [`settle`], answering also the entries the FULL memory let go (a first failure past [`RETRY_MEMORY_MAX`], a
/// deferral past [`DEFER_MEMORY_MAX`]): counted `undelivered` as before, and handed to the outbox by
/// [`settle_noting`] (the lens's LOW-3). Returns `(note back, let go, account)`.
fn settle_splitting<T: Ord + Clone>(
    memory: &mut NotedBack<T>,
    shipped: &Shipped<T>,
) -> (Vec<T>, Vec<T>, Tally) {
    let mut resent = 0usize;
    for posted in shipped.delivered.iter().chain(&shipped.failed) {
        if memory.deferred.remove(posted) || memory.retried.contains(posted) {
            resent += 1;
        }
    }
    let retry_failed = shipped
        .failed
        .iter()
        .filter(|f| memory.retried.contains(*f))
        .count();
    let (mut back, lost, mut let_go) = settle_failed_splitting(
        &mut memory.retried,
        &shipped.delivered,
        shipped.failed.clone(),
    );
    let retried = back.len();
    let mut deferred = 0usize;
    for d in &shipped.deferred {
        if memory.retried.contains(d) || memory.deferred.contains(d) {
            back.push(d.clone()); // still waiting: the same note-back, not a new one
        } else if memory.deferred.len() < DEFER_MEMORY_MAX {
            memory.deferred.insert(d.clone());
            deferred += 1;
            back.push(d.clone());
        } else {
            let_go.push(d.clone());
        }
    }
    let undelivered = shipped.refused + lost + let_go.len();
    (
        back,
        let_go,
        Tally {
            undelivered,
            retry_failed,
            retried,
            deferred,
            resent,
        },
    )
}

/// PURE (bsv-low #560, B-COST-1): [`settle`], answering also the flush's NEW note-backs (`fresh`: entries noted back
/// by this flush that no earlier flush of this isolate had noted back), the entries the flush parks in its D1 outbox
/// ([`FlushBatch`]). An entry still waiting from an earlier flush was parked by that flush. The
/// lens's LOW-3: an entry the FULL memory let go (a first failure past [`RETRY_MEMORY_MAX`], a deferral past
/// [`DEFER_MEMORY_MAX`]) is fresh too, and FIRST, so the outbox's bound ([`crate::change_park::OUTBOX_PARK_MAX`])
/// takes those that have no memory to fall back on before those that do. Returns `(note back, fresh, account)`.
pub fn settle_noting<T: Ord + Clone>(
    memory: &mut NotedBack<T>,
    shipped: &Shipped<T>,
) -> (Vec<T>, Vec<T>, Tally) {
    let held: BTreeSet<T> = shipped
        .failed
        .iter()
        .chain(&shipped.deferred)
        .filter(|e| memory.retried.contains(*e) || memory.deferred.contains(*e))
        .cloned()
        .collect();
    let (back, let_go, tally) = settle_splitting(memory, shipped);
    let fresh = let_go
        .into_iter()
        .chain(back.iter().filter(|e| !held.contains(*e)).cloned())
        .collect();
    (back, fresh, tally)
}

/// PURE: the note-backs never re-sent, from the three counters' totals (`/health/invariants` serves it per set
/// as `*_noted_back_unresent`). Floored at 0 for historical partial accounts; a flush now batches its counters. Read
/// against a recorded baseline, never against 0 (the module doc, D2-L2).
pub fn noted_back_unresent(retried_total: u64, deferred_total: u64, resent_total: u64) -> u64 {
    (retried_total + deferred_total).saturating_sub(resent_total)
}

/// `dropped` in the app layer's 2xx answer (additive since bsv-low #436; an older app layer answers none: 0).
pub fn answered_dropped(answer: &str) -> usize {
    serde_json::from_str::<serde_json::Value>(answer)
        .ok()
        .and_then(|v| v.get("dropped").and_then(serde_json::Value::as_u64))
        .map(|n| n as usize)
        .unwrap_or(0)
}

/// The deploy's webhook target: `(APP_LAYER_URL, INTERNAL_TOKEN)`, or `None` when either is missing.
pub fn configured(env: &Env) -> Option<(String, String)> {
    match (
        env.var("APP_LAYER_URL").map(|v| v.to_string()),
        env.secret("INTERNAL_TOKEN").map(|v| v.to_string()),
    ) {
        (Ok(url), Ok(token)) => Some((url, token)),
        _ => None,
    }
}

/// THE TRANSPORT: POST one body to the app layer's `path`. `Ok(dropped)` on a 2xx, `Err(())` when the POST was
/// not accepted (logged here under `tag`).
pub async fn post_body(
    env: &Env,
    url: &str,
    token: &str,
    path: &str,
    tag: &str,
    body: String,
) -> std::result::Result<usize, ()> {
    post_body_answer(env, url, token, path, tag, body)
        .await
        .map(|answer| answered_dropped(&answer))
}

/// [`post_body`], answering the app layer's 2xx body itself (bsv-low #660, B-OWED-3-FOLD L2: `/internal/tx-word`
/// answers whether the frame was held).
pub async fn post_body_answer(
    env: &Env,
    url: &str,
    token: &str,
    path: &str,
    tag: &str,
    body: String,
) -> std::result::Result<String, ()> {
    let mut init = RequestInit::new();
    init.with_method(Method::Post);
    let headers = Headers::new();
    let _ = headers.set("Authorization", &format!("Bearer {}", token.trim()));
    let _ = headers.set("content-type", "application/json");
    init.with_headers(headers);
    init.with_body(Some(body.into()));
    let Ok(req) = Request::new_with_init(&format!("{}{path}", url.trim_end_matches('/')), &init)
    else {
        console_log!("[{tag}] request build failed: one body not notified");
        return Err(());
    };
    // The app-layer is a Worker on this account: the POST rides the
    // APP_LAYER service binding (Cloudflare refuses a plain fetch between two
    // Workers on one zone: 1042 behind a 404, and every *.workers.dev host
    // of an account is one zone). A deploy without the binding falls back to
    // a public fetch, which is only right for an app-layer on another zone.
    let sent = match env.service("APP_LAYER") {
        Ok(svc) => svc.fetch_request(req).await,
        Err(_) => Fetch::Request(req).send().await,
    };
    match sent {
        Ok(mut r) if (200..300).contains(&r.status_code()) => {
            Ok(r.text().await.unwrap_or_default())
        }
        Ok(mut r) => {
            let status = r.status_code();
            let body = r.text().await.unwrap_or_default();
            let excerpt: String = body
                .chars()
                .take(200)
                .collect::<String>()
                .replace(['\n', '\r'], " ");
            console_log!("[{tag}] app-layer HTTP {status} {excerpt}: one body not notified");
            Err(())
        }
        Err(e) => {
            console_log!("[{tag}] notify failed: {e}: one body not notified");
            Err(())
        }
    }
}

/// The counter names of one set's flush (a [`Tally`]'s five fields), and the name its never-re-sent figure is
/// served under.
pub struct FlushCounters {
    /// Refused by the app layer, failed on the retry too, or past the note-back memory.
    pub undelivered: &'static str,
    /// The part of `undelivered` that was retried and still failed.
    pub retry_failed: &'static str,
    /// Past one flush's bound, noted back.
    pub deferred: &'static str,
    /// A failed POST's entries, noted back once.
    pub retried: &'static str,
    /// Noted-back entries a later flush POSTed again.
    pub resent: &'static str,
    /// Derived on the read, never written: `retried + deferred - resent`.
    pub noted_back_unresent: &'static str,
    /// bsv-low #560: new note-backs parked in the D1 outbox (`change_parked`, this set's kind).
    pub outboxed: &'static str,
}

/// Log the flush's delivery misses; the counters are collected with its outbox rows by [`FlushBatch`].
pub fn log_account(tag: &str, shipped: usize, tally: Tally) {
    let Tally {
        undelivered,
        retry_failed,
        retried,
        deferred,
        resent,
    } = tally;
    if undelivered + retried + deferred + resent == 0 {
        return;
    }
    if undelivered + retried + deferred > 0 {
        console_log!(
            "[{tag}] flush of {shipped} entry(ies): {undelivered} NOT delivered (refused by the app layer, the POST failed twice: {retry_failed} after a retry, or let go past the isolate's bounded memory of note-backs), {retried} noted back to retry once, {deferred} deferred to the next flush"
        );
    }
}

thread_local! {
    // A failed atomic batch has written none of its counters. Retry those deltas with the next flush.
    static PENDING_ACCOUNT: RefCell<BTreeMap<String, u64>> = const { RefCell::new(BTreeMap::new()) };
}

struct ParkSet {
    kind: &'static str,
    counter: &'static str,
    items: Vec<String>,
}

/// All counters and new outbox rows of one flush, committed in one D1 round trip after delivery.
/// The fixed three-second reserve covers this one round trip, regardless of the counter or frame count.
/// A database fault leaves the notes in memory and retries the counter deltas on the next flush.
#[derive(Default)]
pub struct FlushBatch {
    counters: BTreeMap<String, u64>,
    parks: Vec<ParkSet>,
}

// The generation is minted in this transaction and belongs only to its parks. Counting that generation
// derives both successes and capacity refusals inside the same batch, without a follow-up D1 call.
const PARK_ACCOUNT_SQL: &str = "INSERT INTO ops_counters (name, value) \
    SELECT ?1, CASE WHEN ?4 = 1 THEN ?3 - COUNT(*) ELSE COUNT(*) END \
    FROM change_parked WHERE kind = ?2 \
        AND generation = (SELECT n FROM change_parked_seq WHERE id = 1) \
    HAVING CASE WHEN ?4 = 1 THEN ?3 - COUNT(*) ELSE COUNT(*) END > 0 \
    ON CONFLICT(name) DO UPDATE SET value = ops_counters.value + excluded.value";

impl FlushBatch {
    pub(crate) fn bump(&mut self, name: impl Into<String>, delta: u64) {
        if delta > 0 {
            *self.counters.entry(name.into()).or_default() += delta;
        }
    }

    pub(crate) fn tally(&mut self, counters: &FlushCounters, tally: Tally) {
        for (name, n) in [
            (counters.undelivered, tally.undelivered),
            (counters.retry_failed, tally.retry_failed),
            (counters.retried, tally.retried),
            (counters.deferred, tally.deferred),
            (counters.resent, tally.resent),
        ] {
            self.bump(name, n as u64);
        }
    }

    pub(crate) fn park(&mut self, kind: &'static str, items: Vec<String>, counter: &'static str) {
        if items.is_empty() {
            return;
        }
        let at = self
            .parks
            .iter()
            .position(|set| set.kind == kind)
            .unwrap_or_else(|| {
                self.parks.push(ParkSet {
                    kind,
                    counter,
                    items: Vec::new(),
                });
                self.parks.len() - 1
            });
        let set = &mut self.parks[at];
        let mut over = 0;
        for item in items {
            if set.items.contains(&item) {
                continue;
            }
            if set.items.len() < crate::change_park::OUTBOX_PARK_MAX {
                set.items.push(item);
            } else {
                over += 1;
            }
        }
        self.bump(crate::change_park::COUNTER_CHANGE_OUTBOX_OVER_BOUND, over);
    }

    fn queries(&self, now_ms: u64) -> Vec<crate::d1::Query> {
        use crate::{change_park, d1::Query};
        let mut queries: Vec<_> = self
            .counters
            .iter()
            .map(|(name, n)| {
                Query::new(crate::ops::BUMP_COUNTER_SQL)
                    .bind(name.as_str())
                    .bind(*n)
            })
            .collect();
        if !self.parks.is_empty() {
            queries.push(Query::new(change_park::PARK_SEQ_BUMP_SQL).bind(now_ms));
        }
        for set in &self.parks {
            for item in &set.items {
                queries.push(
                    Query::new(change_park::PARK_SQL)
                        .bind(set.kind)
                        .bind(item.as_str())
                        .bind(now_ms)
                        .bind(change_park::CHANGE_PARKED_MAX_ROWS),
                );
            }
            for (counter, full) in [(set.counter, 0), (change_park::COUNTER_CHANGE_PARK_FULL, 1)] {
                queries.push(
                    Query::new(PARK_ACCOUNT_SQL)
                        .bind(counter)
                        .bind(set.kind)
                        .bind(set.items.len() as u64)
                        .bind(full),
                );
            }
        }
        queries
    }

    fn failed(mut self) {
        let rows: usize = self.parks.iter().map(|set| set.items.len()).sum();
        self.bump(crate::change_park::COUNTER_CHANGE_PARK_FAULTS, rows as u64);
        PENDING_ACCOUNT.with(|pending| {
            let mut pending = pending.borrow_mut();
            for (name, n) in self.counters {
                *pending.entry(name).or_default() += n;
            }
        });
        // No further database await spends the reserve after a failed batch.
    }

    pub(crate) async fn flush_with<P: FlushPort>(mut self, port: &P, now_ms: u64) {
        for (name, n) in PENDING_ACCOUNT.with(|pending| std::mem::take(&mut *pending.borrow_mut()))
        {
            self.bump(name, n);
        }
        let queries = self.queries(now_ms);
        if queries.is_empty() {
            return;
        }
        if let Err(error) = port.commit(queries).await {
            port.log(format!("[change-outbox] flush accounting batch FAULTED ({error}); notes stay in memory, counters await the next flush"));
            self.failed();
        }
    }

    pub(crate) async fn flush(self, env: &Env, now_ms: u64) {
        match env.d1("OVERLAY_DB") {
            Ok(db) => self.flush_with(&WorkerFlush { db: &db }, now_ms).await,
            Err(error) => {
                console_log!(
                    "[change-outbox] no OVERLAY_DB: notes and counters stay in memory ({error})"
                );
                self.failed();
            }
        }
    }
}

/// The one post-delivery round trip, shared by D1 and the real-SQLite pins.
pub(crate) trait FlushPort {
    async fn commit(&self, queries: Vec<crate::d1::Query>) -> std::result::Result<(), String>;
    fn log(&self, line: String);
}

struct WorkerFlush<'a> {
    db: &'a D1Database,
}

impl FlushPort for WorkerFlush<'_> {
    async fn commit(&self, queries: Vec<crate::d1::Query>) -> std::result::Result<(), String> {
        let statements = queries
            .iter()
            .map(|query| query.prepare(self.db))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        crate::d1_ledger::counted_batch(self.db, statements)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn log(&self, line: String) {
        console_log!("{line}");
    }
}

/// D2-ACTOR-D: the most events one `/internal/owed-direct` body carries.
pub const OWED_DIRECT_BODY_MAX: usize = 64;
/// The wall of one hand-off POST (the app layer answers before it calls any actor).
pub const OWED_DIRECT_POST_WALL_MS: u64 = 2_000;

/// What one hand-off did ([`ship_owed_direct_with`]): the POSTs made, the chunks that walled or were refused (each
/// one fault, `owed_actor_direct_faults_total`), the events a fault left to the queue, and the operator's lines
/// (the caller logs them; this function never does, so a native pin runs it).
#[derive(Debug, Default, PartialEq, Eq)]
pub struct OwedDirectShipped {
    /// The POSTs made (one a chunk of at most [`OWED_DIRECT_BODY_MAX`] events).
    pub posts: usize,
    /// The chunks that walled, were refused or were never started: each one fault.
    pub faults: u64,
    /// The events those chunks carried: the queue's alone now.
    pub left_to_queue: usize,
    /// The operator's lines, for the caller to log.
    pub lines: Vec<String>,
}

/// D2-ACTOR-D (bsv-low #952; the design's bar 2 as amended: "the writer calls the identity's actor directly after its
/// answer, the queue is the durable backstop"): the overlay's flush point hands its published events (the direct set
/// `owed_events::publish_env` held, [`crate::owed_events::take_direct`]) to the app layer, which calls each identity's
/// actor (`POST /internal/owed-direct`, answered 202 at once, the calls after its answer). Chunked at
/// [`OWED_DIRECT_BODY_MAX`]; each POST raced against [`OWED_DIRECT_POST_WALL_MS`]. A POST that walls or is refused is
/// counted `owed_actor_direct_faults_total` and logged: never a fault of the write (its queue messages were sent).
///
/// D2-ACTOR-B-FOLD-10 (the fifth delta lens's NEW5-L3): the WHOLE hand-off shares one wall, the earlier of
/// [`OWED_DIRECT_POST_WALL_MS`] from its start and the flush's own `deadline_ms` (it was 2 s a chunk, after which the
/// flush computed its 20 s wall: up to 8 chunks of 2 s stacked on it). A chunk not started inside it is left to the
/// queue, logged and counted as one fault.
///
/// FIX-952 (bsv-low #952, item 1): the hand-off is [`ship_owed_direct_with`] over the transport, the clock and the
/// timer, so the pin `fix952_e` runs the shipped chunking, wall and fault rule natively with the app layer's own
/// route body parser and direct pass on the other side of a fake binding; this wrapper supplies the `APP_LAYER`
/// binding's POST ([`post_body_answer`]: the bearer `INTERNAL_TOKEN` the binding's caller holds), the platform clock
/// and `Delay`, and logs the lines.
pub async fn ship_owed_direct(env: &Env, deadline_ms: u64) {
    let events = crate::owed_events::take_direct();
    if events.is_empty() {
        return;
    }
    let Some((url, token)) = configured(env) else {
        console_log!(
            "[owed-direct] not configured (APP_LAYER_URL / INTERNAL_TOKEN): {} event(s) left to the queue",
            events.len()
        );
        return;
    };
    let shipped = ship_owed_direct_with(
        events,
        |body| {
            post_body_answer(
                env,
                &url,
                &token,
                "/internal/owed-direct",
                "owed-direct",
                body,
            )
        },
        || Date::now().as_millis(),
        deadline_ms,
        |ms| Delay::from(std::time::Duration::from_millis(ms)),
    )
    .await;
    for line in &shipped.lines {
        console_log!("{line}");
    }
    if shipped.faults > 0 {
        crate::owed_events::bump(env, &[("owed_actor_direct_faults_total", shipped.faults)]).await;
    }
}

/// THE HAND-OFF over its ports (FIX-952): `events` chunked at [`OWED_DIRECT_BODY_MAX`], each chunk ONE body
/// `{"events": [<the queue's own message text>, ...]}` (every event as it rides the queue: its `seq`, `atMs`, source
/// and facts, so the app layer's actor call carries the queue delivery's key) POSTed through `post` (`Ok`: a 2xx;
/// `Err`: refused), under ONE wall: the earlier of [`OWED_DIRECT_POST_WALL_MS`] from the first `now_ms()` and
/// `deadline_ms` ([`owed_direct_end`]); a chunk not started inside it is left to the queue. Each walled, refused or
/// unstarted chunk is one fault and one line.
pub async fn ship_owed_direct_with<P, Fut, C, S, Sleep>(
    events: Vec<overlay_discovery::owed_event::OwedEvent>,
    mut post: P,
    now_ms: C,
    deadline_ms: u64,
    sleep: S,
) -> OwedDirectShipped
where
    P: FnMut(String) -> Fut,
    Fut: Future<Output = std::result::Result<String, ()>>,
    C: Fn() -> u64,
    S: Fn(u64) -> Sleep,
    Sleep: Future<Output = ()>,
{
    let mut out = OwedDirectShipped::default();
    let end = owed_direct_end(now_ms(), deadline_ms);
    for (n, chunk) in events.chunks(OWED_DIRECT_BODY_MAX).enumerate() {
        let left = end.saturating_sub(now_ms());
        if left == 0 {
            let rest = events.len() - n * OWED_DIRECT_BODY_MAX;
            out.lines.push(format!(
                "[owed-direct] the hand-off's wall fell: {rest} event(s) left to the queue"
            ));
            out.faults += 1;
            out.left_to_queue += rest;
            break;
        }
        let body =
            serde_json::json!({"events": chunk.iter().map(|e| e.encode()).collect::<Vec<_>>()})
                .to_string();
        out.posts += 1;
        match crate::race_or_deadline(post(body), sleep(left)).await {
            Some(Ok(_)) => {}
            Some(Err(())) => {
                out.faults += 1;
                out.left_to_queue += chunk.len();
            }
            None => {
                out.lines.push(format!(
                    "[owed-direct] the hand-off passed its {OWED_DIRECT_POST_WALL_MS} ms wall: {} event(s) left to the queue",
                    chunk.len()
                ));
                out.faults += 1;
                out.left_to_queue += chunk.len();
            }
        }
    }
    out
}

/// PURE (NEW5-L3): the hand-off's end, the earlier of [`OWED_DIRECT_POST_WALL_MS`] from `now_ms` and the flush's
/// `deadline_ms`.
pub fn owed_direct_end(now_ms: u64, deadline_ms: u64) -> u64 {
    (now_ms + OWED_DIRECT_POST_WALL_MS).min(deadline_ms)
}

/// Run a future that never waits on anything outside itself (the pins' fake transports) to its answer.
#[cfg(test)]
pub(crate) fn run<F: Future>(fut: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a current-thread runtime")
        .block_on(fut)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
pub(crate) mod tests {
    use super::*;
    use std::cell::{Cell, RefCell};

    pub(crate) fn commit_queries(
        conn: &rusqlite::Connection,
        queries: Vec<crate::d1::Query>,
    ) -> std::result::Result<(), String> {
        use crate::d1::QVal;
        use rusqlite::types::Value;
        let tx = conn.unchecked_transaction().map_err(|e| e.to_string())?;
        for query in queries {
            let params = query.params().iter().map(|v| match v {
                QVal::Null => Value::Null,
                QVal::Int(i) => Value::Integer(*i),
                QVal::Text(t) => Value::Text(t.clone()),
                QVal::Bool(b) => Value::Integer(i64::from(*b)),
                QVal::Blob(b) => Value::Blob(b.clone()),
                QVal::Float(f) => Value::Real(*f),
            });
            let mut stmt = tx.prepare(query.sql()).map_err(|e| e.to_string())?;
            let mut rows = stmt
                .query(rusqlite::params_from_iter(params))
                .map_err(|e| e.to_string())?;
            while rows.next().map_err(|e| e.to_string())?.is_some() {}
        }
        tx.commit().map_err(|e| e.to_string())
    }

    struct ClockedFlush<'a> {
        conn: rusqlite::Connection,
        clock: &'a Cell<u64>,
        calls: Cell<usize>,
        fail: Cell<bool>,
    }

    impl FlushPort for ClockedFlush<'_> {
        async fn commit(
            &self,
            mut queries: Vec<crate::d1::Query>,
        ) -> std::result::Result<(), String> {
            self.calls.set(self.calls.get() + 1);
            self.clock.set(self.clock.get() + 2_900);
            if self.fail.get() {
                // A failure after both counter and park statements must roll back the whole batch.
                queries.push(crate::d1::Query::new(
                    "INSERT INTO missing_flush_table VALUES (1)",
                ));
            }
            commit_queries(&self.conn, queries)
        }
        fn log(&self, _: String) {}
    }

    fn flush_db() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        for sql in crate::d1::OVERLAY_MIGRATIONS {
            if let Err(error) = conn.execute_batch(sql) {
                assert!(
                    error.to_string().contains("duplicate column"),
                    "{error}: {sql}"
                );
            }
        }
        conn
    }

    fn counter(conn: &rusqlite::Connection, name: &str) -> u64 {
        conn.query_row(
            "SELECT value FROM ops_counters WHERE name = ?",
            [name],
            |r| r.get(0),
        )
        .unwrap_or(0)
    }

    #[test]
    fn d2_low2_full_flush_accounts_and_parks_in_one_reserved_round_trip() {
        use crate::change_park::{KIND_HOP, KIND_POT, KIND_WORD, OUTBOX_PARK_MAX};
        PENDING_ACCOUNT.with(|p| p.borrow_mut().clear());
        let clock = Cell::new(25_000);
        let crate::pot_changes::BackgroundFlush::Ship { deadline_ms } =
            crate::pot_changes::background_flush_plan(0, clock.get())
        else {
            panic!("delivery fits")
        };
        assert_eq!(deadline_ms, 27_000);
        let mut batch = FlushBatch::default();
        for (kind, name) in [
            (KIND_HOP, crate::hop_changes::COUNTER_HOP_CHANGED_OUTBOXED),
            (KIND_POT, crate::pot_changes::COUNTER_POT_CHANGED_OUTBOXED),
            (KIND_WORD, crate::change_park::COUNTER_CHANGE_PARKED),
        ] {
            batch.park(
                kind,
                (0..OUTBOX_PARK_MAX)
                    .map(|n| format!("{kind}-{n}"))
                    .collect(),
                name,
            );
        }
        for n in 0..25 {
            batch.bump(format!("flush-pin-{n}"), n + 1);
        }
        clock.set(deadline_ms);
        let port = ClockedFlush {
            conn: flush_db(),
            clock: &clock,
            calls: Cell::new(0),
            fail: Cell::new(false),
        };
        run(batch.flush_with(&port, clock.get()));
        assert_eq!(
            port.calls.get(),
            1,
            "the reserve covers one database round trip"
        );
        assert_eq!(clock.get(), 29_900);
        assert!(clock.get() < crate::pot_changes::WAIT_UNTIL_WINDOW_MS);
        let rows: usize = port
            .conn
            .query_row("SELECT COUNT(*) FROM change_parked", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 3 * OUTBOX_PARK_MAX);
        for name in [
            crate::hop_changes::COUNTER_HOP_CHANGED_OUTBOXED,
            crate::pot_changes::COUNTER_POT_CHANGED_OUTBOXED,
            crate::change_park::COUNTER_CHANGE_PARKED,
        ] {
            assert_eq!(counter(&port.conn, name), OUTBOX_PARK_MAX as u64);
        }
        assert_eq!(counter(&port.conn, "flush-pin-24"), 25);
        assert_eq!(
            counter(&port.conn, crate::change_park::COUNTER_CHANGE_PARK_FULL),
            0
        );
        let src = include_str!("change_flush.rs");
        let worker = &src[src.find("impl FlushPort for WorkerFlush").unwrap()
            ..src.find("/// Run a future").unwrap()];
        assert_eq!(worker.matches("counted_batch(").count(), 1);
        assert!(!worker.contains("bump_counter("));
    }

    #[test]
    fn d2_low2_batch_counts_full_and_over_bound_and_rolls_back_on_a_fault() {
        use crate::change_park::*;
        PENDING_ACCOUNT.with(|p| p.borrow_mut().clear());
        let clock = Cell::new(0);
        let port = ClockedFlush {
            conn: flush_db(),
            clock: &clock,
            calls: Cell::new(0),
            fail: Cell::new(false),
        };
        for n in 0..CHANGE_PARKED_MAX_ROWS - 1 {
            port.conn.execute("INSERT INTO change_parked (kind, item, parked_at_ms, generation) VALUES ('pot', ?, 0, 1)", [format!("held-{n}")]).unwrap();
        }
        let mut batch = FlushBatch::default();
        batch.park(
            KIND_POT,
            (0..OUTBOX_PARK_MAX + 1)
                .map(|n| format!("new-{n}"))
                .collect(),
            crate::pot_changes::COUNTER_POT_CHANGED_OUTBOXED,
        );
        run(batch.flush_with(&port, 1_000));
        assert_eq!(
            counter(&port.conn, crate::pot_changes::COUNTER_POT_CHANGED_OUTBOXED),
            1
        );
        assert_eq!(counter(&port.conn, COUNTER_CHANGE_PARK_FULL), 127);
        assert_eq!(counter(&port.conn, COUNTER_CHANGE_OUTBOX_OVER_BOUND), 1);
        // Re-parking a held row still succeeds at the ceiling, under a new generation.
        let mut batch = FlushBatch::default();
        batch.park(
            KIND_POT,
            vec!["new-0".into()],
            crate::pot_changes::COUNTER_POT_CHANGED_OUTBOXED,
        );
        run(batch.flush_with(&port, 1_000));
        assert_eq!(
            counter(&port.conn, crate::pot_changes::COUNTER_POT_CHANGED_OUTBOXED),
            2
        );
        port.conn
            .execute(
                "DELETE FROM change_parked WHERE kind = 'pot' AND item = 'held-0'",
                [],
            )
            .unwrap();
        port.fail.set(true);
        let mut batch = FlushBatch::default();
        batch.bump("fault-pin", 5);
        batch.park(KIND_WORD, vec!["word".into()], COUNTER_CHANGE_PARKED);
        run(batch.flush_with(&port, 2_000));
        assert_eq!(counter(&port.conn, "fault-pin"), 0);
        let words: usize = port
            .conn
            .query_row(
                "SELECT COUNT(*) FROM change_parked WHERE kind = 'word'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(words, 0, "the park rolled back with its counter");
        port.fail.set(false);
        run(FlushBatch::default().flush_with(&port, 3_000));
        assert_eq!(counter(&port.conn, "fault-pin"), 5);
        assert_eq!(counter(&port.conn, COUNTER_CHANGE_PARK_FAULTS), 1);
        assert_eq!(counter(&port.conn, COUNTER_CHANGE_PARKED), 0);
        assert!(PENDING_ACCOUNT.with(|p| p.borrow().is_empty()));
    }

    fn items(n: usize) -> Vec<u32> {
        (0..n as u32).collect()
    }

    fn body(chunk: &[u32]) -> String {
        serde_json::json!({ "n": chunk }).to_string()
    }

    /// The loop: every entry in exactly one POST of at most eight, in order, no empty POST; past sixteen POSTs
    /// the rest is deferred, never dropped.
    #[test]
    fn the_flush_is_chunked_at_the_bound_and_a_flood_is_deferred_never_dropped() {
        for n in [0usize, 1, 8, 9, 16, 17, 128, 131] {
            let seen: RefCell<Vec<Vec<u32>>> = RefCell::new(Vec::new());
            let out = run(ship_chunks(
                items(n),
                body,
                |b: String| {
                    let v: serde_json::Value = serde_json::from_str(&b).unwrap();
                    seen.borrow_mut().push(
                        v["n"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|x| x.as_u64().unwrap() as u32)
                            .collect(),
                    );
                    async { Ok(0) }
                },
                || 0,
                1,
            ));
            let bound = CHANGE_CHUNK * CHANGE_MAX_CHUNKS;
            let shipped = n.min(bound);
            assert_eq!(out.posts, shipped.div_ceil(CHANGE_CHUNK), "{n}");
            assert!(
                seen.borrow()
                    .iter()
                    .all(|c| !c.is_empty() && c.len() <= CHANGE_CHUNK),
                "{n}"
            );
            assert_eq!(
                seen.borrow().concat(),
                items(shipped),
                "every shipped entry in exactly one body, in order ({n})"
            );
            assert_eq!(out.delivered, items(shipped));
            assert_eq!(
                out.deferred,
                items(n)[shipped..].to_vec(),
                "past the bound: deferred, never dropped ({n})"
            );
            assert!(out.failed.is_empty() && out.refused == 0);
        }
    }

    /// A POST the receiver did not accept fails its own chunk and no other; a 2xx that answers `dropped` is
    /// summed.
    #[test]
    fn a_failed_post_fails_its_own_chunk_and_a_refusal_is_summed() {
        let calls = Cell::new(0usize);
        let out = run(ship_chunks(
            items(19),
            body,
            |_b: String| {
                calls.set(calls.get() + 1);
                let n = calls.get();
                async move {
                    match n {
                        2 => Err(()),
                        3 => Ok(2),
                        _ => Ok(0),
                    }
                }
            },
            || 0,
            1,
        ));
        assert_eq!(out.posts, 3);
        assert_eq!(
            out.failed,
            items(19)[8..16].to_vec(),
            "the second chunk, whole"
        );
        assert_eq!(out.delivered, [&items(19)[..8], &items(19)[16..]].concat());
        assert_eq!(out.refused, 2);
        assert!(out.deferred.is_empty());
    }

    /// Lens L2: a failed entry is noted back ONCE. Its second failure is a loss; a delivery in between forgets
    /// it (a later failure is a first failure again); the memory is bounded.
    #[test]
    fn a_failed_entry_is_retried_once_and_only_once() {
        let mut retried: BTreeSet<u32> = BTreeSet::new();
        let (again, lost) = settle_failed(&mut retried, &[], vec![1, 2, 3]);
        assert_eq!(
            (again, lost),
            (vec![1, 2, 3], 0),
            "the first failure: noted back"
        );
        // the next flush: 1 is delivered, 2 fails again, 3 fails again, 4 fails for the first time
        let (again, lost) = settle_failed(&mut retried, &[1], vec![2, 3, 4]);
        assert_eq!(
            (again, lost),
            (vec![4], 2),
            "a second failure is counted and let go"
        );
        assert_eq!(
            retried.iter().copied().collect::<Vec<_>>(),
            vec![4],
            "only the entry awaiting its one retry is remembered"
        );
        // 2 is noted afresh by a later write and fails: a first failure again
        let (again, lost) = settle_failed(&mut retried, &[4], vec![2]);
        assert_eq!((again, lost), (vec![2], 0));
        // the memory is bounded: a failure it cannot hold is not retried
        let mut full: BTreeSet<u32> = (0..RETRY_MEMORY_MAX as u32).collect();
        let (again, lost) = settle_failed(&mut full, &[], vec![9_000_000]);
        assert_eq!((again.len(), lost, full.len()), (0, 1, RETRY_MEMORY_MAX));
    }

    /// The delta lens's D-L2: THE NOTE-BACKS MEET THEIR RESENDS, OR THE DIFFERENCE IS THE LOSS. Through the real
    /// loop and the real settle: a failed chunk is counted `retried` when noted back and `resent` when the next
    /// flush POSTs it, `retry_failed` by name when that POST fails too; a deferred chunk is counted `deferred`
    /// once (however often it is deferred again) and `resent` when it is POSTed. An isolate that dies holding
    /// its notes leaves `retried + deferred` ahead of `resent` by exactly what it held: `undelivered` reads 0
    /// there and `noted_back_unresent` does not.
    /// To red: count no `resent` (the base: the figure is the note-backs ever made), or count a re-deferral anew.
    #[test]
    fn a_note_back_is_counted_when_made_and_when_resent_and_the_difference_is_what_was_never_resent(
    ) {
        let post_failing = |fail: &'static [usize]| {
            let calls = Cell::new(0usize);
            move |_b: String| {
                calls.set(calls.get() + 1);
                let n = calls.get();
                async move {
                    if fail.contains(&n) {
                        Err(())
                    } else {
                        Ok(0)
                    }
                }
            }
        };
        let add = |sum: &mut Tally, t: Tally| {
            sum.undelivered += t.undelivered;
            sum.retry_failed += t.retry_failed;
            sum.retried += t.retried;
            sum.deferred += t.deferred;
            sum.resent += t.resent;
        };
        let unresent = |sum: &Tally| {
            noted_back_unresent(sum.retried as u64, sum.deferred as u64, sum.resent as u64)
        };
        let mut memory: NotedBack<u32> = NotedBack::new();
        let mut sum = Tally::default();

        // flush 1: 131 entries, the second POST answered 503: eight retried, three deferred, all noted back
        let (back, t) = settle(
            &mut memory,
            &run(ship_chunks(items(131), body, post_failing(&[2]), || 0, 1)),
        );
        assert_eq!(
            t,
            Tally {
                retried: 8,
                deferred: 3,
                ..Tally::default()
            }
        );
        assert_eq!(back, [&items(131)[8..16], &items(131)[128..]].concat());
        add(&mut sum, t);
        assert_eq!(
            (sum.undelivered, unresent(&sum)),
            (0, 11),
            "eleven note-backs made, none re-sent yet"
        );

        // flush 2 (the same isolate): the eleven ride it; the retried chunk fails AGAIN, the deferred three arrive
        let (back2, t) = settle(
            &mut memory,
            &run(ship_chunks(back.clone(), body, post_failing(&[1]), || 0, 1)),
        );
        assert_eq!(
            t,
            Tally {
                undelivered: 8,
                retry_failed: 8,
                resent: 11,
                ..Tally::default()
            },
            "retried and still failed: its own name"
        );
        assert!(back2.is_empty(), "the retry is bounded at one");
        add(&mut sum, t);
        assert_eq!(
            unresent(&sum),
            0,
            "every note-back was POSTed again: the account closes"
        );
        assert!(memory.retried.is_empty() && memory.deferred.is_empty());

        // a flood deferred twice over is ONE note-back per entry: 300 entries, 128 a flush
        let (back, t) = settle(
            &mut memory,
            &run(ship_chunks(items(300), body, post_failing(&[]), || 0, 1)),
        );
        assert_eq!((t.deferred, back.len()), (172, 172));
        add(&mut sum, t);
        let (back, t) = settle(
            &mut memory,
            &run(ship_chunks(back, body, post_failing(&[]), || 0, 1)),
        );
        assert_eq!(
            (t.resent, t.deferred, back.len()),
            (128, 0, 44),
            "the 44 still waiting are not counted again"
        );
        add(&mut sum, t);
        assert_eq!(unresent(&sum), 44);
        // a deferred entry whose POST then fails: its deferral is re-sent, its failure is a new note-back
        let (back, t) = settle(
            &mut memory,
            &run(ship_chunks(back, body, post_failing(&[1]), || 0, 1)),
        );
        assert_eq!(
            t,
            Tally {
                retried: 8,
                resent: 44,
                ..Tally::default()
            }
        );
        add(&mut sum, t);
        assert_eq!((back.len(), unresent(&sum)), (8, 8));

        // THE HOLE: the isolate is evicted holding those eight. Nothing more is ever counted for them:
        // `undelivered` says nothing was lost since the eight of flush 2, and the unresent figure names them.
        drop(memory);
        assert_eq!(
            (sum.undelivered, sum.retry_failed, unresent(&sum)),
            (8, 8, 8),
            "a loss `undelivered` cannot see, under its own name"
        );

        // a second isolate's flushes do not disturb the figure (its own note-backs meet its own resends)
        let mut other: NotedBack<u32> = NotedBack::new();
        let (back, t) = settle(
            &mut other,
            &run(ship_chunks(items(9), body, post_failing(&[2]), || 0, 1)),
        );
        add(&mut sum, t);
        let (_, t) = settle(
            &mut other,
            &run(ship_chunks(back, body, post_failing(&[]), || 0, 1)),
        );
        add(&mut sum, t);
        assert_eq!(unresent(&sum), 8);

        // the memory is bounded: past it a deferred entry is let go and counted undelivered, never an untracked note
        let mut full: NotedBack<u32> = NotedBack::new();
        full.deferred = (1_000_000..1_000_000 + DEFER_MEMORY_MAX as u32).collect();
        let (back, t) = settle(
            &mut full,
            &run(ship_chunks(items(131), body, post_failing(&[]), || 0, 1)),
        );
        assert_eq!(
            (back.len(), t),
            (
                0,
                Tally {
                    undelivered: 3,
                    ..Tally::default()
                }
            )
        );
        assert_eq!(
            noted_back_unresent(1, 1, 5),
            0,
            "floored for historical partial counter writes"
        );
    }

    /// bsv-low #560 (B-COST-1), the lens's LOW-3: an entry the FULL note-back memory lets go (a first failure past
    /// [`RETRY_MEMORY_MAX`], a deferral past [`DEFER_MEMORY_MAX`]) is a NEW note-back for the outbox, handed to it
    /// FIRST (it has no memory to fall back on), and still counted undelivered by the memory's account; a second
    /// failure (parked by the flush that noted it back) and a refusal are not. RED with the let-go entries kept out
    /// of `fresh` (the fold's base): `left: []`.
    #[test]
    fn b_cost_1_560f_l3_an_entry_the_full_memory_lets_go_is_parked_first() {
        let shipped = |failed: Vec<u32>, deferred: Vec<u32>| Shipped {
            posts: 1,
            delivered: vec![],
            refused: 2,
            failed,
            deferred,
        };
        // both memories full: the failed chunk and the deferred tail are let go, and every one is fresh
        let mut full: NotedBack<u32> = NotedBack::new();
        full.retried = (1_000_000..1_000_000 + RETRY_MEMORY_MAX as u32).collect();
        full.deferred = (2_000_000..2_000_000 + DEFER_MEMORY_MAX as u32).collect();
        let (back, fresh, t) = settle_noting(&mut full, &shipped(vec![1, 2], vec![3]));
        assert!(back.is_empty(), "the memory holds none of them");
        assert_eq!(
            fresh,
            vec![1, 2, 3],
            "every let-go entry goes to the outbox"
        );
        assert_eq!(
            t,
            Tally {
                undelivered: 2 + 3,
                ..Tally::default()
            },
            "the memory's account is unchanged: let go, counted undelivered"
        );
        // a second failure is lost from memory and NOT fresh (its first failure was parked by that flush)
        let (_, fresh, t) = settle_noting(&mut full, &shipped(vec![1_000_000], vec![]));
        assert!(fresh.is_empty());
        assert_eq!((t.undelivered, t.retry_failed), (2 + 1, 1));
        // room in the memory: a let-go deferral comes BEFORE the memory's own new note-backs
        let mut half: NotedBack<u32> = NotedBack::new();
        half.deferred = (2_000_000..2_000_000 + DEFER_MEMORY_MAX as u32).collect();
        let (back, fresh, _) = settle_noting(&mut half, &shipped(vec![7, 8], vec![9]));
        assert_eq!(back, vec![7, 8]);
        assert_eq!(fresh, vec![9, 7, 8], "the entry with no memory first");
    }

    /// Lens L4, THE WORST CASE FLUSH MEASURED: 128 pot outpoints (and three more) in one flush. Sixteen
    /// sequential POSTs, never a seventeenth; the largest body and the flush's bytes; and the wall: on a
    /// modelled clock where the app layer takes 2 s per body, the flush stops STARTING POSTs at its budget and
    /// defers the tail (counted) instead of losing it with the task.
    #[test]
    fn the_worst_case_flush_is_bounded_in_posts_bytes_and_wall() {
        let pots: Vec<(String, u32)> = (0..131u32)
            .map(|i| (format!("{i:064x}"), u32::MAX))
            .collect();
        let bytes: RefCell<Vec<usize>> = RefCell::new(Vec::new());
        let in_flight = Cell::new(0usize);
        let most_in_flight = Cell::new(0usize);
        let fast = run(ship_chunks(
            pots.clone(),
            crate::pot_changes::body_json,
            |b: String| {
                bytes.borrow_mut().push(b.len());
                in_flight.set(in_flight.get() + 1);
                most_in_flight.set(most_in_flight.get().max(in_flight.get()));
                let in_flight = &in_flight;
                async move {
                    in_flight.set(in_flight.get() - 1);
                    Ok(0)
                }
            },
            || 0,
            FLUSH_WALL_BUDGET_MS,
        ));
        assert_eq!(
            fast.posts, CHANGE_MAX_CHUNKS,
            "sixteen POSTs, never a seventeenth"
        );
        assert_eq!((fast.delivered.len(), fast.deferred.len()), (128, 3));
        assert_eq!(
            most_in_flight.get(),
            1,
            "sequential: one subrequest in flight at a time"
        );
        let largest = *bytes.borrow().iter().max().unwrap();
        let total: usize = bytes.borrow().iter().sum();
        println!(
            "worst case pot flush: {} POSTs, largest body {largest} bytes, {total} bytes in all",
            fast.posts
        );
        assert_eq!(
            largest, 767,
            "a body of eight outpoints with the widest vout"
        );
        assert_eq!(total, 16 * 767);

        // the wall: 2 s per POST on a modelled clock
        let clock = Cell::new(0u64);
        let slow = run(ship_chunks(
            pots,
            crate::pot_changes::body_json,
            |_b: String| {
                clock.set(clock.get() + 2_000);
                async { Ok(0) }
            },
            || clock.get(),
            FLUSH_WALL_BUDGET_MS,
        ));
        println!(
            "at 2 s per POST: {} POSTs in {} ms, {} delivered, {} deferred",
            slow.posts,
            clock.get(),
            slow.delivered.len(),
            slow.deferred.len()
        );
        assert_eq!(slow.posts, 10, "no POST is started at or past the budget");
        assert_eq!(clock.get(), FLUSH_WALL_BUDGET_MS);
        assert_eq!(
            (slow.delivered.len(), slow.deferred.len()),
            (80, 51),
            "the tail is deferred and counted, not lost with the task"
        );
        const {
            assert!(
                FLUSH_WALL_BUDGET_MS < 30_000,
                "inside the wall `wait_until` holds a task for"
            )
        };
    }

    /// The app layer's `dropped` answer is read (additive: an answer without it, or not JSON, is 0).
    #[test]
    fn the_app_layers_dropped_answer_is_read() {
        assert_eq!(
            answered_dropped(r#"{"ok":true,"filed":[],"skipped":[],"dropped":3}"#),
            3
        );
        assert_eq!(
            answered_dropped(r#"{"ok":true,"filed":[],"skipped":[]}"#),
            0
        );
        assert_eq!(answered_dropped("not json"), 0);
    }
}
