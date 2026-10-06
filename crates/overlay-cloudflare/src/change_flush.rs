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
//! request, plus one D1 write per counter that moved. The time one POST takes is the app layer's handling of a
//! body of eight (one broadcast push per outpoint, the attribution reads, the per-seat filings), which no native
//! pin can measure: it is beta's figure. The wall budget makes the bound hold whatever that figure is: at a
//! modelled 2 s per POST a flush of 128 gets 10 POSTs out in its 20 s and DEFERS the other 48 entries, counted,
//! where it used to lose them with the task, uncounted (the pin runs 131 entries: 80 delivered, 51 deferred). One POST that hangs past the runtime's wall is still
//! lost with the task (no timeout races it); the budget only stops the flush from starting a POST it cannot
//! afford.
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
//! with an isolate (or with a POST still in flight when the task's wall ended), never re-sent. It errs toward
//! reading a loss: a note-back whose own counter write failed is not counted at all (the floor hides a resend
//! that was), and the per-isolate memory of note-backs is bounded ([`RETRY_MEMORY_MAX`],
//! [`DEFER_MEMORY_MAX`]): past it a failed or deferred entry is not noted back, it is let go and counted
//! undelivered at once. The cost of every one of these is the receiver's own cadence (the owed list's
//! read-aged recompute, the Lobby's next event), never money. Making the note-back durable needs a store of
//! its own (a D1 outbox: a migration, a write per flush and a drain); it is filed as its own issue.
use std::collections::BTreeSet;
use std::future::Future;

use worker::*;

/// The app layer's bound on one webhook body (`internal_events::POT_CHANGED_MAX`, `HOP_CHANGED_MAX`,
/// `LOBBY_CHANGED_MAX`: all eight, pinned equal from its tests).
pub const CHANGE_CHUNK: usize = 8;

/// The most POSTs one flush of one set makes (128 entries).
pub const CHANGE_MAX_CHUNKS: usize = 16;

/// The wall one detached flush task may spend STARTING POSTs, from its own start. `wait_until` holds a task for
/// about thirty seconds after the answer; the rest is left for a POST in flight, the note-back and the counters.
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
    /// Entries past the flush's bound (the chunk count, or the wall): not POSTed.
    pub deferred: Vec<T>,
}

/// THE FLUSH LOOP, transport injected. `post` gets one chunk's body and answers `Ok(dropped)` on a 2xx (what
/// the receiver answered it refused) or `Err(())`; `now_ms` is the clock the wall is read on; no POST is
/// STARTED at or past `deadline_ms`. Sequential: one POST in flight at a time.
pub async fn ship_chunks<T, B, P, Fut, C>(items: Vec<T>, body: B, mut post: P, now_ms: C, deadline_ms: u64) -> Shipped<T>
where
    T: Clone,
    B: Fn(&[T]) -> String,
    P: FnMut(String) -> Fut,
    Fut: Future<Output = std::result::Result<usize, ()>>,
    C: Fn() -> u64,
{
    let mut out = Shipped { posts: 0, delivered: Vec::new(), refused: 0, failed: Vec::new(), deferred: Vec::new() };
    for (i, chunk) in items.chunks(CHANGE_CHUNK).enumerate() {
        if i >= CHANGE_MAX_CHUNKS || now_ms() >= deadline_ms {
            out.deferred.extend_from_slice(chunk);
            continue;
        }
        out.posts += 1;
        match post(body(chunk)).await {
            Ok(dropped) => {
                out.refused += dropped;
                out.delivered.extend_from_slice(chunk);
            }
            Err(()) => out.failed.extend_from_slice(chunk),
        }
    }
    out
}

/// PURE (lens L2): a failed POST's entries are retried ONCE. `retried` is the isolate's memory of the entries
/// already noted back once: a delivered entry leaves it; a failed entry not in it enters it and is returned to
/// be noted back; a failed entry already in it leaves it and is counted lost (so is one the full memory cannot
/// hold). Returns `(note back, lost)`.
pub fn settle_failed<T: Ord + Clone>(retried: &mut BTreeSet<T>, delivered: &[T], failed: Vec<T>) -> (Vec<T>, usize) {
    for d in delivered {
        retried.remove(d);
    }
    let mut again = Vec::new();
    let mut lost = 0usize;
    for f in failed {
        if retried.remove(&f) || retried.len() >= RETRY_MEMORY_MAX {
            lost += 1;
        } else {
            retried.insert(f.clone());
            again.push(f);
        }
    }
    (again, lost)
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
    pub const fn new() -> Self {
        Self { retried: BTreeSet::new(), deferred: BTreeSet::new() }
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
    let mut resent = 0usize;
    for posted in shipped.delivered.iter().chain(&shipped.failed) {
        if memory.deferred.remove(posted) || memory.retried.contains(posted) {
            resent += 1;
        }
    }
    let retry_failed = shipped.failed.iter().filter(|f| memory.retried.contains(*f)).count();
    let (mut back, lost) = settle_failed(&mut memory.retried, &shipped.delivered, shipped.failed.clone());
    let retried = back.len();
    let (mut deferred, mut let_go) = (0usize, 0usize);
    for d in &shipped.deferred {
        if memory.retried.contains(d) || memory.deferred.contains(d) {
            back.push(d.clone()); // still waiting: the same note-back, not a new one
        } else if memory.deferred.len() < DEFER_MEMORY_MAX {
            memory.deferred.insert(d.clone());
            deferred += 1;
            back.push(d.clone());
        } else {
            let_go += 1;
        }
    }
    (back, Tally { undelivered: shipped.refused + lost + let_go, retry_failed, retried, deferred, resent })
}

/// PURE: the note-backs never re-sent, from the three counters' totals (`/health/invariants` serves it per set
/// as `*_noted_back_unresent`). Floored at 0: the counters are bumped one statement at a time.
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
    match (env.var("APP_LAYER_URL").map(|v| v.to_string()), env.secret("INTERNAL_TOKEN").map(|v| v.to_string())) {
        (Ok(url), Ok(token)) => Some((url, token)),
        _ => None,
    }
}

/// THE TRANSPORT: POST one body to the app layer's `path`. `Ok(dropped)` on a 2xx, `Err(())` when the POST was
/// not accepted (logged here under `tag`).
pub async fn post_body(env: &Env, url: &str, token: &str, path: &str, tag: &str, body: String) -> std::result::Result<usize, ()> {
    let mut init = RequestInit::new();
    init.with_method(Method::Post);
    let headers = Headers::new();
    let _ = headers.set("Authorization", &format!("Bearer {}", token.trim()));
    let _ = headers.set("content-type", "application/json");
    init.with_headers(headers);
    init.with_body(Some(body.into()));
    let Ok(req) = Request::new_with_init(&format!("{}{path}", url.trim_end_matches('/')), &init) else {
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
        Ok(mut r) if (200..300).contains(&r.status_code()) => Ok(answered_dropped(&r.text().await.unwrap_or_default())),
        Ok(mut r) => {
            let status = r.status_code();
            let body = r.text().await.unwrap_or_default();
            let excerpt: String = body.chars().take(200).collect::<String>().replace(['\n', '\r'], " ");
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
}

/// The flush's account, after the POSTs: one log line and the counters, only when something did not go through.
pub async fn account(env: &Env, tag: &str, counters: &FlushCounters, shipped: usize, tally: Tally) {
    let Tally { undelivered, retry_failed, retried, deferred, resent } = tally;
    if undelivered + retried + deferred + resent == 0 {
        return;
    }
    if undelivered + retried + deferred > 0 {
        console_log!(
            "[{tag}] flush of {shipped} entry(ies): {undelivered} NOT delivered (refused by the app layer, or the POST failed twice: {retry_failed} after a retry), {retried} noted back to retry once, {deferred} deferred to the next flush"
        );
    }
    if let Ok(db) = env.d1("OVERLAY_DB") {
        crate::ops::bump_counter(&db, counters.undelivered, undelivered as u64).await;
        crate::ops::bump_counter(&db, counters.retry_failed, retry_failed as u64).await;
        crate::ops::bump_counter(&db, counters.retried, retried as u64).await;
        crate::ops::bump_counter(&db, counters.deferred, deferred as u64).await;
        crate::ops::bump_counter(&db, counters.resent, resent as u64).await;
    }
}

/// Run a future that never waits on anything outside itself (the pins' fake transports) to its answer.
#[cfg(test)]
pub(crate) fn run<F: Future>(fut: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread().build().expect("a current-thread runtime").block_on(fut)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::cell::{Cell, RefCell};

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
                    seen.borrow_mut().push(v["n"].as_array().unwrap().iter().map(|x| x.as_u64().unwrap() as u32).collect());
                    async { Ok(0) }
                },
                || 0,
                1,
            ));
            let bound = CHANGE_CHUNK * CHANGE_MAX_CHUNKS;
            let shipped = n.min(bound);
            assert_eq!(out.posts, shipped.div_ceil(CHANGE_CHUNK), "{n}");
            assert!(seen.borrow().iter().all(|c| !c.is_empty() && c.len() <= CHANGE_CHUNK), "{n}");
            assert_eq!(seen.borrow().concat(), items(shipped), "every shipped entry in exactly one body, in order ({n})");
            assert_eq!(out.delivered, items(shipped));
            assert_eq!(out.deferred, items(n)[shipped..].to_vec(), "past the bound: deferred, never dropped ({n})");
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
        assert_eq!(out.failed, items(19)[8..16].to_vec(), "the second chunk, whole");
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
        assert_eq!((again, lost), (vec![1, 2, 3], 0), "the first failure: noted back");
        // the next flush: 1 is delivered, 2 fails again, 3 fails again, 4 fails for the first time
        let (again, lost) = settle_failed(&mut retried, &[1], vec![2, 3, 4]);
        assert_eq!((again, lost), (vec![4], 2), "a second failure is counted and let go");
        assert_eq!(retried.iter().copied().collect::<Vec<_>>(), vec![4], "only the entry awaiting its one retry is remembered");
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
    fn a_note_back_is_counted_when_made_and_when_resent_and_the_difference_is_what_was_never_resent() {
        let post_failing = |fail: &'static [usize]| {
            let calls = Cell::new(0usize);
            move |_b: String| {
                calls.set(calls.get() + 1);
                let n = calls.get();
                async move { if fail.contains(&n) { Err(()) } else { Ok(0) } }
            }
        };
        let add = |sum: &mut Tally, t: Tally| {
            sum.undelivered += t.undelivered;
            sum.retry_failed += t.retry_failed;
            sum.retried += t.retried;
            sum.deferred += t.deferred;
            sum.resent += t.resent;
        };
        let unresent = |sum: &Tally| noted_back_unresent(sum.retried as u64, sum.deferred as u64, sum.resent as u64);
        let mut memory: NotedBack<u32> = NotedBack::new();
        let mut sum = Tally::default();

        // flush 1: 131 entries, the second POST answered 503: eight retried, three deferred, all noted back
        let (back, t) = settle(&mut memory, &run(ship_chunks(items(131), body, post_failing(&[2]), || 0, 1)));
        assert_eq!(t, Tally { retried: 8, deferred: 3, ..Tally::default() });
        assert_eq!(back, [&items(131)[8..16], &items(131)[128..]].concat());
        add(&mut sum, t);
        assert_eq!((sum.undelivered, unresent(&sum)), (0, 11), "eleven note-backs made, none re-sent yet");

        // flush 2 (the same isolate): the eleven ride it; the retried chunk fails AGAIN, the deferred three arrive
        let (back2, t) = settle(&mut memory, &run(ship_chunks(back.clone(), body, post_failing(&[1]), || 0, 1)));
        assert_eq!(t, Tally { undelivered: 8, retry_failed: 8, resent: 11, ..Tally::default() }, "retried and still failed: its own name");
        assert!(back2.is_empty(), "the retry is bounded at one");
        add(&mut sum, t);
        assert_eq!(unresent(&sum), 0, "every note-back was POSTed again: the account closes");
        assert!(memory.retried.is_empty() && memory.deferred.is_empty());

        // a flood deferred twice over is ONE note-back per entry: 300 entries, 128 a flush
        let (back, t) = settle(&mut memory, &run(ship_chunks(items(300), body, post_failing(&[]), || 0, 1)));
        assert_eq!((t.deferred, back.len()), (172, 172));
        add(&mut sum, t);
        let (back, t) = settle(&mut memory, &run(ship_chunks(back, body, post_failing(&[]), || 0, 1)));
        assert_eq!((t.resent, t.deferred, back.len()), (128, 0, 44), "the 44 still waiting are not counted again");
        add(&mut sum, t);
        assert_eq!(unresent(&sum), 44);
        // a deferred entry whose POST then fails: its deferral is re-sent, its failure is a new note-back
        let (back, t) = settle(&mut memory, &run(ship_chunks(back, body, post_failing(&[1]), || 0, 1)));
        assert_eq!(t, Tally { retried: 8, resent: 44, ..Tally::default() });
        add(&mut sum, t);
        assert_eq!((back.len(), unresent(&sum)), (8, 8));

        // THE HOLE: the isolate is evicted holding those eight. Nothing more is ever counted for them:
        // `undelivered` says nothing was lost since the eight of flush 2, and the unresent figure names them.
        drop(memory);
        assert_eq!((sum.undelivered, sum.retry_failed, unresent(&sum)), (8, 8, 8), "a loss `undelivered` cannot see, under its own name");

        // a second isolate's flushes do not disturb the figure (its own note-backs meet its own resends)
        let mut other: NotedBack<u32> = NotedBack::new();
        let (back, t) = settle(&mut other, &run(ship_chunks(items(9), body, post_failing(&[2]), || 0, 1)));
        add(&mut sum, t);
        let (_, t) = settle(&mut other, &run(ship_chunks(back, body, post_failing(&[]), || 0, 1)));
        add(&mut sum, t);
        assert_eq!(unresent(&sum), 8);

        // the memory is bounded: past it a deferred entry is let go and counted undelivered, never an untracked note
        let mut full: NotedBack<u32> = NotedBack::new();
        full.deferred = (1_000_000..1_000_000 + DEFER_MEMORY_MAX as u32).collect();
        let (back, t) = settle(&mut full, &run(ship_chunks(items(131), body, post_failing(&[]), || 0, 1)));
        assert_eq!((back.len(), t), (0, Tally { undelivered: 3, ..Tally::default() }));
        assert_eq!(noted_back_unresent(1, 1, 5), 0, "floored: the counters are bumped one statement at a time");
    }

    /// Lens L4, THE WORST CASE FLUSH MEASURED: 128 pot outpoints (and three more) in one flush. Sixteen
    /// sequential POSTs, never a seventeenth; the largest body and the flush's bytes; and the wall: on a
    /// modelled clock where the app layer takes 2 s per body, the flush stops STARTING POSTs at its budget and
    /// defers the tail (counted) instead of losing it with the task.
    #[test]
    fn the_worst_case_flush_is_bounded_in_posts_bytes_and_wall() {
        let pots: Vec<(String, u32)> = (0..131u32).map(|i| (format!("{i:064x}"), u32::MAX)).collect();
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
        assert_eq!(fast.posts, CHANGE_MAX_CHUNKS, "sixteen POSTs, never a seventeenth");
        assert_eq!((fast.delivered.len(), fast.deferred.len()), (128, 3));
        assert_eq!(most_in_flight.get(), 1, "sequential: one subrequest in flight at a time");
        let largest = *bytes.borrow().iter().max().unwrap();
        let total: usize = bytes.borrow().iter().sum();
        println!("worst case pot flush: {} POSTs, largest body {largest} bytes, {total} bytes in all", fast.posts);
        assert_eq!(largest, 767, "a body of eight outpoints with the widest vout");
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
        println!("at 2 s per POST: {} POSTs in {} ms, {} delivered, {} deferred", slow.posts, clock.get(), slow.delivered.len(), slow.deferred.len());
        assert_eq!(slow.posts, 10, "no POST is started at or past the budget");
        assert_eq!(clock.get(), FLUSH_WALL_BUDGET_MS);
        assert_eq!((slow.delivered.len(), slow.deferred.len()), (80, 51), "the tail is deferred and counted, not lost with the task");
        const { assert!(FLUSH_WALL_BUDGET_MS < 30_000, "inside the wall `wait_until` holds a task for") };
    }

    /// The app layer's `dropped` answer is read (additive: an answer without it, or not JSON, is 0).
    #[test]
    fn the_app_layers_dropped_answer_is_read() {
        assert_eq!(answered_dropped(r#"{"ok":true,"filed":[],"skipped":[],"dropped":3}"#), 3);
        assert_eq!(answered_dropped(r#"{"ok":true,"filed":[],"skipped":[]}"#), 0);
        assert_eq!(answered_dropped("not json"), 0);
    }
}
