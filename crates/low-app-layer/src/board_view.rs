//! S3a (ARCHITECTURE v2, 2026-08-27) — the BoardView ACTOR.
//!
//! One warm copy of the leaderboard serves every poller from memory:
//! single-flight is a CONSEQUENCE of the actor model rather than a lock
//! (the D1 read herd — 32 pollers × per-request recompute — dies here).
//! D1 stays the durable truth; the actor computes through the SAME
//! pipeline the route's direct fallback uses
//! (`routes::compute_leaderboard_body_string` — spine fast path, zero-lie
//! fallback, counting bars, trust model unchanged).
//!
//! Serving contract:
//! - FRESH (≤ 10 s): serve from memory, zero D1 touches.
//! - STALE: the FIRST arrival refreshes inline (one recompute per staleness
//!   window GLOBALLY); every concurrent arrival serves the stale copy
//!   instantly with `X-Board-Stale: 1`. A FAILED refresh also serves the
//!   stale copy — while the actor holds ANY answer it never 503s (the
//!   D1-storm posture, measured 2026-08-26).
//! - COLD: compute inline once, hold, serve.
//!
//! S3b (next): WebSocket hibernation on this same actor — clients SUBSCRIBE
//! and the polling loops this serves are deleted outright.
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use worker::*;

/// Serve-from-memory freshness window (ms). Below the old worker-Cache 15 s,
/// so the actor strictly improves staleness while removing the herd.
const FRESH_MS: u64 = 10_000;

/// bsv-low #642: the board's keyset-cursor pages are cached under keys from
/// this base, one per held `(cursor, limit)`, at most [`CURSOR_SLOTS`] of them
/// (a cursor is a client's string, so the set it names is not bounded by the
/// route; the slots are).
const CURSOR_KEY_BASE: u32 = 2_000_000;
const CURSOR_SLOTS: usize = 32;

#[durable_object]
pub struct BoardView {
    state: State,
    env: Env,
    /// limit-key (0 = default) → (body json, computed-at ms).
    /// `Rc` so DETACHED compute tasks (spawn_local) can write it after the
    /// request that started them is gone — see `spawn_compute`.
    cache: Rc<RefCell<HashMap<u32, (String, u64)>>>,
    /// bsv-low #499: the D1 rows ledger of each key's last compute (served as the `d1view` segment).
    computed: Rc<RefCell<HashMap<u32, crate::d1_ledger::Tally>>>,
    /// limit-keys with a refresh in flight (actor-local single-flight).
    /// Cleared INSIDE the detached task, so an aborted caller can never wedge
    /// a key in "refreshing" forever (the 2026-08-27 layer-9 bug).
    refreshing: Rc<RefCell<HashSet<u32>>>,
    /// #642: slot `i` holds the `(cursor, limit)` cached under
    /// `CURSOR_KEY_BASE + i`.
    cursor_slots: RefCell<Vec<(String, u32)>>,
    /// The alarm and scheduled drain share one actor and never select the same dirty generation concurrently.
    owed_turn: futures_util::lock::Mutex<()>,
}

impl DurableObject for BoardView {
    fn new(state: State, env: Env) -> Self {
        Self {
            state,
            env,
            cache: Rc::new(RefCell::new(HashMap::new())),
            computed: Rc::new(RefCell::new(HashMap::new())),
            refreshing: Rc::new(RefCell::new(HashSet::new())),
            cursor_slots: RefCell::new(Vec::new()),
            owed_turn: futures_util::lock::Mutex::new(()),
        }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        self.install_courier_sink();
        let out = self.serve_fetch(req).await;
        self.flush_counters_if_due();
        out
    }
    /// bsv-low #628: the owed belt's tick runs in this actor's isolate (#893 section 5; it was the deferred owed
    /// walk's), so its counts (the belt's couriers, its faults) are flushed here, under the census's due rule, never
    /// only by a fetch isolate that never saw them.
    async fn alarm(&self) -> Result<Response> {
        self.install_courier_sink();
        let out = self.serve_alarm().await;
        self.flush_counters_if_due();
        out
    }
}

impl BoardView {
    /// bsv-low #731: the owed belt asks couriers from this actor's isolate, which may never have run the front
    /// door's entry, so the actor installs the courier sink itself.
    fn install_courier_sink(&self) {
        crate::routes::set_courier_sink(self.env.var("COURIER_SINK").ok().map(|v| v.to_string()));
    }

    fn flush_counters_if_due(&self) {
        let pending = crate::courier::take_pending_if_due(Date::now().as_millis() as f64);
        if !pending.is_empty() {
            let db = self.env.d1("OVERLAY_DB").ok();
            self.state
                .wait_until(async move { crate::courier::flush(db, pending).await });
        }
        // bsv-low #635: the actor's D1 statements (its walks) in the cost window, written when due. Its fetches are
        // Durable Object requests, not the Worker's, so none is counted as a request.
        let now = Date::now().as_millis() as f64;
        if let (Some(window), Ok(db)) = (
            crate::d1_ledger::take_cost_if_due(now),
            self.env.d1("OVERLAY_DB"),
        ) {
            self.state
                .wait_until(async move { crate::d1_ledger::flush_cost(&db, window, now).await });
        }
    }

    async fn serve_fetch(&self, req: Request) -> Result<Response> {
        let url = req.url()?;
        if req.path() == "/owed-drain" {
            let _turn = self.owed_turn.lock().await;
            return match crate::owed_belt::drain_env(&self.env, true).await {
                Ok(report) => Response::from_json(&serde_json::json!({
                    "subjects": report.subjects,
                    "faults": report.faults.len(),
                })),
                Err(error) => Response::error(error.to_string(), 503),
            };
        }
        // bsv-low #893 section 5: the belt's singleton actor (`owed_belt::OWED_BELT_ACTOR`). The scheduled pass asks
        // it on every cron (the watchdog): an alarm already set is kept, a lost one is set to fire at once.
        if req.path() == "/owed-belt-arm" {
            let storage = self.state.storage();
            storage.put("owed-belt", true).await?;
            if storage.get_alarm().await?.is_none() {
                storage.set_alarm(0).await?;
                return Response::ok("armed");
            }
            return Response::ok("held");
        }
        // The deferred owed walk is retired (#893: the read is one lookup, the belt asks the chain); a stale caller
        // is answered, never served the board.
        if req.path() == "/owed-deferred" || req.path() == "/owed-deferred-note" {
            return Response::error("the deferred owed walk is retired", 410);
        }
        // S3 second iteration: ONE actor class, two view kinds. `/results`
        // instances are NAMED `results:<identity>` (the route resolves the
        // identity at the #318 seam and the name pins the scope); the board
        // instance stays `board:v1`. Same SWR/serve semantics for both.
        if req.path() == "/results" {
            let identity = url
                .query_pairs()
                .find(|(k, _)| k == "identity")
                .map(|(_, v)| v.to_string())
                .unwrap_or_default();
            let after = url
                .query_pairs()
                .find(|(k, _)| k == "after")
                .and_then(|(_, v)| v.parse::<usize>().ok())
                .unwrap_or(0);
            return self.serve_results(&identity, after).await;
        }
        let limit_raw = url
            .query_pairs()
            .find(|(k, _)| k == "limit")
            .and_then(|(_, v)| v.parse::<u32>().ok());
        // #403: the owners-page cursor. Cache key = (after, limit) folded
        // below the results-page key space (1_000_000+): after ≤ 999 pages
        // × 1000 + limit ≤ 999.
        let after_raw = url
            .query_pairs()
            .find(|(k, _)| k == "after")
            .and_then(|(_, v)| v.parse::<u32>().ok());
        // #642: the keyset cursor (validated by the route).
        let cursor_raw = url
            .query_pairs()
            .find(|(k, _)| k == "cursor")
            .map(|(_, v)| v.into_owned());
        let key = match &cursor_raw {
            None => after_raw.unwrap_or(0).min(999) * 1000 + limit_raw.unwrap_or(0).min(999),
            Some(c) => match self.cursor_key(c, limit_raw.unwrap_or(0)) {
                Some(key) => key,
                // Every slot has a compute in flight: serve this page uncached.
                None => {
                    let (status, body) = crate::routes::compute_leaderboard_body_string(
                        &self.env,
                        limit_raw,
                        after_raw,
                        Some(c),
                    )
                    .await?;
                    return crate::routes::json_response_cached(body, status, 5);
                }
            },
        };
        let env = self.env.clone();
        self.serve_view(key, move || {
            let env = env.clone();
            let cursor = cursor_raw.clone();
            async move {
                crate::routes::compute_leaderboard_body_string(
                    &env,
                    limit_raw,
                    after_raw,
                    cursor.as_deref(),
                )
                .await
            }
        })
        .await
    }
    /// bsv-low #893 section 5: this actor's alarm is the belt's tick. The next alarm is set BEFORE the tick runs, so
    /// a tick that faults or throws still leaves the belt armed; the scheduled pass's arm is the watchdog behind it.
    /// Any other actor's alarm (a deferred walk armed before the retirement) clears its storage and ends.
    async fn serve_alarm(&self) -> Result<Response> {
        let storage = self.state.storage();
        if !storage.get::<bool>("owed-belt").await?.unwrap_or(false) {
            storage.delete_all().await?;
            return Response::ok("retired deferred owed alarm");
        }
        storage
            .set_alarm(crate::owed_belt::OWED_BELT_TICK_MS)
            .await?;
        crate::owed_belt::alarm_env(&self.env, &self.owed_turn).await
    }
}

impl BoardView {
    /// S3 — per-identity `/results` with the same SWR contract as the board
    /// (fresh ≤10 s from memory; stale serves instantly while one arrival
    /// refreshes; failed refresh serves held). Keyed by `after` page.
    async fn serve_results(&self, identity: &str, after: usize) -> Result<Response> {
        let key = 1_000_000u32 + after as u32; // page-keyed, disjoint from board keys
        let env = self.env.clone();
        let identity = identity.to_string();
        self.serve_view(key, move || {
            let env = env.clone();
            let identity = identity.clone();
            async move { crate::routes::compute_results_body_string(&env, &identity, after).await }
        })
        .await
    }

    /// The ONE serving shape for both view kinds (2026-08-27, layer 9 of the
    /// enforced-board onion):
    ///
    /// - FRESH: serve from memory.
    /// - STALE: serve the held copy INSTANTLY, always — and kick a DETACHED
    ///   refresh if none is in flight. The old inline refresh died with an
    ///   aborting caller (the browser's fetch timeout cancels the DO request
    ///   future at its next await), which both abandoned the compute AND
    ///   leaked the `refreshing` mark, freezing the key stale forever.
    /// - COLD: kick the same detached compute and POLL-AWAIT the cache (100 ms
    ///   steps, ≤25 s). The detached task outlives any caller (spawn_local is
    ///   not tied to the request future), so an impatient client aborting its
    ///   first fetch still WARMS the cache for its own retry — under load the
    ///   cold window is paid once per key, never per caller.
    async fn serve_view<F, Fut>(&self, key: u32, compute: F) -> Result<Response>
    where
        F: Fn() -> Fut + 'static,
        Fut: std::future::Future<Output = Result<(u16, String)>> + 'static,
    {
        let now = Date::now().as_millis();
        // NOTE: every RefCell borrow is a statement-temporary — none is held
        // across an await (the actor interleaves at awaits).
        let held = self.cache.borrow().get(&key).cloned();
        match held {
            Some((body, at)) if now.saturating_sub(at) <= FRESH_MS => {
                body_response(body, false, self.last_compute(key))
            }
            Some((body, _)) => {
                self.spawn_compute(key, compute);
                body_response(body, true, self.last_compute(key))
            }
            None => {
                self.spawn_compute(key, compute);
                for _ in 0..250u32 {
                    Delay::from(std::time::Duration::from_millis(100)).await;
                    let hit = self.cache.borrow().get(&key).cloned();
                    if let Some((body, _)) = hit {
                        return body_response(body, false, self.last_compute(key));
                    }
                    // Compute finished without caching (non-200): stop waiting.
                    if !self.refreshing.borrow().contains(&key) {
                        break;
                    }
                }
                crate::routes::json_response_cached(
                    "{\"error\":\"view warming\"}".to_string(),
                    503,
                    0,
                )
            }
        }
    }

    /// Start ONE detached compute for `key` (no-op when one is in flight).
    /// The task owns Rc clones of the actor state: it caches the body, emits
    /// the S3b change push (board keys only), and ALWAYS clears the
    /// in-flight mark — caller lifetimes are irrelevant to all three.
    fn spawn_compute<F, Fut>(&self, key: u32, compute: F)
    where
        F: Fn() -> Fut + 'static,
        Fut: std::future::Future<Output = Result<(u16, String)>> + 'static,
    {
        if !self.refreshing.borrow_mut().insert(key) {
            return;
        }
        let cache = Rc::clone(&self.cache);
        let computed = Rc::clone(&self.computed);
        let refreshing = Rc::clone(&self.refreshing);
        let env = self.env.clone();
        wasm_bindgen_futures::spawn_local(async move {
            // bsv-low #499: the compute is detached from every request, so it runs under its own D1 ledger
            let (out, d1) = crate::d1_ledger::scoped(compute()).await;
            computed.borrow_mut().insert(key, d1);
            if let Ok((200, new_body)) = out {
                let prev = cache.borrow().get(&key).map(|(b, _)| b.clone());
                let changed = prev.as_deref() != Some(new_body.as_str());
                cache
                    .borrow_mut()
                    .insert(key, (new_body, Date::now().as_millis()));
                // Board keys live below 1_000_000 and from CURSOR_KEY_BASE
                // (results pages between): only board changes fan out the S3b
                // push.
                if changed && !(1_000_000..CURSOR_KEY_BASE).contains(&key) {
                    push_board_changed(&env).await;
                }
            }
            refreshing.borrow_mut().remove(&key);
        });
    }
}

/// S3b — tell subscribed clients the board CHANGED (they refetch once,
/// hitting this actor's warm copy). Fire-and-forget POST to OUR relay's
/// bearer-gated /broadcast; a lost push costs one safety-poll interval,
/// never correctness. Paid only by the one refresh that found a change.
/// Free function so the DETACHED compute task can call it after its
/// spawning request is gone.
async fn push_board_changed(env: &Env) {
    crate::internal_events::push_broadcast(
        env,
        "broadcast-low-board",
        serde_json::json!({ "kind": "board-changed", "at": Date::now().as_millis() }),
    )
    .await;
}

impl BoardView {
    /// #642: the cache key of a cursor page. A held `(cursor, limit)` keeps its
    /// slot; a new one takes a free slot, or the slot whose copy is oldest among
    /// those with no compute in flight (its copy is dropped). `None` when every
    /// slot has a compute in flight.
    fn cursor_key(&self, cursor: &str, limit: u32) -> Option<u32> {
        let mut slots = self.cursor_slots.borrow_mut();
        if let Some(i) = slots.iter().position(|(c, l)| c == cursor && *l == limit) {
            return Some(CURSOR_KEY_BASE + i as u32);
        }
        if slots.len() < CURSOR_SLOTS {
            slots.push((cursor.to_string(), limit));
            return Some(CURSOR_KEY_BASE + (slots.len() - 1) as u32);
        }
        let refreshing = self.refreshing.borrow();
        let cache = self.cache.borrow();
        let i = (0..slots.len())
            .filter(|i| !refreshing.contains(&(CURSOR_KEY_BASE + *i as u32)))
            .min_by_key(|i| {
                cache
                    .get(&(CURSOR_KEY_BASE + *i as u32))
                    .map_or(0, |(_, at)| *at)
            })?;
        drop((refreshing, cache));
        let key = CURSOR_KEY_BASE + i as u32;
        self.cache.borrow_mut().remove(&key);
        self.computed.borrow_mut().remove(&key);
        slots[i] = (cursor.to_string(), limit);
        Some(key)
    }

    fn last_compute(&self, key: u32) -> Option<crate::d1_ledger::Tally> {
        self.computed.borrow().get(&key).copied()
    }
}

/// The held copy, with the D1 figures of the compute that produced it (`d1view`; bsv-low #499): a served copy
/// reads no row, and the route records the compute's figures under its `:view` key.
fn body_response(
    body: String,
    stale: bool,
    d1: Option<crate::d1_ledger::Tally>,
) -> Result<Response> {
    let mut resp = crate::routes::json_response_cached(body, 200, 5)?;
    if stale {
        resp.headers_mut().set("X-Board-Stale", "1")?;
    }
    if let Some(t) = d1 {
        resp.headers_mut().set(
            "Server-Timing",
            &crate::d1_ledger::named_segment("d1view", t),
        )?;
    }
    Ok(resp)
}
