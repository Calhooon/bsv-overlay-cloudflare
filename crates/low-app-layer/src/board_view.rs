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

#[durable_object]
pub struct BoardView {
    _state: State,
    env: Env,
    /// limit-key (0 = default) → (body json, computed-at ms).
    /// `Rc` so DETACHED compute tasks (spawn_local) can write it after the
    /// request that started them is gone — see `spawn_compute`.
    cache: Rc<RefCell<HashMap<u32, (String, u64)>>>,
    /// limit-keys with a refresh in flight (actor-local single-flight).
    /// Cleared INSIDE the detached task, so an aborted caller can never wedge
    /// a key in "refreshing" forever (the 2026-08-27 layer-9 bug).
    refreshing: Rc<RefCell<HashSet<u32>>>,
}

impl DurableObject for BoardView {
    fn new(state: State, env: Env) -> Self {
        Self {
            _state: state,
            env,
            cache: Rc::new(RefCell::new(HashMap::new())),
            refreshing: Rc::new(RefCell::new(HashSet::new())),
        }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        let url = req.url()?;
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
        let key = after_raw.unwrap_or(0).min(999) * 1000 + limit_raw.unwrap_or(0).min(999);
        let env = self.env.clone();
        self.serve_view(key, move || {
            let env = env.clone();
            async move {
                crate::routes::compute_leaderboard_body_string(&env, limit_raw, after_raw).await
            }
        })
        .await
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
            Some((body, at)) if now.saturating_sub(at) <= FRESH_MS => body_response(body, false),
            Some((body, _)) => {
                self.spawn_compute(key, compute);
                body_response(body, true)
            }
            None => {
                self.spawn_compute(key, compute);
                for _ in 0..250u32 {
                    Delay::from(std::time::Duration::from_millis(100)).await;
                    let hit = self.cache.borrow().get(&key).cloned();
                    if let Some((body, _)) = hit {
                        return body_response(body, false);
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
        let refreshing = Rc::clone(&self.refreshing);
        let env = self.env.clone();
        wasm_bindgen_futures::spawn_local(async move {
            let out = compute().await;
            if let Ok((200, new_body)) = out {
                let prev = cache.borrow().get(&key).map(|(b, _)| b.clone());
                let changed = prev.as_deref() != Some(new_body.as_str());
                cache
                    .borrow_mut()
                    .insert(key, (new_body, Date::now().as_millis()));
                // Board keys live below 1_000_000 (results pages above) — only
                // board changes fan out the S3b push.
                if changed && key < 1_000_000 {
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
    let (Ok(relay), Ok(token)) = (
        env.var("RELAY_URL").map(|v| v.to_string()),
        env.secret("BROADCAST_TOKEN").map(|v| v.to_string()),
    ) else {
        return; // unconfigured deploy — S4 wires prod
    };
    let body = serde_json::json!({
        "room": "broadcast-low-board",
        "body": { "kind": "board-changed", "at": Date::now().as_millis() },
    })
    .to_string();
    let mut init = RequestInit::new();
    init.with_method(Method::Post);
    let headers = Headers::new();
    let _ = headers.set("Authorization", &format!("Bearer {token}"));
    let _ = headers.set("content-type", "application/json");
    init.with_headers(headers);
    init.with_body(Some(body.into()));
    let Ok(req) = Request::new_with_init(&format!("{relay}/broadcast"), &init) else {
        return;
    };
    match Fetch::Request(req).send().await {
        Ok(r) if r.status_code() == 200 => {}
        Ok(r) => console_log!("[board-view] broadcast push HTTP {}", r.status_code()),
        Err(e) => console_log!("[board-view] broadcast push failed: {e}"),
    }
}

fn body_response(body: String, stale: bool) -> Result<Response> {
    let mut resp = crate::routes::json_response_cached(body, 200, 5)?;
    if stale {
        resp.headers_mut().set("X-Board-Stale", "1")?;
    }
    Ok(resp)
}
