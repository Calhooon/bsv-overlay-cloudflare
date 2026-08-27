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
use worker::*;

/// Serve-from-memory freshness window (ms). Below the old worker-Cache 15 s,
/// so the actor strictly improves staleness while removing the herd.
const FRESH_MS: u64 = 10_000;

#[durable_object]
pub struct BoardView {
    _state: State,
    env: Env,
    /// limit-key (0 = default) → (body json, computed-at ms).
    cache: RefCell<HashMap<u32, (String, u64)>>,
    /// limit-keys with a refresh in flight (actor-local single-flight).
    refreshing: RefCell<HashSet<u32>>,
}

impl DurableObject for BoardView {
    fn new(state: State, env: Env) -> Self {
        Self {
            _state: state,
            env,
            cache: RefCell::new(HashMap::new()),
            refreshing: RefCell::new(HashSet::new()),
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
        let key = limit_raw.unwrap_or(0);
        let now = Date::now().as_millis();
        // NOTE: every RefCell borrow below is a statement-temporary — none is
        // held across an await (the actor interleaves at awaits).
        let held = self.cache.borrow().get(&key).cloned();
        match held {
            Some((body, at)) if now.saturating_sub(at) <= FRESH_MS => body_response(body, false),
            Some((body, _)) => {
                if self.refreshing.borrow().contains(&key) {
                    return body_response(body, true);
                }
                self.refreshing.borrow_mut().insert(key);
                let out =
                    crate::routes::compute_leaderboard_body_string(&self.env, limit_raw).await;
                self.refreshing.borrow_mut().remove(&key);
                match out {
                    Ok((200, new_body)) => {
                        let changed = new_body != body;
                        self.cache.borrow_mut().insert(key, (new_body.clone(), now));
                        if changed {
                            self.push_board_changed().await;
                        }
                        body_response(new_body, false)
                    }
                    _ => body_response(body, true),
                }
            }
            None => {
                let (status, body) =
                    crate::routes::compute_leaderboard_body_string(&self.env, limit_raw).await?;
                if status == 200 {
                    self.cache.borrow_mut().insert(key, (body.clone(), now));
                    self.push_board_changed().await;
                }
                crate::routes::json_response_cached(body, status, 5)
            }
        }
    }
}

impl BoardView {
    /// S3 — per-identity `/results` with the same SWR contract as the board
    /// (fresh ≤10 s from memory; stale serves instantly while one arrival
    /// refreshes; failed refresh serves held). Keyed by `after` page.
    async fn serve_results(&self, identity: &str, after: usize) -> Result<Response> {
        let key = 1_000_000u32 + after as u32; // page-keyed, disjoint from board keys
        let now = Date::now().as_millis();
        let held = self.cache.borrow().get(&key).cloned();
        match held {
            Some((body, at)) if now.saturating_sub(at) <= FRESH_MS => body_response(body, false),
            Some((body, _)) => {
                if self.refreshing.borrow().contains(&key) {
                    return body_response(body, true);
                }
                self.refreshing.borrow_mut().insert(key);
                let out =
                    crate::routes::compute_results_body_string(&self.env, identity, after).await;
                self.refreshing.borrow_mut().remove(&key);
                match out {
                    Ok((200, new_body)) => {
                        self.cache.borrow_mut().insert(key, (new_body.clone(), now));
                        body_response(new_body, false)
                    }
                    _ => body_response(body, true),
                }
            }
            None => {
                let (status, body) =
                    crate::routes::compute_results_body_string(&self.env, identity, after).await?;
                if status == 200 {
                    self.cache.borrow_mut().insert(key, (body.clone(), now));
                }
                crate::routes::json_response_cached(body, status, 5)
            }
        }
    }

    /// S3b — tell subscribed clients the board CHANGED (they refetch once,
    /// hitting this actor's warm copy). Fire-and-forget POST to OUR relay's
    /// bearer-gated /broadcast; a lost push costs one safety-poll interval,
    /// never correctness. Paid only by the one refresh that found a change.
    async fn push_board_changed(&self) {
        let (Ok(relay), Ok(token)) = (
            self.env.var("RELAY_URL").map(|v| v.to_string()),
            self.env.secret("BROADCAST_TOKEN").map(|v| v.to_string()),
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
}

fn body_response(body: String, stale: bool) -> Result<Response> {
    let mut resp = crate::routes::json_response_cached(body, 200, 5)?;
    if stale {
        resp.headers_mut().set("X-Board-Stale", "1")?;
    }
    Ok(resp)
}
