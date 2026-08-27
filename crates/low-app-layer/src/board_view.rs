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
                        self.cache.borrow_mut().insert(key, (new_body.clone(), now));
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
                }
                crate::routes::json_response_cached(body, status, 5)
            }
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
