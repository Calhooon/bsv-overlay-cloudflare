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

/// Storage::put_multiple serializes a STRUCT as an object; a serde_json map becomes a JS Map
/// whose properties the storage API does not store.
#[derive(serde::Serialize)]
struct OwedDeferredArm<'a> {
    #[serde(rename = "owed-identity")]
    identity: &'a str,
    #[serde(rename = "owed-due")]
    due: i64,
    #[serde(rename = "owed-retry")]
    retry: bool,
}

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
}

impl DurableObject for BoardView {
    fn new(state: State, env: Env) -> Self {
        Self {
            state,
            env,
            cache: Rc::new(RefCell::new(HashMap::new())),
            computed: Rc::new(RefCell::new(HashMap::new())),
            refreshing: Rc::new(RefCell::new(HashSet::new())),
        }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        let url = req.url()?;
        // Separate object names from the board/results actors; no cache or polling path reaches this arm.
        if req.path() == "/owed-deferred" {
            let identity = url
                .query_pairs()
                .find(|(k, _)| k == "identity")
                .map(|(_, v)| v.to_string())
                .unwrap_or_default();
            let due = url
                .query_pairs()
                .find(|(k, _)| k == "due")
                .and_then(|(_, v)| v.parse::<i64>().ok())
                .unwrap_or(0);
            if !crate::logic::valid_identity(&identity)
                || due
                    <= (Date::now().as_millis() as i64)
                        .saturating_sub(crate::owed::OWED_RECOMPUTE_RATE_WINDOW_MS)
            {
                return Response::error("invalid owed alarm", 400);
            }
            let storage = self.state.storage();
            let previous = storage.get::<i64>("owed-due").await?;
            let consumed = storage.get::<i64>("owed-consumed-due").await?.unwrap_or(0);
            if due <= consumed
                || previous.is_some_and(|old| due < old)
                || (previous == Some(due)
                    && storage.get::<bool>("owed-started").await?.unwrap_or(false))
            {
                if url.query_pairs().any(|(k, v)| k == "read" && v == "1") {
                    return Response::error("first read reservation already consumed", 503);
                }
                return Response::ok("already run");
            }
            if previous != Some(due) {
                // One actor per identity/isolate: a newer window replaces a carried arm, never stacks it.
                storage.delete_all().await?;
                storage
                    .put("owed-consumed-due", consumed.max(previous.unwrap_or(0)))
                    .await?;
                storage
                    .put_multiple(OwedDeferredArm {
                        identity: &identity,
                        due,
                        retry: false,
                    })
                    .await?;
                storage
                    .set_alarm((due - Date::now().as_millis() as i64).max(0))
                    .await?;
            }
            let retry = storage.get::<bool>("owed-retry").await?.unwrap_or(false);
            if url.query_pairs().any(|(k, v)| k == "read" && v == "1") {
                // The first read consumes the alarm's reservation. Keep the claim until the deadline alarm
                // clears it; repeated faults and a concurrently delivered alarm cannot run it twice.
                if storage.get::<i64>("owed-due").await? != Some(due) {
                    return Response::error("first read reservation already consumed", 503);
                }
                storage
                    .set_alarm((due - Date::now().as_millis() as i64).max(0))
                    .await?;
                storage.put("owed-started", true).await?;
                crate::owed::note_recompute_deferred();
                let db = self.env.d1("OVERLAY_DB")?;
                let (walked, d1) = crate::d1_ledger::scoped(crate::routes::owed_compute_on_read(
                    &self.env,
                    &db,
                    &identity,
                    "read-first",
                    None,
                ))
                .await;
                crate::d1_ledger::record(crate::routes::OWED_RECOMPUTE_LEDGER_KEY, d1);
                match walked {
                    Ok(snapshot) => {
                        // Consuming the alarm's slot must retain its push to the identity's other devices.
                        let env = self.env.clone();
                        let id = identity.clone();
                        let event = crate::owed::owed_changed_event_body(
                            &identity,
                            "read-first",
                            snapshot.0.len(),
                            snapshot.2,
                        );
                        self.state.wait_until(async move {
                            crate::internal_events::first_party_push(&env, &id, event).await;
                        });
                        return Response::from_json(&snapshot);
                    }
                    Err(response) => {
                        crate::owed::note_recompute_deferred_faulted();
                        self.owed_clear_consumed_arm().await?;
                        if let Err(e) = self
                            .owed_rearm_faulted_deferred(&identity, due, retry)
                            .await
                        {
                            crate::owed::note_recompute_arm_failed();
                            return Err(e);
                        }
                        return response;
                    }
                }
            }
            if storage.get_alarm().await?.is_none() {
                storage
                    .set_alarm((due - Date::now().as_millis() as i64).max(0))
                    .await?;
            }
            return Response::ok("armed");
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
    async fn alarm(&self) -> Result<Response> {
        if self.owed_alarm_not_due().await? {
            return Response::ok("owed retry not due");
        }
        let storage = self.state.storage();
        if storage.get::<bool>("owed-started").await?.unwrap_or(false) {
            self.owed_clear_consumed_arm().await?;
            return Response::ok("already run");
        }
        let Some(identity) = storage.get::<String>("owed-identity").await? else {
            storage.delete_all().await?;
            return Response::ok("no owed alarm");
        };
        // Claim durably BEFORE walking: an alarm retry must not buy another recompute past ceiling + one.
        let due = storage.get::<i64>("owed-due").await?.unwrap_or(0);
        let retry = storage.get::<bool>("owed-retry").await?.unwrap_or(false);
        storage.put("owed-started", true).await?;
        let walked = crate::routes::owed_deferred_walk_and_push(&self.env, &identity).await;
        if walked.is_err() {
            crate::owed::note_recompute_deferred_faulted();
        }
        self.owed_clear_consumed_arm().await?;
        if let Err(e) = walked {
            console_warn!("[owed] deferred walk faulted: {e}");
            if let Err(e) = self
                .owed_rearm_faulted_deferred(&identity, due, retry)
                .await
            {
                crate::owed::note_recompute_arm_failed();
                return Err(e);
            }
            return Response::ok(if retry {
                "owed retry abandoned"
            } else {
                "owed retry armed for next window"
            });
        }
        Response::ok("owed refreshed")
    }
}

impl BoardView {
    async fn owed_clear_consumed_arm(&self) -> Result<()> {
        let storage = self.state.storage();
        // Persist the tombstone BEFORE clearing keys, until the old deadline can no longer be accepted.
        let now = Date::now().as_millis() as i64;
        let due = storage.get::<i64>("owed-due").await?.unwrap_or(now);
        let expiry = due
            .max(now)
            .saturating_add(crate::owed::OWED_RECOMPUTE_RATE_WINDOW_MS);
        storage.delete_all().await?;
        // Retain only the consumed deadline so a late request cannot reclaim it; a newer window may arm.
        storage.put("owed-consumed-due", due).await?;
        if let Err(e) = storage.set_alarm(expiry.saturating_sub(now)).await {
            crate::owed::note_recompute_arm_failed();
            return Err(e);
        }
        Ok(())
    }

    async fn owed_alarm_not_due(&self) -> Result<bool> {
        let storage = self.state.storage();
        let due = storage.get::<i64>("owed-due").await?.unwrap_or(0);
        let now = Date::now().as_millis() as i64;
        if due > now {
            // A duplicate delivery of the old alarm must not consume its later-window retry.
            storage.set_alarm(due - now).await?;
            return Ok(true);
        }
        Ok(false)
    }

    async fn owed_rearm_faulted_deferred(
        &self,
        identity: &str,
        due: i64,
        retry: bool,
    ) -> Result<()> {
        if retry {
            crate::owed::note_recompute_deferred_abandoned();
            return Ok(());
        }
        let next = crate::owed::owed_fault_retry_due(due, Date::now().as_millis() as i64);
        let storage = self.state.storage();
        let armed = async {
            storage
                .put_multiple(OwedDeferredArm {
                    identity,
                    due: next,
                    retry: true,
                })
                .await?;
            storage
                .set_alarm((next - Date::now().as_millis() as i64).max(0))
                .await?;
            Ok(())
        }
        .await;
        if armed.is_err() {
            storage.delete_all().await?;
        }
        armed
    }

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
    crate::internal_events::push_broadcast(
        env,
        "broadcast-low-board",
        serde_json::json!({ "kind": "board-changed", "at": Date::now().as_millis() }),
    )
    .await
}

impl BoardView {
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
