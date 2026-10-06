//! W2-P4 (bsv-low event-driven client, 2026-09-02) — POT CHANGE NOTIFICATIONS.
//!
//! The D1 pot storage is the single writer of every pot fact a seat's felt
//! renders (admission, spend pointer, confirmation, verdict, spender facts).
//! Each successful write NOTES its outpoint here (a per-isolate set — no I/O
//! inside storage), and the request / queue / cron context that did the
//! work DRAINS the set once and ships ONE bounded notification to the
//! app-layer (`POST /internal/pot-changed`, bearer `INTERNAL_TOKEN`) through
//! `wait_until`, off the critical path. The app-layer assembles the served
//! `/results` entry for both seats and files it into their `low_events`
//! boxes as an EVENT. Over-notification is harmless (the app-layer re-reads
//! the truth); a lost notification costs nothing money-wise (every money
//! view is still served from the same rows on the next read).
use std::cell::RefCell;
use std::collections::BTreeSet;

use worker::*;

thread_local! {
    static CHANGES: RefCell<BTreeSet<(String, u32)>> = const { RefCell::new(BTreeSet::new()) };
}

/// Record that `(txid, vout)`'s row changed. Cheap, never fails.
pub fn note(txid: &str, vout: u32) {
    let key = (txid.to_ascii_lowercase(), vout);
    CHANGES.with(|c| {
        c.borrow_mut().insert(key);
    });
}

/// Take every noted outpoint (deduped), leaving the set empty.
pub fn drain() -> Vec<(String, u32)> {
    CHANGES.with(|c| std::mem::take(&mut *c.borrow_mut()).into_iter().collect())
}

/// The webhook body: `{"outpoints":[{"txid","vout"},…]}`.
pub fn body_json(outpoints: &[(String, u32)]) -> String {
    let arr: Vec<serde_json::Value> = outpoints
        .iter()
        .map(|(t, v)| serde_json::json!({ "txid": t, "vout": v }))
        .collect();
    serde_json::json!({ "outpoints": arr }).to_string()
}

/// bsv-low #436: the app layer's bound on one `/internal/pot-changed` body (`internal_events::POT_CHANGED_MAX`,
/// pinned equal from the app layer's tests, which link this crate). One flush used to ship EVERY drained outpoint
/// in one POST and the app layer kept the first eight: a tip pass confirming nine or more pot spends in one block
/// lost the ninth onward from the `broadcast-low-pots` push and the durable per-seat filing, with no log line
/// there. The flush is chunked at the bound, so nothing a flush ships is refused.
pub const POT_CHANGED_CHUNK: usize = 8;

/// The most POSTs one flush makes (128 outpoints): the flush runs under `wait_until`, whose wall is finite, and
/// each POST is a subrequest. The remainder is NOT dropped: it is noted back and rides the next flush on this
/// isolate ([`split_flush`]), logged and counted.
pub const POT_CHANGED_MAX_CHUNKS: usize = 16;

/// Outpoints the app layer answered it REFUSED (`dropped` in its answer), or a POST it did not accept.
pub const COUNTER_POT_CHANGED_UNDELIVERED: &str = "pot_changed_undelivered_total";
/// Outpoints past one flush's bound, noted back for the next flush.
pub const COUNTER_POT_CHANGED_DEFERRED: &str = "pot_changed_deferred_total";

/// The bodies one flush POSTs: the outpoints in chunks of [`POT_CHANGED_CHUNK`], never an empty body.
pub fn flush_bodies(outpoints: &[(String, u32)]) -> Vec<String> {
    outpoints.chunks(POT_CHANGED_CHUNK).map(body_json).collect()
}

/// Noted outpoints, `(txid, vout)`.
pub type Outpoints = Vec<(String, u32)>;

/// What this flush ships and what waits for the next one (past `POT_CHANGED_CHUNK * POT_CHANGED_MAX_CHUNKS`).
pub fn split_flush(mut outpoints: Outpoints) -> (Outpoints, Outpoints) {
    let bound = POT_CHANGED_CHUNK * POT_CHANGED_MAX_CHUNKS;
    let deferred = if outpoints.len() > bound {
        outpoints.split_off(bound)
    } else {
        Vec::new()
    };
    (outpoints, deferred)
}

/// `dropped` in the app layer's 2xx answer (additive since bsv-low #436; an older app layer answers none: 0).
pub fn answered_dropped(answer: &str) -> usize {
    serde_json::from_str::<serde_json::Value>(answer)
        .ok()
        .and_then(|v| v.get("dropped").and_then(serde_json::Value::as_u64))
        .map(|n| n as usize)
        .unwrap_or(0)
}

/// POST one chunk. `Ok(dropped)` on a 2xx (what the app layer answered it refused), `Err(())` when the POST was
/// not accepted (logged here).
async fn post_chunk(env: &Env, url: &str, token: &str, chunk: &[(String, u32)]) -> std::result::Result<usize, ()> {
    let mut init = RequestInit::new();
    init.with_method(Method::Post);
    let headers = Headers::new();
    let _ = headers.set("Authorization", &format!("Bearer {}", token.trim()));
    let _ = headers.set("content-type", "application/json");
    init.with_headers(headers);
    init.with_body(Some(body_json(chunk).into()));
    let Ok(req) = Request::new_with_init(
        &format!("{}/internal/pot-changed", url.trim_end_matches('/')),
        &init,
    ) else {
        console_log!("[pot-changes] request build failed: {} outpoint(s) not notified", chunk.len());
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
            let dropped = answered_dropped(&r.text().await.unwrap_or_default());
            console_log!("[pot-changes] notified {} outpoint(s)", chunk.len());
            Ok(dropped)
        }
        Ok(mut r) => {
            let status = r.status_code();
            let body = r.text().await.unwrap_or_default();
            let excerpt: String = body
                .chars()
                .take(200)
                .collect::<String>()
                .replace(['\n', '\r'], " ");
            console_log!("[pot-changes] app-layer HTTP {status} {excerpt}: {} outpoint(s) not notified", chunk.len());
            Err(())
        }
        Err(e) => {
            console_log!("[pot-changes] notify failed: {e}: {} outpoint(s) not notified", chunk.len());
            Err(())
        }
    }
}

/// Ship one flush: the outpoints in POSTs of at most [`POT_CHANGED_CHUNK`] (bsv-low #436), one after another.
/// Unconfigured (`APP_LAYER_URL` / `INTERNAL_TOKEN`) ⇒ logs and no-ops. Meant to run under `wait_until`.
/// Nothing is lost silently: a POST the app layer did not accept and an outpoint it answered it refused are
/// logged and counted (`pot_changed_undelivered_total`); the part of a flood past the flush's bound is noted
/// back for the next flush, logged and counted (`pot_changed_deferred_total`).
pub async fn ship(env: Env, outpoints: Vec<(String, u32)>) {
    if outpoints.is_empty() {
        return;
    }
    let (Ok(url), Ok(token)) = (
        env.var("APP_LAYER_URL").map(|v| v.to_string()),
        env.secret("INTERNAL_TOKEN").map(|v| v.to_string()),
    ) else {
        console_log!("[pot-changes] not configured (APP_LAYER_URL / INTERNAL_TOKEN): {} outpoint(s) not notified", outpoints.len());
        return;
    };
    let (now, deferred) = split_flush(outpoints);
    for (txid, vout) in &deferred {
        note(txid, *vout);
    }
    let mut undelivered = 0usize;
    for chunk in now.chunks(POT_CHANGED_CHUNK) {
        match post_chunk(&env, &url, &token, chunk).await {
            Ok(dropped) => undelivered += dropped,
            Err(()) => undelivered += chunk.len(),
        }
    }
    if undelivered == 0 && deferred.is_empty() {
        return;
    }
    console_log!(
        "[pot-changes] flush of {} outpoint(s): {undelivered} NOT delivered (refused by the app layer or the POST failed), {} deferred to the next flush",
        now.len(),
        deferred.len()
    );
    if let Ok(db) = env.d1("OVERLAY_DB") {
        crate::ops::bump_counter(&db, COUNTER_POT_CHANGED_UNDELIVERED, undelivered as u64).await;
        crate::ops::bump_counter(&db, COUNTER_POT_CHANGED_DEFERRED, deferred.len() as u64).await;
    }
}

/// Drain and ship INSIDE a detached task, awaited there (2026-09-04).
///
/// THE STRANDED-NOTE HOLE (found the same day in a sibling overlay by the
/// wallet terminal): a pot write made from a `wait_until` task that runs
/// AFTER the request's [`flush`] queues its note into this isolate-global set
/// — nothing drains it until the NEXT request on this isolate flushes, and it
/// is lost when the isolate goes first. Every detached task that writes pot
/// rows (the post-submit signers self-heal, `routes.rs`) must end with
/// `flush_inline(env.clone()).await` so its own notes ride its own task.
pub async fn flush_inline(env: Env) {
    let changed = drain();
    // bsv-low #469 (2026-09-19): the hop-marker notes ride every flush the pot notes ride (one set of flush points)
    let hops = crate::hop_changes::drain();
    if !hops.is_empty() {
        crate::hop_changes::ship(env.clone(), hops).await;
    }
    if changed.is_empty() {
        return;
    }
    ship(env, changed).await;
}

/// Drain and ship under the given `wait_until` (a request, queue or cron
/// context). One call per unit of work. NOT sufficient for notes queued by a
/// task that runs after this call — see [`flush_inline`].
pub fn flush<F: FnOnce(std::pin::Pin<Box<dyn std::future::Future<Output = ()>>>)>(
    env: &Env,
    wait_until: F,
) {
    let changed = drain();
    // bsv-low #469 (2026-09-19): the hop-marker notes ride the same flush (one detached task carries both sets)
    let hops = crate::hop_changes::drain();
    if changed.is_empty() && hops.is_empty() {
        return;
    }
    let env2 = env.clone();
    wait_until(Box::pin(async move {
        if !hops.is_empty() {
            crate::hop_changes::ship(env2.clone(), hops).await;
        }
        if !changed.is_empty() {
            ship(env2, changed).await;
        }
    }));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn note_dedupes_and_drain_empties() {
        drain();
        note("AA", 0);
        note("aa", 0);
        note("bb", 1);
        let d = drain();
        assert_eq!(d, vec![("aa".to_string(), 0), ("bb".to_string(), 1)]);
        assert!(drain().is_empty());
    }

    /// SOURCE PIN (2026-09-04; admit-fast step 4, 2026-09-15): every detached
    /// pot-writing task in routes.rs ships its own notes. Three such tasks:
    /// the signers self-heal (`backfill_settle_signers(&storage`), and — since
    /// the `network_seen` latch notes the pot rows it witnesses — the two
    /// background latchers, the ungated corroboration closure (`seen_env`)
    /// and the #413 dual push (`dual_env`). A fourth detached writer without a
    /// matching `flush_inline` re-opens the stranded-note hole.
    #[test]
    fn every_detached_pot_writing_task_ships_its_own_notes() {
        let routes = include_str!("routes.rs");
        // Production code only: the route tests carry these needles as literals.
        let routes = &routes[..routes.find("#[cfg(test)]").unwrap_or(routes.len())];
        let heals = routes.matches("backfill_settle_signers(&storage").count();
        let latchers = routes.matches("let seen_env = env.clone();").count()
            + routes.matches("let dual_env = env.clone();").count();
        let inline = routes.matches("pot_changes::flush_inline(").count();
        // bsv-low loop 18 (2026-09-21, #513): the write-side guard re-evicts INSIDE the request after the engine
        // write and answers 422 — an early return that skips the route's end flush, so it ships the eviction's
        // notes inline itself (counted by its counter's name; a comment cannot satisfy a needle).
        let guard = routes.matches("COUNTER_ADMIT_FAST_REEVICTED_AFTER_WRITE").count();
        // round 2 of the same gate (NEW-4): the Rejected arm re-runs an open eviction (an incomplete pass
        // converges on the network's second refusal) and ships its notes inline the same way
        let rejected_rerun = routes.matches("refused again under an open eviction").count();
        assert!(
            heals >= 1,
            "the signers self-heal task moved — re-point this pin"
        );
        assert_eq!(latchers, 2, "the two background latchers capture the env");
        assert_eq!(guard, 1, "the write-side guard re-evicts once, after the write");
        assert_eq!(rejected_rerun, 1, "the Rejected arm re-runs an open eviction once");
        assert_eq!(
            heals + latchers + guard + rejected_rerun,
            inline,
            "a detached task (or the write-side guard's early return) writes pot rows without shipping its own notes (see flush_inline)"
        );
    }

    /// bsv-low #436: a flush is chunked at the app layer's bound, every outpoint in exactly one body, no empty
    /// body; a flood past the flush's own bound is split off for the next flush, never dropped.
    #[test]
    fn a_flush_is_chunked_at_the_bound_and_a_flood_is_deferred_never_dropped() {
        let ops = |n: usize| -> Vec<(String, u32)> { (0..n).map(|i| (format!("{i:064x}"), 0)).collect() };
        assert!(flush_bodies(&[]).is_empty(), "nothing noted: no POST");
        for n in [1, 8, 9, 16, 17, 128] {
            let bodies = flush_bodies(&ops(n));
            assert_eq!(bodies.len(), n.div_ceil(POT_CHANGED_CHUNK), "{n}");
            let mut seen = Vec::new();
            for b in &bodies {
                let v: serde_json::Value = serde_json::from_str(b).unwrap();
                let arr = v["outpoints"].as_array().unwrap();
                assert!(!arr.is_empty() && arr.len() <= POT_CHANGED_CHUNK, "{n}");
                seen.extend(arr.iter().map(|o| (o["txid"].as_str().unwrap().to_string(), o["vout"].as_u64().unwrap() as u32)));
            }
            assert_eq!(seen, ops(n), "every outpoint in exactly one body, in order ({n})");
        }
        let bound = POT_CHANGED_CHUNK * POT_CHANGED_MAX_CHUNKS;
        let (now, deferred) = split_flush(ops(bound));
        assert_eq!((now.len(), deferred.len()), (bound, 0));
        let (now, deferred) = split_flush(ops(bound + 3));
        assert_eq!((now.len(), deferred.len()), (bound, 3));
        assert_eq!([now, deferred].concat(), ops(bound + 3), "shipped now or deferred: nothing else");
    }

    /// The app layer's `dropped` answer is read (additive: an answer without it, or not JSON, is 0).
    #[test]
    fn the_app_layers_dropped_answer_is_read() {
        assert_eq!(answered_dropped(r#"{"ok":true,"filed":[],"skipped":[],"dropped":3}"#), 3);
        assert_eq!(answered_dropped(r#"{"ok":true,"filed":[],"skipped":[]}"#), 0);
        assert_eq!(answered_dropped("not json"), 0);
    }

    /// SOURCE PIN: `ship` POSTs chunk by chunk, notes the deferred part back, and counts what was not
    /// delivered. To red: POST `body_json(&outpoints)` whole again.
    #[test]
    fn ship_posts_in_chunks_and_counts_what_it_does_not_deliver() {
        let code_only = |s: &str| s.lines().map(|l| l.split("//").next().unwrap_or("")).collect::<Vec<_>>().join("\n");
        let squash = |s: &str| s.split_whitespace().collect::<String>();
        let src = include_str!("pot_changes.rs");
        let src = &src[..src.find("#[cfg(test)]").unwrap()];
        let start = src.find("pub async fn ship(").expect("ship");
        let ship = squash(&code_only(&src[start..start + src[start..].find("\npub async fn flush_inline(").expect("the next fn")]));
        assert!(ship.contains(&squash("for chunk in now.chunks(POT_CHANGED_CHUNK) { match post_chunk(&env, &url, &token, chunk).await {")));
        assert!(ship.contains(&squash("for (txid, vout) in &deferred { note(txid, *vout); }")));
        assert!(ship.contains(&squash("bump_counter(&db, COUNTER_POT_CHANGED_UNDELIVERED, undelivered as u64)")));
        assert!(ship.contains(&squash("bump_counter(&db, COUNTER_POT_CHANGED_DEFERRED, deferred.len() as u64)")));
        assert!(!ship.contains("body_json("), "ship never builds one whole body");
    }

    #[test]
    fn body_json_is_the_outpoint_list() {
        let v: serde_json::Value =
            serde_json::from_str(&body_json(&[("aa".into(), 0), ("bb".into(), 2)])).unwrap();
        assert_eq!(v["outpoints"][0]["txid"], "aa");
        assert_eq!(v["outpoints"][1]["vout"], 2);
    }
}
