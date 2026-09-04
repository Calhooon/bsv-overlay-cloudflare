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

/// Ship one notification. Unconfigured (`APP_LAYER_URL` / `INTERNAL_TOKEN`)
/// ⇒ logs and no-ops. Meant to run under `wait_until`.
pub async fn ship(env: Env, outpoints: Vec<(String, u32)>) {
    if outpoints.is_empty() {
        return;
    }
    let (Ok(url), Ok(token)) = (
        env.var("APP_LAYER_URL").map(|v| v.to_string()),
        env.secret("INTERNAL_TOKEN").map(|v| v.to_string()),
    ) else {
        console_log!("[pot-changes] not configured (APP_LAYER_URL / INTERNAL_TOKEN) — {} outpoint(s) not notified", outpoints.len());
        return;
    };
    let body = body_json(&outpoints);
    let mut init = RequestInit::new();
    init.with_method(Method::Post);
    let headers = Headers::new();
    let _ = headers.set("Authorization", &format!("Bearer {}", token.trim()));
    let _ = headers.set("content-type", "application/json");
    init.with_headers(headers);
    init.with_body(Some(body.into()));
    let Ok(req) = Request::new_with_init(
        &format!("{}/internal/pot-changed", url.trim_end_matches('/')),
        &init,
    ) else {
        return;
    };
    // The app-layer is a Worker on this account: the POST rides the
    // APP_LAYER service binding (Cloudflare refuses a plain fetch between two
    // Workers on one zone — 1042 behind a 404 — and every *.workers.dev host
    // of an account is one zone). A deploy without the binding falls back to
    // a public fetch, which is only right for an app-layer on another zone.
    let sent = match env.service("APP_LAYER") {
        Ok(svc) => svc.fetch_request(req).await,
        Err(_) => Fetch::Request(req).send().await,
    };
    match sent {
        Ok(r) if (200..300).contains(&r.status_code()) => {
            console_log!("[pot-changes] notified {} outpoint(s)", outpoints.len())
        }
        Ok(mut r) => {
            let status = r.status_code();
            let body = r.text().await.unwrap_or_default();
            let excerpt: String = body
                .chars()
                .take(200)
                .collect::<String>()
                .replace(['\n', '\r'], " ");
            console_log!("[pot-changes] app-layer HTTP {status} {excerpt}")
        }
        Err(e) => console_log!("[pot-changes] notify failed: {e}"),
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
    if changed.is_empty() {
        return;
    }
    wait_until(Box::pin(ship(env.clone(), changed)));
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

    /// SOURCE PIN (2026-09-04): every detached pot-writing task in routes.rs
    /// ships its own notes. The signers self-heal is the one such task today;
    /// a second `backfill_settle_signers(&storage` under `wait_until` without
    /// a matching `flush_inline` re-opens the stranded-note hole.
    #[test]
    fn every_detached_pot_writing_task_ships_its_own_notes() {
        let routes = include_str!("routes.rs");
        let heals = routes.matches("backfill_settle_signers(&storage").count();
        let inline = routes.matches("pot_changes::flush_inline(").count();
        assert!(
            heals >= 1,
            "the signers self-heal task moved — re-point this pin"
        );
        assert_eq!(
            heals, inline,
            "a detached task writes pot rows without shipping its own notes (see flush_inline)"
        );
    }

    #[test]
    fn body_json_is_the_outpoint_list() {
        let v: serde_json::Value =
            serde_json::from_str(&body_json(&[("aa".into(), 0), ("bb".into(), 2)])).unwrap();
        assert_eq!(v["outpoints"][0]["txid"], "aa");
        assert_eq!(v["outpoints"][1]["vout"], 2);
    }
}
