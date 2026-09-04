//! W2-P6 (bsv-low event-driven client, 2026-09-03) — LOBBY CHANGE NOTIFICATIONS.
//!
//! The `tm_low` / `ls_low` storage is the single writer of every lobby advert
//! the Lobby page lists (a TABLE record admitted, a TABLE record evicted by
//! its spend or by the advert-lifecycle reaps). Each successful write NOTES
//! its outpoint + kind here (a per-isolate set — no I/O inside storage), and
//! the request / queue / cron context that did the work DRAINS the set once
//! and ships ONE bounded notification to the app-layer
//! (`POST /internal/lobby-changed`, bearer `INTERNAL_TOKEN`, through the
//! `APP_LAYER` service binding) under `wait_until`, off the critical path.
//! The app-layer fans a `lobby` event into `broadcast-low-lobby`; every open
//! Lobby refetches ONCE (the 60 s list poll is gone). Over-notification is
//! harmless; a lost notification costs one refetch on the next event.
use std::cell::RefCell;
use std::collections::BTreeSet;
use worker::*;

thread_local! {
    static CHANGES: RefCell<BTreeSet<(String, u32, &'static str)>> = const { RefCell::new(BTreeSet::new()) };
}

/// A TABLE advert was admitted (or re-admitted) at `(txid, vout)`.
pub fn note_admitted(txid: &str, vout: u32) {
    note(txid, vout, "admitted");
}

/// A TABLE advert at `(txid, vout)` was evicted (spent, reaped, or refused).
pub fn note_evicted(txid: &str, vout: u32) {
    note(txid, vout, "evicted");
}

fn note(txid: &str, vout: u32, kind: &'static str) {
    let key = (txid.to_ascii_lowercase(), vout, kind);
    CHANGES.with(|c| {
        c.borrow_mut().insert(key);
    });
}

/// Take every noted change (deduped), leaving the set empty.
pub fn drain() -> Vec<(String, u32, &'static str)> {
    CHANGES.with(|c| std::mem::take(&mut *c.borrow_mut()).into_iter().collect())
}

/// The webhook body: `{"changes":[{"txid","vout","kind"},…]}`.
pub fn body_json(changes: &[(String, u32, &'static str)]) -> String {
    let arr: Vec<serde_json::Value> = changes
        .iter()
        .map(|(t, v, k)| serde_json::json!({ "txid": t, "vout": v, "kind": k }))
        .collect();
    serde_json::json!({ "changes": arr }).to_string()
}

/// Ship one notification through the APP_LAYER service binding (a plain
/// fetch between two Workers on one zone is refused by Cloudflare — 1042
/// behind a 404). Unconfigured ⇒ logs and no-ops. Runs under `wait_until`.
pub async fn ship(env: Env, changes: Vec<(String, u32, &'static str)>) {
    if changes.is_empty() {
        return;
    }
    let (Ok(url), Ok(token)) = (
        env.var("APP_LAYER_URL").map(|v| v.to_string()),
        env.secret("INTERNAL_TOKEN").map(|v| v.to_string()),
    ) else {
        console_log!("[lobby-changes] not configured (APP_LAYER_URL / INTERNAL_TOKEN) — {} change(s) not notified", changes.len());
        return;
    };
    let body = body_json(&changes);
    let mut init = RequestInit::new();
    init.with_method(Method::Post);
    let headers = Headers::new();
    let _ = headers.set("Authorization", &format!("Bearer {}", token.trim()));
    let _ = headers.set("content-type", "application/json");
    init.with_headers(headers);
    init.with_body(Some(body.into()));
    let Ok(req) = Request::new_with_init(
        &format!("{}/internal/lobby-changed", url.trim_end_matches('/')),
        &init,
    ) else {
        return;
    };
    let sent = match env.service("APP_LAYER") {
        Ok(svc) => svc.fetch_request(req).await,
        Err(_) => Fetch::Request(req).send().await,
    };
    match sent {
        Ok(r) if (200..300).contains(&r.status_code()) => {
            console_log!("[lobby-changes] notified {} change(s)", changes.len())
        }
        Ok(mut r) => {
            let status = r.status_code();
            let body = r.text().await.unwrap_or_default();
            let excerpt: String = body
                .chars()
                .take(200)
                .collect::<String>()
                .replace(['\n', '\r'], " ");
            console_log!("[lobby-changes] app-layer HTTP {status} {excerpt}")
        }
        Err(e) => console_log!("[lobby-changes] notify failed: {e}"),
    }
}

/// Drain and ship under the given `wait_until`. One call per unit of work.
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
    fn note_dedupes_by_outpoint_and_kind_and_drain_empties() {
        drain();
        note_admitted("AA", 0);
        note_admitted("aa", 0);
        note_evicted("aa", 0);
        note_admitted("bb", 1);
        let got = drain();
        assert_eq!(got.len(), 3);
        assert!(got.contains(&("aa".into(), 0, "admitted")));
        assert!(got.contains(&("aa".into(), 0, "evicted")));
        assert!(drain().is_empty());
    }

    #[test]
    fn body_json_is_the_changes_shape() {
        let b = body_json(&[
            ("ab".repeat(32), 0, "admitted"),
            ("cd".repeat(32), 2, "evicted"),
        ]);
        let v: serde_json::Value = serde_json::from_str(&b).unwrap();
        assert_eq!(v["changes"].as_array().unwrap().len(), 2);
        assert_eq!(v["changes"][1]["kind"], "evicted");
        assert_eq!(v["changes"][1]["vout"], 2);
    }
}
