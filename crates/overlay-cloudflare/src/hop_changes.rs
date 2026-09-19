//! bsv-low #469 (2026-09-19) — HOP MARKER CHANGE NOTIFICATIONS.
//!
//! The owed list is COMPUTED ON WRITE (design §2.2). Every pot write already
//! notes its outpoint for the app layer (`pot_changes`); a HOP marker admission
//! (`tm_hopparty`: the hop tx's own OP_RETURN output naming the seat, its
//! opponent and the hop outpoint) noted nothing, so an identity whose hop just
//! funded — the very seat a device switch or a strand leaves with money in a
//! hop and no pot — kept its owed rows until the read cadence re-derived them
//! (up to 5 min; the device-switch unit's hop branch read "nothing owed" for
//! its whole 90 s window while the marker sat verified in the index). Each
//! admitted marker now notes BOTH identities it names; the drain rides the
//! same flush points as the pot notes (`pot_changes::flush` ships both sets)
//! and the app layer marks those identities stale and re-derives their rows
//! (`POST /internal/hop-changed`, bearer `INTERNAL_TOKEN`, through the
//! `APP_LAYER` service binding). Over-notification is harmless (a recompute
//! is idempotent); a lost notification costs the cadence, never money.
use std::cell::RefCell;
use std::collections::BTreeSet;

use worker::*;

thread_local! {
    static CHANGES: RefCell<BTreeSet<String>> = const { RefCell::new(BTreeSet::new()) };
}

/// Record that a hop marker naming `identity` (and `opponent`) was admitted. Cheap, never fails.
pub fn note(identity: &str, opponent: &str) {
    CHANGES.with(|c| {
        let mut set = c.borrow_mut();
        for id in [identity, opponent] {
            let id = id.trim().to_ascii_lowercase();
            if id.len() == 66 && id.bytes().all(|b| b.is_ascii_hexdigit()) {
                set.insert(id);
            }
        }
    });
}

/// Take every noted identity (deduped, sorted), leaving the set empty.
pub fn drain() -> Vec<String> {
    CHANGES.with(|c| std::mem::take(&mut *c.borrow_mut()).into_iter().collect())
}

/// The webhook body: `{"identities":["02…","03…"]}`.
pub fn body_json(identities: &[String]) -> String {
    serde_json::json!({ "identities": identities }).to_string()
}

/// Ship one notification. Unconfigured (`APP_LAYER_URL` / `INTERNAL_TOKEN`) ⇒ logs and no-ops. Meant to run under
/// `wait_until` (the pot shipper's twin; the same service binding, the same bearer).
pub async fn ship(env: Env, identities: Vec<String>) {
    if identities.is_empty() {
        return;
    }
    let (Ok(url), Ok(token)) = (
        env.var("APP_LAYER_URL").map(|v| v.to_string()),
        env.secret("INTERNAL_TOKEN").map(|v| v.to_string()),
    ) else {
        console_log!("[hop-changes] not configured (APP_LAYER_URL / INTERNAL_TOKEN) — {} identity(ies) not notified", identities.len());
        return;
    };
    let body = body_json(&identities);
    let mut init = RequestInit::new();
    init.with_method(Method::Post);
    let headers = Headers::new();
    let _ = headers.set("Authorization", &format!("Bearer {}", token.trim()));
    let _ = headers.set("content-type", "application/json");
    init.with_headers(headers);
    init.with_body(Some(body.into()));
    let Ok(req) = Request::new_with_init(&format!("{}/internal/hop-changed", url.trim_end_matches('/')), &init) else {
        return;
    };
    let sent = match env.service("APP_LAYER") {
        Ok(svc) => svc.fetch_request(req).await,
        Err(_) => Fetch::Request(req).send().await,
    };
    match sent {
        Ok(r) if (200..300).contains(&r.status_code()) => console_log!("[hop-changes] notified {} identity(ies)", identities.len()),
        Ok(mut r) => {
            let status = r.status_code();
            let body = r.text().await.unwrap_or_default();
            let excerpt: String = body.chars().take(200).collect::<String>().replace(['\n', '\r'], " ");
            console_log!("[hop-changes] app-layer HTTP {status} {excerpt}")
        }
        Err(e) => console_log!("[hop-changes] notify failed: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn note_keeps_well_formed_identities_once_and_drain_empties() {
        drain();
        let a = format!("02{}", "aa".repeat(32));
        let b = format!("03{}", "bb".repeat(32));
        note(&a.to_ascii_uppercase(), &b);
        note(&b, "not-an-identity");
        let d = drain();
        assert_eq!(d, vec![a.clone(), b.clone()]);
        assert!(drain().is_empty());
        assert_eq!(body_json(&d), format!("{{\"identities\":[\"{a}\",\"{b}\"]}}"));
    }
}
