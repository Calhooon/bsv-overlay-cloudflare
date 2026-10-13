//! Bounded first-party write events; one resend on the next foreground flush.
use serde_json::{json, Value};
use std::{cell::RefCell, collections::BTreeMap};
thread_local! {
    static EVENTS: RefCell<BTreeMap<(String,String),Value>> = const {RefCell::new(BTreeMap::new())};
    static COUNTS: RefCell<BTreeMap<String,u64>> = const {RefCell::new(BTreeMap::new())};
    static PENDING: RefCell<BTreeMap<String,u64>> = const {RefCell::new(BTreeMap::new())};
}
fn source(frame: &Value) -> &'static str {
    match frame["kind"].as_str() {
        Some("proof") => "index",
        Some("submit") => "door",
        _ => "arcade",
    }
}
fn count(frame: &Value, outcome: &str) {
    let name = format!("tracker_events_{}_{outcome}_total", source(frame));
    COUNTS.with(|c| *c.borrow_mut().entry(name.clone()).or_default() += 1);
    PENDING.with(|c| *c.borrow_mut().entry(name).or_default() += 1);
}
fn enqueue(frame: Value) {
    EVENTS.with(|events| {
        let mut events = events.borrow_mut();
        let key = (
            frame["txid"]
                .as_str()
                .unwrap_or_default()
                .to_ascii_lowercase(),
            frame["kind"].as_str().unwrap_or_default().to_string(),
        );
        if events.len() >= 128 && !events.contains_key(&key) {
            count(&frame, "dropped");
            return;
        }
        events.entry(key).or_insert(frame);
    });
}
pub fn note(txid: &str, kind: &str, body: Option<Value>) {
    enqueue(
        json!({"txid":txid.to_ascii_lowercase(),"kind":kind,"body":body.unwrap_or(Value::Null)}),
    );
}
/// Submit hook admission boundary, shared by the route and native pin.
pub fn note_submit(txid: &str, gated: bool, admitted_any: bool) {
    if gated && admitted_any {
        note(txid, "submit", None);
    }
}

/// The public txid bearer is a trigger, not authority for a word row.
pub async fn note_held(
    db: Option<&worker::D1Database>,
    txid: &str,
    kind: &str,
    body: Option<Value>,
) {
    #[derive(serde::Deserialize)]
    struct Held {
        held: f64,
    }
    let Some(db) = db else {
        return;
    };
    let held =
        crate::d1::Query::new("SELECT 1 AS held FROM transactions WHERE txid = lower(?) LIMIT 1")
            .bind(txid)
            .fetch_optional::<Held>(db)
            .await;
    if held.is_ok_and(|row| row.is_some_and(|r| r.held == 1.0)) {
        note(txid, kind, body);
    }
}

pub fn drain() -> Vec<Value> {
    EVENTS.with(|e| std::mem::take(&mut *e.borrow_mut()).into_values().collect())
}
fn failed(mut frame: Value) {
    if frame["retry"] == true {
        count(&frame, "dropped");
    } else {
        frame["retry"] = json!(true);
        enqueue(frame);
    }
}
/// The production POST boundary, also used by the refusal/recovery pin.
pub async fn deliver<F, Fut>(frame: Value, post: F)
where
    F: FnOnce(String) -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    match post(frame.to_string()).await {
        Ok(()) => count(&frame, "shipped"),
        Err(_) => failed(frame),
    }
}
pub fn health_json() -> Value {
    COUNTS.with(|c| json!(*c.borrow()))
}
pub async fn ship(env: &worker::Env, events: Vec<Value>) {
    if events.is_empty() {
        return;
    }
    let configured = crate::change_flush::configured(env);
    let deadline = worker::Date::now().as_millis() + 20_000;
    for frame in events {
        if worker::Date::now().as_millis() >= deadline {
            count(&frame, "walled");
            failed(frame);
            continue;
        }
        let Some((url, token)) = &configured else {
            count(&frame, "dropped");
            continue;
        };
        deliver(frame, |body| async {
            crate::change_flush::post_body(env, url, token, "/internal/tx-word", "tx-word", body)
                .await
                .map(|_| ())
                .map_err(|_| "undelivered".to_string())
        })
        .await;
    }
    let pending = PENDING.with(|p| std::mem::take(&mut *p.borrow_mut()));
    if let Ok(db) = env.d1("OVERLAY_DB") {
        for (name, n) in pending {
            crate::ops::bump_counter(&db, &name, n).await;
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn ns3_m3_refused_frame_is_shipped_on_next_flush() {
        drain();
        note(&"01".repeat(32), "proof", Some(json!({"merklePath":"00"})));
        let first = drain().pop().unwrap();
        deliver(first, |_| async {
            Err("fake APP_LAYER refuses once".into())
        })
        .await;
        let retry = drain();
        assert_eq!(retry.len(), 1);
        assert_eq!(retry[0]["body"]["merklePath"], "00");
        deliver(retry.into_iter().next().unwrap(), |_| async { Ok(()) }).await;
        assert!(drain().is_empty());
        assert_eq!(health_json()["tracker_events_index_shipped_total"], 1);
    }
    #[test]
    fn ns3d2_note3_submit_route_passes_mode_and_real_steak_admission() {
        let routes = include_str!("routes.rs");
        let at = routes.find("crate::tracker_events::note_submit(").unwrap();
        let call = &routes[at..at + routes[at..].find("\n    }").unwrap()];
        assert!(
            call.contains("gated_subject.is_some()"),
            "submit route replaced the gated mode with a constant"
        );
        assert!(
            call.contains("steak.values().any(|a|"),
            "submit route replaced admission with a constant"
        );
        for field in [
            "!a.outputs_to_admit.is_empty()",
            "!a.coins_to_retain.is_empty()",
            "a.coins_removed",
            ".is_some_and(|coins| !coins.is_empty())",
        ] {
            assert!(call.contains(field), "submit admission omits {field}");
        }
    }
    #[test]
    fn ns3d_m3_ungated_stranger_submit_leaves_no_word_event() {
        drain();
        // historical-tx-no-spv under the lenient window, even if its marker was admitted.
        note_submit(&"01".repeat(32), false, true);
        assert!(
            drain().is_empty(),
            "stranger historical-tx-no-spv wrote a word event"
        );
        note_submit(&"02".repeat(32), true, false);
        assert!(
            drain().is_empty(),
            "admitted-nothing submit wrote a word event"
        );
        note_submit(&"03".repeat(32), true, true);
        assert_eq!(drain().len(), 1);
    }
}
