//! First-party write-event courier to the app-layer's tracker host.
use serde_json::{json, Value};
use std::{cell::RefCell, collections::BTreeMap};
thread_local! { static EVENTS: RefCell<BTreeMap<(String, String), Value>> = const { RefCell::new(BTreeMap::new()) }; }
pub fn note(txid: &str, kind: &str, body: Option<Value>) {
    EVENTS.with(|events| {
        let mut events = events.borrow_mut();
        if events.len() >= 128 {
            worker::console_warn!("[tx-word] event buffer full for {txid}; lookup fallback owed");
            return;
        }
        events.insert(
            (txid.to_ascii_lowercase(), kind.into()),
            body.unwrap_or(Value::Null),
        );
    });
}
pub fn drain() -> Vec<Value> {
    EVENTS.with(|events| {
        std::mem::take(&mut *events.borrow_mut())
            .into_iter()
            .map(|((txid, kind), body)| json!({"txid":txid,"kind":kind,"body":body}))
            .collect()
    })
}
pub async fn ship(env: &worker::Env, events: Vec<Value>) {
    if events.is_empty() {
        return;
    }
    let Some((url, token)) = crate::change_flush::configured(env) else {
        worker::console_warn!(
            "[tx-word] unconfigured: {} events not delivered",
            events.len()
        );
        return;
    };
    let deadline = worker::Date::now().as_millis() + 20_000;
    for frame in events {
        if worker::Date::now().as_millis() >= deadline {
            worker::console_warn!(
                "[tx-word] event wall: {} needs lookup fallback",
                frame["txid"]
            );
            continue;
        }
        if crate::change_flush::post_body(
            env,
            &url,
            &token,
            "/internal/tx-word",
            "tx-word",
            frame.to_string(),
        )
        .await
        .is_err()
        {
            worker::console_warn!(
                "[tx-word] undelivered {} (lookup fallback owed)",
                frame["txid"]
            );
        }
    }
}
