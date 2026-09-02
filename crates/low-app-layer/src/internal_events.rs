//! W2-P4 (bsv-low event-driven client, 2026-09-02) — FIRST-PARTY INTERNAL
//! WEBHOOKS + the broadcast fan-out they drive.
//!
//! Our own workers tell the app-layer that something changed (chaintracks:
//! the chain tip); the app-layer turns it into an EVENT the clients hold a
//! socket for — the relay's token-gated `/broadcast` into a `broadcast-*` box
//! every subscriber's own hub delivers (the BoardView's `board-changed`
//! precedent). Bearer-gated (`INTERNAL_TOKEN`): producers are our workers,
//! never end users, and the route is served BEFORE the BRC-103 front door.
//!
//! The tip event replaces the client's `/tip` polls (three components on 60 s
//! timers): one read at mount, then `{kind:'tip', height}` on every block.
use serde_json::{json, Value};
use worker::*;

pub const TIP_ROOM: &str = "broadcast-low-tip";

/// Constant-shape bearer check against the `INTERNAL_TOKEN` secret. An
/// unconfigured deploy refuses everything (never an open webhook).
pub fn internal_bearer_ok(req: &Request, env: &Env) -> bool {
    let Ok(expected) = env.secret("INTERNAL_TOKEN").map(|s| s.to_string()) else {
        return false;
    };
    if expected.trim().is_empty() {
        return false;
    }
    let got = req
        .headers()
        .get("Authorization")
        .ok()
        .flatten()
        .unwrap_or_default();
    got.strip_prefix("Bearer ")
        .map(|t| t.trim() == expected.trim())
        .unwrap_or(false)
}

/// `{ "height": <u64> }` — the only field the tip webhook carries.
pub fn parse_tip_changed(raw: &[u8]) -> Option<u64> {
    let v: Value = serde_json::from_slice(raw).ok()?;
    let h = v.get("height")?.as_u64()?;
    (h > 0).then_some(h)
}

/// The broadcast body clients receive in `broadcast-low-tip`: a SNAPSHOT
/// (the height itself), never a delta.
pub fn tip_event_body(height: u64, at_ms: u64) -> Value {
    json!({ "kind": "tip", "height": height, "at": at_ms })
}

/// Fan one event out through the relay's `/broadcast` (bearer `BROADCAST_TOKEN`).
/// Best-effort and logged; an unconfigured deploy no-ops.
pub async fn push_broadcast(env: &Env, room: &str, body: Value) {
    let (Ok(relay), Ok(token)) = (
        env.var("RELAY_URL").map(|v| v.to_string()),
        env.secret("BROADCAST_TOKEN").map(|v| v.to_string()),
    ) else {
        console_log!("[broadcast] not configured (RELAY_URL / BROADCAST_TOKEN) — {room} event dropped");
        return;
    };
    let payload = json!({ "room": room, "body": body }).to_string();
    let mut init = RequestInit::new();
    init.with_method(Method::Post);
    let headers = Headers::new();
    let _ = headers.set("Authorization", &format!("Bearer {token}"));
    let _ = headers.set("content-type", "application/json");
    init.with_headers(headers);
    init.with_body(Some(payload.into()));
    let Ok(req) = Request::new_with_init(&format!("{}/broadcast", relay.trim_end_matches('/')), &init) else {
        return;
    };
    match Fetch::Request(req).send().await {
        Ok(r) if r.status_code() == 200 => {}
        Ok(r) => console_log!("[broadcast] {room} push HTTP {}", r.status_code()),
        Err(e) => console_log!("[broadcast] {room} push failed: {e}"),
    }
}

/// `POST /internal/tip-changed` (bearer `INTERNAL_TOKEN`, body `{height}`):
/// chaintracks' cron calls it once per synced tip; we broadcast the tip.
pub async fn tip_changed(mut req: Request, env: &Env) -> Result<Response> {
    if !internal_bearer_ok(&req, env) {
        return Response::error("unauthorized", 401);
    }
    let raw = req.bytes().await?;
    let Some(height) = parse_tip_changed(&raw) else {
        return Response::error("body must be {\"height\": <positive integer>}", 400);
    };
    push_broadcast(env, TIP_ROOM, tip_event_body(height, Date::now().as_millis())).await;
    Response::from_json(&json!({ "ok": true, "room": TIP_ROOM, "height": height }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_tip_changed_accepts_a_positive_height_only() {
        assert_eq!(parse_tip_changed(br#"{"height": 965051}"#), Some(965051));
        assert_eq!(parse_tip_changed(br#"{"height": 0}"#), None);
        assert_eq!(parse_tip_changed(br#"{"height": -1}"#), None);
        assert_eq!(parse_tip_changed(br#"{"height": "965051"}"#), None);
        assert_eq!(parse_tip_changed(br#"{}"#), None);
        assert_eq!(parse_tip_changed(b"nope"), None);
    }

    #[test]
    fn tip_event_body_is_a_snapshot_with_the_room_name_pinned() {
        let b = tip_event_body(965051, 1_788_000_000_000);
        assert_eq!(b["kind"], "tip");
        assert_eq!(b["height"], 965051);
        assert_eq!(b["at"], 1_788_000_000_000u64);
        assert_eq!(TIP_ROOM, "broadcast-low-tip");
    }
}
