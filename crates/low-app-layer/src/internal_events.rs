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

/// The first 200 chars of a refusal body on one line — enough to tell WHO
/// answered (an edge 404 page vs the relay's own refusal), never a secret.
pub fn excerpt(body: &str) -> String {
    body.chars()
        .take(200)
        .collect::<String>()
        .replace(['\n', '\r'], " ")
}

/// The first entry of a served `/results` body (`results::results_body`):
/// the array is keyed `results` — the pot-changed handler read `entries`
/// for a night and filed nothing (every real pot answered "serializer
/// produced no entry"). Pinned by a test against the serializer itself.
pub fn first_served_result(served: &Value) -> Option<Value> {
    served
        .get("results")
        .and_then(Value::as_array)
        .and_then(|a| a.first())
        .cloned()
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

/// One bearer-gated JSON POST to the relay at `path`. Rides the `RELAY`
/// SERVICE BINDING when the deploy declares one: the relay is a Worker on
/// this account, and Cloudflare refuses a plain fetch between two Workers on
/// one zone (error 1042 behind a 404; every `*.workers.dev` host of an
/// account is ONE zone — proven on beta 2026-09-03, the tip broadcast's
/// `[broadcast] … HTTP 404 (server=cloudflare) error code: 1042`). Without a
/// binding it is a public fetch, which is only right for a relay on another
/// zone.
async fn relay_post(
    env: &Env,
    relay: &str,
    path: &str,
    token: &str,
    payload: String,
) -> Result<Response> {
    let mut init = RequestInit::new();
    init.with_method(Method::Post);
    let headers = Headers::new();
    headers.set("Authorization", &format!("Bearer {token}"))?;
    headers.set("content-type", "application/json")?;
    init.with_headers(headers);
    init.with_body(Some(payload.into()));
    let req = Request::new_with_init(&format!("{}{path}", relay.trim_end_matches('/')), &init)?;
    match env.service("RELAY") {
        Ok(svc) => svc.fetch_request(req).await,
        Err(_) => Fetch::Request(req).send().await,
    }
}

/// Fan one event out through the relay's `/broadcast` (bearer `BROADCAST_TOKEN`).
/// Best-effort and logged; an unconfigured deploy no-ops.
pub async fn push_broadcast(env: &Env, room: &str, body: Value) {
    let (Ok(relay), Ok(token)) = (
        env.var("RELAY_URL").map(|v| v.to_string()),
        env.secret("BROADCAST_TOKEN").map(|v| v.to_string()),
    ) else {
        console_log!(
            "[broadcast] not configured (RELAY_URL / BROADCAST_TOKEN) — {room} event dropped"
        );
        return;
    };
    let payload = json!({ "room": room, "body": body }).to_string();
    match relay_post(env, &relay, "/broadcast", &token, payload).await {
        Ok(r) if r.status_code() == 200 => {}
        Ok(mut r) => {
            let status = r.status_code();
            let server = r.headers().get("server").ok().flatten().unwrap_or_default();
            let body = r.text().await.unwrap_or_default();
            console_log!(
                "[broadcast] {room} push HTTP {status} (server={server}) {}",
                excerpt(&body)
            );
        }
        Err(e) => console_log!("[broadcast] {room} push failed: {e}"),
    }
}

/// The compressed pubkey hex the relay stores as `sender` for our pushes —
/// this worker's BRC-103 identity (`SERVER_PRIVATE_KEY`).
pub fn sender_pubkey_hex(server_private_key_hex: &str) -> Option<String> {
    let sk = bsv_rs::primitives::ec::PrivateKey::from_hex(server_private_key_hex.trim()).ok()?;
    let hex = sk.public_key().to_hex();
    (hex.len() == 66).then_some(hex.to_ascii_lowercase())
}

/// File one DURABLE event into ONE seat's `low_events` box through the
/// relay's first-party `POST /push` (bearer `BROADCAST_TOKEN`; stored,
/// live-bridged, acknowledged by the client, replayed on reload). Best-effort.
pub async fn first_party_push(env: &Env, recipient: &str, body: Value) {
    let (Ok(relay), Ok(token), Ok(sk)) = (
        env.var("RELAY_URL").map(|v| v.to_string()),
        env.secret("BROADCAST_TOKEN").map(|v| v.to_string()),
        env.secret("SERVER_PRIVATE_KEY").map(|v| v.to_string()),
    ) else {
        console_log!("[push] not configured (RELAY_URL / BROADCAST_TOKEN / SERVER_PRIVATE_KEY) — event dropped");
        return;
    };
    let Some(sender) = sender_pubkey_hex(&sk) else {
        console_log!("[push] SERVER_PRIVATE_KEY does not derive a pubkey — event dropped");
        return;
    };
    let payload = json!({
        "sender": sender,
        "recipient": recipient.to_ascii_lowercase(),
        "messageBox": "low_events",
        "body": body,
    })
    .to_string();
    match relay_post(env, &relay, "/push", &token, payload).await {
        Ok(r) if (200..300).contains(&r.status_code()) => {}
        Ok(mut r) => {
            let status = r.status_code();
            let server = r.headers().get("server").ok().flatten().unwrap_or_default();
            let body = r.text().await.unwrap_or_default();
            console_log!(
                "[push] → {}… HTTP {status} (server={server}) {}",
                &recipient[..12.min(recipient.len())],
                excerpt(&body)
            );
        }
        Err(e) => console_log!(
            "[push] → {}… failed: {e}",
            &recipient[..12.min(recipient.len())]
        ),
    }
}

/// `{"outpoints":[{"txid","vout"},…]}` — the pot-changed webhook body. Capped
/// (a flood is an operator problem, never a fan-out storm).
pub const POT_CHANGED_MAX: usize = 8;

pub fn parse_pot_changed(raw: &[u8]) -> Vec<(String, u32)> {
    let Ok(v) = serde_json::from_slice::<Value>(raw) else {
        return Vec::new();
    };
    let Some(arr) = v.get("outpoints").and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut out: Vec<(String, u32)> = Vec::new();
    for o in arr {
        let txid = o
            .get("txid")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_ascii_lowercase();
        let vout = o.get("vout").and_then(Value::as_u64);
        if txid.len() != 64 || !txid.bytes().all(|b| b.is_ascii_hexdigit()) {
            continue;
        }
        let Some(vout) = vout.and_then(|v| u32::try_from(v).ok()) else {
            continue;
        };
        if !out.iter().any(|(t, v)| t == &txid && *v == vout) {
            out.push((txid, vout));
        }
        if out.len() >= POT_CHANGED_MAX {
            break;
        }
    }
    out
}

/// The `pot` event body: the seat's exact served `/results` entry, wrapped
/// with the routing keys (a SNAPSHOT — the client parses `entry` with the
/// same parser it uses for `/results`).
pub fn pot_event_body(txid: &str, vout: u32, entry: Value, at_ms: u64) -> Value {
    json!({
        "v": 1,
        "kind": "pot",
        "potOutpoint": { "txid": txid, "vout": vout },
        "at": at_ms,
        "entry": entry,
    })
}

pub const LOBBY_ROOM: &str = "broadcast-low-lobby";

/// `{"changes":[{"txid","vout","kind"},…]}` — the lobby-changed webhook body
/// (the overlay's TABLE-advert storage notes admissions and evictions).
/// Validated + deduped + capped; the client only ever REFETCHES on the event.
pub fn parse_lobby_changed(raw: &[u8]) -> Vec<(String, u32, String)> {
    let Ok(v) = serde_json::from_slice::<Value>(raw) else {
        return Vec::new();
    };
    let Some(arr) = v.get("changes").and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut out: Vec<(String, u32, String)> = Vec::new();
    for o in arr {
        let txid = o
            .get("txid")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_ascii_lowercase();
        let vout = o
            .get("vout")
            .and_then(Value::as_u64)
            .and_then(|v| u32::try_from(v).ok());
        let kind = o.get("kind").and_then(Value::as_str).unwrap_or("");
        if txid.len() != 64 || !txid.bytes().all(|b| b.is_ascii_hexdigit()) {
            continue;
        }
        let (Some(vout), true) = (vout, matches!(kind, "admitted" | "evicted")) else {
            continue;
        };
        if !out
            .iter()
            .any(|(t, v, k)| t == &txid && *v == vout && k == kind)
        {
            out.push((txid, vout, kind.to_string()));
        }
        if out.len() >= POT_CHANGED_MAX {
            break;
        }
    }
    out
}

/// The broadcast body clients receive in `broadcast-low-lobby`: a SIGNAL to
/// refetch the open-tables list once (never the list itself — the served
/// list is the truth, this is a carrier), plus what moved, for a log line.
pub fn lobby_event_body(changes: &[(String, u32, String)], at_ms: u64) -> Value {
    let arr: Vec<Value> = changes
        .iter()
        .map(|(t, v, k)| json!({ "txid": t, "vout": v, "kind": k }))
        .collect();
    json!({ "kind": "lobby", "at": at_ms, "changes": arr })
}

/// `POST /internal/lobby-changed` (bearer `INTERNAL_TOKEN`): the overlay's
/// advert storage changed ⇒ fan a `lobby` event into `broadcast-low-lobby`.
pub async fn lobby_changed(mut req: Request, env: &Env) -> Result<Response> {
    if !internal_bearer_ok(&req, env) {
        return Response::error("unauthorized", 401);
    }
    let raw = req.bytes().await?;
    let changes = parse_lobby_changed(&raw);
    if changes.is_empty() {
        return Response::error(
            "body must be {\"changes\":[{\"txid\",\"vout\",\"kind\"}]}",
            400,
        );
    }
    push_broadcast(
        env,
        LOBBY_ROOM,
        lobby_event_body(&changes, Date::now().as_millis()),
    )
    .await;
    Response::from_json(&json!({ "ok": true, "room": LOBBY_ROOM, "changes": changes.len() }))
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
    push_broadcast(
        env,
        TIP_ROOM,
        tip_event_body(height, Date::now().as_millis()),
    )
    .await;
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
    fn sender_pubkey_hex_derives_compressed_g_for_key_one() {
        let one = "0000000000000000000000000000000000000000000000000000000000000001";
        assert_eq!(
            sender_pubkey_hex(one).as_deref(),
            Some("0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798")
        );
        assert!(sender_pubkey_hex("zz").is_none());
    }

    #[test]
    fn first_served_result_reads_the_key_the_results_serializer_writes() {
        // An empty page still carries the array under the SAME key: the two
        // functions agree on the name, or this fails before a deploy does.
        let served: Value =
            serde_json::from_str(&crate::results::results_body("02aa", &[], false, 0)).unwrap();
        assert!(
            served.get("results").map(Value::is_array).unwrap_or(false),
            "results_body must serve `results`"
        );
        assert!(first_served_result(&served).is_none());
        let one: Value =
            serde_json::json!({ "results": [{ "potTxid": "ab" }], "truncated": false });
        assert_eq!(first_served_result(&one).unwrap()["potTxid"], "ab");
    }

    #[test]
    fn parse_lobby_changed_validates_dedupes_and_keeps_the_kind() {
        let t = "ab".repeat(32);
        let raw = format!(
            r#"{{"changes":[{{"txid":"{t}","vout":0,"kind":"admitted"}},{{"txid":"{T}","vout":0,"kind":"admitted"}},{{"txid":"{t}","vout":0,"kind":"evicted"}},{{"txid":"zz","vout":0,"kind":"admitted"}},{{"txid":"{t}","vout":1,"kind":"nope"}}]}}"#,
            T = t.to_ascii_uppercase()
        );
        let got = parse_lobby_changed(raw.as_bytes());
        assert_eq!(got.len(), 2);
        assert_eq!(got[0], (t.clone(), 0, "admitted".to_string()));
        assert_eq!(got[1], (t, 0, "evicted".to_string()));
        assert!(parse_lobby_changed(b"{}").is_empty());
        let body = lobby_event_body(&got, 7);
        assert_eq!(body["kind"], "lobby");
        assert_eq!(body["changes"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn parse_pot_changed_validates_dedupes_and_caps() {
        let t = "ab".repeat(32);
        let raw = format!(
            r#"{{"outpoints":[{{"txid":"{t}","vout":0}},{{"txid":"{T}","vout":0}},{{"txid":"zz","vout":0}},{{"txid":"{t}","vout":"1"}},{{"txid":"{t}","vout":1}}]}}"#,
            T = t.to_ascii_uppercase()
        );
        assert_eq!(
            parse_pot_changed(raw.as_bytes()),
            vec![(t.clone(), 0), (t.clone(), 1)]
        );
        assert!(parse_pot_changed(b"nope").is_empty());
        assert!(parse_pot_changed(br#"{"outpoints":"x"}"#).is_empty());
        let many: Vec<String> = (0..20)
            .map(|i| format!(r#"{{"txid":"{}","vout":{i}}}"#, "cd".repeat(32)))
            .collect();
        let raw = format!(r#"{{"outpoints":[{}]}}"#, many.join(","));
        assert_eq!(parse_pot_changed(raw.as_bytes()).len(), POT_CHANGED_MAX);
    }

    #[test]
    fn pot_event_body_wraps_the_served_entry_as_a_snapshot() {
        let b = pot_event_body("ab", 0, json!({ "outcome": "won" }), 5);
        assert_eq!(b["v"], 1);
        assert_eq!(b["kind"], "pot");
        assert_eq!(b["potOutpoint"]["txid"], "ab");
        assert_eq!(b["entry"]["outcome"], "won");
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
