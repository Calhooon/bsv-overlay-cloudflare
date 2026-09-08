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

/// bsv-low M19 R2 round 2 (review H3): the header HASH chaintracks sends
/// beside the height (`{"height": n, "hash": "<64 hex>"}`, since it announces
/// on ANY tip change); lower-cased; absent or malformed → `None` (an older
/// announcer: the forward reads the header itself).
pub fn parse_tip_changed_hash(raw: &[u8]) -> Option<String> {
    let v: Value = serde_json::from_slice(raw).ok()?;
    let h = v.get("hash")?.as_str()?.trim();
    (h.len() == 64 && h.bytes().all(|b| b.is_ascii_hexdigit())).then(|| h.to_ascii_lowercase())
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

/// bsv-low loop 10 D2 (2026-09-08, the pair-10 finding): the room EVERY
/// pot-outpoint change is announced in BEFORE any seat attribution. The
/// durable per-seat `pot` event needs each seat's own verified marker, which a
/// seat blocked at funding has never published (its felt never learned the
/// pot funded), and the `board-changed` push only follows a refresh some
/// client asked for: a quiet stack never pushed it. This one carries the
/// outpoint alone (no entry, no identities); a seat holding that JOIN reads
/// the NETWORK on it, bounded, and decides there.
pub const POTS_ROOM: &str = "broadcast-low-pots";

/// The `pot-changed` broadcast body: the outpoint and the time, nothing else
/// (the served entry rides the per-seat durable event once attribution exists).
pub fn pot_changed_event_body(txid: &str, vout: u32, at_ms: u64) -> Value {
    json!({
        "v": 1,
        "kind": "pot-changed",
        "potOutpoint": { "txid": txid, "vout": vout },
        "at": at_ms,
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
/// The overlay's block-event pass body — `{"height": n}` (bsv-low loop 6);
/// bsv-low M19 R2 (2026-09-08): `{"height": n, "hash": "<64 hex>"}` when the
/// header hash is in hand — the overlay's reorg detector compares it with
/// the hash it acted on at that height. Absent hash ⇒ the height-only body
/// (the pass still runs; nothing is compared).
pub fn overlay_tip_body(height: u64) -> String {
    json!({ "height": height }).to_string()
}

/// The hash-bearing body (see [`overlay_tip_body`]).
pub fn overlay_tip_body_with_hash(height: u64, hash: &str) -> String {
    json!({ "height": height, "hash": hash.to_ascii_lowercase() }).to_string()
}

/// The two header facts the app-layer reads from chaintracks (bsv-low M19
/// R2): the block hash (forwarded with the tip so the overlay can detect a
/// reorg) and the merkle root (the `/beef` read-side guard's canon).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderLite {
    pub hash: String,
    pub merkle_root: String,
}

/// bsv-low M19 R2: the header chaintracks holds at `height`, read through
/// the CHAINTRACKS service binding (`GET /findHeaderHexForHeight?height=N` →
/// `{"status":"success","value":{"hash": …, "merkleRoot": …, "height": N}}`).
/// Best-effort: any fault is `None`, logged, never a blocked caller.
pub async fn chaintracks_header(env: &Env, height: u64) -> Option<HeaderLite> {
    let svc = match env.service("CHAINTRACKS") {
        Ok(svc) => svc,
        Err(e) => {
            console_log!("[header] CHAINTRACKS binding unavailable ({e}) — no header for {height}");
            return None;
        }
    };
    let mut init = RequestInit::new();
    init.with_method(Method::Get);
    let headers = Headers::new();
    let _ = headers.set("Accept", "application/json");
    init.with_headers(headers);
    let url = format!("https://chaintracks/findHeaderHexForHeight?height={height}");
    let mut resp = match svc.fetch(url, Some(init)).await {
        Ok(r) => r,
        Err(e) => {
            console_log!("[header] chaintracks fetch failed ({e}) — no header for {height}");
            return None;
        }
    };
    if !(200..300).contains(&resp.status_code()) {
        console_log!("[header] chaintracks HTTP {} — no header for {height}", resp.status_code());
        return None;
    }
    let frame: serde_json::Value = match resp.json().await {
        Ok(v) => v,
        Err(e) => {
            console_log!("[header] chaintracks header not JSON ({e}) — no header for {height}");
            return None;
        }
    };
    let header = parse_header(&frame, height);
    if header.is_none() {
        console_log!("[header] chaintracks frame carried no usable header for {height}: {frame}");
    }
    header
}

/// The block hash at `height` (see [`chaintracks_header`]).
pub async fn chaintracks_block_hash(env: &Env, height: u64) -> Option<String> {
    chaintracks_header(env, height).await.map(|h| h.hash)
}

/// PURE: the header out of a chaintracks `findHeaderHexForHeight` frame,
/// only when the frame is a success naming THIS height with 64-hex hash and
/// merkle root (both lower-cased).
pub fn parse_header(frame: &serde_json::Value, height: u64) -> Option<HeaderLite> {
    if frame.get("status")?.as_str()? != "success" {
        return None;
    }
    let value = frame.get("value")?;
    if value.get("height")?.as_u64()? != height {
        return None;
    }
    let hex64 = |key: &str| -> Option<String> {
        let v = value.get(key)?.as_str()?.trim();
        (v.len() == 64 && v.bytes().all(|b| b.is_ascii_hexdigit())).then(|| v.to_ascii_lowercase())
    };
    Some(HeaderLite { hash: hex64("hash")?, merkle_root: hex64("merkleRoot")? })
}

/// PURE: the block hash alone (see [`parse_header`]).
pub fn parse_header_hash(frame: &serde_json::Value, height: u64) -> Option<String> {
    parse_header(frame, height).map(|h| h.hash)
}


/// bsv-low loop 6 (2026-09-07): the tip is ALSO the overlay's cue to confirm
/// its spent-but-unconfirmed pot rows (`POST /internal/tip-changed`, bearer
/// `INTERNAL_TOKEN` — the same shared first-party secret the overlay uses to
/// call us). Through the OVERLAY service binding (a plain fetch between two
/// Workers on one zone is refused, 1042); best-effort and logged; an
/// unconfigured deploy no-ops. Meant to run under `wait_until`.
pub async fn forward_tip_to_overlay(env: Env, height: u64, announced_hash: Option<String>) {
    let (Ok(url), Ok(token)) = (
        env.var("OVERLAY_URL").map(|v| v.to_string()),
        env.secret("INTERNAL_TOKEN").map(|v| v.to_string()),
    ) else {
        console_log!("[tip] overlay block-event pass not configured (OVERLAY_URL / INTERNAL_TOKEN) — height {height} not forwarded");
        return;
    };
    let mut init = RequestInit::new();
    init.with_method(Method::Post);
    let headers = Headers::new();
    if headers.set("Authorization", &format!("Bearer {token}")).is_err()
        || headers.set("content-type", "application/json").is_err()
    {
        console_log!("[tip] overlay block-event pass: header build failed — height {height} not forwarded");
        return;
    }
    init.with_headers(headers);
    // bsv-low M19 R2: carry the header hash so the overlay can detect a reorg
    // (a hash change at a height it already acted on). Round 2: the hash the
    // announcer sent rides through as-is; only a hash-less announce (an older
    // chaintracks) costs a header read; best-effort either way.
    let hash = match announced_hash {
        Some(h) => Some(h),
        None => chaintracks_block_hash(&env, height).await,
    };
    let body = match hash {
        Some(hash) => overlay_tip_body_with_hash(height, &hash),
        None => overlay_tip_body(height),
    };
    init.with_body(Some(body.into()));
    let req = match Request::new_with_init(&format!("{}/internal/tip-changed", url.trim_end_matches('/')), &init) {
        Ok(r) => r,
        Err(e) => {
            console_log!("[tip] overlay block-event pass: request build failed ({e}) — height {height} not forwarded");
            return;
        }
    };
    let res = match env.service("OVERLAY") {
        Ok(svc) => svc.fetch_request(req).await,
        Err(_) => Fetch::Request(req).send().await,
    };
    match res {
        Ok(r) => console_log!("[tip] overlay block-event pass for {height}: HTTP {}", r.status_code()),
        Err(e) => console_log!("[tip] overlay block-event pass for {height} failed: {e}"),
    }
}

pub async fn tip_changed(mut req: Request, env: &Env, ctx: &Context) -> Result<Response> {
    if !internal_bearer_ok(&req, env) {
        return Response::error("unauthorized", 401);
    }
    let raw = req.bytes().await?;
    let Some(height) = parse_tip_changed(&raw) else {
        return Response::error("body must be {\"height\": <positive integer>}", 400);
    };
    let announced_hash = parse_tip_changed_hash(&raw);
    // the `/beef` read-side guard's present-height latch (bsv-low M19 R2)
    crate::beef_guard::latch_present_tip(height);
    push_broadcast(
        env,
        TIP_ROOM,
        tip_event_body(height, Date::now().as_millis()),
    )
    .await;
    // the overlay's block-event pass, off the critical path (the webhook answers now)
    let env2 = env.clone();
    ctx.wait_until(async move { forward_tip_to_overlay(env2, height, announced_hash).await });
    Response::from_json(&json!({ "ok": true, "room": TIP_ROOM, "height": height }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlay_tip_body_is_the_height_object() {
        assert_eq!(overlay_tip_body(965702), r#"{"height":965702}"#);
    }

    #[test]
    fn the_announcers_hash_is_a_lowercased_64_hex_or_nothing() {
        let h = "00000000000000001DE5AA96BAA3566CE66E4941F8295CC44CC85FC75949DB4D";
        assert_eq!(
            parse_tip_changed_hash(format!(r#"{{"height": 965771, "hash": "{h}"}}"#).as_bytes()).as_deref(),
            Some(h.to_ascii_lowercase().as_str())
        );
        assert_eq!(parse_tip_changed_hash(br#"{"height": 965771}"#), None, "an older announcer: the forward reads the header");
        assert_eq!(parse_tip_changed_hash(br#"{"height": 965771, "hash": "abc"}"#), None);
        assert_eq!(parse_tip_changed_hash(br#"{"height": 965771, "hash": 12}"#), None);
        assert_eq!(parse_tip_changed_hash(b"nope"), None);
        assert_eq!(parse_tip_changed(format!(r#"{{"height": 965771, "hash": "{h}"}}"#).as_bytes()), Some(965771), "the height parser is untouched by the hash");
    }

    #[test]
    fn overlay_tip_body_with_hash_carries_the_lowercased_hash() {
        let h = "00000000000000001DE5AA96BAA3566CE66E4941F8295CC44CC85FC75949DB4D";
        let body = overlay_tip_body_with_hash(965771, h);
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["height"], 965771);
        assert_eq!(v["hash"], h.to_ascii_lowercase());
    }

    #[test]
    fn parse_header_takes_only_a_success_frame_for_this_height() {
        let root = "A785".repeat(16);
        let ok = serde_json::json!({"status":"success","value":{"height":965771,"hash":"00000000000000001DE5AA96BAA3566CE66E4941F8295CC44CC85FC75949DB4D","merkleRoot":root}});
        assert_eq!(
            parse_header(&ok, 965771),
            Some(HeaderLite {
                hash: "00000000000000001de5aa96baa3566ce66e4941f8295cc44cc85fc75949db4d".into(),
                merkle_root: "a785".repeat(16),
            })
        );
        assert_eq!(
            parse_header_hash(&ok, 965771).as_deref(),
            Some("00000000000000001de5aa96baa3566ce66e4941f8295cc44cc85fc75949db4d")
        );
        let no_root = serde_json::json!({"status":"success","value":{"height":965771,"hash":"00000000000000001DE5AA96BAA3566CE66E4941F8295CC44CC85FC75949DB4D","merkleRoot":"a785"}});
        assert_eq!(parse_header(&no_root, 965771), None, "a short merkle root is no header");
        assert_eq!(parse_header_hash(&ok, 965772), None, "another height's header is not this tip's hash");
        let err = serde_json::json!({"status":"error","value":null});
        assert_eq!(parse_header_hash(&err, 965771), None);
        let short = serde_json::json!({"status":"success","value":{"height":965771,"hash":"abc"}});
        assert_eq!(parse_header_hash(&short, 965771), None);
        assert_eq!(parse_header_hash(&serde_json::json!("nope"), 965771), None);
    }

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
    fn pot_changed_event_body_carries_the_outpoint_alone_with_the_room_pinned() {
        let b = pot_changed_event_body("ab", 1, 5);
        assert_eq!(b["v"], 1);
        assert_eq!(b["kind"], "pot-changed");
        assert_eq!(b["potOutpoint"]["txid"], "ab");
        assert_eq!(b["potOutpoint"]["vout"], 1);
        assert_eq!(b["at"], 5);
        assert!(b.get("entry").is_none(), "no served entry: attribution is not needed");
        assert!(b.get("identity").is_none());
        assert_eq!(POTS_ROOM, "broadcast-low-pots");
    }

    /// The handler announces EVERY parsed outpoint in the pots room BEFORE the
    /// attribution loop (bsv-low loop 10 D2, the pair-10 finding: a seat
    /// blocked at funding has no marker, so the per-seat event never files for
    /// it; the beta log read "has no attributed seats yet, nothing to file"
    /// for both JOINs). To red: move the push below `attribute_seats(` or
    /// delete it.
    #[test]
    fn internal_pot_changed_announces_every_outpoint_before_attribution() {
        let routes = include_str!("routes.rs");
        let start = routes
            .find("pub(crate) async fn internal_pot_changed(")
            .expect("the handler");
        let body = &routes[start..];
        let push = body
            .find("push_broadcast(env, crate::internal_events::POTS_ROOM, crate::internal_events::pot_changed_event_body(")
            .expect("the pots push");
        let attribution = body.find("attribute_seats(").expect("the attribution");
        assert!(push < attribution, "the pots push comes BEFORE any attribution");
        let filing_loop = body
            .find("for (txid, vout) in outpoints {")
            .expect("the per-outpoint filing loop");
        assert!(push < filing_loop, "the push runs over every parsed outpoint, before the filing loop");
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
