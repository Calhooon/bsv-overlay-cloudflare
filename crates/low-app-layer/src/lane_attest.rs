//! bsv-low #443 step 4 (2026-09-14): `POST /lane/attest` — mint THIS door's lane
//! for an identity the RELAY's hub mirror proved, so a client that already
//! holds its relay lane (one BRC-103 handshake per tab) never pays this door a
//! handshake or a signed read. The body carries the client's hub-lane
//! credentials and the MAC'd inner text (`attestJson`: `{origin, ask,
//! clientNonce}`), never the `x-low-session*` headers (the front door's own
//! lane verify would judge those against THIS store and refuse); the door
//! hashes the text verbatim, binds it to its own origin, forwards the hub's
//! verify body to the relay through the RELAY service binding (`POST
//! /session/attest`, bearer `BROADCAST_TOKEN` — the same first-party bearer as
//! `/broadcast`), and on `ok` mints with the middleware's `mint_attested_lane`
//! (the same `LaneRecord::mint`, label, idle window and lifetime as a
//! signed-read mint). The identity minted for is the one the RELAY verified,
//! never a header claim. A door that keeps no lanes answers 404 (the client
//! latches the origin as a reference door); a refusal is 401
//! `ERR_ATTEST_REFUSED {reason}`; the relay unreachable or faulting is 503
//! (never a refusal). The signed-read mint stays the fallback on the client.
use bsv_middleware_cloudflare::session_lane::{prepare_attest, AttestAnswer, AttestHubBody};
use bsv_middleware_cloudflare::{mint_attested_lane, DoSessionStorage, SessionLaneOptions};
use serde_json::{json, Value};
use worker::*;

use crate::auth::{json_reply, session_lane_configured, AuthState, AUTH_SESSION_STORE_BINDING};
use crate::internal_events::relay_post;

pub const ATTEST_REFUSED_CODE: &str = "ERR_ATTEST_REFUSED";

fn refused(reason: &str) -> Result<Response> {
    json_reply(401, &json!({ "code": ATTEST_REFUSED_CODE, "reason": reason }))
}

/// Ask the relay whether the hub-lane call verifies: `Ok(Some(identity))` on
/// `ok`, `Ok(None)` with the reason on a refusal, `Err` when the relay could
/// not be asked or answered outside its contract (the door answers 503).
pub(crate) async fn relay_attest(env: &Env, hub: &AttestHubBody) -> Result<std::result::Result<String, String>> {
    let relay = env.var("RELAY_URL").map(|v| v.to_string()).map_err(|_| Error::RustError("RELAY_URL unset".into()))?;
    let token = env
        .secret("BROADCAST_TOKEN")
        .map(|v| v.to_string())
        .map_err(|_| Error::RustError("BROADCAST_TOKEN unset".into()))?;
    let payload = serde_json::to_string(hub).map_err(|e| Error::RustError(e.to_string()))?;
    let mut res = relay_post(env, &relay, "/session/attest", &token, payload).await?;
    let status = res.status_code();
    let v: Value = res.json().await.unwrap_or(Value::Null);
    if status == 503 {
        return Err(Error::RustError("relay: hub-unavailable".into()));
    }
    if status != 200 {
        return Err(Error::RustError(format!("relay answered {status}")));
    }
    if v.get("ok").and_then(Value::as_bool) == Some(true) {
        let identity = v.get("identity").and_then(Value::as_str).unwrap_or_default().to_ascii_lowercase();
        return Ok(Ok(identity));
    }
    Ok(Err(v.get("reason").and_then(Value::as_str).unwrap_or("unknown-session").to_string()))
}

pub async fn lane_attest(mut req: Request, ctx: RouteContext<AuthState>) -> Result<Response> {
    let env = &ctx.env;
    if !session_lane_configured(env) {
        return json_reply(404, &json!({ "error": "this door keeps no lanes" }));
    }
    let this_origin = req.url()?.origin().ascii_serialization();
    let body = req.text().await?;
    let (hub, inner) = match prepare_attest(&body, &this_origin) {
        Ok(x) => x,
        Err(r) => return refused(r.as_str()),
    };
    let identity = match relay_attest(env, &hub).await {
        Ok(Ok(identity)) => identity,
        Ok(Err(reason)) => return refused(&reason),
        Err(e) => {
            console_log!("[lane-attest] the relay could not be asked: {e}");
            return json_reply(503, &json!({ "error": "the relay could not be asked", "reason": "relay-unavailable" }));
        }
    };
    if identity != hub.identity {
        // The relay proves the identity it holds the mirror for; a body naming
        // another is refused by name (never minted for the claim).
        return refused("identity-mismatch");
    }
    let mut nonce = [0u8; 32];
    if getrandom::getrandom(&mut nonce).is_err() {
        return json_reply(503, &json!({ "error": "no randomness" }));
    }
    let server_nonce = hex::encode(nonce);
    let storage = DoSessionStorage::from_env(env, AUTH_SESSION_STORE_BINDING, 3600)
        .map_err(|e| Error::RustError(format!("lane store: {e:?}")))?;
    let offer = mint_attested_lane(&storage, &SessionLaneOptions::default(), &identity, &inner.client_nonce, &server_nonce, &inner.ask).await;
    match offer {
        Some(session) => {
            console_log!("[lane-attest] minted for {}… (relay-attested)", &identity[..12]);
            json_reply(200, &serde_json::to_value(AttestAnswer { session, server_nonce }).unwrap_or(Value::Null))
        }
        None => json_reply(503, &json!({ "error": "the lane store refused the mint" })),
    }
}

#[cfg(test)]
mod tests {
    /// Structural: the route binds the origin and hashes the text through the
    /// middleware's `prepare_attest` BEFORE the relay is asked, mints ONLY for
    /// the identity the relay answered (the body's identity must match), never
    /// reads `x-low-session` headers, and answers 503 (never a refusal) when
    /// the relay cannot be asked.
    #[test]
    fn the_attest_route_verifies_through_the_relay_before_it_mints_for_the_relay_s_identity() {
        let src = include_str!("lane_attest.rs");
        let handler = &src[src.find("pub async fn lane_attest").unwrap()..src.find("#[cfg(test)]").unwrap()];
        let prepare = handler.find("prepare_attest(&body, &this_origin)").expect("the pure half");
        let ask = handler.find("relay_attest(env, &hub)").expect("the relay is asked");
        let bind = handler.find("identity != hub.identity").expect("the identity binding");
        let mint = handler.find("mint_attested_lane(").expect("the mint");
        assert!(prepare < ask && ask < bind && bind < mint, "prepare → relay → bind → mint");
        assert!(!handler.contains("x-low-session"), "the hub-lane credentials ride the body, never this door's lane headers");
        assert!(handler.contains("json_reply(503") && handler.contains("relay-unavailable"), "a dead relay is 503");
        assert!(handler.contains("session_lane_configured(env)") && handler.contains("json_reply(404"), "no lanes: 404");
    }
}
