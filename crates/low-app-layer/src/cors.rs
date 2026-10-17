//! Wildcard CORS for a public, read-only surface with a BRC-103/104 front
//! door (bsv-low #318).
//!
//! Every route serves public chain facts, so the fully permissive
//! `Access-Control-Allow-Origin: *` is correct (mirrors the zanaadu
//! app-layer / overlay `routes.rs` CORS). Preflight is answered before
//! routing — a blocked preflight would hide the real request from the
//! browser entirely.
//!
//! Since #318 the browser also speaks BRC-104: the handshake is a POST to
//! `/.well-known/auth`, authenticated GETs carry `x-bsv-auth-*` request
//! headers, and signed replies carry `x-bsv-auth-*` response headers the
//! client must be able to READ cross-origin. So POST is allowed (the
//! handshake), the auth headers are allowed on requests, and the auth
//! headers are exposed on responses. The header names come from the
//! middleware's own `auth_headers` constants — never re-typed strings that
//! could drift from what `process_auth` actually reads/writes.

use bsv_middleware_cloudflare::auth_headers;
use worker::{Method, Request, Response, Result};

/// True for a CORS preflight (`OPTIONS`).
pub fn is_preflight(req: &Request) -> bool {
    req.method() == Method::Options
}

/// The `x-bsv-auth-*` header list, straight from the middleware's constants.
pub(crate) fn auth_header_list() -> String {
    [
        auth_headers::VERSION,
        auth_headers::IDENTITY_KEY,
        auth_headers::NONCE,
        auth_headers::INITIAL_NONCE,
        auth_headers::YOUR_NONCE,
        auth_headers::SIGNATURE,
        auth_headers::MESSAGE_TYPE,
        auth_headers::REQUEST_ID,
        auth_headers::REQUESTED_CERTIFICATES,
    ]
    .join(", ")
}

/// Stamp permissive CORS headers onto any response (success, error, 404 —
/// without them a cross-origin browser sees an opaque network error instead
/// of the real status).
/// bsv-low #441: the session lane's headers (the middleware's constants, never
/// literals): allowed on a request, the seal exposed on the answer.
pub(crate) fn lane_header_lists() -> (String, String) {
    use bsv_middleware_cloudflare::session_lane as lane;
    (
        [
            lane::SESSION_HEADER,
            lane::SESSION_IDENTITY_HEADER,
            lane::SESSION_COUNTER_HEADER,
            lane::SESSION_MAC_HEADER,
            lane::LANE_ASK_HEADER,
        ]
        .join(", "),
        // The seal, and the lane OFFER (a SIGNED response header on the first
        // signed read that asks): unexposed, a cross-origin client neither
        // sees it nor verifies the answer's signature over it — the
        // 2026-09-14 delta-verify NEW-2 (`auth-lane.spec.ts` minted nothing
        // for exactly this).
        [
            lane::SESSION_COUNTER_HEADER,
            lane::SESSION_MAC_HEADER,
            lane::LANE_OFFER_HEADER,
        ]
        .join(", "),
    )
}

/// The response headers a cross-origin client may read: the signed-reply
/// headers the AuthFetch client must read, the lane's seal and offer (#441),
/// and `Retry-After` (bsv-low #670: the auth door's store-fault 503 carries
/// it, and it is not a CORS-safelisted response header). Pure.
pub fn expose_list() -> String {
    let (_, lane_expose) = lane_header_lists();
    format!("{}, {lane_expose}, Retry-After", auth_header_list())
}

pub fn add_cors_headers(resp: &mut Response) {
    let auth_list = auth_header_list();
    let (lane_allow, _) = lane_header_lists();
    let h = resp.headers_mut();
    let _ = h.set("Access-Control-Allow-Origin", "*");
    // POST is the BRC-103/104 handshake (`/.well-known/auth`); every data
    // route stays GET.
    let _ = h.set("Access-Control-Allow-Methods", "GET, HEAD, POST, OPTIONS");
    let _ = h.set(
        "Access-Control-Allow-Headers",
        &format!("Content-Type, {auth_list}, {lane_allow}"),
    );
    // The headers the client must read cross-origin (`expose_list`).
    let _ = h.set("Access-Control-Expose-Headers", &expose_list());
    let _ = h.set("Access-Control-Max-Age", "86400");
}

/// Preflight response: 204 No Content + CORS headers.
pub fn preflight() -> Result<Response> {
    let mut resp = Response::empty()?.with_status(204);
    add_cors_headers(&mut resp);
    Ok(resp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bsv_middleware_cloudflare::session_lane as lane;

    /// #441: the lane's request headers are allowed and the seal + the signed
    /// offer header are EXPOSED (a cross-origin fetch sees exposed headers
    /// only; the SDK rebuilds the signed payload from what it sees — NEW-2).
    #[test]
    fn the_lane_headers_are_allowed_and_the_seal_and_offer_exposed() {
        let (allow, expose) = lane_header_lists();
        for h in [
            lane::SESSION_HEADER,
            lane::SESSION_IDENTITY_HEADER,
            lane::SESSION_COUNTER_HEADER,
            lane::SESSION_MAC_HEADER,
            lane::LANE_ASK_HEADER,
        ] {
            assert!(allow.contains(h), "allow list must carry {h}: {allow}");
        }
        for h in [
            lane::SESSION_COUNTER_HEADER,
            lane::SESSION_MAC_HEADER,
            lane::LANE_OFFER_HEADER,
        ] {
            assert!(expose.contains(h), "expose list must carry {h}: {expose}");
        }
        assert!(
            !expose.contains(lane::LANE_ASK_HEADER),
            "the ask is a request header, never exposed"
        );
    }

    /// bsv-low #670 (the lens's MEDIUM-1): `Retry-After` is exposed on every
    /// door reply, the default CORS and the identity CORS alike, beside the
    /// signed-reply headers and the lane's seal and offer; the default stamp
    /// renders `expose_list`.
    #[test]
    fn retry_after_is_exposed_by_both_cors_sets() {
        let expose = expose_list();
        let names: Vec<&str> = expose.split(", ").collect();
        assert!(names.contains(&"Retry-After"), "{expose}");
        for h in auth_header_list().split(", ").chain([
            lane::SESSION_COUNTER_HEADER,
            lane::SESSION_MAC_HEADER,
            lane::LANE_OFFER_HEADER,
        ]) {
            assert!(names.contains(&h), "expose list must carry {h}: {expose}");
        }
        let identity = crate::identity::cors_headers(Some("https://bsvarcade.com"));
        let (_, identity_expose) = identity
            .iter()
            .find(|(k, _)| *k == "Access-Control-Expose-Headers")
            .unwrap();
        assert!(
            identity_expose.split(", ").any(|h| h == "Retry-After"),
            "{identity_expose}"
        );
        let code: String = include_str!("cors.rs")
            .split("#[cfg(test)]")
            .next()
            .unwrap()
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();
        assert!(code.contains("h.set(\"Access-Control-Expose-Headers\",&expose_list());"));
    }
}
