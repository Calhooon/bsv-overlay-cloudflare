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
fn auth_header_list() -> String {
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
pub fn add_cors_headers(resp: &mut Response) {
    let auth_list = auth_header_list();
    let h = resp.headers_mut();
    let _ = h.set("Access-Control-Allow-Origin", "*");
    // POST is the BRC-103/104 handshake (`/.well-known/auth`); every data
    // route stays GET.
    let _ = h.set("Access-Control-Allow-Methods", "GET, HEAD, POST, OPTIONS");
    let _ = h.set(
        "Access-Control-Allow-Headers",
        &format!("Content-Type, {auth_list}"),
    );
    // The signed-reply headers the AuthFetch client must read cross-origin.
    let _ = h.set("Access-Control-Expose-Headers", &auth_list);
    let _ = h.set("Access-Control-Max-Age", "86400");
}

/// Preflight response: 204 No Content + CORS headers.
pub fn preflight() -> Result<Response> {
    let mut resp = Response::empty()?.with_status(204);
    add_cors_headers(&mut resp);
    Ok(resp)
}
