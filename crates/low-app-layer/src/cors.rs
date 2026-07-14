//! Wildcard CORS for a public, read-only GET surface.
//!
//! Every route serves public chain facts with no auth, so the fully
//! permissive `Access-Control-Allow-Origin: *` is correct (mirrors the
//! zanaadu app-layer / overlay `routes.rs` CORS). Preflight is answered
//! before routing — a blocked preflight would hide the real request from
//! the browser entirely.

use worker::{Method, Request, Response, Result};

/// True for a CORS preflight (`OPTIONS`).
pub fn is_preflight(req: &Request) -> bool {
    req.method() == Method::Options
}

/// Stamp permissive CORS headers onto any response (success, error, 404 —
/// without them a cross-origin browser sees an opaque network error instead
/// of the real status).
pub fn add_cors_headers(resp: &mut Response) {
    let h = resp.headers_mut();
    let _ = h.set("Access-Control-Allow-Origin", "*");
    let _ = h.set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS");
    let _ = h.set("Access-Control-Allow-Headers", "Content-Type");
    let _ = h.set("Access-Control-Max-Age", "86400");
}

/// Preflight response: 204 No Content + CORS headers.
pub fn preflight() -> Result<Response> {
    let mut resp = Response::empty()?.with_status(204);
    add_cors_headers(&mut resp);
    Ok(resp)
}
