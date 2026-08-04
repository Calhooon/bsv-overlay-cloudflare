//! BRC-103/104 identity auth for the identity-scoped read routes (bsv-low #318).
//!
//! ## What this buys (and what it does not)
//!
//! Before #318 every identity-scoped view (`/results`, `/refund-view`,
//! `/live-view`, `/recovery-view`, `/hops-view`) derived "who is asking" from
//! a **query parameter the caller supplies**. This module puts the tower's
//! front-door posture (`bsv-low workers/low-watchtower front_door_case`) on
//! those reads: a BRC-103/104 mutual-auth handshake at `/.well-known/auth`,
//! per-request signature verification via `bsv-middleware-cloudflare`
//! (the canonical public middleware, same major the tower pins), and ONE
//! identity-resolution seam every identity route consumes.
//!
//! It buys **integrity of the identity binding** (the attacker-forced-CPU fix:
//! an authenticated caller can only spend the verify budget on its own view),
//! per-identity **quota readiness** (BRC-105 follow-on), and **#252 stage-3
//! readiness**. It does NOT buy confidentiality — the overlay `/lookup` is a
//! public index by design and serves the same per-identity rows unauthenticated.
//! Do not claim otherwise (see the #318 issue's honest-limitation statement).
//!
//! ## Lenient vs strict — the rollout window (Rule 6c: closure specified NOW)
//!
//! The shipped bsv-low client does NOT yet authenticate app-layer reads, so
//! enforcing today is an outage (every Home money card fails at once — the
//! #316 rollout-note failure shape). Therefore:
//!
//! * **Lenient (default, `AUTH_ENFORCE` unset/false):** an unauthenticated
//!   request is SERVED exactly as before, but **counted** (`anonymousServed`,
//!   surfaced on `/health` — Rule 13: surface, don't consume). A request that
//!   ATTEMPTS auth is fully verified — an invalid signature is refused, never
//!   silently downgraded to anonymous. When a request IS authenticated, the
//!   verified identity WINS over `?identity=`, and a mismatch is REFUSED
//!   (403, honest body), never silently coerced.
//! * **Strict (`AUTH_ENFORCE=true`):** an unauthenticated request to an
//!   identity-scoped view is refused with an honest 401 JSON body naming the
//!   handshake path. Flip the var to enforce; flip it back for instant
//!   rollback. No code change either way.
//!
//! **Closure criteria for the lenient window** (write-once, per Rule 6c —
//! "a compatibility window that never closes is a permanent hole with better
//! manners"):
//!   1. the bsv-low client release that routes all five identity reads
//!      through its existing `AuthFetch` is pinned + released;
//!   2. soak: `/health`'s `anonymousServed` approaches zero while
//!      `authenticatedServed` carries the traffic;
//!   3. then flip `AUTH_ENFORCE=true`.
//!
//! **What a stale client experiences at closure:** an honest `401
//! {"error":"authentication required…"}`. bsv-low `chainReads.ts` treats any
//! non-200 as a thrown/warned failure with route-level fallback (e.g.
//! `/recovery-view` falls back to overlay enumeration) — visible, never a
//! silent wrong answer.
//!
//! ## Rule 8b — no forwardable identity header exists here
//!
//! The tower forwards its verified identity inward to Durable Objects as
//! `X-Identity-Key` and must strip caller-supplied copies at the public edge.
//! This worker has NO inner hop: the verified identity travels **in-process
//! only**, as [`CallerAuth::Verified`] inside the router data, constructed
//! exclusively from the middleware's [`AuthResult::Authenticated`] (a
//! verified-signature result). No request header is ever read as an identity
//! (pinned by `tests/auth_seam_pins.rs`), so a forged `X-Identity-Key` — or
//! any other header — is inert in both modes.
//!
//! ## Rule 15 — one seam, handlers don't choose
//!
//! [`resolve_view_identity`] is the ONLY place the "session identity vs query
//! param vs mode" question is answered. Route handlers receive the resolved
//! decision through `routes::view_identity`; none of them re-derives it.

use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::json;

// ── pure core (native-testable) ─────────────────────────────────────────────

/// The enforcement mode, derived from the `AUTH_ENFORCE` env var.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMode {
    /// Serve unauthenticated requests (counted); verify any attempted auth.
    Lenient,
    /// Refuse unauthenticated requests to identity-scoped views (honest 401).
    Strict,
}

impl AuthMode {
    /// Parse the `AUTH_ENFORCE` var. Only an explicit opt-in enforces —
    /// unset, empty, or anything else is lenient, so a missing var can never
    /// cause a surprise outage, and `"true"` → strict is the documented flip.
    pub fn from_flag(v: Option<&str>) -> Self {
        match v.map(|s| s.trim().to_ascii_lowercase()).as_deref() {
            Some("1") | Some("true") | Some("strict") => AuthMode::Strict,
            _ => AuthMode::Lenient,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            AuthMode::Lenient => "lenient",
            AuthMode::Strict => "strict",
        }
    }
}

/// The caller's transport identity as the FRONT DOOR resolved it.
///
/// `Verified` is constructed in exactly one place — the front door, from the
/// middleware's verified-signature result — and nowhere else (Rule 8b: there
/// is no header or body field a stranger could set to become `Verified`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallerAuth {
    /// BRC-103/104-verified identity key (66-char compressed hex, lowercase).
    Verified(String),
    /// No authentication attempted (lenient mode lets these through).
    Anonymous,
}

impl CallerAuth {
    /// Normalize at the boundary (derive, don't accept): the stored identity
    /// is always lowercase, so every later comparison is case-consistent.
    pub fn verified(identity_key: &str) -> Self {
        CallerAuth::Verified(identity_key.trim().to_ascii_lowercase())
    }
}

/// The one identity-resolution answer every identity-scoped route consumes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentityDecision {
    /// Serve this identity's view. `None` = no identity resolved (anonymous
    /// caller, no/empty `?identity=`) — the route's existing empty-view
    /// behavior applies.
    Serve(Option<String>),
    /// Authenticated identity and `?identity=` disagree → refuse (403).
    /// NEVER silently prefer either one.
    RefuseMismatch {
        session_identity: String,
        query_identity: String,
    },
    /// Strict mode, no authentication → refuse (401). (Defense in depth:
    /// the front door already refuses these before routing.)
    RefuseUnauthenticated,
}

/// THE seam (Rule 15): derive the effective view identity from the verified
/// caller + the query-param claim + the mode. Handlers never re-choose.
///
/// * Verified caller: the session identity WINS. No param / empty param /
///   case-variant of the same key → serve the session identity. A param
///   naming a DIFFERENT identity → `RefuseMismatch` in BOTH modes.
/// * Anonymous caller, lenient → serve the query-param claim (the legacy
///   pre-#318 behavior, counted at the front door).
/// * Anonymous caller, strict → `RefuseUnauthenticated`.
pub fn resolve_view_identity(
    mode: AuthMode,
    caller: &CallerAuth,
    query_identity: Option<&str>,
) -> IdentityDecision {
    // An explicitly-empty `?identity=` is the same claim as no param.
    let query = query_identity.map(str::trim).filter(|q| !q.is_empty());
    match caller {
        CallerAuth::Verified(session_id) => match query {
            None => IdentityDecision::Serve(Some(session_id.clone())),
            Some(q) if q.eq_ignore_ascii_case(session_id) => {
                IdentityDecision::Serve(Some(session_id.clone()))
            }
            Some(q) => IdentityDecision::RefuseMismatch {
                session_identity: session_id.clone(),
                query_identity: q.to_ascii_lowercase(),
            },
        },
        CallerAuth::Anonymous => match mode {
            AuthMode::Lenient => IdentityDecision::Serve(query.map(|q| q.to_ascii_lowercase())),
            AuthMode::Strict => IdentityDecision::RefuseUnauthenticated,
        },
    }
}

/// What the front door does with a request BEFORE any middleware work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    /// No auth attempted, lenient → serve as anonymous (counted).
    ProceedAnonymous,
    /// No auth attempted, strict → honest 401.
    RefuseUnauthenticated,
    /// Auth attempted (or strict) but the worker has no `SERVER_PRIVATE_KEY`
    /// / `AUTH_SESSIONS` → honest 503, never a silent downgrade.
    RefuseMisconfigured,
    /// Auth attempted and configured → run the real middleware
    /// (`process_auth`, `allow_unauthenticated: false`).
    RunMiddleware,
}

/// Pure front-door dispatch (derive, don't accept — the wasm glue is a thin
/// match over this):
///
/// * attempted + configured        → `RunMiddleware`
/// * attempted + !configured       → `RefuseMisconfigured` (an auth-attempting
///   client must never be silently served as anonymous)
/// * !attempted + lenient          → `ProceedAnonymous` (works pre-secret too:
///   lenient does not depend on auth being configured)
/// * !attempted + strict + configured  → `RefuseUnauthenticated`
/// * !attempted + strict + !configured → `RefuseMisconfigured` (strict with no
///   key is an outage-grade misconfig; an honest 503 beats a silent lenient)
pub fn front_door_disposition(
    mode: AuthMode,
    auth_configured: bool,
    auth_attempted: bool,
) -> Disposition {
    if auth_attempted {
        if auth_configured {
            Disposition::RunMiddleware
        } else {
            Disposition::RefuseMisconfigured
        }
    } else {
        match (mode, auth_configured) {
            (AuthMode::Lenient, _) => Disposition::ProceedAnonymous,
            (AuthMode::Strict, true) => Disposition::RefuseUnauthenticated,
            (AuthMode::Strict, false) => Disposition::RefuseMisconfigured,
        }
    }
}

// ── counters (Rule 13: surface, don't consume) ──────────────────────────────
//
// Per-ISOLATE process counters (a Cloudflare Worker isolate is single-request
// at a time for wasm; atomics keep the native test build honest). They reset
// on isolate recycle — they are a SOAK/monitoring surface on `/health`, not
// an accounting ledger. The actionable consumer (Rule 13 corollary): the
// operator watching `anonymousServed` → ~0 before flipping `AUTH_ENFORCE`.

static ANONYMOUS_SERVED: AtomicU64 = AtomicU64::new(0);
static AUTHENTICATED_SERVED: AtomicU64 = AtomicU64::new(0);
static AUTH_REFUSED: AtomicU64 = AtomicU64::new(0);
static MISMATCH_REFUSED: AtomicU64 = AtomicU64::new(0);
static STRICT_REFUSED_UNAUTHENTICATED: AtomicU64 = AtomicU64::new(0);
static MISCONFIGURED_REFUSED: AtomicU64 = AtomicU64::new(0);

pub fn count_anonymous_served() {
    ANONYMOUS_SERVED.fetch_add(1, Ordering::Relaxed);
}
pub fn count_authenticated_served() {
    AUTHENTICATED_SERVED.fetch_add(1, Ordering::Relaxed);
}
pub fn count_auth_refused() {
    AUTH_REFUSED.fetch_add(1, Ordering::Relaxed);
}
pub fn count_mismatch_refused() {
    MISMATCH_REFUSED.fetch_add(1, Ordering::Relaxed);
}
pub fn count_strict_refused_unauthenticated() {
    STRICT_REFUSED_UNAUTHENTICATED.fetch_add(1, Ordering::Relaxed);
}
pub fn count_misconfigured_refused() {
    MISCONFIGURED_REFUSED.fetch_add(1, Ordering::Relaxed);
}

/// A point-in-time copy of the counters (pure input to the health JSON so the
/// renderer is natively testable against explicit values).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AuthCountersSnapshot {
    pub anonymous_served: u64,
    pub authenticated_served: u64,
    pub auth_refused: u64,
    pub mismatch_refused: u64,
    pub strict_refused_unauthenticated: u64,
    pub misconfigured_refused: u64,
}

pub fn counters_snapshot() -> AuthCountersSnapshot {
    AuthCountersSnapshot {
        anonymous_served: ANONYMOUS_SERVED.load(Ordering::Relaxed),
        authenticated_served: AUTHENTICATED_SERVED.load(Ordering::Relaxed),
        auth_refused: AUTH_REFUSED.load(Ordering::Relaxed),
        mismatch_refused: MISMATCH_REFUSED.load(Ordering::Relaxed),
        strict_refused_unauthenticated: STRICT_REFUSED_UNAUTHENTICATED.load(Ordering::Relaxed),
        misconfigured_refused: MISCONFIGURED_REFUSED.load(Ordering::Relaxed),
    }
}

/// The `/health` auth surface (Rule 13): mode + configured + counters, so
/// "unauthenticated but accepted" is a NUMBER an operator watches during the
/// soak, never a silent accept.
pub fn auth_health_json(
    mode: AuthMode,
    auth_configured: bool,
    c: &AuthCountersSnapshot,
) -> serde_json::Value {
    json!({
        "authMode": mode.as_str(),
        "authConfigured": auth_configured,
        // Per-isolate since last isolate recycle — a soak signal, not a ledger.
        "countersScope": "isolate",
        "anonymousServed": c.anonymous_served,
        "authenticatedServed": c.authenticated_served,
        "authRefused": c.auth_refused,
        "mismatchRefused": c.mismatch_refused,
        "strictRefusedUnauthenticated": c.strict_refused_unauthenticated,
        "misconfiguredRefused": c.misconfigured_refused,
    })
}

// ── wasm glue: the front door ───────────────────────────────────────────────

use bsv_middleware_cloudflare::{
    process_auth, AuthMiddlewareOptions, AuthResult, AuthSession, CloudflareTransport,
};
use worker::{Env, Request, Response, Result};

/// Per-request auth state, carried to handlers as the router data (in-process
/// only — never a header, see the module docs' Rule 8b note).
#[derive(Debug, Clone)]
pub struct AuthState {
    pub mode: AuthMode,
    pub caller: CallerAuth,
    pub auth_configured: bool,
    /// Present iff the caller authenticated — used to sign the JSON reply so
    /// `AuthFetch` clients verify the server (the tower's posture).
    pub session: Option<AuthSession>,
}

/// Front-door outcome: either proceed to the router with resolved state, or
/// reply immediately (handshake reply, 401, 503, middleware error).
pub enum FrontDoor {
    Proceed(Request, AuthState),
    Reply(Response),
}

fn json_reply(status: u16, body: &serde_json::Value) -> Result<Response> {
    let mut resp = Response::ok(body.to_string())?.with_status(status);
    resp.headers_mut().set("Content-Type", "application/json")?;
    resp.headers_mut().set("Cache-Control", "no-store")?;
    Ok(resp)
}

/// A secret/var read that treats "bound but empty" as absent.
fn nonempty_var(env: &Env, name: &str) -> Option<String> {
    env.secret(name)
        .ok()
        .map(|s| s.to_string())
        .filter(|s| !s.trim().is_empty())
}

/// Run the BRC-103/104 front door (the tower's `front_door_case` posture,
/// adapted to a read surface with a lenient rollout window).
///
/// The middleware is only invoked when the request ATTEMPTS auth (handshake
/// path or `x-bsv-auth-*` headers — the middleware's OWN predicates, so the
/// dispatch cannot drift from what `process_auth` would treat as auth). It
/// always runs `allow_unauthenticated: false`: lenient-ness lives entirely in
/// [`front_door_disposition`], so an attempted-but-invalid signature is
/// REFUSED in both modes, never downgraded to anonymous.
pub async fn front_door(req: Request, env: &Env) -> Result<FrontDoor> {
    let mode = AuthMode::from_flag(env.var("AUTH_ENFORCE").ok().map(|v| v.to_string()).as_deref());
    let server_key = nonempty_var(env, "SERVER_PRIVATE_KEY");
    let auth_configured = server_key.is_some() && env.kv("AUTH_SESSIONS").is_ok();
    let is_handshake = CloudflareTransport::is_handshake_request(&req);
    let auth_attempted = is_handshake || CloudflareTransport::has_auth_headers(&req);

    match front_door_disposition(mode, auth_configured, auth_attempted) {
        Disposition::ProceedAnonymous => {
            count_anonymous_served();
            Ok(FrontDoor::Proceed(
                req,
                AuthState {
                    mode,
                    caller: CallerAuth::Anonymous,
                    auth_configured,
                    session: None,
                },
            ))
        }
        Disposition::RefuseUnauthenticated => {
            count_strict_refused_unauthenticated();
            json_reply(
                401,
                &json!({
                    "error": "authentication required: AUTH_ENFORCE is on — authenticate via the BRC-103/104 handshake at /.well-known/auth (AuthFetch does this automatically)",
                    "authMode": mode.as_str(),
                }),
            )
            .map(FrontDoor::Reply)
        }
        Disposition::RefuseMisconfigured => {
            count_misconfigured_refused();
            json_reply(
                503,
                &json!({
                    "error": "auth unavailable: SERVER_PRIVATE_KEY / AUTH_SESSIONS is not configured on this worker",
                    "authMode": mode.as_str(),
                }),
            )
            .map(FrontDoor::Reply)
        }
        Disposition::RunMiddleware => {
            let opts = AuthMiddlewareOptions {
                // `auth_configured` guarantees presence; empty-string fallback
                // makes the middleware fail closed rather than panic.
                server_private_key: server_key.unwrap_or_default(),
                // ALWAYS false — see the doc comment above.
                allow_unauthenticated: false,
                session_ttl_seconds: 3600,
                ..Default::default()
            };
            // A malformed handshake makes `process_auth` return Err — map it
            // to an honest 400 (the tower's V2 mapping), never a bare 500.
            let auth = match process_auth(req, env, &opts).await {
                Ok(a) => a,
                Err(e) => {
                    count_auth_refused();
                    return json_reply(
                        400,
                        &json!({ "error": format!("authentication handshake failed: {e}") }),
                    )
                    .map(FrontDoor::Reply);
                }
            };
            match auth {
                AuthResult::Authenticated {
                    context,
                    request,
                    session,
                    // GET reads carry no body; the BRC-104 signature covered
                    // whatever bytes were there.
                    body: _,
                } => match session {
                    Some(session) => {
                        count_authenticated_served();
                        Ok(FrontDoor::Proceed(
                            request,
                            AuthState {
                                mode,
                                // The ONLY `Verified` constructor call site —
                                // fed exclusively by the middleware's
                                // verified-signature result (Rule 8b).
                                caller: CallerAuth::verified(&context.identity_key),
                                auth_configured,
                                session: Some(session),
                            },
                        ))
                    }
                    // `allow_unauthenticated: false` means an Authenticated
                    // result always carries a session; a missing one is a
                    // middleware contract break — refuse, never serve as
                    // anonymous-with-identity.
                    None => {
                        count_auth_refused();
                        json_reply(
                            500,
                            &json!({ "error": "auth middleware returned an authenticated result without a session" }),
                        )
                        .map(FrontDoor::Reply)
                    }
                },
                // Handshake reply or a middleware refusal (401 / certificate
                // flow) — return verbatim; the middleware already stamped its
                // CORS. Count refusals (a handshake reply is not a refusal).
                AuthResult::Response(resp) => {
                    if !is_handshake {
                        count_auth_refused();
                    }
                    Ok(FrontDoor::Reply(resp))
                }
            }
        }
    }
}

// ── pure-core tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    const ID_A: &str = "02aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899";
    const ID_B: &str = "03ffeeddccbbaa99887766554433221100ffeeddccbbaa998877665544332211ff";

    fn verified_a() -> CallerAuth {
        CallerAuth::verified(&ID_A.to_ascii_uppercase())
    }

    // ── AuthMode::from_flag ────────────────────────────────────────────────

    #[test]
    fn auth_mode_only_explicit_optin_enforces() {
        for strict in ["1", "true", "TRUE", " strict ", "True"] {
            assert_eq!(AuthMode::from_flag(Some(strict)), AuthMode::Strict, "{strict:?}");
        }
        for lenient in [None, Some(""), Some("false"), Some("0"), Some("yes"), Some("lenient")] {
            assert_eq!(AuthMode::from_flag(lenient), AuthMode::Lenient, "{lenient:?}");
        }
    }

    // ── resolve_view_identity: the full matrix ─────────────────────────────
    //
    // Both modes × both callers × {no param, empty param, matching param,
    // case-variant matching param, mismatching param}. Every cell asserted —
    // the lenient/strict behavior matrix of the issue, pinned by value.

    #[test]
    fn verified_caller_no_param_serves_session_identity_both_modes() {
        for mode in [AuthMode::Lenient, AuthMode::Strict] {
            assert_eq!(
                resolve_view_identity(mode, &verified_a(), None),
                IdentityDecision::Serve(Some(ID_A.to_string())),
            );
        }
    }

    #[test]
    fn verified_caller_empty_param_is_the_same_as_no_param() {
        for mode in [AuthMode::Lenient, AuthMode::Strict] {
            for empty in ["", "  "] {
                assert_eq!(
                    resolve_view_identity(mode, &verified_a(), Some(empty)),
                    IdentityDecision::Serve(Some(ID_A.to_string())),
                );
            }
        }
    }

    #[test]
    fn verified_caller_matching_param_serves_session_identity_case_insensitive() {
        for mode in [AuthMode::Lenient, AuthMode::Strict] {
            for q in [ID_A.to_string(), ID_A.to_ascii_uppercase()] {
                assert_eq!(
                    resolve_view_identity(mode, &verified_a(), Some(&q)),
                    IdentityDecision::Serve(Some(ID_A.to_string())),
                    "query {q} must serve the (lowercased) session identity"
                );
            }
        }
    }

    /// THE issue's core cell: session identity ≠ query param is REFUSED in
    /// BOTH modes — never silently coerced to either side.
    #[test]
    fn verified_caller_mismatching_param_refused_in_both_modes() {
        for mode in [AuthMode::Lenient, AuthMode::Strict] {
            assert_eq!(
                resolve_view_identity(mode, &verified_a(), Some(ID_B)),
                IdentityDecision::RefuseMismatch {
                    session_identity: ID_A.to_string(),
                    query_identity: ID_B.to_string(),
                },
            );
        }
    }

    /// A malformed claim (not even identity-shaped) with a verified session is
    /// still a MISMATCH refusal — not silently swallowed into the session view.
    #[test]
    fn verified_caller_garbage_param_is_a_mismatch_refusal() {
        assert_eq!(
            resolve_view_identity(AuthMode::Lenient, &verified_a(), Some("not-a-key")),
            IdentityDecision::RefuseMismatch {
                session_identity: ID_A.to_string(),
                query_identity: "not-a-key".to_string(),
            },
        );
    }

    #[test]
    fn anonymous_lenient_serves_the_query_claim_lowercased() {
        assert_eq!(
            resolve_view_identity(
                AuthMode::Lenient,
                &CallerAuth::Anonymous,
                Some(&ID_A.to_ascii_uppercase())
            ),
            IdentityDecision::Serve(Some(ID_A.to_string())),
        );
        assert_eq!(
            resolve_view_identity(AuthMode::Lenient, &CallerAuth::Anonymous, None),
            IdentityDecision::Serve(None),
        );
    }

    #[test]
    fn anonymous_strict_is_refused_with_or_without_a_claim() {
        for query in [None, Some(ID_A), Some("junk")] {
            assert_eq!(
                resolve_view_identity(AuthMode::Strict, &CallerAuth::Anonymous, query),
                IdentityDecision::RefuseUnauthenticated,
            );
        }
    }

    /// `CallerAuth::verified` normalizes to lowercase at the boundary, so no
    /// later comparison can be case-torn.
    #[test]
    fn verified_constructor_lowercases() {
        assert_eq!(
            CallerAuth::verified(&format!(" {} ", ID_A.to_ascii_uppercase())),
            CallerAuth::Verified(ID_A.to_string()),
        );
    }

    // ── front_door_disposition: full 2×2×2 matrix ──────────────────────────

    #[test]
    fn front_door_disposition_full_matrix() {
        use AuthMode::*;
        use Disposition::*;
        // (mode, configured, attempted) → expected
        let cells = [
            (Lenient, true, true, RunMiddleware),
            (Lenient, true, false, ProceedAnonymous),
            (Lenient, false, true, RefuseMisconfigured),
            (Lenient, false, false, ProceedAnonymous),
            (Strict, true, true, RunMiddleware),
            (Strict, true, false, RefuseUnauthenticated),
            (Strict, false, true, RefuseMisconfigured),
            (Strict, false, false, RefuseMisconfigured),
        ];
        // Exhaustiveness: 8 cells cover the whole boolean cube (2 modes × 2 ×
        // 2) — a literal count, so a new enum variant cannot shrink both
        // sides in sympathy.
        assert_eq!(cells.len(), 8);
        for (mode, configured, attempted, expected) in cells {
            assert_eq!(
                front_door_disposition(mode, configured, attempted),
                expected,
                "mode={mode:?} configured={configured} attempted={attempted}"
            );
        }
    }

    // ── health surface (Rule 13) ───────────────────────────────────────────

    #[test]
    fn auth_health_json_surfaces_mode_and_every_counter() {
        let snap = AuthCountersSnapshot {
            anonymous_served: 7,
            authenticated_served: 3,
            auth_refused: 2,
            mismatch_refused: 1,
            strict_refused_unauthenticated: 5,
            misconfigured_refused: 4,
        };
        let v = auth_health_json(AuthMode::Lenient, true, &snap);
        assert_eq!(v["authMode"], "lenient");
        assert_eq!(v["authConfigured"], true);
        assert_eq!(v["anonymousServed"], 7);
        assert_eq!(v["authenticatedServed"], 3);
        assert_eq!(v["authRefused"], 2);
        assert_eq!(v["mismatchRefused"], 1);
        assert_eq!(v["strictRefusedUnauthenticated"], 5);
        assert_eq!(v["misconfiguredRefused"], 4);
        assert_eq!(
            auth_health_json(AuthMode::Strict, false, &snap)["authMode"],
            "strict"
        );
    }

    #[test]
    fn counters_increment_through_the_real_count_fns() {
        // Delta-based (other tests may bump the shared statics).
        let before = counters_snapshot();
        count_anonymous_served();
        count_authenticated_served();
        count_mismatch_refused();
        let after = counters_snapshot();
        assert!(after.anonymous_served > before.anonymous_served);
        assert!(after.authenticated_served > before.authenticated_served);
        assert!(after.mismatch_refused > before.mismatch_refused);
    }
}
