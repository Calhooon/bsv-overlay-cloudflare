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
//!   request is SERVED exactly as before, but **counted** (per-identity-route
//!   `anonymousByRoute` + `publicServed`, surfaced on `/health` — Rule 13:
//!   surface, don't consume, and the count is per route so the operator can
//!   tell WHICH read is still anonymous). A request that ATTEMPTS auth is
//!   fully verified — an invalid signature is refused, never silently
//!   downgraded to anonymous. When a request IS authenticated, the verified
//!   identity WINS over `?identity=`, and a mismatch is REFUSED (403, honest
//!   body), never silently coerced.
//! * **Strict (`AUTH_ENFORCE=true`):** an unauthenticated request to one of
//!   the five **identity-scoped** views ([`route_requires_identity_auth`]) is
//!   refused with an honest 401 naming the handshake path. **Every OTHER route
//!   is PUBLIC and is NEVER refused for lack of auth, in either mode** —
//!   `/health`, `/leaderboard`, `/utxo-status`, `/pots-view`, `/beef`, `/tip`,
//!   `/spent-any`, `/tx-any`, `/`. This is a DELIBERATE exemption
//!   ([`effective_mode`] forces those routes lenient): they serve exactly the
//!   data the public overlay `/lookup` already serves unauthenticated, so
//!   strict-gating them would buy only quota-readiness, not confidentiality,
//!   while breaking every public read (the #316 outage shape) and any liveness
//!   monitor on `/health`. A public route STILL honours + signs auth when a
//!   client presents it (so `AuthFetch` works against public routes too); it
//!   just never REQUIRES it. Flip the var to enforce; flip it back for instant
//!   rollback. No code change either way.
//!
//! **Closure criteria for the lenient window** (write-once, per Rule 6c —
//! "a compatibility window that never closes is a permanent hole with better
//! manners"):
//!   1. **First** provision the worker: set `SERVER_PRIVATE_KEY` and create
//!      the `AUTH_SESSIONS` KV — BEFORE the auth-enabled client ships.
//!      `AuthFetch` ALWAYS initiates the BRC-103 handshake, so an auth client
//!      against an unconfigured worker gets a 503 (`RefuseMisconfigured`) on
//!      every identity read; provisioning must lead, not follow.
//!   2. then pin + release the bsv-low client that routes **every app-layer
//!      read it makes** through its existing `AuthFetch` (the five
//!      identity-scoped reads are the ones that must migrate before the flip;
//!      the public reads may migrate too but are never gated);
//!   3. soak: `/health`'s `anonymousByRoute` map approaches zero for every
//!      identity route while `authenticatedServed` carries the traffic
//!      (`publicServed` staying non-zero is fine — public routes are never
//!      gated);
//!   4. then flip `AUTH_ENFORCE=true` (identity routes only start refusing).
//!
//! **What a stale client experiences at closure:** an honest `401
//! {"error":"authentication required…"}` **on the five identity routes only**;
//! its public reads keep working. bsv-low `chainReads.ts` treats any non-200
//! as a thrown/warned failure with route-level fallback (e.g. `/recovery-view`
//! falls back to overlay enumeration) — visible, never a silent wrong answer.
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

/// The five identity-scoped read routes — the ONLY routes strict enforcement
/// gates. Order is the stable index used by the per-route anonymous counter
/// ([`identity_route_index`] / `anonymousByRoute`); append, never reorder.
pub const IDENTITY_ROUTES: [&str; 5] = [
    "/results",
    "/refund-view",
    "/live-view",
    "/recovery-view",
    "/hops-view",
];

/// True iff `path` is one of the identity-scoped views. Every other route is
/// PUBLIC (never refused for lack of auth — see the module docs' deliberate
/// exemption; the overlay `/lookup` already serves the same data
/// unauthenticated, so gating them buys only quota-readiness).
pub fn route_requires_identity_auth(path: &str) -> bool {
    identity_route_index(path).is_some()
}

/// The stable index of an identity route (for the per-route anon counter), or
/// `None` for a public route.
pub fn identity_route_index(path: &str) -> Option<usize> {
    IDENTITY_ROUTES.iter().position(|r| *r == path)
}

/// The mode ACTUALLY applied to a given route. Global strict only bites the
/// identity-scoped routes; a public route is ALWAYS lenient, so an anonymous
/// public read is never refused regardless of `AUTH_ENFORCE`. (Auth is still
/// honoured + the reply signed when a client presents it — lenient never
/// downgrades an *attempted* auth, it only never *requires* one.)
///
/// The GLOBAL mode is what `/health` reports and what the identity seam reads
/// (for identity routes `effective_mode == global`, so the seam is unaffected
/// by this narrowing).
pub fn effective_mode(global: AuthMode, path: &str) -> AuthMode {
    if route_requires_identity_auth(path) {
        global
    } else {
        AuthMode::Lenient
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
// operator watching `anonymousByRoute` → ~0 PER IDENTITY ROUTE before flipping
// `AUTH_ENFORCE` — the per-route split tells them WHICH client read is still
// unmigrated, not merely that some read is.

// Anonymous serves on the FIVE identity routes, indexed by
// [`IDENTITY_ROUTES`] order — the soak signal that must reach zero. AtomicU64
// is not Copy, so the array is spelled out (a `[x; 5]` initializer won't do).
static ANON_BY_ROUTE: [AtomicU64; 5] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];
// Anonymous serves on PUBLIC routes — expected to stay non-zero (public routes
// are never gated), so NOT a migration blocker; surfaced separately so it
// never masks a stuck identity route.
static PUBLIC_SERVED: AtomicU64 = AtomicU64::new(0);
static AUTHENTICATED_SERVED: AtomicU64 = AtomicU64::new(0);
static AUTH_REFUSED: AtomicU64 = AtomicU64::new(0);
static MISMATCH_REFUSED: AtomicU64 = AtomicU64::new(0);
static STRICT_REFUSED_UNAUTHENTICATED: AtomicU64 = AtomicU64::new(0);
static MISCONFIGURED_REFUSED: AtomicU64 = AtomicU64::new(0);

/// Count an anonymous serve, split by route: `Some(i)` = the identity route at
/// index `i` (the migration-tracked bucket); `None` = a public route.
pub fn count_anonymous_served(identity_route: Option<usize>) {
    match identity_route {
        Some(i) if i < ANON_BY_ROUTE.len() => {
            ANON_BY_ROUTE[i].fetch_add(1, Ordering::Relaxed);
        }
        // A public route, or an out-of-range index (impossible via
        // identity_route_index, but fail toward the non-blocking bucket).
        _ => {
            PUBLIC_SERVED.fetch_add(1, Ordering::Relaxed);
        }
    }
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
    /// Anonymous serves per identity route, in [`IDENTITY_ROUTES`] order.
    pub anon_by_route: [u64; 5],
    pub public_served: u64,
    pub authenticated_served: u64,
    pub auth_refused: u64,
    pub mismatch_refused: u64,
    pub strict_refused_unauthenticated: u64,
    pub misconfigured_refused: u64,
}

pub fn counters_snapshot() -> AuthCountersSnapshot {
    let mut anon_by_route = [0u64; 5];
    for (dst, src) in anon_by_route.iter_mut().zip(ANON_BY_ROUTE.iter()) {
        *dst = src.load(Ordering::Relaxed);
    }
    AuthCountersSnapshot {
        anon_by_route,
        public_served: PUBLIC_SERVED.load(Ordering::Relaxed),
        authenticated_served: AUTHENTICATED_SERVED.load(Ordering::Relaxed),
        auth_refused: AUTH_REFUSED.load(Ordering::Relaxed),
        mismatch_refused: MISMATCH_REFUSED.load(Ordering::Relaxed),
        strict_refused_unauthenticated: STRICT_REFUSED_UNAUTHENTICATED.load(Ordering::Relaxed),
        misconfigured_refused: MISCONFIGURED_REFUSED.load(Ordering::Relaxed),
    }
}

/// The `/health` auth surface (Rule 13): the GLOBAL mode + configured +
/// counters, so "unauthenticated but accepted" is a NUMBER an operator watches
/// during the soak, never a silent accept. `anonymousByRoute` is keyed by the
/// route path so the operator sees exactly which identity read is unmigrated.
pub fn auth_health_json(
    mode: AuthMode,
    auth_configured: bool,
    c: &AuthCountersSnapshot,
) -> serde_json::Value {
    let anon_by_route: serde_json::Map<String, serde_json::Value> = IDENTITY_ROUTES
        .iter()
        .zip(c.anon_by_route.iter())
        .map(|(route, n)| ((*route).to_string(), json!(n)))
        .collect();
    json!({
        "authMode": mode.as_str(),
        "authConfigured": auth_configured,
        // Per-isolate since last isolate recycle — a soak signal, not a ledger.
        "countersScope": "isolate",
        // The migration-tracked bucket: every value must reach ~0 before the
        // flip. Public-route anonymity is a separate, non-blocking count.
        "anonymousByRoute": anon_by_route,
        "publicServed": c.public_served,
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
/// adapted to a read surface with a lenient rollout window + a public-route
/// exemption).
///
/// Strict enforcement gates ONLY the five identity-scoped routes: the
/// disposition runs against [`effective_mode`] (public routes forced lenient),
/// while the [`AuthState`] carries the GLOBAL mode — what `/health` reports and
/// what the identity seam reads (`effective == global` on identity routes, so
/// the seam is unaffected).
///
/// The middleware is only invoked when the request ATTEMPTS auth (handshake
/// path or `x-bsv-auth-*` headers — the middleware's OWN predicates, so the
/// dispatch cannot drift from what `process_auth` would treat as auth). It
/// always runs `allow_unauthenticated: false`: lenient-ness lives entirely in
/// [`front_door_disposition`], so an attempted-but-invalid signature is
/// REFUSED in both modes, never downgraded to anonymous. A PUBLIC route with a
/// valid auth attempt is still verified + signed (so `AuthFetch` works against
/// public routes); it just can never be REQUIRED to authenticate.
pub async fn front_door(req: Request, env: &Env) -> Result<FrontDoor> {
    let global_mode = AuthMode::from_flag(
        env.var("AUTH_ENFORCE")
            .ok()
            .map(|v| v.to_string())
            .as_deref(),
    );
    let path = req.path();
    let route_idx = identity_route_index(&path);
    // Public routes are exempt from strict enforcement (deliberate — see the
    // module docs). The disposition uses the effective (per-route) mode; the
    // state carries the global mode.
    let mode = effective_mode(global_mode, &path);
    let server_key = nonempty_var(env, "SERVER_PRIVATE_KEY");
    let auth_configured = server_key.is_some() && env.kv("AUTH_SESSIONS").is_ok();
    let is_handshake = CloudflareTransport::is_handshake_request(&req);
    let auth_attempted = is_handshake || CloudflareTransport::has_auth_headers(&req);

    match front_door_disposition(mode, auth_configured, auth_attempted) {
        Disposition::ProceedAnonymous => {
            // Split the soak signal: an anonymous serve on an identity route is
            // migration-tracked (`route_idx`); on a public route it's the
            // non-blocking `publicServed` bucket.
            count_anonymous_served(route_idx);
            Ok(FrontDoor::Proceed(
                req,
                AuthState {
                    mode: global_mode,
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
                    "authMode": global_mode.as_str(),
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
                    "authMode": global_mode.as_str(),
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
                                mode: global_mode,
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
            assert_eq!(
                AuthMode::from_flag(Some(strict)),
                AuthMode::Strict,
                "{strict:?}"
            );
        }
        for lenient in [
            None,
            Some(""),
            Some("false"),
            Some("0"),
            Some("yes"),
            Some("lenient"),
        ] {
            assert_eq!(
                AuthMode::from_flag(lenient),
                AuthMode::Lenient,
                "{lenient:?}"
            );
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
            anon_by_route: [7, 6, 5, 4, 3],
            public_served: 9,
            authenticated_served: 3,
            auth_refused: 2,
            mismatch_refused: 1,
            strict_refused_unauthenticated: 5,
            misconfigured_refused: 4,
        };
        let v = auth_health_json(AuthMode::Lenient, true, &snap);
        assert_eq!(v["authMode"], "lenient");
        assert_eq!(v["authConfigured"], true);
        // Per-route anon map, keyed by the actual route paths so the operator
        // sees WHICH read is unmigrated.
        assert_eq!(v["anonymousByRoute"]["/results"], 7);
        assert_eq!(v["anonymousByRoute"]["/refund-view"], 6);
        assert_eq!(v["anonymousByRoute"]["/live-view"], 5);
        assert_eq!(v["anonymousByRoute"]["/recovery-view"], 4);
        assert_eq!(v["anonymousByRoute"]["/hops-view"], 3);
        // Exactly the five identity routes appear — no more, no less (a new
        // identity route without a counter would show up as a missing key).
        assert_eq!(v["anonymousByRoute"].as_object().unwrap().len(), 5);
        assert_eq!(v["publicServed"], 9);
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
        // An anonymous serve on identity route index 2 (/live-view) and a
        // public one — the split must land in the right bucket.
        count_anonymous_served(Some(2));
        count_anonymous_served(None);
        count_authenticated_served();
        count_mismatch_refused();
        let after = counters_snapshot();
        assert!(after.anon_by_route[2] > before.anon_by_route[2]);
        assert!(after.public_served > before.public_served);
        assert!(after.authenticated_served > before.authenticated_served);
        assert!(after.mismatch_refused > before.mismatch_refused);
    }

    // ── route classification / effective mode ──────────────────────────────

    #[test]
    fn only_the_five_identity_routes_require_auth() {
        for r in IDENTITY_ROUTES {
            assert!(route_requires_identity_auth(r), "{r} must require auth");
            assert!(identity_route_index(r).is_some());
        }
        for public in [
            "/health",
            "/leaderboard",
            "/utxo-status",
            "/pots-view",
            "/beef/abc",
            "/tip",
            "/spent-any",
            "/tx-any/abc",
            "/",
            "/.well-known/auth",
        ] {
            assert!(
                !route_requires_identity_auth(public),
                "{public} must be public (never gated)"
            );
            assert!(identity_route_index(public).is_none());
        }
    }

    #[test]
    fn identity_route_index_matches_the_route_order() {
        for (i, r) in IDENTITY_ROUTES.iter().enumerate() {
            assert_eq!(identity_route_index(r), Some(i));
        }
    }

    #[test]
    fn effective_mode_forces_public_routes_lenient_but_keeps_global_on_identity() {
        // Strict global: identity routes stay strict; public routes go lenient.
        assert_eq!(
            effective_mode(AuthMode::Strict, "/results"),
            AuthMode::Strict
        );
        for public in ["/health", "/leaderboard", "/utxo-status", "/tip", "/"] {
            assert_eq!(
                effective_mode(AuthMode::Strict, public),
                AuthMode::Lenient,
                "{public} must be exempt from strict enforcement"
            );
        }
        // Lenient global: everything is lenient (the flip is off).
        for any in ["/results", "/health", "/tip"] {
            assert_eq!(effective_mode(AuthMode::Lenient, any), AuthMode::Lenient);
        }
    }

    /// The exemption's PAYOFF, made executable: under strict global, an
    /// anonymous request to a public route is NEVER refused (it proceeds),
    /// while an anonymous request to an identity route IS refused. This is the
    /// #316-shape outage the coordinator flagged — pinned as a behavior.
    #[test]
    fn strict_global_refuses_identity_but_serves_public_anonymously() {
        let global = AuthMode::Strict;
        // configured worker, no auth attempted (anonymous):
        for identity in IDENTITY_ROUTES {
            let m = effective_mode(global, identity);
            assert_eq!(
                front_door_disposition(m, true, false),
                Disposition::RefuseUnauthenticated,
                "identity route {identity} must refuse anonymous under strict"
            );
        }
        for public in ["/health", "/leaderboard", "/utxo-status", "/tip"] {
            let m = effective_mode(global, public);
            assert_eq!(
                front_door_disposition(m, true, false),
                Disposition::ProceedAnonymous,
                "public route {public} must serve anonymous even under strict"
            );
        }
    }
}
