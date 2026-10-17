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
pub const IDENTITY_ROUTES: [&str; 6] = [
    "/results",
    "/refund-view",
    "/live-view",
    "/recovery-view",
    "/hops-view",
    "/owed", // bsv-low #469: the owed list, an identity read like its four inputs
];

/// True iff `path` is one of the identity-scoped views. Every other route is
/// PUBLIC (never refused for lack of auth — see the module docs' deliberate
/// exemption; the overlay `/lookup` already serves the same data
/// unauthenticated, so gating them buys only quota-readiness).
pub fn route_requires_identity_auth(path: &str) -> bool {
    identity_route_index(path).is_some()
}

/// bsv-low #441 (2026-09-10): the two FREE WRITE routes. Under
/// `AUTH_ENFORCE=true` an anonymous POST here is refused like an anonymous
/// identity read (the poster becomes cryptographic: a BRC-104 session or the
/// session lane); the GET twin of `/proof` stays public (evidence anyone may
/// verify). Their anonymous serves are counted apart (`anonymousWriteByRoute`)
/// so the flip criterion can be read per route. Append, never reorder.
pub const WRITE_ROUTES: [&str; 2] = ["/record", "/proof"];

/// The write-route index of `(method, path)`, or `None` for everything else.
pub fn write_route_index(method_is_post: bool, path: &str) -> Option<usize> {
    if !method_is_post {
        return None;
    }
    WRITE_ROUTES.iter().position(|r| *r == path)
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
static ANON_BY_ROUTE: [AtomicU64; 6] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0), // /owed (#469)
];
// Anonymous serves on PUBLIC routes — expected to stay non-zero (public routes
// are never gated), so NOT a migration blocker; surfaced separately so it
// never masks a stuck identity route.
static ANON_WRITE_BY_ROUTE: [AtomicU64; 2] = [AtomicU64::new(0), AtomicU64::new(0)];
/// bsv-low #441: calls verified on the session lane (zero wallet calls, one
/// store round trip) — the migration signal beside `authenticatedServed`.
static LANE_SERVED: AtomicU64 = AtomicU64::new(0);
static PUBLIC_SERVED: AtomicU64 = AtomicU64::new(0);
static AUTHENTICATED_SERVED: AtomicU64 = AtomicU64::new(0);
/// EVERY front-door refusal, the middleware's arms and the app's three (each of those also counted apart below): the
/// isolate twin of the durable `applayer_auth_refused_total`, bumped by the one [`note_auth_refused`] (the
/// W1-OBS-APPLAYER lens's L2; before it this counted the middleware's arms only).
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
pub fn count_anonymous_write_served(write_route: usize) {
    if write_route < ANON_WRITE_BY_ROUTE.len() {
        ANON_WRITE_BY_ROUTE[write_route].fetch_add(1, Ordering::Relaxed);
    }
}
pub fn count_lane_served() {
    LANE_SERVED.fetch_add(1, Ordering::Relaxed);
}
/// `reason`: the refusal's class in its durable name (bsv-low #628, [`AUTH_REFUSAL_REASONS`]).
pub fn count_auth_refused(reason: &str) {
    note_auth_refused(reason);
}
pub fn count_mismatch_refused() {
    MISMATCH_REFUSED.fetch_add(1, Ordering::Relaxed);
    note_auth_refused("mismatch");
}
pub fn count_strict_refused_unauthenticated() {
    STRICT_REFUSED_UNAUTHENTICATED.fetch_add(1, Ordering::Relaxed);
    note_auth_refused("strict_unauthenticated");
}
pub fn count_misconfigured_refused() {
    MISCONFIGURED_REFUSED.fetch_add(1, Ordering::Relaxed);
    note_auth_refused("misconfigured");
}

/// bsv-low #628 (the observability bar, issue 4): every front-door refusal by reason, mirrored on the overlay's
/// `ops_counters` as `applayer_auth_refused_<reason>_total` beside `applayer_auth_refused_total`, through the
/// courier census's batched flush. The session lane's refusals (`ERR_SESSION_REFUSED`) are `lane_<reason>` by the
/// middleware's own reason; the reference door's 401s are their code; `handshake_fault` is a middleware `Err`
/// (a fault, the 400), `no_session` its contract break (the 500). The isolate fields on `/health.auth` stay;
/// `authRefused` counts the same 19 as the durable total.
pub const AUTH_REFUSAL_REASONS: [&str; 19] = [
    "lane_unknown_session",
    "lane_expired",
    "lane_replay",
    "lane_bad_mac",
    "lane_malformed",
    "lane_unsupported",
    "lane_unavailable",
    "lane_other",
    "unauthorized",
    "invalid_auth",
    "session_not_found",
    "replayed_request",
    "server_misconfigured",
    "middleware_other",
    "handshake_fault",
    "no_session",
    "mismatch",
    "strict_unauthenticated",
    "misconfigured",
];

/// One refusal: the isolate's `authRefused` and the durable total with its reason, together.
fn note_auth_refused(reason: &str) {
    AUTH_REFUSED.fetch_add(1, Ordering::Relaxed);
    crate::courier::note_presence("applayer_auth_refused_total");
    crate::courier::note_presence(&format!("applayer_auth_refused_{reason}_total"));
}

/// Every auth name this worker mirrors (the overlay seeds each at 0).
pub fn auth_mirrored_counter_names() -> Vec<String> {
    std::iter::once("applayer_auth_refused_total".to_string())
        .chain(
            AUTH_REFUSAL_REASONS
                .iter()
                .map(|r| format!("applayer_auth_refused_{r}_total")),
        )
        .collect()
}

/// PURE: the reason of one middleware refusal, read off its body (`{"code": .., "reason": ..}`): the session
/// lane's `ERR_SESSION_REFUSED` by its reason, the reference door's 401 by its code; an unread body or a code
/// this list does not know is `middleware_other` (the cardinality stays bounded).
pub fn middleware_refusal_reason(body: &[u8]) -> &'static str {
    let v: serde_json::Value = serde_json::from_slice(body).unwrap_or_default();
    match v["code"].as_str().unwrap_or("") {
        "ERR_SESSION_REFUSED" => match v["reason"].as_str().unwrap_or("") {
            "unknown-session" => "lane_unknown_session",
            "expired" => "lane_expired",
            "replay" => "lane_replay",
            "bad-mac" => "lane_bad_mac",
            "malformed" => "lane_malformed",
            "lane-unsupported" => "lane_unsupported",
            "lane-unavailable" => "lane_unavailable",
            _ => "lane_other",
        },
        "UNAUTHORIZED" => "unauthorized",
        "ERR_INVALID_AUTH" => "invalid_auth",
        "ERR_SESSION_NOT_FOUND" => "session_not_found",
        "ERR_REPLAYED_REQUEST" => "replayed_request",
        "ERR_SERVER_MISCONFIGURED" => "server_misconfigured",
        _ => "middleware_other",
    }
}

/// The reason of a middleware `Response`, from the bytes it already holds (no read of the stream).
fn response_refusal_reason(resp: &Response) -> &'static str {
    match resp.body() {
        worker::ResponseBody::Body(bytes) => middleware_refusal_reason(bytes),
        _ => "middleware_other",
    }
}

/// A point-in-time copy of the counters (pure input to the health JSON so the
/// renderer is natively testable against explicit values).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AuthCountersSnapshot {
    /// Anonymous serves per identity route, in [`IDENTITY_ROUTES`] order.
    pub anon_by_route: [u64; 5],
    /// Anonymous serves per free write route, in [`WRITE_ROUTES`] order (#441).
    pub anon_write_by_route: [u64; 2],
    /// Calls verified on the session lane (#441).
    pub lane_served: u64,
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
    let mut anon_write_by_route = [0u64; 2];
    for (dst, src) in anon_write_by_route
        .iter_mut()
        .zip(ANON_WRITE_BY_ROUTE.iter())
    {
        *dst = src.load(Ordering::Relaxed);
    }
    AuthCountersSnapshot {
        anon_by_route,
        anon_write_by_route,
        lane_served: LANE_SERVED.load(Ordering::Relaxed),
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
    let anon_write_by_route: serde_json::Map<String, serde_json::Value> = WRITE_ROUTES
        .iter()
        .zip(c.anon_write_by_route.iter())
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
        // #441: the free write routes' anonymous serves and the lane's serves.
        "anonymousWriteByRoute": anon_write_by_route,
        "laneServed": c.lane_served,
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
    process_auth, process_auth_lane, request_presents_lane, session_lane::LaneOffer,
    AuthMiddlewareOptions, AuthResult, AuthSession, CloudflareTransport, LaneAuth, LaneAuthResult,
    SessionLaneOptions,
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
    /// The request body the BRC-104 middleware CONSUMED while verifying the
    /// caller's signature over it (`AuthResult::Authenticated.body`), on the
    /// authenticated lane; `None` on the plain lane, where the request's own
    /// body is untouched. A POST handler must read THIS before falling back
    /// to `req.bytes()` — the first real proof-in-DB hand (beta, 2026-09-02,
    /// game `f1cad298…`) was refused 400 "body has already been read"
    /// because the app-layer's first POST route read the request after the
    /// middleware had.
    pub body: Option<Vec<u8>>,
    /// bsv-low #441: present iff the caller rode the SESSION LANE (verified by
    /// the lane's store, zero wallet calls); the reply is then SEALED under the
    /// lane (`seal_lane_response_text`) instead of signed. Never both `session`
    /// and `lane`.
    pub lane: Option<LaneAuth>,
    /// bsv-low #441: a lane minted for THIS BRC-104-verified request (it carried
    /// the ask): the offer rides the signed reply's `x-bsv-lane-offer` header
    /// (a signable header: the reference signs it). Only ever set beside
    /// `session`, never beside `lane`.
    pub lane_offer: Option<LaneOffer>,
    /// bsv-low #469: the fetch event's context, for work AFTER the answer (`wait_until`) — the owed list's read
    /// serves the rows in hand and refreshes them in the background; `None` outside a fetch event (tests).
    pub wait: Option<std::rc::Rc<worker::Context>>,
}

/// The lane's store binding name (the relay's, the tower's, ours: one name).
pub const AUTH_SESSION_STORE_BINDING: &str = "AUTH_SESSION_STORE";

/// The session lane is ON when the operator flag says so AND the store is bound
/// (`AUTH_SESSION_STORE`); a flag without a binding never mints and never lanes.
pub fn session_lane_configured(env: &Env) -> bool {
    let flag = env
        .var("SESSION_LANE")
        .ok()
        .map(|v| v.to_string().trim().to_ascii_lowercase());
    let on = matches!(flag.as_deref(), Some("1") | Some("true"));
    on && env.durable_object(AUTH_SESSION_STORE_BINDING).is_ok()
}

/// The ONLY `Verified` constructor call site — fed exclusively by the
/// middleware's verified result (a BRC-104 signature, or the session lane's
/// store verdict), Rule 8b. `session` and `lane` are mutually exclusive by
/// construction: the BRC-104 arm passes the session, the lane arm the lane.
fn verified_state(
    mode: AuthMode,
    identity_key: &str,
    auth_configured: bool,
    session: Option<AuthSession>,
    body: Vec<u8>,
    lane: Option<LaneAuth>,
    lane_offer: Option<LaneOffer>,
) -> AuthState {
    AuthState {
        mode,
        caller: CallerAuth::verified(identity_key),
        auth_configured,
        session,
        lane_offer,
        body: Some(body),
        lane,
        wait: None,
    }
}

/// Front-door outcome: either proceed to the router with resolved state, or
/// reply immediately (handshake reply, 401, 503, middleware error).
// The state rides one request and dies with it; boxing it would touch every
// match site for nothing (the lane's `LaneAuth` widened the variant, #441).
#[allow(clippy::large_enum_variant)]
pub enum FrontDoor {
    Proceed(Request, AuthState),
    Reply(Response),
}

pub(crate) fn json_reply(status: u16, body: &serde_json::Value) -> Result<Response> {
    let mut resp = Response::ok(body.to_string())?.with_status(status);
    resp.headers_mut().set("Content-Type", "application/json")?;
    resp.headers_mut().set("Cache-Control", "no-store")?;
    Ok(resp)
}

// ── bsv-low #670: a store FAULT at the auth door is a retryable 503 ──────────

/// What an `Err` out of `process_auth*` is. Since the middleware's 0.4.1 every
/// REFUSAL it decides is its own 401 `Response`, so an `Err` is a fault; this
/// splits the faults the client should retry (the session store could not be
/// asked or written) from the rest, which keep the door's 400 byte for byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthFaultClass {
    /// The session store (KV, the `AuthSessionStore` DO: its stub, its fetch,
    /// its status, its body) could not be asked or written: 503, `Retry-After`.
    Transport,
    /// Everything else, kept at the pre-#670 400 byte for byte. Not only the
    /// caller's faults: server and operator faults land here too (`ConfigError`:
    /// a missing binding or a bad server key; `SdkError`: the session nonce;
    /// `TransportError`: a body read or a Response build; `SerializationError`).
    /// None of these heals by waiting two seconds, so none is told to retry.
    Caller,
}

/// Classify by VARIANT (bsv-middleware-cloudflare 0.6.2's `AuthCloudflareError`,
/// the version in the workers lock; `error.rs`, `middleware/auth.rs` and
/// `storage/` are byte-identical to 0.6.0's). The match is exhaustive on
/// purpose: a crate bump that adds a variant does not compile until it is
/// classified. Every store fault the middleware raises is `KvError`
/// (`storage/kv_session.rs`, `storage/do_session.rs`, and `From<worker::Error>`,
/// which maps EVERY bare-`?` worker error to it). The converse does not hold in
/// the crate: a caller's over-long nonce or handshake body also comes back
/// `KvError` (the platform refusing the key or value), so the door refuses an
/// over-long input first (`auth_input_guard`, the lens's HIGH-1). The guard
/// bounds each input once; it does not make every `KvError` the store's: on the
/// DO backend a caller's own session cell ACCUMULATES its consumed nonces (up
/// to the crate's `CONSUMED_CAP`, consumed before the signature is checked, the
/// whole cell one value under the 128 KiB cap), so the guard's bounds are sized
/// for the cell (round 2's R2-HIGH-1, pinned by `s670_i`), and the crate's own
/// fix (a cell capped in bytes, or the consume after the signature) is asked
/// upstream: <https://github.com/Calhooon/bsv-middleware-cloudflare/issues/14>.
pub fn auth_fault_class(e: &bsv_middleware_cloudflare::AuthCloudflareError) -> AuthFaultClass {
    use bsv_middleware_cloudflare::AuthCloudflareError as E;
    match e {
        E::KvError(_) => AuthFaultClass::Transport,
        E::Unauthorized
        | E::InvalidAuthentication(_)
        | E::SessionNotFound(_)
        | E::ServerMisconfigured
        | E::PaymentInternal(_)
        | E::PaymentRequired { .. }
        | E::MalformedPayment(_)
        | E::InvalidDerivationPrefix
        | E::PaymentFailed(_)
        | E::InvalidPayment(_)
        | E::SdkError(_)
        | E::TransportError(_)
        | E::ConfigError(_)
        | E::SerializationError(_) => AuthFaultClass::Caller,
    }
}

/// The 503's `Retry-After`, in seconds.
pub const AUTH_UNAVAILABLE_RETRY_AFTER: &str = "2";
/// The 503's code.
pub const AUTH_UNAVAILABLE_CODE: &str = "ERR_AUTH_UNAVAILABLE";
/// The durable `ops_counters` row a store fault at the door bumps (through the
/// courier census's batched flush, never one D1 write per fault). It is the
/// ONLY count of a store fault: the isolate `/health` auth tally holds none
/// (not `authRefused`, since a store fault is not a refusal; not
/// `authenticated`), stated (the lens's LOW-3).
pub const COUNTER_AUTH_HANDSHAKE_UNAVAILABLE: &str = "auth_handshake_unavailable_total";

/// The door's answer to an `Err` out of `process_auth*`: `(status, body,
/// Retry-After)`. A store fault names its class only, never the error's text
/// (it carries the platform's reference id). Any other fault is the pre-#670
/// 400, `authentication handshake failed: {e}`.
pub fn auth_fault_answer(
    e: &bsv_middleware_cloudflare::AuthCloudflareError,
) -> (u16, serde_json::Value, Option<&'static str>) {
    match auth_fault_class(e) {
        AuthFaultClass::Transport => (
            503,
            json!({
                "error": "authentication handshake unavailable: session store",
                "code": AUTH_UNAVAILABLE_CODE,
            }),
            Some(AUTH_UNAVAILABLE_RETRY_AFTER),
        ),
        AuthFaultClass::Caller => (
            400,
            json!({ "error": format!("authentication handshake failed: {e}") }),
            None,
        ),
    }
}

/// The headers a fault's answer carries beside `json_reply`'s: the store
/// fault's `Retry-After` (the lens's HIGH-3: one list the reply renders and a
/// pin reads, so a dropped hint reds), none for any other fault (the pre-#670
/// bytes). CORS is stamped after, on every door reply (`serve`'s `stamp_cors`).
pub fn auth_fault_headers(class: AuthFaultClass) -> &'static [(&'static str, &'static str)] {
    match class {
        AuthFaultClass::Transport => &[("Retry-After", AUTH_UNAVAILABLE_RETRY_AFTER)],
        AuthFaultClass::Caller => &[],
    }
}

/// The door's reply to an `Err` out of `process_auth*`: [`auth_fault_answer`]
/// rendered with [`auth_fault_headers`].
pub fn auth_fault_reply(e: &bsv_middleware_cloudflare::AuthCloudflareError) -> Result<Response> {
    let (status, body, _) = auth_fault_answer(e);
    let mut resp = json_reply(status, &body)?;
    for (name, value) in auth_fault_headers(auth_fault_class(e)) {
        resp.headers_mut().set(name, value)?;
    }
    Ok(resp)
}

/// The store fault's one log line: the crate's CODE (`ERR_STORAGE`), never
/// the error's text: the DO's fault carries the platform's `reference = …`
/// id, and under HIGH-1 the text was the caller's (the lens's HIGH-2).
pub fn auth_unavailable_log_line(e: &bsv_middleware_cloudflare::AuthCloudflareError) -> String {
    format!("[auth] handshake unavailable: {}", e.error_code())
}

// ── bsv-low #670 fold (lens HIGH-1): a caller's own bytes never reach the store ──
//
// The middleware (0.6.2) uses the caller's nonce headers as storage keys and
// values without a shape check: `x-bsv-auth-your-nonce` (or `x-bsv-auth-nonce`
// when it is absent) is the session key (`auth:session:<nonce>`, the DO's
// `id_from_name`), `x-bsv-auth-nonce` the consumed-nonce key
// (`auth:nonce:<session>:<nonce>`), and a handshake's `initialNonce` is stored
// whole as the session's `peer_nonce`. An over-long one makes Cloudflare refuse
// the key (KV: 512 bytes) or the value (a KV-backed DO: 128 KiB), which the
// crate raises as `KvError`, the store-fault class: a stranger could raise the
// 503 (and `auth_handshake_unavailable_total`) at will. The door refuses those
// inputs FIRST, with the pre-#670 400.
//
// The bounds are sized for the DO cell, not one input (round 2's R2-HIGH-1):
// the cell holds the session (its `peer_nonce` from the handshake body) and up
// to `CONSUMED_CAP` consumed nonces, each consumed before the signature is
// checked, so a stranger on its own session fills its own cell with unsigned
// messages. `CONSUMED_CAP x (AUTH_NONCE_MAX_BYTES + 16) +
// AUTH_HANDSHAKE_BODY_MAX_BYTES + 1,024` is 103,424 bytes, under 131,072
// (`s670_i`); at the old 128 / 16 KiB it was 164,864 and the lens's model of
// the structured clone 139,952 to 155,909, so the cell overran and every later
// request on that session was the 503 (and a count) until the session's TTL.

/// The longest nonce header handed to the middleware. The SDK's per-request
/// nonce (`x-bsv-auth-nonce`, 32 random bytes, base64) is 44 characters; its
/// `createNonce` (a handshake's `initialNonce`) and the server's session nonce
/// (`x-bsv-auth-your-nonce`, the middleware's `create_session_nonce`) are 16
/// random bytes + a 32-byte HMAC, base64: 64 characters. 80 leaves room for any
/// encoding, keeps every derived KV key under the 512-byte limit, and keeps the
/// worst DO session cell under the 128 KiB value cap (`s670_i`).
pub const AUTH_NONCE_MAX_BYTES: usize = 80;
/// The longest `/.well-known/auth` body handed to the middleware. A real
/// InitialRequest is 266 bytes (`s670_e` builds and measures one); the app
/// layer requests no certificates, so no honest handshake carries a
/// certificate list. The reference bounds an auth message at 16 MiB
/// (`MAX_AUTH_MESSAGE_BYTES`) and a handshake RESPONSE at 1 MiB; 4 KiB is
/// ours: a certificate-bearing request (5 certifiers x 5 types x 5 fields)
/// measured 1,049 bytes with the client's SDK 2.1.4 (bsv-low
/// `docs/audit/W1-DOOR-670-lens-2-2026-10-17.md`, question 1), and the body
/// bounds the session's `peer_nonce` in the cell (`s670_i`).
pub const AUTH_HANDSHAKE_BODY_MAX_BYTES: usize = 4 * 1024;
/// Every `x-bsv-auth-*` header the middleware turns into a storage key or a
/// stored value. The others are parsed to a fixed shape before any store call
/// (the identity key a public key, the request id 32 bytes, the signature
/// bytes, the requested certificates JSON never stored) or only compared.
pub const AUTH_NONCE_HEADERS: [&str; 3] = [
    bsv_middleware_cloudflare::auth_headers::NONCE,
    bsv_middleware_cloudflare::auth_headers::YOUR_NONCE,
    bsv_middleware_cloudflare::auth_headers::INITIAL_NONCE,
];
/// The durable `ops_counters` row an oversize auth input bumps (the census's
/// batched flush), apart from the store-fault row.
pub const COUNTER_AUTH_HANDSHAKE_OVERSIZE: &str = "auth_handshake_oversize_total";

/// The first nonce header over [`AUTH_NONCE_MAX_BYTES`], named.
pub fn auth_nonce_oversize(get: impl Fn(&str) -> Option<String>) -> Option<String> {
    AUTH_NONCE_HEADERS.iter().find_map(|h| {
        get(h)
            .filter(|v| v.len() > AUTH_NONCE_MAX_BYTES)
            .map(|_| format!("{h} over {AUTH_NONCE_MAX_BYTES} bytes"))
    })
}

/// The handshake body's reason, when its length is over the bound.
pub fn handshake_body_oversize(len: usize) -> Option<String> {
    (len > AUTH_HANDSHAKE_BODY_MAX_BYTES)
        .then(|| format!("handshake body over {AUTH_HANDSHAKE_BODY_MAX_BYTES} bytes"))
}

/// Append a chunk of the handshake body; `false` once the bound is passed (the
/// read stops there, so an oversize body is never held whole).
pub fn push_bounded(buf: &mut Vec<u8>, chunk: &[u8]) -> bool {
    if buf.len() + chunk.len() > AUTH_HANDSHAKE_BODY_MAX_BYTES {
        return false;
    }
    buf.extend_from_slice(chunk);
    true
}

/// The oversize answer: the door's pre-#670 400, a caller fault, never the 503.
pub fn auth_oversize_answer(reason: &str) -> (u16, serde_json::Value) {
    (
        400,
        json!({ "error": format!("authentication handshake failed: {reason}") }),
    )
}

/// Read a handshake body up to the bound and hand the middleware a request
/// with the SAME method, URL, headers and bytes. `Err(reason)` when it is over.
async fn bound_handshake_body(mut req: Request) -> Result<std::result::Result<Request, String>> {
    use futures_util::StreamExt;
    let declared = req
        .headers()
        .get("content-length")
        .ok()
        .flatten()
        .and_then(|v| v.trim().parse::<usize>().ok());
    if let Some(reason) = declared.and_then(handshake_body_oversize) {
        return Ok(Err(reason));
    }
    let mut buf = Vec::new();
    // A request with no body has no stream: the middleware reads it as empty.
    if let Ok(mut stream) = req.stream() {
        while let Some(chunk) = stream.next().await {
            let Ok(chunk) = chunk else {
                return Ok(Err("handshake body could not be read".into()));
            };
            if !push_bounded(&mut buf, &chunk) {
                return Ok(Err(handshake_body_oversize(
                    AUTH_HANDSHAKE_BODY_MAX_BYTES + 1,
                )
                .unwrap_or_default()));
            }
        }
    }
    let mut init = worker::RequestInit::new();
    init.with_method(req.method());
    init.with_headers(req.headers().clone());
    if !buf.is_empty() {
        init.with_body(Some(
            worker::js_sys::Uint8Array::from(buf.as_slice()).into(),
        ));
    }
    Request::new_with_init(req.url()?.as_str(), &init).map(Ok)
}

/// The guard the door runs BEFORE `process_auth*`: an over-long nonce header
/// or handshake body is the 400 (counted `auth_handshake_oversize_total`, and
/// in the isolate tally as the refusal the old 400 was); anything else passes,
/// a handshake re-built byte for byte.
pub async fn auth_input_guard(req: Request) -> Result<std::result::Result<Request, Response>> {
    let reason = if let Some(r) = auth_nonce_oversize(|h| req.headers().get(h).ok().flatten()) {
        r
    } else if CloudflareTransport::is_handshake_request(&req) {
        match bound_handshake_body(req).await? {
            Ok(rebuilt) => return Ok(Ok(rebuilt)),
            Err(r) => r,
        }
    } else {
        return Ok(Ok(req));
    };
    crate::courier::note_presence(COUNTER_AUTH_HANDSHAKE_OVERSIZE);
    // The old 400 was the middleware's `Err`: the same durable reason (#628).
    count_auth_refused("handshake_fault");
    let (status, body) = auth_oversize_answer(&reason);
    json_reply(status, &body).map(Err)
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
    // #441: a free WRITE route is judged by the GLOBAL mode (never lenient by
    // the public-route exemption); its anonymous serves are counted apart.
    let write_idx = write_route_index(req.method() == worker::Method::Post, &path);
    let mode = if write_idx.is_some() {
        global_mode
    } else {
        mode
    };
    let server_key = nonempty_var(env, "SERVER_PRIVATE_KEY");
    let auth_configured = server_key.is_some() && env.kv("AUTH_SESSIONS").is_ok();
    let is_handshake = CloudflareTransport::is_handshake_request(&req);
    let lane_on = session_lane_configured(env);
    // #441: a presented lane is an auth ATTEMPT — judged by the middleware,
    // never silently anonymous (the middleware's predicate: no header read
    // in this crate).
    let auth_attempted = is_handshake
        || CloudflareTransport::has_auth_headers(&req)
        || (lane_on && request_presents_lane(&req));

    match front_door_disposition(mode, auth_configured, auth_attempted) {
        Disposition::ProceedAnonymous => {
            // Split the soak signal: an anonymous serve on an identity route is
            // migration-tracked (`route_idx`), on a free write route its own
            // bucket (#441); on a public route it's the non-blocking
            // `publicServed` bucket.
            match write_idx {
                Some(w) => count_anonymous_write_served(w),
                None => count_anonymous_served(route_idx),
            }
            Ok(FrontDoor::Proceed(
                req,
                AuthState {
                    mode: global_mode,
                    caller: CallerAuth::Anonymous,
                    auth_configured,
                    session: None,
                    body: None,
                    lane: None,
                    lane_offer: None,
                    wait: None,
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
                // #441: the lane offer on the first SIGNED general message
                // that asks (never the unsigned handshake), the laned calls
                // verified by the store — only when the operator turned it on
                // AND the store is bound; otherwise the reference door.
                session_lane: if lane_on {
                    Some(SessionLaneOptions::default())
                } else {
                    None
                },
                ..Default::default()
            };
            // ONE outcome shape for both doors: the lane's `Laned` arm folds
            // into the same verified-caller construction as BRC-104's.
            // bsv-low #670 fold (HIGH-1): an over-long nonce or handshake body
            // is the caller's 400 here, before the middleware can turn it into
            // a store fault.
            let req = match auth_input_guard(req).await? {
                Ok(req) => req,
                Err(resp) => return Ok(FrontDoor::Reply(resp)),
            };
            let outcome = if lane_on {
                process_auth_lane(req, env, &opts, AUTH_SESSION_STORE_BINDING).await
            } else {
                process_auth(req, env, &opts)
                    .await
                    .map(LaneAuthResult::Reference)
            };
            // bsv-low #588 (middleware 0.4.1): every authentication REFUSAL
            // is the middleware's own 401 on the `Response` arm below (a
            // malformed handshake, a signature that does not verify, NEW-1's
            // identity that is not the session's; 0.3.8 raised those as `Err`
            // and they were this 400). An `Err` is now a FAULT only (the
            // session store, the server's key, the transport, the SDK); since
            // bsv-low #670 a store fault is the retryable 503 and every other
            // fault the 400 a 0.3.8 fault got, never a bare 500.
            let mut pending_offer: Option<LaneOffer> = None;
            let auth = match outcome {
                Ok(LaneAuthResult::Laned {
                    context,
                    request,
                    body,
                    lane,
                }) => {
                    count_lane_served();
                    return Ok(FrontDoor::Proceed(
                        request,
                        verified_state(
                            global_mode,
                            &context.identity_key,
                            auth_configured,
                            None,
                            body,
                            Some(lane),
                            None,
                        ),
                    ));
                }
                Ok(LaneAuthResult::Reference(a)) => a,
                // #441: this BRC-104-verified request carried the ask and the
                // store minted a lane under the identity the reference PROVED;
                // the offer rides the signed reply (`lane_offer`).
                Ok(LaneAuthResult::Offered { auth, offer }) => {
                    pending_offer = Some(offer);
                    auth
                }
                // bsv-low #670: a store fault is the retryable 503 (counted
                // durably, logged by its code); any other fault the 400.
                Err(e) => {
                    match auth_fault_class(&e) {
                        AuthFaultClass::Transport => {
                            crate::courier::note_presence(COUNTER_AUTH_HANDSHAKE_UNAVAILABLE);
                            worker::console_warn!("{}", auth_unavailable_log_line(&e));
                        }
                        AuthFaultClass::Caller => {
                            count_auth_refused("handshake_fault");
                        }
                    }
                    return auth_fault_reply(&e).map(FrontDoor::Reply);
                }
            };
            match auth {
                AuthResult::Authenticated {
                    context,
                    request,
                    session,
                    // The signature covered these bytes; a POST handler reads
                    // them from `AuthState.body` (the request's own body is
                    // spent).
                    body,
                } => match session {
                    Some(session) => {
                        count_authenticated_served();
                        Ok(FrontDoor::Proceed(
                            request,
                            verified_state(
                                global_mode,
                                &context.identity_key,
                                auth_configured,
                                Some(session),
                                body,
                                None,
                                pending_offer,
                            ),
                        ))
                    }
                    // `allow_unauthenticated: false` means an Authenticated
                    // result always carries a session; a missing one is a
                    // middleware contract break — refuse, never serve as
                    // anonymous-with-identity.
                    None => {
                        count_auth_refused("no_session");
                        json_reply(
                            500,
                            &json!({ "error": "auth middleware returned an authenticated result without a session" }),
                        )
                        .map(FrontDoor::Reply)
                    }
                },
                // Handshake reply or a middleware refusal (401 / certificate
                // flow) — return verbatim; the middleware already stamped its
                // CORS. Count refusals (a handshake reply is not a refusal; a
                // handshake message the middleware REFUSED is one, a 401 since
                // 0.4.1, counted as its `Err` was before, bsv-low #588).
                AuthResult::Response(resp) => {
                    if !is_handshake || resp.status_code() == 401 {
                        count_auth_refused(response_refusal_reason(&resp));
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

    // ── bsv-low #670: a store fault at the auth door is a retryable 503 ──────

    /// A session store whose writes fail the way Cloudflare's KV failed on
    /// 2026-10-15 (`KV PUT failed: 500`) and whose reads fail the way the DO
    /// fetch did (`auth-session-store fetch: Error: internal error; reference = …`).
    struct FailingStore;

    const KV_PUT_500: &str = "js error: KV PUT failed: 500 Internal Server Error";
    const DO_FETCH_FAULT: &str =
        "auth-session-store fetch: Error: internal error; reference = dc291hl7example";

    type Stored = bsv_middleware_cloudflare::types::StoredSession;
    fn kv(text: &str) -> bsv_middleware_cloudflare::AuthCloudflareError {
        bsv_middleware_cloudflare::AuthCloudflareError::KvError(text.into())
    }

    #[async_trait::async_trait(?Send)]
    impl bsv_middleware_cloudflare::SessionStorage for FailingStore {
        async fn get_session(&self, _: &str) -> bsv_middleware_cloudflare::Result<Option<Stored>> {
            Err(kv(DO_FETCH_FAULT))
        }
        async fn get_session_by_identity(
            &self,
            k: &str,
        ) -> bsv_middleware_cloudflare::Result<Option<Stored>> {
            self.get_session(k).await
        }
        async fn save_session(&self, _: &Stored) -> bsv_middleware_cloudflare::Result<()> {
            // `kv_session.rs` maps the worker error through `KvError(e.to_string())`.
            Err(kv(KV_PUT_500))
        }
        async fn update_session(&self, s: &Stored) -> bsv_middleware_cloudflare::Result<()> {
            self.save_session(s).await
        }
        async fn try_consume_nonce(
            &self,
            _: &str,
            _: &str,
            _: Option<u64>,
        ) -> bsv_middleware_cloudflare::Result<bool> {
            Err(kv(KV_PUT_500))
        }
        async fn release_nonce(&self, _: &str, _: &str) -> bsv_middleware_cloudflare::Result<()> {
            Err(kv(KV_PUT_500))
        }
    }

    /// (c, the 503) The CLASSIFIER: a `KvError` of the shape the store raises
    /// (built by a port fake, no middleware code run) is answered 503
    /// `ERR_AUTH_UNAVAILABLE` with `Retry-After: 2`, the body naming the class
    /// only (never the platform's reference id). Before #670: 400. The
    /// middleware's real path over a failing store is `tests/door_wasm.rs`
    /// (wasm32, Node).
    #[tokio::test]
    async fn s670_a_the_classifier_answers_a_store_fault_503() {
        use bsv_middleware_cloudflare::SessionStorage;
        // Every field by hand (`StoredSession::new` reads the JS clock).
        let session = Stored {
            session_nonce: "s-nonce".into(),
            peer_identity_key: ID_A.into(),
            peer_nonce: None,
            is_authenticated: true,
            certificates_required: false,
            certificates_validated: false,
            created_at: 1,
            last_update: 1,
        };
        let put = FailingStore.save_session(&session).await.unwrap_err();
        let read = FailingStore.get_session("s-nonce").await.unwrap_err();
        for e in [&put, &read] {
            assert_eq!(auth_fault_class(e), AuthFaultClass::Transport, "{e}");
            let (status, body, retry_after) = auth_fault_answer(e);
            assert_eq!((status, retry_after), (503, Some("2")), "{e}");
            assert_eq!(
                serde_json::to_string(&body).unwrap(),
                r#"{"code":"ERR_AUTH_UNAVAILABLE","error":"authentication handshake unavailable: session store"}"#
            );
            let text = body.to_string();
            assert!(
                !text.contains("reference") && !text.contains("KV PUT"),
                "{text}"
            );
        }
        assert_eq!(
            COUNTER_AUTH_HANDSHAKE_UNAVAILABLE,
            "auth_handshake_unavailable_total"
        );
    }

    /// (c, the 400) A fault that is not the store's keeps the door's 400 byte
    /// for byte, and the door routes its `Err` through the classifier; the
    /// door's own replies flush the census like a proceeded request.
    #[test]
    fn s670_b_a_non_store_fault_keeps_the_old_400_bytes() {
        use bsv_middleware_cloudflare::AuthCloudflareError as E;
        let ser = E::SerializationError("expected value at line 1 column 1".into());
        let (status, body, retry_after) = auth_fault_answer(&ser);
        assert_eq!((status, retry_after), (400, None));
        assert_eq!(
            serde_json::to_string(&body).unwrap(),
            r#"{"error":"authentication handshake failed: Serialization error: expected value at line 1 column 1"}"#
        );
        let strip = |s: &str| -> String { s.chars().filter(|c| !c.is_whitespace()).collect() };
        let whole = include_str!("auth.rs");
        let code = strip(&whole[..whole.find("#[cfg(test)]").unwrap()]);
        assert_eq!(code.matches("auth_fault_reply(&e)").count(), 1);
        assert_eq!(
            code.matches("json!({\"error\":format!(\"authenticationhandshakefailed:{e}\")})")
                .count(),
            1,
            "the 400's text lives in the classifier's answer only"
        );
        let lib = include_str!("lib.rs");
        let lib = strip(&lib[..lib.find("#[cfg(test)]").unwrap_or(lib.len())]);
        let reply = &lib[lib.find("auth::FrontDoor::Reply(mutresp)=>{").unwrap()..];
        let reply = &reply[..reply.find("returnOk(resp);").unwrap()];
        assert!(reply.contains("flush_counters_if_due(&ctx,&env);"), "{reply}");
        assert!(lib.contains("courier::take_pending_if_due("));
    }

    /// (d) The classifier's table, on every variant of bsv-middleware-cloudflare
    /// 0.6.2's `AuthCloudflareError` (the match in `auth_fault_class` is
    /// exhaustive, so a new variant does not compile until classified). The pin
    /// moves only after re-reading the crate's storage error paths.
    #[test]
    fn s670_d_the_classifier_table() {
        use bsv_middleware_cloudflare::AuthCloudflareError as E;
        use AuthFaultClass::{Caller, Transport};
        let table: Vec<(E, AuthFaultClass)> = vec![
            (kv(KV_PUT_500), Transport),
            (kv(DO_FETCH_FAULT), Transport),
            (kv("auth-session-store PUT answered 500"), Transport),
            (kv("auth-session-store stub: x"), Transport),
            (E::Unauthorized, Caller),
            (E::InvalidAuthentication("x".into()), Caller),
            (E::SessionNotFound("x".into()), Caller),
            (E::ServerMisconfigured, Caller),
            (E::PaymentInternal("x".into()), Caller),
            (
                E::PaymentRequired {
                    satoshis: 1,
                    derivation_prefix: "x".into(),
                },
                Caller,
            ),
            (E::MalformedPayment("x".into()), Caller),
            (E::InvalidDerivationPrefix, Caller),
            (E::PaymentFailed("x".into()), Caller),
            (E::InvalidPayment("x".into()), Caller),
            (E::SdkError("x".into()), Caller),
            (E::TransportError("x".into()), Caller),
            (E::ConfigError("x".into()), Caller),
            (E::SerializationError("x".into()), Caller),
        ];
        for (e, class) in &table {
            assert_eq!(auth_fault_class(e), *class, "{e:?}");
        }
        // A worker::Error out of the store arrives as `KvError` (the crate's From).
        let from_worker: E = worker::Error::RustError("KV PUT failed: 500".into()).into();
        assert_eq!(auth_fault_class(&from_worker), Transport);
        assert!(
            include_str!("../Cargo.toml").contains("bsv-middleware-cloudflare = \"=0.6.2\""),
            "the middleware pin moved: re-read AuthCloudflareError and its KvError sites first"
        );
    }

    /// (e) The lens's HIGH-1: a caller's over-long nonce header or handshake
    /// body is the caller's 400 (`authentication handshake failed: <reason>`),
    /// never the store-fault 503; a real-size handshake passes, bytes unchanged.
    #[test]
    fn s670_e_an_oversize_auth_input_is_the_callers_400() {
        use bsv_middleware_cloudflare::auth_headers as h;
        let long = "A".repeat(600);
        for header in [h::YOUR_NONCE, h::NONCE, h::INITIAL_NONCE] {
            let get = |name: &str| (name == header).then(|| long.clone());
            let reason = auth_nonce_oversize(get).expect(header);
            assert_eq!(reason, format!("{header} over 80 bytes"));
            let (status, body) = auth_oversize_answer(&reason);
            assert_eq!(status, 400);
            assert_eq!(
                serde_json::to_string(&body).unwrap(),
                format!(r#"{{"error":"authentication handshake failed: {header} over 80 bytes"}}"#)
            );
        }
        let nonce = bsv_middleware_cloudflare::core::auth::generate_random_nonce();
        assert_eq!(nonce.len(), 44);
        let at = |n: usize| move |name: &str| (name == h::YOUR_NONCE).then(|| "n".repeat(n));
        assert_eq!(auth_nonce_oversize(|_| Some(nonce.clone())), None);
        assert_eq!(auth_nonce_oversize(at(80)), None);
        assert!(auth_nonce_oversize(at(81)).is_some());
        // Round 2's R2-HIGH-1: a 100-byte nonce (accepted under the old 128) is
        // the 400; at 100 bytes 1,024 consumed entries fill a session cell to
        // about 111 KB, at 128 past the DO's 128 KiB value cap.
        assert_eq!(
            auth_nonce_oversize(at(100)).as_deref(),
            Some("x-bsv-auth-your-nonce over 80 bytes")
        );
        assert_eq!(auth_nonce_oversize(|_| None), None);
        // An 8 KiB handshake body is over (accepted under the old 16 KiB).
        let mut buf = Vec::new();
        let chunk = vec![b'x'; 1024];
        let fits = (0..8)
            .take_while(|_| push_bounded(&mut buf, &chunk))
            .count();
        assert_eq!(
            (fits, buf.len()),
            (4, 4 * 1024),
            "the read stops at the bound"
        );
        assert_eq!(
            handshake_body_oversize(8 * 1024).as_deref(),
            Some("handshake body over 4096 bytes")
        );
        assert_eq!(handshake_body_oversize(4 * 1024), None);
        // A real InitialRequest (the SDK's shape: version, type, identity key,
        // a 64-character `createNonce` nonce, an empty certificate request) is
        // 266 bytes, as the lens measured the SDK's (round 2, question 1).
        let session_nonce = create_nonce_shaped();
        assert_eq!(
            session_nonce.len(),
            64,
            "createNonce: 16 + 32 bytes, base64"
        );
        assert_eq!(auth_nonce_oversize(|_| Some(session_nonce.clone())), None);
        let real =
            serde_json::to_vec(&initial_request_with(session_nonce, Default::default())).unwrap();
        assert_eq!(real.len(), 266, "a real InitialRequest");
        assert_eq!(handshake_body_oversize(real.len()), None);
        // A certificate-bearing one (5 certifiers x 5 types x 5 fields; the
        // lens measured the SDK's at 1,049 bytes) passes the 4 KiB bound.
        let mut certs = bsv_rs::auth::RequestedCertificateSet::default();
        for i in 0..5u8 {
            let certifier = bsv_rs::primitives::PrivateKey::from_hex(&format!("{:064x}", 10 + i))
                .unwrap()
                .public_key()
                .to_hex();
            certs.certifiers.push(certifier);
            let type_id = bsv_middleware_cloudflare::core::auth::generate_random_nonce();
            certs
                .types
                .insert(type_id, (0..5).map(|f| format!("field{f}")).collect());
        }
        let with_certs =
            serde_json::to_vec(&initial_request_with(create_nonce_shaped(), certs)).unwrap();
        assert!(
            (1_000..AUTH_HANDSHAKE_BODY_MAX_BYTES).contains(&with_certs.len()),
            "a certificate-bearing InitialRequest is {} bytes",
            with_certs.len()
        );
        assert_eq!(handshake_body_oversize(with_certs.len()), None);
        let mut read = Vec::new();
        for piece in real.chunks(7) {
            assert!(push_bounded(&mut read, piece));
        }
        assert_eq!(read, real, "the bytes handed on are the bytes received");
        // The door runs the guard BEFORE the middleware, once, in its OWN body
        // (round 2's R2-LOW-2: a search from the guard to the end of the file
        // passes on any later call). The order is also held by the borrow
        // checker: the guard consumes `req` and returns the request the
        // middleware gets, so a swap does not compile; this pin is the
        // readable one.
        let whole = include_str!("auth.rs");
        let code: String = whole[..whole.find("#[cfg(test)]").unwrap()]
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();
        let guard = "letreq=matchauth_input_guard(req).await?{";
        assert!(
            guard_precedes_middleware(fn_body(&code, "pubasyncfnfront_door("), guard),
            "the door runs the guard before the middleware"
        );
        assert_eq!(code.matches("auth_input_guard(req).await?").count(), 1);
        // The check reds on a swap the old search passed: the middleware call
        // first, the guard after it, another call later in the file.
        let swapped = "pubasyncfnfront_door(req:Request){letx=process_auth_lane(req,env);\
            letreq=matchauth_input_guard(req).await?{}}fnlater(){process_auth_lane(req,env)}";
        let at = swapped.find(guard).unwrap();
        assert!(
            swapped[at..].contains("process_auth_lane(req,"),
            "the old search passes"
        );
        assert!(!guard_precedes_middleware(
            fn_body(swapped, "pubasyncfnfront_door("),
            guard
        ));
        assert_eq!(
            COUNTER_AUTH_HANDSHAKE_OVERSIZE,
            "auth_handshake_oversize_total"
        );
        assert_eq!(
            code.matches("crate::courier::note_presence(COUNTER_AUTH_HANDSHAKE_OVERSIZE);")
                .count(),
            1,
            "the oversize 400 is counted apart"
        );
    }

    /// (i) Round 2's R2-HIGH-1: on the DO backend the per-request nonce is
    /// consumed into the caller's own session cell BEFORE the signature is
    /// checked, and the cell is stored whole as one value under the DO's
    /// 128 KiB cap. With the crate's `CONSUMED_CAP` read from the crate (a bump
    /// of the cap reds here), the worst cell the guard admits stays under it:
    /// `CONSUMED_CAP x (nonce + 16) + handshake body + 1,024 < 128 KiB`.
    #[test]
    fn s670_i_the_worst_session_cell_fits_the_do_value_cap() {
        use bsv_middleware_cloudflare::storage::do_session::CONSUMED_CAP;
        const DO_VALUE_CAP: usize = 128 * 1024;
        // The cell bound: per consumed entry its key, a 16-byte allowance for
        // the structured clone's tags, the key's length varint and the expiry
        // (a double, 9 bytes); the session's `peer_nonce` is bounded by the
        // handshake body; 1,024 for the session's fixed fields.
        let bound = |nonce: usize, peer: usize| CONSUMED_CAP * (nonce + 16) + peer + 1_024;
        assert_eq!(CONSUMED_CAP, 1_024, "bsv-middleware-cloudflare =0.6.2");
        let worst = bound(AUTH_NONCE_MAX_BYTES, AUTH_HANDSHAKE_BODY_MAX_BYTES);
        assert_eq!(worst, 103_424);
        assert!(worst < DO_VALUE_CAP, "{worst} >= {DO_VALUE_CAP}");
        // The cell MODEL (round 2's R2-HIGH-1 table: Node's `v8.serialize` of
        // the cell's structured clone, a JS Map of 1,024 entries, measured by
        // the lens): (nonce, peer_nonce, serialized). The bound dominates every
        // measured row, so it is an upper bound and not a guess, and the old
        // bounds (128, 16 KiB) model over the cap.
        for (nonce, peer, measured) in [
            (44, 44, 55_207),
            (64, 44, 75_147),
            (64, 16_000, 91_104),
            (100, 44, 111_039),
            (110, 44, 121_009),
            (128, 44, 139_952),
            (128, 16_000, 155_909),
        ] {
            assert!(bound(nonce, peer) >= measured, "({nonce}, {peer})");
        }
        assert!(
            bound(128, 16 * 1024) > DO_VALUE_CAP,
            "the old bounds overrun"
        );
    }

    /// The body of the function whose signature starts at `sig`: from its first
    /// `{` to the brace that closes it (the door's code has balanced braces in
    /// its strings and macros).
    fn fn_body<'a>(code: &'a str, sig: &str) -> &'a str {
        let start = code.find(sig).unwrap_or_else(|| panic!("{sig}"));
        let open = start + code[start..].find('{').unwrap();
        let mut depth = 0usize;
        for (i, c) in code[open..].char_indices() {
            match c {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        return &code[open..=open + i];
                    }
                }
                _ => {}
            }
        }
        panic!("{sig}: unbalanced braces")
    }

    /// The guard is in this body, and no middleware CALL precedes it while one
    /// follows it.
    fn guard_precedes_middleware(body: &str, guard: &str) -> bool {
        let Some(at) = body.find(guard) else {
            return false;
        };
        let before = &body[..at];
        !before.contains("process_auth(req,")
            && !before.contains("process_auth_lane(req,")
            && body[at..].contains("process_auth_lane(req,")
    }

    /// A nonce of the SDK's `createNonce` shape, made by the middleware's own
    /// `create_session_nonce` (16 random bytes + a 32-byte HMAC: 64 characters).
    fn create_nonce_shaped() -> String {
        use futures_util::FutureExt;
        let key = bsv_rs::primitives::PrivateKey::from_hex(&format!("{:064x}", 3)).unwrap();
        let wallet = bsv_rs::wallet::ProtoWallet::new(Some(key));
        bsv_middleware_cloudflare::core::auth::create_session_nonce(&wallet, "low-670")
            .now_or_never()
            .expect("a ProtoWallet answers at once")
            .unwrap()
    }

    /// An InitialRequest of the SDK's shape with this nonce and certificate ask.
    fn initial_request_with(
        nonce: String,
        certs: bsv_rs::auth::RequestedCertificateSet,
    ) -> bsv_rs::auth::AuthMessage {
        let key = bsv_rs::primitives::PrivateKey::from_hex(&format!("{:064x}", 2)).unwrap();
        let mut m = bsv_rs::auth::AuthMessage::new(
            bsv_rs::auth::MessageType::InitialRequest,
            key.public_key(),
        );
        m.initial_nonce = Some(nonce);
        m.requested_certificates = Some(certs);
        m
    }

    /// (f) The lens's HIGH-2: the store fault's log line carries `ERR_STORAGE`,
    /// never the error's text or a platform reference id.
    #[test]
    fn s670_f_the_unavailable_log_line_carries_no_error_text() {
        let e = kv("auth-session-store fetch: Error: internal error; reference = abc123");
        let line = auth_unavailable_log_line(&e);
        assert_eq!(line, "[auth] handshake unavailable: ERR_STORAGE");
        assert!(
            !line.contains("abc123") && !line.contains("reference"),
            "{line}"
        );
        let whole = include_str!("auth.rs");
        let code = &whole[..whole.find("#[cfg(test)]").unwrap()];
        assert_eq!(
            code.matches("console_warn!(\"{}\", auth_unavailable_log_line(&e));")
                .count(),
            1
        );
        assert!(!code.contains("handshake unavailable: {e}"));
    }

    /// (h) The lens's HIGH-3, natively: the fault's extra headers are one list,
    /// the store fault's carries `Retry-After: 2`, any other none; the reply
    /// renders that list and the door answers with that reply.
    #[test]
    fn s670_h_the_fault_header_list_carries_retry_after() {
        assert_eq!(
            auth_fault_headers(AuthFaultClass::Transport),
            &[("Retry-After", "2")]
        );
        assert!(auth_fault_headers(AuthFaultClass::Caller).is_empty());
        let whole = include_str!("auth.rs");
        let code: String = whole[..whole.find("#[cfg(test)]").unwrap()]
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();
        let reply = &code[code.find("pubfnauth_fault_reply(").unwrap()..];
        let reply = &reply[..reply.find("Ok(resp)").unwrap()];
        assert!(
            reply.contains("for(name,value)inauth_fault_headers(auth_fault_class(e)){resp.headers_mut().set(name,value)?;}"),
            "{reply}"
        );
        assert_eq!(
            code.matches("returnauth_fault_reply(&e).map(FrontDoor::Reply);")
                .count(),
            1
        );
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
            anon_write_by_route: [11, 12],
            lane_served: 13,
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
        assert_eq!(v["anonymousWriteByRoute"]["/record"], 11);
        assert_eq!(v["anonymousWriteByRoute"]["/proof"], 12);
        assert_eq!(v["anonymousWriteByRoute"].as_object().unwrap().len(), 2);
        assert_eq!(v["laneServed"], 13);
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
        // #441: the free write routes' anonymous serves and the lane's serves
        // land in their own buckets; an out-of-range write index is ignored.
        count_anonymous_write_served(1);
        count_anonymous_write_served(9);
        count_lane_served();
        let after = counters_snapshot();
        assert!(after.anon_by_route[2] > before.anon_by_route[2]);
        assert!(after.public_served > before.public_served);
        assert!(after.authenticated_served > before.authenticated_served);
        assert!(after.mismatch_refused > before.mismatch_refused);
        assert_eq!(
            after.anon_write_by_route[1],
            before.anon_write_by_route[1] + 1
        );
        assert_eq!(after.anon_write_by_route[0], before.anon_write_by_route[0]);
        assert_eq!(after.lane_served, before.lane_served + 1);
    }

    // ── #441: the free write routes ──────────────────────────────────────────
    #[test]
    fn the_two_write_routes_are_post_only_and_keep_their_order() {
        assert_eq!(write_route_index(true, "/record"), Some(0));
        assert_eq!(write_route_index(true, "/proof"), Some(1));
        assert_eq!(
            write_route_index(false, "/proof"),
            None,
            "GET /proof is public evidence"
        );
        assert_eq!(write_route_index(false, "/record"), None);
        for other in ["/results", "/health", "/record/x", "/", "/.well-known/auth"] {
            assert_eq!(write_route_index(true, other), None, "{other}");
        }
        assert_eq!(WRITE_ROUTES, ["/record", "/proof"], "append, never reorder");
        // A write route is NOT an identity route: its exemption from the
        // per-route lenient mode is decided in the front door, not here.
        assert!(!route_requires_identity_auth("/record"));
        assert!(!route_requires_identity_auth("/proof"));
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
