//! The identity views (bsv-low #532, half b of M29-3): names and pictures for
//! every seat, read from LOW's identity resolver through the `IDENTITY` service
//! binding and served to the browser under LOW's own hardening, caching, CORS
//! and budgets. This worker holds no identity data: every route is a PROXY.
//!
//! | route | callee path (the resolver) | auth | cache |
//! |---|---|---|---|
//! | `GET /identities?ik=a,b,...` (1 to 100 keys) | `GET /api/pf/identity?ik=...` | anonymous allowed (the front door's public path) | `public, s-maxage=30, stale-while-revalidate=300` |
//! | `GET /identity/:ik` | the same, one key | as above | as above |
//! | `GET /identity/verify/:ik` | the batch for one key, then `GET /api/identity/verify/name/:name` and `GET /api/identity/verify/picture/:imageHash` for the display picks | as above | as above |
//! | `GET /identity/pic/:imageHash` | `GET /api/pf/picture/content/:imageHash` | anonymous only, served BEFORE the front door | `public, immutable, max-age=31536000` on a 200; `no-store` otherwise |
//! | `POST /internal/identity/kill` `{imageHash, reason?, unkill?}` | none (the `IDENTITY_KILL` KV) | the operator's `INTERNAL_TOKEN` bearer, served BEFORE the front door | `no-store` |
//!
//! The callee paths and the batch body are the resolver's own contract,
//! `workers/low-identity-resolver/CONTRACT.md` in bsv-low (Zanaadu's
//! app-layer routes at `2de902a`, plus the resolver's merged `display` pick);
//! the reconciled shape this module answers is bsv-low's
//! `docs/IDENTITY-CONTRACT-2026-10-07.md`. The `IDENTITY_URL` var names the
//! host the callee sees (any host: over the binding only the path counts); the
//! binding carries the request (a URL fetch between two Workers of this
//! account is error 1042).
//!
//! ## The field mapping (resolver answer to the contract shape)
//!
//! LOW re-derives NOTHING here: no display rule lives in this module. Per key,
//! from the resolver's `identities[<ik>]`:
//! - `userNumber`, `names`, `pictures`, `preference`: passed through BYTE FOR
//!   BYTE (raw JSON values, never re-serialized), so the M29-6 parity harness
//!   can compare them with Zanaadu's `/api/pf/identity` directly. `userNumber`
//!   is always `null` on LOW (a PfOnly node). A key the resolver did not answer
//!   is `null`, `[]`, `[]`, `{"name":null,"picture":null}` (Zanaadu's own
//!   "holds nothing" shape).
//! - `display`: the resolver's `display` (the lib's own pick, PF-SPEC 7.4)
//!   MAPPED, never computed:
//!   - `name`, `nameDisplay`: the pick's, as given.
//!   - `imageHash`: the pick's `picture`, renamed; `null` when the pick has
//!     none, when that hash is on the kill list (no fallback to another
//!     picture: the kill list applies to the picture only, never to the name),
//!     or when the kill list cannot be read (fail closed; the answer is then
//!     `no-store`).
//!   - `pictureUrl`: `<this worker's origin>/identity/pic/<imageHash>`, or
//!     `null` with `imageHash`.
//!   - `since`: `{height, headTxid}` of the entry's `names[]` row whose `name`
//!     equals the pick's `name` (`height` `null` until mined); `null` when the
//!     pick has no name, and `null` when no row matches. The pick and the rows
//!     are two reads at the resolver, so a mirror write between them can leave
//!     the pick naming no row of this body (the M29-3a display lens,
//!     MEDIUM-1): a lookup miss, never a fault, counted as
//!     `identity.display.sinceMiss` on `/health`.
//!   - The pick's `ownerCanonical` (and its own `userNumber`) are DROPPED: the
//!     contract's `display` has no such field, and the entry's `userNumber` is
//!     already passed through.
//!   - A resolver entry with NO `display` (a resolver older than M29-3a's
//!     display merge) maps to the all-null display, counted as
//!     `identity.display.absent`; it is never derived here.
//! - The top-level `namespaceIds` is passed through byte for byte.
//!
//! On the shared fixtures the mapped display equals Zanaadu's
//! `expected.display` for every holder; on LOW's live resolver carol, dave and
//! erin's picks would differ, because Zanaadu's pick applies the holder's
//! preference and LOW's mirror holds no preference records (the resolver's
//! CONTRACT.md, "Known differences").
//!
//! The batch answers `{"namespaceIds", "identities": {<ik>: entry}}` with every
//! requested key present (sorted); the single key form answers
//! `{"namespaceIds", "identityKey", "userNumber", "names", "pictures",
//! "preference", "display"}`; the verify route answers `{"identityKey",
//! "display", "name", "picture"}` where `name` / `picture` are the resolver's
//! verify bodies of the MAPPED picks, passed through byte for byte (`shardId`,
//! `head {txid, vout, height}`, `root`, `chainRoot`, `match`, `leaf`, the 256
//! `siblings` and their `directions`: what the client's Verify recomputes),
//! `null` when there is no pick or the resolver answers 404 for it.
//!
//! Byte for byte holds on the ANONYMOUS path, which is the path the client
//! uses (plain `fetch`, `credentials: 'omit'`). A caller that authenticates
//! gets the same JSON re-serialized and signed by `lib.rs` (the crate's
//! posture for every JSON route), `no-store`.
//!
//! ## The picture route's hardening
//!
//! 200 only when ALL of: the hash is a 64-hex sha256, it is not on the kill
//! list, the resolver answers 200, the body is at most [`PICTURE_MAX_BYTES`]
//! (a declared `Content-Length` over it is refused before a byte is read, the
//! stream is cut at it), `sha256(body)` equals the hash in the URL (so the
//! route cannot be poisoned: it is hash-addressed and checked), and the MAGIC
//! BYTES say png, jpeg, webp, gif or avif ([`sniff_image`]; AVIF is `ftyp` at
//! 4 and the brand `avif` at 8, Zanaadu's content route's own rule; the
//! resolver's claimed type is never read, so SVG and HTML can never pass). The 200 carries the
//! sniffed type, `x-content-type-options: nosniff`,
//! `content-security-policy: sandbox` and the immutable cache line. Every
//! refusal is a 404 `no-store` (the same body, not an oracle); a kill list or
//! resolver that cannot answer is a 503 `no-store`.
//!
//! The route is served before the BRC-103 front door: its body is bytes, which
//! the front door's JSON signing would destroy, and an `<img src>` sends no
//! auth. A kill reaches a browser that already cached the bytes only when that
//! cache expires (the immutable line is the issue's; the kill stops every new
//! fetch and every `display`).
//!
//! ## The kill list
//!
//! The `IDENTITY_KILL` KV namespace, owned by this worker: one key per killed
//! hash, `identity-kill:<imageHash>` -> `{"reason", "killedAtMs"}`. Read with
//! one point `get` on the picture route and ONE bulk `get` of the picks (at
//! most 100, one per key) on the JSON routes; written only by the kill route.
//! KV is eventually consistent: a kill is seen everywhere within about a
//! minute. Never wiped (`storage-ownership.json`): a wipe re-exposes every
//! face the operator removed.
//!
//! ## Budgets (per isolate)
//!
//! Every GET route charges keys against two fixed one-minute windows: the
//! caller's IP (`CF-Connecting-IP`) and, when the caller authenticated, its
//! identity ([`IP_KEYS_PER_WINDOW`], [`IDENTITY_KEYS_PER_WINDOW`]; the batch
//! charges its key count, verify three (its three resolver calls), the other
//! routes one). Past either: 429 `ERR_IDENTITY_BUDGET` with `scope`,
//! `retryAfterMs` and `Retry-After`. The windows live in the isolate (no store
//! is asked), so the bound is per isolate; past [`BUDGET_MAX_TRACKED`] callers
//! the expired windows are dropped, and if none expired every window is
//! dropped (fail open, counted as `evictions`: a caller rotating more than that
//! many IPs inside one window clears them all). No route here reads a D1 row:
//! the D1 rows-read ceiling of every route is ZERO beyond the per-isolate
//! latch every route of this worker pays once (`schema::ensure_latch_columns`,
//! pinned).
//!
//! The residuals, stated (the M29-3b lens, L2 to L4 and N9). The client reads
//! anonymously, so the per-identity window binds only an authenticated caller
//! and the per-IP window is the lever in practice. A caller with no
//! `CF-Connecting-IP` (a service binding, local dev) shares the one window
//! `unknown`. The JSON routes' `s-maxage` is a header only: this worker uses no
//! Cache API, so Cloudflare does not store its answers and every request
//! reaches the resolver, whose D1 pays for it (a 100-key batch reads every
//! by-owner row of 100 keys there, twice: the lib's body and its pick). So a
//! determined caller can drive the resolver's D1 at about (isolates it
//! touches) x [`IP_KEYS_PER_WINDOW`] keys per minute per IP. No edge
//! rate-limiting rule is set (CAP's decision of 2026-10-07): the per-isolate
//! budgets stand alone.
//!
//! ## CORS
//!
//! The JSON routes and the picture route answer `Access-Control-Allow-Origin`
//! only to an origin in `IDENTITY_APP_ORIGINS` (comma separated, exact match;
//! unset = none), with `Vary: Origin`, and expose every header a browser must
//! read: the BRC-104 reply headers, the session lane's seal and offer,
//! `Retry-After` and `ETag`. The match is exact, so a Pages preview host
//! (`<sha>.low-pot.pages.dev`) reads no names (fail-safe). `localhost` is on
//! the beta list only, never prod's (pinned against `wrangler.toml`). The kill
//! route answers with no CORS at all, like every `/internal/*` route.
//!
//! ## Counters
//!
//! `/health` carries `identity.routes.<route>.<outcome>` for this isolate,
//! `identity.display.{sinceMiss, absent}`, and the budget's limits and
//! evictions.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use worker::{Env, Headers, Method, Request, RequestInit, Response, Result, RouteContext};

use crate::auth::{AuthState, CallerAuth};

/// `worker::console_warn!` in the Worker build, `eprintln!` on the host (the
/// worker macro calls a wasm-bindgen extern that aborts natively; the
/// `compaction_log` precedent), so the tests can drive every fault path.
macro_rules! identity_warn {
    ($($arg:tt)*) => {{
        #[cfg(target_arch = "wasm32")]
        {
            worker::console_warn!($($arg)*);
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            eprintln!($($arg)*);
        }
    }};
}

/// Distinct keys per batch (Zanaadu's `IDENTITY_BATCH_MAX_KEYS`).
pub const IDENTITY_BATCH_MAX_KEYS: usize = 100;
/// The picture cap, inclusive: 64 KB.
pub const PICTURE_MAX_BYTES: usize = 64 * 1024;
/// The cap on any JSON body read from the resolver (a 100-key batch).
pub const RESOLVER_JSON_MAX_BYTES: usize = 2 * 1024 * 1024;
/// One resolver call's wall-clock bound.
pub const RESOLVER_TIMEOUT_MS: u64 = 8_000;
/// KV's bulk get takes at most 100 keys.
pub const KILL_BULK_CHUNK: usize = 100;
/// The kill reason's cap, in bytes.
pub const KILL_REASON_MAX_BYTES: usize = 500;
/// The kill route's body cap, in bytes (a hash, a reason, a flag).
pub const KILL_BODY_MAX_BYTES: usize = 4 * 1024;

pub const JSON_CACHE_CONTROL: &str = "public, s-maxage=30, stale-while-revalidate=300";
pub const PICTURE_CACHE_CONTROL: &str = "public, immutable, max-age=31536000";
pub const PICTURE_CSP: &str = "sandbox";
pub const NO_STORE: &str = "no-store";

pub const RESOLVER_BATCH_PATH: &str = "/api/pf/identity";
pub const RESOLVER_PICTURE_PATH: &str = "/api/pf/picture/content/";
pub const RESOLVER_VERIFY_NAME_PATH: &str = "/api/identity/verify/name/";
pub const RESOLVER_VERIFY_PICTURE_PATH: &str = "/api/identity/verify/picture/";

pub const BATCH_ROUTE: &str = "/identities";
pub const IDENTITY_ROUTE_PREFIX: &str = "/identity/";
pub const PICTURE_ROUTE_PREFIX: &str = "/identity/pic/";
pub const VERIFY_ROUTE_PREFIX: &str = "verify/";
pub const KILL_ROUTE: &str = "/internal/identity/kill";

pub const RESOLVER_BINDING: &str = "IDENTITY";
pub const RESOLVER_URL_VAR: &str = "IDENTITY_URL";
pub const KILL_BINDING: &str = "IDENTITY_KILL";
pub const KILL_KEY_PREFIX: &str = "identity-kill:";
pub const APP_ORIGINS_VAR: &str = "IDENTITY_APP_ORIGINS";

// ---------------------------------------------------------------------------
// Parameters
// ---------------------------------------------------------------------------

/// A compressed identity key: 66 hex, `02` / `03` prefix, any case; answered
/// lowercase (Zanaadu's `parse_identity_key` rule).
pub fn parse_identity_key(s: &str) -> Option<String> {
    let s = s.trim();
    if s.len() != 66 || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let lc = s.to_ascii_lowercase();
    (lc.starts_with("02") || lc.starts_with("03")).then_some(lc)
}

/// The `ik` list: comma separated keys, empty segments skipped, duplicates
/// collapsed (first seen order), 1 to [`IDENTITY_BATCH_MAX_KEYS`]; a malformed
/// segment is an error, never a silently partial answer.
pub fn parse_identity_keys(raw: &str) -> std::result::Result<Vec<String>, String> {
    let mut keys: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for part in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let key = parse_identity_key(part).ok_or_else(|| {
            format!("'{part}' is not a compressed identity key (66 hex chars, 02/03 prefix)")
        })?;
        if seen.insert(key.clone()) {
            keys.push(key);
        }
    }
    if keys.is_empty() {
        return Err("missing ik (comma-separated compressed identity keys)".to_string());
    }
    if keys.len() > IDENTITY_BATCH_MAX_KEYS {
        return Err(format!(
            "too many identity keys: {} (max {IDENTITY_BATCH_MAX_KEYS})",
            keys.len()
        ));
    }
    Ok(keys)
}

/// A 32-byte image hash: exactly 64 hex, answered lowercase.
pub fn parse_image_hash(s: &str) -> Option<String> {
    (s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())).then(|| s.to_ascii_lowercase())
}

/// A folded pf name safe to put in a path: 1 to 20 bytes of `[a-z0-9_]`
/// (Zanaadu's `is_normalized_name`).
pub fn is_normalized_name(s: &str) -> bool {
    (1..=20).contains(&s.len())
        && s.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

// ---------------------------------------------------------------------------
// The picture gate
// ---------------------------------------------------------------------------

/// The image type the MAGIC BYTES declare, as its canonical mime, or `None`.
/// Only png, jpeg, webp, gif and avif; SVG (text) and everything else is `None`.
pub fn sniff_image(bytes: &[u8]) -> Option<&'static str> {
    // Zanaadu's `sniff_image_kind` and the client's sniff: the 4-byte PNG prefix and `GIF8` (M29-3b fold lens LOW-1, 2026-10-07).
    const PNG: &[u8] = &[0x89, b'P', b'N', b'G'];
    if bytes.starts_with(PNG) {
        return Some("image/png");
    }
    if bytes.starts_with(&[0xFF, 0xD8, 0xFF]) {
        return Some("image/jpeg");
    }
    if bytes.starts_with(b"GIF8") {
        return Some("image/gif");
    }
    if bytes.len() >= 12 && &bytes[0..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
        return Some("image/webp");
    }
    // Zanaadu's content route (`pf_content.rs`): an ISO-BMFF `ftyp` box whose
    // major brand is `avif`.
    if bytes.len() >= 12 && &bytes[4..8] == b"ftyp" && &bytes[8..12] == b"avif" {
        return Some("image/avif");
    }
    None
}

/// Why a picture is not served (each a 404 `no-store`), or the mime it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PictureGate {
    Serve(&'static str),
    Oversize,
    HashMismatch,
    RefusedType,
}

/// The gate over bytes the resolver answered 200 with. Pure.
pub fn picture_gate(image_hash_lc: &str, bytes: &[u8]) -> PictureGate {
    if bytes.len() > PICTURE_MAX_BYTES {
        return PictureGate::Oversize;
    }
    if hex::encode(bsv_rs::primitives::hash::sha256(bytes)) != image_hash_lc {
        return PictureGate::HashMismatch;
    }
    match sniff_image(bytes) {
        Some(mime) => PictureGate::Serve(mime),
        None => PictureGate::RefusedType,
    }
}

// ---------------------------------------------------------------------------
// The resolver's answer and the display mapping
// ---------------------------------------------------------------------------

/// The fields of a resolver name item `since` reads (the item itself is
/// passed through raw).
#[derive(Debug, Clone, Deserialize)]
pub struct NameRow {
    pub name: String,
    #[serde(default)]
    pub height: Option<i64>,
    #[serde(rename = "headTxid", default)]
    pub head_txid: String,
}

/// The resolver's `display` (the lib's pick), the fields the mapping reads.
/// Its `userNumber` and its owner flag are not read (dropped, see the module
/// doc).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ResolverPick {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(rename = "nameDisplay", default)]
    pub name_display: Option<String>,
    #[serde(default)]
    pub picture: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Since {
    pub height: Option<i64>,
    #[serde(rename = "headTxid")]
    pub head_txid: String,
}

/// The contract's `display` block.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Display {
    pub name: Option<String>,
    #[serde(rename = "nameDisplay")]
    pub name_display: Option<String>,
    #[serde(rename = "imageHash")]
    pub image_hash: Option<String>,
    #[serde(rename = "pictureUrl")]
    pub picture_url: Option<String>,
    pub since: Option<Since>,
}

/// `since`: the `names[]` row whose `name` equals the pick's. A lookup, not a
/// rule; `None` when the pick has no name or no row matches. Pure.
pub fn since_of(pick: &ResolverPick, names: &[NameRow]) -> Option<Since> {
    let name = pick.name.as_deref()?;
    names.iter().find(|r| r.name == name).map(|r| Since {
        height: r.height,
        head_txid: r.head_txid.clone(),
    })
}

/// The `display` block MAPPED from the resolver's pick. `pick: None` = the
/// resolver sent no `display`: all null. `killed: None` = the kill list could
/// not be read: no picture (fail closed). A killed pick shows no picture and
/// nothing else in its place. `origin` is this worker's origin
/// (`https://host`), so `pictureUrl` points at THIS worker's picture route.
/// Pure.
pub fn map_display(
    pick: Option<&ResolverPick>,
    names: &[NameRow],
    killed: Option<&HashSet<String>>,
    origin: &str,
) -> Display {
    let Some(pick) = pick else {
        return Display::default();
    };
    let image_hash = match (pick.picture.as_ref(), killed) {
        (Some(h), Some(killed)) if !killed.contains(h) => Some(h.clone()),
        _ => None,
    };
    Display {
        name: pick.name.clone(),
        name_display: pick.name_display.clone(),
        picture_url: image_hash.as_ref().map(|h| format!("{origin}{PICTURE_ROUTE_PREFIX}{h}")),
        image_hash,
        since: since_of(pick, names),
    }
}

fn raw(s: &str) -> Box<RawValue> {
    RawValue::from_string(s.to_string()).expect("a literal JSON value")
}

/// The resolver's batch answer, the passed-through parts kept raw.
#[derive(Deserialize)]
struct ResolverBatch {
    #[serde(rename = "namespaceIds", default)]
    namespace_ids: Option<Box<RawValue>>,
    identities: HashMap<String, ResolverEntry>,
}

#[derive(Deserialize)]
struct ResolverEntry {
    #[serde(rename = "userNumber", default)]
    user_number: Option<Box<RawValue>>,
    names: Box<RawValue>,
    pictures: Box<RawValue>,
    #[serde(default)]
    preference: Option<Box<RawValue>>,
    #[serde(default)]
    display: Option<ResolverPick>,
}

/// One requested key, parsed: the raw parts, the pick and what `since` reads.
pub struct ParsedEntry {
    pub key: String,
    user_number_raw: Box<RawValue>,
    names_raw: Box<RawValue>,
    pictures_raw: Box<RawValue>,
    preference_raw: Box<RawValue>,
    pub names: Vec<NameRow>,
    /// The resolver's pick; `None` when its entry carried no `display`.
    pub pick: Option<ResolverPick>,
}

/// The resolver's batch answer for `keys`, every requested key present.
pub struct ParsedBatch {
    namespace_ids: Box<RawValue>,
    pub entries: Vec<ParsedEntry>,
}

/// Parse the resolver's `/api/pf/identity` body for the requested `keys`. A
/// body that is not that shape (a name item without `name`, a pick whose
/// `picture` is not a 64-hex hash) is an error: the route answers 502, never
/// a guess. A key the resolver did not answer is the empty shape with the
/// all-null pick (what the resolver answers for a key that holds nothing).
pub fn parse_resolver_batch(body: &[u8], keys: &[String]) -> std::result::Result<ParsedBatch, String> {
    let mut batch: ResolverBatch =
        serde_json::from_slice(body).map_err(|e| format!("resolver batch body: {e}"))?;
    let mut entries = Vec::with_capacity(keys.len());
    for key in keys {
        let (user_number_raw, names_raw, pictures_raw, preference_raw, pick) = match batch.identities.remove(key) {
            Some(e) => (
                e.user_number.unwrap_or_else(|| raw("null")),
                e.names,
                e.pictures,
                e.preference.unwrap_or_else(|| raw(r#"{"name":null,"picture":null}"#)),
                e.display,
            ),
            None => (
                raw("null"),
                raw("[]"),
                raw("[]"),
                raw(r#"{"name":null,"picture":null}"#),
                Some(ResolverPick::default()),
            ),
        };
        let names: Vec<NameRow> = serde_json::from_str(names_raw.get())
            .map_err(|e| format!("resolver names for {key}: {e}"))?;
        let pick = match pick {
            Some(mut p) => {
                if let Some(h) = p.picture.take() {
                    p.picture = Some(
                        parse_image_hash(&h).ok_or_else(|| format!("resolver pick for {key}: picture '{h}' is not a hash"))?,
                    );
                }
                Some(p)
            }
            None => None,
        };
        entries.push(ParsedEntry {
            key: key.clone(),
            user_number_raw,
            names,
            names_raw,
            pictures_raw,
            preference_raw,
            pick,
        });
    }
    Ok(ParsedBatch {
        namespace_ids: batch
            .namespace_ids
            .unwrap_or_else(|| raw(r#"{"name":null,"picture":null}"#)),
        entries,
    })
}

#[derive(Serialize)]
struct EntryOut<'a> {
    #[serde(rename = "userNumber")]
    user_number: &'a RawValue,
    names: &'a RawValue,
    pictures: &'a RawValue,
    preference: &'a RawValue,
    display: Display,
}

#[derive(Serialize)]
struct BatchOut<'a> {
    #[serde(rename = "namespaceIds")]
    namespace_ids: &'a RawValue,
    identities: BTreeMap<&'a str, EntryOut<'a>>,
}

#[derive(Serialize)]
struct SingleOut<'a> {
    #[serde(rename = "namespaceIds")]
    namespace_ids: &'a RawValue,
    #[serde(rename = "identityKey")]
    identity_key: &'a str,
    #[serde(rename = "userNumber")]
    user_number: &'a RawValue,
    names: &'a RawValue,
    pictures: &'a RawValue,
    preference: &'a RawValue,
    display: Display,
}

impl ParsedBatch {
    /// The picks' pictures, once each (what the kill list is asked: at most
    /// one per key).
    pub fn kill_candidates(&self) -> Vec<String> {
        let mut seen = HashSet::new();
        self.entries
            .iter()
            .filter_map(|e| e.pick.as_ref().and_then(|p| p.picture.as_ref()))
            .filter(|h| seen.insert(h.as_str()))
            .cloned()
            .collect()
    }

    /// How many entries carried no `display`, and how many picks name no row
    /// of their own `names[]` (`since` missed). Pure.
    pub fn display_notes(&self) -> (u64, u64) {
        let absent = self.entries.iter().filter(|e| e.pick.is_none()).count() as u64;
        let since_miss = self
            .entries
            .iter()
            .filter(|e| {
                e.pick
                    .as_ref()
                    .is_some_and(|p| p.name.is_some() && since_of(p, &e.names).is_none())
            })
            .count() as u64;
        (absent, since_miss)
    }

    fn display_of(e: &ParsedEntry, killed: Option<&HashSet<String>>, origin: &str) -> Display {
        map_display(e.pick.as_ref(), &e.names, killed, origin)
    }

    fn entry_out<'a>(e: &'a ParsedEntry, killed: Option<&HashSet<String>>, origin: &str) -> EntryOut<'a> {
        EntryOut {
            user_number: &e.user_number_raw,
            names: &e.names_raw,
            pictures: &e.pictures_raw,
            preference: &e.preference_raw,
            display: Self::display_of(e, killed, origin),
        }
    }

    /// The `/identities` body. Pure.
    pub fn render_batch(&self, killed: Option<&HashSet<String>>, origin: &str) -> String {
        let out = BatchOut {
            namespace_ids: &self.namespace_ids,
            identities: self
                .entries
                .iter()
                .map(|e| (e.key.as_str(), Self::entry_out(e, killed, origin)))
                .collect(),
        };
        serde_json::to_string(&out).expect("raw values and strings serialize")
    }

    /// The `/identity/:ik` body (the first entry). Pure.
    pub fn render_single(&self, killed: Option<&HashSet<String>>, origin: &str) -> String {
        let e = &self.entries[0];
        let entry = Self::entry_out(e, killed, origin);
        serde_json::to_string(&SingleOut {
            namespace_ids: &self.namespace_ids,
            identity_key: &e.key,
            user_number: entry.user_number,
            names: entry.names,
            pictures: entry.pictures,
            preference: entry.preference,
            display: entry.display,
        })
        .expect("raw values and strings serialize")
    }
}

// ---------------------------------------------------------------------------
// The seams: the resolver binding and the kill list
// ---------------------------------------------------------------------------

/// A resolver answer: status and body (bounded by the caller's cap).
#[derive(Debug, Clone)]
pub struct Upstream {
    pub status: u16,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpstreamError {
    /// No binding, a transport fault, or a body read fault.
    Transport(String),
    Timeout,
    /// The body (declared or read) is over the caller's cap.
    TooLarge,
}

/// The resolver, reached through the `IDENTITY` service binding in the worker
/// and through a fake in the tests.
#[allow(async_fn_in_trait)]
pub trait Resolver {
    async fn get(&self, path_and_query: &str, max_bytes: usize) -> std::result::Result<Upstream, UpstreamError>;
}

/// The kill list (`IDENTITY_KILL` KV in the worker, a set in the tests).
#[allow(async_fn_in_trait)]
pub trait KillList {
    /// The members of `hashes` that are killed.
    async fn killed_among(&self, hashes: &[String]) -> std::result::Result<HashSet<String>, String>;
    async fn kill(&self, hash: &str, reason: &str, now_ms: i64) -> std::result::Result<(), String>;
    async fn unkill(&self, hash: &str) -> std::result::Result<(), String>;
}

// ---------------------------------------------------------------------------
// The answers (framework-neutral, so the tests drive the whole handler)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteId {
    Batch = 0,
    Single,
    Picture,
    Verify,
    Kill,
}

const ROUTE_NAMES: [&str; 5] = ["batch", "single", "picture", "verify", "kill"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Served = 0,
    BadRequest,
    NotFound,
    Killed,
    RefusedType,
    Oversize,
    HashMismatch,
    BudgetRefused,
    Unauthorized,
    UpstreamFault,
    /// The kill list could not be read: the picture route answered 503, a
    /// JSON route answered without pictures (`no-store`).
    KillListFault,
}

const OUTCOME_NAMES: [&str; 11] = [
    "served",
    "badRequest",
    "notFound",
    "killed",
    "refusedType",
    "oversize",
    "hashMismatch",
    "budgetRefused",
    "unauthorized",
    "upstreamFault",
    "killListFault",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answer {
    pub status: u16,
    pub headers: Vec<(&'static str, String)>,
    pub body: Vec<u8>,
    pub outcome: Outcome,
}

impl Answer {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    fn json(status: u16, body: String, cache: &str, outcome: Outcome) -> Self {
        Answer {
            status,
            headers: vec![
                ("Content-Type", "application/json".to_string()),
                ("Cache-Control", cache.to_string()),
                ("X-Content-Type-Options", "nosniff".to_string()),
            ],
            body: body.into_bytes(),
            outcome,
        }
    }

    fn error(status: u16, msg: &str, outcome: Outcome) -> Self {
        Self::json(status, serde_json::json!({ "error": msg }).to_string(), NO_STORE, outcome)
    }

    /// The ONE picture refusal (same status, body and headers whatever the
    /// reason, so the route is not an oracle); `outcome` is for the counters.
    fn picture_not_found(outcome: Outcome) -> Self {
        let mut a = Self::error(404, "no such picture", outcome);
        a.headers.push(("Content-Security-Policy", PICTURE_CSP.to_string()));
        a
    }
}

fn upstream_fault(e: &UpstreamError) -> Answer {
    match e {
        UpstreamError::Timeout | UpstreamError::Transport(_) => {
            Answer::error(503, "identity resolver unavailable", Outcome::UpstreamFault)
        }
        UpstreamError::TooLarge => Answer::error(502, "identity resolver answer too large", Outcome::UpstreamFault),
    }
}

/// Ask the resolver for `keys` and parse the answer, or the error answer.
async fn fetch_batch<R: Resolver>(r: &R, keys: &[String]) -> std::result::Result<ParsedBatch, Answer> {
    let path = format!("{RESOLVER_BATCH_PATH}?ik={}", keys.join(","));
    let up = r.get(&path, RESOLVER_JSON_MAX_BYTES).await.map_err(|e| upstream_fault(&e))?;
    if up.status != 200 {
        identity_warn!("[identity] resolver batch answered {}", up.status);
        return Err(Answer::error(502, "identity resolver refused the read", Outcome::UpstreamFault));
    }
    let batch = parse_resolver_batch(&up.body, keys).map_err(|e| {
        identity_warn!("[identity] {e}");
        Answer::error(502, "identity resolver answered an unexpected shape", Outcome::UpstreamFault)
    })?;
    let (absent, since_miss) = batch.display_notes();
    DISPLAY_ABSENT.fetch_add(absent, Ordering::Relaxed);
    DISPLAY_SINCE_MISS.fetch_add(since_miss, Ordering::Relaxed);
    Ok(batch)
}

/// The kill-list read for the JSON routes: `None` on a fault (no pictures).
async fn killed_or_none<K: KillList>(k: &K, batch: &ParsedBatch) -> Option<HashSet<String>> {
    let hashes = batch.kill_candidates();
    if hashes.is_empty() {
        return Some(HashSet::new());
    }
    match k.killed_among(&hashes).await {
        Ok(set) => Some(set),
        Err(e) => {
            identity_warn!("[identity] kill list read failed, no pictures shown: {e}");
            None
        }
    }
}

fn json_answer(body: String, kill_known: bool) -> Answer {
    if kill_known {
        Answer::json(200, body, JSON_CACHE_CONTROL, Outcome::Served)
    } else {
        Answer::json(200, body, NO_STORE, Outcome::KillListFault)
    }
}

/// `GET /identities?ik=...` for parsed `keys`.
pub async fn batch_answer<R: Resolver, K: KillList>(r: &R, k: &K, keys: &[String], origin: &str) -> Answer {
    let batch = match fetch_batch(r, keys).await {
        Ok(b) => b,
        Err(a) => return a,
    };
    let killed = killed_or_none(k, &batch).await;
    json_answer(batch.render_batch(killed.as_ref(), origin), killed.is_some())
}

/// `GET /identity/:ik` for a parsed key.
pub async fn single_answer<R: Resolver, K: KillList>(r: &R, k: &K, key: &str, origin: &str) -> Answer {
    let batch = match fetch_batch(r, &[key.to_string()]).await {
        Ok(b) => b,
        Err(a) => return a,
    };
    let killed = killed_or_none(k, &batch).await;
    json_answer(batch.render_single(killed.as_ref(), origin), killed.is_some())
}

/// One verify body from the resolver: `Ok(None)` on its 404, the raw body on a
/// 200 that is a JSON object, an error answer otherwise.
async fn fetch_verify<R: Resolver>(r: &R, path: &str) -> std::result::Result<Option<Box<RawValue>>, Answer> {
    let up = r.get(path, RESOLVER_JSON_MAX_BYTES).await.map_err(|e| upstream_fault(&e))?;
    match up.status {
        200 => {
            let body: Box<RawValue> = serde_json::from_slice(&up.body).map_err(|_| {
                Answer::error(502, "identity resolver answered an unexpected shape", Outcome::UpstreamFault)
            })?;
            if !body.get().trim_start().starts_with('{') {
                return Err(Answer::error(
                    502,
                    "identity resolver answered an unexpected shape",
                    Outcome::UpstreamFault,
                ));
            }
            Ok(Some(body))
        }
        404 => Ok(None),
        s => {
            identity_warn!("[identity] resolver verify {path} answered {s}");
            Err(Answer::error(503, "identity proof temporarily unavailable", Outcome::UpstreamFault))
        }
    }
}

#[derive(Serialize)]
struct VerifyOut<'a> {
    #[serde(rename = "identityKey")]
    identity_key: &'a str,
    display: &'a Display,
    name: Option<&'a RawValue>,
    picture: Option<&'a RawValue>,
}

/// `GET /identity/verify/:ik`: the mapped picks, then the resolver's SMT
/// proof of each (at most three resolver calls).
pub async fn verify_answer<R: Resolver, K: KillList>(r: &R, k: &K, key: &str, origin: &str) -> Answer {
    let batch = match fetch_batch(r, &[key.to_string()]).await {
        Ok(b) => b,
        Err(a) => return a,
    };
    let killed = killed_or_none(k, &batch).await;
    let e = &batch.entries[0];
    let display = map_display(e.pick.as_ref(), &e.names, killed.as_ref(), origin);
    let name = match display.name.as_deref().filter(|n| is_normalized_name(n)) {
        Some(n) => match fetch_verify(r, &format!("{RESOLVER_VERIFY_NAME_PATH}{n}")).await {
            Ok(v) => v,
            Err(a) => return a,
        },
        None => None,
    };
    let picture = match display.image_hash.as_deref() {
        Some(h) => match fetch_verify(r, &format!("{RESOLVER_VERIFY_PICTURE_PATH}{h}")).await {
            Ok(v) => v,
            Err(a) => return a,
        },
        None => None,
    };
    let body = serde_json::to_string(&VerifyOut {
        identity_key: key,
        display: &display,
        name: name.as_deref(),
        picture: picture.as_deref(),
    })
    .expect("raw values and strings serialize");
    json_answer(body, killed.is_some())
}

/// `GET /identity/pic/:imageHash`.
pub async fn picture_answer<R: Resolver, K: KillList>(r: &R, k: &K, hash_param: &str) -> Answer {
    let Some(hash) = parse_image_hash(hash_param) else {
        return Answer::picture_not_found(Outcome::BadRequest);
    };
    match k.killed_among(std::slice::from_ref(&hash)).await {
        Ok(set) if set.contains(&hash) => return Answer::picture_not_found(Outcome::Killed),
        Ok(_) => {}
        Err(e) => {
            identity_warn!("[identity] kill list read failed, picture refused: {e}");
            let mut a = Answer::error(503, "picture temporarily unavailable", Outcome::KillListFault);
            a.headers.push(("Content-Security-Policy", PICTURE_CSP.to_string()));
            return a;
        }
    }
    let up = match r.get(&format!("{RESOLVER_PICTURE_PATH}{hash}"), PICTURE_MAX_BYTES).await {
        Ok(up) => up,
        Err(UpstreamError::TooLarge) => return Answer::picture_not_found(Outcome::Oversize),
        Err(e) => {
            let mut a = upstream_fault(&e);
            a.headers.push(("Content-Security-Policy", PICTURE_CSP.to_string()));
            return a;
        }
    };
    if up.status != 200 {
        return Answer::picture_not_found(Outcome::NotFound);
    }
    match picture_gate(&hash, &up.body) {
        PictureGate::Serve(mime) => Answer {
            status: 200,
            headers: vec![
                ("Content-Type", mime.to_string()),
                ("Cache-Control", PICTURE_CACHE_CONTROL.to_string()),
                ("X-Content-Type-Options", "nosniff".to_string()),
                ("Content-Security-Policy", PICTURE_CSP.to_string()),
                ("ETag", format!("\"{hash}\"")),
            ],
            body: up.body,
            outcome: Outcome::Served,
        },
        PictureGate::Oversize => Answer::picture_not_found(Outcome::Oversize),
        PictureGate::HashMismatch => Answer::picture_not_found(Outcome::HashMismatch),
        PictureGate::RefusedType => Answer::picture_not_found(Outcome::RefusedType),
    }
}

fn kill_body_too_large() -> Answer {
    Answer::error(413, "body over 4 KB", Outcome::BadRequest)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct KillBody {
    #[serde(rename = "imageHash")]
    image_hash: String,
    #[serde(default)]
    reason: String,
    #[serde(default)]
    unkill: bool,
}

/// `POST /internal/identity/kill` after the bearer check: `{imageHash,
/// reason?}` kills, `{imageHash, unkill: true}` lifts a kill.
pub async fn kill_answer<K: KillList>(k: &K, body: &[u8], now_ms: i64) -> Answer {
    if body.len() > KILL_BODY_MAX_BYTES {
        return kill_body_too_large();
    }
    let parsed: KillBody = match serde_json::from_slice(body) {
        Ok(b) => b,
        Err(_) => {
            return Answer::error(
                400,
                "body must be {\"imageHash\": <64 hex>, \"reason\"?: <text>, \"unkill\"?: true}",
                Outcome::BadRequest,
            )
        }
    };
    let Some(hash) = parse_image_hash(&parsed.image_hash) else {
        return Answer::error(400, "imageHash must be 64 hex chars", Outcome::BadRequest);
    };
    if parsed.reason.len() > KILL_REASON_MAX_BYTES {
        return Answer::error(400, "reason is over 500 bytes", Outcome::BadRequest);
    }
    let result = if parsed.unkill {
        k.unkill(&hash).await
    } else {
        k.kill(&hash, &parsed.reason, now_ms).await
    };
    match result {
        Ok(()) => Answer::json(
            200,
            serde_json::json!({ "imageHash": hash, "killed": !parsed.unkill }).to_string(),
            NO_STORE,
            Outcome::Served,
        ),
        Err(e) => {
            identity_warn!("[identity] kill list write failed: {e}");
            Answer::error(503, "kill list unavailable", Outcome::KillListFault)
        }
    }
}

// ---------------------------------------------------------------------------
// Budgets (per isolate)
// ---------------------------------------------------------------------------

pub const BUDGET_WINDOW_MS: i64 = 60_000;
pub const IP_KEYS_PER_WINDOW: u32 = 3_000;
pub const IDENTITY_KEYS_PER_WINDOW: u32 = 6_000;
pub const BUDGET_MAX_TRACKED: usize = 4_096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetScope {
    Ip,
    Identity,
}

impl BudgetScope {
    pub fn as_str(self) -> &'static str {
        match self {
            BudgetScope::Ip => "ip",
            BudgetScope::Identity => "identity",
        }
    }
}

/// Fixed one-minute windows per caller: `key -> (window start ms, units used)`.
#[derive(Debug, Default)]
pub struct Budget {
    windows: HashMap<String, (i64, u32)>,
    pub evictions: u64,
}

impl Budget {
    fn used(&self, key: &str, now_ms: i64) -> u32 {
        match self.windows.get(key) {
            Some((start, used)) if now_ms - start < BUDGET_WINDOW_MS => *used,
            _ => 0,
        }
    }

    fn retry_after(&self, key: &str, now_ms: i64) -> i64 {
        self.windows
            .get(key)
            .map(|(start, _)| (start + BUDGET_WINDOW_MS - now_ms).max(0))
            .unwrap_or(0)
    }

    fn add(&mut self, key: String, units: u32, now_ms: i64) {
        if !self.windows.contains_key(&key) && self.windows.len() >= BUDGET_MAX_TRACKED {
            self.windows.retain(|_, (start, _)| now_ms - *start < BUDGET_WINDOW_MS);
            if self.windows.len() >= BUDGET_MAX_TRACKED {
                self.windows.clear();
                self.evictions += 1;
            }
        }
        let w = self.windows.entry(key).or_insert((now_ms, 0));
        if now_ms - w.0 >= BUDGET_WINDOW_MS {
            *w = (now_ms, 0);
        }
        w.1 = w.1.saturating_add(units);
    }

    /// Charge `units` to the IP and (when known) the identity, both or
    /// neither; past either limit the scope and the ms until its window ends.
    pub fn charge(
        &mut self,
        ip: &str,
        identity: Option<&str>,
        units: u32,
        now_ms: i64,
    ) -> std::result::Result<(), (BudgetScope, i64)> {
        let ip_key = format!("ip:{ip}");
        if self.used(&ip_key, now_ms).saturating_add(units) > IP_KEYS_PER_WINDOW {
            return Err((BudgetScope::Ip, self.retry_after(&ip_key, now_ms)));
        }
        let id_key = identity.map(|i| format!("id:{i}"));
        if let Some(k) = &id_key {
            if self.used(k, now_ms).saturating_add(units) > IDENTITY_KEYS_PER_WINDOW {
                return Err((BudgetScope::Identity, self.retry_after(k, now_ms)));
            }
        }
        self.add(ip_key, units, now_ms);
        if let Some(k) = id_key {
            self.add(k, units, now_ms);
        }
        Ok(())
    }
}

/// The 429 for a budget refusal.
pub fn budget_refusal(scope: BudgetScope, retry_after_ms: i64) -> Answer {
    let mut a = Answer::json(
        429,
        serde_json::json!({
            "error": "identity read budget exhausted",
            "code": "ERR_IDENTITY_BUDGET",
            "scope": scope.as_str(),
            "retryAfterMs": retry_after_ms,
        })
        .to_string(),
        NO_STORE,
        Outcome::BudgetRefused,
    );
    a.headers.push(("Retry-After", ((retry_after_ms + 999) / 1000).to_string()));
    a
}

thread_local! {
    static BUDGET: RefCell<Budget> = RefCell::new(Budget::default());
}

fn charge_isolate(ip: &str, identity: Option<&str>, units: u32) -> Option<Answer> {
    let now = worker::Date::now().as_millis() as i64;
    BUDGET.with(|b| b.borrow_mut().charge(ip, identity, units, now).err())
        .map(|(scope, retry)| budget_refusal(scope, retry))
}

// ---------------------------------------------------------------------------
// Counters
// ---------------------------------------------------------------------------

static COUNTS: [[AtomicU64; 11]; 5] = [const { [const { AtomicU64::new(0) }; 11] }; 5];
/// Resolver entries that carried no `display` (an old resolver): mapped all null.
static DISPLAY_ABSENT: AtomicU64 = AtomicU64::new(0);
/// Picks whose `name` is no row of the same body's `names[]`: `since` null.
static DISPLAY_SINCE_MISS: AtomicU64 = AtomicU64::new(0);

pub fn count(route: RouteId, outcome: Outcome) {
    COUNTS[route as usize][outcome as usize].fetch_add(1, Ordering::Relaxed);
}

/// The `/health` block: `routes.<route>.<outcome>`, `display` and the budget.
pub fn health_json() -> serde_json::Value {
    let mut routes = serde_json::Map::new();
    for (r, rname) in ROUTE_NAMES.iter().enumerate() {
        let mut o = serde_json::Map::new();
        for (i, oname) in OUTCOME_NAMES.iter().enumerate() {
            o.insert((*oname).to_string(), COUNTS[r][i].load(Ordering::Relaxed).into());
        }
        routes.insert((*rname).to_string(), serde_json::Value::Object(o));
    }
    let evictions = BUDGET.with(|b| b.borrow().evictions);
    serde_json::json!({
        "routes": routes,
        "display": {
            "sinceMiss": DISPLAY_SINCE_MISS.load(Ordering::Relaxed),
            "absent": DISPLAY_ABSENT.load(Ordering::Relaxed),
        },
        "budget": {
            "windowMs": BUDGET_WINDOW_MS,
            "ipKeysPerWindow": IP_KEYS_PER_WINDOW,
            "identityKeysPerWindow": IDENTITY_KEYS_PER_WINDOW,
            "evictions": evictions,
        },
    })
}

// ---------------------------------------------------------------------------
// CORS (the app origins only)
// ---------------------------------------------------------------------------

/// True for the browser-facing identity paths (the JSON routes and the picture).
pub fn is_identity_path(path: &str) -> bool {
    path == BATCH_ROUTE || path.starts_with(IDENTITY_ROUTE_PREFIX)
}

/// The allowlist from `IDENTITY_APP_ORIGINS`: comma separated, trimmed, any
/// trailing `/` dropped, lowercase.
pub fn parse_origins(v: &str) -> Vec<String> {
    v.split(',')
        .map(|s| s.trim().trim_end_matches('/').to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .collect()
}

/// The origin to answer with, iff the request's `Origin` is on the allowlist.
pub fn allowed_origin(origin: Option<&str>, allow: &[String]) -> Option<String> {
    let o = origin?.trim();
    let lc = o.to_ascii_lowercase();
    allow.contains(&lc).then(|| o.to_string())
}

/// The CORS header set of an identity answer. Pure.
pub fn cors_headers(origin: Option<&str>) -> Vec<(&'static str, String)> {
    let auth_list = crate::cors::auth_header_list();
    let (lane_allow, lane_expose) = crate::cors::lane_header_lists();
    let mut h = vec![("Vary", "Origin".to_string())];
    if let Some(o) = origin {
        h.push(("Access-Control-Allow-Origin", o.to_string()));
        h.push(("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS".to_string()));
        h.push((
            "Access-Control-Allow-Headers",
            format!("Content-Type, {auth_list}, {lane_allow}"),
        ));
        h.push((
            "Access-Control-Expose-Headers",
            format!("{auth_list}, {lane_expose}, Retry-After, ETag"),
        ));
        h.push(("Access-Control-Max-Age", "86400".to_string()));
    }
    h
}

/// The CORS decision for one identity request, taken before the request moves.
#[derive(Debug, Clone)]
pub struct IdentityCors {
    origin: Option<String>,
}

impl IdentityCors {
    /// `Some` iff `req` is on an identity path.
    pub fn for_request(req: &Request, env: &Env) -> Option<Self> {
        if !is_identity_path(&req.path()) {
            return None;
        }
        let allow = env
            .var(APP_ORIGINS_VAR)
            .map(|v| parse_origins(&v.to_string()))
            .unwrap_or_default();
        let origin = req.headers().get("Origin").ok().flatten();
        Some(IdentityCors {
            origin: allowed_origin(origin.as_deref(), &allow),
        })
    }

    /// Replace whatever CORS the response carries with the identity set.
    pub fn apply(&self, resp: &mut Response) {
        let h = resp.headers_mut();
        for name in [
            "Access-Control-Allow-Origin",
            "Access-Control-Allow-Methods",
            "Access-Control-Allow-Headers",
            "Access-Control-Expose-Headers",
            "Access-Control-Max-Age",
        ] {
            let _ = h.delete(name);
        }
        for (k, v) in cors_headers(self.origin.as_deref()) {
            let _ = h.set(k, &v);
        }
    }

    pub fn preflight(&self) -> Result<Response> {
        let mut resp = Response::empty()?.with_status(204);
        self.apply(&mut resp);
        Ok(resp)
    }
}

// ---------------------------------------------------------------------------
// The worker glue
// ---------------------------------------------------------------------------

/// The resolver through the `IDENTITY` service binding.
pub struct BindingResolver {
    svc: worker::Fetcher,
    base: String,
}

impl BindingResolver {
    pub fn from_env(env: &Env) -> Option<Self> {
        let svc = env.service(RESOLVER_BINDING).ok()?;
        let base = env
            .var(RESOLVER_URL_VAR)
            .map(|v| v.to_string().trim().trim_end_matches('/').to_string())
            .ok()
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| "https://low-identity-resolver".to_string());
        Some(BindingResolver { svc, base })
    }
}

async fn with_timeout<F: core::future::Future>(fut: F, ms: u64) -> Option<F::Output> {
    use futures_util::future::Either;
    let delay = worker::Delay::from(core::time::Duration::from_millis(ms));
    futures_util::pin_mut!(fut, delay);
    match futures_util::future::select(fut, delay).await {
        Either::Left((out, _)) => Some(out),
        Either::Right(((), _)) => None,
    }
}

impl Resolver for BindingResolver {
    async fn get(&self, path_and_query: &str, max_bytes: usize) -> std::result::Result<Upstream, UpstreamError> {
        let url = format!("{}{path_and_query}", self.base);
        let fut = async {
            let mut init = RequestInit::new();
            init.with_method(Method::Get);
            let headers = Headers::new();
            let _ = headers.set("Accept", "application/json, image/*");
            init.with_headers(headers);
            let mut resp = self
                .svc
                .fetch(url.as_str(), Some(init))
                .await
                .map_err(|e| UpstreamError::Transport(e.to_string()))?;
            let status = resp.status_code();
            if let Ok(Some(cl)) = resp.headers().get("Content-Length") {
                if cl.trim().parse::<u64>().map(|n| n > max_bytes as u64).unwrap_or(false) {
                    return Err(UpstreamError::TooLarge);
                }
            }
            let mut stream = resp.stream().map_err(|e| UpstreamError::Transport(e.to_string()))?;
            let mut body: Vec<u8> = Vec::new();
            use futures_util::StreamExt;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|e| UpstreamError::Transport(e.to_string()))?;
                if body.len() + chunk.len() > max_bytes {
                    return Err(UpstreamError::TooLarge);
                }
                body.extend_from_slice(&chunk);
            }
            Ok(Upstream { status, body })
        };
        with_timeout(fut, RESOLVER_TIMEOUT_MS).await.unwrap_or(Err(UpstreamError::Timeout))
    }
}

/// A resolver that is not bound: every call is a transport fault.
pub struct Unbound;

impl Resolver for Unbound {
    async fn get(&self, _: &str, _: usize) -> std::result::Result<Upstream, UpstreamError> {
        Err(UpstreamError::Transport("the IDENTITY binding is not configured".to_string()))
    }
}

/// The kill list in the `IDENTITY_KILL` KV namespace.
pub struct KvKillList(Option<worker::KvStore>);

impl KvKillList {
    pub fn from_env(env: &Env) -> Self {
        KvKillList(env.kv(KILL_BINDING).ok())
    }

    fn store(&self) -> std::result::Result<&worker::KvStore, String> {
        self.0.as_ref().ok_or_else(|| format!("the {KILL_BINDING} KV binding is not configured"))
    }
}

pub fn kill_key(hash: &str) -> String {
    format!("{KILL_KEY_PREFIX}{hash}")
}

impl KillList for KvKillList {
    async fn killed_among(&self, hashes: &[String]) -> std::result::Result<HashSet<String>, String> {
        let kv = self.store()?;
        let mut out = HashSet::new();
        for chunk in hashes.chunks(KILL_BULK_CHUNK) {
            let keys: Vec<String> = chunk.iter().map(|h| kill_key(h)).collect();
            let got = kv.get_bulk(&keys).text().await.map_err(|e| e.to_string())?;
            for h in chunk {
                if matches!(got.get(&kill_key(h)), Some(Some(_))) {
                    out.insert(h.clone());
                }
            }
        }
        Ok(out)
    }

    async fn kill(&self, hash: &str, reason: &str, now_ms: i64) -> std::result::Result<(), String> {
        let value = serde_json::json!({ "reason": reason, "killedAtMs": now_ms }).to_string();
        self.store()?
            .put(&kill_key(hash), value)
            .map_err(|e| e.to_string())?
            .execute()
            .await
            .map_err(|e| e.to_string())
    }

    async fn unkill(&self, hash: &str) -> std::result::Result<(), String> {
        self.store()?.delete(&kill_key(hash)).await.map_err(|e| e.to_string())
    }
}

fn to_response(a: Answer) -> Result<Response> {
    let mut resp = Response::from_bytes(a.body)?.with_status(a.status);
    let h = resp.headers_mut();
    for (k, v) in &a.headers {
        h.set(k, v)?;
    }
    Ok(resp)
}

/// The caller's IP; a caller without `CF-Connecting-IP` (a service binding,
/// local dev) shares the one window `unknown`.
fn client_ip(req: &Request) -> String {
    req.headers()
        .get("CF-Connecting-IP")
        .ok()
        .flatten()
        .unwrap_or_else(|| "unknown".to_string())
}

fn origin_of(req: &Request) -> String {
    req.url().map(|u| u.origin().ascii_serialization()).unwrap_or_default()
}

fn verified_identity(state: &AuthState) -> Option<&str> {
    match &state.caller {
        CallerAuth::Verified(k) => Some(k.as_str()),
        CallerAuth::Anonymous => None,
    }
}

async fn run<R: Resolver>(
    r: &R,
    env: &Env,
    route: RouteId,
    req: &Request,
    identity: Option<&str>,
    key_param: &str,
) -> Answer {
    let kill = KvKillList::from_env(env);
    let origin = origin_of(req);
    match route {
        RouteId::Batch => {
            let raw = req
                .url()
                .ok()
                .and_then(|u| u.query_pairs().find(|(k, _)| k == "ik").map(|(_, v)| v.into_owned()))
                .unwrap_or_default();
            let keys = match parse_identity_keys(&raw) {
                Ok(k) => k,
                Err(e) => return Answer::error(400, &e, Outcome::BadRequest),
            };
            if let Some(a) = charge_isolate(&client_ip(req), identity, keys.len() as u32) {
                return a;
            }
            batch_answer(r, &kill, &keys, &origin).await
        }
        RouteId::Single | RouteId::Verify => {
            let Some(key) = parse_identity_key(key_param) else {
                return Answer::error(400, "not a compressed identity key (66 hex chars, 02/03 prefix)", Outcome::BadRequest);
            };
            // Verify costs three resolver calls, so it is charged three keys.
            let units = if route == RouteId::Verify { 3 } else { 1 };
            if let Some(a) = charge_isolate(&client_ip(req), identity, units) {
                return a;
            }
            if route == RouteId::Single {
                single_answer(r, &kill, &key, &origin).await
            } else {
                verify_answer(r, &kill, &key, &origin).await
            }
        }
        RouteId::Picture => {
            if let Some(a) = charge_isolate(&client_ip(req), None, 1) {
                return a;
            }
            picture_answer(r, &kill, key_param).await
        }
        RouteId::Kill => Answer::error(404, "no such route", Outcome::NotFound),
    }
}

async fn serve(env: &Env, route: RouteId, req: &Request, identity: Option<&str>, key_param: &str) -> Result<Response> {
    let answer = match BindingResolver::from_env(env) {
        Some(r) => run(&r, env, route, req, identity, key_param).await,
        None => run(&Unbound, env, route, req, identity, key_param).await,
    };
    count(route, answer.outcome);
    to_response(answer)
}

/// `GET /identities` (router).
pub async fn get_batch(req: Request, ctx: RouteContext<AuthState>) -> Result<Response> {
    serve(&ctx.env, RouteId::Batch, &req, verified_identity(&ctx.data), "").await
}

/// `GET /identity/*rest` (router): `:ik` or `verify/:ik`.
pub async fn get_identity(req: Request, ctx: RouteContext<AuthState>) -> Result<Response> {
    let rest = ctx.param("rest").cloned().unwrap_or_default();
    let identity = verified_identity(&ctx.data);
    match rest.strip_prefix(VERIFY_ROUTE_PREFIX) {
        Some(ik) if !ik.contains('/') => serve(&ctx.env, RouteId::Verify, &req, identity, ik).await,
        None if !rest.contains('/') => serve(&ctx.env, RouteId::Single, &req, identity, &rest).await,
        _ => crate::routes::json_error(&format!("no such route: {}", req.path()), 404),
    }
}

/// The two identity routes served BEFORE the BRC-103 front door: the picture
/// bytes (GET) and the operator's kill list (POST).
pub fn is_front_door_exempt(method: &Method, path: &str) -> bool {
    (*method == Method::Get && path.starts_with(PICTURE_ROUTE_PREFIX))
        || (*method == Method::Post && path == KILL_ROUTE)
}

/// Serve a [`is_front_door_exempt`] request.
pub async fn serve_exempt(mut req: Request, env: &Env) -> Result<Response> {
    let path = req.path();
    if req.method() == Method::Post {
        if !crate::internal_events::internal_bearer_ok(&req, env) {
            count(RouteId::Kill, Outcome::Unauthorized);
            return to_response(Answer::error(401, "unauthorized", Outcome::Unauthorized));
        }
        let declared = req
            .headers()
            .get("Content-Length")
            .ok()
            .flatten()
            .and_then(|v| v.trim().parse::<usize>().ok());
        if declared.is_some_and(|n| n > KILL_BODY_MAX_BYTES) {
            count(RouteId::Kill, Outcome::BadRequest);
            return to_response(kill_body_too_large());
        }
        let body = req.bytes().await.unwrap_or_default();
        let now = worker::Date::now().as_millis() as i64;
        let answer = kill_answer(&KvKillList::from_env(env), &body, now).await;
        count(RouteId::Kill, answer.outcome);
        return to_response(answer);
    }
    let hash = path.strip_prefix(PICTURE_ROUTE_PREFIX).unwrap_or_default().to_string();
    serve(env, RouteId::Picture, &req, None, &hash).await
}

#[cfg(test)]
#[path = "identity_tests.rs"]
mod tests;
