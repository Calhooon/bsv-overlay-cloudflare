//! Worker-side ChainTracker — uses Cloudflare Workers Fetch API to verify
//! merkle roots against the ChainTracks API.
//!
//! Implements the `bsv_rs::transaction::ChainTracker` trait for SPV verification
//! in the overlay Engine.
//!
//! ## Send safety
//!
//! The upstream `ChainTracker` trait requires `Send + Sync` and its async methods
//! return `Pin<Box<dyn Future + Send>>` (via `#[async_trait]`). However, the
//! `worker::Fetch` API uses `JsFuture` which contains `Rc<RefCell<...>>` and is
//! therefore not `Send`.
//!
//! On wasm32, there is only a single thread — `Send` is meaningless and the
//! compiler normally auto-implements it. The `worker` crate's internals break
//! this because they contain explicit `!Send` types. We use an `UnsafeSendFuture`
//! wrapper to assert `Send` on the future, which is sound on wasm32 where no
//! cross-thread transfer can ever occur.

use std::future::Future;
use std::pin::Pin;

use bsv_rs::transaction::{ChainTracker, ChainTrackerError};
use serde::Deserialize;

/// ChainTracker implementation using Cloudflare Workers `Fetch` API
/// against a ChainTracks API server (e.g. `<your-chain-tracker-api>`).
///
/// Endpoints used:
/// - `GET /findHeaderHexForHeight?height={N}` — returns block header with merkle root
/// - `GET /findChainTipHeaderHex`: the served, VERIFIED header tip (its height, hash and
///   merkle root): the engine's chain tip (bsv-low #896), the reader the app layer's `/tip` uses
pub struct WorkerChainTracker {
    base_url: String,
    /// When set, requests route through a Cloudflare SERVICE BINDING instead of a
    /// public-URL fetch. This matters when ChainTracks is another Worker on the
    /// SAME account: a `workers.dev` subrequest to a same-account Worker loops
    /// back to the CALLER (returns the caller's own 404), so a plain URL fetch
    /// silently fails. A service binding routes to the target Worker correctly.
    /// `base_url` still supplies the request URL (only its PATH is used by the
    /// bound service; the host is resolved via the binding).
    service: Option<worker::Fetcher>,
    /// Per-isolate `(fetched_at_ms, tip)` memo for `current_height`
    /// (bsv-low #289): `ls_low`'s expiry filter resolves the tip on EVERY
    /// lobby poll, which was an uncached network subrequest each time. A tip
    /// is stale-tolerant by construction (blocks are ~10 min apart; the
    /// expiry filter fails open on error anyway), so a [`TIP_CACHE_TTL_MS`]
    /// memo is behaviourally invisible while removing the per-poll fetch.
    /// SUCCESS-ONLY: an error is never cached. `is_valid_root_for_height`
    /// (the SPV verify path) is deliberately untouched.
    tip_cache: TipCache,
}

/// The tracker's memo of its last verified tip and when it was read.
type TipCache = std::rc::Rc<std::cell::RefCell<Option<(f64, EngineTip)>>>;

/// bsv-low #896: chaintracks' served, verified header tip as the engine reads it (`GET /findChainTipHeaderHex`, the
/// header chaintracks serves under the same validation ceiling as the proof headers `is_valid_root_for_height`
/// reads). The height and the hash are one chaintracks row; never the couriers' `getPresentHeight` maximum.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineTip {
    /// The verified tip's height.
    pub height: u32,
    /// Its block hash, lowercase hex.
    pub hash: String,
    /// Its merkle root, lowercase hex.
    pub merkle_root: String,
}

/// How long a fetched chain tip is served from memory (30 s ≪ the ~10 min
/// block cadence — worst case the expiry filter runs against a 30 s-old tip,
/// strictly better than its fail-open no-filter degradation).
const TIP_CACHE_TTL_MS: f64 = 30_000.0;

/// Wall-clock ms. `worker::Date` inside the Workers runtime; std time in
/// native unit tests (worker::Date requires a JS host).
fn now_ms() -> f64 {
    #[cfg(target_arch = "wasm32")]
    {
        worker::Date::now().as_millis() as f64
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as f64)
            .unwrap_or(0.0)
    }
}

/// Pure cache-read: `Some(tip)` iff a cached entry exists and is younger
/// than `ttl_ms`. Factored out for direct unit testing.
fn tip_cache_read<T>(cached: Option<(f64, T)>, now_ms: f64, ttl_ms: f64) -> Option<T> {
    cached.and_then(|(at, height)| {
        // A clock that moved backwards (now < at) reads as fresh — harmless
        // for a 30 s tip memo, and impossible within one isolate anyway.
        (now_ms - at < ttl_ms).then_some(height)
    })
}

// SAFETY: wasm32 is single-threaded. There are no other threads to send to.
#[allow(unsafe_code)]
unsafe impl Send for WorkerChainTracker {}
// SAFETY: wasm32 is single-threaded. There are no other threads to share with.
#[allow(unsafe_code)]
unsafe impl Sync for WorkerChainTracker {}

impl WorkerChainTracker {
    /// Create a WorkerChainTracker that fetches ChainTracks over a public URL.
    /// Correct for a ChainTracks on a DIFFERENT origin/account; for a
    /// same-account Worker use [`WorkerChainTracker::with_service`].
    pub fn new(base_url: String) -> Self {
        Self {
            base_url,
            service: None,
            tip_cache: TipCache::default(),
        }
    }

    /// Route ChainTracks requests through a Cloudflare service binding, avoiding
    /// the same-account `workers.dev` loopback (bsv-low #148). `base_url` still
    /// supplies the request path.
    pub fn with_service(base_url: String, service: worker::Fetcher) -> Self {
        Self {
            base_url,
            service: Some(service),
            tip_cache: TipCache::default(),
        }
    }
}

/// Send a GET either through a service binding (no same-account loopback) or a
/// plain URL fetch, returning the worker Response for uniform handling.
async fn ct_get(
    url: String,
    service: &Option<worker::Fetcher>,
    ctx: &str,
) -> Result<worker::Response, ChainTrackerError> {
    let mut init = worker::RequestInit::new();
    init.with_method(worker::Method::Get);
    let headers = worker::Headers::new();
    let _ = headers.set("Accept", "application/json");
    init.with_headers(headers);

    match service {
        Some(f) => f.fetch(url, Some(init)).await.map_err(|e| {
            ChainTrackerError::NetworkError(format!(
                "ChainTracks {ctx} (service binding) failed: {e}"
            ))
        }),
        None => {
            let request = worker::Request::new_with_init(&url, &init)
                .map_err(|e| ChainTrackerError::Other(format!("Failed to create request: {e}")))?;
            worker::Fetch::Request(request).send().await.map_err(|e| {
                ChainTrackerError::NetworkError(format!("ChainTracks {ctx} failed: {e}"))
            })
        }
    }
}

/// Generic response frame from ChainTracks API.
/// All endpoints return `{"status": "success"|"error", "value": T}`.
#[derive(Debug, Deserialize)]
struct ResponseFrame<T> {
    status: String,
    value: Option<T>,
}

impl<T> ResponseFrame<T> {
    fn is_success(&self) -> bool {
        self.status == "success"
    }
}

/// Block header as returned by the ChainTracks API (camelCase JSON).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CtBlockHeader {
    pub merkle_root: String,
}

/// Wrapper that asserts `Send` on a `!Send` future.
///
/// SAFETY: This is only used on wasm32 where there is a single thread.
/// No actual cross-thread sending occurs.
struct UnsafeSendFuture<F>(F);

// SAFETY: wasm32 is single-threaded — no cross-thread transfer occurs.
#[allow(unsafe_code)]
unsafe impl<F> Send for UnsafeSendFuture<F> {}

impl<F: Future> Future for UnsafeSendFuture<F> {
    type Output = F::Output;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        // SAFETY: We only project to the inner future, maintaining pinning.
        #[allow(unsafe_code)]
        unsafe {
            self.map_unchecked_mut(|s| &mut s.0).poll(cx)
        }
    }
}

/// Fetch a block header from ChainTracks and check if the merkle root matches.
async fn fetch_is_valid_root(
    base_url: String,
    service: Option<worker::Fetcher>,
    root: String,
    height: u32,
) -> Result<bool, ChainTrackerError> {
    let url = format!(
        "{}/findHeaderHexForHeight?height={}",
        base_url.trim_end_matches('/'),
        height
    );

    let mut response = ct_get(url, &service, "findHeaderHexForHeight").await?;

    let status = response.status_code();
    if !(200..300).contains(&status) {
        if status == 404 {
            return Err(ChainTrackerError::BlockNotFound(height));
        }
        return Err(ChainTrackerError::InvalidResponse(format!(
            "ChainTracks returned HTTP {status}"
        )));
    }

    let frame: ResponseFrame<CtBlockHeader> = response
        .json()
        .await
        .map_err(|e| ChainTrackerError::InvalidResponse(format!("ChainTracks parse error: {e}")))?;

    if !frame.is_success() {
        return Err(ChainTrackerError::InvalidResponse(format!(
            "ChainTracks findHeaderHexForHeight: status={}",
            frame.status
        )));
    }

    root_matches_frame(frame.value.as_ref(), &root)
}

/// Decide `is_valid_root_for_height` from a parsed `success` frame.
///
/// FAIL-SAFE on an absent header (a `{status:"success", value:null}` body):
/// return `Ok(false)` — an unverifiable root is NOT valid. The former code
/// fail-OPENED here (`Ok(true)`), which — now that `ls_pot`'s money
/// confirmation depends on this check (bsv-low pot landing proof) — would let
/// a forged single-leaf bump (`root == spending_txid`) at a live-range height
/// be recorded as `spentConfirmed`, clobbering a real confirmed pointer.
///
/// This branch is DEAD against the deployed `rust-chaintracks`, which serves a
/// FULL header on success (incl. a verified WoC read-through for fresh
/// tip+1..+6 blocks) or a 404 for anything it cannot verify — it never emits
/// `success + null`. So failing safe changes nothing in practice while closing
/// the latent trap; a genuinely-recent block is served a real header (matched
/// below) or 404s (→ `BlockNotFound` at the caller), never silently accepted.
fn root_matches_frame(
    header: Option<&CtBlockHeader>,
    root: &str,
) -> Result<bool, ChainTrackerError> {
    match header {
        Some(h) => Ok(h.merkle_root == root),
        // bsv-low M19 round 2 (review L1): a 2xx frame with NO header is not
        // a verdict on the root. It used to read as `Ok(false)` (refuted),
        // which the revalidation sweep would have DEMOTED on; a fault is
        // counted and changes nothing, and the confirm arm stays closed on
        // it exactly as before.
        None => Err(ChainTrackerError::InvalidResponse(
            "ChainTracks findHeaderHexForHeight: success with no header (absent)".to_string(),
        )),
    }
}

/// bsv-low M19 round 2 (review H3): the block HASH chaintracks holds at
/// `height`, through the same binding/URL the tracker uses. The block-event
/// pass reads it when the announcer's body carried none (an older
/// chaintracks). `None` on any fault, logged by the caller.
pub(crate) async fn chaintracks_block_hash(env: &worker::Env, height: u64) -> Option<String> {
    chaintracks_block_hash_detailed(env, height)
        .await
        .ok()
        .flatten()
        .map(|h| h.0)
}

/// bsv-low M19B-G1: [`chaintracks_block_hash`] with the READ FAULT kept
/// apart from "no header at that height": `Ok(Some((hash, merkle_root)))` =
/// the canonical header chaintracks holds (round 2, review MED-5: the root
/// rides along so the pass can seed its memo from this one read);
/// `Ok(None)` = chaintracks answered 404 for the height (it has not reached
/// it: a lag, not a fault); `Err` = the source could not be read
/// (unconfigured, transport, a non-2xx other than 404, a malformed frame).
/// The Arcade event consumer holds on `Ok(None)` and counts an `Err`
/// without moving its cursor.
pub(crate) async fn chaintracks_block_hash_detailed(
    env: &worker::Env,
    height: u64,
) -> Result<Option<(String, Option<String>)>, String> {
    Ok(chaintracks_header_detailed(env, height)
        .await?
        .map(|h| (h.hash, h.merkle_root)))
}

/// The facts of the header chaintracks holds at a height (the hash, and the merkle root and header time
/// when well formed).
pub(crate) struct CtHeaderFacts {
    /// The block hash, lowercase hex.
    pub hash: String,
    /// The merkle root, lowercase hex, when well formed.
    pub merkle_root: Option<String>,
    /// The header time, unix seconds, when served.
    pub time: Option<u64>,
}

/// bsv-low #470 PRE557: [`chaintracks_block_hash_detailed`]'s read with the header's `time` kept; the
/// same `Ok(None)` (a 404) and `Err` (could not read) arms.
pub(crate) async fn chaintracks_header_detailed(
    env: &worker::Env,
    height: u64,
) -> Result<Option<CtHeaderFacts>, String> {
    #[derive(serde::Deserialize)]
    struct CtHash {
        hash: String,
        #[serde(rename = "merkleRoot", default)]
        merkle_root: Option<String>,
        #[serde(default)]
        time: Option<u64>,
    }
    let base_url = env
        .var("CHAIN_TRACKER_URL")
        .map(|v| v.to_string())
        .unwrap_or_else(|_| "https://chaintracks.invalid".to_string());
    let service = env.service("CHAINTRACKS").ok();
    if service.is_none() && env.var("CHAIN_TRACKER_URL").is_err() {
        return Err(
            "no header source configured (CHAINTRACKS binding / CHAIN_TRACKER_URL)".to_string(),
        );
    }
    let url = format!(
        "{}/findHeaderHexForHeight?height={height}",
        base_url.trim_end_matches('/')
    );
    let mut response = ct_get(url, &service, "findHeaderHexForHeight")
        .await
        .map_err(|e| format!("chaintracks header read at {height}: {e}"))?;
    let status = response.status_code();
    if status == 404 {
        return Ok(None);
    }
    if !(200..300).contains(&status) {
        return Err(format!(
            "chaintracks header read at {height}: HTTP {status}"
        ));
    }
    let frame: ResponseFrame<CtHash> = response
        .json()
        .await
        .map_err(|e| format!("chaintracks header read at {height}: parse: {e}"))?;
    if !frame.is_success() {
        return Err(format!(
            "chaintracks header read at {height}: status={}",
            frame.status
        ));
    }
    let header = frame
        .value
        .ok_or_else(|| format!("chaintracks header read at {height}: success with no header"))?;
    let hash = header.hash.trim().to_ascii_lowercase();
    if !(hash.len() == 64 && hash.bytes().all(|b| b.is_ascii_hexdigit())) {
        return Err(format!(
            "chaintracks header read at {height}: malformed hash"
        ));
    }
    let root = header
        .merkle_root
        .map(|r| r.trim().to_ascii_lowercase())
        .filter(|r| r.len() == 64 && r.bytes().all(|b| b.is_ascii_hexdigit()));
    Ok(Some(CtHeaderFacts {
        hash,
        merkle_root: root,
        time: header.time,
    }))
}

/// bsv-low #620: one page of chaintracks' chain-event feed, `GET /events?since=<since>` over the CHAINTRACKS binding
/// (or `CHAIN_TRACKER_URL`): its status and body, or the transport's fault. The body is the consumer's to read
/// (`chain_feed`), never interpreted here.
pub(crate) async fn chaintracks_events_page(
    env: &worker::Env,
    since: &str,
) -> Result<(u16, Vec<u8>), String> {
    let base_url = env
        .var("CHAIN_TRACKER_URL")
        .map(|v| v.to_string())
        .unwrap_or_else(|_| "https://chaintracks.invalid".to_string());
    let service = env.service("CHAINTRACKS").ok();
    if service.is_none() && env.var("CHAIN_TRACKER_URL").is_err() {
        return Err(
            "no header source configured (CHAINTRACKS binding / CHAIN_TRACKER_URL)".to_string(),
        );
    }
    let url = format!("{}/events?since={since}", base_url.trim_end_matches('/'));
    let mut response = ct_get(url, &service, "events")
        .await
        .map_err(|e| format!("chaintracks events at {since}: {e}"))?;
    let status = response.status_code();
    let body = response
        .bytes()
        .await
        .map_err(|e| format!("chaintracks events at {since}: body: {e}"))?;
    Ok((status, body))
}

/// bsv-low #470 PRE557: the confirming block's time from OUR header source, never a courier: the header
/// chaintracks holds at the bump's height, accepted only when its merkle root is the root the bump
/// computes (so the time is the time of the block the proof binds to).
pub struct ChaintracksBlockTime {
    env: worker::Env,
}

impl ChaintracksBlockTime {
    /// The source over the env's `CHAINTRACKS` binding (or `CHAIN_TRACKER_URL`), the tracker's own path.
    pub fn new(env: &worker::Env) -> Self {
        Self { env: env.clone() }
    }
}

#[async_trait::async_trait(?Send)]
impl crate::proof_fetcher::BlockTimeSource for ChaintracksBlockTime {
    async fn block_time(&self, height: u32, merkle_root: &str) -> Result<Option<u64>, String> {
        let facts = chaintracks_header_detailed(&self.env, u64::from(height)).await?;
        Ok(header_time_for_root(facts.as_ref(), merkle_root))
    }
}

/// Pure: the header's time when the header carries `merkle_root`; a missing header, another root or a
/// header with no time is `None` (the row stays unstamped and is asked again on a later pass).
pub(crate) fn header_time_for_root(
    facts: Option<&CtHeaderFacts>,
    merkle_root: &str,
) -> Option<u64> {
    let facts = facts?;
    let root = facts.merkle_root.as_deref()?;
    if !root.eq_ignore_ascii_case(merkle_root.trim()) {
        return None;
    }
    facts.time.filter(|t| *t > 0)
}

/// One chaintracks GET over the binding (or the URL): its status and its JSON body (`Null` for a non-2xx answer,
/// whose body is not read). A transport fault is the fault it is.
async fn ct_get_json(
    base_url: &str,
    service: &Option<worker::Fetcher>,
    path: String,
) -> Result<(u16, serde_json::Value), ChainTrackerError> {
    let url = format!("{}/{path}", base_url.trim_end_matches('/'));
    let ctx = path.split('?').next().unwrap_or_default().to_string();
    let mut response = ct_get(url, service, &ctx).await?;
    let status = response.status_code();
    if !(200..300).contains(&status) {
        return Ok((status, serde_json::Value::Null));
    }
    let body = response
        .json()
        .await
        .map_err(|e| ChainTrackerError::InvalidResponse(format!("ChainTracks parse error: {e}")))?;
    Ok((status, body))
}

/// bsv-low #896: the engine's chain tip from chaintracks, through `get` (a route and its answer): the Worker's
/// binding in production, a fixture in the pins. ONE request, `findChainTipHeaderHex`: the verified header tip.
/// Every answer that is not a well formed verified header (a non-2xx, an error frame, a missing or malformed height,
/// hash, merkle root or time) is a FAULT, never a height by a guess, and never the couriers' present height.
pub(crate) async fn verified_tip_with<G, F>(get: G) -> Result<EngineTip, ChainTrackerError>
where
    G: Fn(String) -> F,
    F: Future<Output = Result<(u16, serde_json::Value), ChainTrackerError>>,
{
    let (status, frame) = get("findChainTipHeaderHex".into()).await?;
    if !(200..300).contains(&status) {
        return Err(ChainTrackerError::InvalidResponse(format!(
            "ChainTracks findChainTipHeaderHex returned HTTP {status}"
        )));
    }
    if frame["status"] != "success" {
        return Err(ChainTrackerError::NetworkError(format!(
            "ChainTracks findChainTipHeaderHex: status={}",
            frame["status"]
        )));
    }
    let value = &frame["value"];
    let malformed = |what: &str| {
        ChainTrackerError::InvalidResponse(format!(
            "ChainTracks findChainTipHeaderHex: malformed or missing {what}"
        ))
    };
    let hex64 = |key: &str| {
        value[key]
            .as_str()
            .map(str::trim)
            .filter(|v| v.len() == 64 && v.bytes().all(|b| b.is_ascii_hexdigit()))
            .map(str::to_ascii_lowercase)
            .ok_or_else(|| malformed(key))
    };
    let height = value["height"]
        .as_u64()
        .and_then(|h| u32::try_from(h).ok())
        .ok_or_else(|| malformed("height"))?;
    let hash = hex64("hash")?;
    let merkle_root = hex64("merkleRoot")?;
    // the app layer's reader refuses a tip frame with no time; so does the engine's (one reading of the frame)
    value["time"]
        .as_u64()
        .and_then(|t| u32::try_from(t).ok())
        .ok_or_else(|| malformed("time"))?;
    Ok(EngineTip {
        height,
        hash,
        merkle_root,
    })
}

/// The verified tip from `cache` while it is younger than [`TIP_CACHE_TTL_MS`], else one request through the
/// binding; only a success is memoized.
async fn cached_verified_tip(
    base_url: &str,
    service: &Option<worker::Fetcher>,
    cache: &TipCache,
) -> Result<EngineTip, ChainTrackerError> {
    // Serve a fresh-enough memoized tip without a network subrequest
    // (bsv-low #289: the per-lobby-poll tip read).
    if let Some(tip) = tip_cache_read(cache.borrow().clone(), now_ms(), TIP_CACHE_TTL_MS) {
        return Ok(tip);
    }
    let tip = verified_tip_with(|path| ct_get_json(base_url, service, path)).await?;
    // Success-only write: an error result is never memoized.
    *cache.borrow_mut() = Some((now_ms(), tip.clone()));
    Ok(tip)
}

/// Manual `ChainTracker` impl that matches the `#[async_trait]` desugaring.
///
/// The `#[async_trait]` macro on the trait definition creates early-bound lifetime
/// parameters (`'life0`, `'life1`, `'async_trait`). We match that exact signature
/// and wrap the non-Send worker futures with `UnsafeSendFuture`.
impl ChainTracker for WorkerChainTracker {
    fn is_valid_root_for_height<'life0, 'life1, 'async_trait>(
        &'life0 self,
        root: &'life1 str,
        height: u32,
    ) -> Pin<Box<dyn Future<Output = Result<bool, ChainTrackerError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        // Clone data to avoid lifetime issues — the async fn owns its data.
        let base_url = self.base_url.clone();
        let service = self.service.clone();
        let root = root.to_string();
        Box::pin(UnsafeSendFuture(async move {
            fetch_is_valid_root(base_url, service, root, height).await
        }))
    }

    fn current_height<'life0, 'async_trait>(
        &'life0 self,
    ) -> Pin<Box<dyn Future<Output = Result<u32, ChainTrackerError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        let base_url = self.base_url.clone();
        let service = self.service.clone();
        let tip_cache = self.tip_cache.clone();
        Box::pin(UnsafeSendFuture(async move {
            cached_verified_tip(&base_url, &service, &tip_cache)
                .await
                .map(|tip| tip.height)
        }))
    }
}

#[cfg(test)]
mod root_frame_tests {
    use super::{root_matches_frame, CtBlockHeader};

    fn header(merkle_root: &str) -> CtBlockHeader {
        CtBlockHeader {
            merkle_root: merkle_root.to_string(),
        }
    }

    #[test]
    fn matching_root_is_valid() {
        assert_eq!(root_matches_frame(Some(&header("abc")), "abc"), Ok(true));
    }

    #[test]
    fn differing_root_is_invalid() {
        assert_eq!(root_matches_frame(Some(&header("abc")), "def"), Ok(false));
    }

    #[test]
    fn absent_header_fails_safe_to_invalid_not_open() {
        // The security fix: a success+null body is UNVERIFIABLE → false, never
        // a fail-open true that would confirm an arbitrary root.
        assert!(
            matches!(
                root_matches_frame(None, "anything"),
                Err(bsv_rs::transaction::ChainTrackerError::InvalidResponse(_))
            ),
            "an absent header is a FAULT, never a refutation (review L1)"
        );
    }
}

#[cfg(test)]
mod tip_cache_tests {
    use super::{tip_cache_read, TIP_CACHE_TTL_MS};

    #[test]
    fn empty_cache_misses() {
        assert_eq!(tip_cache_read::<u32>(None, 1_000.0, TIP_CACHE_TTL_MS), None);
    }

    #[test]
    fn fresh_entry_hits_stale_entry_misses() {
        let cached = Some((100_000.0, 900_123));
        // Just inside the TTL — hit.
        assert_eq!(
            tip_cache_read(cached, 100_000.0 + TIP_CACHE_TTL_MS - 1.0, TIP_CACHE_TTL_MS),
            Some(900_123)
        );
        // Exactly at the TTL — miss (strict `<`), so a wedged clock can
        // never serve one entry forever on the boundary.
        assert_eq!(
            tip_cache_read(cached, 100_000.0 + TIP_CACHE_TTL_MS, TIP_CACHE_TTL_MS),
            None
        );
    }
}

/// bsv-low #896: the engine's tip is chaintracks' VERIFIED header tip, never the couriers' present height.
#[cfg(test)]
mod e896 {
    use super::{verified_tip_with, EngineTip};
    use bsv_rs::transaction::ChainTrackerError;
    use serde_json::{json, Value};
    use std::{cell::RefCell, future::ready};

    /// The couriers' maximum, one block past what chaintracks has verified.
    const COURIER_MAX: u32 = 972_186;
    /// Chaintracks' served, verified header tip.
    const VERIFIED_HEIGHT: u32 = 972_185;

    fn verified_hash() -> String {
        "0000000000000000".to_string() + &"5a".repeat(24)
    }

    fn tip_frame() -> Value {
        json!({"status": "success", "value": {
            "height": VERIFIED_HEIGHT,
            "hash": verified_hash().to_ascii_uppercase(),
            "merkleRoot": "a7".repeat(32),
            "time": 1_792_000_000u64,
            "version": 536_870_912u64
        }})
    }

    /// One chaintracks answer: its status and body, or the transport's fault.
    type Answer = Result<(u16, Value), ChainTrackerError>;

    /// Chaintracks as its routes answer: the verified tip and the couriers' maximum differ.
    fn chaintracks(route: &str) -> Answer {
        match route {
            "findChainTipHeaderHex" => Ok((200, tip_frame())),
            "getPresentHeight" => Ok((200, json!({"status": "success", "value": COURIER_MAX}))),
            other => panic!("unexpected chaintracks route: {other}"),
        }
    }

    /// RED on 8e5e73f7 (the read extracted unchanged, 6778f68e, the pin asking its height): `left: Some(972186)
    /// right: Some(972185)`, the couriers' maximum.
    #[tokio::test]
    async fn e896_a_the_engine_tip_is_the_verified_header_not_the_courier_max() {
        let calls = RefCell::new(Vec::new());
        let tip = verified_tip_with(|route| {
            calls.borrow_mut().push(route.clone());
            ready(chaintracks(&route))
        })
        .await;
        assert_eq!(
            tip.as_ref().ok().map(|t| t.height),
            Some(VERIFIED_HEIGHT),
            "the verified header's height"
        );
        assert_eq!(
            tip.ok(),
            Some(EngineTip {
                height: VERIFIED_HEIGHT,
                hash: verified_hash(),
                merkle_root: "a7".repeat(32),
            }),
            "its hash and root, one chaintracks row"
        );
        assert_eq!(
            calls.into_inner(),
            ["findChainTipHeaderHex"],
            "one request, the verified tip"
        );
    }

    /// A chaintracks fault stays a fault: no height by a guess, and never the couriers' maximum in its place.
    #[tokio::test]
    async fn e896_b_a_chaintracks_fault_is_a_fault_never_a_height() {
        let mut faults: Vec<(&str, Answer)> = vec![
            ("404", Ok((404, Value::Null))),
            ("503", Ok((503, Value::Null))),
            (
                "error frame",
                Ok((200, json!({"status": "error", "value": null}))),
            ),
            (
                "transport",
                Err(ChainTrackerError::NetworkError("binding".into())),
            ),
        ];
        for (name, edit) in [
            ("no height", ("height", Value::Null)),
            (
                "height past u32",
                ("height", json!(u64::from(u32::MAX) + 1)),
            ),
            ("no hash", ("hash", Value::Null)),
            ("short hash", ("hash", json!("5a5a"))),
            ("hash not hex", ("hash", json!("zz".repeat(32)))),
            ("no merkle root", ("merkleRoot", Value::Null)),
            ("no time", ("time", Value::Null)),
        ] {
            let mut frame = tip_frame();
            frame["value"][edit.0] = edit.1;
            faults.push((name, Ok((200, frame))));
        }
        for (name, answer) in faults {
            let answer = RefCell::new(Some(answer));
            let calls = RefCell::new(Vec::new());
            let tip = verified_tip_with(|route| {
                calls.borrow_mut().push(route.clone());
                let out = match route.as_str() {
                    "findChainTipHeaderHex" => answer.borrow_mut().take().expect("asked once"),
                    other => chaintracks(other),
                };
                ready(out)
            })
            .await;
            assert!(tip.is_err(), "{name}: a fault, never a height: {tip:?}");
            assert_eq!(
                calls.into_inner(),
                ["findChainTipHeaderHex"],
                "{name}: no fallback to the couriers' present height"
            );
        }
    }
}
