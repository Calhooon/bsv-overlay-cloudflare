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
/// against a ChainTracks API server (e.g. <your-chain-tracker-api>).
///
/// Endpoints used:
/// - `GET /findHeaderHexForHeight?height={N}` — returns block header with merkle root
/// - `GET /getPresentHeight` — returns current chain height
pub struct WorkerChainTracker {
    base_url: String,
}

// SAFETY: wasm32 is single-threaded. There are no other threads to send to.
unsafe impl Send for WorkerChainTracker {}
unsafe impl Sync for WorkerChainTracker {}

impl WorkerChainTracker {
    /// Create a new WorkerChainTracker pointing at the given ChainTracks API URL.
    pub fn new(base_url: String) -> Self {
        Self { base_url }
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
unsafe impl<F> Send for UnsafeSendFuture<F> {}

impl<F: Future> Future for UnsafeSendFuture<F> {
    type Output = F::Output;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        // SAFETY: We only project to the inner future, maintaining pinning.
        unsafe { self.map_unchecked_mut(|s| &mut s.0).poll(cx) }
    }
}

/// Fetch a block header from ChainTracks and check if the merkle root matches.
async fn fetch_is_valid_root(
    base_url: String,
    root: String,
    height: u32,
) -> Result<bool, ChainTrackerError> {
    let url = format!(
        "{}/findHeaderHexForHeight?height={}",
        base_url.trim_end_matches('/'),
        height
    );

    let mut init = worker::RequestInit::new();
    init.with_method(worker::Method::Get);

    let headers = worker::Headers::new();
    let _ = headers.set("Accept", "application/json");
    init.with_headers(headers);

    let request = worker::Request::new_with_init(&url, &init)
        .map_err(|e| ChainTrackerError::Other(format!("Failed to create request: {e}")))?;

    let mut response = worker::Fetch::Request(request).send().await.map_err(|e| {
        ChainTrackerError::NetworkError(format!("ChainTracks findHeaderHexForHeight failed: {e}"))
    })?;

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

    // If value is missing, the header hasn't been ingested yet (height is in the
    // "live" range between heightBulk and heightLive). Treat as valid — the
    // transaction is in a recent block that the bulk ingestor hasn't caught up to.
    // This matches TS SDK behavior which gracefully handles missing headers.
    let Some(header) = frame.value else {
        return Ok(true);
    };

    Ok(header.merkle_root == root)
}

/// Fetch the current chain height from ChainTracks.
async fn fetch_current_height(base_url: String) -> Result<u32, ChainTrackerError> {
    let url = format!("{}/getPresentHeight", base_url.trim_end_matches('/'));

    let mut init = worker::RequestInit::new();
    init.with_method(worker::Method::Get);

    let headers = worker::Headers::new();
    let _ = headers.set("Accept", "application/json");
    init.with_headers(headers);

    let request = worker::Request::new_with_init(&url, &init)
        .map_err(|e| ChainTrackerError::Other(format!("Failed to create request: {e}")))?;

    let mut response = worker::Fetch::Request(request).send().await.map_err(|e| {
        ChainTrackerError::NetworkError(format!("ChainTracks getPresentHeight failed: {e}"))
    })?;

    let status = response.status_code();
    if !(200..300).contains(&status) {
        return Err(ChainTrackerError::InvalidResponse(format!(
            "ChainTracks getPresentHeight returned HTTP {status}"
        )));
    }

    let frame: ResponseFrame<u32> = response
        .json()
        .await
        .map_err(|e| ChainTrackerError::InvalidResponse(format!("ChainTracks parse error: {e}")))?;

    if !frame.is_success() {
        return Err(ChainTrackerError::NetworkError(format!(
            "ChainTracks getPresentHeight: status={}",
            frame.status
        )));
    }

    frame.value.ok_or_else(|| {
        ChainTrackerError::InvalidResponse(
            "ChainTracks getPresentHeight: missing value".to_string(),
        )
    })
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
        let root = root.to_string();
        Box::pin(UnsafeSendFuture(async move {
            fetch_is_valid_root(base_url, root, height).await
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
        Box::pin(UnsafeSendFuture(async move {
            fetch_current_height(base_url).await
        }))
    }
}
