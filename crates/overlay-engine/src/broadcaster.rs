//! Broadcaster traits — propagate transactions to the BSV network and peer
//! overlay nodes.
//!
//! Two traits live here:
//!
//! * [`Broadcaster`] — SHIP propagation to peer overlay nodes (existing).
//! * [`ArcBroadcaster`] — ARC network broadcast to miners (new).
//!
//! After a transaction is admitted by topic managers the Engine:
//! 1. Broadcasts to ARC (network) so the tx reaches miners.
//! 2. Propagates to SHIP peers so other overlay nodes index the tx.
//!
//! Both traits abstract HTTP transport so the engine crate stays
//! platform-agnostic. Implementations (Cloudflare Workers Fetch, reqwest,
//! etc.) live in deployment crates.

use async_trait::async_trait;

use crate::types::TaggedBEEF;

/// Broadcasts tagged BEEF transactions to peer overlay nodes.
///
/// The Engine calls `broadcast_to_host()` for each peer discovered via SHIP
/// lookup after a transaction is admitted. Implementations handle the actual
/// HTTP POST to the peer's `/submit` endpoint.
#[async_trait(?Send)]
pub trait Broadcaster {
    /// Broadcast a tagged BEEF to a specific overlay host URL.
    ///
    /// The implementation should POST to `{host_url}/submit` with:
    /// - Content-Type: `application/octet-stream`
    /// - `x-topics` header: JSON array of topic strings
    /// - Body: raw BEEF bytes
    ///
    /// Returns `Ok(())` on success, `Err(message)` on failure.
    /// Failures are non-fatal — the Engine logs them and continues.
    async fn broadcast_to_host(
        &self,
        host_url: &str,
        tagged_beef: &TaggedBEEF,
    ) -> Result<(), String>;
}

/// Broadcasts a raw transaction to the BSV network via ARC (TAAL).
///
/// The Engine calls `broadcast()` during Phase 2 for `CurrentTx` submissions
/// that do not already have a merkle proof (i.e. not yet mined). The
/// implementation POSTs the transaction to ARC's `/v1/tx` endpoint.
///
/// Broadcast failures are non-fatal — the Engine logs them and continues
/// with SHIP propagation and storage mutations.
#[async_trait(?Send)]
pub trait ArcBroadcaster {
    /// Broadcast a raw transaction (hex EF format) to ARC.
    ///
    /// `raw_tx_hex` is the hex-encoded transaction in Extended Format (EF),
    /// matching the TS SDK's `Transaction.toHexEF()`.
    ///
    /// Returns `Ok(txid)` on success, `Err(description)` on failure.
    async fn broadcast(&self, raw_tx_hex: &str) -> Result<String, String>;
}
