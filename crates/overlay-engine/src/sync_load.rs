//! Per-invocation selection and load accounting for GASP.

use serde::{Deserialize, Serialize};

/// An absent list selects everything; an empty list selects nothing.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyncFilter {
    /// The topics to sync; `None` selects every configured topic.
    pub topics: Option<Vec<String>>,
    /// The peers to sync with; `None` selects every peer.
    pub peers: Option<Vec<String>>,
}

impl SyncFilter {
    /// Parse a request body; an empty body selects everything.
    pub fn parse(body: &str) -> Result<Self, serde_json::Error> {
        if body.trim().is_empty() {
            Ok(Self::default())
        } else {
            serde_json::from_str(body)
        }
    }

    /// Whether the caller named topics or peers.
    pub fn explicit(&self) -> bool {
        self.topics.is_some() || self.peers.is_some()
    }
}

/// Three empty answers tolerate short propagation gaps. Thereafter probe at
/// 60, 120, 240, 480, 960, 1,920 and 3,600 seconds, independent of tick rate.
pub const QUIET_SYNCS_BEFORE_BACKOFF: u32 = 3;
/// The wait after the free quiet syncs, in seconds; each later one doubles.
pub const QUIET_RETRY_BASE_SECS: u64 = 60;
/// The longest wait between probes of a quiet peer, in seconds.
pub const QUIET_RETRY_MAX_SECS: u64 = 3_600;

/// How long a peer waits before its next probe after `syncs` consecutive quiet syncs.
pub fn quiet_retry_secs(syncs: u32) -> u64 {
    if syncs < QUIET_SYNCS_BEFORE_BACKOFF {
        0
    } else {
        (QUIET_RETRY_BASE_SECS << (syncs - QUIET_SYNCS_BEFORE_BACKOFF).min(6))
            .min(QUIET_RETRY_MAX_SECS)
    }
}

/// Storage owns the clock; retry_after_secs is relative both on read and write.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PeerCadence {
    /// Consecutive syncs this peer answered with nothing new.
    pub quiet_syncs: u32,
    /// Seconds before the peer is probed again; 0 when it is due.
    pub retry_after_secs: u64,
    /// The peer's advertisement when the cadence was recorded; a new advertisement resets it.
    pub advertisement: String,
}

/// A peer a sync skipped, and why.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkippedPeer {
    /// The peer's URL.
    pub peer: String,
    /// Why it was skipped: `quarantine` or `no_yield`.
    pub reason: String,
    /// Seconds before the peer is tried again.
    pub retry_after_secs: u64,
}

/// A root whose walk stopped at a bound and was deferred.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootBound {
    /// The peer the root was walked from.
    pub peer: String,
    /// The root UTXO, `txid.outputIndex`.
    pub outpoint: String,
    /// The bound that stopped it: `time` or `courier_rungs`.
    pub reason: String,
}
