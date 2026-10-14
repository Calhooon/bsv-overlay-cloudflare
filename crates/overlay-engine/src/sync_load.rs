//! Per-invocation selection and load accounting for GASP.

use serde::{Deserialize, Serialize};

/// An absent list selects everything; an empty list selects nothing.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyncFilter {
    pub topics: Option<Vec<String>>,
    pub peers: Option<Vec<String>>,
}

impl SyncFilter {
    pub fn parse(body: &str) -> Result<Self, serde_json::Error> {
        if body.trim().is_empty() {
            Ok(Self::default())
        } else {
            serde_json::from_str(body)
        }
    }

    pub fn explicit(&self) -> bool {
        self.topics.is_some() || self.peers.is_some()
    }
}

/// Three empty answers tolerate short propagation gaps. Thereafter probe at
/// 60, 120, 240, 480, 960, 1,920 and 3,600 seconds, independent of tick rate.
pub const QUIET_SYNCS_BEFORE_BACKOFF: u32 = 3;
pub const QUIET_RETRY_BASE_SECS: u64 = 60;
pub const QUIET_RETRY_MAX_SECS: u64 = 3_600;

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
    pub quiet_syncs: u32,
    pub retry_after_secs: u64,
    pub advertisement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkippedPeer {
    pub peer: String,
    pub reason: String,
    pub retry_after_secs: u64,
}
