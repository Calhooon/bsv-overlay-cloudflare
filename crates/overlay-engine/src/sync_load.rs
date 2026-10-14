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
