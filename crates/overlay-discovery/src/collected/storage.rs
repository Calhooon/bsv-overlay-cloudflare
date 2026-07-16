//! COLLECTED Storage trait — backend-agnostic storage for collected-marker
//! records.
//!
//! One row per `(identity, gameId)` pair (`collected_markers` in D1). The
//! concrete implementation (D1, in-memory) is provided by the deployment
//! crate; [`MemoryCollectedStorage`] here backs the unit tests. Structure
//! mirrors `reveal::storage`, with one behavioral pin:
//!
//! **First marker wins.** [`CollectedStorage::store_record`] is
//! insert-if-absent (D1 `INSERT OR IGNORE` on the `(identity, gameId)`
//! primary key) — a later marker for the same pair NEVER overwrites the
//! first, and rows are NEVER deleted (a collected fact is permanent, like a
//! reveal). Replays / floods of markers for an already-recorded pair are
//! therefore harmless no-ops.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// A collected-marker record as stored in the index.
///
/// Keyed by `(identity, gameId)` — one identity collects a game's credit at
/// most once, so the pair is the natural primary key. `txid` + `sig_hex`
/// are carried back verbatim to querying clients (which verify the sig
/// under their own wallet); they are `Option` to mirror the nullable D1
/// columns, though the admit path always stores both.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectedRecord {
    /// The publisher's compressed identity pubkey (33 bytes, lowercase hex).
    pub identity: String,
    /// Game ID (32 bytes, lowercase hex).
    #[serde(rename = "gameId")]
    pub game_id: String,
    /// The txid carrying the marker OP_RETURN.
    pub txid: Option<String>,
    /// The marker's DER signature push (lowercase hex) — verified
    /// CLIENT-side only.
    #[serde(rename = "sigHex")]
    pub sig_hex: Option<String>,
}

/// `ls_collected` query shapes — tagged JSON, e.g.
/// `{"type":"collectedFor","identity":"<hex>","gameIds":["<hex>",…]}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum CollectedQuery {
    /// "Which of these games has this identity already collected?" — the
    /// client's card-gather question. The answer is an input-ordered array,
    /// one entry per requested gameId.
    #[serde(rename = "collectedFor")]
    CollectedFor {
        identity: String,
        #[serde(rename = "gameIds")]
        game_ids: Vec<String>,
    },
}

/// Backend-agnostic storage for collected-marker records.
#[async_trait(?Send)]
pub trait CollectedStorage {
    /// Store a record keyed by `(identity, gameId)` — insert-if-absent
    /// (FIRST MARKER WINS): if a row for the pair already exists it is left
    /// untouched. Mirrors the D1 `INSERT OR IGNORE`. Never overwrites,
    /// never deletes.
    async fn store_record(&self, record: &CollectedRecord) -> Result<(), CollectedStorageError>;

    /// The record for `(identity, gameId)`, or `None` if no marker was ever
    /// admitted for the pair.
    async fn get_record(
        &self,
        identity: &str,
        game_id: &str,
    ) -> Result<Option<CollectedRecord>, CollectedStorageError>;
}

/// COLLECTED storage errors.
#[derive(Debug, thiserror::Error)]
pub enum CollectedStorageError {
    #[error("database error: {0}")]
    Database(String),
    #[error("{0}")]
    Other(String),
}

// ============================================================================
// In-memory implementation (for tests)
// ============================================================================

/// In-memory COLLECTED storage for testing.
#[derive(Debug, Default)]
pub struct MemoryCollectedStorage {
    records: std::sync::Mutex<Vec<CollectedRecord>>,
}

impl MemoryCollectedStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_count(&self) -> usize {
        self.records.lock().unwrap().len()
    }
}

#[async_trait(?Send)]
impl CollectedStorage for MemoryCollectedStorage {
    async fn store_record(&self, record: &CollectedRecord) -> Result<(), CollectedStorageError> {
        let mut records = self.records.lock().unwrap();
        // Insert-if-absent on (identity, gameId) — FIRST MARKER WINS,
        // matching D1's INSERT OR IGNORE on the primary key.
        let exists = records
            .iter()
            .any(|r| r.identity == record.identity && r.game_id == record.game_id);
        if !exists {
            records.push(record.clone());
        }
        Ok(())
    }

    async fn get_record(
        &self,
        identity: &str,
        game_id: &str,
    ) -> Result<Option<CollectedRecord>, CollectedStorageError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter()
            .find(|r| r.identity == identity && r.game_id == game_id)
            .cloned())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn record(identity: &str, game_id: &str, txid: &str, sig_hex: &str) -> CollectedRecord {
        CollectedRecord {
            identity: identity.into(),
            game_id: game_id.into(),
            txid: Some(txid.into()),
            sig_hex: Some(sig_hex.into()),
        }
    }

    #[tokio::test]
    async fn store_then_get_roundtrips() {
        let store = MemoryCollectedStorage::new();
        store
            .store_record(&record("02aa", &"11".repeat(32), "tx1", "3045ab"))
            .await
            .unwrap();
        assert_eq!(store.record_count(), 1);

        let r = store
            .get_record("02aa", &"11".repeat(32))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.txid.as_deref(), Some("tx1"));
        assert_eq!(r.sig_hex.as_deref(), Some("3045ab"));
    }

    #[tokio::test]
    async fn get_unknown_pair_is_none() {
        let store = MemoryCollectedStorage::new();
        assert!(store
            .get_record("02aa", &"11".repeat(32))
            .await
            .unwrap()
            .is_none());
        // Same identity, different game → still unknown.
        store
            .store_record(&record("02aa", &"11".repeat(32), "tx1", "3045ab"))
            .await
            .unwrap();
        assert!(store
            .get_record("02aa", &"22".repeat(32))
            .await
            .unwrap()
            .is_none());
        // Same game, different identity → still unknown.
        assert!(store
            .get_record("02bb", &"11".repeat(32))
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn first_marker_wins_never_overwritten() {
        let store = MemoryCollectedStorage::new();
        store
            .store_record(&record("02aa", &"11".repeat(32), "txFIRST", "sigFIRST"))
            .await
            .unwrap();
        // A second marker for the SAME (identity, gameId) must be ignored —
        // never overwrite, never delete.
        store
            .store_record(&record("02aa", &"11".repeat(32), "txSECOND", "sigSECOND"))
            .await
            .unwrap();

        assert_eq!(store.record_count(), 1);
        let r = store
            .get_record("02aa", &"11".repeat(32))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.txid.as_deref(), Some("txFIRST"), "first marker wins");
        assert_eq!(r.sig_hex.as_deref(), Some("sigFIRST"));
    }

    #[tokio::test]
    async fn distinct_pairs_tracked_independently() {
        let store = MemoryCollectedStorage::new();
        store
            .store_record(&record("02aa", &"11".repeat(32), "tx1", "s1"))
            .await
            .unwrap();
        store
            .store_record(&record("02aa", &"22".repeat(32), "tx2", "s2"))
            .await
            .unwrap();
        store
            .store_record(&record("02bb", &"11".repeat(32), "tx3", "s3"))
            .await
            .unwrap();
        assert_eq!(store.record_count(), 3);

        let r = store
            .get_record("02bb", &"11".repeat(32))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.txid.as_deref(), Some("tx3"));
    }

    #[test]
    fn query_json_shape() {
        let q: CollectedQuery = serde_json::from_value(serde_json::json!({
            "type": "collectedFor",
            "identity": "02".to_string() + &"a1".repeat(32),
            "gameIds": ["11".repeat(32), "22".repeat(32)]
        }))
        .unwrap();
        let CollectedQuery::CollectedFor { identity, game_ids } = q;
        assert_eq!(identity.len(), 66);
        assert_eq!(game_ids.len(), 2);

        // Unknown type is an error.
        assert!(
            serde_json::from_value::<CollectedQuery>(serde_json::json!({"type": "nope"})).is_err()
        );
    }
}
