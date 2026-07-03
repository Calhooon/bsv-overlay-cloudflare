//! REVEAL Storage trait — backend-agnostic storage for reveal records.
//!
//! One row per admitted `tm_reveal` artifact UTXO (`reveal_records` in
//! D1). The concrete implementation (D1, in-memory) is provided by the
//! deployment crate; `MemoryRevealStorage` here backs the unit tests.
//! Structure mirrors `low::storage`, minus the record-type discriminator
//! (there is only one reveal shape) and any spend semantics — a reveal is
//! a PERMANENT fact, so rows are never deleted on spend/eviction.

use async_trait::async_trait;
use overlay_engine::types::UTXOReference;
use serde::{Deserialize, Serialize};

/// A break-glass reveal record as stored in the index.
///
/// Keyed by `(txid, outputIndex)`; queried by `(gameId, seat)`. The full
/// artifact (positions + scalars) lives in the BEEF returned by `/lookup`,
/// not in the index — the index only needs the lookup key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RevealRecord {
    pub txid: String,
    #[serde(rename = "outputIndex")]
    pub output_index: u32,
    /// Game ID (32 bytes, lowercase hex).
    #[serde(rename = "gameId")]
    pub game_id: String,
    /// Revealing seat: 0 = A, 1 = B.
    pub seat: u8,
}

/// `ls_reveal` query shapes — tagged JSON, e.g.
/// `{"type":"byGameSeat","gameId":"<hex>","seat":0}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum RevealQuery {
    /// All reveal records for one game AND seat — the tower's primary
    /// "did the accused seat reveal?" query.
    #[serde(rename = "byGameSeat")]
    ByGameSeat {
        #[serde(rename = "gameId")]
        game_id: String,
        seat: u8,
    },
    /// All reveal records for one game (both seats).
    #[serde(rename = "byGameId")]
    ByGameId {
        #[serde(rename = "gameId")]
        game_id: String,
    },
}

/// Backend-agnostic storage for reveal records.
#[async_trait(?Send)]
pub trait RevealStorage {
    /// Store (or idempotently re-store) a record keyed by (txid, outputIndex).
    async fn store_record(&self, record: &RevealRecord) -> Result<(), RevealStorageError>;

    /// Delete a record by UTXO reference.
    ///
    /// NOTE: the reveal lookup service NEVER calls this on spend/eviction
    /// (a reveal is permanent). It exists for D1/API symmetry and manual
    /// operator use only.
    async fn delete_record(&self, txid: &str, output_index: u32) -> Result<(), RevealStorageError>;

    /// All reveal records for a game ID (lowercase hex) AND seat.
    async fn find_by_game_seat(
        &self,
        game_id: &str,
        seat: u8,
    ) -> Result<Vec<UTXOReference>, RevealStorageError>;

    /// All reveal records for a game ID (lowercase hex), any seat.
    async fn find_by_game_id(
        &self,
        game_id: &str,
    ) -> Result<Vec<UTXOReference>, RevealStorageError>;
}

/// REVEAL storage errors.
#[derive(Debug, thiserror::Error)]
pub enum RevealStorageError {
    #[error("database error: {0}")]
    Database(String),
    #[error("{0}")]
    Other(String),
}

// ============================================================================
// In-memory implementation (for tests)
// ============================================================================

/// In-memory REVEAL storage for testing.
#[derive(Debug, Default)]
pub struct MemoryRevealStorage {
    records: std::sync::Mutex<Vec<RevealRecord>>,
}

impl MemoryRevealStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_count(&self) -> usize {
        self.records.lock().unwrap().len()
    }
}

#[async_trait(?Send)]
impl RevealStorage for MemoryRevealStorage {
    async fn store_record(&self, record: &RevealRecord) -> Result<(), RevealStorageError> {
        let mut records = self.records.lock().unwrap();
        // Idempotent on (txid, outputIndex) — matches D1's INSERT OR REPLACE.
        records.retain(|r| !(r.txid == record.txid && r.output_index == record.output_index));
        records.push(record.clone());
        Ok(())
    }

    async fn delete_record(&self, txid: &str, output_index: u32) -> Result<(), RevealStorageError> {
        self.records
            .lock()
            .unwrap()
            .retain(|r| !(r.txid == txid && r.output_index == output_index));
        Ok(())
    }

    async fn find_by_game_seat(
        &self,
        game_id: &str,
        seat: u8,
    ) -> Result<Vec<UTXOReference>, RevealStorageError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.game_id == game_id && r.seat == seat)
            .map(|r| UTXOReference {
                txid: r.txid.clone(),
                output_index: r.output_index,
            })
            .collect())
    }

    async fn find_by_game_id(
        &self,
        game_id: &str,
    ) -> Result<Vec<UTXOReference>, RevealStorageError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.game_id == game_id)
            .map(|r| UTXOReference {
                txid: r.txid.clone(),
                output_index: r.output_index,
            })
            .collect())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn reveal_record(txid: &str, game_id: &str, seat: u8) -> RevealRecord {
        RevealRecord {
            txid: txid.into(),
            output_index: 0,
            game_id: game_id.into(),
            seat,
        }
    }

    #[tokio::test]
    async fn store_and_find_by_game_seat() {
        let store = MemoryRevealStorage::new();
        store
            .store_record(&reveal_record("tx1", &"11".repeat(32), 0))
            .await
            .unwrap();
        store
            .store_record(&reveal_record("tx2", &"11".repeat(32), 1))
            .await
            .unwrap();
        store
            .store_record(&reveal_record("tx3", &"22".repeat(32), 1))
            .await
            .unwrap();

        // seat filter is precise
        let a = store.find_by_game_seat(&"11".repeat(32), 0).await.unwrap();
        assert_eq!(a.len(), 1);
        assert_eq!(a[0].txid, "tx1");

        let b = store.find_by_game_seat(&"11".repeat(32), 1).await.unwrap();
        assert_eq!(b.len(), 1);
        assert_eq!(b[0].txid, "tx2");

        // wrong game → empty
        assert!(store
            .find_by_game_seat(&"ff".repeat(32), 0)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn find_by_game_id_returns_both_seats() {
        let store = MemoryRevealStorage::new();
        store
            .store_record(&reveal_record("tx1", &"11".repeat(32), 0))
            .await
            .unwrap();
        store
            .store_record(&reveal_record("tx2", &"11".repeat(32), 1))
            .await
            .unwrap();
        store
            .store_record(&reveal_record("tx3", &"22".repeat(32), 0))
            .await
            .unwrap();

        let results = store.find_by_game_id(&"11".repeat(32)).await.unwrap();
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn multiple_matches_for_same_game_seat_all_returned() {
        // A flood shape: several reveal txs for the SAME (game, seat). The
        // index returns them ALL so the tower can adjudicate every candidate
        // (genuine vs cooked). Different outpoints so idempotency keeps both.
        let store = MemoryRevealStorage::new();
        store
            .store_record(&reveal_record("txA", &"11".repeat(32), 1))
            .await
            .unwrap();
        store
            .store_record(&reveal_record("txB", &"11".repeat(32), 1))
            .await
            .unwrap();
        let results = store.find_by_game_seat(&"11".repeat(32), 1).await.unwrap();
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn store_is_idempotent_per_outpoint() {
        let store = MemoryRevealStorage::new();
        store
            .store_record(&reveal_record("tx1", &"11".repeat(32), 0))
            .await
            .unwrap();
        store
            .store_record(&reveal_record("tx1", &"11".repeat(32), 0))
            .await
            .unwrap();
        assert_eq!(store.record_count(), 1);
    }

    #[tokio::test]
    async fn delete_record_removes_only_matching_outpoint() {
        let store = MemoryRevealStorage::new();
        store
            .store_record(&reveal_record("tx1", &"11".repeat(32), 0))
            .await
            .unwrap();
        store
            .store_record(&reveal_record("tx2", &"11".repeat(32), 1))
            .await
            .unwrap();
        store.delete_record("tx1", 0).await.unwrap();
        assert_eq!(store.record_count(), 1);
        // deleting a nonexistent record is fine
        store.delete_record("nope", 9).await.unwrap();
        assert_eq!(store.record_count(), 1);
    }

    #[test]
    fn query_json_shapes() {
        let q: RevealQuery = serde_json::from_value(serde_json::json!({
            "type": "byGameSeat", "gameId": "ab".repeat(32), "seat": 1
        }))
        .unwrap();
        match q {
            RevealQuery::ByGameSeat { seat, .. } => assert_eq!(seat, 1),
            _ => panic!("wrong variant"),
        }

        let q: RevealQuery = serde_json::from_value(serde_json::json!({
            "type": "byGameId", "gameId": "ab".repeat(32)
        }))
        .unwrap();
        assert!(matches!(q, RevealQuery::ByGameId { .. }));

        // Unknown type is an error.
        assert!(
            serde_json::from_value::<RevealQuery>(serde_json::json!({"type": "nope"})).is_err()
        );
    }
}
