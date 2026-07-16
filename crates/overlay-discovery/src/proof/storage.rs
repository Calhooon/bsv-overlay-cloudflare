//! PROOF Storage trait — backend-agnostic storage for transcript-proof
//! bundle records.
//!
//! One row per marker OUTPOINT — `(txid, outputIndex)` (`proof_markers`
//! in D1, bundle as a BLOB like `pot_beefs`). The concrete
//! implementation (D1, in-memory) is provided by the deployment crate;
//! [`MemoryProofStorage`] here backs the unit tests. Structure mirrors
//! `result::storage`, including its censorship lesson:
//!
//! **Keyed by outpoint — every admitted marker is kept.** Admission is
//! byte-format-only, so a `(gameId, winner)`-keyed first-marker-wins
//! index would let a garbage bundle front-run the real proof for one
//! OP_RETURN fee. With outpoint keying, garbage and genuine bundles
//! coexist; the CLIENT verifies each bundle's transcript cryptography
//! and uses the one that proves.
//!
//! [`ProofStorage::store_record`] is insert-if-absent on the outpoint
//! (D1 `INSERT OR IGNORE`) — a replayed / duplicate SUBMIT of the SAME
//! output is a harmless no-op, and rows are NEVER deleted (a published
//! proof is permanent, like a reveal).
//!
//! `created_at` is assigned by the STORAGE layer at insert (D1 stamps
//! the unix time, the memory impl an insertion counter) — the value on
//! the record passed to `store_record` is ignored. Recency ordering
//! (`list_for_game_winner`, newest first) rides on it.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// A proof-marker record as stored in the index.
///
/// Keyed by the marker OUTPOINT `(txid, outputIndex)`. `sig` and
/// `bundle` are carried back VERBATIM to querying clients (which verify
/// the winner's identity sig with the 'anyone' ProtoWallet round-trip
/// AND the bundle's transcript cryptography — the overlay never does).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProofRecord {
    /// Game ID (32 bytes, lowercase hex).
    #[serde(rename = "gameId")]
    pub game_id: String,
    /// The winner's compressed identity pubkey (33 bytes, lowercase hex).
    pub winner: String,
    /// The winner's DER identity signature push (lowercase hex) —
    /// verified CLIENT-side only.
    #[serde(rename = "sigHex")]
    pub sig_hex: String,
    /// The canonical JSON proof bundle BYTES, verbatim (1..=65536).
    /// Stored as a D1 BLOB (the `pot_beefs` idiom); base64-encoded only
    /// at the lookup answer edge.
    pub bundle: Vec<u8>,
    /// The txid carrying the marker OP_RETURN — half of the primary key.
    pub txid: String,
    /// The marker output's index within `txid` — the other half of the
    /// primary key.
    #[serde(rename = "outputIndex")]
    pub output_index: u32,
    /// Unix seconds at insert — assigned by the storage layer (the value
    /// passed to `store_record` is ignored); recency ordering rides on it.
    #[serde(rename = "createdAt")]
    pub created_at: i64,
}

/// `ls_proof` query shapes — tagged JSON, e.g.
/// `{"type":"proofsFor","gameId":"<hex>","winner":"<hex>","limit":3}`.
/// `limit` is optional (default 3, clamped to 1..=10 by the lookup
/// service — bundles are ~10–15 KB each).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ProofQuery {
    /// "Which proof bundles exist for this settled hand's winner?" — the
    /// leaderboard badge-gather question. Newest first.
    #[serde(rename = "proofsFor")]
    ProofsFor {
        #[serde(rename = "gameId")]
        game_id: String,
        winner: String,
        limit: Option<u32>,
    },
}

/// Backend-agnostic storage for proof-marker records.
#[async_trait(?Send)]
pub trait ProofStorage {
    /// Store a record keyed by its OUTPOINT `(txid, outputIndex)` —
    /// insert-if-absent: a replayed / duplicate SUBMIT of the same output
    /// is a no-op, but bundles for the same `(gameId, winner)` from
    /// DIFFERENT txs are ALL KEPT (a garbage bundle can never censor the
    /// real proof — clients verify each bundle and use the one that
    /// proves). Mirrors the D1 `INSERT OR IGNORE`. Never overwrites,
    /// never deletes. `created_at` is assigned here (the record's value
    /// is ignored).
    async fn store_record(&self, record: &ProofRecord) -> Result<(), ProofStorageError>;

    /// Up to `limit` records for the `(gameId, winner)` pair, newest
    /// first.
    async fn list_for_game_winner(
        &self,
        game_id: &str,
        winner: &str,
        limit: usize,
    ) -> Result<Vec<ProofRecord>, ProofStorageError>;
}

/// PROOF storage errors.
#[derive(Debug, thiserror::Error)]
pub enum ProofStorageError {
    #[error("database error: {0}")]
    Database(String),
    #[error("{0}")]
    Other(String),
}

// ============================================================================
// In-memory implementation (for tests)
// ============================================================================

/// In-memory PROOF storage for testing. Insertion order IS recency order
/// (newest = last pushed); `created_at` is stamped with an insertion
/// counter so answers expose a monotone `createdAt` like D1's unix
/// stamp.
#[derive(Debug, Default)]
pub struct MemoryProofStorage {
    records: std::sync::Mutex<Vec<ProofRecord>>,
}

impl MemoryProofStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_count(&self) -> usize {
        self.records.lock().unwrap().len()
    }
}

#[async_trait(?Send)]
impl ProofStorage for MemoryProofStorage {
    async fn store_record(&self, record: &ProofRecord) -> Result<(), ProofStorageError> {
        let mut records = self.records.lock().unwrap();
        // Insert-if-absent on the OUTPOINT (txid, outputIndex) — a
        // replayed submit of the same output is a no-op, but bundles for
        // the same (gameId, winner) from different txs are ALL kept,
        // matching D1's INSERT OR IGNORE on the primary key.
        let exists = records
            .iter()
            .any(|r| r.txid == record.txid && r.output_index == record.output_index);
        if !exists {
            let mut r = record.clone();
            // Storage assigns created_at: an insertion counter here (D1
            // stamps unix seconds) — monotone, so newest-first is rev order.
            r.created_at = records.len() as i64;
            records.push(r);
        }
        Ok(())
    }

    async fn list_for_game_winner(
        &self,
        game_id: &str,
        winner: &str,
        limit: usize,
    ) -> Result<Vec<ProofRecord>, ProofStorageError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter()
            .rev() // newest first (insertion order = recency order)
            .filter(|r| r.game_id == game_id && r.winner == winner)
            .take(limit)
            .cloned()
            .collect())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn record(game_id: &str, winner: &str, txid: &str, bundle: &[u8]) -> ProofRecord {
        ProofRecord {
            game_id: game_id.into(),
            winner: winner.into(),
            sig_hex: "3045ab".into(),
            bundle: bundle.to_vec(),
            txid: txid.into(),
            output_index: 0,
            created_at: 0, // ignored — storage assigns
        }
    }

    #[tokio::test]
    async fn store_then_list_roundtrips() {
        let store = MemoryProofStorage::new();
        store
            .store_record(&record(&"11".repeat(32), "02aa", "tx1", b"{\"v\":1}"))
            .await
            .unwrap();
        assert_eq!(store.record_count(), 1);

        let rows = store
            .list_for_game_winner(&"11".repeat(32), "02aa", 10)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].txid, "tx1");
        assert_eq!(rows[0].bundle, b"{\"v\":1}");
    }

    #[tokio::test]
    async fn list_filters_by_both_game_and_winner() {
        let store = MemoryProofStorage::new();
        store
            .store_record(&record(&"11".repeat(32), "02aa", "tx1", b"b1"))
            .await
            .unwrap();
        store
            .store_record(&record(&"22".repeat(32), "02aa", "tx2", b"b2"))
            .await
            .unwrap();
        store
            .store_record(&record(&"11".repeat(32), "02bb", "tx3", b"b3"))
            .await
            .unwrap();

        // Same game, same winner only.
        let rows = store
            .list_for_game_winner(&"11".repeat(32), "02aa", 10)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].txid, "tx1");
        // No proofs for an unknown pair.
        assert!(store
            .list_for_game_winner(&"33".repeat(32), "02aa", 10)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn same_outpoint_replay_is_a_noop() {
        let store = MemoryProofStorage::new();
        store
            .store_record(&record(&"11".repeat(32), "02aa", "txSAME", b"FIRST"))
            .await
            .unwrap();
        // A replayed submit of the SAME output — ignored.
        store
            .store_record(&record(&"11".repeat(32), "02aa", "txSAME", b"SECOND"))
            .await
            .unwrap();

        assert_eq!(store.record_count(), 1);
        let rows = store
            .list_for_game_winner(&"11".repeat(32), "02aa", 10)
            .await
            .unwrap();
        assert_eq!(rows[0].bundle, b"FIRST", "first insert for the outpoint kept");
    }

    #[tokio::test]
    async fn same_pair_from_different_outpoints_all_kept_newest_first() {
        // The tm_result censorship lesson: bundles for the SAME
        // (gameId, winner) from DIFFERENT txs are ALL kept — a garbage
        // bundle can never front-run the real proof out of the index.
        let store = MemoryProofStorage::new();
        store
            .store_record(&record(&"11".repeat(32), "02aa", "txGARBAGE", b"junk"))
            .await
            .unwrap();
        store
            .store_record(&record(&"11".repeat(32), "02aa", "txGENUINE", b"real"))
            .await
            .unwrap();

        assert_eq!(store.record_count(), 2);
        let rows = store
            .list_for_game_winner(&"11".repeat(32), "02aa", 10)
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].txid, "txGENUINE", "newest first");
        assert_eq!(rows[1].txid, "txGARBAGE");
        assert!(rows[0].created_at > rows[1].created_at);

        // Limit respected.
        let rows = store
            .list_for_game_winner(&"11".repeat(32), "02aa", 1)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].txid, "txGENUINE");
    }

    #[test]
    fn query_json_shape() {
        let q: ProofQuery = serde_json::from_value(serde_json::json!({
            "type": "proofsFor",
            "gameId": "11".repeat(32),
            "winner": "02".to_string() + &"a1".repeat(32),
            "limit": 5
        }))
        .unwrap();
        let ProofQuery::ProofsFor { game_id, winner, limit } = q;
        assert_eq!(game_id.len(), 64);
        assert_eq!(winner.len(), 66);
        assert_eq!(limit, Some(5));

        // limit is optional.
        let q: ProofQuery = serde_json::from_value(serde_json::json!({
            "type": "proofsFor",
            "gameId": "11".repeat(32),
            "winner": "02".to_string() + &"a1".repeat(32)
        }))
        .unwrap();
        let ProofQuery::ProofsFor { limit, .. } = q;
        assert_eq!(limit, None);

        // Unknown type is an error.
        assert!(serde_json::from_value::<ProofQuery>(serde_json::json!({"type": "nope"})).is_err());
    }
}
