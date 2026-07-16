//! RESULT Storage trait — backend-agnostic storage for result-marker
//! records.
//!
//! One row per marker OUTPOINT — `(txid, outputIndex)` (`result_markers_v2`
//! in D1). The concrete implementation (D1, in-memory) is provided by the
//! deployment crate; [`MemoryResultStorage`] here backs the unit tests.
//! Structure mirrors `collected::storage`, with one deliberate divergence:
//!
//! **Keyed by outpoint, NOT by `(gameId, winner)` — every admitted marker
//! is kept.** Admission is byte-format-only (no sig check), so a
//! `(gameId, winner)`-keyed first-marker-wins index would hand a censor a
//! one-OP_RETURN-fee attack: publish a well-formed marker naming the REAL
//! winner with GARBAGE sigs, permanently occupy the slot, and the genuine
//! countersigned marker is silently dropped forever (adversarial-review
//! HIGH, 2026-07-16). Keying on the outpoint closes it: garbage and
//! genuine rows COEXIST, the client aggregation drops invalid-sig rows
//! before dedup, and the verifying one counts.
//!
//! [`ResultStorage::store_record`] is insert-if-absent on the outpoint
//! (D1 `INSERT OR IGNORE` on the `(txid, outputIndex)` primary key) — a
//! replayed / duplicate SUBMIT of the SAME output is a harmless no-op,
//! and rows are NEVER deleted (a settled result is permanent, like a
//! reveal).
//!
//! `created_at` is assigned by the STORAGE layer at insert (D1 stamps the
//! unix time, the memory impl an insertion counter) — the value on the
//! record passed to `store_record` is ignored. Recency ordering
//! (`list_for_winner` / `list_recent`, newest first) rides on it.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// A result-marker record as stored in the index.
///
/// Keyed by the marker OUTPOINT `(txid, outputIndex)` — every admitted
/// marker is kept, so a garbage-sig front-run naming the real winner can
/// never censor the later genuine marker (see the module docs). All byte
/// fields are carried back VERBATIM to querying clients (which verify
/// the sigs with the 'anyone' ProtoWallet round-trip — the overlay never
/// does, and it derives no "confirmed" flag). `loser_sig_hex` is `None`
/// when the marker's loserSig push was empty (an unconfirmed claim).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResultRecord {
    /// Game ID (32 bytes, lowercase hex).
    #[serde(rename = "gameId")]
    pub game_id: String,
    /// The winner's compressed identity pubkey (33 bytes, lowercase hex).
    pub winner: String,
    /// The loser's compressed identity pubkey (33 bytes, lowercase hex).
    pub loser: String,
    /// The pot funding txid the claim anchors to (32 bytes, lowercase hex).
    #[serde(rename = "potTxid")]
    pub pot_txid: String,
    /// The settle txid the claim anchors to (32 bytes, lowercase hex).
    #[serde(rename = "settleTxid")]
    pub settle_txid: String,
    /// The winner's DER signature push (lowercase hex) — verified
    /// CLIENT-side only.
    #[serde(rename = "winnerSigHex")]
    pub winner_sig_hex: String,
    /// The loser's DER countersignature push (lowercase hex), or `None`
    /// when the marker's loserSig push was empty (an unconfirmed claim) —
    /// verified CLIENT-side only.
    #[serde(rename = "loserSigHex")]
    pub loser_sig_hex: Option<String>,
    /// v2 markers only: the winner's five revealed cards (10 lowercase
    /// hex chars — 5 card-index bytes, each 0..=51, distinct, validated
    /// at parse). `None` for a v1 marker. Feeds the "lowest winning
    /// hand" leaderboard; carried back verbatim like every other field.
    #[serde(rename = "cardsHex")]
    pub cards_hex: Option<String>,
    /// The txid carrying the marker OP_RETURN — half of the primary key.
    /// Always known at admission, so required (unlike collected's
    /// nullable txid).
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

/// `ls_result` query shapes — tagged JSON, e.g.
/// `{"type":"resultsFor","identity":"<hex>","limit":50}`. `limit` is
/// optional (default 100, clamped to 1..=500 by the lookup service).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ResultQuery {
    /// "Which hands has this identity won?" — the per-player leaderboard
    /// question. Newest first.
    #[serde(rename = "resultsFor")]
    ResultsFor { identity: String, limit: Option<u32> },
    /// "What settled recently?" — the global leaderboard feed. Newest
    /// first, across all identities.
    #[serde(rename = "recentResults")]
    RecentResults { limit: Option<u32> },
}

/// Backend-agnostic storage for result-marker records.
#[async_trait(?Send)]
pub trait ResultStorage {
    /// Store a record keyed by its OUTPOINT `(txid, outputIndex)` —
    /// insert-if-absent: a replayed / duplicate SUBMIT of the same output
    /// is a no-op, but markers for the same `(gameId, winner)` from
    /// DIFFERENT txs are ALL KEPT (a garbage front-run can never censor a
    /// genuine marker — clients verify sigs and the genuine one counts).
    /// Mirrors the D1 `INSERT OR IGNORE`. Never overwrites, never
    /// deletes. `created_at` is assigned here (the record's value is
    /// ignored).
    async fn store_record(&self, record: &ResultRecord) -> Result<(), ResultStorageError>;

    /// Up to `limit` records whose winner is `winner`, newest first.
    async fn list_for_winner(
        &self,
        winner: &str,
        limit: usize,
    ) -> Result<Vec<ResultRecord>, ResultStorageError>;

    /// Up to `limit` records across all identities, newest first.
    async fn list_recent(&self, limit: usize) -> Result<Vec<ResultRecord>, ResultStorageError>;
}

/// RESULT storage errors.
#[derive(Debug, thiserror::Error)]
pub enum ResultStorageError {
    #[error("database error: {0}")]
    Database(String),
    #[error("{0}")]
    Other(String),
}

// ============================================================================
// In-memory implementation (for tests)
// ============================================================================

/// In-memory RESULT storage for testing. Insertion order IS recency order
/// (newest = last pushed); `created_at` is stamped with an insertion
/// counter so answers expose a monotone `createdAt` like D1's unix stamp.
#[derive(Debug, Default)]
pub struct MemoryResultStorage {
    records: std::sync::Mutex<Vec<ResultRecord>>,
}

impl MemoryResultStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_count(&self) -> usize {
        self.records.lock().unwrap().len()
    }
}

#[async_trait(?Send)]
impl ResultStorage for MemoryResultStorage {
    async fn store_record(&self, record: &ResultRecord) -> Result<(), ResultStorageError> {
        let mut records = self.records.lock().unwrap();
        // Insert-if-absent on the OUTPOINT (txid, outputIndex) — a
        // replayed submit of the same output is a no-op, but markers for
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

    async fn list_for_winner(
        &self,
        winner: &str,
        limit: usize,
    ) -> Result<Vec<ResultRecord>, ResultStorageError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter()
            .rev() // newest first (insertion order = recency order)
            .filter(|r| r.winner == winner)
            .take(limit)
            .cloned()
            .collect())
    }

    async fn list_recent(&self, limit: usize) -> Result<Vec<ResultRecord>, ResultStorageError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter()
            .rev() // newest first
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

    fn record(game_id: &str, winner: &str, loser: &str, txid: &str) -> ResultRecord {
        ResultRecord {
            game_id: game_id.into(),
            winner: winner.into(),
            loser: loser.into(),
            pot_txid: "22".repeat(32),
            settle_txid: "33".repeat(32),
            winner_sig_hex: "3045ab".into(),
            loser_sig_hex: Some("3044cd".into()),
            cards_hex: None,
            txid: txid.into(),
            output_index: 0,
            created_at: 0, // ignored — storage assigns
        }
    }

    #[tokio::test]
    async fn store_then_list_roundtrips() {
        let store = MemoryResultStorage::new();
        store
            .store_record(&record(&"11".repeat(32), "02aa", "03bb", "tx1"))
            .await
            .unwrap();
        assert_eq!(store.record_count(), 1);

        let rows = store.list_for_winner("02aa", 100).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].txid, "tx1");
        assert_eq!(rows[0].output_index, 0);
        assert_eq!(rows[0].loser, "03bb");
        assert_eq!(rows[0].loser_sig_hex.as_deref(), Some("3044cd"));
    }

    #[tokio::test]
    async fn list_for_winner_filters_by_winner_only() {
        let store = MemoryResultStorage::new();
        store
            .store_record(&record(&"11".repeat(32), "02aa", "03bb", "tx1"))
            .await
            .unwrap();
        store
            .store_record(&record(&"22".repeat(32), "03bb", "02aa", "tx2"))
            .await
            .unwrap();

        // Winner 02aa sees only its own win — NOT the hand it lost.
        let rows = store.list_for_winner("02aa", 100).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].txid, "tx1");
        // An identity that never won sees nothing.
        assert!(store.list_for_winner("02cc", 100).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn lists_are_newest_first_and_respect_limit() {
        let store = MemoryResultStorage::new();
        for i in 0..5u8 {
            store
                .store_record(&record(
                    &format!("{:02x}", i).repeat(32),
                    "02aa",
                    "03bb",
                    &format!("tx{i}"),
                ))
                .await
                .unwrap();
        }

        let rows = store.list_recent(3).await.unwrap();
        assert_eq!(rows.len(), 3, "limit respected");
        assert_eq!(rows[0].txid, "tx4", "newest first");
        assert_eq!(rows[1].txid, "tx3");
        assert_eq!(rows[2].txid, "tx2");
        // created_at is monotone (storage-assigned).
        assert!(rows[0].created_at > rows[1].created_at);

        let rows = store.list_for_winner("02aa", 2).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].txid, "tx4", "newest first");
    }

    #[tokio::test]
    async fn same_outpoint_replay_is_a_noop() {
        let store = MemoryResultStorage::new();
        store
            .store_record(&record(&"11".repeat(32), "02aa", "03bb", "txSAME"))
            .await
            .unwrap();
        // A replayed / duplicate SUBMIT of the SAME output — ignored;
        // never overwrite, never delete.
        let mut replay = record(&"11".repeat(32), "02aa", "03cc", "txSAME");
        replay.output_index = 0; // same outpoint
        store.store_record(&replay).await.unwrap();

        assert_eq!(store.record_count(), 1);
        let rows = store.list_for_winner("02aa", 100).await.unwrap();
        assert_eq!(rows[0].loser, "03bb", "first insert for the outpoint kept");
    }

    #[tokio::test]
    async fn same_pair_from_different_outpoints_all_kept() {
        // The censorship fix at the storage level: markers for the SAME
        // (gameId, winner) from DIFFERENT txs (or vouts) are ALL kept — a
        // garbage front-run can never occupy the pair and censor the
        // genuine marker.
        let store = MemoryResultStorage::new();
        store
            .store_record(&record(&"11".repeat(32), "02aa", "03bb", "txGARBAGE"))
            .await
            .unwrap();
        store
            .store_record(&record(&"11".repeat(32), "02aa", "03bb", "txGENUINE"))
            .await
            .unwrap();
        // Same txid, different vout — also a distinct outpoint, also kept.
        let mut vout1 = record(&"11".repeat(32), "02aa", "03bb", "txGENUINE");
        vout1.output_index = 1;
        store.store_record(&vout1).await.unwrap();

        assert_eq!(store.record_count(), 3);
        let rows = store.list_for_winner("02aa", 100).await.unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].txid, "txGENUINE", "newest first");
        assert_eq!(rows[0].output_index, 1);
        assert_eq!(rows[2].txid, "txGARBAGE");
    }

    #[tokio::test]
    async fn distinct_pairs_tracked_independently() {
        let store = MemoryResultStorage::new();
        store
            .store_record(&record(&"11".repeat(32), "02aa", "03bb", "tx1"))
            .await
            .unwrap();
        store
            .store_record(&record(&"22".repeat(32), "02aa", "03bb", "tx2"))
            .await
            .unwrap();
        store
            .store_record(&record(&"11".repeat(32), "03bb", "02aa", "tx3"))
            .await
            .unwrap();
        assert_eq!(store.record_count(), 3);

        let rows = store.list_for_winner("03bb", 100).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].txid, "tx3");
    }

    #[test]
    fn query_json_shapes() {
        let q: ResultQuery = serde_json::from_value(serde_json::json!({
            "type": "resultsFor",
            "identity": "02".to_string() + &"a1".repeat(32),
            "limit": 50
        }))
        .unwrap();
        match q {
            ResultQuery::ResultsFor { identity, limit } => {
                assert_eq!(identity.len(), 66);
                assert_eq!(limit, Some(50));
            }
            other => panic!("expected ResultsFor, got {other:?}"),
        }

        // limit is optional.
        let q: ResultQuery = serde_json::from_value(serde_json::json!({
            "type": "recentResults"
        }))
        .unwrap();
        match q {
            ResultQuery::RecentResults { limit } => assert_eq!(limit, None),
            other => panic!("expected RecentResults, got {other:?}"),
        }

        // Unknown type is an error.
        assert!(
            serde_json::from_value::<ResultQuery>(serde_json::json!({"type": "nope"})).is_err()
        );
    }
}
