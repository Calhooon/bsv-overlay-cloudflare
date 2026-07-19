//! POTPARTY Storage trait — backend-agnostic storage for potparty-marker
//! records.
//!
//! One row per marker OUTPOINT — `(txid, outputIndex)` (`potparty_records`
//! in D1). The concrete implementation (D1, in-memory) is provided by the
//! deployment crate; [`MemoryPotpartyStorage`] here backs the unit tests.
//! Structure mirrors `result::storage`:
//!
//! **Keyed by outpoint, NOT by `(identity, gameId)` — every admitted marker
//! is kept.** Admission is byte-format-only (no sig check). Keying on the
//! outpoint keeps a garbage front-run from occupying an identity's slot and
//! censoring a genuine marker (the `tm_result` censorship lesson); it also
//! makes a replayed / duplicate SUBMIT of the same output a harmless no-op.
//!
//! [`PotpartyStorage::store_record`] is insert-if-absent on the outpoint
//! (D1 `INSERT OR IGNORE` on the `(txid, outputIndex)` primary key) — rows
//! are NEVER deleted (a pot-participation fact is permanent recovery
//! history, like a pot or reveal record).
//!
//! `created_at` is assigned by the STORAGE layer at insert (D1 stamps the
//! unix time, the memory impl an insertion counter) — the value on the
//! record passed to `store_record` is ignored. Recency ordering
//! (`list_for_identity` / `list_for_pot`, newest first) rides on it.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// A potparty-marker record as stored in the index.
///
/// Keyed by the marker OUTPOINT `(txid, outputIndex)` — every admitted
/// marker is kept. Byte fields are carried back VERBATIM to querying
/// clients (which may verify the `sig` themselves — the overlay never
/// does).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PotpartyRecord {
    /// The publishing seat's compressed identity pubkey (33 bytes, lowercase
    /// hex).
    pub identity: String,
    /// The opponent seat's compressed identity pubkey (33 bytes, lowercase
    /// hex).
    #[serde(rename = "opponentIdentity")]
    pub opponent_identity: String,
    /// Game ID (32 bytes, lowercase hex).
    #[serde(rename = "gameId")]
    pub game_id: String,
    /// The pot funding txid (32 bytes, lowercase hex).
    #[serde(rename = "potTxid")]
    pub pot_txid: String,
    /// The pot output index within `pot_txid`.
    #[serde(rename = "potVout")]
    pub pot_vout: u32,
    /// The pre-signed refund's recovery height.
    #[serde(rename = "recoveryHeight")]
    pub recovery_height: u32,
    /// The seat's DER signature push (lowercase hex) — preserved verbatim,
    /// verified CLIENT-side only (the overlay never verifies it).
    #[serde(rename = "sigHex")]
    pub sig_hex: String,
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

/// `ls_potparty` query shapes — tagged JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PotpartyQuery {
    /// "Which pots is this identity a party to?" — the recovery question.
    /// Newest first.
    #[serde(rename = "partyFor")]
    PartyFor { identity: String, limit: Option<u32> },
    /// "Who are the two parties to this pot outpoint?" — returns every
    /// marker naming the pot (each seat publishes its own). Newest first.
    #[serde(rename = "byPot")]
    ByPot {
        #[serde(rename = "potTxid")]
        pot_txid: String,
        #[serde(rename = "potVout")]
        pot_vout: u32,
        limit: Option<u32>,
    },
}

/// Backend-agnostic storage for potparty-marker records.
#[async_trait(?Send)]
pub trait PotpartyStorage {
    /// Store a record keyed by its OUTPOINT `(txid, outputIndex)` —
    /// insert-if-absent: a replayed / duplicate SUBMIT of the same output
    /// is a no-op, but markers for the same identity from DIFFERENT txs are
    /// ALL kept. Mirrors the D1 `INSERT OR IGNORE`. Never overwrites, never
    /// deletes. `created_at` is assigned here (the record's value is
    /// ignored).
    async fn store_record(&self, record: &PotpartyRecord) -> Result<(), PotpartyStorageError>;

    /// Up to `limit` records whose `identity` is `identity`, newest first.
    async fn list_for_identity(
        &self,
        identity: &str,
        limit: usize,
    ) -> Result<Vec<PotpartyRecord>, PotpartyStorageError>;

    /// Up to `limit` records naming the pot outpoint `(pot_txid, pot_vout)`,
    /// newest first — the two parties (one marker each).
    async fn list_for_pot(
        &self,
        pot_txid: &str,
        pot_vout: u32,
        limit: usize,
    ) -> Result<Vec<PotpartyRecord>, PotpartyStorageError>;
}

/// POTPARTY storage errors.
#[derive(Debug, thiserror::Error)]
pub enum PotpartyStorageError {
    #[error("database error: {0}")]
    Database(String),
    #[error("{0}")]
    Other(String),
}

// ============================================================================
// In-memory implementation (for tests)
// ============================================================================

/// In-memory POTPARTY storage for testing. Insertion order IS recency order
/// (newest = last pushed); `created_at` is stamped with an insertion
/// counter so answers expose a monotone `createdAt` like D1's unix stamp.
#[derive(Debug, Default)]
pub struct MemoryPotpartyStorage {
    records: std::sync::Mutex<Vec<PotpartyRecord>>,
}

impl MemoryPotpartyStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_count(&self) -> usize {
        self.records.lock().unwrap().len()
    }
}

#[async_trait(?Send)]
impl PotpartyStorage for MemoryPotpartyStorage {
    async fn store_record(&self, record: &PotpartyRecord) -> Result<(), PotpartyStorageError> {
        let mut records = self.records.lock().unwrap();
        // Insert-if-absent on the OUTPOINT (txid, outputIndex) — a replayed
        // submit of the same output is a no-op, matching D1's INSERT OR
        // IGNORE on the primary key.
        let exists = records
            .iter()
            .any(|r| r.txid == record.txid && r.output_index == record.output_index);
        if !exists {
            let mut r = record.clone();
            r.created_at = records.len() as i64;
            records.push(r);
        }
        Ok(())
    }

    async fn list_for_identity(
        &self,
        identity: &str,
        limit: usize,
    ) -> Result<Vec<PotpartyRecord>, PotpartyStorageError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter()
            .rev() // newest first (insertion order = recency order)
            .filter(|r| r.identity == identity)
            .take(limit)
            .cloned()
            .collect())
    }

    async fn list_for_pot(
        &self,
        pot_txid: &str,
        pot_vout: u32,
        limit: usize,
    ) -> Result<Vec<PotpartyRecord>, PotpartyStorageError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter()
            .rev()
            .filter(|r| r.pot_txid == pot_txid && r.pot_vout == pot_vout)
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

    fn record(identity: &str, opponent: &str, txid: &str) -> PotpartyRecord {
        PotpartyRecord {
            identity: identity.into(),
            opponent_identity: opponent.into(),
            game_id: "11".repeat(32),
            pot_txid: "22".repeat(32),
            pot_vout: 0,
            recovery_height: 850_000,
            sig_hex: "3045ab".into(),
            txid: txid.into(),
            output_index: 0,
            created_at: 0, // ignored — storage assigns
        }
    }

    #[tokio::test]
    async fn store_then_list_roundtrips() {
        let store = MemoryPotpartyStorage::new();
        store
            .store_record(&record("02aa", "03bb", "tx1"))
            .await
            .unwrap();
        assert_eq!(store.record_count(), 1);

        let rows = store.list_for_identity("02aa", 100).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].txid, "tx1");
        assert_eq!(rows[0].opponent_identity, "03bb");
        assert_eq!(rows[0].recovery_height, 850_000);
    }

    #[tokio::test]
    async fn list_for_identity_filters_by_identity_only() {
        let store = MemoryPotpartyStorage::new();
        store
            .store_record(&record("02aa", "03bb", "tx1"))
            .await
            .unwrap();
        // The opponent's OWN marker (seats flipped) is a different row.
        store
            .store_record(&record("03bb", "02aa", "tx2"))
            .await
            .unwrap();

        let rows = store.list_for_identity("02aa", 100).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].txid, "tx1");
        assert!(store
            .list_for_identity("02cc", 100)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn list_for_pot_returns_both_parties() {
        let store = MemoryPotpartyStorage::new();
        // Same pot outpoint, two markers (one per seat).
        store
            .store_record(&record("02aa", "03bb", "txA"))
            .await
            .unwrap();
        store
            .store_record(&record("03bb", "02aa", "txB"))
            .await
            .unwrap();
        // A different pot vout is NOT matched.
        let mut other = record("02aa", "03bb", "txC");
        other.pot_vout = 1;
        store.store_record(&other).await.unwrap();

        let rows = store
            .list_for_pot(&"22".repeat(32), 0, 100)
            .await
            .unwrap();
        assert_eq!(rows.len(), 2, "both parties to vout 0");
        assert_eq!(rows[0].txid, "txB", "newest first");
        assert_eq!(rows[1].txid, "txA");
    }

    #[tokio::test]
    async fn lists_are_newest_first_and_respect_limit() {
        let store = MemoryPotpartyStorage::new();
        for i in 0..5u8 {
            store
                .store_record(&record("02aa", "03bb", &format!("tx{i}")))
                .await
                .unwrap();
        }
        let rows = store.list_for_identity("02aa", 3).await.unwrap();
        assert_eq!(rows.len(), 3, "limit respected");
        assert_eq!(rows[0].txid, "tx4", "newest first");
        assert!(rows[0].created_at > rows[1].created_at);
    }

    #[tokio::test]
    async fn same_outpoint_replay_is_a_noop() {
        let store = MemoryPotpartyStorage::new();
        store
            .store_record(&record("02aa", "03bb", "txSAME"))
            .await
            .unwrap();
        // A replayed / duplicate SUBMIT of the SAME output — ignored.
        let replay = record("02aa", "03cc", "txSAME");
        store.store_record(&replay).await.unwrap();

        assert_eq!(store.record_count(), 1);
        let rows = store.list_for_identity("02aa", 100).await.unwrap();
        assert_eq!(
            rows[0].opponent_identity, "03bb",
            "first insert for the outpoint kept"
        );
    }

    #[test]
    fn query_json_shapes() {
        let q: PotpartyQuery = serde_json::from_value(serde_json::json!({
            "type": "partyFor",
            "identity": "02".to_string() + &"a1".repeat(32),
            "limit": 50
        }))
        .unwrap();
        match q {
            PotpartyQuery::PartyFor { identity, limit } => {
                assert_eq!(identity.len(), 66);
                assert_eq!(limit, Some(50));
            }
            other => panic!("expected PartyFor, got {other:?}"),
        }

        // limit optional; byPot shape.
        let q: PotpartyQuery = serde_json::from_value(serde_json::json!({
            "type": "byPot",
            "potTxid": "22".repeat(32),
            "potVout": 3
        }))
        .unwrap();
        match q {
            PotpartyQuery::ByPot {
                pot_txid,
                pot_vout,
                limit,
            } => {
                assert_eq!(pot_txid.len(), 64);
                assert_eq!(pot_vout, 3);
                assert_eq!(limit, None);
            }
            other => panic!("expected ByPot, got {other:?}"),
        }

        assert!(
            serde_json::from_value::<PotpartyQuery>(serde_json::json!({"type": "nope"})).is_err()
        );
    }
}
