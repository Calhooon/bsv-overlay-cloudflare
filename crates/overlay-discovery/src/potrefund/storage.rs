//! POTREFUND Storage trait — backend-agnostic storage for potrefund-marker
//! records.
//!
//! One row per marker OUTPOINT — `(txid, outputIndex)` (`potrefund_records`
//! in D1). The concrete implementation (D1, in-memory) is provided by the
//! deployment crate; [`MemoryPotrefundStorage`] here backs the unit tests.
//! Structure mirrors `potparty::storage`:
//!
//! **Keyed by outpoint, NOT by `(potTxid, potVout)` — every admitted marker
//! is kept.** Admission is byte-format-only (no sig / tx check). Keying on
//! the outpoint keeps a garbage front-run from occupying a pot's slot and
//! censoring a genuine refund (the `tm_result` censorship lesson); it also
//! makes a replayed / duplicate SUBMIT of the same output a harmless no-op.
//! BOTH seats may publish a refund backup for a pot — each is its own row.
//!
//! [`PotrefundStorage::store_record`] is insert-if-absent on the outpoint
//! (D1 `INSERT OR IGNORE` on the `(txid, outputIndex)` primary key) — rows
//! are NEVER deleted (a pre-signed refund backup is permanent recovery
//! history, like a pot or reveal record).
//!
//! `created_at` is assigned by the STORAGE layer at insert (D1 stamps the
//! unix time, the memory impl an insertion counter) — the value on the
//! record passed to `store_record` is ignored. Recency ordering
//! (`list_for_identity` / `list_for_pot`, newest first) rides on it.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// A potrefund-marker record as stored in the index.
///
/// Keyed by the marker OUTPOINT `(txid, outputIndex)` — every admitted
/// marker is kept. Byte fields are carried back VERBATIM to querying
/// clients (which parse + verify the `refundRawHex` / `sig` themselves — the
/// overlay never does).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PotrefundRecord {
    /// The publishing seat's compressed identity pubkey (33 bytes, lowercase
    /// hex).
    pub identity: String,
    /// Game ID (32 bytes, lowercase hex).
    #[serde(rename = "gameId")]
    pub game_id: String,
    /// The pot funding txid (32 bytes, lowercase hex).
    #[serde(rename = "potTxid")]
    pub pot_txid: String,
    /// The pot output index within `pot_txid`.
    #[serde(rename = "potVout")]
    pub pot_vout: u32,
    /// The pre-signed refund transaction bytes (lowercase hex) — preserved
    /// verbatim, parsed / re-broadcast CLIENT-side only.
    #[serde(rename = "refundRawHex")]
    pub refund_raw_hex: String,
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

/// `ls_potrefund` query shapes — tagged JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PotrefundQuery {
    /// "Give me the pre-signed refund backup(s) for this pot outpoint." —
    /// the recovery question. Returns every marker naming the pot (each seat
    /// may publish its own). Newest first.
    #[serde(rename = "byPot")]
    ByPot {
        #[serde(rename = "potTxid")]
        pot_txid: String,
        #[serde(rename = "potVout")]
        pot_vout: u32,
        limit: Option<u32>,
    },
    /// "Which pots have I published a refund backup for?" — completeness.
    /// Newest first.
    #[serde(rename = "partyFor")]
    PartyFor { identity: String, limit: Option<u32> },
}

/// Backend-agnostic storage for potrefund-marker records.
#[async_trait(?Send)]
pub trait PotrefundStorage {
    /// Store a record keyed by its OUTPOINT `(txid, outputIndex)` —
    /// insert-if-absent: a replayed / duplicate SUBMIT of the same output
    /// is a no-op, but markers for the same pot from DIFFERENT txs are ALL
    /// kept. Mirrors the D1 `INSERT OR IGNORE`. Never overwrites, never
    /// deletes. `created_at` is assigned here (the record's value is
    /// ignored).
    async fn store_record(&self, record: &PotrefundRecord) -> Result<(), PotrefundStorageError>;

    /// Up to `limit` records whose `identity` is `identity`, newest first.
    async fn list_for_identity(
        &self,
        identity: &str,
        limit: usize,
    ) -> Result<Vec<PotrefundRecord>, PotrefundStorageError>;

    /// Up to `limit` records naming the pot outpoint `(pot_txid, pot_vout)`,
    /// newest first — the pre-signed refund backup(s).
    async fn list_for_pot(
        &self,
        pot_txid: &str,
        pot_vout: u32,
        limit: usize,
    ) -> Result<Vec<PotrefundRecord>, PotrefundStorageError>;
}

/// POTREFUND storage errors.
#[derive(Debug, thiserror::Error)]
pub enum PotrefundStorageError {
    #[error("database error: {0}")]
    Database(String),
    #[error("{0}")]
    Other(String),
}

// ============================================================================
// In-memory implementation (for tests)
// ============================================================================

/// In-memory POTREFUND storage for testing. Insertion order IS recency order
/// (newest = last pushed); `created_at` is stamped with an insertion
/// counter so answers expose a monotone `createdAt` like D1's unix stamp.
#[derive(Debug, Default)]
pub struct MemoryPotrefundStorage {
    records: std::sync::Mutex<Vec<PotrefundRecord>>,
}

impl MemoryPotrefundStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_count(&self) -> usize {
        self.records.lock().unwrap().len()
    }
}

#[async_trait(?Send)]
impl PotrefundStorage for MemoryPotrefundStorage {
    async fn store_record(&self, record: &PotrefundRecord) -> Result<(), PotrefundStorageError> {
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
    ) -> Result<Vec<PotrefundRecord>, PotrefundStorageError> {
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
    ) -> Result<Vec<PotrefundRecord>, PotrefundStorageError> {
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

    fn record(identity: &str, pot_vout: u32, txid: &str) -> PotrefundRecord {
        PotrefundRecord {
            identity: identity.into(),
            game_id: "11".repeat(32),
            pot_txid: "22".repeat(32),
            pot_vout,
            refund_raw_hex: "0100000001deadbeef".into(),
            sig_hex: "3045ab".into(),
            txid: txid.into(),
            output_index: 0,
            created_at: 0, // ignored — storage assigns
        }
    }

    #[tokio::test]
    async fn store_then_list_roundtrips() {
        let store = MemoryPotrefundStorage::new();
        store.store_record(&record("02aa", 0, "tx1")).await.unwrap();
        assert_eq!(store.record_count(), 1);

        let rows = store.list_for_identity("02aa", 100).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].txid, "tx1");
        assert_eq!(rows[0].refund_raw_hex, "0100000001deadbeef");
    }

    #[tokio::test]
    async fn list_for_identity_filters_by_identity_only() {
        let store = MemoryPotrefundStorage::new();
        store.store_record(&record("02aa", 0, "tx1")).await.unwrap();
        // The opponent's OWN refund backup for the same pot is a different row.
        store.store_record(&record("03bb", 0, "tx2")).await.unwrap();

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
        let store = MemoryPotrefundStorage::new();
        // Same pot outpoint, two refund backups (one per seat).
        store.store_record(&record("02aa", 0, "txA")).await.unwrap();
        store.store_record(&record("03bb", 0, "txB")).await.unwrap();
        // A different pot vout is NOT matched.
        store.store_record(&record("02aa", 1, "txC")).await.unwrap();

        let rows = store.list_for_pot(&"22".repeat(32), 0, 100).await.unwrap();
        assert_eq!(rows.len(), 2, "both parties' backups for vout 0");
        assert_eq!(rows[0].txid, "txB", "newest first");
        assert_eq!(rows[1].txid, "txA");
    }

    #[tokio::test]
    async fn lists_are_newest_first_and_respect_limit() {
        let store = MemoryPotrefundStorage::new();
        for i in 0..5u8 {
            store
                .store_record(&record("02aa", 0, &format!("tx{i}")))
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
        let store = MemoryPotrefundStorage::new();
        store
            .store_record(&record("02aa", 0, "txSAME"))
            .await
            .unwrap();
        // A replayed / duplicate SUBMIT of the SAME output — ignored.
        let mut replay = record("02aa", 0, "txSAME");
        replay.refund_raw_hex = "ffffffff".into();
        store.store_record(&replay).await.unwrap();

        assert_eq!(store.record_count(), 1);
        let rows = store.list_for_identity("02aa", 100).await.unwrap();
        assert_eq!(
            rows[0].refund_raw_hex, "0100000001deadbeef",
            "first insert for the outpoint kept"
        );
    }

    #[test]
    fn query_json_shapes() {
        let q: PotrefundQuery = serde_json::from_value(serde_json::json!({
            "type": "byPot",
            "potTxid": "22".repeat(32),
            "potVout": 3,
            "limit": 50
        }))
        .unwrap();
        match q {
            PotrefundQuery::ByPot {
                pot_txid,
                pot_vout,
                limit,
            } => {
                assert_eq!(pot_txid.len(), 64);
                assert_eq!(pot_vout, 3);
                assert_eq!(limit, Some(50));
            }
            other => panic!("expected ByPot, got {other:?}"),
        }

        // limit optional; partyFor shape.
        let q: PotrefundQuery = serde_json::from_value(serde_json::json!({
            "type": "partyFor",
            "identity": "02".to_string() + &"a1".repeat(32)
        }))
        .unwrap();
        match q {
            PotrefundQuery::PartyFor { identity, limit } => {
                assert_eq!(identity.len(), 66);
                assert_eq!(limit, None);
            }
            other => panic!("expected PartyFor, got {other:?}"),
        }

        assert!(
            serde_json::from_value::<PotrefundQuery>(serde_json::json!({"type": "nope"})).is_err()
        );
    }
}
