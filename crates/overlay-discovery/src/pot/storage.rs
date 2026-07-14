//! POT Storage trait — backend-agnostic storage for pot-spend records.
//!
//! One row per admitted `tm_pot` covenant UTXO (`pot_records` in D1). The
//! concrete implementation (D1, in-memory) is provided by the deployment
//! crate; [`MemoryPotStorage`] here backs the unit tests.
//!
//! # The ONE difference from `reveal` storage
//!
//! A reveal record is write-once (admit, then never touched). A pot record
//! is written on admission (`spent = false`) and UPDATED on spend (`spent =
//! true` + the `spendingTxid`). Records are NEVER deleted — a spent pot is
//! the permanent landing proof a client asks for. Two invariants make this
//! safe under replay / out-of-order delivery:
//!
//! - [`PotStorage::store_record`] inserts only if the outpoint is absent; it
//!   NEVER clobbers a spent row back to unspent (a re-admission of an
//!   already-recorded spend must not erase the spender).
//! - [`PotStorage::mark_spent`] updates an existing row only (mirrors the D1
//!   `UPDATE ... WHERE`); an outpoint must be admitted before it can be
//!   marked spent.
//!
//! # The BEEF store (`pot_beefs`)
//!
//! Alongside the spend records, this trait durably stores the BEEF of every
//! pot funding AND every pot-spending (settle/refund/sweep) tx, keyed by that
//! tx's own txid. It exists because the engine's `transactions` table is
//! LIFECYCLE-MANAGED: a BEEF row is only written by `insert_output` (a
//! settle, which admits no outputs, never gets one) and is DELETED by the
//! deep-delete when a spent unretained coin is cleaned up. `pot_beefs` is
//! OURS — never deleted — and is the durable source `low-app-layer`'s
//! `/beef/:txid` serves.
//!
//! Store rule (the "vanishing table" lesson — see the engine's
//! `insert_output` BEEF upsert): [`PotStorage::store_beef`] NEVER overwrites
//! an existing row with a shorter/empty beef — it writes only when no row
//! exists or the new beef is LONGER.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// A pot-spend record as stored in the index.
///
/// Keyed by `(txid, outputIndex)` = the pot funding outpoint. `spent` /
/// `spending_txid` carry the landing proof once the settle/refund/sweep is
/// seen by the engine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PotRecord {
    /// The pot funding txid (the SPENT output's txid).
    pub txid: String,
    /// The pot vout (the SPENT output's index).
    #[serde(rename = "outputIndex")]
    pub output_index: u32,
    /// Whether the pot output has been spent (a spender tx was seen).
    pub spent: bool,
    /// The txid that spent the pot (the settle / refund / sweep). `None`
    /// until the spend is recorded.
    #[serde(rename = "spendingTxid")]
    pub spending_txid: Option<String>,
}

/// One outpoint in a `spentStatus` query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutpointJson {
    pub txid: String,
    pub vout: u32,
}

/// `ls_pot` query shapes — tagged JSON, e.g.
/// `{"type":"spentStatus","outpoints":[{"txid":"<hex>","vout":0}]}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PotQuery {
    /// Ask the spent status of a batch of pot outpoints. The answer is an
    /// input-ordered array, one entry per requested outpoint.
    #[serde(rename = "spentStatus")]
    SpentStatus { outpoints: Vec<OutpointJson> },
}

/// Backend-agnostic storage for pot-spend records.
#[async_trait(?Send)]
pub trait PotStorage {
    /// Record an admitted pot outpoint (called with `spent = false`).
    ///
    /// Insert-if-absent: if a row for `(txid, outputIndex)` already exists it
    /// is left untouched — in particular a row already marked spent is NOT
    /// clobbered back to unspent. Mirrors the D1 `INSERT OR IGNORE`.
    async fn store_record(&self, record: &PotRecord) -> Result<(), PotStorageError>;

    /// Mark an admitted outpoint spent by `spending_txid`.
    ///
    /// Updates an existing row only (mirrors D1 `UPDATE ... WHERE`); a
    /// nonexistent outpoint is a no-op (an output must be admitted before it
    /// can be spent).
    async fn mark_spent(
        &self,
        txid: &str,
        output_index: u32,
        spending_txid: &str,
    ) -> Result<(), PotStorageError>;

    /// The record for an outpoint, or `None` if we never admitted it.
    async fn get_spent_status(
        &self,
        txid: &str,
        output_index: u32,
    ) -> Result<Option<PotRecord>, PotStorageError>;

    /// Durably store `beef` under `txid` (the stored tx's OWN txid — the
    /// funding txid for a funding beef, the SETTLE txid for a settle beef).
    ///
    /// Longer-wins, never-clobber (the "vanishing table" lesson): the write
    /// happens only when no row exists or the new beef is strictly LONGER
    /// than the stored one; an empty `beef` is rejected (no-op). A good row
    /// is therefore never replaced by a shorter/empty one.
    async fn store_beef(&self, txid: &str, beef: &[u8]) -> Result<(), PotStorageError>;

    /// The stored BEEF for `txid`, or `None` if we never stored one.
    async fn get_beef(&self, txid: &str) -> Result<Option<Vec<u8>>, PotStorageError>;
}

/// POT storage errors.
#[derive(Debug, thiserror::Error)]
pub enum PotStorageError {
    #[error("database error: {0}")]
    Database(String),
    #[error("{0}")]
    Other(String),
}

// ============================================================================
// In-memory implementation (for tests)
// ============================================================================

/// In-memory POT storage for testing.
#[derive(Debug, Default)]
pub struct MemoryPotStorage {
    records: std::sync::Mutex<Vec<PotRecord>>,
    beefs: std::sync::Mutex<std::collections::HashMap<String, Vec<u8>>>,
}

impl MemoryPotStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_count(&self) -> usize {
        self.records.lock().unwrap().len()
    }

    pub fn beef_count(&self) -> usize {
        self.beefs.lock().unwrap().len()
    }
}

#[async_trait(?Send)]
impl PotStorage for MemoryPotStorage {
    async fn store_record(&self, record: &PotRecord) -> Result<(), PotStorageError> {
        let mut records = self.records.lock().unwrap();
        // Insert-if-absent: an existing row (spent or not) is never clobbered.
        let exists = records
            .iter()
            .any(|r| r.txid == record.txid && r.output_index == record.output_index);
        if !exists {
            records.push(record.clone());
        }
        Ok(())
    }

    async fn mark_spent(
        &self,
        txid: &str,
        output_index: u32,
        spending_txid: &str,
    ) -> Result<(), PotStorageError> {
        let mut records = self.records.lock().unwrap();
        // UPDATE-only: touch an existing row; absent outpoint is a no-op.
        for r in records.iter_mut() {
            if r.txid == txid && r.output_index == output_index {
                r.spent = true;
                r.spending_txid = Some(spending_txid.to_string());
            }
        }
        Ok(())
    }

    async fn get_spent_status(
        &self,
        txid: &str,
        output_index: u32,
    ) -> Result<Option<PotRecord>, PotStorageError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter()
            .find(|r| r.txid == txid && r.output_index == output_index)
            .cloned())
    }

    async fn store_beef(&self, txid: &str, beef: &[u8]) -> Result<(), PotStorageError> {
        // Empty is rejected — never store unusable bytes.
        if beef.is_empty() {
            return Ok(());
        }
        let mut beefs = self.beefs.lock().unwrap();
        // Longer-wins: write only when absent or strictly longer (a good row
        // is never clobbered by a shorter one).
        match beefs.get(txid) {
            Some(existing) if existing.len() >= beef.len() => {}
            _ => {
                beefs.insert(txid.to_string(), beef.to_vec());
            }
        }
        Ok(())
    }

    async fn get_beef(&self, txid: &str) -> Result<Option<Vec<u8>>, PotStorageError> {
        Ok(self.beefs.lock().unwrap().get(txid).cloned())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn pot_record(txid: &str, vout: u32) -> PotRecord {
        PotRecord {
            txid: txid.into(),
            output_index: vout,
            spent: false,
            spending_txid: None,
        }
    }

    #[tokio::test]
    async fn store_then_get_returns_unspent_record() {
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        assert_eq!(store.record_count(), 1);

        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert!(!r.spent);
        assert_eq!(r.spending_txid, None);
    }

    #[tokio::test]
    async fn get_unknown_outpoint_is_none() {
        let store = MemoryPotStorage::new();
        assert!(store.get_spent_status("nope", 0).await.unwrap().is_none());
        // A different vout of a stored txid is still unknown.
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        assert!(store.get_spent_status("potA", 1).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn mark_spent_sets_spender() {
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store.mark_spent("potA", 0, "settleTx").await.unwrap();

        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert!(r.spent);
        assert_eq!(r.spending_txid.as_deref(), Some("settleTx"));
        // No new row was created.
        assert_eq!(store.record_count(), 1);
    }

    #[tokio::test]
    async fn store_is_idempotent_per_outpoint() {
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        assert_eq!(store.record_count(), 1);
    }

    #[tokio::test]
    async fn store_never_clobbers_a_spent_row_back_to_unspent() {
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store.mark_spent("potA", 0, "settleTx").await.unwrap();

        // A re-admission (e.g. GASP replay) must NOT erase the spender.
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        let r = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        assert!(r.spent, "spent status must survive re-admission");
        assert_eq!(r.spending_txid.as_deref(), Some("settleTx"));
        assert_eq!(store.record_count(), 1);
    }

    #[tokio::test]
    async fn mark_spent_on_unknown_outpoint_is_noop() {
        let store = MemoryPotStorage::new();
        // No admission first → mark_spent creates nothing (mirrors D1 UPDATE).
        store.mark_spent("ghost", 0, "settleTx").await.unwrap();
        assert_eq!(store.record_count(), 0);
        assert!(store.get_spent_status("ghost", 0).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn distinct_outpoints_tracked_independently() {
        let store = MemoryPotStorage::new();
        store.store_record(&pot_record("potA", 0)).await.unwrap();
        store.store_record(&pot_record("potB", 0)).await.unwrap();
        store.mark_spent("potA", 0, "settleA").await.unwrap();

        let a = store.get_spent_status("potA", 0).await.unwrap().unwrap();
        let b = store.get_spent_status("potB", 0).await.unwrap().unwrap();
        assert!(a.spent);
        assert!(!b.spent, "spending potA must not affect potB");
    }

    // ── BEEF store ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn store_beef_then_get_roundtrips() {
        let store = MemoryPotStorage::new();
        store.store_beef("fundingTx", &[1, 2, 3]).await.unwrap();
        assert_eq!(store.beef_count(), 1);
        assert_eq!(
            store.get_beef("fundingTx").await.unwrap().as_deref(),
            Some(&[1u8, 2, 3][..])
        );
        // A txid we never stored is None.
        assert!(store.get_beef("ghost").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn store_beef_longer_wins() {
        let store = MemoryPotStorage::new();
        store.store_beef("tx", &[1, 2]).await.unwrap();
        // A strictly longer beef replaces the stored one (re-hydration).
        store.store_beef("tx", &[9, 9, 9, 9]).await.unwrap();
        assert_eq!(
            store.get_beef("tx").await.unwrap().as_deref(),
            Some(&[9u8, 9, 9, 9][..])
        );
    }

    #[tokio::test]
    async fn store_beef_shorter_never_clobbers() {
        let store = MemoryPotStorage::new();
        store.store_beef("tx", &[1, 2, 3, 4]).await.unwrap();
        // Shorter must NOT clobber (the "vanishing table" lesson)…
        store.store_beef("tx", &[7]).await.unwrap();
        // …and equal-length must not either (write only when strictly longer).
        store.store_beef("tx", &[7, 7, 7, 7]).await.unwrap();
        assert_eq!(
            store.get_beef("tx").await.unwrap().as_deref(),
            Some(&[1u8, 2, 3, 4][..])
        );
    }

    #[tokio::test]
    async fn store_beef_empty_rejected() {
        let store = MemoryPotStorage::new();
        // Empty on a fresh key stores nothing…
        store.store_beef("tx", &[]).await.unwrap();
        assert_eq!(store.beef_count(), 0);
        assert!(store.get_beef("tx").await.unwrap().is_none());
        // …and empty never erases a good row.
        store.store_beef("tx", &[1, 2, 3]).await.unwrap();
        store.store_beef("tx", &[]).await.unwrap();
        assert_eq!(
            store.get_beef("tx").await.unwrap().as_deref(),
            Some(&[1u8, 2, 3][..])
        );
    }

    #[tokio::test]
    async fn store_beef_distinct_txids_independent() {
        let store = MemoryPotStorage::new();
        store.store_beef("funding", &[1]).await.unwrap();
        store.store_beef("settle", &[2, 2]).await.unwrap();
        assert_eq!(store.beef_count(), 2);
        assert_eq!(store.get_beef("funding").await.unwrap().as_deref(), Some(&[1u8][..]));
        assert_eq!(store.get_beef("settle").await.unwrap().as_deref(), Some(&[2u8, 2][..]));
    }

    #[test]
    fn query_json_shape() {
        let q: PotQuery = serde_json::from_value(serde_json::json!({
            "type": "spentStatus",
            "outpoints": [{"txid": "ab".repeat(32), "vout": 0}, {"txid": "cd".repeat(32), "vout": 1}]
        }))
        .unwrap();
        let PotQuery::SpentStatus { outpoints } = q;
        assert_eq!(outpoints.len(), 2);
        assert_eq!(outpoints[1].vout, 1);

        // Unknown type is an error.
        assert!(serde_json::from_value::<PotQuery>(serde_json::json!({"type": "nope"})).is_err());
    }
}
