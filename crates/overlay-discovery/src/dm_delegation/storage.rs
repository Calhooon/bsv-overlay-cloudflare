//! Storage trait + in-memory impl for `tm_dm_delegation` records.
//!
//! Mirrors `crates/overlay-discovery/src/agent/storage.rs` but with the
//! delegation revocation field set: `(txid, output_index, serial_number,
//! certifier_key, subject_key, expires_at, created_at)`. The concrete
//! D1-backed implementation lives in `overlay-cloudflare`.

use async_trait::async_trait;
use overlay_engine::types::UTXOReference;
use serde::{Deserialize, Serialize};

/// A stored delegation revocation record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DmDelegationRecord {
    pub txid: String,
    #[serde(rename = "outputIndex")]
    pub output_index: u32,
    /// Cert serial number from the envelope (not the bitcoin tx serial).
    pub serial_number: String,
    /// 66-hex compressed pubkey of the issuer / certifier.
    pub certifier_key: String,
    /// 66-hex compressed pubkey of the recipient / subject.
    pub subject_key: String,
    /// RFC3339 expiry timestamp from the envelope.
    pub expires_at: String,
    /// RFC3339 creation timestamp (storage backend sets this).
    pub created_at: String,
}

#[async_trait(?Send)]
pub trait DmDelegationStorage {
    /// Check if a record with the same `(txid, output_index)` already exists.
    async fn has_duplicate_record(
        &self,
        txid: &str,
        output_index: u32,
    ) -> Result<bool, DmDelegationStorageError>;

    /// Store a new delegation revocation record.
    async fn store_record(
        &self,
        record: &DmDelegationRecord,
    ) -> Result<(), DmDelegationStorageError>;

    /// Delete a record by UTXO reference (called when the UTXO is spent).
    async fn delete_record(
        &self,
        txid: &str,
        output_index: u32,
    ) -> Result<(), DmDelegationStorageError>;

    /// Find a record by exact outpoint. Returns a single-item vec or empty.
    async fn find_by_outpoint(
        &self,
        txid: &str,
        output_index: u32,
    ) -> Result<Vec<UTXOReference>, DmDelegationStorageError>;

    /// Find records by cert serial number.
    async fn find_by_serial(
        &self,
        serial: &str,
    ) -> Result<Vec<UTXOReference>, DmDelegationStorageError>;

    /// Find all records issued by a given certifier.
    async fn find_by_certifier(
        &self,
        certifier_key: &str,
        limit: Option<u32>,
        skip: Option<u32>,
    ) -> Result<Vec<UTXOReference>, DmDelegationStorageError>;

    /// Find all records (debugging / observability).
    async fn find_all(
        &self,
        limit: Option<u32>,
        skip: Option<u32>,
    ) -> Result<Vec<UTXOReference>, DmDelegationStorageError>;
}

#[derive(Debug, thiserror::Error)]
pub enum DmDelegationStorageError {
    #[error("database error: {0}")]
    Database(String),
    #[error("{0}")]
    Other(String),
}

// ============================================================================
// In-memory implementation (for tests)
// ============================================================================

#[derive(Debug, Clone)]
struct MemoryRow {
    txid: String,
    output_index: u32,
    serial_number: String,
    certifier_key: String,
    #[allow(dead_code)]
    subject_key: String,
    #[allow(dead_code)]
    expires_at: String,
    #[allow(dead_code)]
    created_at: std::time::SystemTime,
}

#[derive(Debug, Default)]
pub struct MemoryDmDelegationStorage {
    rows: std::sync::Mutex<Vec<MemoryRow>>,
}

impl MemoryDmDelegationStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_count(&self) -> usize {
        self.rows.lock().unwrap().len()
    }
}

#[async_trait(?Send)]
impl DmDelegationStorage for MemoryDmDelegationStorage {
    async fn has_duplicate_record(
        &self,
        txid: &str,
        output_index: u32,
    ) -> Result<bool, DmDelegationStorageError> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .iter()
            .any(|r| r.txid == txid && r.output_index == output_index))
    }

    async fn store_record(
        &self,
        record: &DmDelegationRecord,
    ) -> Result<(), DmDelegationStorageError> {
        self.rows.lock().unwrap().push(MemoryRow {
            txid: record.txid.clone(),
            output_index: record.output_index,
            serial_number: record.serial_number.clone(),
            certifier_key: record.certifier_key.clone(),
            subject_key: record.subject_key.clone(),
            expires_at: record.expires_at.clone(),
            created_at: std::time::SystemTime::now(),
        });
        Ok(())
    }

    async fn delete_record(
        &self,
        txid: &str,
        output_index: u32,
    ) -> Result<(), DmDelegationStorageError> {
        self.rows
            .lock()
            .unwrap()
            .retain(|r| !(r.txid == txid && r.output_index == output_index));
        Ok(())
    }

    async fn find_by_outpoint(
        &self,
        txid: &str,
        output_index: u32,
    ) -> Result<Vec<UTXOReference>, DmDelegationStorageError> {
        let rows = self.rows.lock().unwrap();
        Ok(rows
            .iter()
            .filter(|r| r.txid == txid && r.output_index == output_index)
            .map(|r| UTXOReference {
                txid: r.txid.clone(),
                output_index: r.output_index,
            })
            .collect())
    }

    async fn find_by_serial(
        &self,
        serial: &str,
    ) -> Result<Vec<UTXOReference>, DmDelegationStorageError> {
        let rows = self.rows.lock().unwrap();
        Ok(rows
            .iter()
            .filter(|r| r.serial_number == serial)
            .map(|r| UTXOReference {
                txid: r.txid.clone(),
                output_index: r.output_index,
            })
            .collect())
    }

    async fn find_by_certifier(
        &self,
        certifier_key: &str,
        limit: Option<u32>,
        skip: Option<u32>,
    ) -> Result<Vec<UTXOReference>, DmDelegationStorageError> {
        let rows = self.rows.lock().unwrap();
        let skip = skip.unwrap_or(0) as usize;
        Ok(rows
            .iter()
            .filter(|r| r.certifier_key == certifier_key)
            .skip(skip)
            .take(limit.map_or(usize::MAX, |l| l as usize))
            .map(|r| UTXOReference {
                txid: r.txid.clone(),
                output_index: r.output_index,
            })
            .collect())
    }

    async fn find_all(
        &self,
        limit: Option<u32>,
        skip: Option<u32>,
    ) -> Result<Vec<UTXOReference>, DmDelegationStorageError> {
        let rows = self.rows.lock().unwrap();
        let skip = skip.unwrap_or(0) as usize;
        Ok(rows
            .iter()
            .skip(skip)
            .take(limit.map_or(usize::MAX, |l| l as usize))
            .map(|r| UTXOReference {
                txid: r.txid.clone(),
                output_index: r.output_index,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_record(
        txid: &str,
        output_index: u32,
        serial: &str,
        certifier: &str,
    ) -> DmDelegationRecord {
        DmDelegationRecord {
            txid: txid.into(),
            output_index,
            serial_number: serial.into(),
            certifier_key: certifier.into(),
            subject_key: "02".repeat(33),
            expires_at: "2026-04-12T18:00:00+00:00".into(),
            created_at: String::new(),
        }
    }

    #[tokio::test]
    async fn store_and_find_by_outpoint() {
        let store = MemoryDmDelegationStorage::new();
        store
            .store_record(&make_record("tx1", 0, "ser-1", &"03".repeat(33)))
            .await
            .unwrap();
        store
            .store_record(&make_record("tx2", 0, "ser-2", &"03".repeat(33)))
            .await
            .unwrap();

        let hits = store.find_by_outpoint("tx1", 0).await.unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].txid, "tx1");

        let misses = store.find_by_outpoint("tx3", 0).await.unwrap();
        assert!(misses.is_empty());
    }

    #[tokio::test]
    async fn store_and_find_by_serial() {
        let store = MemoryDmDelegationStorage::new();
        store
            .store_record(&make_record("tx1", 0, "ser-1", &"03".repeat(33)))
            .await
            .unwrap();
        store
            .store_record(&make_record("tx2", 0, "ser-2", &"03".repeat(33)))
            .await
            .unwrap();

        let hits = store.find_by_serial("ser-2").await.unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].txid, "tx2");
    }

    #[tokio::test]
    async fn find_by_certifier_with_pagination() {
        let store = MemoryDmDelegationStorage::new();
        let cert = "03".repeat(33);
        for i in 0..7 {
            store
                .store_record(&make_record(
                    &format!("tx{i}"),
                    0,
                    &format!("ser-{i}"),
                    &cert,
                ))
                .await
                .unwrap();
        }

        let page = store
            .find_by_certifier(&cert, Some(3), Some(2))
            .await
            .unwrap();
        assert_eq!(page.len(), 3);
    }

    #[tokio::test]
    async fn delete_record_removes_from_lookup() {
        let store = MemoryDmDelegationStorage::new();
        store
            .store_record(&make_record("tx1", 0, "ser-1", &"03".repeat(33)))
            .await
            .unwrap();
        assert_eq!(store.record_count(), 1);

        store.delete_record("tx1", 0).await.unwrap();
        assert_eq!(store.record_count(), 0);
        assert!(store.find_by_outpoint("tx1", 0).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn delete_only_matching_outpoint() {
        let store = MemoryDmDelegationStorage::new();
        store
            .store_record(&make_record("tx1", 0, "ser-a", &"03".repeat(33)))
            .await
            .unwrap();
        store
            .store_record(&make_record("tx1", 1, "ser-b", &"03".repeat(33)))
            .await
            .unwrap();
        store.delete_record("tx1", 0).await.unwrap();
        assert_eq!(store.record_count(), 1);
        assert!(store.find_by_outpoint("tx1", 0).await.unwrap().is_empty());
        assert_eq!(store.find_by_outpoint("tx1", 1).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn has_duplicate_record_uses_outpoint() {
        let store = MemoryDmDelegationStorage::new();
        assert!(!store.has_duplicate_record("tx1", 0).await.unwrap());
        store
            .store_record(&make_record("tx1", 0, "ser-1", &"03".repeat(33)))
            .await
            .unwrap();
        assert!(store.has_duplicate_record("tx1", 0).await.unwrap());
        assert!(!store.has_duplicate_record("tx1", 1).await.unwrap());
        assert!(!store.has_duplicate_record("tx2", 0).await.unwrap());
    }

    #[tokio::test]
    async fn record_serde_round_trip() {
        let r = make_record("abc", 5, "ser", &"03".repeat(33));
        let json = serde_json::to_string(&r).unwrap();
        let back: DmDelegationRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(back.txid, "abc");
        assert_eq!(back.output_index, 5);
        assert_eq!(back.serial_number, "ser");
    }
}
