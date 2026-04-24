//! SLAP Storage trait — backend-agnostic storage for SLAP advertisement records.
//!
//! Same pattern as SHIPStorage but queries by `service` (single string) instead of
//! `topics` (array of strings).
//! Ported from `~/bsv/overlay-discovery-services/src/SLAP/SLAPStorage.ts`.

use async_trait::async_trait;
use overlay_engine::types::UTXOReference;
use serde::{Deserialize, Serialize};

use crate::ship::storage::SortOrder;

/// A SLAP record with its associated domain — used by the Janitor to health-check hosts
/// AND by `CloudflareAdvertiser::find_all_advertisements` to distinguish our own
/// published advertisements from peer advertisements we've GASP-synced.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SLAPDiscoveryRecord {
    pub txid: String,
    #[serde(rename = "outputIndex")]
    pub output_index: u32,
    /// Publisher's identity public key (33-byte compressed, hex-encoded).
    /// Same rationale as `SHIPDiscoveryRecord::identity_key`.
    #[serde(rename = "identityKey")]
    pub identity_key: String,
    pub domain: String,
    pub service: String,
}

/// Query parameters for finding SLAP records.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SLAPQuery {
    pub find_all: Option<bool>,
    pub domain: Option<String>,
    /// Single service name (not an array, unlike SHIP's topics).
    pub service: Option<String>,
    pub identity_key: Option<String>,
    pub limit: Option<u32>,
    pub skip: Option<u32>,
    pub sort_order: Option<SortOrder>,
}

/// Backend-agnostic storage for SLAP advertisement records.
#[async_trait(?Send)]
pub trait SLAPStorage {
    async fn has_duplicate_record(
        &self,
        identity_key: &str,
        domain: &str,
        service: &str,
    ) -> Result<bool, SLAPStorageError>;

    async fn store_record(
        &self,
        txid: &str,
        output_index: u32,
        identity_key: &str,
        domain: &str,
        service: &str,
    ) -> Result<(), SLAPStorageError>;

    async fn delete_record(&self, txid: &str, output_index: u32) -> Result<(), SLAPStorageError>;

    async fn find_record(&self, query: &SLAPQuery) -> Result<Vec<UTXOReference>, SLAPStorageError>;

    async fn find_all(
        &self,
        limit: Option<u32>,
        skip: Option<u32>,
        sort_order: Option<SortOrder>,
    ) -> Result<Vec<UTXOReference>, SLAPStorageError>;

    /// Find all records with domain information (for Janitor health checks).
    ///
    /// Returns full discovery records including domain, service, txid, and outputIndex.
    async fn find_all_records(&self) -> Result<Vec<SLAPDiscoveryRecord>, SLAPStorageError>;
}

#[derive(Debug, thiserror::Error)]
pub enum SLAPStorageError {
    #[error("database error: {0}")]
    Database(String),
    #[error("{0}")]
    Other(String),
}

// ── In-memory implementation ───────────────────────────────────────────

#[derive(Debug, Clone)]
struct SLAPRecord {
    txid: String,
    output_index: u32,
    identity_key: String,
    domain: String,
    service: String,
    created_at: std::time::SystemTime,
}

#[derive(Debug, Default)]
pub struct MemorySLAPStorage {
    records: std::sync::Mutex<Vec<SLAPRecord>>,
}

impl MemorySLAPStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_count(&self) -> usize {
        self.records.lock().unwrap().len()
    }
}

#[async_trait(?Send)]
impl SLAPStorage for MemorySLAPStorage {
    async fn has_duplicate_record(
        &self,
        identity_key: &str,
        domain: &str,
        service: &str,
    ) -> Result<bool, SLAPStorageError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter()
            .any(|r| r.identity_key == identity_key && r.domain == domain && r.service == service))
    }

    async fn store_record(
        &self,
        txid: &str,
        output_index: u32,
        identity_key: &str,
        domain: &str,
        service: &str,
    ) -> Result<(), SLAPStorageError> {
        self.records.lock().unwrap().push(SLAPRecord {
            txid: txid.into(),
            output_index,
            identity_key: identity_key.into(),
            domain: domain.into(),
            service: service.into(),
            created_at: std::time::SystemTime::now(),
        });
        Ok(())
    }

    async fn delete_record(&self, txid: &str, output_index: u32) -> Result<(), SLAPStorageError> {
        self.records
            .lock()
            .unwrap()
            .retain(|r| !(r.txid == txid && r.output_index == output_index));
        Ok(())
    }

    async fn find_record(&self, query: &SLAPQuery) -> Result<Vec<UTXOReference>, SLAPStorageError> {
        let records = self.records.lock().unwrap();
        let mut results: Vec<&SLAPRecord> = records
            .iter()
            .filter(|r| {
                if let Some(ref d) = query.domain {
                    if r.domain != *d {
                        return false;
                    }
                }
                if let Some(ref s) = query.service {
                    if r.service != *s {
                        return false;
                    }
                }
                if let Some(ref ik) = query.identity_key {
                    if r.identity_key != *ik {
                        return false;
                    }
                }
                true
            })
            .collect();

        match query.sort_order {
            Some(SortOrder::Asc) => results.sort_by_key(|r| r.created_at),
            _ => {
                results.sort_by_key(|r| r.created_at);
                results.reverse();
            }
        }

        let skip = query.skip.unwrap_or(0) as usize;
        Ok(results
            .into_iter()
            .skip(skip)
            .take(query.limit.map_or(usize::MAX, |l| l as usize))
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
        sort_order: Option<SortOrder>,
    ) -> Result<Vec<UTXOReference>, SLAPStorageError> {
        self.find_record(&SLAPQuery {
            find_all: Some(true),
            limit,
            skip,
            sort_order,
            ..Default::default()
        })
        .await
    }

    async fn find_all_records(&self) -> Result<Vec<SLAPDiscoveryRecord>, SLAPStorageError> {
        let records = self.records.lock().unwrap();
        Ok(records
            .iter()
            .map(|r| SLAPDiscoveryRecord {
                txid: r.txid.clone(),
                output_index: r.output_index,
                identity_key: r.identity_key.clone(),
                domain: r.domain.clone(),
                service: r.service.clone(),
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn store_and_find() {
        let store = MemorySLAPStorage::new();
        store
            .store_record("tx1", 0, "k1", "https://a.com", "ls_test")
            .await
            .unwrap();
        store
            .store_record("tx2", 0, "k2", "https://b.com", "ls_other")
            .await
            .unwrap();

        let results = store
            .find_record(&SLAPQuery {
                service: Some("ls_test".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx1");
    }

    #[tokio::test]
    async fn has_duplicate() {
        let store = MemorySLAPStorage::new();
        assert!(!store.has_duplicate_record("k", "d", "s").await.unwrap());
        store.store_record("tx1", 0, "k", "d", "s").await.unwrap();
        assert!(store.has_duplicate_record("k", "d", "s").await.unwrap());
    }

    #[tokio::test]
    async fn delete() {
        let store = MemorySLAPStorage::new();
        store.store_record("tx1", 0, "k", "d", "s").await.unwrap();
        store.delete_record("tx1", 0).await.unwrap();
        assert_eq!(store.record_count(), 0);
    }

    #[tokio::test]
    async fn pagination() {
        let store = MemorySLAPStorage::new();
        for i in 0..10 {
            store
                .store_record(&format!("tx{i}"), 0, "k", "d", "s")
                .await
                .unwrap();
        }
        let results = store.find_all(Some(3), Some(2), None).await.unwrap();
        assert_eq!(results.len(), 3);
    }

    // ========================================================================
    // Additional storage tests
    // ========================================================================

    #[tokio::test]
    async fn record_count_starts_at_zero() {
        let store = MemorySLAPStorage::new();
        assert_eq!(store.record_count(), 0);
    }

    #[tokio::test]
    async fn record_count_tracks_insertions() {
        let store = MemorySLAPStorage::new();
        for i in 0..5 {
            store
                .store_record(&format!("tx{i}"), 0, "k", "d", "s")
                .await
                .unwrap();
        }
        assert_eq!(store.record_count(), 5);
    }

    #[tokio::test]
    async fn has_duplicate_requires_all_three_fields_to_match() {
        let store = MemorySLAPStorage::new();
        store
            .store_record("tx1", 0, "key1", "domain1", "svc1")
            .await
            .unwrap();

        // Same identity_key but different domain
        assert!(!store
            .has_duplicate_record("key1", "other_domain", "svc1")
            .await
            .unwrap());
        // Same identity_key but different service
        assert!(!store
            .has_duplicate_record("key1", "domain1", "other_svc")
            .await
            .unwrap());
        // Same domain+service but different identity_key
        assert!(!store
            .has_duplicate_record("key2", "domain1", "svc1")
            .await
            .unwrap());
        // All match
        assert!(store
            .has_duplicate_record("key1", "domain1", "svc1")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn delete_only_matching_utxo() {
        let store = MemorySLAPStorage::new();
        store.store_record("tx1", 0, "k", "d", "s").await.unwrap();
        store.store_record("tx1", 1, "k", "d2", "s2").await.unwrap();
        store.store_record("tx2", 0, "k", "d3", "s3").await.unwrap();

        store.delete_record("tx1", 0).await.unwrap();
        assert_eq!(store.record_count(), 2);

        let results = store.find_all(None, None, None).await.unwrap();
        let utxos: Vec<(&str, u32)> = results
            .iter()
            .map(|r| (r.txid.as_str(), r.output_index))
            .collect();
        assert!(utxos.contains(&("tx1", 1)));
        assert!(utxos.contains(&("tx2", 0)));
    }

    #[tokio::test]
    async fn delete_nonexistent_record_is_ok() {
        let store = MemorySLAPStorage::new();
        store.delete_record("nonexistent", 0).await.unwrap();
        assert_eq!(store.record_count(), 0);
    }

    #[tokio::test]
    async fn find_by_domain() {
        let store = MemorySLAPStorage::new();
        store
            .store_record("tx1", 0, "k", "https://a.com", "s")
            .await
            .unwrap();
        store
            .store_record("tx2", 0, "k", "https://b.com", "s")
            .await
            .unwrap();

        let results = store
            .find_record(&SLAPQuery {
                domain: Some("https://a.com".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx1");
    }

    #[tokio::test]
    async fn find_by_identity_key() {
        let store = MemorySLAPStorage::new();
        store
            .store_record("tx1", 0, "key_a", "d", "s")
            .await
            .unwrap();
        store
            .store_record("tx2", 0, "key_b", "d", "s")
            .await
            .unwrap();

        let results = store
            .find_record(&SLAPQuery {
                identity_key: Some("key_b".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx2");
    }

    #[tokio::test]
    async fn find_combined_domain_and_service() {
        let store = MemorySLAPStorage::new();
        store
            .store_record("tx1", 0, "k", "https://a.com", "ls_foo")
            .await
            .unwrap();
        store
            .store_record("tx2", 0, "k", "https://b.com", "ls_foo")
            .await
            .unwrap();
        store
            .store_record("tx3", 0, "k", "https://a.com", "ls_bar")
            .await
            .unwrap();

        let results = store
            .find_record(&SLAPQuery {
                domain: Some("https://a.com".into()),
                service: Some("ls_foo".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx1");
    }

    #[tokio::test]
    async fn find_combined_all_three_filters() {
        let store = MemorySLAPStorage::new();
        store
            .store_record("tx1", 0, "key_a", "https://a.com", "ls_foo")
            .await
            .unwrap();
        store
            .store_record("tx2", 0, "key_b", "https://a.com", "ls_foo")
            .await
            .unwrap();
        store
            .store_record("tx3", 0, "key_a", "https://b.com", "ls_foo")
            .await
            .unwrap();
        store
            .store_record("tx4", 0, "key_a", "https://a.com", "ls_bar")
            .await
            .unwrap();

        let results = store
            .find_record(&SLAPQuery {
                identity_key: Some("key_a".into()),
                domain: Some("https://a.com".into()),
                service: Some("ls_foo".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx1");
    }

    #[tokio::test]
    async fn find_with_no_filters_returns_all() {
        let store = MemorySLAPStorage::new();
        store
            .store_record("tx1", 0, "k1", "d1", "s1")
            .await
            .unwrap();
        store
            .store_record("tx2", 0, "k2", "d2", "s2")
            .await
            .unwrap();
        store
            .store_record("tx3", 0, "k3", "d3", "s3")
            .await
            .unwrap();

        let results = store.find_record(&SLAPQuery::default()).await.unwrap();
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn find_all_with_no_pagination_returns_all() {
        let store = MemorySLAPStorage::new();
        for i in 0..7 {
            store
                .store_record(&format!("tx{i}"), 0, "k", "d", "s")
                .await
                .unwrap();
        }
        let results = store.find_all(None, None, None).await.unwrap();
        assert_eq!(results.len(), 7);
    }

    #[tokio::test]
    async fn find_all_skip_beyond_total_returns_empty() {
        let store = MemorySLAPStorage::new();
        for i in 0..3 {
            store
                .store_record(&format!("tx{i}"), 0, "k", "d", "s")
                .await
                .unwrap();
        }
        let results = store.find_all(None, Some(100), None).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn find_all_limit_larger_than_total() {
        let store = MemorySLAPStorage::new();
        for i in 0..3 {
            store
                .store_record(&format!("tx{i}"), 0, "k", "d", "s")
                .await
                .unwrap();
        }
        let results = store.find_all(Some(100), None, None).await.unwrap();
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn sort_order_returns_correct_count() {
        let store = MemorySLAPStorage::new();
        for i in 0..3 {
            store
                .store_record(&format!("tx{i}"), 0, "k", "d", "s")
                .await
                .unwrap();
        }

        // Default sort (desc) returns all records
        let results_desc = store.find_all(None, None, None).await.unwrap();
        assert_eq!(results_desc.len(), 3);

        // Asc sort returns all records
        let results_asc = store
            .find_all(None, None, Some(SortOrder::Asc))
            .await
            .unwrap();
        assert_eq!(results_asc.len(), 3);

        // Both contain the same set of txids
        let mut desc_txids: Vec<String> = results_desc.iter().map(|r| r.txid.clone()).collect();
        let mut asc_txids: Vec<String> = results_asc.iter().map(|r| r.txid.clone()).collect();
        desc_txids.sort();
        asc_txids.sort();
        assert_eq!(desc_txids, asc_txids);
    }

    #[tokio::test]
    async fn preserves_output_index() {
        let store = MemorySLAPStorage::new();
        store.store_record("tx1", 7, "k", "d", "s").await.unwrap();

        let results = store.find_all(None, None, None).await.unwrap();
        assert_eq!(results[0].output_index, 7);
    }

    #[tokio::test]
    async fn multiple_outputs_same_txid() {
        let store = MemorySLAPStorage::new();
        store.store_record("tx1", 0, "k", "d", "s1").await.unwrap();
        store.store_record("tx1", 1, "k", "d", "s2").await.unwrap();
        store.store_record("tx1", 2, "k", "d", "s3").await.unwrap();

        assert_eq!(store.record_count(), 3);
        let results = store.find_all(None, None, None).await.unwrap();
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn query_serialization_roundtrip() {
        let query = SLAPQuery {
            find_all: Some(true),
            domain: Some("https://example.com".into()),
            service: Some("ls_test".into()),
            identity_key: Some("key123".into()),
            limit: Some(10),
            skip: Some(5),
            sort_order: Some(SortOrder::Asc),
        };
        let json = serde_json::to_value(&query).unwrap();
        let deserialized: SLAPQuery = serde_json::from_value(json).unwrap();
        assert_eq!(deserialized.domain.as_deref(), Some("https://example.com"));
        assert_eq!(deserialized.service.as_deref(), Some("ls_test"));
        assert_eq!(deserialized.limit, Some(10));
        assert_eq!(deserialized.skip, Some(5));
        assert_eq!(deserialized.sort_order, Some(SortOrder::Asc));
    }

    #[tokio::test]
    async fn find_record_with_pagination() {
        let store = MemorySLAPStorage::new();
        for i in 0..10 {
            store
                .store_record(&format!("tx{i}"), 0, "k", "d", "ls_test")
                .await
                .unwrap();
        }

        // Limit + skip via find_record (not find_all)
        let results = store
            .find_record(&SLAPQuery {
                service: Some("ls_test".into()),
                limit: Some(3),
                skip: Some(2),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn find_all_records_returns_domains() {
        let store = MemorySLAPStorage::new();
        store
            .store_record("tx1", 0, "k1", "https://a.com", "ls_foo")
            .await
            .unwrap();
        store
            .store_record("tx2", 1, "k2", "https://b.com", "ls_bar")
            .await
            .unwrap();

        let records = store.find_all_records().await.unwrap();
        assert_eq!(records.len(), 2);

        let domains: Vec<&str> = records.iter().map(|r| r.domain.as_str()).collect();
        assert!(domains.contains(&"https://a.com"));
        assert!(domains.contains(&"https://b.com"));

        let services: Vec<&str> = records.iter().map(|r| r.service.as_str()).collect();
        assert!(services.contains(&"ls_foo"));
        assert!(services.contains(&"ls_bar"));
    }

    #[tokio::test]
    async fn find_all_records_empty() {
        let store = MemorySLAPStorage::new();
        let records = store.find_all_records().await.unwrap();
        assert!(records.is_empty());
    }

    #[tokio::test]
    async fn discovery_record_serde() {
        let rec = SLAPDiscoveryRecord {
            txid: "abc123".to_string(),
            output_index: 0,
            identity_key: "02".repeat(33),
            domain: "https://example.com".to_string(),
            service: "ls_test".to_string(),
        };
        let json = serde_json::to_string(&rec).unwrap();
        let back: SLAPDiscoveryRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(back.txid, "abc123");
        assert_eq!(back.identity_key, "02".repeat(33));
        assert_eq!(back.domain, "https://example.com");
        assert_eq!(back.service, "ls_test");
    }
}
