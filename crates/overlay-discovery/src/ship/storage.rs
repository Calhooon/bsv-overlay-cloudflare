//! SHIP Storage trait — backend-agnostic storage for SHIP advertisement records.
//!
//! The concrete implementation (D1, SQLite, in-memory) is provided by the deployment crate.
//! Ported from `~/bsv/overlay-discovery-services/src/SHIP/SHIPStorage.ts`.

use async_trait::async_trait;
use overlay_engine::types::UTXOReference;
use serde::{Deserialize, Serialize};

/// A SHIP record with its associated domain — used by the Janitor to health-check hosts
/// AND by `CloudflareAdvertiser::find_all_advertisements` to distinguish our own
/// published advertisements from peer advertisements we've GASP-synced.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SHIPDiscoveryRecord {
    pub txid: String,
    #[serde(rename = "outputIndex")]
    pub output_index: u32,
    /// Publisher's identity public key (33-byte compressed, hex-encoded).
    /// Required so the advertiser can filter to records it published
    /// rather than peer records it's ingested.
    #[serde(rename = "identityKey")]
    pub identity_key: String,
    pub domain: String,
    pub topic: String,
}

/// Sort order for query results.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SortOrder {
    #[serde(rename = "asc")]
    Asc,
    #[serde(rename = "desc")]
    Desc,
}

/// Query parameters for finding SHIP records.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SHIPQuery {
    /// If true, return all records with pagination.
    pub find_all: Option<bool>,
    /// Filter by advertised domain.
    pub domain: Option<String>,
    /// Filter by topic names (matches any in the list).
    pub topics: Option<Vec<String>>,
    /// Filter by identity key.
    pub identity_key: Option<String>,
    /// Pagination limit.
    pub limit: Option<u32>,
    /// Pagination offset.
    pub skip: Option<u32>,
    /// Sort order by creation time.
    pub sort_order: Option<SortOrder>,
}

/// Backend-agnostic storage for SHIP advertisement records.
#[async_trait(?Send)]
pub trait SHIPStorage {
    /// Check if a duplicate record exists with the same identity, domain, and topic.
    async fn has_duplicate_record(
        &self,
        identity_key: &str,
        domain: &str,
        topic: &str,
    ) -> Result<bool, SHIPStorageError>;

    /// Store a new SHIP record.
    async fn store_record(
        &self,
        txid: &str,
        output_index: u32,
        identity_key: &str,
        domain: &str,
        topic: &str,
    ) -> Result<(), SHIPStorageError>;

    /// Delete a SHIP record by UTXO reference.
    async fn delete_record(&self, txid: &str, output_index: u32) -> Result<(), SHIPStorageError>;

    /// Find records matching a query.
    async fn find_record(&self, query: &SHIPQuery) -> Result<Vec<UTXOReference>, SHIPStorageError>;

    /// Find all records with optional pagination.
    async fn find_all(
        &self,
        limit: Option<u32>,
        skip: Option<u32>,
        sort_order: Option<SortOrder>,
    ) -> Result<Vec<UTXOReference>, SHIPStorageError>;

    /// Find all records with domain information (for Janitor health checks).
    ///
    /// Returns full discovery records including domain, topic, txid, and outputIndex.
    async fn find_all_records(&self) -> Result<Vec<SHIPDiscoveryRecord>, SHIPStorageError>;
}

/// SHIP storage errors.
#[derive(Debug, thiserror::Error)]
pub enum SHIPStorageError {
    #[error("database error: {0}")]
    Database(String),
    #[error("{0}")]
    Other(String),
}

// ============================================================================
// In-memory implementation (for tests)
// ============================================================================

/// A SHIP record stored in memory.
#[derive(Debug, Clone)]
struct SHIPRecord {
    txid: String,
    output_index: u32,
    identity_key: String,
    domain: String,
    topic: String,
    created_at: std::time::SystemTime,
}

/// In-memory SHIP storage for testing.
#[derive(Debug, Default)]
pub struct MemorySHIPStorage {
    records: std::sync::Mutex<Vec<SHIPRecord>>,
}

impl MemorySHIPStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_count(&self) -> usize {
        self.records.lock().unwrap().len()
    }
}

#[async_trait(?Send)]
impl SHIPStorage for MemorySHIPStorage {
    async fn has_duplicate_record(
        &self,
        identity_key: &str,
        domain: &str,
        topic: &str,
    ) -> Result<bool, SHIPStorageError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter()
            .any(|r| r.identity_key == identity_key && r.domain == domain && r.topic == topic))
    }

    async fn store_record(
        &self,
        txid: &str,
        output_index: u32,
        identity_key: &str,
        domain: &str,
        topic: &str,
    ) -> Result<(), SHIPStorageError> {
        self.records.lock().unwrap().push(SHIPRecord {
            txid: txid.into(),
            output_index,
            identity_key: identity_key.into(),
            domain: domain.into(),
            topic: topic.into(),
            created_at: std::time::SystemTime::now(),
        });
        Ok(())
    }

    async fn delete_record(&self, txid: &str, output_index: u32) -> Result<(), SHIPStorageError> {
        self.records
            .lock()
            .unwrap()
            .retain(|r| !(r.txid == txid && r.output_index == output_index));
        Ok(())
    }

    async fn find_record(&self, query: &SHIPQuery) -> Result<Vec<UTXOReference>, SHIPStorageError> {
        let records = self.records.lock().unwrap();
        let mut results: Vec<&SHIPRecord> = records
            .iter()
            .filter(|r| {
                if let Some(ref d) = query.domain {
                    if r.domain != *d {
                        return false;
                    }
                }
                if let Some(ref topics) = query.topics {
                    if !topics.contains(&r.topic) {
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

        // Sort
        match query.sort_order {
            Some(SortOrder::Asc) => results.sort_by_key(|r| r.created_at),
            // desc default: sort ASC then reverse; saves a closure allocation.
            _ => {
                results.sort_by_key(|r| r.created_at);
                results.reverse();
            }
        }

        // Pagination
        let skip = query.skip.unwrap_or(0) as usize;
        let results: Vec<UTXOReference> = results
            .into_iter()
            .skip(skip)
            .take(query.limit.map_or(usize::MAX, |l| l as usize))
            .map(|r| UTXOReference {
                txid: r.txid.clone(),
                output_index: r.output_index,
            })
            .collect();

        Ok(results)
    }

    async fn find_all(
        &self,
        limit: Option<u32>,
        skip: Option<u32>,
        sort_order: Option<SortOrder>,
    ) -> Result<Vec<UTXOReference>, SHIPStorageError> {
        self.find_record(&SHIPQuery {
            find_all: Some(true),
            limit,
            skip,
            sort_order,
            ..Default::default()
        })
        .await
    }

    async fn find_all_records(&self) -> Result<Vec<SHIPDiscoveryRecord>, SHIPStorageError> {
        let records = self.records.lock().unwrap();
        Ok(records
            .iter()
            .map(|r| SHIPDiscoveryRecord {
                txid: r.txid.clone(),
                output_index: r.output_index,
                identity_key: r.identity_key.clone(),
                domain: r.domain.clone(),
                topic: r.topic.clone(),
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

    #[tokio::test]
    async fn store_and_find_record() {
        let store = MemorySHIPStorage::new();
        store
            .store_record("tx1", 0, "key1", "https://a.com", "tm_test")
            .await
            .unwrap();
        store
            .store_record("tx2", 0, "key2", "https://b.com", "tm_other")
            .await
            .unwrap();

        let results = store
            .find_record(&SHIPQuery {
                topics: Some(vec!["tm_test".into()]),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx1");
    }

    #[tokio::test]
    async fn has_duplicate_record() {
        let store = MemorySHIPStorage::new();
        assert!(!store.has_duplicate_record("k", "d", "t").await.unwrap());
        store.store_record("tx1", 0, "k", "d", "t").await.unwrap();
        assert!(store.has_duplicate_record("k", "d", "t").await.unwrap());
    }

    #[tokio::test]
    async fn delete_record() {
        let store = MemorySHIPStorage::new();
        store.store_record("tx1", 0, "k", "d", "t").await.unwrap();
        assert_eq!(store.record_count(), 1);
        store.delete_record("tx1", 0).await.unwrap();
        assert_eq!(store.record_count(), 0);
    }

    #[tokio::test]
    async fn find_all_with_pagination() {
        let store = MemorySHIPStorage::new();
        for i in 0..10 {
            store
                .store_record(&format!("tx{i}"), 0, "k", "d", "t")
                .await
                .unwrap();
        }

        let results = store.find_all(Some(3), Some(2), None).await.unwrap();
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn find_by_domain() {
        let store = MemorySHIPStorage::new();
        store
            .store_record("tx1", 0, "k", "https://a.com", "t")
            .await
            .unwrap();
        store
            .store_record("tx2", 0, "k", "https://b.com", "t")
            .await
            .unwrap();

        let results = store
            .find_record(&SHIPQuery {
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
        let store = MemorySHIPStorage::new();
        store
            .store_record("tx1", 0, "key_a", "d", "t")
            .await
            .unwrap();
        store
            .store_record("tx2", 0, "key_b", "d", "t")
            .await
            .unwrap();

        let results = store
            .find_record(&SHIPQuery {
                identity_key: Some("key_b".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx2");
    }

    // ========================================================================
    // Additional storage tests
    // ========================================================================

    #[tokio::test]
    async fn record_count_starts_at_zero() {
        let store = MemorySHIPStorage::new();
        assert_eq!(store.record_count(), 0);
    }

    #[tokio::test]
    async fn record_count_tracks_insertions() {
        let store = MemorySHIPStorage::new();
        for i in 0..5 {
            store
                .store_record(&format!("tx{i}"), 0, "k", "d", "t")
                .await
                .unwrap();
        }
        assert_eq!(store.record_count(), 5);
    }

    #[tokio::test]
    async fn has_duplicate_requires_all_three_fields_to_match() {
        let store = MemorySHIPStorage::new();
        store
            .store_record("tx1", 0, "key1", "domain1", "topic1")
            .await
            .unwrap();

        // Same identity_key but different domain
        assert!(!store
            .has_duplicate_record("key1", "other_domain", "topic1")
            .await
            .unwrap());
        // Same identity_key but different topic
        assert!(!store
            .has_duplicate_record("key1", "domain1", "other_topic")
            .await
            .unwrap());
        // Same domain+topic but different identity_key
        assert!(!store
            .has_duplicate_record("key2", "domain1", "topic1")
            .await
            .unwrap());
        // All match
        assert!(store
            .has_duplicate_record("key1", "domain1", "topic1")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn delete_only_matching_utxo() {
        let store = MemorySHIPStorage::new();
        store.store_record("tx1", 0, "k", "d", "t").await.unwrap();
        store.store_record("tx1", 1, "k", "d2", "t2").await.unwrap();
        store.store_record("tx2", 0, "k", "d3", "t3").await.unwrap();

        store.delete_record("tx1", 0).await.unwrap();
        assert_eq!(store.record_count(), 2);

        // tx1:1 should still be findable
        let results = store.find_all(None, None, None).await.unwrap();
        let txids: Vec<(&str, u32)> = results
            .iter()
            .map(|r| (r.txid.as_str(), r.output_index))
            .collect();
        assert!(txids.contains(&("tx1", 1)));
        assert!(txids.contains(&("tx2", 0)));
    }

    #[tokio::test]
    async fn delete_nonexistent_record_is_ok() {
        let store = MemorySHIPStorage::new();
        store.delete_record("nonexistent", 0).await.unwrap();
        assert_eq!(store.record_count(), 0);
    }

    #[tokio::test]
    async fn find_by_multiple_topics() {
        let store = MemorySHIPStorage::new();
        store
            .store_record("tx1", 0, "k", "d", "tm_a")
            .await
            .unwrap();
        store
            .store_record("tx2", 0, "k", "d", "tm_b")
            .await
            .unwrap();
        store
            .store_record("tx3", 0, "k", "d", "tm_c")
            .await
            .unwrap();

        let results = store
            .find_record(&SHIPQuery {
                topics: Some(vec!["tm_a".into(), "tm_c".into()]),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(results.len(), 2);
        let txids: Vec<&str> = results.iter().map(|r| r.txid.as_str()).collect();
        assert!(txids.contains(&"tx1"));
        assert!(txids.contains(&"tx3"));
    }

    #[tokio::test]
    async fn find_combined_domain_and_topic() {
        let store = MemorySHIPStorage::new();
        store
            .store_record("tx1", 0, "k", "https://a.com", "tm_foo")
            .await
            .unwrap();
        store
            .store_record("tx2", 0, "k", "https://b.com", "tm_foo")
            .await
            .unwrap();
        store
            .store_record("tx3", 0, "k", "https://a.com", "tm_bar")
            .await
            .unwrap();

        let results = store
            .find_record(&SHIPQuery {
                domain: Some("https://a.com".into()),
                topics: Some(vec!["tm_foo".into()]),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx1");
    }

    #[tokio::test]
    async fn find_combined_all_three_filters() {
        let store = MemorySHIPStorage::new();
        store
            .store_record("tx1", 0, "key_a", "https://a.com", "tm_foo")
            .await
            .unwrap();
        store
            .store_record("tx2", 0, "key_b", "https://a.com", "tm_foo")
            .await
            .unwrap();
        store
            .store_record("tx3", 0, "key_a", "https://b.com", "tm_foo")
            .await
            .unwrap();
        store
            .store_record("tx4", 0, "key_a", "https://a.com", "tm_bar")
            .await
            .unwrap();

        let results = store
            .find_record(&SHIPQuery {
                identity_key: Some("key_a".into()),
                domain: Some("https://a.com".into()),
                topics: Some(vec!["tm_foo".into()]),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx1");
    }

    #[tokio::test]
    async fn find_with_no_filters_returns_all() {
        let store = MemorySHIPStorage::new();
        store
            .store_record("tx1", 0, "k1", "d1", "t1")
            .await
            .unwrap();
        store
            .store_record("tx2", 0, "k2", "d2", "t2")
            .await
            .unwrap();
        store
            .store_record("tx3", 0, "k3", "d3", "t3")
            .await
            .unwrap();

        let results = store.find_record(&SHIPQuery::default()).await.unwrap();
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn find_all_with_no_pagination_returns_all() {
        let store = MemorySHIPStorage::new();
        for i in 0..7 {
            store
                .store_record(&format!("tx{i}"), 0, "k", "d", "t")
                .await
                .unwrap();
        }
        let results = store.find_all(None, None, None).await.unwrap();
        assert_eq!(results.len(), 7);
    }

    #[tokio::test]
    async fn find_all_skip_beyond_total_returns_empty() {
        let store = MemorySHIPStorage::new();
        for i in 0..3 {
            store
                .store_record(&format!("tx{i}"), 0, "k", "d", "t")
                .await
                .unwrap();
        }
        let results = store.find_all(None, Some(100), None).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn find_all_limit_larger_than_total() {
        let store = MemorySHIPStorage::new();
        for i in 0..3 {
            store
                .store_record(&format!("tx{i}"), 0, "k", "d", "t")
                .await
                .unwrap();
        }
        let results = store.find_all(Some(100), None, None).await.unwrap();
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn sort_order_returns_correct_count() {
        let store = MemorySHIPStorage::new();
        for i in 0..3 {
            store
                .store_record(&format!("tx{i}"), 0, "k", "d", "t")
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
        let store = MemorySHIPStorage::new();
        store.store_record("tx1", 7, "k", "d", "t").await.unwrap();

        let results = store.find_all(None, None, None).await.unwrap();
        assert_eq!(results[0].output_index, 7);
    }

    #[tokio::test]
    async fn multiple_outputs_same_txid() {
        let store = MemorySHIPStorage::new();
        store.store_record("tx1", 0, "k", "d", "t1").await.unwrap();
        store.store_record("tx1", 1, "k", "d", "t2").await.unwrap();
        store.store_record("tx1", 2, "k", "d", "t3").await.unwrap();

        assert_eq!(store.record_count(), 3);
        let results = store.find_all(None, None, None).await.unwrap();
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn query_serialization_roundtrip() {
        let query = SHIPQuery {
            find_all: Some(true),
            domain: Some("https://example.com".into()),
            topics: Some(vec!["tm_a".into(), "tm_b".into()]),
            identity_key: Some("key123".into()),
            limit: Some(10),
            skip: Some(5),
            sort_order: Some(SortOrder::Asc),
        };
        let json = serde_json::to_value(&query).unwrap();
        let deserialized: SHIPQuery = serde_json::from_value(json).unwrap();
        assert_eq!(deserialized.domain.as_deref(), Some("https://example.com"));
        assert_eq!(deserialized.topics.as_ref().unwrap().len(), 2);
        assert_eq!(deserialized.limit, Some(10));
        assert_eq!(deserialized.skip, Some(5));
        assert_eq!(deserialized.sort_order, Some(SortOrder::Asc));
    }

    #[tokio::test]
    async fn sort_order_serialization() {
        let asc_json = serde_json::to_string(&SortOrder::Asc).unwrap();
        assert_eq!(asc_json, "\"asc\"");
        let desc_json = serde_json::to_string(&SortOrder::Desc).unwrap();
        assert_eq!(desc_json, "\"desc\"");

        let asc: SortOrder = serde_json::from_str("\"asc\"").unwrap();
        assert_eq!(asc, SortOrder::Asc);
        let desc: SortOrder = serde_json::from_str("\"desc\"").unwrap();
        assert_eq!(desc, SortOrder::Desc);
    }

    #[tokio::test]
    async fn find_all_records_returns_domains() {
        let store = MemorySHIPStorage::new();
        store
            .store_record("tx1", 0, "k1", "https://a.com", "tm_foo")
            .await
            .unwrap();
        store
            .store_record("tx2", 1, "k2", "https://b.com", "tm_bar")
            .await
            .unwrap();

        let records = store.find_all_records().await.unwrap();
        assert_eq!(records.len(), 2);

        let domains: Vec<&str> = records.iter().map(|r| r.domain.as_str()).collect();
        assert!(domains.contains(&"https://a.com"));
        assert!(domains.contains(&"https://b.com"));

        let topics: Vec<&str> = records.iter().map(|r| r.topic.as_str()).collect();
        assert!(topics.contains(&"tm_foo"));
        assert!(topics.contains(&"tm_bar"));
    }

    #[tokio::test]
    async fn find_all_records_empty() {
        let store = MemorySHIPStorage::new();
        let records = store.find_all_records().await.unwrap();
        assert!(records.is_empty());
    }

    #[tokio::test]
    async fn discovery_record_serde() {
        let rec = SHIPDiscoveryRecord {
            txid: "abc123".to_string(),
            output_index: 0,
            identity_key: "02".repeat(33),
            domain: "https://example.com".to_string(),
            topic: "tm_test".to_string(),
        };
        let json = serde_json::to_string(&rec).unwrap();
        let back: SHIPDiscoveryRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(back.txid, "abc123");
        assert_eq!(back.identity_key, "02".repeat(33));
        assert_eq!(back.domain, "https://example.com");
        assert_eq!(back.topic, "tm_test");
    }
}
