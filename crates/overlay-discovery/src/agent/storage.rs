//! Agent Storage trait — backend-agnostic storage for Agent Registry records.
//!
//! The concrete implementation (D1, SQLite, in-memory) is provided by the deployment crate.
//! Follows the same pattern as SHIPStorage / SLAPStorage.

use async_trait::async_trait;
use overlay_engine::types::UTXOReference;
use serde::{Deserialize, Serialize};

/// An Agent discovery record — used by the Janitor to health-check agents.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentDiscoveryRecord {
    pub txid: String,
    #[serde(rename = "outputIndex")]
    pub output_index: u32,
    pub name: String,
}

/// A stored Agent record with all fields from the PushDrop output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentRecord {
    pub txid: String,
    #[serde(rename = "outputIndex")]
    pub output_index: u32,
    pub identity_key: String,
    pub certifier_key: String,
    pub name: String,
    pub capabilities: Vec<String>,
    pub created_at: String,
}

/// Backend-agnostic storage for Agent Registry records.
#[async_trait(?Send)]
pub trait AgentStorage {
    /// Check if a duplicate record exists with the same identity key and name.
    async fn has_duplicate_record(
        &self,
        identity_key: &str,
        name: &str,
    ) -> Result<bool, AgentStorageError>;

    /// Store a new Agent record.
    async fn store_record(&self, record: &AgentRecord) -> Result<(), AgentStorageError>;

    /// Delete an Agent record by UTXO reference.
    async fn delete_record(&self, txid: &str, output_index: u32) -> Result<(), AgentStorageError>;

    /// Find records by capability.
    async fn find_by_capability(
        &self,
        capability: &str,
        limit: Option<u32>,
        skip: Option<u32>,
    ) -> Result<Vec<UTXOReference>, AgentStorageError>;

    /// Find records by identity key.
    async fn find_by_identity_key(
        &self,
        identity_key: &str,
    ) -> Result<Vec<UTXOReference>, AgentStorageError>;

    /// Find records by certifier key.
    async fn find_by_certifier(
        &self,
        certifier_key: &str,
        limit: Option<u32>,
        skip: Option<u32>,
    ) -> Result<Vec<UTXOReference>, AgentStorageError>;

    /// Find records by name.
    async fn find_by_name(&self, name: &str) -> Result<Vec<UTXOReference>, AgentStorageError>;

    /// Find all records with optional pagination.
    async fn find_all(
        &self,
        limit: Option<u32>,
        skip: Option<u32>,
    ) -> Result<Vec<UTXOReference>, AgentStorageError>;

    /// Find all records with name information (for Janitor health checks).
    async fn find_all_records(&self) -> Result<Vec<AgentDiscoveryRecord>, AgentStorageError>;

    /// Find existing rows for a given (identity_key, name) tuple.
    ///
    /// Used by `output_admitted_by_topic` to evict stale rows when an agent
    /// re-registers with a changed certifier_key or capabilities. Returns
    /// every UTXO matching the tuple — there should normally be at most one
    /// after the eviction loop, but multiple are possible if the spend
    /// processing was incomplete or the table was migrated from older code
    /// that allowed duplicates. EPIC #329 Phase 3.
    ///
    /// Default impl returns an empty vec for backward compat with custom
    /// storage backends; the in-memory and D1 implementations override.
    async fn find_existing_by_identity_and_name(
        &self,
        _identity_key: &str,
        _name: &str,
    ) -> Result<Vec<UTXOReference>, AgentStorageError> {
        Ok(Vec::new())
    }
}

/// Agent storage errors.
#[derive(Debug, thiserror::Error)]
pub enum AgentStorageError {
    #[error("database error: {0}")]
    Database(String),
    #[error("{0}")]
    Other(String),
}

// ============================================================================
// In-memory implementation (for tests)
// ============================================================================

/// An Agent record stored in memory.
#[derive(Debug, Clone)]
struct MemoryAgentRecord {
    txid: String,
    output_index: u32,
    identity_key: String,
    certifier_key: String,
    name: String,
    capabilities: Vec<String>,
    #[allow(dead_code)] // stored for future sort-order support (mirrors SHIP/SLAP)
    created_at: std::time::SystemTime,
}

/// In-memory Agent storage for testing.
#[derive(Debug, Default)]
pub struct MemoryAgentStorage {
    records: std::sync::Mutex<Vec<MemoryAgentRecord>>,
}

impl MemoryAgentStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_count(&self) -> usize {
        self.records.lock().unwrap().len()
    }
}

#[async_trait(?Send)]
impl AgentStorage for MemoryAgentStorage {
    async fn has_duplicate_record(
        &self,
        identity_key: &str,
        name: &str,
    ) -> Result<bool, AgentStorageError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter()
            .any(|r| r.identity_key == identity_key && r.name == name))
    }

    async fn store_record(&self, record: &AgentRecord) -> Result<(), AgentStorageError> {
        self.records.lock().unwrap().push(MemoryAgentRecord {
            txid: record.txid.clone(),
            output_index: record.output_index,
            identity_key: record.identity_key.clone(),
            certifier_key: record.certifier_key.clone(),
            name: record.name.clone(),
            capabilities: record.capabilities.clone(),
            created_at: std::time::SystemTime::now(),
        });
        Ok(())
    }

    async fn delete_record(&self, txid: &str, output_index: u32) -> Result<(), AgentStorageError> {
        self.records
            .lock()
            .unwrap()
            .retain(|r| !(r.txid == txid && r.output_index == output_index));
        Ok(())
    }

    async fn find_by_capability(
        &self,
        capability: &str,
        limit: Option<u32>,
        skip: Option<u32>,
    ) -> Result<Vec<UTXOReference>, AgentStorageError> {
        let records = self.records.lock().unwrap();
        let skip = skip.unwrap_or(0) as usize;
        Ok(records
            .iter()
            .filter(|r| r.capabilities.iter().any(|c| c == capability))
            .skip(skip)
            .take(limit.map_or(usize::MAX, |l| l as usize))
            .map(|r| UTXOReference {
                txid: r.txid.clone(),
                output_index: r.output_index,
            })
            .collect())
    }

    async fn find_by_identity_key(
        &self,
        identity_key: &str,
    ) -> Result<Vec<UTXOReference>, AgentStorageError> {
        let records = self.records.lock().unwrap();
        Ok(records
            .iter()
            .filter(|r| r.identity_key == identity_key)
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
    ) -> Result<Vec<UTXOReference>, AgentStorageError> {
        let records = self.records.lock().unwrap();
        let skip = skip.unwrap_or(0) as usize;
        Ok(records
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

    async fn find_by_name(&self, name: &str) -> Result<Vec<UTXOReference>, AgentStorageError> {
        let records = self.records.lock().unwrap();
        Ok(records
            .iter()
            .filter(|r| r.name == name)
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
    ) -> Result<Vec<UTXOReference>, AgentStorageError> {
        let records = self.records.lock().unwrap();
        let skip = skip.unwrap_or(0) as usize;
        Ok(records
            .iter()
            .skip(skip)
            .take(limit.map_or(usize::MAX, |l| l as usize))
            .map(|r| UTXOReference {
                txid: r.txid.clone(),
                output_index: r.output_index,
            })
            .collect())
    }

    async fn find_all_records(&self) -> Result<Vec<AgentDiscoveryRecord>, AgentStorageError> {
        let records = self.records.lock().unwrap();
        Ok(records
            .iter()
            .map(|r| AgentDiscoveryRecord {
                txid: r.txid.clone(),
                output_index: r.output_index,
                name: r.name.clone(),
            })
            .collect())
    }

    async fn find_existing_by_identity_and_name(
        &self,
        identity_key: &str,
        name: &str,
    ) -> Result<Vec<UTXOReference>, AgentStorageError> {
        let records = self.records.lock().unwrap();
        Ok(records
            .iter()
            .filter(|r| r.identity_key == identity_key && r.name == name)
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

    fn make_record(txid: &str, output_index: u32) -> AgentRecord {
        AgentRecord {
            txid: txid.into(),
            output_index,
            identity_key: "id_key_1".into(),
            certifier_key: "cert_key_1".into(),
            name: "test-agent".into(),
            capabilities: vec!["image-generation".into(), "upscaling".into()],
            created_at: String::new(),
        }
    }

    fn make_record_full(
        txid: &str,
        output_index: u32,
        identity_key: &str,
        certifier_key: &str,
        name: &str,
        capabilities: Vec<&str>,
    ) -> AgentRecord {
        AgentRecord {
            txid: txid.into(),
            output_index,
            identity_key: identity_key.into(),
            certifier_key: certifier_key.into(),
            name: name.into(),
            capabilities: capabilities.into_iter().map(String::from).collect(),
            created_at: String::new(),
        }
    }

    #[tokio::test]
    async fn store_and_find_by_capability() {
        let store = MemoryAgentStorage::new();
        store.store_record(&make_record("tx1", 0)).await.unwrap();
        store
            .store_record(&make_record_full(
                "tx2",
                0,
                "k2",
                "c2",
                "https://b.com",
                vec!["text-analysis"],
            ))
            .await
            .unwrap();

        let results = store
            .find_by_capability("image-generation", None, None)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx1");

        let results = store
            .find_by_capability("upscaling", None, None)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx1");

        let results = store
            .find_by_capability("text-analysis", None, None)
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx2");
    }

    #[tokio::test]
    async fn store_and_find_by_identity_key() {
        let store = MemoryAgentStorage::new();
        store
            .store_record(&make_record_full(
                "tx1",
                0,
                "key_a",
                "c1",
                "https://a.com",
                vec!["cap"],
            ))
            .await
            .unwrap();
        store
            .store_record(&make_record_full(
                "tx2",
                0,
                "key_b",
                "c1",
                "https://b.com",
                vec!["cap"],
            ))
            .await
            .unwrap();

        let results = store.find_by_identity_key("key_b").await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx2");
    }

    #[tokio::test]
    async fn store_and_find_by_certifier() {
        let store = MemoryAgentStorage::new();
        store
            .store_record(&make_record_full(
                "tx1",
                0,
                "k1",
                "cert_a",
                "https://a.com",
                vec!["cap"],
            ))
            .await
            .unwrap();
        store
            .store_record(&make_record_full(
                "tx2",
                0,
                "k2",
                "cert_b",
                "https://b.com",
                vec!["cap"],
            ))
            .await
            .unwrap();
        store
            .store_record(&make_record_full(
                "tx3",
                0,
                "k3",
                "cert_a",
                "https://c.com",
                vec!["cap"],
            ))
            .await
            .unwrap();

        let results = store.find_by_certifier("cert_a", None, None).await.unwrap();
        assert_eq!(results.len(), 2);
        let txids: Vec<&str> = results.iter().map(|r| r.txid.as_str()).collect();
        assert!(txids.contains(&"tx1"));
        assert!(txids.contains(&"tx3"));
    }

    #[tokio::test]
    async fn store_and_find_by_name() {
        let store = MemoryAgentStorage::new();
        store
            .store_record(&make_record_full(
                "tx1",
                0,
                "k1",
                "c1",
                "https://a.com",
                vec!["cap"],
            ))
            .await
            .unwrap();
        store
            .store_record(&make_record_full(
                "tx2",
                0,
                "k2",
                "c1",
                "https://b.com",
                vec!["cap"],
            ))
            .await
            .unwrap();

        let results = store.find_by_name("https://a.com").await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx1");
    }

    #[tokio::test]
    async fn find_all_with_pagination() {
        let store = MemoryAgentStorage::new();
        for i in 0..10 {
            store
                .store_record(&make_record_full(
                    &format!("tx{i}"),
                    0,
                    "k",
                    "c",
                    "https://a.com",
                    vec!["cap"],
                ))
                .await
                .unwrap();
        }

        let results = store.find_all(Some(3), Some(2)).await.unwrap();
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn find_all_no_pagination_returns_all() {
        let store = MemoryAgentStorage::new();
        for i in 0..7 {
            store
                .store_record(&make_record_full(
                    &format!("tx{i}"),
                    0,
                    "k",
                    "c",
                    "https://a.com",
                    vec!["cap"],
                ))
                .await
                .unwrap();
        }
        let results = store.find_all(None, None).await.unwrap();
        assert_eq!(results.len(), 7);
    }

    #[tokio::test]
    async fn find_all_skip_beyond_total_returns_empty() {
        let store = MemoryAgentStorage::new();
        for i in 0..3 {
            store
                .store_record(&make_record_full(
                    &format!("tx{i}"),
                    0,
                    "k",
                    "c",
                    "https://a.com",
                    vec!["cap"],
                ))
                .await
                .unwrap();
        }
        let results = store.find_all(None, Some(100)).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn has_duplicate_record() {
        let store = MemoryAgentStorage::new();
        assert!(!store
            .has_duplicate_record("key1", "https://a.com")
            .await
            .unwrap());

        store
            .store_record(&make_record_full(
                "tx1",
                0,
                "key1",
                "c1",
                "https://a.com",
                vec!["cap"],
            ))
            .await
            .unwrap();

        assert!(store
            .has_duplicate_record("key1", "https://a.com")
            .await
            .unwrap());
        // Different identity key
        assert!(!store
            .has_duplicate_record("key2", "https://a.com")
            .await
            .unwrap());
        // Different name
        assert!(!store
            .has_duplicate_record("key1", "https://b.com")
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn delete_record() {
        let store = MemoryAgentStorage::new();
        store
            .store_record(&make_record_full(
                "tx1",
                0,
                "k",
                "c",
                "https://a.com",
                vec!["cap"],
            ))
            .await
            .unwrap();
        assert_eq!(store.record_count(), 1);
        store.delete_record("tx1", 0).await.unwrap();
        assert_eq!(store.record_count(), 0);
    }

    #[tokio::test]
    async fn delete_only_matching_utxo() {
        let store = MemoryAgentStorage::new();
        store
            .store_record(&make_record_full(
                "tx1",
                0,
                "k",
                "c",
                "https://a.com",
                vec!["cap"],
            ))
            .await
            .unwrap();
        store
            .store_record(&make_record_full(
                "tx1",
                1,
                "k",
                "c",
                "https://b.com",
                vec!["cap"],
            ))
            .await
            .unwrap();
        store
            .store_record(&make_record_full(
                "tx2",
                0,
                "k",
                "c",
                "https://c.com",
                vec!["cap"],
            ))
            .await
            .unwrap();

        store.delete_record("tx1", 0).await.unwrap();
        assert_eq!(store.record_count(), 2);

        let results = store.find_all(None, None).await.unwrap();
        let utxos: Vec<(&str, u32)> = results
            .iter()
            .map(|r| (r.txid.as_str(), r.output_index))
            .collect();
        assert!(utxos.contains(&("tx1", 1)));
        assert!(utxos.contains(&("tx2", 0)));
    }

    #[tokio::test]
    async fn delete_nonexistent_record_is_ok() {
        let store = MemoryAgentStorage::new();
        store.delete_record("nonexistent", 0).await.unwrap();
        assert_eq!(store.record_count(), 0);
    }

    #[tokio::test]
    async fn find_all_records_for_janitor() {
        let store = MemoryAgentStorage::new();
        store
            .store_record(&make_record_full(
                "tx1",
                0,
                "k1",
                "c1",
                "https://a.com",
                vec!["cap_a"],
            ))
            .await
            .unwrap();
        store
            .store_record(&make_record_full(
                "tx2",
                1,
                "k2",
                "c2",
                "https://b.com",
                vec!["cap_b"],
            ))
            .await
            .unwrap();

        let records = store.find_all_records().await.unwrap();
        assert_eq!(records.len(), 2);

        let names: Vec<&str> = records.iter().map(|r| r.name.as_str()).collect();
        assert!(names.contains(&"https://a.com"));
        assert!(names.contains(&"https://b.com"));
    }

    #[tokio::test]
    async fn find_all_records_empty() {
        let store = MemoryAgentStorage::new();
        let records = store.find_all_records().await.unwrap();
        assert!(records.is_empty());
    }

    #[tokio::test]
    async fn multiple_capabilities_per_agent() {
        let store = MemoryAgentStorage::new();
        store
            .store_record(&make_record_full(
                "tx1",
                0,
                "k1",
                "c1",
                "https://a.com",
                vec!["image-generation", "upscaling", "text-analysis"],
            ))
            .await
            .unwrap();

        // Should be findable by any of its capabilities
        for cap in &["image-generation", "upscaling", "text-analysis"] {
            let results = store.find_by_capability(cap, None, None).await.unwrap();
            assert_eq!(results.len(), 1, "should find agent by capability: {cap}");
            assert_eq!(results[0].txid, "tx1");
        }

        // Should NOT be found by a capability it doesn't have
        let results = store
            .find_by_capability("video-generation", None, None)
            .await
            .unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn find_by_capability_with_pagination() {
        let store = MemoryAgentStorage::new();
        for i in 0..10 {
            store
                .store_record(&make_record_full(
                    &format!("tx{i}"),
                    0,
                    &format!("k{i}"),
                    "c",
                    "https://a.com",
                    vec!["shared-cap"],
                ))
                .await
                .unwrap();
        }

        let results = store
            .find_by_capability("shared-cap", Some(3), Some(2))
            .await
            .unwrap();
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn find_by_certifier_with_pagination() {
        let store = MemoryAgentStorage::new();
        for i in 0..10 {
            store
                .store_record(&make_record_full(
                    &format!("tx{i}"),
                    0,
                    &format!("k{i}"),
                    "shared_cert",
                    &format!("https://a{i}.com"),
                    vec!["cap"],
                ))
                .await
                .unwrap();
        }

        let results = store
            .find_by_certifier("shared_cert", Some(3), Some(2))
            .await
            .unwrap();
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn record_count_starts_at_zero() {
        let store = MemoryAgentStorage::new();
        assert_eq!(store.record_count(), 0);
    }

    #[tokio::test]
    async fn preserves_output_index() {
        let store = MemoryAgentStorage::new();
        store
            .store_record(&make_record_full(
                "tx1",
                7,
                "k",
                "c",
                "https://a.com",
                vec!["cap"],
            ))
            .await
            .unwrap();

        let results = store.find_all(None, None).await.unwrap();
        assert_eq!(results[0].output_index, 7);
    }

    #[tokio::test]
    async fn discovery_record_serde() {
        let rec = AgentDiscoveryRecord {
            txid: "abc123".to_string(),
            output_index: 0,
            name: "test-agent".to_string(),
        };
        let json = serde_json::to_string(&rec).unwrap();
        let back: AgentDiscoveryRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(back.txid, "abc123");
        assert_eq!(back.name, "test-agent");
    }

    #[tokio::test]
    async fn agent_record_serde() {
        let rec = AgentRecord {
            txid: "abc123".to_string(),
            output_index: 0,
            identity_key: "id_key".to_string(),
            certifier_key: "cert_key".to_string(),
            name: "test-agent".to_string(),
            capabilities: vec!["image-generation".to_string()],
            created_at: "2025-01-01T00:00:00Z".to_string(),
        };
        let json = serde_json::to_string(&rec).unwrap();
        let back: AgentRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(back.txid, "abc123");
        assert_eq!(back.identity_key, "id_key");
        assert_eq!(back.capabilities, vec!["image-generation"]);
    }
}
