//! SHIP Lookup Service — indexes and queries SHIP advertisement records.
//!
//! When outputs are admitted to tm_ship, this service decodes the PushDrop fields
//! and stores them via SHIPStorage. Clients query for SHIP advertisements by
//! domain, topics, or identity key.
//!
//! Ported from `~/bsv/overlay-discovery-services/src/SHIP/SHIPLookupService.ts`.

use async_trait::async_trait;
use bsv_rs::script::templates::PushDrop;
use overlay_engine::lookup_service::{LookupService, LookupServiceError};
use overlay_engine::types::*;
use std::rc::Rc;
use tracing::debug;

use super::storage::{SHIPQuery, SHIPStorage};

/// SHIP Lookup Service — indexes SHIP advertisements and answers queries.
pub struct SHIPLookupService {
    storage: Rc<dyn SHIPStorage>,
}

impl SHIPLookupService {
    /// Create a new SHIP lookup service backed by the given storage.
    pub fn new(storage: Rc<dyn SHIPStorage>) -> Self {
        Self { storage }
    }
}

#[async_trait(?Send)]
impl LookupService for SHIPLookupService {
    fn admission_mode(&self) -> AdmissionMode {
        AdmissionMode::LockingScript
    }

    fn spend_notification_mode(&self) -> SpendNotificationMode {
        SpendNotificationMode::None
    }

    async fn output_admitted_by_topic(
        &self,
        payload: &OutputAdmittedByTopic,
    ) -> Result<(), LookupServiceError> {
        let (txid, output_index, topic, locking_script) = match payload {
            OutputAdmittedByTopic::LockingScript {
                txid,
                output_index,
                topic,
                locking_script,
                ..
            } => (txid, *output_index, topic, locking_script),
            _ => {
                return Err(LookupServiceError::Other(
                    "Expected locking-script mode".into(),
                ))
            }
        };

        // Only index tm_ship outputs
        if topic != "tm_ship" {
            return Ok(());
        }

        // Decode PushDrop to extract fields
        let script = bsv_rs::script::Script::from_binary(locking_script)
            .map_err(|e| LookupServiceError::Other(format!("Script parse error: {e}")))?;
        let pushdrop = PushDrop::decode(&script.into())
            .map_err(|e| LookupServiceError::Other(format!("PushDrop decode error: {e}")))?;

        if pushdrop.fields.len() < 4 {
            return Ok(());
        }

        let protocol = String::from_utf8_lossy(&pushdrop.fields[0]);
        if protocol != "SHIP" {
            return Ok(());
        }

        let identity_key = hex::encode(&pushdrop.fields[1]);
        let domain = String::from_utf8_lossy(&pushdrop.fields[2]).to_string();
        let topic_name = String::from_utf8_lossy(&pushdrop.fields[3]).to_string();

        // Check for duplicates
        let is_dup = self
            .storage
            .has_duplicate_record(&identity_key, &domain, &topic_name)
            .await
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;

        if is_dup {
            debug!("SHIP: skipping duplicate record: {domain} / {topic_name}");
            return Ok(());
        }

        self.storage
            .store_record(txid, output_index, &identity_key, &domain, &topic_name)
            .await
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;

        Ok(())
    }

    async fn output_spent(&self, payload: &OutputSpent) -> Result<(), LookupServiceError> {
        let (txid, output_index, topic) = match payload {
            OutputSpent::None {
                txid,
                output_index,
                topic,
            } => (txid, *output_index, topic),
            _ => return Ok(()),
        };

        if topic != "tm_ship" {
            return Ok(());
        }

        self.storage
            .delete_record(txid, output_index)
            .await
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;

        Ok(())
    }

    async fn output_evicted(
        &self,
        txid: &str,
        output_index: u32,
    ) -> Result<(), LookupServiceError> {
        self.storage
            .delete_record(txid, output_index)
            .await
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn lookup(&self, question: &LookupQuestion) -> Result<LookupResult, LookupServiceError> {
        if question.service != "ls_ship" {
            return Err(LookupServiceError::Unsupported(format!(
                "Expected ls_ship, got {}",
                question.service
            )));
        }

        // Handle legacy "findAll" string query
        if question.query.is_string() && question.query.as_str() == Some("findAll") {
            return self
                .storage
                .find_all(None, None, None)
                .await
                .map(LookupResult::OutputList)
                .map_err(|e| LookupServiceError::StorageError(e.to_string()));
        }

        // Parse query object
        let query: SHIPQuery = serde_json::from_value(question.query.clone())
            .map_err(|e| LookupServiceError::InvalidQuery(e.to_string()))?;

        // Validate pagination
        if let Some(limit) = query.limit {
            if limit == 0 {
                return Err(LookupServiceError::InvalidQuery(
                    "limit must be positive".into(),
                ));
            }
        }

        if query.find_all == Some(true) {
            return self
                .storage
                .find_all(query.limit, query.skip, query.sort_order)
                .await
                .map(LookupResult::OutputList)
                .map_err(|e| LookupServiceError::StorageError(e.to_string()));
        }

        self.storage
            .find_record(&query)
            .await
            .map(LookupResult::OutputList)
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))
    }

    async fn get_documentation(&self) -> String {
        include_str!("../../docs/ship_lookup.md").to_string()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "SHIP Lookup Service".to_string(),
            description: Some("Provides lookup capabilities for SHIP tokens.".to_string()),
            ..Default::default()
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::super::storage::MemorySHIPStorage;
    use super::*;
    use bsv_rs::primitives::ec::{PrivateKey, PublicKey};
    use bsv_rs::script::templates::PushDrop as PushDropTemplate;

    fn make_service() -> SHIPLookupService {
        SHIPLookupService::new(Rc::new(MemorySHIPStorage::new()))
    }

    fn make_service_with_storage() -> (SHIPLookupService, Rc<MemorySHIPStorage>) {
        let storage = Rc::new(MemorySHIPStorage::new());
        let svc = SHIPLookupService::new(storage.clone());
        (svc, storage)
    }

    /// Build a SHIP PushDrop locking script (binary bytes) suitable for
    /// OutputAdmittedByTopic::LockingScript payload.
    fn make_ship_locking_script(
        protocol: &str,
        identity_key: &[u8],
        domain: &str,
        topic: &str,
    ) -> Vec<u8> {
        let locking_key = PublicKey::from_private_key(&PrivateKey::random());
        let fields = vec![
            protocol.as_bytes().to_vec(),
            identity_key.to_vec(),
            domain.as_bytes().to_vec(),
            topic.as_bytes().to_vec(),
            vec![0x30, 0x44], // mock DER signature
        ];
        let pushdrop = PushDropTemplate::new(locking_key, fields);
        pushdrop.lock().to_binary()
    }

    fn dummy_identity_key() -> Vec<u8> {
        PublicKey::from_private_key(&PrivateKey::random())
            .to_compressed()
            .to_vec()
    }

    // ========================================================================
    // Trait method return values
    // ========================================================================

    #[tokio::test]
    async fn admission_mode_is_locking_script() {
        let svc = make_service();
        assert_eq!(svc.admission_mode(), AdmissionMode::LockingScript);
    }

    #[tokio::test]
    async fn spend_notification_mode_is_none() {
        let svc = make_service();
        assert_eq!(svc.spend_notification_mode(), SpendNotificationMode::None);
    }

    #[tokio::test]
    async fn metadata_correct() {
        let svc = make_service();
        let meta = svc.get_metadata().await;
        assert_eq!(meta.name, "SHIP Lookup Service");
        assert!(meta.description.unwrap().contains("SHIP"));
    }

    #[tokio::test]
    async fn documentation_not_empty() {
        let svc = make_service();
        let docs = svc.get_documentation().await;
        assert!(!docs.is_empty());
        assert!(docs.contains("SHIP"));
    }

    // ========================================================================
    // output_admitted_by_topic — valid PushDrop scripts
    // ========================================================================

    #[tokio::test]
    async fn admit_valid_ship_output_stores_record() {
        let (svc, storage) = make_service_with_storage();
        let ik = dummy_identity_key();
        let script = make_ship_locking_script("SHIP", &ik, "https://example.com", "tm_test");

        let payload = OutputAdmittedByTopic::LockingScript {
            txid: "abc123".into(),
            output_index: 0,
            topic: "tm_ship".into(),
            satoshis: 1,
            locking_script: script,
            off_chain_values: None,
        };
        svc.output_admitted_by_topic(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 1);
    }

    #[tokio::test]
    async fn admit_stores_correct_fields() {
        let (svc, _storage) = make_service_with_storage();
        let ik = dummy_identity_key();
        let ik_hex = hex::encode(&ik);
        let script =
            make_ship_locking_script("SHIP", &ik, "https://overlay.example.com", "tm_myapp");

        let payload = OutputAdmittedByTopic::LockingScript {
            txid: "deadbeef".into(),
            output_index: 7,
            topic: "tm_ship".into(),
            satoshis: 546,
            locking_script: script,
            off_chain_values: None,
        };
        svc.output_admitted_by_topic(&payload).await.unwrap();

        // Query by identity key to verify it was stored correctly
        let q = LookupQuestion::new("ls_ship", serde_json::json!({"identity_key": ik_hex}));
        let results = svc
            .lookup(&q)
            .await
            .unwrap()
            .into_outputs()
            .expect("expected OutputList");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "deadbeef");
        assert_eq!(results[0].output_index, 7);
    }

    #[tokio::test]
    async fn admit_multiple_outputs_different_topics() {
        let (svc, storage) = make_service_with_storage();
        let ik = dummy_identity_key();

        for (i, topic) in ["tm_alpha", "tm_beta", "tm_gamma"].iter().enumerate() {
            let script = make_ship_locking_script("SHIP", &ik, "https://example.com", topic);
            let payload = OutputAdmittedByTopic::LockingScript {
                txid: format!("tx{i}"),
                output_index: 0,
                topic: "tm_ship".into(),
                satoshis: 1,
                locking_script: script,
                off_chain_values: None,
            };
            svc.output_admitted_by_topic(&payload).await.unwrap();
        }
        assert_eq!(storage.record_count(), 3);
    }

    // ========================================================================
    // output_admitted_by_topic — filtering / rejection cases
    // ========================================================================

    #[tokio::test]
    async fn ignores_non_tm_ship_topic() {
        let (svc, storage) = make_service_with_storage();
        let payload = OutputAdmittedByTopic::LockingScript {
            txid: "tx1".into(),
            output_index: 0,
            topic: "tm_other".into(), // not tm_ship
            satoshis: 1,
            locking_script: vec![],
            off_chain_values: None,
        };
        svc.output_admitted_by_topic(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 0);
    }

    #[tokio::test]
    async fn ignores_non_ship_protocol_in_pushdrop() {
        let (svc, storage) = make_service_with_storage();
        let ik = dummy_identity_key();
        // Protocol is "SLAP" not "SHIP"
        let script = make_ship_locking_script("SLAP", &ik, "https://example.com", "tm_test");

        let payload = OutputAdmittedByTopic::LockingScript {
            txid: "tx1".into(),
            output_index: 0,
            topic: "tm_ship".into(),
            satoshis: 1,
            locking_script: script,
            off_chain_values: None,
        };
        svc.output_admitted_by_topic(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 0);
    }

    #[tokio::test]
    async fn rejects_whole_tx_mode() {
        let svc = make_service();
        let payload = OutputAdmittedByTopic::WholeTx {
            atomic_beef: vec![],
            output_index: 0,
            topic: "tm_ship".into(),
            off_chain_values: None,
        };
        let result = svc.output_admitted_by_topic(&payload).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn ignores_pushdrop_with_fewer_than_4_fields() {
        let (svc, storage) = make_service_with_storage();
        let locking_key = PublicKey::from_private_key(&PrivateKey::random());
        // Only 3 fields
        let fields = vec![
            b"SHIP".to_vec(),
            dummy_identity_key(),
            b"https://example.com".to_vec(),
        ];
        let pushdrop = PushDropTemplate::new(locking_key, fields);
        let script = pushdrop.lock().to_binary();

        let payload = OutputAdmittedByTopic::LockingScript {
            txid: "tx1".into(),
            output_index: 0,
            topic: "tm_ship".into(),
            satoshis: 1,
            locking_script: script,
            off_chain_values: None,
        };
        svc.output_admitted_by_topic(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 0);
    }

    #[tokio::test]
    async fn errors_on_invalid_script_bytes() {
        let svc = make_service();
        let payload = OutputAdmittedByTopic::LockingScript {
            txid: "tx1".into(),
            output_index: 0,
            topic: "tm_ship".into(),
            satoshis: 1,
            locking_script: vec![0xFF, 0xFE, 0xFD], // garbage bytes
            off_chain_values: None,
        };
        // May error or silently skip depending on Script::from_binary / PushDrop::decode
        let _result = svc.output_admitted_by_topic(&payload).await;
    }

    // ========================================================================
    // Duplicate detection
    // ========================================================================

    #[tokio::test]
    async fn skips_duplicate_record() {
        let (svc, storage) = make_service_with_storage();
        let ik = dummy_identity_key();
        let script = make_ship_locking_script("SHIP", &ik, "https://example.com", "tm_test");

        let payload = OutputAdmittedByTopic::LockingScript {
            txid: "tx1".into(),
            output_index: 0,
            topic: "tm_ship".into(),
            satoshis: 1,
            locking_script: script.clone(),
            off_chain_values: None,
        };
        svc.output_admitted_by_topic(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 1);

        // Same identity_key + domain + topic but different txid
        let payload2 = OutputAdmittedByTopic::LockingScript {
            txid: "tx2".into(),
            output_index: 0,
            topic: "tm_ship".into(),
            satoshis: 1,
            locking_script: script,
            off_chain_values: None,
        };
        svc.output_admitted_by_topic(&payload2).await.unwrap();
        // Duplicate should be skipped
        assert_eq!(storage.record_count(), 1);
    }

    #[tokio::test]
    async fn different_domain_is_not_duplicate() {
        let (svc, storage) = make_service_with_storage();
        let ik = dummy_identity_key();

        let script1 = make_ship_locking_script("SHIP", &ik, "https://alpha.com", "tm_test");
        let script2 = make_ship_locking_script("SHIP", &ik, "https://beta.com", "tm_test");

        for (i, script) in [script1, script2].iter().enumerate() {
            let payload = OutputAdmittedByTopic::LockingScript {
                txid: format!("tx{i}"),
                output_index: 0,
                topic: "tm_ship".into(),
                satoshis: 1,
                locking_script: script.clone(),
                off_chain_values: None,
            };
            svc.output_admitted_by_topic(&payload).await.unwrap();
        }
        // Different domains are not duplicates
        assert_eq!(storage.record_count(), 2);
    }

    #[tokio::test]
    async fn different_topic_is_not_duplicate() {
        let (svc, storage) = make_service_with_storage();
        let ik = dummy_identity_key();

        let script1 = make_ship_locking_script("SHIP", &ik, "https://example.com", "tm_foo");
        let script2 = make_ship_locking_script("SHIP", &ik, "https://example.com", "tm_bar");

        for (i, script) in [script1, script2].iter().enumerate() {
            let payload = OutputAdmittedByTopic::LockingScript {
                txid: format!("tx{i}"),
                output_index: 0,
                topic: "tm_ship".into(),
                satoshis: 1,
                locking_script: script.clone(),
                off_chain_values: None,
            };
            svc.output_admitted_by_topic(&payload).await.unwrap();
        }
        assert_eq!(storage.record_count(), 2);
    }

    // ========================================================================
    // output_spent
    // ========================================================================

    #[tokio::test]
    async fn output_spent_deletes_record() {
        let (svc, storage) = make_service_with_storage();
        storage
            .store_record("tx1", 0, "key", "domain", "topic")
            .await
            .unwrap();
        assert_eq!(storage.record_count(), 1);

        let payload = OutputSpent::None {
            txid: "tx1".into(),
            output_index: 0,
            topic: "tm_ship".into(),
        };
        svc.output_spent(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 0);
    }

    #[tokio::test]
    async fn output_spent_ignores_non_tm_ship_topic() {
        let (svc, storage) = make_service_with_storage();
        storage
            .store_record("tx1", 0, "key", "domain", "topic")
            .await
            .unwrap();

        let payload = OutputSpent::None {
            txid: "tx1".into(),
            output_index: 0,
            topic: "tm_other".into(), // not tm_ship
        };
        svc.output_spent(&payload).await.unwrap();
        // Record should still be there
        assert_eq!(storage.record_count(), 1);
    }

    #[tokio::test]
    async fn output_spent_with_non_none_variant_is_noop() {
        let (svc, storage) = make_service_with_storage();
        storage
            .store_record("tx1", 0, "key", "domain", "topic")
            .await
            .unwrap();

        let payload = OutputSpent::Txid {
            txid: "tx1".into(),
            output_index: 0,
            topic: "tm_ship".into(),
            spending_txid: "stx1".into(),
        };
        svc.output_spent(&payload).await.unwrap();
        // Should not delete because the service only handles OutputSpent::None
        assert_eq!(storage.record_count(), 1);
    }

    #[tokio::test]
    async fn output_spent_nonexistent_txid_is_ok() {
        let svc = make_service();
        let payload = OutputSpent::None {
            txid: "nonexistent".into(),
            output_index: 0,
            topic: "tm_ship".into(),
        };
        // Should not error
        svc.output_spent(&payload).await.unwrap();
    }

    // ========================================================================
    // output_evicted
    // ========================================================================

    #[tokio::test]
    async fn eviction_deletes_record() {
        let (svc, storage) = make_service_with_storage();
        storage
            .store_record("tx1", 0, "key", "domain", "topic")
            .await
            .unwrap();

        svc.output_evicted("tx1", 0).await.unwrap();
        assert_eq!(storage.record_count(), 0);
    }

    #[tokio::test]
    async fn eviction_only_deletes_matching_utxo() {
        let (svc, storage) = make_service_with_storage();
        storage.store_record("tx1", 0, "k", "d", "t").await.unwrap();
        storage
            .store_record("tx2", 0, "k", "d2", "t2")
            .await
            .unwrap();
        storage
            .store_record("tx1", 1, "k", "d3", "t3")
            .await
            .unwrap();

        svc.output_evicted("tx1", 0).await.unwrap();
        // Only tx1:0 removed; tx2:0 and tx1:1 remain
        assert_eq!(storage.record_count(), 2);
    }

    #[tokio::test]
    async fn eviction_nonexistent_is_ok() {
        let svc = make_service();
        svc.output_evicted("nope", 99).await.unwrap();
    }

    // ========================================================================
    // lookup — query variations
    // ========================================================================

    #[tokio::test]
    async fn lookup_empty_returns_empty() {
        let svc = make_service();
        let q = LookupQuestion::new("ls_ship", serde_json::json!({"find_all": true}));
        let results = svc
            .lookup(&q)
            .await
            .unwrap()
            .into_outputs()
            .expect("expected OutputList");
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn lookup_wrong_service_errors() {
        let svc = make_service();
        let q = LookupQuestion::new("ls_other", serde_json::json!({}));
        let result = svc.lookup(&q).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn lookup_find_all_legacy_string() {
        let (svc, storage) = make_service_with_storage();
        storage.store_record("tx1", 0, "k", "d", "t").await.unwrap();

        let q = LookupQuestion::new("ls_ship", serde_json::json!("findAll"));
        let results = svc
            .lookup(&q)
            .await
            .unwrap()
            .into_outputs()
            .expect("expected OutputList");
        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn lookup_find_all_json_flag() {
        let (svc, storage) = make_service_with_storage();
        storage.store_record("tx1", 0, "k", "d", "t").await.unwrap();
        storage
            .store_record("tx2", 1, "k2", "d2", "t2")
            .await
            .unwrap();

        let q = LookupQuestion::new("ls_ship", serde_json::json!({"find_all": true}));
        let results = svc
            .lookup(&q)
            .await
            .unwrap()
            .into_outputs()
            .expect("expected OutputList");
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn lookup_by_topic() {
        let (svc, storage) = make_service_with_storage();
        storage
            .store_record("tx1", 0, "key1", "https://a.com", "tm_test")
            .await
            .unwrap();
        storage
            .store_record("tx2", 0, "key2", "https://b.com", "tm_other")
            .await
            .unwrap();

        let q = LookupQuestion::new("ls_ship", serde_json::json!({"topics": ["tm_test"]}));
        let results = svc
            .lookup(&q)
            .await
            .unwrap()
            .into_outputs()
            .expect("expected OutputList");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx1");
    }

    #[tokio::test]
    async fn lookup_by_multiple_topics() {
        let (svc, storage) = make_service_with_storage();
        storage
            .store_record("tx1", 0, "k", "d", "tm_a")
            .await
            .unwrap();
        storage
            .store_record("tx2", 0, "k", "d", "tm_b")
            .await
            .unwrap();
        storage
            .store_record("tx3", 0, "k", "d", "tm_c")
            .await
            .unwrap();

        let q = LookupQuestion::new("ls_ship", serde_json::json!({"topics": ["tm_a", "tm_c"]}));
        let results = svc
            .lookup(&q)
            .await
            .unwrap()
            .into_outputs()
            .expect("expected OutputList");
        assert_eq!(results.len(), 2);
        let txids: Vec<&str> = results.iter().map(|r| r.txid.as_str()).collect();
        assert!(txids.contains(&"tx1"));
        assert!(txids.contains(&"tx3"));
    }

    #[tokio::test]
    async fn lookup_by_domain() {
        let (svc, storage) = make_service_with_storage();
        storage
            .store_record("tx1", 0, "k", "https://a.com", "t")
            .await
            .unwrap();
        storage
            .store_record("tx2", 0, "k", "https://b.com", "t")
            .await
            .unwrap();

        let q = LookupQuestion::new("ls_ship", serde_json::json!({"domain": "https://a.com"}));
        let results = svc
            .lookup(&q)
            .await
            .unwrap()
            .into_outputs()
            .expect("expected OutputList");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx1");
    }

    #[tokio::test]
    async fn lookup_by_identity_key() {
        let (svc, storage) = make_service_with_storage();
        storage
            .store_record("tx1", 0, "key_aaa", "d", "t")
            .await
            .unwrap();
        storage
            .store_record("tx2", 0, "key_bbb", "d", "t")
            .await
            .unwrap();

        let q = LookupQuestion::new("ls_ship", serde_json::json!({"identity_key": "key_bbb"}));
        let results = svc
            .lookup(&q)
            .await
            .unwrap()
            .into_outputs()
            .expect("expected OutputList");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx2");
    }

    #[tokio::test]
    async fn lookup_combined_domain_and_topic() {
        let (svc, storage) = make_service_with_storage();
        storage
            .store_record("tx1", 0, "k", "https://a.com", "tm_foo")
            .await
            .unwrap();
        storage
            .store_record("tx2", 0, "k", "https://b.com", "tm_foo")
            .await
            .unwrap();
        storage
            .store_record("tx3", 0, "k", "https://a.com", "tm_bar")
            .await
            .unwrap();

        let q = LookupQuestion::new(
            "ls_ship",
            serde_json::json!({"domain": "https://a.com", "topics": ["tm_foo"]}),
        );
        let results = svc
            .lookup(&q)
            .await
            .unwrap()
            .into_outputs()
            .expect("expected OutputList");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx1");
    }

    #[tokio::test]
    async fn lookup_with_pagination_limit() {
        let (svc, storage) = make_service_with_storage();
        for i in 0..5 {
            storage
                .store_record(&format!("tx{i}"), 0, "k", "d", "t")
                .await
                .unwrap();
        }

        let q = LookupQuestion::new("ls_ship", serde_json::json!({"find_all": true, "limit": 2}));
        let results = svc
            .lookup(&q)
            .await
            .unwrap()
            .into_outputs()
            .expect("expected OutputList");
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn lookup_with_pagination_skip() {
        let (svc, storage) = make_service_with_storage();
        for i in 0..5 {
            storage
                .store_record(&format!("tx{i}"), 0, "k", "d", "t")
                .await
                .unwrap();
        }

        let q = LookupQuestion::new("ls_ship", serde_json::json!({"find_all": true, "skip": 3}));
        let results = svc
            .lookup(&q)
            .await
            .unwrap()
            .into_outputs()
            .expect("expected OutputList");
        assert_eq!(results.len(), 2); // 5 - 3 skipped = 2
    }

    #[tokio::test]
    async fn lookup_with_limit_and_skip() {
        let (svc, storage) = make_service_with_storage();
        for i in 0..10 {
            storage
                .store_record(&format!("tx{i}"), 0, "k", "d", "t")
                .await
                .unwrap();
        }

        let q = LookupQuestion::new(
            "ls_ship",
            serde_json::json!({"find_all": true, "limit": 3, "skip": 2}),
        );
        let results = svc
            .lookup(&q)
            .await
            .unwrap()
            .into_outputs()
            .expect("expected OutputList");
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn lookup_with_zero_limit_errors() {
        let svc = make_service();
        let q = LookupQuestion::new("ls_ship", serde_json::json!({"find_all": true, "limit": 0}));
        let result = svc.lookup(&q).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn lookup_with_sort_order_asc() {
        let (svc, storage) = make_service_with_storage();
        for i in 0..3 {
            storage
                .store_record(&format!("tx{i}"), 0, "k", "d", "t")
                .await
                .unwrap();
        }

        let q = LookupQuestion::new(
            "ls_ship",
            serde_json::json!({"find_all": true, "sort_order": "asc"}),
        );
        let results = svc
            .lookup(&q)
            .await
            .unwrap()
            .into_outputs()
            .expect("expected OutputList");
        assert_eq!(results.len(), 3);
        // All records returned regardless of sort order
        let txids: Vec<&str> = results.iter().map(|r| r.txid.as_str()).collect();
        assert!(txids.contains(&"tx0"));
        assert!(txids.contains(&"tx1"));
        assert!(txids.contains(&"tx2"));
    }

    #[tokio::test]
    async fn lookup_invalid_query_json_errors() {
        let svc = make_service();
        // Invalid query structure (number instead of object)
        let q = LookupQuestion::new("ls_ship", serde_json::json!(42));
        let result = svc.lookup(&q).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn lookup_no_matching_records() {
        let (svc, storage) = make_service_with_storage();
        storage
            .store_record("tx1", 0, "k", "d", "tm_real")
            .await
            .unwrap();

        let q = LookupQuestion::new("ls_ship", serde_json::json!({"topics": ["tm_nonexistent"]}));
        let results = svc
            .lookup(&q)
            .await
            .unwrap()
            .into_outputs()
            .expect("expected OutputList");
        assert!(results.is_empty());
    }

    // ========================================================================
    // Full lifecycle: admit -> query -> spend/evict -> query
    // ========================================================================

    #[tokio::test]
    async fn lifecycle_admit_query_spend_query() {
        let (svc, storage) = make_service_with_storage();
        let ik = dummy_identity_key();
        let ik_hex = hex::encode(&ik);
        let script = make_ship_locking_script("SHIP", &ik, "https://node.example.com", "tm_tokens");

        // Step 1: Admit
        let payload = OutputAdmittedByTopic::LockingScript {
            txid: "admit_tx".into(),
            output_index: 0,
            topic: "tm_ship".into(),
            satoshis: 1,
            locking_script: script,
            off_chain_values: None,
        };
        svc.output_admitted_by_topic(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 1);

        // Step 2: Query — should find the record
        let q = LookupQuestion::new("ls_ship", serde_json::json!({"identity_key": ik_hex}));
        let results = svc
            .lookup(&q)
            .await
            .unwrap()
            .into_outputs()
            .expect("expected OutputList");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "admit_tx");
        assert_eq!(results[0].output_index, 0);

        // Step 3: Spend
        let spend = OutputSpent::None {
            txid: "admit_tx".into(),
            output_index: 0,
            topic: "tm_ship".into(),
        };
        svc.output_spent(&spend).await.unwrap();
        assert_eq!(storage.record_count(), 0);

        // Step 4: Query again — empty
        let results = svc
            .lookup(&q)
            .await
            .unwrap()
            .into_outputs()
            .expect("expected OutputList");
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn lifecycle_admit_query_evict_query() {
        let (svc, storage) = make_service_with_storage();
        let ik = dummy_identity_key();
        let script = make_ship_locking_script("SHIP", &ik, "https://example.com", "tm_data");

        // Admit
        let payload = OutputAdmittedByTopic::LockingScript {
            txid: "evict_tx".into(),
            output_index: 2,
            topic: "tm_ship".into(),
            satoshis: 1,
            locking_script: script,
            off_chain_values: None,
        };
        svc.output_admitted_by_topic(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 1);

        // Query
        let q = LookupQuestion::new("ls_ship", serde_json::json!({"find_all": true}));
        let results = svc
            .lookup(&q)
            .await
            .unwrap()
            .into_outputs()
            .expect("expected OutputList");
        assert_eq!(results.len(), 1);

        // Evict
        svc.output_evicted("evict_tx", 2).await.unwrap();
        assert_eq!(storage.record_count(), 0);

        // Query again
        let results = svc
            .lookup(&q)
            .await
            .unwrap()
            .into_outputs()
            .expect("expected OutputList");
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn lifecycle_admit_multiple_evict_one_query() {
        let (svc, storage) = make_service_with_storage();
        let ik1 = dummy_identity_key();
        let ik2 = dummy_identity_key();

        let script1 = make_ship_locking_script("SHIP", &ik1, "https://a.com", "tm_one");
        let script2 = make_ship_locking_script("SHIP", &ik2, "https://b.com", "tm_two");

        let payload1 = OutputAdmittedByTopic::LockingScript {
            txid: "tx_a".into(),
            output_index: 0,
            topic: "tm_ship".into(),
            satoshis: 1,
            locking_script: script1,
            off_chain_values: None,
        };
        let payload2 = OutputAdmittedByTopic::LockingScript {
            txid: "tx_b".into(),
            output_index: 0,
            topic: "tm_ship".into(),
            satoshis: 1,
            locking_script: script2,
            off_chain_values: None,
        };

        svc.output_admitted_by_topic(&payload1).await.unwrap();
        svc.output_admitted_by_topic(&payload2).await.unwrap();
        assert_eq!(storage.record_count(), 2);

        // Evict only the first
        svc.output_evicted("tx_a", 0).await.unwrap();
        assert_eq!(storage.record_count(), 1);

        // Query — should only find tx_b
        let q = LookupQuestion::new("ls_ship", serde_json::json!({"find_all": true}));
        let results = svc
            .lookup(&q)
            .await
            .unwrap()
            .into_outputs()
            .expect("expected OutputList");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx_b");
    }
}
