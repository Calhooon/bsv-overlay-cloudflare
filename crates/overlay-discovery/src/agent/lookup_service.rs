//! Agent Lookup Service — indexes and queries Agent Registry records.
//!
//! When outputs are admitted to tm_agent, this service decodes the PushDrop fields
//! and stores them via AgentStorage. Clients query for registered agents by
//! capability, identity key, certifier, or name.

use async_trait::async_trait;
use bsv_rs::script::templates::PushDrop;
use overlay_engine::lookup_service::{LookupService, LookupServiceError};
use overlay_engine::types::*;
use std::rc::Rc;
use tracing::debug;

use super::storage::{AgentRecord, AgentStorage};

/// Agent Lookup Service — indexes agent registrations and answers queries.
pub struct AgentLookupService {
    storage: Rc<dyn AgentStorage>,
}

impl AgentLookupService {
    /// Create a new Agent lookup service backed by the given storage.
    pub fn new(storage: Rc<dyn AgentStorage>) -> Self {
        Self { storage }
    }
}

#[async_trait(?Send)]
impl LookupService for AgentLookupService {
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

        // Only index tm_agent outputs
        if topic != "tm_agent" {
            return Ok(());
        }

        // Decode PushDrop to extract fields
        let script = bsv_rs::script::Script::from_binary(locking_script)
            .map_err(|e| LookupServiceError::Other(format!("Script parse error: {e}")))?;
        let pushdrop = PushDrop::decode(&script.into())
            .map_err(|e| LookupServiceError::Other(format!("PushDrop decode error: {e}")))?;

        // AGENT PushDrop has 6 fields: protocol, identity_key, certifier_key, name, capabilities, signature
        if pushdrop.fields.len() < 5 {
            return Ok(());
        }

        let protocol = String::from_utf8_lossy(&pushdrop.fields[0]);
        if protocol != "AGENT" {
            return Ok(());
        }

        let identity_key = hex::encode(&pushdrop.fields[1]);
        let certifier_key = hex::encode(&pushdrop.fields[2]);
        let name = String::from_utf8_lossy(&pushdrop.fields[3]).to_string();
        let capabilities_str = String::from_utf8_lossy(&pushdrop.fields[4]).to_string();

        // Split capabilities on comma into individual strings, trim whitespace
        let capabilities: Vec<String> = capabilities_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        // Replace-by-name semantics for legitimate re-registration:
        //
        // Operators routinely re-register an agent to update its certifier_key
        // (e.g., after a parent wallet starts vouching for it) or its
        // capability set. The dolphin-milk side spends the old UTXO and
        // submits the spending tx so we can call delete_record(old_txid,
        // old_vout) — but if that processing happens out of order with the
        // new admission, the old row may still be present when this code
        // runs. Previously we silently dropped the new record as a "dup",
        // leaving the old certifier_key in place forever.
        //
        // New behavior: when an existing row matches (identity_key, name),
        // delete the existing row(s) FIRST, then insert the new one. The
        // PRIMARY KEY (txid, outputIndex) prevents conflict between rows
        // with different txids, so this is safe. EPIC #329 Phase 3 fix.
        let existing_for_name = self
            .storage
            .find_existing_by_identity_and_name(&identity_key, &name)
            .await
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;
        if !existing_for_name.is_empty() {
            debug!(
                "AGENT: re-registration detected for {identity_key} / {name} — \
                 evicting {} stale row(s) before insert",
                existing_for_name.len()
            );
            for utxo in &existing_for_name {
                if let Err(e) = self
                    .storage
                    .delete_record(&utxo.txid, utxo.output_index)
                    .await
                {
                    debug!(
                        "AGENT: failed to evict stale row {}/{}: {e}",
                        utxo.txid, utxo.output_index
                    );
                }
            }
        }

        let record = AgentRecord {
            txid: txid.clone(),
            output_index,
            identity_key,
            certifier_key,
            name,
            capabilities,
            created_at: String::new(), // storage backend sets this
        };

        self.storage
            .store_record(&record)
            .await
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;

        Ok(())
    }

    async fn output_spent(&self, payload: &OutputSpent) -> Result<(), LookupServiceError> {
        // Extract txid, output_index, topic from ANY variant — they all carry these fields.
        let (txid, output_index, topic) = match payload {
            OutputSpent::None {
                txid,
                output_index,
                topic,
            } => (txid.as_str(), *output_index, topic.as_str()),
            OutputSpent::Txid {
                txid,
                output_index,
                topic,
                ..
            } => (txid.as_str(), *output_index, topic.as_str()),
            OutputSpent::Script {
                txid,
                output_index,
                topic,
                ..
            } => (txid.as_str(), *output_index, topic.as_str()),
            OutputSpent::WholeTx {
                txid,
                output_index,
                topic,
                ..
            } => (txid.as_str(), *output_index, topic.as_str()),
        };

        if topic != "tm_agent" {
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

    async fn lookup(
        &self,
        question: &LookupQuestion,
    ) -> Result<Vec<UTXOReference>, LookupServiceError> {
        if question.service != "ls_agent" {
            return Err(LookupServiceError::Unsupported(format!(
                "Expected ls_agent, got {}",
                question.service
            )));
        }

        // Handle legacy "findAll" string query
        if question.query.is_string() && question.query.as_str() == Some("findAll") {
            return self
                .storage
                .find_all(Some(1000), None)
                .await
                .map_err(|e| LookupServiceError::StorageError(e.to_string()));
        }

        // Parse query as JSON object and dispatch
        let query = &question.query;

        if !query.is_object() {
            return Err(LookupServiceError::InvalidQuery(
                "query must be a JSON object".into(),
            ));
        }

        let limit = query.get("limit").and_then(|v| v.as_u64()).unwrap_or(1000) as u32;
        let skip = query.get("skip").and_then(|v| v.as_u64()).unwrap_or(0) as u32;

        if limit == 0 {
            return Err(LookupServiceError::InvalidQuery(
                "limit must be positive".into(),
            ));
        }

        // Dispatch based on query type
        if query.get("findAll").and_then(|v| v.as_bool()) == Some(true) {
            return self
                .storage
                .find_all(Some(limit), Some(skip))
                .await
                .map_err(|e| LookupServiceError::StorageError(e.to_string()));
        }

        if let Some(capability) = query.get("findByCapability").and_then(|v| v.as_str()) {
            return self
                .storage
                .find_by_capability(capability, Some(limit), Some(skip))
                .await
                .map_err(|e| LookupServiceError::StorageError(e.to_string()));
        }

        if let Some(identity_key) = query.get("findByIdentityKey").and_then(|v| v.as_str()) {
            return self
                .storage
                .find_by_identity_key(identity_key)
                .await
                .map_err(|e| LookupServiceError::StorageError(e.to_string()));
        }

        if let Some(certifier_key) = query.get("findByCertifier").and_then(|v| v.as_str()) {
            return self
                .storage
                .find_by_certifier(certifier_key, Some(limit), Some(skip))
                .await
                .map_err(|e| LookupServiceError::StorageError(e.to_string()));
        }

        if let Some(name) = query.get("findByName").and_then(|v| v.as_str()) {
            return self
                .storage
                .find_by_name(name)
                .await
                .map_err(|e| LookupServiceError::StorageError(e.to_string()));
        }

        // Empty object {} is treated as findAll (convenience for "give me everything")
        let has_query_key = query
            .as_object()
            .is_some_and(|obj| obj.keys().any(|k| k != "limit" && k != "skip"));
        if !has_query_key {
            return self
                .storage
                .find_all(Some(limit), Some(skip))
                .await
                .map_err(|e| LookupServiceError::StorageError(e.to_string()));
        }

        Err(LookupServiceError::InvalidQuery(
            "unrecognized query type — use findAll, findByCapability, findByIdentityKey, findByCertifier, or findByName".into(),
        ))
    }

    async fn get_documentation(&self) -> String {
        "Agent Lookup Service — query for registered agents by capability, identity, certifier, or name.".to_string()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "Agent Lookup Service".to_string(),
            description: Some(
                "Provides lookup capabilities for the Agent Registry protocol.".to_string(),
            ),
            ..Default::default()
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::super::storage::MemoryAgentStorage;
    use super::*;
    use bsv_rs::primitives::ec::{PrivateKey, PublicKey};
    use bsv_rs::script::templates::PushDrop as PushDropTemplate;

    fn make_service() -> AgentLookupService {
        AgentLookupService::new(Rc::new(MemoryAgentStorage::new()))
    }

    fn make_service_with_storage() -> (AgentLookupService, Rc<MemoryAgentStorage>) {
        let storage = Rc::new(MemoryAgentStorage::new());
        let svc = AgentLookupService::new(storage.clone());
        (svc, storage)
    }

    /// Build an AGENT PushDrop locking script (binary bytes) suitable for
    /// OutputAdmittedByTopic::LockingScript payload.
    fn make_agent_locking_script(
        protocol: &str,
        identity_key: &[u8],
        certifier_key: &[u8],
        name: &str,
        capabilities: &str,
    ) -> Vec<u8> {
        let locking_key = PublicKey::from_private_key(&PrivateKey::random());
        let fields = vec![
            protocol.as_bytes().to_vec(),
            identity_key.to_vec(),
            certifier_key.to_vec(),
            name.as_bytes().to_vec(),
            capabilities.as_bytes().to_vec(),
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
        assert_eq!(meta.name, "Agent Lookup Service");
        assert!(meta.description.unwrap().contains("Agent Registry"));
    }

    #[tokio::test]
    async fn documentation_not_empty() {
        let svc = make_service();
        let docs = svc.get_documentation().await;
        assert!(!docs.is_empty());
        assert!(docs.contains("Agent"));
    }

    // ========================================================================
    // output_admitted_by_topic — valid PushDrop scripts
    // ========================================================================

    #[tokio::test]
    async fn admit_valid_agent_output_stores_record() {
        let (svc, storage) = make_service_with_storage();
        let ik = dummy_identity_key();
        let ck = dummy_identity_key();
        let script = make_agent_locking_script(
            "AGENT",
            &ik,
            &ck,
            "https://agent.example.com",
            "image-generation,upscaling",
        );

        let payload = OutputAdmittedByTopic::LockingScript {
            txid: "abc123".into(),
            output_index: 0,
            topic: "tm_agent".into(),
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
        let ck = dummy_identity_key();
        let ik_hex = hex::encode(&ik);
        let script = make_agent_locking_script(
            "AGENT",
            &ik,
            &ck,
            "https://agent.example.com",
            "image-generation,upscaling",
        );

        let payload = OutputAdmittedByTopic::LockingScript {
            txid: "deadbeef".into(),
            output_index: 7,
            topic: "tm_agent".into(),
            satoshis: 546,
            locking_script: script,
            off_chain_values: None,
        };
        svc.output_admitted_by_topic(&payload).await.unwrap();

        // Query by identity key to verify it was stored correctly
        let q = LookupQuestion::new("ls_agent", serde_json::json!({"findByIdentityKey": ik_hex}));
        let results = svc.lookup(&q).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "deadbeef");
        assert_eq!(results[0].output_index, 7);
    }

    #[tokio::test]
    async fn admit_multiple_capabilities_stores_all() {
        let (svc, storage) = make_service_with_storage();
        let ik = dummy_identity_key();
        let ck = dummy_identity_key();
        let script = make_agent_locking_script(
            "AGENT",
            &ik,
            &ck,
            "https://agent.example.com",
            "image-generation,upscaling,text-to-speech",
        );

        let payload = OutputAdmittedByTopic::LockingScript {
            txid: "tx1".into(),
            output_index: 0,
            topic: "tm_agent".into(),
            satoshis: 1,
            locking_script: script,
            off_chain_values: None,
        };
        svc.output_admitted_by_topic(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 1);

        // Each capability should be findable
        for cap in &["image-generation", "upscaling", "text-to-speech"] {
            let q = LookupQuestion::new("ls_agent", serde_json::json!({"findByCapability": cap}));
            let results = svc.lookup(&q).await.unwrap();
            assert_eq!(results.len(), 1, "capability {cap} should be findable");
        }
    }

    // ========================================================================
    // output_admitted_by_topic — filtering / rejection cases
    // ========================================================================

    #[tokio::test]
    async fn ignores_non_tm_agent_topic() {
        let (svc, storage) = make_service_with_storage();
        let payload = OutputAdmittedByTopic::LockingScript {
            txid: "tx1".into(),
            output_index: 0,
            topic: "tm_other".into(), // not tm_agent
            satoshis: 1,
            locking_script: vec![],
            off_chain_values: None,
        };
        svc.output_admitted_by_topic(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 0);
    }

    #[tokio::test]
    async fn ignores_non_agent_protocol_in_pushdrop() {
        let (svc, storage) = make_service_with_storage();
        let ik = dummy_identity_key();
        let ck = dummy_identity_key();
        // Protocol is "SHIP" not "AGENT"
        let script = make_agent_locking_script("SHIP", &ik, &ck, "https://example.com", "cap1");

        let payload = OutputAdmittedByTopic::LockingScript {
            txid: "tx1".into(),
            output_index: 0,
            topic: "tm_agent".into(),
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
            topic: "tm_agent".into(),
            off_chain_values: None,
        };
        let result = svc.output_admitted_by_topic(&payload).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn ignores_pushdrop_with_fewer_than_5_fields() {
        let (svc, storage) = make_service_with_storage();
        let locking_key = PublicKey::from_private_key(&PrivateKey::random());
        // Only 4 fields (missing capabilities)
        let fields = vec![
            b"AGENT".to_vec(),
            dummy_identity_key(),
            dummy_identity_key(),
            b"https://example.com".to_vec(),
        ];
        let pushdrop = PushDropTemplate::new(locking_key, fields);
        let script = pushdrop.lock().to_binary();

        let payload = OutputAdmittedByTopic::LockingScript {
            txid: "tx1".into(),
            output_index: 0,
            topic: "tm_agent".into(),
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
            topic: "tm_agent".into(),
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
        let ck = dummy_identity_key();
        let script =
            make_agent_locking_script("AGENT", &ik, &ck, "https://agent.example.com", "cap1");

        let payload = OutputAdmittedByTopic::LockingScript {
            txid: "tx1".into(),
            output_index: 0,
            topic: "tm_agent".into(),
            satoshis: 1,
            locking_script: script.clone(),
            off_chain_values: None,
        };
        svc.output_admitted_by_topic(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 1);

        // Same identity_key + name but different txid
        let payload2 = OutputAdmittedByTopic::LockingScript {
            txid: "tx2".into(),
            output_index: 0,
            topic: "tm_agent".into(),
            satoshis: 1,
            locking_script: script,
            off_chain_values: None,
        };
        svc.output_admitted_by_topic(&payload2).await.unwrap();
        // Duplicate should be skipped
        assert_eq!(storage.record_count(), 1);
    }

    #[tokio::test]
    async fn different_name_is_not_duplicate() {
        let (svc, storage) = make_service_with_storage();
        let ik = dummy_identity_key();
        let ck = dummy_identity_key();

        let script1 = make_agent_locking_script("AGENT", &ik, &ck, "https://alpha.com", "cap1");
        let script2 = make_agent_locking_script("AGENT", &ik, &ck, "https://beta.com", "cap1");

        for (i, script) in [script1, script2].iter().enumerate() {
            let payload = OutputAdmittedByTopic::LockingScript {
                txid: format!("tx{i}"),
                output_index: 0,
                topic: "tm_agent".into(),
                satoshis: 1,
                locking_script: script.clone(),
                off_chain_values: None,
            };
            svc.output_admitted_by_topic(&payload).await.unwrap();
        }
        // Different names are not duplicates
        assert_eq!(storage.record_count(), 2);
    }

    // ========================================================================
    // output_spent
    // ========================================================================

    #[tokio::test]
    async fn output_spent_deletes_record() {
        let (svc, storage) = make_service_with_storage();
        let record = AgentRecord {
            txid: "tx1".into(),
            output_index: 0,
            identity_key: "key".into(),
            certifier_key: "cert".into(),
            name: "test-agent".into(),
            capabilities: vec!["cap1".into()],
            created_at: String::new(),
        };
        storage.store_record(&record).await.unwrap();
        assert_eq!(storage.record_count(), 1);

        let payload = OutputSpent::None {
            txid: "tx1".into(),
            output_index: 0,
            topic: "tm_agent".into(),
        };
        svc.output_spent(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 0);
    }

    #[tokio::test]
    async fn output_spent_ignores_non_tm_agent_topic() {
        let (svc, storage) = make_service_with_storage();
        let record = AgentRecord {
            txid: "tx1".into(),
            output_index: 0,
            identity_key: "key".into(),
            certifier_key: "cert".into(),
            name: "test-agent".into(),
            capabilities: vec!["cap1".into()],
            created_at: String::new(),
        };
        storage.store_record(&record).await.unwrap();

        let payload = OutputSpent::None {
            txid: "tx1".into(),
            output_index: 0,
            topic: "tm_other".into(), // not tm_agent
        };
        svc.output_spent(&payload).await.unwrap();
        // Record should still be there
        assert_eq!(storage.record_count(), 1);
    }

    #[tokio::test]
    async fn output_spent_with_txid_variant_deletes_record() {
        let (svc, storage) = make_service_with_storage();
        let record = AgentRecord {
            txid: "tx1".into(),
            output_index: 0,
            identity_key: "key".into(),
            certifier_key: "cert".into(),
            name: "test-agent".into(),
            capabilities: vec!["cap1".into()],
            created_at: String::new(),
        };
        storage.store_record(&record).await.unwrap();

        let payload = OutputSpent::Txid {
            txid: "tx1".into(),
            output_index: 0,
            topic: "tm_agent".into(),
            spending_txid: "stx1".into(),
        };
        svc.output_spent(&payload).await.unwrap();
        // All variants should delete — spend detection works regardless of notification detail level
        assert_eq!(storage.record_count(), 0);
    }

    #[tokio::test]
    async fn output_spent_nonexistent_txid_is_ok() {
        let svc = make_service();
        let payload = OutputSpent::None {
            txid: "nonexistent".into(),
            output_index: 0,
            topic: "tm_agent".into(),
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
        let record = AgentRecord {
            txid: "tx1".into(),
            output_index: 0,
            identity_key: "key".into(),
            certifier_key: "cert".into(),
            name: "test-agent".into(),
            capabilities: vec!["cap1".into()],
            created_at: String::new(),
        };
        storage.store_record(&record).await.unwrap();

        svc.output_evicted("tx1", 0).await.unwrap();
        assert_eq!(storage.record_count(), 0);
    }

    #[tokio::test]
    async fn eviction_only_deletes_matching_utxo() {
        let (svc, storage) = make_service_with_storage();
        for i in 0..3 {
            let record = AgentRecord {
                txid: format!("tx{i}"),
                output_index: 0,
                identity_key: format!("key{i}"),
                certifier_key: "cert".into(),
                name: format!("agent-{i}"),
                capabilities: vec!["cap1".into()],
                created_at: String::new(),
            };
            storage.store_record(&record).await.unwrap();
        }

        svc.output_evicted("tx0", 0).await.unwrap();
        // Only tx0:0 removed; tx1:0 and tx2:0 remain
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
        let q = LookupQuestion::new("ls_agent", serde_json::json!({"findAll": true}));
        let results = svc.lookup(&q).await.unwrap();
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
        let record = AgentRecord {
            txid: "tx1".into(),
            output_index: 0,
            identity_key: "key".into(),
            certifier_key: "cert".into(),
            name: "test-agent".into(),
            capabilities: vec!["cap1".into()],
            created_at: String::new(),
        };
        storage.store_record(&record).await.unwrap();

        let q = LookupQuestion::new("ls_agent", serde_json::json!("findAll"));
        let results = svc.lookup(&q).await.unwrap();
        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn lookup_find_all_returns_all_agents() {
        let (svc, storage) = make_service_with_storage();
        for i in 0..3 {
            let record = AgentRecord {
                txid: format!("tx{i}"),
                output_index: 0,
                identity_key: format!("key{i}"),
                certifier_key: "cert".into(),
                name: format!("agent-{i}"),
                capabilities: vec!["cap1".into()],
                created_at: String::new(),
            };
            storage.store_record(&record).await.unwrap();
        }

        let q = LookupQuestion::new("ls_agent", serde_json::json!({"findAll": true}));
        let results = svc.lookup(&q).await.unwrap();
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn lookup_find_by_capability() {
        let (svc, storage) = make_service_with_storage();
        let rec1 = AgentRecord {
            txid: "tx1".into(),
            output_index: 0,
            identity_key: "key1".into(),
            certifier_key: "cert".into(),
            name: "agent-1".into(),
            capabilities: vec!["image-generation".into()],
            created_at: String::new(),
        };
        let rec2 = AgentRecord {
            txid: "tx2".into(),
            output_index: 0,
            identity_key: "key2".into(),
            certifier_key: "cert".into(),
            name: "agent-2".into(),
            capabilities: vec!["text-generation".into()],
            created_at: String::new(),
        };
        storage.store_record(&rec1).await.unwrap();
        storage.store_record(&rec2).await.unwrap();

        let q = LookupQuestion::new(
            "ls_agent",
            serde_json::json!({"findByCapability": "image-generation"}),
        );
        let results = svc.lookup(&q).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx1");
    }

    #[tokio::test]
    async fn lookup_find_by_identity_key() {
        let (svc, storage) = make_service_with_storage();
        let rec1 = AgentRecord {
            txid: "tx1".into(),
            output_index: 0,
            identity_key: "key_aaa".into(),
            certifier_key: "cert".into(),
            name: "agent-1".into(),
            capabilities: vec!["cap1".into()],
            created_at: String::new(),
        };
        let rec2 = AgentRecord {
            txid: "tx2".into(),
            output_index: 0,
            identity_key: "key_bbb".into(),
            certifier_key: "cert".into(),
            name: "agent-2".into(),
            capabilities: vec!["cap1".into()],
            created_at: String::new(),
        };
        storage.store_record(&rec1).await.unwrap();
        storage.store_record(&rec2).await.unwrap();

        let q = LookupQuestion::new(
            "ls_agent",
            serde_json::json!({"findByIdentityKey": "key_bbb"}),
        );
        let results = svc.lookup(&q).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx2");
    }

    #[tokio::test]
    async fn lookup_find_by_certifier() {
        let (svc, storage) = make_service_with_storage();
        let rec1 = AgentRecord {
            txid: "tx1".into(),
            output_index: 0,
            identity_key: "key1".into(),
            certifier_key: "certifier_a".into(),
            name: "agent-1".into(),
            capabilities: vec!["cap1".into()],
            created_at: String::new(),
        };
        let rec2 = AgentRecord {
            txid: "tx2".into(),
            output_index: 0,
            identity_key: "key2".into(),
            certifier_key: "certifier_b".into(),
            name: "agent-2".into(),
            capabilities: vec!["cap1".into()],
            created_at: String::new(),
        };
        storage.store_record(&rec1).await.unwrap();
        storage.store_record(&rec2).await.unwrap();

        let q = LookupQuestion::new(
            "ls_agent",
            serde_json::json!({"findByCertifier": "certifier_a"}),
        );
        let results = svc.lookup(&q).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx1");
    }

    #[tokio::test]
    async fn lookup_find_by_name() {
        let (svc, storage) = make_service_with_storage();
        let rec1 = AgentRecord {
            txid: "tx1".into(),
            output_index: 0,
            identity_key: "key1".into(),
            certifier_key: "cert".into(),
            name: "agent-1".into(),
            capabilities: vec!["cap1".into()],
            created_at: String::new(),
        };
        let rec2 = AgentRecord {
            txid: "tx2".into(),
            output_index: 0,
            identity_key: "key2".into(),
            certifier_key: "cert".into(),
            name: "agent-2".into(),
            capabilities: vec!["cap1".into()],
            created_at: String::new(),
        };
        storage.store_record(&rec1).await.unwrap();
        storage.store_record(&rec2).await.unwrap();

        let q = LookupQuestion::new("ls_agent", serde_json::json!({"findByName": "agent-1"}));
        let results = svc.lookup(&q).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx1");
    }

    #[tokio::test]
    async fn lookup_with_pagination() {
        let (svc, storage) = make_service_with_storage();
        for i in 0..10 {
            let record = AgentRecord {
                txid: format!("tx{i}"),
                output_index: 0,
                identity_key: format!("key{i}"),
                certifier_key: "cert".into(),
                name: format!("agent-{i}"),
                capabilities: vec!["cap1".into()],
                created_at: String::new(),
            };
            storage.store_record(&record).await.unwrap();
        }

        let q = LookupQuestion::new(
            "ls_agent",
            serde_json::json!({"findAll": true, "limit": 3, "skip": 2}),
        );
        let results = svc.lookup(&q).await.unwrap();
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn lookup_with_zero_limit_errors() {
        let svc = make_service();
        let q = LookupQuestion::new("ls_agent", serde_json::json!({"findAll": true, "limit": 0}));
        let result = svc.lookup(&q).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn lookup_empty_object_returns_all() {
        let (svc, storage) = make_service_with_storage();
        for i in 0..3 {
            let record = AgentRecord {
                txid: format!("tx{i}"),
                output_index: 0,
                identity_key: format!("key{i}"),
                certifier_key: "cert".into(),
                name: format!("agent-{i}"),
                capabilities: vec!["cap1".into()],
                created_at: String::new(),
            };
            storage.store_record(&record).await.unwrap();
        }

        // Empty {} should behave like findAll
        let q = LookupQuestion::new("ls_agent", serde_json::json!({}));
        let results = svc.lookup(&q).await.unwrap();
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn lookup_empty_object_with_pagination() {
        let (svc, storage) = make_service_with_storage();
        for i in 0..10 {
            let record = AgentRecord {
                txid: format!("tx{i}"),
                output_index: 0,
                identity_key: format!("key{i}"),
                certifier_key: "cert".into(),
                name: format!("agent-{i}"),
                capabilities: vec!["cap1".into()],
                created_at: String::new(),
            };
            storage.store_record(&record).await.unwrap();
        }

        // {} with limit/skip should paginate
        let q = LookupQuestion::new("ls_agent", serde_json::json!({"limit": 3, "skip": 2}));
        let results = svc.lookup(&q).await.unwrap();
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn lookup_invalid_query_json_errors() {
        let svc = make_service();
        // Invalid query structure (number instead of object)
        let q = LookupQuestion::new("ls_agent", serde_json::json!(42));
        let result = svc.lookup(&q).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn lookup_unrecognized_query_type_errors() {
        let svc = make_service();
        let q = LookupQuestion::new("ls_agent", serde_json::json!({"unknownField": "value"}));
        let result = svc.lookup(&q).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn lookup_no_matching_records() {
        let (svc, storage) = make_service_with_storage();
        let record = AgentRecord {
            txid: "tx1".into(),
            output_index: 0,
            identity_key: "key1".into(),
            certifier_key: "cert".into(),
            name: "agent".into(),
            capabilities: vec!["cap1".into()],
            created_at: String::new(),
        };
        storage.store_record(&record).await.unwrap();

        let q = LookupQuestion::new(
            "ls_agent",
            serde_json::json!({"findByCapability": "nonexistent"}),
        );
        let results = svc.lookup(&q).await.unwrap();
        assert!(results.is_empty());
    }

    // ========================================================================
    // Full lifecycle: admit -> query -> spend/evict -> query
    // ========================================================================

    #[tokio::test]
    async fn lifecycle_admit_query_spend_query() {
        let (svc, storage) = make_service_with_storage();
        let ik = dummy_identity_key();
        let ck = dummy_identity_key();
        let ik_hex = hex::encode(&ik);
        let script = make_agent_locking_script(
            "AGENT",
            &ik,
            &ck,
            "https://agent.example.com",
            "image-generation",
        );

        // Step 1: Admit
        let payload = OutputAdmittedByTopic::LockingScript {
            txid: "admit_tx".into(),
            output_index: 0,
            topic: "tm_agent".into(),
            satoshis: 1,
            locking_script: script,
            off_chain_values: None,
        };
        svc.output_admitted_by_topic(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 1);

        // Step 2: Query — should find the record
        let q = LookupQuestion::new("ls_agent", serde_json::json!({"findByIdentityKey": ik_hex}));
        let results = svc.lookup(&q).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "admit_tx");
        assert_eq!(results[0].output_index, 0);

        // Step 3: Spend
        let spend = OutputSpent::None {
            txid: "admit_tx".into(),
            output_index: 0,
            topic: "tm_agent".into(),
        };
        svc.output_spent(&spend).await.unwrap();
        assert_eq!(storage.record_count(), 0);

        // Step 4: Query again — empty
        let results = svc.lookup(&q).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn lifecycle_admit_query_evict_query() {
        let (svc, storage) = make_service_with_storage();
        let ik = dummy_identity_key();
        let ck = dummy_identity_key();
        let script =
            make_agent_locking_script("AGENT", &ik, &ck, "https://agent.example.com", "cap1");

        // Admit
        let payload = OutputAdmittedByTopic::LockingScript {
            txid: "evict_tx".into(),
            output_index: 2,
            topic: "tm_agent".into(),
            satoshis: 1,
            locking_script: script,
            off_chain_values: None,
        };
        svc.output_admitted_by_topic(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 1);

        // Query
        let q = LookupQuestion::new("ls_agent", serde_json::json!({"findAll": true}));
        let results = svc.lookup(&q).await.unwrap();
        assert_eq!(results.len(), 1);

        // Evict
        svc.output_evicted("evict_tx", 2).await.unwrap();
        assert_eq!(storage.record_count(), 0);

        // Query again
        let results = svc.lookup(&q).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn lifecycle_admit_multiple_evict_one_query() {
        let (svc, storage) = make_service_with_storage();
        let ik1 = dummy_identity_key();
        let ik2 = dummy_identity_key();
        let ck = dummy_identity_key();

        let script1 = make_agent_locking_script("AGENT", &ik1, &ck, "https://a.com", "cap1");
        let script2 = make_agent_locking_script("AGENT", &ik2, &ck, "https://b.com", "cap1");

        let payload1 = OutputAdmittedByTopic::LockingScript {
            txid: "tx_a".into(),
            output_index: 0,
            topic: "tm_agent".into(),
            satoshis: 1,
            locking_script: script1,
            off_chain_values: None,
        };
        let payload2 = OutputAdmittedByTopic::LockingScript {
            txid: "tx_b".into(),
            output_index: 0,
            topic: "tm_agent".into(),
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
        let q = LookupQuestion::new("ls_agent", serde_json::json!({"findAll": true}));
        let results = svc.lookup(&q).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx_b");
    }
}
