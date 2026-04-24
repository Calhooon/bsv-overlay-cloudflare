//! `ls_dm_delegation` lookup service — indexes delegation revocation
//! records and answers queries.

use async_trait::async_trait;
use bsv_rs::script::templates::PushDrop;
use overlay_engine::lookup_service::{LookupService, LookupServiceError};
use overlay_engine::types::*;
use std::rc::Rc;
use tracing::debug;

use super::storage::{DmDelegationRecord, DmDelegationStorage};
use super::topic_manager::DM_DELEGATION_MARKER;

pub struct DmDelegationLookupService {
    storage: Rc<dyn DmDelegationStorage>,
}

impl DmDelegationLookupService {
    pub fn new(storage: Rc<dyn DmDelegationStorage>) -> Self {
        Self { storage }
    }

    /// Parse a `"<txid>.<vout>"` outpoint into its parts.
    ///
    /// Returns `None` for malformed inputs (caller treats as "no match").
    fn parse_outpoint(s: &str) -> Option<(String, u32)> {
        let (txid, vout_s) = s.split_once('.')?;
        if txid.len() != 64 || !txid.chars().all(|c| c.is_ascii_hexdigit()) {
            return None;
        }
        let vout: u32 = vout_s.parse().ok()?;
        Some((txid.to_string(), vout))
    }
}

#[async_trait(?Send)]
impl LookupService for DmDelegationLookupService {
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

        // Only index tm_dm_delegation outputs.
        if topic != "tm_dm_delegation" {
            return Ok(());
        }

        // Decode the PushDrop and extract the envelope.
        let script = bsv_rs::script::Script::from_binary(locking_script)
            .map_err(|e| LookupServiceError::Other(format!("Script parse error: {e}")))?;
        let pushdrop = PushDrop::decode(&script.into())
            .map_err(|e| LookupServiceError::Other(format!("PushDrop decode error: {e}")))?;

        if pushdrop.fields.len() != 3 {
            // Topic manager should have rejected this; defensive skip.
            return Ok(());
        }
        if pushdrop.fields[0].as_slice() != DM_DELEGATION_MARKER {
            return Ok(());
        }

        let envelope: serde_json::Value = match serde_json::from_slice(&pushdrop.fields[1]) {
            Ok(v) => v,
            Err(e) => {
                debug!("DM_DELEGATION: skipping output, envelope JSON error: {e}");
                return Ok(());
            }
        };
        let obj = match envelope.as_object() {
            Some(o) => o,
            None => return Ok(()),
        };

        let serial_number = obj
            .get("serial_number")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let certifier_key = obj
            .get("certifier")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let subject_key = obj
            .get("subject")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let expires_at = obj
            .get("expires_at")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        // Skip duplicates rather than store twice.
        let is_dup = self
            .storage
            .has_duplicate_record(txid, output_index)
            .await
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;
        if is_dup {
            debug!("DM_DELEGATION: duplicate {txid}.{output_index} ignored");
            return Ok(());
        }

        let record = DmDelegationRecord {
            txid: txid.clone(),
            output_index,
            serial_number,
            certifier_key,
            subject_key,
            expires_at,
            created_at: String::new(), // backend sets this
        };
        self.storage
            .store_record(&record)
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

        if topic != "tm_dm_delegation" {
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
        if question.service != "ls_dm_delegation" {
            return Err(LookupServiceError::Unsupported(format!(
                "Expected ls_dm_delegation, got {}",
                question.service
            )));
        }

        let query = &question.query;
        if !query.is_object() {
            return Err(LookupServiceError::InvalidQuery(
                "query must be a JSON object".into(),
            ));
        }

        let limit = query.get("limit").and_then(|v| v.as_u64()).unwrap_or(1000) as u32;
        let skip = query.get("skip").and_then(|v| v.as_u64()).unwrap_or(0) as u32;

        // findByOutpoint — primary query, exact-match
        if let Some(outpoint) = query.get("findByOutpoint").and_then(|v| v.as_str()) {
            let (txid, vout) = match Self::parse_outpoint(outpoint) {
                Some(p) => p,
                None => {
                    // Malformed outpoint → empty result (caller treats as
                    // "not found", which means revoked from their POV).
                    return Ok(vec![]);
                }
            };
            return self
                .storage
                .find_by_outpoint(&txid, vout)
                .await
                .map_err(|e| LookupServiceError::StorageError(e.to_string()));
        }

        if let Some(serial) = query.get("findBySerial").and_then(|v| v.as_str()) {
            return self
                .storage
                .find_by_serial(serial)
                .await
                .map_err(|e| LookupServiceError::StorageError(e.to_string()));
        }

        if let Some(certifier) = query.get("findByCertifier").and_then(|v| v.as_str()) {
            return self
                .storage
                .find_by_certifier(certifier, Some(limit), Some(skip))
                .await
                .map_err(|e| LookupServiceError::StorageError(e.to_string()));
        }

        if query.get("findAll").and_then(|v| v.as_bool()) == Some(true) {
            return self
                .storage
                .find_all(Some(limit), Some(skip))
                .await
                .map_err(|e| LookupServiceError::StorageError(e.to_string()));
        }

        // Empty query object {} → findAll convenience
        let has_filter = query
            .as_object()
            .is_some_and(|obj| obj.keys().any(|k| k != "limit" && k != "skip"));
        if !has_filter {
            return self
                .storage
                .find_all(Some(limit), Some(skip))
                .await
                .map_err(|e| LookupServiceError::StorageError(e.to_string()));
        }

        Err(LookupServiceError::InvalidQuery(
            "unrecognized query type — use findByOutpoint, findBySerial, findByCertifier, or findAll".into(),
        ))
    }

    async fn get_documentation(&self) -> String {
        "Dolphin Milk Delegation Revocation Lookup Service — query for cert revocation status by outpoint, serial number, or certifier.".to_string()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "Dolphin Milk Delegation Revocation Lookup Service".to_string(),
            description: Some(
                "Provides revocation status lookups for dolphin-milk delegation certificates."
                    .to_string(),
            ),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::storage::MemoryDmDelegationStorage;
    use super::*;
    use bsv_rs::primitives::ec::{PrivateKey, PublicKey};
    use bsv_rs::script::templates::PushDrop as PushDropTemplate;

    fn make_service() -> (DmDelegationLookupService, Rc<MemoryDmDelegationStorage>) {
        let storage = Rc::new(MemoryDmDelegationStorage::new());
        let svc = DmDelegationLookupService::new(storage.clone());
        (svc, storage)
    }

    fn make_envelope(serial: &str, certifier: &str, subject: &str) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "type": "DelegationRevocation",
            "serial_number": serial,
            "subject": subject,
            "certifier": certifier,
            "purpose_hash": format!("sha256:{}", "ab".repeat(32)),
            "issued_at": "2026-04-12T17:00:00+00:00",
            "expires_at": "2026-04-12T17:10:00+00:00",
        }))
        .unwrap()
    }

    fn make_locking_script(serial: &str, certifier: &str, subject: &str) -> Vec<u8> {
        let locking_key = PublicKey::from_private_key(&PrivateKey::random());
        let fields = vec![
            DM_DELEGATION_MARKER.to_vec(),
            make_envelope(serial, certifier, subject),
            b"1700000000".to_vec(),
        ];
        let pd = PushDropTemplate::new(locking_key, fields);
        pd.lock().to_binary()
    }

    #[tokio::test]
    async fn admission_mode_is_locking_script() {
        let (svc, _) = make_service();
        assert_eq!(svc.admission_mode(), AdmissionMode::LockingScript);
    }

    #[tokio::test]
    async fn metadata_correct() {
        let (svc, _) = make_service();
        let meta = svc.get_metadata().await;
        assert!(meta.name.contains("Delegation Revocation"));
    }

    #[tokio::test]
    async fn admit_indexes_record() {
        let (svc, storage) = make_service();
        let cert_key = "03".repeat(33);
        let subject_key = "02".repeat(33);
        let script = make_locking_script("ser-1", &cert_key, &subject_key);
        let payload = OutputAdmittedByTopic::LockingScript {
            txid: "abc123".into(),
            output_index: 0,
            topic: "tm_dm_delegation".into(),
            satoshis: 1,
            locking_script: script,
            off_chain_values: None,
        };
        svc.output_admitted_by_topic(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 1);
    }

    #[tokio::test]
    async fn ignores_other_topics() {
        let (svc, storage) = make_service();
        let script = make_locking_script("ser-1", &"03".repeat(33), &"02".repeat(33));
        let payload = OutputAdmittedByTopic::LockingScript {
            txid: "abc".into(),
            output_index: 0,
            topic: "tm_agent".into(), // wrong topic
            satoshis: 1,
            locking_script: script,
            off_chain_values: None,
        };
        svc.output_admitted_by_topic(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 0);
    }

    #[tokio::test]
    async fn lookup_by_outpoint_canonical() {
        let (svc, storage) = make_service();
        // Insert via storage directly to use a proper 64-hex txid the
        // outpoint parser will accept.
        let txid = "a".repeat(64);
        storage
            .store_record(&DmDelegationRecord {
                txid: txid.clone(),
                output_index: 0,
                serial_number: "ser-1".into(),
                certifier_key: "03".repeat(33),
                subject_key: "02".repeat(33),
                expires_at: "2026-04-12T18:00:00+00:00".into(),
                created_at: String::new(),
            })
            .await
            .unwrap();

        let q = LookupQuestion {
            service: "ls_dm_delegation".into(),
            query: serde_json::json!({"findByOutpoint": format!("{txid}.0")}),
        };
        let hits = svc.lookup(&q).await.unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].txid, txid);
    }

    #[tokio::test]
    async fn lookup_by_outpoint_returns_empty_when_spent() {
        let (svc, _) = make_service();
        let txid = "f".repeat(64);
        // Nothing inserted — outpoint is "spent or never existed"
        let q = LookupQuestion {
            service: "ls_dm_delegation".into(),
            query: serde_json::json!({"findByOutpoint": format!("{txid}.0")}),
        };
        let hits = svc.lookup(&q).await.unwrap();
        assert!(hits.is_empty());
    }

    #[tokio::test]
    async fn lookup_by_outpoint_malformed_returns_empty() {
        let (svc, _) = make_service();
        let q = LookupQuestion {
            service: "ls_dm_delegation".into(),
            query: serde_json::json!({"findByOutpoint": "not-an-outpoint"}),
        };
        let hits = svc.lookup(&q).await.unwrap();
        assert!(hits.is_empty());
    }

    #[tokio::test]
    async fn lookup_by_serial() {
        let (svc, storage) = make_service();
        storage
            .store_record(&DmDelegationRecord {
                txid: "tx1".into(),
                output_index: 0,
                serial_number: "ser-target".into(),
                certifier_key: "03".repeat(33),
                subject_key: "02".repeat(33),
                expires_at: "x".into(),
                created_at: String::new(),
            })
            .await
            .unwrap();

        let q = LookupQuestion {
            service: "ls_dm_delegation".into(),
            query: serde_json::json!({"findBySerial": "ser-target"}),
        };
        let hits = svc.lookup(&q).await.unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].txid, "tx1");
    }

    #[tokio::test]
    async fn lookup_by_certifier_with_pagination() {
        let (svc, storage) = make_service();
        let cert = "03".repeat(33);
        for i in 0..7 {
            storage
                .store_record(&DmDelegationRecord {
                    txid: format!("tx{i}"),
                    output_index: 0,
                    serial_number: format!("ser-{i}"),
                    certifier_key: cert.clone(),
                    subject_key: "02".repeat(33),
                    expires_at: "x".into(),
                    created_at: String::new(),
                })
                .await
                .unwrap();
        }
        let q = LookupQuestion {
            service: "ls_dm_delegation".into(),
            query: serde_json::json!({"findByCertifier": cert, "limit": 3, "skip": 2}),
        };
        let hits = svc.lookup(&q).await.unwrap();
        assert_eq!(hits.len(), 3);
    }

    #[tokio::test]
    async fn empty_query_returns_all() {
        let (svc, storage) = make_service();
        storage
            .store_record(&DmDelegationRecord {
                txid: "tx1".into(),
                output_index: 0,
                serial_number: "ser".into(),
                certifier_key: "03".repeat(33),
                subject_key: "02".repeat(33),
                expires_at: "x".into(),
                created_at: String::new(),
            })
            .await
            .unwrap();
        let q = LookupQuestion {
            service: "ls_dm_delegation".into(),
            query: serde_json::json!({}),
        };
        let hits = svc.lookup(&q).await.unwrap();
        assert_eq!(hits.len(), 1);
    }

    #[tokio::test]
    async fn rejects_wrong_service_name() {
        let (svc, _) = make_service();
        let q = LookupQuestion {
            service: "ls_agent".into(),
            query: serde_json::json!({}),
        };
        let err = svc.lookup(&q).await.unwrap_err();
        assert!(matches!(err, LookupServiceError::Unsupported(_)));
    }

    #[tokio::test]
    async fn unrecognized_query_key_errors() {
        let (svc, _) = make_service();
        let q = LookupQuestion {
            service: "ls_dm_delegation".into(),
            query: serde_json::json!({"findByGarbage": "x"}),
        };
        let err = svc.lookup(&q).await.unwrap_err();
        assert!(matches!(err, LookupServiceError::InvalidQuery(_)));
    }

    #[tokio::test]
    async fn output_spent_deletes_record() {
        let (svc, storage) = make_service();
        storage
            .store_record(&DmDelegationRecord {
                txid: "tx1".into(),
                output_index: 0,
                serial_number: "ser".into(),
                certifier_key: "03".repeat(33),
                subject_key: "02".repeat(33),
                expires_at: "x".into(),
                created_at: String::new(),
            })
            .await
            .unwrap();
        assert_eq!(storage.record_count(), 1);

        let payload = OutputSpent::None {
            txid: "tx1".into(),
            output_index: 0,
            topic: "tm_dm_delegation".into(),
        };
        svc.output_spent(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 0);
    }

    #[tokio::test]
    async fn output_evicted_deletes_record() {
        let (svc, storage) = make_service();
        storage
            .store_record(&DmDelegationRecord {
                txid: "tx1".into(),
                output_index: 0,
                serial_number: "ser".into(),
                certifier_key: "03".repeat(33),
                subject_key: "02".repeat(33),
                expires_at: "x".into(),
                created_at: String::new(),
            })
            .await
            .unwrap();
        svc.output_evicted("tx1", 0).await.unwrap();
        assert_eq!(storage.record_count(), 0);
    }

    #[tokio::test]
    async fn parse_outpoint_canonical() {
        let txid = "a".repeat(64);
        let parsed = DmDelegationLookupService::parse_outpoint(&format!("{txid}.5")).unwrap();
        assert_eq!(parsed.0, txid);
        assert_eq!(parsed.1, 5);
    }

    #[tokio::test]
    async fn parse_outpoint_rejects_short_txid() {
        assert!(DmDelegationLookupService::parse_outpoint("abc.0").is_none());
    }

    #[tokio::test]
    async fn parse_outpoint_rejects_no_dot() {
        let txid = "a".repeat(64);
        assert!(DmDelegationLookupService::parse_outpoint(&txid).is_none());
    }
}
