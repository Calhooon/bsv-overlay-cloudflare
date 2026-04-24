//! UHRP Lookup Service — indexes and queries UHRP advertisement records.
//!
//! When outputs are admitted to `tm_uhrp`, this service decodes the PushDrop
//! fields (5 data + signature) and stores key metadata via `UHRPStorage`.
//! Clients query `ls_uhrp` by `uhrp_url`, `identity_key`, or `find_all`.
//!
//! No TypeScript reference exists — `@bsv/overlay-discovery-services` ships
//! SHIP + SLAP only. Structure mirrors
//! `super::super::ship::lookup_service::SHIPLookupService`.

use async_trait::async_trait;
use bsv_rs::primitives::ec::PublicKey;
use bsv_rs::primitives::encoding::Reader;
use bsv_rs::script::templates::PushDrop;
use overlay_engine::lookup_service::{LookupService, LookupServiceError};
use overlay_engine::types::*;
use std::rc::Rc;
use tracing::debug;

use super::storage::{UHRPQuery, UHRPStorage};

/// UHRP Lookup Service — indexes UHRP advertisements and answers queries.
pub struct UHRPLookupService {
    storage: Rc<dyn UHRPStorage>,
}

impl UHRPLookupService {
    /// Create a new UHRP lookup service backed by the given storage.
    pub fn new(storage: Rc<dyn UHRPStorage>) -> Self {
        Self { storage }
    }
}

#[async_trait(?Send)]
impl LookupService for UHRPLookupService {
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

        // Only index tm_uhrp outputs.
        if topic != "tm_uhrp" {
            return Ok(());
        }

        let script = bsv_rs::script::Script::from_binary(locking_script)
            .map_err(|e| LookupServiceError::Other(format!("Script parse error: {e}")))?;
        let pushdrop = PushDrop::decode(&script.into())
            .map_err(|e| LookupServiceError::Other(format!("PushDrop decode error: {e}")))?;

        // Strictly need 6 fields (5 data + signature). Anything else is
        // silently ignored — the topic manager admits only well-formed
        // UHRP advertisements, so in practice this branch is defensive.
        if pushdrop.fields.len() != 6 {
            debug!(
                "UHRP: unexpected field count on admitted output: {}",
                pushdrop.fields.len()
            );
            return Ok(());
        }

        // Defensive: re-validate the identity key shape. The topic manager
        // already checked this, but the lookup service must not trust
        // pre-validated state — indexing service code is a separate
        // trust boundary from admittance code.
        if pushdrop.fields[0].len() != 33 || PublicKey::from_bytes(&pushdrop.fields[0]).is_err() {
            return Ok(());
        }
        if pushdrop.fields[1].len() != 32 {
            return Ok(());
        }

        let identity_key = hex::encode(&pushdrop.fields[0]);

        let download_url = match std::str::from_utf8(&pushdrop.fields[2]) {
            Ok(s) => s.to_string(),
            Err(_) => return Ok(()),
        };

        let expiry_time: i64 = {
            let mut r = Reader::new(&pushdrop.fields[3]);
            match r.read_var_int() {
                Ok(v) => i64::try_from(v).unwrap_or(i64::MAX),
                Err(_) => return Ok(()),
            }
        };
        let content_length: i64 = {
            let mut r = Reader::new(&pushdrop.fields[4]);
            match r.read_var_int() {
                Ok(v) => i64::try_from(v).unwrap_or(i64::MAX),
                Err(_) => return Ok(()),
            }
        };

        // Canonical `uhrp_url`: `uhrp://<base58check(0x01 || sha256)>`, matching
        // `StorageUtils.getURLForHash` in `@bsv/sdk` and the form stored by
        // TS `UHRPLookupServiceFactory`. Clients query with this exact string,
        // so indexing by hex (the earlier shortcut) makes lookups return 0.
        let uhrp_url_hex = format!(
            "uhrp://{}",
            bsv_rs::primitives::encoding::to_base58_check(&pushdrop.fields[1], &[0x01])
        );

        // Dedup: skip if (txid, output_index) is already indexed.
        let is_dup = self
            .storage
            .has_duplicate_record(txid, output_index)
            .await
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;
        if is_dup {
            debug!("UHRP: skipping duplicate record for {txid}:{output_index}");
            return Ok(());
        }

        self.storage
            .store_record(
                txid,
                output_index,
                &uhrp_url_hex,
                &identity_key,
                &download_url,
                expiry_time,
                content_length,
            )
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

        if topic != "tm_uhrp" {
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
        if question.service != "ls_uhrp" {
            return Err(LookupServiceError::Unsupported(format!(
                "Expected ls_uhrp, got {}",
                question.service
            )));
        }

        // Accept the legacy `"findAll"` string form for parity with SHIP.
        // Policy: the expiry filter still applies here — legacy callers
        // get the new "hide past-expiry" default. If they want the old
        // "really all records" behavior, they switch to the JSON form
        // with `includeExpired: true`.
        if question.query.is_string() && question.query.as_str() == Some("findAll") {
            return self
                .storage
                .find_record(&UHRPQuery {
                    find_all: Some(true),
                    ..Default::default()
                })
                .await
                .map_err(|e| LookupServiceError::StorageError(e.to_string()));
        }

        let query: UHRPQuery = serde_json::from_value(question.query.clone())
            .map_err(|e| LookupServiceError::InvalidQuery(e.to_string()))?;

        if let Some(limit) = query.limit {
            if limit == 0 {
                return Err(LookupServiceError::InvalidQuery(
                    "limit must be positive".into(),
                ));
            }
        }

        // Uniform path: storage.find_record handles both findAll and
        // filtered queries. The branch that used to short-circuit to
        // `find_all(limit, skip, sort)` dropped `include_expired` +
        // `now_unix_seconds` on the floor, so every findAll query ran
        // with filter defaults regardless of the caller's flags.
        self.storage
            .find_record(&query)
            .await
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))
    }

    async fn get_documentation(&self) -> String {
        "UHRP Lookup Service — query for UHRP advertisements (hosting commitments) by uhrp_url or identity_key.".to_string()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "UHRP Lookup Service".to_string(),
            description: Some(
                "Provides lookup capabilities for UHRP advertisement tokens.".to_string(),
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
    #![allow(clippy::unwrap_used)]
    #![allow(clippy::expect_used)]
    use super::super::storage::MemoryUHRPStorage;
    use super::*;
    use bsv_rs::primitives::ec::PrivateKey;
    use bsv_rs::primitives::encoding::Writer;
    use bsv_rs::script::templates::PushDrop as PushDropTemplate;

    fn make_service() -> UHRPLookupService {
        UHRPLookupService::new(Rc::new(MemoryUHRPStorage::new()))
    }

    fn make_service_with_storage() -> (UHRPLookupService, Rc<MemoryUHRPStorage>) {
        let storage = Rc::new(MemoryUHRPStorage::new());
        let svc = UHRPLookupService::new(storage.clone());
        (svc, storage)
    }

    fn varint(v: u64) -> Vec<u8> {
        let mut w = Writer::new();
        w.write_var_int(v);
        w.into_bytes()
    }

    /// Build a UHRP PushDrop locking script (binary bytes) suitable for
    /// `OutputAdmittedByTopic::LockingScript`. Signature is a stub since
    /// the lookup service does NOT re-verify (that's the topic manager's job).
    fn make_uhrp_locking_script(
        identity_pubkey: &[u8],
        hash32: &[u8],
        url: &str,
        expiry: u64,
        length: u64,
    ) -> Vec<u8> {
        let locking_key = PublicKey::from_private_key(&PrivateKey::random());
        let fields = vec![
            identity_pubkey.to_vec(),
            hash32.to_vec(),
            url.as_bytes().to_vec(),
            varint(expiry),
            varint(length),
            vec![0x30, 0x04, 0x02, 0x01, 0x01, 0x02, 0x01, 0x01], // mock sig
        ];
        let pushdrop = PushDropTemplate::new(locking_key, fields);
        pushdrop.lock().to_binary()
    }

    fn sample_identity() -> Vec<u8> {
        PublicKey::from_private_key(&PrivateKey::random())
            .to_compressed()
            .to_vec()
    }

    // ---- Trait surface --------------------------------------------------

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
        assert_eq!(meta.name, "UHRP Lookup Service");
    }

    #[tokio::test]
    async fn documentation_mentions_uhrp() {
        let svc = make_service();
        let docs = svc.get_documentation().await;
        assert!(docs.contains("UHRP"));
    }

    // ---- admission ------------------------------------------------------

    #[tokio::test]
    async fn admits_and_stores_record() {
        let (svc, storage) = make_service_with_storage();
        let ident = sample_identity();
        let hash = [0x42u8; 32];
        let script =
            make_uhrp_locking_script(&ident, &hash, "https://a.example", 1_900_000_000, 1024);
        let payload = OutputAdmittedByTopic::LockingScript {
            txid: "abc123".into(),
            output_index: 0,
            topic: "tm_uhrp".into(),
            satoshis: 1,
            locking_script: script,
            off_chain_values: None,
        };
        svc.output_admitted_by_topic(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 1);

        let records = storage.find_all_records().await.unwrap();
        assert_eq!(records[0].identity_key, hex::encode(&ident));
        // uhrp_url stored in canonical form: "uhrp://<base58check(0x01 || hash)>".
        // Derivation matches `StorageUtils.getURLForHash` in @bsv/sdk and the TS
        // reference `UHRPLookupServiceFactory.ts` — see `lookup_service.rs:117-123`.
        let expected_uhrp_url = format!(
            "uhrp://{}",
            bsv_rs::primitives::encoding::to_base58_check(&hash, &[0x01])
        );
        assert_eq!(records[0].uhrp_url, expected_uhrp_url);
        assert_eq!(records[0].download_url, "https://a.example");
        assert_eq!(records[0].expiry_time, 1_900_000_000);
        assert_eq!(records[0].content_length, 1024);
    }

    #[tokio::test]
    async fn ignores_non_uhrp_topic() {
        let (svc, storage) = make_service_with_storage();
        let payload = OutputAdmittedByTopic::LockingScript {
            txid: "abc".into(),
            output_index: 0,
            topic: "tm_ship".into(),
            satoshis: 1,
            locking_script: vec![],
            off_chain_values: None,
        };
        svc.output_admitted_by_topic(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 0);
    }

    #[tokio::test]
    async fn duplicate_utxo_is_ignored() {
        let (svc, storage) = make_service_with_storage();
        let ident = sample_identity();
        let hash = [0x42u8; 32];
        let script =
            make_uhrp_locking_script(&ident, &hash, "https://a.example", 1_900_000_000, 1024);
        let payload = OutputAdmittedByTopic::LockingScript {
            txid: "abc".into(),
            output_index: 0,
            topic: "tm_uhrp".into(),
            satoshis: 1,
            locking_script: script.clone(),
            off_chain_values: None,
        };
        svc.output_admitted_by_topic(&payload).await.unwrap();
        svc.output_admitted_by_topic(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 1);
    }

    #[tokio::test]
    async fn rejects_whole_tx_mode() {
        let svc = make_service();
        let payload = OutputAdmittedByTopic::WholeTx {
            atomic_beef: vec![],
            output_index: 0,
            topic: "tm_uhrp".into(),
            off_chain_values: None,
        };
        assert!(svc.output_admitted_by_topic(&payload).await.is_err());
    }

    // ---- spend / evict --------------------------------------------------

    #[tokio::test]
    async fn spend_deletes_record() {
        let (svc, storage) = make_service_with_storage();
        storage
            .store_record("tx1", 0, "uh", "ik", "u", 0, 1)
            .await
            .unwrap();
        let payload = OutputSpent::None {
            txid: "tx1".into(),
            output_index: 0,
            topic: "tm_uhrp".into(),
        };
        svc.output_spent(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 0);
    }

    #[tokio::test]
    async fn evict_deletes_record() {
        let (svc, storage) = make_service_with_storage();
        storage
            .store_record("tx1", 0, "uh", "ik", "u", 0, 1)
            .await
            .unwrap();
        svc.output_evicted("tx1", 0).await.unwrap();
        assert_eq!(storage.record_count(), 0);
    }

    // ---- lookup queries -------------------------------------------------

    #[tokio::test]
    async fn lookup_wrong_service_errors() {
        let svc = make_service();
        let q = LookupQuestion::new("ls_other", serde_json::json!({}));
        assert!(svc.lookup(&q).await.is_err());
    }

    #[tokio::test]
    async fn lookup_find_all_string_legacy() {
        let (svc, storage) = make_service_with_storage();
        storage
            .store_record("tx1", 0, "uh", "ik", "u", 0, 1)
            .await
            .unwrap();
        let q = LookupQuestion::new("ls_uhrp", serde_json::json!("findAll"));
        let results = svc.lookup(&q).await.unwrap();
        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn lookup_by_uhrp_url() {
        let (svc, storage) = make_service_with_storage();
        storage
            .store_record("tx1", 0, "aa", "key1", "u", 0, 1)
            .await
            .unwrap();
        storage
            .store_record("tx2", 0, "bb", "key2", "u", 0, 1)
            .await
            .unwrap();

        let q = LookupQuestion::new("ls_uhrp", serde_json::json!({"uhrpUrl": "aa"}));
        let results = svc.lookup(&q).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx1");
    }

    #[tokio::test]
    async fn lookup_by_identity_key() {
        let (svc, storage) = make_service_with_storage();
        storage
            .store_record("tx1", 0, "aa", "keyA", "u", 0, 1)
            .await
            .unwrap();
        storage
            .store_record("tx2", 0, "bb", "keyB", "u", 0, 1)
            .await
            .unwrap();

        let q = LookupQuestion::new("ls_uhrp", serde_json::json!({"hostIdentityKey": "keyB"}));
        let results = svc.lookup(&q).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx2");
    }

    #[tokio::test]
    async fn lookup_find_all_with_pagination() {
        let (svc, storage) = make_service_with_storage();
        for i in 0..7 {
            storage
                .store_record(&format!("tx{i}"), 0, "u", "k", "d", 0, 1)
                .await
                .unwrap();
        }
        let q = LookupQuestion::new(
            "ls_uhrp",
            serde_json::json!({"findAll": true, "limit": 3, "skip": 2}),
        );
        let results = svc.lookup(&q).await.unwrap();
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn lookup_zero_limit_errors() {
        let svc = make_service();
        let q = LookupQuestion::new("ls_uhrp", serde_json::json!({"findAll": true, "limit": 0}));
        assert!(svc.lookup(&q).await.is_err());
    }

    // ---- expired-record filter (task #33) -------------------------------

    /// Seed helper: load two records, one past-expired, one still valid,
    /// at `now = 1_700_000_000`.
    async fn seed_mixed_expiry(storage: &MemoryUHRPStorage) {
        // Past-expired (expiry=1_699_999_999, now=1_700_000_000 → filtered).
        storage
            .store_record("tx_past", 0, "uh_past", "ik", "u", 1_699_999_999, 1)
            .await
            .unwrap();
        // Still valid (expiry=1_700_000_100, now=1_700_000_000 → kept).
        storage
            .store_record("tx_live", 0, "uh_live", "ik", "u", 1_700_000_100, 1)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn lookup_default_hides_past_expiry_records() {
        let (svc, storage) = make_service_with_storage();
        seed_mixed_expiry(&storage).await;
        // `nowUnixSeconds` injection keeps the test deterministic across
        // wall-clock advances (see UHRPQuery docs). Not a wire field.
        let q = LookupQuestion::new(
            "ls_uhrp",
            serde_json::json!({
                "findAll": true,
                "nowUnixSeconds": 1_700_000_000i64,
            }),
        );
        let results = svc.lookup(&q).await.unwrap();
        assert_eq!(
            results.len(),
            1,
            "default filter must drop past-expiry records"
        );
        assert_eq!(results[0].txid, "tx_live");
    }

    #[tokio::test]
    async fn lookup_include_expired_returns_all() {
        let (svc, storage) = make_service_with_storage();
        seed_mixed_expiry(&storage).await;
        let q = LookupQuestion::new(
            "ls_uhrp",
            serde_json::json!({
                "findAll": true,
                "includeExpired": true,
                "nowUnixSeconds": 1_700_000_000i64,
            }),
        );
        let results = svc.lookup(&q).await.unwrap();
        assert_eq!(
            results.len(),
            2,
            "includeExpired=true must return all records"
        );
    }

    #[tokio::test]
    async fn lookup_zero_expiry_is_never_filtered() {
        let (svc, storage) = make_service_with_storage();
        // expiry=0 is the UHRP convention for "never expires" — must be
        // visible even when now is far in the future.
        storage
            .store_record("tx_forever", 0, "uh_forever", "ik", "u", 0, 1)
            .await
            .unwrap();
        let q = LookupQuestion::new(
            "ls_uhrp",
            serde_json::json!({
                "findAll": true,
                "nowUnixSeconds": 4_000_000_000i64, // year 2096
            }),
        );
        let results = svc.lookup(&q).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx_forever");
    }

    #[tokio::test]
    async fn lookup_by_identity_key_respects_expiry_filter() {
        // Filter must compose with other filters, not bypass them.
        let (svc, storage) = make_service_with_storage();
        storage
            .store_record("tx_a_past", 0, "uh", "keyA", "u", 1_699_999_999, 1)
            .await
            .unwrap();
        storage
            .store_record("tx_a_live", 0, "uh", "keyA", "u", 1_700_000_100, 1)
            .await
            .unwrap();
        storage
            .store_record("tx_b_live", 0, "uh", "keyB", "u", 1_700_000_100, 1)
            .await
            .unwrap();

        let q = LookupQuestion::new(
            "ls_uhrp",
            serde_json::json!({
                "hostIdentityKey": "keyA",
                "nowUnixSeconds": 1_700_000_000i64,
            }),
        );
        let results = svc.lookup(&q).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx_a_live");
    }

    // ---- Full lifecycle -------------------------------------------------

    #[tokio::test]
    async fn lifecycle_admit_query_spend_query() {
        let (svc, storage) = make_service_with_storage();
        let ident = sample_identity();
        let ident_hex = hex::encode(&ident);
        let hash = [0xAAu8; 32];
        let script =
            make_uhrp_locking_script(&ident, &hash, "https://n.example", 1_900_000_000, 4096);

        let admit = OutputAdmittedByTopic::LockingScript {
            txid: "live_tx".into(),
            output_index: 0,
            topic: "tm_uhrp".into(),
            satoshis: 1,
            locking_script: script,
            off_chain_values: None,
        };
        svc.output_admitted_by_topic(&admit).await.unwrap();
        assert_eq!(storage.record_count(), 1);

        let q = LookupQuestion::new("ls_uhrp", serde_json::json!({"hostIdentityKey": ident_hex}));
        let results = svc.lookup(&q).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "live_tx");

        let spend = OutputSpent::None {
            txid: "live_tx".into(),
            output_index: 0,
            topic: "tm_uhrp".into(),
        };
        svc.output_spent(&spend).await.unwrap();
        assert_eq!(storage.record_count(), 0);
    }
}
