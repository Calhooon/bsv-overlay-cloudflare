//! LOW Lookup Service — indexes and queries LOW poker lobby records.
//!
//! When outputs are admitted to `tm_low`, this service decodes the
//! PushDrop fields and stores them via [`LowStorage`]. Clients query
//! for open tables (with an optional stake range), by game ID, or by
//! host identity key. Structure mirrors `ship::lookup_service`.
//!
//! Spend semantics: a spent TABLE_OPEN = table closed; a spent
//! GAME_UTXO pointer = superseded by a newer pointer. Either way the
//! row is deleted — the index only ever holds live records.

use async_trait::async_trait;
use bsv_rs::script::templates::PushDrop;
use overlay_engine::lookup_service::{LookupService, LookupServiceError};
use overlay_engine::types::*;
use std::rc::Rc;
use tracing::{debug, warn};

use super::storage::{LowQuery, LowRecord, LowRecordType, LowStorage};
use super::topic_manager::{
    LOW_GAMEUTXO_FIELD_COUNT, LOW_GAMEUTXO_TAG, LOW_TABLE_FIELD_COUNT, LOW_TABLE_TAG,
};

/// LOW Lookup Service — indexes lobby records and answers queries.
pub struct LowLookupService {
    storage: Rc<dyn LowStorage>,
    /// Optional chain-tip source used to enforce table-expiry at query time
    /// (bsv-low #148). When present, `findOpenTables` hides rows whose
    /// `expiryHeight <= tip`. When absent (or the fetch fails) the query
    /// fails open — no expiry filter — so the lobby never goes dark on a
    /// transient ChainTracks outage.
    chain_tracker: Option<Rc<dyn bsv_rs::transaction::ChainTracker>>,
}

impl LowLookupService {
    /// Create a new LOW lookup service backed by the given storage.
    ///
    /// Constructed without a chain-tip source: `findOpenTables` applies no
    /// expiry filter until [`LowLookupService::with_chain_tracker`] wires one
    /// in (the deployed worker does; unit tests opt in via a mock).
    pub fn new(storage: Rc<dyn LowStorage>) -> Self {
        Self {
            storage,
            chain_tracker: None,
        }
    }

    /// Attach a chain-tip source so `findOpenTables` enforces table expiry.
    /// LOW-local: only the LOW lobby query consults it — the reveal index and
    /// all other services are untouched.
    pub fn with_chain_tracker(
        mut self,
        chain_tracker: Rc<dyn bsv_rs::transaction::ChainTracker>,
    ) -> Self {
        self.chain_tracker = Some(chain_tracker);
        self
    }

    /// Resolve the current chain tip for expiry filtering. Returns `None`
    /// (fail-open) when no source is wired or the fetch errors — an expired
    /// table lingering is a lesser evil than a lobby that shows nothing.
    async fn resolve_tip(&self) -> Option<u32> {
        match &self.chain_tracker {
            Some(ct) => match ct.current_height().await {
                Ok(h) => Some(h),
                // Fail-open, but NEVER silently. A swallowed tip fetch is exactly
                // how this filter became a no-op in production: the worker's
                // same-account `workers.dev` subrequest to ChainTracks 404s
                // (loopback), so `current_height()` errors and the filter falls
                // open. Surfacing it here is what makes that diagnosable. The
                // real fix is a Cloudflare service binding to ChainTracks (it
                // also affects the engine's SPV via the same tracker).
                Err(e) => {
                    warn!("ls_low: chain-tip fetch failed, expiry filter falling open: {e}");
                    None
                }
            },
            None => None,
        }
    }
}

#[async_trait(?Send)]
impl LookupService for LowLookupService {
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

        // Only index tm_low outputs
        if topic != "tm_low" {
            return Ok(());
        }

        // Decode PushDrop to extract fields
        let script = bsv_rs::script::Script::from_binary(locking_script)
            .map_err(|e| LookupServiceError::Other(format!("Script parse error: {e}")))?;
        let pushdrop = PushDrop::decode(&script.into())
            .map_err(|e| LookupServiceError::Other(format!("PushDrop decode error: {e}")))?;

        // The topic manager already fully validated the token; here we only
        // re-discriminate the record type to know what to index (same trust
        // model as SHIP's lookup service).
        let record = match Self::extract_record(txid, output_index, &pushdrop) {
            Some(r) => r,
            None => {
                debug!("LOW: admitted output is not an indexable LOW token — skipped");
                return Ok(());
            }
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
            } => (txid, *output_index, topic),
            _ => return Ok(()),
        };

        if topic != "tm_low" {
            return Ok(());
        }

        // Spent TABLE_OPEN = table closed; spent GAME_UTXO = superseded.
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
        if question.service != "ls_low" {
            return Err(LookupServiceError::Unsupported(format!(
                "Expected ls_low, got {}",
                question.service
            )));
        }

        let query: LowQuery = serde_json::from_value(question.query.clone())
            .map_err(|e| LookupServiceError::InvalidQuery(e.to_string()))?;

        let result = match query {
            LowQuery::FindOpenTables {
                stake_min,
                stake_max,
            } => {
                if let (Some(min), Some(max)) = (stake_min, stake_max) {
                    if min > max {
                        return Err(LookupServiceError::InvalidQuery(
                            "stakeMin must be <= stakeMax".into(),
                        ));
                    }
                }
                // Enforce table expiry at query time against the live chain
                // tip (bsv-low #148). `None` => fail-open (no expiry filter).
                let tip = self.resolve_tip().await;
                self.storage
                    .find_open_tables(stake_min, stake_max, tip)
                    .await
            }
            LowQuery::ByGameId { game_id } => {
                let game_id = normalize_hex(&game_id, 32, "gameId")?;
                self.storage.find_by_game_id(&game_id).await
            }
            LowQuery::ByHost { identity_key } => {
                let identity_key = normalize_hex(&identity_key, 33, "identityKey")?;
                self.storage.find_by_host(&identity_key).await
            }
        };

        result
            .map(LookupResult::OutputList)
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))
    }

    async fn get_documentation(&self) -> String {
        include_str!("../../docs/low_lookup.md").to_string()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "LOW Lookup Service".to_string(),
            description: Some(
                "Provides lookup capabilities for LOW poker lobby tokens.".to_string(),
            ),
            ..Default::default()
        }
    }
}

/// Validate a hex parameter and return it lowercased (stored values are
/// lowercase `hex::encode` output, so comparisons must be canonical).
fn normalize_hex(
    value: &str,
    expected_bytes: usize,
    param: &str,
) -> Result<String, LookupServiceError> {
    let lower = value.to_ascii_lowercase();
    if lower.len() != expected_bytes * 2 || !lower.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(LookupServiceError::InvalidQuery(format!(
            "{param} must be {} hex chars",
            expected_bytes * 2
        )));
    }
    Ok(lower)
}

impl LowLookupService {
    /// Decode an admitted `tm_low` PushDrop into an index record.
    ///
    /// Returns `None` when the output isn't one of the two LOW record
    /// shapes (defensive — the TM should never admit such an output).
    fn extract_record(txid: &str, output_index: u32, pushdrop: &PushDrop) -> Option<LowRecord> {
        if pushdrop.fields.is_empty() {
            return None;
        }

        match pushdrop.fields[0].as_slice() {
            tag if tag == LOW_TABLE_TAG => {
                if pushdrop.fields.len() != LOW_TABLE_FIELD_COUNT {
                    return None;
                }
                let stake_sats =
                    u64::from_le_bytes(pushdrop.fields[3].as_slice().try_into().ok()?);
                let expiry_height =
                    u32::from_le_bytes(pushdrop.fields[6].as_slice().try_into().ok()?);
                Some(LowRecord {
                    record_type: LowRecordType::Table,
                    txid: txid.to_string(),
                    output_index,
                    host_identity: hex::encode(&pushdrop.fields[1]),
                    game_id: hex::encode(&pushdrop.fields[2]),
                    stake_sats: Some(stake_sats),
                    rules_hash: Some(hex::encode(&pushdrop.fields[4])),
                    relay_url: Some(String::from_utf8_lossy(&pushdrop.fields[5]).to_string()),
                    expiry_height: Some(expiry_height),
                })
            }
            tag if tag == LOW_GAMEUTXO_TAG => {
                if pushdrop.fields.len() != LOW_GAMEUTXO_FIELD_COUNT {
                    return None;
                }
                Some(LowRecord {
                    record_type: LowRecordType::GameUtxo,
                    txid: txid.to_string(),
                    output_index,
                    host_identity: hex::encode(&pushdrop.fields[1]),
                    game_id: hex::encode(&pushdrop.fields[2]),
                    stake_sats: None,
                    rules_hash: None,
                    relay_url: None,
                    expiry_height: None,
                })
            }
            _ => None,
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::super::storage::MemoryLowStorage;
    use super::super::topic_manager::tests::{
        game_utxo_data_fields, make_signed_low_output, table_open_data_fields,
    };
    use super::*;
    use bsv_rs::primitives::ec::PrivateKey;

    fn make_service_with_storage() -> (LowLookupService, Rc<MemoryLowStorage>) {
        let storage = Rc::new(MemoryLowStorage::new());
        let svc = LowLookupService::new(storage.clone());
        (svc, storage)
    }

    fn make_service() -> LowLookupService {
        make_service_with_storage().0
    }

    /// Binary locking script for a signed TABLE_OPEN token.
    fn table_open_script(signer: &PrivateKey, stake: u64) -> Vec<u8> {
        let fields = table_open_data_fields(signer, stake, "https://relay.example.com", 900000);
        make_signed_low_output(signer, fields)
            .locking_script
            .to_binary()
    }

    /// Binary locking script for a signed GAME_UTXO token.
    fn game_utxo_script(signer: &PrivateKey) -> Vec<u8> {
        make_signed_low_output(signer, game_utxo_data_fields(signer, 0))
            .locking_script
            .to_binary()
    }

    fn admit(txid: &str, output_index: u32, script: Vec<u8>) -> OutputAdmittedByTopic {
        OutputAdmittedByTopic::LockingScript {
            txid: txid.into(),
            output_index,
            topic: "tm_low".into(),
            satoshis: 1,
            locking_script: script,
            off_chain_values: None,
        }
    }

    // ── Trait plumbing ───────────────────────────────────────────────────

    #[tokio::test]
    async fn modes_and_metadata() {
        let svc = make_service();
        assert_eq!(svc.admission_mode(), AdmissionMode::LockingScript);
        assert_eq!(svc.spend_notification_mode(), SpendNotificationMode::None);
        let meta = svc.get_metadata().await;
        assert_eq!(meta.name, "LOW Lookup Service");
        assert!(!svc.get_documentation().await.is_empty());
    }

    // ── Admission ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn admit_table_open_stores_full_record() {
        let (svc, storage) = make_service_with_storage();
        let signer = PrivateKey::random();
        svc.output_admitted_by_topic(&admit("tx1", 0, table_open_script(&signer, 2500)))
            .await
            .unwrap();
        assert_eq!(storage.record_count(), 1);

        // Findable through the stake filter
        let q = LookupQuestion::new(
            "ls_low",
            serde_json::json!({"type": "findOpenTables", "stakeMin": 2000, "stakeMax": 3000}),
        );
        let results = svc.lookup(&q).await.unwrap().into_outputs().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx1");
        assert_eq!(results[0].output_index, 0);
    }

    #[tokio::test]
    async fn admit_game_utxo_stores_pointer_record() {
        let (svc, storage) = make_service_with_storage();
        let signer = PrivateKey::random();
        svc.output_admitted_by_topic(&admit("tx2", 1, game_utxo_script(&signer)))
            .await
            .unwrap();
        assert_eq!(storage.record_count(), 1);

        // gameId in the test fixtures is [0x11; 32]
        let q = LookupQuestion::new(
            "ls_low",
            serde_json::json!({"type": "byGameId", "gameId": "11".repeat(32)}),
        );
        let results = svc.lookup(&q).await.unwrap().into_outputs().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx2");
        assert_eq!(results[0].output_index, 1);
    }

    #[tokio::test]
    async fn ignores_non_tm_low_topic() {
        let (svc, storage) = make_service_with_storage();
        let signer = PrivateKey::random();
        let mut payload = admit("tx1", 0, table_open_script(&signer, 1000));
        if let OutputAdmittedByTopic::LockingScript { ref mut topic, .. } = payload {
            *topic = "tm_other".into();
        }
        svc.output_admitted_by_topic(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 0);
    }

    #[tokio::test]
    async fn ignores_non_low_pushdrop() {
        let (svc, storage) = make_service_with_storage();
        let signer = PrivateKey::random();
        let mut fields = table_open_data_fields(&signer, 1000, "https://r.example.com", 900000);
        fields[0] = b"SHIP".to_vec();
        let script = make_signed_low_output(&signer, fields)
            .locking_script
            .to_binary();
        svc.output_admitted_by_topic(&admit("tx1", 0, script))
            .await
            .unwrap();
        assert_eq!(storage.record_count(), 0);
    }

    #[tokio::test]
    async fn rejects_whole_tx_mode() {
        let svc = make_service();
        let payload = OutputAdmittedByTopic::WholeTx {
            atomic_beef: vec![],
            output_index: 0,
            topic: "tm_low".into(),
            off_chain_values: None,
        };
        assert!(svc.output_admitted_by_topic(&payload).await.is_err());
    }

    // ── Lookup queries ───────────────────────────────────────────────────

    #[tokio::test]
    async fn lookup_wrong_service_errors() {
        let svc = make_service();
        let q = LookupQuestion::new("ls_ship", serde_json::json!({"type": "findOpenTables"}));
        assert!(svc.lookup(&q).await.is_err());
    }

    #[tokio::test]
    async fn lookup_invalid_query_errors() {
        let svc = make_service();
        for bad in [
            serde_json::json!({"type": "unknownQuery"}),
            serde_json::json!("findOpenTables"),
            serde_json::json!(42),
            serde_json::json!({"type": "byGameId", "gameId": "zz".repeat(32)}),
            serde_json::json!({"type": "byGameId", "gameId": "ab"}),
            serde_json::json!({"type": "byHost", "identityKey": "02"}),
            serde_json::json!({"type": "findOpenTables", "stakeMin": 10, "stakeMax": 1}),
        ] {
            let q = LookupQuestion::new("ls_low", bad.clone());
            assert!(svc.lookup(&q).await.is_err(), "expected error for {bad}");
        }
    }

    #[tokio::test]
    async fn lookup_by_host_and_case_insensitive_hex() {
        let (svc, _storage) = make_service_with_storage();
        let signer = PrivateKey::random();
        let identity = bsv_rs::wallet::ProtoWallet::new(Some(signer.clone())).identity_key_hex();
        svc.output_admitted_by_topic(&admit("tx1", 0, table_open_script(&signer, 1000)))
            .await
            .unwrap();

        // Query with uppercase hex — must still match
        let q = LookupQuestion::new(
            "ls_low",
            serde_json::json!({"type": "byHost", "identityKey": identity.to_uppercase()}),
        );
        let results = svc.lookup(&q).await.unwrap().into_outputs().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx1");
    }

    #[tokio::test]
    async fn find_open_tables_empty_when_no_records() {
        let svc = make_service();
        let q = LookupQuestion::new("ls_low", serde_json::json!({"type": "findOpenTables"}));
        let results = svc.lookup(&q).await.unwrap().into_outputs().unwrap();
        assert!(results.is_empty());
    }

    // ── Query-time expiry enforcement (bsv-low #148) ──────────────────────

    /// Admit a TABLE_OPEN with an explicit expiry block height.
    fn table_open_script_expiry(signer: &PrivateKey, stake: u64, expiry: u32) -> Vec<u8> {
        let fields = table_open_data_fields(signer, stake, "https://relay.example.com", expiry);
        make_signed_low_output(signer, fields)
            .locking_script
            .to_binary()
    }

    #[tokio::test]
    async fn find_open_tables_hides_expired_with_chain_tip() {
        let storage = Rc::new(MemoryLowStorage::new());
        // Chain tip = 899_500: the 899_000 table is expired, the 900_000 fresh.
        let svc = LowLookupService::new(storage.clone())
            .with_chain_tracker(Rc::new(bsv_rs::transaction::MockChainTracker::new(899_500)));
        let signer = PrivateKey::random();

        svc.output_admitted_by_topic(&admit(
            "fresh",
            0,
            table_open_script_expiry(&signer, 1000, 900_000),
        ))
        .await
        .unwrap();
        svc.output_admitted_by_topic(&admit(
            "stale",
            0,
            table_open_script_expiry(&signer, 1000, 899_000),
        ))
        .await
        .unwrap();

        let q = LookupQuestion::new("ls_low", serde_json::json!({"type": "findOpenTables"}));
        let results = svc.lookup(&q).await.unwrap().into_outputs().unwrap();
        assert_eq!(results.len(), 1, "expired table must be excluded at query time");
        assert_eq!(results[0].txid, "fresh");
    }

    #[tokio::test]
    async fn find_open_tables_no_chain_tip_shows_all() {
        // No chain tracker wired → fail-open: both tables (even the expired
        // one) return rather than blanking the lobby.
        let (svc, _storage) = make_service_with_storage();
        let signer = PrivateKey::random();

        svc.output_admitted_by_topic(&admit(
            "fresh",
            0,
            table_open_script_expiry(&signer, 1000, 900_000),
        ))
        .await
        .unwrap();
        svc.output_admitted_by_topic(&admit(
            "stale",
            0,
            table_open_script_expiry(&signer, 1000, 1),
        ))
        .await
        .unwrap();

        let q = LookupQuestion::new("ls_low", serde_json::json!({"type": "findOpenTables"}));
        let results = svc.lookup(&q).await.unwrap().into_outputs().unwrap();
        assert_eq!(results.len(), 2, "tip unavailable must fail open");
    }

    // ── Full lifecycle: admit → query → spend → query empty ─────────────

    #[tokio::test]
    async fn lifecycle_admit_query_spend_query() {
        let (svc, storage) = make_service_with_storage();
        let signer = PrivateKey::random();

        svc.output_admitted_by_topic(&admit("table_tx", 0, table_open_script(&signer, 1000)))
            .await
            .unwrap();
        assert_eq!(storage.record_count(), 1);

        let q = LookupQuestion::new("ls_low", serde_json::json!({"type": "findOpenTables"}));
        let results = svc.lookup(&q).await.unwrap().into_outputs().unwrap();
        assert_eq!(results.len(), 1);

        // Spend = table closed
        let spend = OutputSpent::None {
            txid: "table_tx".into(),
            output_index: 0,
            topic: "tm_low".into(),
        };
        svc.output_spent(&spend).await.unwrap();
        assert_eq!(storage.record_count(), 0);

        let results = svc.lookup(&q).await.unwrap().into_outputs().unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn spend_ignores_other_topics() {
        let (svc, storage) = make_service_with_storage();
        let signer = PrivateKey::random();
        svc.output_admitted_by_topic(&admit("tx1", 0, table_open_script(&signer, 1000)))
            .await
            .unwrap();

        let spend = OutputSpent::None {
            txid: "tx1".into(),
            output_index: 0,
            topic: "tm_ship".into(),
        };
        svc.output_spent(&spend).await.unwrap();
        assert_eq!(storage.record_count(), 1);
    }

    #[tokio::test]
    async fn eviction_deletes_record() {
        let (svc, storage) = make_service_with_storage();
        let signer = PrivateKey::random();
        svc.output_admitted_by_topic(&admit("tx1", 0, game_utxo_script(&signer)))
            .await
            .unwrap();
        assert_eq!(storage.record_count(), 1);

        svc.output_evicted("tx1", 0).await.unwrap();
        assert_eq!(storage.record_count(), 0);
    }
}
