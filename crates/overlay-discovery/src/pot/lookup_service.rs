//! POT Lookup Service — indexes pot covenant spends and answers
//! "is this pot outpoint spent, and by which txid?".
//!
//! When an output is admitted to `tm_pot` this service records it
//! (`spent = false`) via [`PotStorage`]. When the engine later sees the
//! SPENDING (settle / refund / sweep) tx, it fires the spend notification
//! and this service PERSISTS the spender — `spent = true` + the
//! `spendingTxid`. That persisted spend is the on-chain landing proof a LOW
//! client requires before crediting a payout.
//!
//! # The ONE difference from `ls_reveal`
//!
//! `ls_reveal` uses [`SpendNotificationMode::None`] and no-ops on spend (a
//! reveal is never spent). `ls_pot` uses [`SpendNotificationMode::Txid`] and
//! records the spend. Neither ever DELETES a record — the pot record, spent
//! or not, is permanent history. `output_evicted` is a deliberate no-op for
//! the same reason (contrast `ls_low`, where an evicted/spent row is
//! removed).

use async_trait::async_trait;
use overlay_engine::lookup_service::{LookupService, LookupServiceError};
use overlay_engine::types::*;
use std::rc::Rc;
use tracing::debug;

use super::is_pot_covenant_script;
use super::storage::{PotQuery, PotRecord, PotStorage};

/// POT Lookup Service — indexes pot spends and answers spent-status queries.
pub struct PotLookupService {
    storage: Rc<dyn PotStorage>,
}

impl PotLookupService {
    /// Create a new POT lookup service backed by the given storage.
    pub fn new(storage: Rc<dyn PotStorage>) -> Self {
        Self { storage }
    }
}

#[async_trait(?Send)]
impl LookupService for PotLookupService {
    fn admission_mode(&self) -> AdmissionMode {
        AdmissionMode::LockingScript
    }

    fn spend_notification_mode(&self) -> SpendNotificationMode {
        // We NEED the spend: recording it is this index's whole purpose.
        // `Txid` gives us the spending (settle) txid — the landing proof.
        SpendNotificationMode::Txid
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

        // Only index tm_pot outputs.
        if topic != "tm_pot" {
            return Ok(());
        }

        // The topic manager already recognized the covenant; re-check
        // defensively (the TM should never admit a non-covenant output here).
        if !is_pot_covenant_script(locking_script) {
            debug!("POT: admitted output is not a pot covenant script — skipped");
            return Ok(());
        }

        let record = PotRecord {
            txid: txid.to_string(),
            output_index,
            spent: false,
            spending_txid: None,
        };

        self.storage
            .store_record(&record)
            .await
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;

        Ok(())
    }

    async fn output_spent(&self, payload: &OutputSpent) -> Result<(), LookupServiceError> {
        // We opted into `SpendNotificationMode::Txid`, so the engine delivers
        // the `Txid` variant: `txid`/`output_index` = the SPENT pot outpoint,
        // `spending_txid` = the SPENDER (settle) txid.
        let (txid, output_index, topic, spending_txid) = match payload {
            OutputSpent::Txid {
                txid,
                output_index,
                topic,
                spending_txid,
            } => (txid, *output_index, topic, spending_txid),
            // Any other variant (None/Script/WholeTx) carries no spender we
            // opted into; nothing to persist.
            _ => return Ok(()),
        };

        if topic != "tm_pot" {
            return Ok(());
        }

        // PERSIST the spender — never delete. This is the landing proof.
        self.storage
            .mark_spent(txid, output_index, spending_txid)
            .await
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;

        Ok(())
    }

    async fn output_evicted(
        &self,
        _txid: &str,
        _output_index: u32,
    ) -> Result<(), LookupServiceError> {
        // No-op: a pot record (spent or not) is permanent landing-proof
        // history and is never removed (mirrors ls_reveal's permanence).
        Ok(())
    }

    async fn lookup(&self, question: &LookupQuestion) -> Result<LookupResult, LookupServiceError> {
        if question.service != "ls_pot" {
            return Err(LookupServiceError::Unsupported(format!(
                "Expected ls_pot, got {}",
                question.service
            )));
        }

        let query: PotQuery = serde_json::from_value(question.query.clone())
            .map_err(|e| LookupServiceError::InvalidQuery(e.to_string()))?;

        let PotQuery::SpentStatus { outpoints } = query;

        // Build an input-ordered array: one entry per requested outpoint.
        let mut entries = Vec::with_capacity(outpoints.len());
        for op in &outpoints {
            // txids are canonical lowercase hex on-chain; normalize the query
            // so an uppercase-hex client still matches the stored record.
            let key = op.txid.to_ascii_lowercase();
            let record: Option<PotRecord> = self
                .storage
                .get_spent_status(&key, op.vout)
                .await
                .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;

            // Fail-safe: an outpoint we never admitted is `known:false` with
            // null spent/spendingTxid — never assert "unspent" for an output
            // we never saw.
            let (known, spent, spending_txid) = match record {
                Some(r) => (
                    true,
                    serde_json::Value::Bool(r.spent),
                    r.spending_txid
                        .map(serde_json::Value::String)
                        .unwrap_or(serde_json::Value::Null),
                ),
                None => (
                    false,
                    serde_json::Value::Null,
                    serde_json::Value::Null,
                ),
            };

            entries.push(serde_json::json!({
                "txid": op.txid,
                "vout": op.vout,
                "known": known,
                "spent": spent,
                "spendingTxid": spending_txid,
            }));
        }

        Ok(LookupResult::Answer(LookupAnswer::Freeform {
            result: serde_json::Value::Array(entries),
        }))
    }

    async fn get_documentation(&self) -> String {
        include_str!("../../docs/pot_lookup.md").to_string()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "POT Lookup Service".to_string(),
            description: Some(
                "Answers spent-status queries over LOW pot covenant outpoints \
                 (txid:vout → spent? + spendingTxid)."
                    .to_string(),
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
    use super::super::storage::MemoryPotStorage;
    use super::super::topic_manager::tests::{covenant_script, dummy_params};
    use super::*;

    fn make_service_with_storage() -> (PotLookupService, Rc<MemoryPotStorage>) {
        let storage = Rc::new(MemoryPotStorage::new());
        let svc = PotLookupService::new(storage.clone());
        (svc, storage)
    }

    fn make_service() -> PotLookupService {
        make_service_with_storage().0
    }

    fn admit(txid: &str, output_index: u32, script: Vec<u8>) -> OutputAdmittedByTopic {
        OutputAdmittedByTopic::LockingScript {
            txid: txid.into(),
            output_index,
            topic: "tm_pot".into(),
            satoshis: 2500,
            locking_script: script,
            off_chain_values: None,
        }
    }

    fn spent(txid: &str, output_index: u32, spending_txid: &str) -> OutputSpent {
        OutputSpent::Txid {
            txid: txid.into(),
            output_index,
            topic: "tm_pot".into(),
            spending_txid: spending_txid.into(),
        }
    }

    /// Run a spentStatus lookup and return the JSON array.
    async fn spent_status(svc: &PotLookupService, outpoints: serde_json::Value) -> serde_json::Value {
        let q = LookupQuestion::new(
            "ls_pot",
            serde_json::json!({"type": "spentStatus", "outpoints": outpoints}),
        );
        match svc.lookup(&q).await.unwrap() {
            LookupResult::Answer(LookupAnswer::Freeform { result }) => result,
            other => panic!("expected Freeform answer, got {other:?}"),
        }
    }

    // ── Trait plumbing ───────────────────────────────────────────────────

    #[tokio::test]
    async fn modes_and_metadata() {
        let svc = make_service();
        assert_eq!(svc.admission_mode(), AdmissionMode::LockingScript);
        assert_eq!(svc.spend_notification_mode(), SpendNotificationMode::Txid);
        let meta = svc.get_metadata().await;
        assert_eq!(meta.name, "POT Lookup Service");
        assert!(!svc.get_documentation().await.is_empty());
    }

    // ── Admit → known, unspent ───────────────────────────────────────────

    #[tokio::test]
    async fn admit_then_lookup_known_unspent() {
        let (svc, storage) = make_service_with_storage();
        svc.output_admitted_by_topic(&admit("pot1", 0, covenant_script(&dummy_params())))
            .await
            .unwrap();
        assert_eq!(storage.record_count(), 1);

        let arr = spent_status(&svc, serde_json::json!([{"txid": "pot1", "vout": 0}])).await;
        let e = &arr[0];
        assert_eq!(e["txid"], "pot1");
        assert_eq!(e["vout"], 0);
        assert_eq!(e["known"], true);
        assert_eq!(e["spent"], false);
        assert!(e["spendingTxid"].is_null());
    }

    // ── Spend → known, spent + spendingTxid ──────────────────────────────

    #[tokio::test]
    async fn spend_then_lookup_spent_with_spender() {
        let (svc, _storage) = make_service_with_storage();
        svc.output_admitted_by_topic(&admit("pot1", 0, covenant_script(&dummy_params())))
            .await
            .unwrap();
        // The settle spends the pot input.
        svc.output_spent(&spent("pot1", 0, "settleTx")).await.unwrap();

        let arr = spent_status(&svc, serde_json::json!([{"txid": "pot1", "vout": 0}])).await;
        let e = &arr[0];
        assert_eq!(e["known"], true);
        assert_eq!(e["spent"], true);
        assert_eq!(e["spendingTxid"], "settleTx");
    }

    // ── Unknown → fail-safe known:false ──────────────────────────────────

    #[tokio::test]
    async fn unknown_outpoint_known_false() {
        let svc = make_service();
        let arr = spent_status(&svc, serde_json::json!([{"txid": "ghost", "vout": 0}])).await;
        let e = &arr[0];
        assert_eq!(e["known"], false);
        assert!(e["spent"].is_null());
        assert!(e["spendingTxid"].is_null());
    }

    // ── Input-ordered, mixed batch ───────────────────────────────────────

    #[tokio::test]
    async fn batch_is_input_ordered_and_mixed() {
        let (svc, _storage) = make_service_with_storage();
        // txids are canonical lowercase hex on-chain.
        svc.output_admitted_by_topic(&admit("potunspent", 0, covenant_script(&dummy_params())))
            .await
            .unwrap();
        svc.output_admitted_by_topic(&admit("potspent", 1, covenant_script(&dummy_params())))
            .await
            .unwrap();
        svc.output_spent(&spent("potspent", 1, "settletx")).await.unwrap();

        let arr = spent_status(
            &svc,
            serde_json::json!([
                {"txid": "potspent", "vout": 1},
                {"txid": "ghost", "vout": 7},
                {"txid": "potunspent", "vout": 0}
            ]),
        )
        .await;
        let arr = arr.as_array().unwrap();
        assert_eq!(arr.len(), 3);

        // Order preserved.
        assert_eq!(arr[0]["txid"], "potspent");
        assert_eq!(arr[0]["spent"], true);
        assert_eq!(arr[0]["spendingTxid"], "settletx");

        assert_eq!(arr[1]["txid"], "ghost");
        assert_eq!(arr[1]["known"], false);

        assert_eq!(arr[2]["txid"], "potunspent");
        assert_eq!(arr[2]["known"], true);
        assert_eq!(arr[2]["spent"], false);
    }

    // ── Case-insensitive txid ────────────────────────────────────────────

    #[tokio::test]
    async fn lookup_case_insensitive_txid() {
        let (svc, _storage) = make_service_with_storage();
        // Admitted with a lowercase hex txid (as the engine delivers it).
        svc.output_admitted_by_topic(&admit(&"ab".repeat(32), 0, covenant_script(&dummy_params())))
            .await
            .unwrap();
        // Query with uppercase hex — still matches.
        let arr = spent_status(
            &svc,
            serde_json::json!([{"txid": "AB".repeat(32), "vout": 0}]),
        )
        .await;
        assert_eq!(arr[0]["known"], true);
    }

    // ── Admission filters ────────────────────────────────────────────────

    #[tokio::test]
    async fn ignores_non_tm_pot_topic() {
        let (svc, storage) = make_service_with_storage();
        let mut payload = admit("pot1", 0, covenant_script(&dummy_params()));
        if let OutputAdmittedByTopic::LockingScript { ref mut topic, .. } = payload {
            *topic = "tm_low".into();
        }
        svc.output_admitted_by_topic(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 0);
    }

    #[tokio::test]
    async fn ignores_non_covenant_script() {
        let (svc, storage) = make_service_with_storage();
        // A P2PKH — not a covenant.
        let p2pkh = bsv_rs::script::LockingScript::from_hex(
            "76a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac",
        )
        .unwrap()
        .to_binary();
        svc.output_admitted_by_topic(&admit("pot1", 0, p2pkh))
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
            topic: "tm_pot".into(),
            off_chain_values: None,
        };
        assert!(svc.output_admitted_by_topic(&payload).await.is_err());
    }

    // ── Spend filters + permanence ───────────────────────────────────────

    #[tokio::test]
    async fn spend_ignores_other_topics() {
        let (svc, _storage) = make_service_with_storage();
        svc.output_admitted_by_topic(&admit("pot1", 0, covenant_script(&dummy_params())))
            .await
            .unwrap();

        let mut sp = spent("pot1", 0, "settleTx");
        if let OutputSpent::Txid { ref mut topic, .. } = sp {
            *topic = "tm_low".into();
        }
        svc.output_spent(&sp).await.unwrap();

        // Wrong topic → not recorded as spent.
        let arr = spent_status(&svc, serde_json::json!([{"txid": "pot1", "vout": 0}])).await;
        assert_eq!(arr[0]["spent"], false);
    }

    #[tokio::test]
    async fn spend_none_variant_is_noop() {
        let (svc, _storage) = make_service_with_storage();
        svc.output_admitted_by_topic(&admit("pot1", 0, covenant_script(&dummy_params())))
            .await
            .unwrap();
        // A `None`-variant spend carries no spender we opted into → no-op.
        svc.output_spent(&OutputSpent::None {
            txid: "pot1".into(),
            output_index: 0,
            topic: "tm_pot".into(),
        })
        .await
        .unwrap();
        let arr = spent_status(&svc, serde_json::json!([{"txid": "pot1", "vout": 0}])).await;
        assert_eq!(arr[0]["spent"], false);
    }

    #[tokio::test]
    async fn eviction_does_not_remove_record() {
        let (svc, storage) = make_service_with_storage();
        svc.output_admitted_by_topic(&admit("pot1", 0, covenant_script(&dummy_params())))
            .await
            .unwrap();
        svc.output_spent(&spent("pot1", 0, "settleTx")).await.unwrap();

        // Eviction is a no-op — the landing proof is permanent.
        svc.output_evicted("pot1", 0).await.unwrap();
        assert_eq!(storage.record_count(), 1);
        let arr = spent_status(&svc, serde_json::json!([{"txid": "pot1", "vout": 0}])).await;
        assert_eq!(arr[0]["spent"], true);
        assert_eq!(arr[0]["spendingTxid"], "settleTx");
    }

    // ── Query validation ─────────────────────────────────────────────────

    #[tokio::test]
    async fn lookup_wrong_service_errors() {
        let svc = make_service();
        let q = LookupQuestion::new(
            "ls_low",
            serde_json::json!({"type": "spentStatus", "outpoints": []}),
        );
        assert!(svc.lookup(&q).await.is_err());
    }

    #[tokio::test]
    async fn lookup_invalid_query_errors() {
        let svc = make_service();
        for bad in [
            serde_json::json!({"type": "unknownQuery"}),
            serde_json::json!("spentStatus"),
            serde_json::json!(42),
            serde_json::json!({"type": "spentStatus"}), // missing outpoints
            serde_json::json!({"type": "spentStatus", "outpoints": [{"txid": "x"}]}), // missing vout
        ] {
            let q = LookupQuestion::new("ls_pot", bad.clone());
            assert!(svc.lookup(&q).await.is_err(), "expected error for {bad}");
        }
    }

    #[tokio::test]
    async fn empty_outpoints_returns_empty_array() {
        let svc = make_service();
        let arr = spent_status(&svc, serde_json::json!([])).await;
        assert!(arr.as_array().unwrap().is_empty());
    }
}
