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
//! # Differences from `ls_reveal`
//!
//! `ls_reveal` uses [`SpendNotificationMode::None`] and no-ops on spend (a
//! reveal is never spent). `ls_pot` opts into spend notifications and
//! records the spend. Neither ever DELETES a record — the pot record, spent
//! or not, is permanent history. `output_evicted` is a deliberate no-op for
//! the same reason (contrast `ls_low`, where an evicted/spent row is
//! removed).
//!
//! # Why `WholeTx` mode — the durable BEEF store
//!
//! Both hooks run in whole-tx mode ([`AdmissionMode::WholeTx`] /
//! [`SpendNotificationMode::WholeTx`]) so this service receives the full
//! Atomic BEEF of the funding tx (on admit) and of the spending settle /
//! refund tx (on spend), and durably persists both via
//! [`PotStorage::store_beef`], keyed by each tx's OWN txid. The store exists
//! because the engine's `transactions` table is LIFECYCLE-MANAGED: a BEEF
//! row is only written by `insert_output` — so a settle, which admits no
//! outputs, never enters it — and is DELETED by the deep-delete when a spent
//! unretained coin is cleaned up. `pot_beefs` is OURS (mirrors how
//! `pot_records` survives eviction) and is the durable source
//! `low-app-layer`'s `/beef/:txid` serves.

use async_trait::async_trait;
use bsv_rs::transaction::Transaction;
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
        // Whole-tx: we persist the FULL funding BEEF (the engine's
        // `transactions` table is lifecycle-managed — see the module docs).
        AdmissionMode::WholeTx
    }

    fn spend_notification_mode(&self) -> SpendNotificationMode {
        // We NEED the spend: recording it is this index's whole purpose.
        // `WholeTx` gives us the spending (settle) tx's full BEEF — the
        // landing proof AND the durable copy /beef/:txid serves.
        SpendNotificationMode::WholeTx
    }

    async fn output_admitted_by_topic(
        &self,
        payload: &OutputAdmittedByTopic,
    ) -> Result<(), LookupServiceError> {
        let (atomic_beef, output_index, topic) = match payload {
            OutputAdmittedByTopic::WholeTx {
                atomic_beef,
                output_index,
                topic,
                ..
            } => (atomic_beef, *output_index, topic),
            _ => return Err(LookupServiceError::Other("Expected whole-tx mode".into())),
        };

        // Only index tm_pot outputs.
        if topic != "tm_pot" {
            return Ok(());
        }

        // Parse the funding tx out of the BEEF (same subject-tx selection the
        // topic manager used to admit it). Unparseable → no-op, never a
        // spurious record.
        let tx = match Transaction::from_beef(atomic_beef, None) {
            Ok(tx) => tx,
            Err(e) => {
                debug!("POT: admitted beef did not parse — skipped: {e}");
                return Ok(());
            }
        };

        // The topic manager already recognized the covenant; re-check
        // defensively (the TM should never admit a non-covenant output here).
        let is_covenant = tx
            .outputs
            .get(output_index as usize)
            .is_some_and(|o| is_pot_covenant_script(&o.locking_script.to_binary()));
        if !is_covenant {
            debug!("POT: admitted output is not a pot covenant script — skipped");
            return Ok(());
        }

        // The funding txid comes from the parsed tx (whole-tx payloads carry
        // no txid field).
        let txid = tx.id();

        let record = PotRecord {
            txid: txid.clone(),
            output_index,
            spent: false,
            spending_txid: None,
        };

        self.storage
            .store_record(&record)
            .await
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;

        // Durably persist the FUNDING beef under the funding txid (the
        // engine's own copy is lifecycle-managed and may be deleted).
        self.storage
            .store_beef(&txid, atomic_beef)
            .await
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;

        Ok(())
    }

    async fn output_spent(&self, payload: &OutputSpent) -> Result<(), LookupServiceError> {
        // We opted into `SpendNotificationMode::WholeTx`, so the engine
        // delivers the `WholeTx` variant: `txid`/`output_index` = the SPENT
        // pot outpoint; the SPENDER (settle) is the subject tx of
        // `spending_atomic_beef` — its txid comes from parsing the beef.
        let (txid, output_index, topic, spending_atomic_beef) = match payload {
            OutputSpent::WholeTx {
                txid,
                output_index,
                topic,
                spending_atomic_beef,
                ..
            } => (txid, *output_index, topic, spending_atomic_beef),
            // Any other variant (None/Txid/Script) is not the mode we opted
            // into; nothing to persist.
            _ => return Ok(()),
        };

        if topic != "tm_pot" {
            return Ok(());
        }

        // Derive the SPENDING txid from the beef's subject tx. Unparseable →
        // no-op (the engine parsed this beef to process the submission, so a
        // delivered payload always parses in practice).
        let spending_tx = match Transaction::from_beef(spending_atomic_beef, None) {
            Ok(tx) => tx,
            Err(e) => {
                debug!("POT: spending beef did not parse — skipped: {e}");
                return Ok(());
            }
        };
        let spending_txid = spending_tx.id();

        // PERSIST the spender — never delete. This is the landing proof.
        self.storage
            .mark_spent(txid, output_index, &spending_txid)
            .await
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;

        // Durably persist the SETTLE/refund beef under the SPENDER's txid —
        // the settle admits no outputs, so the engine's `transactions` table
        // never gets a row for it. This store is the only durable copy.
        self.storage
            .store_beef(&spending_txid, spending_atomic_beef)
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
    use super::super::topic_manager::tests::{dummy_params, make_covenant_output};
    use super::*;
    use bsv_rs::script::LockingScript;
    use bsv_rs::transaction::{Transaction as Tx, TransactionInput, TransactionOutput};

    fn make_service_with_storage() -> (PotLookupService, Rc<MemoryPotStorage>) {
        let storage = Rc::new(MemoryPotStorage::new());
        let svc = PotLookupService::new(storage.clone());
        (svc, storage)
    }

    fn make_service() -> PotLookupService {
        make_service_with_storage().0
    }

    /// A P2PKH output (a settle payout / change — not a covenant).
    fn p2pkh_output() -> TransactionOutput {
        TransactionOutput {
            satoshis: Some(546),
            locking_script: LockingScript::from_hex(
                "76a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac",
            )
            .unwrap(),
            change: false,
        }
    }

    /// A funding tx: one input (`salt`-derived outpoint, so distinct salts
    /// give distinct txids), vout 0 = the covenant pot.
    fn funding_tx(salt: u8) -> Tx {
        let mut tx = Tx::new();
        tx.add_input(TransactionInput::new(hex::encode([salt; 32]), 0))
            .unwrap();
        tx.add_output(make_covenant_output(&dummy_params())).unwrap();
        tx
    }

    /// A settle tx: spends `pot_txid:vout`, pays plain P2PKH (admits no
    /// outputs to tm_pot). Its own `id()` is the SPENDING txid the service
    /// must derive from the beef.
    fn settle_tx(pot_txid: &str, vout: u32) -> Tx {
        let mut tx = Tx::new();
        tx.add_input(TransactionInput::new(pot_txid.to_string(), vout))
            .unwrap();
        tx.add_output(p2pkh_output()).unwrap();
        tx
    }

    /// A real mini-BEEF for `tx` (allow_partial: inputs carry no ancestry).
    fn beef_of(tx: &Tx) -> Vec<u8> {
        tx.to_beef(true).expect("BEEF serialization")
    }

    fn admit(atomic_beef: Vec<u8>, output_index: u32) -> OutputAdmittedByTopic {
        OutputAdmittedByTopic::WholeTx {
            atomic_beef,
            output_index,
            topic: "tm_pot".into(),
            off_chain_values: None,
        }
    }

    /// `pot_txid`/`output_index` = the SPENT pot outpoint; the spender is the
    /// subject tx of `spending_atomic_beef`.
    fn spent(pot_txid: &str, output_index: u32, spending_atomic_beef: Vec<u8>) -> OutputSpent {
        OutputSpent::WholeTx {
            txid: pot_txid.into(),
            output_index,
            topic: "tm_pot".into(),
            spending_atomic_beef,
            off_chain_values: None,
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
        assert_eq!(svc.admission_mode(), AdmissionMode::WholeTx);
        assert_eq!(svc.spend_notification_mode(), SpendNotificationMode::WholeTx);
        let meta = svc.get_metadata().await;
        assert_eq!(meta.name, "POT Lookup Service");
        assert!(!svc.get_documentation().await.is_empty());
    }

    // ── Admit → known, unspent + funding beef stored ─────────────────────

    #[tokio::test]
    async fn admit_then_lookup_known_unspent() {
        let (svc, storage) = make_service_with_storage();
        let funding = funding_tx(1);
        let pot_txid = funding.id();
        svc.output_admitted_by_topic(&admit(beef_of(&funding), 0))
            .await
            .unwrap();
        assert_eq!(storage.record_count(), 1);

        let arr = spent_status(&svc, serde_json::json!([{"txid": pot_txid, "vout": 0}])).await;
        let e = &arr[0];
        assert_eq!(e["txid"], pot_txid);
        assert_eq!(e["vout"], 0);
        assert_eq!(e["known"], true);
        assert_eq!(e["spent"], false);
        assert!(e["spendingTxid"].is_null());
    }

    #[tokio::test]
    async fn admit_stores_funding_beef_under_funding_txid() {
        let (svc, storage) = make_service_with_storage();
        let funding = funding_tx(1);
        let beef = beef_of(&funding);
        svc.output_admitted_by_topic(&admit(beef.clone(), 0))
            .await
            .unwrap();

        // The funding beef is retrievable by the FUNDING txid (derived from
        // the parsed tx — the whole-tx payload carries no txid field).
        assert_eq!(
            storage.get_beef(&funding.id()).await.unwrap(),
            Some(beef),
            "funding beef must be durably stored under the funding txid"
        );
        assert_eq!(storage.beef_count(), 1);
    }

    // ── Spend → known, spent + spendingTxid + settle beef stored ─────────

    #[tokio::test]
    async fn spend_then_lookup_spent_with_spender() {
        let (svc, _storage) = make_service_with_storage();
        let funding = funding_tx(1);
        let pot_txid = funding.id();
        svc.output_admitted_by_topic(&admit(beef_of(&funding), 0))
            .await
            .unwrap();
        // The settle spends the pot input; its txid is derived from the beef.
        let settle = settle_tx(&pot_txid, 0);
        svc.output_spent(&spent(&pot_txid, 0, beef_of(&settle)))
            .await
            .unwrap();

        let arr = spent_status(&svc, serde_json::json!([{"txid": pot_txid, "vout": 0}])).await;
        let e = &arr[0];
        assert_eq!(e["known"], true);
        assert_eq!(e["spent"], true);
        assert_eq!(e["spendingTxid"], settle.id());
    }

    #[tokio::test]
    async fn spend_stores_settle_beef_under_settle_txid() {
        let (svc, storage) = make_service_with_storage();
        let funding = funding_tx(1);
        let pot_txid = funding.id();
        svc.output_admitted_by_topic(&admit(beef_of(&funding), 0))
            .await
            .unwrap();

        let settle = settle_tx(&pot_txid, 0);
        let settle_beef = beef_of(&settle);
        svc.output_spent(&spent(&pot_txid, 0, settle_beef.clone()))
            .await
            .unwrap();

        // The settle beef is retrievable by the SETTLE txid (the engine's
        // transactions table never gets a row for it — this store is the
        // only durable copy)…
        assert_eq!(
            storage.get_beef(&settle.id()).await.unwrap(),
            Some(settle_beef),
            "settle beef must be durably stored under the SETTLE txid"
        );
        // …alongside the funding beef (both survive).
        assert!(storage.get_beef(&pot_txid).await.unwrap().is_some());
        assert_eq!(storage.beef_count(), 2);
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
        let pot_unspent = funding_tx(1);
        let pot_spent = funding_tx(2);
        svc.output_admitted_by_topic(&admit(beef_of(&pot_unspent), 0))
            .await
            .unwrap();
        svc.output_admitted_by_topic(&admit(beef_of(&pot_spent), 0))
            .await
            .unwrap();
        let settle = settle_tx(&pot_spent.id(), 0);
        svc.output_spent(&spent(&pot_spent.id(), 0, beef_of(&settle)))
            .await
            .unwrap();

        let arr = spent_status(
            &svc,
            serde_json::json!([
                {"txid": pot_spent.id(), "vout": 0},
                {"txid": "ghost", "vout": 7},
                {"txid": pot_unspent.id(), "vout": 0}
            ]),
        )
        .await;
        let arr = arr.as_array().unwrap();
        assert_eq!(arr.len(), 3);

        // Order preserved.
        assert_eq!(arr[0]["txid"], pot_spent.id());
        assert_eq!(arr[0]["spent"], true);
        assert_eq!(arr[0]["spendingTxid"], settle.id());

        assert_eq!(arr[1]["txid"], "ghost");
        assert_eq!(arr[1]["known"], false);

        assert_eq!(arr[2]["txid"], pot_unspent.id());
        assert_eq!(arr[2]["known"], true);
        assert_eq!(arr[2]["spent"], false);
    }

    // ── Case-insensitive txid ────────────────────────────────────────────

    #[tokio::test]
    async fn lookup_case_insensitive_txid() {
        let (svc, _storage) = make_service_with_storage();
        // Admitted under the parsed tx id (canonical lowercase hex).
        let funding = funding_tx(1);
        svc.output_admitted_by_topic(&admit(beef_of(&funding), 0))
            .await
            .unwrap();
        // Query with uppercase hex — still matches.
        let arr = spent_status(
            &svc,
            serde_json::json!([{"txid": funding.id().to_uppercase(), "vout": 0}]),
        )
        .await;
        assert_eq!(arr[0]["known"], true);
    }

    // ── Admission filters ────────────────────────────────────────────────

    #[tokio::test]
    async fn ignores_non_tm_pot_topic() {
        let (svc, storage) = make_service_with_storage();
        let mut payload = admit(beef_of(&funding_tx(1)), 0);
        if let OutputAdmittedByTopic::WholeTx { ref mut topic, .. } = payload {
            *topic = "tm_low".into();
        }
        svc.output_admitted_by_topic(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 0);
        assert_eq!(storage.beef_count(), 0, "wrong topic must not store a beef");
    }

    #[tokio::test]
    async fn ignores_non_covenant_output() {
        let (svc, storage) = make_service_with_storage();
        // A tx whose only output is P2PKH — not a covenant.
        let mut tx = Tx::new();
        tx.add_input(TransactionInput::new("00".repeat(32), 0)).unwrap();
        tx.add_output(p2pkh_output()).unwrap();
        svc.output_admitted_by_topic(&admit(beef_of(&tx), 0))
            .await
            .unwrap();
        assert_eq!(storage.record_count(), 0);
        assert_eq!(storage.beef_count(), 0, "non-covenant must not store a beef");
    }

    #[tokio::test]
    async fn ignores_out_of_range_output_index() {
        let (svc, storage) = make_service_with_storage();
        // output_index beyond the parsed tx's outputs → defensive no-op.
        svc.output_admitted_by_topic(&admit(beef_of(&funding_tx(1)), 5))
            .await
            .unwrap();
        assert_eq!(storage.record_count(), 0);
        assert_eq!(storage.beef_count(), 0);
    }

    #[tokio::test]
    async fn admit_unparseable_beef_is_noop() {
        let (svc, storage) = make_service_with_storage();
        // Garbage bytes are not a BEEF → no-op, never a spurious record.
        svc.output_admitted_by_topic(&admit(vec![0xde, 0xad, 0xbe, 0xef], 0))
            .await
            .unwrap();
        assert_eq!(storage.record_count(), 0);
        assert_eq!(storage.beef_count(), 0);
    }

    #[tokio::test]
    async fn rejects_locking_script_mode() {
        let svc = make_service();
        // We opted into whole-tx mode; a locking-script payload is a
        // plumbing error, not a skippable event.
        let payload = OutputAdmittedByTopic::LockingScript {
            txid: "ab".repeat(32),
            output_index: 0,
            topic: "tm_pot".into(),
            satoshis: 2500,
            locking_script: vec![],
            off_chain_values: None,
        };
        assert!(svc.output_admitted_by_topic(&payload).await.is_err());
    }

    // ── Spend filters + permanence ───────────────────────────────────────

    #[tokio::test]
    async fn spend_ignores_other_topics() {
        let (svc, storage) = make_service_with_storage();
        let funding = funding_tx(1);
        let pot_txid = funding.id();
        svc.output_admitted_by_topic(&admit(beef_of(&funding), 0))
            .await
            .unwrap();

        let settle = settle_tx(&pot_txid, 0);
        let mut sp = spent(&pot_txid, 0, beef_of(&settle));
        if let OutputSpent::WholeTx { ref mut topic, .. } = sp {
            *topic = "tm_low".into();
        }
        svc.output_spent(&sp).await.unwrap();

        // Wrong topic → not recorded as spent, and no settle beef stored.
        let arr = spent_status(&svc, serde_json::json!([{"txid": pot_txid, "vout": 0}])).await;
        assert_eq!(arr[0]["spent"], false);
        assert!(storage.get_beef(&settle.id()).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn spend_non_whole_tx_variants_are_noop() {
        let (svc, _storage) = make_service_with_storage();
        let funding = funding_tx(1);
        let pot_txid = funding.id();
        svc.output_admitted_by_topic(&admit(beef_of(&funding), 0))
            .await
            .unwrap();
        // Variants we did not opt into carry no beef to persist → no-op.
        svc.output_spent(&OutputSpent::None {
            txid: pot_txid.clone(),
            output_index: 0,
            topic: "tm_pot".into(),
        })
        .await
        .unwrap();
        svc.output_spent(&OutputSpent::Txid {
            txid: pot_txid.clone(),
            output_index: 0,
            topic: "tm_pot".into(),
            spending_txid: "settleTx".into(),
        })
        .await
        .unwrap();
        let arr = spent_status(&svc, serde_json::json!([{"txid": pot_txid, "vout": 0}])).await;
        assert_eq!(arr[0]["spent"], false);
    }

    #[tokio::test]
    async fn spend_unparseable_beef_is_noop() {
        let (svc, storage) = make_service_with_storage();
        let funding = funding_tx(1);
        let pot_txid = funding.id();
        svc.output_admitted_by_topic(&admit(beef_of(&funding), 0))
            .await
            .unwrap();
        // Garbage spending beef → no spender derivable → no-op (fail-safe:
        // never a phantom spent-marking).
        svc.output_spent(&spent(&pot_txid, 0, vec![0xba, 0xad]))
            .await
            .unwrap();
        let arr = spent_status(&svc, serde_json::json!([{"txid": pot_txid, "vout": 0}])).await;
        assert_eq!(arr[0]["spent"], false);
        // Only the funding beef is stored.
        assert_eq!(storage.beef_count(), 1);
    }

    #[tokio::test]
    async fn eviction_does_not_remove_record() {
        let (svc, storage) = make_service_with_storage();
        let funding = funding_tx(1);
        let pot_txid = funding.id();
        svc.output_admitted_by_topic(&admit(beef_of(&funding), 0))
            .await
            .unwrap();
        let settle = settle_tx(&pot_txid, 0);
        svc.output_spent(&spent(&pot_txid, 0, beef_of(&settle)))
            .await
            .unwrap();

        // Eviction is a no-op — the landing proof (and both beefs) is
        // permanent.
        svc.output_evicted(&pot_txid, 0).await.unwrap();
        assert_eq!(storage.record_count(), 1);
        assert_eq!(storage.beef_count(), 2);
        let arr = spent_status(&svc, serde_json::json!([{"txid": pot_txid, "vout": 0}])).await;
        assert_eq!(arr[0]["spent"], true);
        assert_eq!(arr[0]["spendingTxid"], settle.id());
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
