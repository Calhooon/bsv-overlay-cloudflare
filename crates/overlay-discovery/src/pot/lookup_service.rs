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
use bsv_rs::transaction::{Beef, ChainTracker, MerklePath, Transaction};
use overlay_engine::lookup_service::{LookupService, LookupServiceError};
use overlay_engine::types::*;
use std::rc::Rc;
use tracing::debug;

use super::is_pot_covenant_script;
use super::storage::{PotQuery, PotRecord, PotStorage};

/// Hard cap on outpoints per `spentStatus` query (bsv-low #291). Requests
/// over the cap are refused with an explicit `InvalidQuery` — this is the
/// money landing-proof path, so the answer is never silently truncated.
pub const MAX_SPENT_STATUS_OUTPOINTS: usize = 100;

/// POT Lookup Service — indexes pot spends and answers spent-status queries.
pub struct PotLookupService {
    storage: Rc<dyn PotStorage>,
    /// Optional SPV source used to derive the `confirmed` hint on spends
    /// (prefer-confirmed / never-clobber-with-unconfirmed — see
    /// [`PotStorage::mark_spent`]). When present, a spending BEEF whose
    /// subject tx carries a merkle path that this tracker validates records
    /// the spend as CONFIRMED. When absent (or any verification hiccup) the
    /// spend degrades fail-safe to an UNCONFIRMED hint — never an error that
    /// blocks the spend record.
    chain_tracker: Option<Rc<dyn ChainTracker>>,
}

impl PotLookupService {
    /// Create a new POT lookup service backed by the given storage.
    ///
    /// Constructed without an SPV source: every spend records as
    /// UNCONFIRMED until [`PotLookupService::with_chain_tracker`] wires one
    /// in (the deployed worker does; unit tests opt in via a mock).
    pub fn new(storage: Rc<dyn PotStorage>) -> Self {
        Self {
            storage,
            chain_tracker: None,
        }
    }

    /// Attach a chain tracker so `output_spent` can SPV-verify the spending
    /// tx's merkle path and record the spend as CONFIRMED (mirrors
    /// `LowLookupService::with_chain_tracker`).
    pub fn with_chain_tracker(mut self, chain_tracker: Rc<dyn ChainTracker>) -> Self {
        self.chain_tracker = Some(chain_tracker);
        self
    }

    /// Derive the `confirmed` hint for a spend, as the VERIFIED BLOCK HEIGHT:
    /// `Some(height)` ONLY when a tracker is configured AND the spending BEEF
    /// carries a bump containing `spending_txid` AND the bump's root computes
    /// AND the tracker answers `Ok(true)` for it at the bump's height. EVERY
    /// other outcome (no tracker, no bump, compute error, tracker
    /// error/false) → `None` — FAIL-SAFE: a verification hiccup degrades to
    /// "unconfirmed hint", never an error that blocks the spend record. The
    /// height is what `mark_spent` persists as `spentHeight` (#284): a fact
    /// of the SPV-verified BUMP, never a courier's word.
    ///
    /// NOTE: `Transaction::from_beef` never populates the returned tx's
    /// `merkle_path` (bsv-rs keeps the bump as a `bump_index` into
    /// `Beef.bumps`), so we re-parse the BEEF and look the bump up directly.
    async fn spend_confirmed_height(
        &self,
        spending_atomic_beef: &[u8],
        spending_txid: &str,
    ) -> Option<u64> {
        let tracker = self.chain_tracker.as_ref()?;
        let beef = match Beef::from_binary(spending_atomic_beef) {
            Ok(b) => b,
            Err(e) => {
                debug!("POT: spending beef re-parse for SPV failed — unconfirmed: {e}");
                return None;
            }
        };
        let Some(bump) = beef.find_bump(spending_txid) else {
            // No merkle path for the SPENDING tx itself (0-conf spend).
            return None;
        };
        if bump_verifies(tracker.as_ref(), bump, spending_txid).await {
            Some(u64::from(bump.block_height))
        } else {
            None
        }
    }
}

/// SPV-check one bump against the tracker: compute the root from
/// `txid`'s leaf and ask the tracker whether it is the root at the bump's
/// height. Any error (txid not a leaf, tracker fault) → `false` (fail-safe).
async fn bump_verifies(tracker: &dyn ChainTracker, bump: &MerklePath, txid: &str) -> bool {
    let root = match bump.compute_root(Some(txid)) {
        Ok(r) => r,
        Err(e) => {
            debug!("POT: bump root computation failed — unconfirmed: {e}");
            return false;
        }
    };
    match tracker
        .is_valid_root_for_height(&root, bump.block_height)
        .await
    {
        Ok(valid) => valid,
        Err(e) => {
            debug!("POT: chain-tracker root check failed — unconfirmed: {e}");
            false
        }
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

        // Index tm_pot (covenant) AND tm_lowfund (hop P2PKH) outputs — both
        // land in the same pot_records/pot_beefs landing-proof store, so the
        // app-layer's /utxo-status, /pots-view and /beef answer either kind.
        if !crate::name_matches(topic, "tm_pot") && !crate::name_matches(topic, "tm_lowfund") {
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

        // The topic manager already recognized the shape; re-check defensively
        // per topic (the TM should never admit a mismatched output here):
        // tm_pot admits the pot covenant, tm_lowfund the hop P2PKH.
        let Some(output) = tx.outputs.get(output_index as usize) else {
            debug!("POT: admitted output index out of range — skipped");
            return Ok(());
        };
        let lock_bytes = output.locking_script.to_binary();
        // Which of the two admitted topics this is. `name_matches` so an
        // environment-suffixed deployment (`tm_pot_beta`) still classifies as
        // the pot covenant: with a bare `== "tm_pot"` it would fall through to
        // the hop arm and be shape-checked as P2PKH, then stored with
        // `lockKind='p2pkh'` and no committed params — a silently mis-typed
        // money record, not a clean rejection.
        let is_pot_topic = crate::name_matches(topic, "tm_pot");
        let shape_ok = if is_pot_topic {
            is_pot_covenant_script(&lock_bytes)
        } else {
            crate::pot::is_p2pkh_script(&lock_bytes) // tm_lowfund (gated above)
        };
        if !shape_ok {
            debug!("POT: admitted output does not match its topic's script shape — skipped");
            return Ok(());
        }

        // The funding txid comes from the parsed tx (whole-tx payloads carry
        // no txid field).
        let txid = tx.id();

        // #284: decode ONCE at admission. Everything stored is a pure
        // re-presentation of the admitted bytes: the lock's committed param
        // pushes and the funding output's value. Admission itself stays
        // byte-format-only — a failed extraction on a recognizer-matched
        // script (should be impossible: the recognizer pinned the frozen
        // HEAD/TAIL) is logged and stored as `lockKind='covenant'` with NULL
        // params, and NEVER refuses the admission.
        let mut record = PotRecord {
            txid: txid.clone(),
            output_index,
            spent: false,
            spending_txid: None,
            spent_confirmed: false,
            pot_sats: output.satoshis,
            params_decoded: true,
            ..Default::default()
        };
        // Same classification as the shape gate above — the two MUST agree, or
        // an output shape-checked as a covenant would be stored as a hop.
        if is_pot_topic {
            {
                record.lock_kind = Some("covenant".into());
                match crate::pot::extract_covenant_params(&lock_bytes) {
                    Some(p) => {
                        record.pub_a = Some(hex::encode(p.pub_a));
                        record.pub_b = Some(hex::encode(p.pub_b));
                        record.pub_tower = Some(hex::encode(p.pub_tower));
                        record.pay_pkh_a = Some(hex::encode(p.pay_pkh_a));
                        record.pay_pkh_b = Some(hex::encode(p.pay_pkh_b));
                        record.rake_pkh = Some(hex::encode(p.rake_pkh));
                        record.stake_a = Some(p.stake_a);
                        record.stake_b = Some(p.stake_b);
                        record.fee_sats = Some(p.fee_sats);
                        record.recovery_height = Some(p.recovery_height);
                    }
                    None => {
                        debug!(
                            "POT: recognizer-matched covenant lock failed param extraction \
                             (admitted with NULL params): {txid}:{output_index}"
                        );
                    }
                }
            }
        } else {
            // tm_lowfund: a plain P2PKH hop — no committed params exist.
            record.lock_kind = Some("p2pkh".into());
        }

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

        if !crate::name_matches(topic, "tm_pot") && !crate::name_matches(topic, "tm_lowfund") {
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

        // Derive the CONFIRMED hint (SPV against the pinned tracker), as the
        // verified block height; any hiccup degrades fail-safe to `None` —
        // never blocks the record. This is what makes the public /submit
        // surface safe: an arbitrary unconfirmed claim can never clobber a
        // confirmed pointer (`PotStorage::mark_spent` semantics).
        let spent_height = self
            .spend_confirmed_height(spending_atomic_beef, &spending_txid)
            .await;
        let confirmed = spent_height.is_some();

        // #284: classify the spend verdict ONCE, from the DECODED covenant
        // params stored at admission — a pure template-match of the spending
        // tx's outputs (chain bytes) against the four covenant-mandated
        // exits. Bare pots NEVER get a stored verdict (their refund rule
        // depends on an unverified marker hint — app-layer-only); a row
        // without decoded params (pre-#284 admission) gets `None` and is
        // covered by the backfill pass / the read-path fallback. The
        // stake-conservation check mirrors the app-layer's
        // `classify_pot_spend`: the covenant asserts `ctx.utxo.value ==
        // stakeA + stakeB` in-script, so a funding value that disagrees is
        // not the pot the params describe → no verdict.
        let verdict = match self.storage.get_spent_status(txid, output_index).await {
            Ok(Some(record)) => record
                .decoded_covenant_params()
                .and_then(|params| {
                    let pot_sats = record.pot_sats?;
                    if params.stake_a.checked_add(params.stake_b)? != pot_sats {
                        return None; // conservation failed — never classify
                    }
                    // The spend notification names the spent outpoint; find
                    // the spending tx's OWN input for it (its sequence feeds
                    // the refund height-gate check).
                    let pot_input_sequence = spending_tx.inputs.iter().find_map(|i| {
                        (i.source_txid
                            .as_deref()
                            .is_some_and(|t| t.eq_ignore_ascii_case(txid))
                            && i.source_output_index == output_index)
                            .then_some(i.sequence)
                    })?;
                    let spender = crate::pot::RawTx::from_transaction(&spending_tx)?;
                    crate::pot::classify_covenant(&params, &spender, pot_input_sequence)
                })
                .map(|v| v.as_str()),
            Ok(None) => None, // never admitted — mark_spent is a no-op anyway
            Err(e) => {
                debug!("POT: record read for verdict classification failed — no verdict: {e}");
                None
            }
        };

        // bsv-low #371: latch the spender's BYTES-FINALITY once, at record
        // time, from the tx we already parsed (epoch Rule 25 — never re-parse
        // at read). Final = not(nLockTime armed AND any input non-final);
        // this is the exact client-side test (`stake.ts` settle-landing) and
        // the inverse of the parked-refund shape (#323): a tower-parked
        // refund is non-final by construction and stays behind the merkle
        // bar; a cooperative settle is final and CANNOT be parked.
        let spender_final = Some(
            !(spending_tx.lock_time > 0
                && spending_tx.inputs.iter().any(|i| i.sequence < 0xffff_ffff)),
        );

        // PERSIST the spender — never delete. This is the landing proof. The
        // verdict (+ verified height + bytes-finality) rides the SAME write,
        // atomic with the pointer.
        self.storage
            .mark_spent(
                txid,
                output_index,
                &spending_txid,
                confirmed,
                verdict,
                spent_height,
                spender_final,
            )
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
        if !crate::name_matches(&question.service, "ls_pot") {
            return Err(LookupServiceError::Unsupported(format!(
                "Expected ls_pot, got {}",
                question.service
            )));
        }

        let query: PotQuery = serde_json::from_value(question.query.clone())
            .map_err(|e| LookupServiceError::InvalidQuery(e.to_string()))?;

        let PotQuery::SpentStatus { outpoints } = query;

        // Bound the request array (bsv-low #291): this is a MONEY answer, so
        // an over-cap request is an EXPLICIT InvalidQuery — never a silent
        // truncation that could hide a landing proof from a crediting
        // client. Legitimate clients ask about a handful of pots per call.
        if outpoints.len() > MAX_SPENT_STATUS_OUTPOINTS {
            return Err(LookupServiceError::InvalidQuery(format!(
                "at most {MAX_SPENT_STATUS_OUTPOINTS} outpoints per query \
                 (got {}) — split the request",
                outpoints.len()
            )));
        }

        // txids are canonical lowercase hex on-chain; normalize the query
        // so an uppercase-hex client still matches the stored record.
        let keys: Vec<(String, u32)> = outpoints
            .iter()
            .map(|op| (op.txid.to_ascii_lowercase(), op.vout))
            .collect();

        // ONE batched storage call for the whole set (bsv-low #289) —
        // results aligned index-for-index with the request.
        let records = self
            .storage
            .get_spent_statuses(&keys)
            .await
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;

        // Build an input-ordered array: one entry per requested outpoint.
        let mut entries = Vec::with_capacity(outpoints.len());
        for (op, record) in outpoints.iter().zip(records) {
            // Fail-safe: an outpoint we never admitted is `known:false` with
            // null spent/spendingTxid/spentConfirmed — never assert
            // "unspent" for an output we never saw.
            let (known, spent, spending_txid, spent_confirmed) = match record {
                Some(r) => (
                    true,
                    serde_json::Value::Bool(r.spent),
                    r.spending_txid
                        .map(serde_json::Value::String)
                        .unwrap_or(serde_json::Value::Null),
                    serde_json::Value::Bool(r.spent_confirmed),
                ),
                None => (
                    false,
                    serde_json::Value::Null,
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
                "spentConfirmed": spent_confirmed,
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
    use bsv_rs::transaction::{
        ChainTrackerError, MerklePathLeaf, MockChainTracker, Transaction as Tx, TransactionInput,
        TransactionOutput,
    };

    fn make_service_with_storage() -> (PotLookupService, Rc<MemoryPotStorage>) {
        let storage = Rc::new(MemoryPotStorage::new());
        let svc = PotLookupService::new(storage.clone());
        (svc, storage)
    }

    /// A tracker that always FAULTS — the "verification hiccup" case that
    /// must degrade to an unconfirmed hint, never a blocked spend record.
    struct FailingTracker;

    #[async_trait::async_trait]
    impl ChainTracker for FailingTracker {
        async fn is_valid_root_for_height(
            &self,
            _root: &str,
            _height: u32,
        ) -> Result<bool, ChainTrackerError> {
            Err(ChainTrackerError::NetworkError("boom".into()))
        }

        async fn current_height(&self) -> Result<u32, ChainTrackerError> {
            Err(ChainTrackerError::NetworkError("boom".into()))
        }
    }

    /// A minimal valid BUMP proving `txid` as the sole tx of a block at
    /// `height` — the single-tx-block special case, whose root IS the txid.
    fn single_tx_bump(txid: &str, height: u32) -> MerklePath {
        MerklePath::new(height, vec![vec![MerklePathLeaf::new_txid(0, txid.into())]])
            .expect("valid single-leaf merkle path")
    }

    /// A tracker that accepts exactly the single-tx-block proof for `txid`
    /// at `height` (root == txid).
    fn tracker_accepting(txid: &str, height: u32) -> Rc<dyn ChainTracker> {
        let mut t = MockChainTracker::new(height + 6);
        t.add_root(height, txid.to_string());
        Rc::new(t)
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
        tx.add_output(make_covenant_output(&dummy_params()))
            .unwrap();
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
    async fn spent_status(
        svc: &PotLookupService,
        outpoints: serde_json::Value,
    ) -> serde_json::Value {
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
        assert_eq!(
            svc.spend_notification_mode(),
            SpendNotificationMode::WholeTx
        );
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
        assert_eq!(e["spentConfirmed"], false);
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
        // No tracker configured → the spend is an UNCONFIRMED hint.
        assert_eq!(e["spentConfirmed"], false);
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
        // Fail-safe: unknown outpoint → spentConfirmed is NULL, not false.
        assert!(e["spentConfirmed"].is_null());
    }

    // ── spentConfirmed derivation (SPV against the chain tracker) ────────

    /// Admit a pot, then spend it with a settle carrying `merkle_path`
    /// (or not) through the REAL producer path (`output_spent`), and return
    /// what the storage recorded.
    async fn run_spend(
        tracker: Option<Rc<dyn ChainTracker>>,
        bump: Option<MerklePath>,
    ) -> (PotRecord, String) {
        let storage = Rc::new(MemoryPotStorage::new());
        let mut svc = PotLookupService::new(storage.clone());
        if let Some(t) = tracker {
            svc = svc.with_chain_tracker(t);
        }

        let funding = funding_tx(1);
        let pot_txid = funding.id();
        svc.output_admitted_by_topic(&admit(beef_of(&funding), 0))
            .await
            .unwrap();

        let mut settle = settle_tx(&pot_txid, 0);
        settle.merkle_path = bump;
        let settle_txid = settle.id();
        svc.output_spent(&spent(&pot_txid, 0, beef_of(&settle)))
            .await
            .unwrap();

        let rec = storage
            .get_spent_status(&pot_txid, 0)
            .await
            .unwrap()
            .unwrap();
        (rec, settle_txid)
    }

    #[tokio::test]
    async fn spend_with_valid_bump_and_tracker_true_records_confirmed() {
        // Pre-compute the settle txid so the tracker can pin its root: the
        // settle shape is deterministic for a given pot outpoint.
        let pot_txid = funding_tx(1).id();
        let settle_txid = settle_tx(&pot_txid, 0).id();

        let (rec, spender) = run_spend(
            Some(tracker_accepting(&settle_txid, 800_000)),
            Some(single_tx_bump(&settle_txid, 800_000)),
        )
        .await;
        assert_eq!(spender, settle_txid);
        assert!(rec.spent);
        assert_eq!(rec.spending_txid.as_deref(), Some(settle_txid.as_str()));
        assert!(
            rec.spent_confirmed,
            "valid bump + tracker-true must record CONFIRMED"
        );
    }

    #[tokio::test]
    async fn spend_without_bump_records_unconfirmed() {
        // Tracker configured but the spending tx carries no merkle path
        // (0-conf spend) → unconfirmed hint.
        let pot_txid = funding_tx(1).id();
        let settle_txid = settle_tx(&pot_txid, 0).id();

        let (rec, _) = run_spend(Some(tracker_accepting(&settle_txid, 800_000)), None).await;
        assert!(rec.spent, "the spend is still recorded");
        assert!(!rec.spent_confirmed);
    }

    #[tokio::test]
    async fn spend_with_bump_but_tracker_false_records_unconfirmed() {
        // The tracker knows no root at that height → Ok(false) → unconfirmed.
        let pot_txid = funding_tx(1).id();
        let settle_txid = settle_tx(&pot_txid, 0).id();

        let (rec, _) = run_spend(
            Some(Rc::new(MockChainTracker::new(900_000))),
            Some(single_tx_bump(&settle_txid, 800_000)),
        )
        .await;
        assert!(rec.spent, "the spend is still recorded");
        assert!(!rec.spent_confirmed);
    }

    #[tokio::test]
    async fn spend_with_bump_but_tracker_error_records_unconfirmed() {
        // FAIL-SAFE: a tracker fault degrades to an unconfirmed hint — it
        // must never error out and block the spend record.
        let pot_txid = funding_tx(1).id();
        let settle_txid = settle_tx(&pot_txid, 0).id();

        let (rec, _) = run_spend(
            Some(Rc::new(FailingTracker)),
            Some(single_tx_bump(&settle_txid, 800_000)),
        )
        .await;
        assert!(rec.spent, "a tracker fault must not block the spend record");
        assert!(!rec.spent_confirmed);
    }

    #[tokio::test]
    async fn spend_without_tracker_records_unconfirmed_even_with_bump() {
        // No tracker configured → confirmed can never be derived, even for a
        // bump-carrying spend (we can't validate the root against anything).
        let pot_txid = funding_tx(1).id();
        let settle_txid = settle_tx(&pot_txid, 0).id();

        let (rec, _) = run_spend(None, Some(single_tx_bump(&settle_txid, 800_000))).await;
        assert!(rec.spent);
        assert!(!rec.spent_confirmed);
    }

    #[tokio::test]
    async fn bump_verifies_rejects_txid_not_a_leaf() {
        // compute_root errors when the txid is not a leaf of the path →
        // fail-safe false, even under an always-accepting tracker.
        let bump = single_tx_bump(&"aa".repeat(32), 800_000);
        let tracker = bsv_rs::transaction::AlwaysValidChainTracker::new(800_100);
        assert!(!bump_verifies(&tracker, &bump, &"bb".repeat(32)).await);
        // Sanity: the actual leaf DOES verify under the same tracker.
        assert!(bump_verifies(&tracker, &bump, &"aa".repeat(32)).await);
    }

    #[tokio::test]
    async fn confirmed_pointer_survives_later_unconfirmed_claim_end_to_end() {
        // Through the REAL producer path: a confirmed settle lands first,
        // then an attacker submits an unconfirmed tx claiming the same pot —
        // the pointer and flag must be unchanged.
        let storage = Rc::new(MemoryPotStorage::new());
        let funding = funding_tx(1);
        let pot_txid = funding.id();
        let mut real_settle = settle_tx(&pot_txid, 0);
        let settle_txid = real_settle.id();
        real_settle.merkle_path = Some(single_tx_bump(&settle_txid, 800_000));

        let svc = PotLookupService::new(storage.clone())
            .with_chain_tracker(tracker_accepting(&settle_txid, 800_000));
        svc.output_admitted_by_topic(&admit(beef_of(&funding), 0))
            .await
            .unwrap();
        svc.output_spent(&spent(&pot_txid, 0, beef_of(&real_settle)))
            .await
            .unwrap();

        // The attacker's forged spend: same pot outpoint, different tx, no
        // proof (the public /submit + historical-tx-no-spv path).
        let mut forged = settle_tx(&pot_txid, 0);
        forged.add_output(p2pkh_output()).unwrap(); // distinct txid
        assert_ne!(forged.id(), settle_txid);
        svc.output_spent(&spent(&pot_txid, 0, beef_of(&forged)))
            .await
            .unwrap();

        let arr = spent_status(&svc, serde_json::json!([{"txid": pot_txid, "vout": 0}])).await;
        let e = &arr[0];
        assert_eq!(e["spent"], true);
        assert_eq!(
            e["spendingTxid"], settle_txid,
            "an unconfirmed claim must never clobber the confirmed pointer"
        );
        assert_eq!(e["spentConfirmed"], true);
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
        tx.add_input(TransactionInput::new("00".repeat(32), 0))
            .unwrap();
        tx.add_output(p2pkh_output()).unwrap();
        svc.output_admitted_by_topic(&admit(beef_of(&tx), 0))
            .await
            .unwrap();
        assert_eq!(storage.record_count(), 0);
        assert_eq!(
            storage.beef_count(),
            0,
            "non-covenant must not store a beef"
        );
    }

    // ── tm_lowfund (the hop-side index into the SAME store) ─────────────

    /// Re-tag a payload with the tm_lowfund topic.
    fn as_lowfund_admit(mut payload: OutputAdmittedByTopic) -> OutputAdmittedByTopic {
        if let OutputAdmittedByTopic::WholeTx { ref mut topic, .. } = payload {
            *topic = "tm_lowfund".into();
        }
        payload
    }

    fn with_topic(mut payload: OutputAdmittedByTopic, name: &str) -> OutputAdmittedByTopic {
        if let OutputAdmittedByTopic::WholeTx { ref mut topic, .. } = payload {
            *topic = name.into();
        }
        payload
    }

    // ── Environment-suffixed topics (the isolated beta stack) ────────────────

    /// A `TOPIC_SUFFIX` deployment registers `tm_pot_beta`, and the pot record
    /// it writes MUST be identical to prod's — same store, same `covenant`
    /// lock kind, same decoded params.
    ///
    /// Under the previous exact-match guard this failed in the worst way
    /// available: `output_admitted_by_topic` returned `Ok(())`, so `/submit`
    /// answered 200 while indexing nothing, and every later lookup — the ones a
    /// wiped device uses to find its own money — stayed empty forever.
    #[tokio::test]
    async fn suffixed_pot_topic_indexes_as_a_covenant() {
        let (svc, storage) = make_service_with_storage();
        // The REAL covenant builder, so this exercises the whole decode path —
        // recognizer, shape gate and param extraction — under the suffix.
        let p = real_params();
        let funding = real_funding_tx(7, &p);
        svc.output_admitted_by_topic(&with_topic(admit(beef_of(&funding), 0), "tm_pot_beta"))
            .await
            .unwrap();

        assert_eq!(storage.record_count(), 1, "the suffixed topic IS indexed");
        let r = storage
            .get_spent_status(&funding.id(), 0)
            .await
            .unwrap()
            .unwrap();
        // Not merely stored — stored with the RIGHT type and the RIGHT params.
        // The classification and the shape gate must agree, or a covenant pot
        // is filed as a hop with its committed params thrown away.
        assert_eq!(r.lock_kind.as_deref(), Some("covenant"));
        assert!(r.params_decoded);
        assert_eq!(r.pub_tower.as_deref(), Some(hex::encode(p.pub_tower).as_str()));
        assert_eq!(r.stake_a, Some(p.stake_a));
    }

    /// The hop side of the same suffixed deployment.
    #[tokio::test]
    async fn suffixed_lowfund_topic_indexes_as_a_hop() {
        let (svc, storage) = make_service_with_storage();
        let mut tx = Tx::new();
        tx.add_input(TransactionInput::new("13".repeat(32), 0))
            .unwrap();
        tx.add_output(p2pkh_output()).unwrap();
        svc.output_admitted_by_topic(&with_topic(admit(beef_of(&tx), 0), "tm_lowfund_beta"))
            .await
            .unwrap();

        let r = storage
            .get_spent_status(&tx.id(), 0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.lock_kind.as_deref(), Some("p2pkh"));
    }

    /// Suffix matching must not become "any name starting with tm_pot".
    /// `tm_potparty` and `tm_potrefund` are DIFFERENT protocols with their own
    /// stores; swallowing them here would file recovery markers as pots.
    #[tokio::test]
    async fn sibling_pot_topics_are_still_refused() {
        for sibling in ["tm_potparty", "tm_potrefund", "tm_potparty_beta"] {
            let (svc, storage) = make_service_with_storage();
            svc.output_admitted_by_topic(&with_topic(admit(beef_of(&funding_tx(9)), 0), sibling))
                .await
                .unwrap();
            assert_eq!(
                storage.record_count(),
                0,
                "{sibling} must not be indexed by the pot service"
            );
        }
    }

    /// The lookup half: a suffixed deployment answers `ls_pot_beta`, and the
    /// engine has already routed by exact key before this guard runs.
    #[tokio::test]
    async fn suffixed_service_name_is_answered() {
        let (svc, _storage) = make_service_with_storage();
        let q = LookupQuestion::new(
            "ls_pot_beta",
            serde_json::json!({"type": "spentStatus", "outpoints": []}),
        );
        assert!(svc.lookup(&q).await.is_ok());

        // A different protocol's service name is still refused.
        let wrong = LookupQuestion::new(
            "ls_potparty_beta",
            serde_json::json!({"type": "spentStatus", "outpoints": []}),
        );
        assert!(svc.lookup(&wrong).await.is_err());
    }

    #[tokio::test]
    async fn lowfund_admits_p2pkh_hop_into_the_same_store() {
        let (svc, storage) = make_service_with_storage();
        // A hop-carrying funding tx: vout 0 = plain P2PKH (the hop).
        let mut tx = Tx::new();
        tx.add_input(TransactionInput::new("11".repeat(32), 0))
            .unwrap();
        tx.add_output(p2pkh_output()).unwrap();
        let hop_txid = tx.id();
        svc.output_admitted_by_topic(&as_lowfund_admit(admit(beef_of(&tx), 0)))
            .await
            .unwrap();
        assert_eq!(storage.record_count(), 1);
        assert_eq!(storage.beef_count(), 1, "the hop funding beef is stored");

        let arr = spent_status(&svc, serde_json::json!([{"txid": hop_txid, "vout": 0}])).await;
        assert_eq!(arr[0]["known"], true);
        assert_eq!(arr[0]["spent"], false);
    }

    #[tokio::test]
    async fn lowfund_rejects_a_covenant_output_shape() {
        let (svc, storage) = make_service_with_storage();
        // The pot covenant admitted under tm_lowfund is a shape mismatch —
        // the defensive per-topic re-check must skip it (tm_pot owns it).
        svc.output_admitted_by_topic(&as_lowfund_admit(admit(beef_of(&funding_tx(1)), 0)))
            .await
            .unwrap();
        assert_eq!(storage.record_count(), 0);
        assert_eq!(storage.beef_count(), 0);
    }

    #[tokio::test]
    async fn lowfund_spend_records_the_join_as_spender() {
        let (svc, storage) = make_service_with_storage();
        // Admit the hop under tm_lowfund…
        let mut hop = Tx::new();
        hop.add_input(TransactionInput::new("22".repeat(32), 0))
            .unwrap();
        hop.add_output(p2pkh_output()).unwrap();
        let hop_txid = hop.id();
        svc.output_admitted_by_topic(&as_lowfund_admit(admit(beef_of(&hop), 0)))
            .await
            .unwrap();
        // …then the JOIN spends it, submitted under tm_lowfund.
        let join = settle_tx(&hop_txid, 0);
        let join_txid = join.id();
        let mut payload = spent(&hop_txid, 0, beef_of(&join));
        if let OutputSpent::WholeTx { ref mut topic, .. } = payload {
            *topic = "tm_lowfund".into();
        }
        svc.output_spent(&payload).await.unwrap();

        let arr = spent_status(&svc, serde_json::json!([{"txid": hop_txid, "vout": 0}])).await;
        assert_eq!(arr[0]["known"], true);
        assert_eq!(arr[0]["spent"], true);
        assert_eq!(arr[0]["spendingTxid"], join_txid);
        // The spender's beef is durably stored too (the /beef + /pots-view raw source).
        assert!(storage.get_beef(&join_txid).await.unwrap().is_some());
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

    // ── #284: decode-once at admission + classify-once at spend ──────────

    use crate::pot::covenant::{encode_covenant_param_pushes, p2pkh_lock, CovenantParams};
    use crate::pot::topic_manager::tests::covenant_script;

    /// Committed params whose stakes sum to `make_covenant_output`'s 2500
    /// funded satoshis (conservation holds): pot 2500, fee 100 → net 2400,
    /// rake floor(2500/100)=25 → winner payout 2375.
    fn real_params() -> CovenantParams {
        CovenantParams {
            pub_a: [0x02; 33],
            pub_b: [0x03; 33],
            pub_tower: [0x04; 33],
            pay_pkh_a: [0xAA; 20],
            pay_pkh_b: [0xBB; 20],
            rake_pkh: [0xCC; 20],
            stake_a: 1250,
            stake_b: 1250,
            fee_sats: 100,
            recovery_height: 900_000,
        }
    }

    /// A funding tx whose vout 0 is the covenant over REAL param pushes.
    fn real_funding_tx(salt: u8, p: &CovenantParams) -> Tx {
        let mut tx = Tx::new();
        tx.add_input(TransactionInput::new(hex::encode([salt; 32]), 0))
            .unwrap();
        tx.add_output(TransactionOutput {
            satoshis: Some(2500),
            locking_script: LockingScript::from_binary(&covenant_script(
                &encode_covenant_param_pushes(p),
            ))
            .unwrap(),
            change: false,
        })
        .unwrap();
        tx
    }

    /// A spender paying an explicit output set (a template match candidate).
    fn spender_paying(pot_txid: &str, vout: u32, outs: &[(u64, Vec<u8>)]) -> Tx {
        let mut tx = Tx::new();
        tx.add_input(TransactionInput::new(pot_txid.to_string(), vout))
            .unwrap();
        for (sats, script) in outs {
            tx.add_output(TransactionOutput {
                satoshis: Some(*sats),
                locking_script: LockingScript::from_binary(script).unwrap(),
                change: false,
            })
            .unwrap();
        }
        tx
    }

    #[tokio::test]
    async fn admission_decodes_the_committed_params_into_the_record() {
        let (svc, storage) = make_service_with_storage();
        let p = real_params();
        let funding = real_funding_tx(1, &p);
        svc.output_admitted_by_topic(&admit(beef_of(&funding), 0))
            .await
            .unwrap();

        let r = storage
            .get_spent_status(&funding.id(), 0)
            .await
            .unwrap()
            .unwrap();
        assert!(r.params_decoded);
        assert_eq!(r.lock_kind.as_deref(), Some("covenant"));
        assert_eq!(r.pub_a.as_deref(), Some(hex::encode(p.pub_a).as_str()));
        assert_eq!(r.pub_b.as_deref(), Some(hex::encode(p.pub_b).as_str()));
        assert_eq!(
            r.pay_pkh_a.as_deref(),
            Some(hex::encode(p.pay_pkh_a).as_str())
        );
        assert_eq!(
            r.rake_pkh.as_deref(),
            Some(hex::encode(p.rake_pkh).as_str())
        );
        assert_eq!(r.stake_a, Some(1250));
        assert_eq!(r.stake_b, Some(1250));
        assert_eq!(r.fee_sats, Some(100));
        assert_eq!(r.recovery_height, Some(900_000));
        assert_eq!(r.pot_sats, Some(2500), "funding output value captured");
        // The round-trip: the stored columns rebuild the exact params.
        assert_eq!(r.decoded_covenant_params(), Some(p));
        // No spend yet → no verdict.
        assert_eq!(r.verdict, None);
    }

    #[tokio::test]
    async fn recognizer_matched_but_unextractable_params_still_admit() {
        // The existing dummy fixture's 45×0xAB middle is NOT a valid push
        // sequence — extraction fails. The admission must still land
        // (lockKind covenant, params NULL, paramsDecoded latched) — decode
        // failure NEVER refuses an admission.
        let (svc, storage) = make_service_with_storage();
        let funding = funding_tx(1);
        svc.output_admitted_by_topic(&admit(beef_of(&funding), 0))
            .await
            .unwrap();
        let r = storage
            .get_spent_status(&funding.id(), 0)
            .await
            .unwrap()
            .unwrap();
        assert!(r.params_decoded, "decode was ATTEMPTED and recorded");
        assert_eq!(r.lock_kind.as_deref(), Some("covenant"));
        assert_eq!(r.pub_a, None);
        assert_eq!(r.decoded_covenant_params(), None);
    }

    #[tokio::test]
    async fn spend_classifies_the_winner_template_and_stores_the_verdict() {
        let (svc, storage) = make_service_with_storage();
        let p = real_params();
        let funding = real_funding_tx(1, &p);
        let pot_txid = funding.id();
        svc.output_admitted_by_topic(&admit(beef_of(&funding), 0))
            .await
            .unwrap();

        // The exact winner-A template: [25 → rakePkh, 2375 → payPkhA].
        let settle = spender_paying(
            &pot_txid,
            0,
            &[
                (25, p2pkh_lock(&p.rake_pkh)),
                (2375, p2pkh_lock(&p.pay_pkh_a)),
            ],
        );
        svc.output_spent(&spent(&pot_txid, 0, beef_of(&settle)))
            .await
            .unwrap();

        let r = storage
            .get_spent_status(&pot_txid, 0)
            .await
            .unwrap()
            .unwrap();
        assert!(r.spent);
        assert_eq!(r.spending_txid.as_deref(), Some(settle.id().as_str()));
        assert_eq!(r.verdict.as_deref(), Some("winner-a"));
        assert_eq!(
            r.verdict_txid.as_deref(),
            Some(settle.id().as_str()),
            "the verdict is bound to the spender it was computed from"
        );
        // Unconfirmed spend (no tracker) → no spentHeight.
        assert_eq!(r.spent_height, None);
    }

    #[tokio::test]
    async fn confirmed_spend_stores_the_bump_height_alongside_the_verdict() {
        let storage = Rc::new(MemoryPotStorage::new());
        let p = real_params();
        let funding = real_funding_tx(1, &p);
        let pot_txid = funding.id();
        let mut settle = spender_paying(
            &pot_txid,
            0,
            &[
                (25, p2pkh_lock(&p.rake_pkh)),
                (2375, p2pkh_lock(&p.pay_pkh_a)),
            ],
        );
        let settle_txid = settle.id();
        settle.merkle_path = Some(single_tx_bump(&settle_txid, 800_000));

        let svc = PotLookupService::new(storage.clone())
            .with_chain_tracker(tracker_accepting(&settle_txid, 800_000));
        svc.output_admitted_by_topic(&admit(beef_of(&funding), 0))
            .await
            .unwrap();
        svc.output_spent(&spent(&pot_txid, 0, beef_of(&settle)))
            .await
            .unwrap();

        let r = storage
            .get_spent_status(&pot_txid, 0)
            .await
            .unwrap()
            .unwrap();
        assert!(r.spent_confirmed);
        assert_eq!(
            r.spent_height,
            Some(800_000),
            "the SPV-verified BUMP height"
        );
        assert_eq!(r.verdict.as_deref(), Some("winner-a"));
    }

    #[tokio::test]
    async fn non_template_spend_and_conservation_mismatch_store_no_verdict() {
        // A spend paying a NON-template shape → spent recorded, verdict NULL.
        let (svc, storage) = make_service_with_storage();
        let p = real_params();
        let funding = real_funding_tx(1, &p);
        let pot_txid = funding.id();
        svc.output_admitted_by_topic(&admit(beef_of(&funding), 0))
            .await
            .unwrap();
        let redirect = spender_paying(&pot_txid, 0, &[(2400, p2pkh_lock(&[0xEE; 20]))]);
        svc.output_spent(&spent(&pot_txid, 0, beef_of(&redirect)))
            .await
            .unwrap();
        let r = storage
            .get_spent_status(&pot_txid, 0)
            .await
            .unwrap()
            .unwrap();
        assert!(r.spent);
        assert_eq!(r.verdict, None, "a non-template spend never classifies");

        // Conservation mismatch: committed stakes (500+500) ≠ the funded
        // 2500 sats — even an exact template match must refuse (the params
        // do not describe THIS pot's money).
        let (svc2, storage2) = make_service_with_storage();
        let bad = CovenantParams {
            stake_a: 500,
            stake_b: 500,
            ..real_params()
        };
        let funding2 = real_funding_tx(2, &bad);
        let pot2 = funding2.id();
        svc2.output_admitted_by_topic(&admit(beef_of(&funding2), 0))
            .await
            .unwrap();
        // The template derived from the (lying) params: pot 1000, fee 100 →
        // net 900, rake 10 → winner 890.
        let settle2 = spender_paying(
            &pot2,
            0,
            &[
                (10, p2pkh_lock(&bad.rake_pkh)),
                (890, p2pkh_lock(&bad.pay_pkh_a)),
            ],
        );
        svc2.output_spent(&spent(&pot2, 0, beef_of(&settle2)))
            .await
            .unwrap();
        let r2 = storage2.get_spent_status(&pot2, 0).await.unwrap().unwrap();
        assert!(r2.spent, "the landing proof still lands");
        assert_eq!(r2.verdict, None, "stake conservation failed → no verdict");
    }

    #[tokio::test]
    async fn lowfund_rows_are_p2pkh_kind_and_never_get_a_verdict() {
        let (svc, storage) = make_service_with_storage();
        let mut hop = Tx::new();
        hop.add_input(TransactionInput::new("22".repeat(32), 0))
            .unwrap();
        hop.add_output(p2pkh_output()).unwrap();
        let hop_txid = hop.id();
        svc.output_admitted_by_topic(&as_lowfund_admit(admit(beef_of(&hop), 0)))
            .await
            .unwrap();
        let r = storage
            .get_spent_status(&hop_txid, 0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.lock_kind.as_deref(), Some("p2pkh"));
        assert!(r.params_decoded);
        assert_eq!(r.pub_a, None, "a hop has no committed params");
        assert_eq!(r.pot_sats, Some(546));

        // The JOIN spends it — spent recorded, but NEVER a verdict.
        let join = settle_tx(&hop_txid, 0);
        let mut payload = spent(&hop_txid, 0, beef_of(&join));
        if let OutputSpent::WholeTx { ref mut topic, .. } = payload {
            *topic = "tm_lowfund".into();
        }
        svc.output_spent(&payload).await.unwrap();
        let r = storage
            .get_spent_status(&hop_txid, 0)
            .await
            .unwrap()
            .unwrap();
        assert!(r.spent);
        assert_eq!(r.verdict, None, "p2pkh rows never carry a verdict");
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

    /// #291: an over-cap request is an EXPLICIT error, never a silent
    /// truncation — this is the money landing-proof path, so a dropped
    /// entry must be impossible (a client would read an absent entry as
    /// "unknown" and could stall a credit forever).
    #[tokio::test]
    async fn over_cap_outpoints_is_an_explicit_invalid_query() {
        let svc = make_service();
        let at_cap: Vec<_> = (0..MAX_SPENT_STATUS_OUTPOINTS)
            .map(|i| serde_json::json!({"txid": format!("{i:064x}"), "vout": 0}))
            .collect();
        // Exactly at the cap: fine (every entry answered).
        let arr = spent_status(&svc, serde_json::json!(at_cap)).await;
        assert_eq!(arr.as_array().unwrap().len(), MAX_SPENT_STATUS_OUTPOINTS);

        // One over: refused loudly.
        let mut over = at_cap;
        over.push(serde_json::json!({"txid": "ff".repeat(32), "vout": 0}));
        let q = LookupQuestion::new(
            "ls_pot",
            serde_json::json!({"type": "spentStatus", "outpoints": over}),
        );
        let err = svc.lookup(&q).await.unwrap_err();
        assert!(
            matches!(err, LookupServiceError::InvalidQuery(_)),
            "expected InvalidQuery, got {err:?}"
        );
    }
}
