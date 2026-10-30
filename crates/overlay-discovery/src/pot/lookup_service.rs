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
//!
//! # A held pot with no row here is recorded at its spend (bsv-low #936)
//!
//! The engine tells this service of a spend only for a coin the TOPIC
//! HOLDS (`Engine::submit`'s previous coins), so a spent outpoint with no
//! record here is an output the topic admitted whose admission hook never
//! ran: the write was cut between the engine's `insert_output` and this
//! service's `store_record` (beta 2026-10-27, the JOIN `fad01028...`: the
//! gated request cancelled by the client while it owned the write; since
//! overlay PR 188 the write is owned by `wait_until`, and the platform's
//! own cut of that task remains). The spender's admission then records the
//! pot's transaction applied and deletes its coin (#559's H2), which made
//! the half write permanent: an applied row over no output, no
//! `pot_records`, no `pot_beefs`, no eviction record, and the owed list
//! never saw the refund. So `output_spent` records such a pot FIRST, from
//! the spender's own bytes (a valid BEEF carries its input's source
//! transaction), through the admission hook itself (its shape gate
//! included), then writes the pointer: the pot stands recorded and spent
//! exactly as a whole write leaves it. A spender that does not carry the
//! source (txid-only ancestry) has the pot recorded from the ENGINE's
//! stored copy (`PotStorage::held_transaction_beef`), hash-bound to the
//! outpoint either way. A pot recorded without its bytes (a cut between
//! `store_record` and `store_beef`) has them written at its spend (a
//! verified row is never written over). A faulted record read is "not
//! now". The heal writes NOTHING while the eviction ledger holds the txid
//! OPEN (lens 3's LOW-1: a spend straddling the shadow move, a
//! readmission whose `pot_records` copy back faulted): no guard re-evicts
//! a heal, and the readmission's restore brings the real row back. A
//! healed record's `createdAt` (and its bytes') is the SPEND's time, not
//! the admission's (lens 3's NOTE-5): ordering and history only, never
//! across an era (a wipe removes the engine's coin).

use async_trait::async_trait;
use bsv_rs::transaction::{ChainTracker, MerklePath, Transaction};
use overlay_engine::beef_limits;
use overlay_engine::lookup_service::{LookupService, LookupServiceError};
use overlay_engine::types::*;
use std::rc::Rc;
use tracing::{debug, warn};

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
        let beef = match beef_limits::parse_beef(
            spending_atomic_beef,
            &beef_limits::DISCOVERY_BEEF_LIMITS,
        ) {
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
    /// Nothing told to this service reads an off-chain value.
    fn reads_off_chain_values(&self, _topic: &str) -> bool {
        false
    }

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
        let tx = match beef_limits::transaction_from_beef(
            atomic_beef,
            None,
            &beef_limits::DISCOVERY_BEEF_LIMITS,
        ) {
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
        // P4 slice 2: the funding tx's own size + exact fee from the admitted
        // BEEF (None when it cannot be named — never an estimate).
        let facts = crate::tx_facts::facts_from_atomic_beef(atomic_beef, &txid);

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
            funding_size_bytes: facts.as_ref().map(|f| f.size_bytes),
            funding_fee_sats: facts.as_ref().and_then(|f| f.fee_sats),
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
        let spending_tx = match beef_limits::transaction_from_beef(
            spending_atomic_beef,
            None,
            &beef_limits::DISCOVERY_BEEF_LIMITS,
        ) {
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
        // bsv-low #936 (the FIX-936 lens's LOW-1): a record read that FAULTS
        // is "not now", never "no record": the hook answers the fault, so
        // the engine keeps the coin unapplied (#657) and the replay tells
        // the hook again. Proceeding (as before the fold) let a genuinely
        // half-written pot's only spend land with the coin deleted and no
        // record, a loss no replay could reach.
        let admitted = self
            .storage
            .get_spent_status(txid, output_index)
            .await
            .map_err(|e| {
                LookupServiceError::StorageError(format!(
                    "pot record read for {txid}:{output_index} faulted (not now, bsv-low #936): {e}"
                ))
            })?;
        // bsv-low #936: a held output whose record the read ANSWERED absent
        // is recorded from the spender's bytes (or the engine's stored copy)
        // BEFORE its pointer is written (the module docs). A record without
        // its bytes (a heal or an admission cut at the byte write) has them
        // written now (the lens's LOW-3).
        let admitted = match admitted {
            Some(record) => {
                self.complete_held_output_bytes(txid, output_index, topic, &spending_tx)
                    .await?;
                Some(record)
            }
            None => {
                self.record_held_output_from_spender(txid, output_index, topic, &spending_tx)
                    .await?
            }
        };
        // bsv-low #986 (FIX-986): the ONE classifier every verdict writer calls (`pot::classify_record_spend`:
        // conservation, the spend's own input for the outpoint, `classify_covenant`, then #406's WHO SIGNED from
        // the spend's signatures against the committed triple over the BIP-143 digest). The door writes a pair not
        // established as NULL ("not established"; the signers backfill retries once from the durable spender
        // BEEF), and the #284 verdict + #406 signers travel as ONE group (typed so the signers can never be
        // written without the verdict they ride with).
        let verdict_write = admitted
            .as_ref()
            .and_then(|record| crate::pot::classify_record_spend(record, &spending_tx))
            .map(|group| group.write());

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
                verdict_write,
                spent_height,
                spender_final,
            )
            .await
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;

        // bsv-low P4 slice 2: the spender's OWN size + exact fee, described
        // under the pointer just written (CAS — a moved pointer is a no-op).
        // The fee comes from the BEEF's parents when it carries them, else
        // from the ADMITTED pot value for a single-input pot spend (the only
        // input is the pot, so fee = potSats − Σ outputs — never a caller
        // claim). Display-tier and BEST-EFFORT: a failure here never fails
        // the landing proof above.
        if let Some(record) = admitted.as_ref() {
            let mut facts =
                crate::tx_facts::facts_from_atomic_beef(spending_atomic_beef, &spending_txid);
            if let Some(f) = facts.as_mut() {
                if f.fee_sats.is_none() {
                    f.fee_sats =
                        single_input_pot_fee(&spending_tx, txid, output_index, record.pot_sats);
                }
            }
            if let Some(f) = facts {
                if let Err(e) = self
                    .storage
                    .store_spender_facts(txid, output_index, &spending_txid, f)
                    .await
                {
                    debug!("POT: spender facts not recorded (display tier, best-effort): {e}");
                }
            }
            // bsv-low #468 (2026-09-19): the spend's outputs to the COMMITTED pay
            // homes, from the tx already parsed and the record's decoded homes
            // (the funding output's own bytes, #284) — never a caller claim.
            // History's amount column reads it; no client fetches a settle raw
            // for a number. Display-tier, best-effort like the facts above.
            let (pay_a, pay_b) = spend_payouts(
                &spending_tx,
                record.pay_pkh_a.as_deref(),
                record.pay_pkh_b.as_deref(),
            );
            if pay_a.is_some() || pay_b.is_some() {
                if let Err(e) = self
                    .storage
                    .store_spender_payouts(txid, output_index, &spending_txid, pay_a, pay_b)
                    .await
                {
                    debug!("POT: spender payouts not recorded (display tier, best-effort): {e}");
                }
            }
        }

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
        // admit-fast step 4: the pot's OWN network witness (the overlay's
        // broadcaster-witnessed SEEN latch, D3) — one batched read, aligned.
        let txids: Vec<String> = keys.iter().map(|(t, _)| t.clone()).collect();
        // bsv-low #621 (L5): the same read with each latch's witness (its broadcaster and time); `networkSeen` is
        // `is_some()` of it, byte for byte the bool it was.
        let witnessed = self
            .storage
            .network_seen_witness_for(&txids)
            .await
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;

        // bsv-low E649 L6 fold (L1): the eviction ledger, the same projection `/utxo-status` serves. One read of
        // the DISTINCT txids (bounded by the cap above: at most two primary-key statements); a store without the
        // ledger or a faulted read omits the key (never "no eviction" from a fault, and never a failed answer:
        // the money fields stand on their own reads).
        let mut distinct: Vec<String> = Vec::new();
        for (t, _) in &keys {
            if !distinct.contains(t) {
                distinct.push(t.clone());
            }
        }
        let ledger: Option<
            std::collections::HashMap<String, Option<super::storage::EvictionLedgerEntry>>,
        > = match self.storage.evictions_for(&distinct).await {
            Ok(Some(rows)) if rows.len() == distinct.len() => {
                Some(distinct.iter().cloned().zip(rows).collect())
            }
            Ok(Some(_)) | Ok(None) => None,
            Err(e) => {
                tracing::warn!(
                    "ls_pot spentStatus: eviction ledger read faulted, the field is omitted: {e}"
                );
                None
            }
        };

        // Build an input-ordered array: one entry per requested outpoint.
        let mut entries = Vec::with_capacity(outpoints.len());
        for ((op, record), seen) in outpoints.iter().zip(records).zip(witnessed) {
            // bsv-low #937: a row under an OPEN eviction marker is served as its evicted twin is (`known:false`, the
            // ledger's word beside it), never held: the write that landed after the marker is the write-side
            // guard's to move, and until it does the read honours the marker. Zero reads more (the ledger above).
            // A faulted ledger read cannot see a marker: the row is served as stored, as before (stated).
            let record = match &ledger {
                Some(l)
                    if super::storage::open_eviction_hides_row(
                        l.get(&op.txid.to_ascii_lowercase())
                            .and_then(|e| e.as_ref()),
                    ) =>
                {
                    None
                }
                _ => record,
            };
            // Fail-safe: an outpoint we never admitted is `known:false` with
            // null spent/spendingTxid/spentConfirmed — never assert
            // "unspent" for an output we never saw.
            let (known, spent, spending_txid, spent_confirmed, settle_signers) = match record {
                Some(r) => (
                    true,
                    serde_json::Value::Bool(r.spent),
                    r.spending_txid
                        .clone()
                        .map(serde_json::Value::String)
                        .unwrap_or(serde_json::Value::Null),
                    serde_json::Value::Bool(r.spent_confirmed),
                    // bsv-low #406: the signer classification, served ONLY
                    // while it shares the CURRENT pointer's lineage (the
                    // verdict-group guard `verdictTxid == spendingTxid`) —
                    // a stale group is a fact about a displaced spender.
                    // 'unresolved' is a storage sentinel, not an answer.
                    match (&r.verdict_txid, &r.spending_txid, &r.settle_signers) {
                        (Some(vt), Some(st), Some(sig))
                            if vt.eq_ignore_ascii_case(st) && sig != "unresolved" =>
                        {
                            serde_json::Value::String(sig.clone())
                        }
                        _ => serde_json::Value::Null,
                    },
                ),
                None => (
                    false,
                    serde_json::Value::Null,
                    serde_json::Value::Null,
                    serde_json::Value::Null,
                    serde_json::Value::Null,
                ),
            };

            let mut entry = serde_json::json!({
                "txid": op.txid,
                "vout": op.vout,
                "known": known,
                "spent": spent,
                "spendingTxid": spending_txid,
                "spentConfirmed": spent_confirmed,
                // #406 (ADDITIVE): who signed the recorded spend — 'coop' /
                // 'tower-a' / 'tower-b', null = not established.
                "settleSigners": settle_signers,
                // admit-fast step 4 (ADDITIVE): has the overlay itself
                // witnessed the network holding THIS tx (the #371 latch —
                // never a caller claim)? null for an outpoint we never
                // admitted. Under admit-fast an admitted pot can be
                // unwitnessed for seconds: the felt says "awaiting the
                // network" until this flips, from the pots-room push.
                "networkSeen": if known { serde_json::Value::Bool(seen.is_some()) } else { serde_json::Value::Null },
                // bsv-low #621 (L5, ADDITIVE): the witness as the tracker's `seen` HINT with its source and time
                // (`{word: "seen", tier: "hint", source, at}`; `source` null on a row latched before it shipped);
                // null when unwitnessed or unknown. A hint, never evidence: no money view reads it.
                "networkSeenBy": if known { super::storage::served_seen_by_value(seen.as_ref()) } else { serde_json::Value::Null },
            });
            if let Some(ledger) = &ledger {
                let row = ledger.get(&op.txid.to_ascii_lowercase()).copied().flatten();
                entry["evicted"] = match row {
                    Some(r) => super::storage::served_eviction_value(
                        r.evicted_at,
                        r.readmitted_at,
                        r.chain_word,
                    ),
                    None => serde_json::Value::Null,
                };
            }
            entries.push(entry);
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

impl PotLookupService {
    /// bsv-low #936: the pot's own atomic BEEF, named `txid`, for the spend
    /// of `txid:output_index`: from the source the spender's bytes CARRY,
    /// else (the lens's LOW-2: a proven spender, txid-only ancestry) from
    /// the ENGINE's stored copy (`PotStorage::held_transaction_beef`).
    /// Either is hash-bound: its subject's id must be `txid`. `Ok(None)`
    /// when neither yields it; a fault of the stored copy's read is the
    /// hook's fault ("not now", as the record read's is).
    async fn held_output_bytes(
        &self,
        txid: &str,
        output_index: u32,
        spending_tx: &Transaction,
    ) -> Result<Option<Vec<u8>>, LookupServiceError> {
        let carried = spending_tx
            .inputs
            .iter()
            .find(|i| {
                i.source_txid
                    .as_deref()
                    .is_some_and(|t| t.eq_ignore_ascii_case(txid))
                    && i.source_output_index == output_index
            })
            .and_then(|i| i.source_transaction.as_deref());
        let (from, beef) = match carried {
            Some(source) => {
                match source.to_beef(true) {
                    Ok(bytes) => ("the spender's carried source", bytes),
                    Err(e) => {
                        debug!("POT: the carried source of {txid}:{output_index} did not serialize: {e}");
                        return Ok(None);
                    }
                }
            }
            None => match self
                .storage
                .held_transaction_beef(txid)
                .await
                .map_err(|e| {
                    LookupServiceError::StorageError(format!(
                        "the engine's copy of {txid} faulted (not now, bsv-low #936): {e}"
                    ))
                })? {
                Some(bytes) => ("the engine's stored copy", bytes),
                None => {
                    debug!(
                        "POT: the spend of {txid}:{output_index} names an outpoint this index \
                         holds no bytes for, the spender does not carry its transaction and \
                         the engine holds no copy: nothing recorded (bsv-low #936)"
                    );
                    return Ok(None);
                }
            },
        };
        let atomic = match beef_limits::parse_beef(&beef, &beef_limits::DISCOVERY_BEEF_LIMITS)
            .and_then(|mut beef| beef.to_binary_atomic(txid))
        {
            Ok(bytes) => bytes,
            Err(e) => {
                debug!("POT: {from} of {txid}:{output_index} did not re-serialize: {e}");
                return Ok(None);
            }
        };
        match beef_limits::transaction_from_beef(&atomic, None, &beef_limits::DISCOVERY_BEEF_LIMITS)
        {
            Ok(tx) if tx.id().eq_ignore_ascii_case(txid) => Ok(Some(atomic)),
            Ok(tx) => {
                debug!(
                    "POT: {from} of {txid}:{output_index} is {}: nothing recorded",
                    tx.id()
                );
                Ok(None)
            }
            Err(e) => {
                debug!("POT: {from} of {txid}:{output_index} did not parse: {e}");
                Ok(None)
            }
        }
    }

    /// bsv-low #936 (the FIX-936 lens 3's LOW-1): whether the eviction
    /// ledger holds `txid` OPEN, asked on the heal path only (a record or
    /// bytes absent), never on the common spend. Two half-done ledger
    /// states put a held coin with no record in front of this hook: a
    /// spend straddling the shadow move (`pot_records` moved first, the
    /// coin still held) and a readmission whose `pot_records` copy back
    /// faulted (the coin restored, the ledger open). No guard re-evicts a
    /// heal (the write-side guard names the subject, `guard_landed` the
    /// landed bodies), so the heal writes nothing under an open marker:
    /// the pointer is then the no-op it was before #936, and the
    /// readmission's restore brings the real row back with its history.
    /// The ledger read is [`PotStorage::evictions_for`] (one primary-key
    /// statement on D1; a store that serves no ledger answers `None`, and
    /// heals as before). A faulted read is the hook's fault ("not now").
    async fn heal_under_open_eviction(&self, txid: &str) -> Result<bool, LookupServiceError> {
        let key = txid.to_ascii_lowercase();
        let rows = self
            .storage
            .evictions_for(std::slice::from_ref(&key))
            .await
            .map_err(|e| {
                LookupServiceError::StorageError(format!(
                    "eviction ledger read for {txid} faulted on the heal path (not now, \
                     bsv-low #936): {e}"
                ))
            })?;
        let open = rows.is_some_and(|rows| {
            super::storage::open_eviction_hides_row(rows.first().and_then(|e| e.as_ref()))
        });
        if open {
            warn!(
                "POT: {txid} is under an OPEN eviction: the spend's heal writes nothing (the \
                 readmission restores its row; bsv-low #936, lens 3 LOW-1)"
            );
        }
        Ok(open)
    }

    /// bsv-low #936 (the FIX-936 lens's LOW-3): a pot RECORDED without its
    /// bytes (a heal or an admission whose byte write faulted after its
    /// record landed: a replay then finds the record and the heal does not
    /// run) has its bytes written at the spend. One probe read; a write
    /// only when the store holds no bytes for `txid` (so no stored row,
    /// verified or not, is ever touched). A fault is the hook's fault.
    async fn complete_held_output_bytes(
        &self,
        txid: &str,
        output_index: u32,
        topic: &str,
        spending_tx: &Transaction,
    ) -> Result<(), LookupServiceError> {
        let held = self.storage.has_beef(txid).await.map_err(|e| {
            LookupServiceError::StorageError(format!(
                "pot bytes probe for {txid} faulted (not now, bsv-low #936): {e}"
            ))
        })?;
        if held || self.heal_under_open_eviction(txid).await? {
            return Ok(());
        }
        let Some(atomic) = self
            .held_output_bytes(txid, output_index, spending_tx)
            .await?
        else {
            return Ok(());
        };
        self.storage
            .store_beef(txid, &atomic)
            .await
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;
        let spending_txid = spending_tx.id();
        warn!(
            "POT: {txid}:{output_index} under {topic} was recorded without its bytes: written at \
             its spend by {spending_txid} (bsv-low #936, LOW-3)"
        );
        self.storage
            .note_bytes_completed_at_spend(txid, output_index, topic, &spending_txid);
        Ok(())
    }

    /// bsv-low #936: the spend of `txid:output_index` under `topic` names
    /// an outpoint this index never recorded while the engine HELD it (the
    /// module docs). Record it from the pot's own bytes (the spender's
    /// carried source, else the engine's stored copy) through the admission
    /// hook itself (one writer, its shape gate included), and answer the
    /// record; `Ok(None)` when neither yields the bytes, or the hook wrote
    /// nothing (not the topic's shape). A storage fault is the hook's
    /// fault, as the admission's own is.
    async fn record_held_output_from_spender(
        &self,
        txid: &str,
        output_index: u32,
        topic: &str,
        spending_tx: &Transaction,
    ) -> Result<Option<PotRecord>, LookupServiceError> {
        if self.heal_under_open_eviction(txid).await? {
            return Ok(None);
        }
        let Some(atomic_beef) = self
            .held_output_bytes(txid, output_index, spending_tx)
            .await?
        else {
            return Ok(None);
        };
        self.output_admitted_by_topic(&OutputAdmittedByTopic::WholeTx {
            atomic_beef,
            output_index,
            topic: topic.to_string(),
            off_chain_values: None,
        })
        .await?;
        let recorded = self
            .storage
            .get_spent_status(txid, output_index)
            .await
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;
        if recorded.is_some() {
            let spending_txid = spending_tx.id();
            warn!(
                "POT: {txid}:{output_index} under {topic} was held by the engine with no record \
                 here (its admission write was cut after the engine's insert): recorded from the \
                 spender {spending_txid}'s bytes before its spend pointer (bsv-low #936)"
            );
            self.storage
                .note_recorded_at_spend(txid, output_index, topic, &spending_txid);
        }
        Ok(recorded)
    }
}

// ============================================================================
// Tests
// ============================================================================

/// bsv-low P4 slice 2: the exact fee of a SINGLE-input spend of the admitted
/// pot outpoint, from the ADMITTED funding value: `pot_sats − Σ outputs`.
/// `None` unless the tx's only input is exactly `(pot_txid, pot_vout)`, every
/// output value is present and the subtraction does not underflow — never an
/// estimate, never a multi-input guess (other inputs' values are unknown).
/// bsv-low #468: the spend's total output value to each COMMITTED pay home
/// (the P2PKH of the record's decoded `payPkhA` / `payPkhB`). `None` for a
/// home the record does not name (or a malformed hash); `Some(0)` for a named
/// home this spend pays nothing (the loser's, a refund to the other seat). A
/// spend with no output value known contributes nothing to a sum (the raw was
/// parsed without values only on a truncated wire, which the caller rejects
/// earlier). Pure over the parsed tx.
pub fn spend_payouts(
    spending_tx: &Transaction,
    pay_pkh_a_hex: Option<&str>,
    pay_pkh_b_hex: Option<&str>,
) -> (Option<u64>, Option<u64>) {
    fn lock_of(pkh_hex: Option<&str>) -> Option<Vec<u8>> {
        let hex = pkh_hex?;
        let bytes = hex::decode(hex).ok()?;
        let pkh: [u8; 20] = bytes.as_slice().try_into().ok()?;
        Some(crate::pot::covenant::p2pkh_lock(&pkh))
    }
    fn sum_to(spending_tx: &Transaction, lock: &[u8]) -> u64 {
        spending_tx
            .outputs
            .iter()
            .filter(|o| o.locking_script.to_binary() == lock)
            .filter_map(|o| o.satoshis)
            .fold(0u64, |acc, v| acc.saturating_add(v))
    }
    let a = lock_of(pay_pkh_a_hex).map(|l| sum_to(spending_tx, &l));
    let b = lock_of(pay_pkh_b_hex).map(|l| sum_to(spending_tx, &l));
    (a, b)
}

pub(crate) fn single_input_pot_fee(
    spending_tx: &Transaction,
    pot_txid: &str,
    pot_vout: u32,
    pot_sats: Option<u64>,
) -> Option<u64> {
    let pot_sats = pot_sats?;
    let [only] = spending_tx.inputs.as_slice() else {
        return None;
    };
    let sources_pot = only
        .source_txid
        .as_deref()
        .is_some_and(|t| t.eq_ignore_ascii_case(pot_txid))
        && only.source_output_index == pot_vout;
    if !sources_pot {
        return None;
    }
    let mut out_sum: u64 = 0;
    for o in &spending_tx.outputs {
        out_sum = out_sum.checked_add(o.satoshis?)?;
    }
    pot_sats.checked_sub(out_sum)
}

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

    /// LOOP-2 FLEET PIN (2026-09-05, pair-17 DEFINITIVE): the real 57,100-byte
    /// JOIN body p2's felt POSTed — its ancestry lacks one grandparent, so the
    /// wire-last (and sorted-last) tx is p2's HOP, not the JOIN `5ad2764c…`.
    /// This hook re-parses its payload with `from_beef(_, None)`: fed the
    /// PLAIN body it judged the hop, failed the pot shape check and wrote NO
    /// record (the engine had admitted the pot: `known:false` for a mined
    /// pot). The engine now hands lookup services a body that NAMES the
    /// subject (BRC-95 atomic prefix over the whole body) — both halves
    /// pinned here: the named body records the pot; the same bytes un-named
    /// still skip (the trap, documented at the service's own door).
    const LOOP2_INCOMPLETE_JOIN_BEEF: &[u8] = include_bytes!(
        "../../../overlay-cloudflare/tests/fixtures/ef/loop2_join_5ad2764c_incomplete.beef"
    );
    const LOOP2_JOIN_TXID: &str =
        "5ad2764c5151592915ccfc2e1ac2cbc763a34c3c522aa6f98655f1fc88559bb8";

    #[tokio::test]
    async fn loop2_named_body_records_the_pot_the_plain_body_missed() {
        use bsv_rs::transaction::Beef;
        // the trap: un-named, the hook sees the wire-last hop → no record
        let (svc, storage) = make_service_with_storage();
        svc.output_admitted_by_topic(&admit(LOOP2_INCOMPLETE_JOIN_BEEF.to_vec(), 0))
            .await
            .unwrap();
        assert_eq!(
            storage.record_count(),
            0,
            "the plain body's wire-last is a hop — skipped"
        );

        // the fix: the engine names the subject; the hook records the pot
        let mut beef = Beef::from_binary(LOOP2_INCOMPLETE_JOIN_BEEF).unwrap();
        let named = beef.to_binary_atomic(LOOP2_JOIN_TXID).unwrap();
        let (svc, storage) = make_service_with_storage();
        svc.output_admitted_by_topic(&admit(named, 0))
            .await
            .unwrap();
        assert_eq!(storage.record_count(), 1);
        let arr = spent_status(
            &svc,
            serde_json::json!([{"txid": LOOP2_JOIN_TXID, "vout": 0}]),
        )
        .await;
        assert_eq!(arr[0]["known"], true);
        assert_eq!(arr[0]["spent"], false);
    }

    /// bsv-low E649 L6 fold (L1): `spentStatus` serves the eviction ledger beside `known`, the same projection
    /// `/utxo-status` serves: `{at, word, readmittedAt}` for a row (`rejected` while open, null once
    /// readmitted), `null` when the ledger holds none, the key ABSENT where the store serves no ledger or the
    /// read faulted (never "no eviction" from a fault).
    #[tokio::test]
    async fn spent_status_serves_the_eviction_ledger() {
        let (svc, storage) = make_service_with_storage();
        let open = "ab".repeat(32);
        let readmitted = "cd".repeat(32);
        let never = "ef".repeat(32);
        let ask = serde_json::json!([
            {"txid": open.to_ascii_uppercase(), "vout": 0},
            {"txid": readmitted, "vout": 0},
            {"txid": never, "vout": 0},
        ]);
        // a store that serves no ledger: the key is absent on every entry
        let arr = spent_status(&svc, ask.clone()).await;
        assert!(
            arr[0].get("evicted").is_none(),
            "not served: absent, never null"
        );
        storage.serve_evictions();
        storage.record_eviction(&open, 1_760_000_000_000, None);
        storage.record_eviction(&readmitted, 1_760_000_000_000, Some(1_760_000_060_000));
        let arr = spent_status(&svc, ask.clone()).await;
        assert_eq!(arr[0]["known"], false);
        assert_eq!(
            arr[0]["evicted"],
            serde_json::json!({"at": 1_760_000_000_000_i64, "word": "rejected", "readmittedAt": null}),
            "an open row (case-insensitive txid)"
        );
        assert_eq!(
            arr[1]["evicted"],
            serde_json::json!({"at": 1_760_000_000_000_i64, "word": null, "readmittedAt": 1_760_000_060_000_i64})
        );
        assert_eq!(
            arr[2]["evicted"],
            serde_json::Value::Null,
            "read, no row: null"
        );
        // a faulted ledger read omits the key and still answers the money fields
        storage.fault_evictions();
        let arr = spent_status(&svc, ask).await;
        assert_eq!(arr[0]["known"], false);
        assert!(arr[0].get("evicted").is_none(), "a fault is not served");
    }

    /// bsv-low #937 (L52 pair 13): a pot row written AFTER its open eviction marker (the gated arm's engine write
    /// landing past its answer deadline) is served as the evicted twin is: `known:false`, every money field and
    /// the witness null, the ledger's word beside it; never held. RED on eaaee0ec: `known: true`.
    #[tokio::test]
    async fn fix937_a_a_row_under_an_open_marker_is_served_evicted() {
        let (svc, storage) = make_service_with_storage();
        let pot = funding_tx(41);
        let other = funding_tx(42);
        let txid = pot.id();
        // the marker first (the eviction's step 1), then the late write lands the row
        storage.record_eviction_word(
            &txid,
            1_760_000_000_000,
            None,
            Some(crate::pot::storage::CONFLICTED_WORD),
        );
        svc.output_admitted_by_topic(&admit(beef_of(&pot), 0))
            .await
            .unwrap();
        svc.output_admitted_by_topic(&admit(beef_of(&other), 0))
            .await
            .unwrap();
        storage.mark_network_seen(&txid);
        assert_eq!(storage.record_count(), 2, "the late write landed the row");
        let arr = spent_status(
            &svc,
            serde_json::json!([
                {"txid": txid.to_ascii_uppercase(), "vout": 0},
                {"txid": other.id(), "vout": 0},
            ]),
        )
        .await;
        assert_eq!(
            arr[0],
            serde_json::json!({
                "txid": txid.to_ascii_uppercase(), "vout": 0, "known": false, "spent": null,
                "spendingTxid": null, "spentConfirmed": null, "settleSigners": null,
                "networkSeen": null, "networkSeenBy": null,
                "evicted": {"at": 1_760_000_000_000_i64, "word": "conflicted", "readmittedAt": null},
            }),
            "the evicted twin's shape, the eviction's word"
        );
        assert_eq!(arr[1]["known"], true, "a pot with no marker stays held");
        assert_eq!(arr[1]["evicted"], serde_json::Value::Null);
    }

    /// bsv-low #937: a CLOSED marker (readmitted) holds the row at once (no memo on this path); a faulted ledger
    /// read cannot see a marker and serves the row as stored (the pre-#937 answer, stated).
    #[tokio::test]
    async fn fix937_b_a_readmitted_row_is_held() {
        let (svc, storage) = make_service_with_storage();
        let pot = funding_tx(43);
        let txid = pot.id();
        storage.record_eviction(&txid, 1_760_000_000_000, None);
        svc.output_admitted_by_topic(&admit(beef_of(&pot), 0))
            .await
            .unwrap();
        let ask = serde_json::json!([{"txid": txid, "vout": 0}]);
        assert_eq!(spent_status(&svc, ask.clone()).await[0]["known"], false);
        storage.record_eviction(&txid, 1_760_000_000_000, Some(1_760_000_060_000));
        let arr = spent_status(&svc, ask.clone()).await;
        assert_eq!(arr[0]["known"], true, "readmitted: held at the next read");
        assert_eq!(arr[0]["spent"], false);
        assert_eq!(arr[0]["networkSeen"], false);
        assert_eq!(arr[0]["evicted"]["readmittedAt"], 1_760_000_060_000_i64);
        storage.fault_evictions();
        let arr = spent_status(&svc, ask).await;
        assert_eq!(arr[0]["known"], true);
        assert!(arr[0].get("evicted").is_none());
    }

    /// The rule itself: open hides, readmitted and absent hold; the SQL conjunct is the same predicate.
    #[test]
    fn fix937_the_rule_and_its_sql() {
        use crate::pot::storage::{
            not_under_open_eviction_sql, open_eviction_hides_row, EvictionLedgerEntry,
        };
        let open = EvictionLedgerEntry {
            evicted_at: 1,
            readmitted_at: None,
            chain_word: None,
        };
        let closed = EvictionLedgerEntry {
            readmitted_at: Some(2),
            ..open
        };
        assert!(open_eviction_hides_row(Some(&open)));
        assert!(!open_eviction_hides_row(Some(&closed)));
        assert!(!open_eviction_hides_row(None));
        assert_eq!(
            not_under_open_eviction_sql("p.txid"),
            "NOT EXISTS (SELECT 1 FROM pot_evictions ev WHERE ev.txid = p.txid AND ev.readmittedAt IS NULL)"
        );
    }

    /// admit-fast step 4: `spentStatus` serves the pot's OWN network witness —
    /// `true` once the overlay latched it, `false` for an admitted-but-
    /// unwitnessed pot (the fast admission's window), `null` for an outpoint
    /// never admitted (never "seen" for something we never saw).
    #[tokio::test]
    async fn spent_status_serves_the_pots_own_network_witness() {
        let (svc, storage) = make_service_with_storage();
        let seen = funding_tx(1);
        let unseen = funding_tx(2);
        let seen_txid = seen.id();
        let unseen_txid = unseen.id();
        svc.output_admitted_by_topic(&admit(beef_of(&seen), 0))
            .await
            .unwrap();
        svc.output_admitted_by_topic(&admit(beef_of(&unseen), 0))
            .await
            .unwrap();
        storage.mark_network_seen(&seen_txid.to_ascii_uppercase());
        let arr = spent_status(
            &svc,
            serde_json::json!([
                {"txid": seen_txid, "vout": 0},
                {"txid": unseen_txid, "vout": 0},
                {"txid": "ab".repeat(32), "vout": 0},
            ]),
        )
        .await;
        assert_eq!(arr[0]["known"], true);
        assert_eq!(
            arr[0]["networkSeen"], true,
            "the latched pot is witnessed (case-insensitive)"
        );
        assert_eq!(arr[1]["known"], true);
        assert_eq!(
            arr[1]["networkSeen"], false,
            "admitted, unwitnessed: the fast window"
        );
        assert_eq!(arr[2]["known"], false);
        assert!(
            arr[2]["networkSeen"].is_null(),
            "never admitted: never a witness"
        );
    }

    /// bsv-low #621 (L5): beside the old `networkSeen` (its bytes unchanged), `spentStatus` serves the witness as
    /// the tracker's `seen` hint WITH its source and time; a legacy latch (no pair) serves `source: null`; an
    /// unwitnessed or unknown outpoint serves `null`.
    #[tokio::test]
    async fn spent_status_serves_the_witness_with_its_source_and_time() {
        let (svc, storage) = make_service_with_storage();
        let named = funding_tx(1);
        let legacy = funding_tx(2);
        let unseen = funding_tx(3);
        for tx in [&named, &legacy, &unseen] {
            svc.output_admitted_by_topic(&admit(beef_of(tx), 0))
                .await
                .unwrap();
        }
        storage.mark_network_seen_by(&named.id(), "taal", 1_760_000_000_123);
        storage.mark_network_seen(&legacy.id());
        let arr = spent_status(
            &svc,
            serde_json::json!([
                {"txid": named.id(), "vout": 0},
                {"txid": legacy.id(), "vout": 0},
                {"txid": unseen.id(), "vout": 0},
                {"txid": "ab".repeat(32), "vout": 0},
            ]),
        )
        .await;
        assert_eq!(
            (
                arr[0]["networkSeen"].clone(),
                arr[0]["networkSeenBy"].clone()
            ),
            (
                serde_json::json!(true),
                serde_json::json!({"word": "seen", "tier": "hint", "source": "taal", "at": 1_760_000_000_123_i64})
            )
        );
        assert_eq!(
            arr[1]["networkSeenBy"],
            serde_json::json!({"word": "seen", "tier": "hint", "source": null, "at": null}),
            "a legacy latch names no source"
        );
        assert_eq!(arr[1]["networkSeen"], true);
        assert_eq!(
            (
                arr[2]["networkSeen"].clone(),
                arr[2]["networkSeenBy"].clone()
            ),
            (serde_json::json!(false), serde_json::Value::Null)
        );
        assert!(arr[3]["networkSeen"].is_null() && arr[3]["networkSeenBy"].is_null());
    }

    /// bsv-low #621 (L5): the served eviction word is the chain word the eviction rests on: `conflicted` for the
    /// conflict pass, `rejected` for Arcade's own terminal word, `null` for an eviction on absence alone (the felt
    /// never voids on it).
    #[tokio::test]
    async fn spent_status_serves_the_evictions_chain_word_never_an_absence() {
        let (svc, storage) = make_service_with_storage();
        let conflicted = "ab".repeat(32);
        let rejected = "cd".repeat(32);
        let absent = "ef".repeat(32);
        storage.serve_evictions();
        storage.record_eviction_word(
            &conflicted,
            5,
            None,
            crate::pot::storage::eviction_chain_word(
                "input aa:0 spent by confirmed competitor bb at 9",
            ),
        );
        storage.record_eviction_word(
            &rejected,
            6,
            None,
            crate::pot::storage::eviction_chain_word(
                "REJECTED (arcade live REJECTED: bad; both indexers absent)",
            ),
        );
        storage.record_eviction_word(
            &absent,
            7,
            None,
            crate::pot::storage::eviction_chain_word(
                "SEEN_ON_NETWORK (arcade missing, both indexers absent)",
            ),
        );
        let arr = spent_status(
            &svc,
            serde_json::json!([{"txid": conflicted, "vout": 0}, {"txid": rejected, "vout": 0}, {"txid": absent, "vout": 0}]),
        )
        .await;
        assert_eq!(arr[0]["evicted"]["word"], "conflicted");
        assert_eq!(arr[1]["evicted"]["word"], "rejected");
        assert_eq!(
            arr[2]["evicted"],
            serde_json::json!({"at": 7, "word": null, "readmittedAt": null}),
            "an eviction on absence alone serves no chain word"
        );
    }

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

    /// A parent tx paying `value` sats to a P2PKH — attached as the SOURCE of
    /// a funding input so the BEEF carries it (exact fee derivable).
    fn parent_tx(value: u64, salt: u8) -> Tx {
        let mut p = Tx::new();
        p.add_input(TransactionInput::new(hex::encode([salt; 32]), 1))
            .unwrap();
        let mut o = p2pkh_output();
        o.satoshis = Some(value);
        p.add_output(o).unwrap();
        p
    }

    /// bsv-low P4 slice 2: the funding tx's OWN size + exact fee are read from
    /// the admitted BEEF (fee only when the parent rides along) and stored on
    /// the record — never estimated.
    #[tokio::test]
    async fn admit_records_the_funding_size_and_fee_from_the_beef() {
        let (svc, storage) = make_service_with_storage();
        // Exact: the BEEF carries the parent (100_000 in, 2_500 pot out).
        let parent = parent_tx(100_000, 0x51);
        let mut funding = Tx::new();
        funding
            .add_input(TransactionInput::with_source_transaction(parent.clone(), 0))
            .unwrap();
        funding
            .add_output(make_covenant_output(&dummy_params()))
            .unwrap();
        let ftx = funding.id();
        svc.output_admitted_by_topic(&admit(beef_of(&funding), 0))
            .await
            .unwrap();
        let r = storage
            .get_spent_status(&ftx, 0)
            .await
            .unwrap()
            .expect("admitted");
        let pot_sats = r.pot_sats.expect("pot value from the admitted output");
        assert_eq!(r.funding_size_bytes, Some(funding.to_binary().len() as u64));
        assert_eq!(r.funding_fee_sats, Some(100_000 - pot_sats));
        // Partial BEEF (no parent): size only, fee None — never a guess.
        let lone = funding_tx(0x52);
        svc.output_admitted_by_topic(&admit(beef_of(&lone), 0))
            .await
            .unwrap();
        let r2 = storage
            .get_spent_status(&lone.id(), 0)
            .await
            .unwrap()
            .expect("admitted");
        assert_eq!(r2.funding_size_bytes, Some(lone.to_binary().len() as u64));
        assert_eq!(r2.funding_fee_sats, None);
    }

    /// bsv-low P4 slice 2: the SPENDER's facts ride the pointer — exact fee
    /// from the ADMITTED pot value for a single-input pot spend even when the
    /// spending BEEF is partial, keyed by `spenderFactsTxid`.
    #[tokio::test]
    async fn spend_records_the_spender_size_and_fee_under_the_pointer() {
        let (svc, storage) = make_service_with_storage();
        let funding = funding_tx(0x61);
        let ftx = funding.id();
        svc.output_admitted_by_topic(&admit(beef_of(&funding), 0))
            .await
            .unwrap();
        let pot_sats = storage
            .get_spent_status(&ftx, 0)
            .await
            .unwrap()
            .unwrap()
            .pot_sats
            .expect("pot value");
        let settle = settle_tx(&ftx, 0); // one input (the pot), one 546-sat P2PKH output
        let stx = settle.id();
        svc.output_spent(&spent(&ftx, 0, beef_of(&settle)))
            .await
            .unwrap();
        let r = storage.get_spent_status(&ftx, 0).await.unwrap().unwrap();
        assert_eq!(r.spending_txid.as_deref(), Some(stx.as_str()));
        assert_eq!(r.spender_facts_txid.as_deref(), Some(stx.as_str()));
        assert_eq!(r.spender_size_bytes, Some(settle.to_binary().len() as u64));
        assert_eq!(r.spender_fee_sats, Some(pot_sats - 546));
    }

    #[test]
    fn single_input_pot_fee_refuses_multi_input_foreign_input_and_underflow() {
        let pot = "ab".repeat(32);
        let mut one = Tx::new();
        one.add_input(TransactionInput::new(pot.clone(), 0))
            .unwrap();
        one.add_output(p2pkh_output()).unwrap(); // 546 out
        assert_eq!(
            single_input_pot_fee(&one, &pot, 0, Some(2_500)),
            Some(2_500 - 546)
        );
        assert_eq!(
            single_input_pot_fee(&one, &pot, 0, None),
            None,
            "no admitted value → no fee"
        );
        assert_eq!(
            single_input_pot_fee(&one, &pot, 1, Some(2_500)),
            None,
            "wrong vout → not the pot"
        );
        assert_eq!(
            single_input_pot_fee(&one, &"cd".repeat(32), 0, Some(2_500)),
            None,
            "wrong txid"
        );
        assert_eq!(
            single_input_pot_fee(&one, &pot, 0, Some(100)),
            None,
            "outputs exceed the pot → underflow refused"
        );
        let mut two = Tx::new();
        two.add_input(TransactionInput::new(pot.clone(), 0))
            .unwrap();
        two.add_input(TransactionInput::new("ef".repeat(32), 0))
            .unwrap();
        two.add_output(p2pkh_output()).unwrap();
        assert_eq!(
            single_input_pot_fee(&two, &pot, 0, Some(2_500)),
            None,
            "a second input's value is unknown"
        );
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

    /// bsv-low #406: the spentStatus answer serves the signer classification
    /// under the verdict-group guard — current-pointer lineage only, and the
    /// 'unresolved' storage sentinel never reaches the wire.
    #[tokio::test]
    async fn spent_status_serves_settle_signers_under_the_verdict_guard() {
        use crate::pot::storage::VerdictWrite;
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
        // The synthetic settle carries no signatures → the live latch stored
        // nothing → null on the wire.
        let arr = spent_status(&svc, serde_json::json!([{"txid": pot_txid, "vout": 0}])).await;
        assert!(
            arr[0]["settleSigners"].is_null(),
            "no classification = null"
        );

        // The backfill attaches the group for the CURRENT pointer → served.
        storage
            .mark_verdict_for_spender(
                &pot_txid,
                0,
                &settle.id(),
                VerdictWrite {
                    verdict: "winner-b",
                    settle_signers: Some("tower-b"),
                },
            )
            .await
            .unwrap();
        let arr = spent_status(&svc, serde_json::json!([{"txid": pot_txid, "vout": 0}])).await;
        assert_eq!(arr[0]["settleSigners"], "tower-b");

        // 'unresolved' is a storage sentinel, never an answer.
        storage
            .mark_verdict_for_spender(
                &pot_txid,
                0,
                &settle.id(),
                VerdictWrite {
                    verdict: "winner-b",
                    settle_signers: Some("unresolved"),
                },
            )
            .await
            .unwrap();
        let arr = spent_status(&svc, serde_json::json!([{"txid": pot_txid, "vout": 0}])).await;
        assert!(
            arr[0]["settleSigners"].is_null(),
            "'unresolved' never narrates"
        );

        // A DISPLACED pointer strands the group → the guard nulls it.
        storage
            .mark_verdict_for_spender(
                &pot_txid,
                0,
                &settle.id(),
                VerdictWrite {
                    verdict: "winner-b",
                    settle_signers: Some("coop"),
                },
            )
            .await
            .unwrap();
        let displacing = settle_tx(&pot_txid, 0);
        // A different spender claims the outpoint (unconfirmed overwrite) —
        // same tx shape, but mark_spent with a foreign txid moves the pointer.
        storage
            .mark_spent(
                &pot_txid,
                0,
                "ff".repeat(32).as_str(),
                false,
                None,
                None,
                None,
            )
            .await
            .unwrap();
        let _ = displacing;
        let arr = spent_status(&svc, serde_json::json!([{"txid": pot_txid, "vout": 0}])).await;
        assert!(
            arr[0]["settleSigners"].is_null(),
            "a stale verdict group is a fact about a displaced spender — never served"
        );
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
        assert_eq!(
            r.pub_tower.as_deref(),
            Some(hex::encode(p.pub_tower).as_str())
        );
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

    /// bsv-low #936 (loop 52's S4, beta 2026-10-27): the JOIN `fad01028...`
    /// was admitted PENDING and SEEN-latched, and at 17:5xZ D1 held its
    /// `tm_pot` applied row over ZERO outputs, no `pot_records`, no
    /// `pot_beefs`, no eviction record. READ-936 named the two steps: the
    /// gated request was cancelled by the client while it owned the engine
    /// write, which stopped AFTER the engine's `insert_output` and BEFORE
    /// this service's `store_record` (a held coin, no index row, no applied
    /// row); the refund's admission at 16:26Z then found the coin, recorded
    /// the JOIN applied (#559's H2) and deleted it. These pins reproduce the
    /// two steps on the engine over a cut lookup hook and hold the fix: the
    /// spend hook, told of a spend the engine made of a coin the TOPIC HELD
    /// with no record here, records the pot from the spender's own bytes
    /// before it writes the pointer, so the pot stands recorded and spent by
    /// its refund, exactly as a whole write leaves it.
    mod fix936 {
        use super::*;
        use overlay_engine::builder::EngineBuilder;
        use overlay_engine::engine::Engine;
        use overlay_engine::storage::memory::MemoryStorage;
        use overlay_engine::storage::Storage;
        use overlay_engine::types::{AppliedTransaction, SubmitMode, TaggedBEEF};
        use std::cell::Cell;
        use std::future::Future;
        use std::task::{Context, Poll};

        const TOPIC: &str = "tm_pot";

        /// `ls_pot` behind a door that HANGS the first admission hook it is
        /// told: the engine's write then stops exactly where the cancelled
        /// request's did on beta (after `insert_output`, before
        /// `store_record`), and dropping the submit's future is the cut.
        struct CutOnce {
            inner: PotLookupService,
            armed: Cell<bool>,
            reached: Rc<Cell<bool>>,
        }

        #[async_trait(?Send)]
        impl LookupService for CutOnce {
            fn admission_mode(&self) -> AdmissionMode {
                self.inner.admission_mode()
            }
            fn spend_notification_mode(&self) -> SpendNotificationMode {
                self.inner.spend_notification_mode()
            }
            fn reads_off_chain_values(&self, topic: &str) -> bool {
                self.inner.reads_off_chain_values(topic)
            }
            async fn output_admitted_by_topic(
                &self,
                payload: &OutputAdmittedByTopic,
            ) -> Result<(), LookupServiceError> {
                if self.armed.replace(false) {
                    self.reached.set(true);
                    std::future::pending::<()>().await;
                }
                self.inner.output_admitted_by_topic(payload).await
            }
            async fn output_spent(&self, payload: &OutputSpent) -> Result<(), LookupServiceError> {
                self.inner.output_spent(payload).await
            }
            async fn output_evicted(
                &self,
                txid: &str,
                output_index: u32,
            ) -> Result<(), LookupServiceError> {
                self.inner.output_evicted(txid, output_index).await
            }
            async fn lookup(
                &self,
                question: &LookupQuestion,
            ) -> Result<LookupResult, LookupServiceError> {
                self.inner.lookup(question).await
            }
            async fn get_documentation(&self) -> String {
                self.inner.get_documentation().await
            }
            async fn get_metadata(&self) -> ServiceMetadata {
                self.inner.get_metadata().await
            }
        }

        struct Fixture {
            engine: Engine,
            store: Rc<MemoryStorage>,
            pots: Rc<MemoryPotStorage>,
            reached: Rc<Cell<bool>>,
        }

        /// A real engine over `tm_pot` and `ls_pot` (the production pair),
        /// the service's first admission hook armed to hang.
        fn fixture() -> Fixture {
            fixture_armed(true)
        }

        /// The same pair; `armed: false` admits the first write whole.
        fn fixture_armed(armed: bool) -> Fixture {
            let store = Rc::new(MemoryStorage::new());
            let pots = Rc::new(MemoryPotStorage::new());
            let reached = Rc::new(Cell::new(false));
            let svc = CutOnce {
                inner: PotLookupService::new(pots.clone()),
                armed: Cell::new(armed),
                reached: reached.clone(),
            };
            let engine = EngineBuilder::new(Box::new(store.clone()))
                .with_topic(
                    TOPIC,
                    Box::new(crate::pot::topic_manager::PotTopicManager::new()),
                )
                .with_lookup("ls_pot", Box::new(svc))
                .build();
            Fixture {
                engine,
                store,
                pots,
                reached,
            }
        }

        fn tagged(tx: &Tx) -> TaggedBEEF {
            TaggedBEEF::new(beef_of(tx), vec![TOPIC.into()])
        }

        /// Drive `fut` until it is Pending with the cut hook reached, then
        /// DROP it: the write's cut, as the platform's cancel left it. A
        /// future that completes first is the test's failure (nothing cut).
        fn cut_at_the_hook<F: Future>(fut: F, reached: &Cell<bool>) {
            let mut fut = Box::pin(fut);
            let mut cx = Context::from_waker(std::task::Waker::noop());
            for _ in 0..64 {
                match fut.as_mut().poll(&mut cx) {
                    Poll::Ready(_) => panic!("the write completed: nothing was cut"),
                    Poll::Pending if reached.get() => return,
                    Poll::Pending => continue,
                }
            }
            panic!("the write never reached the admission hook");
        }

        async fn applied(store: &MemoryStorage, txid: &str) -> bool {
            store
                .does_applied_transaction_exist(&AppliedTransaction {
                    txid: txid.into(),
                    topic: TOPIC.into(),
                })
                .await
                .unwrap()
        }

        async fn coin_held(store: &MemoryStorage, txid: &str) -> bool {
            store
                .find_output(txid, 0, Some(TOPIC), None, false)
                .await
                .unwrap()
                .is_some()
        }

        /// The pre-signed refund of `pot` (the spender the live shape had):
        /// the whole pot less the fee to seat A's committed home, the pot's
        /// own transaction carried as its input's source, as every valid
        /// BEEF carries it.
        fn refund_of(pot: &Tx, p: &CovenantParams) -> Tx {
            let mut refund = spender_paying(&pot.id(), 0, &[(2400, p2pkh_lock(&p.pay_pkh_a))]);
            refund.inputs[0].source_transaction = Some(Box::new(pot.clone()));
            refund
        }

        /// The two steps of the live shape, then the index after the fix:
        /// the JOIN's write cut at the hook (the coin held, no row here, no
        /// applied row), the SEEN latch, the refund admitted whole (the JOIN
        /// recorded applied by H2, its coin deleted: the engine's normal end
        /// state of every spent pot), and the index holding the pot
        /// RECORDED and SPENT by the refund with the JOIN's bytes under its
        /// txid. RED on 1ab40c95 at the record: `None`.
        #[tokio::test]
        async fn fix936_a_a_write_cut_at_the_hook_then_its_refund_leaves_the_pot_recorded() {
            let f = fixture();
            let p = real_params();
            let join = real_funding_tx(0x36, &p);
            let join_txid = join.id();

            // Step 1: the gated write, cut between the engine's insert and
            // this service's store.
            cut_at_the_hook(
                f.engine
                    .submit_with_report(&tagged(&join), SubmitMode::HistoricalTxNoSpv),
                &f.reached,
            );
            assert!(
                coin_held(&f.store, &join_txid).await,
                "the engine holds the coin"
            );
            assert!(!applied(&f.store, &join_txid).await, "no applied row yet");
            assert!(
                f.pots
                    .get_spent_status(&join_txid, 0)
                    .await
                    .unwrap()
                    .is_none(),
                "the half write: no pot_records row"
            );
            assert!(
                f.pots.get_beef(&join_txid).await.unwrap().is_none(),
                "the half write: no pot_beefs row"
            );
            // The pending watch's SEEN latch lands on the worker's own table,
            // keyed by txid; it stands whatever the index holds.
            f.pots.mark_network_seen(&join_txid);

            // Step 2: the refund's admission, whole.
            let refund = refund_of(&join, &p);
            let refund_txid = refund.id();
            let (_, report) = f
                .engine
                .submit_with_report(&tagged(&refund), SubmitMode::HistoricalTxNoSpv)
                .await
                .unwrap();
            assert!(report.is_durable(), "the refund's write landed: {report:?}");
            assert_eq!(report.applied_topics, vec![TOPIC.to_string()]);

            // The engine's end state is every spent pot's: the JOIN recorded
            // applied by the spender (H2), its coin deleted, the refund applied.
            assert!(
                applied(&f.store, &join_txid).await,
                "H2 recorded the JOIN applied"
            );
            assert!(
                !coin_held(&f.store, &join_txid).await,
                "the coin is deleted"
            );
            assert!(
                f.store
                    .find_outputs_for_transaction(&join_txid, false)
                    .await
                    .unwrap()
                    .is_empty(),
                "an applied row over zero outputs is a spent pot's normal shape"
            );
            assert!(applied(&f.store, &refund_txid).await);

            // The index (RED on 1ab40c95): the pot recorded, spent by the
            // refund, its params decoded from its own lock, its bytes held.
            let rec = f
                .pots
                .get_spent_status(&join_txid, 0)
                .await
                .unwrap()
                .expect("the pot the engine admitted is recorded (bsv-low #936)");
            assert!(rec.spent, "spent by its refund");
            assert_eq!(rec.spending_txid.as_deref(), Some(refund_txid.as_str()));
            assert_eq!(rec.lock_kind.as_deref(), Some("covenant"));
            assert_eq!(rec.decoded_covenant_params(), Some(p));
            assert_eq!(rec.pot_sats, Some(2500));
            let pot_beef = f
                .pots
                .get_beef(&join_txid)
                .await
                .unwrap()
                .expect("the JOIN's bytes under its own txid");
            assert_eq!(
                beef_limits::transaction_from_beef(
                    &pot_beef,
                    None,
                    &beef_limits::DISCOVERY_BEEF_LIMITS
                )
                .unwrap()
                .id(),
                join_txid,
                "the stored bytes name the pot"
            );
            assert!(
                f.pots.get_beef(&refund_txid).await.unwrap().is_some(),
                "the refund's bytes under its txid, as before"
            );
        }

        /// The ledger and the rows agree for every pot the engine recorded
        /// applied: one JOIN written whole and one cut at the hook, each
        /// spent by its refund; every applied `tm_pot` row of a covenant
        /// transaction has its record here, spent by its spender. RED on
        /// 1ab40c95 for the cut one.
        #[tokio::test]
        async fn fix936_b_the_index_holds_every_pot_the_engine_recorded_applied() {
            let f = fixture();
            let p = real_params();
            let cut = real_funding_tx(0x37, &p);
            let whole = real_funding_tx(0x38, &p);
            cut_at_the_hook(
                f.engine
                    .submit_with_report(&tagged(&cut), SubmitMode::HistoricalTxNoSpv),
                &f.reached,
            );
            let (_, report) = f
                .engine
                .submit_with_report(&tagged(&whole), SubmitMode::HistoricalTxNoSpv)
                .await
                .unwrap();
            assert!(report.is_durable(), "{report:?}");

            let mut spenders = Vec::new();
            for pot in [&cut, &whole] {
                let refund = refund_of(pot, &p);
                let (_, report) = f
                    .engine
                    .submit_with_report(&tagged(&refund), SubmitMode::HistoricalTxNoSpv)
                    .await
                    .unwrap();
                assert!(report.is_durable(), "{report:?}");
                spenders.push((pot.id(), refund.id()));
            }

            for (pot_txid, refund_txid) in spenders {
                assert!(applied(&f.store, &pot_txid).await, "{pot_txid} is applied");
                assert!(!coin_held(&f.store, &pot_txid).await);
                let rec = f
                    .pots
                    .get_spent_status(&pot_txid, 0)
                    .await
                    .unwrap()
                    .unwrap_or_else(|| {
                        panic!("{pot_txid}: applied in {TOPIC} with no record (bsv-low #936)")
                    });
                assert!(rec.spent);
                assert_eq!(rec.spending_txid.as_deref(), Some(refund_txid.as_str()));
                assert!(f.pots.get_beef(&pot_txid).await.unwrap().is_some());
            }
            assert_eq!(f.pots.record_count(), 2);
        }

        /// The hook alone: a spend of an outpoint this index never recorded
        /// is recorded from the spender's bytes, through the admission
        /// writer and its shape gate, once; a spender whose bytes do not
        /// carry the source records nothing (the hook as before); a carried
        /// source whose output is not the topic's shape records nothing; a
        /// recorded pot is untouched (its bytes stay the admitted ones); and
        /// the hop arm records a held hop spent by its JOIN. RED on 1ab40c95
        /// at the first record.
        #[tokio::test]
        async fn fix936_c_the_spend_hook_records_a_held_pot_from_the_spenders_bytes() {
            let (svc, pots) = make_service_with_storage();
            let p = real_params();

            // (1) A never-recorded pot, its refund carrying it: recorded,
            // spent, its bytes stored; told twice, one record, one pointer.
            let pot = real_funding_tx(0x39, &p);
            let refund = refund_of(&pot, &p);
            for _ in 0..2 {
                svc.output_spent(&spent(&pot.id(), 0, beef_of(&refund)))
                    .await
                    .unwrap();
            }
            let rec = pots
                .get_spent_status(&pot.id(), 0)
                .await
                .unwrap()
                .expect("recorded from the spender's bytes (bsv-low #936)");
            assert!(rec.spent);
            assert_eq!(rec.spending_txid.as_deref(), Some(refund.id().as_str()));
            assert_eq!(rec.decoded_covenant_params(), Some(p.clone()));
            assert_eq!(pots.record_count(), 1);
            assert_eq!(pots.beef_count(), 2, "the pot's bytes and the refund's");

            // (2) A spender that does not carry its source: nothing recorded.
            let bare = spender_paying(
                &real_funding_tx(0x3a, &p).id(),
                0,
                &[(2400, p2pkh_lock(&p.pay_pkh_a))],
            );
            let bare_pot = bare.inputs[0].source_txid.clone().unwrap();
            svc.output_spent(&spent(&bare_pot, 0, beef_of(&bare)))
                .await
                .unwrap();
            assert!(pots.get_spent_status(&bare_pot, 0).await.unwrap().is_none());
            assert!(pots.get_beef(&bare_pot).await.unwrap().is_none());

            // (3) A carried source whose spent output is P2PKH under tm_pot:
            // the shape gate refuses the record, as at an admission.
            let mut plain = Tx::new();
            plain
                .add_input(TransactionInput::new(hex::encode([0x3b; 32]), 0))
                .unwrap();
            plain.add_output(p2pkh_output()).unwrap();
            let mut plain_spend = settle_tx(&plain.id(), 0);
            plain_spend.inputs[0].source_transaction = Some(Box::new(plain.clone()));
            svc.output_spent(&spent(&plain.id(), 0, beef_of(&plain_spend)))
                .await
                .unwrap();
            assert!(pots
                .get_spent_status(&plain.id(), 0)
                .await
                .unwrap()
                .is_none());

            // (4) A recorded pot is untouched: its bytes stay the admitted ones.
            let known = real_funding_tx(0x3c, &p);
            let admitted_bytes = beef_of(&known);
            svc.output_admitted_by_topic(&admit(admitted_bytes.clone(), 0))
                .await
                .unwrap();
            svc.output_spent(&spent(&known.id(), 0, beef_of(&refund_of(&known, &p))))
                .await
                .unwrap();
            assert_eq!(
                pots.get_beef(&known.id()).await.unwrap().as_deref(),
                Some(admitted_bytes.as_slice())
            );

            // (5) The hop arm: a held P2PKH hop spent by a JOIN that carries
            // it, under tm_lowfund, recorded `p2pkh` and spent by the JOIN.
            let mut hop = Tx::new();
            hop.add_input(TransactionInput::new(hex::encode([0x3d; 32]), 0))
                .unwrap();
            hop.add_output(p2pkh_output()).unwrap();
            let mut join = Tx::new();
            join.add_input(TransactionInput::new(hop.id(), 0)).unwrap();
            join.inputs[0].source_transaction = Some(Box::new(hop.clone()));
            join.add_output(make_covenant_output(&dummy_params()))
                .unwrap();
            svc.output_spent(&OutputSpent::WholeTx {
                txid: hop.id(),
                output_index: 0,
                topic: "tm_lowfund".into(),
                spending_atomic_beef: beef_of(&join),
                off_chain_values: None,
            })
            .await
            .unwrap();
            let hop_rec = pots
                .get_spent_status(&hop.id(), 0)
                .await
                .unwrap()
                .expect("the held hop recorded from the JOIN's bytes");
            assert_eq!(hop_rec.lock_kind.as_deref(), Some("p2pkh"));
            assert!(hop_rec.spent);
            assert_eq!(hop_rec.spending_txid.as_deref(), Some(join.id().as_str()));
        }

        /// The FIX-936 lens's LOW-1, through the real engine: the JOIN's
        /// write cut at the hook, then its refund's spend hook meets a
        /// FAULTED record read. The read is "not now": the refund does not
        /// land, the engine keeps the JOIN's coin (#657's notification
        /// fault), and the replay records the pot and spends it. RED on
        /// 5eb071da: the faulted read proceeded, the refund landed, the coin
        /// was deleted with no record, and the replay was a dupe.
        #[tokio::test]
        async fn fix936f_l1_a_faulted_record_read_at_the_spend_is_not_now_and_the_replay_records_the_pot(
        ) {
            let f = fixture();
            let p = real_params();
            let join = real_funding_tx(0x41, &p);
            let join_txid = join.id();
            cut_at_the_hook(
                f.engine
                    .submit_with_report(&tagged(&join), SubmitMode::HistoricalTxNoSpv),
                &f.reached,
            );
            assert!(coin_held(&f.store, &join_txid).await);

            let refund = refund_of(&join, &p);
            let refund_txid = refund.id();
            f.pots.fault_next_spent_status_reads(1);
            let first = f
                .engine
                .submit_with_report(&tagged(&refund), SubmitMode::HistoricalTxNoSpv)
                .await;
            let landed = matches!(&first, Ok((_, r)) if r.is_durable());
            assert!(
                !landed,
                "a faulted record read is not now, never a landed spend: {first:?}"
            );
            assert!(
                coin_held(&f.store, &join_txid).await,
                "the engine keeps the pot's coin for the replay"
            );
            assert!(!applied(&f.store, &refund_txid).await);

            // The replay (the queue's): the read answers, the pot is recorded.
            let (_, report) = f
                .engine
                .submit_with_report(&tagged(&refund), SubmitMode::HistoricalTxNoSpv)
                .await
                .unwrap();
            assert!(report.is_durable(), "the replay landed: {report:?}");
            let rec = f
                .pots
                .get_spent_status(&join_txid, 0)
                .await
                .unwrap()
                .expect("the replay records the cut pot (bsv-low #936, LOW-1)");
            assert!(rec.spent);
            assert_eq!(rec.spending_txid.as_deref(), Some(refund_txid.as_str()));
            assert!(f.pots.get_beef(&join_txid).await.unwrap().is_some());
            assert!(!coin_held(&f.store, &join_txid).await);
        }

        /// The lens's LOW-2: a held pot whose spender does NOT carry its
        /// source (a proven spender, txid-only ancestry) is recorded from
        /// the ENGINE's stored copy of the pot's transaction, hash-bound to
        /// the outpoint; a stored copy naming another transaction records
        /// nothing; no copy at all records nothing (the hook as before).
        /// RED on 5eb071da with the store's hook grafted: nothing recorded.
        #[tokio::test]
        async fn fix936f_l2_a_spender_without_the_pots_bytes_records_it_from_the_engines_copy() {
            let (svc, pots) = make_service_with_storage();
            let p = real_params();

            // (1) The engine's copy is the pot's own BEEF.
            let pot = real_funding_tx(0x42, &p);
            let bare = spender_paying(&pot.id(), 0, &[(2400, p2pkh_lock(&p.pay_pkh_a))]);
            assert!(bare.inputs[0].source_transaction.is_none());
            pots.hold_transaction_beef(&pot.id(), &beef_of(&pot));
            svc.output_spent(&spent(&pot.id(), 0, beef_of(&bare)))
                .await
                .unwrap();
            let rec = pots
                .get_spent_status(&pot.id(), 0)
                .await
                .unwrap()
                .expect("recorded from the engine's stored copy (bsv-low #936, LOW-2)");
            assert!(rec.spent);
            assert_eq!(rec.spending_txid.as_deref(), Some(bare.id().as_str()));
            assert_eq!(rec.decoded_covenant_params(), Some(p.clone()));
            let stored = pots
                .get_beef(&pot.id())
                .await
                .unwrap()
                .expect("the pot's bytes under its own txid");
            assert_eq!(
                beef_limits::transaction_from_beef(
                    &stored,
                    None,
                    &beef_limits::DISCOVERY_BEEF_LIMITS
                )
                .unwrap()
                .id(),
                pot.id()
            );

            // (2) A copy under the pot's txid that is another transaction:
            // the hash binding refuses it, nothing recorded.
            let other_pot = real_funding_tx(0x43, &p);
            let decoy = real_funding_tx(0x44, &p);
            let bare2 = spender_paying(&other_pot.id(), 0, &[(2400, p2pkh_lock(&p.pay_pkh_a))]);
            pots.hold_transaction_beef(&other_pot.id(), &beef_of(&decoy));
            svc.output_spent(&spent(&other_pot.id(), 0, beef_of(&bare2)))
                .await
                .unwrap();
            assert!(pots
                .get_spent_status(&other_pot.id(), 0)
                .await
                .unwrap()
                .is_none());
            assert!(pots
                .get_spent_status(&decoy.id(), 0)
                .await
                .unwrap()
                .is_none());
        }

        /// The lens's LOW-3: a pot RECORDED without its bytes (a heal or an
        /// admission whose byte write faulted after its record landed) gets
        /// its bytes at the spend, from the spender's carried source. Both
        /// shapes: (1) the heal itself cut at its byte write, then its
        /// replay; (2) an admission cut at its byte write, then the spend.
        /// A recorded pot WITH bytes keeps them (fix936_c (4)). RED on
        /// 5eb071da: the record present, the heal skipped, no bytes.
        #[tokio::test]
        async fn fix936f_l3_a_pot_recorded_without_its_bytes_gets_them_at_the_spend() {
            let (svc, pots) = make_service_with_storage();
            let p = real_params();

            // (1) The heal's own window: record lands, bytes fault.
            let pot = real_funding_tx(0x45, &p);
            let refund = refund_of(&pot, &p);
            pots.fault_next_beef_writes(1);
            assert!(
                svc.output_spent(&spent(&pot.id(), 0, beef_of(&refund)))
                    .await
                    .is_err(),
                "the heal's byte fault is the hook's fault"
            );
            assert!(pots.get_spent_status(&pot.id(), 0).await.unwrap().is_some());
            assert!(pots.get_beef(&pot.id()).await.unwrap().is_none());
            svc.output_spent(&spent(&pot.id(), 0, beef_of(&refund)))
                .await
                .unwrap();
            let rec = pots.get_spent_status(&pot.id(), 0).await.unwrap().unwrap();
            assert_eq!(rec.spending_txid.as_deref(), Some(refund.id().as_str()));
            assert!(
                pots.get_beef(&pot.id()).await.unwrap().is_some(),
                "the replay completes the heal's bytes (bsv-low #936, LOW-3)"
            );

            // (2) An admission whose byte write faulted, then its spend.
            let known = real_funding_tx(0x46, &p);
            pots.fault_next_beef_writes(1);
            assert!(svc
                .output_admitted_by_topic(&admit(beef_of(&known), 0))
                .await
                .is_err());
            assert!(pots.get_beef(&known.id()).await.unwrap().is_none());
            svc.output_spent(&spent(&known.id(), 0, beef_of(&refund_of(&known, &p))))
                .await
                .unwrap();
            let bytes = pots
                .get_beef(&known.id())
                .await
                .unwrap()
                .expect("the spend completes a recorded pot's missing bytes (LOW-3)");
            assert_eq!(
                beef_limits::transaction_from_beef(
                    &bytes,
                    None,
                    &beef_limits::DISCOVERY_BEEF_LIMITS
                )
                .unwrap()
                .id(),
                known.id()
            );
        }

        /// The FIX-936 lens 3's LOW-1: the heal writes the pot's index rows
        /// below the eviction ledger, and no guard re-evicts a heal (the
        /// write-side guard names the subject, `guard_landed` the landed
        /// bodies). Two real half-done ledger states put a spend's hook in
        /// front of a held coin with no record and an OPEN ledger row.
        ///
        /// (1) THE STRADDLE: the JOIN admitted whole; the eviction's marker
        /// opened and its first table (`pot_records`) moved to the twin
        /// while the engine still holds the coin (`outputs` is the second
        /// table; a spend whose validation read the coin before it). The
        /// refund's hook must record NOTHING under the open marker: the
        /// pointer is the no-op it was before #936, and the readmission's
        /// restore brings the real row back. RED on e8866847: the heal
        /// recorded the pot live, spent by the refund, under the open ledger.
        #[tokio::test]
        async fn fix936f2_l1_a_straddled_eviction_is_never_healed_under_its_open_marker() {
            let f = fixture_armed(false);
            let p = real_params();
            let join = real_funding_tx(0x51, &p);
            let join_txid = join.id();
            let (_, report) = f
                .engine
                .submit_with_report(&tagged(&join), SubmitMode::HistoricalTxNoSpv)
                .await
                .unwrap();
            assert!(report.is_durable(), "the JOIN admitted whole: {report:?}");
            assert!(f
                .pots
                .get_spent_status(&join_txid, 0)
                .await
                .unwrap()
                .is_some());

            // The eviction: the marker FIRST, then pot_records to its twin.
            f.pots.record_eviction(&join_txid, 1_000, None);
            let twin = f.pots.take_record(&join_txid, 0);
            assert!(twin.is_some(), "the shadow move's first table");
            assert!(
                coin_held(&f.store, &join_txid).await,
                "outputs not yet moved"
            );

            let refund = refund_of(&join, &p);
            let (_, report) = f
                .engine
                .submit_with_report(&tagged(&refund), SubmitMode::HistoricalTxNoSpv)
                .await
                .unwrap();
            assert!(
                report.is_durable(),
                "the spend lands as before #936: {report:?}"
            );
            assert!(
                f.pots
                    .get_spent_status(&join_txid, 0)
                    .await
                    .unwrap()
                    .is_none(),
                "nothing healed under an OPEN eviction (bsv-low #936, lens 3 LOW-1)"
            );
        }

        /// The LOW-1's second state, AN INCOMPLETE READMISSION: the eviction
        /// complete (the record in its twin, the coin moved), then the
        /// readmission's restore copies `outputs` back while its
        /// `pot_records` copy faulted (`restore_twins` continues past a
        /// faulted table): the coin held again, no record, the ledger OPEN
        /// (`admit_fast_readmit_incomplete_total`). A spend meanwhile must
        /// heal nothing, or the next restore's `INSERT OR IGNORE` keeps the
        /// heal's row and drops the twin's history. RED on e8866847: healed.
        #[tokio::test]
        async fn fix936f2_l1_an_incomplete_readmission_is_never_healed_under_its_open_marker() {
            let f = fixture_armed(false);
            let p = real_params();
            let join = real_funding_tx(0x52, &p);
            let join_txid = join.id();
            let (_, report) = f
                .engine
                .submit_with_report(&tagged(&join), SubmitMode::HistoricalTxNoSpv)
                .await
                .unwrap();
            assert!(report.is_durable());

            // The eviction, complete: marker, record and coin moved.
            f.pots.record_eviction(&join_txid, 1_000, None);
            assert!(f.pots.take_record(&join_txid, 0).is_some());
            let coin = f
                .store
                .find_output(&join_txid, 0, Some(TOPIC), None, false)
                .await
                .unwrap()
                .expect("the held coin");
            f.store.delete_output(&join_txid, 0, TOPIC).await.unwrap();
            assert!(!coin_held(&f.store, &join_txid).await);
            // The readmission's restore: `outputs` copied back, the
            // `pot_records` copy faulted, the ledger left OPEN.
            f.store.insert_output(&coin).await.unwrap();
            assert!(coin_held(&f.store, &join_txid).await);

            let refund = refund_of(&join, &p);
            let (_, report) = f
                .engine
                .submit_with_report(&tagged(&refund), SubmitMode::HistoricalTxNoSpv)
                .await
                .unwrap();
            assert!(report.is_durable(), "{report:?}");
            assert!(
                f.pots
                    .get_spent_status(&join_txid, 0)
                    .await
                    .unwrap()
                    .is_none(),
                "nothing healed while the readmission is incomplete (lens 3 LOW-1)"
            );
        }

        /// The fold's edges: (1) a READMITTED ledger row (`readmittedAt`
        /// set) is held, so a cut pot under it is healed as before; (2) the
        /// ledger read faulting on the heal path is the hook's fault ("not
        /// now", the coin kept, the replay heals); (3) a RECORDED pot without
        /// its bytes under an open marker gets no bytes (the completion arm
        /// is a heal too); (4) the common spend (record and bytes held)
        /// never reads the ledger: its fault cannot reach it.
        #[tokio::test]
        async fn fix936f2_l1_the_ledger_guard_reads_only_on_the_heal_path() {
            // (1) and (2) over the real engine.
            let f = fixture();
            let p = real_params();
            let join = real_funding_tx(0x53, &p);
            let join_txid = join.id();
            cut_at_the_hook(
                f.engine
                    .submit_with_report(&tagged(&join), SubmitMode::HistoricalTxNoSpv),
                &f.reached,
            );
            f.pots.fault_evictions();
            let refund = refund_of(&join, &p);
            let first = f
                .engine
                .submit_with_report(&tagged(&refund), SubmitMode::HistoricalTxNoSpv)
                .await;
            assert!(
                !matches!(&first, Ok((_, r)) if r.is_durable()),
                "a faulted ledger read on the heal path is not now: {first:?}"
            );
            assert!(coin_held(&f.store, &join_txid).await);
            f.pots.record_eviction(&join_txid, 1_000, Some(2_000));
            let (_, report) = f
                .engine
                .submit_with_report(&tagged(&refund), SubmitMode::HistoricalTxNoSpv)
                .await
                .unwrap();
            assert!(report.is_durable(), "{report:?}");
            let rec = f
                .pots
                .get_spent_status(&join_txid, 0)
                .await
                .unwrap()
                .expect("a readmitted ledger row is held: the cut pot is healed");
            assert_eq!(rec.spending_txid.as_deref(), Some(refund.id().as_str()));

            // (3) The completion arm under an open marker.
            let (svc, pots) = make_service_with_storage();
            let known = real_funding_tx(0x54, &p);
            pots.fault_next_beef_writes(1);
            assert!(svc
                .output_admitted_by_topic(&admit(beef_of(&known), 0))
                .await
                .is_err());
            assert!(pots
                .get_spent_status(&known.id(), 0)
                .await
                .unwrap()
                .is_some());
            pots.record_eviction(&known.id(), 1_000, None);
            svc.output_spent(&spent(&known.id(), 0, beef_of(&refund_of(&known, &p))))
                .await
                .unwrap();
            assert!(
                pots.get_beef(&known.id()).await.unwrap().is_none(),
                "no bytes written under an open marker (lens 3 LOW-1)"
            );

            // (4) The common spend reads no ledger.
            let (svc, pots) = make_service_with_storage();
            let whole = real_funding_tx(0x55, &p);
            svc.output_admitted_by_topic(&admit(beef_of(&whole), 0))
                .await
                .unwrap();
            pots.fault_evictions();
            svc.output_spent(&spent(&whole.id(), 0, beef_of(&refund_of(&whole, &p))))
                .await
                .unwrap();
            assert!(
                pots.get_spent_status(&whole.id(), 0)
                    .await
                    .unwrap()
                    .unwrap()
                    .spent
            );
        }
    }
}
