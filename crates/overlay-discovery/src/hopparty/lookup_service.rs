//! HOPPARTY Lookup Service — indexes and queries LOW hop-in-flight markers
//! (bsv-low #315).
//!
//! Admission mode is WHOLE-TX (the `ls_pot` precedent, not `ls_potparty`'s
//! locking-script mode) for one reason: since the 2026-08-04 wire
//! revision the marker names its hop by VOUT ONLY and the CONTAINING
//! transaction supplies the txid — so the container is part of the
//! record's meaning, and the service must see it. Having it, the service
//! also decodes the container's output at `hopVout` ONCE at write
//! (`hopLockHex`, `hopSatsOnChain`, `containerOutputs` — #310
//! decode-at-write): a pure re-presentation of the very bytes being
//! admitted, exactly the #284 posture. **This is not validation** — the
//! record is stored whatever the container says, and NOTHING is refused
//! on it; the READER (`low-app-layer /hops-view`) compares the marker's
//! claims against those facts and labels the row.
//!
//! When outputs are admitted to `tm_hopparty`, this service parses the
//! `LOW/hopparty/v1` marker and stores one row per marker OUTPOINT
//! `(txid, outputIndex)` via [`HoppartyStorage`] — EVERY admitted marker is
//! kept (a duplicate submit of the same output is a no-op; outpoint keying
//! keeps a garbage front-run from censoring a genuine marker, the
//! `tm_result` lesson). A client (or the app-layer `/hops-view`) asks
//! `hopsFor` (its own identity → every hop it marked) or `byHop` (a hop
//! outpoint → the markers naming it); the answer is a freeform JSON array
//! carrying the marker's bytes back VERBATIM. The overlay never verifies
//! either signature — the record surface is bytes in, bytes out.
//!
//! Permanence: a hop-in-flight fact is permanent recovery history and the
//! admitted output is a provably-unspendable OP_RETURN.
//! `spend_notification_mode` is [`SpendNotificationMode::None`], and
//! `output_spent` / `output_evicted` are deliberate no-ops — a hopparty
//! record is NEVER removed (mirrors `ls_potparty`'s permanence).

use async_trait::async_trait;
use bsv_rs::transaction::Transaction;
use overlay_engine::lookup_service::{LookupService, LookupServiceError};
use overlay_engine::types::*;
use std::rc::Rc;
use tracing::debug;

use super::parse_hopparty_marker;
use super::storage::{HoppartyQuery, HoppartyRecord, HoppartyStorage};

/// Default number of records returned when a query omits `limit`.
const DEFAULT_LIMIT: usize = 100;
/// Hard cap on the number of records a single query can return.
const MAX_LIMIT: usize = 500;

/// HOPPARTY Lookup Service — indexes markers and answers `hopsFor` /
/// `byHop`.
pub struct HoppartyLookupService {
    storage: Rc<dyn HoppartyStorage>,
}

impl HoppartyLookupService {
    /// Create a new HOPPARTY lookup service backed by the given storage.
    pub fn new(storage: Rc<dyn HoppartyStorage>) -> Self {
        Self { storage }
    }
}

#[async_trait(?Send)]
impl LookupService for HoppartyLookupService {
    fn admission_mode(&self) -> AdmissionMode {
        // Whole-tx: the CONTAINER supplies the hop txid and the hop
        // output's script/value (module docs).
        AdmissionMode::WholeTx
    }

    fn spend_notification_mode(&self) -> SpendNotificationMode {
        // A hopparty marker is permanent history; the OP_RETURN can't be
        // spent anyway — never let a notification touch the index.
        SpendNotificationMode::None
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

        // Only index tm_hopparty outputs.
        if topic != "tm_hopparty" {
            return Ok(());
        }

        // Parse the CONTAINING tx out of the BEEF (the same subject-tx
        // selection the topic manager used). Unparseable → no-op, never a
        // spurious record (the `ls_pot` posture).
        let tx = match Transaction::from_beef(atomic_beef, None) {
            Ok(tx) => tx,
            Err(e) => {
                debug!("HOPPARTY: admitted beef did not parse — skipped: {e}");
                return Ok(());
            }
        };
        let Some(marker_output) = tx.outputs.get(output_index as usize) else {
            debug!("HOPPARTY: admitted output index out of range — skipped");
            return Ok(());
        };

        // The topic manager already validated the marker; re-parse to
        // recover the fields for the index (defensive — the TM should never
        // admit anything this can't parse).
        let Some(marker) = parse_hopparty_marker(&marker_output.locking_script.to_binary()) else {
            debug!("HOPPARTY: admitted output is not a parseable marker — skipped");
            return Ok(());
        };

        // The hop txid IS this tx (the marker rides the hop).
        let txid = tx.id();

        // #310 decode-at-write: the CONTAINER's own output at hopVout.
        // Absent (hopVout >= |outputs|) stores NULLs, and `containerOutputs`
        // makes that absence PROVEN rather than ambiguous. No refusal here —
        // the reader labels.
        let container_outputs = tx.outputs.len() as u32;
        let hop_output = tx.outputs.get(marker.hop_vout as usize);
        let hop_lock_hex = hop_output.map(|o| hex::encode(o.locking_script.to_binary()));
        let hop_sats_on_chain = hop_output.and_then(|o| o.satoshis);

        let record = HoppartyRecord {
            identity: hex::encode(&marker.identity),
            opponent_identity: hex::encode(&marker.opponent),
            game_id: hex::encode(marker.game_id),
            hop_vout: marker.hop_vout,
            hop_sats: marker.hop_sats,
            seat_settle_pubkey: hex::encode(&marker.seat_settle_pubkey),
            seat_sig_hex: hex::encode(&marker.seat_sig),
            identity_sig_hex: hex::encode(&marker.identity_sig),
            hop_lock_hex,
            hop_sats_on_chain,
            container_outputs,
            txid,
            output_index,
            created_at: 0, // assigned by the storage layer at insert
        };

        // Keyed by the OUTPOINT: insert-if-absent makes a replayed submit
        // of the same output a harmless no-op, while markers from different
        // txs are ALL kept.
        self.storage
            .store_record(&record)
            .await
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;

        Ok(())
    }

    async fn output_spent(&self, _payload: &OutputSpent) -> Result<(), LookupServiceError> {
        // No-op: a hopparty marker is PERMANENT history (see module docs).
        Ok(())
    }

    async fn output_evicted(
        &self,
        _txid: &str,
        _output_index: u32,
    ) -> Result<(), LookupServiceError> {
        // No-op: hopparty records are never evicted (permanence — above).
        Ok(())
    }

    async fn lookup(&self, question: &LookupQuestion) -> Result<LookupResult, LookupServiceError> {
        if question.service != "ls_hopparty" {
            return Err(LookupServiceError::Unsupported(format!(
                "Expected ls_hopparty, got {}",
                question.service
            )));
        }

        let query: HoppartyQuery = serde_json::from_value(question.query.clone())
            .map_err(|e| LookupServiceError::InvalidQuery(e.to_string()))?;

        let records = match query {
            HoppartyQuery::HopsFor { identity, limit } => {
                let identity = normalize_identity(&identity)?;
                self.storage
                    .list_for_identity(&identity, clamp_limit(limit))
                    .await
                    .map_err(|e| LookupServiceError::StorageError(e.to_string()))?
            }
            HoppartyQuery::ByHop {
                hop_txid,
                hop_vout,
                limit,
            } => {
                let hop_txid = normalize_txid(&hop_txid)?;
                self.storage
                    .list_for_hop(&hop_txid, hop_vout, clamp_limit(limit))
                    .await
                    .map_err(|e| LookupServiceError::StorageError(e.to_string()))?
            }
        };

        // Carry the stored bytes back VERBATIM — the overlay is an index,
        // not an authority (readers verify the signatures themselves).
        let entries: Vec<serde_json::Value> = records
            .into_iter()
            .map(|r| {
                serde_json::json!({
                    "identity": r.identity,
                    "opponentIdentity": r.opponent_identity,
                    "gameId": r.game_id,
                    // The container IS the hop tx, so `hopTxid` is a
                    // re-presentation of `txid` computed in exactly ONE
                    // place — never a second stored source that could drift.
                    "hopTxid": r.txid,
                    "hopVout": r.hop_vout,
                    "hopSats": r.hop_sats,
                    "seatSettlePubkey": r.seat_settle_pubkey,
                    "seatSigHex": r.seat_sig_hex,
                    "identitySigHex": r.identity_sig_hex,
                    // The CONTAINER's decoded facts (#310) — what a reader
                    // compares the claims above against.
                    "hopLockHex": r.hop_lock_hex,
                    "hopSatsOnChain": r.hop_sats_on_chain,
                    "containerOutputs": r.container_outputs,
                    "txid": r.txid,
                    "outputIndex": r.output_index,
                    "createdAt": r.created_at,
                })
            })
            .collect();

        Ok(LookupResult::Answer(LookupAnswer::Freeform {
            result: serde_json::Value::Array(entries),
        }))
    }

    async fn get_documentation(&self) -> String {
        include_str!("../../docs/hopparty_lookup.md").to_string()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "HOPPARTY Lookup Service".to_string(),
            description: Some(
                "Answers 'which funding hops did this identity mark?' \
                 (hopsFor) and 'which markers name this hop outpoint?' \
                 (byHop) over LOW hopparty markers — the hops-in-flight \
                 index (bsv-low #315)."
                    .to_string(),
            ),
            ..Default::default()
        }
    }
}

/// Clamp an optional query limit to `1..=MAX_LIMIT` (default
/// [`DEFAULT_LIMIT`] when absent) — the `ls_potparty` clamp verbatim.
fn clamp_limit(limit: Option<u32>) -> usize {
    (limit.map(|l| l as usize).unwrap_or(DEFAULT_LIMIT)).clamp(1, MAX_LIMIT)
}

/// Validate a 33-byte identity-key hex param and return it lowercased
/// (stored values are lowercase `hex::encode` output).
fn normalize_identity(value: &str) -> Result<String, LookupServiceError> {
    let lower = value.to_ascii_lowercase();
    if lower.len() != 66 || !lower.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(LookupServiceError::InvalidQuery(
            "identity must be 66 hex chars (a 33-byte compressed pubkey)".into(),
        ));
    }
    Ok(lower)
}

/// Validate a 32-byte txid hex param and return it lowercased.
fn normalize_txid(value: &str) -> Result<String, LookupServiceError> {
    let lower = value.to_ascii_lowercase();
    if lower.len() != 64 || !lower.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(LookupServiceError::InvalidQuery(
            "hopTxid must be 64 hex chars (a 32-byte txid)".into(),
        ));
    }
    Ok(lower)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::super::storage::MemoryHoppartyStorage;
    use super::super::tests::{
        build_golden_marker, golden_game_id, golden_identity, golden_marker, golden_opponent,
        golden_sats, golden_settle_pubkey, golden_sig, golden_vector_inputs, marker_script,
    };
    use super::*;
    use bsv_rs::script::LockingScript;
    use bsv_rs::transaction::{Transaction as Tx, TransactionInput, TransactionOutput};

    fn make_service_with_storage() -> (HoppartyLookupService, Rc<MemoryHoppartyStorage>) {
        let storage = Rc::new(MemoryHoppartyStorage::new());
        let svc = HoppartyLookupService::new(storage.clone());
        (svc, storage)
    }

    fn make_service() -> HoppartyLookupService {
        make_service_with_storage().0
    }

    /// The P2PKH lock a hop paying `pubkey_hex` carries.
    fn p2pkh_for(pubkey_hex: &str) -> String {
        let pk = hex::decode(pubkey_hex).unwrap();
        format!(
            "76a914{}88ac",
            hex::encode(bsv_rs::primitives::hash::hash160(&pk))
        )
    }

    /// Build the PRODUCTION container: a tx whose output 0 is the hop
    /// P2PKH and whose output 1 is the marker OP_RETURN — the exact shape
    /// the client's hop `createAction` emits (`randomizeOutputs: false`
    /// keeps the hop at a stable vout). Returns (atomic BEEF, txid).
    fn container(hop_lock_hex: &str, hop_sats: u64, marker_script: &[u8], nonce: u8) -> (Vec<u8>, String) {
        let mut tx = Tx::new();
        // The nonce varies the input so distinct containers get distinct
        // txids (a replay of the SAME bytes is a same-outpoint no-op).
        tx.add_input(TransactionInput::new(format!("{nonce:02x}").repeat(32), 0))
            .unwrap();
        tx.add_output(TransactionOutput {
            satoshis: Some(hop_sats),
            locking_script: LockingScript::from_hex(hop_lock_hex).unwrap(),
            change: false,
        })
        .unwrap();
        tx.add_output(TransactionOutput {
            satoshis: Some(0),
            locking_script: LockingScript::from_binary(marker_script).unwrap(),
            change: false,
        })
        .unwrap();
        let beef = tx.to_beef(true).expect("BEEF serialization");
        let txid = Tx::from_beef(&beef, None).expect("engine-side parse").id();
        (beef, txid)
    }

    fn admit_beef(beef: Vec<u8>, output_index: u32) -> OutputAdmittedByTopic {
        OutputAdmittedByTopic::WholeTx {
            atomic_beef: beef,
            output_index,
            topic: "tm_hopparty".into(),
            off_chain_values: None,
        }
    }

    /// Admit a marker inside a well-formed container paying `settle_pub`.
    async fn admit_marker(
        svc: &HoppartyLookupService,
        script: &[u8],
        settle_pub_hex: &str,
        hop_sats: u64,
        nonce: u8,
    ) -> String {
        let (beef, txid) = container(&p2pkh_for(settle_pub_hex), hop_sats, script, nonce);
        svc.output_admitted_by_topic(&admit_beef(beef, 1)).await.unwrap();
        txid
    }

    async fn run_lookup(svc: &HoppartyLookupService, query: serde_json::Value) -> serde_json::Value {
        let q = LookupQuestion::new("ls_hopparty", query);
        match svc.lookup(&q).await.unwrap() {
            LookupResult::Answer(LookupAnswer::Freeform { result }) => result,
            other => panic!("expected Freeform answer, got {other:?}"),
        }
    }

    async fn hops_for(
        svc: &HoppartyLookupService,
        identity: &str,
        limit: Option<u32>,
    ) -> serde_json::Value {
        let mut q = serde_json::json!({"type": "hopsFor", "identity": identity});
        if let Some(l) = limit {
            q["limit"] = serde_json::json!(l);
        }
        run_lookup(svc, q).await
    }

    async fn by_hop(svc: &HoppartyLookupService, hop_txid: &str, hop_vout: u32) -> serde_json::Value {
        let q = serde_json::json!({"type": "byHop", "hopTxid": hop_txid, "hopVout": hop_vout});
        run_lookup(svc, q).await
    }

    fn golden_identity_hex() -> String {
        hex::encode(golden_identity())
    }
    fn golden_opponent_hex() -> String {
        hex::encode(golden_opponent())
    }

    // ── Trait plumbing ───────────────────────────────────────────────────

    #[tokio::test]
    async fn modes_and_metadata() {
        let svc = make_service();
        assert_eq!(
            svc.admission_mode(),
            AdmissionMode::WholeTx,
            "the CONTAINER supplies the hop txid — the service must see it"
        );
        assert_eq!(svc.spend_notification_mode(), SpendNotificationMode::None);
        let meta = svc.get_metadata().await;
        assert_eq!(meta.name, "HOPPARTY Lookup Service");
        assert!(!svc.get_documentation().await.is_empty());
    }

    // ── Admission + hopsFor (end-to-end through the real LS path) ────────

    /// The container supplies the txid, and the container's OWN output at
    /// hopVout is decoded once at write (#310) — hopTxid on the wire IS the
    /// containing txid.
    #[tokio::test]
    async fn marker_admitted_container_supplies_txid_and_decoded_facts() {
        let (svc, storage) = make_service_with_storage();
        let settle = hex::encode(golden_settle_pubkey());
        let script = golden_marker(&golden_game_id(), 0);
        let txid = admit_marker(&svc, &script, &settle, golden_sats(), 0x01).await;
        assert_eq!(storage.record_count(), 1);

        let arr = hops_for(&svc, &golden_identity_hex(), None).await;
        let e = &arr[0];
        assert_eq!(e["identity"], golden_identity_hex());
        assert_eq!(e["opponentIdentity"], golden_opponent_hex());
        assert_eq!(e["gameId"], "11".repeat(32));
        assert_eq!(e["hopVout"], 0);
        assert_eq!(e["hopSats"], golden_sats());
        assert_eq!(e["seatSettlePubkey"], settle);
        assert_eq!(e["seatSigHex"], hex::encode(golden_sig()));
        assert_eq!(e["identitySigHex"], hex::encode(golden_sig()));
        // The container's identity, in both senses.
        assert_eq!(e["txid"], txid, "the marker's containing tx");
        assert_eq!(e["hopTxid"], txid, "…which IS the hop tx");
        assert_eq!(e["outputIndex"], 1);
        // The decoded container facts.
        assert_eq!(e["hopLockHex"], p2pkh_for(&settle));
        assert_eq!(e["hopSatsOnChain"], golden_sats());
        assert_eq!(e["containerOutputs"], 2);
        assert!(e["createdAt"].is_i64());

        // byHop is keyed on that same txid.
        let arr = by_hop(&svc, &txid, 0).await;
        assert_eq!(arr.as_array().unwrap().len(), 1);
        let arr = by_hop(&svc, &txid, 9).await;
        assert!(arr.as_array().unwrap().is_empty());
    }

    /// A marker naming a vout its OWN container does not have: stored (the
    /// overlay never refuses), with the absence PROVEN by
    /// `containerOutputs` rather than left ambiguous.
    #[tokio::test]
    async fn a_vout_the_container_lacks_is_stored_with_proven_absence() {
        let (svc, storage) = make_service_with_storage();
        let settle = hex::encode(golden_settle_pubkey());
        // The marker claims hopVout 7; the container has 2 outputs.
        let script = golden_marker(&golden_game_id(), 7);
        admit_marker(&svc, &script, &settle, golden_sats(), 0x02).await;
        assert_eq!(storage.record_count(), 1, "stored — admission never refuses");

        let e = &hops_for(&svc, &golden_identity_hex(), None).await[0];
        assert!(e["hopLockHex"].is_null());
        assert!(e["hopSatsOnChain"].is_null());
        assert_eq!(e["containerOutputs"], 2, "absence is PROVEN, not guessed");
    }

    #[tokio::test]
    async fn hops_for_filters_by_identity_only() {
        let (svc, _storage) = make_service_with_storage();
        let settle = hex::encode(golden_settle_pubkey());
        admit_marker(&svc, &golden_marker(&golden_game_id(), 0), &settle, golden_sats(), 0x01).await;
        // Seat B's OWN marker on its own hop (seats flipped).
        let b = marker_script(
            &golden_opponent(),
            &golden_identity(),
            &golden_game_id(),
            0,
            golden_sats(),
            &golden_settle_pubkey(),
            &golden_sig(),
            &golden_sig(),
        );
        admit_marker(&svc, &b, &settle, golden_sats(), 0x02).await;

        let arr = hops_for(&svc, &golden_identity_hex(), None).await;
        assert_eq!(arr.as_array().unwrap().len(), 1);
        let arr = hops_for(&svc, &golden_opponent_hex(), None).await;
        assert_eq!(arr.as_array().unwrap().len(), 1);
        let stranger = "02".to_string() + &"ee".repeat(32);
        assert!(hops_for(&svc, &stranger, None).await.as_array().unwrap().is_empty());
    }

    // ── Ordering + limit ──────────────────────────────────────────────────

    #[tokio::test]
    async fn newest_outpoint_first_and_limit() {
        let (svc, _storage) = make_service_with_storage();
        let settle = hex::encode(golden_settle_pubkey());
        let mut txids = Vec::new();
        for i in 1u8..=5 {
            txids.push(
                admit_marker(&svc, &golden_marker(&[i; 32], 0), &settle, golden_sats(), i).await,
            );
        }
        let arr = hops_for(&svc, &golden_identity_hex(), Some(3)).await;
        let arr = arr.as_array().unwrap();
        assert_eq!(arr.len(), 3, "limit respected");
        assert_eq!(arr[0]["txid"], txids[4], "newest hop first");

        assert_eq!(clamp_limit(None), DEFAULT_LIMIT);
        assert_eq!(clamp_limit(Some(0)), 1);
        assert_eq!(clamp_limit(Some(1_000_000)), MAX_LIMIT);
    }

    // ── Outpoint keying ──────────────────────────────────────────────────

    #[tokio::test]
    async fn same_outpoint_replay_is_a_noop() {
        let (svc, storage) = make_service_with_storage();
        let settle = hex::encode(golden_settle_pubkey());
        let script = golden_marker(&golden_game_id(), 0);
        let (beef, _) = container(&p2pkh_for(&settle), golden_sats(), &script, 0x01);
        svc.output_admitted_by_topic(&admit_beef(beef.clone(), 1)).await.unwrap();
        svc.output_admitted_by_topic(&admit_beef(beef, 1)).await.unwrap();
        assert_eq!(storage.record_count(), 1, "same-outpoint replay is a no-op");
    }

    // ── Admission filters ────────────────────────────────────────────────

    #[tokio::test]
    async fn ignores_non_tm_hopparty_topic() {
        let (svc, storage) = make_service_with_storage();
        let settle = hex::encode(golden_settle_pubkey());
        let (beef, _) = container(&p2pkh_for(&settle), golden_sats(), &golden_marker(&golden_game_id(), 0), 0x01);
        let mut payload = admit_beef(beef, 1);
        if let OutputAdmittedByTopic::WholeTx { ref mut topic, .. } = payload {
            *topic = "tm_potparty".into();
        }
        svc.output_admitted_by_topic(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 0);
    }

    /// The cross-leak refusal through the real LS admission path, both
    /// directions: a potparty script admitted under tm_hopparty stores
    /// NOTHING here, and a hopparty script admitted under tm_potparty
    /// stores nothing THERE (the ledgered #315 "can never leak into
    /// `potparty_records`" claim, executed rather than asserted).
    #[tokio::test]
    async fn cross_tag_scripts_never_stored_either_direction() {
        let (svc, storage) = make_service_with_storage();
        let settle = hex::encode(golden_settle_pubkey());
        for (i, script) in [
            crate::potparty::tests::golden_marker(&golden_game_id(), &[0x22u8; 32], 0),
            crate::potparty::tests::golden_marker_v2(&golden_game_id(), &[0x22u8; 32], 0),
        ]
        .into_iter()
        .enumerate()
        {
            let (beef, _) = container(&p2pkh_for(&settle), golden_sats(), &script, 0x10 + i as u8);
            svc.output_admitted_by_topic(&admit_beef(beef, 1)).await.unwrap();
        }
        assert_eq!(storage.record_count(), 0, "potparty scripts never stored");

        // The reverse: a hopparty script through the POTPARTY LS.
        let pp_storage = Rc::new(crate::potparty::storage::MemoryPotpartyStorage::new());
        let pp_svc = crate::potparty::lookup_service::PotpartyLookupService::new(pp_storage.clone());
        pp_svc
            .output_admitted_by_topic(&OutputAdmittedByTopic::LockingScript {
                txid: "txHOP".into(),
                output_index: 1,
                topic: "tm_potparty".into(),
                satoshis: 0,
                locking_script: golden_marker(&golden_game_id(), 0),
                off_chain_values: None,
            })
            .await
            .unwrap();
        assert_eq!(pp_storage.record_count(), 0, "hopparty never stored in potparty");

        // Positive control: the same hopparty script under ITS topic stores.
        admit_marker(&svc, &golden_marker(&golden_game_id(), 0), &settle, golden_sats(), 0x20).await;
        assert_eq!(storage.record_count(), 1);
    }

    #[tokio::test]
    async fn ignores_non_marker_script() {
        let (svc, storage) = make_service_with_storage();
        let p2pkh = "76a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac";
        let (beef, _) = container(p2pkh, 546, &hex::decode(p2pkh).unwrap(), 0x01);
        svc.output_admitted_by_topic(&admit_beef(beef, 1)).await.unwrap();
        assert_eq!(storage.record_count(), 0);
    }

    #[tokio::test]
    async fn rejects_locking_script_mode() {
        // The service opted into WHOLE-TX; the locking-script variant
        // carries no container and must be refused outright.
        let svc = make_service();
        let payload = OutputAdmittedByTopic::LockingScript {
            txid: "tx1".into(),
            output_index: 1,
            topic: "tm_hopparty".into(),
            satoshis: 0,
            locking_script: golden_marker(&golden_game_id(), 0),
            off_chain_values: None,
        };
        assert!(svc.output_admitted_by_topic(&payload).await.is_err());
    }

    #[tokio::test]
    async fn unparseable_beef_is_a_noop_never_a_spurious_record() {
        let (svc, storage) = make_service_with_storage();
        svc.output_admitted_by_topic(&admit_beef(vec![0xde, 0xad], 1))
            .await
            .unwrap();
        assert_eq!(storage.record_count(), 0);
        // An output index past the container is likewise a no-op.
        let settle = hex::encode(golden_settle_pubkey());
        let (beef, _) = container(&p2pkh_for(&settle), golden_sats(), &golden_marker(&golden_game_id(), 0), 0x01);
        svc.output_admitted_by_topic(&admit_beef(beef, 9)).await.unwrap();
        assert_eq!(storage.record_count(), 0);
    }

    // ── Permanence: spend / eviction are no-ops ──────────────────────────

    #[tokio::test]
    async fn spend_and_eviction_never_remove_a_record() {
        let (svc, storage) = make_service_with_storage();
        let settle = hex::encode(golden_settle_pubkey());
        let txid = admit_marker(&svc, &golden_marker(&golden_game_id(), 0), &settle, golden_sats(), 0x01).await;
        assert_eq!(storage.record_count(), 1);

        let spent = OutputSpent::None {
            txid: txid.clone(),
            output_index: 1,
            topic: "tm_hopparty".into(),
        };
        svc.output_spent(&spent).await.unwrap();
        assert_eq!(storage.record_count(), 1, "marker must survive a spend");
        svc.output_evicted(&txid, 1).await.unwrap();
        assert_eq!(storage.record_count(), 1, "marker must survive an eviction");
    }

    // ── Case-insensitive hex + query validation ──────────────────────────

    #[tokio::test]
    async fn lookup_case_insensitive_hex() {
        let (svc, _storage) = make_service_with_storage();
        let settle = hex::encode(golden_settle_pubkey());
        admit_marker(&svc, &golden_marker(&golden_game_id(), 0), &settle, golden_sats(), 0x01).await;
        let arr = hops_for(&svc, &golden_identity_hex().to_uppercase(), None).await;
        let arr = arr.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["identity"], golden_identity_hex());
    }

    #[tokio::test]
    async fn lookup_wrong_service_errors() {
        let svc = make_service();
        let q = LookupQuestion::new("ls_potparty", serde_json::json!({"type": "hopsFor"}));
        assert!(svc.lookup(&q).await.is_err());
    }

    #[tokio::test]
    async fn lookup_invalid_query_errors() {
        let svc = make_service();
        for bad in [
            serde_json::json!({"type": "unknownQuery"}),
            serde_json::json!("hopsFor"),
            serde_json::json!(42),
            serde_json::json!({"type": "hopsFor"}), // missing identity
            serde_json::json!({"type": "hopsFor", "identity": "zz".repeat(33)}),
            serde_json::json!({"type": "hopsFor", "identity": "02a1"}),
            serde_json::json!({"type": "byHop", "hopVout": 0}), // missing hopTxid
            serde_json::json!({"type": "byHop", "hopTxid": "zz", "hopVout": 0}),
            // The potparty query shapes are NOT this service's.
            serde_json::json!({"type": "partyFor", "identity": "02".to_string() + &"a1".repeat(32)}),
            serde_json::json!({"type": "byPot", "potTxid": "22".repeat(32), "potVout": 0}),
        ] {
            let q = LookupQuestion::new("ls_hopparty", bad.clone());
            assert!(svc.lookup(&q).await.is_err(), "expected error for {bad}");
        }
    }

    /// bsv-low #315 gate HIGH-2 (lookup half) — the SERVED answer example
    /// must publish exactly the keys the real serializer emits.
    ///
    /// `get_documentation()` `include_str!`s `docs/hopparty_lookup.md` and
    /// the overlay serves it publicly. The gate found the example omitting
    /// `hopLockHex`/`hopSatsOnChain`/`containerOutputs`, which the real
    /// answer carries — a client written to the doc would not know the
    /// fields that decide `markerVerified` exist. This drives the REAL
    /// `lookup()` and compares its key set to the documented one, so the
    /// two cannot drift (Rule 16: a property spanning doc and code cannot
    /// be pinned inside either).
    #[tokio::test]
    async fn served_documentation_answer_keys_match_the_serializer() {
        let (svc, _storage) = make_service_with_storage();
        let settle = hex::encode(golden_settle_pubkey());
        admit_marker(&svc, &golden_marker(&golden_game_id(), 0), &settle, golden_sats(), 0x01)
            .await;
        let answer = hops_for(&svc, &hex::encode(golden_identity()), None).await;
        let mut real_keys: Vec<String> = answer[0]
            .as_object()
            .expect("the serializer emits objects")
            .keys()
            .cloned()
            .collect();
        real_keys.sort();

        // Extract the quoted keys from the doc's ```json answer example.
        // The example carries `<hex|null>` placeholders so it is not valid
        // JSON; scan for `"key":` instead (a real scan of the example
        // block, not of the whole document).
        let doc = svc.get_documentation().await;
        let example = doc
            .split("## Answer")
            .nth(1)
            .and_then(|tail| tail.split("```json").nth(1))
            .and_then(|tail| tail.split("```").next())
            .expect("the served doc must carry a ```json answer example");
        let mut doc_keys: Vec<String> = Vec::new();
        let bytes: Vec<char> = example.chars().collect();
        let mut i = 0usize;
        while i < bytes.len() {
            if bytes[i] == '"' {
                if let Some(end) = bytes[i + 1..].iter().position(|c| *c == '"') {
                    let key: String = bytes[i + 1..i + 1 + end].iter().collect();
                    // A KEY is a quoted run immediately followed by ':'.
                    let after = bytes.get(i + 1 + end + 1).copied().unwrap_or(' ');
                    if after == ':' && !doc_keys.contains(&key) {
                        doc_keys.push(key);
                    }
                    i = i + 1 + end + 1;
                    continue;
                }
            }
            i += 1;
        }
        doc_keys.sort();

        // POSITIVE count first: "matched nothing" must fail loudly.
        assert_eq!(
            doc_keys.len(),
            real_keys.len(),
            "the served answer example publishes {} keys, the serializer emits {}:\n\
             documented: {doc_keys:?}\n serialized: {real_keys:?}",
            doc_keys.len(),
            real_keys.len()
        );
        assert_eq!(
            doc_keys, real_keys,
            "the served answer example drifted from the real serializer"
        );
    }

    /// bsv-low #315 Rule-16 CROSS-REPO BOUNDARY PIN (overlay half).
    ///
    /// `fixtures/ls_hopparty_hopsfor.fixture.json` is the EXACT `result`
    /// array the real `lookup()` serializer emits for `hopsFor` over two
    /// markers admitted through the REAL `output_admitted_by_topic`
    /// producer path (whole-tx, so the containers really do supply the
    /// txids and the decoded facts). Row 1 carries the GOLDEN cross-repo
    /// marker (real BRC-42 + RFC6979 crypto from PrivateKey(1)/(2)). The
    /// bsv-low CLIENT will pin a byte-identical copy and drive it through
    /// ITS parser: rename a serialized field HERE and this cell goes red;
    /// drift the client's parser and its cell goes red. A property
    /// spanning two repos cannot be pinned inside either one — regenerate
    /// BOTH copies together or not at all.
    #[tokio::test]
    async fn hopsfor_answer_matches_cross_repo_fixture() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/ls_hopparty_hopsfor.fixture.json"))
                .expect("fixture parses");

        let (svc, storage) = make_service_with_storage();
        let (wallet, opponent, game_id, hop_vout, hop_sats) = golden_vector_inputs();
        // Row 1: the GOLDEN marker in the PRODUCTION container — the hop
        // P2PKH at output 0 paying its REAL derived settle key exactly the
        // hopSats it claims, the marker at output 1. This row is the
        // canonical FULLY-VERIFYING example the client half pins against.
        let golden = build_golden_marker();
        let golden_settle = hex::encode(
            &parse_hopparty_marker(&golden).expect("golden parses").seat_settle_pubkey,
        );
        let (beef, _) = container(&p2pkh_for(&golden_settle), hop_sats, &golden, 0xc1);
        svc.output_admitted_by_topic(&admit_beef(beef, 1)).await.unwrap();
        // Row 2: a second marker for the SAME identity on another hop tx —
        // deterministic dummy-DER sigs (admission is byte-shape only; the
        // sig VALUES are exercised by the golden row).
        let identity = hex::decode(wallet.identity_key_hex()).unwrap();
        let dummy_sig = {
            let mut s = vec![0x30u8, 0x45];
            s.extend_from_slice(&[0x77u8; 69]);
            s
        };
        let second = marker_script(
            &identity,
            &opponent,
            &game_id,
            0,
            42_000,
            &golden_settle_pubkey(),
            &dummy_sig,
            &dummy_sig,
        );
        let (beef, _) = container(
            &p2pkh_for(&hex::encode(golden_settle_pubkey())),
            42_000,
            &second,
            0xc2,
        );
        svc.output_admitted_by_topic(&admit_beef(beef, 1)).await.unwrap();
        assert_eq!(storage.record_count(), 2);
        assert_eq!(hop_vout, 0, "the golden vector rides the production layout");

        let answer = hops_for(&svc, &wallet.identity_key_hex(), None).await;
        assert_eq!(
            answer, fixture,
            "the serialized hopsFor answer must match the cross-repo fixture BYTE-FOR-BYTE \
             (field names + values are the wire contract with the bsv-low client)"
        );
        // Loud-count guard: the pin must be comparing two real rows, not two
        // empty arrays agreeing with each other.
        assert_eq!(answer.as_array().map(|a| a.len()), Some(2));
    }
}
