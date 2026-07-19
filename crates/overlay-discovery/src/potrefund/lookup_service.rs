//! POTREFUND Lookup Service — indexes and queries LOW pre-signed
//! refund-backup markers for keyless recovery re-broadcast (bsv-low #191).
//!
//! When outputs are admitted to `tm_potrefund`, this service parses the
//! `LOW/potrefund/v1` marker and stores one row per marker OUTPOINT
//! `(txid, outputIndex)` via [`PotrefundStorage`] — EVERY admitted marker is
//! kept (a duplicate submit of the same output is a no-op; outpoint keying
//! keeps a garbage front-run from censoring a genuine refund, the
//! `tm_result` lesson). A recovering client asks `byPot` (a pot outpoint →
//! the pre-signed refund backup(s), both seats) or `partyFor` (its own
//! identity → every pot it has backed up); the answer is a freeform,
//! newest-first JSON array carrying the marker's bytes back VERBATIM. The
//! overlay never parses or verifies the refund tx / `sig` — the record
//! surface is bytes in, bytes out.
//!
//! Permanence: a pre-signed refund backup is permanent recovery history and
//! the admitted output is a provably-unspendable OP_RETURN.
//! `spend_notification_mode` is [`SpendNotificationMode::None`], and
//! `output_spent` / `output_evicted` are deliberate no-ops — a potrefund
//! record is NEVER removed (mirrors `ls_pot`'s permanence).

use async_trait::async_trait;
use overlay_engine::lookup_service::{LookupService, LookupServiceError};
use overlay_engine::types::*;
use std::rc::Rc;
use tracing::debug;

use super::parse_potrefund_marker;
use super::storage::{PotrefundQuery, PotrefundRecord, PotrefundStorage};

/// Default number of records returned when a query omits `limit`.
const DEFAULT_LIMIT: usize = 100;
/// Hard cap on the number of records a single query can return.
const MAX_LIMIT: usize = 500;

/// POTREFUND Lookup Service — indexes markers and answers `byPot` /
/// `partyFor`.
pub struct PotrefundLookupService {
    storage: Rc<dyn PotrefundStorage>,
}

impl PotrefundLookupService {
    /// Create a new POTREFUND lookup service backed by the given storage.
    pub fn new(storage: Rc<dyn PotrefundStorage>) -> Self {
        Self { storage }
    }
}

#[async_trait(?Send)]
impl LookupService for PotrefundLookupService {
    fn admission_mode(&self) -> AdmissionMode {
        AdmissionMode::LockingScript
    }

    fn spend_notification_mode(&self) -> SpendNotificationMode {
        // A potrefund marker is permanent recovery history; we never want a
        // spend notification (the OP_RETURN can't be spent anyway) to touch
        // the index.
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

        // Only index tm_potrefund outputs.
        if topic != "tm_potrefund" {
            return Ok(());
        }

        // The topic manager already validated the marker; re-parse to
        // recover the fields for the index (defensive — the TM should never
        // admit anything this can't parse).
        let Some(marker) = parse_potrefund_marker(locking_script) else {
            debug!("POTREFUND: admitted output is not a parseable marker — skipped");
            return Ok(());
        };

        let record = PotrefundRecord {
            identity: hex::encode(&marker.identity),
            game_id: hex::encode(marker.game_id),
            pot_txid: hex::encode(marker.pot_txid),
            pot_vout: marker.pot_vout,
            refund_raw_hex: hex::encode(&marker.refund_raw),
            sig_hex: hex::encode(&marker.sig),
            txid: txid.to_string(),
            output_index,
            created_at: 0, // assigned by the storage layer at insert
        };

        // Keyed by the OUTPOINT: the storage layer's insert-if-absent makes
        // a replayed submit of the same output a harmless no-op, while
        // markers from different txs are ALL kept.
        self.storage
            .store_record(&record)
            .await
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;

        Ok(())
    }

    async fn output_spent(&self, _payload: &OutputSpent) -> Result<(), LookupServiceError> {
        // No-op: a potrefund marker is PERMANENT recovery history. The
        // admitted output is an unspendable OP_RETURN, so this never fires
        // anyway — but even if it did, we must not evict the record.
        Ok(())
    }

    async fn output_evicted(
        &self,
        _txid: &str,
        _output_index: u32,
    ) -> Result<(), LookupServiceError> {
        // No-op: potrefund records are never evicted (permanence — above).
        Ok(())
    }

    async fn lookup(&self, question: &LookupQuestion) -> Result<LookupResult, LookupServiceError> {
        if question.service != "ls_potrefund" {
            return Err(LookupServiceError::Unsupported(format!(
                "Expected ls_potrefund, got {}",
                question.service
            )));
        }

        let query: PotrefundQuery = serde_json::from_value(question.query.clone())
            .map_err(|e| LookupServiceError::InvalidQuery(e.to_string()))?;

        let records = match query {
            PotrefundQuery::ByPot {
                pot_txid,
                pot_vout,
                limit,
            } => {
                let pot_txid = normalize_txid(&pot_txid)?;
                self.storage
                    .list_for_pot(&pot_txid, pot_vout, clamp_limit(limit))
                    .await
                    .map_err(|e| LookupServiceError::StorageError(e.to_string()))?
            }
            PotrefundQuery::PartyFor { identity, limit } => {
                let identity = normalize_identity(&identity)?;
                self.storage
                    .list_for_identity(&identity, clamp_limit(limit))
                    .await
                    .map_err(|e| LookupServiceError::StorageError(e.to_string()))?
            }
        };

        // Carry the stored bytes back VERBATIM — the overlay is an index,
        // not an authority (a client parses + verifies the refund itself).
        let entries: Vec<serde_json::Value> = records
            .into_iter()
            .map(|r| {
                serde_json::json!({
                    "identity": r.identity,
                    "gameId": r.game_id,
                    "potTxid": r.pot_txid,
                    "potVout": r.pot_vout,
                    "refundRawHex": r.refund_raw_hex,
                    "sigHex": r.sig_hex,
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
        include_str!("../../docs/potrefund_lookup.md").to_string()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "POTREFUND Lookup Service".to_string(),
            description: Some(
                "Answers 'give me the pre-signed refund backup(s) for this \
                 pot?' (byPot) and 'which pots have I backed up?' (partyFor) \
                 over LOW potrefund markers — the keyless recovery index."
                    .to_string(),
            ),
            ..Default::default()
        }
    }
}

/// Clamp an optional query limit to `1..=MAX_LIMIT` (default
/// [`DEFAULT_LIMIT`] when absent). A `limit: 0` still returns one page of
/// nothing-useful rather than erroring — clamped up to 1.
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
            "potTxid must be 64 hex chars (a 32-byte txid)".into(),
        ));
    }
    Ok(lower)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::super::storage::MemoryPotrefundStorage;
    use super::super::tests::{
        golden_game_id, golden_identity, golden_marker, golden_pot_txid, golden_refund, golden_sig,
        golden_vout, marker_script,
    };
    use super::*;

    fn make_service_with_storage() -> (PotrefundLookupService, Rc<MemoryPotrefundStorage>) {
        let storage = Rc::new(MemoryPotrefundStorage::new());
        let svc = PotrefundLookupService::new(storage.clone());
        (svc, storage)
    }

    fn make_service() -> PotrefundLookupService {
        make_service_with_storage().0
    }

    fn admit(txid: &str, output_index: u32, script: Vec<u8>) -> OutputAdmittedByTopic {
        OutputAdmittedByTopic::LockingScript {
            txid: txid.into(),
            output_index,
            topic: "tm_potrefund".into(),
            satoshis: 0,
            locking_script: script,
            off_chain_values: None,
        }
    }

    async fn run_lookup(
        svc: &PotrefundLookupService,
        query: serde_json::Value,
    ) -> serde_json::Value {
        let q = LookupQuestion::new("ls_potrefund", query);
        match svc.lookup(&q).await.unwrap() {
            LookupResult::Answer(LookupAnswer::Freeform { result }) => result,
            other => panic!("expected Freeform answer, got {other:?}"),
        }
    }

    async fn by_pot(
        svc: &PotrefundLookupService,
        pot_txid: &str,
        pot_vout: u32,
    ) -> serde_json::Value {
        let q = serde_json::json!({"type": "byPot", "potTxid": pot_txid, "potVout": pot_vout});
        run_lookup(svc, q).await
    }

    async fn party_for(
        svc: &PotrefundLookupService,
        identity: &str,
        limit: Option<u32>,
    ) -> serde_json::Value {
        let mut q = serde_json::json!({"type": "partyFor", "identity": identity});
        if let Some(l) = limit {
            q["limit"] = serde_json::json!(l);
        }
        run_lookup(svc, q).await
    }

    fn golden_identity_hex() -> String {
        hex::encode(golden_identity())
    }
    fn opponent_identity() -> Vec<u8> {
        let mut k = vec![0x03u8];
        k.extend_from_slice(&[0xb2u8; 32]);
        k
    }
    fn opponent_identity_hex() -> String {
        hex::encode(opponent_identity())
    }

    // ── Trait plumbing ───────────────────────────────────────────────────

    #[tokio::test]
    async fn modes_and_metadata() {
        let svc = make_service();
        assert_eq!(svc.admission_mode(), AdmissionMode::LockingScript);
        assert_eq!(svc.spend_notification_mode(), SpendNotificationMode::None);
        let meta = svc.get_metadata().await;
        assert_eq!(meta.name, "POTREFUND Lookup Service");
        assert!(!svc.get_documentation().await.is_empty());
    }

    // ── Admission + byPot (end-to-end) ───────────────────────────────────

    #[tokio::test]
    async fn marker_admitted_and_found_by_pot() {
        let (svc, storage) = make_service_with_storage();
        let script = golden_marker(&golden_game_id(), &golden_pot_txid(), 3);
        svc.output_admitted_by_topic(&admit("markerTx1", 0, script))
            .await
            .unwrap();
        assert_eq!(storage.record_count(), 1);

        let arr = by_pot(&svc, &hex::encode(golden_pot_txid()), 3).await;
        let e = &arr[0];
        assert_eq!(e["identity"], golden_identity_hex());
        assert_eq!(e["gameId"], "11".repeat(32));
        assert_eq!(e["potTxid"], "22".repeat(32));
        assert_eq!(e["potVout"], 3);
        assert_eq!(e["refundRawHex"], hex::encode(golden_refund()));
        assert_eq!(e["sigHex"], hex::encode(golden_sig()));
        assert_eq!(e["txid"], "markerTx1");
        assert!(e["createdAt"].is_i64());
    }

    // ── byPot returns both parties' backups ───────────────────────────────

    #[tokio::test]
    async fn by_pot_returns_both_parties() {
        let (svc, _storage) = make_service_with_storage();
        svc.output_admitted_by_topic(&admit(
            "txA",
            0,
            golden_marker(&golden_game_id(), &golden_pot_txid(), 0),
        ))
        .await
        .unwrap();
        // The opponent's OWN refund backup for the same pot.
        svc.output_admitted_by_topic(&admit(
            "txB",
            0,
            marker_script(
                &opponent_identity(),
                &golden_game_id(),
                &golden_pot_txid(),
                0,
                &golden_refund(),
                &golden_sig(),
            ),
        ))
        .await
        .unwrap();

        let arr = by_pot(&svc, &hex::encode(golden_pot_txid()), 0).await;
        let arr = arr.as_array().unwrap();
        assert_eq!(arr.len(), 2, "both parties' backups for the pot");
        // Newest first.
        assert_eq!(arr[0]["txid"], "txB");
        assert_eq!(arr[1]["txid"], "txA");

        // A different vout matches nobody.
        let arr = by_pot(&svc, &hex::encode(golden_pot_txid()), 9).await;
        assert!(arr.as_array().unwrap().is_empty());
    }

    // ── partyFor filters by identity only ─────────────────────────────────

    #[tokio::test]
    async fn party_for_filters_by_identity_only() {
        let (svc, _storage) = make_service_with_storage();
        svc.output_admitted_by_topic(&admit(
            "txA",
            0,
            golden_marker(&golden_game_id(), &golden_pot_txid(), 0),
        ))
        .await
        .unwrap();
        svc.output_admitted_by_topic(&admit(
            "txB",
            0,
            marker_script(
                &opponent_identity(),
                &golden_game_id(),
                &golden_pot_txid(),
                0,
                &golden_refund(),
                &golden_sig(),
            ),
        ))
        .await
        .unwrap();

        let arr = party_for(&svc, &golden_identity_hex(), None).await;
        let arr = arr.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["txid"], "txA");

        let arr = party_for(&svc, &opponent_identity_hex(), None).await;
        assert_eq!(arr.as_array().unwrap().len(), 1);

        let stranger = "02".to_string() + &"ee".repeat(32);
        let arr = party_for(&svc, &stranger, None).await;
        assert!(arr.as_array().unwrap().is_empty());
    }

    // ── Ordering + limit ──────────────────────────────────────────────────

    #[tokio::test]
    async fn newest_first_and_limit() {
        let (svc, _storage) = make_service_with_storage();
        for i in 1u8..=5 {
            svc.output_admitted_by_topic(&admit(
                &format!("tx{i}"),
                0,
                golden_marker(&golden_game_id(), &golden_pot_txid(), 0),
            ))
            .await
            .unwrap();
        }
        let arr = by_pot(&svc, &hex::encode(golden_pot_txid()), 0).await;
        // limit default returns all 5, newest first.
        let arr = arr.as_array().unwrap();
        assert_eq!(arr.len(), 5);
        assert_eq!(arr[0]["txid"], "tx5", "newest first");

        // limit clamps.
        assert_eq!(clamp_limit(None), DEFAULT_LIMIT);
        assert_eq!(clamp_limit(Some(0)), 1);
        assert_eq!(clamp_limit(Some(1_000_000)), MAX_LIMIT);
    }

    // ── Outpoint keying ──────────────────────────────────────────────────

    #[tokio::test]
    async fn same_outpoint_replay_is_a_noop() {
        let (svc, storage) = make_service_with_storage();
        let script = golden_marker(&golden_game_id(), &golden_pot_txid(), 0);
        svc.output_admitted_by_topic(&admit("txSAME", 0, script.clone()))
            .await
            .unwrap();
        svc.output_admitted_by_topic(&admit("txSAME", 0, script))
            .await
            .unwrap();
        assert_eq!(storage.record_count(), 1, "same-outpoint replay is a no-op");
    }

    // ── Admission filters ────────────────────────────────────────────────

    #[tokio::test]
    async fn ignores_non_tm_potrefund_topic() {
        let (svc, storage) = make_service_with_storage();
        let mut payload = admit(
            "tx1",
            0,
            golden_marker(&golden_game_id(), &golden_pot_txid(), 0),
        );
        if let OutputAdmittedByTopic::LockingScript { ref mut topic, .. } = payload {
            *topic = "tm_result".into();
        }
        svc.output_admitted_by_topic(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 0);
    }

    #[tokio::test]
    async fn ignores_non_marker_script() {
        let (svc, storage) = make_service_with_storage();
        let p2pkh = hex::decode("76a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac").unwrap();
        svc.output_admitted_by_topic(&admit("tx1", 0, p2pkh))
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
            topic: "tm_potrefund".into(),
            off_chain_values: None,
        };
        assert!(svc.output_admitted_by_topic(&payload).await.is_err());
    }

    // ── Permanence: spend / eviction are no-ops ──────────────────────────

    #[tokio::test]
    async fn spend_and_eviction_never_remove_a_record() {
        let (svc, storage) = make_service_with_storage();
        svc.output_admitted_by_topic(&admit(
            "tx1",
            0,
            golden_marker(&golden_game_id(), &golden_pot_txid(), 0),
        ))
        .await
        .unwrap();
        assert_eq!(storage.record_count(), 1);

        let spent = OutputSpent::None {
            txid: "tx1".into(),
            output_index: 0,
            topic: "tm_potrefund".into(),
        };
        svc.output_spent(&spent).await.unwrap();
        assert_eq!(storage.record_count(), 1, "marker must survive a spend");
        svc.output_evicted("tx1", 0).await.unwrap();
        assert_eq!(storage.record_count(), 1, "marker must survive an eviction");
    }

    // ── Case-insensitive hex ──────────────────────────────────────────────

    #[tokio::test]
    async fn lookup_case_insensitive_hex() {
        let (svc, _storage) = make_service_with_storage();
        svc.output_admitted_by_topic(&admit(
            "txA",
            0,
            golden_marker(&golden_game_id(), &golden_pot_txid(), 0),
        ))
        .await
        .unwrap();
        let arr = by_pot(&svc, &hex::encode(golden_pot_txid()).to_uppercase(), 0).await;
        let arr = arr.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["potTxid"], hex::encode(golden_pot_txid()));
    }

    // ── Query validation ─────────────────────────────────────────────────

    #[tokio::test]
    async fn lookup_wrong_service_errors() {
        let svc = make_service();
        let q = LookupQuestion::new("ls_result", serde_json::json!({"type": "byPot"}));
        assert!(svc.lookup(&q).await.is_err());
    }

    #[tokio::test]
    async fn lookup_invalid_query_errors() {
        let svc = make_service();
        let _ = golden_vout(); // keep the shared import warning-free
        for bad in [
            serde_json::json!({"type": "unknownQuery"}),
            serde_json::json!("byPot"),
            serde_json::json!(42),
            serde_json::json!({"type": "byPot", "potVout": 0}), // missing potTxid
            serde_json::json!({"type": "byPot", "potTxid": "zz", "potVout": 0}),
            serde_json::json!({"type": "partyFor"}), // missing identity
            serde_json::json!({"type": "partyFor", "identity": "zz".repeat(33)}),
            serde_json::json!({"type": "partyFor", "identity": "02a1"}),
        ] {
            let q = LookupQuestion::new("ls_potrefund", bad.clone());
            assert!(svc.lookup(&q).await.is_err(), "expected error for {bad}");
        }
    }
}
