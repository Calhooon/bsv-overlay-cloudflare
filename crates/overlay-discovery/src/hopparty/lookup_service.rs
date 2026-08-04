//! HOPPARTY Lookup Service — indexes and queries LOW hop-in-flight markers
//! (bsv-low #315).
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
        AdmissionMode::LockingScript
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

        // Only index tm_hopparty outputs.
        if topic != "tm_hopparty" {
            return Ok(());
        }

        // The topic manager already validated the marker; re-parse to
        // recover the fields for the index (defensive — the TM should never
        // admit anything this can't parse).
        let Some(marker) = parse_hopparty_marker(locking_script) else {
            debug!("HOPPARTY: admitted output is not a parseable marker — skipped");
            return Ok(());
        };

        let record = HoppartyRecord {
            identity: hex::encode(&marker.identity),
            opponent_identity: hex::encode(&marker.opponent),
            game_id: hex::encode(marker.game_id),
            hop_txid: hex::encode(marker.hop_txid),
            hop_vout: marker.hop_vout,
            hop_sats: marker.hop_sats,
            seat_settle_pubkey: hex::encode(&marker.seat_settle_pubkey),
            seat_sig_hex: hex::encode(&marker.seat_sig),
            identity_sig_hex: hex::encode(&marker.identity_sig),
            txid: txid.to_string(),
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
                    "hopTxid": r.hop_txid,
                    "hopVout": r.hop_vout,
                    "hopSats": r.hop_sats,
                    "seatSettlePubkey": r.seat_settle_pubkey,
                    "seatSigHex": r.seat_sig_hex,
                    "identitySigHex": r.identity_sig_hex,
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
        build_golden_marker, golden_game_id, golden_hop_txid, golden_identity, golden_marker,
        golden_opponent, golden_sats, golden_settle_pubkey, golden_sig, golden_vector_inputs,
        marker_script,
    };
    use super::*;

    fn make_service_with_storage() -> (HoppartyLookupService, Rc<MemoryHoppartyStorage>) {
        let storage = Rc::new(MemoryHoppartyStorage::new());
        let svc = HoppartyLookupService::new(storage.clone());
        (svc, storage)
    }

    fn make_service() -> HoppartyLookupService {
        make_service_with_storage().0
    }

    fn admit(txid: &str, output_index: u32, script: Vec<u8>) -> OutputAdmittedByTopic {
        OutputAdmittedByTopic::LockingScript {
            txid: txid.into(),
            output_index,
            topic: "tm_hopparty".into(),
            satoshis: 0,
            locking_script: script,
            off_chain_values: None,
        }
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
        assert_eq!(svc.admission_mode(), AdmissionMode::LockingScript);
        assert_eq!(svc.spend_notification_mode(), SpendNotificationMode::None);
        let meta = svc.get_metadata().await;
        assert_eq!(meta.name, "HOPPARTY Lookup Service");
        assert!(!svc.get_documentation().await.is_empty());
    }

    // ── Admission + hopsFor (end-to-end through the real LS path) ────────

    #[tokio::test]
    async fn marker_admitted_and_found_by_identity() {
        let (svc, storage) = make_service_with_storage();
        let script = golden_marker(&golden_game_id(), &golden_hop_txid(), 3);
        svc.output_admitted_by_topic(&admit("markerTx1", 0, script))
            .await
            .unwrap();
        assert_eq!(storage.record_count(), 1);

        let arr = hops_for(&svc, &golden_identity_hex(), None).await;
        let e = &arr[0];
        assert_eq!(e["identity"], golden_identity_hex());
        assert_eq!(e["opponentIdentity"], golden_opponent_hex());
        assert_eq!(e["gameId"], "11".repeat(32));
        assert_eq!(e["hopTxid"], "22".repeat(32));
        assert_eq!(e["hopVout"], 3);
        assert_eq!(e["hopSats"], golden_sats());
        assert_eq!(e["seatSettlePubkey"], hex::encode(golden_settle_pubkey()));
        assert_eq!(e["seatSigHex"], hex::encode(golden_sig()));
        assert_eq!(e["identitySigHex"], hex::encode(golden_sig()));
        assert_eq!(e["txid"], "markerTx1");
        assert!(e["createdAt"].is_i64());

        // byHop surfaces the same row.
        let arr = by_hop(&svc, &"22".repeat(32), 3).await;
        assert_eq!(arr.as_array().unwrap().len(), 1);
        // A different vout matches nobody.
        let arr = by_hop(&svc, &"22".repeat(32), 9).await;
        assert!(arr.as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn hops_for_filters_by_identity_only() {
        let (svc, _storage) = make_service_with_storage();
        // Seat A's marker.
        svc.output_admitted_by_topic(&admit(
            "txA",
            0,
            golden_marker(&golden_game_id(), &golden_hop_txid(), 0),
        ))
        .await
        .unwrap();
        // Seat B's OWN marker for its own hop (seats flipped).
        svc.output_admitted_by_topic(&admit(
            "txB",
            0,
            marker_script(
                &golden_opponent(),
                &golden_identity(),
                &golden_game_id(),
                &[0x33u8; 32],
                0,
                golden_sats(),
                &golden_settle_pubkey(),
                &golden_sig(),
                &golden_sig(),
            ),
        ))
        .await
        .unwrap();

        let arr = hops_for(&svc, &golden_identity_hex(), None).await;
        let arr = arr.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["txid"], "txA");

        let arr = hops_for(&svc, &golden_opponent_hex(), None).await;
        assert_eq!(arr.as_array().unwrap().len(), 1);

        // An identity in no hop sees an empty array.
        let stranger = "02".to_string() + &"ee".repeat(32);
        let arr = hops_for(&svc, &stranger, None).await;
        assert!(arr.as_array().unwrap().is_empty());
    }

    // ── Ordering + limit ──────────────────────────────────────────────────

    #[tokio::test]
    async fn newest_outpoint_first_and_limit() {
        let (svc, _storage) = make_service_with_storage();
        // FIVE DISTINCT HOPS — the window counts outpoints.
        for i in 1u8..=5 {
            svc.output_admitted_by_topic(&admit(
                &format!("tx{i}"),
                0,
                golden_marker(&[i; 32], &[0xd0 + i; 32], 0),
            ))
            .await
            .unwrap();
        }
        let arr = hops_for(&svc, &golden_identity_hex(), Some(3)).await;
        let arr = arr.as_array().unwrap();
        assert_eq!(arr.len(), 3, "limit respected");
        assert_eq!(arr[0]["txid"], "tx5", "newest hop first");

        // limit clamps.
        assert_eq!(clamp_limit(None), DEFAULT_LIMIT);
        assert_eq!(clamp_limit(Some(0)), 1);
        assert_eq!(clamp_limit(Some(1_000_000)), MAX_LIMIT);
    }

    // ── Outpoint keying ──────────────────────────────────────────────────

    #[tokio::test]
    async fn same_outpoint_replay_is_a_noop() {
        let (svc, storage) = make_service_with_storage();
        let script = golden_marker(&golden_game_id(), &golden_hop_txid(), 0);
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
    async fn ignores_non_tm_hopparty_topic() {
        let (svc, storage) = make_service_with_storage();
        let mut payload = admit(
            "tx1",
            0,
            golden_marker(&golden_game_id(), &golden_hop_txid(), 0),
        );
        if let OutputAdmittedByTopic::LockingScript { ref mut topic, .. } = payload {
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
        // potparty v1 + v2 scripts under tm_hopparty → skipped.
        let (svc, storage) = make_service_with_storage();
        for script in [
            crate::potparty::tests::golden_marker(&golden_game_id(), &golden_hop_txid(), 0),
            crate::potparty::tests::golden_marker_v2(&golden_game_id(), &golden_hop_txid(), 0),
        ] {
            svc.output_admitted_by_topic(&admit("txPP", 0, script))
                .await
                .unwrap();
        }
        assert_eq!(storage.record_count(), 0, "potparty scripts never stored");

        // The reverse: a hopparty script through the POTPARTY LS.
        let pp_storage = Rc::new(crate::potparty::storage::MemoryPotpartyStorage::new());
        let pp_svc =
            crate::potparty::lookup_service::PotpartyLookupService::new(pp_storage.clone());
        pp_svc
            .output_admitted_by_topic(&OutputAdmittedByTopic::LockingScript {
                txid: "txHOP".into(),
                output_index: 0,
                topic: "tm_potparty".into(),
                satoshis: 0,
                locking_script: golden_marker(&golden_game_id(), &golden_hop_txid(), 0),
                off_chain_values: None,
            })
            .await
            .unwrap();
        assert_eq!(pp_storage.record_count(), 0, "hopparty scripts never stored in potparty");

        // Positive control: the same hopparty script under ITS topic stores.
        svc.output_admitted_by_topic(&admit(
            "txOK",
            0,
            golden_marker(&golden_game_id(), &golden_hop_txid(), 0),
        ))
        .await
        .unwrap();
        assert_eq!(storage.record_count(), 1);
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
            topic: "tm_hopparty".into(),
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
            golden_marker(&golden_game_id(), &golden_hop_txid(), 0),
        ))
        .await
        .unwrap();
        assert_eq!(storage.record_count(), 1);

        let spent = OutputSpent::None {
            txid: "tx1".into(),
            output_index: 0,
            topic: "tm_hopparty".into(),
        };
        svc.output_spent(&spent).await.unwrap();
        assert_eq!(storage.record_count(), 1, "marker must survive a spend");
        svc.output_evicted("tx1", 0).await.unwrap();
        assert_eq!(storage.record_count(), 1, "marker must survive an eviction");
    }

    // ── Case-insensitive hex + query validation ──────────────────────────

    #[tokio::test]
    async fn lookup_case_insensitive_hex() {
        let (svc, _storage) = make_service_with_storage();
        svc.output_admitted_by_topic(&admit(
            "txA",
            0,
            golden_marker(&golden_game_id(), &golden_hop_txid(), 0),
        ))
        .await
        .unwrap();
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

    /// bsv-low #315 Rule-16 CROSS-REPO BOUNDARY PIN (overlay half).
    ///
    /// `fixtures/ls_hopparty_hopsfor.fixture.json` is the EXACT `result`
    /// array the real `lookup()` serializer emits for `hopsFor` over two
    /// admitted markers — the first being the GOLDEN cross-repo marker
    /// (real BRC-42 + RFC6979 crypto, `PrivateKey(1)`/`PrivateKey(2)`),
    /// admitted through the REAL `output_admitted_by_topic` producer path
    /// (never a hand-inserted row). The bsv-low CLIENT will pin a
    /// byte-identical copy and drive it through ITS parser: rename a
    /// serialized field HERE and this cell goes red; drift the client's
    /// parser and its cell goes red. A property spanning two repos cannot
    /// be pinned inside either one — regenerate BOTH copies together or
    /// not at all.
    #[tokio::test]
    async fn hopsfor_answer_matches_cross_repo_fixture() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/ls_hopparty_hopsfor.fixture.json"))
                .expect("fixture parses");

        let (svc, storage) = make_service_with_storage();
        // Row 1: the GOLDEN marker (hop outpoint dd…:1), admitted via the
        // real LS admission path at marker outpoint c1…:1.
        svc.output_admitted_by_topic(&admit(&"c1".repeat(32), 1, build_golden_marker()))
            .await
            .unwrap();
        // Row 2: a second marker for the SAME identity naming a different
        // hop (ee…:0) — deterministic dummy-DER sigs (admission is
        // byte-shape only; the sig VALUES are exercised by the golden row).
        let (wallet, opponent, game_id, _, _, _) = golden_vector_inputs();
        let identity = hex::decode(wallet.identity_key_hex()).unwrap();
        let dummy_sig = {
            let mut s = vec![0x30u8, 0x45];
            s.extend_from_slice(&[0x77u8; 69]);
            s
        };
        svc.output_admitted_by_topic(&admit(
            &"c2".repeat(32),
            1,
            marker_script(
                &identity,
                &opponent,
                &game_id,
                &[0xeeu8; 32],
                0,
                42_000,
                &golden_settle_pubkey(),
                &dummy_sig,
                &dummy_sig,
            ),
        ))
        .await
        .unwrap();
        assert_eq!(storage.record_count(), 2);

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
