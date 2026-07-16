//! PROOF Lookup Service — indexes and queries LOW transcript-proof
//! bundle markers (leaderboard rung 3).
//!
//! When outputs are admitted to `tm_proof`, this service parses the
//! `LOW/proof/v1` marker and stores one row per marker OUTPOINT
//! `(txid, outputIndex)` via [`ProofStorage`] — EVERY admitted marker is
//! kept (a duplicate submit of the same output is a no-op). The
//! tm_result censorship lesson applies identically: admission is
//! byte-format-only, so a `(gameId, winner)`-keyed first-marker-wins
//! index would let a garbage bundle front-run the real proof; with
//! outpoint keying garbage and genuine bundles coexist and the CLIENT
//! verifies each one's transcript cryptography, using the one that
//! proves.
//!
//! Leaderboard clients ask `proofsFor` during the badge gather; the
//! answer is a freeform, newest-first JSON array carrying the marker's
//! bytes back VERBATIM (`sigHex` + `bundleBase64`) — the overlay never
//! verifies a signature or parses the bundle JSON, and a bundle that
//! fails the client verify simply earns no badge (the claim stays merely
//! countersigned — never hidden, never upgraded). Limits are small
//! (default 3, max 10): bundles run ~10–15 KB each.
//!
//! Permanence: a published proof is a permanent fact and the admitted
//! output is a provably-unspendable OP_RETURN. `spend_notification_mode`
//! is [`SpendNotificationMode::None`], and `output_spent` /
//! `output_evicted` are deliberate no-ops — a proof record is NEVER
//! removed (mirrors `ls_result` / `ls_collected` / `ls_reveal`).

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use overlay_engine::lookup_service::{LookupService, LookupServiceError};
use overlay_engine::types::*;
use std::rc::Rc;
use tracing::debug;

use super::parse_proof_marker;
use super::storage::{ProofQuery, ProofRecord, ProofStorage};

/// Default number of records returned when a query omits `limit`.
/// Deliberately small — each row carries a ~10–15 KB bundle.
const DEFAULT_LIMIT: usize = 3;
/// Hard cap on the number of records a single query can return.
const MAX_LIMIT: usize = 10;

/// PROOF Lookup Service — indexes markers and answers `proofsFor`.
pub struct ProofLookupService {
    storage: Rc<dyn ProofStorage>,
}

impl ProofLookupService {
    /// Create a new PROOF lookup service backed by the given storage.
    pub fn new(storage: Rc<dyn ProofStorage>) -> Self {
        Self { storage }
    }
}

#[async_trait(?Send)]
impl LookupService for ProofLookupService {
    fn admission_mode(&self) -> AdmissionMode {
        AdmissionMode::LockingScript
    }

    fn spend_notification_mode(&self) -> SpendNotificationMode {
        // A proof marker is permanent; we never want a spend notification
        // (the OP_RETURN can't be spent anyway) to touch the index.
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

        // Only index tm_proof outputs.
        if topic != "tm_proof" {
            return Ok(());
        }

        // The topic manager already validated the marker; re-parse to
        // recover the fields for the index (defensive — the TM should
        // never admit anything this can't parse).
        let Some(marker) = parse_proof_marker(locking_script) else {
            debug!("PROOF: admitted output is not a parseable marker — skipped");
            return Ok(());
        };

        let record = ProofRecord {
            game_id: hex::encode(marker.game_id),
            winner: hex::encode(&marker.winner),
            sig_hex: hex::encode(&marker.sig),
            // The bundle BYTES verbatim — content never validated here.
            bundle: marker.bundle,
            txid: txid.to_string(),
            output_index,
            created_at: 0, // assigned by the storage layer at insert
        };

        // Keyed by the OUTPOINT: a replayed submit of the same output is a
        // harmless no-op, while bundles for the same (gameId, winner) from
        // different txs are ALL kept (censorship fix — see module docs).
        self.storage
            .store_record(&record)
            .await
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;

        Ok(())
    }

    async fn output_spent(&self, _payload: &OutputSpent) -> Result<(), LookupServiceError> {
        // No-op: a proof marker is a PERMANENT fact. The admitted output
        // is an unspendable OP_RETURN, so this never fires anyway — but
        // even if it did, we must not evict the record.
        Ok(())
    }

    async fn output_evicted(
        &self,
        _txid: &str,
        _output_index: u32,
    ) -> Result<(), LookupServiceError> {
        // No-op: proof records are never evicted (permanence — above).
        Ok(())
    }

    async fn lookup(&self, question: &LookupQuestion) -> Result<LookupResult, LookupServiceError> {
        if question.service != "ls_proof" {
            return Err(LookupServiceError::Unsupported(format!(
                "Expected ls_proof, got {}",
                question.service
            )));
        }

        let query: ProofQuery = serde_json::from_value(question.query.clone())
            .map_err(|e| LookupServiceError::InvalidQuery(e.to_string()))?;

        let ProofQuery::ProofsFor {
            game_id,
            winner,
            limit,
        } = query;
        let game_id = normalize_game_id(&game_id)?;
        let winner = normalize_identity(&winner)?;

        let records = self
            .storage
            .list_for_game_winner(&game_id, &winner, clamp_limit(limit))
            .await
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;

        // Carry the stored bytes back VERBATIM — the bundle base64-encoded
        // at this edge only (it is raw JSON bytes in storage). No derived
        // "verified" flag: the overlay is an index, not an authority.
        let entries: Vec<serde_json::Value> = records
            .into_iter()
            .map(|r| {
                serde_json::json!({
                    "gameId": r.game_id,
                    "winner": r.winner,
                    "sigHex": r.sig_hex,
                    "bundleBase64": BASE64.encode(&r.bundle),
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
        include_str!("../../docs/proof_lookup.md").to_string()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "PROOF Lookup Service".to_string(),
            description: Some(
                "Answers 'which transcript-proof bundles exist for this \
                 hand's winner?' over LOW proof markers (bundles returned \
                 verbatim — verified client-side)."
                    .to_string(),
            ),
            ..Default::default()
        }
    }
}

/// Clamp an optional query limit to `1..=MAX_LIMIT` (default
/// [`DEFAULT_LIMIT`] when absent). A `limit: 0` clamps up to 1 — never an
/// error, never unbounded.
fn clamp_limit(limit: Option<u32>) -> usize {
    (limit.map(|l| l as usize).unwrap_or(DEFAULT_LIMIT)).clamp(1, MAX_LIMIT)
}

/// Validate a 33-byte identity-key hex param and return it lowercased
/// (stored values are lowercase `hex::encode` output).
fn normalize_identity(value: &str) -> Result<String, LookupServiceError> {
    let lower = value.to_ascii_lowercase();
    if lower.len() != 66 || !lower.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(LookupServiceError::InvalidQuery(
            "winner must be 66 hex chars (a 33-byte compressed pubkey)".into(),
        ));
    }
    Ok(lower)
}

/// Validate a 32-byte gameId hex param and return it lowercased.
fn normalize_game_id(value: &str) -> Result<String, LookupServiceError> {
    let lower = value.to_ascii_lowercase();
    if lower.len() != 64 || !lower.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(LookupServiceError::InvalidQuery(
            "gameId must be 64 hex chars".into(),
        ));
    }
    Ok(lower)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::super::storage::MemoryProofStorage;
    use super::super::tests::{
        golden_bundle, golden_marker, golden_sig, golden_winner, GOLDEN_PROOF_HEX,
    };
    use super::*;

    fn make_service_with_storage() -> (ProofLookupService, Rc<MemoryProofStorage>) {
        let storage = Rc::new(MemoryProofStorage::new());
        let svc = ProofLookupService::new(storage.clone());
        (svc, storage)
    }

    fn make_service() -> ProofLookupService {
        make_service_with_storage().0
    }

    fn admit(txid: &str, output_index: u32, script: Vec<u8>) -> OutputAdmittedByTopic {
        OutputAdmittedByTopic::LockingScript {
            txid: txid.into(),
            output_index,
            topic: "tm_proof".into(),
            satoshis: 0,
            locking_script: script,
            off_chain_values: None,
        }
    }

    /// Run a proofsFor lookup and return the JSON array.
    async fn proofs_for(
        svc: &ProofLookupService,
        game_id: &str,
        winner: &str,
        limit: Option<u32>,
    ) -> serde_json::Value {
        let mut q = serde_json::json!({"type": "proofsFor", "gameId": game_id, "winner": winner});
        if let Some(l) = limit {
            q["limit"] = serde_json::json!(l);
        }
        let q = LookupQuestion::new("ls_proof", q);
        match svc.lookup(&q).await.unwrap() {
            LookupResult::Answer(LookupAnswer::Freeform { result }) => result,
            other => panic!("expected Freeform answer, got {other:?}"),
        }
    }

    fn golden_winner_hex() -> String {
        hex::encode(golden_winner())
    }

    // ── Trait plumbing ───────────────────────────────────────────────────

    #[tokio::test]
    async fn modes_and_metadata() {
        let svc = make_service();
        assert_eq!(svc.admission_mode(), AdmissionMode::LockingScript);
        assert_eq!(svc.spend_notification_mode(), SpendNotificationMode::None);
        let meta = svc.get_metadata().await;
        assert_eq!(meta.name, "PROOF Lookup Service");
        assert!(!svc.get_documentation().await.is_empty());
    }

    // ── Admission + lookup (the golden vector end-to-end) ────────────────

    #[tokio::test]
    async fn golden_marker_admitted_and_found() {
        let (svc, storage) = make_service_with_storage();
        let script = hex::decode(GOLDEN_PROOF_HEX).unwrap();
        svc.output_admitted_by_topic(&admit("markerTx1", 0, script))
            .await
            .unwrap();
        assert_eq!(storage.record_count(), 1);

        let arr = proofs_for(&svc, &"11".repeat(32), &golden_winner_hex(), None).await;
        let e = &arr[0];
        assert_eq!(e["gameId"], "11".repeat(32));
        assert_eq!(e["winner"], golden_winner_hex());
        assert_eq!(e["sigHex"], hex::encode(golden_sig()));
        // The golden bundle `{"v":1,"test":true}` base64-encodes to this
        // exact string — bytes verbatim through the whole pipe.
        assert_eq!(e["bundleBase64"], "eyJ2IjoxLCJ0ZXN0Ijp0cnVlfQ==");
        assert_eq!(
            BASE64.decode(e["bundleBase64"].as_str().unwrap()).unwrap(),
            golden_bundle()
        );
        assert_eq!(e["txid"], "markerTx1");
        assert_eq!(e["outputIndex"], 0);
        assert!(e["createdAt"].is_i64());
    }

    #[tokio::test]
    async fn big_bundle_survives_the_producer_path() {
        // A 20 KB (OP_PUSHDATA2) bundle admits and comes back verbatim.
        let (svc, _storage) = make_service_with_storage();
        let bundle: Vec<u8> = (0..20_000u32).map(|i| (i % 251) as u8).collect();
        svc.output_admitted_by_topic(&admit(
            "txBIG",
            0,
            golden_marker(&[0x11u8; 32], &bundle),
        ))
        .await
        .unwrap();

        let arr = proofs_for(&svc, &"11".repeat(32), &golden_winner_hex(), None).await;
        let got = BASE64
            .decode(arr[0]["bundleBase64"].as_str().unwrap())
            .unwrap();
        assert_eq!(got, bundle, "20 KB bundle round-trips byte-for-byte");
    }

    // ── The pair filter ───────────────────────────────────────────────────

    #[tokio::test]
    async fn proofs_for_filters_by_game_and_winner() {
        let (svc, _storage) = make_service_with_storage();
        svc.output_admitted_by_topic(&admit(
            "txA",
            0,
            golden_marker(&[0x11u8; 32], b"game11"),
        ))
        .await
        .unwrap();
        svc.output_admitted_by_topic(&admit(
            "txB",
            0,
            golden_marker(&[0x22u8; 32], b"game22"),
        ))
        .await
        .unwrap();

        // Only game 11's bundle for this winner.
        let arr = proofs_for(&svc, &"11".repeat(32), &golden_winner_hex(), None).await;
        let arr = arr.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["txid"], "txA");

        // A different winner for the same game sees nothing.
        let other = "03".to_string() + &"b2".repeat(32);
        let arr = proofs_for(&svc, &"11".repeat(32), &other, None).await;
        assert!(arr.as_array().unwrap().is_empty());

        // An unknown game sees nothing.
        let arr = proofs_for(&svc, &"33".repeat(32), &golden_winner_hex(), None).await;
        assert!(arr.as_array().unwrap().is_empty());
    }

    // ── Outpoint keying through the producer path ─────────────────────────

    #[tokio::test]
    async fn same_outpoint_replay_is_a_noop() {
        let (svc, storage) = make_service_with_storage();
        let script = golden_marker(&[0x11u8; 32], &golden_bundle());
        svc.output_admitted_by_topic(&admit("txSAME", 0, script.clone()))
            .await
            .unwrap();
        svc.output_admitted_by_topic(&admit("txSAME", 0, script))
            .await
            .unwrap();
        assert_eq!(storage.record_count(), 1, "same-outpoint replay is a no-op");
    }

    #[tokio::test]
    async fn garbage_bundle_cannot_censor_a_later_genuine_proof() {
        // The tm_result censorship lesson applied to proofs: a garbage
        // bundle published first for (gameId, winner) must NOT hide the
        // real proof published later — both rows come back, newest first,
        // and the CLIENT's transcript verify picks the one that proves.
        let (svc, storage) = make_service_with_storage();
        let game = [0x11u8; 32];

        svc.output_admitted_by_topic(&admit(
            "txGARBAGE",
            0,
            golden_marker(&game, b"{\"junk\":true}"),
        ))
        .await
        .unwrap();
        svc.output_admitted_by_topic(&admit(
            "txGENUINE",
            0,
            golden_marker(&game, &golden_bundle()),
        ))
        .await
        .unwrap();

        assert_eq!(storage.record_count(), 2);
        let arr = proofs_for(&svc, &"11".repeat(32), &golden_winner_hex(), None).await;
        let arr = arr.as_array().unwrap();
        assert_eq!(arr.len(), 2, "garbage AND genuine bundles coexist");
        assert_eq!(arr[0]["txid"], "txGENUINE", "newest first");
        assert_eq!(arr[1]["txid"], "txGARBAGE");
    }

    // ── Limits ────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn limit_defaults_and_clamps() {
        let (svc, _storage) = make_service_with_storage();
        // Five bundles for the same pair from distinct txs.
        for i in 1u8..=5 {
            svc.output_admitted_by_topic(&admit(
                &format!("tx{i}"),
                0,
                golden_marker(&[0x11u8; 32], &[i]),
            ))
            .await
            .unwrap();
        }

        // No limit → default 3 (bundles are big).
        let arr = proofs_for(&svc, &"11".repeat(32), &golden_winner_hex(), None).await;
        let arr = arr.as_array().unwrap();
        assert_eq!(arr.len(), 3, "default limit is 3");
        assert_eq!(arr[0]["txid"], "tx5", "newest first");

        // limit 0 clamps UP to 1; a huge limit clamps DOWN to 10.
        let arr = proofs_for(&svc, &"11".repeat(32), &golden_winner_hex(), Some(0)).await;
        assert_eq!(arr.as_array().unwrap().len(), 1);
        let arr =
            proofs_for(&svc, &"11".repeat(32), &golden_winner_hex(), Some(1_000_000)).await;
        assert_eq!(arr.as_array().unwrap().len(), 5, "all five under the cap");

        // The clamp helper itself.
        assert_eq!(clamp_limit(None), DEFAULT_LIMIT);
        assert_eq!(clamp_limit(Some(0)), 1);
        assert_eq!(clamp_limit(Some(7)), 7);
        assert_eq!(clamp_limit(Some(1_000_000)), MAX_LIMIT);
    }

    // ── Admission filters ────────────────────────────────────────────────

    #[tokio::test]
    async fn ignores_non_tm_proof_topic() {
        let (svc, storage) = make_service_with_storage();
        let mut payload = admit("tx1", 0, golden_marker(&[0x11u8; 32], &golden_bundle()));
        if let OutputAdmittedByTopic::LockingScript { ref mut topic, .. } = payload {
            *topic = "tm_result".into();
        }
        svc.output_admitted_by_topic(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 0);
    }

    #[tokio::test]
    async fn ignores_non_marker_script() {
        let (svc, storage) = make_service_with_storage();
        // A P2PKH — not a proof marker.
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
            topic: "tm_proof".into(),
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
            golden_marker(&[0x11u8; 32], &golden_bundle()),
        ))
        .await
        .unwrap();
        assert_eq!(storage.record_count(), 1);

        let spent = OutputSpent::None {
            txid: "tx1".into(),
            output_index: 0,
            topic: "tm_proof".into(),
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
            golden_marker(&[0xABu8; 32], &golden_bundle()),
        ))
        .await
        .unwrap();

        // Uppercase query hex (gameId AND winner) still matches the
        // lowercase stored values, and the answer echoes the normalized
        // lowercase forms.
        let arr = proofs_for(
            &svc,
            &"AB".repeat(32),
            &golden_winner_hex().to_uppercase(),
            None,
        )
        .await;
        let arr = arr.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["gameId"], "ab".repeat(32));
        assert_eq!(arr[0]["winner"], golden_winner_hex());
    }

    // ── Query validation ─────────────────────────────────────────────────

    #[tokio::test]
    async fn lookup_wrong_service_errors() {
        let svc = make_service();
        let q = LookupQuestion::new(
            "ls_result",
            serde_json::json!({"type": "proofsFor", "gameId": "11".repeat(32),
                               "winner": "02".to_string() + &"a1".repeat(32)}),
        );
        assert!(svc.lookup(&q).await.is_err());
    }

    #[tokio::test]
    async fn lookup_invalid_query_errors() {
        let svc = make_service();
        let good_game = "11".repeat(32);
        let good_winner = "02".to_string() + &"a1".repeat(32);
        for bad in [
            serde_json::json!({"type": "unknownQuery"}),
            serde_json::json!("proofsFor"),
            serde_json::json!(42),
            // missing winner
            serde_json::json!({"type": "proofsFor", "gameId": good_game}),
            // missing gameId
            serde_json::json!({"type": "proofsFor", "winner": good_winner}),
            // winner not hex / wrong length
            serde_json::json!({"type": "proofsFor", "gameId": good_game, "winner": "zz".repeat(33)}),
            serde_json::json!({"type": "proofsFor", "gameId": good_game, "winner": "02a1"}),
            // gameId not hex / wrong length
            serde_json::json!({"type": "proofsFor", "gameId": "zz".repeat(32), "winner": good_winner}),
            serde_json::json!({"type": "proofsFor", "gameId": "ab", "winner": good_winner}),
            // limit of the wrong type
            serde_json::json!({"type": "proofsFor", "gameId": good_game, "winner": good_winner, "limit": "three"}),
        ] {
            let q = LookupQuestion::new("ls_proof", bad.clone());
            assert!(svc.lookup(&q).await.is_err(), "expected error for {bad}");
        }
    }
}
