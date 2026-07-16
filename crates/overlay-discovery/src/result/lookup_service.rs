//! RESULT Lookup Service — indexes and queries LOW hand-result markers
//! for the on-chain leaderboard (bsv-low #38).
//!
//! When outputs are admitted to `tm_result`, this service parses the
//! `LOW/result/v1` marker and stores one row per marker OUTPOINT
//! `(txid, outputIndex)` via [`ResultStorage`] — EVERY admitted marker is
//! kept (a duplicate submit of the same output is a no-op). Admission is
//! byte-format-only, so a `(gameId, winner)`-keyed index would let a
//! garbage-sig front-run permanently censor the genuine countersigned
//! marker (adversarial-review HIGH, 2026-07-16); outpoint keying makes
//! garbage and genuine rows coexist, and the client aggregation drops
//! invalid-sig rows before dedup so the verifying one counts.
//! Leaderboard clients ask
//! `resultsFor` (one identity's wins) or `recentResults` (the global
//! feed); the answer is a freeform, newest-first JSON array carrying the
//! marker's bytes back VERBATIM — winnerSigHex / loserSigHex / potTxid /
//! settleTxid — so the CLIENT can verify both signatures over the
//! canonical challenge (the 'anyone' ProtoWallet round-trip) and anchor
//! the claim to a REAL settled pot via `/pots-view`. The overlay never
//! verifies a signature and derives no "confirmed" flag: a client judges
//! a claim confirmed IFF the loser countersignature it verifies is
//! present. The record surface must never lie — bytes in, bytes out.
//!
//! Permanence: a settled result is a permanent fact and the admitted
//! output is a provably-unspendable OP_RETURN. `spend_notification_mode`
//! is [`SpendNotificationMode::None`], and `output_spent` /
//! `output_evicted` are deliberate no-ops — a result record is NEVER
//! removed (mirrors `ls_collected` / `ls_reveal`).

use async_trait::async_trait;
use overlay_engine::lookup_service::{LookupService, LookupServiceError};
use overlay_engine::types::*;
use std::rc::Rc;
use tracing::debug;

use super::parse_result_marker;
use super::storage::{ResultQuery, ResultRecord, ResultStorage};

/// Default number of records returned when a query omits `limit`.
const DEFAULT_LIMIT: usize = 100;
/// Hard cap on the number of records a single query can return.
const MAX_LIMIT: usize = 500;

/// RESULT Lookup Service — indexes markers and answers `resultsFor` /
/// `recentResults`.
pub struct ResultLookupService {
    storage: Rc<dyn ResultStorage>,
}

impl ResultLookupService {
    /// Create a new RESULT lookup service backed by the given storage.
    pub fn new(storage: Rc<dyn ResultStorage>) -> Self {
        Self { storage }
    }
}

#[async_trait(?Send)]
impl LookupService for ResultLookupService {
    fn admission_mode(&self) -> AdmissionMode {
        AdmissionMode::LockingScript
    }

    fn spend_notification_mode(&self) -> SpendNotificationMode {
        // A result marker is permanent; we never want a spend
        // notification (the OP_RETURN can't be spent anyway) to touch the
        // index.
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

        // Only index tm_result outputs.
        if topic != "tm_result" {
            return Ok(());
        }

        // The topic manager already validated the marker; re-parse to
        // recover the fields for the index (defensive — the TM should
        // never admit anything this can't parse).
        let Some(marker) = parse_result_marker(locking_script) else {
            debug!("RESULT: admitted output is not a parseable marker — skipped");
            return Ok(());
        };

        let record = ResultRecord {
            game_id: hex::encode(marker.game_id),
            winner: hex::encode(&marker.winner),
            loser: hex::encode(&marker.loser),
            pot_txid: hex::encode(marker.pot_txid),
            settle_txid: hex::encode(marker.settle_txid),
            winner_sig_hex: hex::encode(&marker.winner_sig),
            // None when the marker's loserSig push was empty — an
            // UNCONFIRMED claim, preserved verbatim (never synthesized).
            loser_sig_hex: marker.loser_sig.as_deref().map(hex::encode),
            txid: txid.to_string(),
            output_index,
            created_at: 0, // assigned by the storage layer at insert
        };

        // Keyed by the OUTPOINT: the storage layer's insert-if-absent makes
        // a replayed submit of the same output a harmless no-op, while
        // markers for the same (gameId, winner) from different txs are ALL
        // kept (censorship fix — see the module docs).
        self.storage
            .store_record(&record)
            .await
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;

        Ok(())
    }

    async fn output_spent(&self, _payload: &OutputSpent) -> Result<(), LookupServiceError> {
        // No-op: a result marker is a PERMANENT fact. The admitted output
        // is an unspendable OP_RETURN, so this never fires anyway — but even
        // if it did, we must not evict the record.
        Ok(())
    }

    async fn output_evicted(
        &self,
        _txid: &str,
        _output_index: u32,
    ) -> Result<(), LookupServiceError> {
        // No-op: result records are never evicted (permanence — above).
        Ok(())
    }

    async fn lookup(&self, question: &LookupQuestion) -> Result<LookupResult, LookupServiceError> {
        if question.service != "ls_result" {
            return Err(LookupServiceError::Unsupported(format!(
                "Expected ls_result, got {}",
                question.service
            )));
        }

        let query: ResultQuery = serde_json::from_value(question.query.clone())
            .map_err(|e| LookupServiceError::InvalidQuery(e.to_string()))?;

        let records = match query {
            ResultQuery::ResultsFor { identity, limit } => {
                let identity = normalize_identity(&identity)?;
                self.storage
                    .list_for_winner(&identity, clamp_limit(limit))
                    .await
                    .map_err(|e| LookupServiceError::StorageError(e.to_string()))?
            }
            ResultQuery::RecentResults { limit } => self
                .storage
                .list_recent(clamp_limit(limit))
                .await
                .map_err(|e| LookupServiceError::StorageError(e.to_string()))?,
        };

        // Carry the stored bytes back VERBATIM — no derived "confirmed"
        // flag (the overlay is an index, not an authority; clients derive
        // confirmation by verifying the sigs themselves).
        let entries: Vec<serde_json::Value> = records
            .into_iter()
            .map(|r| {
                serde_json::json!({
                    "gameId": r.game_id,
                    "winner": r.winner,
                    "loser": r.loser,
                    "potTxid": r.pot_txid,
                    "settleTxid": r.settle_txid,
                    "winnerSigHex": r.winner_sig_hex,
                    "loserSigHex": r.loser_sig_hex
                        .map(serde_json::Value::String)
                        .unwrap_or(serde_json::Value::Null),
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
        include_str!("../../docs/result_lookup.md").to_string()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "RESULT Lookup Service".to_string(),
            description: Some(
                "Answers 'which hands has this identity won?' and 'what \
                 settled recently?' over LOW result markers (leaderboard \
                 index — sigs verified client-side)."
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

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::super::storage::MemoryResultStorage;
    use super::super::tests::{
        golden_loser, golden_loser_sig, golden_marker, golden_pot_txid, golden_settle_txid,
        golden_winner, golden_winner_sig, GOLDEN_RESULT_HEX, GOLDEN_RESULT_UNCONFIRMED_HEX,
    };
    use super::*;

    fn make_service_with_storage() -> (ResultLookupService, Rc<MemoryResultStorage>) {
        let storage = Rc::new(MemoryResultStorage::new());
        let svc = ResultLookupService::new(storage.clone());
        (svc, storage)
    }

    fn make_service() -> ResultLookupService {
        make_service_with_storage().0
    }

    fn admit(txid: &str, output_index: u32, script: Vec<u8>) -> OutputAdmittedByTopic {
        OutputAdmittedByTopic::LockingScript {
            txid: txid.into(),
            output_index,
            topic: "tm_result".into(),
            satoshis: 0,
            locking_script: script,
            off_chain_values: None,
        }
    }

    /// Run a lookup and return the JSON array.
    async fn run_lookup(svc: &ResultLookupService, query: serde_json::Value) -> serde_json::Value {
        let q = LookupQuestion::new("ls_result", query);
        match svc.lookup(&q).await.unwrap() {
            LookupResult::Answer(LookupAnswer::Freeform { result }) => result,
            other => panic!("expected Freeform answer, got {other:?}"),
        }
    }

    async fn results_for(
        svc: &ResultLookupService,
        identity: &str,
        limit: Option<u32>,
    ) -> serde_json::Value {
        let mut q = serde_json::json!({"type": "resultsFor", "identity": identity});
        if let Some(l) = limit {
            q["limit"] = serde_json::json!(l);
        }
        run_lookup(svc, q).await
    }

    async fn recent_results(svc: &ResultLookupService, limit: Option<u32>) -> serde_json::Value {
        let mut q = serde_json::json!({"type": "recentResults"});
        if let Some(l) = limit {
            q["limit"] = serde_json::json!(l);
        }
        run_lookup(svc, q).await
    }

    fn golden_winner_hex() -> String {
        hex::encode(golden_winner())
    }
    fn golden_loser_hex() -> String {
        hex::encode(golden_loser())
    }

    // ── Trait plumbing ───────────────────────────────────────────────────

    #[tokio::test]
    async fn modes_and_metadata() {
        let svc = make_service();
        assert_eq!(svc.admission_mode(), AdmissionMode::LockingScript);
        assert_eq!(svc.spend_notification_mode(), SpendNotificationMode::None);
        let meta = svc.get_metadata().await;
        assert_eq!(meta.name, "RESULT Lookup Service");
        assert!(!svc.get_documentation().await.is_empty());
    }

    // ── Admission + lookup (the golden vectors end-to-end) ───────────────

    #[tokio::test]
    async fn golden_marker_admitted_and_found() {
        let (svc, storage) = make_service_with_storage();
        let script = hex::decode(GOLDEN_RESULT_HEX).unwrap();
        svc.output_admitted_by_topic(&admit("markerTx1", 0, script))
            .await
            .unwrap();
        assert_eq!(storage.record_count(), 1);

        let arr = results_for(&svc, &golden_winner_hex(), None).await;
        let e = &arr[0];
        assert_eq!(e["gameId"], "11".repeat(32));
        assert_eq!(e["winner"], golden_winner_hex());
        assert_eq!(e["loser"], golden_loser_hex());
        assert_eq!(e["potTxid"], "22".repeat(32));
        assert_eq!(e["settleTxid"], "33".repeat(32));
        assert_eq!(e["winnerSigHex"], hex::encode(golden_winner_sig()));
        assert_eq!(e["loserSigHex"], hex::encode(golden_loser_sig()));
        assert_eq!(e["txid"], "markerTx1");
        assert!(e["createdAt"].is_i64());

        // The same entry rides the recent feed.
        let arr = recent_results(&svc, None).await;
        assert_eq!(arr.as_array().unwrap().len(), 1);
        assert_eq!(arr[0]["txid"], "markerTx1");
    }

    #[tokio::test]
    async fn golden_unconfirmed_marker_has_null_loser_sig() {
        let (svc, storage) = make_service_with_storage();
        let script = hex::decode(GOLDEN_RESULT_UNCONFIRMED_HEX).unwrap();
        svc.output_admitted_by_topic(&admit("markerTx2", 0, script))
            .await
            .unwrap();
        assert_eq!(storage.record_count(), 1);

        let arr = results_for(&svc, &golden_winner_hex(), None).await;
        let e = &arr[0];
        assert_eq!(e["winnerSigHex"], hex::encode(golden_winner_sig()));
        assert!(
            e["loserSigHex"].is_null(),
            "empty loserSig push ⇒ null loserSigHex (an unconfirmed claim, \
             preserved verbatim)"
        );
    }

    // ── resultsFor filters by winner only ─────────────────────────────────

    #[tokio::test]
    async fn results_for_filters_by_winner_only() {
        let (svc, _storage) = make_service_with_storage();
        // Game 11: golden winner beats golden loser.
        svc.output_admitted_by_topic(&admit(
            "txA",
            0,
            golden_marker(&[0x11u8; 32], &golden_loser_sig()),
        ))
        .await
        .unwrap();
        // Game 22: the seats flip — golden loser WINS.
        svc.output_admitted_by_topic(&admit(
            "txB",
            0,
            super::super::tests::marker_script(
                &[0x22u8; 32],
                &golden_loser(),
                &golden_winner(),
                &golden_pot_txid(),
                &golden_settle_txid(),
                &golden_winner_sig(),
                &golden_loser_sig(),
            ),
        ))
        .await
        .unwrap();

        // The golden winner sees only its own WIN — never the hand it lost.
        let arr = results_for(&svc, &golden_winner_hex(), None).await;
        let arr = arr.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["txid"], "txA");
        assert_eq!(arr[0]["winner"], golden_winner_hex());

        // Flip side: the other identity's wins list carries only game 22.
        let arr = results_for(&svc, &golden_loser_hex(), None).await;
        let arr = arr.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["txid"], "txB");

        // An identity that never won sees an empty array.
        let never_won = "02".to_string() + &"ee".repeat(32);
        let arr = results_for(&svc, &never_won, None).await;
        assert!(arr.as_array().unwrap().is_empty());
    }

    // ── recentResults ordering + limit ────────────────────────────────────

    #[tokio::test]
    async fn recent_results_newest_first_respects_limit() {
        let (svc, _storage) = make_service_with_storage();
        for i in 1u8..=5 {
            svc.output_admitted_by_topic(&admit(
                &format!("tx{i}"),
                0,
                golden_marker(&[i; 32], &golden_loser_sig()),
            ))
            .await
            .unwrap();
        }

        let arr = recent_results(&svc, Some(3)).await;
        let arr = arr.as_array().unwrap();
        assert_eq!(arr.len(), 3, "limit respected");
        assert_eq!(arr[0]["txid"], "tx5", "newest first");
        assert_eq!(arr[1]["txid"], "tx4");
        assert_eq!(arr[2]["txid"], "tx3");

        // resultsFor is newest-first too.
        let arr = results_for(&svc, &golden_winner_hex(), Some(2)).await;
        let arr = arr.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["txid"], "tx5");
    }

    #[tokio::test]
    async fn limit_defaults_and_clamps() {
        let (svc, _storage) = make_service_with_storage();
        for i in 1u8..=5 {
            svc.output_admitted_by_topic(&admit(
                &format!("tx{i}"),
                0,
                golden_marker(&[i; 32], &golden_loser_sig()),
            ))
            .await
            .unwrap();
        }

        // No limit → default (100) — all 5 come back.
        let arr = recent_results(&svc, None).await;
        assert_eq!(arr.as_array().unwrap().len(), 5);

        // limit 0 clamps UP to 1 (never an error, never unbounded).
        let arr = recent_results(&svc, Some(0)).await;
        assert_eq!(arr.as_array().unwrap().len(), 1);

        // A huge limit clamps DOWN to MAX_LIMIT — all 5 still come back.
        let arr = recent_results(&svc, Some(1_000_000)).await;
        assert_eq!(arr.as_array().unwrap().len(), 5);

        // The clamp helper itself.
        assert_eq!(clamp_limit(None), DEFAULT_LIMIT);
        assert_eq!(clamp_limit(Some(0)), 1);
        assert_eq!(clamp_limit(Some(50)), 50);
        assert_eq!(clamp_limit(Some(1_000_000)), MAX_LIMIT);
    }

    // ── Outpoint keying through the producer path ─────────────────────────

    #[tokio::test]
    async fn same_outpoint_replay_is_a_noop() {
        let (svc, storage) = make_service_with_storage();
        let script = golden_marker(&[0x11u8; 32], &golden_loser_sig());
        svc.output_admitted_by_topic(&admit("txSAME", 0, script.clone()))
            .await
            .unwrap();
        // A replayed / duplicate SUBMIT of the SAME output — ignored.
        svc.output_admitted_by_topic(&admit("txSAME", 0, script))
            .await
            .unwrap();

        assert_eq!(storage.record_count(), 1, "same-outpoint replay is a no-op");
        let arr = results_for(&svc, &golden_winner_hex(), None).await;
        assert_eq!(arr.as_array().unwrap().len(), 1);
        assert_eq!(arr[0]["txid"], "txSAME");
        assert_eq!(arr[0]["outputIndex"], 0);
    }

    #[tokio::test]
    async fn front_run_garbage_cannot_censor_a_later_genuine_marker() {
        // THE CENSORSHIP REGRESSION (adversarial-review HIGH, 2026-07-16):
        // admission is byte-format-only, so a sore loser who knows
        // (gameId, realWinner, potTxid, settleTxid) can publish a
        // well-formed marker naming the REAL winner with GARBAGE sigs for
        // one OP_RETURN fee. Under (gameId, winner)-keyed first-marker-wins
        // storage that row would permanently occupy the slot and the
        // winner's later genuine countersigned marker would be silently
        // dropped — the legitimately-won game shows NOTHING forever
        // (clients verify sigs, so the garbage row fails their verify).
        // Outpoint keying is the fix: BOTH rows must come back and the
        // client counts the one that verifies.
        let (svc, storage) = make_service_with_storage();
        let game = [0x11u8; 32];

        // The attacker's front-run: same (gameId, winner, loser, pot,
        // settle) but garbage sig bytes — well-formed lengths, so the
        // byte-format-only topic manager admits it.
        let garbage = super::super::tests::marker_script(
            &game,
            &golden_winner(),
            &golden_loser(),
            &golden_pot_txid(),
            &golden_settle_txid(),
            &vec![0x30u8; 71], // garbage "winnerSig"
            &vec![0x30u8; 70], // garbage "loserSig" — fakes "confirmed"
        );
        svc.output_admitted_by_topic(&admit("txGARBAGE", 0, garbage))
            .await
            .unwrap();

        // The real winner's genuine countersigned marker lands later
        // (the 45s countersign wait hands the attacker the race).
        let genuine = golden_marker(&game, &golden_loser_sig());
        svc.output_admitted_by_topic(&admit("txGENUINE", 0, genuine))
            .await
            .unwrap();

        // BOTH rows are kept and returned — the genuine marker was NOT
        // censored.
        assert_eq!(storage.record_count(), 2);
        let arr = results_for(&svc, &golden_winner_hex(), None).await;
        let arr = arr.as_array().unwrap();
        assert_eq!(arr.len(), 2, "garbage AND genuine rows coexist");
        assert_eq!(arr[0]["txid"], "txGENUINE", "newest first");
        assert_eq!(arr[0]["winnerSigHex"], hex::encode(golden_winner_sig()));
        assert_eq!(arr[0]["loserSigHex"], hex::encode(golden_loser_sig()));
        assert_eq!(arr[1]["txid"], "txGARBAGE");
        assert_eq!(arr[1]["winnerSigHex"], hex::encode(vec![0x30u8; 71]));
        // Bytes back verbatim for both — the CLIENT's sig verify is what
        // separates them.
    }

    // ── Admission filters ────────────────────────────────────────────────

    #[tokio::test]
    async fn ignores_non_tm_result_topic() {
        let (svc, storage) = make_service_with_storage();
        let mut payload = admit("tx1", 0, golden_marker(&[0x11u8; 32], &golden_loser_sig()));
        if let OutputAdmittedByTopic::LockingScript { ref mut topic, .. } = payload {
            *topic = "tm_collected".into();
        }
        svc.output_admitted_by_topic(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 0);
    }

    #[tokio::test]
    async fn ignores_non_marker_script() {
        let (svc, storage) = make_service_with_storage();
        // A P2PKH — not a result marker.
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
            topic: "tm_result".into(),
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
            golden_marker(&[0x11u8; 32], &golden_loser_sig()),
        ))
        .await
        .unwrap();
        assert_eq!(storage.record_count(), 1);

        let spent = OutputSpent::None {
            txid: "tx1".into(),
            output_index: 0,
            topic: "tm_result".into(),
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
            golden_marker(&[0xABu8; 32], &golden_loser_sig()),
        ))
        .await
        .unwrap();

        // Uppercase query identity still matches the lowercase stored
        // value, and the answer echoes the normalized lowercase forms.
        let arr = results_for(&svc, &golden_winner_hex().to_uppercase(), None).await;
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
            "ls_collected",
            serde_json::json!({"type": "recentResults"}),
        );
        assert!(svc.lookup(&q).await.is_err());
    }

    #[tokio::test]
    async fn lookup_invalid_query_errors() {
        let svc = make_service();
        for bad in [
            serde_json::json!({"type": "unknownQuery"}),
            serde_json::json!("resultsFor"),
            serde_json::json!(42),
            // missing identity
            serde_json::json!({"type": "resultsFor"}),
            // identity not hex / wrong length
            serde_json::json!({"type": "resultsFor", "identity": "zz".repeat(33)}),
            serde_json::json!({"type": "resultsFor", "identity": "02a1"}),
            // limit of the wrong type
            serde_json::json!({"type": "recentResults", "limit": "fifty"}),
        ] {
            let q = LookupQuestion::new("ls_result", bad.clone());
            assert!(svc.lookup(&q).await.is_err(), "expected error for {bad}");
        }
    }
}
