//! COLLECTED Lookup Service — indexes and queries LOW "already collected"
//! markers (bsv-low #161).
//!
//! When outputs are admitted to `tm_collected`, this service parses the
//! `LOW/collected/v1` marker and stores one row per marker OUTPOINT
//! `(txid, outputIndex)` via [`CollectedStorage`]. LOW clients ask
//! `collectedFor` during the home/History card gather; the answer is a
//! freeform, input-ordered JSON array (like `ls_pot`'s `spentStatus`), ONE
//! ENTRY PER STORED ROW, carrying the stored `txid` + `outputIndex` +
//! `sigHex` so the CLIENT can verify the signature under its own wallet —
//! the overlay never verifies it.
//!
//! **A gameId may appear more than once (bsv-low #327 S8).** Markers for one
//! `(identity, gameId)` from different txs all coexist, so a consumer must not
//! assume one entry per requested gameId nor treat the first as authoritative:
//! admission is byte-format-only, so any single row may be a stranger's.
//! Keying on the outpoint is what removes the old censorship — under the
//! superseded `(identity, gameId)` key a squatter's row permanently displaced
//! the victim's genuine marker.
//!
//! Fail-safe shape: a `(identity, gameId)` with no stored marker answers
//! exactly one entry, `{"present": false, "txid": null, "outputIndex": null,
//! "sigHex": null}` — an absent marker means "still offer Collect", never a
//! hidden card.
//!
//! Permanence: a collected marker is a permanent fact and the admitted
//! output is a provably-unspendable OP_RETURN. `spend_notification_mode`
//! is [`SpendNotificationMode::None`], and `output_spent` /
//! `output_evicted` are deliberate no-ops — a collected record is NEVER
//! removed (mirrors `ls_reveal`).

use async_trait::async_trait;
use overlay_engine::lookup_service::{LookupService, LookupServiceError};
use overlay_engine::types::*;
use std::rc::Rc;
use tracing::debug;

use super::parse_collected_marker;
use super::storage::{CollectedQuery, CollectedRecord, CollectedStorage};

/// Hard cap on gameIds per `collectedFor` query (bsv-low #291). Requests
/// over the cap are refused with an explicit `InvalidQuery` — never a silent
/// truncation (an absent entry reads as "not collected").
pub const MAX_COLLECTED_GAME_IDS: usize = 100;

/// COLLECTED Lookup Service — indexes markers and answers `collectedFor`.
pub struct CollectedLookupService {
    storage: Rc<dyn CollectedStorage>,
}

impl CollectedLookupService {
    /// Create a new COLLECTED lookup service backed by the given storage.
    pub fn new(storage: Rc<dyn CollectedStorage>) -> Self {
        Self { storage }
    }
}

#[async_trait(?Send)]
impl LookupService for CollectedLookupService {
    fn admission_mode(&self) -> AdmissionMode {
        AdmissionMode::LockingScript
    }

    fn spend_notification_mode(&self) -> SpendNotificationMode {
        // A collected marker is permanent; we never want a spend
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

        // Only index tm_collected outputs.
        if !crate::name_matches(topic, "tm_collected") {
            return Ok(());
        }

        // The topic manager already validated the marker; re-parse to
        // recover (identity, gameId, sig) for the index (defensive — the TM
        // should never admit anything this can't parse).
        let Some(marker) = parse_collected_marker(locking_script) else {
            debug!("COLLECTED: admitted output is not a parseable marker — skipped");
            return Ok(());
        };

        let record = CollectedRecord {
            identity: hex::encode(&marker.identity_key),
            game_id: hex::encode(marker.game_id),
            txid: txid.to_string(),
            output_index,
            sig_hex: Some(hex::encode(&marker.sig)),
        };

        // Keyed on the OUTPOINT: a replay of the same output is a harmless
        // no-op, while a marker for the same (identity, gameId) from another
        // tx becomes its own row — a squatter can no longer censor the
        // victim's genuine marker (#327 S8).
        self.storage
            .store_record(&record)
            .await
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;

        Ok(())
    }

    async fn output_spent(&self, _payload: &OutputSpent) -> Result<(), LookupServiceError> {
        // No-op: a collected marker is a PERMANENT fact. The admitted output
        // is an unspendable OP_RETURN, so this never fires anyway — but even
        // if it did, we must not evict the record.
        Ok(())
    }

    async fn output_evicted(
        &self,
        _txid: &str,
        _output_index: u32,
    ) -> Result<(), LookupServiceError> {
        // No-op: collected records are never evicted (permanence — above).
        Ok(())
    }

    async fn lookup(&self, question: &LookupQuestion) -> Result<LookupResult, LookupServiceError> {
        if !crate::name_matches(&question.service, "ls_collected") {
            return Err(LookupServiceError::Unsupported(format!(
                "Expected ls_collected, got {}",
                question.service
            )));
        }

        let query: CollectedQuery = serde_json::from_value(question.query.clone())
            .map_err(|e| LookupServiceError::InvalidQuery(e.to_string()))?;

        let CollectedQuery::CollectedFor { identity, game_ids } = query;
        let identity = normalize_identity(&identity)?;

        // Bound the request array (bsv-low #291): an over-cap request is an
        // EXPLICIT InvalidQuery — never a silent truncation (an absent entry
        // reads as "not collected", which could double-surface a Collect
        // card). The client batches ≤ a couple dozen gameIds per pass.
        if game_ids.len() > MAX_COLLECTED_GAME_IDS {
            return Err(LookupServiceError::InvalidQuery(format!(
                "at most {MAX_COLLECTED_GAME_IDS} gameIds per query (got {}) \
                 — split the request",
                game_ids.len()
            )));
        }

        let keys = game_ids
            .iter()
            .map(|g| normalize_game_id(g))
            .collect::<Result<Vec<_>, _>>()?;

        // ONE batched storage call for the whole set (bsv-low #289) —
        // results aligned index-for-index with the request.
        let records = self
            .storage
            .get_records(&identity, &keys)
            .await
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;

        // Build an input-ordered array. ONE ENTRY PER ROW (#327 S8): a gameId
        // with two admitted markers yields two entries, so a squatted row can
        // no longer hide the genuine one — the CLIENT verifies each `sigHex`
        // under its own identity and selects the one that checks out
        // (`app/src/lib/collected.ts` groupByKey + selectVerified, which this
        // re-key makes reachable for the first time).
        //
        // A gameId with NO markers still emits exactly one `present:false`
        // entry: the fail-safe that keeps an absent marker from ever hiding a
        // Collect card is unchanged.
        let mut entries = Vec::with_capacity(game_ids.len());
        for (key, rows) in keys.iter().zip(records) {
            if rows.is_empty() {
                entries.push(serde_json::json!({
                    "gameId": key,
                    "identity": identity,
                    "txid": serde_json::Value::Null,
                    "outputIndex": serde_json::Value::Null,
                    "sigHex": serde_json::Value::Null,
                    "present": false,
                }));
                continue;
            }
            for r in rows {
                entries.push(serde_json::json!({
                    "gameId": key,
                    "identity": identity,
                    "txid": r.txid,
                    "outputIndex": r.output_index,
                    "sigHex": r
                        .sig_hex
                        .map(serde_json::Value::String)
                        .unwrap_or(serde_json::Value::Null),
                    "present": true,
                }));
            }
        }

        Ok(LookupResult::Answer(LookupAnswer::Freeform {
            result: serde_json::Value::Array(entries),
        }))
    }

    async fn get_documentation(&self) -> String {
        include_str!("../../docs/collected_lookup.md").to_string()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "COLLECTED Lookup Service".to_string(),
            description: Some(
                "Answers 'which of these games has this identity already \
                 collected?' over LOW collected markers (identity + gameIds \
                 → present/txid/sigHex)."
                    .to_string(),
            ),
            ..Default::default()
        }
    }
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
    use super::super::storage::MemoryCollectedStorage;
    use super::super::tests::{golden_identity_key, golden_sig, marker_script, GOLDEN_MARKER_HEX};
    use super::*;

    fn make_service_with_storage() -> (CollectedLookupService, Rc<MemoryCollectedStorage>) {
        let storage = Rc::new(MemoryCollectedStorage::new());
        let svc = CollectedLookupService::new(storage.clone());
        (svc, storage)
    }

    fn make_service() -> CollectedLookupService {
        make_service_with_storage().0
    }

    fn admit(txid: &str, output_index: u32, script: Vec<u8>) -> OutputAdmittedByTopic {
        OutputAdmittedByTopic::LockingScript {
            txid: txid.into(),
            output_index,
            topic: "tm_collected".into(),
            satoshis: 0,
            locking_script: script,
            off_chain_values: None,
        }
    }

    /// Run a collectedFor lookup and return the JSON array.
    async fn collected_for(
        svc: &CollectedLookupService,
        identity: &str,
        game_ids: serde_json::Value,
    ) -> serde_json::Value {
        let q = LookupQuestion::new(
            "ls_collected",
            serde_json::json!({"type": "collectedFor", "identity": identity, "gameIds": game_ids}),
        );
        match svc.lookup(&q).await.unwrap() {
            LookupResult::Answer(LookupAnswer::Freeform { result }) => result,
            other => panic!("expected Freeform answer, got {other:?}"),
        }
    }

    fn golden_identity_hex() -> String {
        hex::encode(golden_identity_key())
    }

    // ── Trait plumbing ───────────────────────────────────────────────────

    #[tokio::test]
    async fn modes_and_metadata() {
        let svc = make_service();
        assert_eq!(svc.admission_mode(), AdmissionMode::LockingScript);
        assert_eq!(svc.spend_notification_mode(), SpendNotificationMode::None);
        let meta = svc.get_metadata().await;
        assert_eq!(meta.name, "COLLECTED Lookup Service");
        assert!(!svc.get_documentation().await.is_empty());
    }

    /// #291: an over-cap request is an EXPLICIT error, never a silent
    /// truncation (an absent entry reads as "not collected" and could
    /// double-surface a Collect card).
    #[tokio::test]
    async fn over_cap_game_ids_is_an_explicit_invalid_query() {
        let svc = make_service();
        let identity = golden_identity_hex();
        let at_cap: Vec<_> = (0..MAX_COLLECTED_GAME_IDS)
            .map(|i| serde_json::json!(format!("{i:064x}")))
            .collect();
        // Exactly at the cap: fine (every entry answered, absent ⇒ present:false).
        let arr = collected_for(&svc, &identity, serde_json::json!(at_cap)).await;
        assert_eq!(arr.as_array().unwrap().len(), MAX_COLLECTED_GAME_IDS);

        // One over: refused loudly.
        let mut over = at_cap;
        over.push(serde_json::json!("ff".repeat(32)));
        let q = LookupQuestion::new(
            "ls_collected",
            serde_json::json!({"type": "collectedFor", "identity": identity, "gameIds": over}),
        );
        let err = svc.lookup(&q).await.unwrap_err();
        assert!(
            matches!(err, LookupServiceError::InvalidQuery(_)),
            "expected InvalidQuery, got {err:?}"
        );
    }

    // ── Admission + lookup (the golden vector end-to-end) ────────────────

    #[tokio::test]
    async fn golden_marker_admitted_and_found() {
        let (svc, storage) = make_service_with_storage();
        let script = hex::decode(GOLDEN_MARKER_HEX).unwrap();
        svc.output_admitted_by_topic(&admit("markerTx1", 0, script))
            .await
            .unwrap();
        assert_eq!(storage.record_count(), 1);

        let arr = collected_for(
            &svc,
            &golden_identity_hex(),
            serde_json::json!(["11".repeat(32)]),
        )
        .await;
        let e = &arr[0];
        assert_eq!(e["gameId"], "11".repeat(32));
        assert_eq!(e["identity"], golden_identity_hex());
        assert_eq!(e["txid"], "markerTx1");
        assert_eq!(e["sigHex"], hex::encode(golden_sig()));
        assert_eq!(e["present"], true);
    }

    // ── Input ordering + the absent shape ─────────────────────────────────

    #[tokio::test]
    async fn lookup_is_input_ordered_with_absent_shape() {
        let (svc, _storage) = make_service_with_storage();
        // Markers for games 11 and 33; game 22 has none.
        svc.output_admitted_by_topic(&admit(
            "txA",
            0,
            marker_script(&[0x11u8; 32], &golden_identity_key(), &golden_sig()),
        ))
        .await
        .unwrap();
        svc.output_admitted_by_topic(&admit(
            "txC",
            0,
            marker_script(&[0x33u8; 32], &golden_identity_key(), &golden_sig()),
        ))
        .await
        .unwrap();

        let arr = collected_for(
            &svc,
            &golden_identity_hex(),
            serde_json::json!(["33".repeat(32), "22".repeat(32), "11".repeat(32)]),
        )
        .await;
        let arr = arr.as_array().unwrap();
        assert_eq!(arr.len(), 3);

        // Order preserved: 33, then the ABSENT 22, then 11.
        assert_eq!(arr[0]["gameId"], "33".repeat(32));
        assert_eq!(arr[0]["present"], true);
        assert_eq!(arr[0]["txid"], "txC");

        assert_eq!(arr[1]["gameId"], "22".repeat(32));
        assert_eq!(arr[1]["present"], false);
        assert!(arr[1]["txid"].is_null(), "absent gameId has null txid");
        assert!(arr[1]["sigHex"].is_null(), "absent gameId has null sigHex");
        assert_eq!(arr[1]["identity"], golden_identity_hex());

        assert_eq!(arr[2]["gameId"], "11".repeat(32));
        assert_eq!(arr[2]["present"], true);
        assert_eq!(arr[2]["txid"], "txA");
    }

    #[tokio::test]
    async fn foreign_identity_never_matches() {
        let (svc, _storage) = make_service_with_storage();
        svc.output_admitted_by_topic(&admit(
            "txA",
            0,
            marker_script(&[0x11u8; 32], &golden_identity_key(), &golden_sig()),
        ))
        .await
        .unwrap();

        // Another identity asking about the same game sees NOTHING — the
        // index is keyed per identity.
        let other_identity = "03".to_string() + &"b2".repeat(32);
        let arr = collected_for(&svc, &other_identity, serde_json::json!(["11".repeat(32)])).await;
        assert_eq!(arr[0]["present"], false);
        assert!(arr[0]["txid"].is_null());
    }

    #[tokio::test]
    async fn empty_game_ids_returns_empty_array() {
        let svc = make_service();
        let arr = collected_for(&svc, &golden_identity_hex(), serde_json::json!([])).await;
        assert!(arr.as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn lookup_case_insensitive_hex() {
        let (svc, _storage) = make_service_with_storage();
        svc.output_admitted_by_topic(&admit(
            "txA",
            0,
            marker_script(&[0xABu8; 32], &golden_identity_key(), &golden_sig()),
        ))
        .await
        .unwrap();

        // Uppercase query hex (identity AND gameId) still matches the
        // lowercase stored values, and the answer echoes the normalized
        // lowercase forms.
        let arr = collected_for(
            &svc,
            &golden_identity_hex().to_uppercase(),
            serde_json::json!(["AB".repeat(32)]),
        )
        .await;
        assert_eq!(arr[0]["present"], true);
        assert_eq!(arr[0]["gameId"], "ab".repeat(32));
        assert_eq!(arr[0]["identity"], golden_identity_hex());
    }

    // ── #327 S8: every marker survives, through the producer path ────────

    /// The INVERSION of the old `duplicate_marker_first_wins`.
    ///
    /// That cell asserted the DEFECT as the contract — a second marker for one
    /// `(identity, gameId)` being dropped is precisely how a squatter censored
    /// the victim's genuine marker forever. The behaviour was inherited and
    /// never questioned, and the cell blessed it (Rule 11).
    ///
    /// Driven through the REAL producer (`output_admitted_by_topic` → storage →
    /// `lookup`), not hand-fed records, so it proves the admission path itself
    /// keeps both rows (Rule 6b).
    #[tokio::test]
    async fn a_squat_cannot_censor_the_genuine_marker_through_the_producer() {
        let (svc, storage) = make_service_with_storage();
        let script = marker_script(&[0x11u8; 32], &golden_identity_key(), &golden_sig());
        // The squatter's marker naming the victim lands FIRST…
        svc.output_admitted_by_topic(&admit("txSQUAT", 0, script.clone()))
            .await
            .unwrap();
        // …the victim's genuine marker, from a DIFFERENT tx, lands second.
        svc.output_admitted_by_topic(&admit("txGENUINE", 0, script))
            .await
            .unwrap();

        assert_eq!(storage.record_count(), 2, "both markers stored");
        let arr = collected_for(
            &svc,
            &golden_identity_hex(),
            serde_json::json!(["11".repeat(32)]),
        )
        .await;
        let arr = arr.as_array().expect("freeform answer is an array");
        assert_eq!(arr.len(), 2, "the wire answer carries BOTH rows");
        let txids: Vec<&str> = arr.iter().map(|e| e["txid"].as_str().unwrap()).collect();
        assert!(txids.contains(&"txSQUAT"));
        assert!(
            txids.contains(&"txGENUINE"),
            "the genuine marker must reach the client so its sig can verify"
        );
        for e in arr {
            assert_eq!(e["present"], true);
            assert_eq!(e["outputIndex"], 0);
        }
    }

    /// A REPLAY of the same outpoint is still a no-op through the producer —
    /// the property that keeps the set from being inflated by resubmission.
    #[tokio::test]
    async fn replaying_one_outpoint_through_the_producer_is_a_noop() {
        let (svc, storage) = make_service_with_storage();
        let script = marker_script(&[0x11u8; 32], &golden_identity_key(), &golden_sig());
        svc.output_admitted_by_topic(&admit("txA", 0, script.clone()))
            .await
            .unwrap();
        svc.output_admitted_by_topic(&admit("txA", 0, script))
            .await
            .unwrap();
        assert_eq!(storage.record_count(), 1);
    }

    // ── Admission filters ────────────────────────────────────────────────

    #[tokio::test]
    async fn ignores_non_tm_collected_topic() {
        let (svc, storage) = make_service_with_storage();
        let mut payload = admit(
            "tx1",
            0,
            marker_script(&[0x11u8; 32], &golden_identity_key(), &golden_sig()),
        );
        if let OutputAdmittedByTopic::LockingScript { ref mut topic, .. } = payload {
            *topic = "tm_reveal".into();
        }
        svc.output_admitted_by_topic(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 0);
    }

    #[tokio::test]
    async fn ignores_non_marker_script() {
        let (svc, storage) = make_service_with_storage();
        // A P2PKH — not a collected marker.
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
            topic: "tm_collected".into(),
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
            marker_script(&[0x11u8; 32], &golden_identity_key(), &golden_sig()),
        ))
        .await
        .unwrap();
        assert_eq!(storage.record_count(), 1);

        let spent = OutputSpent::None {
            txid: "tx1".into(),
            output_index: 0,
            topic: "tm_collected".into(),
        };
        svc.output_spent(&spent).await.unwrap();
        assert_eq!(storage.record_count(), 1, "marker must survive a spend");

        svc.output_evicted("tx1", 0).await.unwrap();
        assert_eq!(storage.record_count(), 1, "marker must survive an eviction");
    }

    // ── Query validation ─────────────────────────────────────────────────

    #[tokio::test]
    async fn lookup_wrong_service_errors() {
        let svc = make_service();
        let q = LookupQuestion::new(
            "ls_reveal",
            serde_json::json!({"type": "collectedFor", "identity": "02".to_string() + &"a1".repeat(32), "gameIds": []}),
        );
        assert!(svc.lookup(&q).await.is_err());
    }

    #[tokio::test]
    async fn lookup_invalid_query_errors() {
        let svc = make_service();
        let good_identity = "02".to_string() + &"a1".repeat(32);
        for bad in [
            serde_json::json!({"type": "unknownQuery"}),
            serde_json::json!("collectedFor"),
            serde_json::json!(42),
            // missing gameIds
            serde_json::json!({"type": "collectedFor", "identity": good_identity}),
            // missing identity
            serde_json::json!({"type": "collectedFor", "gameIds": []}),
            // identity not hex / wrong length
            serde_json::json!({"type": "collectedFor", "identity": "zz".repeat(33), "gameIds": ["11".repeat(32)]}),
            serde_json::json!({"type": "collectedFor", "identity": "02a1", "gameIds": ["11".repeat(32)]}),
            // a bad gameId in the batch
            serde_json::json!({"type": "collectedFor", "identity": good_identity, "gameIds": ["ab"]}),
            serde_json::json!({"type": "collectedFor", "identity": good_identity, "gameIds": ["zz".repeat(32)]}),
        ] {
            let q = LookupQuestion::new("ls_collected", bad.clone());
            assert!(svc.lookup(&q).await.is_err(), "expected error for {bad}");
        }
    }
}
