//! HAND Lookup Service — indexes and queries LOW showdown-hand markers
//! (bsv-low #382).
//!
//! When outputs are admitted to `tm_hand`, this service parses the
//! `LOW/hand/v1` marker and stores one row per marker OUTPOINT
//! `(txid, outputIndex)` via [`HandStorage`]. LOW clients ask
//! `handsForGames` from the history drill-down; the answer is a freeform
//! JSON array, ONE ENTRY PER STORED ROW, carrying the stored fields +
//! `sigHex` so the CLIENT can verify each row publicly under its named
//! identity — the overlay never verifies anything.
//!
//! **A gameId may appear any number of times.** Both seats publish for one
//! game (the whole point), and admission is byte-format-only so junk rows
//! coexist with genuine ones — the reader's per-row sig verify separates
//! them (the collected/result-v2 discipline; no per-pair slot exists to
//! squat).
//!
//! Fail-safe shape: a gameId with no stored marker contributes NO entries —
//! silence, and the history row simply omits the hand (never a guess).
//!
//! Permanence: a revealed hand is a permanent fact and the admitted output
//! is a provably-unspendable OP_RETURN. `spend_notification_mode` is
//! [`SpendNotificationMode::None`], and `output_spent` / `output_evicted`
//! are deliberate no-ops (mirrors `ls_collected` / `ls_reveal`).

use async_trait::async_trait;
use overlay_engine::lookup_service::{LookupService, LookupServiceError};
use overlay_engine::types::*;
use std::rc::Rc;
use tracing::debug;

use super::parse_hand_marker;
use super::storage::{HandQuery, HandRecord, HandStorage};

/// Hard cap on gameIds per `handsForGames` query (the ls_collected #291
/// posture). Over-cap requests are refused with an explicit `InvalidQuery` —
/// never a silent truncation (an absent entry reads as "no hand recorded").
pub const MAX_HAND_GAME_IDS: usize = 100;

/// HAND Lookup Service — indexes markers and answers `handsForGames`.
pub struct HandLookupService {
    storage: Rc<dyn HandStorage>,
}

impl HandLookupService {
    /// Create a new HAND lookup service backed by the given storage.
    pub fn new(storage: Rc<dyn HandStorage>) -> Self {
        Self { storage }
    }
}

#[async_trait(?Send)]
impl LookupService for HandLookupService {
    fn admission_mode(&self) -> AdmissionMode {
        AdmissionMode::LockingScript
    }

    fn spend_notification_mode(&self) -> SpendNotificationMode {
        // A revealed hand is permanent; the OP_RETURN can't be spent anyway.
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

        // Only index tm_hand outputs.
        if topic != "tm_hand" {
            return Ok(());
        }

        // The topic manager already validated the marker; re-parse to recover
        // the fields for the index (defensive — the TM should never admit
        // anything this can't parse).
        let Some(marker) = parse_hand_marker(locking_script) else {
            debug!("HAND: admitted output is not a parseable marker — skipped");
            return Ok(());
        };

        let record = HandRecord {
            game_id: hex::encode(marker.game_id),
            identity: hex::encode(&marker.identity_key),
            pot_txid: hex::encode(marker.pot_txid),
            cards_hex: hex::encode(marker.cards),
            txid: txid.to_string(),
            output_index,
            sig_hex: Some(hex::encode(&marker.sig)),
        };

        // Keyed on the OUTPOINT: a replay of the same output is a harmless
        // no-op; rows for one (gameId, identity) from different txs all
        // coexist — no slot to squat (the #327 S8 lesson applied from birth).
        self.storage
            .store_record(&record)
            .await
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;

        Ok(())
    }

    async fn output_spent(&self, _payload: &OutputSpent) -> Result<(), LookupServiceError> {
        // No-op: a revealed hand is a PERMANENT fact (see the module doc).
        Ok(())
    }

    async fn output_evicted(
        &self,
        _txid: &str,
        _output_index: u32,
    ) -> Result<(), LookupServiceError> {
        // No-op: hand records are never evicted (permanence — above).
        Ok(())
    }

    async fn lookup(&self, question: &LookupQuestion) -> Result<LookupResult, LookupServiceError> {
        if question.service != "ls_hand" {
            return Err(LookupServiceError::Unsupported(format!(
                "Expected ls_hand, got {}",
                question.service
            )));
        }

        let query: HandQuery = serde_json::from_value(question.query.clone())
            .map_err(|e| LookupServiceError::InvalidQuery(e.to_string()))?;

        let HandQuery::HandsForGames { game_ids } = query;

        if game_ids.len() > MAX_HAND_GAME_IDS {
            return Err(LookupServiceError::InvalidQuery(format!(
                "at most {MAX_HAND_GAME_IDS} gameIds per query (got {}) — split the request",
                game_ids.len()
            )));
        }

        let keys = game_ids
            .iter()
            .map(|g| normalize_game_id(g))
            .collect::<Result<Vec<_>, _>>()?;

        // ONE batched storage call for the whole set — results aligned
        // index-for-index with the request.
        let records = self
            .storage
            .get_records(&keys)
            .await
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;

        // ONE ENTRY PER ROW; a gameId with no markers contributes nothing
        // (silence is the fail-safe — the drill-down omits the hand).
        let mut entries = Vec::new();
        for (key, rows) in keys.iter().zip(records) {
            for r in rows {
                entries.push(serde_json::json!({
                    "gameId": key,
                    "identity": r.identity,
                    "potTxid": r.pot_txid,
                    "cardsHex": r.cards_hex,
                    "txid": r.txid,
                    "outputIndex": r.output_index,
                    "sigHex": r
                        .sig_hex
                        .map(serde_json::Value::String)
                        .unwrap_or(serde_json::Value::Null),
                }));
            }
        }

        Ok(LookupResult::Answer(LookupAnswer::Freeform {
            result: serde_json::Value::Array(entries),
        }))
    }

    async fn get_documentation(&self) -> String {
        include_str!("../../docs/hand_lookup.md").to_string()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "HAND Lookup Service".to_string(),
            description: Some(
                "Answers 'which revealed hands are recorded for these games?' \
                 over LOW hand markers (gameIds → identity/potTxid/cardsHex/sigHex \
                 rows, client-verified)."
                    .to_string(),
            ),
            ..Default::default()
        }
    }
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
    use super::super::storage::MemoryHandStorage;
    use super::super::tests::{
        golden_cards, golden_game_id, golden_identity_key, golden_pot_txid, golden_sig,
        marker_script,
    };
    use super::*;

    fn make_service_with_storage() -> (HandLookupService, Rc<MemoryHandStorage>) {
        let storage = Rc::new(MemoryHandStorage::new());
        let svc = HandLookupService::new(storage.clone());
        (svc, storage)
    }

    fn admitted(script: Vec<u8>, txid: &str, output_index: u32) -> OutputAdmittedByTopic {
        OutputAdmittedByTopic::LockingScript {
            txid: txid.to_string(),
            output_index,
            topic: "tm_hand".to_string(),
            locking_script: script,
            satoshis: 0,
            off_chain_values: None,
        }
    }

    fn question(game_ids: Vec<String>) -> LookupQuestion {
        LookupQuestion {
            service: "ls_hand".to_string(),
            query: serde_json::json!({"type": "handsForGames", "gameIds": game_ids}),
        }
    }

    fn answer_rows(result: LookupResult) -> Vec<serde_json::Value> {
        match result {
            LookupResult::Answer(LookupAnswer::Freeform {
                result: serde_json::Value::Array(rows),
            }) => rows,
            other => panic!("expected freeform array, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn admit_then_lookup_roundtrips() {
        let (svc, _) = make_service_with_storage();
        let script = marker_script(
            &golden_game_id(),
            &golden_identity_key(),
            &golden_pot_txid(),
            &golden_cards(),
            &golden_sig(),
        );
        svc.output_admitted_by_topic(&admitted(script, "aa".repeat(32).as_str(), 0))
            .await
            .unwrap();

        let rows = answer_rows(
            svc.lookup(&question(vec![hex::encode(golden_game_id())]))
                .await
                .unwrap(),
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["identity"], hex::encode(golden_identity_key()));
        assert_eq!(rows[0]["potTxid"], hex::encode(golden_pot_txid()));
        assert_eq!(rows[0]["cardsHex"], hex::encode(golden_cards()));
        assert!(rows[0]["sigHex"].is_string());
    }

    #[tokio::test]
    async fn absent_game_contributes_no_rows() {
        let (svc, _) = make_service_with_storage();
        let rows = answer_rows(
            svc.lookup(&question(vec!["44".repeat(32)]))
                .await
                .unwrap(),
        );
        assert!(rows.is_empty(), "silence is the fail-safe, never a guess");
    }

    #[tokio::test]
    async fn both_seats_rows_returned() {
        let (svc, _) = make_service_with_storage();
        let a = marker_script(
            &golden_game_id(),
            &golden_identity_key(),
            &golden_pot_txid(),
            &golden_cards(),
            &golden_sig(),
        );
        let mut other_identity = vec![0x03u8];
        other_identity.extend_from_slice(&[0xb2u8; 32]);
        let b = marker_script(
            &golden_game_id(),
            &other_identity,
            &golden_pot_txid(),
            &[0x01, 0x02, 0x03, 0x04, 0x05],
            &golden_sig(),
        );
        svc.output_admitted_by_topic(&admitted(a, "aa".repeat(32).as_str(), 0))
            .await
            .unwrap();
        svc.output_admitted_by_topic(&admitted(b, "bb".repeat(32).as_str(), 0))
            .await
            .unwrap();

        let rows = answer_rows(
            svc.lookup(&question(vec![hex::encode(golden_game_id())]))
                .await
                .unwrap(),
        );
        assert_eq!(rows.len(), 2, "both seats' markers coexist and both return");
    }

    #[tokio::test]
    async fn foreign_topic_not_indexed() {
        let (svc, storage) = make_service_with_storage();
        let script = marker_script(
            &golden_game_id(),
            &golden_identity_key(),
            &golden_pot_txid(),
            &golden_cards(),
            &golden_sig(),
        );
        let payload = OutputAdmittedByTopic::LockingScript {
            txid: "cc".repeat(32),
            output_index: 0,
            topic: "tm_collected".to_string(),
            locking_script: script,
            satoshis: 0,
            off_chain_values: None,
        };
        svc.output_admitted_by_topic(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 0);
    }

    #[tokio::test]
    async fn over_cap_request_is_refused_never_truncated() {
        let (svc, _) = make_service_with_storage();
        let ids: Vec<String> = (0..=MAX_HAND_GAME_IDS).map(|_| "11".repeat(32)).collect();
        let err = svc.lookup(&question(ids)).await.unwrap_err();
        assert!(matches!(err, LookupServiceError::InvalidQuery(_)));
    }

    #[tokio::test]
    async fn malformed_game_id_refused() {
        let (svc, _) = make_service_with_storage();
        let err = svc
            .lookup(&question(vec!["nothex".to_string()]))
            .await
            .unwrap_err();
        assert!(matches!(err, LookupServiceError::InvalidQuery(_)));
    }

    #[tokio::test]
    async fn wrong_service_name_unsupported() {
        let (svc, _) = make_service_with_storage();
        let q = LookupQuestion {
            service: "ls_collected".to_string(),
            query: serde_json::json!({"type": "handsForGames", "gameIds": []}),
        };
        assert!(matches!(
            svc.lookup(&q).await.unwrap_err(),
            LookupServiceError::Unsupported(_)
        ));
    }
}
