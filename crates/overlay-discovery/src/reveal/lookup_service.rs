//! REVEAL Lookup Service — indexes and queries LOW break-glass reveals.
//!
//! When outputs are admitted to `tm_reveal`, this service parses the
//! `LOW/reveal/v2` artifact and stores `(gameId, seat)` via
//! [`RevealStorage`]. The watchtower queries `byGameSeat` (its primary
//! "did the accused seat reveal?" question) or `byGameId`; results are
//! the standard output list, hydrated to BEEF by `/lookup`, from which
//! the tower recovers the raw reveal tx.
//!
//! Permanence: a reveal is a permanent on-chain fact and the admitted
//! output is a provably-unspendable OP_RETURN. `spend_notification_mode`
//! is [`SpendNotificationMode::None`], and `output_spent` /
//! `output_evicted` are deliberate no-ops — a reveal record is NEVER
//! removed (contrast `ls_low`, where a spent token is deleted).

use async_trait::async_trait;
use overlay_engine::lookup_service::{LookupService, LookupServiceError};
use overlay_engine::types::*;
use std::rc::Rc;
use tracing::debug;

use super::storage::{RevealQuery, RevealRecord, RevealStorage};
use super::topic_manager::parse_reveal_artifact_script;

/// REVEAL Lookup Service — indexes reveal artifacts and answers queries.
pub struct RevealLookupService {
    storage: Rc<dyn RevealStorage>,
}

impl RevealLookupService {
    /// Create a new REVEAL lookup service backed by the given storage.
    pub fn new(storage: Rc<dyn RevealStorage>) -> Self {
        Self { storage }
    }
}

#[async_trait(?Send)]
impl LookupService for RevealLookupService {
    fn admission_mode(&self) -> AdmissionMode {
        AdmissionMode::LockingScript
    }

    fn spend_notification_mode(&self) -> SpendNotificationMode {
        // A reveal is permanent; we never want a spend notification (the
        // OP_RETURN can't be spent anyway) to touch the index.
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

        // Only index tm_reveal outputs.
        if topic != "tm_reveal" {
            return Ok(());
        }

        // The topic manager already validated the artifact; re-parse to
        // recover (gameId, seat) for the index (defensive — the TM should
        // never admit anything this can't parse).
        let artifact = match parse_reveal_artifact_script(locking_script) {
            Ok(Some(a)) => a,
            Ok(None) | Err(_) => {
                debug!("REVEAL: admitted output is not a parseable reveal artifact — skipped");
                return Ok(());
            }
        };

        let record = RevealRecord {
            txid: txid.to_string(),
            output_index,
            game_id: hex::encode(artifact.game_id),
            seat: artifact.seat,
        };

        self.storage
            .store_record(&record)
            .await
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))?;

        Ok(())
    }

    async fn output_spent(&self, _payload: &OutputSpent) -> Result<(), LookupServiceError> {
        // No-op: a reveal is a PERMANENT fact. The admitted output is an
        // unspendable OP_RETURN, so this never fires anyway — but even if
        // it did, we must not evict the record.
        Ok(())
    }

    async fn output_evicted(
        &self,
        _txid: &str,
        _output_index: u32,
    ) -> Result<(), LookupServiceError> {
        // No-op: reveal records are never evicted (permanence — see above).
        Ok(())
    }

    async fn lookup(&self, question: &LookupQuestion) -> Result<LookupResult, LookupServiceError> {
        if question.service != "ls_reveal" {
            return Err(LookupServiceError::Unsupported(format!(
                "Expected ls_reveal, got {}",
                question.service
            )));
        }

        let query: RevealQuery = serde_json::from_value(question.query.clone())
            .map_err(|e| LookupServiceError::InvalidQuery(e.to_string()))?;

        let result = match query {
            RevealQuery::ByGameSeat { game_id, seat } => {
                let game_id = normalize_game_id(&game_id)?;
                let seat = normalize_seat(seat)?;
                self.storage.find_by_game_seat(&game_id, seat).await
            }
            RevealQuery::ByGameId { game_id } => {
                let game_id = normalize_game_id(&game_id)?;
                self.storage.find_by_game_id(&game_id).await
            }
        };

        result
            .map(LookupResult::OutputList)
            .map_err(|e| LookupServiceError::StorageError(e.to_string()))
    }

    async fn get_documentation(&self) -> String {
        include_str!("../../docs/reveal_lookup.md").to_string()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "REVEAL Lookup Service".to_string(),
            description: Some(
                "Looks up LOW break-glass reveal artifacts by (gameId, seat).".to_string(),
            ),
            ..Default::default()
        }
    }
}

/// Validate a 32-byte gameId hex param and return it lowercased (stored
/// values are lowercase `hex::encode` output).
fn normalize_game_id(value: &str) -> Result<String, LookupServiceError> {
    let lower = value.to_ascii_lowercase();
    if lower.len() != 64 || !lower.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(LookupServiceError::InvalidQuery(
            "gameId must be 64 hex chars".into(),
        ));
    }
    Ok(lower)
}

/// Validate the seat is 0 (A) or 1 (B).
fn normalize_seat(seat: u8) -> Result<u8, LookupServiceError> {
    if seat > 1 {
        return Err(LookupServiceError::InvalidQuery(
            "seat must be 0 (A) or 1 (B)".into(),
        ));
    }
    Ok(seat)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::super::storage::MemoryRevealStorage;
    use super::super::topic_manager::tests::{
        artifact_script, GOLDEN_GAME_ID_HEX, GOLDEN_REVEAL_RAW, GOLDEN_SEAT,
    };
    use super::*;

    fn make_service_with_storage() -> (RevealLookupService, Rc<MemoryRevealStorage>) {
        let storage = Rc::new(MemoryRevealStorage::new());
        let svc = RevealLookupService::new(storage.clone());
        (svc, storage)
    }

    fn make_service() -> RevealLookupService {
        make_service_with_storage().0
    }

    /// A reveal artifact locking script (binary) for the given (game, seat).
    fn reveal_script(game_id: &[u8; 32], seat: u8) -> Vec<u8> {
        artifact_script(
            game_id,
            seat,
            &[0u8, 2, 4, 6, 8],
            &[[0x11u8; 32]; 5],
            &[[0x22u8; 32]; 5],
        )
    }

    /// The golden mainnet reveal's OP_RETURN artifact locking script (binary).
    fn golden_reveal_script() -> Vec<u8> {
        let tx = bsv_rs::transaction::Transaction::from_hex(GOLDEN_REVEAL_RAW).unwrap();
        tx.outputs[0].locking_script.to_binary()
    }

    fn admit(txid: &str, output_index: u32, script: Vec<u8>) -> OutputAdmittedByTopic {
        OutputAdmittedByTopic::LockingScript {
            txid: txid.into(),
            output_index,
            topic: "tm_reveal".into(),
            satoshis: 0,
            locking_script: script,
            off_chain_values: None,
        }
    }

    // ── Trait plumbing ───────────────────────────────────────────────────

    #[tokio::test]
    async fn modes_and_metadata() {
        let svc = make_service();
        assert_eq!(svc.admission_mode(), AdmissionMode::LockingScript);
        assert_eq!(svc.spend_notification_mode(), SpendNotificationMode::None);
        let meta = svc.get_metadata().await;
        assert_eq!(meta.name, "REVEAL Lookup Service");
        assert!(!svc.get_documentation().await.is_empty());
    }

    // ── Admission + lookup (golden mainnet reveal) ───────────────────────

    #[tokio::test]
    async fn golden_reveal_admitted_and_found_by_game_seat() {
        let (svc, storage) = make_service_with_storage();
        svc.output_admitted_by_topic(&admit(
            "a0e644db698f510db0d1e50b9fec7a2d72ce328a8a1b51dfea90e6ce6cbf4c24",
            0,
            golden_reveal_script(),
        ))
        .await
        .unwrap();
        assert_eq!(storage.record_count(), 1);

        // byGameSeat with the golden (gameId, seat) → the reveal.
        let q = LookupQuestion::new(
            "ls_reveal",
            serde_json::json!({"type": "byGameSeat", "gameId": GOLDEN_GAME_ID_HEX, "seat": GOLDEN_SEAT}),
        );
        let results = svc.lookup(&q).await.unwrap().into_outputs().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].txid,
            "a0e644db698f510db0d1e50b9fec7a2d72ce328a8a1b51dfea90e6ce6cbf4c24"
        );
        assert_eq!(results[0].output_index, 0);

        // Wrong seat (A) → nothing.
        let q_a = LookupQuestion::new(
            "ls_reveal",
            serde_json::json!({"type": "byGameSeat", "gameId": GOLDEN_GAME_ID_HEX, "seat": 0}),
        );
        assert!(svc
            .lookup(&q_a)
            .await
            .unwrap()
            .into_outputs()
            .unwrap()
            .is_empty());

        // byGameId returns it too.
        let q_g = LookupQuestion::new(
            "ls_reveal",
            serde_json::json!({"type": "byGameId", "gameId": GOLDEN_GAME_ID_HEX}),
        );
        assert_eq!(
            svc.lookup(&q_g)
                .await
                .unwrap()
                .into_outputs()
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn admit_synthetic_reveal_stores_record() {
        let (svc, storage) = make_service_with_storage();
        svc.output_admitted_by_topic(&admit("tx1", 0, reveal_script(&[0x11u8; 32], 1)))
            .await
            .unwrap();
        assert_eq!(storage.record_count(), 1);

        let q = LookupQuestion::new(
            "ls_reveal",
            serde_json::json!({"type": "byGameSeat", "gameId": "11".repeat(32), "seat": 1}),
        );
        let results = svc.lookup(&q).await.unwrap().into_outputs().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx1");
    }

    #[tokio::test]
    async fn ignores_non_tm_reveal_topic() {
        let (svc, storage) = make_service_with_storage();
        let mut payload = admit("tx1", 0, reveal_script(&[0x11u8; 32], 0));
        if let OutputAdmittedByTopic::LockingScript { ref mut topic, .. } = payload {
            *topic = "tm_low".into();
        }
        svc.output_admitted_by_topic(&payload).await.unwrap();
        assert_eq!(storage.record_count(), 0);
    }

    #[tokio::test]
    async fn ignores_non_reveal_script() {
        let (svc, storage) = make_service_with_storage();
        // A P2PKH beacon-style script — not a reveal artifact.
        let p2pkh = bsv_rs::script::LockingScript::from_hex(
            "76a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac",
        )
        .unwrap()
        .to_binary();
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
            topic: "tm_reveal".into(),
            off_chain_values: None,
        };
        assert!(svc.output_admitted_by_topic(&payload).await.is_err());
    }

    // ── Permanence: spend / eviction are no-ops ──────────────────────────

    #[tokio::test]
    async fn spend_does_not_evict_reveal() {
        let (svc, storage) = make_service_with_storage();
        svc.output_admitted_by_topic(&admit("tx1", 0, reveal_script(&[0x11u8; 32], 0)))
            .await
            .unwrap();
        assert_eq!(storage.record_count(), 1);

        // Even a (hypothetical) spend notification must NOT remove the record.
        let spent = OutputSpent::None {
            txid: "tx1".into(),
            output_index: 0,
            topic: "tm_reveal".into(),
        };
        svc.output_spent(&spent).await.unwrap();
        assert_eq!(storage.record_count(), 1, "reveal must survive a spend");

        // Nor an eviction.
        svc.output_evicted("tx1", 0).await.unwrap();
        assert_eq!(storage.record_count(), 1, "reveal must survive an eviction");
    }

    // ── Query validation ─────────────────────────────────────────────────

    #[tokio::test]
    async fn lookup_wrong_service_errors() {
        let svc = make_service();
        let q = LookupQuestion::new(
            "ls_low",
            serde_json::json!({"type": "byGameId", "gameId": "11".repeat(32)}),
        );
        assert!(svc.lookup(&q).await.is_err());
    }

    #[tokio::test]
    async fn lookup_invalid_query_errors() {
        let svc = make_service();
        for bad in [
            serde_json::json!({"type": "unknownQuery"}),
            serde_json::json!("byGameId"),
            serde_json::json!(42),
            serde_json::json!({"type": "byGameId", "gameId": "zz".repeat(32)}),
            serde_json::json!({"type": "byGameId", "gameId": "ab"}),
            serde_json::json!({"type": "byGameSeat", "gameId": "11".repeat(32), "seat": 2}),
            serde_json::json!({"type": "byGameSeat", "gameId": "ab", "seat": 0}),
        ] {
            let q = LookupQuestion::new("ls_reveal", bad.clone());
            assert!(svc.lookup(&q).await.is_err(), "expected error for {bad}");
        }
    }

    #[tokio::test]
    async fn lookup_case_insensitive_game_id() {
        let (svc, _storage) = make_service_with_storage();
        svc.output_admitted_by_topic(&admit("tx1", 0, reveal_script(&[0xABu8; 32], 1)))
            .await
            .unwrap();

        // Uppercase query hex must still match the lowercase stored value.
        let q = LookupQuestion::new(
            "ls_reveal",
            serde_json::json!({"type": "byGameSeat", "gameId": "AB".repeat(32), "seat": 1}),
        );
        let results = svc.lookup(&q).await.unwrap().into_outputs().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].txid, "tx1");
    }

    #[tokio::test]
    async fn empty_when_no_records() {
        let svc = make_service();
        let q = LookupQuestion::new(
            "ls_reveal",
            serde_json::json!({"type": "byGameSeat", "gameId": "11".repeat(32), "seat": 0}),
        );
        assert!(svc
            .lookup(&q)
            .await
            .unwrap()
            .into_outputs()
            .unwrap()
            .is_empty());
    }
}
