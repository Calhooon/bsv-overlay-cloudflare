//! POTPARTY Topic Manager — validates LOW pot-participation markers.
//!
//! See [`super`] for the full on-wire format. One record version is
//! admitted: the `LOW/potparty/v1` `OP_RETURN` data carrier (8 pushes: tag
//! / identity / opponentIdentity / gameId / potTxid / potVout /
//! recoveryHeight / sig).
//!
//! Like `tm_result` / `tm_collected`, there is NO server-side signature
//! verification: the marker is admitted by BYTE FORMAT ONLY (plus the
//! structural `identity != opponentIdentity` rule). The `sig` it carries is
//! preserved verbatim and verified CLIENT-side if at all. The overlay is an
//! INDEX, not an authority.

use async_trait::async_trait;
use bsv_rs::transaction::Transaction;
use overlay_engine::topic_manager::{TopicManager, TopicManagerError};
use overlay_engine::types::{AdmittanceInstructions, ServiceMetadata, SubmitMode};
use tracing::{debug, warn};

use super::is_potparty_marker_script;

/// POTPARTY Topic Manager — identifies admissible LOW potparty markers.
pub struct PotpartyTopicManager;

impl PotpartyTopicManager {
    /// Create a new POTPARTY Topic Manager.
    pub fn new() -> Self {
        Self
    }

    /// Validate a single output as a LOW potparty marker: true IFF its
    /// locking script is a well-formed `LOW/potparty/v1` marker (exact tag
    /// + exact push lengths + identity != opponentIdentity). Everything
    /// else (P2PKH change, foreign OP_RETURNs, malformed tags) is simply
    /// not admitted.
    pub fn validate_potparty_output(output: &bsv_rs::transaction::TransactionOutput) -> bool {
        is_potparty_marker_script(&output.locking_script.to_binary())
    }
}

impl Default for PotpartyTopicManager {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait(?Send)]
impl TopicManager for PotpartyTopicManager {
    async fn identify_admissible_outputs(
        &self,
        beef: &[u8],
        _previous_coins: &[u8],
        _off_chain_values: Option<&[u8]>,
        _mode: SubmitMode,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        let mut outputs_to_admit = Vec::new();

        let tx = match Transaction::from_beef(beef, None) {
            Ok(tx) => tx,
            Err(e) => {
                return Err(TopicManagerError::InvalidBeef(e.to_string()));
            }
        };

        for (i, output) in tx.outputs.iter().enumerate() {
            if Self::validate_potparty_output(output) {
                debug!("POTPARTY: admitted output {i}");
                outputs_to_admit.push(i as u32);
            }
            // Not a potparty marker (change P2PKH, foreign OP_RETURN, a
            // malformed tag, …) — skip. The strict byte-format parse keeps
            // junk out of the index.
        }

        if outputs_to_admit.is_empty() {
            warn!("POTPARTY: no outputs admitted");
        }

        Ok(AdmittanceInstructions {
            outputs_to_admit,
            coins_to_retain: vec![],
            coins_removed: None,
        })
    }

    async fn get_documentation(&self) -> String {
        include_str!("../../docs/potparty_topic.md").to_string()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "POTPARTY Topic Manager".to_string(),
            description: Some(
                "Indexes LOW pot-participation markers \
                 (LOW/potparty/v1 OP_RETURN) keyed by identity for \
                 seed-only recovery."
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
pub(crate) mod tests {
    use super::super::tests::{
        golden_game_id, golden_identity, golden_marker, golden_opponent, golden_pot_txid,
        golden_recovery_height, golden_sig, golden_vout, marker_script, push_data,
    };
    use super::super::POTPARTY_TAG;
    use super::*;
    use bsv_rs::script::LockingScript;
    use bsv_rs::transaction::{Transaction as Tx, TransactionInput, TransactionOutput};

    /// A valid potparty-marker `TransactionOutput` (0-sat OP_RETURN).
    pub(crate) fn make_potparty_output(game_id: &[u8; 32]) -> TransactionOutput {
        let script = golden_marker(game_id, &golden_pot_txid(), golden_vout());
        TransactionOutput {
            satoshis: Some(0),
            locking_script: LockingScript::from_binary(&script).unwrap(),
            change: false,
        }
    }

    // ── Valid markers ─────────────────────────────────────────────────────

    #[test]
    fn valid_marker_admitted() {
        let out = make_potparty_output(&[0x42u8; 32]);
        assert!(PotpartyTopicManager::validate_potparty_output(&out));
    }

    // ── Not-a-marker (skip) ───────────────────────────────────────────────

    #[test]
    fn p2pkh_not_admitted() {
        let output = TransactionOutput {
            satoshis: Some(1000),
            locking_script: LockingScript::from_hex(
                "76a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac",
            )
            .unwrap(),
            change: false,
        };
        assert!(!PotpartyTopicManager::validate_potparty_output(&output));
    }

    #[test]
    fn foreign_op_return_not_admitted() {
        let mut s = vec![0x00, 0x6au8];
        s.extend(push_data(b"SOMETHINGELSE"));
        s.extend(push_data(&[0x11u8; 32]));
        let output = TransactionOutput {
            satoshis: Some(0),
            locking_script: LockingScript::from_binary(&s).unwrap(),
            change: false,
        };
        assert!(!PotpartyTopicManager::validate_potparty_output(&output));
    }

    #[test]
    fn self_paired_marker_not_admitted() {
        // identity == opponent — a pot is between two distinct seats.
        let script = marker_script(
            &golden_identity(),
            &golden_identity(),
            &golden_game_id(),
            &golden_pot_txid(),
            golden_vout(),
            golden_recovery_height(),
            &golden_sig(),
        );
        let output = TransactionOutput {
            satoshis: Some(0),
            locking_script: LockingScript::from_binary(&script).unwrap(),
            change: false,
        };
        assert!(!PotpartyTopicManager::validate_potparty_output(&output));
    }

    #[test]
    fn malformed_tagged_marker_not_admitted() {
        // potparty-TAGGED but a short gameId — strict lengths reject it.
        let mut s = vec![0x00, 0x6au8];
        s.extend(push_data(POTPARTY_TAG));
        s.extend(push_data(&golden_identity()));
        s.extend(push_data(&golden_opponent()));
        s.extend(push_data(&[0x11u8; 31])); // short gameId
        s.extend(push_data(&golden_pot_txid()));
        s.extend(push_data(&golden_vout().to_le_bytes()));
        s.extend(push_data(&golden_recovery_height().to_le_bytes()));
        s.extend(push_data(&golden_sig()));
        let output = TransactionOutput {
            satoshis: Some(0),
            locking_script: LockingScript::from_binary(&s).unwrap(),
            change: false,
        };
        assert!(!PotpartyTopicManager::validate_potparty_output(&output));
    }

    // ── Whole-transaction admission via BEEF ─────────────────────────────

    #[tokio::test]
    async fn identify_admissible_outputs_over_beef() {
        let marker = make_potparty_output(&golden_game_id());
        // A change-style P2PKH first — must be skipped.
        let p2pkh = TransactionOutput {
            satoshis: Some(546),
            locking_script: LockingScript::from_hex(
                "76a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac",
            )
            .unwrap(),
            change: false,
        };

        let mut tx = Tx::new();
        tx.add_input(TransactionInput::new("00".repeat(32), 0))
            .unwrap();
        tx.add_output(p2pkh).unwrap();
        tx.add_output(marker).unwrap();
        let beef = tx.to_beef(true).expect("BEEF serialization");

        let mgr = PotpartyTopicManager::new();
        let instructions = mgr
            .identify_admissible_outputs(&beef, &[], None, SubmitMode::HistoricalTxNoSpv)
            .await
            .unwrap();
        // Only the marker (index 1) is admitted.
        assert_eq!(instructions.outputs_to_admit, vec![1]);
    }

    #[tokio::test]
    async fn invalid_beef_is_an_error() {
        let mgr = PotpartyTopicManager::new();
        assert!(mgr
            .identify_admissible_outputs(&[0xde, 0xad], &[], None, SubmitMode::HistoricalTxNoSpv)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn topic_manager_trait_works() {
        let mgr = PotpartyTopicManager::new();
        let meta = mgr.get_metadata().await;
        assert_eq!(meta.name, "POTPARTY Topic Manager");
        assert!(!mgr.get_documentation().await.is_empty());
    }
}
