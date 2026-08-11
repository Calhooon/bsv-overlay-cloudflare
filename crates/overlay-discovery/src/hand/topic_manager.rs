//! HAND Topic Manager — validates LOW showdown-hand markers (bsv-low #382).
//!
//! See [`super`] for the full on-wire format: the `LOW/hand/v1` `OP_RETURN`
//! data carrier (6 pushes: tag / gameId / identityKey / potTxid / cards /
//! sig).
//!
//! Like `tm_collected`, there is NO server-side signature verification: the
//! marker is admitted by BYTE FORMAT ONLY (including the cards rule — five
//! distinct ordinals 0..=51). The signature it carries is verified
//! CLIENT-side, publicly, under the marker's own named identity — and the
//! history drill-down renders only the hands it can verify. The overlay is
//! an INDEX, not an authority.

use async_trait::async_trait;
use bsv_rs::transaction::Transaction;
use overlay_engine::topic_manager::{TopicManager, TopicManagerError};
use overlay_engine::types::{AdmittanceInstructions, ServiceMetadata, SubmitMode};
use tracing::{debug, warn};

use super::is_hand_marker_script;

/// HAND Topic Manager — identifies admissible LOW hand markers.
pub struct HandTopicManager;

impl HandTopicManager {
    /// Create a new HAND Topic Manager.
    pub fn new() -> Self {
        Self
    }

    /// Validate a single output as a LOW hand marker: true IFF its locking
    /// script is a well-formed `LOW/hand/v1` marker (exact tag + exact push
    /// lengths + the cards rule). Everything else (P2PKH change, foreign
    /// OP_RETURNs, malformed tags) is simply not admitted.
    pub fn validate_hand_output(output: &bsv_rs::transaction::TransactionOutput) -> bool {
        is_hand_marker_script(&output.locking_script.to_binary())
    }
}

impl Default for HandTopicManager {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait(?Send)]
impl TopicManager for HandTopicManager {
    async fn identify_admissible_outputs(
        &self,
        tx: &Transaction,
        _previous_coins: &[u8],
        _off_chain_values: Option<&[u8]>,
        _mode: SubmitMode,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        let mut outputs_to_admit = Vec::new();

        for (i, output) in tx.outputs.iter().enumerate() {
            if Self::validate_hand_output(output) {
                debug!("HAND: admitted output {i}");
                outputs_to_admit.push(i as u32);
            }
            // Not a hand marker (change P2PKH, foreign OP_RETURN, a malformed
            // tag, …) — skip. The strict byte-format parse keeps junk out of
            // the index.
        }

        if outputs_to_admit.is_empty() {
            warn!("HAND: no outputs admitted");
        }

        Ok(AdmittanceInstructions {
            outputs_to_admit,
            coins_to_retain: vec![],
            coins_removed: None,
        })
    }

    async fn get_documentation(&self) -> String {
        include_str!("../../docs/hand_topic.md").to_string()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "HAND Topic Manager".to_string(),
            description: Some(
                "Indexes LOW per-seat showdown-hand markers \
                 (LOW/hand/v1 OP_RETURN) keyed by marker outpoint."
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
        golden_cards, golden_game_id, golden_identity_key, golden_pot_txid, golden_sig,
        marker_script, push_data, GOLDEN_HAND_HEX,
    };
    use super::super::HAND_TAG;
    use super::*;
    use bsv_rs::script::LockingScript;
    use bsv_rs::transaction::{Transaction as Tx, TransactionInput, TransactionOutput};

    /// A valid hand-marker `TransactionOutput` (0-sat OP_RETURN) over the
    /// golden fields.
    pub(crate) fn make_hand_output(game_id: &[u8; 32]) -> TransactionOutput {
        let script = marker_script(
            game_id,
            &golden_identity_key(),
            &golden_pot_txid(),
            &golden_cards(),
            &golden_sig(),
        );
        TransactionOutput {
            satoshis: Some(0),
            locking_script: LockingScript::from_binary(&script).unwrap(),
            change: false,
        }
    }

    // ── Valid markers ─────────────────────────────────────────────────────

    #[test]
    fn golden_vector_output_admitted() {
        let output = TransactionOutput {
            satoshis: Some(0),
            locking_script: LockingScript::from_hex(GOLDEN_HAND_HEX).unwrap(),
            change: false,
        };
        assert!(HandTopicManager::validate_hand_output(&output));
    }

    #[test]
    fn valid_marker_admitted() {
        assert!(HandTopicManager::validate_hand_output(&make_hand_output(
            &[0x42u8; 32]
        )));
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
        assert!(!HandTopicManager::validate_hand_output(&output));
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
        assert!(!HandTopicManager::validate_hand_output(&output));
    }

    #[test]
    fn malformed_tagged_marker_not_admitted() {
        // Hand-TAGGED but a duplicate card — the cards rule rejects it.
        let mut cards = golden_cards();
        cards[1] = cards[0];
        let mut s = vec![0x00, 0x6au8];
        s.extend(push_data(HAND_TAG));
        s.extend(push_data(&golden_game_id()));
        s.extend(push_data(&golden_identity_key()));
        s.extend(push_data(&golden_pot_txid()));
        s.extend(push_data(&cards));
        s.extend(push_data(&golden_sig()));
        let output = TransactionOutput {
            satoshis: Some(0),
            locking_script: LockingScript::from_binary(&s).unwrap(),
            change: false,
        };
        assert!(!HandTopicManager::validate_hand_output(&output));
    }

    #[test]
    fn empty_script_not_admitted() {
        let output = TransactionOutput {
            satoshis: Some(0),
            locking_script: LockingScript::from_binary(&[]).unwrap(),
            change: false,
        };
        assert!(!HandTopicManager::validate_hand_output(&output));
    }

    // ── Whole-transaction admission via BEEF ─────────────────────────────

    #[tokio::test]
    async fn identify_admissible_outputs_over_beef() {
        let marker = make_hand_output(&golden_game_id());
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

        let mgr = HandTopicManager::new();
        let instructions = mgr
            .identify_admissible_outputs(
                &Tx::from_beef(&beef, None).expect("engine-side parse"),
                &[],
                None,
                SubmitMode::HistoricalTxNoSpv,
            )
            .await
            .unwrap();
        assert_eq!(instructions.outputs_to_admit, vec![1]);
    }

    #[tokio::test]
    async fn topic_manager_trait_works() {
        let mgr = HandTopicManager::new();
        let meta = mgr.get_metadata().await;
        assert_eq!(meta.name, "HAND Topic Manager");
        assert!(!mgr.get_documentation().await.is_empty());
    }
}
