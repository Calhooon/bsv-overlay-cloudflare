//! RESULT Topic Manager — validates LOW hand-result markers.
//!
//! See [`super`] for the full on-wire format. A single record type is
//! admitted: the `LOW/result/v1` `OP_RETURN` data carrier (8 pushes:
//! tag / gameId / winnerIdentity / loserIdentity / potTxid / settleTxid /
//! winnerSig / loserSig-or-empty).
//!
//! Like `tm_collected`, there is NO server-side signature verification:
//! the marker is admitted by BYTE FORMAT ONLY (plus the structural
//! winner != loser rule). The signatures it carries are verified
//! CLIENT-side — both over the SAME canonical challenge, checked with the
//! 'anyone' ProtoWallet round-trip — and leaderboard clients count only
//! the claims they can verify. The overlay is an INDEX, not an authority
//! (see the module docs' security notes).

use async_trait::async_trait;
use bsv_rs::transaction::Transaction;
use overlay_engine::topic_manager::{TopicManager, TopicManagerError};
use overlay_engine::types::{AdmittanceInstructions, ServiceMetadata, SubmitMode};
use tracing::{debug, warn};

use super::is_result_marker_script;

/// RESULT Topic Manager — identifies admissible LOW result markers.
pub struct ResultTopicManager;

impl ResultTopicManager {
    /// Create a new RESULT Topic Manager.
    pub fn new() -> Self {
        Self
    }

    /// Validate a single output as a LOW result marker: true IFF its
    /// locking script is a well-formed `LOW/result/v1` marker (exact tag
    /// + exact push lengths + winner != loser). Everything else (P2PKH
    /// change, foreign OP_RETURNs, malformed tags) is simply not admitted.
    pub fn validate_result_output(output: &bsv_rs::transaction::TransactionOutput) -> bool {
        is_result_marker_script(&output.locking_script.to_binary())
    }
}

impl Default for ResultTopicManager {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait(?Send)]
impl TopicManager for ResultTopicManager {
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
            if Self::validate_result_output(output) {
                debug!("RESULT: admitted output {i}");
                outputs_to_admit.push(i as u32);
            }
            // Not a result marker (change P2PKH, foreign OP_RETURN, a
            // malformed tag, …) — skip. The strict byte-format parse keeps
            // junk out of the index.
        }

        if outputs_to_admit.is_empty() {
            warn!("RESULT: no outputs admitted");
        }

        Ok(AdmittanceInstructions {
            outputs_to_admit,
            coins_to_retain: vec![],
            coins_removed: None,
        })
    }

    async fn get_documentation(&self) -> String {
        include_str!("../../docs/result_topic.md").to_string()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "RESULT Topic Manager".to_string(),
            description: Some(
                "Indexes LOW hand-result leaderboard markers \
                 (LOW/result/v1 OP_RETURN) keyed by (gameId, winner)."
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
        golden_game_id, golden_loser_sig, golden_marker, push_data, GOLDEN_RESULT_HEX,
        GOLDEN_RESULT_UNCONFIRMED_HEX,
    };
    use super::super::RESULT_TAG;
    use super::*;
    use bsv_rs::script::LockingScript;
    use bsv_rs::transaction::{Transaction as Tx, TransactionInput, TransactionOutput};

    /// A valid result-marker `TransactionOutput` (0-sat OP_RETURN) over the
    /// golden identities. `loser_sig = &[]` builds the unconfirmed shape.
    pub(crate) fn make_result_output(game_id: &[u8; 32], loser_sig: &[u8]) -> TransactionOutput {
        let script = golden_marker(game_id, loser_sig);
        TransactionOutput {
            satoshis: Some(0),
            locking_script: LockingScript::from_binary(&script).unwrap(),
            change: false,
        }
    }

    fn golden_output(hex: &str) -> TransactionOutput {
        TransactionOutput {
            satoshis: Some(0),
            locking_script: LockingScript::from_hex(hex).unwrap(),
            change: false,
        }
    }

    // ── Valid markers ─────────────────────────────────────────────────────

    #[test]
    fn golden_vector_outputs_admitted() {
        assert!(ResultTopicManager::validate_result_output(&golden_output(
            GOLDEN_RESULT_HEX
        )));
        assert!(ResultTopicManager::validate_result_output(&golden_output(
            GOLDEN_RESULT_UNCONFIRMED_HEX
        )));
    }

    #[test]
    fn valid_marker_admitted() {
        let out = make_result_output(&[0x42u8; 32], &golden_loser_sig());
        assert!(ResultTopicManager::validate_result_output(&out));
        // The unconfirmed shape (empty loserSig) is equally admissible.
        let out = make_result_output(&[0x42u8; 32], &[]);
        assert!(ResultTopicManager::validate_result_output(&out));
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
        assert!(!ResultTopicManager::validate_result_output(&output));
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
        assert!(!ResultTopicManager::validate_result_output(&output));
    }

    #[test]
    fn malformed_tagged_marker_not_admitted() {
        // Result-TAGGED but a short gameId — strict lengths reject it.
        let mut s = vec![0x00, 0x6au8];
        s.extend(push_data(RESULT_TAG));
        s.extend(push_data(&[0x11u8; 31]));
        s.extend(push_data(&super::super::tests::golden_winner()));
        s.extend(push_data(&super::super::tests::golden_loser()));
        s.extend(push_data(&super::super::tests::golden_pot_txid()));
        s.extend(push_data(&super::super::tests::golden_settle_txid()));
        s.extend(push_data(&super::super::tests::golden_winner_sig()));
        s.extend(push_data(&golden_loser_sig()));
        let output = TransactionOutput {
            satoshis: Some(0),
            locking_script: LockingScript::from_binary(&s).unwrap(),
            change: false,
        };
        assert!(!ResultTopicManager::validate_result_output(&output));
    }

    #[test]
    fn empty_script_not_admitted() {
        let output = TransactionOutput {
            satoshis: Some(0),
            locking_script: LockingScript::from_binary(&[]).unwrap(),
            change: false,
        };
        assert!(!ResultTopicManager::validate_result_output(&output));
    }

    // ── Whole-transaction admission via BEEF ─────────────────────────────

    #[tokio::test]
    async fn identify_admissible_outputs_over_beef() {
        let marker = make_result_output(&golden_game_id(), &golden_loser_sig());
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

        let mgr = ResultTopicManager::new();
        let instructions = mgr
            .identify_admissible_outputs(&beef, &[], None, SubmitMode::HistoricalTxNoSpv)
            .await
            .unwrap();
        // Only the marker (index 1) is admitted.
        assert_eq!(instructions.outputs_to_admit, vec![1]);
    }

    #[tokio::test]
    async fn invalid_beef_is_an_error() {
        let mgr = ResultTopicManager::new();
        assert!(mgr
            .identify_admissible_outputs(&[0xde, 0xad], &[], None, SubmitMode::HistoricalTxNoSpv)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn topic_manager_trait_works() {
        let mgr = ResultTopicManager::new();
        let meta = mgr.get_metadata().await;
        assert_eq!(meta.name, "RESULT Topic Manager");
        assert!(!mgr.get_documentation().await.is_empty());
    }
}
