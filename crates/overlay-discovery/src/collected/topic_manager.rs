//! COLLECTED Topic Manager — validates LOW "already collected" markers.
//!
//! See [`super`] for the full on-wire format. A single record type is
//! admitted: the `LOW/collected/v1` `OP_RETURN` data carrier (4 pushes:
//! tag / gameId / identityKey / sig).
//!
//! Like `tm_reveal`, there is NO server-side signature verification: the
//! marker is admitted by BYTE FORMAT ONLY. The signature it carries is
//! verified CLIENT-side, by the querying device's own wallet
//! (`verifySignature` under `[1,'low collected']` / keyID = gameId /
//! counterparty = 'self') — only the identity owner can produce a marker a
//! device of that identity accepts, and the marker is a UI hint that never
//! gates money (see the module docs' security notes).

use async_trait::async_trait;
use bsv_rs::transaction::Transaction;
use overlay_engine::topic_manager::{TopicManager, TopicManagerError};
use overlay_engine::types::{AdmittanceInstructions, ServiceMetadata, SubmitMode};
use tracing::{debug, warn};

use super::is_collected_marker_script;

/// COLLECTED Topic Manager — identifies admissible LOW collected markers.
pub struct CollectedTopicManager;

impl CollectedTopicManager {
    /// Create a new COLLECTED Topic Manager.
    pub fn new() -> Self {
        Self
    }

    /// Validate a single output as a LOW collected marker: true IFF its
    /// locking script is a well-formed `LOW/collected/v1` marker (exact tag
    /// + exact push lengths). Everything else (P2PKH change, foreign
    /// OP_RETURNs, malformed tags) is simply not admitted.
    pub fn validate_collected_output(output: &bsv_rs::transaction::TransactionOutput) -> bool {
        is_collected_marker_script(&output.locking_script.to_binary())
    }
}

impl Default for CollectedTopicManager {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait(?Send)]
impl TopicManager for CollectedTopicManager {
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
            if Self::validate_collected_output(output) {
                debug!("COLLECTED: admitted output {i}");
                outputs_to_admit.push(i as u32);
            }
            // Not a collected marker (change P2PKH, foreign OP_RETURN, a
            // malformed tag, …) — skip. The strict byte-format parse keeps
            // junk out of the index.
        }

        if outputs_to_admit.is_empty() {
            warn!("COLLECTED: no outputs admitted");
        }

        Ok(AdmittanceInstructions {
            outputs_to_admit,
            coins_to_retain: vec![],
            coins_removed: None,
        })
    }

    async fn get_documentation(&self) -> String {
        include_str!("../../docs/collected_topic.md").to_string()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "COLLECTED Topic Manager".to_string(),
            description: Some(
                "Indexes LOW cross-device 'already collected' markers \
                 (LOW/collected/v1 OP_RETURN) keyed by (identityKey, gameId)."
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
        golden_game_id, golden_identity_key, golden_sig, marker_script, push_data,
        GOLDEN_MARKER_HEX,
    };
    use super::super::COLLECTED_TAG;
    use super::*;
    use bsv_rs::script::LockingScript;
    use bsv_rs::transaction::{Transaction as Tx, TransactionInput, TransactionOutput};

    /// A valid collected-marker `TransactionOutput` (0-sat OP_RETURN).
    pub(crate) fn make_collected_output(
        game_id: &[u8; 32],
        identity_key: &[u8],
        sig: &[u8],
    ) -> TransactionOutput {
        let script = marker_script(game_id, identity_key, sig);
        TransactionOutput {
            satoshis: Some(0),
            locking_script: LockingScript::from_binary(&script).unwrap(),
            change: false,
        }
    }

    fn golden_output() -> TransactionOutput {
        TransactionOutput {
            satoshis: Some(0),
            locking_script: LockingScript::from_hex(GOLDEN_MARKER_HEX).unwrap(),
            change: false,
        }
    }

    // ── Valid markers ─────────────────────────────────────────────────────

    #[test]
    fn golden_vector_output_admitted() {
        assert!(CollectedTopicManager::validate_collected_output(
            &golden_output()
        ));
    }

    #[test]
    fn valid_marker_admitted() {
        let out = make_collected_output(&[0x42u8; 32], &golden_identity_key(), &golden_sig());
        assert!(CollectedTopicManager::validate_collected_output(&out));
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
        assert!(!CollectedTopicManager::validate_collected_output(&output));
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
        assert!(!CollectedTopicManager::validate_collected_output(&output));
    }

    #[test]
    fn malformed_tagged_marker_not_admitted() {
        // Collected-TAGGED but a short gameId — strict lengths reject it.
        let mut s = vec![0x00, 0x6au8];
        s.extend(push_data(COLLECTED_TAG));
        s.extend(push_data(&[0x11u8; 31]));
        s.extend(push_data(&golden_identity_key()));
        s.extend(push_data(&golden_sig()));
        let output = TransactionOutput {
            satoshis: Some(0),
            locking_script: LockingScript::from_binary(&s).unwrap(),
            change: false,
        };
        assert!(!CollectedTopicManager::validate_collected_output(&output));
    }

    #[test]
    fn empty_script_not_admitted() {
        let output = TransactionOutput {
            satoshis: Some(0),
            locking_script: LockingScript::from_binary(&[]).unwrap(),
            change: false,
        };
        assert!(!CollectedTopicManager::validate_collected_output(&output));
    }

    // ── Whole-transaction admission via BEEF ─────────────────────────────

    #[tokio::test]
    async fn identify_admissible_outputs_over_beef() {
        let marker = make_collected_output(&golden_game_id(), &golden_identity_key(), &golden_sig());
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

        let mgr = CollectedTopicManager::new();
        let instructions = mgr
            .identify_admissible_outputs(&beef, &[], None, SubmitMode::HistoricalTxNoSpv)
            .await
            .unwrap();
        // Only the marker (index 1) is admitted.
        assert_eq!(instructions.outputs_to_admit, vec![1]);
    }

    #[tokio::test]
    async fn invalid_beef_is_an_error() {
        let mgr = CollectedTopicManager::new();
        assert!(mgr
            .identify_admissible_outputs(&[0xde, 0xad], &[], None, SubmitMode::HistoricalTxNoSpv)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn topic_manager_trait_works() {
        let mgr = CollectedTopicManager::new();
        let meta = mgr.get_metadata().await;
        assert_eq!(meta.name, "COLLECTED Topic Manager");
        assert!(!mgr.get_documentation().await.is_empty());
    }
}
