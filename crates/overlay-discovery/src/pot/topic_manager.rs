//! POT Topic Manager — admits LOW `Poc5TemplatePot` covenant outputs.
//!
//! See [`super`] for the design. A single record type is admitted: an
//! output whose locking script IS the pot covenant — the fixed 122-byte
//! HEAD + 10 per-game param pushes + fixed 2850-byte TAIL, recognized by
//! [`super::is_pot_covenant_script`]. Everything else (the settle's P2PKH
//! payouts, change, foreign tokens) is skipped silently.
//!
//! There is no signature to check here: recognition is purely structural
//! against the compiled covenant template. The variable middle (param
//! pushes) is not examined — any committed params are admitted, exactly as
//! the on-chain contract accepts any params.

use async_trait::async_trait;
use bsv_rs::transaction::Transaction;
use overlay_engine::topic_manager::{TopicManager, TopicManagerError};
use overlay_engine::types::{AdmittanceInstructions, ServiceMetadata, SubmitMode};
use tracing::{debug, warn};

use super::is_pot_covenant_script;

/// POT Topic Manager — identifies admissible LOW pot covenant outputs.
pub struct PotTopicManager;

impl PotTopicManager {
    /// Create a new POT Topic Manager.
    pub fn new() -> Self {
        Self
    }
}

impl Default for PotTopicManager {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait(?Send)]
impl TopicManager for PotTopicManager {
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
            if is_pot_covenant_script(&output.locking_script.to_binary()) {
                debug!("POT: admitted covenant output {i}");
                outputs_to_admit.push(i as u32);
            }
            // Non-covenant outputs (settle P2PKH payouts, change, …) are
            // skipped silently — the settle spends the pot but its own
            // outputs are ordinary P2PKH.
        }

        if outputs_to_admit.is_empty() {
            warn!("POT: no covenant outputs admitted");
        }

        Ok(AdmittanceInstructions {
            outputs_to_admit,
            coins_to_retain: vec![],
            coins_removed: None,
        })
    }

    async fn get_documentation(&self) -> String {
        include_str!("../../docs/pot_topic.md").to_string()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "POT Topic Manager".to_string(),
            description: Some(
                "Indexes LOW Poc5TemplatePot covenant pot outputs so their spend \
                 (settle / refund / sweep) is queryable as an on-chain landing proof."
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
    use super::*;
    use crate::pot::POC5_TEMPLATE_HEX;
    use bsv_rs::script::LockingScript;
    use bsv_rs::transaction::TransactionOutput;

    /// The fixed HEAD hex (before the first `<` param marker).
    fn head_hex() -> &'static str {
        &POC5_TEMPLATE_HEX[..POC5_TEMPLATE_HEX.find('<').unwrap()]
    }

    /// The fixed TAIL hex (after the last `>` param marker).
    fn tail_hex() -> &'static str {
        &POC5_TEMPLATE_HEX[POC5_TEMPLATE_HEX.rfind('>').unwrap() + 1..]
    }

    /// A covenant locking script = HEAD + `middle` (the param pushes) + TAIL.
    /// `middle` stands in for the 10 per-game param pushes; its exact bytes
    /// are irrelevant to recognition (only HEAD/TAIL are matched).
    pub(crate) fn covenant_script(middle: &[u8]) -> Vec<u8> {
        let mut s = hex::decode(head_hex()).unwrap();
        s.extend_from_slice(middle);
        s.extend(hex::decode(tail_hex()).unwrap());
        s
    }

    /// A representative 45-byte param middle (the real one is ~45 bytes of
    /// 10 pushes; exact contents don't affect recognition).
    pub(crate) fn dummy_params() -> Vec<u8> {
        vec![0xABu8; 45]
    }

    /// A covenant `TransactionOutput` (a funded pot).
    pub(crate) fn make_covenant_output(middle: &[u8]) -> TransactionOutput {
        TransactionOutput {
            satoshis: Some(2500),
            locking_script: LockingScript::from_binary(&covenant_script(middle)).unwrap(),
            change: false,
        }
    }

    // ── Recognizer: accept ───────────────────────────────────────────────

    #[test]
    fn covenant_head_params_tail_admitted() {
        assert!(is_pot_covenant_script(&covenant_script(&dummy_params())));
    }

    #[test]
    fn covenant_with_empty_middle_admitted() {
        // HEAD directly followed by TAIL (zero-length param middle) still
        // satisfies starts_with(HEAD) && ends_with(TAIL) with len == HEAD+TAIL.
        assert!(is_pot_covenant_script(&covenant_script(&[])));
    }

    #[test]
    fn covenant_with_long_middle_admitted() {
        assert!(is_pot_covenant_script(&covenant_script(&[0x11u8; 200])));
    }

    // ── Recognizer: reject ───────────────────────────────────────────────

    #[test]
    fn p2pkh_not_admitted() {
        // A standard P2PKH (a settle payout / change output) is not a covenant.
        let p2pkh = LockingScript::from_hex("76a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac")
            .unwrap()
            .to_binary();
        assert!(!is_pot_covenant_script(&p2pkh));
    }

    #[test]
    fn altered_tail_not_admitted() {
        let mut s = covenant_script(&dummy_params());
        let last = s.len() - 1;
        s[last] ^= 0xff; // corrupt the final TAIL byte
        assert!(!is_pot_covenant_script(&s));
    }

    #[test]
    fn altered_head_not_admitted() {
        let mut s = covenant_script(&dummy_params());
        s[0] ^= 0xff; // corrupt the first HEAD byte
        assert!(!is_pot_covenant_script(&s));
    }

    #[test]
    fn truncated_tail_not_admitted() {
        let mut s = covenant_script(&dummy_params());
        s.truncate(s.len() - 1); // drop the last TAIL byte
        assert!(!is_pot_covenant_script(&s));
    }

    #[test]
    fn head_only_not_admitted() {
        // HEAD with no TAIL is too short and lacks the ending.
        let head = hex::decode(head_hex()).unwrap();
        assert!(!is_pot_covenant_script(&head));
    }

    #[test]
    fn tail_only_not_admitted() {
        let tail = hex::decode(tail_hex()).unwrap();
        assert!(!is_pot_covenant_script(&tail));
    }

    #[test]
    fn empty_script_not_admitted() {
        assert!(!is_pot_covenant_script(&[]));
    }

    // ── Whole-transaction admission via BEEF ─────────────────────────────

    #[tokio::test]
    async fn identify_admissible_outputs_over_beef() {
        use bsv_rs::transaction::{Transaction as Tx, TransactionInput, TransactionOutput as TxOut};

        // A P2PKH change/payout output (skipped) then the covenant pot (admitted).
        let p2pkh = TxOut {
            satoshis: Some(546),
            locking_script: LockingScript::from_hex(
                "76a9146bfd5c7fbe21529d45803dbcf0c87dd3c71efbc288ac",
            )
            .unwrap(),
            change: false,
        };
        let pot = make_covenant_output(&dummy_params());

        let mut tx = Tx::new();
        tx.add_input(TransactionInput::new("00".repeat(32), 0))
            .unwrap();
        tx.add_output(p2pkh).unwrap();
        tx.add_output(pot).unwrap();
        let beef = tx.to_beef(true).expect("BEEF serialization");

        let mgr = PotTopicManager::new();
        let instructions = mgr
            .identify_admissible_outputs(&beef, &[], None, SubmitMode::HistoricalTxNoSpv)
            .await
            .unwrap();
        // Only the covenant pot (index 1) is admitted.
        assert_eq!(instructions.outputs_to_admit, vec![1]);
    }

    #[tokio::test]
    async fn identify_admits_nothing_for_all_p2pkh() {
        use bsv_rs::transaction::{Transaction as Tx, TransactionInput, TransactionOutput as TxOut};
        let p2pkh = TxOut {
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
        let beef = tx.to_beef(true).unwrap();

        let mgr = PotTopicManager::new();
        let instructions = mgr
            .identify_admissible_outputs(&beef, &[], None, SubmitMode::HistoricalTxNoSpv)
            .await
            .unwrap();
        assert!(instructions.outputs_to_admit.is_empty());
    }

    #[tokio::test]
    async fn topic_manager_trait_works() {
        let mgr = PotTopicManager::new();
        let meta = mgr.get_metadata().await;
        assert_eq!(meta.name, "POT Topic Manager");
        assert!(!mgr.get_documentation().await.is_empty());
    }
}
