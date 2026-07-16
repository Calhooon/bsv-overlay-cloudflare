//! LOWFUND Topic Manager — admits LOW HOP (funding staging coin) outputs.
//!
//! See [`super`] for the design. The hop is an ORDINARY P2PKH to the seat's
//! derived stake key — there is nothing structural to distinguish it from
//! any other P2PKH, so this manager admits every P2PKH output of a tx that
//! is EXPLICITLY submitted under `tm_lowfund` (the LOW client submits the
//! hop-carrying funding tx at hop time, and the JOIN / hop-sweep spenders
//! under the same topic so the engine records the hop's spend). Wallet
//! change outputs get rows too — harmless extra facts about already-public
//! txs; the store's rows are HINTS the client anchors before any money
//! decision (the same public-/submit trust posture as `tm_pot`).

use async_trait::async_trait;
use bsv_rs::transaction::Transaction;
use overlay_engine::topic_manager::{TopicManager, TopicManagerError};
use overlay_engine::types::{AdmittanceInstructions, ServiceMetadata, SubmitMode};
use tracing::debug;

use super::is_p2pkh_script;

/// LOWFUND Topic Manager — identifies admissible LOW hop (P2PKH) outputs.
pub struct LowFundTopicManager;

impl LowFundTopicManager {
    /// Create a new LOWFUND Topic Manager.
    pub fn new() -> Self {
        Self
    }
}

impl Default for LowFundTopicManager {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait(?Send)]
impl TopicManager for LowFundTopicManager {
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
            if is_p2pkh_script(&output.locking_script.to_binary()) {
                debug!("LOWFUND: admitted P2PKH output {i}");
                outputs_to_admit.push(i as u32);
            }
            // Non-P2PKH outputs (the pot covenant, OP_RETURN carriers, …)
            // are skipped silently — the pot side is tm_pot's job.
        }

        Ok(AdmittanceInstructions {
            outputs_to_admit,
            coins_to_retain: vec![],
            coins_removed: None,
        })
    }

    async fn get_documentation(&self) -> String {
        "tm_lowfund — LOW hop (funding staging coin) index. Admits every \
         standard P2PKH output of an explicitly-submitted tx so the hop \
         outpoint's spend (by the JOIN or the pre-signed hop sweep) is \
         queryable from the same pot_records landing-proof store as tm_pot. \
         Rows are hints; money decisions anchor on ARC/SPV evidence."
            .to_string()
    }

    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "LOWFUND Topic Manager".to_string(),
            description: Some(
                "Indexes LOW hop (P2PKH funding staging) outputs so their spend \
                 (JOIN / hop sweep) is queryable — the last WhatsOnChain client \
                 read retires."
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
mod tests {
    use super::*;
    use crate::pot::topic_manager::tests::{covenant_script, dummy_params};
    use bsv_rs::script::LockingScript;
    use bsv_rs::transaction::{Transaction as Tx, TransactionInput, TransactionOutput};

    /// A standard 25-byte P2PKH lock with a salted pkh.
    fn p2pkh_script(salt: u8) -> Vec<u8> {
        let mut s = vec![0x76, 0xa9, 0x14];
        s.extend([salt; 20]);
        s.extend([0x88, 0xac]);
        s
    }

    /// A one-input tx carrying the given locking scripts, as BEEF bytes.
    fn beef_with_outputs(scripts: Vec<Vec<u8>>) -> Vec<u8> {
        let mut tx = Tx::new();
        tx.add_input(TransactionInput::new("00".repeat(32), 0)).unwrap();
        for s in scripts {
            tx.add_output(TransactionOutput {
                satoshis: Some(546),
                locking_script: LockingScript::from_binary(&s).unwrap(),
                change: false,
            })
            .unwrap();
        }
        tx.to_beef(true).expect("BEEF serialization")
    }

    #[tokio::test]
    async fn admits_every_p2pkh_output_and_nothing_else() {
        let mgr = LowFundTopicManager::new();
        // vout 0: P2PKH (the hop) · vout 1: pot covenant · vout 2: OP_RETURN
        // · vout 3: P2PKH (wallet change).
        let beef = beef_with_outputs(vec![
            p2pkh_script(0x11),
            covenant_script(&dummy_params()),
            vec![0x00, 0x6a, 0x04, 0xde, 0xad, 0xbe, 0xef],
            p2pkh_script(0x22),
        ]);
        let got = mgr
            .identify_admissible_outputs(&beef, &[], None, SubmitMode::CurrentTx)
            .await
            .unwrap();
        assert_eq!(got.outputs_to_admit, vec![0, 3]);
    }

    #[tokio::test]
    async fn no_p2pkh_outputs_admits_nothing() {
        let mgr = LowFundTopicManager::new();
        let beef = beef_with_outputs(vec![covenant_script(&dummy_params())]);
        let got = mgr
            .identify_admissible_outputs(&beef, &[], None, SubmitMode::CurrentTx)
            .await
            .unwrap();
        assert!(got.outputs_to_admit.is_empty());
    }

    #[tokio::test]
    async fn garbage_beef_is_a_typed_error() {
        let mgr = LowFundTopicManager::new();
        let got = mgr
            .identify_admissible_outputs(&[0x00, 0x01], &[], None, SubmitMode::CurrentTx)
            .await;
        assert!(matches!(got, Err(TopicManagerError::InvalidBeef(_))));
    }

    #[test]
    fn p2pkh_recognizer_is_exact() {
        use crate::pot::is_p2pkh_script;
        let good = p2pkh_script(0x33);
        assert!(is_p2pkh_script(&good));
        // Wrong length (24 / 26 bytes).
        assert!(!is_p2pkh_script(&good[..24]));
        let mut long = good.clone();
        long.push(0x00);
        assert!(!is_p2pkh_script(&long));
        // Wrong opcodes at each fixed position.
        for i in [0usize, 1, 2, 23, 24] {
            let mut bad = good.clone();
            bad[i] ^= 0xff;
            assert!(!is_p2pkh_script(&bad), "byte {i} must be pinned");
        }
        // The covenant is not a P2PKH.
        assert!(!is_p2pkh_script(&covenant_script(&dummy_params())));
    }
}
