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
//!
//! # A JOIN names its hops (bsv-low #791, FIX-791-C; the owner's ruling,
//! record 901 item 1)
//!
//! The client's hop post is best-effort, so a JOIN can reach this topic over
//! hops the ledger never saw: the JOIN was recorded and the hops' spend was
//! not. [`LowFundTopicManager::identify_needed_inputs`] now NAMES, for a JOIN
//! (a transaction with a pot covenant output, as `tm_pot` recognises it),
//! each input whose source body the BEEF CARRIES and whose spent output is a
//! P2PKH (the rule this manager admits by). The door then lands each named
//! carried hop first (D17 rule 2) and the JOIN's admission spends its coin.
//! The bound: the names are the JOIN's own inputs (the covenant's two seats'
//! hops), never a walk; a hop's own body is no JOIN and names nothing, so
//! nothing above the hops is named. An input whose source the BEEF does not
//! carry is never named (its lock cannot be read, and an absent named body
//! would hold the JOIN "not now" until it landed: a proven JOIN presented
//! alone names nothing and is recorded as before).

use async_trait::async_trait;
use bsv_rs::transaction::Transaction;
use overlay_engine::beef_limits;
use overlay_engine::topic_manager::{TopicManager, TopicManagerError};
use overlay_engine::types::{
    AdmittanceInstructions, Outpoint, ServiceMetadata, SubmitMode, TopicAdmittanceContext,
};
use tracing::debug;

use super::{is_p2pkh_script, is_pot_covenant_script};

/// The hop inputs of the JOIN `beef` names (its atomic subject, else its last
/// transaction, as `Transaction::from_beef(_, None)` reads it): every input
/// whose source body the BEEF carries and whose spent output is a P2PKH, in
/// input order. Nothing for a transaction with no pot covenant output, or for
/// bytes that do not parse. No source is hydrated: each is looked up by txid.
pub fn join_hop_inputs(beef: &[u8]) -> Vec<Outpoint> {
    let Ok(beef) = beef_limits::parse_beef(beef, &beef_limits::DISCOVERY_BEEF_LIMITS) else {
        return Vec::new();
    };
    let subject = match beef.atomic_txid.as_deref() {
        Some(txid) => beef.find_txid(txid),
        None => beef.txs.last(),
    };
    let Some(join) = subject.and_then(|subject| subject.tx()) else {
        return Vec::new();
    };
    if !join
        .outputs
        .iter()
        .any(|o| is_pot_covenant_script(&o.locking_script.to_binary()))
    {
        return Vec::new();
    }
    join.inputs
        .iter()
        .filter_map(|input| {
            let source = input.get_source_txid().ok()?;
            let lock = beef
                .find_txid(&source)?
                .tx()?
                .outputs
                .get(input.source_output_index as usize)?
                .locking_script
                .to_binary();
            is_p2pkh_script(&lock).then(|| Outpoint::new(source, input.source_output_index))
        })
        .collect()
}

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
    /// Admission is judged on the transaction alone: no off-chain value is read.
    fn reads_off_chain_values(&self) -> bool {
        false
    }

    async fn identify_admissible_outputs(
        &self,
        tx: &Transaction,
        _previous_coins: &[u8],
        _off_chain_values: Option<&[u8]>,
        _mode: SubmitMode,
        // Pure: this manager holds no state, so a dry run is the same work.
        _context: &TopicAdmittanceContext,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        let mut outputs_to_admit = Vec::new();

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

    /// A JOIN's hop inputs whose bodies the BEEF carries ([`join_hop_inputs`]);
    /// nothing for any other transaction.
    async fn identify_needed_inputs(
        &self,
        beef: &[u8],
        _off_chain_values: Option<&[u8]>,
    ) -> Result<Vec<Outpoint>, TopicManagerError> {
        Ok(join_hop_inputs(beef))
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
        tx.add_input(TransactionInput::new("00".repeat(32), 0))
            .unwrap();
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
            .identify_admissible_outputs(
                &Tx::from_beef(&beef, None).expect("engine-side parse"),
                &[],
                None,
                SubmitMode::CurrentTx,
                &TopicAdmittanceContext::default(),
            )
            .await
            .unwrap();
        assert_eq!(got.outputs_to_admit, vec![0, 3]);
    }

    #[tokio::test]
    async fn no_p2pkh_outputs_admits_nothing() {
        let mgr = LowFundTopicManager::new();
        let beef = beef_with_outputs(vec![covenant_script(&dummy_params())]);
        let got = mgr
            .identify_admissible_outputs(
                &Tx::from_beef(&beef, None).expect("engine-side parse"),
                &[],
                None,
                SubmitMode::CurrentTx,
                &TopicAdmittanceContext::default(),
            )
            .await
            .unwrap();
        assert!(got.outputs_to_admit.is_empty());
    }

    /// A JOIN (a covenant output) over carried sources: its P2PKH-sourced
    /// inputs are named, a non-P2PKH source and an absent one are not, and
    /// a transaction with no covenant names nothing (FIX-791-C).
    #[tokio::test]
    async fn names_a_joins_carried_p2pkh_hop_inputs_and_nothing_else() {
        let source = |scripts: Vec<Vec<u8>>| {
            let mut tx = Tx::new();
            tx.add_input(TransactionInput::new("00".repeat(32), 0))
                .unwrap();
            for s in scripts {
                tx.add_output(TransactionOutput {
                    satoshis: Some(546),
                    locking_script: LockingScript::from_binary(&s).unwrap(),
                    change: false,
                })
                .unwrap();
            }
            tx
        };
        let hop_a = source(vec![p2pkh_script(0x11)]);
        let hop_b = source(vec![vec![0x51], p2pkh_script(0x12)]);
        let spend = |tx: &Tx, vout: u32| {
            let mut input = TransactionInput::new(tx.id(), vout);
            input.source_transaction = Some(Box::new(tx.clone()));
            input
        };
        let mut join = Tx::new();
        join.inputs.push(spend(&hop_a, 0));
        join.inputs.push(spend(&hop_b, 0)); // its source output is no P2PKH
        join.inputs.push(spend(&hop_b, 1));
        join.inputs.push(TransactionInput::new("cc".repeat(32), 0)); // absent
        join.outputs.push(TransactionOutput {
            satoshis: Some(1000),
            locking_script: LockingScript::from_binary(&covenant_script(&dummy_params())).unwrap(),
            change: false,
        });
        let atomic = |tx: &Tx| {
            bsv_rs::transaction::Beef::from_binary(&tx.to_beef(true).unwrap())
                .unwrap()
                .to_binary_atomic(&tx.id())
                .unwrap()
        };
        let mgr = LowFundTopicManager::new();
        let named = mgr
            .identify_needed_inputs(&atomic(&join), None)
            .await
            .unwrap();
        assert_eq!(
            named,
            vec![Outpoint::new(hop_a.id(), 0), Outpoint::new(hop_b.id(), 1)]
        );
        // The same inputs with no covenant: no JOIN, nothing named.
        let mut plain = join.clone();
        plain.outputs[0].locking_script = LockingScript::from_binary(&p2pkh_script(0x33)).unwrap();
        plain.invalidate_caches();
        assert!(mgr
            .identify_needed_inputs(&atomic(&plain), None)
            .await
            .unwrap()
            .is_empty());
        // A hop names nothing; bytes that do not parse name nothing.
        assert!(mgr
            .identify_needed_inputs(&atomic(&hop_a), None)
            .await
            .unwrap()
            .is_empty());
        assert!(join_hop_inputs(&[0xde, 0xad]).is_empty());
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
