//! P4 slice 2 (bsv-low, 2026-09-02) — the per-transaction FACTS the app-layer
//! can establish at admission, once, from the very bytes the engine hands a
//! lookup service: the subject's serialized SIZE and, when the BEEF carries
//! every input's source transaction, its exact FEE (Σ inputs − Σ outputs).
//!
//! Display tier by contract: these ride the money views (`/results`' `money`)
//! so a player's finished hand can list every tx's size and fee — including
//! the OPPONENT's, which the player's device never held. Nothing money-gating
//! reads them. A missing ancestor is an honest `fee_sats: None`, never an
//! estimate (the client's own estimate is labelled ≈ and stays its own).
use bsv_rs::transaction::Beef;

/// What one admitted tx can prove about itself from its own BEEF.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxFacts {
    /// The subject's serialized byte length.
    pub size_bytes: u64,
    /// Σ inputs − Σ outputs, exact, when EVERY input's source tx (and the
    /// spent output's value) is in the BEEF; `None` otherwise.
    pub fee_sats: Option<u64>,
}

/// Parse an (Atomic)BEEF and read the subject `txid`'s facts. `None` when the
/// BEEF does not parse or does not carry the subject.
#[must_use]
pub fn facts_from_atomic_beef(atomic_beef: &[u8], txid: &str) -> Option<TxFacts> {
    let beef = Beef::from_binary(atomic_beef).ok()?;
    let subject = beef.find_txid(txid)?;
    let tx = subject.tx()?;
    let size_bytes = subject
        .raw_tx()
        .map(|r| r.len() as u64)
        .unwrap_or_else(|| tx.to_binary().len() as u64);
    let out_sum: u64 = tx.outputs.iter().map(|o| o.satoshis.unwrap_or(0)).sum();
    let mut in_sum: u64 = 0;
    let mut complete = !tx.inputs.is_empty();
    for input in &tx.inputs {
        let Some(src_txid) = input.source_txid.as_deref() else {
            complete = false;
            break;
        };
        let Some(src) = beef.find_txid(src_txid).and_then(|b| b.tx()) else {
            complete = false;
            break;
        };
        let Some(value) = src
            .outputs
            .get(input.source_output_index as usize)
            .and_then(|o| o.satoshis)
        else {
            complete = false;
            break;
        };
        in_sum = in_sum.saturating_add(value);
    }
    let fee_sats = if complete && in_sum >= out_sum {
        Some(in_sum - out_sum)
    } else {
        None
    };
    Some(TxFacts {
        size_bytes,
        fee_sats,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bsv_rs::script::LockingScript;
    use bsv_rs::transaction::{Transaction, TransactionInput, TransactionOutput};

    fn p2pkh_like(seed: u8) -> LockingScript {
        // any parseable script — the value is what the fee reads
        LockingScript::from_binary(&[0x76, 0xa9, 0x14, seed, 0x88, 0xac]).unwrap()
    }

    fn parent(value: u64) -> Transaction {
        let mut p = Transaction::new();
        p.add_input(TransactionInput::new("11".repeat(32), 0))
            .unwrap();
        p.add_output(TransactionOutput {
            satoshis: Some(value),
            locking_script: p2pkh_like(1),
            change: false,
        })
        .unwrap();
        p
    }

    #[test]
    fn exact_fee_and_size_when_the_beef_carries_the_parent() {
        let p = parent(1_000);
        let mut child = Transaction::new();
        child
            .add_input(TransactionInput::with_source_transaction(p.clone(), 0))
            .unwrap();
        child
            .add_output(TransactionOutput {
                satoshis: Some(900),
                locking_script: p2pkh_like(2),
                change: false,
            })
            .unwrap();
        child
            .add_output(TransactionOutput {
                satoshis: Some(0),
                locking_script: LockingScript::from_binary(&[0x00, 0x6a, 0x02, 0xaa, 0xbb])
                    .unwrap(),
                change: false,
            })
            .unwrap();
        let beef = child.to_beef(true).expect("beef");
        let txid = child.id();
        let facts = facts_from_atomic_beef(&beef, &txid).expect("subject in beef");
        assert_eq!(
            facts.fee_sats,
            Some(100),
            "1,000 in − 900 out − 0-sat marker"
        );
        assert_eq!(facts.size_bytes, child.to_binary().len() as u64);
    }

    #[test]
    fn no_parent_in_the_beef_means_size_only_never_an_estimate() {
        let mut child = Transaction::new();
        child
            .add_input(TransactionInput::new("22".repeat(32), 0))
            .unwrap();
        child
            .add_output(TransactionOutput {
                satoshis: Some(900),
                locking_script: p2pkh_like(2),
                change: false,
            })
            .unwrap();
        let beef = child.to_beef(true).expect("beef (partial allowed)");
        let facts = facts_from_atomic_beef(&beef, &child.id()).expect("subject in beef");
        assert_eq!(facts.fee_sats, None);
        assert_eq!(facts.size_bytes, child.to_binary().len() as u64);
    }

    #[test]
    fn garbage_or_a_foreign_txid_is_none() {
        assert!(facts_from_atomic_beef(&[1, 2, 3], &"ab".repeat(32)).is_none());
        let child = parent(5);
        let beef = child.to_beef(true).unwrap();
        assert!(facts_from_atomic_beef(&beef, &"ff".repeat(32)).is_none());
    }
}
