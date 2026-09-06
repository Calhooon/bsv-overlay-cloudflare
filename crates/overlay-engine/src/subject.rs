//! The SUBJECT of a submitted BEEF — order-independent (loop-2 fleet
//! hardening, 2026-09-05; bsv-low divergence D5).
//!
//! The reference's `Transaction.fromBEEF` rule is `txid ?? beef.atomicTxid ??
//! lastTx`. This crate keeps that rule and adds ONE rung between the atomic
//! name and the last-tx fallback: the UNIQUE TIP — the one transaction no
//! other transaction in the BEEF spends. A wallet's BEEF that lacks one
//! grandparent source sorts the not-valid subject FIRST and a fully-sourced
//! ancestor LAST (both the ts-sdk's `toBinary()` and bsv-rs `sort_txs`), so
//! every "last tx" reader — the route's gate, this engine's admission, the
//! GASP root walk — judged a HOP instead of the JOIN: `tm_pot` admitted
//! nothing, the route answered 200, the pot never entered the index while its
//! JOIN mined. Two tips (a malformed multi-subject body) fall back to the
//! reference's sorted-last so an old client is never refused for a shape it
//! always sent. `None` only for a body with no transaction data.
use std::collections::HashSet;

use bsv_rs::transaction::Beef;

/// The subject txid (lowercase hex) by `atomic ?? unique tip ?? sorted-last`.
pub fn subject_txid_of(beef: &mut Beef) -> Option<String> {
    if let Some(atomic) = beef.atomic_txid.clone() {
        if beef.txs.iter().any(|b| b.txid().eq_ignore_ascii_case(&atomic)) {
            return Some(atomic.to_ascii_lowercase());
        }
    }
    let mut spent: HashSet<String> = HashSet::new();
    for b in &beef.txs {
        if let Some(tx) = b.tx() {
            for input in &tx.inputs {
                let src = input
                    .source_txid
                    .clone()
                    .or_else(|| input.source_transaction.as_ref().map(|t| t.id()));
                if let Some(src) = src {
                    spent.insert(src.to_ascii_lowercase());
                }
            }
        }
    }
    let tips: Vec<String> = beef
        .txs
        .iter()
        .filter(|b| b.tx().is_some())
        .map(|b| b.txid().to_ascii_lowercase())
        .filter(|t| !spent.contains(t))
        .collect();
    if tips.len() == 1 {
        return tips.into_iter().next();
    }
    beef.sort_txs();
    beef.txs
        .last()
        .filter(|b| b.tx().is_some())
        .map(|b| b.txid().to_ascii_lowercase())
}

/// The sorted-last txid — the pre-loop-2 rule, kept ONLY so callers and pins
/// can name the disagreement with [`subject_txid_of`] (a log line, never a
/// decision).
pub fn sorted_last_txid_of(beef: &Beef) -> Option<String> {
    let mut sorted = beef.clone();
    sorted.sort_txs();
    sorted.txs.last().map(|b| b.txid().to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bsv_rs::script::LockingScript;
    use bsv_rs::transaction::{Transaction, TransactionInput, TransactionOutput};

    fn stray(seed: u8) -> Transaction {
        let mut tx = Transaction::new();
        tx.inputs.push(TransactionInput {
            source_txid: Some(format!("{seed:02x}").repeat(32)),
            source_output_index: 0,
            ..Default::default()
        });
        tx.outputs
            .push(TransactionOutput::new(1_000, LockingScript::from_hex("51").unwrap()));
        tx
    }

    fn child_of(parent: &Transaction) -> Transaction {
        let mut tx = Transaction::new();
        tx.inputs.push(TransactionInput {
            source_txid: Some(parent.id()),
            source_output_index: 0,
            ..Default::default()
        });
        tx.outputs
            .push(TransactionOutput::new(900, LockingScript::from_hex("52").unwrap()));
        tx
    }

    #[test]
    fn the_unique_tip_is_the_subject_whatever_the_wire_order() {
        // parent (its own source absent → "with missing inputs") + child.
        let parent = stray(0xaa);
        let child = child_of(&parent);
        for order in [vec![parent.clone(), child.clone()], vec![child.clone(), parent.clone()]] {
            let mut beef = Beef::new();
            for tx in order {
                beef.merge_transaction(tx);
            }
            let mut parsed = Beef::from_binary(&beef.to_binary()).unwrap();
            assert_eq!(subject_txid_of(&mut parsed).as_deref(), Some(child.id().as_str()));
        }
    }

    #[test]
    fn an_atomic_name_wins_and_a_stray_name_falls_to_the_tip() {
        let parent = stray(0xbb);
        let child = child_of(&parent);
        let mut beef = Beef::new();
        beef.merge_transaction(parent.clone());
        beef.merge_transaction(child.clone());
        let atomic = beef.to_binary_atomic(&parent.id()).unwrap();
        let mut named = Beef::from_binary(&atomic).unwrap();
        assert!(named.is_atomic());
        assert_eq!(subject_txid_of(&mut named).as_deref(), Some(parent.id().as_str()), "the name wins");
        let mut stray_named = Beef::from_binary(&atomic).unwrap();
        stray_named.atomic_txid = Some("00".repeat(32));
        assert_eq!(subject_txid_of(&mut stray_named).as_deref(), Some(child.id().as_str()), "a name not in the body is ignored");
    }

    #[test]
    fn two_tips_fall_back_to_the_reference_sorted_last() {
        let (a, b) = (stray(0xcc), stray(0xdd));
        let mut two = Beef::new();
        two.merge_transaction(a);
        two.merge_transaction(b);
        let expect = sorted_last_txid_of(&two);
        assert!(expect.is_some());
        assert_eq!(subject_txid_of(&mut two), expect);
        assert!(subject_txid_of(&mut Beef::new()).is_none());
    }
}
