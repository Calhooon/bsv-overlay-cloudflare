//! Small boundary shapes only. No deep ancestry or large-leaf BUMP.
#![allow(dead_code)]

use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{Beef, MerklePath, Transaction, TransactionInput, TransactionOutput};

fn funding(data_len: usize) -> Transaction {
    let mut script = vec![0x00, 0x6a, 0x4e]; // OP_FALSE OP_RETURN PUSHDATA4
    script.extend_from_slice(&(data_len as u32).to_le_bytes());
    script.resize(script.len() + data_len, 0x42);
    let mut tx = Transaction::new();
    let mut input = TransactionInput::new("aa".repeat(32), 0);
    input.unlocking_script = Some(UnlockingScript::new());
    tx.inputs.push(input);
    tx.outputs.push(TransactionOutput::new(1, LockingScript::from_binary(&script).unwrap()));
    tx
}

pub fn body(data_len: usize) -> (Vec<u8>, String) {
    let tx = funding(data_len);
    let id = tx.id();
    let mut beef = Beef::new();
    let bump = beef.merge_bump(MerklePath::from_coinbase_txid(&id, 800_000));
    beef.merge_raw_tx(tx.to_binary(), Some(bump));
    (beef.to_binary(), id)
}

pub fn sized_body(size: usize) -> (Vec<u8>, String) {
    let overhead = body(100_000).0.len() - 100_000;
    let result = body(size - overhead);
    assert_eq!(result.0.len(), size);
    result
}

/// A proven subject and unrelated txid-only funding references, an ordinary
/// partial BEEF shape. No input chain is built or linked.
pub fn transactions(count: usize) -> (Vec<u8>, String) {
    assert!((1..=257).contains(&count));
    let (bytes, id) = body(1);
    let mut beef = Beef::from_binary(&bytes).unwrap();
    for n in 1..count {
        beef.merge_txid_only(format!("{n:064x}"));
    }
    (beef.to_binary(), id)
}

/// Distinct blocks, each with a single-leaf proof. Never a many-leaf BUMP.
pub fn bumps(count: usize) -> (Vec<u8>, String) {
    assert!((1..=257).contains(&count));
    let (bytes, id) = body(1);
    let mut beef = Beef::from_binary(&bytes).unwrap();
    for n in 1..count {
        beef.bumps.push(MerklePath::from_coinbase_txid(&format!("{n:064x}"), 800_000 + n as u32));
    }
    (beef.to_binary(), id)
}
