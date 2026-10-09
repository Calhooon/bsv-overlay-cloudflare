//! Small shapes only. No deep ancestry or large-leaf BUMP: the sizes and
//! counts here cross the former bounds (P0-5f) and nothing more.
#![allow(dead_code)]

use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{
    Beef, MerklePath, MerklePathLeaf, Transaction, TransactionInput, TransactionOutput,
};

fn funding(data_len: usize) -> Transaction {
    let mut script = vec![0x00, 0x6a, 0x4e]; // OP_FALSE OP_RETURN PUSHDATA4
    script.extend_from_slice(&(data_len as u32).to_le_bytes());
    script.resize(script.len() + data_len, 0x42);
    let mut tx = Transaction::new();
    let mut input = TransactionInput::new("aa".repeat(32), 0);
    input.unlocking_script = Some(UnlockingScript::new());
    tx.inputs.push(input);
    tx.outputs.push(TransactionOutput::new(
        1,
        LockingScript::from_binary(&script).unwrap(),
    ));
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
    assert!(count >= 1);
    let (bytes, id) = body(1);
    let mut beef = Beef::from_binary(&bytes).unwrap();
    for n in 1..count {
        beef.merge_txid_only(format!("{n:064x}"));
    }
    (beef.to_binary(), id)
}

/// Distinct blocks, each with a single-leaf proof. Never a many-leaf BUMP.
pub fn bumps(count: usize) -> (Vec<u8>, String) {
    assert!(count >= 1);
    let (bytes, id) = body(1);
    let mut beef = Beef::from_binary(&bytes).unwrap();
    for n in 1..count {
        beef.bumps.push(MerklePath::from_coinbase_txid(
            &format!("{n:064x}"),
            800_000 + n as u32,
        ));
    }
    (beef.to_binary(), id)
}

/// Two valid shared proofs of exactly 4095 and 4096 bytes. At most 128 leaves
/// in any constructed BUMP, no exhaustion shape. The smaller proves a
/// 120-transaction block; the larger carries the last 120 of a 248-tx block,
/// with the preceding 128 represented by their subtree root.
pub fn proof_boundaries() -> (MerklePath, MerklePath) {
    let leaf = |offset| MerklePathLeaf::new_txid(offset, format!("{offset:064x}"));
    let mut at = vec![Vec::new(); 7];
    at[0] = (0..120).map(leaf).collect();
    at[3].push(MerklePathLeaf::new_duplicate(15));
    let at = MerklePath::new(800_000, at).unwrap();

    let mut left = vec![Vec::new(); 7];
    left[0] = (0..128).map(leaf).collect();
    let left_root = MerklePath::new(800_000, left)
        .unwrap()
        .compute_root(None)
        .unwrap();
    let mut pair = vec![leaf(246), leaf(247)];
    pair[0].offset = 0;
    pair[1].offset = 1;
    let pair_root = MerklePath::new(800_000, vec![pair])
        .unwrap()
        .compute_root(None)
        .unwrap();
    let mut over = vec![Vec::new(); 8];
    over[0] = (128..246).map(leaf).collect();
    over[1].push(MerklePathLeaf::new(123, pair_root));
    over[3].push(MerklePathLeaf::new_duplicate(31));
    over[7].push(MerklePathLeaf::new(0, left_root));
    let over = MerklePath::new(800_000, over).unwrap();
    assert_eq!(at.to_binary().len(), 4095);
    assert_eq!(over.to_binary().len(), 4096);
    (at, over)
}

/// Invalid bytes, each with the offset and the kind the door must name
/// (bsv-rs 0.4.0 `Refusal`: `invalid BEEF at byte <offset>: <kind> ...`).
/// None is over any former bound: every one is refused for its bytes.
pub fn invalid() -> Vec<(Vec<u8>, usize, &'static str)> {
    // `0200BEEF`, one BUMP at height 800,000 whose tree-height byte is 65.
    let mut tree_height_65 = vec![0x02, 0x00, 0xbe, 0xef, 0x01];
    tree_height_65.extend_from_slice(&[0xfe, 0x00, 0x35, 0x0c, 0x00]);
    tree_height_65.push(65);
    // The prefix claims 300 BUMPs and carries none.
    let claims_300_bumps = vec![0x02, 0x00, 0xbe, 0xef, 0xfd, 0x2c, 0x01];
    // A valid body and one byte after its frame.
    let (mut trailing, _) = body(1);
    let frame = trailing.len();
    trailing.push(0x00);
    // The same body, cut inside the transaction's four-byte lock time.
    let mut cut = trailing[..frame - 1].to_vec();
    cut.shrink_to_fit();
    vec![
        (vec![0xde, 0xad, 0xbe, 0xef], 0, "BadVersion"),
        (tree_height_65, 10, "TreeHeightOver64"),
        // The stream ends where the first BUMP's height varint would start.
        (claims_300_bumps, 7, "BadVarint"),
        (cut, frame - 4, "Truncated"),
        (trailing, frame, "TrailingBytes"),
    ]
}
