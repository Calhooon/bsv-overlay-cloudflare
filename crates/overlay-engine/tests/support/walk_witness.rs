//! The bodies of bsv-low #592's pins, written byte by byte (as
//! `script_door_parse.rs`): the engine's `engine_walk_budget.rs` and the
//! worker's queue replay pin (`beef_door_replay.rs`) build the same bytes.
#![allow(dead_code)]

use bsv_rs::primitives::sha256d;
use bsv_rs::script::op::*;

pub type Hash32 = [u8; 32];

pub fn varint(n: u64) -> Vec<u8> {
    match n {
        0..=0xfc => vec![n as u8],
        0xfd..=0xffff => [&[0xfd][..], &(n as u16).to_le_bytes()].concat(),
        0x1_0000..=0xffff_ffff => [&[0xfe][..], &(n as u32).to_le_bytes()].concat(),
        _ => [&[0xff][..], &n.to_le_bytes()].concat(),
    }
}

/// A version 2 transaction of one input (`prev`:0, `unlock`) and one output.
pub fn one_in_one_out(prev: &Hash32, unlock: &[u8], sats: u64, lock: &[u8]) -> Vec<u8> {
    let mut raw = 2u32.to_le_bytes().to_vec();
    raw.push(1);
    raw.extend_from_slice(prev);
    raw.extend_from_slice(&0u32.to_le_bytes());
    raw.extend(varint(unlock.len() as u64));
    raw.extend_from_slice(unlock);
    raw.extend_from_slice(&u32::MAX.to_le_bytes());
    raw.push(1);
    raw.extend_from_slice(&sats.to_le_bytes());
    raw.extend(varint(lock.len() as u64));
    raw.extend_from_slice(lock);
    raw.extend_from_slice(&0u32.to_le_bytes());
    raw
}

pub fn display(wire: &Hash32) -> String {
    let mut bytes = *wire;
    bytes.reverse();
    hex::encode(bytes)
}

/// A V1 BEEF of one BUMP proving the first transaction, then the rest
/// unproven.
pub fn beef_v1(txs: &[Vec<u8>]) -> Vec<u8> {
    let mut body = vec![0x01, 0x00, 0xbe, 0xef, 0x01];
    body.extend(varint(800_000));
    body.extend_from_slice(&[0x01, 0x01, 0x00, 0x02]);
    body.extend_from_slice(&sha256d(&txs[0]));
    body.extend(varint(txs.len() as u64));
    for (i, raw) in txs.iter().enumerate() {
        body.extend_from_slice(raw);
        body.extend_from_slice(if i == 0 { &[0x01, 0x00] } else { &[0x00] });
    }
    body
}

/// THE WITNESS (the doors delta-2 lens, E585-D12-DELTA2-M1; the shape of
/// `script_door_parse.rs`'s route pin): a small subject over an UNPROVEN
/// parent whose unlocking script is 1.9 MB of `OP_1` (push-only, as an
/// unlocking script must be), over a proven funding. Valid bytes. The BEEF
/// and its subject's txid.
pub fn witness() -> (Vec<u8>, String) {
    let funding = one_in_one_out(&[0xaa; 32], &[], 2_000, &[OP_DROP, OP_1]);
    let parent = one_in_one_out(
        &sha256d(&funding),
        &vec![OP_1; 1_900_000],
        1_000,
        &[OP_DROP, OP_1],
    );
    let subject = one_in_one_out(&sha256d(&parent), &[0x01, 0x42], 900, &[OP_1]);
    let subject_txid = display(&sha256d(&subject));
    (beef_v1(&[funding, parent, subject]), subject_txid)
}

/// `lock` on a PROVEN funding, spent by the subject with `unlock`.
pub fn on_a_proven_source(lock: &[u8], unlock: &[u8]) -> (Vec<u8>, String) {
    let funding = one_in_one_out(&[0xaa; 32], &[], 1_000, lock);
    let subject = one_in_one_out(&sha256d(&funding), unlock, 900, &[OP_1]);
    let subject_txid = display(&sha256d(&subject));
    (beef_v1(&[funding, subject]), subject_txid)
}
