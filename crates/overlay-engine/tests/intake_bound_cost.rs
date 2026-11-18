//! Lane INTAKE-BOUND: what the SDK's parse of a merkle path costs per node,
//! measured natively in the release profile, so it is re-run at every
//! bsv-rs bump (CLAUDE.md, "bsv-rs 0.8.11"). bsv-rs computes a path's root
//! when it parses it (a hash a node and a string a leaf), so the CPU of a
//! body's parse follows its BUMP nodes; the proof limb
//! (`DoorBudget::max_proof_nodes`) bounds the nodes before any parse.
//!
//! Two shapes of the default's node count (65,536): (a) ONE BUMP that wide
//! (level 0 full); (b) the same number of nodes as minimal paths of 14 nodes
//! each (a transaction and its sibling at level 0, one node at each of 12
//! levels above: a block of 8,192 transactions), 4,681 paths. Measured on the
//! lane's host (Apple silicon, macOS, `cargo +1.95.0 test --release`), the
//! best of five runs: the figures are `MEASURED_NS_PER_NODE_*` below. The pin
//! fails if a later SDK makes a node more than twice as costly. Not measured
//! on wasm32.
// bsv-stack-lean #134: test code
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Instant;

use bsv_overlay_engine::beef_limits::LINK_ANCESTRY_PROOF_NODES_MAX;
use bsv_rs::transaction::{MerklePath, MerklePathLeaf};

/// (a), ns a node, measured (see the module doc).
const MEASURED_NS_PER_NODE_WIDE: f64 = MEASURED_WIDE;
/// (b), ns a node, measured (see the module doc).
const MEASURED_NS_PER_NODE_MINIMAL: f64 = MEASURED_MINIMAL;

const MEASURED_WIDE: f64 = 3_200.0;
const MEASURED_MINIMAL: f64 = 880.0;

fn hash(i: u64, salt: u64) -> String {
    format!("{:032x}{:032x}", i, salt)
}

/// (a): one BUMP, `n` level-0 leaves (`n` a power of two).
fn wide(n: u64) -> Vec<u8> {
    let mut level = vec![MerklePathLeaf::new_txid(0, hash(0, 1))];
    for offset in 1..n {
        level.push(MerklePathLeaf::new(offset, hash(offset, 1)));
    }
    let height = 64 - (n - 1).leading_zeros() as usize;
    let mut path = vec![level];
    path.resize_with(height, Vec::new);
    MerklePath::new(900_000, path).unwrap().to_binary()
}

/// (b): one minimal path of 14 nodes for transaction `i`.
fn minimal(i: u64) -> Vec<u8> {
    let mut path = vec![vec![
        MerklePathLeaf::new_txid(0, hash(i, 2)),
        MerklePathLeaf::new(1, hash(i, 3)),
    ]];
    for level in 1..13u64 {
        path.push(vec![MerklePathLeaf::new(1, hash(i, 100 + level))]);
    }
    MerklePath::new(900_000, path).unwrap().to_binary()
}

/// The best of five: ns a node of parsing `paths` (each `MerklePath::from_binary`).
fn ns_per_node(paths: &[Vec<u8>], nodes: u64) -> f64 {
    let mut best = f64::MAX;
    for _ in 0..5 {
        let at = Instant::now();
        for p in paths {
            std::hint::black_box(MerklePath::from_binary(std::hint::black_box(p)).unwrap());
        }
        best = best.min(at.elapsed().as_nanos() as f64 / nodes as f64);
    }
    best
}

#[test]
#[cfg_attr(
    debug_assertions,
    ignore = "release only: cargo test --release --features memory-storage --test intake_bound_cost -- --nocapture"
)]
fn ib_cost_the_sdks_parse_costs_per_node_at_most_twice_the_measured() {
    let n = LINK_ANCESTRY_PROOF_NODES_MAX;
    let a = vec![wide(n)];
    let a_ns = ns_per_node(&a, n);
    let count = n / 14;
    let b: Vec<Vec<u8>> = (0..count).map(minimal).collect();
    let b_ns = ns_per_node(&b, count * 14);
    println!(
        "ib_cost: (a) one BUMP of {n} nodes: {a_ns:.1} ns a node, {:.2} ms the path; \
         (b) {count} minimal paths of 14 nodes: {b_ns:.1} ns a node, {:.2} ms in all",
        a_ns * n as f64 / 1e6,
        b_ns * (count * 14) as f64 / 1e6
    );
    // One parse of a BODY at the bound, as every door makes it
    // (`beef_limits::parse_beef`: the stream's pass, then the hydrated
    // `Beef`), for a proven transaction whose own BUMP is the default wide.
    let mut tx = bsv_rs::transaction::Transaction::new();
    tx.inputs.push(bsv_rs::transaction::TransactionInput {
        source_txid: Some("aa".repeat(32)),
        source_output_index: 0,
        unlocking_script: Some(bsv_rs::script::UnlockingScript::from_script(
            bsv_rs::script::Script::new(),
        )),
        ..Default::default()
    });
    tx.outputs.push(bsv_rs::transaction::TransactionOutput::new(
        1,
        bsv_rs::script::LockingScript::from_script(
            bsv_rs::script::Script::from_binary(&[0x51]).unwrap(),
        ),
    ));
    let mut level = vec![MerklePathLeaf::new_txid(0, tx.id())];
    for offset in 1..n {
        level.push(MerklePathLeaf::new(offset, hash(offset, 1)));
    }
    let mut path = vec![level];
    path.resize_with(64 - (n - 1).leading_zeros() as usize, Vec::new);
    let mut beef = bsv_rs::transaction::Beef::new();
    let bump = beef.merge_bump(MerklePath::new(900_000, path).unwrap());
    beef.merge_raw_tx(tx.to_binary(), Some(bump));
    let body = beef.to_binary();
    let mut best = f64::MAX;
    for _ in 0..5 {
        let at = Instant::now();
        std::hint::black_box(
            bsv_overlay_engine::beef_limits::parse_beef(
                &body,
                &bsv_overlay_engine::beef_limits::ENGINE_BEEF_LIMITS,
            )
            .unwrap(),
        );
        best = best.min(at.elapsed().as_secs_f64() * 1e3);
    }
    println!(
        "ib_cost: one parse_beef of a body at the bound ({} bytes, {n} nodes): {best:.2} ms",
        body.len()
    );
    if MEASURED_NS_PER_NODE_WIDE > 0.0 {
        assert!(
            a_ns <= 2.0 * MEASURED_NS_PER_NODE_WIDE,
            "(a) {a_ns:.1} ns a node, past twice the measured {MEASURED_NS_PER_NODE_WIDE}"
        );
        assert!(
            b_ns <= 2.0 * MEASURED_NS_PER_NODE_MINIMAL,
            "(b) {b_ns:.1} ns a node, past twice the measured {MEASURED_NS_PER_NODE_MINIMAL}"
        );
    }
}
