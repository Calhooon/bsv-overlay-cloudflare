//! Lane INTAKE-BOUND: a body's merkle paths are bounded from the frame
//! before anything is parsed or hashed (`DoorBudget::max_proof_nodes`, the
//! PROOF limb; `stream_sizing::proof_limb`).
//!
//! The pins: the default is the link estimate's own figure; the stop holds
//! nothing; at every door of the engine a body one node past the limb is
//! stopped with nothing parsed and a body AT the limb goes on as before; the
//! limb is configuration; the shapes the existing pins link today sit under
//! the default. Most door pins run under a SMALL configured limb (a body of
//! 32 and of 33 nodes) so the at-the-limb leg parses a few dozen nodes, not
//! the default's 65,536; the default itself is pinned by a body one node past
//! it, which the stop never parses.
// bsv-stack-lean #134: test code
#![allow(unsafe_code, clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bsv_overlay_engine::beef_limits::{
    self, link_heap_estimate, LINK_ANCESTRY_PROOF_NODES_MAX, LINK_CHARGE_BUMP_LEAF,
    LINK_HEAP_MAX_BYTES,
};
use bsv_overlay_engine::builder::EngineBuilder;
use bsv_overlay_engine::engine::{
    DoorBudget, DoorLimb, Engine, EngineError, WalkBreachPolicy, WalkLimb,
};
use bsv_overlay_engine::gasp::{GASPError, GASPStorage};
use bsv_overlay_engine::gasp_overlay::{new_finalized_graph_sink, OverlayGASPStorage};
use bsv_overlay_engine::storage::memory::MemoryStorage;
use bsv_overlay_engine::stream_sizing::{path_proof_limb, proof_limb};
use bsv_overlay_engine::topic_manager::{TopicManager, TopicManagerError};
use bsv_overlay_engine::types::*;
use bsv_rs::script::{LockingScript, Script, UnlockingScript};
use bsv_rs::transaction::{
    Beef, MerklePath, MerklePathLeaf, Transaction, TransactionInput, TransactionOutput,
};

// ---------------------------------------------------------------------------
// The heap held by a call (a counting allocator, as `e585f_l3` holds a
// breached door call to a few hundred bytes).
// ---------------------------------------------------------------------------

struct Counting;
static LIVE: AtomicU64 = AtomicU64::new(0);
static PEAK: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            let now = LIVE.fetch_add(layout.size() as u64, Ordering::SeqCst) + layout.size() as u64;
            PEAK.fetch_max(now, Ordering::SeqCst);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        unsafe { System.dealloc(p, layout) };
        LIVE.fetch_sub(layout.size() as u64, Ordering::SeqCst);
    }
    unsafe fn realloc(&self, p: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let q = unsafe { System.realloc(p, layout, size) };
        if !q.is_null() {
            let now = if size >= layout.size() {
                LIVE.fetch_add((size - layout.size()) as u64, Ordering::SeqCst)
                    + (size - layout.size()) as u64
            } else {
                LIVE.fetch_sub((layout.size() - size) as u64, Ordering::SeqCst)
                    - (layout.size() - size) as u64
            };
            PEAK.fetch_max(now, Ordering::SeqCst);
        }
        q
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

/// The peak growth of the live heap while `f` runs. Tests of this binary run
/// one at a time where they measure (`HEAP` lock).
fn held_by<T>(f: impl FnOnce() -> T) -> (T, u64) {
    let base = LIVE.load(Ordering::SeqCst);
    PEAK.store(base, Ordering::SeqCst);
    let out = f();
    (out, PEAK.load(Ordering::SeqCst).saturating_sub(base))
}

static HEAP: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Every test of this binary takes the lock: the counting allocator is the
/// process's, so a test allocating beside a measured one would be counted.
fn one_at_a_time() -> std::sync::MutexGuard<'static, ()> {
    HEAP.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// What a stopped door call may hold beside the body: its error and its
/// words, nothing that follows the body's nodes.
const STOP_HELD_MAX: u64 = 64 * 1024;

// ---------------------------------------------------------------------------
// Bodies.
// ---------------------------------------------------------------------------

const TOPIC: &str = "tm_intake";

fn one() -> LockingScript {
    LockingScript::from_script(Script::from_binary(&[0x51]).unwrap())
}

fn spending(txid: &str, vout: u32) -> TransactionInput {
    TransactionInput {
        source_txid: Some(txid.into()),
        source_output_index: vout,
        unlocking_script: Some(UnlockingScript::from_script(Script::new())),
        ..Default::default()
    }
}

/// A transaction spending `spent` and paying one `OP_1` output of 1 sat.
fn spend(spent: &[(String, u32)], tag: u32) -> Transaction {
    let mut tx = Transaction::new();
    for (txid, vout) in spent {
        tx.inputs.push(spending(txid, *vout));
    }
    tx.outputs.push(TransactionOutput::new(1, one()));
    tx.lock_time = tag;
    tx
}

/// `tx` proven by a BUMP of `level0` level-0 leaves (`level0` even or 1),
/// with a duplicate leaf at every higher level whose width is odd (the
/// builder of `p0812_link_heap_trips`).
fn prove_wide(mut tx: Transaction, level0: u64) -> Transaction {
    let mut level = vec![MerklePathLeaf::new_txid(0, tx.id())];
    for offset in 1..level0 {
        level.push(MerklePathLeaf::new(offset, format!("{offset:064x}")));
    }
    let mut path = vec![level];
    let mut width = level0;
    while width > 1 {
        width = width.div_ceil(2);
        let mut row = Vec::new();
        if width > 1 && width % 2 == 1 {
            row.push(MerklePathLeaf {
                offset: width,
                hash: None,
                txid: false,
                duplicate: true,
            });
        }
        path.push(row);
    }
    if path.len() > 1 {
        path.pop();
    }
    tx.merkle_path = Some(MerklePath::new(900_000, path).unwrap());
    tx
}

fn nodes_of(tx: &Transaction) -> u64 {
    tx.merkle_path
        .as_ref()
        .map_or(0, |m| m.path.iter().map(|l| l.len() as u64).sum())
}

fn beef_of(txs: &[&Transaction]) -> Vec<u8> {
    let mut beef = Beef::new();
    for tx in txs {
        let bump = tx.merkle_path.clone().map(|m| beef.merge_bump(m));
        beef.merge_raw_tx(tx.to_binary(), bump);
    }
    beef.to_binary()
}

/// A coin with no ancestry of its own (it spends an outpoint nobody holds).
fn coin(tag: u32) -> Transaction {
    spend(&[(format!("{:064x}", u64::from(tag) + 1), 0)], tag)
}

/// THE SMALL LIMB's two bodies: an unproven subject spending A (proven by a
/// BUMP of 32 nodes) and, past the limb, B too (proven by one node).
const SMALL_LIMB: u64 = 32;

struct Bodies {
    at: (Vec<u8>, Transaction),
    past: (Vec<u8>, Transaction),
}

fn small_bodies() -> Bodies {
    let a = prove_wide(coin(1), SMALL_LIMB);
    assert_eq!(nodes_of(&a), SMALL_LIMB);
    let b = prove_wide(coin(2), 1);
    assert_eq!(nodes_of(&b), 1);
    let s_at = spend(&[(a.id(), 0)], 10);
    let s_past = spend(&[(a.id(), 0), (b.id(), 0)], 11);
    Bodies {
        at: (beef_of(&[&a, &s_at]), s_at),
        past: (beef_of(&[&a, &b, &s_past]), s_past),
    }
}

fn small_budget() -> DoorBudget {
    DoorBudget {
        max_proof_nodes: SMALL_LIMB,
        ..DoorBudget::DEFAULT
    }
}

/// A frame one node past the DEFAULT, built by hand (nothing parsed or
/// hashed to build it): a V1 BEEF whose one BUMP carries
/// `LINK_ANCESTRY_PROOF_NODES_MAX + 1` level-0 nodes. The stop never reads
/// past the node that takes the count over, so what follows is immaterial;
/// a transaction follows all the same.
fn one_past_the_default_frame(atomic: Option<[u8; 32]>) -> Vec<u8> {
    let mut out = Vec::new();
    if let Some(id) = atomic {
        out.extend(bsv_rs::transaction::ATOMIC_BEEF.to_le_bytes());
        out.extend(id);
    }
    out.extend(bsv_rs::transaction::BEEF_V1.to_le_bytes());
    out.push(1); // one BUMP
    out.extend(one_past_the_default_bump());
    let tx = coin(9);
    out.push(1);
    out.extend(tx.to_binary());
    out.push(1);
    out.push(0);
    out
}

/// One BUMP's bytes (BRC-74) of `LINK_ANCESTRY_PROOF_NODES_MAX + 1` level-0
/// nodes, by hand.
fn one_past_the_default_bump() -> Vec<u8> {
    let n = LINK_ANCESTRY_PROOF_NODES_MAX + 1;
    let mut out = Vec::with_capacity(38 * n as usize + 64);
    out.extend(bsv_rs::primitives::to_var_int(900_000));
    out.push(17); // tree height
    out.extend(bsv_rs::primitives::to_var_int(n));
    for offset in 0..n {
        out.extend(bsv_rs::primitives::to_var_int(offset));
        out.push(0);
        out.extend([7u8; 32]);
    }
    out.extend([0u8; 16]); // sixteen empty levels
    out
}

// ---------------------------------------------------------------------------
// The managers.
// ---------------------------------------------------------------------------

struct AdmitZero;

#[async_trait(?Send)]
impl TopicManager for AdmitZero {
    fn reads_off_chain_values(&self) -> bool {
        false
    }
    async fn identify_admissible_outputs(
        &self,
        _: &Transaction,
        _: &[u8],
        _: Option<&[u8]>,
        _: SubmitMode,
        _context: &TopicAdmittanceContext,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        Ok(AdmittanceInstructions {
            outputs_to_admit: vec![0],
            coins_to_retain: vec![],
            coins_removed: None,
        })
    }
    async fn get_documentation(&self) -> String {
        "admits output 0".into()
    }
    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "intake".into(),
            ..Default::default()
        }
    }
}

fn engine_under(budget: DoorBudget) -> Engine {
    EngineBuilder::new(Box::new(MemoryStorage::new()))
        .with_topic(TOPIC, Box::new(AdmitZero))
        .with_walk_budget(budget)
        .build()
}

fn tagged(beef: &[u8]) -> TaggedBEEF {
    TaggedBEEF::new(beef.to_vec(), vec![TOPIC.into()])
}

fn over_proof(e: &EngineError) -> bool {
    matches!(e, EngineError::WalkCouldNotRun(stop) if stop.limb == WalkLimb::OverProof)
}

/// Every submitting door of the engine, under `engine`: the three submits,
/// each policy, each mode.
async fn every_submit(engine: &Engine, beef: &[u8]) -> Vec<(String, Result<(), EngineError>)> {
    let mut out = Vec::new();
    for mode in [
        SubmitMode::CurrentTx,
        SubmitMode::HistoricalTx,
        SubmitMode::HistoricalTxNoSpv,
    ] {
        for policy in [WalkBreachPolicy::NotNow, WalkBreachPolicy::NetworkAccepted] {
            // A fresh engine each time: an admission must not make the next a dupe.
            let r = engine_under(engine.walk_budget())
                .submit_with_report_under(&tagged(beef), mode, policy)
                .await
                .map(|(steak, report)| {
                    assert!(report.is_durable(), "{report:?}");
                    assert_eq!(
                        steak.get(TOPIC).map(|a| a.outputs_to_admit.clone()),
                        Some(vec![0]),
                        "admitted"
                    );
                });
            out.push((format!("submit {mode:?} {policy:?}"), r));
        }
    }
    let r = engine
        .submit_validate_only(&tagged(beef), SubmitMode::CurrentTx)
        .await
        .map(|_| ());
    out.push(("submit_validate_only".into(), r));
    out
}

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(f)
}

// ---------------------------------------------------------------------------
// The pins.
// ---------------------------------------------------------------------------

/// The default is the link estimate's OWN figure, never a literal: the most
/// BUMP nodes an ancestry the estimate admits can carry. An unproven subject
/// spending one parent proven by a BUMP of `n` nodes: the estimate admits it
/// up to a few nodes under the default (the parent's own weight, charged
/// twice) and declines it AT the default.
#[test]
fn ib_the_default_is_the_link_estimates_own_figure() {
    let _g = one_at_a_time();
    assert_eq!(
        DoorBudget::DEFAULT.max_proof_nodes,
        LINK_ANCESTRY_PROOF_NODES_MAX
    );
    assert_eq!(
        LINK_ANCESTRY_PROOF_NODES_MAX,
        LINK_HEAP_MAX_BYTES / (2 * LINK_CHARGE_BUMP_LEAF)
    );
    // The parent's weight in the estimate, read off a narrow body.
    let beyond_of = |level0: u64| -> (u64, u64, Vec<u8>) {
        let p = prove_wide(coin(1), level0);
        let s = spend(&[(p.id(), 0)], 2);
        let body = beef_of(&[&p, &s]);
        let parsed = Beef::from_binary(&body).unwrap();
        (
            link_heap_estimate(&parsed, &s.id(), u64::MAX).beyond,
            nodes_of(&p),
            body,
        )
    };
    let (beyond2, nodes2, _) = beyond_of(2);
    let twice_weight = beyond2 - 2 * LINK_CHARGE_BUMP_LEAF * nodes2;
    // The most nodes the estimate admits for this shape, by its own charges.
    let admitted_max = (LINK_HEAP_MAX_BYTES - twice_weight) / (2 * LINK_CHARGE_BUMP_LEAF);
    // The widest even level 0 whose nodes (its duplicates added) the estimate admits.
    let mut level0 = admitted_max & !1;
    let (beyond, nodes, body) = loop {
        let got = beyond_of(level0);
        if got.0 <= LINK_HEAP_MAX_BYTES {
            break got;
        }
        level0 -= 2;
    };
    assert!(
        beyond <= LINK_HEAP_MAX_BYTES,
        "{nodes} nodes are admitted by the estimate: {beyond}"
    );
    println!(
        "ib derivation: the widest ancestry the estimate admits for this shape carries {nodes} \
         nodes (beyond {beyond}); the estimate's own figure {admitted_max}; the default {}",
        DoorBudget::DEFAULT.max_proof_nodes
    );
    assert!(
        proof_limb(&body, DoorBudget::DEFAULT.max_proof_nodes).is_ok(),
        "the largest ancestry the estimate admits passes the proof limb ({nodes} nodes)"
    );
    let (beyond, nodes, _) = beyond_of(LINK_ANCESTRY_PROOF_NODES_MAX);
    assert_eq!(nodes, LINK_ANCESTRY_PROOF_NODES_MAX);
    assert!(
        beyond > LINK_HEAP_MAX_BYTES,
        "the default's nodes in an ancestry are declined at the link: {beyond}"
    );
    assert!(
        DoorBudget::DEFAULT.max_proof_nodes - admitted_max <= 16,
        "the default is the estimate's figure, within the parent's own weight: default {}, \
         the estimate admits {admitted_max}",
        DoorBudget::DEFAULT.max_proof_nodes
    );
}

/// The stop holds nothing: a frame one node past the default is stopped at
/// that node, its words and offset named, and the call's heap stays a few
/// hundred bytes whatever the body's nodes (O(frame), no hash).
#[test]
fn ib_a_the_stop_holds_nothing() {
    let _g = one_at_a_time();
    for atomic in [None, Some([0x11u8; 32])] {
        let body = one_past_the_default_frame(atomic);
        let (got, held) = held_by(|| proof_limb(&body, DoorBudget::DEFAULT.max_proof_nodes));
        let stop = got.expect_err("one node past the default is stopped");
        assert_eq!(stop.nodes, LINK_ANCESTRY_PROOF_NODES_MAX + 1);
        assert_eq!(stop.bound, LINK_ANCESTRY_PROOF_NODES_MAX);
        assert_eq!(stop.subject.is_some(), atomic.is_some());
        assert!(held <= 512, "the stop held {held} bytes");
        assert!(stop.to_string().contains("(the proof limb;"), "{stop}");
        // The same frame, limb lifted: every node counted, still nothing held.
        let (got, held) = held_by(|| proof_limb(&body, u64::MAX));
        assert_eq!(got.unwrap().nodes, LINK_ANCESTRY_PROOF_NODES_MAX + 1);
        assert!(held <= 512, "the count held {held} bytes");
    }
    // One merkle path's bytes: the same count.
    let p = prove_wide(coin(3), SMALL_LIMB);
    let bytes = p.merkle_path.as_ref().unwrap().to_binary();
    assert_eq!(
        path_proof_limb(&bytes, SMALL_LIMB),
        Ok(bsv_overlay_engine::stream_sizing::ProofFrame {
            nodes: SMALL_LIMB,
            read: true
        })
    );
    assert!(path_proof_limb(&bytes, SMALL_LIMB - 1).is_err());
}

/// The submits (every mode, both policies, and the validate-only call): one
/// node past the limb is `WalkCouldNotRun` `OverProof` before any parse
/// under `NotNow` in EVERY mode (`historical-tx-no-spv` included), and goes on
/// under `NetworkAccepted` (the fold's M1: as past every other limb); AT the
/// limb the body is admitted as on the base. Under the small limb.
#[test]
fn ib_b_every_submit_stops_one_past_the_limb_and_admits_at_it() {
    let _g = one_at_a_time();
    let bodies = small_bodies();
    let engine = engine_under(small_budget());
    for (door, r) in block_on(every_submit(&engine, &bodies.past.0)) {
        match r {
            Err(e) if !door.contains("NetworkAccepted") => {
                assert!(over_proof(&e), "{door}: {e}");
                assert!(
                    e.to_string()
                        .contains("the same bytes are stopped at any time"),
                    "{door}: {e}"
                );
            }
            Err(e) => panic!("{door}: the network's accept goes on past the limb: {e}"),
            Ok(()) if door.contains("NetworkAccepted") => {}
            Ok(()) => panic!("{door}: one node past the limb was admitted"),
        }
    }
    for (door, r) in block_on(every_submit(&engine, &bodies.at.0)) {
        r.unwrap_or_else(|e| panic!("{door}: at the limb the body goes on: {e}"));
    }
}

/// The DEFAULT at the submits: a frame one node past it is stopped with the
/// call's heap a few KB (the base parses it: every merkle path's root).
#[test]
fn ib_b_the_default_stops_the_submit_before_its_parse() {
    let _g = one_at_a_time();
    let body = one_past_the_default_frame(Some([0x22u8; 32]));
    let engine = engine_under(DoorBudget::DEFAULT);
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    // `NotNow` alone: under `NetworkAccepted` the submit goes on and reads the
    // body (the fold's M1; `ibf_m1_*`).
    {
        let policy = WalkBreachPolicy::NotNow;
        let t = tagged(&body);
        let (r, held) = held_by(|| {
            rt.block_on(engine.submit_with_report_under(&t, SubmitMode::HistoricalTxNoSpv, policy))
        });
        let e = r.expect_err("stopped");
        assert!(
            held <= STOP_HELD_MAX,
            "{policy:?}: the submit held {held} bytes: {e}"
        );
        assert!(over_proof(&e), "{policy:?}: {e}");
        let EngineError::WalkCouldNotRun(stop) = e else {
            unreachable!()
        };
        // The atomic prefix names the subject, read without a parse.
        assert_eq!(stop.subject_txid, {
            let mut id = [0x22u8; 32];
            id.reverse();
            hex::encode(id)
        });
    }
}

/// The script door (`verify_scripts_only`, the gated arm's pre-broadcast
/// walk): one past the limb is `ScriptWalkOverBudget` `Proof`, the stream
/// never opened; at the limb it walks. A body past the memory limb too names
/// the memory limb (read first, both from the frame).
#[test]
fn ib_c_the_script_door_stops_one_past_the_limb() {
    let _g = one_at_a_time();
    let bodies = small_bodies();
    let engine = engine_under(small_budget());
    let past = block_on(engine.verify_scripts_only(&bodies.past.0, &bodies.past.1.id()));
    match past {
        Err(EngineError::ScriptWalkOverBudget { limb, what, .. }) => {
            assert_eq!(limb, DoorLimb::Proof, "{what}");
        }
        other => panic!("the door stops it: {other:?}"),
    }
    block_on(engine.verify_scripts_only(&bodies.at.0, &bodies.at.1.id()))
        .expect("at the limb the door walks");
    let both = DoorBudget {
        max_memory_bytes: 1024,
        ..small_budget()
    };
    match block_on(engine.verify_scripts_only_under(&bodies.past.0, &bodies.past.1.id(), both)) {
        Err(EngineError::ScriptWalkOverBudget { limb, .. }) => assert_eq!(limb, DoorLimb::Memory),
        other => panic!("{other:?}"),
    }
    // The default: a frame one past it, the door's heap a few KB.
    let body = one_past_the_default_frame(None);
    let default = engine_under(DoorBudget::DEFAULT);
    let (r, held) = held_by(|| block_on(default.verify_scripts_only(&body, &"00".repeat(32))));
    assert!(held <= STOP_HELD_MAX, "the door held {held} bytes");
    assert!(
        matches!(
            r,
            Err(EngineError::ScriptWalkOverBudget {
                limb: DoorLimb::Proof,
                ..
            })
        ),
        "{r:?}"
    );
}

/// The GASP anchor: a root whose proof is past the limb is HELD
/// (`AnchorWalkHeld`), from its node's proof read (the default) and from the
/// root BEEF (the engine's budget, here the small limb); at the limb it
/// passes the Bitcoin step.
#[test]
fn ib_d_the_anchor_holds_a_root_past_the_limb() {
    let _g = one_at_a_time();
    block_on(async {
        // A root proven by a path of 64 nodes: under the default, past the small limb.
        let root_tx = prove_wide(coin(5), 2 * SMALL_LIMB);
        let store = MemoryStorage::new();
        let manager = AdmitZero;
        let held = |budget: DoorBudget| {
            OverlayGASPStorage::new(&store, TOPIC, new_finalized_graph_sink())
                .with_topic_manager(&manager)
                .with_walk_budget(budget)
        };
        let node = |tx: &Transaction| GASPNode {
            graph_id: format!("{}.0", tx.id()),
            raw_tx: tx.to_hex(),
            output_index: 0,
            proof: tx.merkle_path.as_ref().map(MerklePath::to_hex),
            tx_metadata: None,
            output_metadata: None,
            inputs: None,
        };
        let gasp = held(small_budget());
        gasp.append_to_graph(&node(&root_tx), None).await.unwrap();
        match gasp
            .validate_graph_anchor(&format!("{}.0", root_tx.id()))
            .await
        {
            Err(GASPError::AnchorWalkHeld(why)) => assert!(why.contains("over_proof"), "{why}"),
            other => panic!("held at the root BEEF: {other:?}"),
        }
        let gasp = held(DoorBudget {
            max_proof_nodes: 2 * SMALL_LIMB,
            ..DoorBudget::DEFAULT
        });
        gasp.append_to_graph(&node(&root_tx), None).await.unwrap();
        gasp.validate_graph_anchor(&format!("{}.0", root_tx.id()))
            .await
            .expect("at the limb the anchor passes");
    });
    // The node's own proof past the default: held at its read, never parsed.
    let hex = hex::encode(one_past_the_default_bump());
    let e = beef_limits::proof_from_hex(&hex).expect_err("not parsed");
    assert!(beef_limits::is_proof_limb(&e), "{e}");
}

/// The limb is configuration: a consumer's budget with a larger
/// `max_proof_nodes` admits what a smaller one stops, at every submit.
#[test]
fn ib_e_the_limb_is_configuration() {
    let _g = one_at_a_time();
    let bodies = small_bodies();
    let roomy = engine_under(DoorBudget {
        max_proof_nodes: SMALL_LIMB + 1,
        ..DoorBudget::DEFAULT
    });
    for (door, r) in block_on(every_submit(&roomy, &bodies.past.0)) {
        r.unwrap_or_else(|e| panic!("{door}: the roomier limb admits it: {e}"));
    }
    assert_eq!(roomy.walk_budget().max_proof_nodes, SMALL_LIMB + 1);
    // And the DEFAULT width: lifted, a frame one past the default is read
    // (its parse refuses it, a hand-built BUMP; never the proof limb).
    let lifted = engine_under(DoorBudget {
        max_proof_nodes: u64::MAX,
        ..DoorBudget::DEFAULT
    });
    let body = one_past_the_default_frame(None);
    if let Err(e) = block_on(lifted.submit(&tagged(&body), SubmitMode::HistoricalTxNoSpv)) {
        assert!(!over_proof(&e), "lifted, the limb does not stop it: {e}");
    }
}

/// THE SURVEY, kept: every shape the existing pins link or admit today that
/// carries a BUMP, and every BEEF fixture of the repository, sits under the
/// default. (The whole suites were run with the limb lifted and every body
/// counted; the lane's report names the bodies past it: each is a pin built
/// to be wide.)
#[test]
fn ib_f_no_body_linked_today_is_stopped() {
    let _g = one_at_a_time();
    let default = DoorBudget::DEFAULT.max_proof_nodes;
    let mut shapes: Vec<(&str, Vec<u8>)> = Vec::new();
    // `p0811_link_heap`'s widest admitted parent: 16,384 leaves, 200 outputs spent.
    let mut p = coin(7);
    for _ in 1..200 {
        p.outputs.push(TransactionOutput::new(1, one()));
    }
    let p = prove_wide(p, 16_384);
    let spent: Vec<(String, u32)> = (0..200).map(|i| (p.id(), i)).collect();
    let s = spend(&spent, 8);
    shapes.push((
        "p0811 a BUMP of 16,384 leaves, 200 outputs spent",
        beef_of(&[&p, &s]),
    ));
    // `p0811_link_heap`'s 4,096-leaf parent.
    let p = prove_wide(coin(9), 4_096);
    let s = spend(&[(p.id(), 0)], 10);
    shapes.push(("p0811 a BUMP of 4,096 leaves", beef_of(&[&p, &s])));
    // Every BEEF fixture of the repository.
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    for dir in [
        "crates/overlay-engine/tests/fixtures",
        "crates/overlay-cloudflare/tests/fixtures/ef",
    ] {
        for entry in std::fs::read_dir(root.join(dir)).unwrap() {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_string_lossy().to_string();
            let bytes = std::fs::read(&path).unwrap();
            let body = if name.ends_with(".hex") {
                hex::decode(String::from_utf8(bytes).unwrap().trim()).unwrap()
            } else if name.ends_with(".beef") {
                bytes
            } else {
                continue;
            };
            shapes.push((Box::leak(name.into_boxed_str()), body));
        }
    }
    assert!(
        shapes.len() >= 8,
        "the fixtures were found: {}",
        shapes.len()
    );
    for (name, body) in &shapes {
        let frame = proof_limb(body, u64::MAX).unwrap();
        assert!(
            frame.nodes <= default,
            "{name}: {} nodes, past the default {default}",
            frame.nodes
        );
        assert!(proof_limb(body, default).is_ok(), "{name}");
    }
}

// ---------------------------------------------------------------------------
// The fold (lane INTAKE-BOUND-FOLD).
// ---------------------------------------------------------------------------

/// The nodes bsv-rs parsed in `beef`'s BUMPs.
fn sdk_nodes_of_beef(beef: &Beef) -> u64 {
    beef.bumps
        .iter()
        .map(|m| m.path.iter().map(|l| l.len() as u64).sum::<u64>())
        .sum()
}

/// A deterministic xorshift, so the differential is the same run to run.
struct Xorshift(u64);
impl Xorshift {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

/// H1: THE FRAME READ NEVER COUNTS FEWER NODES THAN A READER THAT PAYS. Over
/// generated and mutated merkle paths and BEEFs (V1, V2, atomic; a byte set,
/// a bit flipped, a flag byte set to 0..=6, 0x80, 0xfd..=0xff, a byte
/// doubled), deterministic seed: whenever bsv-rs's `MerklePath::from_binary`
/// parses a path, the path read followed it (`read`) and counted at least its
/// nodes, `proof_from_hex_under` refuses none of them and stops each one a
/// node under its count; whenever the house's stream-first parse
/// (`parse_beef`) or bsv-rs's `Beef::from_binary` parses a BEEF, the frame
/// read counted at least its nodes and the limb one under them stops it.
/// RED with `69cee436`'s rule grafted (a flag byte outside 0, 1 and 2 ended the frame read
/// with the nodes before it, and bsv-rs read on).
#[test]
fn ibf_h1_the_frame_read_never_counts_fewer_nodes_than_the_sdk() {
    let _g = one_at_a_time();
    let mut rng = Xorshift(0x1234_5678_9abc_def0);
    let (mut bodies, mut paths_parsed, mut beefs_parsed, mut stream_parsed) = (0u64, 0, 0, 0);
    for round in 0..3_000u32 {
        let n = 2 * (1 + rng.next() % 20);
        let a = prove_wide(coin(round), n);
        let mp = a.merkle_path.clone().unwrap();
        let s = spend(&[(a.id(), 0)], round + 7);
        let mut beef = if rng.next().is_multiple_of(2) {
            Beef::with_version(bsv_rs::transaction::BEEF_V1)
        } else {
            Beef::new()
        };
        let bi = beef.merge_bump(mp.clone());
        beef.merge_raw_tx(a.to_binary(), Some(bi));
        beef.merge_raw_tx(s.to_binary(), None);
        let mut originals = vec![(mp.to_binary(), true), (beef.to_binary(), false)];
        if rng.next().is_multiple_of(3) {
            originals.push((beef.to_binary_atomic(&s.id()).unwrap(), false));
        }
        for (orig, is_path) in originals {
            for _ in 0..6 {
                let mut b = orig.clone();
                for _ in 0..1 + rng.next() % 3 {
                    let i = (rng.next() as usize) % b.len();
                    match rng.next() % 4 {
                        0 => b[i] = rng.next() as u8,
                        1 => b[i] ^= 1 << (rng.next() % 8),
                        2 => {
                            b[i] = [0u8, 1, 2, 3, 4, 5, 6, 0x80, 0xfd, 0xfe, 0xff]
                                [(rng.next() % 11) as usize]
                        }
                        _ => {
                            let v = b[i];
                            b.insert(i, v);
                        }
                    }
                }
                bodies += 1;
                if is_path {
                    let Ok(m) = MerklePath::from_binary(&b) else {
                        continue;
                    };
                    paths_parsed += 1;
                    let sdk: u64 = m.path.iter().map(|l| l.len() as u64).sum();
                    let frame = path_proof_limb(&b, u64::MAX)
                        .unwrap_or_else(|s| panic!("an unbounded read never stops: {s}"));
                    assert!(
                        frame.read && frame.nodes >= sdk,
                        "a path bsv-rs parses ({sdk} nodes): the frame read {frame:?}: {}",
                        hex::encode(&b)
                    );
                    beef_limits::proof_from_hex_under(&hex::encode(&b), u64::MAX)
                        .unwrap_or_else(|e| panic!("refused a path bsv-rs parses: {e}"));
                    if sdk > 0 {
                        let e = beef_limits::proof_from_hex_under(&hex::encode(&b), sdk - 1)
                            .expect_err("one node under the count stops it");
                        assert!(beef_limits::is_proof_limb(&e), "{e}");
                    }
                } else {
                    for (reader, parsed) in [
                        (
                            "Beef::from_binary",
                            Beef::from_binary(&b).ok().map(|x| sdk_nodes_of_beef(&x)),
                        ),
                        (
                            "parse_beef",
                            beef_limits::parse_beef(&b, &beef_limits::ENGINE_BEEF_LIMITS)
                                .ok()
                                .map(|x| sdk_nodes_of_beef(&x)),
                        ),
                    ] {
                        let Some(sdk) = parsed else {
                            continue;
                        };
                        if reader == "parse_beef" {
                            stream_parsed += 1;
                        } else {
                            beefs_parsed += 1;
                        }
                        let frame = proof_limb(&b, u64::MAX).unwrap();
                        assert!(
                            frame.nodes >= sdk,
                            "{reader} parsed {sdk} nodes, the frame read {frame:?}: {}",
                            hex::encode(&b)
                        );
                        if sdk > 0 {
                            assert!(proof_limb(&b, sdk - 1).is_err(), "{reader}");
                        }
                    }
                }
            }
        }
    }
    println!(
        "ibf_h1: {bodies} bodies; paths bsv-rs parsed {paths_parsed}; BEEFs Beef::from_binary \
         parsed {beefs_parsed}, parse_beef parsed {stream_parsed}"
    );
    assert!(bodies > 40_000 && paths_parsed > 1_000 && beefs_parsed > 1_000);
}

/// A path's bytes (BRC-74) of `n` level-0 hashed leaves, every flag `flag`,
/// by hand (nothing hashed to build it).
fn path_by_hand(n: u64, flag: u8) -> Vec<u8> {
    let mut out = Vec::with_capacity(38 * n as usize + 64);
    out.extend(bsv_rs::primitives::to_var_int(900_000));
    out.push(18);
    out.extend(bsv_rs::primitives::to_var_int(n));
    for offset in 0..n {
        out.extend(bsv_rs::primitives::to_var_int(offset));
        out.push(flag);
        out.extend([7u8; 32]);
    }
    out.extend([0u8; 17]);
    out
}

/// H1: a path of 200,000 nodes whose flag bytes are 0x06 (bsv-rs reads a
/// hash: bit 0 is clear) is stopped at the limb unparsed: the read holds a
/// few hundred bytes, `proof_from_hex` no more than its own hex decode (under
/// twice the path's bytes). RED
/// with `69cee436`'s rule grafted (`Ok(0)`, then every node parsed).
#[test]
fn ibf_h1_a_flag_0x06_path_of_200000_nodes_is_stopped_unparsed() {
    let _g = one_at_a_time();
    for flag in [0x06u8, 0x82, 0xfe] {
        let bytes = path_by_hand(200_000, flag);
        let (r, held) = held_by(|| path_proof_limb(&bytes, LINK_ANCESTRY_PROOF_NODES_MAX));
        let stop = r.expect_err("stopped");
        assert_eq!(stop.nodes, LINK_ANCESTRY_PROOF_NODES_MAX + 1, "{flag:#04x}");
        assert!(held <= 512, "{flag:#04x}: the read held {held} bytes");
        let hex = hex::encode(&bytes);
        let (r, held) = held_by(|| beef_limits::proof_from_hex(&hex));
        let e = r.expect_err("not parsed");
        assert!(beef_limits::is_proof_limb(&e), "{flag:#04x}: {e}");
        // The hex decode's growing buffer: under twice the path's bytes. A
        // parse holds each leaf's hash as a hex string, several times more.
        assert!(
            held <= 2 * bytes.len() as u64,
            "{flag:#04x}: proof_from_hex held {held} bytes for {} bytes of path",
            bytes.len()
        );
    }
    // The same path in a BEEF: the frame read counts it whatever its flags.
    let mut body = Vec::new();
    body.extend(bsv_rs::transaction::BEEF_V2.to_le_bytes());
    body.push(1);
    body.extend(path_by_hand(200_000, 0x06));
    body.push(0);
    let stop = proof_limb(&body, LINK_ANCESTRY_PROOF_NODES_MAX).expect_err("stopped");
    assert_eq!(stop.nodes, LINK_ANCESTRY_PROOF_NODES_MAX + 1);
}

/// H1: what the path read does not follow is refused unparsed, a parse
/// refusal and never the limb's stop; bsv-rs refuses each too.
#[test]
fn ibf_h1_a_path_the_read_does_not_follow_is_refused_unparsed() {
    let _g = one_at_a_time();
    let whole = prove_wide(coin(4), 8)
        .merkle_path
        .clone()
        .unwrap()
        .to_binary();
    for cut in 1..whole.len() {
        let b = &whole[..cut];
        let e = beef_limits::proof_from_hex(&hex::encode(b)).expect_err("cut short");
        assert!(!beef_limits::is_proof_limb(&e), "{cut}: {e}");
        assert!(
            MerklePath::from_binary(b).is_err(),
            "{cut}: bsv-rs parses it"
        );
        if path_proof_limb(b, u64::MAX).is_ok_and(|f| !f.read) {
            assert!(
                e.to_string()
                    .contains(beef_limits::PROOF_NOT_FOLLOWED_WORDS),
                "{cut}: {e}"
            );
        }
    }
    // A non-canonical varint (bsv-rs refuses it too).
    let mut b = whole.clone();
    b.splice(0..1, [0xfd, 0x00, 0x00]);
    assert!(MerklePath::from_binary(&b).is_err());
    assert!(beef_limits::proof_from_hex(&hex::encode(&b)).is_err());
    // A stored proof is read as on `main`: no limb.
    let wide = hex::encode(path_by_hand(LINK_ANCESTRY_PROOF_NODES_MAX + 1, 0));
    let e = beef_limits::stored_proof_from_hex(&wide).expect_err("junk hashes");
    assert!(
        !beef_limits::is_proof_limb(&e),
        "the stored reader reads it: {e}"
    );
}

/// M1 (ruling 2): under `NetworkAccepted` a submit past the proof limb goes
/// on as past every other limb, in every mode: admitted, the trip reported
/// (`walk_could_not_run`, limb `OverProof`, `walk_went_on`
/// `NetworkAccepted`, the subject named). Under `NotNow` it stops in every
/// mode, `historical-tx-no-spv` included. RED with `69cee436`'s rule grafted (stopped under
/// both policies).
#[test]
fn ibf_m1_the_networks_accept_goes_on_past_the_proof_limb() {
    let _g = one_at_a_time();
    let bodies = small_bodies();
    for mode in [
        SubmitMode::CurrentTx,
        SubmitMode::HistoricalTx,
        SubmitMode::HistoricalTxNoSpv,
    ] {
        let (steak, report) = block_on(engine_under(small_budget()).submit_with_report_under(
            &tagged(&bodies.past.0),
            mode,
            WalkBreachPolicy::NetworkAccepted,
        ))
        .unwrap_or_else(|e| panic!("{mode:?}: goes on: {e}"));
        assert_eq!(
            steak.get(TOPIC).map(|a| a.outputs_to_admit.clone()),
            Some(vec![0]),
            "{mode:?}"
        );
        let stop = report.walk_could_not_run.as_ref().expect("reported");
        assert_eq!(stop.limb, WalkLimb::OverProof, "{mode:?}");
        assert_eq!(stop.subject_txid, bodies.past.1.id(), "{mode:?}");
        assert_eq!(
            report.walk_went_on,
            Some(bsv_overlay_engine::engine::WalkWentOn::NetworkAccepted),
            "{mode:?}"
        );
        let e = block_on(engine_under(small_budget()).submit_with_report_under(
            &tagged(&bodies.past.0),
            mode,
            WalkBreachPolicy::NotNow,
        ))
        .expect_err("not now");
        assert!(over_proof(&e), "{mode:?}: {e}");
    }
}

/// M3 and N1: the anchor's ASSEMBLY reads each node's proof under the
/// engine's CONFIGURED limb and HOLDS the graph (`AnchorWalkHeld`) on one
/// past it; the ROOT BEEF's own read holds a graph whose node proofs each fit
/// and together do not. Debug profile, a small limb. RED with the assembly's
/// proof-limb branch made `Other`, or the `unassembled` hold arm removed
/// (`AnchorUnavailable`), or the root's read removed.
#[test]
fn ibf_m3_the_anchor_holds_at_its_assembly_and_at_its_root() {
    let _g = one_at_a_time();
    block_on(async {
        let store = MemoryStorage::new();
        let manager = AdmitZero;
        let gasp = || {
            OverlayGASPStorage::new(&store, TOPIC, new_finalized_graph_sink())
                .with_topic_manager(&manager)
                .with_walk_budget(small_budget())
        };
        let node = |graph: &str, tx: &Transaction| GASPNode {
            graph_id: graph.to_string(),
            raw_tx: tx.to_hex(),
            output_index: 0,
            proof: tx.merkle_path.as_ref().map(MerklePath::to_hex),
            tx_metadata: None,
            output_metadata: None,
            inputs: None,
        };
        // A root proven by a path past the small limb: held at the assembly.
        let root_tx = prove_wide(coin(5), 2 * SMALL_LIMB);
        let graph = format!("{}.0", root_tx.id());
        let g = gasp();
        g.append_to_graph(&node(&graph, &root_tx), None)
            .await
            .unwrap();
        match g.validate_graph_anchor(&graph).await {
            Err(GASPError::AnchorWalkHeld(why)) => {
                assert!(why.contains(&format!("the proof of {graph}")), "{why}");
                assert!(why.contains("over_proof"), "{why}");
            }
            other => panic!("held at the assembly: {other:?}"),
        }
        // An unproven root over two parents of 22 nodes each: each fits, the
        // root BEEF (44) does not: held at the root BEEF.
        let a = prove_wide(coin(6), 20);
        let b = prove_wide(coin(7), 20);
        assert!(nodes_of(&a) <= SMALL_LIMB && nodes_of(&a) + nodes_of(&b) > SMALL_LIMB);
        let root = spend(&[(a.id(), 0), (b.id(), 0)], 12);
        let graph = format!("{}.0", root.id());
        let g = gasp();
        g.append_to_graph(&node(&graph, &root), None).await.unwrap();
        for parent in [&a, &b] {
            g.append_to_graph(&node(&graph, parent), Some(&graph))
                .await
                .unwrap();
        }
        match g.validate_graph_anchor(&graph).await {
            Err(GASPError::AnchorWalkHeld(why)) => {
                assert!(why.contains("the root BEEF could not be read"), "{why}");
            }
            other => panic!("held at the root BEEF: {other:?}"),
        }
    });
}

/// N1: the GASP walk reads a peer's node proof under the engine's configured
/// limb (`DoorBudget::proof_from_hex`), not the fixed default.
#[test]
fn ibf_n1_the_budget_reads_a_path_under_its_own_limb() {
    let _g = one_at_a_time();
    let hex = prove_wide(coin(8), 2 * SMALL_LIMB)
        .merkle_path
        .clone()
        .unwrap()
        .to_hex();
    assert!(DoorBudget::DEFAULT.proof_from_hex(&hex).is_ok());
    let e = small_budget()
        .proof_from_hex(&hex)
        .expect_err("past the small limb");
    assert!(beef_limits::is_proof_limb(&e), "{e}");
    // The GASP WALK's read of a peer's node proof (`find_needed_inputs`):
    // under the configured small limb the UTXO fails (`GASPError::Other`, the
    // fold's N2); under the default it is read. RED with the walk reading the
    // fixed default.
    block_on(async {
        let store = MemoryStorage::new();
        let manager = AdmitZero;
        let tx = prove_wide(coin(9), 2 * SMALL_LIMB);
        let node = GASPNode {
            graph_id: format!("{}.0", tx.id()),
            raw_tx: tx.to_hex(),
            output_index: 0,
            proof: tx.merkle_path.as_ref().map(MerklePath::to_hex),
            tx_metadata: None,
            output_metadata: None,
            inputs: None,
        };
        let walk = |budget: DoorBudget| {
            OverlayGASPStorage::new(&store, TOPIC, new_finalized_graph_sink())
                .with_topic_manager(&manager)
                .with_walk_budget(budget)
        };
        match walk(small_budget()).find_needed_inputs(&node).await {
            Err(GASPError::Other(why)) => assert!(why.contains("(the proof limb;"), "{why}"),
            other => panic!("the walk's read fails the UTXO: {other:?}"),
        }
        walk(DoorBudget::DEFAULT)
            .find_needed_inputs(&node)
            .await
            .expect("under the default the proof is read");
    });
}

/// H1 (ruling 1): every production caller of the BEEF proof limb reads the
/// house's stream-first parse next (or answers its stop and returns), never
/// `Beef::from_binary` or `Transaction::from_beef` directly; a new caller
/// fails this pin until it is listed. (The frame read counts every node any
/// bsv-rs reader parses whatever `read` says, the differential above: this is
/// the belt.)
#[test]
fn ibf_h1_every_caller_of_the_limb_reads_the_stream_next() {
    let _g = one_at_a_time();
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    // (file, the limb's call, the read that follows it)
    let callers: &[(&str, &str, &str)] = &[
        (
            "overlay-engine/src/engine.rs",
            "self.walk_budget.proof_limb(&tagged_beef.beef)",
            "beef_limits::parse_beef(&tagged_beef.beef",
        ),
        (
            "overlay-engine/src/engine.rs",
            "budget.proof_limb(beef_bytes)",
            "Engine::verify_beef_linear(",
        ),
        (
            "overlay-engine/src/engine.rs",
            "DoorBudget::DEFAULT.proof_limb(&output.beef)",
            "beef_limits::subject_from_beef(&output.beef",
        ),
        (
            "overlay-engine/src/script_door.rs",
            "budget.proof_limb(beef_bytes)",
            "DoorIndex::read(beef_bytes)",
        ),
        (
            "overlay-engine/src/gasp_overlay.rs",
            "self.walk_budget.proof_limb(&root_beef)",
            "beef_limits::parse_beef(&root_beef",
        ),
        (
            "overlay-cloudflare/src/mainnet_fanout.rs",
            "WORKER_WALK_BUDGET.proof_limb(beef)",
            "beef_limits::parse_beef(beef",
        ),
        (
            "overlay-cloudflare/src/peer_crawler.rs",
            "WORKER_WALK_BUDGET.proof_limb(&entry.beef)",
            "beef_limits::subject_from_beef(",
        ),
        (
            "overlay-cloudflare/src/routes.rs",
            "WORKER_WALK_BUDGET\n        .proof_limb(beef)",
            "",
        ),
        (
            "overlay-cloudflare/src/queue.rs",
            "WORKER_WALK_BUDGET\n        .proof_limb(bytes)",
            "",
        ),
        (
            "overlay-cloudflare/src/stored_rows.rs",
            "WORKER_WALK_BUDGET.proof_limb(bytes)",
            "beef_limits::read_beef(bytes)",
        ),
    ];
    let mut per_file: std::collections::BTreeMap<&str, usize> = Default::default();
    for (file, limb, next) in callers {
        *per_file.entry(file).or_default() += 1;
        let src = std::fs::read_to_string(root.join(file)).unwrap();
        let at = src
            .find(limb)
            .unwrap_or_else(|| panic!("{file} reads the limb by {limb}"));
        if next.is_empty() {
            continue; // a helper: its callers' reads are the queue's and the route's own (pinned there)
        }
        let after = &src[at..];
        let next_at = after
            .find(next)
            .unwrap_or_else(|| panic!("{file}: {next} follows {limb}"));
        for direct in [
            "Beef::from_binary(",
            "Transaction::from_beef(",
            "from_binary_with_limits(",
        ] {
            assert!(
                !after[..next_at].contains(direct),
                "{file}: {direct} between the limb and the stream-first read"
            );
        }
    }
    // Every production call of the limb is listed (tests cut off).
    for (file, listed) in per_file {
        let src = std::fs::read_to_string(root.join(file)).unwrap();
        let prod = src.split("#[cfg(test)]\nmod ").next().unwrap();
        let calls = prod.matches(".proof_limb(").count();
        assert_eq!(calls, listed, "{file}: every call of the limb is listed");
    }
}
