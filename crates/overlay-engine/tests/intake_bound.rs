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
    assert_eq!(path_proof_limb(&bytes, SMALL_LIMB), Ok(SMALL_LIMB));
    assert!(path_proof_limb(&bytes, SMALL_LIMB - 1).is_err());
}

/// The submits (every mode, both policies, and the validate-only call): one
/// node past the limb is `WalkCouldNotRun` `OverProof` before any parse, and
/// AT the limb the body is admitted as on the base. Under the small limb.
#[test]
fn ib_b_every_submit_stops_one_past_the_limb_and_admits_at_it() {
    let _g = one_at_a_time();
    let bodies = small_bodies();
    let engine = engine_under(small_budget());
    for (door, r) in block_on(every_submit(&engine, &bodies.past.0)) {
        match r {
            Err(e) => {
                assert!(over_proof(&e), "{door}: {e}");
                assert!(
                    e.to_string()
                        .contains("the same bytes are stopped at any time"),
                    "{door}: {e}"
                );
            }
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
    for policy in [WalkBreachPolicy::NotNow, WalkBreachPolicy::NetworkAccepted] {
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
