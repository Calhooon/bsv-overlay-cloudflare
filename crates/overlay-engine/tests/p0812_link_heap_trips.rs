//! LOCK-0812: a trip of bsv-rs 0.8.12's HEAP meter takes the road of every
//! link trip, at every door that links.
//!
//! Each pin builds a body the engine's own estimate admits and bsv-rs's heap
//! meter declines under the engine's figure
//! (`beef_limits::LINK_METER_HEAP_MAX_BYTES`): a transaction proven by its own
//! BUMP of 564,610 level-0 leaves (564,622 in all, past the 564,617 the figure
//! holds at 104 bytes a leaf), whose link has no ancestry for the estimate to
//! charge. Each door answers in the words it gives the estimate's and the wire
//! meter's trips (`tests/p0811_link_trips.rs`); on bsv-rs 0.8.11 each of these
//! bodies linked. The door pins parse a 21 MB body several times each: release
//! profile only (`cargo test --release --features memory-storage --test
//! p0812_link_heap_trips`; 22 s, where the debug profile took minutes a pin).
//! The submit in a mode that walks names the walk's memory limb first (as the
//! anchor does, below); the no-spv submit walks nothing and names the link.
//! The predecessor question's names call is not reached by the heap meter
//! alone: a proven spender is not asked, and an unproven one spending the wide
//! parent is declined first by the engine's estimate (its `p0811_link_trips`
//! pin). Every door reads one function (`beef_limits::link_bounded`), whose
//! heap-meter trip is pinned here by its class and its words.
//!
//! Lane INTAKE-BOUND: the wide body carries 564,622 BUMP nodes, past the
//! door's proof limb (`DoorBudget::max_proof_nodes`, 65,536 by default), so
//! under the engine's DEFAULT budget every door that reads a body now stops it
//! at the frame, before anything is parsed: each submit pin asserts that
//! first (`WalkCouldNotRun`, limb `OverProof`), then drives the same door with
//! the proof limb lifted (`lifted`) to keep the heap meter's trip pinned at
//! the door. The anchor reads a node's proof under the limb's default
//! (`beef_limits::proof_from_hex`, not a budget), so it holds the graph at
//! that read (`over_proof`) and no longer reaches its walk. The meter's trip
//! at the link function itself (`p0812_t_the_wide_parent_is_the_heap_meters_alone`,
//! and every pin of `p0812_link_heap_meter`) is unchanged.
// bsv-stack-lean #134: test code
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::OnceLock;

use async_trait::async_trait;
use bsv_overlay_engine::builder::EngineBuilder;
use bsv_overlay_engine::engine::{
    DoorBudget, Engine, EngineError, WalkBreachPolicy, WalkLimb, WalkWentOn,
};
use bsv_overlay_engine::gasp::{GASPError, GASPStorage};
use bsv_overlay_engine::gasp_overlay::{new_finalized_graph_sink, OverlayGASPStorage};
use bsv_overlay_engine::storage::memory::MemoryStorage;
use bsv_overlay_engine::storage::Storage;
use bsv_overlay_engine::topic_manager::{TopicManager, TopicManagerError};
use bsv_overlay_engine::types::*;
use bsv_rs::script::{LockingScript, Script, UnlockingScript};
use bsv_rs::transaction::{
    Beef, MerklePath, MerklePathLeaf, Transaction, TransactionInput, TransactionOutput,
};

const TOPIC: &str = "tm_p0812";

/// The level-0 leaves of the wide BUMP: past the heap figure.
const PAST_LEVEL0: u64 = 564_610;

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

/// A transaction spending `spent` and paying one `OP_1` output.
fn spend(spent: &[(String, u32)], tag: u32) -> Transaction {
    let mut tx = Transaction::new();
    for (txid, vout) in spent {
        tx.inputs.push(spending(txid, *vout));
    }
    tx.outputs.push(TransactionOutput::new(1, one()));
    tx.lock_time = tag;
    tx
}

/// The grandparent: proven by a one-leaf BUMP, ten `OP_1` outputs.
fn grandparent() -> Transaction {
    let mut tx = Transaction::new();
    tx.inputs.push(spending(&"aa".repeat(32), 0));
    for _ in 0..10 {
        tx.outputs.push(TransactionOutput::new(1, one()));
    }
    let txid = tx.id();
    tx.merkle_path =
        Some(MerklePath::new(900_000, vec![vec![MerklePathLeaf::new_txid(0, txid)]]).unwrap());
    tx
}

/// `tx` proven by its own BUMP of `level0` level-0 leaves (`level0` even),
/// with a duplicate leaf at every higher level whose width is odd.
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
    path.pop();
    tx.merkle_path = Some(MerklePath::new(900_000, path).unwrap());
    tx
}

/// THE PARENT: spends the grandparent's first output, proven by the wide BUMP
/// (its bytes and its BUMP built once for the binary).
fn wide_parent() -> (Transaction, Transaction) {
    static WIDE: OnceLock<(Vec<u8>, MerklePath)> = OnceLock::new();
    let g = grandparent();
    let (raw, path) = WIDE.get_or_init(|| {
        let p = prove_wide(spend(&[(g.id(), 0)], 1), PAST_LEVEL0);
        (p.to_binary(), p.merkle_path.clone().unwrap())
    });
    let mut p = Transaction::from_binary(raw).unwrap();
    p.merkle_path = Some(path.clone());
    (g, p)
}

fn beef_of(txs: &[&Transaction]) -> Vec<u8> {
    let mut beef = Beef::new();
    for tx in txs {
        let bump = tx.merkle_path.clone().map(|m| beef.merge_bump(m));
        beef.merge_raw_tx(tx.to_binary(), bump);
    }
    beef.to_binary()
}

/// The heap meter's own words in a door's answer.
fn by_the_heap_meter(why: &str) -> bool {
    why.contains("heap bytes of re-materialized")
}

/// Names, for a BEEF, the inputs of its subject's carried sources (`deep`)
/// or the subject's own inputs; admits output 0 when `admits`.
struct Naming {
    deep: bool,
    admits: bool,
    names: bool,
}

#[async_trait(?Send)]
impl TopicManager for Naming {
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
            outputs_to_admit: if self.admits { vec![0] } else { vec![] },
            coins_to_retain: vec![],
            coins_removed: None,
        })
    }
    async fn identify_needed_inputs(
        &self,
        beef: &[u8],
        _: Option<&[u8]>,
    ) -> Result<Vec<Outpoint>, TopicManagerError> {
        if !self.names {
            return Ok(Vec::new());
        }
        let lim = &bsv_overlay_engine::beef_limits::ENGINE_BEEF_LIMITS;
        let subject = bsv_overlay_engine::beef_limits::subject_from_beef(beef, None, lim)
            .map_err(|e| TopicManagerError::InvalidBeef(e.to_string()))?;
        let parsed = bsv_overlay_engine::beef_limits::parse_beef(beef, lim).unwrap();
        let inputs_of = |tx: &Transaction| {
            tx.inputs
                .iter()
                .map(|i| Outpoint {
                    txid: i.source_txid.clone().unwrap_or_default(),
                    output_index: i.source_output_index,
                })
                .collect::<Vec<_>>()
        };
        if !self.deep {
            return Ok(inputs_of(&subject));
        }
        Ok(subject
            .inputs
            .iter()
            .filter_map(|i| parsed.find_txid(i.source_txid.as_deref()?)?.tx().cloned())
            .flat_map(|source| inputs_of(&source))
            .collect())
    }
    async fn get_documentation(&self) -> String {
        "names inputs".into()
    }
    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "p0811-naming".into(),
            ..Default::default()
        }
    }
}

fn engine_with(manager: Naming, storage: MemoryStorage) -> Engine {
    EngineBuilder::new(Box::new(storage))
        .with_topic(TOPIC, Box::new(manager))
        .build()
}

/// The engine with the proof limb lifted (lane INTAKE-BOUND), so the door
/// reads the wide body and the heap meter's trip is reached.
fn lifted(manager: Naming, storage: MemoryStorage) -> Engine {
    EngineBuilder::new(Box::new(storage))
        .with_topic(TOPIC, Box::new(manager))
        .with_walk_budget(DoorBudget {
            max_proof_nodes: u64::MAX,
            ..DoorBudget::DEFAULT
        })
        .build()
}

/// Under the DEFAULT budget the door stops the wide body at the frame, before
/// any parse, under `NotNow` (lane INTAKE-BOUND); under `NetworkAccepted` it
/// goes on past the proof limb, the trip reported (the fold's ruling 2).
async fn stopped_at_intake(beef: &[u8], mode: SubmitMode) {
    let engine = || {
        engine_with(
            Naming {
                deep: false,
                admits: true,
                names: true,
            },
            MemoryStorage::new(),
        )
    };
    let tagged = TaggedBEEF::new(beef.to_vec(), vec![TOPIC.into()]);
    match engine()
        .submit_with_report_under(&tagged, mode, WalkBreachPolicy::NotNow)
        .await
    {
        Err(EngineError::WalkCouldNotRun(stop)) => {
            assert_eq!(stop.limb, WalkLimb::OverProof, "NotNow: {}", stop.what);
        }
        other => panic!("NotNow: the proof limb stops it first: {other:?}"),
    }
    match engine()
        .submit_with_report_under(&tagged, mode, WalkBreachPolicy::NetworkAccepted)
        .await
    {
        Ok((_, report)) => assert_eq!(
            report.walk_could_not_run.map(|stop| stop.limb),
            Some(WalkLimb::OverProof),
            "NetworkAccepted: the proof trip is reported"
        ),
        Err(EngineError::WalkCouldNotRun(stop)) => {
            panic!("NetworkAccepted goes on past the proof limb: {}", stop.what)
        }
        Err(_) => {}
    }
}

/// Submits `beef` under the network's accept (the subject's own walk or link
/// stop goes on) and returns the predecessor question's fault text.
async fn question(engine: &Engine, beef: Vec<u8>) -> String {
    let tagged = TaggedBEEF::new(beef, vec![TOPIC.into()]);
    let (_, report) = engine
        .submit_with_report_under(
            &tagged,
            SubmitMode::CurrentTx,
            WalkBreachPolicy::NetworkAccepted,
        )
        .await
        .expect("under the network's accept the submit goes on");
    report
        .faults
        .iter()
        .find(|f| f.site == "predecessor_not_landed")
        .map(|f| f.error.clone())
        .unwrap_or_else(|| panic!("the topic waits: {:?}", report.faults))
}

/// The parent alone links nothing past the estimate: the trip is the meter's.
#[test]
fn p0812_t_the_wide_parent_is_the_heap_meters_alone() {
    let (_, p) = &wide_parent();
    let beef = beef_of(&[p]);
    let parsed = bsv_overlay_engine::beef_limits::parse_beef(
        &beef,
        &bsv_overlay_engine::beef_limits::ENGINE_BEEF_LIMITS,
    )
    .unwrap();
    assert_eq!(
        bsv_overlay_engine::beef_limits::link_heap_estimate(&parsed, &p.id(), u64::MAX).beyond,
        0
    );
    match bsv_overlay_engine::beef_limits::linked(&parsed, &p.id()) {
        Err(e) => {
            assert!(bsv_overlay_engine::beef_limits::is_link_bound(&e), "{e}");
            assert!(by_the_heap_meter(&e.to_string()), "{e}");
        }
        Ok(tx) => panic!("the heap meter declines it: {:?}", tx.map(|t| t.id())),
    }
}

/// The dry run (the E1D opener): the candidate's heap-meter trip makes the
/// question wait, in its own words.
#[tokio::test]
#[cfg_attr(
    debug_assertions,
    ignore = "release only: cargo test --release --features memory-storage --test p0812_link_heap_trips"
)]
async fn p0812_t_the_dry_run_waits_on_a_heap_meter_trip() {
    let (g, p) = &wide_parent();
    let s = spend(&[(p.id(), 0)], 2);
    stopped_at_intake(&beef_of(&[g, p, &s]), SubmitMode::CurrentTx).await;
    let engine = lifted(
        Naming {
            deep: false,
            admits: true,
            names: true,
        },
        MemoryStorage::new(),
    );
    let why = question(&engine, beef_of(&[g, p, &s])).await;
    assert!(
        why.contains(&format!(
            "{} is not known to have landed: it could not be linked within the link bound",
            p.id()
        )),
        "{why}"
    );
}

/// The carried landing: a predecessor that spends a coin the topic holds is
/// landed first; its heap-meter trip is the landing's "not now".
#[tokio::test]
#[cfg_attr(
    debug_assertions,
    ignore = "release only: cargo test --release --features memory-storage --test p0812_link_heap_trips"
)]
async fn p0812_t_the_landing_waits_on_a_heap_meter_trip() {
    let (g, p) = &wide_parent();
    let s = spend(&[(p.id(), 0)], 2);
    stopped_at_intake(&beef_of(&[g, p, &s]), SubmitMode::CurrentTx).await;
    let storage = MemoryStorage::new();
    storage
        .insert_output(&Output {
            txid: g.id(),
            output_index: 0,
            output_script: vec![0x51],
            satoshis: 1,
            topic: TOPIC.into(),
            spent: false,
            outputs_consumed: vec![],
            consumed_by: vec![],
            beef: None,
            block_height: None,
            score: Some(1.0),
        })
        .await
        .unwrap();
    let engine = lifted(
        Naming {
            deep: false,
            admits: true,
            names: true,
        },
        storage,
    );
    let why = question(&engine, beef_of(&[g, p, &s])).await;
    assert!(
        why.contains(&format!(
            "{}: its body could not be linked within the link bound",
            p.id()
        )),
        "{why}"
    );
}

/// The no-spv submit: the subject's heap-meter trip goes on with the subject
/// unlinked, reported (`OverLink`, the meter's words), never "not now".
#[tokio::test]
#[cfg_attr(
    debug_assertions,
    ignore = "release only: cargo test --release --features memory-storage --test p0812_link_heap_trips"
)]
async fn p0812_t_the_no_spv_submit_goes_on_over_a_heap_meter_trip() {
    let (_, p) = &wide_parent();
    stopped_at_intake(&beef_of(&[p]), SubmitMode::HistoricalTxNoSpv).await;
    let tagged = TaggedBEEF::new(beef_of(&[p]), vec![TOPIC.into()]);
    let engine = lifted(
        Naming {
            deep: false,
            admits: true,
            names: false,
        },
        MemoryStorage::new(),
    );
    let (steak, report) = engine
        .submit_with_report(&tagged, SubmitMode::HistoricalTxNoSpv)
        .await
        .expect("no-spv goes on");
    assert!(steak
        .get(TOPIC)
        .is_some_and(|a| a.outputs_to_admit == vec![0]));
    assert_eq!(report.applied_topics, vec![TOPIC.to_string()]);
    let stop = report
        .walk_could_not_run
        .as_ref()
        .expect("the link stop is reported");
    assert_eq!(stop.limb, WalkLimb::OverLink);
    assert!(by_the_heap_meter(&stop.what), "{}", stop.what);
    assert_eq!(report.walk_went_on, Some(WalkWentOn::NoSpvMode));
}

/// The foreign-node read: a stored body whose link the heap meter declines is
/// read directly (the asked txid, nothing linked), never a parse error.
#[tokio::test]
#[cfg_attr(
    debug_assertions,
    ignore = "release only: cargo test --release --features memory-storage --test p0812_link_heap_trips"
)]
async fn p0812_t_the_foreign_node_read_answers_over_a_heap_meter_trip() {
    let (_, p) = &wide_parent();
    let storage = MemoryStorage::new();
    storage
        .insert_output(&Output {
            txid: p.id(),
            output_index: 0,
            output_script: vec![0x51],
            satoshis: 1,
            topic: TOPIC.into(),
            spent: false,
            outputs_consumed: vec![],
            consumed_by: vec![],
            beef: Some(beef_of(&[p])),
            block_height: None,
            score: Some(1.0),
        })
        .await
        .unwrap();
    let engine = engine_with(
        Naming {
            deep: false,
            admits: true,
            names: true,
        },
        storage,
    );
    let node = engine
        .provide_foreign_gasp_node(&format!("{}.0", p.id()), &p.id(), 0)
        .await
        .expect("the direct read answers");
    assert_eq!(Transaction::from_hex(&node.raw_tx).unwrap().id(), p.id());
}

/// The GASP anchor does not reach its link on such a graph. Since lane
/// INTAKE-BOUND it does not reach its walk either: the root node's proof is
/// read under the proof limb's default (`beef_limits::proof_from_hex`), which
/// stops the wide path at its frame, and the graph is HELD there
/// (`AnchorWalkHeld`, `over_proof`, the cursor below it). Before the lane the
/// root's walk held it at its memory limb (48 MiB) after the path was parsed.
/// Stated: no shape trips the heap meter at the anchor's link.
#[tokio::test]
#[cfg_attr(
    debug_assertions,
    ignore = "release only: cargo test --release --features memory-storage --test p0812_link_heap_trips"
)]
async fn p0812_t_the_anchor_holds_such_a_graph_at_its_walk_first() {
    let (_, p) = &wide_parent();
    let store = MemoryStorage::new();
    let manager = Naming {
        deep: false,
        admits: true,
        names: true,
    };
    let gasp = OverlayGASPStorage::new(&store, TOPIC, new_finalized_graph_sink())
        .with_topic_manager(&manager);
    let root = format!("{}.0", p.id());
    let node = GASPNode {
        graph_id: root.clone(),
        raw_tx: p.to_hex(),
        output_index: 0,
        proof: p.merkle_path.as_ref().map(MerklePath::to_hex),
        tx_metadata: None,
        output_metadata: None,
        inputs: None,
    };
    gasp.append_to_graph(&node, None).await.unwrap();
    match gasp.validate_graph_anchor(&root).await {
        Err(GASPError::AnchorWalkHeld(why)) => {
            assert!(
                why.contains("could not be read within the budget (over_proof"),
                "the proof limb holds it at the node's proof, before the walk: {why}"
            );
        }
        other => panic!("the graph is held: {other:?}"),
    }
}
