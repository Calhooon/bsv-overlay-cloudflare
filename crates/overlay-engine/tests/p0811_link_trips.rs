//! PORT-0811: a link trip is never a verdict, at every door that links.
//!
//! Each pin builds a body whose link is past the engine's link bound
//! (`beef_limits::LINK_HEAP_MAX_BYTES`: a proven grandparent of 1,000 `OP_1`
//! outputs, 100 of them spent by one transaction) and reads the door's answer:
//! the predecessor question's names call, its dry run, the carried landing,
//! the foreign-node read, the GASP anchor's replay and the no-spv submit.
//! Each answer names the link bound in that door's own words; a revert of the
//! door's arm alone changes it.
// bsv-stack-lean #134: test code
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use async_trait::async_trait;
use bsv_overlay_engine::builder::EngineBuilder;
use bsv_overlay_engine::engine::{Engine, EngineError, WalkBreachPolicy, WalkLimb, WalkWentOn};
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

const TOPIC: &str = "tm_p0811";
const OUTPUTS: u32 = 1_000;
const FAN: u32 = 100;

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

/// The grandparent: proven at 900,000, `OUTPUTS` outputs of `OP_1`.
fn grandparent() -> Transaction {
    let mut tx = Transaction::new();
    tx.inputs.push(spending(&"aa".repeat(32), 0));
    for _ in 0..OUTPUTS {
        tx.outputs.push(TransactionOutput::new(1, one()));
    }
    let txid = tx.id();
    tx.merkle_path =
        Some(MerklePath::new(900_000, vec![vec![MerklePathLeaf::new_txid(0, txid)]]).unwrap());
    tx
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

/// The fan: one transaction spending the grandparent's first `FAN` outputs.
fn fan(g: &Transaction) -> Transaction {
    let id = g.id();
    spend(&(0..FAN).map(|v| (id.clone(), v)).collect::<Vec<_>>(), 1)
}

fn beef_of(txs: &[&Transaction]) -> Vec<u8> {
    let mut beef = Beef::new();
    for tx in txs {
        let bump = tx.merkle_path.clone().map(|m| beef.merge_bump(m));
        beef.merge_raw_tx(tx.to_binary(), bump);
    }
    beef.to_binary()
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

/// Submits `beef` under the network's accept (the subject's own link trip
/// goes on) and returns the predecessor question's fault text.
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
    assert_eq!(
        report.walk_could_not_run.map(|s| s.limb),
        Some(WalkLimb::OverLink),
        "the subject's own link tripped and went on"
    );
    report
        .faults
        .iter()
        .find(|f| f.site == "predecessor_not_landed")
        .map(|f| f.error.clone())
        .unwrap_or_else(|| panic!("the topic waits: {:?}", report.faults))
}

/// The dry run (the E1D opener): the candidate's link trip makes the question
/// wait, in its own words; never the candidate's body read unlinked.
#[tokio::test]
async fn p0811_t_the_dry_run_waits_on_a_link_trip() {
    let g = grandparent();
    let p = fan(&g);
    let s = spend(&[(p.id(), 0)], 2);
    let engine = engine_with(
        Naming {
            deep: false,
            admits: true,
            names: true,
        },
        MemoryStorage::new(),
    );
    let why = question(&engine, beef_of(&[&g, &p, &s])).await;
    assert!(
        why.contains(&format!(
            "{} is not known to have landed: it could not be linked within the link bound",
            p.id()
        )),
        "{why}"
    );
}

/// The names call over a spender that is not the subject: its link trip makes
/// the question wait; never "names nothing" (which records the successor).
#[tokio::test]
async fn p0811_t_the_names_call_waits_on_a_link_trip() {
    let g = grandparent();
    let p = fan(&g);
    let q = spend(&[(p.id(), 0)], 2);
    let s = spend(&[(q.id(), 0)], 3);
    let engine = engine_with(
        Naming {
            deep: true,
            admits: false,
            names: true,
        },
        MemoryStorage::new(),
    );
    let why = question(&engine, beef_of(&[&g, &p, &q, &s])).await;
    assert!(
        why.contains(&format!(
            "its spender {} could not be linked within the link bound",
            q.id()
        )),
        "{why}"
    );
}

/// The carried landing: a predecessor that spends coins the topic holds is
/// landed first; its link trip is the landing's "not now", never "not in the
/// BEEF".
#[tokio::test]
async fn p0811_t_the_landing_waits_on_a_link_trip() {
    let g = grandparent();
    let p = fan(&g);
    let s = spend(&[(p.id(), 0)], 2);
    let storage = MemoryStorage::new();
    for vout in 0..FAN {
        storage
            .insert_output(&Output {
                txid: g.id(),
                output_index: vout,
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
    }
    let engine = engine_with(
        Naming {
            deep: false,
            admits: true,
            names: true,
        },
        storage,
    );
    let why = question(&engine, beef_of(&[&g, &p, &s])).await;
    assert!(
        why.contains(&format!(
            "{}: its body could not be linked within the link bound",
            p.id()
        )),
        "{why}"
    );
}

/// The no-spv submit (the gated arm's admission, a GASP finalize): a link trip
/// goes on with the subject unlinked and is reported, under the default
/// policy; never "not now".
#[tokio::test]
async fn p0811_t_the_no_spv_submit_goes_on_over_a_link_trip() {
    let g = grandparent();
    let p = fan(&g);
    let tagged = TaggedBEEF::new(beef_of(&[&g, &p]), vec![TOPIC.into()]);
    let engine = engine_with(
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
    assert_eq!(
        report.walk_could_not_run.as_ref().map(|s| s.limb),
        Some(WalkLimb::OverLink)
    );
    // The mode alone claims no word of the network: the report says the
    // subject was read unlinked in no-spv, and the line's words say so.
    assert_eq!(report.walk_went_on, Some(WalkWentOn::NoSpvMode));
    assert!(!WalkWentOn::NoSpvMode.words().contains("network accepted"));
    assert!(WalkWentOn::NoSpvMode
        .words()
        .starts_with("no-spv: the subject is read unlinked"));
    // The caller's policy is what names the network's accept, in any mode.
    for mode in [SubmitMode::HistoricalTxNoSpv, SubmitMode::HistoricalTx] {
        let (_, report) = engine_with(
            Naming {
                deep: false,
                admits: true,
                names: false,
            },
            MemoryStorage::new(),
        )
        .submit_with_report_under(&tagged, mode, WalkBreachPolicy::NetworkAccepted)
        .await
        .expect("the caller's policy goes on");
        assert_eq!(
            report.walk_could_not_run.as_ref().map(|s| s.limb),
            Some(WalkLimb::OverLink)
        );
        assert_eq!(report.walk_went_on, Some(WalkWentOn::NetworkAccepted));
    }
    assert!(WalkWentOn::NetworkAccepted
        .words()
        .contains("the network accepted these bytes"));
    // Any other mode under the default policy is "not now".
    match engine_with(
        Naming {
            deep: false,
            admits: true,
            names: false,
        },
        MemoryStorage::new(),
    )
    .submit_with_report(&tagged, SubmitMode::HistoricalTx)
    .await
    {
        Err(EngineError::WalkCouldNotRun(stop)) => assert_eq!(stop.limb, WalkLimb::OverLink),
        other => panic!("historical-tx is not now: {other:?}"),
    }
}

/// The foreign-node read: a stored body whose link trips is read directly
/// (the asked txid, nothing linked), never a parse error.
#[tokio::test]
async fn p0811_t_the_foreign_node_read_answers_over_a_link_trip() {
    let g = grandparent();
    let p = fan(&g);
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
            beef: Some(beef_of(&[&g, &p])),
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

/// The GASP anchor's replay: a node whose link trips HOLDS the graph (the
/// cursor stays below it), never a refusal that discards it.
#[tokio::test]
async fn p0811_t_the_anchor_replay_holds_over_a_link_trip() {
    let g = grandparent();
    let p = fan(&g);
    let store = MemoryStorage::new();
    let manager = Naming {
        deep: false,
        admits: true,
        names: true,
    };
    let gasp = OverlayGASPStorage::new(&store, TOPIC, new_finalized_graph_sink())
        .with_topic_manager(&manager);
    let root = format!("{}.0", p.id());
    let node = |tx: &Transaction, vout: u32, proof: Option<String>| GASPNode {
        graph_id: root.clone(),
        raw_tx: tx.to_hex(),
        output_index: vout,
        proof,
        tx_metadata: None,
        output_metadata: None,
        inputs: None,
    };
    gasp.append_to_graph(&node(&p, 0, None), None)
        .await
        .unwrap();
    let proof = g.merkle_path.as_ref().map(MerklePath::to_hex);
    for vout in 0..FAN {
        gasp.append_to_graph(&node(&g, vout, proof.clone()), Some(&root))
            .await
            .unwrap();
    }
    match gasp.validate_graph_anchor(&root).await {
        Err(GASPError::AnchorWalkHeld(why)) => {
            assert!(
                why.contains("could not be linked within the link bound"),
                "{why}"
            )
        }
        other => panic!("the graph is held: {other:?}"),
    }
}
