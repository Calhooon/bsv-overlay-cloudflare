//! PORT-0811: what bsv-rs 0.8.10 and 0.8.11 ask of this engine as a caller.
//!
//! - The BEEF link bound (0.8.11, GHSA-gvmh-f5gf-5vjj): linking a subject's
//!   ancestry re-materializes a shared parent once per spending input; the
//!   engine bounds a link in heap (`beef_limits::LINK_HEAP_MAX_BYTES`, bsv-rs's
//!   wire meter its belt), a trip `Error::BeefResourceLimit`.
//!   The engine's linking door names a trip apart from an invalid BEEF and an
//!   absent txid (`beef_limits::is_link_bound`), reads the subject alone where
//!   it needs no ancestry (`beef_limits::subject_from_beef`), and at the submit
//!   a trip is the submit's `WalkCouldNotRun` (limb `over_link`): "not now"
//!   under `NotNow`, never a refusal. LOW's real shapes link under the bound.
//! - The coin's era (0.8.10, GHSA-fr33-7jw7-mwwh and GHSA-h4m7-c6p8-725m, the
//!   migration's steps 7 and 8 for a direct caller of `Spend`): the script door
//!   declines (the network judges; the submit's "not now") a lock of the
//!   pay-to-script-hash shape, and a coin proven below the Genesis height whose
//!   spend is not one both eras judge alike.
//! - A stored body read by its txid (GHSA-gg5p-wgff-v3hr): a BEEF a 0.7.2
//!   build stored can end with a proven ancestor; the GASP node read names the
//!   txid it is asked for.
// bsv-stack-lean #134: test code
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use async_trait::async_trait;
use bsv_overlay_engine::beef_limits;
use bsv_overlay_engine::builder::EngineBuilder;
use bsv_overlay_engine::engine::{Engine, EngineError, WalkBreachPolicy};
use bsv_overlay_engine::gasp::GASPStorage;
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

struct AdmitOutputZero;

#[async_trait(?Send)]
impl TopicManager for AdmitOutputZero {
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
            name: "p0811".into(),
            ..Default::default()
        }
    }
}

fn engine() -> Engine {
    EngineBuilder::new(Box::new(MemoryStorage::new()))
        .with_topic(TOPIC, Box::new(AdmitOutputZero))
        .build()
}

fn script(bytes: &[u8]) -> Script {
    Script::from_binary(bytes).expect("a script")
}

/// A transaction of one input from an absent txid and these outputs, proven
/// by a one-leaf BUMP at `height`.
fn proven_funding(outputs: Vec<TransactionOutput>, height: u32) -> Transaction {
    let mut tx = Transaction::new();
    tx.inputs.push(TransactionInput {
        source_txid: Some("aa".repeat(32)),
        source_output_index: 0,
        unlocking_script: Some(UnlockingScript::from_script(Script::new())),
        ..Default::default()
    });
    tx.outputs = outputs;
    let txid = tx.id();
    tx.merkle_path = Some(
        MerklePath::new(height, vec![vec![MerklePathLeaf::new_txid(0, txid)]])
            .expect("a one-leaf BUMP"),
    );
    tx
}

/// A spend of `spent` (vouts) of `parent`, each unlocked by `unlock`, paying
/// `sats` to `OP_1`.
fn spend_of(parent: &Transaction, spent: &[u32], unlock: &[u8], sats: u64) -> Transaction {
    let mut tx = Transaction::new();
    let parent_txid = parent.id();
    for vout in spent {
        tx.inputs.push(TransactionInput {
            source_txid: Some(parent_txid.clone()),
            source_output_index: *vout,
            unlocking_script: Some(UnlockingScript::from_script(script(unlock))),
            ..Default::default()
        });
    }
    tx.outputs.push(TransactionOutput::new(
        sats,
        LockingScript::from_script(script(&[0x51])),
    ));
    tx
}

/// A BEEF of a proven parent and its unproven spender, in that order.
fn beef_of(parent: &Transaction, child: &Transaction) -> Vec<u8> {
    let mut beef = Beef::new();
    let bump = beef.merge_bump(parent.merkle_path.clone().expect("a BUMP"));
    beef.merge_raw_tx(parent.to_binary(), Some(bump));
    beef.merge_raw_tx(child.to_binary(), None);
    beef.to_binary()
}

/// The link bound's shape (GHSA-gvmh-f5gf-5vjj): one proven parent of `n`
/// `OP_1` outputs of 1 sat, and one child spending every one of them. Each
/// input after the first re-materializes the whole parent (about 10 bytes an
/// output), so 1,400 of each is past the engine's link bound; the body itself is about 72 KB. Every script runs (`OP_1` under an
/// empty unlock), so the walk passes and only the link trips.
fn shared_parent(n: u32) -> (Vec<u8>, String) {
    let outputs = (0..n)
        .map(|_| TransactionOutput::new(1, LockingScript::from_script(script(&[0x51]))))
        .collect();
    let parent = proven_funding(outputs, 900_000);
    let child = spend_of(&parent, &(0..n).collect::<Vec<_>>(), &[], 1);
    (beef_of(&parent, &child), child.id())
}

/// THE PIN (b): a link-bound trip is its own class, never an invalid BEEF and
/// never an absent txid, and the subject alone reads through it.
#[test]
fn p0811_b_a_a_link_trip_is_named_apart_and_the_subject_reads_alone() {
    let (beef, child) = shared_parent(1_400);
    let linked =
        beef_limits::transaction_from_beef(&beef, Some(&child), &beef_limits::ENGINE_BEEF_LIMITS);
    let err = linked.expect_err("the engine's link bound declines the shared-parent shape");
    assert!(
        beef_limits::is_link_bound(&err),
        "a link trip is the link bound's class: {err}"
    );
    let parsed = beef_limits::parse_beef(&beef, &beef_limits::ENGINE_BEEF_LIMITS).unwrap();
    assert!(
        beef_limits::linked(&parsed, &child).is_err(),
        "the parsed BEEF's link names the trip, never None"
    );
    assert!(
        matches!(beef_limits::linked(&parsed, &"ab".repeat(32)), Ok(None)),
        "an absent txid is Ok(None)"
    );
    let subject =
        beef_limits::subject_from_beef(&beef, Some(&child), &beef_limits::ENGINE_BEEF_LIMITS)
            .expect("the subject alone reads: nothing is linked");
    assert_eq!(subject.id(), child);
    assert_eq!(subject.inputs.len(), 1_400);
    assert!(subject
        .inputs
        .iter()
        .all(|i| i.source_transaction.is_none()));
}

/// THE PIN (b), LOW's shapes: every real BEEF the tree holds links under the
/// default bound (the JOIN of two seats, its re-presentation, Zanaadu's
/// proven head, the covenant legs' parent) and reads alone the same subject.
#[test]
fn p0811_b_b_lows_real_shapes_link_under_the_default() {
    let read = |rel: &str| -> Vec<u8> {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(rel);
        let raw = std::fs::read(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        if rel.ends_with(".hex") {
            hex::decode(String::from_utf8(raw).unwrap().trim()).unwrap()
        } else {
            raw
        }
    };
    let fixtures = [
        "../overlay-cloudflare/tests/fixtures/ef/loop2_join_5ad2764c_incomplete.beef",
        "../overlay-cloudflare/tests/fixtures/ef/loop2_join_f9e85aab_represent.beef",
        "../overlay-cloudflare/tests/fixtures/ef/parent_a7d76588_beef.hex",
        "tests/fixtures/zanaadu-pf-head-dc4ca9ea.beef",
        "../../tools/lane-script/fixtures/valid.beef.hex",
    ];
    for rel in fixtures {
        let bytes = read(rel);
        let parsed = beef_limits::parse_beef(&bytes, &beef_limits::STORED_BEEF_LIMITS).unwrap();
        for entry in &parsed.txs {
            let Some(tx) = entry.tx() else { continue };
            let txid = tx.id();
            let linked = beef_limits::transaction_from_beef(
                &bytes,
                Some(&txid),
                &beef_limits::STORED_BEEF_LIMITS,
            )
            .unwrap_or_else(|e| panic!("{rel} {txid}: {e}"));
            let alone = beef_limits::subject_from_beef(
                &bytes,
                Some(&txid),
                &beef_limits::STORED_BEEF_LIMITS,
            )
            .unwrap();
            assert_eq!(linked.id(), alone.id(), "{rel}");
            assert_eq!(linked.to_binary(), alone.to_binary(), "{rel}");
            assert_eq!(
                linked.merkle_path.as_ref().map(MerklePath::to_hex),
                alone.merkle_path.as_ref().map(MerklePath::to_hex),
                "{rel}: the subject's own proof is attached as the link attaches it"
            );
        }
    }
}

/// THE PIN (b) at the submit: a link trip is "not now" under `NotNow` (never a
/// refusal, never an admission), and under `NetworkAccepted` the submit goes
/// on with its subject unlinked and says so. On the base (bsv-rs 0.7.2, no
/// bound) the submit linked the 19.6 MB of repeats and admitted.
#[tokio::test]
async fn p0811_b_c_the_submit_reads_a_link_trip_as_not_now() {
    let (beef, child) = shared_parent(1_400);
    let tagged = TaggedBEEF::new(beef, vec![TOPIC.into()]);
    let not_now = engine()
        .submit_with_report_under(&tagged, SubmitMode::CurrentTx, WalkBreachPolicy::NotNow)
        .await;
    match not_now {
        Err(EngineError::WalkCouldNotRun(stop)) => {
            assert_eq!(stop.limb.as_str(), "over_link");
            assert_eq!(stop.subject_txid, child);
        }
        other => panic!("a link trip is not now: {other:?}"),
    }
    let went_on = engine()
        .submit_with_report_under(
            &tagged,
            SubmitMode::CurrentTx,
            WalkBreachPolicy::NetworkAccepted,
        )
        .await
        .expect("under the network's accept the submit goes on");
    let stop = went_on
        .1
        .walk_could_not_run
        .expect("the report names the link trip");
    assert_eq!(stop.limb.as_str(), "over_link");
    assert!(went_on
        .0
        .get(TOPIC)
        .is_some_and(|a| a.outputs_to_admit == vec![0]));
}

/// The pay-to-script-hash shape's spend: the redeem script `OP_0` pushed alone
/// (the node runs it and refuses; `Spend` checks the hash and answers true).
fn p2sh_spend() -> (Vec<u8>, String) {
    let redeem = [0x00u8];
    let mut lock = vec![0xa9, 0x14];
    lock.extend_from_slice(&bsv_rs::primitives::hash::hash160(&redeem));
    lock.push(0x87);
    let parent = proven_funding(
        vec![TransactionOutput::new(
            1_000,
            LockingScript::from_script(script(&lock)),
        )],
        800_000,
    );
    let child = spend_of(&parent, &[0], &[0x01, 0x00], 900);
    (beef_of(&parent, &child), child.id())
}

/// A coin proven at `height` locked `OP_DROP OP_1`, spent by a 521-byte push:
/// the post-Genesis rules accept it, the rules of a coin from before Genesis
/// refuse it (a push above 520 bytes).
fn big_push_spend(height: u32) -> (Vec<u8>, String) {
    let parent = proven_funding(
        vec![TransactionOutput::new(
            1_000,
            LockingScript::from_script(script(&[0x75, 0x51])),
        )],
        height,
    );
    let mut unlock = vec![0x4d, 0x09, 0x02];
    unlock.extend(std::iter::repeat_n(0x42u8, 521));
    let child = spend_of(&parent, &[0], &unlock, 900);
    (beef_of(&parent, &child), child.id())
}

fn declined(answer: Result<bsv_overlay_engine::engine::WalkStats, EngineError>) -> String {
    match answer {
        Err(EngineError::ScriptWalkOverBudget { what, .. }) => what,
        other => panic!("the door declines (the network judges): {other:?}"),
    }
}

/// THE PIN (c): the script door declines what `Spend` cannot judge as the node
/// does. On the base both spends walked `Ok`.
#[tokio::test]
async fn p0811_c_a_the_door_declines_a_p2sh_lock_and_a_pre_genesis_spend_it_cannot_judge() {
    let engine = engine();
    let (beef, txid) = p2sh_spend();
    let why = declined(engine.verify_scripts_only(&beef, &txid).await);
    assert!(why.contains("pay-to-script-hash"), "{why}");

    let (beef, txid) = big_push_spend(600_000);
    let why = declined(engine.verify_scripts_only(&beef, &txid).await);
    assert!(why.contains("below the Genesis height"), "{why}");

    // The same spend of a coin proven AT the Genesis height is walked.
    let (beef, txid) = big_push_spend(620_538);
    engine
        .verify_scripts_only(&beef, &txid)
        .await
        .expect("a post-Genesis coin is judged by the interpreter");

    // A pre-Genesis coin whose spend both eras judge alike is walked.
    let parent = proven_funding(
        vec![TransactionOutput::new(
            1_000,
            LockingScript::from_script(script(&[0x51])),
        )],
        600_000,
    );
    let child = spend_of(&parent, &[0], &[], 900);
    engine
        .verify_scripts_only(&beef_of(&parent, &child), &child.id())
        .await
        .expect("an era-neutral spend of an old coin is judged");
}

/// THE PIN (c) at the submit: the era's decline is "not now", never an
/// admission. On the base the submit admitted the spend the node refuses.
#[tokio::test]
async fn p0811_c_b_the_submit_reads_the_eras_decline_as_not_now() {
    let (beef, txid) = p2sh_spend();
    let tagged = TaggedBEEF::new(beef, vec![TOPIC.into()]);
    match engine()
        .submit_with_report_under(&tagged, SubmitMode::CurrentTx, WalkBreachPolicy::NotNow)
        .await
    {
        Err(EngineError::WalkCouldNotRun(stop)) => {
            assert_eq!(stop.subject_txid, txid);
            assert!(stop.what.contains("pay-to-script-hash"), "{}", stop.what);
            assert_eq!(stop.limb.as_str(), "coin_era");
        }
        other => panic!("the era's decline is not now: {other:?}"),
    }
}

/// THE PIN (open item 1): a stored body that ENDS with a proven ancestor (the
/// order a 0.7.2 `to_beef` could write, GHSA-gg5p-wgff-v3hr) is read for the
/// txid it is stored under. On the base the GASP node read named no txid and
/// answered the ancestor's raw transaction.
#[tokio::test]
async fn p0811_n1_a_stored_body_ending_with_a_proven_ancestor_reads_its_own_txid() {
    let parent = proven_funding(
        vec![TransactionOutput::new(
            1_000,
            LockingScript::from_script(script(&[0x51])),
        )],
        800_000,
    );
    let child = spend_of(&parent, &[0], &[], 900);
    // Written by hand: `Beef::to_binary` sorts a source before its spender.
    // BEEF V1: the version, one BUMP, two transactions, each its raw bytes
    // and its BUMP flag (and index).
    let mut stored = vec![0x01, 0x00, 0xbe, 0xef, 0x01];
    stored.extend(parent.merkle_path.clone().unwrap().to_binary());
    stored.push(0x02);
    stored.extend(child.to_binary());
    stored.push(0x00);
    stored.extend(parent.to_binary());
    stored.extend([0x01, 0x00]);
    // The premise: the bytes end with the proven ancestor.
    let wire = beef_limits::read_beef(&stored).unwrap();
    assert_eq!(wire.last_txid.as_deref(), Some(parent.id().as_str()));

    let storage = MemoryStorage::new();
    storage
        .insert_output(&Output {
            txid: child.id(),
            output_index: 0,
            output_script: vec![0x51],
            satoshis: 900,
            topic: TOPIC.into(),
            spent: false,
            outputs_consumed: vec![],
            consumed_by: vec![],
            beef: Some(stored),
            block_height: None,
            score: Some(1.0),
        })
        .await
        .unwrap();
    let gasp = OverlayGASPStorage::new(&storage, TOPIC, new_finalized_graph_sink());
    let graph_id = format!("{}.0", child.id());
    let node = gasp
        .hydrate_gasp_node(&graph_id, &child.id(), 0, false)
        .await
        .expect("the stored node");
    let read = Transaction::from_hex(&node.raw_tx).unwrap();
    assert_eq!(read.id(), child.id(), "the node is the txid asked for");
    assert!(
        node.proof.is_none(),
        "the unproven subject carries no proof"
    );

    // A stored body of TWO tips, asked for the tip the subject rule does not
    // pick: only the name selects it, at both of the node read's lookups (the
    // output held under the read's topic, and under another).
    let parent = proven_funding(
        vec![
            TransactionOutput::new(1_000, LockingScript::from_script(script(&[0x51]))),
            TransactionOutput::new(1_000, LockingScript::from_script(script(&[0x51]))),
        ],
        800_000,
    );
    let tips = [
        spend_of(&parent, &[0], &[], 900),
        spend_of(&parent, &[1], &[], 900),
    ];
    let mut beef = Beef::new();
    let bump = beef.merge_bump(parent.merkle_path.clone().unwrap());
    beef.merge_raw_tx(parent.to_binary(), Some(bump));
    for tip in &tips {
        beef.merge_raw_tx(tip.to_binary(), None);
    }
    let stored = beef.to_binary();
    let mut parsed = beef_limits::parse_beef(&stored, &beef_limits::STORED_BEEF_LIMITS).unwrap();
    let by_rule = bsv_overlay_engine::subject::subject_txid_of(&mut parsed).unwrap();
    let asked = tips.iter().find(|t| t.id() != by_rule).unwrap();
    for held_under in [TOPIC, "tm_other"] {
        let storage = MemoryStorage::new();
        storage
            .insert_output(&Output {
                txid: asked.id(),
                output_index: 0,
                output_script: vec![0x51],
                satoshis: 900,
                topic: held_under.into(),
                spent: false,
                outputs_consumed: vec![],
                consumed_by: vec![],
                beef: Some(stored.clone()),
                block_height: None,
                score: Some(1.0),
            })
            .await
            .unwrap();
        let gasp = OverlayGASPStorage::new(&storage, TOPIC, new_finalized_graph_sink());
        let node = gasp
            .hydrate_gasp_node(&format!("{}.0", asked.id()), &asked.id(), 0, false)
            .await
            .expect("the stored node");
        assert_eq!(
            Transaction::from_hex(&node.raw_tx).unwrap().id(),
            asked.id(),
            "held under {held_under}: the node is the txid asked for, not the subject rule's {by_rule}"
        );
    }
}

/// A body that ENDS with its subject's proven ancestor, written by hand
/// (`Beef::to_binary` sorts a source before its spender): the order a 0.7.2
/// `to_beef` could store, and any order a stranger may send.
fn ending_with_its_ancestor() -> (Vec<u8>, Transaction, Transaction) {
    let parent = proven_funding(
        vec![TransactionOutput::new(
            1_000,
            LockingScript::from_script(script(&[0x51])),
        )],
        800_000,
    );
    let child = spend_of(&parent, &[0], &[], 900);
    let mut body = vec![0x01, 0x00, 0xbe, 0xef, 0x01];
    body.extend(parent.merkle_path.clone().unwrap().to_binary());
    body.push(0x02);
    body.extend(child.to_binary());
    body.push(0x00);
    body.extend(parent.to_binary());
    body.extend([0x01, 0x00]);
    (body, parent, child)
}

/// PORT-0811 fold: a read that names no txid takes the house's ONE subject
/// rule (`subject::subject_txid_of`: the atomic name, then the unique tip,
/// then the reference's sorted-last), never the wire-last entry. Over a body
/// that ends with its subject's proven ancestor, both the linked read and the
/// subject-alone read answer the subject; an atomic body answers its name;
/// a body of two tips answers the subject rule's sorted-last. On the base both
/// reads answered the ancestor (`txs.last()`).
#[test]
fn p0811_f3_a_read_naming_no_txid_takes_the_subject_rule() {
    let (body, parent, child) = ending_with_its_ancestor();
    let wire = beef_limits::read_beef(&body).unwrap();
    assert_eq!(
        wire.last_txid.as_deref(),
        Some(parent.id().as_str()),
        "the premise"
    );
    let alone = beef_limits::subject_from_beef(&body, None, &beef_limits::STORED_BEEF_LIMITS)
        .expect("reads");
    assert_eq!(
        alone.id(),
        child.id(),
        "the subject, not the wire-last ancestor"
    );
    let linked = beef_limits::transaction_from_beef(&body, None, &beef_limits::STORED_BEEF_LIMITS)
        .expect("links");
    assert_eq!(linked.id(), child.id());
    // An atomic body is read by its name, whatever its graph.
    let mut atomic = Beef::new();
    let bump = atomic.merge_bump(parent.merkle_path.clone().unwrap());
    atomic.merge_raw_tx(parent.to_binary(), Some(bump));
    atomic.merge_raw_tx(child.to_binary(), None);
    let atomic_bytes = atomic.to_binary_atomic(&child.id()).unwrap();
    assert_eq!(
        beef_limits::subject_from_beef(&atomic_bytes, None, &beef_limits::STORED_BEEF_LIMITS)
            .unwrap()
            .id(),
        child.id()
    );
    // Two unrelated tips: the subject rule's sorted-last, the same txid the
    // engine's admission names.
    let other = spend_of(&parent, &[0], &[0x51], 800);
    let mut two = Beef::new();
    let bump = two.merge_bump(parent.merkle_path.clone().unwrap());
    two.merge_raw_tx(parent.to_binary(), Some(bump));
    two.merge_raw_tx(child.to_binary(), None);
    two.merge_raw_tx(other.to_binary(), None);
    let two_bytes = two.to_binary();
    let mut parsed = Beef::from_binary(&two_bytes).unwrap();
    let rule = bsv_overlay_engine::subject::subject_txid_of(&mut parsed).unwrap();
    assert_eq!(
        beef_limits::subject_from_beef(&two_bytes, None, &beef_limits::STORED_BEEF_LIMITS)
            .unwrap()
            .id(),
        rule
    );
}

/// One spend of a coin proven at 600,000 (below Genesis): the door's verdict,
/// and bsv-rs's own verifier under the coin's era (`Transaction::verify`, the
/// source's BUMP at its height). `None` when the door declines it (the
/// network judges); `Some((door judged it valid, bsv-rs judged it valid))`.
async fn door_and_sdk(parent: &Transaction, child: &Transaction) -> Option<(bool, bool)> {
    let beef = beef_of(parent, child);
    let txid = child.id();
    let door = engine().verify_scripts_only(&beef, &txid).await;
    if let Err(EngineError::ScriptWalkOverBudget { limb, what, .. }) = &door {
        assert_eq!(
            *limb,
            bsv_overlay_engine::engine::DoorLimb::Era,
            "only the era's decline here: {what}"
        );
        return None;
    }
    let door_valid = match &door {
        Ok(stats) => {
            assert!(stats.subject_judged, "{stats:?}");
            true
        }
        Err(EngineError::ScriptVerificationFailed { .. }) => false,
        Err(other) => panic!("the door answers a verdict or the era's decline: {other}"),
    };
    let tracker = bsv_rs::transaction::MockChainTracker::always_valid(900_000);
    let sdk = Transaction::from_beef(&beef, Some(&txid))
        .unwrap()
        .verify(&tracker, None)
        .await;
    Some((door_valid, matches!(sdk, Ok(true))))
}

/// The signed spend of `lock` (output 0 of a coin proven at 600,000) by
/// `template`, at `version`, with `junk` prepended to the signed unlock.
async fn signed_spend(
    lock: LockingScript,
    template: bsv_rs::script::ScriptTemplateUnlock,
    version: u32,
    junk: &[u8],
) -> (Transaction, Transaction) {
    let parent = proven_funding(vec![TransactionOutput::new(10_000, lock)], 600_000);
    let mut child = spend_of(&parent, &[0], &[], 9_000);
    child.version = version;
    child.inputs[0].source_transaction = Some(Box::new(parent.clone()));
    child.inputs[0].unlocking_script = None;
    child.inputs[0].unlocking_script_template = Some(Box::new(template));
    child.sign().await.unwrap();
    let signed = child.inputs[0]
        .unlocking_script
        .clone()
        .unwrap()
        .to_binary();
    let mut unlock = junk.to_vec();
    unlock.extend_from_slice(&signed);
    child.inputs[0].unlocking_script = Some(UnlockingScript::from_script(script(&unlock)));
    child.inputs[0].source_transaction = None;
    (parent, child)
}

/// `n` one-byte pushes (`OP_1`).
fn ones(n: usize) -> Vec<u8> {
    vec![0x51; n]
}

/// A minimal push of `len` bytes.
fn push_of(len: usize) -> Vec<u8> {
    let mut out = match len {
        0 => vec![0x00],
        1..=75 => vec![len as u8],
        76..=255 => vec![0x4c, len as u8],
        _ => vec![0x4d, (len & 0xff) as u8, (len >> 8) as u8],
    };
    out.extend(std::iter::repeat_n(0x42u8, len));
    out
}

/// THE PIN (c), the differential: for every generated spend of a coin proven
/// below the Genesis height that the door does not decline, the door's verdict
/// equals bsv-rs's own verifier under the coin's era. The shapes: P2PKH, P2PK,
/// 1-of-1, 2-of-3, 1-of-10 and 1-of-16 multisig, and push-only locks; the
/// stack's peak at and around 1,000 elements; an unlock and a lock at and
/// around 10,000 bytes; a push of 520 and 521 bytes; versions 1 and 2. The
/// lens's row (1-of-16, version 2, 987 pushes before the signature) is one of
/// them: bsv-rs refuses it, and the door does not walk it valid.
#[tokio::test]
async fn p0811_c_c_the_eras_exemption_is_held_to_the_sdks_own_verdict() {
    use bsv_rs::primitives::ec::PrivateKey;
    use bsv_rs::script::templates::{Multisig, P2PK, P2PKH};
    use bsv_rs::script::{ScriptTemplate, SignOutputs};

    let mut compared = 0usize;
    let mut declined = 0usize;
    let mut valid = 0usize;
    let mut differ = Vec::new();
    let mut check = |label: String, answer: Option<(bool, bool)>| match answer {
        None => declined += 1,
        Some((door, sdk)) => {
            compared += 1;
            valid += usize::from(sdk);
            if door != sdk {
                differ.push(format!("{label}: door {door}, bsv-rs {sdk}"));
            }
        }
    };

    // The lens's row.
    let keys: Vec<PrivateKey> = (0..16).map(|_| PrivateKey::random()).collect();
    let pubs: Vec<_> = keys.iter().map(PrivateKey::public_key).collect();
    let (p, c) = signed_spend(
        Multisig::new(1).lock_from_keys(&pubs).unwrap(),
        Multisig::unlock(&keys[..1], SignOutputs::All, false),
        2,
        &ones(987),
    )
    .await;
    let lens_row = door_and_sdk(&p, &c).await;
    assert!(
        !matches!(lens_row, Some((true, _))),
        "the lens's row is not walked valid: {lens_row:?}"
    );
    check("the lens's row".into(), lens_row);

    for version in [1u32, 2] {
        // Signed locks: (label, lock, template, the signed unlock's pushes,
        // the lock's own growth).
        type Template = bsv_rs::script::ScriptTemplateUnlock;
        type Signed = (
            String,
            LockingScript,
            Box<dyn Fn() -> Template>,
            usize,
            usize,
        );
        let mut signed: Vec<Signed> = Vec::new();
        let key = PrivateKey::random();
        let k1 = key.clone();
        signed.push((
            "P2PKH".into(),
            P2PKH::new().lock(&key.public_key().hash160()).unwrap(),
            Box::new(move || P2PKH::unlock(&k1, SignOutputs::All, false)),
            2,
            2,
        ));
        let k2 = key.clone();
        signed.push((
            "P2PK".into(),
            P2PK::new().lock(&key.public_key().to_compressed()).unwrap(),
            Box::new(move || P2PK::unlock(&k2, SignOutputs::All, false)),
            1,
            1,
        ));
        for (m, n) in [(1usize, 1usize), (2, 3), (1, 10), (1, 16)] {
            let keys: Vec<PrivateKey> = (0..n).map(|_| PrivateKey::random()).collect();
            let pubs: Vec<_> = keys.iter().map(PrivateKey::public_key).collect();
            let signers = keys[..m].to_vec();
            signed.push((
                format!("{m}-of-{n}"),
                Multisig::new(m as u8).lock_from_keys(&pubs).unwrap(),
                Box::new(move || Multisig::unlock(&signers, SignOutputs::All, false)),
                1 + m,
                n + 2,
            ));
        }
        for (label, lock, template, signed_pushes, growth) in &signed {
            // The stack's peak at 0 and around 1,000.
            let mut peaks = vec![0usize];
            peaks.extend(998..=1_002);
            for peak in peaks {
                let junk = if peak == 0 {
                    0
                } else {
                    peak - signed_pushes - growth
                };
                let (p, c) = signed_spend(lock.clone(), template(), version, &ones(junk)).await;
                check(
                    format!("{label} v{version} peak {}", junk + signed_pushes + growth),
                    door_and_sdk(&p, &c).await,
                );
            }
            // The unlock at and around 10,000 bytes; a push of 520 and 521.
            for total in [9_999usize, 10_000, 10_001] {
                let (_, probe) = signed_spend(lock.clone(), template(), version, &[]).await;
                let signed_len = probe.inputs[0]
                    .unlocking_script
                    .as_ref()
                    .unwrap()
                    .to_binary()
                    .len();
                let mut junk = Vec::new();
                for _ in 0..18 {
                    junk.extend(push_of(520));
                }
                let rest = total - junk.len() - signed_len;
                junk.extend(push_of(rest - 3));
                let (p, c) = signed_spend(lock.clone(), template(), version, &junk).await;
                assert_eq!(
                    c.inputs[0]
                        .unlocking_script
                        .as_ref()
                        .unwrap()
                        .to_binary()
                        .len(),
                    total
                );
                check(
                    format!("{label} v{version} unlock {total} bytes"),
                    door_and_sdk(&p, &c).await,
                );
            }
            for size in [520usize, 521] {
                let (p, c) = signed_spend(lock.clone(), template(), version, &push_of(size)).await;
                check(
                    format!("{label} v{version} a push of {size}"),
                    door_and_sdk(&p, &c).await,
                );
            }
        }
        // Push-only locks: `lock_pushes` of OP_1 under `unlock_pushes` of OP_1.
        for (lock_pushes, unlock_pushes) in [
            (1usize, 0usize),
            (500, 498),
            (500, 500),
            (500, 501),
            (1, 999),
            (1, 1_000),
        ] {
            let parent = proven_funding(
                vec![TransactionOutput::new(
                    1_000,
                    LockingScript::from_script(script(&ones(lock_pushes))),
                )],
                600_000,
            );
            let mut child = spend_of(&parent, &[0], &ones(unlock_pushes), 900);
            child.version = version;
            check(
                format!("push-only v{version} {lock_pushes} + {unlock_pushes}"),
                door_and_sdk(&parent, &child).await,
            );
        }
        // A push-only lock at and around 10,000 bytes, under an empty unlock.
        for total in [9_999usize, 10_000, 10_001] {
            let mut lock = Vec::new();
            for _ in 0..18 {
                lock.extend(push_of(520));
            }
            let rest = total - lock.len();
            lock.extend(push_of(rest - 3));
            assert_eq!(lock.len(), total);
            let parent = proven_funding(
                vec![TransactionOutput::new(
                    1_000,
                    LockingScript::from_script(script(&lock)),
                )],
                600_000,
            );
            let mut child = spend_of(&parent, &[0], &[], 900);
            child.version = version;
            check(
                format!("push-only lock v{version} {total} bytes"),
                door_and_sdk(&parent, &child).await,
            );
        }
    }
    println!(
        "p0811_c_c: {compared} spends compared ({valid} valid), {declined} declined, {} differ",
        differ.len()
    );
    assert!(
        differ.is_empty(),
        "the door and bsv-rs differ:\n{}",
        differ.join("\n")
    );
    assert!(compared >= 80 && valid >= 40 && declined >= 40);
}

/// The era is read per source (the lens's LOW): a BEEF holding a BUMP below
/// the Genesis height and one above it. A push of 521 bytes (refused by the
/// rules of a coin from before Genesis) spending the coin proven above Genesis
/// is judged; the same push spending the coin proven below it is declined.
#[tokio::test]
async fn p0811_c_d_the_era_is_read_per_source() {
    let lock = |bytes: &[u8]| LockingScript::from_script(script(bytes));
    let old = proven_funding(vec![TransactionOutput::new(1_000, lock(&[0x51]))], 600_000);
    let new = proven_funding(
        vec![TransactionOutput::new(1_000, lock(&[0x75, 0x51]))],
        800_000,
    );
    let big = push_of(521);
    let spend = |old_unlock: &[u8], new_unlock: &[u8]| {
        let mut child = Transaction::new();
        for (parent, unlock) in [(&old, old_unlock), (&new, new_unlock)] {
            child.inputs.push(TransactionInput {
                source_txid: Some(parent.id()),
                source_output_index: 0,
                unlocking_script: Some(UnlockingScript::from_script(script(unlock))),
                ..Default::default()
            });
        }
        child.outputs.push(TransactionOutput::new(
            1_500,
            LockingScript::from_script(script(&[0x51])),
        ));
        let mut beef = Beef::new();
        for parent in [&old, &new] {
            let bump = beef.merge_bump(parent.merkle_path.clone().unwrap());
            beef.merge_raw_tx(parent.to_binary(), Some(bump));
        }
        beef.merge_raw_tx(child.to_binary(), None);
        (beef.to_binary(), child.id())
    };
    let (beef, txid) = spend(&[], &big);
    engine()
        .verify_scripts_only(&beef, &txid)
        .await
        .expect("the post-Genesis coin's spend is judged beside an old BUMP");
    let (beef, txid) = spend(&big, &[0x01, 0x42]);
    let why = declined(engine().verify_scripts_only(&beef, &txid).await);
    assert!(
        why.contains("input 0") && why.contains("below the Genesis height"),
        "{why}"
    );
}
