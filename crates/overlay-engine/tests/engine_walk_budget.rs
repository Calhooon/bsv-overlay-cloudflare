//! bsv-low #592 (the owner's ruling 3a of 2026-10-10): the engine's own script
//! walk runs under the door's two limbs on every mode that walks.
//!
//! Until #592 `Engine::submit` ran the reference's walk (`historical-tx`,
//! `current-tx`) with no memory charge and no work bound: a valid BEEF whose
//! unproven parent carries 1.9 MB of push-only unlocking script held about
//! 220 MB natively in the walk, an isolate kill on Workers (128 MB). The
//! queue's replay of every gated submit is `historical-tx`, so the consumer
//! died on each redelivery of such a body until it dead-lettered.
//!
//! Now the walk is the door's stream walk under the engine's `DoorBudget`
//! (`Engine::set_walk_budget`, `DoorBudget::DEFAULT` unless set). A breach is
//! "the walk could not run", never a refusal and (since the E592 lens fold,
//! H1) never an admission without the network's word: "not now"
//! (`EngineError::WalkCouldNotRun`) under `WalkBreachPolicy::NotNow`, every
//! door's default; under `NetworkAccepted` (the queue replay of a gated
//! submission) the submit goes on exactly as under `historical-tx-no-spv`
//! and says so on `MutationReport::walk_could_not_run`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use async_trait::async_trait;
use bsv_overlay_engine::builder::EngineBuilder;
use bsv_overlay_engine::engine::{
    DoorBudget, DoorLimb, Engine, EngineError, WalkBreachPolicy, WalkLimb,
};
use bsv_overlay_engine::storage::memory::MemoryStorage;
use bsv_overlay_engine::storage::Storage;
use bsv_overlay_engine::topic_manager::{TopicManager, TopicManagerError};
use bsv_overlay_engine::types::*;
use bsv_rs::primitives::{sha256d, PrivateKey};
use bsv_rs::script::op::*;
use bsv_rs::script::templates::P2PKH;
use bsv_rs::script::{
    LockingScript, Script, ScriptTemplate, ScriptTemplateUnlock, SignOutputs, SigningContext,
    UnlockingScript,
};
use bsv_rs::transaction::{
    ChainTracker, MerklePath, MerklePathLeaf, MockChainTracker, Transaction, TransactionInput,
    TransactionOutput,
};

// ── The counting allocator (as `gasp_fanin_memory.rs`) ──────────────────

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
/// One test at a time: the allocator is the binary's.
static SERIAL: Mutex<()> = Mutex::new(());

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc(layout);
        if !p.is_null() {
            let live = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        p
    }

    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        System.dealloc(p, layout);
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn one_at_a_time<F: std::future::Future>(test: F) -> F::Output {
    let _one = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime")
        .block_on(test)
}

const TOPIC: &str = "tm_test";

/// Admits output 0 of whatever it is shown.
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
            name: "admit-zero".into(),
            ..Default::default()
        }
    }
}

fn engine() -> Engine {
    EngineBuilder::new(Box::new(MemoryStorage::new()))
        .with_topic(TOPIC, Box::new(AdmitOutputZero))
        .build()
}

#[path = "support/walk_witness.rs"]
mod walk_witness;
use walk_witness::{beef_v1, display, on_a_proven_source, one_in_one_out, witness};

/// The submit under `mode`, and the peak of the live heap it reached beside
/// what was held when it began.
async fn submitted(
    engine: &Engine,
    body: &[u8],
    mode: SubmitMode,
) -> (
    Result<
        (Steak, bsv_overlay_engine::engine::MutationReport),
        bsv_overlay_engine::engine::EngineError,
    >,
    usize,
) {
    submitted_under(engine, body, mode, WalkBreachPolicy::NotNow).await
}

/// [`submitted`] under an explicit breach policy.
async fn submitted_under(
    engine: &Engine,
    body: &[u8],
    mode: SubmitMode,
    policy: WalkBreachPolicy,
) -> (
    Result<
        (Steak, bsv_overlay_engine::engine::MutationReport),
        bsv_overlay_engine::engine::EngineError,
    >,
    usize,
) {
    let tagged = TaggedBEEF::new(body.to_vec(), vec![TOPIC.to_string()]);
    let at_entry = LIVE.load(Ordering::Relaxed);
    PEAK.store(at_entry, Ordering::Relaxed);
    let answer = engine.submit_with_report_under(&tagged, mode, policy).await;
    (
        answer,
        PEAK.load(Ordering::Relaxed).saturating_sub(at_entry),
    )
}

/// THE PIN (a): the witness through `Engine::submit_with_report_under`
/// under `historical-tx`, the queue replay's mode. The walk could not run
/// (the parent's input is charged about 996 MB parsed, past the 48 MiB limb,
/// and is never parsed). Under `NotNow` (every door's default; the E592 lens
/// fold) that is `EngineError::WalkCouldNotRun` naming the subject and the
/// limb, nothing written. Under `NetworkAccepted` (the gated replay) the
/// report says so, the admission is the one `historical-tx-no-spv` makes of
/// the same bytes. Both peaks are under the memory limb plus the body.
///
/// RED on `54dbb16` (run there with the report's new field unread): "the
/// submit held 217765812 bytes (the memory limb plus the body is 52231890):
/// Ok(MutationReport { faults: [], applied_topics: [\"tm_test\"], .. })".
#[test]
fn e592_a_the_witness_under_historical_tx_is_walked_within_the_limbs() {
    one_at_a_time(async {
        let (body, subject) = witness();
        let engine = engine();
        let (not_now, not_now_peak) = submitted(&engine, &body, SubmitMode::HistoricalTx).await;
        println!(
            "e592_a: body {} bytes, NotNow: the submit's peak {not_now_peak} bytes ({:.1}x the body); {:?}",
            body.len(),
            not_now_peak as f64 / body.len() as f64,
            not_now.as_ref().map(|(_, report)| report)
        );
        match not_now {
            Err(EngineError::WalkCouldNotRun(stop)) => {
                assert_eq!(stop.subject_txid, subject);
                assert_eq!(stop.limb, WalkLimb::OverMemory);
            }
            other => panic!("not now, nothing written: {other:?}"),
        }
        let bound = DoorBudget::DEFAULT.max_memory_bytes as usize + body.len();
        assert!(not_now_peak <= bound, "not now held {not_now_peak} bytes");
        let (answer, peak) = submitted_under(
            &engine,
            &body,
            SubmitMode::HistoricalTx,
            WalkBreachPolicy::NetworkAccepted,
        )
        .await;
        println!(
            "e592_a: body {} bytes, the submit's peak {peak} bytes ({:.1}x the body); {:?}",
            body.len(),
            peak as f64 / body.len() as f64,
            answer.as_ref().map(|(_, report)| report)
        );
        assert!(
            peak <= bound,
            "the submit held {peak} bytes (the memory limb plus the body is {bound}): {:?}",
            answer.as_ref().map(|(_, report)| report)
        );
        let (steak, report) = answer.expect("the network accepted it: the walk could not run");
        let stop = report
            .walk_could_not_run
            .clone()
            .expect("the report says the walk could not run");
        assert_eq!(stop.subject_txid, subject);
        assert_eq!(stop.limb, WalkLimb::OverMemory);
        assert!(stop.subject_judged, "the subject's own input ran first");
        assert!(
            report.is_durable(),
            "a breach never makes a report undurable"
        );
        assert_eq!(report.applied_topics, vec![TOPIC.to_string()]);

        // The same bytes under historical-tx-no-spv, on a fresh engine: the
        // same admission, and no walk to report.
        let (no_spv, _) = submitted(&engine_fresh(), &body, SubmitMode::HistoricalTxNoSpv).await;
        let (no_spv_steak, no_spv_report) = no_spv.expect("no-spv admits");
        let admitted = |steak: &Steak| {
            steak
                .get(TOPIC)
                .map(|a| (a.outputs_to_admit.clone(), a.coins_to_retain.clone()))
        };
        assert_eq!(
            admitted(&steak),
            admitted(&no_spv_steak),
            "the admission is no-spv's"
        );
        assert_eq!(no_spv_report.applied_topics, report.applied_topics);
        assert_eq!(no_spv_report.walk_could_not_run, None);
    });
}

fn engine_fresh() -> Engine {
    engine()
}

/// Each limb is told apart. The work limb: a lock of `n` `OP_SHA256`s whose
/// estimate passes 64 MiB from the bytes (the shape of
/// `door_over_budget_is_inconclusive_*`). The interpreter's memory limit:
/// `<12 x (OP_DUP OP_CAT)>` over an 8 KB push (the shape of
/// `door_memory_limit_trip_*`). Each is not now under `NotNow` and admitted
/// under `NetworkAccepted`, `current-tx` as `historical-tx`, and each names
/// its limb.
#[test]
fn e592_a_each_limb_is_named_not_now_or_goes_on_by_the_policy() {
    one_at_a_time(async {
        let n = (DoorBudget::DEFAULT.max_work_bytes / DoorBudget::DEFAULT.memory_limit as u64)
            as usize
            + 100;
        let hash_lock = [vec![OP_SHA256; n], vec![OP_DROP, OP_1]].concat();
        let cat_lock = [[OP_DUP, OP_CAT].repeat(12), vec![OP_DROP, OP_1]].concat();
        let eight_kb = [&[OP_PUSHDATA2][..], &8192u16.to_le_bytes(), &[0x42; 8192]].concat();
        for (name, lock, unlock, limb) in [
            (
                "hash-heavy",
                hash_lock,
                vec![0x01, 0x42],
                WalkLimb::OverWork,
            ),
            (
                "OP_CAT doubling",
                cat_lock,
                eight_kb,
                WalkLimb::InterpreterMemory,
            ),
        ] {
            let (body, subject) = on_a_proven_source(&lock, &unlock);
            for mode in [SubmitMode::HistoricalTx, SubmitMode::CurrentTx] {
                match submitted(&engine(), &body, mode).await.0 {
                    Err(EngineError::WalkCouldNotRun(stop)) => {
                        assert_eq!(stop.limb, limb, "{name} {mode:?}");
                        assert_eq!(stop.subject_txid, subject);
                    }
                    other => panic!("{name} {mode:?}: not now: {other:?}"),
                }
                let (answer, _) =
                    submitted_under(&engine(), &body, mode, WalkBreachPolicy::NetworkAccepted)
                        .await;
                let (_, report) = answer.unwrap_or_else(|e| panic!("{name} {mode:?}: {e}"));
                let stop = report.walk_could_not_run.clone().expect(name);
                assert_eq!(stop.limb, limb, "{name} {mode:?}");
                assert_eq!(stop.subject_txid, subject);
                assert!(!stop.subject_judged);
                assert_eq!(
                    report.applied_topics,
                    vec![TOPIC.to_string()],
                    "{name} {mode:?}"
                );
            }
        }
    });
}

/// THE PIN (c): parity under the budget. A clean FALSE from the interpreter
/// under `historical-tx` is the refusal it was, in the same words; a
/// structural fault (a source the BEEF lacks, the value rule) is the same
/// `SpvError`; and a body that fits is walked with nothing reported.
#[test]
fn e592_c_a_clean_false_and_a_structural_fault_are_unchanged() {
    one_at_a_time(async {
        // `OP_DROP OP_0` spent by a push: the interpreter answers false.
        let (body, subject) = on_a_proven_source(&[OP_DROP, OP_0], &[0x01, 0x42]);
        let (answer, _) = submitted(&engine(), &body, SubmitMode::HistoricalTx).await;
        match answer {
            Err(EngineError::ScriptVerificationFailed {
                subject_txid,
                input_index,
                reason,
            }) => {
                assert_eq!(subject_txid, subject);
                assert_eq!(input_index, 0);
                assert_eq!(
                    reason,
                    "The top stack element must be truthy after script evaluation."
                );
            }
            other => panic!("a clean false is refused: {other:?}"),
        }

        // A subject whose source the BEEF does not carry.
        let funding = one_in_one_out(&[0xaa; 32], &[], 1_000, &[OP_DROP, OP_1]);
        let parent = one_in_one_out(&sha256d(&funding), &[0x01, 0x42], 900, &[OP_DROP, OP_1]);
        let subject = one_in_one_out(&sha256d(&parent), &[0x01, 0x42], 800, &[OP_1]);
        // A V1 BEEF of no BUMP and the subject alone: its one tip.
        let lacking = [&[0x01, 0x00, 0xbe, 0xef, 0x00, 0x01][..], &subject, &[0x00]].concat();
        let (answer, _) = submitted(&engine(), &lacking, SubmitMode::HistoricalTx).await;
        match answer {
            Err(EngineError::SpvError(why)) => assert_eq!(
                why,
                format!(
                    "Unable to verify SPV information: input 0 of transaction {} has no source transaction",
                    display(&sha256d(&subject))
                )
            ),
            other => panic!("a missing source is the SpvError it was: {other:?}"),
        }

        // The value rule: an unproven spend creating satoshis.
        let greedy = one_in_one_out(&sha256d(&funding), &[0x01, 0x42], 5_000, &[OP_1]);
        let greedy_txid = display(&sha256d(&greedy));
        let (answer, _) = submitted(
            &engine(),
            &beef_v1(&[funding.clone(), greedy]),
            SubmitMode::HistoricalTx,
        )
        .await;
        match answer {
            Err(EngineError::SpvError(why)) => assert_eq!(
                why,
                format!(
                    "Unable to verify SPV information: transaction {greedy_txid} creates 5000 sats from 1000 sats of inputs"
                )
            ),
            other => panic!("the value rule is the SpvError it was: {other:?}"),
        }

        // A body that fits: walked, nothing to report.
        let (answer, _) = submitted(
            &engine(),
            &beef_v1(&[funding, parent]),
            SubmitMode::HistoricalTx,
        )
        .await;
        let (_, report) = answer.expect("a valid small spend is admitted");
        assert_eq!(report.walk_could_not_run, None);
        assert_eq!(report.applied_topics, vec![TOPIC.to_string()]);
    });
}

/// The budget is the engine's configuration: a library consumer that sets
/// nothing has the door's default, and a caller's own budget is the one the
/// submit walks under (a lifted memory limb walks the witness's subject and
/// stops at the parent's input on the work limb instead).
#[test]
fn e592_the_budget_is_configured_on_the_engine() {
    one_at_a_time(async {
        assert_eq!(engine().walk_budget(), DoorBudget::DEFAULT);
        let tight = DoorBudget {
            max_memory_bytes: 1024,
            ..DoorBudget::DEFAULT
        };
        let built = EngineBuilder::new(Box::new(MemoryStorage::new()))
            .with_topic(TOPIC, Box::new(AdmitOutputZero))
            .with_walk_budget(tight)
            .build();
        assert_eq!(built.walk_budget(), tight);
        let (body, _) = on_a_proven_source(&[OP_DROP, OP_1], &[0x01, 0x42]);
        match submitted(&built, &body, SubmitMode::HistoricalTx).await.0 {
            Err(EngineError::WalkCouldNotRun(stop)) => assert_eq!(
                stop.limb,
                WalkLimb::OverMemory,
                "a 1 KiB memory limb stops even a small body"
            ),
            other => panic!("not now: {other:?}"),
        }
    });
}

/// A raw version 2 transaction of `inputs` (source, output index, unlocking
/// script) and `outputs` (satoshis, locking script).
fn raw_tx(inputs: &[(&[u8; 32], u32, &[u8])], outputs: &[(u64, &[u8])]) -> Vec<u8> {
    let mut raw = 2u32.to_le_bytes().to_vec();
    raw.extend(walk_witness::varint(inputs.len() as u64));
    for (prev, vout, unlock) in inputs {
        raw.extend_from_slice(*prev);
        raw.extend_from_slice(&vout.to_le_bytes());
        raw.extend(walk_witness::varint(unlock.len() as u64));
        raw.extend_from_slice(unlock);
        raw.extend_from_slice(&u32::MAX.to_le_bytes());
    }
    raw.extend(walk_witness::varint(outputs.len() as u64));
    for (sats, lock) in outputs {
        raw.extend_from_slice(&sats.to_le_bytes());
        raw.extend(walk_witness::varint(lock.len() as u64));
        raw.extend_from_slice(lock);
    }
    raw.extend_from_slice(&0u32.to_le_bytes());
    raw
}

/// The E592 delta lens's D1-L1: a static refusal is a refusal across the
/// WHOLE walk, not per input. Input 0 of the subject spends a lock whose
/// census breaches the work limb (the hash-heavy shape above); input 1 (or
/// an unproven ancestor's input) carries an unlocking script holding one
/// non-push opcode. The answer is the interpreter's refusal of that input,
/// under both policies, never "the walk could not run" and never an
/// admission: the walk's push-only pre-pass reads every reachable unlocking
/// script before any charge. The control (input 1 push-only) shows input 0
/// alone breaches.
///
/// RED with the pre-pass made inert (`d899aa1`'s walk): "refused true,
/// NotNow: Err(WalkCouldNotRun(.. limb: OverWork, what: \"estimated work
/// 80216680 bytes exceeds the door budget of 67108864 (input 0 of ..)\"".
#[test]
fn e592_d1_l1_a_non_push_unlock_anywhere_is_refused_before_a_breach() {
    one_at_a_time(async {
        let n = (DoorBudget::DEFAULT.max_work_bytes / DoorBudget::DEFAULT.memory_limit as u64)
            as usize
            + 100;
        let hash_lock = [vec![OP_SHA256; n], vec![OP_DROP, OP_1]].concat();
        let spendable = [OP_DROP, OP_1];
        let push = [0x01, 0x42];
        let not_push = [0x01, 0x42, OP_NOP];
        let funding = raw_tx(
            &[(&[0xaa; 32], 0, &[])],
            &[(1_000, &hash_lock), (1_000, &spendable)],
        );
        let funding_id = sha256d(&funding);

        // Input 1 of the subject itself; then the control.
        for (unlock_1, refused) in [(&not_push[..], true), (&push[..], false)] {
            let subject = raw_tx(
                &[(&funding_id, 0, &push), (&funding_id, 1, unlock_1)],
                &[(1_500, &[OP_1])],
            );
            let subject_txid = display(&sha256d(&subject));
            let body = beef_v1(&[funding.clone(), subject]);
            for policy in [WalkBreachPolicy::NotNow, WalkBreachPolicy::NetworkAccepted] {
                let (answer, _) =
                    submitted_under(&engine(), &body, SubmitMode::HistoricalTx, policy).await;
                match answer {
                    Err(EngineError::ScriptVerificationFailed {
                        subject_txid: at,
                        input_index,
                        reason,
                    }) if refused => {
                        assert_eq!(at, subject_txid);
                        assert_eq!(input_index, 1);
                        assert_eq!(
                            reason,
                            "Unlocking scripts can only contain push operations, and no other opcodes."
                        );
                    }
                    Err(EngineError::WalkCouldNotRun(stop))
                        if !refused && policy == WalkBreachPolicy::NotNow =>
                    {
                        assert_eq!(
                            stop.limb,
                            WalkLimb::OverWork,
                            "the control breaches at input 0"
                        );
                    }
                    Ok((_, report)) if !refused => {
                        assert_eq!(
                            report.walk_could_not_run.map(|stop| stop.limb),
                            Some(WalkLimb::OverWork),
                            "the control goes on under the network's accept"
                        );
                    }
                    other => panic!("refused {refused}, {policy:?}: {other:?}"),
                }
            }
        }

        // An unproven ancestor's input: the subject's input 0 breaches, its
        // input 1 spends a parent whose own unlocking script is not push-only.
        let parent = raw_tx(&[(&funding_id, 1, &not_push)], &[(900, &spendable)]);
        let parent_txid = display(&sha256d(&parent));
        let subject = raw_tx(
            &[(&funding_id, 0, &push), (&sha256d(&parent), 0, &push)],
            &[(1_500, &[OP_1])],
        );
        let body = beef_v1(&[funding, parent, subject]);
        for policy in [WalkBreachPolicy::NotNow, WalkBreachPolicy::NetworkAccepted] {
            match submitted_under(&engine(), &body, SubmitMode::HistoricalTx, policy)
                .await
                .0
            {
                Err(EngineError::ScriptVerificationFailed {
                    subject_txid: at,
                    input_index,
                    ..
                }) => {
                    assert_eq!(at, parent_txid, "the refused input is the parent's");
                    assert_eq!(input_index, 0);
                }
                other => panic!("the ancestor's refusal, {policy:?}: {other:?}"),
            }
        }
    });
}

// ── The delta lens's conformance pins on invalid bodies (question 8,
// 2026-10-10) ─────────────────────────────────────────────────────────────
//
// Deterministic, network-free, test-built bodies over `MemoryStorage`: each
// states what the engine answers on a malformed or unverifiable body under
// the owner's ruling 3a — a static refusal is the interpreter's or the SPV
// check's verdict, a breach is "the walk could not run" (or, under the
// network's accept, a report on the walk), and nothing is admitted
// unverified. Pins 1 and 3 here (and 4, in `gasp_topic_manager.rs`) are RED
// on `0313648`: the pre-E592-fold submit went on as `historical-tx-no-spv`
// on a breach, so it admitted these bodies where these refuse or report.

/// Block height of every fabricated single-transaction BUMP here (the same
/// height `beef_v1` writes for the BEEF's own BUMP).
const HEIGHT: u32 = 800_000;

/// The census-heavy lock: `n` `OP_SHA256`s whose estimate alone passes the
/// work limb (`n` = the limb over the per-hash 128 KiB, plus a hundred),
/// so the charge of one input reading it breaches before that input runs.
fn heavy_hash_lock() -> Vec<u8> {
    let n = (DoorBudget::DEFAULT.max_work_bytes / DoorBudget::DEFAULT.memory_limit as u64) as usize
        + 100;
    [vec![OP_SHA256; n], vec![OP_DROP, OP_1]].concat()
}

/// `OP_DROP OP_TRUE`, unlocked by any push: a valid spend with no signature
/// check and no hash opcode (the plain control's lock).
fn open_lock() -> LockingScript {
    let mut script = Script::new();
    script.write_opcode(OP_DROP).write_opcode(OP_TRUE);
    LockingScript::from_script(script)
}

/// The push of `0x42` eight times, as an unlock template.
fn open_unlock() -> ScriptTemplateUnlock {
    ScriptTemplateUnlock::new(
        |_ctx: &SigningContext| {
            let mut script = Script::new();
            script.write_bin(&[0x42; 8]);
            Ok(UnlockingScript::from_script(script))
        },
        || 16,
    )
}

/// A BRC-74 BUMP for a block containing only `txid`; its root IS the txid.
fn single_tx_block_proof(txid: &str) -> MerklePath {
    MerklePath::new(
        HEIGHT,
        vec![vec![MerklePathLeaf::new_txid(0, txid.to_string())]],
    )
    .expect("a one-leaf BUMP is valid")
}

/// A chain tracker that knows exactly one (height, root): the fabricated
/// single-transaction block of a fixture's funding tx.
fn tracker_knowing(funding_txid: &str) -> Box<dyn ChainTracker> {
    let mut tracker = MockChainTracker::new(HEIGHT + 10);
    tracker.add_root(HEIGHT, funding_txid.to_string());
    Box::new(tracker)
}

/// A chain tracker that knows nothing (every root it is asked is rejected).
fn tracker_knowing_nothing() -> Box<dyn ChainTracker> {
    Box::new(MockChainTracker::new(HEIGHT + 10))
}

/// The engine over `store` (an `Rc<MemoryStorage>` is itself a `Storage`, so
/// the test keeps a handle to assert on what was written) with an optional
/// chain tracker.
fn engine_over(store: Rc<MemoryStorage>, tracker: Option<Box<dyn ChainTracker>>) -> Engine {
    let mut builder =
        EngineBuilder::new(Box::new(store)).with_topic(TOPIC, Box::new(AdmitOutputZero));
    if let Some(tracker) = tracker {
        builder = builder.with_chain_tracker(tracker);
    }
    builder.build()
}

/// Whether the topic still holds `txid:vout` unspent.
async fn held_unspent(store: &Rc<MemoryStorage>, txid: &str, vout: u32) -> bool {
    store
        .find_output(txid, vout, Some(TOPIC), Some(false), false)
        .await
        .expect("the storage reads")
        .is_some()
}

/// Whether the topic holds an applied row for `txid`.
async fn applied_row(store: &Rc<MemoryStorage>, txid: &str) -> bool {
    store
        .does_applied_transaction_exist(&AppliedTransaction {
            txid: txid.to_string(),
            topic: TOPIC.to_string(),
        })
        .await
        .expect("the storage reads")
}

/// THE DELTA LENS'S PIN 1: a non-push unlocking script on an otherwise
/// well-formed spend is `ScriptVerificationFailed` with the interpreter's
/// own words, whatever the breach policy. Nothing of the refused body is
/// written: the held output stays unspent and no applied row appears.
///
/// Input 0 of the spend reads a census-heavy lock (a breach on a body the
/// pre-pass does not refuse); input 1's unlocking script carries one
/// non-push opcode, so the walk's push-only pre-pass refuses the body
/// before any charge. RED on `0313648`: the pre-fold submit walked with no
/// pre-pass and went on as `historical-tx-no-spv` on the breach, admitting
/// the spend unverified — the heavy coin spent, an applied row written.
#[test]
fn e592_q8_1_a_non_push_unlock_refuses_and_writes_nothing() {
    one_at_a_time(async {
        let heavy = heavy_hash_lock();
        let spendable = [OP_DROP, OP_1];
        let push = [0x01, 0x42];
        let not_push = [0x01, 0x42, OP_NOP];
        let funding = raw_tx(
            &[(&[0xaa; 32], 0, &[])],
            &[(1_000, &heavy), (1_000, &spendable)],
        );
        let funding_txid = display(&sha256d(&funding));

        for policy in [WalkBreachPolicy::NotNow, WalkBreachPolicy::NetworkAccepted] {
            // The heavy coin is held first: the funding alone, PROVEN (the
            // walk trusts it), its output 0 admitted.
            let store = Rc::new(MemoryStorage::new());
            let engine = engine_over(store.clone(), None);
            submitted(
                &engine,
                &beef_v1(&[funding.clone()]),
                SubmitMode::HistoricalTx,
            )
            .await
            .0
            .expect("the proven funding alone is admitted");
            assert!(
                held_unspent(&store, &funding_txid, 0).await,
                "the heavy coin is held, {policy:?}"
            );

            // The refused spend: input 1's unlocking script is not push-only.
            let subject = raw_tx(
                &[
                    (&sha256d(&funding), 0, &push),
                    (&sha256d(&funding), 1, &not_push),
                ],
                &[(1_500, &[OP_1])],
            );
            let subject_txid = display(&sha256d(&subject));
            match submitted_under(
                &engine,
                &beef_v1(&[funding.clone(), subject]),
                SubmitMode::HistoricalTx,
                policy,
            )
            .await
            .0
            {
                Err(EngineError::ScriptVerificationFailed {
                    subject_txid: at,
                    input_index,
                    reason,
                }) => {
                    assert_eq!(at, subject_txid);
                    assert_eq!(input_index, 1, "the non-push input, {policy:?}");
                    assert_eq!(
                        reason,
                        "Unlocking scripts can only contain push operations, and no other opcodes."
                    );
                }
                other => panic!("a static refusal, {policy:?}: {other:?}"),
            }
            assert!(
                held_unspent(&store, &funding_txid, 0).await,
                "the refused spend leaves the held output unspent, {policy:?}"
            );
            assert!(
                !applied_row(&store, &subject_txid).await,
                "nothing of the refused body is written, {policy:?}"
            );
        }
    });
}

/// A "mined" funding transaction of one throwaway input (never verified: the
/// tx carries a merkle path, so the walk trusts it) and two outputs: 0
/// under `first`, 1 a real P2PKH to `key`.
fn two_output_funding(first: LockingScript, key: &PrivateKey) -> Transaction {
    let mut tx = Transaction::new();
    tx.inputs.push(TransactionInput {
        source_txid: Some("aa".repeat(32)),
        source_output_index: 0,
        unlocking_script: Some(UnlockingScript::from_script(Script::new())),
        ..Default::default()
    });
    tx.outputs.push(TransactionOutput::new(10_000, first));
    tx.outputs.push(TransactionOutput::new(
        10_000,
        P2PKH::new().lock(&key.public_key().hash160()).unwrap(),
    ));
    let txid = tx.id();
    tx.merkle_path = Some(single_tx_block_proof(&txid));
    tx
}

/// Flip one bit of `input`'s unlocking script at `offset`, after signing
/// (`script_verification.rs`'s `flip_unlocking_byte` reaches input 0; this
/// one reaches any input).
fn flip_unlocking_byte_of(tx: &mut Transaction, input: usize, offset: usize) {
    let mut bytes = tx.inputs[input]
        .unlocking_script
        .as_ref()
        .unwrap()
        .to_binary();
    bytes[offset] ^= 0x01;
    tx.inputs[input].unlocking_script = Some(UnlockingScript::from_script(
        Script::from_binary(&bytes).unwrap(),
    ));
    tx.invalidate_caches();
}

/// Pin 2's body: a signed two-input spend of one proven funding transaction
/// (input 0 of the `first` lock, input 1 the P2PKH), then one bit of
/// input 1's DER signature flipped after signing: DER stays well-formed, the
/// signature no longer verifies.
async fn two_input_spend_over(first: LockingScript) -> (Vec<u8>, String) {
    let key = PrivateKey::random();
    let funding = two_output_funding(first, &key);
    let mut tx = Transaction::new();
    tx.add_input_from_tx(funding.clone(), 0, open_unlock())
        .unwrap();
    tx.add_input_from_tx(funding, 1, P2PKH::unlock(&key, SignOutputs::All, false))
        .unwrap();
    tx.outputs.push(TransactionOutput::new(15_000, open_lock()));
    tx.sign().await.expect("template signing");
    flip_unlocking_byte_of(&mut tx, 1, 10);
    let txid = tx.id();
    let beef = tx.to_beef(false).expect("the two-input spend's BEEF");
    (beef, txid)
}

/// THE DELTA LENS'S PIN 2 (reading (b)): the charges are per input, in
/// order, before the input runs. A spend whose input 0 reads a census-heavy
/// lock answers `WalkCouldNotRun` BEFORE input 1's corrupted signature is
/// executed — the charge of input 0 comes first and input 0 never ran; the
/// control (a plain lock on input 0) is refused AT input 1 by the
/// interpreter, which is the same order seen from the other side. Nothing
/// is written either way.
#[test]
fn e592_q8_2_the_charge_at_input_0_precedes_input_1s_invalid_signature() {
    one_at_a_time(async {
        // The heavy arm: the charge of input 0 breaches before input 1 runs.
        let (body, subject_txid) = two_input_spend_over(LockingScript::from_script(
            Script::from_binary(&heavy_hash_lock()).unwrap(),
        ))
        .await;
        let store = Rc::new(MemoryStorage::new());
        let engine = engine_over(store.clone(), None);
        match submitted(&engine, &body, SubmitMode::HistoricalTx).await.0 {
            Err(EngineError::WalkCouldNotRun(stop)) => {
                assert_eq!(stop.subject_txid, subject_txid);
                assert_eq!(
                    stop.at_txid, subject_txid,
                    "the stop is at the subject's input 0"
                );
                assert!(
                    !stop.subject_judged,
                    "input 0 never ran: its charge came first"
                );
                assert_eq!(stop.limb, WalkLimb::OverWork);
                assert!(
                    stop.what.contains("(input 0 of "),
                    "the charge names its input: {}",
                    stop.what
                );
            }
            other => panic!("the heavy lock on input 0: {other:?}"),
        }
        assert!(
            !applied_row(&store, &subject_txid).await,
            "nothing of a walk that could not run is written"
        );

        // The control: input 0 runs (a plain lock), so input 1's corrupted
        // signature is reached and refused.
        let (body, subject_txid) = two_input_spend_over(open_lock()).await;
        let store = Rc::new(MemoryStorage::new());
        let engine = engine_over(store.clone(), None);
        match submitted(&engine, &body, SubmitMode::HistoricalTx).await.0 {
            Err(EngineError::ScriptVerificationFailed {
                subject_txid: at,
                input_index,
                reason,
            }) => {
                assert_eq!(at, subject_txid);
                assert_eq!(
                    input_index, 1,
                    "input 0 ran; the corrupted signature is what refused"
                );
                // The P2PKH lock's `OP_CHECKSIG` pushes false (the corrupted
                // signature fails the EC check) and the interpreter's truthy
                // rule is what names it.
                assert_eq!(
                    reason,
                    "The top stack element must be truthy after script evaluation."
                );
            }
            other => panic!("the plain lock on input 0: {other:?}"),
        }
        assert!(
            !applied_row(&store, &subject_txid).await,
            "nothing of the refused body is written"
        );
    });
}

/// THE DELTA LENS'S PIN 3: the roots of a breached walk are checked (both
/// policies). A breach whose reached proven transaction carries a merkle
/// root the chain tracker rejects is the `SpvError` a walk that RAN would
/// answer — the root check preempts the breach, never the reverse. The
/// control (a plain lock, a walk that runs to its end) is the same
/// `SpvError`; with a tracker that knows the root, the breach is the
/// answer under `NotNow` and a report under `NetworkAccepted`, and a clean
/// walk over an accepted root is admitted with nothing to report.
///
/// RED on `0313648`: the pre-fold submit went on as `historical-tx-no-spv`
/// on the breach and asked no root, so the heavy arms were admitted
/// unverified.
#[test]
fn e592_q8_3_the_roots_of_a_breached_walk_are_checked() {
    one_at_a_time(async {
        for (name, lock) in [
            ("the heavy lock (a breach)", heavy_hash_lock()),
            ("a plain lock (the control)", vec![OP_DROP, OP_1]),
        ] {
            let funding = one_in_one_out(&[0xaa; 32], &[], 1_000, &lock);
            let funding_txid = display(&sha256d(&funding));
            let subject = one_in_one_out(&sha256d(&funding), &[0x01, 0x42], 900, &[OP_1]);
            let body = beef_v1(&[funding, subject]);
            let root = single_tx_block_proof(&funding_txid)
                .compute_root(Some(&funding_txid))
                .expect("the funding's root");
            let rejected = format!(
                "Invalid merkle path for transaction {funding_txid}: \
                 root {root} is not valid for block height {HEIGHT}"
            );
            for knows in [true, false] {
                for policy in [WalkBreachPolicy::NotNow, WalkBreachPolicy::NetworkAccepted] {
                    let tracker = if knows {
                        tracker_knowing(&funding_txid)
                    } else {
                        tracker_knowing_nothing()
                    };
                    let engine = engine_over(Rc::new(MemoryStorage::new()), Some(tracker));
                    match submitted_under(&engine, &body, SubmitMode::HistoricalTx, policy)
                        .await
                        .0
                    {
                        Err(EngineError::SpvError(why)) => {
                            assert!(!knows, "a tracker that knows the root, {name}, {policy:?}");
                            assert_eq!(why, rejected, "{name}, {policy:?}");
                        }
                        Err(EngineError::WalkCouldNotRun(stop)) => {
                            assert!(knows, "{name}, {policy:?}");
                            assert_eq!(stop.limb, WalkLimb::OverWork, "{name}, {policy:?}");
                            assert!(!stop.subject_judged, "the charge of input 0, {name}");
                        }
                        Ok((_, report)) => {
                            assert!(knows, "{name}, {policy:?}");
                            if name.starts_with("the heavy") {
                                assert_eq!(
                                    report.walk_could_not_run.map(|stop| stop.limb),
                                    Some(WalkLimb::OverWork),
                                    "the breach goes on under the network's accept, {policy:?}"
                                );
                                assert_eq!(
                                    policy,
                                    WalkBreachPolicy::NetworkAccepted,
                                    "not now under NotNow, {name}"
                                );
                            } else {
                                assert_eq!(
                                    report.walk_could_not_run, None,
                                    "a clean walk over an accepted root, {policy:?}"
                                );
                            }
                        }
                        other => panic!("{name}, knows {knows}, {policy:?}: {other:?}"),
                    }
                }
            }
        }
    });
}

/// Pin 6's manager: admits output 0 when the transaction has exactly one
/// output, NAMES its input 0's outpoint (so the engine's predecessor
/// question fires for a carried predecessor), reads no off-chain values (so
/// the door may land a carried body first), and re-admits the same
/// transaction idempotently.
struct AdmitsOneOutput;

#[async_trait(?Send)]
impl TopicManager for AdmitsOneOutput {
    async fn identify_admissible_outputs(
        &self,
        tx: &Transaction,
        _: &[u8],
        _: Option<&[u8]>,
        _: SubmitMode,
        _context: &TopicAdmittanceContext,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        if tx.outputs.len() != 1 {
            return Ok(AdmittanceInstructions::default());
        }
        Ok(AdmittanceInstructions {
            outputs_to_admit: vec![0],
            coins_to_retain: vec![],
            coins_removed: None,
        })
    }
    async fn identify_needed_inputs(
        &self,
        beef: &[u8],
        _off_chain_values: Option<&[u8]>,
    ) -> Result<Vec<Outpoint>, TopicManagerError> {
        let tx = Transaction::from_beef(beef, None).expect("the door's BEEF parses");
        Ok(tx
            .inputs
            .first()
            .filter(|i| {
                i.get_source_txid()
                    .is_ok_and(|txid| txid != "00".repeat(32))
            })
            .map(|i| Outpoint::new(i.get_source_txid().unwrap(), i.source_output_index))
            .into_iter()
            .collect())
    }
    fn reads_off_chain_values(&self) -> bool {
        false
    }
    async fn get_documentation(&self) -> String {
        "admits one output".into()
    }
    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata {
            name: "admit-one".into(),
            ..Default::default()
        }
    }
}

/// Pin 6's engine: the store is held by the test, the manager names its
/// inputs.
fn engine_naming(store: Rc<MemoryStorage>) -> Engine {
    EngineBuilder::new(Box::new(store))
        .with_topic(TOPIC, Box::new(AdmitsOneOutput))
        .build()
}

/// THE DELTA LENS'S PIN 6 (E592-M1): the landing trusts only a walk that
/// ran. A successor under `NetworkAccepted` whose own walk breaches carries
/// a predecessor; the predecessor is walked by its own submit (its breach
/// reported on `landed_walks_could_not_run`), never trusted, and lands;
/// the successor is then judged over the coin it now finds. Under `NotNow`
/// nothing lands and nothing is written.
#[test]
fn e592_q8_6_a_breached_landing_walks_its_predecessor_and_never_trusts_it() {
    one_at_a_time(async {
        let heavy = heavy_hash_lock();
        // The funding's own input is a coinbase-shaped prevout (the all-zero
        // txid the manager's names filter excludes), so its own admission
        // names nothing and needs no predecessor.
        let funding = one_in_one_out(&[0x00; 32], &[], 2_000, &heavy);
        let funding_txid = display(&sha256d(&funding));
        let predecessor =
            one_in_one_out(&sha256d(&funding), &[0x01, 0x42], 1_000, &[OP_DROP, OP_1]);
        let predecessor_txid = display(&sha256d(&predecessor));
        let subject = one_in_one_out(&sha256d(&predecessor), &[0x01, 0x42], 900, &[OP_1]);
        let subject_txid = display(&sha256d(&subject));
        let body = beef_v1(&[funding.clone(), predecessor, subject]);

        // Under `NotNow`: the successor's own walk breaches at the
        // predecessor's input (the heavy lock of the funding), the submit is
        // "not now", nothing lands, nothing is written.
        let store = Rc::new(MemoryStorage::new());
        let engine = engine_naming(store.clone());
        submitted(
            &engine,
            &beef_v1(&[funding.clone()]),
            SubmitMode::HistoricalTx,
        )
        .await
        .0
        .expect("the proven funding alone is admitted");
        match submitted(&engine, &body, SubmitMode::HistoricalTx).await.0 {
            Err(EngineError::WalkCouldNotRun(stop)) => {
                assert_eq!(stop.subject_txid, subject_txid);
                assert_eq!(
                    stop.at_txid, predecessor_txid,
                    "the breach is at the predecessor's input 0"
                );
                assert!(
                    stop.subject_judged,
                    "the subject's own input finished first"
                );
                assert_eq!(stop.limb, WalkLimb::OverWork);
            }
            other => panic!("not now, nothing lands: {other:?}"),
        }
        assert!(
            held_unspent(&store, &funding_txid, 0).await,
            "the held coin is unspent under NotNow"
        );
        assert!(
            !applied_row(&store, &predecessor_txid).await,
            "nothing lands"
        );
        assert!(!applied_row(&store, &subject_txid).await, "nothing lands");

        // Under `NetworkAccepted`: the same submit goes on, the predecessor is
        // landed by its own submit (which breaches at its own input 0 and
        // goes on too, reported, never trusted), and both are applied.
        let store = Rc::new(MemoryStorage::new());
        let engine = engine_naming(store.clone());
        submitted(
            &engine,
            &beef_v1(&[funding.clone()]),
            SubmitMode::HistoricalTx,
        )
        .await
        .0
        .expect("the proven funding alone is admitted");
        let (answer, _) = submitted_under(
            &engine,
            &body,
            SubmitMode::HistoricalTx,
            WalkBreachPolicy::NetworkAccepted,
        )
        .await;
        let (_, report) = answer.expect("the network accepted it: the walk could not run");
        let stop_a = report
            .walk_could_not_run
            .clone()
            .expect("the subject's own breach is reported");
        assert_eq!(stop_a.subject_txid, subject_txid);
        assert_eq!(stop_a.at_txid, predecessor_txid);
        assert_eq!(stop_a.limb, WalkLimb::OverWork);
        assert_eq!(
            report.landed_predecessors,
            vec![(predecessor_txid.clone(), TOPIC.to_string())],
            "the carried predecessor is landed first, and reported"
        );
        assert_eq!(
            report.landed_walks_could_not_run.len(),
            1,
            "the landing's own breach is reported, never trusted: {:?}",
            report.landed_walks_could_not_run
        );
        let stop_b = &report.landed_walks_could_not_run[0];
        assert_eq!(stop_b.subject_txid, predecessor_txid);
        assert_eq!(
            stop_b.at_txid, predecessor_txid,
            "the predecessor's own input 0"
        );
        assert!(
            !stop_b.subject_judged,
            "the predecessor's input 0 never ran"
        );
        assert_eq!(stop_b.limb, WalkLimb::OverWork);
        assert!(
            report.faults.is_empty(),
            "no storage fault: {:?}",
            report.faults
        );
        assert_eq!(report.applied_topics, vec![TOPIC.to_string()]);
        assert!(
            applied_row(&store, &predecessor_txid).await,
            "the predecessor landed"
        );
        assert!(
            applied_row(&store, &subject_txid).await,
            "the successor landed"
        );
        assert!(
            held_unspent(&store, &subject_txid, 0).await,
            "the successor's output is held"
        );
        assert!(
            !held_unspent(&store, &funding_txid, 0).await,
            "the landed predecessor spent the held coin"
        );
    });
}

/// THE DELTA LENS'S PIN 7: the engine-side equality of the door's budget
/// and the submit's — `Engine::verify_scripts_only` (the gated door's walk)
/// runs under the ENGINE's `walk_budget`, not a hard-coded default. A lock
/// of 406 `OP_SHA256`s (under the default 64 MiB work limb, past a 32 MiB
/// one) is walked by the default engine and is "not now" under a tighter
/// one, at the door and at the submit alike.
#[test]
fn e592_q8_7_the_doors_walk_runs_under_the_engines_budget() {
    one_at_a_time(async {
        let lock = [vec![OP_SHA256; 406], vec![OP_DROP, OP_1]].concat();
        let (body, subject) = on_a_proven_source(&lock, &[0x01, 0x42]);

        // The default: the door walks it, and so does the submit.
        let engine = engine();
        engine
            .verify_scripts_only(&body, &subject)
            .await
            .expect("406 hashes are under the default work limb");
        let (answer, _) = submitted(&engine, &body, SubmitMode::HistoricalTx).await;
        let (_, report) = answer.expect("the submit walks it too");
        assert_eq!(report.walk_could_not_run, None);

        // A tighter engine: the door's walk and the submit's walk answer the
        // same "over budget".
        let tight = DoorBudget {
            max_work_bytes: DoorBudget::DEFAULT.max_work_bytes / 2,
            ..DoorBudget::DEFAULT
        };
        let engine = EngineBuilder::new(Box::new(MemoryStorage::new()))
            .with_topic(TOPIC, Box::new(AdmitOutputZero))
            .with_walk_budget(tight)
            .build();
        match engine.verify_scripts_only(&body, &subject).await {
            Err(EngineError::ScriptWalkOverBudget { limb, .. }) => {
                assert_eq!(limb, DoorLimb::Work);
            }
            other => panic!("the door under the engine's tight budget: {other:?}"),
        }
        match submitted(&engine, &body, SubmitMode::HistoricalTx).await.0 {
            Err(EngineError::WalkCouldNotRun(stop)) => {
                assert_eq!(stop.limb, WalkLimb::OverWork);
            }
            other => panic!("the submit under the engine's tight budget: {other:?}"),
        }
    });
}
