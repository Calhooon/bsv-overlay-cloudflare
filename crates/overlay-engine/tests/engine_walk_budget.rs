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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use async_trait::async_trait;
use bsv_overlay_engine::builder::EngineBuilder;
use bsv_overlay_engine::engine::{DoorBudget, Engine, EngineError, WalkBreachPolicy, WalkLimb};
use bsv_overlay_engine::storage::memory::MemoryStorage;
use bsv_overlay_engine::topic_manager::{TopicManager, TopicManagerError};
use bsv_overlay_engine::types::*;
use bsv_rs::primitives::sha256d;
use bsv_rs::script::op::*;
use bsv_rs::transaction::Transaction;

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
