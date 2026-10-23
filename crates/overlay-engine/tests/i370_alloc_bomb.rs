//! bsv-low #370: a body DECLARING a huge element count (a varint) before any
//! element, on the ungated `/submit` (reachable by any caller wherever
//! `SUBMIT_ENFORCE` is unset). Found on overlay `e1348da` by the #366 census
//! gate: bsv-rs then sized its vectors from the declared count
//! (`Vec::with_capacity(count)`), a capacity-overflow panic natively and an
//! allocation stall on miniflare, the Worker's hung 500.
//!
//! THE MEASURE: every bomb shape (a declared BUMP count, transaction count,
//! input count, output count, script length, BUMP level leaf count; V1, V2
//! and atomic frames) through what the ungated arm runs on the bytes: the
//! door's streaming read (`beef_limits::read_beef`), its parse
//! (`beef_limits::parse_beef`, the route's census and EF conversion and the
//! engine's `run_validation`), and `Engine::submit` under both ungated modes.
//! Each is refused (an `Err`, never a panic), the live heap's peak growth is
//! bounded by a constant beside the body, and the time by a constant.
//!
//! On this tip the bound is the SDK's and the door's, before any vector is
//! sized: bsv-rs's `bounded_capacity` caps a declared count by the bytes left
//! (`transaction.rs`, "Panic-hardening: adversarial count varints must Err,
//! never OOM-abort"), and since NL-6 every parse of the door is the streaming
//! reader first. So this pin is GREEN on `45b83a32` too: it holds the bound
//! that closed #370 (a bump of bsv-rs or a parse that skips the stream must
//! keep it), it does not fix a defect of this tip. Not measured on wasm32.
//!
//! Measured, and why the stream comes first: the SDK's OWN `Beef::from_binary`
//! is bounded by the bytes, not by a constant. A V1 body declaring 2^32 - 1
//! transactions over 64 KiB of zeros holds 4,309,280 bytes in it (about 66
//! times the body) before it refuses; the door's `parse_beef` refuses the same
//! bytes in its streaming read at 126. So a `Beef::from_binary` of REQUEST
//! bytes that skips `beef_limits::read_beef` would reopen #370 at about 66
//! bytes of heap a byte of body; the SDK row below is printed, not bounded.
// bsv-stack-lean #134: test code
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bsv_overlay_engine::beef_limits::{self, ENGINE_BEEF_LIMITS};
use bsv_overlay_engine::builder::EngineBuilder;
use bsv_overlay_engine::engine::Engine;
use bsv_overlay_engine::storage::memory::MemoryStorage;
use bsv_overlay_engine::topic_manager::{TopicManager, TopicManagerError};
use bsv_overlay_engine::types::*;
use bsv_rs::transaction::{Beef, Transaction};

// ── The counting allocator (as `gasp_fanin_memory.rs`) ──────────────────

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
/// One test at a time: the allocator is the binary's.
static SERIAL: Mutex<()> = Mutex::new(());

struct Counting;

// SAFETY: every call forwards to `System` with the caller's own layout and
// pointer, so `System`'s contract holds; the counters are atomics.
#[allow(unsafe_code)]
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

/// The peak growth of the live heap while `f` runs, beside what was live at its start.
fn peak_of<T>(f: impl FnOnce() -> T) -> (T, usize) {
    let at_entry = LIVE.load(Ordering::Relaxed);
    PEAK.store(at_entry, Ordering::Relaxed);
    let out = f();
    (out, PEAK.load(Ordering::Relaxed).saturating_sub(at_entry))
}

/// A constant beside the body: what a refused bomb may hold at its peak.
const BOMB_HEAP_BOUND: usize = 256 * 1024;
/// A constant in time (a debug build): a refused bomb is answered at once.
const BOMB_TIME_BOUND: Duration = Duration::from_secs(2);

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

// ── The bombs ──────────────────────────────────────────────────────────

const V1: [u8; 4] = [0x01, 0x00, 0xbe, 0xef];
const V2: [u8; 4] = [0x02, 0x00, 0xbe, 0xef];
/// A varint declaring 2^64 - 1.
const HUGE_U64: [u8; 9] = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff];
/// A varint declaring 2^32 - 1.
const HUGE_U32: [u8; 5] = [0xfe, 0xff, 0xff, 0xff, 0xff];

fn cat(parts: &[&[u8]]) -> Vec<u8> {
    parts.iter().flat_map(|p| p.iter().copied()).collect()
}

/// A raw transaction's head: version 1, one input spending `[7; 32]:0`.
fn tx_head_one_input() -> Vec<u8> {
    cat(&[&[1, 0, 0, 0], &[1], &[7; 32], &[0, 0, 0, 0]])
}

/// Every shape #370 names, and its neighbours: a declared count or length the bytes cannot hold.
fn bombs() -> Vec<(&'static str, Vec<u8>)> {
    let atomic = |inner: Vec<u8>| cat(&[&[0x01, 0x01, 0x01, 0x01], &[9; 32], &inner]);
    vec![
        ("V1 BUMP count 2^64-1", cat(&[&V1, &HUGE_U64])),
        ("V1 BUMP count 2^32-1", cat(&[&V1, &HUGE_U32])),
        ("V1 transaction count 2^64-1", cat(&[&V1, &[0], &HUGE_U64])),
        ("V2 transaction count 2^32-1", cat(&[&V2, &[0], &HUGE_U32])),
        ("V1 input count 2^32-1", cat(&[&V1, &[0], &[1], &[1, 0, 0, 0], &HUGE_U32])),
        ("V2 raw-tx input count 2^64-1", cat(&[&V2, &[0], &[1], &[0], &[1, 0, 0, 0], &HUGE_U64])),
        (
            "V1 unlocking script length 2^64-1",
            cat(&[&V1, &[0], &[1], &tx_head_one_input(), &HUGE_U64]),
        ),
        (
            "V1 output count 2^32-1",
            cat(&[&V1, &[0], &[1], &tx_head_one_input(), &[0], &[0xff; 4], &HUGE_U32]),
        ),
        (
            "V1 locking script length 2^32-1",
            cat(&[
                &V1,
                &[0],
                &[1],
                &tx_head_one_input(),
                &[0],
                &[0xff; 4],
                &[1],
                &[0; 8],
                &HUGE_U32,
            ]),
        ),
        // a BUMP: block height 1, tree height 1, level 0 declaring 2^32-1 leaves
        ("V1 BUMP leaf count 2^32-1", cat(&[&V1, &[1], &[1], &[1], &HUGE_U32])),
        ("V1 BUMP tree height 255", cat(&[&V1, &[1], &[1], &[0xff], &HUGE_U32])),
        ("atomic V1 transaction count 2^64-1", atomic(cat(&[&V1, &[0], &HUGE_U64]))),
        // the same declarations after a run of padding the bytes do hold (a body that is not tiny)
        (
            "V1 transaction count 2^32-1, 64 KiB of tail",
            cat(&[&V1, &[0], &HUGE_U32, &vec![0u8; 64 * 1024]]),
        ),
    ]
}

/// THE PIN: every bomb is refused by every reader the ungated arm runs, inside a constant of heap and time.
#[test]
fn i370_a_declared_huge_count_is_refused_before_anything_is_sized() {
    let _one = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime");
    let engine = engine();
    for (name, body) in bombs() {
        let started = Instant::now();
        let (read, read_peak) = peak_of(|| beef_limits::read_beef(&body).is_err());
        assert!(read, "{name}: the door's streaming read refuses it");
        let (parsed, parse_peak) =
            peak_of(|| beef_limits::parse_beef(&body, &ENGINE_BEEF_LIMITS).is_err());
        assert!(parsed, "{name}: the door's parse refuses it");
        let (sdk, sdk_peak) = peak_of(|| Beef::from_binary(&body).is_err());
        assert!(sdk, "{name}: the SDK's own parse refuses it (its peak is printed: bounded by the bytes only)");
        for mode in [SubmitMode::CurrentTx, SubmitMode::HistoricalTx] {
            let tagged = TaggedBEEF::new(body.clone(), vec![TOPIC.to_string()]);
            let (answer, submit_peak) = peak_of(|| runtime.block_on(engine.submit(&tagged, mode)));
            assert!(answer.is_err(), "{name}: the ungated submit ({mode:?}) refuses it: {answer:?}");
            assert!(
                submit_peak <= BOMB_HEAP_BOUND + body.len(),
                "{name}: the submit ({mode:?}) held {submit_peak} bytes, past {BOMB_HEAP_BOUND} beside the body"
            );
        }
        for (reader, peak) in [("read_beef", read_peak), ("parse_beef", parse_peak)] {
            assert!(
                peak <= BOMB_HEAP_BOUND + body.len(),
                "{name}: {reader} held {peak} bytes, past {BOMB_HEAP_BOUND} beside the body"
            );
        }
        let took = started.elapsed();
        assert!(took < BOMB_TIME_BOUND, "{name}: answered in {took:?}");
        println!("{name}: refused, peaks read {read_peak} parse {parse_peak} sdk {sdk_peak}, {took:?}");
    }
}
