//! bsv-low #585, the fold of the doors delta lens (E585-D12-DELTA-M1): the
//! memory limb charges each input's scripts as they are once PARSED.
//!
//! bsv-rs cuts a script into one 32 byte record per chunk (`ScriptChunk`),
//! twice for the census and twice more for `Spend`, again for a signature's
//! subscript, and the interpreter can push three empty stack entries per
//! opcode, which its memory limit does not count. A script of one-byte
//! opcodes was 67 to 232 bytes of heap a byte, allocated before either limb
//! looked at that input: on `74c2c15` a 1 MB `OP_NOP` lock walked `Ok` at a
//! peak of 67.5 MB against an estimate of 4.0 MB, and an 8 MB one at 540 MB.
//! Each input's two scripts are now charged by their chunk count and their
//! bytes (`script_door::SCRIPT_CHARGE_PER_CHUNK`, `..._PER_BYTE`) beside the
//! frame's estimate, BEFORE either is parsed: past the limb the input is
//! never parsed, the door's bound, "the network judges", never a refusal.
//!
//! The file uses only what `74c2c15` has (`verify_scripts_only`,
//! `verify_scripts_only_under`, the error's `limb`): it is run there as it
//! stands, and each pin's RED word is in its doc comment.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use bsv_overlay_engine::builder::EngineBuilder;
use bsv_overlay_engine::engine::{DoorBudget, DoorLimb, Engine, EngineError, WalkStats};
use bsv_overlay_engine::storage::memory::MemoryStorage;
use bsv_rs::primitives::sha256d;
use bsv_rs::script::op::*;

// ── The counting allocator (as `script_door_stream.rs`) ─────────────────

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

fn engine() -> Engine {
    EngineBuilder::new(Box::new(MemoryStorage::new())).build()
}

/// The walk under `budget`, and the peak of the live heap it reached beside
/// what was held when it began.
async fn walked(
    body: &[u8],
    subject: &str,
    budget: DoorBudget,
) -> (Result<WalkStats, EngineError>, usize) {
    let engine = engine();
    let at_entry = LIVE.load(Ordering::Relaxed);
    PEAK.store(at_entry, Ordering::Relaxed);
    let verdict = engine
        .verify_scripts_only_under(body, subject, budget)
        .await;
    (
        verdict,
        PEAK.load(Ordering::Relaxed).saturating_sub(at_entry),
    )
}

// ── Bodies written byte by byte ─────────────────────────────────────────

type Hash32 = [u8; 32];

fn varint(n: u64) -> Vec<u8> {
    match n {
        0..=0xfc => vec![n as u8],
        0xfd..=0xffff => [&[0xfd][..], &(n as u16).to_le_bytes()].concat(),
        0x1_0000..=0xffff_ffff => [&[0xfe][..], &(n as u32).to_le_bytes()].concat(),
        _ => [&[0xff][..], &n.to_le_bytes()].concat(),
    }
}

/// A version 2 transaction of one input (`prev`:0, `unlock`) and one output.
fn one_in_one_out(prev: &Hash32, unlock: &[u8], sats: u64, lock: &[u8]) -> Vec<u8> {
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

fn display(wire: &Hash32) -> String {
    let mut bytes = *wire;
    bytes.reverse();
    hex::encode(bytes)
}

/// The BUMP of a block of one transaction.
fn one_leaf_bump(txid: &Hash32) -> Vec<u8> {
    let mut bump = varint(800_000);
    bump.extend_from_slice(&[0x01, 0x01, 0x00, 0x02]);
    bump.extend_from_slice(txid);
    bump
}

/// A V1 BEEF of one BUMP proving the first transaction, then the rest
/// unproven.
fn beef_v1(txs: &[Vec<u8>]) -> Vec<u8> {
    let mut body = vec![0x01, 0x00, 0xbe, 0xef, 0x01];
    body.extend(one_leaf_bump(&sha256d(&txs[0])));
    body.extend(varint(txs.len() as u64));
    for (i, raw) in txs.iter().enumerate() {
        body.extend_from_slice(raw);
        body.extend_from_slice(if i == 0 { &[0x01, 0x00] } else { &[0x00] });
    }
    body
}

/// `lock` on a PROVEN funding, spent by the subject with `unlock`.
fn on_a_proven_source(lock: &[u8], unlock: &[u8]) -> (Vec<u8>, String) {
    let funding = one_in_one_out(&[0xaa; 32], &[], 1_000, lock);
    let subject = one_in_one_out(&sha256d(&funding), unlock, 900, &[OP_1]);
    let subject_txid = display(&sha256d(&subject));
    (beef_v1(&[funding, subject]), subject_txid)
}

/// `lock` on an UNPROVEN parent (itself spending a proven funding's
/// `OP_1`), spent by the subject with `unlock`.
fn on_an_unproven_source(lock: &[u8], unlock: &[u8]) -> (Vec<u8>, String) {
    let funding = one_in_one_out(&[0xaa; 32], &[], 2_000, &[OP_1]);
    let parent = one_in_one_out(&sha256d(&funding), &[], 1_000, lock);
    let subject = one_in_one_out(&sha256d(&parent), unlock, 900, &[OP_1]);
    let subject_txid = display(&sha256d(&subject));
    (beef_v1(&[funding, parent, subject]), subject_txid)
}

/// `n` one-byte `OP_NOP`s, then `OP_1`: the lens's lock.
fn nop_lock(n: usize) -> Vec<u8> {
    [vec![OP_NOP; n], vec![OP_1]].concat()
}

/// The door stopped at the MEMORY limb having parsed nothing: its peak is
/// the frame's own (the stream's element of the largest transaction, 3
/// bytes a byte, and the index), at most 4 bytes a byte of the body and a
/// MiB, which a parse of one-byte opcodes (32 bytes a chunk at the least)
/// cannot fit in; and under the limb.
fn stopped_before_the_parse(
    name: &str,
    body: &[u8],
    verdict: &Result<WalkStats, EngineError>,
    peak: usize,
) {
    let frame = 4 * body.len() + 1024 * 1024;
    println!(
        "e585f2_m1 {name}: body {} bytes, peak {peak} bytes ({:.2}x the body, {:.1} % of the \
         memory limb), {verdict:?}",
        body.len(),
        peak as f64 / body.len() as f64,
        peak as f64 * 100.0 / DoorBudget::DEFAULT.max_memory_bytes as f64,
    );
    assert!(
        peak <= frame && (peak as u64) < DoorBudget::DEFAULT.max_memory_bytes,
        "{name}: the door held {peak} bytes before its answer (the frame is at most {frame}): \
         {verdict:?}"
    );
    match verdict {
        Err(EngineError::ScriptWalkOverBudget {
            limb: DoorLimb::Memory,
            ..
        }) => {}
        other => panic!("{name}: expected the memory limb, got {other:?}"),
    }
}

/// THE PIN (DELTA-M1), an UNPROVEN source: its 1,000,001 byte `OP_NOP` lock
/// is charged 524 MB parsed and is never parsed. RED on `74c2c15`: "the door
/// held 67555578 bytes before its answer (the frame is at most 5049528):
/// Ok(WalkStats { unproven_txs: 2, inputs_executed: 2, .., memory_bytes:
/// 4002612, .. })".
#[test]
fn e585f2_m1_a_1mb_nop_lock_on_an_unproven_source_is_never_parsed() {
    one_at_a_time(async {
        let (body, subject) = on_an_unproven_source(&nop_lock(1_000_000), &[]);
        let (verdict, peak) = walked(&body, &subject, DoorBudget::DEFAULT).await;
        stopped_before_the_parse("a 1 MB OP_NOP lock, unproven source", &body, &verdict, peak);
    });
}

/// THE PIN (DELTA-M1), a PROVEN (BUMP-carried) source, the class that
/// predates #585: never bounded before. RED on `74c2c15`: "the door held
/// 67555578 bytes before its answer (the frame is at most 5049280):
/// Ok(WalkStats { unproven_txs: 1, .., memory_bytes: 4002042, .. })".
#[test]
fn e585f2_m1_a_1mb_nop_lock_on_a_proven_source_is_never_parsed() {
    one_at_a_time(async {
        let (body, subject) = on_a_proven_source(&nop_lock(1_000_000), &[]);
        let (verdict, peak) = walked(&body, &subject, DoorBudget::DEFAULT).await;
        stopped_before_the_parse("a 1 MB OP_NOP lock, proven source", &body, &verdict, peak);
    });
}

/// THE PIN (DELTA-M1), the lens's 8 MB lock: 32 MB by the frame, under the
/// limb, and 4.2 GB parsed. RED on `74c2c15`: "the door held 540436602
/// bytes before its answer (the frame is at most 33049280): Ok(WalkStats {
/// .., memory_bytes: 32002042, .. })".
#[test]
fn e585f2_m1_an_8mb_nop_lock_is_never_parsed() {
    one_at_a_time(async {
        let (body, subject) = on_a_proven_source(&nop_lock(8_000_000), &[]);
        let (verdict, peak) = walked(&body, &subject, DoorBudget::DEFAULT).await;
        stopped_before_the_parse("an 8 MB OP_NOP lock", &body, &verdict, peak);
    });
}

/// THE PIN (DELTA-M1), the route's shape: the subject's EF is capped at 256
/// KB and the batch's at 2 MB, so what reaches the door is a small subject
/// over an unproven PARENT whose unlocking script is 1.9 MB of `OP_1`
/// (push-only, as an unlocking script must be). The subject is judged, then
/// the parent's input is charged 996 MB parsed and is never parsed. RED on
/// `74c2c15`: "the door held 136364598 bytes before its answer (the frame
/// is at most 8649544): Err(ScriptWalkOverBudget { .., subject_judged: true,
/// limb: Work, .. })": the interpreter's own stack limit tripped, AFTER the
/// parse (natively; a 128 MB isolate does not get that far).
#[test]
fn e585f2_m1_a_19mb_push_only_unproven_parent_is_never_parsed() {
    one_at_a_time(async {
        let funding = one_in_one_out(&[0xaa; 32], &[], 2_000, &[OP_DROP, OP_1]);
        let parent = one_in_one_out(
            &sha256d(&funding),
            &vec![OP_1; 1_900_000],
            1_000,
            &[OP_DROP, OP_1],
        );
        let subject = one_in_one_out(&sha256d(&parent), &[0x01, 0x42], 900, &[OP_1]);
        let subject_txid = display(&sha256d(&subject));
        let body = beef_v1(&[funding, parent, subject]);
        let (verdict, peak) = walked(&body, &subject_txid, DoorBudget::DEFAULT).await;
        stopped_before_the_parse("a 1.9 MB push-only unproven parent", &body, &verdict, peak);
        assert!(
            matches!(
                verdict,
                Err(EngineError::ScriptWalkOverBudget {
                    subject_judged: true,
                    ..
                })
            ),
            "the subject was judged first: {verdict:?}"
        );
    });
}

/// THE CHARGE IS ABOVE THE MEASURED HEAP. For each shape, the walk is run
/// with the memory limb lifted and the peak of the live heap measured; then
/// again under a limb EQUAL to that peak, where it must stop at the memory
/// limb: the estimate (the frame's and the scripts' charge) is above what
/// the walk held. Each lock is 262,145 chunks, one past a power of two, so
/// every growing buffer is at its slackest. The shapes: `OP_NOP` (the parse
/// alone), `OP_0` (an empty stack entry per opcode), `OP_NOP`s then a
/// reached `OP_CHECKSIG` (the subscript), `OP_3DUP` over empty entries (three
/// stack entries per opcode) with and without a reached `OP_CHECKSIG` after,
/// `0x01 xx OP_DROP` (pushes), `0x01 xx` in an unlocking script, and ten
/// 100,000 byte pushes then a reached `OP_CHECKSIG` (the per-byte charge).
/// Measured on the fold (debug; the bytes a script byte, the frame's
/// included): `OP_NOP` 98.0, `OP_0` 169.0, `OP_NOP` then `OP_CHECKSIG` 162.0,
/// `OP_3DUP` 241.0, `OP_3DUP` then `OP_CHECKSIG` 258.0, `0x01 xx OP_DROP`
/// 66.7, `0x01 xx` unlocking 62.5, the pushes 6.7.
/// RED on `74c2c15`: "OP_NOP: walked under a memory limb of its own peak
/// (25691292): Ok(WalkStats { .., memory_bytes: 1050622, .. })".
///
/// THE ERROR PATH (the doors delta-2 lens E585-D12-DELTA2-L1): bsv-rs's
/// `Spend::error` CLONES the stack, the alt stack and the if-stack into the
/// error, so a script that fails after building a stack of empty entries
/// (uncounted by the interpreter's 128 KB limit) holds a third copy on top
/// of the doubled buffer. `OP_0` x3, `OP_3DUP` x 262,145, `OP_VERIFY`
/// (refused) is 265.0 a byte; with 349,526 `OP_3DUP`s the stack is
/// 1,048,581 entries, just past a power of two, and the shape is 297.0 a
/// byte, THE WORST KNOWN: the charge, 520 a one-byte opcode with the frame's
/// 4, is 1.76 times it natively (about 3.5 times on wasm32 by arithmetic, a
/// chunk 16 bytes and a `Vec` 12 there; not run). The pin asserts the worst
/// it measures is that shape's and holds the charge's margin over it. RED on
/// `e2c561c` by the shape's absence: "the worst shape measured is 258.0 a
/// script byte, the error path's 297 is not among the shapes".
#[test]
fn e585f2_m1_the_scripts_charge_is_above_the_measured_heap() {
    one_at_a_time(async {
        let n = (1usize << 18) + 1;
        // `OP_0 <the generator>`: an empty signature, so a reached
        // `OP_CHECKSIG` builds its subscript and answers false.
        let generator =
            hex::decode("0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798")
                .expect("hex");
        let checksig = [&[OP_0, 0x21][..], &generator, &[OP_CHECKSIG, OP_DROP, OP_1]].concat();
        let pushes: Vec<u8> = (0..10)
            .flat_map(|_| {
                [
                    &[OP_PUSHDATA4][..],
                    &100_000u32.to_le_bytes(),
                    &[0x5a; 100_000],
                    &[OP_DROP],
                ]
                .concat()
            })
            .collect();
        let shapes: Vec<(&str, Vec<u8>, Vec<u8>)> = vec![
            ("OP_NOP", nop_lock(n), vec![]),
            ("OP_0", [vec![OP_0; n], vec![OP_1]].concat(), vec![]),
            (
                "OP_NOP then OP_CHECKSIG",
                [vec![OP_NOP; n], checksig.clone()].concat(),
                vec![],
            ),
            (
                "OP_3DUP",
                [vec![OP_0; 3], vec![OP_3DUP; n], vec![OP_1]].concat(),
                vec![],
            ),
            (
                "OP_3DUP then OP_CHECKSIG",
                [vec![OP_0; 3], vec![OP_3DUP; n], checksig.clone()].concat(),
                vec![],
            ),
            (
                "0x01 xx OP_DROP",
                [[0x01, 0x42, OP_DROP].repeat(n), vec![OP_1]].concat(),
                vec![],
            ),
            ("0x01 xx unlocking", vec![OP_DEPTH], [0x01, 0x42].repeat(n)),
            (
                "ten 100,000 byte pushes then OP_CHECKSIG",
                [pushes, checksig.clone()].concat(),
                vec![],
            ),
            (
                "OP_3DUP then a refused OP_VERIFY",
                [vec![OP_0; 3], vec![OP_3DUP; n], vec![OP_VERIFY]].concat(),
                vec![],
            ),
            (
                // 3 + 3 x 349,526 = 1,048,581 entries: 2^20 + 5.
                "OP_3DUP to a stack just past 2^20 then a refused OP_VERIFY",
                [vec![OP_0; 3], vec![OP_3DUP; 349_526], vec![OP_VERIFY]].concat(),
                vec![],
            ),
        ];
        let mut worst = (0.0f64, "");
        for (name, lock, unlock) in &shapes {
            let (body, subject) = on_a_proven_source(lock, unlock);
            let lifted = DoorBudget {
                max_memory_bytes: u64::MAX,
                ..DoorBudget::DEFAULT
            };
            let (free, peak) = walked(&body, &subject, lifted).await;
            let script = (lock.len() + unlock.len()) as f64;
            if peak as f64 / script > worst.0 {
                worst = (peak as f64 / script, name);
            }
            println!(
                "e585f2_m1 measured {name}: scripts {script} bytes, peak {peak} bytes = {:.1} a \
                 script byte; {}",
                peak as f64 / script,
                match &free {
                    Ok(stats) => format!("Ok, estimated {}", stats.memory_bytes),
                    Err(e) => format!("{e}").chars().take(100).collect(),
                }
            );
            let at_its_peak = DoorBudget {
                max_memory_bytes: peak as u64,
                ..DoorBudget::DEFAULT
            };
            let (bounded, _) = walked(&body, &subject, at_its_peak).await;
            assert!(
                matches!(
                    bounded,
                    Err(EngineError::ScriptWalkOverBudget {
                        limb: DoorLimb::Memory,
                        ..
                    })
                ),
                "{name}: walked under a memory limb of its own peak ({peak}): {bounded:?}"
            );
        }
        // The pin holds the worst known shape (DELTA2-L1), and the charge's
        // margin over it: 520 a one-byte opcode and the frame's 4.
        println!(
            "e585f2_m1 the worst measured: {} at {:.1} a script byte; the charge 524 is {:.2}x it",
            worst.1,
            worst.0,
            524.0 / worst.0
        );
        assert!(
            worst.0 >= 290.0,
            "the worst shape measured is {:.1} a script byte, the error path's 297 is not among \
             the shapes",
            worst.0
        );
        assert!(
            524.0 / worst.0 >= 1.7,
            "the charge is {:.2}x the worst ({}, {:.1} a byte)",
            524.0 / worst.0,
            worst.1,
            worst.0
        );
    });
}
