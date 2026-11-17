//! PORT-0811: the engine's link bound is a bound in HEAP.
//!
//! bsv-rs 0.8.11 meters a link by the wire bytes of each repeated
//! shared-parent stub; a parsed output holds about fifteen times its wire
//! bytes. `beef_limits::link_heap_estimate` charges what a link holds (its
//! inputs, outputs, wire bytes and BUMP leaves) before anything is linked,
//! against `beef_limits::LINK_HEAP_MAX_BYTES` (16 MiB); bsv-rs's meter is the
//! belt at `LINK_WIRE_MAX_BYTES`. These pins hold the production read
//! (`beef_limits::transaction_from_beef`) under the heap bound on the worst
//! shapes built here, the estimate above the measured heap of every shape it
//! admits, and name the largest honest same-parent P2PKH sweep it links.
//!
//! Its own test binary: a counting global allocator, one test at a time.
// bsv-stack-lean #134: test code
#![allow(
    unsafe_code,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::cast_precision_loss
)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use bsv_overlay_engine::beef_limits;
use bsv_rs::script::{LockingScript, Script, UnlockingScript};
use bsv_rs::transaction::{
    Beef, BeefLinkLimits, MerklePath, MerklePathLeaf, Transaction, TransactionInput,
    TransactionOutput,
};

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

struct Counting;

// SAFETY: every call forwards to the system allocator unchanged; the counters
// are bookkeeping only.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded.
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            let live = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        // SAFETY: forwarded.
        unsafe { System.dealloc(p, layout) };
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }
    unsafe fn realloc(&self, p: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: forwarded.
        let q = unsafe { System.realloc(p, layout, size) };
        if !q.is_null() {
            if size > layout.size() {
                let live =
                    LIVE.fetch_add(size - layout.size(), Ordering::Relaxed) + size - layout.size();
                PEAK.fetch_max(live, Ordering::Relaxed);
            } else {
                LIVE.fetch_sub(layout.size() - size, Ordering::Relaxed);
            }
        }
        q
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// The peak growth of the live heap inside `f`.
fn peak_of<T>(f: impl FnOnce() -> T) -> (T, u64) {
    let base = LIVE.load(Ordering::Relaxed);
    PEAK.store(base, Ordering::Relaxed);
    let out = f();
    (
        out,
        (PEAK.load(Ordering::Relaxed).saturating_sub(base)) as u64,
    )
}

fn lock(bytes: Vec<u8>) -> LockingScript {
    LockingScript::from_script(Script::from_binary(&bytes).unwrap())
}

fn input(txid: &str, vout: u32, unlock: &[u8]) -> TransactionInput {
    TransactionInput {
        source_txid: Some(txid.into()),
        source_output_index: vout,
        unlocking_script: Some(UnlockingScript::from_script(
            Script::from_binary(unlock).unwrap(),
        )),
        ..Default::default()
    }
}

fn p2pkh() -> Vec<u8> {
    let mut v = vec![0x76, 0xa9, 0x14];
    v.extend([7u8; 20]);
    v.extend([0x88, 0xac]);
    v
}

/// A parent of `inputs` inputs and `outputs` outputs (each `lock(i)`), proven
/// by a BUMP of `leaves` level-0 leaves when `leaves` is `Some`.
fn parent(
    tag: u32,
    inputs: u32,
    outputs: u32,
    lock_of: &dyn Fn(u32) -> Vec<u8>,
    leaves: Option<u64>,
) -> Transaction {
    let mut tx = Transaction::new();
    for i in 0..inputs {
        tx.inputs.push(input(
            &format!("{:056x}{tag:08x}", u64::from(i) + 7),
            0,
            &[],
        ));
    }
    for i in 0..outputs {
        tx.outputs.push(TransactionOutput::new(1, lock(lock_of(i))));
    }
    if let Some(n) = leaves {
        let mut level = vec![MerklePathLeaf::new_txid(0, tx.id())];
        for offset in 1..n {
            level.push(MerklePathLeaf::new(offset, format!("{offset:064x}")));
        }
        let height = 64 - (n.max(2) - 1).leading_zeros() as usize;
        let mut path = vec![level];
        path.resize_with(height, Vec::new);
        tx.merkle_path = Some(MerklePath::new(900_000, path).unwrap());
    }
    tx
}

/// A child spending the first `each` outputs of every parent; the BEEF holds
/// the parents (with their BUMPs) then the child.
fn spend_all(parents: &[Transaction], each: u32) -> (Vec<u8>, String) {
    let mut child = Transaction::new();
    for p in parents {
        let id = p.id();
        for vout in 0..each.min(p.outputs.len() as u32) {
            child.inputs.push(input(&id, vout, &[]));
        }
    }
    child
        .outputs
        .push(TransactionOutput::new(1, lock(vec![0x51])));
    let mut beef = Beef::new();
    for p in parents {
        let bump = p.merkle_path.clone().map(|m| beef.merge_bump(m));
        beef.merge_raw_tx(p.to_binary(), bump);
    }
    beef.merge_raw_tx(child.to_binary(), None);
    (beef.to_binary(), child.id())
}

/// An unproven chain of `k` one-input, one-output transactions over a proven root.
fn chain(k: u32) -> (Vec<u8>, String) {
    let root = parent(1, 1, 1, &|_| vec![0x51], Some(1));
    let mut beef = Beef::new();
    let bump = beef.merge_bump(root.merkle_path.clone().unwrap());
    beef.merge_raw_tx(root.to_binary(), Some(bump));
    let mut id = root.id();
    for _ in 0..k {
        let mut tx = Transaction::new();
        tx.inputs.push(input(&id, 0, &[]));
        tx.outputs.push(TransactionOutput::new(1, lock(vec![0x51])));
        id = tx.id();
        beef.merge_raw_tx(tx.to_binary(), None);
    }
    (beef.to_binary(), id)
}

struct Shape {
    name: &'static str,
    beef: Vec<u8>,
    subject: String,
}

fn shape(name: &'static str, (beef, subject): (Vec<u8>, String)) -> Shape {
    Shape {
        name,
        beef,
        subject,
    }
}

fn one(_: u32) -> Vec<u8> {
    vec![0x51]
}

fn empty(_: u32) -> Vec<u8> {
    Vec::new()
}

/// The worst shapes built here: one-byte and empty locks, P2PKH, large locks,
/// an input-heavy parent, an unproven parent, several parents, a parent
/// proven by a wide BUMP, a deep chain; and the largest sweep and chain that
/// link.
fn worst_shapes() -> Vec<Shape> {
    vec![
        shape(
            "one-byte locks, 4,000 outputs, 420 spent",
            spend_all(&[parent(1, 1, 4_000, &one, Some(1))], 420),
        ),
        shape(
            "one-byte locks, 1,000 outputs, all spent",
            spend_all(&[parent(1, 1, 1_000, &one, Some(1))], 1_000),
        ),
        shape(
            "empty locks, 4,000 outputs, 400 spent",
            spend_all(&[parent(1, 1, 4_000, &empty, Some(1))], 400),
        ),
        shape(
            "P2PKH, 1,000 outputs, 200 spent",
            spend_all(&[parent(1, 1, 1_000, &|_| p2pkh(), Some(1))], 200),
        ),
        shape(
            "200-byte locks, 500 outputs, 200 spent",
            spend_all(&[parent(1, 1, 500, &|_| vec![0x61; 200], Some(1))], 200),
        ),
        shape(
            "10,000-byte locks, 100 outputs, all spent",
            spend_all(&[parent(1, 1, 100, &|_| vec![0x61; 10_000], Some(1))], 100),
        ),
        shape(
            "2,000 inputs, 400 one-byte outputs, all spent",
            spend_all(&[parent(1, 2_000, 400, &one, Some(1))], 400),
        ),
        shape(
            "unproven parent, 4,000 one-byte outputs, 420 spent",
            spend_all(&[parent(1, 1, 4_000, &one, None)], 420),
        ),
        shape(
            "five unproven parents, 1,000 outputs, 200 of each spent",
            spend_all(
                &(0..5)
                    .map(|k| parent(k, 1, 1_000, &one, None))
                    .collect::<Vec<_>>(),
                200,
            ),
        ),
        shape(
            "a BUMP of 16,384 leaves, 200 outputs, all spent",
            spend_all(&[parent(1, 1, 200, &one, Some(16_384))], 200),
        ),
        shape("an unproven chain of 20,000", chain(20_000)),
        // The largest that link, so an admitted link is held to the bound too.
        shape(
            "P2PKH, 170 outputs, all spent",
            spend_all(&[parent(1, 1, 170, &|_| p2pkh(), Some(1))], 170),
        ),
        shape("an unproven chain of 6,700", chain(6_700)),
    ]
}

/// What the production read may hold: the link bound, and what is linear in
/// the body (the door's parse, a hydrated `Beef`, and the subject's clone).
fn read_bound(body: usize) -> u64 {
    beef_limits::LINK_HEAP_MAX_BYTES + 24 * body as u64 + 1024 * 1024
}

/// THE PIN: the production linking read, on every worst shape, holds at most
/// the link bound beside the body's parse, whether it links or declines.
#[test]
fn p0811_l_the_link_holds_at_most_its_heap_bound() {
    let _one = ONE_AT_A_TIME.lock().unwrap();
    for s in worst_shapes() {
        let (answer, peak) = peak_of(|| {
            beef_limits::transaction_from_beef(
                &s.beef,
                Some(&s.subject),
                &beef_limits::ENGINE_BEEF_LIMITS,
            )
            .map(|tx| tx.inputs.len())
        });
        let verdict = match &answer {
            Ok(n) => format!("linked ({n} inputs)"),
            Err(e) if beef_limits::is_link_bound(e) => "declined (link bound)".to_string(),
            Err(e) => panic!("{}: not a link-bound answer: {e}", s.name),
        };
        println!(
            "LINK {}: body {} bytes, peak {} bytes ({:.1}x the body), {verdict}",
            s.name,
            s.beef.len(),
            peak,
            peak as f64 / s.beef.len() as f64
        );
        assert!(
            peak <= read_bound(s.beef.len()),
            "{}: the read held {peak} bytes for a body of {} (bound {})",
            s.name,
            s.beef.len(),
            read_bound(s.beef.len())
        );
    }
}

/// The estimate is above the heap bsv-rs's link holds, on every shape it
/// admits (each worst shape's admitted size, linked UNBOUNDED on a parsed
/// BEEF, and LOW's real fixtures).
#[test]
fn p0811_l_the_estimate_is_above_the_measured_link() {
    let _one = ONE_AT_A_TIME.lock().unwrap();
    let mut shapes = vec![
        shape(
            "one-byte locks, 400 outputs, all spent",
            spend_all(&[parent(1, 1, 400, &one, Some(1))], 400),
        ),
        shape(
            "empty locks, 400 outputs, all spent",
            spend_all(&[parent(1, 1, 400, &empty, Some(1))], 400),
        ),
        shape(
            "P2PKH, 150 outputs, all spent",
            spend_all(&[parent(1, 1, 150, &|_| p2pkh(), Some(1))], 150),
        ),
        shape(
            "200-byte locks, 100 outputs, all spent",
            spend_all(&[parent(1, 1, 100, &|_| vec![0x61; 200], Some(1))], 100),
        ),
        shape(
            "10,000-byte locks, 10 outputs, all spent",
            spend_all(&[parent(1, 1, 10, &|_| vec![0x61; 10_000], Some(1))], 10),
        ),
        shape(
            "500 inputs, 100 one-byte outputs, all spent",
            spend_all(&[parent(1, 500, 100, &one, Some(1))], 100),
        ),
        shape(
            "five unproven parents, 100 outputs, 100 of each spent",
            spend_all(
                &(0..5)
                    .map(|k| parent(k, 1, 100, &one, None))
                    .collect::<Vec<_>>(),
                100,
            ),
        ),
        shape(
            "a BUMP of 4,096 leaves, 10 outputs, all spent",
            spend_all(&[parent(1, 1, 10, &one, Some(4_096))], 10),
        ),
        shape("an unproven chain of 2,000", chain(2_000)),
    ];
    for rel in [
        "../overlay-cloudflare/tests/fixtures/ef/loop2_join_5ad2764c_incomplete.beef",
        "../overlay-cloudflare/tests/fixtures/ef/loop2_join_f9e85aab_represent.beef",
        "tests/fixtures/zanaadu-pf-head-dc4ca9ea.beef",
    ] {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(rel);
        let beef = std::fs::read(&path).unwrap();
        let mut parsed = beef_limits::parse_beef(&beef, &beef_limits::STORED_BEEF_LIMITS).unwrap();
        let subject = bsv_overlay_engine::subject::subject_txid_of(&mut parsed).unwrap();
        shapes.push(Shape {
            name: "a real fixture",
            beef,
            subject,
        });
    }
    for s in shapes {
        let parsed = beef_limits::parse_beef(&s.beef, &beef_limits::ENGINE_BEEF_LIMITS).unwrap();
        let estimate = beef_limits::link_heap_estimate(&parsed, &s.subject, u64::MAX).total();
        let (linked, measured) = peak_of(|| {
            parsed
                .find_atomic_transaction_limited(&s.subject, &BeefLinkLimits::unbounded())
                .unwrap()
                .map(|tx| tx.inputs.len())
        });
        assert!(linked.is_some(), "{}: the subject links", s.name);
        println!(
            "ESTIMATE {}: measured {measured}, estimated {estimate} ({:.2}x)",
            s.name,
            estimate as f64 / measured.max(1) as f64
        );
        assert!(
            estimate >= measured,
            "{}: the estimate {estimate} is under the measured {measured}",
            s.name
        );
    }
}

/// Honest shapes link: LOW's fixtures (`p0811_b_b` links every transaction of
/// each), a single child sweeping one P2PKH parent and an unproven chain, up
/// to the sizes named here, and a proven subject of any size (its own clone).
#[test]
fn p0811_l_the_largest_p2pkh_sweep_it_links() {
    let _one = ONE_AT_A_TIME.lock().unwrap();
    let links = |n: u32| {
        let (beef, child) = spend_all(&[parent(1, 1, n, &|_| p2pkh(), Some(1))], n);
        beef_limits::transaction_from_beef(&beef, Some(&child), &beef_limits::ENGINE_BEEF_LIMITS)
            .is_ok()
    };
    let (mut lo, mut hi) = (1u32, 2_000u32);
    assert!(links(lo) && !links(hi));
    while hi - lo > 1 {
        let mid = (lo + hi) / 2;
        if links(mid) {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    println!("SWEEP the largest P2PKH same-parent sweep that links: {lo} outputs");
    assert!(
        lo >= 100,
        "LOW's fans are at most 20; a sweep of {lo} is the bound"
    );
    let deep = |k: u32| {
        let (beef, tip) = chain(k);
        beef_limits::transaction_from_beef(&beef, Some(&tip), &beef_limits::ENGINE_BEEF_LIMITS)
            .is_ok()
    };
    let (mut lo, mut hi) = (1u32, 20_000u32);
    assert!(deep(lo) && !deep(hi));
    while hi - lo > 1 {
        let mid = (lo + hi) / 2;
        if deep(mid) {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    println!("SWEEP the deepest unproven chain that links: {lo} transactions");
    assert!(
        lo >= 1_000,
        "LOW's deepest unproven ancestry is about 40 hops; a chain of {lo} is the bound"
    );
    // A proven subject of any size is its own clone: never declined.
    let mut big = Transaction::new();
    big.inputs.push(input(&"cc".repeat(32), 0, &[]));
    let mut data = vec![0x00, 0x6a, 0x4e];
    data.extend(&5_000_000u32.to_le_bytes());
    data.resize(data.len() + 5_000_000, 0x42);
    big.outputs.push(TransactionOutput::new(1, lock(data)));
    let (beef, id) = {
        let tx = parent_proven(big);
        let mut beef = Beef::new();
        let bump = beef.merge_bump(tx.merkle_path.clone().unwrap());
        beef.merge_raw_tx(tx.to_binary(), Some(bump));
        (beef.to_binary(), tx.id())
    };
    beef_limits::transaction_from_beef(&beef, Some(&id), &beef_limits::ENGINE_BEEF_LIMITS)
        .expect("a proven 5 MB subject links");
}

/// `tx` proven by a one-leaf BUMP at 900,000.
fn parent_proven(mut tx: Transaction) -> Transaction {
    let id = tx.id();
    tx.merkle_path =
        Some(MerklePath::new(900_000, vec![vec![MerklePathLeaf::new_txid(0, id)]]).unwrap());
    tx
}
