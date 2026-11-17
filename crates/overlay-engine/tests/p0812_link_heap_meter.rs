//! LOCK-0812: bsv-rs 0.8.12's heap meter beside the engine's link bound.
//!
//! bsv-rs 0.8.12 (GHSA-33fm-jcm5-4m3m) meters a link in two dimensions: the
//! wire bytes of each repeated stub (`max_link_bytes`, which the engine sets
//! at `beef_limits::LINK_WIRE_MAX_BYTES`) and the heap each clone holds
//! (`max_link_heap_bytes`). These pins measure, for every shape the PORT-0811
//! heap pins build, what bsv-rs's heap meter counts beside the engine's own
//! estimate and the heap the link holds, and hold the figure the engine hands
//! bsv-rs above every link the estimate admits.
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

fn one(_: u32) -> Vec<u8> {
    vec![0x51]
}

fn empty(_: u32) -> Vec<u8> {
    Vec::new()
}

fn p2pkh_sweep(n: u32) -> (Vec<u8>, String) {
    spend_all(&[parent(1, 1, n, &|_| p2pkh(), Some(1))], n)
}

fn fifty_byte_locks(spent: u32) -> (Vec<u8>, String) {
    spend_all(&[parent(1, 1, 300, &|_| vec![0x61u8; 50], Some(1))], spent)
}

/// A proven subject of one input and one output whose own BUMP carries
/// `level0` level-0 leaves (`level0` even), plus a duplicate leaf at every
/// higher level whose width is odd, so the path computes a root. Answers the
/// BEEF, the subject's txid and the BUMP's leaves over every level.
fn proven_subject_with(level0: u64) -> (Vec<u8>, String, u64) {
    assert!(level0 >= 2 && level0 % 2 == 0);
    let mut tx = Transaction::new();
    tx.inputs.push(input(&format!("{:064x}", 0x5b_u64), 0, &[]));
    tx.outputs.push(TransactionOutput::new(1, lock(vec![0x51])));
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
    let leaves = path.iter().map(|l| l.len() as u64).sum();
    tx.merkle_path = Some(MerklePath::new(900_000, path).unwrap());
    let mut beef = Beef::new();
    let bump = beef.merge_bump(tx.merkle_path.clone().unwrap());
    beef.merge_raw_tx(tx.to_binary(), Some(bump));
    (beef.to_binary(), tx.id(), leaves)
}

fn proven_subject(level0: u64) -> (Vec<u8>, String) {
    let (beef, id, _) = proven_subject_with(level0);
    (beef, id)
}

/// A proven subject of 5 MB (one data output), its BUMP of one leaf.
fn proven_five_mb() -> (Vec<u8>, String) {
    let mut big = Transaction::new();
    big.inputs.push(input(&"cc".repeat(32), 0, &[]));
    let mut data = vec![0x00, 0x6a, 0x4e];
    data.extend(&5_000_000u32.to_le_bytes());
    data.resize(data.len() + 5_000_000, 0x42);
    big.outputs.push(TransactionOutput::new(1, lock(data)));
    let id = big.id();
    big.merkle_path = Some(
        MerklePath::new(900_000, vec![vec![MerklePathLeaf::new_txid(0, id.clone())]]).unwrap(),
    );
    let mut beef = Beef::new();
    let bump = beef.merge_bump(big.merkle_path.clone().unwrap());
    beef.merge_raw_tx(big.to_binary(), Some(bump));
    (beef.to_binary(), id)
}

/// The search's ceiling: a count past it is reported as past it.
const SEARCH_CAP: u64 = 512 * 1024 * 1024;

/// The least figure under which `links(figure)` holds (the meter refuses only
/// a count PAST its figure, so this is the count), or `None` past the cap.
fn least_figure(links: impl Fn(u64) -> bool) -> Option<u64> {
    if links(0) {
        return Some(0);
    }
    let mut hi = 1024u64;
    while !links(hi) {
        if hi >= SEARCH_CAP {
            return None;
        }
        hi = (hi * 2).min(SEARCH_CAP);
    }
    let mut lo = hi / 2;
    if hi == 1024 {
        lo = 0;
    }
    while hi - lo > 1 {
        let mid = lo + (hi - lo) / 2;
        if links(mid) {
            hi = mid;
        } else {
            lo = mid;
        }
    }
    Some(hi)
}

/// What bsv-rs's HEAP meter counts for the link (the wire meter lifted).
fn heap_count(parsed: &Beef, subject: &str) -> Option<u64> {
    least_figure(|f| {
        parsed
            .find_atomic_transaction_limited(
                subject,
                &BeefLinkLimits::unbounded().with_max_link_heap_bytes(Some(f)),
            )
            .is_ok()
    })
}

/// What bsv-rs's WIRE meter counts for the link (the heap meter lifted).
fn wire_count(parsed: &Beef, subject: &str) -> Option<u64> {
    least_figure(|f| {
        parsed
            .find_atomic_transaction_limited(
                subject,
                &BeefLinkLimits::unbounded().with_max_link_bytes(Some(f)),
            )
            .is_ok()
    })
}

/// Which bound answers the production read: `linked`, or the bound that
/// declines (the engine's estimate, bsv-rs's wire meter, its heap meter).
fn verdict(beef: &[u8], subject: &str) -> &'static str {
    match beef_limits::transaction_from_beef(beef, Some(subject), &beef_limits::ENGINE_BEEF_LIMITS)
    {
        Ok(_) => "linked",
        Err(e) if beef_limits::is_link_bound(&e) => {
            let words = e.to_string();
            if words.contains("the engine's link bound") {
                "declined: the engine's estimate"
            } else if words.contains("heap bytes") {
                "declined: bsv-rs's heap meter"
            } else if words.contains("would re-materialize more than") {
                // The wire meter's words on bsv-rs 0.8.11 and 0.8.12 alike.
                "declined: bsv-rs's wire meter"
            } else {
                panic!("a link bound in no known words: {words}")
            }
        }
        Err(e) => panic!("not a link-bound answer: {e}"),
    }
}

struct Row {
    name: String,
    body: usize,
    beyond: u64,
    linear: u64,
    heap: Option<u64>,
    wire: Option<u64>,
    measured: Option<u64>,
    verdict: &'static str,
}

fn mib(n: Option<u64>) -> String {
    n.map_or_else(|| "-".into(), |n| format!("{n}"))
}

fn measure(name: &str, (beef, subject): (Vec<u8>, String), count: bool) -> Row {
    let parsed = beef_limits::parse_beef(&beef, &beef_limits::ENGINE_BEEF_LIMITS).unwrap();
    let est = beef_limits::link_heap_estimate(&parsed, &subject, u64::MAX);
    let heap = count.then(|| heap_count(&parsed, &subject)).flatten();
    let wire = count.then(|| wire_count(&parsed, &subject)).flatten();
    let measured = if count {
        let (linked, peak) = peak_of(|| {
            parsed
                .find_atomic_transaction_limited(&subject, &BeefLinkLimits::unbounded())
                .unwrap()
                .map(|tx| tx.inputs.len())
        });
        assert!(linked.is_some(), "{name}: the subject links unbounded");
        Some(peak)
    } else {
        None
    };
    let row = Row {
        name: name.into(),
        body: beef.len(),
        beyond: est.beyond,
        linear: est.linear,
        heap,
        wire,
        measured,
        verdict: verdict(&beef, &subject),
    };
    println!(
        "ROW | {} | {} | {} | {} | {} | {} | {} | {} |",
        row.name,
        row.body,
        row.beyond,
        row.linear,
        mib(row.heap),
        mib(row.wire),
        row.measured.map_or_else(|| "-".into(), |m| m.to_string()),
        row.verdict
    );
    row
}

fn fixture(rel: &str) -> (Vec<u8>, String) {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(rel);
    let beef = std::fs::read(&path).unwrap();
    let mut parsed = beef_limits::parse_beef(&beef, &beef_limits::STORED_BEEF_LIMITS).unwrap();
    let subject = bsv_overlay_engine::subject::subject_txid_of(&mut parsed).unwrap();
    (beef, subject)
}

/// The level-0 leaves of a proven subject's own BUMP on either side of the
/// heap figure (with the duplicates the path needs, 564,615 and 564,622 leaves
/// in all; the figure holds 564,617 at 104 bytes a leaf).
const FITS_LEVEL0: u64 = 564_608;
const PAST_LEVEL0: u64 = 564_610;

/// THE FIGURE: the engine hands bsv-rs its wire figure and its own heap figure
/// (56 MiB), and every per-clone charge of bsv-rs's heap meter is at most the
/// estimate's charge for the same thing, so on the ancestry bsv-rs counts
/// under the estimate, which `LINK_HEAP_MAX_BYTES` bounds. RED against a figure
/// under the estimate's bound, against bsv-rs's default handed instead once
/// that default moves, and against a later bsv-rs whose structs outgrow the
/// estimate's charges.
#[test]
fn p0812_the_engine_names_its_heap_figure() {
    let limits = beef_limits::engine_link_limits();
    assert_eq!(limits.max_link_bytes, Some(986_895));
    assert_eq!(beef_limits::LINK_METER_HEAP_MAX_BYTES, 56 * 1024 * 1024);
    assert_eq!(
        limits.max_link_heap_bytes,
        Some(beef_limits::LINK_METER_HEAP_MAX_BYTES)
    );
    assert!(beef_limits::LINK_METER_HEAP_MAX_BYTES >= beef_limits::LINK_HEAP_MAX_BYTES);
    let size = |n: usize| n as u64;
    assert!(size(std::mem::size_of::<Transaction>()) <= beef_limits::LINK_CHARGE_TX);
    assert!(size(std::mem::size_of::<TransactionInput>()) <= beef_limits::LINK_CHARGE_INPUT);
    assert!(size(std::mem::size_of::<TransactionOutput>()) <= beef_limits::LINK_CHARGE_OUTPUT);
    // bsv-rs's BUMP leaf: its struct and its 64-character hex hash.
    assert!(size(std::mem::size_of::<MerklePathLeaf>()) + 64 <= beef_limits::LINK_CHARGE_BUMP_LEAF);
    println!(
        "FIGURE wire {:?}, heap {:?} (bsv-rs's default {}); sizes: Transaction {}, input {}, output {}, BUMP leaf {} + 64",
        limits.max_link_bytes,
        limits.max_link_heap_bytes,
        BeefLinkLimits::DEFAULT_MAX_LINK_HEAP_BYTES,
        std::mem::size_of::<Transaction>(),
        std::mem::size_of::<TransactionInput>(),
        std::mem::size_of::<TransactionOutput>(),
        std::mem::size_of::<MerklePathLeaf>()
    );
}

const ESTIMATE: &str = "declined: the engine's estimate";
const WIRE: &str = "declined: bsv-rs's wire meter";
const LINKED: &str = "linked";

/// Every shape the PORT-0811 pins build: the bound that answers it (linked, or
/// declined by the estimate or by the wire meter, as before the heap meter),
/// and, for each one that links, bsv-rs's heap count under the estimate's and
/// under the engine's heap figure. RED against a heap figure under any linked
/// shape's count (the largest, a proven subject's BUMP of 262,144 leaves,
/// counts 27,262,976), and against a bsv-rs whose heap meter declines any of
/// them first.
#[test]
fn p0812_every_shape_keeps_its_bound() {
    let _one = ONE_AT_A_TIME.lock().unwrap();
    let shapes: Vec<(&str, (Vec<u8>, String), &str)> = vec![
        (
            "one-byte locks, 4,000 outputs, 420 spent",
            spend_all(&[parent(1, 1, 4_000, &one, Some(1))], 420),
            ESTIMATE,
        ),
        (
            "one-byte locks, 1,000 outputs, all spent",
            spend_all(&[parent(1, 1, 1_000, &one, Some(1))], 1_000),
            ESTIMATE,
        ),
        (
            "empty locks, 4,000 outputs, 400 spent",
            spend_all(&[parent(1, 1, 4_000, &empty, Some(1))], 400),
            ESTIMATE,
        ),
        (
            "P2PKH, 1,000 outputs, 200 spent",
            spend_all(&[parent(1, 1, 1_000, &|_| p2pkh(), Some(1))], 200),
            ESTIMATE,
        ),
        (
            "200-byte locks, 500 outputs, 200 spent",
            spend_all(&[parent(1, 1, 500, &|_| vec![0x61; 200], Some(1))], 200),
            ESTIMATE,
        ),
        (
            "10,000-byte locks, 100 outputs, all spent",
            spend_all(&[parent(1, 1, 100, &|_| vec![0x61; 10_000], Some(1))], 100),
            ESTIMATE,
        ),
        (
            "2,000 inputs, 400 one-byte outputs, all spent",
            spend_all(&[parent(1, 2_000, 400, &one, Some(1))], 400),
            ESTIMATE,
        ),
        (
            "unproven parent, 4,000 one-byte outputs, 420 spent",
            spend_all(&[parent(1, 1, 4_000, &one, None)], 420),
            ESTIMATE,
        ),
        (
            "five unproven parents, 1,000 outputs, 200 of each spent",
            spend_all(
                &(0..5)
                    .map(|k| parent(k, 1, 1_000, &one, None))
                    .collect::<Vec<_>>(),
                200,
            ),
            ESTIMATE,
        ),
        (
            "a BUMP of 16,384 leaves, 200 outputs, all spent",
            spend_all(&[parent(1, 1, 200, &one, Some(16_384))], 200),
            ESTIMATE,
        ),
        ("an unproven chain of 20,000", chain(20_000), ESTIMATE),
        (
            "one-byte locks, 400 outputs, all spent",
            spend_all(&[parent(1, 1, 400, &one, Some(1))], 400),
            ESTIMATE,
        ),
        (
            "empty locks, 400 outputs, all spent",
            spend_all(&[parent(1, 1, 400, &empty, Some(1))], 400),
            ESTIMATE,
        ),
        (
            "500 inputs, 100 one-byte outputs, all spent",
            spend_all(&[parent(1, 500, 100, &one, Some(1))], 100),
            ESTIMATE,
        ),
        (
            "200-byte locks, 100 outputs, all spent",
            spend_all(&[parent(1, 1, 100, &|_| vec![0x61; 200], Some(1))], 100),
            WIRE,
        ),
        (
            "P2PKH, 150 outputs, all spent",
            spend_all(&[parent(1, 1, 150, &|_| p2pkh(), Some(1))], 150),
            LINKED,
        ),
        (
            "10,000-byte locks, 10 outputs, all spent",
            spend_all(&[parent(1, 1, 10, &|_| vec![0x61; 10_000], Some(1))], 10),
            LINKED,
        ),
        (
            "five unproven parents, 100 outputs, 100 of each spent",
            spend_all(
                &(0..5)
                    .map(|k| parent(k, 1, 100, &one, None))
                    .collect::<Vec<_>>(),
                100,
            ),
            LINKED,
        ),
        (
            "a BUMP of 4,096 leaves, 10 outputs, all spent",
            spend_all(&[parent(1, 1, 10, &one, Some(4_096))], 10),
            LINKED,
        ),
        ("an unproven chain of 2,000", chain(2_000), LINKED),
        ("the largest P2PKH sweep, 170", p2pkh_sweep(170), LINKED),
        ("a P2PKH sweep of 171", p2pkh_sweep(171), WIRE),
        ("the deepest unproven chain, 6,700", chain(6_700), LINKED),
        ("an unproven chain of 6,701", chain(6_701), ESTIMATE),
        (
            "50-byte locks, 300 outputs, 56 spent",
            fifty_byte_locks(56),
            LINKED,
        ),
        (
            "50-byte locks, 300 outputs, 57 spent",
            fifty_byte_locks(57),
            WIRE,
        ),
        ("a proven 5 MB subject", proven_five_mb(), LINKED),
        (
            "a proven subject, its BUMP of 16,384 leaves",
            proven_subject(16_384),
            LINKED,
        ),
        (
            "a proven subject, its BUMP of 262,144 leaves",
            proven_subject(262_144),
            LINKED,
        ),
        (
            "LOW fixture loop2_join_5ad2764c_incomplete",
            fixture("../overlay-cloudflare/tests/fixtures/ef/loop2_join_5ad2764c_incomplete.beef"),
            LINKED,
        ),
        (
            "LOW fixture loop2_join_f9e85aab_represent",
            fixture("../overlay-cloudflare/tests/fixtures/ef/loop2_join_f9e85aab_represent.beef"),
            LINKED,
        ),
        (
            "Zanaadu fixture pf-head-dc4ca9ea",
            fixture("tests/fixtures/zanaadu-pf-head-dc4ca9ea.beef"),
            LINKED,
        ),
    ];
    println!("ROW | shape | body | estimate beyond | estimate linear | bsv-rs heap count | bsv-rs wire count | measured link heap | verdict |");
    let mut largest = 0u64;
    for (name, s, expected) in shapes {
        let row = measure(name, s, expected == LINKED);
        assert_eq!(row.verdict, expected, "{name}: the bound that answers it");
        if expected == LINKED {
            let heap = row.heap.expect("a linked shape is counted");
            assert!(
                heap <= row.beyond.saturating_add(row.linear),
                "{name}: bsv-rs counts {heap}, over the estimate's {} + {}",
                row.beyond,
                row.linear
            );
            if row.beyond > 0 {
                assert!(
                    heap <= row.beyond,
                    "{name}: an ancestry counts {heap} over the estimate's {}",
                    row.beyond
                );
            }
            assert!(heap <= beef_limits::LINK_METER_HEAP_MAX_BYTES);
            largest = largest.max(heap);
        }
    }
    println!(
        "LARGEST bsv-rs heap count of a linked shape: {largest} ({:.2}x under the figure)",
        beef_limits::LINK_METER_HEAP_MAX_BYTES as f64 / largest as f64
    );
}

/// A PROVEN subject's own BUMP, which the estimate counts as linear (never
/// declined), is declined by bsv-rs's heap meter past the figure: 564,615
/// leaves link, 564,622 are declined, each with an ancestry of nothing. On
/// bsv-rs 0.8.11 both linked. Never "not found": `is_link_bound`, the heap
/// meter's words. Release profile only (two bodies of 21 MB).
#[test]
#[cfg_attr(
    debug_assertions,
    ignore = "release only: cargo test --release --features memory-storage --test p0812_link_heap_meter"
)]
fn p0812_a_proven_subjects_own_bump_past_the_figure_is_declined() {
    let _one = ONE_AT_A_TIME.lock().unwrap();
    for (level0, expected) in [(FITS_LEVEL0, None), (PAST_LEVEL0, Some(()))] {
        let (beef, id, leaves) = proven_subject_with(level0);
        let parsed = beef_limits::parse_beef(&beef, &beef_limits::ENGINE_BEEF_LIMITS).unwrap();
        assert_eq!(
            beef_limits::link_heap_estimate(&parsed, &id, u64::MAX).beyond,
            0,
            "a proven subject has no ancestry to link"
        );
        let answer = beef_limits::linked(&parsed, &id);
        println!(
            "SUBJECT BUMP of {leaves} leaves ({} bytes a leaf, figure {}), a body of {} bytes: {:?}",
            std::mem::size_of::<MerklePathLeaf>() + 64,
            beef_limits::LINK_METER_HEAP_MAX_BYTES,
            beef.len(),
            answer.as_ref().map(|t| t.as_ref().map(Transaction::id)).map_err(ToString::to_string)
        );
        match (answer, expected) {
            (Ok(Some(tx)), None) => assert_eq!(tx.id(), id),
            (Err(e), Some(())) => {
                assert!(beef_limits::is_link_bound(&e), "{e}");
                assert!(
                    e.to_string().contains("heap bytes"),
                    "the heap meter's words: {e}"
                );
            }
            (other, _) => panic!("{leaves} leaves: {:?}", other.map(|t| t.map(|t| t.id()))),
        }
    }
}
