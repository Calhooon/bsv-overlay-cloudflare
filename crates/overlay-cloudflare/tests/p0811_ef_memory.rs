//! PORT-0811 fold: the Extended Format batch's memory follows the body.
//!
//! `ef::beef_to_ef_batch` writes each input's EF entry from the one output it
//! spends, read in place from the source the BEEF holds; it links no source.
//! The peak growth of the live heap (a counting global allocator, the
//! `gasp_fanin_memory.rs` shape) inside each call is held under a constant
//! multiple of the body's bytes over a ladder of fan-in BEEFs (one unproven
//! parent of N outputs, one unproven child spending M of them), P2PKH-shaped
//! and one-byte-lock shaped, through the batch itself, through the census
//! (`submit_census::census_verdict`, which calls it first on every ungated
//! submit), and for the function the deferred EF job's run reaches (the same
//! `beef_to_ef_batch`, through `submit_resumed` and `submit_parts`; pinned by
//! source below). The gated arm's earlier parse (`ef::missing_source_txids`)
//! is measured and printed beside them, not bounded here.
//!
//! Native only: the heap of a wasm32 build is not measured (its
//! `TransactionOutput` is 80 bytes where it is 136 natively, so its per-byte
//! figures are no larger).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::cast_possible_truncation,
    clippy::print_stdout,
    reason = "test code"
)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use bsv_overlay_cloudflare::{ef, submit_census};
use bsv_rs::script::{LockingScript, UnlockingScript};
use bsv_rs::transaction::{Beef, Transaction, TransactionInput, TransactionOutput};

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

struct Counting;

// SAFETY: every call forwards to `System` with the caller's own layout and
// pointer; the counters are only arithmetic beside it.
#[allow(unsafe_code)]
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded as is.
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            let live = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        // SAFETY: forwarded as is.
        unsafe { System.dealloc(p, layout) };
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// The peak growth of the live heap while `f` runs, over the level at its entry.
fn peak_of<T>(f: impl FnOnce() -> T) -> (T, usize) {
    let base = LIVE.load(Ordering::Relaxed);
    PEAK.store(base, Ordering::Relaxed);
    let out = f();
    (out, PEAK.load(Ordering::Relaxed).saturating_sub(base))
}

fn p2pkh(i: u32) -> LockingScript {
    let mut s = vec![0x76, 0xa9, 0x14];
    s.extend_from_slice(&[0u8; 16]);
    s.extend_from_slice(&i.to_le_bytes());
    s.extend_from_slice(&[0x88, 0xac]);
    LockingScript::from_binary(&s).unwrap()
}

fn one_byte(_: u32) -> LockingScript {
    LockingScript::from_binary(&[0x51]).unwrap()
}

/// A signature-and-key shaped push-only unlock (107 bytes), or an empty one.
fn unlock(k: u32, minimal: bool) -> UnlockingScript {
    if minimal {
        return UnlockingScript::from_binary(&[]).unwrap();
    }
    let mut s = vec![0x48];
    s.extend_from_slice(&[0x30; 68]);
    s.extend_from_slice(&k.to_le_bytes());
    s.push(0x21);
    s.extend_from_slice(&[0x02; 33]);
    UnlockingScript::from_binary(&s).unwrap()
}

/// One unproven parent of `n` outputs (`lock(i)` each; its own input names a
/// txid the BEEF does not carry), one unproven child spending vouts `0..m`.
fn fan_in(n: u32, m: u32, lock: fn(u32) -> LockingScript, minimal: bool) -> (Vec<u8>, String) {
    let mut parent = Transaction::new();
    parent.inputs.push(TransactionInput {
        source_txid: Some("ab".repeat(32)),
        unlocking_script: Some(unlock(0, minimal)),
        sequence: u32::MAX,
        ..Default::default()
    });
    for i in 0..n {
        parent.outputs.push(TransactionOutput::new(1, lock(i)));
    }
    let parent_id = parent.id();
    let mut child = Transaction::new();
    for k in 0..m {
        child.inputs.push(TransactionInput {
            source_txid: Some(parent_id.clone()),
            source_output_index: k,
            unlocking_script: Some(unlock(k, minimal)),
            sequence: u32::MAX,
            ..Default::default()
        });
    }
    child.outputs.push(TransactionOutput::new(1, p2pkh(0)));
    let child_id = child.id();
    let mut beef = Beef::new();
    beef.merge_raw_tx(parent.to_binary(), None);
    beef.merge_raw_tx(child.to_binary(), None);
    (beef.to_binary(), child_id)
}

/// The bound: the batch's peak growth is at most this many times the body's
/// bytes, plus a constant floor for a small body. Measured on this fold's
/// tip at 6.6 to 14.8 times, at or under the gated arm's own earlier parse
/// (`missing_source_txids`, 7.2 to 15.1 times on the same shapes).
const PEAK_PER_BODY_BYTE: usize = 24;
const PEAK_FLOOR: usize = 64 * 1024;

fn bound(body: usize) -> usize {
    PEAK_PER_BODY_BYTE * body + PEAK_FLOOR
}

/// The ladders the fold's report measured (N parent outputs x M inputs).
const P2PKH_LADDER: &[(u32, u32)] = &[
    (100, 100),
    (250, 250),
    (500, 500),
    (1_000, 1_000),
    (2_000, 2_000),
    (2_000, 250),
    (1_720, 400),
];
const ONE_BYTE_LADDER: &[(u32, u32)] = &[
    (1_000, 1_000),
    (2_000, 500),
    (1_900, 460),
    (1_930, 470),
    (20_000, 20_000),
];

/// PORT-0811 fold: the batch's peak heap is a constant multiple of the body
/// over both ladders, however many of the parent's outputs the child spends.
/// The subject is converted and its EF carries every input.
#[test]
fn p0811_f1_the_ef_batch_memory_follows_the_body() {
    let _one = ONE_AT_A_TIME
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for (ladder, lock, minimal, label) in [
        (
            P2PKH_LADDER,
            p2pkh as fn(u32) -> LockingScript,
            false,
            "p2pkh",
        ),
        (
            ONE_BYTE_LADDER,
            one_byte as fn(u32) -> LockingScript,
            true,
            "one-byte",
        ),
    ] {
        for &(n, m) in ladder {
            let (beef, child) = fan_in(n, m, lock, minimal);
            let (_, parse_peak) = peak_of(|| ef::missing_source_txids(&beef));
            let (converted, peak) = peak_of(|| ef::beef_to_ef_batch(&beef));
            let (efs, subject) = converted.expect("the fan-in converts");
            assert_eq!(subject, child);
            let subject_ef = efs
                .iter()
                .find(|e| e.txid == subject)
                .expect("the subject's leg");
            println!(
                "EF {label} N={n} M={m} body={} batch_peak={peak} ({:.2}x) parse_peak={parse_peak} ({:.2}x) subject_ef={}",
                beef.len(),
                peak as f64 / beef.len() as f64,
                parse_peak as f64 / beef.len() as f64,
                subject_ef.ef.len()
            );
            assert!(
                peak <= bound(beef.len()),
                "{label} {n}x{m}: the batch held {peak} bytes for a body of {} (bound {})",
                beef.len(),
                bound(beef.len())
            );
        }
    }
}

/// PORT-0811 fold: the census (every ungated submit's classification, which
/// runs the gated arm's own conversion first) holds the same bound over both
/// ladders' widest shapes.
#[test]
fn p0811_f1_the_census_memory_follows_the_body() {
    let _one = ONE_AT_A_TIME
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for (n, m, lock, minimal, label) in [
        (
            1_930u32,
            470u32,
            one_byte as fn(u32) -> LockingScript,
            true,
            "one-byte",
        ),
        (1_720, 400, p2pkh, false, "p2pkh"),
        (2_000, 2_000, p2pkh, false, "p2pkh"),
        (20_000, 20_000, one_byte, true, "one-byte"),
    ] {
        let (beef, _) = fan_in(n, m, lock, minimal);
        let (verdict, peak) = peak_of(|| submit_census::census_verdict(&beef));
        println!(
            "CENSUS {label} N={n} M={m} body={} peak={peak} ({:.2}x) verdict={}",
            beef.len(),
            peak as f64 / beef.len() as f64,
            verdict.as_str()
        );
        assert!(
            peak <= bound(beef.len()),
            "{label} {n}x{m}: the census held {peak} bytes for a body of {} (bound {})",
            beef.len(),
            bound(beef.len())
        );
    }
}

/// PORT-0811 fold: the deferred EF job's run converts through the same
/// function the request does (`submit_resumed` hands its bytes to
/// `submit_parts`, whose gated arm calls `ef::beef_to_ef_batch`), so the
/// bound above is the job's too. A second conversion would be a second thing
/// to bound: none exists.
#[test]
fn p0811_f1_the_deferred_run_converts_through_the_same_batch() {
    let routes = include_str!("../src/routes.rs");
    let resumed = &routes[routes.find("pub(crate) async fn submit_resumed(").unwrap()..];
    let resumed = &resumed[..resumed.find("async fn submit_parts(").unwrap()];
    assert!(
        resumed.contains("submit_parts("),
        "the job's run is the request's arm"
    );
    let parts = &routes[routes.find("async fn submit_parts(").unwrap()..];
    let parts = &parts[..parts
        .find("\nasync fn ")
        .or_else(|| parts.find("\npub"))
        .unwrap_or(parts.len())];
    assert_eq!(
        parts.matches("crate::ef::beef_to_ef_batch(").count(),
        1,
        "one conversion in the arm"
    );
    let ef_src = include_str!("../src/ef.rs");
    let production = &ef_src[..ef_src.find("#[cfg(test)]").unwrap()];
    assert!(
        !production.contains("source_transaction = Some("),
        "the batch links no source"
    );
    assert!(
        !production.contains(".to_ef()"),
        "the batch writes the EF itself"
    );
}
