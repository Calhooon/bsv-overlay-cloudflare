//! bsv-low #585, door 1: THE SCRIPT DOOR READS THE STREAM.
//!
//! `Engine::verify_scripts_only` (the walk the gated `/submit` runs before a
//! broadcast) had four bounds on the BODY: 64 unproven transactions, 256
//! inputs per transaction, 512 KB per transaction, and the parse's own size
//! and counts. A valid BEEF past any of them was left unjudged ("over
//! budget: the network judges"). They are gone. The one limb that stays is
//! the WORK budget, the interpreter's estimated work.
//!
//! THE PIN (`e585_d1_a`): a valid BEEF whose subject is a 2 MB transaction
//! with 300 inputs, each spending an output of one of 100 UNPROVEN parents
//! (101 unproven transactions), is walked to its end: every input executed,
//! the subject judged, `Ok`, which is the route's broadcast arm. On
//! `d6d2774` it is `ScriptWalkOverBudget` (the 65th unproven transaction, or
//! the subject's 300 inputs, or its 2 MB, whichever the walk meets first).
//!
//! THE MEASURE: the peak growth of the LIVE HEAP (a counting global
//! allocator; the tests of this binary run one at a time, see `SERIAL`)
//! inside `verify_scripts_only`, over the level at its entry. The body is the
//! caller's and is not counted.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use bsv_overlay_engine::builder::EngineBuilder;
use bsv_overlay_engine::engine::{DoorBudget, Engine, EngineError};
use bsv_overlay_engine::storage::memory::MemoryStorage;
use bsv_rs::primitives::PrivateKey;
use bsv_rs::script::op::*;
use bsv_rs::script::templates::P2PKH;
use bsv_rs::script::{
    LockingScript, Script, ScriptTemplate, ScriptTemplateUnlock, SignOutputs, SigningContext,
    UnlockingScript,
};
use bsv_rs::transaction::{
    MerklePath, MerklePathLeaf, Transaction, TransactionInput, TransactionOutput,
};

// ── The counting allocator ──────────────────────────────────────────────

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
/// One measured test at a time: the allocator is the binary's.
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

/// Runs one measured test at a time, on its own single-threaded runtime.
fn one_at_a_time<F: std::future::Future>(test: F) -> F::Output {
    let _one = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime")
        .block_on(test)
}

/// The live heap now, and the peak restarted from it.
fn mark() -> usize {
    let live = LIVE.load(Ordering::Relaxed);
    PEAK.store(live, Ordering::Relaxed);
    live
}

/// The peak growth since `mark`.
fn peak_over(mark: usize) -> usize {
    PEAK.load(Ordering::Relaxed).saturating_sub(mark)
}

// ── The bodies ──────────────────────────────────────────────────────────

const HEIGHT: u32 = 800_000;
const PARENTS: usize = 100;
const OUTPUTS_PER_PARENT: usize = 3;
const SUBJECT_INPUTS: usize = PARENTS * OUTPUTS_PER_PARENT;
const TWO_MB: usize = 2 * 1024 * 1024;

fn engine() -> Engine {
    EngineBuilder::new(Box::new(MemoryStorage::new())).build()
}

/// `OP_DROP OP_TRUE`, unlocked by any push: a valid spend with no signature
/// check and no hash, so its estimated work is its script bytes.
fn open_lock() -> LockingScript {
    let mut script = Script::new();
    script.write_opcode(OP_DROP).write_opcode(OP_TRUE);
    LockingScript::from_script(script)
}

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

/// `OP_FALSE OP_RETURN <bytes>`: an unspendable output that carries `len`
/// bytes of data.
fn data_lock(len: usize) -> LockingScript {
    let mut script = Script::new();
    script
        .write_opcode(OP_FALSE)
        .write_opcode(OP_RETURN)
        .write_bin(&vec![0x5a; len]);
    LockingScript::from_script(script)
}

/// A "mined" funding transaction paying `outputs` outputs of `sats` to
/// `lock`, with a one-leaf BUMP (a block of one transaction: its root is the
/// txid).
fn proven_funding(lock: &LockingScript, outputs: usize, sats: u64) -> Transaction {
    let mut tx = Transaction::new();
    tx.inputs.push(TransactionInput {
        source_txid: Some("aa".repeat(32)),
        source_output_index: 0,
        unlocking_script: Some(UnlockingScript::from_script(Script::new())),
        ..Default::default()
    });
    for _ in 0..outputs {
        tx.outputs.push(TransactionOutput::new(sats, lock.clone()));
    }
    let txid = tx.id();
    tx.merkle_path = Some(
        MerklePath::new(HEIGHT, vec![vec![MerklePathLeaf::new_txid(0, txid)]])
            .expect("a one-leaf BUMP is valid"),
    );
    tx
}

struct Body {
    beef: Vec<u8>,
    subject_txid: String,
    subject_raw: Vec<u8>,
}

impl Body {
    /// Where the subject's raw bytes lie in the BEEF (it is the last
    /// transaction `to_beef` writes).
    fn subject_at(&self) -> usize {
        let n = self.subject_raw.len();
        (0..=self.beef.len() - n)
            .rev()
            .find(|at| self.beef[*at..*at + n] == self.subject_raw[..])
            .expect("the BEEF carries the subject")
    }
}

/// One proven funding transaction, `PARENTS` unproven parents each spending
/// one of its outputs into `OUTPUTS_PER_PARENT` outputs, and a subject
/// spending every one of those (300 inputs) into one small output and one
/// data output of `data` bytes. `lock` and `unlock` are the lock of every
/// spent output and its unlock.
async fn wide_body(
    lock: LockingScript,
    unlock: impl Fn() -> ScriptTemplateUnlock,
    data: usize,
) -> Body {
    let funding = proven_funding(&lock, PARENTS, 10_000);
    let mut subject = Transaction::new();
    for vout in 0..PARENTS {
        let mut parent = Transaction::new();
        parent
            .add_input_from_tx(funding.clone(), vout as u32, unlock())
            .expect("the parent spends the funding");
        for _ in 0..OUTPUTS_PER_PARENT {
            parent
                .outputs
                .push(TransactionOutput::new(3_000, lock.clone()));
        }
        parent.sign().await.expect("the parent signs");
        for spent in 0..OUTPUTS_PER_PARENT {
            subject
                .add_input_from_tx(parent.clone(), spent as u32, unlock())
                .expect("the subject spends the parent");
        }
    }
    subject
        .outputs
        .push(TransactionOutput::new(1_000, lock.clone()));
    if data > 0 {
        subject
            .outputs
            .push(TransactionOutput::new(0, data_lock(data)));
    }
    subject.sign().await.expect("the subject signs");
    assert_eq!(subject.inputs.len(), SUBJECT_INPUTS);
    Body {
        beef: subject.to_beef(false).expect("the BEEF of the subject"),
        subject_txid: subject.id(),
        subject_raw: subject.to_binary(),
    }
}

// ── The pins ────────────────────────────────────────────────────────────

/// THE PIN. RED on `d6d2774`: `ScriptWalkOverBudget`.
#[test]
fn e585_d1_a_a_2mb_transaction_with_300_unproven_inputs_is_walked() {
    one_at_a_time(async {
        let body = wide_body(open_lock(), open_unlock, TWO_MB).await;
        assert!(
            body.subject_raw.len() > TWO_MB,
            "{} bytes",
            body.subject_raw.len()
        );
        let engine = engine();

        let at_entry = mark();
        let started = std::time::Instant::now();
        let verdict = engine
            .verify_scripts_only(&body.beef, &body.subject_txid)
            .await;
        let took = started.elapsed();
        let peak = peak_over(at_entry);

        let stats = verdict.expect("a valid BEEF is walked whatever its size and its counts");
        assert!(stats.subject_judged, "{stats:?}");
        assert_eq!(stats.unproven_txs, PARENTS + 1, "{stats:?}");
        assert_eq!(stats.inputs_executed, SUBJECT_INPUTS + PARENTS, "{stats:?}");
        assert!(
            stats.work_bytes < DoorBudget::DEFAULT.max_work_bytes,
            "{stats:?}"
        );
        println!(
            "e585_d1_a: body {} bytes, subject {} bytes with {} inputs, {} unproven transactions; \
             walked in {took:?} {stats:?}; peak heap of the walk {peak} bytes ({:.3}x its largest \
             element)",
            body.beef.len(),
            body.subject_raw.len(),
            SUBJECT_INPUTS,
            stats.unproven_txs,
            peak as f64 / body.subject_raw.len() as f64,
        );
        // Beside the caller's body the door holds ONE element of the stream (the
        // SDK's copy of the transaction in hand, with its buffer's slack) and the
        // index: the peak follows the largest element, not the body's elements.
        assert!(
            peak < body.subject_raw.len() * 2,
            "the walk's heap peaked at {peak} bytes over a {} byte largest element",
            body.subject_raw.len()
        );
    });
}

/// The same 300 inputs with REAL signatures, in a small subject: every one is
/// checked (the other inputs and the outputs are in each digest), inside the
/// work budget. RED on `d6d2774`: `ScriptWalkOverBudget`.
#[test]
fn e585_d1_b_300_signed_inputs_over_101_unproven_transactions_are_walked() {
    one_at_a_time(async {
        let key = PrivateKey::random();
        let lock = P2PKH::new()
            .lock(&key.public_key().hash160())
            .expect("a P2PKH lock");
        let body = wide_body(lock, || P2PKH::unlock(&key, SignOutputs::All, false), 0).await;
        let engine = engine();

        let at_entry = mark();
        let verdict = engine
            .verify_scripts_only(&body.beef, &body.subject_txid)
            .await;
        let peak = peak_over(at_entry);

        let stats = verdict.expect("300 valid signatures are 300 valid spends");
        assert!(stats.subject_judged, "{stats:?}");
        assert_eq!(stats.unproven_txs, PARENTS + 1);
        assert_eq!(stats.inputs_executed, SUBJECT_INPUTS + PARENTS);
        assert_eq!(stats.sig_ops, SUBJECT_INPUTS + PARENTS);
        println!(
            "e585_d1_b: body {} bytes, subject {} bytes; walked {stats:?}; peak heap {peak} bytes",
            body.beef.len(),
            body.subject_raw.len()
        );

        // A signature that does not verify is still the interpreter's verdict,
        // at input 299 of a transaction the old door never opened.
        let mut beef = body.beef.clone();
        let subject_at = body.subject_at();
        let last_script = find_last_input_script(&beef[subject_at..]) + subject_at;
        beef[last_script + 10] ^= 0x01;
        let corrupted = txid_of(&beef[subject_at..subject_at + body.subject_raw.len()]);
        let err = engine
            .verify_scripts_only(&beef, &corrupted)
            .await
            .expect_err("a corrupted signature is refused");
        match &err {
            EngineError::ScriptVerificationFailed { input_index, .. } => {
                assert_eq!(*input_index as usize, SUBJECT_INPUTS - 1, "{err}")
            }
            other => panic!("expected the interpreter's verdict, got {other}"),
        }
    });
}

/// `OP_DROP OP_0 OP_IF OP_CHECKSIG OP_ENDIF OP_TRUE`: a valid spend whose
/// lock holds a signature check it never reaches. The census counts it (the
/// estimate is from the bytes), so each input is charged one digest of its
/// transaction.
fn unreached_checksig_lock() -> LockingScript {
    let mut script = Script::new();
    script
        .write_opcode(OP_DROP)
        .write_opcode(OP_0)
        .write_opcode(OP_IF)
        .write_opcode(OP_CHECKSIG)
        .write_opcode(OP_ENDIF)
        .write_opcode(OP_TRUE);
    LockingScript::from_script(script)
}

/// THE ONE LIMB. A 2 MB subject whose 300 inputs each hold a signature check
/// is 300 digests of 2 MB by the estimate: about 600 MB of work against a
/// budget of 64 MB. The door's answer is the one it always was: over budget,
/// the network judges, never a refusal; and it is given from the bytes, at
/// the input whose charge crosses the budget, before that input runs.
#[test]
fn e585_d1_c_the_work_budget_is_the_one_limb() {
    one_at_a_time(async {
        let body = wide_body(unreached_checksig_lock(), open_unlock, TWO_MB).await;
        let err = engine()
            .verify_scripts_only(&body.beef, &body.subject_txid)
            .await
            .expect_err("300 digests of 2 MB are past the work budget");
        match &err {
            EngineError::ScriptWalkOverBudget {
                at_txid,
                subject_judged,
                what,
            } => {
                assert_eq!(at_txid, &body.subject_txid);
                assert!(!*subject_judged);
                let crossing = DoorBudget::DEFAULT.max_work_bytes as usize / body.subject_raw.len();
                assert!(
                    what.starts_with("estimated work ")
                        && what.contains("exceeds the door budget of 67108864")
                        && what.contains(&format!("(input {crossing} of ")),
                    "the work, and nothing else, is what stopped it: {what}"
                );
            }
            other => panic!("expected the door's own bound, got {other}"),
        }

        // The same lock under a SMALL subject: the 400 inputs are charged one
        // digest of a small transaction each, and are walked.
        let small = wide_body(unreached_checksig_lock(), open_unlock, 0).await;
        let stats = engine()
            .verify_scripts_only(&small.beef, &small.subject_txid)
            .await
            .expect("inside the work budget");
        assert_eq!(stats.sig_ops, SUBJECT_INPUTS + PARENTS);
        assert!(stats.subject_judged);
    });
}

/// What the door keeps of a body when it walks nothing: the subject named is
/// the PROVEN funding transaction, so the walk ends at its first step and the
/// heap is the stream's chunk, its one element in hand and the index.
#[test]
fn e585_d1_d_the_index_is_small_beside_the_body() {
    one_at_a_time(async {
        // 102 small transactions and one BUMP: 103 elements.
        let body = wide_body(open_lock(), open_unlock, 0).await;
        let funding_txid = first_txid(&body.beef);
        let engine = engine();
        let live_before = LIVE.load(Ordering::Relaxed);
        let at_entry = mark();
        let stats = engine
            .verify_scripts_only(&body.beef, &funding_txid)
            .await
            .expect("a proven subject is trusted as it stands");
        let peak = peak_over(at_entry);
        assert_eq!(stats.unproven_txs, 0);
        assert_eq!(LIVE.load(Ordering::Relaxed), live_before, "nothing is kept");
        // The stream reads the source 16 KiB at a time, and hands out one element:
        // the SDK's copy of the transaction in hand and the places of its fields
        // (64 bytes an input), each a growing buffer. The index itself is measured
        // by `script_door::tests::e585_d1_the_index_is_an_entry_per_element`.
        const CHUNK: usize = 16 * 1024;
        let beside = peak.saturating_sub(CHUNK);
        println!(
            "e585_d1_d: body {} bytes, 103 elements, the largest {} bytes with {} inputs; peak \
             heap of the index pass {peak} bytes = the stream's chunk ({CHUNK}) + {beside} bytes",
            body.beef.len(),
            body.subject_raw.len(),
            SUBJECT_INPUTS,
        );
        assert!(
            beside < 8 * body.subject_raw.len() + 103 * 128,
            "the index pass held {beside} bytes beside the stream's chunk"
        );
    });
}

// ── Reading the test's own bodies ───────────────────────────────────────

fn txid_of(raw: &[u8]) -> String {
    Transaction::from_binary(raw).expect("a transaction").id()
}

/// The txid of the first transaction of a BEEF (the funding: `to_beef` writes
/// a source before its spender).
fn first_txid(beef: &[u8]) -> String {
    let parsed = bsv_rs::transaction::Beef::from_binary(beef).expect("the BEEF parses");
    parsed.txs.first().expect("a transaction").txid()
}

/// The offset, in a raw transaction followed by anything, of the last input's
/// unlocking script.
fn find_last_input_script(raw: &[u8]) -> usize {
    let mut at = 4;
    let (count, used) = varint(&raw[at..]);
    at += used;
    let mut script_at = 0;
    for _ in 0..count {
        at += 36;
        let (len, used) = varint(&raw[at..]);
        at += used;
        script_at = at;
        at += len as usize + 4;
    }
    script_at
}

fn varint(bytes: &[u8]) -> (u64, usize) {
    match bytes[0] {
        0xfd => (u64::from(u16::from_le_bytes([bytes[1], bytes[2]])), 3),
        0xfe => (
            u64::from(u32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]])),
            5,
        ),
        short => (u64::from(short), 1),
    }
}
