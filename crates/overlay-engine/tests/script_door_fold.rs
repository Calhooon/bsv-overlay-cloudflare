//! bsv-low #585, the fold of the doors lens (E585-D12) on door 1.
//!
//! - M1, THE SIGNATURE FLOOR (`e585f_m1_*`). A signature check was charged
//!   its transaction's bytes, so a chain of 106 byte `OP_CHECKSIG OP_NOT`
//!   transactions bought a full EC verification for 152 bytes of budget:
//!   17,000 of them walked `Ok` at 3.9 % of it. A check is now charged
//!   `max(transaction bytes, DoorBudget::sig_check_floor)`, and the same body
//!   STOPS at the work budget ("the network judges", never a refusal).
//! - L3, THE MEMORY LIMB (`e585f_l3_*`). The door's memory followed the
//!   element count and a BUMP's leaves. The two bodies the lens measured (77
//!   MB and 108 MB of heap beside a 10 MB body) now stop at the memory limb
//!   before the stream is opened.
//! - N4, THE SHORTCUT (`e585f_n4_*`). An input with no signature opcode runs
//!   with no copy of its transaction; a signed input beside it still commits
//!   to it, and a script is judged the same with and without the copy.
//! - N2, THE TABLE (`e585f_n2_*`). A table of deterministic bodies with the
//!   door's answer frozen for each, and the digest of the table.
//!
//! Everything above the line `THE FOLD'S OWN API` uses only what `8c92671`
//! has (`Engine::verify_scripts_only` and the error's `what`), so the file,
//! cut at that line, is run there as it stands: each pin's RED word is in its
//! doc comment. Below the line: the typed limb, and the memory estimate held
//! against the measured heap with the limb lifted.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use bsv_overlay_engine::builder::EngineBuilder;
use bsv_overlay_engine::engine::{DoorBudget, Engine, EngineError, WalkStats};
use bsv_overlay_engine::storage::memory::MemoryStorage;
use bsv_rs::primitives::bsv::TransactionSignature;
use bsv_rs::primitives::{sha256, sha256d, PrivateKey};
use bsv_rs::script::op::*;
use bsv_rs::script::template::compute_sighash_scope;
use bsv_rs::script::templates::P2PKH;
use bsv_rs::script::{
    LockingScript, Script, ScriptTemplate, ScriptTemplateUnlock, SignOutputs, SigningContext,
    UnlockingScript,
};
use bsv_rs::transaction::{
    Beef, MerklePath, MerklePathLeaf, Transaction, TransactionInput, TransactionOutput,
};

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

fn mark() -> usize {
    let live = LIVE.load(Ordering::Relaxed);
    PEAK.store(live, Ordering::Relaxed);
    live
}

fn peak_over(mark: usize) -> usize {
    PEAK.load(Ordering::Relaxed).saturating_sub(mark)
}

fn engine() -> Engine {
    EngineBuilder::new(Box::new(MemoryStorage::new())).build()
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

struct In {
    prev: Hash32,
    vout: u32,
    script: Vec<u8>,
    sequence: u32,
}

fn spend(prev: Hash32, vout: u32, script: &[u8]) -> In {
    In {
        prev,
        vout,
        script: script.to_vec(),
        sequence: u32::MAX,
    }
}

fn raw_tx(version: u32, inputs: &[In], outputs: &[(u64, Vec<u8>)], lock_time: u32) -> Vec<u8> {
    let mut raw = version.to_le_bytes().to_vec();
    raw.extend(varint(inputs.len() as u64));
    for input in inputs {
        raw.extend_from_slice(&input.prev);
        raw.extend_from_slice(&input.vout.to_le_bytes());
        raw.extend(varint(input.script.len() as u64));
        raw.extend_from_slice(&input.script);
        raw.extend_from_slice(&input.sequence.to_le_bytes());
    }
    raw.extend(varint(outputs.len() as u64));
    for (satoshis, script) in outputs {
        raw.extend_from_slice(&satoshis.to_le_bytes());
        raw.extend(varint(script.len() as u64));
        raw.extend_from_slice(script);
    }
    raw.extend_from_slice(&lock_time.to_le_bytes());
    raw
}

/// The txid of a raw transaction, in wire order.
fn wire_txid(raw: &[u8]) -> Hash32 {
    sha256d(raw)
}

/// A wire-order txid as the hex a subject is named by.
fn display(wire: &Hash32) -> String {
    let mut bytes = *wire;
    bytes.reverse();
    hex::encode(bytes)
}

/// A "mined" transaction paying `outputs`: its one input spends nothing the
/// body carries (it is proven, so it is never walked).
fn funding_raw(outputs: &[(u64, Vec<u8>)]) -> Vec<u8> {
    raw_tx(1, &[spend([0xaa; 32], 0, &[])], outputs, 0)
}

/// The BUMP of a block of one transaction: one level, one leaf, the txid.
fn one_leaf_bump(txid: &Hash32) -> Vec<u8> {
    let mut bump = varint(800_000);
    bump.extend_from_slice(&[0x01, 0x01, 0x00, 0x02]);
    bump.extend_from_slice(txid);
    bump
}

/// A V1 BEEF of `bumps` and `txs` (each with the index of its BUMP, if any),
/// in the order given.
fn beef_v1(bumps: &[Vec<u8>], txs: &[(Vec<u8>, Option<u64>)]) -> Vec<u8> {
    let mut body = vec![0x01, 0x00, 0xbe, 0xef];
    body.extend(varint(bumps.len() as u64));
    for bump in bumps {
        body.extend_from_slice(bump);
    }
    body.extend(varint(txs.len() as u64));
    for (raw, bump) in txs {
        body.extend_from_slice(raw);
        match bump {
            Some(index) => {
                body.push(0x01);
                body.extend(varint(*index));
            }
            None => body.push(0x00),
        }
    }
    body
}

/// A proven funding paying `locks` (1,000 sats each) and one unproven
/// transaction of `version` spending every one with the matching unlock.
fn spend_of_locks(
    version: u32,
    lock_time: u32,
    spends: &[(Vec<u8>, Vec<u8>)],
) -> (Vec<u8>, String) {
    let outputs: Vec<(u64, Vec<u8>)> = spends
        .iter()
        .map(|(lock, _)| (1_000, lock.clone()))
        .collect();
    let funding = funding_raw(&outputs);
    let funding_txid = wire_txid(&funding);
    let inputs: Vec<In> = spends
        .iter()
        .enumerate()
        .map(|(vout, (_, unlock))| In {
            prev: funding_txid,
            vout: vout as u32,
            script: unlock.clone(),
            sequence: 0xffff_fff0 + vout as u32,
        })
        .collect();
    let subject = raw_tx(version, &inputs, &[(900, vec![OP_1])], lock_time);
    let subject_txid = display(&wire_txid(&subject));
    (
        beef_v1(
            &[one_leaf_bump(&funding_txid)],
            &[(funding, Some(0)), (subject, None)],
        ),
        subject_txid,
    )
}

// ── M1: the signature floor ─────────────────────────────────────────────

/// `OP_CHECKSIG OP_NOT`: a lock any well-formed signature that does NOT
/// verify satisfies. One full EC verification, no hash opcode.
const CHECKSIG_NOT: [u8; 2] = [OP_CHECKSIG, OP_NOT];

/// `<DER r = 1, s = 1, SIGHASH_ALL | FORKID> <the generator's public key>`.
fn failing_signature_unlock() -> Vec<u8> {
    let generator =
        hex::decode("0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798")
            .expect("hex");
    let mut script = vec![
        0x09, 0x30, 0x06, 0x02, 0x01, 0x01, 0x02, 0x01, 0x01, 0x41, 0x21,
    ];
    script.extend_from_slice(&generator);
    script
}

/// The lens's body: a proven funding, then a chain of `n` one-input
/// transactions of 106 bytes, each a full EC verification that fails and an
/// `OP_NOT`. The subject is the last.
fn checksig_chain(n: usize) -> (Vec<u8>, String) {
    let funding = funding_raw(&[(1_000, CHECKSIG_NOT.to_vec())]);
    let mut prev = wire_txid(&funding);
    let bump = one_leaf_bump(&prev);
    let unlock = failing_signature_unlock();
    let mut txs = vec![(funding, Some(0))];
    for _ in 0..n {
        let raw = raw_tx(
            1,
            &[spend(prev, 0, &unlock)],
            &[(1_000, CHECKSIG_NOT.to_vec())],
            0,
        );
        assert_eq!(raw.len(), 106);
        prev = wire_txid(&raw);
        txs.push((raw, None));
    }
    (beef_v1(&[bump], &txs), display(&prev))
}

/// THE PIN (M1). 17,000 EC verifications in 1,819,113 bytes of valid BEEF (the
/// lens's body, regenerated: its own was 34 bytes longer) are past the
/// work budget: the door's own bound, from the bytes, never a refusal. RED on
/// `8c92671`: the body walks, `Ok(WalkStats { unproven_txs: 17000,
/// inputs_executed: 17000, .., sig_ops: 17000, work_bytes: 2584000, .. })`,
/// 3.9 % of the budget.
#[test]
fn e585f_m1_a_chain_of_17000_bare_signature_checks_stops_at_the_work_budget() {
    one_at_a_time(async {
        let (body, subject) = checksig_chain(17_000);
        assert_eq!(body.len(), 1_819_113);
        let started = std::time::Instant::now();
        let verdict = engine().verify_scripts_only(&body, &subject).await;
        let took = started.elapsed();
        println!(
            "e585f_m1: {} bytes, 17,000 signature checks: {took:?}, {}",
            body.len(),
            match &verdict {
                Ok(stats) => format!("WALKED {stats:?}"),
                Err(e) => e.to_string(),
            }
        );
        match verdict {
            Err(EngineError::ScriptWalkOverBudget {
                subject_judged,
                what,
                ..
            }) => {
                assert!(
                    subject_judged,
                    "the subject is the first input the walk ran"
                );
                assert!(
                    what.starts_with("estimated work ")
                        && what.contains("exceeds the door budget of 67108864"),
                    "the work stopped it: {what}"
                );
            }
            other => panic!("expected the door's own bound, got {other:?}"),
        }
    });
}

/// The bound the floor gives, at its edge: the budget is 1,024 floors, so a
/// chain of 1,000 bare checks walks and its time is the door's worst EC cost
/// to within 2 % (printed). Each is charged the floor and its 46 script
/// bytes, whatever its 106 byte transaction weighs.
#[test]
fn e585f_m1_the_work_budget_bounds_the_ec_verifications_of_a_walk() {
    one_at_a_time(async {
        let (body, subject) = checksig_chain(1_000);
        let started = std::time::Instant::now();
        let stats = engine()
            .verify_scripts_only(&body, &subject)
            .await
            .expect("1,000 checks are inside the budget");
        let took = started.elapsed();
        assert_eq!(stats.sig_ops, 1_000);
        assert_eq!(stats.inputs_executed, 1_000);
        let budget = DoorBudget::DEFAULT.max_work_bytes;
        println!(
            "e585f_m1: 1,000 EC verifications walked in {took:?} (this profile), work {} of \
             {budget} ({:.1} %): {} a check",
            stats.work_bytes,
            stats.work_bytes as f64 * 100.0 / budget as f64,
            stats.work_bytes / 1_000,
        );
        assert!(
            stats.work_bytes / 1_000 >= 64 * 1024,
            "a check is charged at least the floor: {stats:?}"
        );
        // And the floor is where the bound is: 24 more are the budget.
        let (body, subject) = checksig_chain(1_024);
        let err = engine()
            .verify_scripts_only(&body, &subject)
            .await
            .expect_err("1,024 floors and their script bytes are past the budget");
        assert!(
            matches!(err, EngineError::ScriptWalkOverBudget { .. }),
            "{err}"
        );
    });
}

// ── L3: the memory limb ─────────────────────────────────────────────────

/// The lens's first body: `n` minimal transactions (10 bytes: a version, no
/// input, no output, a lock time). The subject is the first.
fn minimal_transactions(n: u32) -> (Vec<u8>, String) {
    let mut body = vec![0x01, 0x00, 0xbe, 0xef, 0x00];
    body.extend(varint(u64::from(n)));
    for i in 0..n {
        body.extend_from_slice(&raw_tx(i, &[], &[], 0));
        body.push(0x00);
    }
    (body, display(&wire_txid(&raw_tx(0, &[], &[], 0))))
}

/// `OP_DROP OP_1`, spent by any push.
const OPEN_LOCK: [u8; 2] = [OP_DROP, OP_1];
const OPEN_UNLOCK: [u8; 2] = [0x01, 0x42];

/// The lens's second body: ONE BUMP whose level 0 carries `2^levels` leaves
/// (the first the funding's txid, every upper level computed), the funding,
/// and one unproven transaction spending it.
fn wide_bump(levels: u8) -> (Vec<u8>, String) {
    let funding = funding_raw(&[(1_000, OPEN_LOCK.to_vec())]);
    let funding_txid = wire_txid(&funding);
    let subject = raw_tx(
        1,
        &[spend(funding_txid, 0, &OPEN_UNLOCK)],
        &[(900, vec![OP_1])],
        0,
    );
    let subject_txid = display(&wire_txid(&subject));
    let leaves = 1u64 << levels;
    let mut bump = varint(800_000);
    bump.push(levels);
    bump.extend(varint(leaves));
    for i in 0..leaves {
        bump.extend(varint(i));
        bump.push(0x00);
        if i == 0 {
            bump.extend_from_slice(&funding_txid);
        } else {
            let mut hash = [0x11u8; 32];
            hash[..8].copy_from_slice(&i.to_le_bytes());
            bump.extend_from_slice(&hash);
        }
    }
    // Every upper level carries no leaf: its nodes are computed.
    bump.extend(vec![0x00; usize::from(levels) - 1]);
    (
        beef_v1(&[bump], &[(funding, Some(0)), (subject, None)]),
        subject_txid,
    )
}

/// The door's answer to a body past the memory limb, and what the call held.
async fn stopped_at_the_memory_limb(name: &str, body: &[u8], subject: &str) {
    let engine = engine();
    let at_entry = mark();
    let started = std::time::Instant::now();
    let verdict = engine.verify_scripts_only(body, subject).await;
    let took = started.elapsed();
    let peak = peak_over(at_entry);
    println!(
        "e585f_l3 {name}: body {} bytes, peak heap of the call {peak} bytes, {took:?}: {}",
        body.len(),
        match &verdict {
            Ok(stats) => format!("WALKED {stats:?}"),
            Err(e) => e.to_string(),
        }
    );
    match verdict {
        Err(EngineError::ScriptWalkOverBudget {
            subject_judged,
            what,
            ..
        }) => {
            assert!(!subject_judged);
            assert!(
                what.starts_with("estimated memory ")
                    && what.contains("exceeds the door memory budget of 50331648"),
                "the memory stopped it: {what}"
            );
        }
        other => panic!("expected the door's own bound, got {other:?}"),
    }
    assert!(
        peak < 4 * 1024,
        "the limb is charged before anything is allocated: the call held {peak} bytes"
    );
}

/// THE PIN (L3), the element count. 900,000 minimal transactions in 9.9 MB
/// are an index of 77 MB: past the memory limb, stopped from the frame, the
/// network judges. RED on `8c92671`: walked, `Ok(WalkStats { unproven_txs: 1,
/// inputs_executed: 0, .. })`, the call's heap peaking at 77,089,816 bytes.
#[test]
fn e585f_l3_900000_minimal_transactions_stop_at_the_memory_limb() {
    one_at_a_time(async {
        let (body, subject) = minimal_transactions(900_000);
        assert_eq!(body.len(), 9_900_010);
        stopped_at_the_memory_limb("900,000 minimal transactions", &body, &subject).await;
    });
}

/// THE PIN (L3), a BUMP's leaves. One BUMP of 2^18 leaves in 9.8 MB is 108 MB
/// of `Leaf`s and root tables: past the memory limb, stopped before the BUMP
/// is parsed. RED on `8c92671`: walked, `Ok(WalkStats { unproven_txs: 1,
/// inputs_executed: 1, .. })`, the call's heap peaking at 108 MB.
#[test]
fn e585f_l3_one_bump_of_2_18_leaves_stops_at_the_memory_limb() {
    one_at_a_time(async {
        let (body, subject) = wide_bump(18);
        println!("e585f_l3: the wide BUMP body is {} bytes", body.len());
        assert!(body.len() > 9_800_000 && body.len() < 10_000_000);
        stopped_at_the_memory_limb("one BUMP of 2^18 leaves", &body, &subject).await;
    });
}

// ── N4: the shortcut ────────────────────────────────────────────────────

const KEY: &str = "1111111111111111111111111111111111111111111111111111111111111111";

fn fixed_key() -> PrivateKey {
    PrivateKey::from_hex(KEY).expect("a fixed key")
}

fn open_lock() -> LockingScript {
    LockingScript::from_script(Script::from_binary(&OPEN_LOCK).expect("a script"))
}

fn open_unlock() -> ScriptTemplateUnlock {
    ScriptTemplateUnlock::new(
        |_ctx: &SigningContext| {
            Ok(UnlockingScript::from_script(
                Script::from_binary(&OPEN_UNLOCK).expect("a script"),
            ))
        },
        || 2,
    )
}

fn proven(mut tx: Transaction) -> Transaction {
    let txid = tx.id();
    tx.merkle_path = Some(
        MerklePath::new(800_000, vec![vec![MerklePathLeaf::new_txid(0, txid)]])
            .expect("a one-leaf BUMP is valid"),
    );
    tx
}

fn funding_of(outputs: Vec<TransactionOutput>) -> Transaction {
    let mut tx = Transaction::new();
    tx.inputs.push(TransactionInput {
        source_txid: Some("aa".repeat(32)),
        source_output_index: 0,
        unlocking_script: Some(UnlockingScript::from_script(Script::new())),
        ..Default::default()
    });
    tx.outputs = outputs;
    proven(tx)
}

/// A MIXED transaction: inputs 0 and 2 check no signature (run with no copy
/// of the transaction), input 1 is a P2PKH signed SIGHASH_ALL.
async fn mixed_transaction() -> Transaction {
    let key = fixed_key();
    let p2pkh = P2PKH::new()
        .lock(&key.public_key().hash160())
        .expect("a P2PKH lock");
    let funding = funding_of(vec![
        TransactionOutput::new(1_000, open_lock()),
        TransactionOutput::new(1_000, p2pkh),
        TransactionOutput::new(1_000, open_lock()),
    ]);
    let mut tx = Transaction::new();
    tx.add_input_from_tx(funding.clone(), 0, open_unlock())
        .expect("input 0");
    tx.add_input_from_tx(
        funding.clone(),
        1,
        P2PKH::unlock(&key, SignOutputs::All, false),
    )
    .expect("input 1");
    tx.add_input_from_tx(funding, 2, open_unlock())
        .expect("input 2");
    tx.outputs.push(TransactionOutput::new(2_500, open_lock()));
    tx.outputs.push(TransactionOutput::new(400, open_lock()));
    tx.sign().await.expect("the mixed transaction signs");
    tx
}

fn beef_and_subject(tx: &Transaction) -> (Vec<u8>, String) {
    tx.invalidate_caches();
    (tx.to_beef(false).expect("the BEEF"), tx.id())
}

/// The mixed transaction, intact and with one thing changed after signing.
async fn mixed_bodies() -> Vec<(&'static str, Vec<u8>, String)> {
    let mut bodies = Vec::new();
    let intact = mixed_transaction().await;
    let (beef, subject) = beef_and_subject(&intact);
    bodies.push(("mixed: intact", beef, subject));

    let mut tx = mixed_transaction().await;
    tx.inputs[0].sequence = 7;
    let (beef, subject) = beef_and_subject(&tx);
    bodies.push((
        "mixed: the sequence of sig-less input 0 changed",
        beef,
        subject,
    ));

    let mut tx = mixed_transaction().await;
    tx.lock_time = 9;
    let (beef, subject) = beef_and_subject(&tx);
    bodies.push(("mixed: the lock time changed", beef, subject));

    let mut tx = mixed_transaction().await;
    tx.outputs[1].satoshis = Some(399);
    let (beef, subject) = beef_and_subject(&tx);
    bodies.push(("mixed: an output's value changed", beef, subject));

    let mut tx = mixed_transaction().await;
    tx.inputs[0].unlocking_script = Some(UnlockingScript::from_script(
        Script::from_binary(&[0x01, 0x43]).expect("a script"),
    ));
    let (beef, subject) = beef_and_subject(&tx);
    bodies.push((
        "mixed: the unlocking script of sig-less input 0 changed",
        beef,
        subject,
    ));
    bodies
}

/// THE PIN (N4). The signed input's digest carries the sig-less inputs as
/// they are in the bytes: changing the sequence of sig-less input 0 (or the
/// lock time, or an output) after signing is refused AT THE SIGNED INPUT, by
/// the interpreter. Changing input 0's unlocking script is not (BIP-143
/// covers no other input's script), as in the reference.
#[test]
fn e585f_n4_a_tampered_sig_less_input_is_refused_at_the_signed_input() {
    one_at_a_time(async {
        let engine = engine();
        for (name, beef, subject) in mixed_bodies().await {
            let verdict = engine.verify_scripts_only(&beef, &subject).await;
            let accepted = name.ends_with("intact") || name.contains("unlocking script");
            match verdict {
                Ok(stats) if accepted => {
                    assert_eq!((stats.inputs_executed, stats.sig_ops), (3, 1), "{name}");
                }
                Err(EngineError::ScriptVerificationFailed { input_index, .. }) if !accepted => {
                    assert_eq!(input_index, 1, "{name}: refused at the signed input");
                }
                other => panic!("{name}: {other:?}"),
            }
        }
    });
}

/// `OP_0 OP_IF OP_CHECKSIG OP_ENDIF`: a signature opcode the script never
/// reaches. The census counts it, so the input is run WITH the copy of its
/// transaction (the other inputs and the outputs).
const UNREACHED_CHECKSIG: [u8; 4] = [OP_0, OP_IF, OP_CHECKSIG, OP_ENDIF];

/// Locks that read, or once read, something of the spending transaction
/// other than a signature's digest: each consumes the one push of
/// [`OPEN_UNLOCK`].
fn reader_locks() -> Vec<(&'static str, Vec<u8>)> {
    vec![
        ("plain", vec![OP_DROP, OP_1]),
        (
            "lock time (OP_NOP2, once CHECKLOCKTIMEVERIFY)",
            vec![OP_DROP, 0x03, 0x20, 0xa1, 0x07, OP_NOP2, OP_DROP, OP_1],
        ),
        (
            "sequence (OP_NOP3, once CHECKSEQUENCEVERIFY)",
            vec![OP_DROP, OP_7, OP_NOP3, OP_DROP, OP_1],
        ),
        ("OP_CODESEPARATOR", vec![OP_DROP, OP_CODESEPARATOR, OP_1]),
        ("a hash", vec![OP_SHA256, OP_DROP, OP_1]),
        (
            "OP_VER against the version",
            vec![OP_DROP, OP_VER, OP_BIN2NUM, OP_2, OP_EQUAL],
        ),
        (
            "OP_VER against 9",
            vec![OP_DROP, OP_VER, OP_BIN2NUM, OP_9, OP_EQUAL],
        ),
    ]
}

/// What the door said, without the txids and the place in the script (two
/// bodies are compared).
fn outcome(verdict: &Result<WalkStats, EngineError>) -> String {
    match verdict {
        Ok(stats) => format!("OK inputs={}", stats.inputs_executed),
        Err(EngineError::ScriptVerificationFailed {
            input_index,
            reason,
            ..
        }) => {
            // The interpreter names the opcode's place, which the unreached
            // check moves by its four bytes.
            let reason = reason.split(" (PC: ").next().unwrap_or(reason);
            format!("REFUSED input {input_index}: {reason}")
        }
        Err(EngineError::ScriptWalkOverBudget { .. }) => "OVER".into(),
        Err(EngineError::ScriptWalkInconclusive { .. }) => "INCONCLUSIVE".into(),
        Err(other) => format!("OTHER {other}"),
    }
}

/// THE PIN (N4), the readers. A script that holds no signature opcode is run
/// with no copy of its transaction; the same script behind an unreached
/// `OP_CHECKSIG` is run with it. The verdicts are the same for the lock-time
/// and sequence opcodes (NOPs since Genesis), `OP_CODESEPARATOR`, a hash and
/// `OP_VER`, at transaction versions 1 and 2, with a lock time and a
/// sequence set: what the interpreter reads without a signature is passed on
/// both paths.
#[test]
fn e585f_n4_a_script_is_judged_the_same_with_and_without_its_transaction() {
    one_at_a_time(async {
        let engine = engine();
        let (mut accepted, mut refused) = (0, 0);
        for version in [1u32, 2] {
            for (name, lock) in reader_locks() {
                let behind_a_check = [&UNREACHED_CHECKSIG[..], &lock].concat();
                let (bare, bare_subject) =
                    spend_of_locks(version, 500_000, &[(lock, OPEN_UNLOCK.to_vec())]);
                let (full, full_subject) =
                    spend_of_locks(version, 500_000, &[(behind_a_check, OPEN_UNLOCK.to_vec())]);
                let without = engine.verify_scripts_only(&bare, &bare_subject).await;
                let with = engine.verify_scripts_only(&full, &full_subject).await;
                if let (Ok(without), Ok(with)) = (&without, &with) {
                    assert_eq!((without.sig_ops, with.sig_ops), (0, 1), "{name}");
                }
                let (without, with) = (outcome(&without), outcome(&with));
                println!("e585f_n4 version {version}, {name}: {without}");
                assert_eq!(without, with, "version {version}, {name}");
                assert!(
                    without.starts_with("OK") || without.starts_with("REFUSED"),
                    "version {version}, {name}: {without}"
                );
                if without.starts_with("OK") {
                    accepted += 1;
                } else {
                    refused += 1;
                }
            }
        }
        assert!(
            accepted >= 8 && refused >= 2,
            "{accepted} accepted, {refused} refused"
        );
    });
}

// ── N2: the table ───────────────────────────────────────────────────────

const ENFORCED_FUNDING_HEX: &str =
    include_str!("fixtures/c571d433b8234e225af0c631f076b137b7c164cfa72f86b3e713f9ba67e3b563.hex");
const ENFORCED_SETTLE_HEX: &str =
    include_str!("fixtures/91309122f5630052f7e57f7db843d26d32ae4426a9dd9b2fc2955f2fab8cf9a6.hex");
const REFUND_FUNDING_HEX: &str =
    include_str!("fixtures/5533ca32a296c58778a240cd7649392bf2e6b11ef63e1c71765913ebba093c59.hex");
const REFUND_HEX: &str =
    include_str!("fixtures/3ca368b0ca4dcb31ba87977d7aaf3a4671eafa2c980864c880f96080c68cee36.hex");

/// A real mainnet covenant leg over its real funding (given a one-leaf
/// BUMP), with an optional bit flip in the spend's unlocking script.
fn covenant_leg(funding_hex: &str, spend_hex: &str, tamper_at: Option<usize>) -> (Vec<u8>, String) {
    let funding = proven(Transaction::from_hex(funding_hex.trim()).expect("the funding"));
    let mut spend = Transaction::from_hex(spend_hex.trim()).expect("the spend");
    if let Some(offset) = tamper_at {
        let mut bytes = spend.inputs[0]
            .unlocking_script
            .as_ref()
            .expect("an unlocking script")
            .to_binary();
        bytes[offset] ^= 0x01;
        spend.inputs[0].unlocking_script = Some(UnlockingScript::from_script(
            Script::from_binary(&bytes).expect("a script"),
        ));
    }
    spend.inputs[0].source_transaction = Some(Box::new(funding));
    beef_and_subject(&spend)
}

/// A P2PKH spend of a proven funding with fixed keys: `sats_out` of 10,000.
async fn p2pkh_spend(sats_out: u64) -> Transaction {
    let key = fixed_key();
    let lock = P2PKH::new()
        .lock(&key.public_key().hash160())
        .expect("a P2PKH lock");
    let funding = funding_of(vec![TransactionOutput::new(10_000, lock.clone())]);
    let mut tx = Transaction::new();
    tx.add_input_from_tx(funding, 0, P2PKH::unlock(&key, SignOutputs::All, false))
        .expect("the input");
    tx.outputs.push(TransactionOutput::new(sats_out, lock));
    tx.sign().await.expect("the spend signs");
    tx
}

/// A diamond chain of `levels` unproven P2PKH transactions, each spending
/// both outputs of the one before.
async fn diamond_chain(levels: u32) -> (Vec<u8>, String) {
    let key = fixed_key();
    let lock = P2PKH::new()
        .lock(&key.public_key().hash160())
        .expect("a P2PKH lock");
    let funding = funding_of(vec![TransactionOutput::new(4_000_000, lock.clone())]);
    let shallow = |tx: &Transaction| {
        let mut t = tx.clone();
        for input in &mut t.inputs {
            input.source_transaction = None;
        }
        t
    };
    let mut beef = Beef::new();
    let bump = beef.merge_bump(funding.merkle_path.clone().expect("a BUMP"));
    beef.merge_raw_tx(funding.to_binary(), Some(bump));
    let mut prev = shallow(&funding);
    let mut sats: u64 = 4_000_000;
    for level in 0..levels {
        let mut tx = Transaction::new();
        for vout in 0..=u32::from(level > 0) {
            tx.add_input_from_tx(
                prev.clone(),
                vout,
                P2PKH::unlock(&key, SignOutputs::All, false),
            )
            .expect("an input");
        }
        sats -= 1_000;
        tx.outputs
            .push(TransactionOutput::new(sats / 2, lock.clone()));
        tx.outputs
            .push(TransactionOutput::new(sats - sats / 2, lock.clone()));
        tx.sign().await.expect("the level signs");
        beef.merge_raw_tx(tx.to_binary(), None);
        prev = shallow(&tx);
    }
    (beef.to_binary(), prev.id())
}

/// `OP_1 <pk> <pk> <pk> OP_1 OP_2 OP_ADD OP_CHECKMULTISIG`, signed: a
/// CHECKMULTISIG whose key count the script computes.
async fn computed_multisig() -> (Vec<u8>, String) {
    let key = fixed_key();
    let pk = key.public_key().to_compressed();
    let mut script = Script::new();
    script
        .write_opcode(OP_1)
        .write_bin(&pk)
        .write_bin(&pk)
        .write_bin(&pk)
        .write_opcode(OP_1)
        .write_opcode(OP_2)
        .write_opcode(OP_ADD)
        .write_opcode(OP_CHECKMULTISIG);
    let funding = funding_of(vec![TransactionOutput::new(
        5_000,
        LockingScript::from_script(script),
    )]);
    let unlock = ScriptTemplateUnlock::new(
        move |ctx: &SigningContext| {
            let scope = compute_sighash_scope(SignOutputs::All, false);
            let sig = key.sign(&ctx.compute_sighash(scope)?)?;
            let mut s = Script::new();
            s.write_opcode(OP_0)
                .write_bin(&TransactionSignature::new(sig, scope).to_checksig_format());
            Ok(UnlockingScript::from_script(s))
        },
        || 80,
    );
    let mut tx = Transaction::new();
    tx.add_input_from_tx(funding, 0, unlock).expect("the input");
    tx.outputs.push(TransactionOutput::new(4_000, open_lock()));
    tx.sign().await.expect("the spend signs");
    beef_and_subject(&tx)
}

/// The bodies of the table, each with the subject the door is asked about.
async fn table_bodies() -> Vec<(String, Vec<u8>, String)> {
    let mut bodies: Vec<(String, Vec<u8>, String)> = Vec::new();
    let mut add = |name: &str, (beef, subject): (Vec<u8>, String)| {
        bodies.push((name.to_string(), beef, subject));
    };

    // The real covenant legs.
    add(
        "covenant settle (real)",
        covenant_leg(ENFORCED_FUNDING_HEX, ENFORCED_SETTLE_HEX, None),
    );
    add(
        "covenant settle, preimage tampered",
        covenant_leg(ENFORCED_FUNDING_HEX, ENFORCED_SETTLE_HEX, Some(1000)),
    );
    add(
        "covenant refund (real)",
        covenant_leg(REFUND_FUNDING_HEX, REFUND_HEX, None),
    );
    add(
        "covenant refund, preimage tampered",
        covenant_leg(REFUND_FUNDING_HEX, REFUND_HEX, Some(1000)),
    );

    // P2PKH, and what is asked of it.
    let valid = p2pkh_spend(9_000).await;
    let (p2pkh, p2pkh_subject) = beef_and_subject(&valid);
    add("P2PKH", (p2pkh.clone(), p2pkh_subject.clone()));
    let mut corrupted = p2pkh_spend(9_000).await;
    let mut bytes = corrupted.inputs[0]
        .unlocking_script
        .as_ref()
        .expect("an unlocking script")
        .to_binary();
    bytes[10] ^= 0x01;
    corrupted.inputs[0].unlocking_script = Some(UnlockingScript::from_script(
        Script::from_binary(&bytes).expect("a script"),
    ));
    add("P2PKH, signature corrupted", beef_and_subject(&corrupted));
    let funding_txid = valid.inputs[0]
        .source_transaction
        .as_ref()
        .expect("a source")
        .id();
    add(
        "P2PKH, the subject is its proven funding",
        (p2pkh.clone(), funding_txid.clone()),
    );
    add(
        "P2PKH, the subject is absent",
        (p2pkh.clone(), "ab".repeat(32)),
    );
    add(
        "P2PKH, the subject is not a txid",
        (p2pkh.clone(), "zz".into()),
    );
    add(
        "P2PKH, the subject in upper case",
        (p2pkh.clone(), p2pkh_subject.to_uppercase()),
    );
    add(
        "P2PKH, V1 frame",
        (valid.to_beef_v1(false).expect("V1"), p2pkh_subject.clone()),
    );
    add(
        "P2PKH, atomic frame",
        (
            valid.to_atomic_beef(false).expect("atomic"),
            p2pkh_subject.clone(),
        ),
    );
    add(
        "P2PKH, cut 10 bytes short",
        (p2pkh[..p2pkh.len() - 10].to_vec(), p2pkh_subject.clone()),
    );
    add(
        "P2PKH, a trailing byte",
        ([&p2pkh[..], &[0x00]].concat(), p2pkh_subject.clone()),
    );
    // The flag byte of the BUMP's one leaf stands before the funding's txid.
    let mut funding_wire = hex::decode(&funding_txid).expect("hex");
    funding_wire.reverse();
    let leaf_at = p2pkh
        .windows(32)
        .position(|w| w == funding_wire)
        .expect("the BUMP carries the funding's txid");
    let mut bad_flag = p2pkh.clone();
    bad_flag[leaf_at - 1] = 0x82;
    add(
        "P2PKH, a BUMP leaf flag with unknown bits",
        (bad_flag, p2pkh_subject.clone()),
    );
    add(
        "P2PKH, creating satoshis (the value rule)",
        beef_and_subject(&p2pkh_spend(11_000).await),
    );
    // The funding as a txid-only entry: no source to execute against.
    let mut stub = Beef::from_binary(&p2pkh).expect("the BEEF");
    stub.make_txid_only(&funding_txid);
    add(
        "P2PKH, its funding txid-only",
        (stub.to_binary(), p2pkh_subject.clone()),
    );
    // The spend alone: its source is not in the BEEF.
    let mut alone = Beef::new();
    alone.merge_raw_tx(valid.to_binary(), None);
    add(
        "P2PKH, its funding absent",
        (alone.to_binary(), p2pkh_subject.clone()),
    );

    add("P2PKH diamond chain, 8 levels", diamond_chain(8).await);
    // A parent and a child with no funding: the subject is judged first.
    let (diamond, _) = diamond_chain(2).await;
    let parsed = Beef::from_binary(&diamond).expect("the BEEF");
    let mut orphan = Beef::new();
    for btx in &parsed.txs[1..] {
        orphan.merge_raw_tx(btx.tx().expect("a raw transaction").to_binary(), None);
    }
    let child = parsed.txs.last().expect("a child").txid();
    add(
        "P2PKH parent and child, no funding",
        (orphan.to_binary(), child),
    );

    for (name, beef, subject) in mixed_bodies().await {
        add(name, (beef, subject));
    }
    add(
        "CHECKMULTISIG, key count computed",
        computed_multisig().await,
    );

    // Scripts with no signature.
    let open = |lock: Vec<u8>, unlock: Vec<u8>| spend_of_locks(1, 0, &[(lock, unlock)]);
    add("open lock", open(OPEN_LOCK.to_vec(), OPEN_UNLOCK.to_vec()));
    add(
        "a script that leaves FALSE",
        open(vec![OP_DROP, OP_0], OPEN_UNLOCK.to_vec()),
    );
    let hashes = |n: usize| [vec![OP_SHA256; n], vec![OP_DROP, OP_1]].concat();
    add("511 hash opcodes", open(hashes(511), OPEN_UNLOCK.to_vec()));
    add("513 hash opcodes", open(hashes(513), OPEN_UNLOCK.to_vec()));
    let mut fat = vec![0x4d, 0x00, 0x20];
    fat.extend_from_slice(&[0x42; 8 * 1024]);
    add(
        "OP_DUP OP_CAT to 32 MB",
        open(
            [[OP_DUP, OP_CAT].repeat(12), vec![OP_DROP, OP_1]].concat(),
            fat,
        ),
    );
    add(
        "OP_NUM2BIN of 1 GB",
        open(
            vec![
                OP_1, 0x04, 0x00, 0xca, 0x9a, 0x3b, OP_NUM2BIN, OP_DROP, OP_1,
            ],
            vec![],
        ),
    );
    for version in [1u32, 2] {
        let spends: Vec<(Vec<u8>, Vec<u8>)> = reader_locks()
            .into_iter()
            .filter(|(name, _)| !name.ends_with("against 9"))
            .map(|(_, lock)| (lock, OPEN_UNLOCK.to_vec()))
            .collect();
        add(
            &format!("six sig-less readers, version {version}"),
            spend_of_locks(version, 500_000, &spends),
        );
    }

    // The shapes the two limbs are for.
    add("5 bare signature checks", checksig_chain(5));
    add("1,100 bare signature checks", checksig_chain(1_100));
    add("2,000 minimal transactions", minimal_transactions(2_000));
    add("one BUMP of 2^10 leaves", wide_bump(10));
    add("one BUMP of 2^17 leaves", wide_bump(17));
    bodies
}

/// One row of the table: the door's answer, in full.
fn row(name: &str, verdict: &Result<WalkStats, EngineError>) -> String {
    match verdict {
        Ok(s) => format!(
            "{name} | OK txs={} inputs={} bytes={} hash={} sig={} work={} judged={}",
            s.unproven_txs,
            s.inputs_executed,
            s.script_bytes,
            s.hash_ops,
            s.sig_ops,
            s.work_bytes,
            s.subject_judged
        ),
        Err(e) => {
            let class = match e {
                EngineError::ScriptVerificationFailed { .. } => "REFUSED",
                EngineError::ScriptWalkOverBudget { .. } => "OVER",
                EngineError::ScriptWalkInconclusive { .. } => "INCONCLUSIVE",
                EngineError::BeefParseError(_) => "PARSE",
                _ => "OTHER",
            };
            format!("{name} | {class} {e}")
        }
    }
}

/// The door's answer for every body of the table, frozen on the fold's head.
/// Run on `8c92671` this test fails on 13 of the 39 rows, the fold and
/// nothing else: the ten walked rows with a signature check have a lower
/// `work` there (a check was charged its transaction's bytes: P2PKH 131,394
/// against 196,739), and three rows WALK there that are the door's bound
/// here, "CHECKMULTISIG, key count computed" (3,971 keys at 135 bytes each;
/// at the floor they are 260 MB), "1,100 bare signature checks" and "one
/// BUMP of 2^17 leaves". The other 26, every refusal, every structural fault
/// and every parse error among them, are the same to the byte.
const TABLE: &[&str] = &[
    "covenant settle (real) | OK txs=1 inputs=1 bytes=6608 hash=5 sig=4 work=924112 judged=true",
    "covenant settle, preimage tampered | REFUSED script verification failed (subject 33501b10e9b5289dc98140e1970cfed7f65de8f7221e5b290fa00d85395340ba): input 0: OP_VERIFY requires the top stack value to be truthy.",
    "covenant refund (real) | OK txs=1 inputs=1 bytes=6608 hash=5 sig=4 work=924112 judged=true",
    "covenant refund, preimage tampered | REFUSED script verification failed (subject 046765cce8819ef7b47cd12a225fc59ccb9a5ab049e82598ac4b84d06a51a150): input 0: OP_VERIFY requires the top stack value to be truthy.",
    "P2PKH | OK txs=1 inputs=1 bytes=131 hash=1 sig=1 work=196739 judged=true",
    "P2PKH, signature corrupted | REFUSED script verification failed (subject 7e7770d785fad7c7ebe73150a278b28c488a4814fd3426b96656dc74a58dcfca): input 0: The top stack element must be truthy after script evaluation.",
    "P2PKH, the subject is its proven funding | OK txs=0 inputs=0 bytes=0 hash=0 sig=0 work=0 judged=false",
    "P2PKH, the subject is absent | INCONCLUSIVE script walk inconclusive at abababababababababababababababababababababababababababababababab (subject judged: false): transaction abababababababababababababababababababababababababababababababab is not in the BEEF",
    "P2PKH, the subject is not a txid | INCONCLUSIVE script walk inconclusive at zz (subject judged: false): transaction zz is not in the BEEF",
    "P2PKH, the subject in upper case | OK txs=1 inputs=1 bytes=131 hash=1 sig=1 work=196739 judged=true",
    "P2PKH, V1 frame | OK txs=1 inputs=1 bytes=131 hash=1 sig=1 work=196739 judged=true",
    "P2PKH, atomic frame | OK txs=1 inputs=1 bytes=131 hash=1 sig=1 work=196739 judged=true",
    "P2PKH, cut 10 bytes short | PARSE BEEF parsing failed: invalid BEEF at byte 297: Truncated { needed: 25 }",
    "P2PKH, a trailing byte | PARSE BEEF parsing failed: invalid BEEF at byte 326: TrailingBytes",
    "P2PKH, a BUMP leaf flag with unknown bits | PARSE BEEF parsing failed: invalid BEEF at byte 13: BadFlag { byte: 130 }",
    "P2PKH, creating satoshis (the value rule) | INCONCLUSIVE script walk inconclusive at 87c85d92569097da31c052ccd2648fc189a17f628433291eaab22e91ab1a4433 (subject judged: true): transaction 87c85d92569097da31c052ccd2648fc189a17f628433291eaab22e91ab1a4433 creates 11000 sats from 10000 sats of inputs",
    "P2PKH, its funding txid-only | INCONCLUSIVE script walk inconclusive at 0679da8db8badbe76292be47ef14a3c6ac8b060996f250611297b39bdc2f23ed (subject judged: false): input 0 of transaction 0679da8db8badbe76292be47ef14a3c6ac8b060996f250611297b39bdc2f23ed has no source transaction",
    "P2PKH, its funding absent | INCONCLUSIVE script walk inconclusive at 0679da8db8badbe76292be47ef14a3c6ac8b060996f250611297b39bdc2f23ed (subject judged: false): input 0 of transaction 0679da8db8badbe76292be47ef14a3c6ac8b060996f250611297b39bdc2f23ed has no source transaction",
    "P2PKH diamond chain, 8 levels | OK txs=8 inputs=15 bytes=1973 hash=15 sig=15 work=2951093 judged=true",
    "P2PKH parent and child, no funding | INCONCLUSIVE script walk inconclusive at 92cf93e39c8f6c684908b79548345cea7be5c01c0fd4f2dbbb98c9e581c69bdb (subject judged: true): input 0 of transaction 92cf93e39c8f6c684908b79548345cea7be5c01c0fd4f2dbbb98c9e581c69bdb has no source transaction",
    "mixed: intact | OK txs=1 inputs=3 bytes=139 hash=1 sig=1 work=196747 judged=true",
    "mixed: the sequence of sig-less input 0 changed | REFUSED script verification failed (subject 013f846a8940273066dfca285ae04579c1cb64b3d222078b861b159490b072f3): input 1: The top stack element must be truthy after script evaluation.",
    "mixed: the lock time changed | REFUSED script verification failed (subject 75e9c2a12a463f7112e72312cc4786bf4d6e55dad8ea9db039162e0deb5bf2d9): input 1: The top stack element must be truthy after script evaluation.",
    "mixed: an output's value changed | REFUSED script verification failed (subject 3eefc7253e39f1fa907dfb1229fca9b4b89b2c5377c9628ae5cefe9f5514a0ae): input 1: The top stack element must be truthy after script evaluation.",
    "mixed: the unlocking script of sig-less input 0 changed | OK txs=1 inputs=3 bytes=139 hash=1 sig=1 work=196747 judged=true",
    "CHECKMULTISIG, key count computed | OVER script walk over budget at cc11a4f49a51c6791acefbaf44c263aaba8df4275c55de65399768e836fdb2a2 (subject judged: false): estimated work 260243636 bytes exceeds the door budget of 67108864 (input 0 of cc11a4f49a51c6791acefbaf44c263aaba8df4275c55de65399768e836fdb2a2)",
    "open lock | OK txs=1 inputs=1 bytes=4 hash=0 sig=0 work=4 judged=true",
    "a script that leaves FALSE | REFUSED script verification failed (subject e185c17b089f23b883f73906049cef2fadba46ef09ce1719d693616e0cbc3d33): input 0: The top stack element must be truthy after script evaluation.",
    "511 hash opcodes | OK txs=1 inputs=1 bytes=515 hash=511 sig=0 work=66978307 judged=true",
    "513 hash opcodes | OVER script walk over budget at 6e36e2ba09f4ad3f80b88922a1bb0131c1c213c194bd3d7b81ca06188b68a7f3 (subject judged: false): estimated work 67240453 bytes exceeds the door budget of 67108864 (input 0 of 6e36e2ba09f4ad3f80b88922a1bb0131c1c213c194bd3d7b81ca06188b68a7f3)",
    "OP_DUP OP_CAT to 32 MB | OVER script walk over budget at 71a4c162bd2136b497e41391ce8a5337d914a1eeb1f62679b657b2619ffe9283 (subject judged: false): input 0 of transaction 71a4c162bd2136b497e41391ce8a5337d914a1eeb1f62679b657b2619ffe9283: Stack memory usage has exceeded 131072 bytes",
    "OP_NUM2BIN of 1 GB | OVER script walk over budget at b2d62ac31d410d52c712a98f5259b61cf68598d975e7a6a960502dcdcee0558e (subject judged: false): input 0 of transaction b2d62ac31d410d52c712a98f5259b61cf68598d975e7a6a960502dcdcee0558e: Script element allocation has exceeded 131072 bytes",
    "six sig-less readers, version 1 | REFUSED script verification failed (subject 70c50b8d8a78da8bea0e15ca80d641cf95652944b9e68c36906b7375859e308c): input 5: OP_VER is disabled until Chronicle.",
    "six sig-less readers, version 2 | OK txs=1 inputs=6 bytes=38 hash=1 sig=0 work=131110 judged=true",
    "5 bare signature checks | OK txs=5 inputs=5 bytes=230 hash=0 sig=5 work=327910 judged=true",
    "1,100 bare signature checks | OVER script walk over budget at 1bde628f079f00365ef5ea90d00a6798c32b669fe26814777fc7712c4a837118 (subject judged: true): estimated work 67155968 bytes exceeds the door budget of 67108864 (input 0 of 1bde628f079f00365ef5ea90d00a6798c32b669fe26814777fc7712c4a837118)",
    "2,000 minimal transactions | OK txs=1 inputs=0 bytes=0 hash=0 sig=0 work=0 judged=true",
    "one BUMP of 2^10 leaves | OK txs=1 inputs=1 bytes=4 hash=0 sig=0 work=4 judged=true",
    "one BUMP of 2^17 leaves | OVER script walk over budget at 57b7478b7f84560e965a831cfb84ad2022982a8e355e76f68d4a6c33cef7bea0 (subject judged: false): estimated memory 50332050 bytes exceeds the door memory budget of 50331648 (the element at byte 5 of the BEEF)",
];

/// The SHA-256 of the rows, each followed by a line feed.
const TABLE_DIGEST: &str = "dc166ec499abe55d55e80cfac0943eb4e352f793d3ec641bc0fbd73e446d3e9b";

/// THE PIN (N2). The parity of the door, kept in the tree: a body table with
/// the door's answer frozen per body, statistics and error text, and the
/// digest of the whole.
#[test]
fn e585f_n2_the_doors_answers_are_frozen_per_body() {
    one_at_a_time(async {
        let engine = engine();
        let mut rows = Vec::new();
        for (name, beef, subject) in table_bodies().await {
            let verdict = engine.verify_scripts_only(&beef, &subject).await;
            rows.push(row(&name, &verdict));
        }
        let digest = hex::encode(sha256(
            rows.iter()
                .flat_map(|row| [row.as_bytes(), b"\n"].concat())
                .collect::<Vec<u8>>()
                .as_slice(),
        ));
        if std::env::var_os("E585F_PRINT_TABLE").is_some() {
            for row in &rows {
                println!("    {row:?},");
            }
            println!("digest {digest}");
        }
        let mut differing = Vec::new();
        for (i, row) in rows.iter().enumerate() {
            if TABLE.get(i).copied() != Some(row.as_str()) {
                differing.push(format!(
                    "  now:    {row}\n  frozen: {}",
                    TABLE.get(i).copied().unwrap_or("(no row)")
                ));
            }
        }
        assert!(
            differing.is_empty() && rows.len() == TABLE.len(),
            "{} of {} rows differ from the frozen table:\n{}",
            differing.len(),
            rows.len(),
            differing.join("\n")
        );
        assert_eq!(digest, TABLE_DIGEST);
    });
}

// ── THE FOLD'S OWN API ──────────────────────────────────────────────────

use bsv_overlay_engine::engine::DoorLimb;

/// A breach names its limb, so a caller counts each on its own counter.
#[test]
fn e585f_a_breach_names_its_limb() {
    one_at_a_time(async {
        let engine = engine();
        let limb_of = |verdict: Result<WalkStats, EngineError>| match verdict {
            Err(EngineError::ScriptWalkOverBudget { limb, .. }) => limb,
            other => panic!("expected the door's own bound, got {other:?}"),
        };
        let (body, subject) = checksig_chain(1_100);
        assert_eq!(
            limb_of(engine.verify_scripts_only(&body, &subject).await),
            DoorLimb::Work
        );
        let (body, subject) = wide_bump(17);
        assert_eq!(
            limb_of(engine.verify_scripts_only(&body, &subject).await),
            DoorLimb::Memory
        );
        // The same BUMP under a budget that holds it is walked: the limb is a
        // budget of the platform, not a rule about a BUMP.
        let roomy = DoorBudget {
            max_memory_bytes: 128 * 1024 * 1024,
            ..DoorBudget::DEFAULT
        };
        let stats = engine
            .verify_scripts_only_under(&body, &subject, roomy)
            .await
            .expect("a valid BEEF, walked");
        assert!(stats.subject_judged);
    });
}

/// THE ESTIMATE IS AN UPPER BOUND. With the memory limb lifted, the door's
/// estimate of each shape (made from the frame before the stream is opened)
/// is held against the peak of the live heap the walk then reached: the two
/// bodies of the lens, a transaction of 200,000 outputs, one of 300 inputs
/// and 2 MB, a signed one of 1 MB (the digest's copies), and 199 signed
/// inputs over 100 unproven transactions.
#[test]
fn e585f_l3_the_memory_estimate_is_above_the_measured_heap() {
    one_at_a_time(async {
        let mut shapes: Vec<(&str, Vec<u8>, String)> = Vec::new();
        let (body, subject) = minimal_transactions(900_000);
        shapes.push(("900,000 minimal transactions", body, subject));
        let (body, subject) = wide_bump(18);
        shapes.push(("one BUMP of 2^18 leaves", body, subject));

        // One unproven transaction of 200,000 empty outputs.
        let funding = funding_raw(&[(1_000, OPEN_LOCK.to_vec())]);
        let funding_txid = wire_txid(&funding);
        let wide = raw_tx(
            1,
            &[spend(funding_txid, 0, &OPEN_UNLOCK)],
            &vec![(0, Vec::new()); 200_000],
            0,
        );
        let subject = display(&wire_txid(&wide));
        shapes.push((
            "a transaction of 200,000 outputs",
            beef_v1(
                &[one_leaf_bump(&funding_txid)],
                &[(funding, Some(0)), (wide, None)],
            ),
            subject,
        ));

        // One unproven transaction of 300 inputs and 2 MB of data.
        let funding = funding_raw(&vec![(1_000, OPEN_LOCK.to_vec()); 300]);
        let funding_txid = wire_txid(&funding);
        let inputs: Vec<In> = (0..300)
            .map(|vout| spend(funding_txid, vout, &OPEN_UNLOCK))
            .collect();
        let mut data = vec![OP_0, OP_RETURN, 0x4e];
        data.extend_from_slice(&(2u32 * 1024 * 1024).to_le_bytes());
        data.resize(data.len() + 2 * 1024 * 1024, 0x5a);
        let heavy = raw_tx(1, &inputs, &[(900, vec![OP_1]), (0, data)], 0);
        let subject = display(&wire_txid(&heavy));
        shapes.push((
            "a transaction of 300 inputs and 2 MB",
            beef_v1(
                &[one_leaf_bump(&funding_txid)],
                &[(funding, Some(0)), (heavy, None)],
            ),
            subject,
        ));

        // A signed transaction of 1 MB: the digest's copies of it.
        let key = fixed_key();
        let lock = P2PKH::new()
            .lock(&key.public_key().hash160())
            .expect("a P2PKH lock");
        let funding = funding_of(vec![TransactionOutput::new(10_000, lock)]);
        let mut tx = Transaction::new();
        tx.add_input_from_tx(funding, 0, P2PKH::unlock(&key, SignOutputs::All, false))
            .expect("the input");
        let mut data = Script::new();
        data.write_opcode(OP_0)
            .write_opcode(OP_RETURN)
            .write_bin(&vec![0x5a; 1024 * 1024]);
        tx.outputs
            .push(TransactionOutput::new(0, LockingScript::from_script(data)));
        tx.sign().await.expect("the spend signs");
        let (body, subject) = beef_and_subject(&tx);
        shapes.push(("a signed transaction of 1 MB", body, subject));

        let (body, subject) = diamond_chain(100).await;
        shapes.push(("199 signed inputs over 100 transactions", body, subject));

        let lifted = DoorBudget {
            max_memory_bytes: u64::MAX,
            ..DoorBudget::DEFAULT
        };
        let engine = engine();
        for (name, body, subject) in &shapes {
            let at_entry = mark();
            let verdict = engine
                .verify_scripts_only_under(body, subject, lifted)
                .await;
            let peak = peak_over(at_entry) as u64;
            let stats = verdict.unwrap_or_else(|e| panic!("{name}: {e}"));
            println!(
                "e585f_l3 {name}: body {} bytes, estimated {} bytes, measured peak {peak} bytes \
                 ({:.2}x the body; the estimate is {:.2}x the peak)",
                body.len(),
                stats.memory_bytes,
                peak as f64 / body.len() as f64,
                stats.memory_bytes as f64 / peak as f64,
            );
            assert!(
                peak <= stats.memory_bytes,
                "{name}: the walk held {peak} bytes, the estimate was {}",
                stats.memory_bytes
            );
        }
    });
}
