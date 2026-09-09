//! Submit-time SCRIPT verification: reference parity (2026-09-08).
//!
//! The reference (`overlay-express` `Engine.submit` → ts-sdk
//! `Transaction.verify`) executes every unproven input's unlocking script,
//! and checks every merkle path against the chain tracker, before a topic
//! manager ever sees the transaction. This engine used to check BEEF
//! structure only (and only when a chain tracker was configured), so an
//! invalid spend that no broadcaster had yet refused (or one arriving through
//! a `historical-tx` submit) could be admitted on structure alone.
//!
//! These tests are the executable proof of the port:
//! - P2PKH spends built and signed here (a valid one is admitted, the same
//!   spend with one signature byte flipped is refused with the SCRIPT error,
//!   with and without a chain tracker, and is still admitted under
//!   `historical-tx-no-spv`, exactly as the reference skips it there);
//! - REAL MAINNET OP_PUSH_TX covenant legs (`fixtures/*.hex`, each raw tx
//!   hashes to its filename; the Poc5 pot covenant's tower-enforced settle
//!   and its pre-signed refund, each spending its real 3150-byte funding
//!   lock): admitted intact, refused with a tampered preimage;
//! - a CAT / SHA256 / CHECKSIG covenant whose wrong witness is refused;
//! - the reference's value rule (an unproven spend may not create satoshis);
//! - the builder's escape hatch (`with_script_verification(false)`);
//! - one deliberately expensive spend (~7 KB lock, ~20 KB unlock, ~7000
//!   opcodes) whose verification time is printed; run with `--nocapture`.

use async_trait::async_trait;
use bsv_overlay_engine::builder::EngineBuilder;
use bsv_overlay_engine::engine::{Engine, EngineError};
use bsv_overlay_engine::storage::memory::MemoryStorage;
use bsv_overlay_engine::storage::Storage;
use bsv_overlay_engine::topic_manager::{TopicManager, TopicManagerError};
use bsv_overlay_engine::types::*;
use bsv_rs::primitives::bsv::TransactionSignature;
use bsv_rs::primitives::{sha256, PrivateKey};
use bsv_rs::script::op::*;
use bsv_rs::script::template::compute_sighash_scope;
use bsv_rs::script::templates::P2PKH;
use bsv_rs::script::{
    LockingScript, Script, ScriptTemplate, ScriptTemplateUnlock, SignOutputs, SigningContext,
    UnlockingScript,
};
use bsv_rs::transaction::{
    Beef, ChainTracker, ChainTrackerError, MerklePath, MerklePathLeaf, MockChainTracker,
    Transaction, TransactionInput, TransactionOutput,
};
use std::rc::Rc;
use std::time::Instant;

// ============================================================================
// Fixtures
// ============================================================================

/// Block height every fabricated single-transaction BUMP claims.
const HEIGHT: u32 = 800_000;
const TOPIC: &str = "tm_test";

/// REAL MAINNET Poc5 pot covenant (bsv-low): the funding tx whose output 0 is
/// the 3150-byte covenant lock, and the TOWER-ENFORCED settle that spends it
/// with `<sig> <sig> <3309-byte OP_PUSH_TX preimage>` (decision log
/// 2026-07-05). Copied from `crates/low-app-layer/tests/fixtures/`.
const ENFORCED_FUNDING_TXID: &str =
    "c571d433b8234e225af0c631f076b137b7c164cfa72f86b3e713f9ba67e3b563";
const ENFORCED_FUNDING_HEX: &str =
    include_str!("fixtures/c571d433b8234e225af0c631f076b137b7c164cfa72f86b3e713f9ba67e3b563.hex");
const ENFORCED_SETTLE_TXID: &str =
    "91309122f5630052f7e57f7db843d26d32ae4426a9dd9b2fc2955f2fab8cf9a6";
const ENFORCED_SETTLE_HEX: &str =
    include_str!("fixtures/91309122f5630052f7e57f7db843d26d32ae4426a9dd9b2fc2955f2fab8cf9a6.hex");

/// The 2026-07-21 pre-signed REFUND leg of the same covenant family
/// (nLockTime 958846, nSequence 0xfffffffe: the preimage's locktime path).
const REFUND_FUNDING_TXID: &str =
    "5533ca32a296c58778a240cd7649392bf2e6b11ef63e1c71765913ebba093c59";
const REFUND_FUNDING_HEX: &str =
    include_str!("fixtures/5533ca32a296c58778a240cd7649392bf2e6b11ef63e1c71765913ebba093c59.hex");
const REFUND_TXID: &str = "3ca368b0ca4dcb31ba87977d7aaf3a4671eafa2c980864c880f96080c68cee36";
const REFUND_HEX: &str =
    include_str!("fixtures/3ca368b0ca4dcb31ba87977d7aaf3a4671eafa2c980864c880f96080c68cee36.hex");

/// Byte offset inside the settle's unlocking script that lands in the
/// OP_PUSH_TX preimage's scriptCode region (chunks: push72 @1, push72 @74,
/// PUSHDATA2 3309 bytes @149).
const PREIMAGE_TAMPER_OFFSET: usize = 1000;

struct AdmitOutputZero;

#[async_trait(?Send)]
impl TopicManager for AdmitOutputZero {
    async fn identify_admissible_outputs(
        &self,
        _: &Transaction,
        _: &[u8],
        _: Option<&[u8]>,
        _: SubmitMode,
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

/// A chain tracker whose lookups FAIL (an outage), never answer.
struct BrokenTracker;

#[async_trait]
impl ChainTracker for BrokenTracker {
    async fn is_valid_root_for_height(&self, _: &str, _: u32) -> Result<bool, ChainTrackerError> {
        Err(ChainTrackerError::NetworkError(
            "chaintracks unreachable".into(),
        ))
    }
    async fn current_height(&self) -> Result<u32, ChainTrackerError> {
        Err(ChainTrackerError::NetworkError(
            "chaintracks unreachable".into(),
        ))
    }
}

/// A chain tracker that knows exactly one (height, root): the fabricated
/// single-transaction block of a fixture's funding tx.
fn tracker_knowing(funding_txid: &str) -> Box<dyn ChainTracker> {
    let mut tracker = MockChainTracker::new(HEIGHT + 10);
    tracker.add_root(HEIGHT, funding_txid.to_string());
    Box::new(tracker)
}

/// A chain tracker that knows nothing (every root is wrong).
fn tracker_knowing_nothing() -> Box<dyn ChainTracker> {
    Box::new(MockChainTracker::new(HEIGHT + 10))
}

/// The engine's storage is `Box<dyn Storage>`; an `Rc<MemoryStorage>` is
/// itself a `Storage`, so the test keeps a handle to assert on admissions.
fn engine_with(storage: Rc<MemoryStorage>, tracker: Option<Box<dyn ChainTracker>>) -> Engine {
    let mut builder =
        EngineBuilder::new(Box::new(storage)).with_topic(TOPIC, Box::new(AdmitOutputZero));
    if let Some(tracker) = tracker {
        builder = builder.with_chain_tracker(tracker);
    }
    builder.build()
}

fn engine(tracker: Option<Box<dyn ChainTracker>>) -> Engine {
    engine_with(Rc::new(MemoryStorage::new()), tracker)
}

/// A BRC-74 BUMP for a block containing only `txid`; its root IS the txid.
fn single_tx_block_proof(txid: &str) -> MerklePath {
    MerklePath::new(
        HEIGHT,
        vec![vec![MerklePathLeaf::new_txid(0, txid.to_string())]],
    )
    .expect("a one-leaf BUMP is valid")
}

/// A "mined" funding transaction: one throwaway input (never verified: the
/// tx carries a merkle path, so the walk trusts it) paying `sats` to `lock`.
fn proven_funding(lock: LockingScript, sats: u64) -> Transaction {
    let mut tx = Transaction::new();
    tx.inputs.push(TransactionInput {
        source_txid: Some("aa".repeat(32)),
        source_output_index: 0,
        unlocking_script: Some(UnlockingScript::from_script(Script::new())),
        ..Default::default()
    });
    tx.outputs.push(TransactionOutput::new(sats, lock));
    let txid = tx.id();
    tx.merkle_path = Some(single_tx_block_proof(&txid));
    tx
}

struct SpendFixture {
    beef: Vec<u8>,
    funding_txid: String,
    subject_txid: String,
}

/// Spend output 0 of a fresh proven funding tx (`funding_sats` under `lock`)
/// with `unlock`, paying `output_sats` to a throwaway P2PKH.
async fn spend_of(
    lock: LockingScript,
    unlock: ScriptTemplateUnlock,
    funding_sats: u64,
    output_sats: u64,
    tamper: impl FnOnce(&mut Transaction),
) -> SpendFixture {
    let funding = proven_funding(lock, funding_sats);
    let funding_txid = funding.id();
    let mut tx = Transaction::new();
    tx.add_input_from_tx(funding, 0, unlock).unwrap();
    let pay_to = PrivateKey::random().public_key().hash160();
    tx.outputs.push(TransactionOutput::new(
        output_sats,
        P2PKH::new().lock(&pay_to).unwrap(),
    ));
    tx.sign().await.expect("template signing");
    tamper(&mut tx);
    SpendFixture {
        beef: tx.to_beef(false).expect("BEEF with the proven parent"),
        funding_txid,
        subject_txid: tx.id(),
    }
}

/// Flip one bit of input 0's unlocking script at `offset`.
///
/// `Transaction` caches its txid AND its serialization; a field write bypasses
/// both, so the caches are dropped explicitly or `id()` / `to_binary()` keep
/// answering for the untampered bytes.
fn flip_unlocking_byte(tx: &mut Transaction, offset: usize) {
    let mut bytes = tx.inputs[0].unlocking_script.as_ref().unwrap().to_binary();
    bytes[offset] ^= 0x01;
    tx.inputs[0].unlocking_script = Some(UnlockingScript::from_script(
        Script::from_binary(&bytes).unwrap(),
    ));
    tx.invalidate_caches();
}

/// A signed P2PKH spend (10 000 sats in, `output_sats` out). With
/// `corrupt_signature`, one byte inside the DER `r` value is flipped AFTER
/// signing: DER stays well-formed, the signature no longer verifies.
async fn p2pkh_spend(corrupt_signature: bool, output_sats: u64) -> SpendFixture {
    let key = PrivateKey::random();
    let lock = P2PKH::new().lock(&key.public_key().hash160()).unwrap();
    spend_of(
        lock,
        P2PKH::unlock(&key, SignOutputs::All, false),
        10_000,
        output_sats,
        |tx| {
            if corrupt_signature {
                // [0x47/0x48 push][0x30 len][0x02 rlen][r...]: byte 10 is in r.
                flip_unlocking_byte(tx, 10);
            }
        },
    )
    .await
}

/// A REAL covenant leg: the mainnet funding tx (given a fabricated
/// single-tx-block BUMP so the walk trusts it) plus the mainnet spend, with
/// an optional bit flip in the spend's unlocking script.
fn real_covenant_leg(
    funding_hex: &str,
    expected_funding_txid: &str,
    spend_hex: &str,
    expected_spend_txid: &str,
    tamper_at: Option<usize>,
) -> SpendFixture {
    let mut funding = Transaction::from_hex(funding_hex.trim()).expect("funding fixture parses");
    let funding_txid = funding.id();
    assert_eq!(
        funding_txid, expected_funding_txid,
        "funding fixture is byte-exact"
    );
    funding.merkle_path = Some(single_tx_block_proof(&funding_txid));

    let mut spend = Transaction::from_hex(spend_hex.trim()).expect("spend fixture parses");
    assert_eq!(
        spend.id(),
        expected_spend_txid,
        "spend fixture is byte-exact"
    );
    assert_eq!(
        spend.inputs[0].source_txid.as_deref(),
        Some(expected_funding_txid),
        "the spend consumes the funding tx"
    );
    if let Some(offset) = tamper_at {
        flip_unlocking_byte(&mut spend, offset);
    }
    spend.inputs[0].source_transaction = Some(Box::new(funding));
    SpendFixture {
        beef: spend
            .to_beef(false)
            .expect("BEEF with the proven funding tx"),
        funding_txid,
        subject_txid: spend.id(),
    }
}

fn tagged(fixture: &SpendFixture) -> TaggedBEEF {
    TaggedBEEF::new(fixture.beef.clone(), vec![TOPIC.into()])
}

async fn is_admitted(storage: &MemoryStorage, txid: &str) -> bool {
    storage
        .find_output(txid, 0, Some(TOPIC), None, false)
        .await
        .unwrap()
        .is_some()
}

fn expect_script_error(err: &EngineError) -> (u32, String) {
    match err {
        EngineError::ScriptVerificationFailed {
            input_index,
            reason,
            ..
        } => (*input_index, reason.clone()),
        other => panic!("expected ScriptVerificationFailed, got {other:?}"),
    }
}

// ============================================================================
// (a) / (b): P2PKH with a chain tracker
// ============================================================================

/// (a) A valid P2PKH spend whose parent is proven passes: root checked, script
/// executed, output admitted.
#[tokio::test]
async fn valid_p2pkh_spend_is_admitted_with_a_chain_tracker() {
    let fixture = p2pkh_spend(false, 9_000).await;
    let storage = Rc::new(MemoryStorage::new());
    let engine = engine_with(
        Rc::clone(&storage),
        Some(tracker_knowing(&fixture.funding_txid)),
    );

    let steak = engine
        .submit(&tagged(&fixture), SubmitMode::CurrentTx)
        .await
        .expect("a valid spend with a valid proof is admitted");
    assert_eq!(steak[TOPIC].outputs_to_admit, vec![0]);
    assert!(is_admitted(&storage, &fixture.subject_txid).await);
}

/// (b) The same spend with one signature byte flipped is REFUSED with the
/// script error (input index + interpreter message), and nothing is admitted.
#[tokio::test]
async fn corrupted_signature_is_refused_with_the_script_error() {
    let fixture = p2pkh_spend(true, 9_000).await;
    let storage = Rc::new(MemoryStorage::new());
    let engine = engine_with(
        Rc::clone(&storage),
        Some(tracker_knowing(&fixture.funding_txid)),
    );

    let err = engine
        .submit(&tagged(&fixture), SubmitMode::CurrentTx)
        .await
        .expect_err("a spend whose signature does not verify must be refused");
    let (input_index, reason) = expect_script_error(&err);
    assert_eq!(input_index, 0);
    assert!(!reason.is_empty(), "the interpreter's message is carried");
    let rendered = err.to_string();
    assert!(rendered.contains(&fixture.subject_txid), "{rendered}");
    assert!(rendered.contains("input 0"), "{rendered}");
    assert!(
        !is_admitted(&storage, &fixture.subject_txid).await,
        "must not be admitted"
    );
}

/// The reference verifies `historical-tx` too (only `-no-spv` skips): the
/// corrupted spend is refused there as well.
#[tokio::test]
async fn historical_tx_mode_also_executes_scripts() {
    let fixture = p2pkh_spend(true, 9_000).await;
    let engine = engine(Some(tracker_knowing(&fixture.funding_txid)));
    let err = engine
        .submit(&tagged(&fixture), SubmitMode::HistoricalTx)
        .await
        .expect_err("historical-tx is verified like current-tx");
    expect_script_error(&err);
}

// ============================================================================
// (c): historical-tx-no-spv still skips, per the reference
// ============================================================================

#[tokio::test]
async fn historical_tx_no_spv_still_admits_the_corrupted_spend() {
    let fixture = p2pkh_spend(true, 9_000).await;
    let storage = Rc::new(MemoryStorage::new());
    let engine = engine_with(
        Rc::clone(&storage),
        Some(tracker_knowing(&fixture.funding_txid)),
    );

    let steak = engine
        .submit(&tagged(&fixture), SubmitMode::HistoricalTxNoSpv)
        .await
        .expect("no-spv skips verification exactly as the reference does");
    assert_eq!(steak[TOPIC].outputs_to_admit, vec![0]);
    assert!(is_admitted(&storage, &fixture.subject_txid).await);
}

// ============================================================================
// (d): no chain tracker, 'scripts only'
// ============================================================================

#[tokio::test]
async fn scripts_run_without_a_chain_tracker() {
    // Valid spend, no tracker: roots are accepted unchecked, script runs, admitted.
    let valid = p2pkh_spend(false, 9_000).await;
    let storage = Rc::new(MemoryStorage::new());
    let engine = engine_with(Rc::clone(&storage), None);
    engine
        .submit(&tagged(&valid), SubmitMode::CurrentTx)
        .await
        .expect("scripts-only admits a valid spend");
    assert!(is_admitted(&storage, &valid.subject_txid).await);

    // Corrupted spend, no tracker: the script still runs and refuses it.
    let corrupted = p2pkh_spend(true, 9_000).await;
    let err = engine
        .submit(&tagged(&corrupted), SubmitMode::CurrentTx)
        .await
        .expect_err("scripts-only still executes the script");
    let (input_index, _) = expect_script_error(&err);
    assert_eq!(input_index, 0);
    assert!(!is_admitted(&storage, &corrupted.subject_txid).await);
}

// ============================================================================
// Bad proof vs bad spend are distinguishable
// ============================================================================

#[tokio::test]
async fn a_wrong_merkle_root_is_an_spv_error_not_a_script_error() {
    let fixture = p2pkh_spend(false, 9_000).await;
    let engine = engine(Some(tracker_knowing_nothing()));
    let err = engine
        .submit(&tagged(&fixture), SubmitMode::CurrentTx)
        .await
        .expect_err("a root the tracker does not know is refused");
    assert!(matches!(err, EngineError::SpvError(_)), "got {err:?}");
    assert!(err.to_string().contains("Invalid merkle path"), "{err}");
}

#[tokio::test]
async fn a_chain_tracker_outage_is_an_spv_error() {
    let fixture = p2pkh_spend(false, 9_000).await;
    let engine = engine(Some(Box::new(BrokenTracker)));
    let err = engine
        .submit(&tagged(&fixture), SubmitMode::CurrentTx)
        .await
        .expect_err("an unverifiable proof is refused");
    assert!(matches!(err, EngineError::SpvError(_)), "got {err:?}");
    assert!(err.to_string().contains("Chain tracker error"), "{err}");
}

// ============================================================================
// (e): REAL OP_PUSH_TX covenant legs
// ============================================================================

/// The mainnet tower-enforced settle of a Poc5 pot: `<sig> <sig> <preimage>`
/// against the real 3150-byte covenant lock. The interpreter runs the whole
/// OP_PUSH_TX leg and admits it.
#[tokio::test]
async fn real_pushtx_covenant_settle_is_admitted() {
    let fixture = real_covenant_leg(
        ENFORCED_FUNDING_HEX,
        ENFORCED_FUNDING_TXID,
        ENFORCED_SETTLE_HEX,
        ENFORCED_SETTLE_TXID,
        None,
    );
    let storage = Rc::new(MemoryStorage::new());
    let engine = engine_with(
        Rc::clone(&storage),
        Some(tracker_knowing(&fixture.funding_txid)),
    );
    let started = Instant::now();
    let steak = engine
        .submit(&tagged(&fixture), SubmitMode::CurrentTx)
        .await
        .expect("the real covenant settle satisfies its lock");
    println!(
        "real Poc5 covenant settle {ENFORCED_SETTLE_TXID}: submit (parse + OP_PUSH_TX verify + admit) took {} ms",
        started.elapsed().as_millis()
    );
    assert_eq!(steak[TOPIC].outputs_to_admit, vec![0]);
    assert!(is_admitted(&storage, &fixture.subject_txid).await);
}

/// The same settle with one bit of the OP_PUSH_TX preimage flipped: the
/// covenant's own preimage check fails and the spend is refused.
#[tokio::test]
async fn real_pushtx_covenant_settle_with_a_tampered_preimage_is_refused() {
    let fixture = real_covenant_leg(
        ENFORCED_FUNDING_HEX,
        ENFORCED_FUNDING_TXID,
        ENFORCED_SETTLE_HEX,
        ENFORCED_SETTLE_TXID,
        Some(PREIMAGE_TAMPER_OFFSET),
    );
    assert_ne!(
        fixture.subject_txid, ENFORCED_SETTLE_TXID,
        "the tamper changed the txid"
    );
    let storage = Rc::new(MemoryStorage::new());
    let engine = engine_with(
        Rc::clone(&storage),
        Some(tracker_knowing(&fixture.funding_txid)),
    );
    let err = engine
        .submit(&tagged(&fixture), SubmitMode::CurrentTx)
        .await
        .expect_err("a tampered preimage cannot satisfy the covenant");
    let (input_index, reason) = expect_script_error(&err);
    assert_eq!(input_index, 0);
    println!("tampered covenant preimage refused: {reason}");
    assert!(!is_admitted(&storage, &fixture.subject_txid).await);
}

/// The mainnet pre-signed REFUND leg (nLockTime 958846 / nSequence
/// 0xfffffffe; the covenant reads the locktime out of the preimage).
#[tokio::test]
async fn real_pushtx_covenant_refund_is_admitted() {
    let fixture = real_covenant_leg(
        REFUND_FUNDING_HEX,
        REFUND_FUNDING_TXID,
        REFUND_HEX,
        REFUND_TXID,
        None,
    );
    let engine = engine(None);
    let steak = engine
        .submit(&tagged(&fixture), SubmitMode::CurrentTx)
        .await
        .expect("the real covenant refund satisfies its lock (scripts-only walk)");
    assert_eq!(steak[TOPIC].outputs_to_admit, vec![0]);
}

// ============================================================================
// (e, synthetic): CAT / SHA256 / CHECKSIG covenant
// ============================================================================

/// `<pubkey> OP_CHECKSIGVERIFY OP_CAT OP_SHA256 <sha256(a‖b)> OP_EQUAL`,
/// unlocked by `<a> <b> <sig>`.
fn cat_sha256_checksig_lock(pubkey: &[u8; 33], committed: &[u8; 32]) -> LockingScript {
    let mut script = Script::new();
    script
        .write_bin(pubkey)
        .write_opcode(OP_CHECKSIGVERIFY)
        .write_opcode(OP_CAT)
        .write_opcode(OP_SHA256)
        .write_bin(committed)
        .write_opcode(OP_EQUAL);
    LockingScript::from_script(script)
}

/// Push-only unlock `<a> <b> <sig over the spend>` for the lock above.
fn witness_unlock(key: PrivateKey, a: Vec<u8>, b: Vec<u8>) -> ScriptTemplateUnlock {
    ScriptTemplateUnlock::new(
        move |ctx: &SigningContext| {
            let scope = compute_sighash_scope(SignOutputs::All, false);
            let sig = key.sign(&ctx.compute_sighash(scope)?)?;
            let mut script = Script::new();
            script
                .write_bin(&a)
                .write_bin(&b)
                .write_bin(&TransactionSignature::new(sig, scope).to_checksig_format());
            Ok(UnlockingScript::from_script(script))
        },
        || 200,
    )
}

#[tokio::test]
async fn cat_sha256_checksig_covenant_admits_the_right_witness_and_refuses_a_wrong_one() {
    let key = PrivateKey::random();
    let pubkey = key.public_key().to_compressed();
    let a = b"left half of the committed witness".to_vec();
    let b = b"right half".to_vec();
    let committed = sha256(&[a.clone(), b.clone()].concat());
    let lock = cat_sha256_checksig_lock(&pubkey, &committed);

    // Right witness: admitted (no tracker, the scripts-only walk).
    let right = spend_of(
        lock.clone(),
        witness_unlock(key.clone(), a.clone(), b.clone()),
        5_000,
        4_000,
        |_| {},
    )
    .await;
    let engine = engine(None);
    engine
        .submit(&tagged(&right), SubmitMode::CurrentTx)
        .await
        .expect("the committed witness satisfies the covenant");

    // Wrong witness (b' ≠ b): the signature is still valid (it does not
    // cover the unlocking script), so ONLY the CAT/SHA256 check refuses it.
    let wrong = spend_of(
        lock.clone(),
        witness_unlock(key.clone(), a.clone(), b"wrong half".to_vec()),
        5_000,
        4_000,
        |_| {},
    )
    .await;
    let err = engine
        .submit(&tagged(&wrong), SubmitMode::CurrentTx)
        .await
        .expect_err("a wrong witness is refused");
    let (input_index, reason) = expect_script_error(&err);
    assert_eq!(input_index, 0);
    assert!(
        reason.contains("truthy"),
        "the EQUAL left false on the stack: {reason}"
    );

    // Wrong key: CHECKSIGVERIFY refuses before the witness is even looked at.
    let stranger = spend_of(
        lock,
        witness_unlock(PrivateKey::random(), a, b),
        5_000,
        4_000,
        |_| {},
    )
    .await;
    let err = engine
        .submit(&tagged(&stranger), SubmitMode::CurrentTx)
        .await
        .expect_err("a signature by the wrong key is refused");
    expect_script_error(&err);
}

// ============================================================================
// The reference's value rule
// ============================================================================

/// A correctly signed P2PKH spend that pays out more than it consumes: every
/// script passes, and the reference's `outputTotal > inputTotal` rule refuses
/// it (as an SPV failure, exactly the reference's "Unable to verify SPV
/// information.").
#[tokio::test]
async fn an_unproven_spend_creating_satoshis_is_refused() {
    let fixture = p2pkh_spend(false, 20_000).await;
    let engine = engine(Some(tracker_knowing(&fixture.funding_txid)));
    let err = engine
        .submit(&tagged(&fixture), SubmitMode::CurrentTx)
        .await
        .expect_err("20 000 sats out of 10 000 in");
    assert!(matches!(err, EngineError::SpvError(_)), "got {err:?}");
    assert!(
        err.to_string()
            .contains("creates 20000 sats from 10000 sats"),
        "{err}"
    );
}

// ============================================================================
// The escape hatch
// ============================================================================

/// `with_script_verification(false)` restores the pre-2026-09-08 behavior:
/// no tracker ⇒ nothing is checked (the corrupted spend is admitted); a
/// tracker ⇒ BEEF structure + roots only (a wrong root is still refused, a
/// bad script is not).
#[tokio::test]
async fn the_escape_hatch_restores_the_structural_check() {
    let fixture = p2pkh_spend(true, 9_000).await;

    let no_tracker = EngineBuilder::new(Box::new(MemoryStorage::new()))
        .with_topic(TOPIC, Box::new(AdmitOutputZero))
        .with_script_verification(false)
        .build();
    assert!(!no_tracker.script_verification());
    no_tracker
        .submit(&tagged(&fixture), SubmitMode::CurrentTx)
        .await
        .expect("escape hatch + no tracker: admitted on structure alone, as before");

    let structural = EngineBuilder::new(Box::new(MemoryStorage::new()))
        .with_topic(TOPIC, Box::new(AdmitOutputZero))
        .with_chain_tracker(tracker_knowing(&fixture.funding_txid))
        .with_script_verification(false)
        .build();
    structural
        .submit(&tagged(&fixture), SubmitMode::CurrentTx)
        .await
        .expect("escape hatch + tracker: roots pass, the bad script is never run");

    let wrong_root = EngineBuilder::new(Box::new(MemoryStorage::new()))
        .with_topic(TOPIC, Box::new(AdmitOutputZero))
        .with_chain_tracker(tracker_knowing_nothing())
        .with_script_verification(false)
        .build();
    let err = wrong_root
        .submit(&tagged(&fixture), SubmitMode::CurrentTx)
        .await
        .expect_err("escape hatch + tracker: a wrong root is still refused");
    assert!(matches!(err, EngineError::SpvError(_)), "got {err:?}");
}

/// The default, with no builder call at all, is ON.
#[tokio::test]
async fn script_verification_is_on_by_default() {
    let fixture = p2pkh_spend(true, 9_000).await;
    let engine = EngineBuilder::new(Box::new(MemoryStorage::new()))
        .with_topic(TOPIC, Box::new(AdmitOutputZero))
        .build();
    assert!(engine.script_verification());
    let err = engine
        .submit(&tagged(&fixture), SubmitMode::CurrentTx)
        .await
        .expect_err("default engine refuses the corrupted spend");
    expect_script_error(&err);
}

// ============================================================================
// Cost: one large spend
// ============================================================================

/// `OP_SWAP <pubkey> OP_CHECKSIGVERIFY` then `rounds` × `OP_DUP OP_SHA256
/// OP_DROP` over the 20 KB witness, then `OP_SHA256 <sha256(blob)> OP_EQUAL`.
fn expensive_lock(pubkey: &[u8; 33], blob_hash: &[u8; 32], rounds: usize) -> LockingScript {
    let mut script = Script::new();
    script
        .write_opcode(OP_SWAP)
        .write_bin(pubkey)
        .write_opcode(OP_CHECKSIGVERIFY);
    for _ in 0..rounds {
        script
            .write_opcode(OP_DUP)
            .write_opcode(OP_SHA256)
            .write_opcode(OP_DROP);
    }
    script
        .write_opcode(OP_SHA256)
        .write_bin(blob_hash)
        .write_opcode(OP_EQUAL);
    LockingScript::from_script(script)
}

/// Push-only unlock `<sig> <blob>` for the lock above.
fn expensive_unlock(key: PrivateKey, blob: Vec<u8>) -> ScriptTemplateUnlock {
    ScriptTemplateUnlock::new(
        move |ctx: &SigningContext| {
            let scope = compute_sighash_scope(SignOutputs::All, false);
            let sig = key.sign(&ctx.compute_sighash(scope)?)?;
            let mut script = Script::new();
            script
                .write_bin(&TransactionSignature::new(sig, scope).to_checksig_format())
                .write_bin(&blob);
            Ok(UnlockingScript::from_script(script))
        },
        || 20_100,
    )
}

/// Measures (and prints) what one deliberately expensive spend costs to
/// verify: a ~7 KB locking script executing ~7000 opcodes (2330 rounds of
/// DUP/SHA256/DROP over a 20 KB witness, i.e. ~46 MB hashed) with a ~20 KB
/// push-only unlocking script. Run with `--nocapture` to read the numbers.
#[tokio::test]
async fn large_spend_verification_cost() {
    const ROUNDS: usize = 2_330;
    let key = PrivateKey::random();
    let pubkey = key.public_key().to_compressed();
    let blob: Vec<u8> = (0..20_000u32).map(|i| (i % 251) as u8).collect();
    let lock = expensive_lock(&pubkey, &sha256(&blob), ROUNDS);
    let lock_len = lock.to_binary().len();
    let fixture = spend_of(lock, expensive_unlock(key, blob), 10_000, 9_000, |_| {}).await;

    // The walk alone (what `submit` adds on top of parse + admission).
    let tx = Transaction::from_beef(&fixture.beef, Some(&fixture.subject_txid)).unwrap();
    let unlock_len = tx.inputs[0]
        .unlocking_script
        .as_ref()
        .unwrap()
        .to_binary()
        .len();
    let tracker = tracker_knowing(&fixture.funding_txid);
    let started = Instant::now();
    tx.verify(tracker.as_ref(), None)
        .await
        .expect("the expensive spend is valid");
    let verify_ms = started.elapsed().as_millis();

    // The whole submit (parse + verify + admission), with a chain tracker.
    let engine = engine(Some(tracker_knowing(&fixture.funding_txid)));
    let started = Instant::now();
    engine
        .submit(&tagged(&fixture), SubmitMode::CurrentTx)
        .await
        .expect("the expensive spend is admitted");
    let submit_ms = started.elapsed().as_millis();

    println!(
        "LARGE SPEND: locking script {lock_len} B, unlocking script {unlock_len} B, \
         ~{} opcodes executed: Transaction::verify {verify_ms} ms, full submit {submit_ms} ms \
         (native, {} build)",
        ROUNDS * 3 + 7,
        if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        }
    );
}

// ============================================================================
// Error rendering
// ============================================================================

#[test]
fn script_error_display_names_the_input_and_the_reason() {
    let err = EngineError::ScriptVerificationFailed {
        subject_txid: "ab".repeat(32),
        input_index: 3,
        reason: "The top stack element must be truthy after script evaluation.".into(),
    };
    let rendered = err.to_string();
    assert_eq!(
        rendered,
        format!(
            "script verification failed (subject {}): input 3: The top stack element must be truthy after script evaluation.",
            "ab".repeat(32)
        )
    );
}

/// Two inputs of one spend sourcing the SAME unproven parent (a head output
/// and that parent's own change): bsv-rs 0.3.20 links the parent for the
/// first input and leaves the second input's clone bare, and the verifier
/// popped the bare clone first ("Input 0 has no source transaction"). Found
/// live on zanaadu beta, 2026-09-08, on the first recase of a fresh name.
#[tokio::test]
async fn two_inputs_from_one_unproven_parent_verify() {
    let key = PrivateKey::random();
    let lock = P2PKH::new().lock(&key.public_key().hash160()).unwrap();
    // F (proven) -> M (unproven, two outputs) -> R spending M:0 AND M:1.
    let funding = proven_funding(lock.clone(), 100_000);
    let funding_txid = funding.id();
    let mut middle = Transaction::new();
    middle
        .add_input_from_tx(funding, 0, P2PKH::unlock(&key, SignOutputs::All, false))
        .unwrap();
    middle
        .outputs
        .push(TransactionOutput::new(40_000, lock.clone()));
    middle
        .outputs
        .push(TransactionOutput::new(50_000, lock.clone()));
    middle.sign().await.expect("middle signs");
    let mut subject = Transaction::new();
    subject
        .add_input_from_tx(
            middle.clone(),
            0,
            P2PKH::unlock(&key, SignOutputs::All, false),
        )
        .unwrap();
    subject
        .add_input_from_tx(
            middle.clone(),
            1,
            P2PKH::unlock(&key, SignOutputs::All, false),
        )
        .unwrap();
    subject.outputs.push(TransactionOutput::new(80_000, lock));
    subject.sign().await.expect("subject signs");
    let beef = subject
        .to_beef(false)
        .expect("BEEF with the unproven middle and the proven funding");
    // the BEEF carries the middle ONCE: the shape that tripped the walk
    assert_eq!(
        Beef::from_binary(&beef).unwrap().txs.len(),
        3,
        "funding, middle, subject"
    );

    // bsv-rs 0.3.22 links duplicate inputs as bare stubs (linear structure)
    // and its own verify walks by txid, so the bare SDK walk verifies this
    // BEEF too (0.3.20 failed it on the bare clone; 0.3.21 linked every clone
    // in full and was exponential on diamond chains, yanked). The engine keeps
    // `verify_beef_linear` regardless: it is the reference algorithm and
    // depends on no clone structure.
    let bare = Transaction::from_beef(&beef, None).unwrap();
    let bare_result = bare.verify(&*tracker_knowing(&funding_txid), None).await;
    assert!(
        matches!(bare_result, Ok(true)),
        "bsv-rs 0.3.22 verifies both inputs of one unproven parent: {bare_result:?}"
    );

    let storage = Rc::new(MemoryStorage::new());
    let engine = engine_with(Rc::clone(&storage), Some(tracker_knowing(&funding_txid)));
    let steak = engine
        .submit(
            &TaggedBEEF::new(beef, vec![TOPIC.into()]),
            SubmitMode::CurrentTx,
        )
        .await
        .expect("a complete two-level ancestry is admitted, every clone linked");
    assert_eq!(steak[TOPIC].outputs_to_admit, vec![0]);
    assert!(is_admitted(&storage, &subject.id()).await);
}

/// A DIAMOND chain: every level spends BOTH outputs of the previous unproven
/// level (a head output and its change, the shape of every second head spend
/// of a wallet), 24 levels deep and all unproven. A walker that links a clone
/// per occurrence materializes 2^24 subtrees and dies; the map-based walk is
/// linear. Found on beta 2026-09-08: a 12-deep unmined chain took the Worker
/// past its memory on the picture buy of the identity-market soak. The BEEF is
/// assembled by hand (`merge_raw_tx`) and each level signs against a SHALLOW
/// copy of its parent, so the test itself stays linear too.
#[tokio::test]
async fn a_deep_diamond_chain_of_unproven_spends_verifies_in_linear_time() {
    let key = PrivateKey::random();
    let lock = P2PKH::new().lock(&key.public_key().hash160()).unwrap();
    let funding = proven_funding(lock.clone(), 4_000_000);
    let funding_txid = funding.id();
    let funding_bump = funding
        .merkle_path
        .clone()
        .expect("proven funding carries a BUMP");

    let shallow = |tx: &Transaction| {
        let mut t = tx.clone();
        for input in &mut t.inputs {
            input.source_transaction = None;
        }
        t
    };
    let mut beef = Beef::new();
    let bump_index = beef.merge_bump(funding_bump);
    beef.merge_raw_tx(funding.to_binary(), Some(bump_index));

    let mut prev = shallow(&funding);
    let mut sats: u64 = 4_000_000;
    for level in 0..24u32 {
        let mut tx = Transaction::new();
        tx.add_input_from_tx(
            prev.clone(),
            0,
            P2PKH::unlock(&key, SignOutputs::All, false),
        )
        .unwrap();
        if level > 0 {
            tx.add_input_from_tx(
                prev.clone(),
                1,
                P2PKH::unlock(&key, SignOutputs::All, false),
            )
            .unwrap();
        }
        sats -= 1_000; // two outputs, a little less than the inputs (the value rule)
        tx.outputs
            .push(TransactionOutput::new(sats / 2, lock.clone()));
        tx.outputs
            .push(TransactionOutput::new(sats - sats / 2, lock.clone()));
        tx.sign().await.expect("level signs");
        beef.merge_raw_tx(tx.to_binary(), None);
        prev = shallow(&tx);
    }
    let subject_txid = prev.id();
    let beef_bytes = beef.to_binary();
    assert_eq!(
        Beef::from_binary(&beef_bytes).unwrap().txs.len(),
        25,
        "funding + 24 levels, each once"
    );

    let storage = Rc::new(MemoryStorage::new());
    let engine = engine_with(Rc::clone(&storage), Some(tracker_knowing(&funding_txid)));
    let t0 = std::time::Instant::now();
    let steak = engine
        .submit(
            &TaggedBEEF::new(beef_bytes, vec![TOPIC.into()]),
            SubmitMode::CurrentTx,
        )
        .await
        .expect("a 24-deep diamond of valid unproven spends is admitted");
    let elapsed = t0.elapsed();
    assert_eq!(steak[TOPIC].outputs_to_admit, vec![0]);
    assert!(is_admitted(&storage, &subject_txid).await);
    assert!(
        elapsed.as_secs() < 20,
        "the walk must be linear in the chain, not exponential: took {elapsed:?}"
    );
    println!("24-deep diamond verified in {elapsed:?}");
}
