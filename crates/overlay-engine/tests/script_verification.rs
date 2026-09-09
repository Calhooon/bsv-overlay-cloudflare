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

// ============================================================================
// (f): `verify_scripts_only` — the reference's 'scripts only' as an EXPLICIT
// ask at an admission door whose bar is the network (bsv-low W-A, #437 step
// 2, 2026-09-09). Only the interpreter's verdict is a refusal there; a proven
// ancestor is trusted as-is and the chain tracker is never consulted.
// ============================================================================

/// A chain tracker that PANICS when asked: 'scripts only' must never ask it.
struct MustNotAskTracker;

#[async_trait]
impl ChainTracker for MustNotAskTracker {
    async fn is_valid_root_for_height(
        &self,
        root: &str,
        height: u32,
    ) -> Result<bool, ChainTrackerError> {
        panic!("'scripts only' consulted the chain tracker (root {root} at height {height})")
    }
    async fn current_height(&self) -> Result<u32, ChainTrackerError> {
        Ok(HEIGHT + 10)
    }
}

#[tokio::test]
async fn scripts_only_refuses_the_corrupted_spend_with_the_interpreters_verdict() {
    let engine = engine(None);
    let corrupted = p2pkh_spend(true, 9_000).await;
    let err = engine
        .verify_scripts_only(&corrupted.beef, &corrupted.subject_txid)
        .await
        .expect_err("a corrupted signature is the interpreter's refusal");
    let (input_index, reason) = expect_script_error(&err);
    assert_eq!(input_index, 0);
    assert!(!reason.is_empty(), "the interpreter names its reason");

    let valid = p2pkh_spend(false, 9_000).await;
    engine
        .verify_scripts_only(&valid.beef, &valid.subject_txid)
        .await
        .expect("a valid spend passes the door");
}

#[tokio::test]
async fn scripts_only_trusts_a_proven_ancestor_without_asking_the_tracker() {
    let valid = p2pkh_spend(false, 9_000).await;
    // The FULL walk checks the fixture's fabricated root and a tracker that
    // knows nothing refuses it…
    let refusing = engine(Some(tracker_knowing_nothing()));
    let err = refusing
        .submit(&tagged(&valid), SubmitMode::CurrentTx)
        .await
        .expect_err("the full walk checks the root against the tracker");
    assert!(
        matches!(err, EngineError::SpvError(_)),
        "a wrong root is an SPV error under the full walk: {err}"
    );
    // …while 'scripts only' never asks: a tracker that panics when consulted
    // stays silent and the spend passes on its scripts alone.
    let never_asked = engine(Some(Box::new(MustNotAskTracker)));
    never_asked
        .verify_scripts_only(&valid.beef, &valid.subject_txid)
        .await
        .expect("roots are accepted unchecked under 'scripts only'");
}

#[tokio::test]
async fn scripts_only_is_independent_of_the_escape_hatch() {
    let corrupted = p2pkh_spend(true, 9_000).await;
    let mut engine = engine(None);
    engine.set_script_verification(false);
    // The hatch admits the corrupted spend on structure alone (`submit`)…
    engine
        .submit(&tagged(&corrupted), SubmitMode::CurrentTx)
        .await
        .expect("the escape hatch restores the structural check for submit");
    // …the explicit ask still executes the script and refuses it.
    let err = engine
        .verify_scripts_only(&corrupted.beef, &corrupted.subject_txid)
        .await
        .expect_err("the explicit ask executes regardless of the hatch");
    expect_script_error(&err);
}

#[tokio::test]
async fn scripts_only_still_applies_the_value_rule_as_a_structural_fault() {
    // 10 000 in, 11 000 out: the reference's value rule, NOT the interpreter's
    // verdict — a caller with the network behind it classifies it apart.
    let inflating = p2pkh_spend(false, 11_000).await;
    let engine = engine(None);
    let err = engine
        .verify_scripts_only(&inflating.beef, &inflating.subject_txid)
        .await
        .expect_err("an unproven spend may not create satoshis");
    assert!(
        matches!(
            &err,
            EngineError::ScriptWalkInconclusive {
                subject_judged: true,
                ..
            }
        ),
        "the value rule is structural (the subject's inputs all ran first), never a script fault: {err}"
    );
}

#[tokio::test]
async fn scripts_only_names_a_missing_source_structurally_never_as_a_script_fault() {
    // A proofless single-tx BEEF whose subject spends a source the BEEF does
    // not carry: structural (the caller completes sources or lets the network
    // judge), never the interpreter's verdict.
    let mut tx = Transaction::new();
    tx.inputs.push(TransactionInput {
        source_txid: Some("bb".repeat(32)),
        source_output_index: 0,
        unlocking_script: Some(UnlockingScript::from_script(Script::new())),
        ..Default::default()
    });
    let pay_to = PrivateKey::random().public_key().hash160();
    tx.outputs.push(TransactionOutput::new(
        1_000,
        P2PKH::new().lock(&pay_to).unwrap(),
    ));
    let subject_txid = tx.id();
    let mut beef = Beef::new();
    beef.merge_raw_tx(tx.to_binary(), None);
    let engine = engine(None);
    let err = engine
        .verify_scripts_only(&beef.to_binary(), &subject_txid)
        .await
        .expect_err("a source the BEEF does not carry cannot be executed");
    assert!(
        matches!(
            &err,
            EngineError::ScriptWalkInconclusive {
                subject_judged: false,
                ..
            }
        ),
        "a missing source on the SUBJECT is structural and leaves the subject UNJUDGED: {err}"
    );
}

#[tokio::test]
async fn scripts_only_runs_the_real_covenant_leg_and_refuses_the_tampered_preimage() {
    // The mainnet Poc5 tower-enforced settle, intact: passes the door on its
    // OP_PUSH_TX leg with NO tracker (the fabricated block proof is trusted).
    let intact = real_covenant_leg(
        ENFORCED_FUNDING_HEX,
        ENFORCED_FUNDING_TXID,
        ENFORCED_SETTLE_HEX,
        ENFORCED_SETTLE_TXID,
        None,
    );
    let engine = engine(Some(Box::new(MustNotAskTracker)));
    let started = Instant::now();
    engine
        .verify_scripts_only(&intact.beef, &intact.subject_txid)
        .await
        .expect("the real covenant settle satisfies its lock at the door");
    println!(
        "real Poc5 covenant settle {ENFORCED_SETTLE_TXID}: 'scripts only' at the door took {} ms",
        started.elapsed().as_millis()
    );
    // The same leg with one preimage bit flipped: the interpreter's verdict.
    let tampered = real_covenant_leg(
        ENFORCED_FUNDING_HEX,
        ENFORCED_FUNDING_TXID,
        ENFORCED_SETTLE_HEX,
        ENFORCED_SETTLE_TXID,
        Some(PREIMAGE_TAMPER_OFFSET),
    );
    let err = engine
        .verify_scripts_only(&tampered.beef, &tampered.subject_txid)
        .await
        .expect_err("a tampered preimage cannot satisfy the covenant at the door");
    let (input_index, _) = expect_script_error(&err);
    assert_eq!(input_index, 0);
}

/// A script that runs to completion and leaves FALSE (a CAT/SHA256 covenant
/// with a wrong witness; the signature still verifies) is the interpreter's
/// verdict exactly like a script that ERRORS (a bad signature): two fixture
/// shapes, one refusal class.
#[tokio::test]
async fn scripts_only_refuses_a_witness_that_evaluates_to_false_cleanly() {
    let key = PrivateKey::random();
    let pubkey = key.public_key().to_compressed();
    let a = b"left half of the committed witness".to_vec();
    let b = b"right half".to_vec();
    let committed = sha256(&[a.clone(), b.clone()].concat());
    let lock = cat_sha256_checksig_lock(&pubkey, &committed);
    let wrong = spend_of(
        lock,
        witness_unlock(key, a, b"wrong half".to_vec()),
        5_000,
        4_000,
        |_| {},
    )
    .await;
    let engine = engine(None);
    let err = engine
        .verify_scripts_only(&wrong.beef, &wrong.subject_txid)
        .await
        .expect_err("a wrong witness leaves FALSE on the stack");
    let (input_index, reason) = expect_script_error(&err);
    assert_eq!(input_index, 0);
    // bsv-rs 0.3.22 reports a clean FALSE as an interpreter ERROR ("The top
    // stack element must be truthy after script evaluation."), so this too
    // arrives through the `Err` arm; the engine's `Ok(false)` arm is a
    // type-completeness arm no fixture reaches with this interpreter. The
    // wording is the interpreter's own and is NOT pinned — the verdict is.
    assert!(!reason.is_empty(), "the interpreter names its reason");
}

// ============================================================================
// (g): the `make ci-route` lane fixtures (tools/lane-script) — the door's
// refusal, driven through the REAL wasm route. The bytes are PRODUCED here
// (fixed keys, so a regeneration is byte-identical) and committed; the lane
// cell asserts the door's refusal names THIS corrupted subject and that the
// valid sibling reaches the (fixture) network. Never retype them: regenerate
// with
//   cargo test -p bsv-overlay-engine --test script_verification \
//     emit_lane_script_fixtures -- --ignored --nocapture
// ============================================================================

/// A signed P2PKH spend of a proven funding output with FIXED keys (10 000
/// sats in, 9 000 out), optionally with one DER `r` byte flipped after
/// signing — the same shape as `p2pkh_spend`, made reproducible.
async fn lane_p2pkh_spend(corrupt_signature: bool) -> SpendFixture {
    let key = PrivateKey::from_hex(&"11".repeat(32)).expect("a fixed key");
    let pay_to = PrivateKey::from_hex(&"22".repeat(32))
        .expect("a fixed key")
        .public_key()
        .hash160();
    let lock = P2PKH::new().lock(&key.public_key().hash160()).unwrap();
    let funding = proven_funding(lock, 10_000);
    let funding_txid = funding.id();
    let mut tx = Transaction::new();
    tx.add_input_from_tx(funding, 0, P2PKH::unlock(&key, SignOutputs::All, false))
        .unwrap();
    tx.outputs.push(TransactionOutput::new(
        9_000,
        P2PKH::new().lock(&pay_to).unwrap(),
    ));
    tx.sign().await.expect("template signing");
    if corrupt_signature {
        flip_unlocking_byte(&mut tx, 10);
    }
    SpendFixture {
        beef: tx.to_beef(false).expect("BEEF with the proven parent"),
        funding_txid,
        subject_txid: tx.id(),
    }
}

#[tokio::test]
#[ignore = "writes the committed lane fixtures under tools/lane-script/fixtures — run on purpose"]
async fn emit_lane_script_fixtures() {
    let dir =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tools/lane-script/fixtures");
    std::fs::create_dir_all(&dir).unwrap();
    let valid = lane_p2pkh_spend(false).await;
    let corrupted = lane_p2pkh_spend(true).await;
    // Self-check before writing: the engine's own verdicts on the bytes.
    let engine = engine(None);
    engine
        .verify_scripts_only(&valid.beef, &valid.subject_txid)
        .await
        .expect("the valid fixture passes the door");
    let err = engine
        .verify_scripts_only(&corrupted.beef, &corrupted.subject_txid)
        .await
        .expect_err("the corrupted fixture is refused at the door");
    expect_script_error(&err);
    std::fs::write(dir.join("valid.beef.hex"), hex::encode(&valid.beef)).unwrap();
    std::fs::write(dir.join("corrupted.beef.hex"), hex::encode(&corrupted.beef)).unwrap();
    let raw_of = |beef_bytes: &[u8], txid: &str| -> String {
        let b = Beef::from_binary(beef_bytes).expect("the fixture parses");
        let btx = b.find_txid(txid).expect("the subject is in its BEEF");
        hex::encode(btx.tx().expect("a full tx").to_binary())
    };
    let manifest = serde_json::json!({
        "producer": "crates/overlay-engine/tests/script_verification.rs emit_lane_script_fixtures (fixed keys; regenerate, never retype)",
        "valid": { "file": "valid.beef.hex", "subject_txid": valid.subject_txid, "funding_txid": valid.funding_txid, "subject_raw_hex": raw_of(&valid.beef, &valid.subject_txid) },
        "corrupted": { "file": "corrupted.beef.hex", "subject_txid": corrupted.subject_txid, "funding_txid": corrupted.funding_txid, "subject_raw_hex": raw_of(&corrupted.beef, &corrupted.subject_txid), "defect": "one DER r byte of input 0's signature flipped after signing" },
    });
    std::fs::write(
        dir.join("manifest.json"),
        serde_json::to_string_pretty(&manifest).unwrap(),
    )
    .unwrap();
    println!(
        "lane fixtures written to {}: valid {} / corrupted {}",
        dir.display(),
        valid.subject_txid,
        corrupted.subject_txid
    );
}

// ============================================================================
// (h): the door's BUDGET and its STATS (bsv-low W-A gate MED-1 / MED-2 /
// LOW-1 / LOW-2, 2026-09-09). Script has no loops, so the work of an input is
// bounded from its bytes BEFORE anything runs; a breach is the DOOR's verdict
// (over budget: the network judges), never the interpreter's; the interpreter's
// memory limit tripping is the same class; and a structural fault names
// whether the SUBJECT was judged before it.
// ============================================================================

use bsv_overlay_engine::engine::{DoorBudget, WalkStats};

/// `<n × OP_SHA256> OP_DROP OP_TRUE`, unlocked by any push: VALID under the
/// reference walk, and its static work estimate is n × the element limit.
fn hash_heavy_lock(n: usize) -> LockingScript {
    let mut script = Script::new();
    for _ in 0..n {
        script.write_opcode(OP_SHA256);
    }
    script.write_opcode(OP_DROP).write_opcode(OP_TRUE);
    LockingScript::from_script(script)
}

fn push_unlock(bytes: Vec<u8>) -> ScriptTemplateUnlock {
    ScriptTemplateUnlock::new(
        move |_ctx: &SigningContext| {
            let mut script = Script::new();
            script.write_bin(&bytes);
            Ok(UnlockingScript::from_script(script))
        },
        || 40,
    )
}

#[tokio::test]
async fn door_stats_describe_the_real_covenant_leg() {
    let intact = real_covenant_leg(
        ENFORCED_FUNDING_HEX,
        ENFORCED_FUNDING_TXID,
        ENFORCED_SETTLE_HEX,
        ENFORCED_SETTLE_TXID,
        None,
    );
    let engine = engine(None);
    let stats: WalkStats = engine
        .verify_scripts_only(&intact.beef, &intact.subject_txid)
        .await
        .expect("the real covenant settle passes the door");
    assert_eq!(
        stats.unproven_txs, 1,
        "the settle alone is unproven (its funding is proven)"
    );
    assert_eq!(stats.inputs_executed, 1);
    assert!(
        stats.sig_ops >= 1,
        "OP_PUSH_TX checks at least one signature: {stats:?}"
    );
    assert!(
        stats.script_bytes > 3_000,
        "the covenant lock is ~3 KB: {stats:?}"
    );
    assert!(stats.subject_judged);
    assert!(
        stats.work_bytes < DoorBudget::DEFAULT.max_work_bytes / 8,
        "LOW's real covenant spend sits far inside the budget: {stats:?}"
    );
}

#[tokio::test]
async fn door_over_budget_is_inconclusive_never_a_refusal_and_the_reference_walk_is_untouched() {
    // 600 hash ops × the 128 KB element limit ≈ 77 MB of estimated work: over
    // the 64 MB budget from the BYTES alone, before anything executes.
    let n = (DoorBudget::DEFAULT.max_work_bytes / DoorBudget::DEFAULT.memory_limit as u64) as usize
        + 100;
    let heavy = spend_of(
        hash_heavy_lock(n),
        push_unlock(vec![0x42; 8]),
        5_000,
        4_000,
        |_| {},
    )
    .await;
    let engine = engine(None);
    let err = engine
        .verify_scripts_only(&heavy.beef, &heavy.subject_txid)
        .await
        .expect_err("over the door budget");
    assert!(
        matches!(
            &err,
            EngineError::ScriptWalkOverBudget {
                subject_judged: false,
                ..
            }
        ),
        "the door's own bound, never the interpreter's verdict: {err}"
    );
    // The reference walk (`submit`) has no budget: the same valid spend is admitted.
    let storage = Rc::new(MemoryStorage::new());
    let reference = engine_with(Rc::clone(&storage), None);
    reference
        .submit(&tagged(&heavy), SubmitMode::CurrentTx)
        .await
        .expect("the reference walk admits a valid spend whatever its cost");
    assert!(is_admitted(&storage, &heavy.subject_txid).await);
}

#[tokio::test]
async fn door_memory_limit_trip_is_the_doors_verdict_not_the_networks() {
    // `<12 × (OP_DUP OP_CAT)> OP_DROP OP_TRUE` on an 8 KB push doubles the
    // element to 32 MB: cheap by the static census (no hash ops), so it runs,
    // and the interpreter's 128 KB memory limit trips mid-way. That is the
    // DOOR's limit (the ts-sdk's `Spend` default is `Infinity`, bsv-rs's own
    // default 32 MB, the node's policy larger still), so it must read over
    // budget, never refused.
    let mut script = Script::new();
    for _ in 0..12 {
        script.write_opcode(OP_DUP).write_opcode(OP_CAT);
    }
    script.write_opcode(OP_DROP).write_opcode(OP_TRUE);
    let cat_lock = LockingScript::from_script(script);
    let fat = spend_of(
        cat_lock,
        push_unlock(vec![0x42; 8 * 1024]),
        5_000,
        4_000,
        |_| {},
    )
    .await;
    let engine = engine(None);
    let err = engine
        .verify_scripts_only(&fat.beef, &fat.subject_txid)
        .await
        .expect_err("the door's memory limit trips");
    assert!(
        matches!(
            &err,
            EngineError::ScriptWalkOverBudget {
                subject_judged: false,
                ..
            }
        ),
        "a memory-limit trip is the door's verdict: {err}"
    );
    if let EngineError::ScriptWalkOverBudget { what, .. } = &err {
        assert!(
            what.contains("memory usage has exceeded"),
            "names the interpreter's own reason: {what}"
        );
    }
}

#[tokio::test]
async fn door_names_a_judged_subject_apart_from_an_unjudged_one() {
    // funding (proven) → parent P (unproven) → child C (the subject). The BEEF
    // carries P and C but NOT the funding: C executes fully (P is its source),
    // then P's own source is missing — the subject WAS judged.
    let key = PrivateKey::random();
    let lock = P2PKH::new().lock(&key.public_key().hash160()).unwrap();
    let funding = proven_funding(lock.clone(), 10_000);
    let mut parent = Transaction::new();
    parent
        .add_input_from_tx(funding, 0, P2PKH::unlock(&key, SignOutputs::All, false))
        .unwrap();
    parent
        .outputs
        .push(TransactionOutput::new(9_000, lock.clone()));
    parent.sign().await.unwrap();
    let mut child = Transaction::new();
    child
        .add_input_from_tx(
            parent.clone(),
            0,
            P2PKH::unlock(&key, SignOutputs::All, false),
        )
        .unwrap();
    child.outputs.push(TransactionOutput::new(8_000, lock));
    child.sign().await.unwrap();
    let mut beef = Beef::new();
    beef.merge_raw_tx(parent.to_binary(), None);
    beef.merge_raw_tx(child.to_binary(), None);
    let engine = engine(None);
    let err = engine
        .verify_scripts_only(&beef.to_binary(), &child.id())
        .await
        .expect_err("the parent's source is absent");
    match &err {
        EngineError::ScriptWalkInconclusive {
            at_txid,
            subject_judged,
            ..
        } => {
            assert_eq!(at_txid, &parent.id(), "the fault is on the ANCESTOR");
            assert!(
                *subject_judged,
                "the subject's inputs all executed before the ancestor faulted: {err}"
            );
        }
        other => panic!("expected an inconclusive walk, got {other}"),
    }
}

/// The census weights a CHECKMULTISIG by its STATED key count (the Poc5
/// covenant states `OP_3`); a count the script COMPUTES is charged the most
/// keys the element limit admits.
#[tokio::test]
async fn door_census_weights_a_multisig_by_its_stated_key_count() {
    let intact = real_covenant_leg(
        ENFORCED_FUNDING_HEX,
        ENFORCED_FUNDING_TXID,
        ENFORCED_SETTLE_HEX,
        ENFORCED_SETTLE_TXID,
        None,
    );
    let engine = engine(None);
    let stats = engine
        .verify_scripts_only(&intact.beef, &intact.subject_txid)
        .await
        .expect("the real covenant settle passes the door");
    assert!(
        stats.sig_ops < 64,
        "the covenant's multisig is charged by its stated OP_3, not the maximum: {stats:?}"
    );
    // `OP_1 <pk> <pk> <pk> OP_1 OP_2 OP_ADD OP_CHECKMULTISIG`: the key count is
    // computed, not stated — charged the most keys the element limit admits.
    let key = PrivateKey::random();
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
    let computed_count_lock = LockingScript::from_script(script);
    let unlock = ScriptTemplateUnlock::new(
        {
            let key = key.clone();
            move |ctx: &SigningContext| {
                let scope = compute_sighash_scope(SignOutputs::All, false);
                let sig = key.sign(&ctx.compute_sighash(scope)?)?;
                let mut s = Script::new();
                s.write_opcode(OP_0)
                    .write_bin(&TransactionSignature::new(sig, scope).to_checksig_format());
                Ok(UnlockingScript::from_script(s))
            }
        },
        || 80,
    );
    let spend = spend_of(computed_count_lock, unlock, 5_000, 4_000, |_| {}).await;
    let outcome = engine
        .verify_scripts_only(&spend.beef, &spend.subject_txid)
        .await;
    let charged = match &outcome {
        Ok(stats) => stats.sig_ops,
        Err(EngineError::ScriptWalkOverBudget { .. }) => usize::MAX,
        Err(e) => panic!("unexpected: {e}"),
    };
    assert!(
        charged >= DoorBudget::DEFAULT.memory_limit / 33,
        "a computed key count is charged the maximum the element limit admits: {outcome:?}"
    );
}
