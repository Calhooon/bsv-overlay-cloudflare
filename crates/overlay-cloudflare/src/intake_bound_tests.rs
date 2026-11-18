//! Lane INTAKE-BOUND: the Worker's doors read the proof limb
//! (`DoorBudget::max_proof_nodes`, `WORKER_WALK_BUDGET`) from the frame before
//! anything of a body is parsed. No native harness drives `/submit`, so the
//! route's order is pinned by its source, as the house does; its decision (one
//! pure function), its answer, the queue's read, the reason and the counters
//! are run. The fold (lane INTAKE-BOUND-FOLD): every stop is run over a VALID
//! body past the limb (a proven transaction whose own BUMP carries 65,538
//! nodes), so a stop made inert with its text kept is RED.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use overlay_engine::beef_limits::LINK_ANCESTRY_PROOF_NODES_MAX;
use overlay_engine::engine::{EngineError, WalkCouldNotRun, WalkLimb};

/// A frame one node past the default (a V1 BEEF of one BUMP of
/// `LINK_ANCESTRY_PROOF_NODES_MAX + 1` level-0 nodes), built by hand.
fn one_past_the_default() -> Vec<u8> {
    let n = LINK_ANCESTRY_PROOF_NODES_MAX + 1;
    let mut out = Vec::with_capacity(36 * n as usize + 64);
    out.extend(bsv_rs::transaction::BEEF_V1.to_le_bytes());
    out.push(1);
    out.extend([0xfe, 0xa0, 0xbb, 0x0d, 0x00]);
    out.push(17);
    out.push(0xfe);
    out.extend((n as u32).to_le_bytes());
    for offset in 0..n {
        out.extend(bsv_rs::primitives::to_var_int(offset));
        out.push(0);
        out.extend([7u8; 32]);
    }
    out.extend([0u8; 16]);
    out.push(0);
    out
}

/// A transaction spending a coin nobody holds, paying `script`.
fn tx_paying(script: bsv_rs::script::LockingScript, tag: u32) -> bsv_rs::transaction::Transaction {
    let mut tx = bsv_rs::transaction::Transaction::new();
    tx.inputs.push(bsv_rs::transaction::TransactionInput {
        source_txid: Some(format!("{:064x}", u64::from(tag) + 0xaa)),
        source_output_index: 0,
        unlocking_script: Some(bsv_rs::script::UnlockingScript::from_script(
            bsv_rs::script::Script::new(),
        )),
        ..Default::default()
    });
    tx.outputs
        .push(bsv_rs::transaction::TransactionOutput::new(1, script));
    tx.lock_time = tag;
    tx
}

fn op_1() -> bsv_rs::script::LockingScript {
    bsv_rs::script::LockingScript::from_script(
        bsv_rs::script::Script::from_binary(&[0x51]).unwrap(),
    )
}

/// `tx` proven by a VALID BUMP of `level0` level-0 leaves (`level0` even or
/// 1), a duplicate at each higher odd width; its BEEF.
fn proven_beef(mut tx: bsv_rs::transaction::Transaction, level0: u64) -> Vec<u8> {
    use bsv_rs::transaction::{MerklePath, MerklePathLeaf};
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
    if path.len() > 1 {
        path.pop();
    }
    tx.merkle_path = Some(MerklePath::new(900_000, path).unwrap());
    tx.to_beef(false).unwrap()
}

/// A VALID body past the Worker's limb: a proven transaction whose own BUMP
/// carries 65,538 nodes.
fn valid_past(tag: u32) -> Vec<u8> {
    let beef = proven_beef(tx_paying(op_1(), tag), LINK_ANCESTRY_PROOF_NODES_MAX + 2);
    assert!(crate::WORKER_WALK_BUDGET.proof_limb(&beef).is_err());
    assert!(
        overlay_engine::beef_limits::read_beef(&beef).is_ok(),
        "valid"
    );
    beef
}

/// A small valid body: a proven transaction (one-node BUMP).
fn small_body() -> Vec<u8> {
    proven_beef(tx_paying(op_1(), 1), 1)
}

fn source(file: &str) -> String {
    std::fs::read_to_string(format!("{}/src/{file}", env!("CARGO_MANIFEST_DIR"))).unwrap()
}

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(f)
}

/// The Worker's limb is the engine's default (the link estimate's own figure).
#[test]
fn ib_w_the_workers_limb_is_the_default() {
    assert_eq!(
        crate::WORKER_WALK_BUDGET.max_proof_nodes,
        LINK_ANCESTRY_PROOF_NODES_MAX
    );
}

/// The route's stop: one node past the default is stopped (limb
/// `over_proof`), a small body is not; the answer's body and status are
/// frozen (413, the fold's ruling 4; it was 503 on `69cee436`).
#[test]
fn ib_w_the_routes_stop_and_its_answer() {
    let stop = crate::routes::intake_proof_stop(&one_past_the_default()).expect("stopped");
    assert_eq!(stop.limb, WalkLimb::OverProof);
    assert!(
        stop.what.contains("the subject proven after its block"),
        "{}",
        stop.what
    );
    assert!(crate::routes::intake_proof_stop(&small_body()).is_none());
    let frozen = WalkCouldNotRun {
        subject_txid: String::new(),
        at_txid: String::new(),
        subject_judged: false,
        limb: WalkLimb::OverProof,
        what: "W".into(),
    };
    assert_eq!(
        crate::routes::proof_limb_body(&frozen),
        serde_json::json!({
            "status": "error",
            "code": "walk-could-not-run",
            "limb": "over_proof",
            "retryable": false,
            "sameBytesWillFail": true,
            "message": "the body was not read: W; nothing was broadcast, admitted or queued, so a sender who needs \
                        this body broadcast takes it to a broadcaster",
        })
    );
    assert_eq!(crate::routes::PROOF_LIMB_STATUS, 413);
    assert_eq!(crate::routes::walk_could_not_run_status(&frozen), 413);
    assert_eq!(
        crate::routes::engine_error_status(&EngineError::WalkCouldNotRun(frozen.clone())),
        413
    );
    // The engine's stop reaches the same answer at the walking doors (/submit's
    // ungated arm, /admin/readmit), never "after the block".
    assert_eq!(
        crate::routes::walk_not_now_body(&frozen),
        crate::routes::proof_limb_body(&frozen)
    );
    let other = WalkCouldNotRun {
        limb: WalkLimb::OverMemory,
        ..frozen
    };
    assert_eq!(crate::routes::walk_not_now_body(&other)["retryable"], true);
    assert_eq!(crate::routes::walk_could_not_run_status(&other), 503);
    assert_eq!(
        crate::routes::engine_error_status(&EngineError::WalkCouldNotRun(other)),
        503
    );
}

/// M3: the route's entry decision is ONE pure function (`intake_proof_answer`)
/// run over a VALID body past the limb: every action that proceeds is stopped,
/// a caller the operator check refuses is not (its refusal answers first and
/// reads no body), a small body is not. RED with the stop made inert, and with
/// the operator guard inverted.
#[test]
fn ibf_w_m3_the_route_entry_decides_by_one_function() {
    use crate::submit_gate::{AdmissionPath, AdmitPolicy, ScriptPolicy, SubmitAction};
    let past = valid_past(2);
    let proceed = [
        SubmitAction::ProceedWithNetworkGate {
            path: AdmissionPath::NetworkGated,
            scripts: ScriptPolicy::Execute,
            admit: AdmitPolicy::Fast,
        },
        SubmitAction::ProceedWithoutGate {
            path: AdmissionPath::HistoricalSpv,
            lenient_unbarred: false,
        },
        SubmitAction::ProceedWithoutGate {
            path: AdmissionPath::HistoricalUngated,
            lenient_unbarred: false,
        },
    ];
    for action in &proceed {
        let stop = crate::routes::intake_proof_answer(action, &past)
            .unwrap_or_else(|| panic!("{action:?}: a valid body past the limb is stopped"));
        assert_eq!(stop.limb, WalkLimb::OverProof);
        assert!(crate::routes::intake_proof_answer(action, &small_body()).is_none());
    }
    let refused = SubmitAction::RefuseUnauthenticated(AdmissionPath::CurrentTx);
    assert!(crate::routes::intake_proof_answer(&refused, &past).is_none());
}

/// The route's ORDER, by its source (no native harness drives `/submit`):
/// in `submit_parts` the decision stands after the operator refusal is decided
/// (it reads no body) and BEFORE every parse of the body on either arm (the
/// census, `ef::missing_source_txids`, the subject's log parse, the EF
/// conversion, the door, the engine) and before any broadcast or enqueue; it
/// returns 413, and `walk_not_now` answers the limb's status with no
/// `Retry-After`.
#[test]
fn ib_w_the_route_stops_before_any_parse() {
    let src = source("routes.rs");
    let parts = src.find("async fn submit_parts(").unwrap();
    let body = &src[parts..];
    let at = |needle: &str| {
        body.find(needle)
            .unwrap_or_else(|| panic!("submit_parts holds {needle}"))
    };
    let stop = at("intake_proof_answer(&action, &tagged_beef.beef)");
    assert!(at("let action = crate::submit_gate::action_for(") < stop);
    for later in [
        "crate::submit_census::census_verdict(&tagged_beef.beef)",
        "ef::missing_source_txids(",
        "ef::beef_to_ef_batch(",
        ".verify_scripts_only(",
        "broadcast_efs_gated(",
        ".submit_with_report(",
        "enqueue_replay(",
    ] {
        assert!(stop < at(later), "the stop precedes {later}");
    }
    let arm = &body[stop..stop + 900];
    assert!(arm.contains("SubmitReason::WalkOverProof"), "{arm}");
    assert!(arm.contains("COUNTER_SUBMIT_INTAKE_OVER_PROOF"), "{arm}");
    assert!(
        arm.contains("return json_response(&proof_limb_body(&stop), PROOF_LIMB_STATUS);"),
        "{arm}"
    );
    let not_now = src.find("async fn walk_not_now(").unwrap();
    let not_now = &src[not_now..not_now + 1400];
    assert!(not_now.contains("let status = walk_could_not_run_status(stop);"));
    assert!(not_now.contains("json_response(&walk_not_now_body(stop), status)"));
    assert!(not_now.contains("if !stop.limb.same_bytes_will_fail()"));
}

/// L1: a deferred job whose run is answered the limb's 413 SETTLES (its bytes
/// cannot pass at any time); a 503 is run again. RED with the limb's status
/// 503 (the job re-runs to its attempts).
#[test]
fn ibf_w_l1_a_deferred_job_past_the_limb_settles() {
    assert_eq!(
        crate::ef_deferred::settle(crate::routes::PROOF_LIMB_STATUS),
        crate::ef_deferred::Settle::Done
    );
    assert_eq!(
        crate::ef_deferred::settle(503),
        crate::ef_deferred::Settle::Again
    );
}

/// The consumer's port over one object.
struct Port(Vec<u8>);
impl crate::queue::ReplayBytes for Port {
    async fn read_beef(
        &self,
        r: &crate::queue::BeefRef,
        limbed: bool,
    ) -> Result<Vec<u8>, crate::queue::BlobFault> {
        crate::queue::replay_object(r, Some(self.0.clone()), limbed)
    }
    async fn judge_missing(
        &self,
        _: &[String],
        _: &crate::queue::BeefRef,
    ) -> crate::queue::MissingVerdict {
        crate::queue::MissingVerdict::Twin
    }
}

/// The queue's read (the fold's M1, ruling 2): a message WITHOUT the gated
/// mark whose body (inline, or a keyed object) is past the limb is `OverProof`
/// before its parse, and the consumer notes it `not_now` (`over_proof_failing`,
/// the one function its arm calls); a message WITH the mark is read through to
/// the engine (which goes on under `NetworkAccepted`). RED with `69cee436`'s rule grafted (the
/// marked message stopped), with the mark not read, and with the arm's class
/// `Fault`.
#[test]
fn ib_w_the_queue_reads_the_limb_before_its_parse() {
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    let past = valid_past(3);
    let port = Port(past.clone());
    let mut m = crate::queue::MutationMessage {
        beef_b64: STANDARD.encode(&past),
        r2: None,
        topics: vec!["tm_a".into()],
        mode: "historical-tx".into(),
        reason: String::new(),
        redrive: None,
        ef_job: None,
        gated: false,
    };
    let sha = hex::encode(bsv_rs::primitives::hash::sha256(&past));
    let keyed = crate::queue::BeefRef {
        key: crate::queue::r2_key(&sha, &m.topics, &m.mode),
        sha256: sha,
        bytes: past.len() as u64,
        txid: None,
    };
    for carried_by_key in [false, true] {
        m.r2 = carried_by_key.then(|| keyed.clone());
        m.gated = false;
        assert!(crate::queue::replay_reads_the_limb(&m));
        let crate::queue::ReadStep::OverProof { fault } =
            block_on(crate::queue::read_for_replay(&port, &m))
        else {
            panic!("keyed {carried_by_key}: an unmarked body is stopped before its parse")
        };
        assert!(fault.contains("(the proof limb;"), "{fault}");
        let (note, class) = crate::queue::over_proof_failing(&fault);
        assert_eq!(class, crate::dead_letters::LetterClass::NotNow);
        assert!(note.contains("over_proof"), "{note}");
        m.gated = true;
        assert!(!crate::queue::replay_reads_the_limb(&m));
        assert_eq!(
            crate::queue::replay_breach_policy(&m),
            overlay_engine::engine::WalkBreachPolicy::NetworkAccepted
        );
        match block_on(crate::queue::read_for_replay(&port, &m)) {
            crate::queue::ReadStep::Bytes(b) => assert_eq!(b, past),
            other => panic!("keyed {carried_by_key}: a marked body goes to the engine: {other:?}"),
        }
    }
    // The shared decode (the dead letters' subject read and the lever's) reads
    // no limb: as on `main` (the fold's M2).
    assert_eq!(
        crate::queue::decode_replay_beef(&STANDARD.encode(&past)).unwrap(),
        past
    );
    // a small body is read as before
    m.r2 = None;
    m.gated = false;
    m.beef_b64 = STANDARD.encode(small_body());
    assert!(matches!(
        block_on(crate::queue::read_for_replay(&port, &m)),
        crate::queue::ReadStep::Bytes(_)
    ));
    // the consumer's arm: counted, noted by the one function, handed back
    let lib = source("lib.rs");
    let arm = lib
        .find("crate::queue::ReadStep::OverProof { fault } => {")
        .unwrap();
    let end = lib[arm..].find("msg.retry();").unwrap() + arm + "msg.retry();".len();
    let arm = &lib[arm..end];
    for needle in [
        "COUNTER_QUEUE_REPLAY_OVER_PROOF",
        "crate::queue::over_proof_failing(&fault)",
        "note_failing(db, body, None, &note, class)",
        "msg.retry();",
    ] {
        assert!(arm.contains(needle), "{needle}");
    }
}

/// M1 (ruling 2): the gated arm never MARKS bytes past the limb, keys them or
/// puts them at rest as a job: the check is made where it enqueues and where
/// it puts a job at rest (`gated_bytes_within_the_limb`), and what it
/// enqueues is the request's body (which the entry read) or that body with
/// raw sources merged (`merge_raw_sources`: no merkle path added) or its
/// subject's bump stripped (fewer nodes).
#[test]
fn ibf_w_m1_the_gated_arm_never_marks_bytes_past_the_limb() {
    let past = valid_past(4);
    let e = crate::queue::gated_bytes_within_the_limb(&past).expect_err("refused");
    assert!(e.contains("(the proof limb;"), "{e}");
    assert!(crate::queue::gated_bytes_within_the_limb(&small_body()).is_ok());
    let queue = source("queue.rs");
    let f = queue.find("pub async fn enqueue_replay(").unwrap();
    assert!(queue[f..f + 600].contains("if gated {\n        gated_bytes_within_the_limb(beef)?;"));
    let ef = source("ef_deferred.rs");
    let f = ef.find("async fn put_at_rest(").unwrap();
    assert!(ef[f..f + 800].contains("crate::queue::gated_bytes_within_the_limb(&tagged.beef)?;"));
    // Completion adds no node; the strip removes the subject's.
    let nodes = |b: &[u8]| {
        crate::WORKER_WALK_BUDGET
            .proof_limb(b)
            .map(|f| f.nodes)
            .unwrap_or(u64::MAX)
    };
    let wide = proven_beef(tx_paying(op_1(), 5), 40);
    let parent = tx_paying(op_1(), 6);
    let merged = crate::ef::merge_raw_sources(&wide, &[(parent.id(), parent.to_hex())]);
    assert_ne!(merged, wide, "a source was merged");
    assert_eq!(nodes(&merged), nodes(&wide));
    let subject = overlay_engine::beef_limits::read_beef(&wide)
        .unwrap()
        .last_txid
        .unwrap();
    if let Some(stripped) = crate::ef::strip_subject_bump(&wide, &subject) {
        assert!(nodes(&stripped) < nodes(&wide));
    }
}

/// The reason class and the counters, served from 0.
#[test]
fn ib_w_the_reason_and_the_counters() {
    let stop = WalkCouldNotRun {
        subject_txid: String::new(),
        at_txid: String::new(),
        subject_judged: false,
        limb: WalkLimb::OverProof,
        what: String::new(),
    };
    assert_eq!(
        crate::submit_counters::Reason::engine(&EngineError::WalkCouldNotRun(stop.clone())),
        crate::submit_counters::Reason::WalkOverProof
    );
    assert_eq!(
        crate::submit_counters::Reason::engine(&EngineError::WalkCouldNotRun(WalkCouldNotRun {
            limb: WalkLimb::OverMemory,
            ..stop
        })),
        crate::submit_counters::Reason::WalkCouldNotRun
    );
    assert!(crate::submit_counters::all_names()
        .contains(&"submit_answer_reason_walk_over_proof_total".to_string()));
    assert_eq!(
        crate::ops::engine_walk_over_counter(WalkLimb::OverProof),
        crate::ops::COUNTER_SUBMIT_ENGINE_WALK_OVER_PROOF
    );
    assert_eq!(
        crate::ops::script_walk_over_counter(overlay_engine::engine::DoorLimb::Proof),
        crate::ops::COUNTER_SUBMIT_SCRIPT_WALK_OVER_PROOF
    );
    let ops = source("ops.rs");
    let seed = ops
        .find("// lane INTAKE-BOUND: the proof limb at each door, served from 0")
        .unwrap();
    for name in [
        "COUNTER_SUBMIT_ENGINE_WALK_OVER_PROOF,",
        "COUNTER_SUBMIT_SCRIPT_WALK_OVER_PROOF,",
        "COUNTER_SUBMIT_INTAKE_OVER_PROOF,",
        "COUNTER_QUEUE_REPLAY_OVER_PROOF,",
    ] {
        assert!(ops[seed..seed + 400].contains(name), "{name} served from 0");
    }
    // The ungated engine stop is counted by the engine's reason (walk_over_proof for this limb).
    let routes = source("routes.rs");
    assert!(routes.contains(
        "counters.reason(SubmitReason::engine(&EngineError::WalkCouldNotRun(stop.clone())));"
    ));
}

/// M3: the mainnet fan-out reads no tracker answer past the limb: a VALID,
/// admitted advert whose subject is proven by 65,538 nodes is not read; the
/// same advert proven by two nodes is. RED with the stop made inert.
#[test]
fn ib_w_the_fanout_reads_no_advert_past_the_limb() {
    use overlay_engine::types::Protocol;
    let key = bsv_rs::primitives::PrivateKey::random();
    let advert = || {
        tx_paying(
            bsv_rs::overlay::create_signed_overlay_admin_token(
                &key,
                Protocol::Ship,
                "https://past.example",
                "tm_a",
            ),
            7,
        )
    };
    let past = proven_beef(advert(), LINK_ANCESTRY_PROOF_NODES_MAX + 2);
    assert!(crate::WORKER_WALK_BUDGET.proof_limb(&past).is_err());
    assert!(crate::mainnet_fanout::read_advert(&past, 0, "tm_a").is_none());
    assert!(crate::mainnet_fanout::read_advert(&proven_beef(advert(), 2), 0, "tm_a").is_some());
}

/// M3: the peer crawler's expiry read of a peer's answer reads no answer past
/// the limb: a VALID body past it is the entry's error; a small one is read.
/// RED with the stop made inert (`Ok(0)`).
#[test]
fn ib_w_the_crawlers_expiry_read_takes_the_limb_first() {
    let entry = |beef: Vec<u8>| crate::peer_crawler::OutputEntry { beef };
    let e = crate::peer_crawler::expired_answer(&entry(valid_past(8)), "tm_uhrp", 0)
        .expect_err("not read");
    assert!(e.starts_with("crawl BEEF not read:"), "{e}");
    assert_eq!(
        crate::peer_crawler::expired_answer(&entry(small_body()), "tm_uhrp", 0),
        Ok(0)
    );
}
