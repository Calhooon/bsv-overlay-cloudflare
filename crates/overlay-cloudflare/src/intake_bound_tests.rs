//! Lane INTAKE-BOUND: the Worker's doors read the proof limb
//! (`DoorBudget::max_proof_nodes`, `WORKER_WALK_BUDGET`) from the frame before
//! anything of a body is parsed. No native harness drives `/submit`, so the
//! route's order is pinned by its source, as the house does; its answer, the
//! queue's read, the reason and the counters are run.
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

/// A small valid body: a proven transaction (one-node BUMP).
fn small_body() -> Vec<u8> {
    let mut tx = bsv_rs::transaction::Transaction::new();
    tx.inputs.push(bsv_rs::transaction::TransactionInput {
        source_txid: Some("aa".repeat(32)),
        source_output_index: 0,
        unlocking_script: Some(bsv_rs::script::UnlockingScript::from_script(
            bsv_rs::script::Script::new(),
        )),
        ..Default::default()
    });
    tx.outputs.push(bsv_rs::transaction::TransactionOutput::new(
        1,
        bsv_rs::script::LockingScript::from_script(
            bsv_rs::script::Script::from_binary(&[0x51]).unwrap(),
        ),
    ));
    let id = tx.id();
    tx.merkle_path = Some(
        bsv_rs::transaction::MerklePath::new(
            900_000,
            vec![vec![bsv_rs::transaction::MerklePathLeaf::new_txid(0, id)]],
        )
        .unwrap(),
    );
    tx.to_beef(false).unwrap()
}

fn source(file: &str) -> String {
    std::fs::read_to_string(format!("{}/src/{file}", env!("CARGO_MANIFEST_DIR"))).unwrap()
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
/// `over_proof`), a small body is not; the answer's body is frozen.
#[test]
fn ib_w_the_routes_stop_and_its_answer() {
    let stop = crate::routes::intake_proof_stop(&one_past_the_default()).expect("stopped");
    assert_eq!(stop.limb, WalkLimb::OverProof);
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
            "message": "the body was not read: W; nothing was broadcast, admitted or queued",
        })
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
}

/// The route's ORDER, by its source (no native harness drives `/submit`):
/// in `submit_parts` the stop stands after the operator refusal is decided
/// (it reads no body) and BEFORE every parse of the body on either arm (the
/// census, `ef::missing_source_txids`, the subject's log parse, the EF
/// conversion, the door, the engine) and before any broadcast or enqueue; it
/// returns, and `walk_not_now` sets no `Retry-After` for the limb.
#[test]
fn ib_w_the_route_stops_before_any_parse() {
    let src = source("routes.rs");
    let parts = src.find("async fn submit_parts(").unwrap();
    let body = &src[parts..];
    let at = |needle: &str| {
        body.find(needle)
            .unwrap_or_else(|| panic!("submit_parts holds {needle}"))
    };
    let stop = at("intake_proof_stop(&tagged_beef.beef)");
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
    assert!(arm.contains("return json_response(&proof_limb_body(&stop), 503);"), "{arm}");
    let not_now = src.find("async fn walk_not_now(").unwrap();
    assert!(src[not_now..not_now + 1400].contains("if !stop.limb.same_bytes_will_fail()"));
}

/// The queue's read: an inline body and a keyed object past the limb are
/// `OverProof` before their parse, and the consumer notes them `not_now`.
#[test]
fn ib_w_the_queue_reads_the_limb_before_its_parse() {
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    let past = one_past_the_default();
    // inline
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
    struct Port;
    impl crate::queue::ReplayBytes for Port {
        async fn read_beef(
            &self,
            r: &crate::queue::BeefRef,
        ) -> Result<Vec<u8>, crate::queue::BlobFault> {
            crate::queue::replay_object(r, Some(one_past_the_default()))
        }
        async fn judge_missing(
            &self,
            _: &[String],
            _: &crate::queue::BeefRef,
        ) -> crate::queue::MissingVerdict {
            crate::queue::MissingVerdict::Twin
        }
    }
    let read = |m: &crate::queue::MutationMessage| {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(crate::queue::read_for_replay(&Port, m))
    };
    let crate::queue::ReadStep::OverProof { fault } = read(&m) else {
        panic!("the inline body is stopped before its parse")
    };
    assert!(fault.contains("(the proof limb;"), "{fault}");
    // keyed
    let sha = hex::encode(bsv_rs::primitives::hash::sha256(&past));
    m.beef_b64 = String::new();
    m.r2 = Some(crate::queue::BeefRef {
        key: crate::queue::r2_key(&sha, &m.topics, &m.mode),
        sha256: sha,
        bytes: past.len() as u64,
        txid: None,
    });
    assert!(matches!(read(&m), crate::queue::ReadStep::OverProof { .. }));
    // the decode the lever and the stored-rows reader share
    assert!(crate::queue::decode_replay_beef(&STANDARD.encode(&past))
        .unwrap_err()
        .contains("(the proof limb;"));
    // a small body is read as before
    m.r2 = None;
    m.beef_b64 = STANDARD.encode(small_body());
    assert!(matches!(read(&m), crate::queue::ReadStep::Bytes(_)));
    // the consumer's arm: counted, noted `not_now`, handed back
    let lib = source("lib.rs");
    let arm = lib
        .find("crate::queue::ReadStep::OverProof { fault } => {")
        .unwrap();
    let arm = &lib[arm..arm + 1400];
    for needle in [
        "COUNTER_QUEUE_REPLAY_OVER_PROOF",
        "crate::queue::replay_error_class(&e)",
        "msg.retry();",
    ] {
        assert!(arm.contains(needle), "{needle}");
    }
    assert_eq!(
        crate::queue::replay_error_class(&crate::queue::over_proof_error("w")),
        crate::dead_letters::LetterClass::NotNow
    );
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
    let seed = ops.find("// lane INTAKE-BOUND: the proof limb at each door, served from 0").unwrap();
    for name in [
        "COUNTER_SUBMIT_ENGINE_WALK_OVER_PROOF,",
        "COUNTER_SUBMIT_SCRIPT_WALK_OVER_PROOF,",
        "COUNTER_SUBMIT_INTAKE_OVER_PROOF,",
        "COUNTER_QUEUE_REPLAY_OVER_PROOF,",
    ] {
        assert!(ops[seed..seed + 400].contains(name), "{name} served from 0");
    }
}

/// The mainnet fan-out reads no tracker answer past the limb.
#[test]
fn ib_w_the_fanout_reads_no_advert_past_the_limb() {
    assert!(crate::mainnet_fanout::read_advert(&one_past_the_default(), 0, "tm_a").is_none());
    let src = source("mainnet_fanout.rs");
    let f = src.find("pub fn read_advert(").unwrap();
    let limb = src[f..].find("proof_limb(beef)").unwrap();
    let parse = src[f..].find("parse_beef(beef").unwrap();
    assert!(limb < parse);
}

/// The peer crawler's expiry read of a peer's answer reads the limb before its parse.
#[test]
fn ib_w_the_crawlers_expiry_read_takes_the_limb_first() {
    let src = source("peer_crawler.rs");
    let f = src.find("fn expired_answer(").unwrap();
    let limb = src[f..].find("WORKER_WALK_BUDGET.proof_limb(&entry.beef)").unwrap();
    let parse = src[f..].find("subject_from_beef(").unwrap();
    assert!(limb < parse);
}
