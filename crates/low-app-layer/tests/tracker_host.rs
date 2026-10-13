use bsv_rs::transaction::MerklePath;
use bsv_tracker::{
    Evidence, HeaderError, Headers, HintSource, HostAction, Input, Params, Proof, ProofFetcher,
    Reask, State, Word,
};
use low_app_layer::tracker_host::*;
use serde_json::json;
use std::collections::{BTreeMap, VecDeque};

fn snapshot() -> HeaderSnapshot {
    HeaderSnapshot {
        tip: Ok(10),
        headers: BTreeMap::from([(
            10,
            header_response(
                10,
                200,
                &json!({"status":"success", "value":{"height":10,"hash":"02".repeat(32),"merkleRoot":"01".repeat(32)}}),
            ),
        )]),
    }
}
#[test]
fn ns3_headers_fake_service_fails_closed_on_missing_behind_and_fault() {
    let proof = || {
        Proof::new(
            "01".repeat(32),
            MerklePath::from_coinbase_txid(&"01".repeat(32), 10),
        )
        .unwrap()
    };
    assert_eq!(snapshot().check(proof()).unwrap().height(), 10);
    assert_eq!(header_response(10, 404, &json!(null)).unwrap(), None);
    assert!(header_response(10, 500, &json!({})).is_err());
    assert!(header_response(11, 200, &json!({"status":"success","value":{"height":10}})).is_err());
    let mut h = snapshot();
    h.tip = Ok(9);
    assert!(h.check(proof()).is_err());
    h.tip = Err(HeaderError("transport".into()));
    assert!(h.check(proof()).is_err());
}
#[test]
fn ns3_proof_fetcher_fake_couriers_are_inputs_checked_by_tracker() {
    let txid = "01".repeat(32);
    let p = Proof::new(&txid, MerklePath::from_coinbase_txid(&txid, 10)).unwrap();
    let mut f = CourierProofs(VecDeque::from([
        Err("arcade transport".into()),
        Ok(None),
        Ok(Some(p)),
    ]));
    assert!(f.fetch(&txid, &Reask::Age).is_err());
    assert!(f.fetch(&txid, &Reask::Age).unwrap().is_none());
    let mut state = State::new(&txid);
    state
        .step(
            &Params { age_threshold: 60 },
            &snapshot(),
            Input::Evidence(Evidence::Proof(
                f.fetch(&txid, &Reask::Age).unwrap().unwrap(),
            )),
        )
        .unwrap();
    assert!(matches!(state.word(), Word::Mined(_)));
    assert!(tsc_proof(&txid, 10, br#"{"index":0,"txOrId":"wrong","nodes":["*"]}"#).is_err());
}
#[test]
fn ns3_hint_source_fake_arcade_cannot_write_chain_words() {
    let txid = "01".repeat(32);
    let params = Params { age_threshold: 60 };
    for status in [
        "MINED",
        "REJECTED",
        "DOUBLE_SPEND_ATTEMPTED",
        "SEEN_ON_NETWORK",
        "ACCEPTED_BY_NETWORK",
    ] {
        let mut source = BroadcasterHints::arc(
            &txid,
            json!({"txid":txid,"txStatus":status,"blockHeight":10})
                .to_string()
                .as_bytes(),
            123,
        )
        .unwrap();
        let (id, hint) = source.next_hint().unwrap().unwrap();
        assert_eq!(id, txid);
        assert_eq!(hint.source, "arcade");
        assert_eq!(hint.observed, 123);
        let mut s = State::new(&id);
        s.step(&params, &snapshot(), Input::Host(HostAction::Build))
            .unwrap();
        s.step(&params, &snapshot(), Input::Hint(hint)).unwrap();
        assert!(s.word().chain().is_none());
        assert!(s.evidence().is_none());
        assert!(source.next_hint().unwrap().is_none());
    }
}
#[test]
fn ns3_clock_fake_worker_milliseconds_are_tracker_seconds() {
    assert_eq!(seconds(123_999), 123);
}

#[tokio::test]
async fn ns3_reask_arcade_first_and_foreign_never_asks_arcade() {
    use std::cell::RefCell;
    let txid = "01".repeat(32);
    let calls = RefCell::new(Vec::new());
    let path = MerklePath::from_coinbase_txid(&txid, 10).to_hex();
    let get = |url: String| {
        calls.borrow_mut().push(url);
        let body = json!({"merklePath":path}).to_string().into_bytes();
        async move { Some((200, body)) }
    };
    let p = execute_reask(&txid, true, "https://arcade", &Reask::Age, get, |_| async {
        snapshot()
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(p.height(), 10);
    assert_eq!(
        calls.borrow().as_slice(),
        &[format!("https://arcade/tx/{txid}")]
    );
    calls.borrow_mut().clear();
    let get = |url: String| {
        calls.borrow_mut().push(url);
        async { Some((404, Vec::new())) }
    };
    assert!(execute_reask(
        &txid,
        false,
        "https://arcade",
        &Reask::Age,
        get,
        |_| async { snapshot() }
    )
    .await
    .unwrap()
    .is_none());
    assert_eq!(calls.borrow().len(), 3);
    assert!(calls.borrow().iter().all(|u| !u.contains("arcade")));
}

#[test]
fn ns3_banana_captured_tsc_root_reduces_through_sdk() {
    let p = tsc_proof(
        "91309122f5630052f7e57f7db843d26d32ae4426a9dd9b2fc2955f2fab8cf9a6",
        956597,
        include_bytes!("fixtures/ns3/banana-tsc.json"),
    )
    .unwrap();
    assert_eq!(
        p.root().unwrap(),
        "d204a3ac7ac071862b1b96a18e6465da929fa2fc349a11605346e8e9647fcd57"
    );
}

#[tokio::test]
async fn ns3_m2_bracketed_snapshot_retries_once() {
    let calls = std::cell::Cell::new(0);
    let answer = retry_snapshot(|| {
        let n = calls.get();
        calls.set(n + 1);
        async move {
            if n == 0 {
                HeaderSnapshot {
                    tip: Err(HeaderError("header snapshot changed".into())),
                    headers: Default::default(),
                }
            } else {
                snapshot()
            }
        }
    })
    .await;
    assert_eq!(calls.get(), 2);
    assert_eq!(answer.tip, Ok(10));
    calls.set(0);
    let answer = retry_snapshot(|| {
        calls.set(calls.get() + 1);
        async {
            HeaderSnapshot {
                tip: Err(HeaderError("transport".into())),
                headers: Default::default(),
            }
        }
    })
    .await;
    assert_eq!(calls.get(), 2);
    assert!(answer.tip.is_err());
}

#[test]
fn ns3_m3_courier_rotation_and_three_fault_circuit() {
    let mut p = low_app_layer::tracker_host::pacing::Pacing::default();
    p.begin_tick(1);
    assert_eq!(p.order(), [0, 1, 2]);
    assert_eq!(p.order(), [1, 2, 0]);
    for _ in 0..3 {
        p.fault(1);
    }
    assert!(!p.allowed(1));
    assert!(p.allowed(0));
    p.begin_tick(1);
    assert!(!p.allowed(1));
    p.begin_tick(2);
    assert!(p.allowed(1));
}

#[test]
fn ns3d_note4_courier_circuit_uses_one_scheduled_pass_window() {
    let runtime = include_str!("../src/tracker_host/runtime.rs");
    assert!(
        !runtime.contains("begin_tick(now / 60)"),
        "event resets courier circuit per minute"
    );
}
