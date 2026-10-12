//! NS-3 step 1: the tracker consumes this workspace's SDK path type and
//! writes mined only after its own header check. No Worker routes change.
use bsv_rs::transaction::MerklePath;
use bsv_tracker::{
    Evidence, Header, HeaderError, Headers, HostAction, Input, Params, Proof, State, Verdict, Word,
};

struct FixtureHeaders;

impl Headers for FixtureHeaders {
    fn header_at(&self, height: u32) -> Result<Option<Header>, HeaderError> {
        Ok((height == 10).then(|| Header {
            hash: "02".repeat(32),
            merkle_root: "01".repeat(32),
        }))
    }

    fn tip_height(&self) -> Result<u32, HeaderError> {
        Ok(10)
    }
}

#[test]
fn ns3_dependency_uses_sdk_050_and_checked_evidence() {
    let txid = "01".repeat(32);
    let params = Params { age_threshold: 60 };
    let mut state = State::new(&txid);
    state
        .step(&params, &FixtureHeaders, Input::Host(HostAction::Build))
        .unwrap();
    let body = format!("{{\"txid\":\"{txid}\",\"txStatus\":\"MINED\",\"blockHeight\":10}}");
    let hint = Verdict::from_arc_body(&txid, body.as_bytes())
        .unwrap()
        .into_hint("arcade", 100);
    state
        .step(&params, &FixtureHeaders, Input::Hint(hint))
        .unwrap();
    assert_eq!(state.word(), &Word::Built, "a mined hint is no proof");
    let path = MerklePath::from_coinbase_txid(&txid, 10);
    let proof = Proof::new(&txid, path).unwrap();
    state
        .step(
            &params,
            &FixtureHeaders,
            Input::Evidence(Evidence::Proof(proof)),
        )
        .unwrap();
    let Word::Mined(mined) = state.word() else {
        panic!("checked evidence must write the tracker word");
    };
    assert_eq!(mined.height(), 10);
    assert_eq!(mined.checked().root(), txid);
    assert_eq!(mined.checked().header().hash, "02".repeat(32));
    assert!(!state.suspect());
}
