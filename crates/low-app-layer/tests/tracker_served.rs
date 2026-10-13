use bsv_rs::transaction::MerklePath;
use bsv_tracker::{
    Evidence, Header, HeaderError, Headers, Hint, HintStatus, HostAction, Input, Params, Proof,
    State,
};
use low_app_layer::tracker_host::served;
use serde_json::json;
struct Active;
impl Headers for Active {
    fn header_at(&self, _: u32) -> Result<Option<Header>, HeaderError> {
        Ok(Some(Header {
            hash: "02".repeat(32),
            merkle_root: "01".repeat(32),
        }))
    }
    fn tip_height(&self) -> Result<u32, HeaderError> {
        Ok(10)
    }
}
#[test]
fn ns3_captured_tx_body_preserves_old_fields_and_badges_only_checked_state() {
    let id = "01".repeat(32);
    let before = json!({"txid":id,"present":true,"confirmed":true,"height":null,"rawHex":"0100","source":"external","unconfirmable":false,"claimedHeight":10});
    let mut s = State::new(&id);
    let params = Params { age_threshold: 60 };
    s.step(&params, &Active, Input::Host(HostAction::Build))
        .unwrap();
    s.step(
        &params,
        &Active,
        Input::Hint(Hint::new("woc", HintStatus::Seen, 123)),
    )
    .unwrap();
    let mut after = before.clone();
    served::transaction(&mut after, &s);
    for (key, value) in before.as_object().unwrap() {
        assert_eq!(&after[key], value);
    }
    assert_eq!(after["tracker"]["word"], "seen");
    assert_eq!(after["tracker"]["tier"], "hint");
    assert!(after["tracker"]["verifyPath"].is_null());
    // Finding: the old courier flag says confirmed, the tracker says seen.
    assert_eq!(after["confirmed"], true);
    s.step(
        &params,
        &Active,
        Input::Evidence(Evidence::Proof(
            Proof::new(&id, MerklePath::from_coinbase_txid(&id, 10)).unwrap(),
        )),
    )
    .unwrap();
    served::transaction(&mut after, &s);
    assert_eq!(after["tracker"]["word"], "mined");
    assert_eq!(after["tracker"]["height"], 10);
    assert_eq!(after["tracker"]["verifyPath"]["root"], id);
}
#[test]
fn ns3_spender_fields_use_spender_subject_and_keep_legacy_body() {
    let id = "01".repeat(32);
    let mut value =
        json!({"statuses":[{"txid":"03".repeat(32),"spendingTxid":id,"spentConfirmed":true}]});
    let before = value.clone();
    served::spenders(&mut value, &Default::default());
    assert_eq!(value["statuses"][0]["spenderWord"]["word"], "unknown");
    value["statuses"][0]
        .as_object_mut()
        .unwrap()
        .remove("spenderWord");
    assert_eq!(before, value);
}

#[test]
fn ns3_route_body_boundary_adds_words_to_single_batch_and_spenders() {
    let id = "01".repeat(32);
    let single: serde_json::Value = serde_json::from_str(&served::body(
        &json!({"confirmed":true}).to_string(),
        Some(&id),
        &Default::default(),
    ))
    .unwrap();
    assert_eq!(single["tracker"]["word"], "unknown");
    assert_eq!(single["confirmed"], true);
    let batch =
        json!({"answers":{id.clone():{"height":null}},"rows":[{"facts":{"spendingTxid":id}}]});
    let after: serde_json::Value =
        serde_json::from_str(&served::body(&batch.to_string(), None, &Default::default())).unwrap();
    assert_eq!(after["answers"][&id]["tracker"]["word"], "unknown");
    assert_eq!(after["rows"][0]["facts"]["spenderWord"]["word"], "unknown");
}

#[test]
fn ns3d_l3_real_main_body_builders_preserve_every_legacy_key() {
    use low_app_layer::{logic, owed, tracker_host::runtime, txany};
    use std::collections::BTreeMap;
    fn check(raw: String, subject: Option<&str>, views: &BTreeMap<String, serde_json::Value>) {
        fn legacy(value: &mut serde_json::Value) {
            match value {
                serde_json::Value::Object(obj) => {
                    for key in ["tracker", "spenderWord", "coinWord"] { obj.remove(key); }
                    for child in obj.values_mut() { legacy(child); }
                }
                serde_json::Value::Array(arr) => for child in arr { legacy(child); },
                _ => {}
            }
        }
        let before: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let mut after = before.clone();
        runtime::decorate(&mut after, subject, views);
        legacy(&mut after);
        assert_eq!(after, before, "production body key or value changed");
    }
    let id = "01".repeat(32);
    let spender = "02".repeat(32);
    let views = BTreeMap::from([(spender.clone(), json!({"word":"seen", "tier":"hint", "source":"arcade"}))]);
    let observations = [
        txany::TxObservation::FirstPartyPresent {confirmed:false, raw_hex:Some("0100".into())},
        txany::TxObservation::Present {confirmed:true, raw_hex:Some("0100".into())},
        txany::TxObservation::CourierHint {raw_hex:Some("0100".into()), source:"woc", observed_at_ms:123000},
        txany::TxObservation::Fault,
    ];
    for obs in observations {
        let answer = txany::decide_tx_any(None, None, Some(&obs), txany::AbsenceCorroboration::Unknown);
        check(txany::tx_any_body(&id, &answer), Some(&id), &views);
        check(txany::tx_any_batch_body(&[(id.clone(),answer)], &[(spender.clone(), "fault")]), None, &views);
    }
    let entries = [logic::OutpointStatus {
        txid:id.clone(), vout:0, known:true, spent:Some(true), spending_txid:Some(spender.clone()),
        spent_confirmed:Some(false), spender_seen:Some(true), spender_final:Some(true),
        reason:None, funding_seen:Some(true),
    }];
    check(txany::spent_any_body(&entries, &[false]), None, &views);
    check(txany::spent_any_body(&entries, &[true]), None, &views);
    check(logic::utxo_status_body(&entries), None, &views);
    let rows = [owed::OwedRow {identity:id.clone(), outpoint:format!("{id}:0"), family:owed::OwedFamily::Payout,
        game_id:"game".into(), sats:Some(100), opponent_identity:Some(spender.clone()), at_height:Some(10),
        facts:json!({"claimable":true,"spendingTxid":spender,"spentConfirmed":true,"tier":"proven"}), reason:None}];
    check(owed::owed_body(&id, Some(10), &rows, 123000, false), None, &views);
}
