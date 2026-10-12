//! bsv-low #639: a short lived absence cannot retire a monitor entry.
use low_app_layer::txany::*;

#[test]
fn only_definitive_verdicts_are_terminal_on_single_and_batch_bodies() {
    for (kind, expected) in [
        (VerdictKind::Absent, false),
        (VerdictKind::Refused, true),
        (VerdictKind::Retired, true),
        (VerdictKind::Unconfirmable, true),
    ] {
        let memo = VerdictMemo {
            txid: "ab".repeat(32),
            verdict_at_ms: 1_000,
            input_outpoint: String::new(),
            spender_txid: String::new(),
            kind,
            evidence: "fixture".into(),
        };
        let answer = answer_from_verdict_memo(&memo, Some("0100".into()));
        let mut body = tx_any_value(&memo.txid, &answer);
        assert_eq!(body["terminal"], expected);
        body.as_object_mut().unwrap().remove("terminal");
        assert_eq!(
            body,
            serde_json::json!({ "txid": memo.txid, "present": false, "confirmed": null, "height": null, "rawHex": "0100", "source": "memo", "unconfirmable": kind == VerdictKind::Unconfirmable, "claimedHeight": null, "tier": "unknown", "hintSource": null, "observedAtMs": null })
        );
        let batch: serde_json::Value =
            serde_json::from_str(&tx_any_batch_body(&[(memo.txid.clone(), answer)], &[])).unwrap();
        assert_eq!(batch["answers"][&memo.txid]["terminal"], expected);
    }
}

#[test]
fn spent_any_absence_finality_is_additive_and_young_absence_is_false() {
    let mut absent =
        low_app_layer::logic::OutpointStatus::unknown(&low_app_layer::logic::Outpoint {
            txid: "ab".repeat(32),
            vout: 0,
        });
    absent.reason = Some(SPENT_ANY_REASON_TX_ABSENT);
    for terminal in [false, true] {
        let before = low_app_layer::logic::utxo_status_body(std::slice::from_ref(&absent));
        let body = spent_any_body(std::slice::from_ref(&absent), &[terminal]);
        let mut after: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(after[0]["terminal"], terminal);
        after[0].as_object_mut().unwrap().remove("terminal");
        assert_eq!(
            after.to_string(),
            before,
            "every existing served byte is retained"
        );
    }
    absent.reason = Some("provider-fault");
    assert_eq!(
        spent_any_body(std::slice::from_ref(&absent), &[false]),
        low_app_layer::logic::utxo_status_body(&[absent])
    );
}
