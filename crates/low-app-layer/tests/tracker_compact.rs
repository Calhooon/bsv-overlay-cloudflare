use bsv_rs::transaction::MerklePath;
use bsv_tracker::{Evidence, Header, Hint, HintStatus, Input, Params, Proof, State};
use low_app_layer::tracker_host::{
    runtime,
    stored::{self, Row},
    HeaderSnapshot,
};
use serde_json::json;
use std::collections::BTreeMap;
fn snapshot(root: &str) -> HeaderSnapshot {
    HeaderSnapshot {
        tip: Ok(10),
        headers: BTreeMap::from([(
            10,
            Ok(Some(Header {
                hash: "02".repeat(32),
                merkle_root: root.into(),
            })),
        )]),
    }
}
fn mined() -> Row {
    let id = "01".repeat(32);
    let mut s = State::new(&id);
    s.step(
        &Params { age_threshold: 60 },
        &snapshot(&id),
        Input::Evidence(Evidence::Proof(
            Proof::new(&id, MerklePath::from_coinbase_txid(&id, 10)).unwrap(),
        )),
    )
    .unwrap();
    Row::from_state(
        &s,
        100,
        Some(json!({"confirmed":true,"height":10}).to_string()),
    )
}
#[test]
fn compact_lookup_rechecks_only_one_retained_proof_and_fails_closed() {
    let row = mined();
    assert_eq!(
        row.rechecked(&snapshot(&row.txid)).unwrap()["word"],
        "mined"
    );
    let stale = row.rechecked(&snapshot(&"03".repeat(32))).unwrap();
    assert_eq!(stale["word"], "stale");
    assert!(stale["verifyPath"].is_null());
    let unavailable = row
        .rechecked(&HeaderSnapshot {
            tip: Ok(0),
            headers: Default::default(),
        })
        .unwrap();
    assert!(unavailable["verifyPath"].is_null());
    assert!(unavailable["cannotCheck"].is_string());
    let mut corrupt = row;
    corrupt.proof_root = Some("04".repeat(32));
    assert!(corrupt.rechecked(&snapshot(&corrupt.txid)).is_err());
}
#[test]
fn compact_hints_never_replace_a_chain_word_and_seen_survives_rejection() {
    let old = mined();
    let mut s = stored::seed_hint(None, &old.txid);
    s.step(
        &Params { age_threshold: 60 },
        &snapshot(&old.txid),
        Input::Host(bsv_tracker::HostAction::Build),
    )
    .unwrap();
    s.step(
        &Params { age_threshold: 60 },
        &snapshot(&old.txid),
        Input::Hint(Hint::new(
            "arcade",
            HintStatus::Rejected {
                reason: "policy".into(),
            },
            101,
        )),
    )
    .unwrap();
    assert_eq!(stored::project(Some(&old), &s, 101, None).word, "mined");
    s.step(
        &Params { age_threshold: 60 },
        &snapshot(&old.txid),
        Input::Hint(Hint::new("woc", HintStatus::Seen, 102)),
    )
    .unwrap();
    let seen = Row::from_state(&s, 102, None);
    let mut restored = stored::seed_hint(Some(&seen), &seen.txid);
    restored
        .step(
            &Params { age_threshold: 60 },
            &snapshot(&old.txid),
            Input::Hint(Hint::new(
                "arcade",
                HintStatus::Rejected {
                    reason: "policy".into(),
                },
                103,
            )),
        )
        .unwrap();
    assert_eq!(
        stored::project(Some(&seen), &restored, 103, None).word,
        "seen"
    );
}
#[test]
fn compact_real_sqlite_persists_one_subject_and_guards_out_of_order_writes() {
    let db = rusqlite::Connection::open_in_memory().unwrap();
    db.execute_batch(stored::CREATE).unwrap();
    let row = mined();
    let write = |word: &str, at: i64| {
        db.execute(
            stored::UPSERT,
            rusqlite::params![
                row.txid,
                word,
                "chain",
                10,
                row.proof_root,
                10,
                row.proof_hex,
                row.header_hash,
                Option::<String>::None,
                Option::<i64>::None,
                at,
                row.legacy_body
            ],
        )
        .unwrap()
    };
    write("mined", 100);
    write("unknown", 99);
    let word: String = db
        .query_row(&stored::read_sql(1), [&row.txid], |r| r.get("word"))
        .unwrap();
    assert_eq!(word, "mined");
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM tx_words", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
}
#[test]
fn compact_route_projection_keeps_legacy_fields_and_uses_the_spender_subject() {
    let row = mined();
    let before = json!({"statuses":[{"txid":"04".repeat(32),"spendingTxid":row.txid,"spentConfirmed":false}]});
    let mut after = before.clone();
    runtime::decorate(
        &mut after,
        None,
        &BTreeMap::from([(row.txid.clone(), row.view())]),
    );
    assert_eq!(after["statuses"][0]["spenderWord"]["word"], "mined");
    after["statuses"][0]
        .as_object_mut()
        .unwrap()
        .remove("spenderWord");
    assert_eq!(after, before);
}

#[test]
fn write_event_courier_captures_subject_kind_and_proof_without_a_chain_claim() {
    bsv_overlay_cloudflare::tracker_events::drain();
    for kind in ["submit", "callback", "seen", "proof", "retire"] {
        bsv_overlay_cloudflare::tracker_events::note(
            &"01".repeat(32),
            kind,
            Some(json!({"merklePath":"01"})),
        );
    }
    let events = bsv_overlay_cloudflare::tracker_events::drain();
    assert_eq!(events.len(), 5);
    assert!(events
        .iter()
        .all(|v| v["txid"] == "01".repeat(32) && v["body"]["merklePath"] == "01"));
    assert!(bsv_overlay_cloudflare::tracker_events::drain().is_empty());
}
