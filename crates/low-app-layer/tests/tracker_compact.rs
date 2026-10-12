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

#[test]
fn compact_concurrent_hint_cannot_demote_a_checked_word_or_a_seen_word() {
    let db = rusqlite::Connection::open_in_memory().unwrap();
    db.execute_batch(stored::CREATE).unwrap();
    let row = mined();
    let write = |word: &str, tier: &str, at: i64| {
        db.execute(
            stored::UPSERT,
            rusqlite::params![
                row.txid,
                word,
                tier,
                10,
                row.proof_root,
                10,
                row.proof_hex,
                row.header_hash,
                "woc",
                at,
                at,
                row.legacy_body
            ],
        )
        .unwrap()
    };
    write("mined", "chain", 100);
    write("seen", "hint", 100);
    write("announced", "hint", 101);
    assert_eq!(
        db.query_row("SELECT word FROM tx_words", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "mined"
    );
    let other = "05".repeat(32);
    db.execute(
        "INSERT INTO tx_words (txid,word,tier,updated_at) VALUES (?,'seen','hint',200)",
        [&other],
    )
    .unwrap();
    db.execute(
        stored::UPSERT,
        rusqlite::params![
            other,
            "announced",
            "hint",
            Option::<i64>::None,
            Option::<String>::None,
            Option::<i64>::None,
            Option::<String>::None,
            Option::<String>::None,
            "arcade",
            201,
            201,
            Option::<String>::None
        ],
    )
    .unwrap();
    assert_eq!(
        db.query_row("SELECT word FROM tx_words WHERE txid=?", [&other], |r| {
            r.get::<_, String>(0)
        })
        .unwrap(),
        "seen"
    );
    // A checked proof whose transport started before that hint still wins;
    // the storage stamp stays monotonic.
    db.execute(
        stored::UPSERT,
        rusqlite::params![
            other,
            "mined",
            "chain",
            10,
            other,
            10,
            row.proof_hex,
            row.header_hash,
            Option::<String>::None,
            Option::<i64>::None,
            199,
            Option::<String>::None
        ],
    )
    .unwrap();
    assert_eq!(
        db.query_row(
            "SELECT word,updated_at FROM tx_words WHERE txid=?",
            [&other],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
        )
        .unwrap(),
        ("mined".into(), 200)
    );
}
#[test]
fn compact_fleet_lookup_is_one_bind_and_an_index_search() {
    let db = rusqlite::Connection::open_in_memory().unwrap();
    db.execute_batch(stored::CREATE).unwrap();
    let ids: Vec<_> = (0..600).map(|i| format!("{i:064x}")).collect();
    for id in &ids {
        db.execute(
            "INSERT INTO tx_words (txid,word,tier,updated_at) VALUES (?,'unknown','host',0)",
            [id],
        )
        .unwrap();
    }
    let keys = serde_json::to_string(&ids).unwrap();
    let mut stmt = db.prepare(stored::READ).unwrap();
    assert_eq!(stmt.parameter_count(), 1);
    assert_eq!(
        stmt.query_map([&keys], |r| r.get::<_, String>(0))
            .unwrap()
            .count(),
        600
    );
    let mut plan = db
        .prepare(&format!("EXPLAIN QUERY PLAN {}", stored::READ))
        .unwrap();
    let details: Vec<_> = plan
        .query_map([&keys], |r| r.get::<_, String>(3))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert!(
        details
            .iter()
            .any(|s| s.contains("SEARCH tx_words USING INDEX")),
        "{details:?}"
    );
    assert!(
        !details.iter().any(|s| s.contains("SCAN tx_words")),
        "{details:?}"
    );
}

#[test]
fn owed_clock_reasks_aged_hint_rows_at_writes_and_never_age_polls_chain_words() {
    let mut row = mined();
    assert!(!runtime::owed_reask_due(&row, 1000));
    row.word = "seen".into();
    row.tier = "hint".into();
    row.updated_at = 100.0;
    assert!(!runtime::owed_reask_due(&row, 159));
    assert!(runtime::owed_reask_due(&row, 160));
    assert!(!runtime::owed_reask_due(&row, 99));
    let mut state = stored::seed_hint(Some(&row), &row.txid);
    state
        .step(
            &Params { age_threshold: 60 },
            &snapshot(&row.txid),
            Input::Host(bsv_tracker::HostAction::Tick(160)),
        )
        .unwrap();
    assert!(matches!(state.reask(), Some(bsv_tracker::Reask::Age)));
}

#[test]
fn ns3_h2_main_fields_survive_tracker_decoration() {
    let id = "01".repeat(32);
    for tier in ["first-party", "proven", "hint", "unknown"] {
        let before = json!({"tier":tier,"hintSource":"woc","observedAtMs":123,"confirmed":false,"height":null});
        let mut subject = before.clone();
        runtime::decorate(&mut subject, Some(&id), &Default::default());
        let mut batch = json!({"answers":{id.clone():before.clone()}});
        runtime::decorate(&mut batch, None, &Default::default());
        for value in [&subject, &batch["answers"][&id]] {
            for (key, expected) in before.as_object().unwrap() { assert_eq!(&value[key], expected, "main key {key}"); }
            assert_eq!(value["tracker"]["word"], "unknown");
            assert!(value.get("word").is_none());
        }
    }
}

#[test]
fn ns3_h1_expired_memo_is_reasked_even_when_spender_is_tracked() {
    use low_app_layer::hops_view::{ProbeMemo,ChainSpendProbe};
    let db = rusqlite::Connection::open_in_memory().unwrap();
    db.execute_batch(stored::CREATE).unwrap();
    db.execute_batch("CREATE TABLE hop_chain_probes (outpoint TEXT,probedAtMs INTEGER,spent INTEGER,spendingTxid TEXT,spentConfirmed INTEGER); INSERT INTO hop_chain_probes VALUES ('P.0',640000,1,'R',0)").unwrap();
    let row = Row::from_state(&State::new("R"),1000,None);
    db.execute("INSERT INTO tx_words (txid,word,tier,updated_at) VALUES ('R','unknown','host',1000)",[]).unwrap();
    let memo = db.query_row("SELECT * FROM hop_chain_probes",[],|r| Ok(ProbeMemo {outpoint:r.get(0)?,probed_at_ms:r.get(1)?,spent:r.get(2)?,spending_txid:r.get(3)?,spent_confirmed:r.get(4)?})).unwrap();
    let (mut answers, ask) = runtime::spent_any_memo_plan(&[("P".into(),0)], &[memo], 1_000_000, &BTreeMap::from([("R".into(),row)]));
    let mut probes = 0;
    for (id,vout) in ask { probes += 1; answers.push((id,vout,ChainSpendProbe {known:true,spent:Some(true),spending_txid:Some("S".into()),spent_confirmed:Some(true)})); }
    assert_eq!(answers[0].2.spending_txid.as_deref(),Some("S"), "expired memo must serve PROBE spender S");
    assert_eq!(answers[0].2.spent_confirmed,Some(true));
    assert_eq!(probes,1);
}

#[test]
fn ns3_m1_woc_mempool_presence_is_never_broadcaster_seen() {
    let id = "01".repeat(32);
    let woc_200 = json!({"txid":id,"hash":id,"vin":[],"vout":[]});
    assert_eq!(low_app_layer::txany::parse_woc_tx_claim(&woc_200,&id),Some(false));
    let mut s = State::new(&id);
    let params = Params {age_threshold:60};
    s.step(&params,&snapshot(&id),Input::Host(bsv_tracker::HostAction::Build)).unwrap();
    let legacy = json!({"present":true,"source":"external"});
    let hint = runtime::courier_presence_hint(&legacy,123).unwrap();
    s.step(&params,&snapshot(&id),Input::Hint(hint)).unwrap();
    let view = low_app_layer::tracker_host::served::fields(&s);
    assert_ne!(view["word"], "seen", "courier presence is a live-race hint");
    assert_eq!(view["source"],"woc");
    assert!(view["verifyPath"].is_null());
}

#[test]
fn ns3_m2_backfill_page_is_bounded_resumed_and_certified() {
    let db=rusqlite::Connection::open_in_memory().unwrap();
    db.execute_batch(stored::BACKFILL_CREATE).unwrap();
    db.execute_batch("CREATE TABLE transactions(txid TEXT,beef BLOB,has_proof INTEGER); CREATE TABLE pot_beefs(txid TEXT,beef BLOB,has_proof INTEGER)").unwrap();
    for n in 0..9 { db.execute("INSERT INTO transactions VALUES (?1,x'00',1)",[format!("{n:064x}")]).unwrap(); }
    let mut after=String::new(); let mut count=0;
    loop {
        let rows:Vec<String>=db.prepare(stored::BACKFILL_PAGE).unwrap().query_map([&after],|r| r.get(0)).unwrap().map(Result::unwrap).collect();
        assert!(rows.len()<=4);
        let complete=rows.len()<4;
        count+=rows.len();
        if let Some(t)=rows.last(){after=t.clone();}
        db.execute(stored::BACKFILL_SAVE,rusqlite::params![stored::BACKFILL_VERSION,after,complete]).unwrap();
        after=db.query_row("SELECT cursor FROM tracker_backfill WHERE version=?1",[stored::BACKFILL_VERSION],|r|r.get(0)).unwrap();
        if complete {break;}
    }
    assert_eq!(count,9);
    assert_eq!(db.query_row("SELECT complete FROM tracker_backfill",[],|r|r.get::<_,bool>(0)).unwrap(),true);
}
