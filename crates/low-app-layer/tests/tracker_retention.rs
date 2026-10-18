#[test]
fn ns3_l1_public_reads_create_zero_rows_and_projection_is_reaskable() {
    let manifest: serde_json::Value =
        serde_json::from_str(include_str!("../../../storage-ownership.json")).unwrap();
    let text = manifest.to_string();
    let row = manifest["tables"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == "tx_words")
        .unwrap_or_else(|| panic!("manifest shape: {text}"));
    assert_eq!(
        row["rebuild_class"], "re-askable",
        "tx_words is still permanent stranger-fillable storage"
    );
    assert_eq!(row["never_wipe"], false);
    let runtime = include_str!("../src/tracker_host/runtime.rs");
    assert!(runtime.contains("pub const PUBLIC_ROWS_PER_MINUTE: usize = 0;"));
    assert!(runtime.contains("stored::SWEEP"));
}
#[test]
fn ns3_l1_sweep_is_eight_days_bounded_and_spares_first_party() {
    use low_app_layer::tracker_host::stored;
    let db = rusqlite::Connection::open_in_memory().unwrap();
    db.execute_batch(stored::CREATE).unwrap();
    db.execute_batch("CREATE TABLE transactions(txid TEXT PRIMARY KEY); CREATE TABLE pot_beefs(txid TEXT PRIMARY KEY); CREATE TABLE pot_records(txid TEXT, outputIndex INTEGER, spendingTxid TEXT)").unwrap();
    for n in 0..100 {
        db.execute(
            "INSERT INTO tx_words(txid,word,tier,updated_at) VALUES (?,'unknown','host',1)",
            [format!("{n:064x}")],
        )
        .unwrap();
    }
    db.execute(
        "INSERT INTO transactions VALUES (?)",
        [format!("{:064x}", 0)],
    )
    .unwrap();
    db.execute(
        "UPDATE tx_words SET hint_source='arcade' WHERE txid=?",
        [format!("{:064x}", 1)],
    )
    .unwrap();
    db.execute(
        "UPDATE tx_words SET updated_at=700000 WHERE txid=?",
        [format!("{:064x}", 2)],
    )
    .unwrap();
    assert_eq!(db.execute(stored::SWEEP, [700001 - 8 * 86400]).unwrap(), 64);
    assert_eq!(db.execute(stored::SWEEP, [700001 - 8 * 86400]).unwrap(), 34);
    let remaining: i64 = db
        .query_row("SELECT count(*) FROM tx_words", [], |r| r.get(0))
        .unwrap();
    assert_eq!(remaining, 2);
}

#[test]
fn ns3d_m3_untouched_built_stranger_is_swept_after_eight_days() {
    use low_app_layer::tracker_host::stored;
    let db = rusqlite::Connection::open_in_memory().unwrap();
    db.execute_batch(stored::CREATE).unwrap();
    db.execute_batch("CREATE TABLE transactions(txid TEXT PRIMARY KEY); CREATE TABLE pot_beefs(txid TEXT PRIMARY KEY); CREATE TABLE pot_records(txid TEXT, outputIndex INTEGER, spendingTxid TEXT); INSERT INTO tx_words(txid,word,tier,updated_at) VALUES ('stranger','built','host',1),('held','built','host',1); INSERT INTO transactions VALUES ('held')").unwrap();
    let due: Vec<String> = db
        .prepare(stored::DUE)
        .unwrap()
        .query_map([700001 - 900], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(
        due,
        vec!["held"],
        "stranger built row renewed by scheduled reask"
    );
    assert_eq!(
        db.execute(stored::SWEEP, [700001 - 8 * 86400]).unwrap(),
        1,
        "untouched built stranger was spared by the sweep"
    );
    assert_eq!(
        db.query_row("SELECT txid FROM tx_words", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "held"
    );
}

#[test]
fn ns3d_note5_age_scans_use_a_production_index() {
    use low_app_layer::tracker_host::stored;
    let db = rusqlite::Connection::open_in_memory().unwrap();
    for sql in bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS {
        if let Err(e) = db.execute_batch(sql) {
            assert!(e.to_string().contains("duplicate column"));
        }
    }
    assert_eq!(
        stored::AGE_INDEX,
        bsv_overlay_cloudflare::d1::TX_WORDS_AGE_INDEX
    );
    for sql in [stored::DUE, stored::SWEEP] {
        let plans: Vec<String> = db
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
            .unwrap()
            .query_map([100], |r| r.get(3))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert!(
            plans.iter().any(|p| p.contains("tx_words_age")),
            "age scan lacks index: {plans:?}"
        );
    }
}

#[tokio::test]
async fn ns3d2_m1_novel_callback_and_acceptance_have_no_effects() {
    use low_app_layer::tracker_host::{runtime, stored};
    for kind in ["callback", "accepted"] {
        let writes = std::cell::Cell::new(0);
        let result = runtime::event_for_subject(false, false, || async {
            writes.set(writes.get() + 1);
            let mut state = bsv_tracker::State::new("novel");
            let headers = low_app_layer::tracker_host::HeaderSnapshot {
                tip: Ok(0),
                headers: Default::default(),
            };
            state
                .step(
                    &bsv_tracker::Params { age_threshold: 60 },
                    &headers,
                    bsv_tracker::Input::Hint(bsv_tracker::Hint::new(
                        "arcade",
                        bsv_tracker::HintStatus::Seen,
                        100,
                    )),
                )
                .unwrap();
            Ok(stored::Row::from_state(&state, 100, None))
        })
        .await
        .unwrap();
        assert!(
            result.is_none(),
            "{kind} for novel subject planted an arcade row"
        );
        assert_eq!(writes.get(), 0);
    }
    let held = runtime::event_for_subject(true, false, || async {
        Ok(stored::Row::from_state(
            &bsv_tracker::State::new("held"),
            100,
            None,
        ))
    })
    .await
    .unwrap();
    assert!(held.is_some(), "held callback still writes");
}
#[test]
fn ns3d2_m1_evicted_arcade_row_is_swept_and_terminal_is_not_due() {
    use low_app_layer::tracker_host::stored;
    let db = rusqlite::Connection::open_in_memory().unwrap();
    db.execute_batch(stored::CREATE).unwrap();
    db.execute_batch("CREATE TABLE transactions(txid TEXT PRIMARY KEY); CREATE TABLE pot_beefs(txid TEXT PRIMARY KEY); CREATE TABLE pot_records(txid TEXT, outputIndex INTEGER, spendingTxid TEXT); INSERT INTO tx_words(txid,word,tier,hint_source,updated_at,legacy_body) VALUES ('evicted','seen','hint','arcade',1,NULL),('held','seen','hint','arcade',1,'{\"trackerTerminal\":true}'); INSERT INTO transactions VALUES ('held')").unwrap();
    assert_eq!(
        db.execute(stored::SWEEP, [700001 - 8 * 86400]).unwrap(),
        1,
        "evicted arcade row is permanent"
    );
    assert_eq!(
        db.prepare(stored::DUE)
            .unwrap()
            .query_map([700001 - 900], |r| r.get::<_, String>(0))
            .unwrap()
            .count(),
        0,
        "terminal row still due"
    );
}

#[test]
fn ns3d2_m1_both_route_doors_and_event_belt_require_a_live_hold() {
    let source = include_str!("../../overlay-cloudflare/src/routes.rs");
    let routes = source
        .split_whitespace()
        .collect::<String>()
        .replace(",)", ")");
    assert!(!routes.contains(
        &("tracker_events::note(&subject_txid, \"accepted\""
            .split_whitespace()
            .collect::<String>())
    ));
    assert!(!routes.contains(
        &("tracker_events::note(&txid, \"callback\""
            .split_whitespace()
            .collect::<String>())
    ));
    assert!(routes.contains(&("tracker_events::note_held(env.d1(\"OVERLAY_DB\").ok().as_ref(), &subject_txid, \"accepted\", None).await".split_whitespace().collect::<String>())));
    assert!(routes.contains(&("tracker_events::note_held(ops_db, &txid, \"callback\", serde_json::from_str(&raw).ok()).await".split_whitespace().collect::<String>())));
    let runtime = include_str!("../src/tracker_host/runtime.rs");
    assert!(runtime.contains("event_for_subject(held, kind == \"submit\""));
    assert!(runtime.contains("let first_party = live || kind == \"submit\";"));
}
