#[test]
fn minutes_0113_fault_expiry_sql_and_probe_reset_are_scoped() {
    let db = sqlite();
    for (name, reason, expiry) in [
        ("expired.0", "dead:courier", Some(1)),
        ("unanchorable.0", "unanchorable:not_found", Some(1)),
        ("future.0", "dead:courier", Some(u32::MAX as u64)),
        ("unknown.0", "dead:courier", None),
        ("memo.0", "dead:courier", None),
        ("hold.0", "time", Some(1)),
    ] {
        let mut r = record(name, 10, 0, 0);
        r.reason = reason.into();
        r.cursor_passable = true;
        r.faults = 3;
        r.expiry = expiry;
        upsert(&db, &r);
        db.execute("UPDATE gasp_deferred_graphs SET reason = ?1 WHERE host = ?2 AND topic = ?3 AND outpoint = ?4", rusqlite::params![retained_reason(&r), r.peer, r.topic, r.outpoint]).unwrap();
    }
    db.execute(
        EXPIRED_OUTPOINT_PUT_SQL,
        rusqlite::params!["tm_x", "memo.0", 1, 1000],
    )
    .unwrap();
    let mut swept: Vec<String> = db
        .prepare(FAULT_EXPIRY_SWEEP_SQL)
        .unwrap()
        .query_map([], |r| r.get(2))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    swept.sort();
    assert_eq!(swept, ["expired.0", "memo.0", "unanchorable.0"]);
    let mut r = record("future.0", 10, 0, 0);
    r.reason = "dead:courier".into();
    r.cursor_passable = true;
    r.topic = "tm_other".into();
    upsert(&db, &r);
    db.execute("UPDATE gasp_deferred_graphs SET reason = ?1 WHERE host = ?2 AND topic = ?3 AND outpoint = ?4", rusqlite::params![retained_reason(&r), r.peer, r.topic, r.outpoint]).unwrap();
    let cleared: Vec<String> = db
        .prepare(FAULT_READMIT_SQL)
        .unwrap()
        .query_map(["tm_x", "future.0"], |r| r.get(1))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(cleared, ["tm_x"]);
    let held: u64 = db.query_row("SELECT COUNT(*) FROM gasp_deferred_graphs WHERE topic = 'tm_other' OR outpoint IN ('hold.0', 'unknown.0')", [], |r| r.get(0)).unwrap();
    assert_eq!(held, 3);
}

#[test]
fn minutes_0113_dead_and_swept_counters_are_additive() {
    let topic = serde_json::from_value(serde_json::json!({
        "peers": [], "sync_type": "peers", "errors": [], "dead_graphs": 2, "expired_swept": 3
    }))
    .unwrap();
    let values = counter_deltas(&std::collections::HashMap::from([("tm_x".into(), topic)]));
    assert_eq!(
        values,
        vec![
            (COUNTER_GASP_DEAD.into(), 2),
            (COUNTER_GASP_FAULT_EXPIRED.into(), 3)
        ]
    );
}

#[test]
fn minutes_0113_retry_claim_sql_is_atomic_bounded_and_time_gated() {
    // 0.1.14 moves the same retry contract to the compact memo. Partial-graph
    // byte ceilings no longer apply; stale claims, time, score and retry bounds do.
    let db = sqlite();
    let mut r = record("retry.0", 10, 0, 0);
    r.reason = "unanchorable:courier".into();
    r.faults = 3;
    r.cursor_passable = true;
    upsert(&db, &r);
    db.execute(
        crate::gasp_fault_memo::FOLD,
        rusqlite::params![r.peer, r.topic],
    )
    .unwrap();
    let now: u64 = db
        .query_row("SELECT unixepoch()", [], |row| row.get(0))
        .unwrap();
    let claim = |candidate: &overlay_engine::gasp::DeferredGraph| {
        db.prepare(crate::gasp_fault_memo::RESERVE)
            .unwrap()
            .query_map(
                rusqlite::params![
                    candidate.peer,
                    candidate.topic,
                    candidate.outpoint,
                    candidate.unanchorable_retries,
                    candidate.retry_at,
                    candidate.score,
                    candidate.fault_class(),
                    overlay_engine::gasp::UNANCHORABLE_RETRIES
                ],
                |row| row.get::<_, String>(0),
            )
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .len()
    };
    r.unanchorable_retries = 1;
    r.retry_at = now + overlay_engine::gasp::UNANCHORABLE_RETRY_SECS;
    assert_eq!(claim(&r), 1);
    assert_eq!(claim(&r), 0);
    r.unanchorable_retries = 2;
    assert_eq!(claim(&r), 0, "backoff stands");
    db.execute("UPDATE gasp_fault_memo SET next_at=0", [])
        .unwrap();
    r.score = 11;
    assert_eq!(claim(&r), 0, "score changed");
    r.score = 10;
    assert_eq!(claim(&r), 1);
    db.execute("UPDATE gasp_fault_memo SET next_at=0,retries=4", [])
        .unwrap();
    r.unanchorable_retries = 5;
    assert_eq!(claim(&r), 0, "retry ceiling stands");
}
