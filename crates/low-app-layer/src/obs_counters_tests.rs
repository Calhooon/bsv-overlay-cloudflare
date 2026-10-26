//! bsv-low #628 (the observability bar, issue 4): each mirrored family's durable deltas, made by its producer, read
//! back from the census's batched flush (`courier::take_pending_if_due`), the flush's bound, and the seed list the
//! overlay serves (with #625's and #626's names). Each `#[test]` runs on its own thread, so the isolate's pending
//! map starts empty and its flush count at 0.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::BTreeMap;

const NOW: f64 = 1_000_000.0;

/// The deltas one flush would write (the head is exact, so the first takes always flush).
fn drained() -> BTreeMap<String, u64> {
    crate::courier::take_pending_if_due(NOW)
        .into_iter()
        .collect()
}

/// The flush's bound: past the isolate's exact head (4 flushes), N increments inside a minute are ONE hop (one D1
/// batch) at the 64th, and a minute since the last flush flushes what is pending.
#[test]
fn obs_628_the_flush_is_one_hop_per_64_increments_or_per_minute() {
    use crate::courier::{
        note_presence, take_pending_if_due, COURIER_FLUSH_AT_CALLS, COURIER_FLUSH_EXACT_HEAD,
    };
    for _ in 0..COURIER_FLUSH_EXACT_HEAD {
        note_presence("applayer_owed_recompute_faults_total");
        assert_eq!(
            take_pending_if_due(NOW).len(),
            1,
            "the head flushes at once"
        );
    }
    let mut hops = 0;
    let mut written = 0;
    for _ in 0..COURIER_FLUSH_AT_CALLS {
        note_presence("applayer_owed_read_faults_total");
        let batch = take_pending_if_due(NOW + 1.0);
        if !batch.is_empty() {
            hops += 1;
            written += batch.iter().map(|(_, n)| n).sum::<u64>();
        }
    }
    assert_eq!(hops, 1, "64 increments produce one hop, at the 64th");
    assert_eq!(written, COURIER_FLUSH_AT_CALLS);
    for _ in 0..5 {
        crate::auth::count_misconfigured_refused();
    }
    assert!(
        take_pending_if_due(NOW + 2.0).is_empty(),
        "inside the minute, 10 increments wait"
    );
    let tail: u64 = take_pending_if_due(NOW + 60_001.0)
        .iter()
        .map(|(_, n)| n)
        .sum();
    assert_eq!(
        tail, 10,
        "a minute since the last flush takes the rest in one hop (total and reason, 5 each)"
    );
    assert!(
        take_pending_if_due(NOW + 120_000.0).is_empty(),
        "nothing pending: no hop"
    );
}

/// bsv-low #893 section 6.2: the walk's recompute counters retired with it (their durable rows stay seeded and
/// served); the judge's markerless read fault is mirrored as a read fault of its kind beside the sum.
#[test]
fn obs_628_the_owed_read_faults_are_mirrored() {
    crate::owed::note_markerless_read_fault();
    let d = drained();
    for (name, want) in [
        ("applayer_owed_read_faults_total", 1),
        ("applayer_owed_read_faults_markerless_total", 1),
    ] {
        assert_eq!(d.get(name).copied(), Some(want), "{name} in {d:?}");
    }
}

/// The isolate fields on `/health.owed` since the walk retired (bsv-low #893 section 6.2): the judge's markerless
/// read, the probe memo read, the spender-bytes sweep, and the stranded window. `recomputeSeq` is #566's
/// per-identity word on `/owed`, never an isolate field.
#[test]
fn obs_628_the_owed_health_keys_are_the_judges() {
    let v = crate::owed::owed_health_json();
    let mut keys: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
    let mut want = vec![
        "countersScope",
        "markerlessReadFaults",
        "markerlessIgnoredMarkers",
        "probeMemoReadFaults",
        "spenderStoreSweepPasses",
        "hopStrandedAfterMs",
    ];
    keys.sort_unstable();
    want.sort_unstable();
    assert_eq!(keys, want);
}

#[test]
fn obs_628_a_record_refusal_is_mirrored_by_its_class() {
    use crate::record_post::{verify_record_post, RecordKind, RecordRefusal};
    let refusal = verify_record_post(RecordKind::Potparty, &[0x6a, 0x01, 0x00], &"02".repeat(33))
        .expect_err("a script that is no marker is refused");
    crate::record_post::mirror_refused(refusal.class());
    crate::record_post::mirror_refused(RecordRefusal::DailyCapReached.class());
    let d = drained();
    assert_eq!(d["applayer_record_refused_total"], 2);
    assert_eq!(
        d[&format!("applayer_record_refused_{}_total", refusal.class())],
        1
    );
    assert_eq!(d["applayer_record_refused_daily_cap_reached_total"], 1);
    // the door's one refusal funnel mirrors every refusal it counts
    let src: String = include_str!("record_post.rs").split_whitespace().collect();
    assert!(src.contains("bump(&REFUSED_BY_KIND,k);}mirror_refused(r.class());"));
    assert!(src.contains("bump(&REFUSED_BY_KIND,kind);mirror_refused(REFUSED_BAD_BODY_CLASS);"));
}

#[test]
fn obs_628_an_auth_refusal_is_mirrored_by_its_reason() {
    use crate::auth::{count_auth_refused, middleware_refusal_reason};
    // the session lane's 401, as the middleware renders it (`lane_refusal_response`)
    let lane = br#"{"status":"error","code":"ERR_SESSION_REFUSED","reason":"bad-mac","description":"session lane refused: bad-mac"}"#;
    let reference = br#"{"status":"error","code":"ERR_INVALID_AUTH","description":"x"}"#;
    assert_eq!(middleware_refusal_reason(lane), "lane_bad_mac");
    assert_eq!(middleware_refusal_reason(reference), "invalid_auth");
    assert_eq!(
        middleware_refusal_reason(br#"{"code":"ERR_SESSION_REFUSED","reason":"?"}"#),
        "lane_other"
    );
    assert_eq!(middleware_refusal_reason(b"not json"), "middleware_other");
    let before = crate::auth::counters_snapshot().auth_refused;
    count_auth_refused(middleware_refusal_reason(lane));
    count_auth_refused(middleware_refusal_reason(reference));
    crate::auth::count_strict_refused_unauthenticated();
    let d = drained();
    assert_eq!(d["applayer_auth_refused_total"], 3);
    assert_eq!(d["applayer_auth_refused_lane_bad_mac_total"], 1);
    assert_eq!(d["applayer_auth_refused_invalid_auth_total"], 1);
    assert_eq!(d["applayer_auth_refused_strict_unauthenticated_total"], 1);
    assert!(
        crate::auth::counters_snapshot().auth_refused >= before + 2,
        "the isolate field still counts"
    );
    // the middleware arm reads the reason off the refusal it returns
    let src: String = include_str!("auth.rs").split_whitespace().collect();
    assert!(src.contains("count_auth_refused(response_refusal_reason(&resp));"));
}

/// Every name of the observability bar's families this worker writes on the overlay's `ops_counters`.
fn observability_counter_names() -> Vec<String> {
    let mut names = crate::courier::shaped_counter_names();
    names.extend(crate::txany::memo_counter_names());
    names.extend(crate::owed::owed_mirrored_counter_names());
    names.extend(crate::record_post::record_mirrored_counter_names());
    names.extend(crate::auth::auth_mirrored_counter_names());
    names.extend(crate::identity::identity_mirrored_counter_names());
    names.extend(crate::tracker_host::runtime::tracker_mirrored_counter_names());
    names.extend(crate::owed_belt::counter_names());
    names.extend(crate::owed_backfill::counter_names());
    names.extend(crate::owed_judge::counter_names());
    names
}

#[test]
fn obs_628_every_name_is_seeded_by_the_overlay() {
    let ours: std::collections::BTreeSet<String> =
        observability_counter_names().into_iter().collect();
    let seeded: std::collections::BTreeSet<String> =
        bsv_overlay_cloudflare::applayer_counters::names()
            .into_iter()
            .collect();
    assert_eq!(
        ours, seeded,
        "the app layer's names and the overlay's seed list"
    );
    for prefix in [
        "courier_applayer_",
        "applayer_txany_memo_",
        "applayer_owed_",
        "applayer_record_refused_",
        "applayer_auth_refused_",
        "applayer_identity_",
    ] {
        assert!(ours.iter().any(|n| n.starts_with(prefix)), "{prefix}");
    }
}

/// The flush the router's answer, the front door's refusals and the exempt identity routes ride, and the
/// BoardView actor's (where the owed belt's tick runs).
#[test]
fn obs_628_every_door_flushes_under_the_due_rule() {
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let lib = squash(include_str!("lib.rs"));
    let door = &lib[lib.find("auth::FrontDoor::Reply(mutresp)=>{").unwrap()..];
    assert!(door[..120].contains("flush_counters_if_due(&ctx,&env);"));
    let exempt = &lib[lib
        .find("identity::serve_exempt(req,&env).await?;")
        .unwrap()..];
    assert!(exempt[..400].contains("flush_counters_if_due(&ctx,&env);"));
    assert!(lib.contains(
        "letpending=courier::take_pending_if_due(worker::Date::now().as_millis()asf64);"
    ));
    let actor = squash(include_str!("board_view.rs"));
    for arm in [
        "letout=self.serve_fetch(req).await;self.flush_counters_if_due();out",
        "letout=self.serve_alarm().await;self.flush_counters_if_due();out",
    ] {
        assert!(actor.contains(arm), "{arm}");
    }
}

/// bsv-low #566 (the W1-OBS-APPLAYER lens's M1): `recomputeSeq` is PER IDENTITY, stored in the identity's
/// `owed_state` row and bumped by the judge's state statement once per judgement (bsv-low #893: the judge's batch,
/// where it was the walk's snapshot upsert), monotone across isolates (it lives in the row), and served on the
/// `/owed` answer beside `computedAtMs`. Real SQLite under the overlay's migrations, the judge's shipped statements.
#[test]
fn obs_566_recompute_seq_is_per_identity_and_rides_the_state_row() {
    use crate::owed_judge::{STATE_CREATE_SQL, STATE_UPDATE_SQL};
    const A: &str = "02aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const B: &str = "03bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    for sql in bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS {
        if let Err(e) = conn.execute_batch(sql) {
            assert!(
                bsv_overlay_cloudflare::d1::migration_error_is_benign(sql, &e.to_string()),
                "{sql}: {e}"
            );
        }
    }
    let judge = |identity: &str, at: i64| -> i64 {
        conn.execute(STATE_CREATE_SQL, rusqlite::params![identity, at])
            .unwrap();
        conn.execute(
            STATE_UPDATE_SQL,
            rusqlite::params![identity, at, 900_000, 0],
        )
        .unwrap();
        conn.query_row(crate::owed_read::STATE_SQL, [identity], |r| {
            r.get("recomputeSeq")
        })
        .unwrap()
    };
    assert_eq!(judge(A, 1_000), 1);
    assert_eq!(
        judge(A, 2_000),
        2,
        "two judgements of one identity: N then N+1"
    );
    assert_eq!(
        judge(B, 1_200),
        1,
        "a second identity counts from its own first judgement"
    );
    assert_eq!(judge(A, 3_000), 3, "and does not move the first's");
    // the column reaches a cold isolate's schema and the overlay's migrations, byte-identical
    assert!(crate::schema::LATCH_COLUMN_ALTERS.contains(&crate::owed::OWED_STATE_SEQ_ALTER));
    assert!(
        bsv_overlay_cloudflare::d1::OVERLAY_MIGRATIONS.contains(&crate::owed::OWED_STATE_SEQ_ALTER)
    );
}

/// The W1-OBS-APPLAYER lens's L2: the isolate field `/health.auth.authRefused` is the twin of the durable
/// `applayer_auth_refused_total`, so both count EVERY front-door refusal: the middleware's arms and the app's three
/// (`mismatch`, `strict_unauthenticated`, `misconfigured`, each still counted apart in its own isolate field). One
/// function bumps both, so they cannot drift again.
#[test]
fn obs_628_the_isolate_auth_refused_counts_every_arm_the_durable_total_does() {
    use crate::auth::{
        count_misconfigured_refused, count_mismatch_refused, count_strict_refused_unauthenticated,
        counters_snapshot,
    };
    let before = counters_snapshot().auth_refused;
    count_mismatch_refused();
    count_strict_refused_unauthenticated();
    count_misconfigured_refused();
    let d = drained();
    assert_eq!(d["applayer_auth_refused_total"], 3);
    assert!(
        counters_snapshot().auth_refused >= before + 3,
        "the isolate field counts the app's three arms as the durable total does"
    );
    let squash = |s: &str| s.split_whitespace().collect::<String>();
    let src = squash(include_str!("auth.rs"));
    assert!(
        src.contains(&squash(
            "fn note_auth_refused(reason: &str) { AUTH_REFUSED.fetch_add(1, Ordering::Relaxed); crate::courier::note_presence(\"applayer_auth_refused_total\");"
        )),
        "one function bumps the isolate field and the durable total"
    );
    assert_eq!(src.matches("AUTH_REFUSED.fetch_add").count(), 1);
}
