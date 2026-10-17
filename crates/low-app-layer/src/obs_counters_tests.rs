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
        crate::owed::note_recompute_shed();
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

#[test]
fn obs_628_the_owed_faults_and_the_recompute_seq_are_mirrored() {
    let before = crate::owed::owed_health_json();
    crate::owed::note_recompute_fault();
    crate::owed::note_recompute_shed();
    crate::owed::note_recompute_deferred_abandoned();
    crate::owed::note_recompute_deferred_faulted();
    crate::owed::note_recompute_arm_failed();
    crate::owed::note_spender_read_fault();
    crate::owed::note_markerless_read_fault();
    crate::owed::note_sweep_proof_contradictions(3);
    crate::owed::note_recompute("read-first", &[]);
    let d = drained();
    for (name, want) in [
        ("applayer_owed_recompute_faults_total", 1),
        ("applayer_owed_recompute_shed_total", 1),
        ("applayer_owed_recompute_deferred_abandoned_total", 1),
        ("applayer_owed_recompute_deferred_faulted_total", 1),
        ("applayer_owed_recompute_arm_failed_total", 1),
        ("applayer_owed_read_faults_total", 2),
        ("applayer_owed_read_faults_spender_total", 1),
        ("applayer_owed_read_faults_markerless_total", 1),
        ("applayer_owed_sweep_proof_contradictions_total", 3),
        (crate::owed::COUNTER_RECOMPUTE_SEQ, 1),
    ] {
        assert_eq!(d.get(name).copied(), Some(want), "{name} in {d:?}");
    }
    let after = crate::owed::owed_health_json();
    let seq = |v: &serde_json::Value| v["recomputeSeq"].as_u64().unwrap();
    assert!(
        seq(&after) > seq(&before),
        "the isolate's recomputeSeq is served beside them"
    );
}

/// The isolate fields on `/health.owed` keep their keys; `recomputeSeq` is the one added.
#[test]
fn obs_628_the_owed_health_keys_are_the_old_ones_and_recompute_seq() {
    let v = crate::owed::owed_health_json();
    let keys: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
    let mut want = vec![
        "countersScope",
        "recomputeBySource",
        "rowsWrittenByFamily",
        "recomputeFaults",
        "recomputeCoalesced",
        "recomputeLockTakeovers",
        "recomputeWritesSuperseded",
        "recomputeShed",
        "owed_recompute_shed_total",
        "owed_recompute_deferred_total",
        "owed_recompute_arm_failed_total",
        "owed_recompute_deferred_abandoned_total",
        "owed_recompute_deferred_faulted_total",
        "recomputesPerIdentityPerMinute",
        "recomputeCeilingScope",
        "collectedReadFaults",
        "heldFilingsMatched",
        "heldFilingsKeyMismatch",
        "payVoutReadFaults",
        "potSpendersReadFaults",
        "hopSweepsReadFaults",
        "sweepProofsReadFaults",
        "markerlessReadFaults",
        "markerlessIgnoredMarkers",
        "probeMemoReadFaults",
        "sweepProofContradictions",
        "owed_swept_confirmed_by_proof_total",
        "spenderReadFaults",
        "withheldPreviousReadFaults",
        "spenderStoreHits",
        "spenderStorePersisted",
        "spenderStoreCarried",
        "spenderStoreReadFaults",
        "spenderStoreSweepPasses",
        "spenderReadsPerRecompute",
        "recomputeTimeBudgetMs",
        "readRefreshesKicked",
        "readRefreshesSkippedInFlight",
        "hopStrandedAfterMs",
        "recomputeOpenAfterMs",
        "recomputeAnyAfterMs",
        "recomputeSeq",
    ];
    let mut keys = keys;
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
/// BoardView actor's (where the deferred owed walk runs).
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
