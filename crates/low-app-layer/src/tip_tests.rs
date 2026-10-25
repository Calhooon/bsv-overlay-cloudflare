use super::tip_reply_with;
use crate::tracker_host::{headers_with, last_verified_tip, VerifiedTip};
use bsv_rs::transaction::MerklePath;
use bsv_tracker::{HeaderError, Headers, Proof};
use serde_json::{json, Value};
use std::{
    cell::{Cell, RefCell},
    future::ready,
};

const AT: u64 = 1_790_000_000_000;
const VERIFIED_HEIGHT: u32 = 970_001;
const COURIER_MAX: u32 = 990_000;
const FRESHNESS_MS: u64 = 30 * 60 * 1_000;

fn tip() -> VerifiedTip {
    VerifiedTip {
        height: VERIFIED_HEIGHT,
        hash: "02".repeat(32),
        merkle_root: "01".repeat(32),
        at: AT,
        last_pass_at: Some(AT),
        last_seen_height: Some(VERIFIED_HEIGHT),
    }
}

fn header() -> Value {
    json!({"status": "success", "value": {
        "height": VERIFIED_HEIGHT,
        "hash": tip().hash,
        "merkleRoot": "01".repeat(32),
        "time": AT / 1_000
    }})
}

fn info() -> Value {
    json!({"status": "success", "value": {
        "heightLive": VERIFIED_HEIGHT,
        "validationComplete": true,
        "validatedHeight": VERIFIED_HEIGHT,
        "lastSyncedHeight": VERIFIED_HEIGHT,
        "lastSyncedAt": "2026-09-21T14:13:20Z",
        "lastPassAt": "2026-09-21T14:13:20Z",
        "lastSeenHeight": VERIFIED_HEIGHT
    }})
}

#[tokio::test]
async fn tip877_fold2_a_ninety_minute_old_header_with_a_fresh_pass_is_200() {
    let mut quiet = header();
    quiet["value"]["time"] = json!(AT / 1_000 - 90 * 60);
    let mut sync = info();
    sync["value"]["lastSyncedAt"] = json!("2026-01-01T00:00:00Z");
    let reply = tip_reply_with(
        |route| {
            ready(match route.as_str() {
                "getInfo" => Ok((200, sync.clone())),
                _ => response(route, quiet.clone()),
            })
        },
        || None,
        || AT + 20_000,
    )
    .await;
    assert_eq!(reply.status, 200, "a quiet chain is a healthy snapshot");
    assert_eq!(reply.body["at"], AT - 90 * 60_000);
    assert_eq!(reply.body["ageMs"], 20_000);
    assert_eq!(reply.body["freshness"], "fresh");
    assert_eq!(reply.body["lastPassAt"], AT);
}

#[tokio::test]
async fn tip877_fold2_a_five_minute_old_header_with_a_forty_minute_old_pass_is_503() {
    let mut young = header();
    young["value"]["time"] = json!(AT / 1_000 + 35 * 60);
    let mut sync = info();
    sync["value"]["lastSyncedAt"] = json!("2026-09-21T14:53:20Z");
    let reply = tip_reply_with(
        |route| {
            ready(match route.as_str() {
                "getInfo" => Ok((200, sync.clone())),
                _ => response(route, young.clone()),
            })
        },
        || None,
        || AT + 40 * 60_000,
    )
    .await;
    assert_eq!(
        reply.status, 503,
        "a young block or new tip cannot freshen a stale pass"
    );
    assert_eq!(reply.body["ageMs"], 40 * 60_000);
    assert_eq!(reply.body["freshness"], "stale");
    assert_eq!(reply.body["code"], "ERR_TIP_UNAVAILABLE");
}

#[tokio::test]
async fn tip877_fold2_absent_last_pass_on_a_quiet_chain_is_200_unknown() {
    let mut quiet = header();
    quiet["value"]["time"] = json!(AT / 1_000 - 90 * 60);
    let mut sync = info();
    sync["value"].as_object_mut().unwrap().remove("lastPassAt");
    let reply = tip_reply_with(
        |route| {
            ready(if route == "getInfo" {
                Ok((200, sync.clone()))
            } else {
                response(route, quiet.clone())
            })
        },
        || None,
        || AT + 90 * 60_000,
    )
    .await;
    assert_eq!(reply.status, 200);
    assert_eq!(reply.body["height"], VERIFIED_HEIGHT);
    assert_eq!(reply.body["at"], AT - 90 * 60_000);
    assert_eq!(reply.body["freshness"], "unknown");
    assert!(reply.body["lastPassAt"].is_null());
    assert!(reply.body["ageMs"].is_null());
    assert!(reply.body.get("code").is_none());
    assert_eq!(reply.retry_after, None);
}

#[tokio::test]
async fn tip877_fold2_legacy_stamps_are_never_liveness_inputs() {
    for pass_present in [false, true] {
        for legacy in [
            None,
            Some(Value::Null),
            Some(json!("not a timestamp")),
            Some(json!("2026-01-01T00:00:00Z")),
        ] {
            let mut sync = info();
            if !pass_present {
                sync["value"].as_object_mut().unwrap().remove("lastPassAt");
            }
            for key in ["lastSyncedAt", "lastSeenAt"] {
                match &legacy {
                    Some(value) => sync["value"][key] = value.clone(),
                    None => {
                        sync["value"].as_object_mut().unwrap().remove(key);
                    }
                }
            }
            let reply = tip_reply_with(
                |route| {
                    ready(if route == "getInfo" {
                        Ok((200, sync.clone()))
                    } else {
                        response(route, header())
                    })
                },
                || Some(tip()),
                || AT + 123,
            )
            .await;
            assert_eq!(reply.status, 200);
            assert_eq!(
                reply.body["freshness"],
                if pass_present { "fresh" } else { "unknown" }
            );
            assert_eq!(
                reply.body["lastPassAt"],
                if pass_present { json!(AT) } else { Value::Null }
            );
        }
    }
}

#[tokio::test]
async fn tip877_fold2_proof_snapshot_keeps_two_plus_n_requests_without_info() {
    for moving in [false, true] {
        let calls = RefCell::new(Vec::new());
        let reads = Cell::new(0);
        let snapshot = headers_with(
            |route| {
                calls.borrow_mut().push(route.clone());
                if route == "findChainTipHeaderHex" {
                    let n = reads.get();
                    reads.set(n + 1);
                    let mut frame = header();
                    if moving && n % 2 != 0 {
                        frame["value"]["hash"] = json!("03".repeat(32));
                    }
                    ready(response(route, frame))
                } else {
                    ready(tracker_response(route))
                }
            },
            &[VERIFIED_HEIGHT - 1, VERIFIED_HEIGHT],
        )
        .await;
        assert_eq!(snapshot.tip_height().is_err(), moving);
        let calls = calls.into_inner();
        assert!(!calls.iter().any(|r| r == "getInfo"), "{calls:?}");
        assert_eq!(
            calls.len(),
            if moving { 8 } else { 4 },
            "2 + N, once or twice"
        );
    }
}

#[tokio::test]
async fn tip877_fold2_direct_verified_height_is_one_request_without_info() {
    let calls = RefCell::new(Vec::new());
    let height = super::chaintracks_present_height_with(|route| {
        calls.borrow_mut().push(route.clone());
        ready(tracker_response(route))
    })
    .await
    .unwrap();
    assert_eq!(height, u64::from(VERIFIED_HEIGHT));
    assert_eq!(calls.into_inner(), ["findChainTipHeaderHex"]);
}

fn response(route: impl AsRef<str>, verified: Value) -> Result<(u16, Value), HeaderError> {
    let mut sync = info();
    for key in ["heightLive", "lastSyncedHeight", "lastSeenHeight"] {
        sync["value"][key] = verified["value"]["height"].clone();
    }
    match route.as_ref() {
        "findChainTipHeaderHex" => Ok((200, verified)),
        "getInfo" => Ok((200, sync)),
        "getPresentHeight" => Ok((200, json!({"status": "success", "value": COURIER_MAX}))),
        route if route.starts_with("findHeaderHexForHeight?height=") => Ok((200, verified)),
        other => panic!("unexpected chaintracks route: {other}"),
    }
}

fn tracker_response(route: impl AsRef<str>) -> Result<(u16, Value), HeaderError> {
    let route = route.as_ref();
    if let Some(height) = route.strip_prefix("findHeaderHexForHeight?height=") {
        let mut frame = header();
        frame["value"]["height"] = json!(height.parse::<u32>().unwrap());
        Ok((200, frame))
    } else {
        response(route, header())
    }
}

#[tokio::test]
async fn tip877_route_serves_verified_header_not_courier_max() {
    let calls = RefCell::new(Vec::new());
    let reply = tip_reply_with(
        |route| {
            calls.borrow_mut().push(route.to_owned());
            ready(response(route, header()))
        },
        || None,
        || AT + 45_123,
    )
    .await;
    assert_eq!(reply.status, 200);
    assert_eq!(reply.body["height"], VERIFIED_HEIGHT);
    assert_eq!(reply.body["hash"], tip().hash);
    assert_eq!(reply.body["source"], "verified-header");
    assert_eq!(
        calls.into_inner(),
        [
            "findChainTipHeaderHex",
            "getInfo",
            "findHeaderHexForHeight?height=970001"
        ]
    );
    assert_eq!(reply.retry_after, None);
}

#[tokio::test]
async fn tip877_age_is_the_last_successful_pass_age_not_the_fetch_age() {
    for age in [45_123, 46_789] {
        let reply = tip_reply_with(
            |route| ready(response(route, header())),
            || None,
            || AT + age,
        )
        .await;
        assert_eq!(reply.status, 200);
        assert_eq!(reply.body["at"], AT);
        assert_eq!(reply.body["ageMs"], age);
    }
}

#[tokio::test]
async fn tip877_unavailable_is_named_503_with_and_without_last_known_tip() {
    for last_known in [None, Some(tip())] {
        let reply = tip_reply_with(
            |_| ready(Err(HeaderError("binding or transport unavailable".into()))),
            || last_known.clone(),
            || AT + 60_123,
        )
        .await;
        assert_eq!(reply.status, 503);
        assert_eq!(reply.body["code"], "ERR_TIP_UNAVAILABLE");
        assert_eq!(reply.body["stale"], true);
        assert_eq!(reply.retry_after, Some("60"));
        if last_known.is_some() {
            assert_eq!(reply.body["height"], VERIFIED_HEIGHT);
            assert_eq!(reply.body["hash"], tip().hash);
            assert_eq!(reply.body["at"], AT);
            assert_eq!(reply.body["ageMs"], 60_123);
        } else {
            assert!(reply.body["height"].is_null());
            assert!(reply.body["ageMs"].is_null());
        }
    }
}

#[tokio::test]
async fn tip877_stale_pass_is_503_past_thirty_minutes() {
    for age in [FRESHNESS_MS, FRESHNESS_MS + 1] {
        let reply = tip_reply_with(
            |route| ready(response(route, header())),
            || None,
            || AT + age,
        )
        .await;
        assert_eq!(reply.status, if age == FRESHNESS_MS { 200 } else { 503 });
        assert_eq!(reply.body["height"], VERIFIED_HEIGHT);
        assert_eq!(reply.body["ageMs"], age);
        if age > FRESHNESS_MS {
            assert_eq!(reply.body["code"], "ERR_TIP_UNAVAILABLE");
            assert_eq!(reply.body["stale"], true);
            assert_eq!(reply.retry_after, Some("60"));
        }
    }
}

#[tokio::test]
async fn tip877_http_error_and_malformed_snapshot_cannot_serve_courier_max() {
    let mut missing_time = header();
    missing_time["value"]
        .as_object_mut()
        .unwrap()
        .remove("time");
    let mut bad_hash = header();
    bad_hash["value"]["hash"] = json!("bad");
    let mut bad_root = header();
    bad_root["value"]["merkleRoot"] = json!("bad");
    let mut bad_height = header();
    bad_height["value"]["height"] = json!(u64::from(u32::MAX) + 1);
    let mut bad_time = header();
    bad_time["value"]["time"] = json!(-1);
    for (status, frame) in [
        (503, header()),
        (404, Value::Null),
        (200, json!({"status": "error", "value": header()["value"]})),
        (200, missing_time),
        (200, bad_hash),
        (200, bad_root),
        (200, bad_height),
        (200, bad_time),
        (200, json!({"status": "success", "value": COURIER_MAX})),
    ] {
        let reply = tip_reply_with(
            |route| match route.as_str() {
                "findChainTipHeaderHex" => ready(Ok((status, frame.clone()))),
                _ => ready(response(route, header())),
            },
            || Some(tip()),
            || AT + 123,
        )
        .await;
        assert_eq!(reply.status, 503, "{status}: {frame}");
        assert_eq!(reply.body["code"], "ERR_TIP_UNAVAILABLE");
        assert_eq!(reply.body["height"], VERIFIED_HEIGHT);
        assert_eq!(reply.body["ageMs"], 123);
        assert_eq!(reply.body["stale"], true);
    }
}

#[tokio::test]
async fn tip877_tracker_and_route_share_the_verified_snapshot() {
    let snapshot = headers_with(
        |route| ready(tracker_response(&route)),
        &[VERIFIED_HEIGHT, COURIER_MAX],
    )
    .await;
    let reply = tip_reply_with(|route| ready(tracker_response(route)), || None, || AT + 10).await;
    assert_eq!(snapshot.tip_height().unwrap(), VERIFIED_HEIGHT);
    assert_eq!(reply.body["height"], snapshot.tip_height().unwrap());
    assert_eq!(
        reply.body["hash"],
        snapshot.header_at(VERIFIED_HEIGHT).unwrap().unwrap().hash
    );

    let txid = "01".repeat(32);
    let proof = |height| Proof::new(&txid, MerklePath::from_coinbase_txid(&txid, height)).unwrap();
    assert!(snapshot.check(proof(VERIFIED_HEIGHT)).is_ok());
    assert!(snapshot.check(proof(COURIER_MAX)).is_err());
}

#[tokio::test]
async fn tip877_a_same_height_reorg_cannot_pass_the_snapshot_bracket() {
    let reads = Cell::new(0);
    let snapshot = headers_with(
        |route| {
            if route == "findChainTipHeaderHex" {
                let n = reads.get();
                reads.set(n + 1);
                let mut frame = header();
                if n % 2 != 0 {
                    frame["value"]["hash"] = json!("03".repeat(32));
                }
                ready(response(&route, frame))
            } else {
                ready(tracker_response(&route))
            }
        },
        &[VERIFIED_HEIGHT],
    )
    .await;
    assert!(snapshot.tip_height().is_err());
    assert_eq!(reads.get(), 4, "one retry of the changed snapshot");
}

#[tokio::test]
async fn tip877_last_known_follows_a_lower_verified_tip_and_keeps_its_pass_age() {
    let mut lower = header();
    lower["value"]["height"] = json!(VERIFIED_HEIGHT - 2);
    lower["value"]["hash"] = json!("03".repeat(32));
    lower["value"]["time"] = json!(AT / 1_000 - 1);
    let reply = tip_reply_with(
        |route| ready(response(route, lower.clone())),
        || Some(tip()),
        || AT + 10,
    )
    .await;
    assert_eq!(reply.status, 200);
    assert_eq!(reply.body["height"], VERIFIED_HEIGHT - 2);
    assert_eq!(reply.body["ageMs"], 10);
    assert_eq!(last_verified_tip().unwrap().height, VERIFIED_HEIGHT - 2);

    let unavailable = tip_reply_with(
        |_| ready(Err(HeaderError("transport".into()))),
        last_verified_tip,
        || AT + 20,
    )
    .await;
    assert_eq!(unavailable.status, 503);
    assert_eq!(unavailable.body["height"], VERIFIED_HEIGHT - 2);
    assert_eq!(unavailable.body["hash"], "03".repeat(32));
    assert_eq!(unavailable.body["at"], AT - 1_000);
    assert_eq!(unavailable.body["ageMs"], 20);
    assert_eq!(unavailable.body["stale"], true);
}

#[tokio::test]
async fn tip877_age_uses_the_clock_after_the_read_and_never_goes_negative() {
    let now = Cell::new(AT);
    let reply = tip_reply_with(
        |route| {
            now.set(AT + 123);
            ready(response(route, header()))
        },
        || None,
        || now.get(),
    )
    .await;
    assert_eq!(reply.body["ageMs"], 123);
    let reply = tip_reply_with(|route| ready(response(route, header())), || None, || AT - 1).await;
    assert_eq!(reply.status, 200);
    assert_eq!(reply.body["ageMs"], 0);
}

#[tokio::test]
async fn tip877_failure_reads_last_known_after_the_await() {
    let last = RefCell::new(None);
    let reply = tip_reply_with(
        |_| {
            // Another read can learn the tip while this request is in flight.
            *last.borrow_mut() = Some(tip());
            ready(Err(HeaderError("transport".into())))
        },
        || last.borrow().clone(),
        || AT + 123,
    )
    .await;
    assert_eq!(reply.status, 503);
    assert_eq!(reply.body["height"], VERIFIED_HEIGHT);
    assert_eq!(reply.body["ageMs"], 123);
    assert_eq!(reply.body["stale"], true);
}

#[tokio::test]
async fn tip877_fold_seen_height_is_bounded_by_the_reorg_margin() {
    for (lead, pass_present) in [0, 1, 2, 3]
        .into_iter()
        .flat_map(|lead| [false, true].map(|present| (lead, present)))
    {
        let mut sync = info();
        sync["value"]["lastSeenHeight"] = json!(VERIFIED_HEIGHT + lead);
        if !pass_present {
            sync["value"].as_object_mut().unwrap().remove("lastPassAt");
        }
        let reply = tip_reply_with(
            |route| {
                ready(if route == "getInfo" {
                    Ok((200, sync.clone()))
                } else {
                    response(route, header())
                })
            },
            || None,
            || AT + 20_000,
        )
        .await;
        assert_eq!(
            reply.status,
            if lead <= 1 { 200 } else { 503 },
            "lead {lead}"
        );
        assert_eq!(reply.body["height"], VERIFIED_HEIGHT);
        assert_eq!(
            reply.body["ageMs"],
            if pass_present {
                json!(20_000)
            } else {
                Value::Null
            }
        );
        assert_eq!(
            reply.body["freshness"],
            if lead > 1 {
                "stale"
            } else if pass_present {
                "fresh"
            } else {
                "unknown"
            }
        );
        if lead > 1 {
            assert_eq!(reply.body["code"], "ERR_TIP_UNAVAILABLE");
            assert_eq!(reply.retry_after, Some("60"));
        }
    }
}

#[tokio::test]
async fn tip877_fold_pass_frames_and_their_timestamps_must_be_usable() {
    let mut frames = vec![
        (503, info()),
        (404, Value::Null),
        (200, json!({"status":"error"})),
    ];
    for (key, value) in [
        ("lastPassAt", Value::Null),
        ("lastPassAt", json!("2026-09-21 14:13:20 garbage")),
        ("lastPassAt", json!("not a timestamp")),
        ("lastPassAt", json!("4294967295-09-21T14:13:20Z")),
        ("lastSeenHeight", Value::Null),
        ("lastSeenHeight", json!(u64::from(u32::MAX) + 1)),
        ("heightLive", Value::Null),
        ("validationComplete", Value::Null),
    ] {
        let mut sync = info();
        sync["value"][key] = value;
        frames.push((200, sync));
    }
    for (status, sync) in frames {
        let reply = tip_reply_with(
            |route| {
                ready(if route == "getInfo" {
                    Ok((status, sync.clone()))
                } else {
                    response(route, header())
                })
            },
            || Some(tip()),
            || AT + 123,
        )
        .await;
        assert_eq!(reply.status, 503, "{status}: {sync}");
        assert_eq!(reply.body["stale"], true);
        assert_eq!(reply.body["ageMs"], 123);
    }
    for stamp in [
        "2026-09-21 14:13:20",
        "2026-09-21T14:13:20Z",
        "2026-09-21T14:13:20.000+00:00",
    ] {
        let mut sync = info();
        sync["value"]["lastPassAt"] = json!(stamp);
        let reply = tip_reply_with(
            |route| {
                ready(if route == "getInfo" {
                    Ok((200, sync.clone()))
                } else {
                    response(route, header())
                })
            },
            || None,
            || AT + 123,
        )
        .await;
        assert_eq!(reply.status, 200, "{stamp}");
        assert_eq!(reply.body["ageMs"], 123);
    }
}

#[tokio::test]
async fn tip877_fold_tip_is_bound_to_the_active_header_at_its_height() {
    let mut frames = vec![(404, Value::Null), (503, header())];
    for (key, value) in [
        ("height", json!(VERIFIED_HEIGHT + 1)),
        ("hash", json!("03".repeat(32))),
        ("merkleRoot", json!("04".repeat(32))),
        ("time", json!(AT / 1_000 + 1)),
    ] {
        let mut active = header();
        active["value"][key] = value;
        frames.push((200, active));
    }
    for (status, active) in frames {
        let reply = tip_reply_with(
            |route| {
                ready(if route.starts_with("findHeaderHexForHeight?") {
                    Ok((status, active.clone()))
                } else {
                    response(route, header())
                })
            },
            || Some(tip()),
            || AT + 123,
        )
        .await;
        assert_eq!(reply.status, 503, "{status}: {active}");
    }
}

#[tokio::test]
async fn tip877_fold_info_and_tip_must_agree_under_the_validation_ceiling() {
    for complete in [true, false] {
        let mut sync = info();
        sync["value"]["heightLive"] = json!(VERIFIED_HEIGHT + 1);
        sync["value"]["validationComplete"] = json!(complete);
        let reply = tip_reply_with(
            |route| {
                ready(if route == "getInfo" {
                    Ok((200, sync.clone()))
                } else {
                    response(route, header())
                })
            },
            || None,
            || AT + 123,
        )
        .await;
        assert_eq!(reply.status, if complete { 503 } else { 200 });
    }
}

#[tokio::test]
async fn tip877_fold_a_pass_refresh_is_not_read_by_the_proof_snapshot() {
    let reads = Cell::new(0);
    let snapshot = headers_with(
        |route| {
            ready(if route == "getInfo" {
                let mut sync = info();
                sync["value"]["lastPassAt"] = json!(if reads.get() == 0 {
                    "2026-09-21T14:13:20Z"
                } else {
                    "2026-09-21T14:13:21Z"
                });
                reads.set(reads.get() + 1);
                Ok((200, sync))
            } else {
                tracker_response(route)
            })
        },
        &[VERIFIED_HEIGHT],
    )
    .await;
    assert_eq!(snapshot.tip_height().unwrap(), VERIFIED_HEIGHT);
    assert_eq!(reads.get(), 0);
}

#[tokio::test]
async fn tip877_fold_tip_503_recomputes_proven_rows_and_marks_unknown_gates() {
    use crate::hops_view::HopStatus;
    use crate::owed::{
        self,
        tests::{entry, hop, inputs, tx, ME},
        OwedBind, OwedFamily,
    };
    use crate::results::{Outcome, PotVerdict, SeatLetter};
    use std::collections::{BTreeMap, HashMap, HashSet};

    let db = rusqlite::Connection::open_in_memory().unwrap();
    db.execute_batch(owed::OWED_ROWS_CREATE).unwrap();
    db.execute_batch(owed::OWED_STATE_CREATE).unwrap();
    db.execute_batch("ALTER TABLE owed_state ADD COLUMN recomputeSeq INTEGER NOT NULL DEFAULT 0")
        .unwrap();
    db.execute(
        "INSERT INTO owed_state(identity, computedAtMs, tip, rows) VALUES (?1, 1, ?2, 1)",
        rusqlite::params![ME, VERIFIED_HEIGHT],
    )
    .unwrap();
    db.execute("INSERT INTO owed_rows(identity, outpoint, family, gameId, facts, updatedAtMs) VALUES (?1, ?2, 'in-progress', ?3, '{\"claim\":\"rejoin\",\"claimable\":true}', 1)",
        rusqlite::params![ME, owed::outpoint_key(&tx(2), 0), tx(1)]).unwrap();

    let mut won = entry(
        Some(true),
        Some(PotVerdict::WinnerA),
        Outcome::Won,
        Some(SeatLetter::A),
    );
    won.at_height = None; // The index's latch alone decides, even when the word cannot be checked.
    let mut gated = entry(Some(false), None, Outcome::Unresolved, Some(SeatLetter::A));
    gated.pot_txid = tx(9);
    let results = [won, gated];
    let hops = [hop(
        HopStatus::Unspent,
        None,
        Some(owed::HOP_STRANDED_AFTER_MS + 1),
    )];
    let (valid, collected, pots) = (HashMap::new(), HashSet::new(), HashSet::new());
    let words = BTreeMap::from([(tx(3), json!({"word":"stale", "verifyPath":null}))]);
    let reply = tip_reply_with(
        |_| ready(Ok((503, json!({"status":"error"})))),
        || Some(tip()),
        || AT + 123,
    )
    .await;
    assert_eq!(reply.status, 503);
    let db_ref = &db;
    let computed = super::owed_recompute_with(
        None,
        async {
            (reply.status == 200)
                .then(|| reply.body["height"].as_u64())
                .flatten()
        },
        |tip| async move {
            let db = db_ref;
            assert_eq!(tip, None, "503 diagnostics are never a gate height");
            let mut i = inputs(&results, &[], &hops, &valid, &collected, &pots, tip);
            i.spender_words = Some(&words);
            let rows = owed::derive_owed_rows(&i);
            for (sql, binds) in owed::owed_write_plan(ME, &rows, i.now_ms, tip, false) {
                let binds: Vec<rusqlite::types::Value> = binds
                    .into_iter()
                    .map(|b| match b {
                        OwedBind::Text(v) => v.into(),
                        OwedBind::Int(v) => v.into(),
                        OwedBind::Null => rusqlite::types::Value::Null,
                    })
                    .collect();
                if sql == owed::OWED_STATE_STAMP_SQL {
                    let stamp: i64 = db
                        .query_row(sql, rusqlite::params_from_iter(binds), |r| r.get(0))
                        .unwrap();
                    assert_eq!(stamp, i.now_ms);
                } else {
                    db.execute(sql, rusqlite::params_from_iter(binds)).unwrap();
                }
            }
            Ok(rows)
        },
    )
    .await
    .expect("a missing tip must not stop the proof-decided recompute");
    let payout = computed
        .iter()
        .find(|r| r.outpoint == owed::outpoint_key(&tx(2), 0))
        .unwrap();
    assert_eq!(payout.family, OwedFamily::Payout);
    assert_eq!(payout.facts["claimable"], true);
    let gate = computed
        .iter()
        .find(|r| r.outpoint == owed::outpoint_key(&tx(9), 0))
        .unwrap();
    assert_eq!(gate.facts["tipUnknown"], true);
    assert_ne!(gate.facts["claimable"], true);
    assert!(gate.facts["tip"].is_null());
    let probe = computed
        .iter()
        .find(|r| r.family == OwedFamily::HopStranded)
        .unwrap();
    assert_eq!(probe.facts["tipUnknown"], true);
    assert_eq!(probe.facts["claimable"], false);
    assert!(probe.facts["claim"].is_null());
    assert_eq!(
        probe.reason.as_deref(),
        Some(owed::SWEEP_TIP_UNKNOWN_REASON)
    );
    for row in [gate, probe] {
        assert!(!owed::should_recompute(
            false,
            100,
            None,
            None,
            std::slice::from_ref(row)
        ));
        assert!(
            owed::should_recompute(
                false,
                100,
                None,
                Some(u64::from(VERIFIED_HEIGHT)),
                std::slice::from_ref(row)
            ),
            "an unknown gate must re-arm when the tip is readable"
        );
    }
    let stored_tip: Option<u64> = db
        .query_row(
            "SELECT tip FROM owed_state WHERE identity = ?1",
            [ME],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(stored_tip, None);
    let stored_claim: bool = db.query_row("SELECT json_extract(facts, '$.claimable') FROM owed_rows WHERE identity = ?1 AND outpoint = ?2",
        rusqlite::params![ME, owed::outpoint_key(&tx(2), 0)], |r| r.get(0)).unwrap();
    assert!(stored_claim, "the settled row was written during the 503");
}
