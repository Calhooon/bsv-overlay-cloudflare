use crate::tracker_host::{headers_with, last_verified_tip, VerifiedTip};
use bsv_rs::transaction::MerklePath;
use bsv_tracker::{HeaderError, Headers, Proof};
use serde_json::{json, Value};
use std::{
    cell::{Cell, RefCell},
    future::ready,
};

/// The reply over a live read that never passes its deadline (the tests' own
/// wrapper of `tip_reply_within`; kept here so `routes.rs`'s production text
/// ends at its first `#[cfg(test)]`, which several source pins cut at).
async fn tip_reply_with<G, F, L, N>(get: G, last_known: L, now_ms: N) -> super::TipReply
where
    G: Fn(String) -> F,
    F: std::future::Future<
        Output = std::result::Result<(u16, serde_json::Value), bsv_tracker::HeaderError>,
    >,
    L: FnOnce() -> Option<crate::tracker_host::VerifiedTip>,
    N: Fn() -> u64,
{
    super::tip_reply_within(
        get,
        last_known,
        now_ms,
        std::future::pending::<()>(),
        || ready(Vec::new()),
        Default::default(),
    )
    .await
}

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
        live_lag_blocks: None,
        linked: None,
        read_at: Some(AT),
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
        if last_known.is_some() {
            // bsv-low #1081: a transport fault serves the fresh memo, 200.
            assert_eq!(reply.status, 200);
            assert_eq!(reply.body["servedFrom"], "memo");
            assert_eq!(reply.body["freshness"], "fresh");
            assert_eq!(reply.body["height"], VERIFIED_HEIGHT);
            assert_eq!(reply.body["hash"], tip().hash);
            assert_eq!(reply.body["at"], AT);
            assert_eq!(reply.body["ageMs"], 60_123);
        } else {
            assert_eq!(reply.status, 503);
            assert_eq!(reply.body["code"], "ERR_TIP_UNAVAILABLE");
            assert_eq!(reply.body["stale"], true);
            assert_eq!(reply.retry_after, Some("60"));
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
        // bsv-low #1081: the store's 5xx is a read fault, the fresh memo's
        // case; a frame that does not check is the store's word, a 503.
        let faulted = status >= 500;
        assert_eq!(
            reply.status,
            if faulted { 200 } else { 503 },
            "{status}: {frame}"
        );
        assert_eq!(reply.body["height"], VERIFIED_HEIGHT);
        assert_eq!(reply.body["ageMs"], 123);
        if !faulted {
            assert_eq!(reply.body["code"], "ERR_TIP_UNAVAILABLE");
            assert_eq!(reply.body["stale"], true);
        }
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
    // bsv-low #1081: the transport fault serves the memo, the lower tip.
    assert_eq!(unavailable.status, 200);
    assert_eq!(unavailable.body["servedFrom"], "memo");
    assert_eq!(unavailable.body["height"], VERIFIED_HEIGHT - 2);
    assert_eq!(unavailable.body["hash"], "03".repeat(32));
    assert_eq!(unavailable.body["at"], AT - 1_000);
    assert_eq!(unavailable.body["ageMs"], 20);
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
    // bsv-low #1081: the memo learned during the read is served, fresh.
    assert_eq!(reply.status, 200);
    assert_eq!(reply.body["servedFrom"], "memo");
    assert_eq!(reply.body["height"], VERIFIED_HEIGHT);
    assert_eq!(reply.body["ageMs"], 123);
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
        // bsv-low #1081: the store's 5xx is the fresh memo's case.
        let faulted = status >= 500;
        assert_eq!(
            reply.status,
            if faulted { 200 } else { 503 },
            "{status}: {sync}"
        );
        assert_eq!(
            reply.body["stale"],
            if faulted { Value::Null } else { json!(true) }
        );
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
    // (status, frame, a disagreement of the active header with the tip)
    let mut frames = vec![(404, Value::Null, false), (503, header(), false)];
    for (key, value) in [
        ("height", json!(VERIFIED_HEIGHT + 1)),
        ("hash", json!("03".repeat(32))),
        ("merkleRoot", json!("04".repeat(32))),
        ("time", json!(AT / 1_000 + 1)),
    ] {
        let mut active = header();
        active["value"][key] = value;
        frames.push((200, active, key != "height"));
    }
    for (status, active, disagrees) in frames {
        for memo in [Some(tip()), None] {
            let reply = tip_reply_with(
                |route| {
                    ready(if route.starts_with("findHeaderHexForHeight?") {
                        Ok((status, active.clone()))
                    } else {
                        response(route, header())
                    })
                },
                || memo.clone(),
                || AT + 123,
            )
            .await;
            // bsv-low #1081: the store's 5xx is the fresh memo's case. APP-L59 fold 1 (C MEDIUM-1): an active header
            // that disagrees with the tip twice is a read fault too (a block moving mid-snapshot), the memo's case;
            // the tip is never trusted over it, and with no memo it is the named 503.
            let served = memo.is_some() && (status >= 500 || disagrees);
            assert_eq!(
                reply.status,
                if served { 200 } else { 503 },
                "{status}: {active}"
            );
            if served {
                assert_eq!(reply.body["servedFrom"], "memo", "{}", reply.body);
            }
        }
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

// ── bsv-low #1081: a read fault serves the isolate's last fresh snapshot ──

/// A memo whose pass and whose read are both `pass_age_ms` old at `AT`.
fn memo_at(pass_age_ms: u64) -> VerifiedTip {
    VerifiedTip {
        last_pass_at: Some(AT - pass_age_ms),
        read_at: Some(AT - pass_age_ms),
        ..tip()
    }
}

#[tokio::test]
async fn fix1081_a_an_aborted_read_with_a_ten_second_old_memo_serves_200_fresh() {
    // The store canceled the read (L59 pair 29, 20:00:26Z) and the transport
    // answered an error; the memo's pass is 10 s old.
    let reply = tip_reply_with(
        |_| {
            ready(Err(HeaderError(
                "canceled: the caller closed the connection before the answer was sent".into(),
            )))
        },
        || Some(memo_at(10_000)),
        || AT,
    )
    .await;
    assert_eq!(reply.status, 200, "{}", reply.body);
    assert_eq!(reply.body["freshness"], "fresh");
    assert_eq!(reply.body["ageMs"], 10_000);
    assert_eq!(reply.body["height"], VERIFIED_HEIGHT);
    assert_eq!(reply.body["hash"], tip().hash);
    assert!(reply.body.get("code").is_none());
    assert_eq!(reply.retry_after, None);
}

#[tokio::test]
async fn fix1081_c_a_memo_past_the_bar_serves_503_stale() {
    let reply = tip_reply_with(
        |_| ready(Err(HeaderError("transport".into()))),
        || Some(memo_at(FRESHNESS_MS + 1)),
        || AT,
    )
    .await;
    assert_eq!(reply.status, 503);
    assert_eq!(reply.body["freshness"], "stale");
    assert_eq!(reply.body["code"], "ERR_TIP_UNAVAILABLE");
    assert_eq!(reply.body["ageMs"], FRESHNESS_MS + 1);
    assert_eq!(reply.retry_after, Some("60"));
}

#[tokio::test]
async fn fix1081_b_a_read_past_the_deadline_serves_the_memo_and_names_it() {
    let reply = super::tip_reply_within(
        |_| std::future::pending(),
        || Some(memo_at(10_000)),
        || AT,
        ready(()),
        || ready(Vec::new()),
        Default::default(),
    )
    .await;
    assert_eq!(reply.status, 200, "{}", reply.body);
    assert_eq!(reply.body["servedFrom"], "memo");
    assert_eq!(reply.body["ageMs"], 10_000);
    let fault = reply.body["readFault"].as_str().unwrap();
    assert!(fault.contains("3000 ms deadline"), "{fault}");
    assert!(reply.reason.as_deref().unwrap().contains("deadline"));
    // No memo: the deadline is the 503's reason.
    let reply = super::tip_reply_within(
        |_| std::future::pending(),
        || None,
        || AT,
        ready(()),
        || ready(Vec::new()),
        Default::default(),
    )
    .await;
    assert_eq!(reply.status, 503);
    let reason = reply.reason.unwrap();
    assert!(
        reason.contains("deadline") && reason.contains("no memo"),
        "{reason}"
    );
}

#[tokio::test]
async fn fix1081_d_every_503_names_its_reason_and_the_route_logs_it() {
    // The store's status (a 5xx), the transport, a frame that does not check,
    // and the freshness bar: each 503 carries its reason in the reply and body.
    type Read = Box<dyn Fn(String) -> Result<(u16, Value), HeaderError>>;
    let cases: Vec<(&str, Read, Option<VerifiedTip>, &str)> = vec![
        (
            "5xx",
            Box::new(|route| {
                if route == "getInfo" {
                    Ok((503, Value::Null))
                } else {
                    response(route, header())
                }
            }),
            None,
            "getInfo: the store answered HTTP 503",
        ),
        (
            "transport",
            Box::new(|_| Err(HeaderError("canceled".into()))),
            Some(memo_at(FRESHNESS_MS + 5)),
            "the memo is not fresh",
        ),
        (
            "disagree",
            Box::new(|route| {
                if route.starts_with("findHeaderHexForHeight?") {
                    let mut h = header();
                    h["value"]["hash"] = json!("03".repeat(32));
                    Ok((200, h))
                } else {
                    response(route, header())
                }
            }),
            Some(memo_at(10)),
            "disagreed twice",
        ),
        (
            "stale",
            Box::new(|route| response(route, header())),
            None,
            "past the 1800000 ms bar",
        ),
    ];
    for (name, get, memo, needle) in cases {
        let reply = tip_reply_with(
            |route| ready(get(route)),
            || memo.clone(),
            || AT + FRESHNESS_MS + 1,
        )
        .await;
        assert_eq!(reply.status, 503, "{name}: {}", reply.body);
        let reason = reply.reason.clone().unwrap_or_default();
        assert!(reason.contains(needle), "{name}: {reason}");
        assert_eq!(reply.body["reason"], json!(reason), "{name}");
    }
    // The handler logs the reason of every answer that carries one.
    let source = include_str!("routes.rs");
    let handler = source
        .split("pub async fn tip(")
        .nth(1)
        .unwrap()
        .split("\n}\n")
        .next()
        .unwrap();
    assert!(
        handler.contains("log_tip_reply(&reply)"),
        "the /tip handler logs its reason"
    );
    let log = source
        .split("fn log_tip_reply(")
        .nth(1)
        .unwrap()
        .split("\n}\n")
        .next()
        .unwrap();
    assert!(log.contains("console_warn!(\"[tip] 503 ERR_TIP_UNAVAILABLE: {reason}\")"));
}

// ── bsv-low #1077: the freshness carries the store's own courier lag ──

fn info_with(f: impl Fn(&mut Value)) -> Value {
    let mut sync = info();
    f(&mut sync["value"]);
    sync
}

async fn reply_over_info(sync: Value) -> super::TipReply {
    tip_reply_with(
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
    .await
}

#[tokio::test]
async fn fix1077_a_a_young_pass_two_behind_the_seen_tip_serves_stale() {
    let reply = reply_over_info(info_with(|v| {
        v["lastSeenHeight"] = json!(VERIFIED_HEIGHT + 2);
        v["liveLagBlocks"] = json!(2);
    }))
    .await;
    assert_eq!(reply.status, 503);
    assert_eq!(reply.body["freshness"], "stale");
    assert_eq!(
        reply.body["lagBlocks"], 2,
        "the lag is named: {}",
        reply.body
    );
}

#[tokio::test]
async fn fix1077_b_the_stores_own_lag_word_is_read_beside_its_seen_height() {
    // The store's `liveLagBlocks` (seen less its stored tip) says two blocks
    // where its seen height alone reads none: the larger word stands.
    let reply = reply_over_info(info_with(|v| v["liveLagBlocks"] = json!(2))).await;
    assert_eq!(reply.status, 503, "{}", reply.body);
    assert_eq!(reply.body["freshness"], "stale");
    assert_eq!(reply.body["lagBlocks"], 2);
}

/// APP-L59 fold 1 (C MEDIUM-2) inverted this pin: a broken link state (a hole five blocks below a right tip) is a
/// diagnostic, served as `linked` and logged, never a stale clock.
#[tokio::test]
async fn fix1077_c_a_broken_link_state_is_a_diagnostic_never_stale() {
    let reply = reply_over_info(info_with(|v| {
        v["linkState"] = json!({"from": VERIFIED_HEIGHT - 399, "to": VERIFIED_HEIGHT,
            "linked": false, "broken": [VERIFIED_HEIGHT - 5], "brokenCount": 1});
    }))
    .await;
    assert_eq!(reply.status, 200, "{}", reply.body);
    assert_eq!(reply.body["freshness"], "fresh");
    assert_eq!(reply.body["linked"], false);
    // the handler logs it
    let log = include_str!("routes.rs")
        .split("fn log_tip_reply(")
        .nth(1)
        .unwrap()
        .split("\n}\n")
        .next()
        .unwrap();
    assert!(log.contains("reply.body[\"linked\"] == false"), "{log}");
    // A linked chain, one block of lag: fresh, the lag named.
    let reply = reply_over_info(info_with(|v| {
        v["lastSeenHeight"] = json!(VERIFIED_HEIGHT + 1);
        v["liveLagBlocks"] = json!(1);
        v["linkState"] = json!({"from": 1, "to": VERIFIED_HEIGHT, "linked": true,
            "broken": [], "brokenCount": 0});
    }))
    .await;
    assert_eq!(reply.status, 200, "{}", reply.body);
    assert_eq!(reply.body["freshness"], "fresh");
    assert_eq!(reply.body["lagBlocks"], 1);
}

// ── bsv-low #1084: the /owed read's words recheck their headers at once ──

/// fix1084_a (L59 S-O): an owed list whose mined words sit at ten distinct heights rechecks them under the burst at
/// 300 ms a CHAINTRACKS read. The header reads were made one after another (ten in a row between the two tip reads:
/// 3,600 ms, the read's tail beside the actor's 1.5 s wall and the client's 8 s timeout); they are made at once,
/// eight at a time, so the step is the two tip reads and two header reads' time (1,200 ms). The snapshot's answer is unchanged.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn fix1084_a_the_header_rechecks_of_one_read_run_at_once() {
    let heights: Vec<u32> = (0..10).map(|n| VERIFIED_HEIGHT - 10 + n).collect();
    let started = tokio::time::Instant::now();
    let reads = Cell::new(0u32);
    let snapshot = headers_with(
        |route| {
            reads.set(reads.get() + 1);
            async move {
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                tracker_response(&route)
            }
        },
        &heights,
    )
    .await;
    let ms = started.elapsed().as_millis() as u64;
    assert_eq!(snapshot.tip_height().unwrap(), VERIFIED_HEIGHT);
    for h in &heights {
        assert!(snapshot.header_at(*h).unwrap().is_some(), "height {h}");
    }
    assert_eq!(reads.get(), 12, "two tip reads and one a height");
    let waves = heights
        .len()
        .div_ceil(crate::tracker_host::HEADER_READS_AT_ONCE) as u64;
    assert!(
        ms <= (2 + waves) * 300,
        "the step took {ms} ms of the read's wall: the heights are read one after another"
    );
}

// ── APP-L59 fold 1 (the ensemble's findings) ──

/// A store whose tip moves on its first `moves` snapshot attempts: the tip read answers `VERIFIED_HEIGHT`, then
/// `getInfo` answers one block on (a block landed between the reads); after them every read is at the new block.
fn moving_store(
    moves: usize,
) -> impl Fn(String) -> std::future::Ready<Result<(u16, Value), HeaderError>> {
    let tip_reads = Cell::new(0usize);
    move |route: String| {
        let next = {
            let mut h = header();
            h["value"]["height"] = json!(VERIFIED_HEIGHT + 1);
            h["value"]["hash"] = json!("05".repeat(32));
            h
        };
        if route == "findChainTipHeaderHex" {
            tip_reads.set(tip_reads.get() + 1);
        }
        let attempt = tip_reads.get();
        ready(if attempt <= moves {
            if route == "findChainTipHeaderHex" {
                Ok((200, header()))
            } else {
                response(route, next)
            }
        } else {
            response(route, next)
        })
    }
}

/// fold1_m1 (C MEDIUM-1): a block landing between `/tip`'s three reads is a race, never the store's word. Once: the
/// route reads the snapshot again and answers the new block live. Twice: a read fault, the fresh memo served with its
/// age. RED on d9076aad: 503 ("tip and sync snapshot heights disagree") with a 10 s memo in hand.
#[tokio::test]
async fn fold1_m1_a_mid_snapshot_tip_move_serves_200_fresh() {
    let reply = tip_reply_with(moving_store(1), || Some(memo_at(10_000)), || AT).await;
    assert_eq!(reply.status, 200, "one move, read again: {}", reply.body);
    assert_eq!(reply.body["freshness"], "fresh");
    assert_eq!(
        reply.body["height"],
        VERIFIED_HEIGHT + 1,
        "the new block, live"
    );
    assert!(reply.body.get("servedFrom").is_none(), "{}", reply.body);
    let reply = tip_reply_with(moving_store(usize::MAX), || Some(memo_at(10_000)), || AT).await;
    assert_eq!(reply.status, 200, "two moves, the memo: {}", reply.body);
    assert_eq!(reply.body["freshness"], "fresh");
    assert_eq!(reply.body["servedFrom"], "memo");
    assert_eq!(reply.body["ageMs"], 10_000);
    let fault = reply.body["readFault"].as_str().unwrap();
    assert!(fault.contains("disagree"), "{fault}");
    // No memo: the disagreement is the 503's named fault.
    let reply = tip_reply_with(moving_store(usize::MAX), || None, || AT).await;
    assert_eq!(reply.status, 503);
    let reason = reply.reason.unwrap();
    assert!(
        reason.contains("disagree") && reason.contains("no memo"),
        "{reason}"
    );
}

/// fold1_m2 (C MEDIUM-2): the link state never marks the clock stale. A hole five blocks below a right tip is served
/// fresh, the link state its own field (`linked` false) and logged; a store that names no link state changes nothing.
/// RED on d9076aad: 503 stale.
#[tokio::test]
async fn fold1_m2_a_broken_link_below_a_right_tip_serves_fresh() {
    let reply = reply_over_info(info_with(|v| {
        v["linkState"] = json!({"from": VERIFIED_HEIGHT - 399, "to": VERIFIED_HEIGHT,
            "linked": false, "broken": [VERIFIED_HEIGHT - 5], "brokenCount": 1});
    }))
    .await;
    assert_eq!(reply.status, 200, "{}", reply.body);
    assert_eq!(reply.body["freshness"], "fresh");
    assert_eq!(reply.body["linked"], false, "served as its own field");
    // absent: null, nothing else moves
    let reply = reply_over_info(info()).await;
    assert_eq!(reply.status, 200, "{}", reply.body);
    assert_eq!(reply.body["linked"], Value::Null);
}

/// fold1_l1 (B LOW-1, C LOW-2): the memo's own READ age bounds its freshness: one block's expectation (10 min) after
/// its read the memo serves stale (its lag word is as of that read), the reason named. RED on d9076aad: a 29-minute
/// memo on a read fault served 200 fresh.
#[tokio::test]
async fn fold1_l1_the_memos_read_age_bounds_its_freshness() {
    let fault = |_| ready(Err(HeaderError("transport".into())));
    let reply = tip_reply_with(fault, || Some(memo_at(29 * 60 * 1_000)), || AT).await;
    assert_eq!(reply.status, 503, "{}", reply.body);
    assert_eq!(reply.body["freshness"], "stale");
    let reason = reply.reason.unwrap();
    assert!(reason.contains("read 1740000 ms ago"), "{reason}");
    let reply = tip_reply_with(fault, || Some(memo_at(2 * 60 * 1_000)), || AT).await;
    assert_eq!(reply.status, 200, "{}", reply.body);
    assert_eq!(reply.body["freshness"], "fresh");
    assert_eq!(reply.body["servedFrom"], "memo");
    assert_eq!(reply.body["memoAgeMs"], 120_000);
    // a memo with no read stamp has no age: never served
    let unstamped = VerifiedTip {
        read_at: None,
        ..memo_at(10_000)
    };
    let reply = tip_reply_with(fault, move || Some(unstamped), || AT).await;
    assert_eq!(reply.status, 503, "{}", reply.body);
}

// ── bsv-low #1165: the chain clock's second opinion (the design's R1 to R6) ──

use crate::tip_second_opinion::{SourceAnswer, TipKnobs};

const MIN: u64 = 60 * 1_000;

fn src(name: &str, height: u32, hash: &str, at: u64) -> SourceAnswer {
    SourceAnswer {
        name: name.into(),
        read: Ok((height, hash.into())),
        at,
    }
}

fn src_fault(name: &str, at: u64) -> SourceAnswer {
    SourceAnswer {
        name: name.into(),
        read: Err("HTTP 503".into()),
        at,
    }
}

/// The reply over a healthy store (its pass at `AT`) read at `AT + pass_age`, the sources scripted; answers the
/// reply and how many times the sources were asked. The clock moves as production's does (the fold's F4): the ask
/// answers at the latest source's `at`, so the clock read after the ask is that time.
async fn reply_with_sources(
    pass_age: u64,
    sources: Vec<SourceAnswer>,
    knobs: TipKnobs,
) -> (super::TipReply, usize) {
    let asked = Cell::new(0usize);
    let clock = Cell::new(AT + pass_age);
    let reply = super::tip_reply_within(
        |route| ready(response(route, header())),
        || None,
        || clock.get(),
        std::future::pending::<()>(),
        || {
            asked.set(asked.get() + 1);
            let answered = sources.iter().map(|s| s.at).max().unwrap_or(0);
            clock.set(clock.get().max(answered));
            ready(sources)
        },
        knobs,
    )
    .await;
    (reply, asked.get())
}

/// Two sources naming the stored tip at `at`, the third faulted.
fn matching(at_a: u64, at_b: u64) -> Vec<SourceAnswer> {
    let h = tip().hash;
    vec![
        src("woc", VERIFIED_HEIGHT, &h, at_a),
        src("bitails", VERIFIED_HEIGHT, &h.to_uppercase(), at_b),
        src_fault("bananablocks", at_b),
    ]
}

/// The client's stake rule (bsv-low `app/src/lib/chainReads.ts stakeTipReadOnce`, as pinned on prod): fresh only on
/// 200, no `code`, `stale` not true, `freshness: "fresh"`, a finite `ageMs` at most 10 minutes, a positive height.
fn client_accepts(status: u16, body: &Value) -> bool {
    if body["code"].is_string() || !(200..300).contains(&status) || body["stale"] == true {
        return false;
    }
    if body["freshness"] != "fresh" {
        return false;
    }
    match body["ageMs"].as_f64() {
        Some(age) if age.is_finite() && (0.0..=600_000.0).contains(&age) => {}
        _ => return false,
    }
    body["height"].as_u64().is_some_and(|h| h > 0)
}

/// The tower's arm clock (bsv-low `workers/low-watchtower/src/chain_oracle.rs parse_arm_clock`): no `code`, 200,
/// `stale` not true, `freshness: "fresh"`, a height.
fn tower_accepts(status: u16, body: &Value) -> bool {
    body.get("code").and_then(Value::as_str).is_none()
        && status == 200
        && body.get("stale").and_then(Value::as_bool) != Some(true)
        && body["freshness"] == "fresh"
        && body["height"].as_u64().is_some()
}

/// tip1165_a: a pass six minutes old, two sources naming the stored tip: 200 fresh by the couriers, `ageMs` the
/// older corroboration's age at the clock read after the ask, `passAgeMs` the true pass age. RED on main: `ageMs`
/// 360000, no `freshBy`. The fold's F4: the answers come AFTER the request's first clock, as in production (each
/// stamped on arrival); the clock after the ask is the later one, so the age is 600 - 100.
#[tokio::test]
async fn tip1165_a_a_six_minute_pass_corroborated_by_two_sources_is_fresh_by_the_couriers() {
    let now = AT + 6 * MIN;
    let (reply, asked) =
        reply_with_sources(6 * MIN, matching(now + 100, now + 600), TipKnobs::default()).await;
    assert_eq!(reply.status, 200, "{}", reply.body);
    assert_eq!(asked, 1);
    assert_eq!(reply.body["freshBy"], "couriers", "{}", reply.body);
    assert_eq!(reply.body["freshness"], "fresh");
    assert_eq!(
        reply.body["ageMs"], 500,
        "the older of the two corroborations, at the clock after the ask"
    );
    assert_eq!(reply.body["passAgeMs"], 360_000);
    assert_eq!(reply.body["lastPassAt"], AT, "the true stamp");
    assert_eq!(reply.body["height"], VERIFIED_HEIGHT);
    assert_eq!(reply.body["hash"], tip().hash);
    assert_eq!(reply.body["source"], "verified-header");
    assert_eq!(reply.body["corroborationFrom"], "live");
    let c = reply.body["corroboration"].as_array().unwrap();
    assert_eq!(c.len(), 3);
    assert_eq!(c[0]["source"], "woc");
    assert_eq!(c[0]["matched"], true);
    assert_eq!(c[1]["matched"], true, "compared as lowercase hex");
    assert_eq!(c[2]["matched"], false);
    assert!(c[2]["fault"].is_string());
    assert!(reply.body.get("code").is_none());
    assert_eq!(reply.retry_after, None);
    assert!(reply
        .second_opinion
        .as_deref()
        .is_some_and(|l| l.contains("corroborated")));
}

/// tip1165_b: a pass 45 minutes old (past the 30-minute bar), the same sources: the same 200. RED on main: the 503.
#[tokio::test]
async fn tip1165_b_a_forty_five_minute_pass_corroborated_is_200() {
    let now = AT + 45 * MIN;
    let (reply, asked) = reply_with_sources(
        45 * MIN,
        matching(now + 100, now + 1_000),
        TipKnobs::default(),
    )
    .await;
    assert_eq!(reply.status, 200, "{}", reply.body);
    assert_eq!(asked, 1);
    assert_eq!(reply.body["freshBy"], "couriers");
    assert_eq!(reply.body["ageMs"], 900);
    assert_eq!(reply.body["passAgeMs"], 45 * MIN);
}

/// tip1165_c: one source two blocks above (past the lag bar's one block, the fold's F1), two matching: the 503,
/// reason `behind`, at 45 and at 6 minutes. One block above is `within-lag`: today's answer under the 30-minute bar
/// (byte for byte main's), the 503 past it, never a corroboration.
#[tokio::test]
async fn tip1165_c_a_source_above_is_behind_and_the_503() {
    for pass_age in [45 * MIN, 6 * MIN] {
        let now = AT + pass_age;
        let h = tip().hash;
        let sources = vec![
            src("woc", VERIFIED_HEIGHT, &h, now),
            src("bitails", VERIFIED_HEIGHT, &h, now),
            src("bananablocks", VERIFIED_HEIGHT + 2, &"cd".repeat(32), now),
        ];
        crate::tip_second_opinion::clear_memo();
        let (reply, asked) = reply_with_sources(pass_age, sources, TipKnobs::default()).await;
        assert_eq!(reply.status, 503, "{}", reply.body);
        assert_eq!(asked, 1);
        assert_eq!(reply.body["code"], "ERR_TIP_UNAVAILABLE");
        assert_eq!(reply.body["secondOpinion"], "behind");
        assert_eq!(reply.body["freshness"], "stale");
        let reason = reply.reason.unwrap();
        assert!(reason.contains("behind"), "{reason}");
        if pass_age > FRESHNESS_MS {
            assert!(reason.contains("past the 1800000 ms bar"), "{reason}");
        }
        assert!(reply.body.get("freshBy").is_none());
        // one block above: within the lag bar's tolerance
        let one_above = vec![
            src("woc", VERIFIED_HEIGHT, &h, now),
            src("bitails", VERIFIED_HEIGHT, &h, now),
            src("bananablocks", VERIFIED_HEIGHT + 1, &"cd".repeat(32), now),
        ];
        crate::tip_second_opinion::clear_memo();
        let (reply, asked) = reply_with_sources(pass_age, one_above, TipKnobs::default()).await;
        assert_eq!(asked, 1);
        assert!(reply.body.get("freshBy").is_none(), "{}", reply.body);
        if pass_age > FRESHNESS_MS {
            assert_eq!(reply.status, 503, "{}", reply.body);
            assert_eq!(reply.body["secondOpinion"], "within-lag");
            assert_eq!(reply.body["refusal"], "within-lag");
        } else {
            let (status, body) = main_body(pass_age).await;
            assert_eq!(
                (reply.status, reply.body.to_string()),
                (status, body.to_string())
            );
        }
    }
}

/// tip1165_d: another hash at the stored height, two matching: the 503, reason `contested`.
#[tokio::test]
async fn tip1165_d_another_hash_at_the_stored_height_is_contested() {
    for pass_age in [45 * MIN, 6 * MIN] {
        let now = AT + pass_age;
        let h = tip().hash;
        let sources = vec![
            src("woc", VERIFIED_HEIGHT, &h, now),
            src("bitails", VERIFIED_HEIGHT, &h, now),
            src("bananablocks", VERIFIED_HEIGHT, &"cd".repeat(32), now),
        ];
        crate::tip_second_opinion::clear_memo();
        let (reply, _) = reply_with_sources(pass_age, sources, TipKnobs::default()).await;
        assert_eq!(reply.status, 503, "{}", reply.body);
        assert_eq!(reply.body["secondOpinion"], "contested");
        assert!(reply.reason.unwrap().contains("contested"));
    }
}

/// tip1165_e: one matching, two faulted: past the bar the 503, reason `short`; one matching, one below, one faulted:
/// the 503. Under the 30-minute bar a `short` verdict is today's answer (a design departure, stated in the PR).
#[tokio::test]
async fn tip1165_e_fewer_than_two_corroborations_are_short() {
    let now = AT + 45 * MIN;
    let h = tip().hash;
    let (reply, _) = reply_with_sources(
        45 * MIN,
        vec![
            src("woc", VERIFIED_HEIGHT, &h, now),
            src_fault("bitails", now),
            src_fault("bananablocks", now),
        ],
        TipKnobs::default(),
    )
    .await;
    assert_eq!(reply.status, 503, "{}", reply.body);
    assert_eq!(reply.body["secondOpinion"], "short");
    assert!(reply.reason.unwrap().contains("short"));
    crate::tip_second_opinion::clear_memo();
    let (reply, _) = reply_with_sources(
        45 * MIN,
        vec![
            src("woc", VERIFIED_HEIGHT, &h, now),
            src("bitails", VERIFIED_HEIGHT - 1, &"cd".repeat(32), now),
            src_fault("bananablocks", now),
        ],
        TipKnobs::default(),
    )
    .await;
    assert_eq!(reply.status, 503, "{}", reply.body);
    assert_eq!(reply.body["secondOpinion"], "short");
    // under the bar: today's 200, its true age, no additive field
    crate::tip_second_opinion::clear_memo();
    let now = AT + 6 * MIN;
    let (reply, asked) = reply_with_sources(
        6 * MIN,
        vec![
            src("woc", VERIFIED_HEIGHT, &h, now),
            src_fault("bitails", now),
        ],
        TipKnobs::default(),
    )
    .await;
    assert_eq!(asked, 1);
    assert_eq!(reply.status, 200, "{}", reply.body);
    assert_eq!(reply.body["ageMs"], 6 * MIN);
    assert!(reply.body.get("freshBy").is_none());
    assert!(reply.body.get("corroboration").is_none());
}

/// The body of main's route for the same snapshot (no source asked), for the byte-identity pins.
async fn main_body(pass_age: u64) -> (u16, Value) {
    let reply = tip_reply_with(
        |route| ready(response(route, header())),
        || None,
        || AT + pass_age,
    )
    .await;
    (reply.status, reply.body)
}

/// tip1165_f: a pass four minutes old: no source asked, the body byte for byte today's.
#[tokio::test]
async fn tip1165_f_the_fresh_path_asks_nobody_and_is_todays_body() {
    let (reply, asked) = reply_with_sources(4 * MIN, matching(AT, AT), TipKnobs::default()).await;
    assert_eq!(asked, 0, "R1: no courier on the fresh path");
    let (status, body) = main_body(4 * MIN).await;
    assert_eq!(reply.status, status);
    assert_eq!(reply.body.to_string(), body.to_string());
    assert_eq!(reply.body["ageMs"], 4 * MIN);
    assert!(reply.second_opinion.is_none());
    // and at exactly five minutes too
    let (_, asked) = reply_with_sources(5 * MIN, matching(AT, AT), TipKnobs::default()).await;
    assert_eq!(asked, 0);
}

/// tip1165_g: the lag bar failing: today's 503, no source asked.
#[tokio::test]
async fn tip1165_g_a_failing_lag_bar_asks_nobody() {
    let asked = Cell::new(0usize);
    let lagging = |route: String| {
        ready(if route == "getInfo" {
            let mut sync = info();
            sync["value"]["lastSeenHeight"] = json!(VERIFIED_HEIGHT + 2);
            Ok((200, sync))
        } else {
            response(route, header())
        })
    };
    let reply = super::tip_reply_within(
        lagging,
        || None,
        || AT + 45 * MIN,
        std::future::pending::<()>(),
        || {
            asked.set(asked.get() + 1);
            ready(matching(AT, AT))
        },
        TipKnobs::default(),
    )
    .await;
    assert_eq!(asked.get(), 0);
    assert_eq!(reply.status, 503);
    assert!(reply.body.get("secondOpinion").is_none());
    let main = tip_reply_with(lagging, || None, || AT + 45 * MIN).await;
    assert_eq!(reply.body.to_string(), main.body.to_string());
}

/// tip1165_h: no verified snapshot (the read faulted, no memo): today's 503, no source asked.
#[tokio::test]
async fn tip1165_h_no_verified_snapshot_asks_nobody() {
    let asked = Cell::new(0usize);
    let fault = |_| ready(Err(HeaderError("transport".into())));
    let reply = super::tip_reply_within(
        fault,
        || None,
        || AT + 45 * MIN,
        std::future::pending::<()>(),
        || {
            asked.set(asked.get() + 1);
            ready(matching(AT, AT))
        },
        TipKnobs::default(),
    )
    .await;
    assert_eq!(asked.get(), 0);
    assert_eq!(reply.status, 503);
    let main = tip_reply_with(fault, || None, || AT + 45 * MIN).await;
    assert_eq!(reply.body.to_string(), main.body.to_string());
}

/// tip1165_i: the memo. A second call inside 30 s asks nobody and serves the memo's sources with a grown `ageMs`; a
/// new tip hash asks again; a refusal is memoised 10 s.
#[tokio::test]
async fn tip1165_i_the_memo_is_per_tip_hash_and_bounded() {
    let first = AT + 45 * MIN;
    let (reply, asked) = reply_with_sources(
        45 * MIN,
        matching(first - 900, first - 400),
        TipKnobs::default(),
    )
    .await;
    assert_eq!((reply.status, asked), (200, 1));
    let (reply, asked) =
        reply_with_sources(45 * MIN + 20_000, matching(AT, AT), TipKnobs::default()).await;
    assert_eq!(asked, 0, "inside 30 s the memo answers");
    assert_eq!(reply.status, 200);
    assert_eq!(reply.body["ageMs"], 20_900, "the memo's age grows");
    assert_eq!(reply.body["corroborationFrom"], "memo");
    assert!(reply.second_opinion.is_none(), "a memo logs no verdict");
    // past 30 s: asked again
    let (_, asked) = reply_with_sources(
        45 * MIN + 30_000,
        matching(first + 30_000, first + 30_000),
        TipKnobs::default(),
    )
    .await;
    assert_eq!(asked, 1);
    // a new tip hash asks again inside 30 s
    let moved = |route: String| {
        let mut frame = header();
        frame["value"]["hash"] = json!("03".repeat(32));
        ready(response(route, frame))
    };
    let asked = Cell::new(0usize);
    let h3 = "03".repeat(32);
    let reply = super::tip_reply_within(
        moved,
        || None,
        || AT + 45 * MIN + 31_000,
        std::future::pending::<()>(),
        || {
            asked.set(asked.get() + 1);
            ready(vec![
                src("woc", VERIFIED_HEIGHT, &h3, AT + 45 * MIN + 31_000),
                src("bitails", VERIFIED_HEIGHT, &h3, AT + 45 * MIN + 31_000),
            ])
        },
        TipKnobs::default(),
    )
    .await;
    assert_eq!(asked.get(), 1, "a memo for another tip is never served");
    assert_eq!(reply.status, 200, "{}", reply.body);
    assert_eq!(reply.body["hash"], h3);
}

#[tokio::test]
async fn tip1165_i_a_refusal_is_memoised_ten_seconds() {
    let short = || vec![src_fault("woc", AT)];
    let (r, asked) = reply_with_sources(45 * MIN, short(), TipKnobs::default()).await;
    assert_eq!((r.status, asked), (503, 1));
    let (r, asked) =
        reply_with_sources(45 * MIN + 9_999, matching(AT, AT), TipKnobs::default()).await;
    assert_eq!((r.status, asked), (503, 0));
    assert_eq!(r.body["corroborationFrom"], "memo");
    let now = AT + 45 * MIN + 10_000;
    let (r, asked) =
        reply_with_sources(45 * MIN + 10_000, matching(now, now), TipKnobs::default()).await;
    assert_eq!((r.status, asked), (200, 1));
}

/// tip1165_j: the knobs: honoured with the beta marker (the pass read as the knob's age, the first source one above),
/// inert without it.
#[tokio::test]
async fn tip1165_j_the_drill_knobs_need_the_beta_marker() {
    let on = TipKnobs::of(Some("beta"), Some("2700000"), None);
    // a healthy 20 s pass under the pass-age knob: the second opinion asked, corroborated
    let now = AT + 20_000;
    let (reply, asked) = reply_with_sources(20_000, matching(now, now), on).await;
    assert_eq!(asked, 1);
    assert_eq!(reply.status, 200, "{}", reply.body);
    assert_eq!(reply.body["freshBy"], "couriers");
    assert_eq!(reply.body["passAgeMs"], 2_700_000);
    assert_eq!(reply.body["testPassAgeMs"], 2_700_000);
    // the source-ahead knob: one block above, `within-lag` since the fold's F1 (it was `behind`); past the
    // 30-minute bar (the pass-age knob's 45 minutes) the 503 all the same
    let both = TipKnobs::of(Some("beta"), Some("2700000"), Some("1"));
    crate::tip_second_opinion::clear_memo();
    let (reply, _) = reply_with_sources(21_000, matching(now, now), both).await;
    assert_eq!(reply.status, 503, "{}", reply.body);
    assert_eq!(reply.body["secondOpinion"], "within-lag");
    // without the marker: inert, today's body, nobody asked
    let off = TipKnobs::of(None, Some("2700000"), Some("1"));
    assert_eq!(off, TipKnobs::default());
    let (reply, asked) = reply_with_sources(20_000, matching(now, now), off).await;
    assert_eq!(asked, 0);
    assert_eq!(
        reply.body.to_string(),
        main_body(20_000).await.1.to_string()
    );
    // listed on /health.testKnobs by the route's own lines
    let src: String = include_str!("routes.rs").split_whitespace().collect();
    assert!(src.contains(
        "ifletSome(knobs)=body[\"testKnobs\"].as_array_mut(){knobs.extend(crate::tip_second_opinion::knobs_env(&ctx.env).listed()"
    ));
}

/// tip1165_k: the two pinned readers. Bodies (a) and (b) pass the client's stake rule and the tower's arm clock;
/// bodies (c) to (e) fail both.
#[tokio::test]
async fn tip1165_k_the_pinned_readers_take_a_and_b_and_refuse_c_to_e() {
    let h = tip().hash;
    for pass_age in [6 * MIN, 45 * MIN] {
        let now = AT + pass_age;
        crate::tip_second_opinion::clear_memo();
        let (r, _) = reply_with_sources(
            pass_age,
            matching(now - 900, now - 400),
            TipKnobs::default(),
        )
        .await;
        assert!(client_accepts(r.status, &r.body), "{}", r.body);
        assert!(tower_accepts(r.status, &r.body), "{}", r.body);
    }
    let now = AT + 45 * MIN;
    let refused = [
        vec![
            src("woc", VERIFIED_HEIGHT, &h, now),
            src("bitails", VERIFIED_HEIGHT, &h, now),
            src("bananablocks", VERIFIED_HEIGHT + 1, &h, now),
        ],
        vec![
            src("woc", VERIFIED_HEIGHT, &h, now),
            src("bitails", VERIFIED_HEIGHT, &h, now),
            src("bananablocks", VERIFIED_HEIGHT, &"cd".repeat(32), now),
        ],
        vec![
            src("woc", VERIFIED_HEIGHT, &h, now),
            src_fault("bitails", now),
        ],
    ];
    for (i, sources) in refused.into_iter().enumerate() {
        crate::tip_second_opinion::clear_memo();
        let (r, _) =
            reply_with_sources(45 * MIN + 11_000 * i as u64, sources, TipKnobs::default()).await;
        assert!(!client_accepts(r.status, &r.body), "{}", r.body);
        assert!(!tower_accepts(r.status, &r.body), "{}", r.body);
    }
    // the 45-minute body on main is refused by both (the outage's shape)
    let (status, body) = main_body(45 * MIN).await;
    assert!(!client_accepts(status, &body) && !tower_accepts(status, &body));
}

// ── the TIP-SECOND-OPINION ensemble's pins (lenses A, B and C), adopted by the fold; where a fold changed the
// behaviour a pin demonstrated, its expectation is the folded one and its comment says so ──

/// The reply over a store whose `getInfo` names `seen` as its last seen height, read at `AT + pass_age` (lens A).
async fn lensa_reply(
    pass_age: u64,
    seen: u32,
    sources: Vec<SourceAnswer>,
) -> (super::TipReply, usize) {
    crate::tip_second_opinion::clear_memo();
    let asked = Cell::new(0usize);
    let get = move |route: String| {
        ready(if route == "getInfo" {
            let mut sync = info();
            sync["value"]["lastSeenHeight"] = json!(seen);
            Ok((200, sync))
        } else {
            response(route, header())
        })
    };
    let reply = super::tip_reply_within(
        get,
        || None,
        || AT + pass_age,
        std::future::pending::<()>(),
        || {
            asked.set(asked.get() + 1);
            ready(sources)
        },
        TipKnobs::default(),
    )
    .await;
    (reply, asked.get())
}

/// Main's answer for the snapshot (lens A): the branch with NO source answering is main's body under the 30-minute
/// bar and main's 503 past it (fields added), so both readers judge it as main's; the memo is cleared first.
async fn lensa_main(pass_age: u64, seen: u32) -> (u16, Value) {
    crate::tip_second_opinion::clear_memo();
    let get = move |route: String| {
        ready(if route == "getInfo" {
            let mut sync = info();
            sync["value"]["lastSeenHeight"] = json!(seen);
            Ok((200, sync))
        } else {
            response(route, header())
        })
    };
    let r = tip_reply_with(get, || None, || AT + pass_age).await;
    (r.status, r.body)
}

/// lens A M1: the store one block behind by its own word (`lastSeenHeight` = tip + 1, inside `TIP_LAG_MAX_BLOCKS`),
/// its pass 7 minutes old, the couriers on that same block. FOLDED (F1): it demonstrated a new 503 `behind`; one
/// block is now `within-lag`, served as main serves it (byte for byte), which both pinned readers take. Second leg:
/// lag 0, two couriers matching, one at tip + 1: the same, main's body.
#[tokio::test]
async fn lensa_m1_a_one_block_lag_main_serves_is_a_new_503_between_five_and_ten_minutes() {
    let pass_age = 7 * MIN;
    let now = AT + pass_age;
    let h = tip().hash;
    let other = "cd".repeat(32);
    let (main_status, main_body) = lensa_main(pass_age, VERIFIED_HEIGHT + 1).await;
    assert!(client_accepts(main_status, &main_body), "{main_body}");
    assert!(tower_accepts(main_status, &main_body), "{main_body}");
    let (reply, asked) = lensa_reply(
        pass_age,
        VERIFIED_HEIGHT + 1,
        vec![
            src("woc", VERIFIED_HEIGHT + 1, &other, now),
            src("bitails", VERIFIED_HEIGHT + 1, &other, now),
            src("bananablocks", VERIFIED_HEIGHT + 1, &other, now),
        ],
    )
    .await;
    assert_eq!(asked, 1);
    assert_eq!(
        (reply.status, reply.body.to_string()),
        (main_status, main_body.to_string()),
        "the fold's F1: main's answer"
    );
    assert!(client_accepts(reply.status, &reply.body));
    assert!(tower_accepts(reply.status, &reply.body));
    let (main_status, main_body) = lensa_main(pass_age, VERIFIED_HEIGHT).await;
    assert!(client_accepts(main_status, &main_body));
    let (reply, _) = lensa_reply(
        pass_age,
        VERIFIED_HEIGHT,
        vec![
            src("woc", VERIFIED_HEIGHT, &h, now),
            src("bitails", VERIFIED_HEIGHT, &h, now),
            src("bananablocks", VERIFIED_HEIGHT + 1, &other, now),
        ],
    )
    .await;
    assert_eq!(
        (reply.status, reply.body.to_string()),
        (main_status, main_body.to_string())
    );
}

/// lens A L1: the store's own word one block above its verified tip, its pass 45 minutes old, two couriers naming
/// the verified tip. FOLDED (F2): it demonstrated a 200 corroborated over the store's own word; the store's word is
/// now a voice, so the verdict is `within-lag`, the 503 past the 30-minute bar, as main.
#[tokio::test]
async fn lensa_l1_corroboration_overrides_the_stores_own_seen_height() {
    let pass_age = 45 * MIN;
    let now = AT + pass_age;
    let (main_status, _) = lensa_main(pass_age, VERIFIED_HEIGHT + 1).await;
    assert_eq!(main_status, 503);
    let (reply, _) = lensa_reply(pass_age, VERIFIED_HEIGHT + 1, matching(now, now)).await;
    assert_eq!(reply.status, 503, "{}", reply.body);
    assert_eq!(reply.body["secondOpinion"], "within-lag");
    assert_eq!(reply.body["refusal"], "within-lag");
    assert!(reply.body.get("freshBy").is_none());
    assert!(!client_accepts(reply.status, &reply.body));
    assert!(!tower_accepts(reply.status, &reply.body));
}

/// lens A N1 (unchanged): a corroboration memo serves for up to 30 s without asking, whatever lands meanwhile; the
/// served `ageMs` stays under the client's bar.
#[tokio::test]
async fn lensa_n1_the_corroboration_memo_serves_across_a_block() {
    let first = AT + 6 * MIN;
    crate::tip_second_opinion::clear_memo();
    let (r, asked) = reply_with_sources(6 * MIN, matching(first, first), TipKnobs::default()).await;
    assert_eq!((r.status, asked), (200, 1));
    let other = "cd".repeat(32);
    let (r, asked) = reply_with_sources(
        6 * MIN + 29_999,
        vec![
            src("woc", VERIFIED_HEIGHT + 1, &other, first + 29_999),
            src("bitails", VERIFIED_HEIGHT + 1, &other, first + 29_999),
        ],
        TipKnobs::default(),
    )
    .await;
    assert_eq!(asked, 0);
    assert_eq!(r.status, 200);
    assert_eq!(r.body["ageMs"], 29_999);
    assert!(client_accepts(r.status, &r.body));
}

/// lens A N2: two answers stamped 11 minutes back. FOLDED (F4): it demonstrated a 200 with `ageMs` 660000 that the
/// tower took and the client refused; such answers are now `short`, never a corroboration: at a 6-minute pass
/// today's answer (both readers judge it as main's), and at 45 minutes the 503.
#[tokio::test]
async fn lensa_n2_an_old_checked_at_splits_the_readers() {
    let pass_age = 6 * MIN;
    let now = AT + pass_age;
    let (r, _) = lensa_reply(
        pass_age,
        VERIFIED_HEIGHT,
        matching(now - 11 * MIN, now - 11 * MIN),
    )
    .await;
    assert!(r.body.get("freshBy").is_none(), "{}", r.body);
    let (ms, mb) = lensa_main(pass_age, VERIFIED_HEIGHT).await;
    assert_eq!((r.status, r.body.to_string()), (ms, mb.to_string()));
    let now = AT + 45 * MIN;
    let (r, _) = lensa_reply(
        45 * MIN,
        VERIFIED_HEIGHT,
        matching(now - 11 * MIN, now - 11 * MIN),
    )
    .await;
    assert_eq!(r.status, 503, "{}", r.body);
    assert_eq!(r.body["secondOpinion"], "short");
    assert!(!tower_accepts(r.status, &r.body) && !client_accepts(r.status, &r.body));
}

/// lens A K (unchanged expectation): the two pinned readers over every body shape the route emits, on a grid of
/// pass ages, lag words, source sets and the read-fault memo path. A body the readers split on must ALSO be split on
/// main for the same snapshot; the client never accepts a body the tower refuses.
#[tokio::test]
async fn lensa_k_the_readers_agree_wherever_main_agreed() {
    let h = tip().hash;
    let other = "cd".repeat(32);
    let ages = [0, 4, 5, 6, 9, 10, 11, 29, 30, 31, 45, 600]
        .map(|m| m * MIN)
        .into_iter()
        .chain([5 * MIN + 1, 10 * MIN + 1, 30 * MIN + 1]);
    let mut divergent_new = Vec::new();
    let mut shapes = 0;
    for pass_age in ages {
        let now = AT + pass_age;
        let sets: Vec<(&str, Vec<SourceAnswer>)> = vec![
            ("none", vec![]),
            ("match2", matching(now, now)),
            (
                "match3",
                vec![
                    src("woc", VERIFIED_HEIGHT, &h, now),
                    src("bitails", VERIFIED_HEIGHT, &h, now),
                    src("bananablocks", VERIFIED_HEIGHT, &h, now),
                ],
            ),
            (
                "above",
                vec![
                    src("woc", VERIFIED_HEIGHT, &h, now),
                    src("bitails", VERIFIED_HEIGHT, &h, now),
                    src("bananablocks", VERIFIED_HEIGHT + 1, &other, now),
                ],
            ),
            (
                "above2",
                vec![
                    src("woc", VERIFIED_HEIGHT, &h, now),
                    src("bitails", VERIFIED_HEIGHT, &h, now),
                    src("bananablocks", VERIFIED_HEIGHT + 2, &other, now),
                ],
            ),
            (
                "contested",
                vec![
                    src("woc", VERIFIED_HEIGHT, &h, now),
                    src("bitails", VERIFIED_HEIGHT, &h, now),
                    src("bananablocks", VERIFIED_HEIGHT, &other, now),
                ],
            ),
            (
                "short",
                vec![
                    src("woc", VERIFIED_HEIGHT, &h, now),
                    src_fault("bitails", now),
                ],
            ),
            (
                "below",
                vec![
                    src("woc", VERIFIED_HEIGHT, &h, now),
                    src("bitails", VERIFIED_HEIGHT - 1, &other, now),
                ],
            ),
        ];
        for seen in [VERIFIED_HEIGHT, VERIFIED_HEIGHT + 1, VERIFIED_HEIGHT + 2] {
            let (ms, mb) = lensa_main(pass_age, seen).await;
            let main_disagrees = client_accepts(ms, &mb) != tower_accepts(ms, &mb);
            for (name, sources) in sets.clone() {
                let (r, _) = lensa_reply(pass_age, seen, sources).await;
                shapes += 1;
                let (c, t) = (
                    client_accepts(r.status, &r.body),
                    tower_accepts(r.status, &r.body),
                );
                assert!(
                    !c || t,
                    "the client accepts what the tower refuses: {name} {pass_age} {}",
                    r.body
                );
                if c != t && !main_disagrees {
                    divergent_new.push(format!("{name} pass {pass_age} seen {seen}: {}", r.body));
                }
            }
        }
    }
    for memo_age in [4 * MIN, 9 * MIN, 11 * MIN, 29 * MIN] {
        let mut memo = tip();
        memo.read_at = Some(AT + memo_age - 1_000);
        let fault = |_| ready(Err(HeaderError("transport".into())));
        let asked = Cell::new(0usize);
        let r = super::tip_reply_within(
            fault,
            move || Some(memo),
            || AT + memo_age,
            std::future::pending::<()>(),
            || {
                asked.set(asked.get() + 1);
                ready(Vec::new())
            },
            TipKnobs::default(),
        )
        .await;
        assert_eq!(asked.get(), 0);
        shapes += 1;
        let (c, t) = (
            client_accepts(r.status, &r.body),
            tower_accepts(r.status, &r.body),
        );
        assert!(!c || t);
        if memo_age > 10 * MIN {
            assert!(t && !c, "main's own disagreement, unchanged: {}", r.body);
        }
    }
    assert!(
        divergent_new.is_empty(),
        "{shapes} shapes; new disagreements: {divergent_new:#?}"
    );
}

/// One request through `tip_reply_within` over a healthy store at `now`, its ask counted in `asked` and pending one
/// poll before it answers `sources` (lenses B and C's herd shape).
async fn herd_one(now: u64, asked: &Cell<usize>, sources: Vec<SourceAnswer>) -> super::TipReply {
    super::tip_reply_within(
        |route| ready(response(route, header())),
        || None,
        || now,
        std::future::pending::<()>(),
        || {
            asked.set(asked.get() + 1);
            async move {
                tokio::task::yield_now().await;
                sources
            }
        },
        TipKnobs::default(),
    )
    .await
}

/// lens B herd: 100 concurrent `/tip` requests over a stale pass with no memo. FOLDED (F3): it demonstrated 100
/// asks; the in-flight marker makes it ONE. At 6 minutes the 99 get main's body byte for byte; at 12 and at 45
/// minutes the 99 get the 503 `asking` with `Retry-After: 2`. After the memo lands, the 101st asks nobody.
#[tokio::test]
async fn lensb_herd_concurrent_requests_each_ask() {
    for pass_age in [6 * MIN, 12 * MIN, 45 * MIN] {
        crate::tip_second_opinion::clear_memo();
        let (ms, mb) = main_body(pass_age).await;
        crate::tip_second_opinion::clear_memo();
        let asked = Cell::new(0usize);
        let now = AT + pass_age;
        let replies = futures_util::future::join_all(
            (0..100).map(|_| herd_one(now, &asked, matching(now, now))),
        )
        .await;
        assert_eq!(asked.get(), 1, "{pass_age}: one ask for the herd");
        assert_eq!(replies[0].status, 200, "{}", replies[0].body);
        assert_eq!(replies[0].body["freshBy"], "couriers");
        for r in &replies[1..] {
            if pass_age <= 10 * MIN {
                assert_eq!((r.status, r.body.to_string()), (ms, mb.to_string()));
                assert_eq!(r.retry_after, None);
            } else {
                assert_eq!(r.status, 503, "{}", r.body);
                assert_eq!(r.body["refusal"], "asking");
                assert_eq!(r.body["secondOpinion"], "asking");
                assert_eq!(r.retry_after, Some("2"));
                assert!(r.reason.as_deref().unwrap().contains("asking"));
            }
        }
        let r = herd_one(now, &asked, matching(now, now)).await;
        assert_eq!(r.status, 200);
        assert_eq!(asked.get(), 1, "the memo answers the 101st");
    }
}

/// The fold's F3: a marker the asking request left when it died (older than the deadline plus 500 ms) is dead and
/// the next request asks; one younger than that is live and the request does not ask.
#[tokio::test]
async fn tip1165_fold_f3_a_dead_marker_lets_the_next_request_ask() {
    use crate::tip_second_opinion::{plant_asking, ASKING_DEAD_AFTER_MS};
    let now = AT + 12 * MIN;
    crate::tip_second_opinion::clear_memo();
    plant_asking(&tip().hash, now - ASKING_DEAD_AFTER_MS);
    let asked = Cell::new(0usize);
    let r = herd_one(now, &asked, matching(now, now)).await;
    assert_eq!(asked.get(), 0, "a live marker: not asked");
    assert_eq!(r.body["refusal"], "asking");
    plant_asking(&tip().hash, now - ASKING_DEAD_AFTER_MS - 1);
    let r = herd_one(now, &asked, matching(now, now)).await;
    assert_eq!(asked.get(), 1, "a dead marker: asked");
    assert_eq!(r.status, 200, "{}", r.body);
    // a marker for another tip's hash never holds this one
    crate::tip_second_opinion::clear_memo();
    plant_asking(&"03".repeat(32), now);
    let r = herd_one(now, &asked, matching(now, now)).await;
    assert_eq!(asked.get(), 2);
    assert_eq!(r.status, 200);
    // a request dropped mid-ask clears its own marker
    crate::tip_second_opinion::clear_memo();
    {
        let dropped = herd_one(now, &asked, matching(now, now));
        futures_util::pin_mut!(dropped);
        let waker = futures_util::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        assert!(std::future::Future::poll(dropped.as_mut(), &mut cx).is_pending());
        assert!(crate::tip_second_opinion::asking_for(&tip().hash, now));
    }
    assert!(!crate::tip_second_opinion::asking_for(&tip().hash, now));
}

/// lens B: one source alone one block above, the other two faulted, at 6 and 25 minutes. FOLDED (F1): it
/// demonstrated the 503 closing the 5 to 30 minute window; one block is `within-lag`, main's answer, which the tower
/// takes (and the client, at 6 minutes). Two blocks above still close it.
#[tokio::test]
async fn lensb_one_source_alone_closes_the_5_to_30_minute_window() {
    for pass_age in [6 * MIN, 25 * MIN] {
        let now = AT + pass_age;
        crate::tip_second_opinion::clear_memo();
        let (main_like, _) = reply_with_sources(pass_age, vec![], TipKnobs::default()).await;
        assert_eq!(main_like.status, 200, "{}", main_like.body);
        assert!(tower_accepts(main_like.status, &main_like.body));
        crate::tip_second_opinion::clear_memo();
        let liar = vec![
            src("woc", VERIFIED_HEIGHT + 1, &"cd".repeat(32), now),
            src_fault("bitails", now),
            src_fault("bananablocks", now),
        ];
        let (reply, _) = reply_with_sources(pass_age, liar, TipKnobs::default()).await;
        assert_eq!(
            (reply.status, reply.body.to_string()),
            (main_like.status, main_like.body.to_string())
        );
        assert!(tower_accepts(reply.status, &reply.body));
        assert_eq!(
            client_accepts(reply.status, &reply.body),
            pass_age <= 10 * MIN
        );
        crate::tip_second_opinion::clear_memo();
        let liar = vec![src("woc", VERIFIED_HEIGHT + 2, &"cd".repeat(32), now)];
        let (reply, _) = reply_with_sources(pass_age, liar, TipKnobs::default()).await;
        assert_eq!(reply.status, 503, "{}", reply.body);
        assert_eq!(reply.body["refusal"], "behind");
        assert!(!tower_accepts(reply.status, &reply.body));
    }
}

/// lens B (unchanged; the residual F7 states): a source that would dissent but faults counts for nothing, so two
/// agreeing sources corroborate.
#[tokio::test]
async fn lensb_a_faulted_dissenter_counts_for_nothing() {
    crate::tip_second_opinion::clear_memo();
    let now = AT + 45 * MIN;
    let h = tip().hash;
    let sources = vec![
        src("woc", VERIFIED_HEIGHT, &h, now),
        src("bananablocks", VERIFIED_HEIGHT, &h, now),
        SourceAnswer {
            name: "bitails".into(),
            read: Err("HTTP 429".into()),
            at: now,
        },
    ];
    let (reply, _) = reply_with_sources(45 * MIN, sources, TipKnobs::default()).await;
    assert_eq!(reply.status, 200);
    assert!(tower_accepts(reply.status, &reply.body));
    assert!(client_accepts(reply.status, &reply.body));
}

/// lens B age zero: answers stamped after the request's first clock. FOLDED (F4): it demonstrated `ageMs` 0; the
/// clock read after the ask makes it the true age of the older of the two newest answers (1500 - 400).
#[tokio::test]
async fn lensb_live_age_is_zero_when_answers_come_after_now() {
    crate::tip_second_opinion::clear_memo();
    let now = AT + 6 * MIN;
    let (reply, _) = reply_with_sources(
        6 * MIN,
        matching(now + 1_500, now + 400),
        TipKnobs::default(),
    )
    .await;
    assert_eq!(reply.status, 200);
    assert_eq!(reply.body["ageMs"], 1_100);
}

/// lens B (unchanged): a prod render (no beta marker) with both knob vars set behaves as with no knobs.
#[tokio::test]
async fn lensb_knobs_are_inert_through_the_route_without_the_marker() {
    crate::tip_second_opinion::clear_memo();
    let knobs = TipKnobs::of(None, Some("2700000"), Some("1"));
    assert!(knobs.listed().is_empty());
    let (reply, asked) = reply_with_sources(20_000, matching(AT, AT), knobs).await;
    assert_eq!(asked, 0, "{}", reply.body);
    assert_eq!(reply.status, 200);
    assert!(reply.body.get("freshBy").is_none());
    assert!(reply.body.get("testPassAgeMs").is_none());
}

fn lensc_lag1(route: String) -> std::future::Ready<std::result::Result<(u16, Value), HeaderError>> {
    ready(if route == "getInfo" {
        let mut s = info();
        s["value"]["lastSeenHeight"] = json!(VERIFIED_HEIGHT + 1);
        Ok((200, s))
    } else {
        response(route, header())
    })
}

/// lens C P1: the store has seen one block it has not verified (lag 1), pass 7 minutes old, two couriers naming
/// that block. FOLDED (F1): it demonstrated a 503 `behind`; it is now `within-lag`, today's body byte for byte.
#[tokio::test]
async fn lensc_p1_a_one_block_lag_the_bar_tolerates_is_refused_behind() {
    crate::tip_second_opinion::clear_memo();
    let now = AT + 7 * MIN;
    let today = tip_reply_with(lensc_lag1, || None, || now).await;
    assert_eq!(today.status, 200, "{}", today.body);
    assert!(client_accepts(today.status, &today.body), "{}", today.body);
    assert!(tower_accepts(today.status, &today.body));
    crate::tip_second_opinion::clear_memo();
    let next = "0f".repeat(32);
    let reply = super::tip_reply_within(
        lensc_lag1,
        || None,
        || now,
        std::future::pending::<()>(),
        || {
            ready(vec![
                src("woc", VERIFIED_HEIGHT + 1, &next, now + 300),
                src("bitails", VERIFIED_HEIGHT + 1, &next, now + 400),
                src_fault("bananablocks", now + 2_000),
            ])
        },
        TipKnobs::default(),
    )
    .await;
    assert_eq!(
        (reply.status, reply.body.to_string()),
        (today.status, today.body.to_string())
    );
    assert!(client_accepts(reply.status, &reply.body));
    assert!(tower_accepts(reply.status, &reply.body));
}

/// lens C P2: answers after the request's first clock. FOLDED (F4): it demonstrated `ageMs` 0; now 1900 - 1200.
#[tokio::test]
async fn lensc_p2_a_live_corroboration_serves_age_zero() {
    crate::tip_second_opinion::clear_memo();
    let now = AT + 7 * MIN;
    let (reply, _) = reply_with_sources(
        7 * MIN,
        matching(now + 1_200, now + 1_900),
        TipKnobs::default(),
    )
    .await;
    assert_eq!(reply.status, 200, "{}", reply.body);
    assert_eq!(reply.body["ageMs"], 700, "{}", reply.body);
}

/// lens C P3: two concurrent `/tip` in one isolate with no memo. FOLDED (F3): it demonstrated two asks; one now.
#[tokio::test]
async fn lensc_p3_concurrent_requests_each_ask() {
    crate::tip_second_opinion::clear_memo();
    let now = AT + 7 * MIN;
    let asked = Cell::new(0usize);
    let (a, b) = futures_util::future::join(
        herd_one(now, &asked, matching(now, now)),
        herd_one(now, &asked, matching(now, now)),
    )
    .await;
    assert_eq!((a.status, b.status), (200, 200));
    assert_eq!(
        asked.get(),
        1,
        "one ask: the second request finds the marker"
    );
}

/// The fold's F6: every 503 arm of `/tip` carries `refusal`, one machine word; no 200 carries it; and both pinned
/// readers refuse every 503 and read the same with the field removed (they ignore it).
#[tokio::test]
async fn tip1165_fold_f6_every_503_names_its_refusal() {
    let h = tip().hash;
    let other = "cd".repeat(32);
    let mut arms: Vec<(&str, super::TipReply)> = Vec::new();
    // read: a transport fault, no memo
    let fault = |_| ready(Err(HeaderError("transport".into())));
    arms.push(("read", tip_reply_with(fault, || None, || AT).await));
    // read: the deadline, no memo (a store that never answers)
    let r = super::tip_reply_within(
        |_route: String| std::future::pending::<Result<(u16, Value), HeaderError>>(),
        || None,
        || AT,
        ready(()),
        || ready(Vec::new()),
        TipKnobs::default(),
    )
    .await;
    arms.push(("read", r));
    // read: the store's 5xx
    let five = |_| ready(Ok((503, json!({}))));
    arms.push(("read", tip_reply_with(five, || None, || AT).await));
    // read: the snapshot disagreeing twice
    arms.push((
        "read",
        tip_reply_with(moving_store(usize::MAX), || None, || AT).await,
    ));
    // lag: a young pass two behind its own seen tip
    crate::tip_second_opinion::clear_memo();
    let (r, _) = lensa_reply(MIN, VERIFIED_HEIGHT + 2, vec![]).await;
    arms.push(("lag", r));
    // stale: the 30-minute bar on a live read (the lag bar failing too, so no second opinion is asked)
    let (r, _) = lensa_reply(45 * MIN, VERIFIED_HEIGHT + 2, vec![]).await;
    arms.push(("stale", r));
    // the second opinion's words
    let at45 = AT + 45 * MIN;
    for (word, pass_age, sources) in [
        (
            "behind",
            6 * MIN,
            vec![
                src("woc", VERIFIED_HEIGHT, &h, AT + 6 * MIN),
                src("bitails", VERIFIED_HEIGHT + 2, &other, AT + 6 * MIN),
            ],
        ),
        (
            "contested",
            6 * MIN,
            vec![
                src("woc", VERIFIED_HEIGHT, &h, AT + 6 * MIN),
                src("bitails", VERIFIED_HEIGHT, &other, AT + 6 * MIN),
            ],
        ),
        (
            "short",
            45 * MIN,
            vec![
                src("woc", VERIFIED_HEIGHT, &h, at45),
                src_fault("bitails", at45),
            ],
        ),
        (
            "within-lag",
            45 * MIN,
            vec![
                src("woc", VERIFIED_HEIGHT, &h, at45),
                src("bitails", VERIFIED_HEIGHT, &h, at45),
                src("bananablocks", VERIFIED_HEIGHT + 1, &other, at45),
            ],
        ),
    ] {
        crate::tip_second_opinion::clear_memo();
        let (r, _) = reply_with_sources(pass_age, sources, TipKnobs::default()).await;
        arms.push((word, r));
    }
    // asking: another request of this isolate is asking, past the client's 10-minute bar
    crate::tip_second_opinion::clear_memo();
    crate::tip_second_opinion::plant_asking(&h, AT + 12 * MIN);
    let (r, asked) = reply_with_sources(12 * MIN, matching(AT, AT), TipKnobs::default()).await;
    assert_eq!(asked, 0);
    arms.push(("asking", r));
    crate::tip_second_opinion::clear_memo();
    for (word, r) in &arms {
        assert_eq!(r.status, 503, "{word}: {}", r.body);
        assert_eq!(r.body["refusal"], *word, "{}", r.body);
        let mut without = r.body.clone();
        without.as_object_mut().unwrap().remove("refusal");
        for body in [&r.body, &without] {
            assert!(!client_accepts(r.status, body), "{word}");
            assert!(!tower_accepts(r.status, body), "{word}");
        }
    }
    // no 200 carries it: the fresh path, a corroboration, a read-fault memo, `short` and `within-lag` under the bar
    let mut oks = vec![main_body(4 * MIN).await.1];
    let now = AT + 6 * MIN;
    oks.push(
        reply_with_sources(6 * MIN, matching(now, now), TipKnobs::default())
            .await
            .0
            .body,
    );
    oks.push(
        tip_reply_with(fault, || Some(memo_at(10_000)), || AT)
            .await
            .body,
    );
    crate::tip_second_opinion::clear_memo();
    oks.push(
        reply_with_sources(6 * MIN, vec![src_fault("woc", now)], TipKnobs::default())
            .await
            .0
            .body,
    );
    let (r, _) = lensa_reply(
        6 * MIN,
        VERIFIED_HEIGHT + 1,
        vec![src("woc", VERIFIED_HEIGHT + 1, &other, now)],
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.body);
    oks.push(r.body);
    for body in oks {
        assert!(body.get("refusal").is_none(), "{body}");
        assert_eq!(body["freshness"], "fresh", "{body}");
    }
}

/// The fold's F4 and F2 together: a corroboration needs the store's own lag to be 0 or absent (the store one block
/// ahead of its verified tip and two couriers at the verified height is `within-lag`, never `freshBy: couriers`); and
/// an answer stamped 11 minutes back cannot corroborate.
#[tokio::test]
async fn tip1165_fold_f2_f4_no_corroboration_over_the_stores_word_or_an_old_answer() {
    for pass_age in [6 * MIN, 45 * MIN] {
        let now = AT + pass_age;
        let (r, _) = lensa_reply(pass_age, VERIFIED_HEIGHT + 1, matching(now, now)).await;
        assert!(r.body.get("freshBy").is_none(), "{}", r.body);
        let (r, _) = lensa_reply(
            pass_age,
            VERIFIED_HEIGHT,
            matching(now - 11 * MIN, now - 11 * MIN),
        )
        .await;
        assert!(r.body.get("freshBy").is_none(), "{}", r.body);
        let (r, _) = lensa_reply(pass_age, VERIFIED_HEIGHT, matching(now, now)).await;
        assert_eq!(r.body["freshBy"], "couriers", "the control: {}", r.body);
    }
}
