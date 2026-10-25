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
        at: AT,
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

fn response(route: &str, verified: Value) -> Result<(u16, Value), HeaderError> {
    match route {
        "findChainTipHeaderHex" => Ok((200, verified)),
        "getPresentHeight" => Ok((200, json!({"status": "success", "value": COURIER_MAX}))),
        other => panic!("unexpected chaintracks route: {other}"),
    }
}

fn tracker_response(route: &str) -> Result<(u16, Value), HeaderError> {
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
    assert_eq!(calls.into_inner(), ["findChainTipHeaderHex"]);
    assert_eq!(reply.retry_after, None);
}

#[tokio::test]
async fn tip877_age_is_the_header_snapshot_age_not_the_fetch_age() {
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
async fn tip877_stale_verified_header_is_503_past_thirty_minutes() {
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
            |route| match route {
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
async fn tip877_last_known_follows_a_lower_verified_tip_and_keeps_its_age() {
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
    assert_eq!(reply.body["ageMs"], 1_010);
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
    assert_eq!(unavailable.body["ageMs"], 1_020);
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
