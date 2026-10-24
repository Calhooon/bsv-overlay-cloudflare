//! bsv-low #868 (lane COURIER-RUNG): the #519 presence ladder's mempool witness beside WoC's node. Bitails is a
//! POSITIVE-only witness (its 404 is "could not look"), and BananaBlocks' native `GET /api/v1/tx/{txid}` is the
//! second capable source, so an absence needs WoC's clean 404 AND BananaBlocks' recorded not-found. Every body is
//! one the probe recorded (bsv-low `docs/audit/COURIER-CAPABILITY-2026-10-24.md`, the raw answers at
//! `COURIER-PROBE/requests.jsonl`), kept verbatim in the app layer's fixture.
use super::*;
use crate::admit_fast::{ConflictPresence, CourierConflictPresence};
use crate::courier_counters::{Counters as CourierCounters, Reader};

const FIXTURE: &str = include_str!("../../../low-app-layer/tests/fixtures/courier_probe_868.json");

/// A recorded answer: (status, body, the subject its url asked about).
/// A key the fixture lacks reads as status 0, an empty body and an empty subject, which every case's assertion
/// refuses (no unwrap: the crate denies `clippy::unwrap_used`, test helpers included).
fn recorded(key: &str) -> (u16, String, String) {
    let v: serde_json::Value = serde_json::from_str(FIXTURE).unwrap_or_default();
    let b = &v["bodies"][key];
    let subject = b["url"]
        .as_str()
        .and_then(|url| url.rsplit('/').next())
        .unwrap_or_default();
    (
        b["status"]
            .as_u64()
            .and_then(|s| u16::try_from(s).ok())
            .unwrap_or_default(),
        b["body"].as_str().unwrap_or_default().to_string(),
        subject.to_string(),
    )
}

fn answer(url: String, status: u16, body: &str) {
    TEST_HTTP_RESPONSES.with(|r| r.borrow_mut().insert(url, (status, body.to_string())));
}

fn reset() {
    crate::courier_counters::clear_cooldowns();
    TEST_HTTP_RESPONSES.with(|r| r.borrow_mut().clear());
}

/// The three couriers' answers for `txid` (a `None` leaves the mock's default, an empty 404).
fn couriers(
    txid: &str,
    bitails: Option<(u16, &str)>,
    woc: Option<(u16, &str)>,
    banana: Option<(u16, &str)>,
) {
    reset();
    if let Some((s, b)) = bitails {
        answer(format!("{DEFAULT_BITAILS_BASE}/tx/{txid}"), s, b);
    }
    if let Some((s, b)) = woc {
        answer(format!("{DEFAULT_WOC_BASE}/tx/hash/{txid}"), s, b);
    }
    if let Some((s, b)) = banana {
        answer(format!("{DEFAULT_BANANABLOCKS_BASE}/tx/{txid}"), s, b);
    }
}

/// The conflict pass's competitor presence (`admit_fast::CourierConflictPresence`), a production fold of the ladder.
async fn presence(txid: &str, counters: Option<CourierCounters>) -> Option<NetworkPresence> {
    CourierConflictPresence {
        sources: ProofSources::parse("arcade,bananablocks,woc,bitails"),
        woc_api_key: None,
        counters,
    }
    .presence(txid)
    .await
}

/// bsv-low #868: the ladder's fold over the recorded bodies. RED on `db3a0931`: Bitails' 404 beside WoC's 404 was
/// an ABSENCE whatever BananaBlocks held (`left: Some(Absent)`), and BananaBlocks was never asked.
#[tokio::test]
async fn c868_the_presence_ladder_asks_bananablocks_and_bitails_404_is_no_absence() {
    let (_, dropped_404, dropped) = recorded("bitails_tx_dropped_404");
    let (nf_status, nf_body, _) = recorded("bb_tx_not_found");
    let (_, bb_unmined, unmined) = recorded("bb_tx_unmined");
    let (_, bb_mined, mined) = recorded("bb_tx_mined");
    let (_, bitails_unmined, bitails_subject) = recorded("bitails_tx_unmined");

    // (1) the record's own case: Bitails dropped `e3340463...` (404) while BananaBlocks still held it unmined
    couriers(&dropped, Some((404, &dropped_404)), Some((404, "")), None);
    assert_eq!(
        presence(&dropped, None).await,
        Some(NetworkPresence::Inconclusive),
        "Bitails' 404 and WoC's 404 are one capable vote, no absence"
    );
    // (2) BananaBlocks unmined beside both 404s: present, in the mempool
    couriers(
        &unmined,
        Some((404, &dropped_404)),
        Some((404, "")),
        Some((200, &bb_unmined)),
    );
    assert_eq!(
        presence(&unmined, None).await,
        Some(NetworkPresence::Present { height: None })
    );
    // (3) BananaBlocks mined (its recorded body has no `confirmations`) under a WoC and Bitails fault: the height
    couriers(
        &mined,
        Some((429, "")),
        Some((429, "")),
        Some((200, &bb_mined)),
    );
    assert_eq!(
        presence(&mined, None).await,
        Some(NetworkPresence::Present {
            height: Some(972_476)
        })
    );
    // (4) WoC's clean 404 and BananaBlocks' recorded not-found: the two capable sources, an absence
    couriers(
        &dropped,
        Some((404, &dropped_404)),
        Some((404, "")),
        Some((nf_status, &nf_body)),
    );
    assert_eq!(
        presence(&dropped, None).await,
        Some(NetworkPresence::Absent)
    );
    // (5) BananaBlocks' not-found beside a WoC fault, and a 404 that is not its recorded body: no absence
    couriers(
        &dropped,
        Some((404, &dropped_404)),
        Some((429, "")),
        Some((nf_status, &nf_body)),
    );
    assert_eq!(
        presence(&dropped, None).await,
        Some(NetworkPresence::Inconclusive)
    );
    couriers(
        &dropped,
        Some((404, &dropped_404)),
        Some((404, "")),
        Some((404, "")),
    );
    assert_eq!(
        presence(&dropped, None).await,
        Some(NetworkPresence::Inconclusive)
    );
    // (6) `confirmation_unknown: true` (the docs' shape over the recorded body) is "could not look"
    let mut unknown: serde_json::Value = serde_json::from_str(&bb_unmined).unwrap();
    unknown["confirmation_unknown"] = serde_json::json!(true);
    unknown["partial"] = serde_json::json!(true);
    couriers(
        &unmined,
        Some((404, &dropped_404)),
        Some((404, "")),
        Some((200, &unknown.to_string())),
    );
    assert_eq!(
        presence(&unmined, None).await,
        Some(NetworkPresence::Inconclusive)
    );
    // (7) Bitails' 200 with `confirmations: 0` is presence, and BananaBlocks is then not asked
    couriers(
        &bitails_subject,
        Some((200, &bitails_unmined)),
        Some((404, "")),
        None,
    );
    let door = CourierCounters::new(Reader::Door);
    assert_eq!(
        presence(&bitails_subject, Some(door.clone())).await,
        Some(NetworkPresence::Present { height: None })
    );
    assert!(
        !door.take().keys().any(|k| k.contains("bananablocks")),
        "a Bitails positive needs no second witness"
    );
    // (8) the WoC-style mirror is never read: it answers present, BananaBlocks native and WoC answer not-found
    couriers(
        &dropped,
        Some((404, &dropped_404)),
        Some((404, "")),
        Some((nf_status, &nf_body)),
    );
    answer(
        format!("{DEFAULT_BANANABLOCKS_BASE}/bsv/main/tx/hash/{dropped}"),
        200,
        &serde_json::json!({"txid": dropped, "vin": [], "vout": []}).to_string(),
    );
    assert_eq!(
        presence(&dropped, None).await,
        Some(NetworkPresence::Absent)
    );
    // (9) BananaBlocks' rung is counted under its provider's existing family
    couriers(
        &unmined,
        Some((404, &dropped_404)),
        Some((404, "")),
        Some((200, &bb_unmined)),
    );
    let door = CourierCounters::new(Reader::Door);
    let _ = presence(&unmined, Some(door.clone())).await;
    let rows = door.take();
    assert_eq!(
        rows.get("courier_door_bananablocks_ok_total"),
        Some(&1),
        "{rows:?}"
    );
    assert_eq!(
        rows.get("courier_door_bitails_notfound_total"),
        Some(&1),
        "{rows:?}"
    );
    reset();
}

/// bsv-low #868: the two readers over the recorded bodies.
#[test]
fn c868_the_presence_readers_read_the_recorded_bodies() {
    for (key, want) in [
        (
            "bb_tx_unmined",
            Some(NetworkPresence::Present { height: None }),
        ),
        (
            "bb_tx_flip_unmined",
            Some(NetworkPresence::Present { height: None }),
        ),
        (
            "bb_tx_mined",
            Some(NetworkPresence::Present {
                height: Some(972_476),
            }),
        ),
        (
            "bb_tx_mined_with_confirmations",
            Some(NetworkPresence::Present {
                height: Some(972_341),
            }),
        ),
        (
            "bb_tx_flip_mined",
            Some(NetworkPresence::Present {
                height: Some(972_477),
            }),
        ),
        ("bb_tx_not_found", Some(NetworkPresence::Absent)),
    ] {
        let (status, body, txid) = recorded(key);
        assert_eq!(
            bananablocks_presence_response(status, &body, &txid),
            want,
            "{key}"
        );
    }
    let (status, body, _) = recorded("bb_tx_unmined");
    assert_eq!(
        bananablocks_presence_response(status, &body, &"00".repeat(32)),
        None,
        "another txid's body"
    );
    assert_eq!(
        bananablocks_presence_response(404, "", &"00".repeat(32)),
        None
    );
    for (key, want) in [
        (
            "bitails_tx_unmined",
            Some(NetworkPresence::Present { height: None }),
        ),
        (
            "bitails_tx_mined",
            Some(NetworkPresence::Present {
                height: Some(972_476),
            }),
        ),
        ("bitails_tx_dropped_404", None),
        ("bitails_tx_not_found", None),
    ] {
        let (status, body, _) = recorded(key);
        assert_eq!(bitails_presence_response(status, &body), want, "{key}");
    }
    // WoC's reader is unchanged: its empty 404 is its clean absence
    assert_eq!(presence_response(404, ""), Some(NetworkPresence::Absent));
}
