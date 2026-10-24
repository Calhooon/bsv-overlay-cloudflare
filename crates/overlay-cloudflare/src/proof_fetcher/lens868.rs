//! Lane COURIER-RUNG-LENS (the one lens on PR 170, bsv-low #868): the #519 presence ladder's negative cases the PR's
//! `c868` pins leave out. Every BananaBlocks answer that is not its recorded not-found is "could not look", and an
//! absence needs WoC's 404 beside it.
use super::*;
use crate::admit_fast::{ConflictPresence, CourierConflictPresence};

const TXID: &str = "e3340463baaceb6943dec5d82f612a247d2d1227f1b64f6e5c1438e5ff4e8d58";
const NOT_FOUND: &str = r#"{"error":"Transaction not found"}"#;

fn answer(url: String, status: u16, body: &str) {
    TEST_HTTP_RESPONSES.with(|r| r.borrow_mut().insert(url, (status, body.to_string())));
}

fn couriers(bitails: (u16, &str), woc: (u16, &str), banana: Option<(u16, &str)>) {
    crate::courier_counters::clear_cooldowns();
    TEST_HTTP_RESPONSES.with(|r| r.borrow_mut().clear());
    answer(
        format!("{DEFAULT_BITAILS_BASE}/tx/{TXID}"),
        bitails.0,
        bitails.1,
    );
    answer(format!("{DEFAULT_WOC_BASE}/tx/hash/{TXID}"), woc.0, woc.1);
    if let Some((s, b)) = banana {
        answer(format!("{DEFAULT_BANANABLOCKS_BASE}/tx/{TXID}"), s, b);
    }
}

async fn presence(sources: &str) -> Option<NetworkPresence> {
    CourierConflictPresence {
        sources: ProofSources::parse(sources),
        woc_api_key: None,
        counters: None,
    }
    .presence(TXID)
    .await
}

/// (a) A deployment whose `PROOF_SOURCES` omits `bananablocks` (`wrangler.toml` sets none) can no longer read ANY
/// absence: Bitails' 404 is "could not look" and BananaBlocks is never asked, so WoC's 404 stands alone.
#[tokio::test]
async fn lens868_a_without_bananablocks_enabled_no_absence_is_read() {
    couriers((404, ""), (404, ""), Some((404, NOT_FOUND)));
    assert_eq!(
        presence("arcade,woc,bitails").await,
        Some(NetworkPresence::Inconclusive)
    );
    assert_eq!(presence("").await, Some(NetworkPresence::Inconclusive));
    TEST_HTTP_RESPONSES.with(|r| r.borrow_mut().clear());
}

/// (b) Beside WoC's 404, no BananaBlocks answer but its recorded 404 is a vote.
#[tokio::test]
async fn lens868_b_no_bananablocks_word_beside_wocs_404_is_never_an_absence() {
    let mined_no_height = serde_json::json!({"txid": TXID, "confirmed": true}).to_string();
    let mined_zero =
        serde_json::json!({"txid": TXID, "confirmed": true, "block_height": 0}).to_string();
    let unknown = serde_json::json!({"txid": TXID, "confirmed": false, "block_height": 0,
        "confirmation_unknown": true, "partial": true})
    .to_string();
    let string_confirmed = serde_json::json!({"txid": TXID, "confirmed": "false"}).to_string();
    for (label, banana) in [
        ("not-found under 200", Some((200u16, NOT_FOUND))),
        ("not-found under 410", Some((410, NOT_FOUND))),
        (
            "404 lower case",
            Some((404, r#"{"error":"transaction not found"}"#)),
        ),
        ("404 plain text", Some((404, "Transaction not found"))),
        ("mined, no height", Some((200, mined_no_height.as_str()))),
        ("mined, height 0", Some((200, mined_zero.as_str()))),
        ("confirmation_unknown", Some((200, unknown.as_str()))),
        ("confirmed a string", Some((200, string_confirmed.as_str()))),
        ("429", Some((429, ""))),
        ("unanswered", None),
    ] {
        couriers((404, ""), (404, ""), banana);
        assert_eq!(
            presence("arcade,bananablocks,woc,bitails").await,
            Some(NetworkPresence::Inconclusive),
            "{label}"
        );
    }
    TEST_HTTP_RESPONSES.with(|r| r.borrow_mut().clear());
}

/// (c) BananaBlocks' clean not-found never outvotes a positive: WoC's mempool body, or WoC's mined height.
#[tokio::test]
async fn lens868_c_a_bananablocks_absence_never_outvotes_a_positive() {
    let woc_mempool = serde_json::json!({"txid": TXID, "hash": TXID, "version": 1, "size": 2168,
        "locktime": 0, "vin": [], "vout": []})
    .to_string();
    couriers((404, ""), (200, &woc_mempool), Some((404, NOT_FOUND)));
    assert_eq!(
        presence("arcade,bananablocks,woc,bitails").await,
        Some(NetworkPresence::Present { height: None })
    );
    let woc_mined =
        serde_json::json!({"txid": TXID, "blockheight": 972_476, "confirmations": 1}).to_string();
    couriers((404, ""), (200, &woc_mined), Some((404, NOT_FOUND)));
    assert_eq!(
        presence("arcade,bananablocks,woc,bitails").await,
        Some(NetworkPresence::Present {
            height: Some(972_476)
        })
    );
    TEST_HTTP_RESPONSES.with(|r| r.borrow_mut().clear());
}

/// (d) OBSERVED, the M2 of the lens: in the overlay's fold a BananaBlocks MINED body is a mined HEIGHT with no proof
/// checked (`door_words::courier_presence`'s `mined_claim`, the #214 admission's and #602's confirmation input),
/// and it is taken FIRST, ahead of WoC's own height, when Bitails holds no positive.
#[tokio::test]
async fn lens868_d_a_bananablocks_mined_body_is_an_unchecked_height_taken_first() {
    let bb_mined =
        serde_json::json!({"txid": TXID, "confirmed": true, "block_height": 972_400}).to_string();
    let woc_mined =
        serde_json::json!({"txid": TXID, "blockheight": 972_476, "confirmations": 1}).to_string();
    couriers((404, ""), (200, &woc_mined), Some((200, &bb_mined)));
    assert_eq!(
        presence("arcade,bananablocks,woc,bitails").await,
        Some(NetworkPresence::Present {
            height: Some(972_400)
        })
    );
    TEST_HTTP_RESPONSES.with(|r| r.borrow_mut().clear());
}
