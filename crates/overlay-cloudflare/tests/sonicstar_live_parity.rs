//! Live byte-parity test for the sonicstar plugin against Ruth's
//! `https://sonicstar.net/api/overlay-parity/{admit,lookup,docs}` endpoints.
//!
//! This is the **authoritative** parity check for `tm_sonicstar` /
//! `ls_sonicstar`. The standard `parity-harness` (in `parity-harness/`)
//! diffs us against vanilla `@bsv/overlay-express@2.2.0`, which has no
//! sonicstar plugin — so its scenarios are flagged as expected
//! divergences (see `parity-harness/corpus/sonicstar/`). Only Ruth's
//! reference endpoint exercises the actual SonicStar TS classes
//! (`SonicStarTopicManager`, `SonicStarLookupService`), and that's what
//! this test compares against.
//!
//! ## Running
//!
//! ```text
//! SONICSTAR_REFERENCE_URL=https://sonicstar.net/api/overlay-parity \
//! OVERLAY_URL=https://your-overlay.workers.dev \
//! cargo test -p bsv-overlay-cloudflare \
//!     --test sonicstar_live_parity -- --ignored --nocapture
//! ```
//!
//! ## Required env vars
//!
//! - `OVERLAY_URL` — base URL of our deployed worker, e.g.
//!   `https://your-overlay.workers.dev`. Must have `tm_sonicstar` /
//!   `ls_sonicstar` enabled in `TOPIC_MANAGERS` / `LOOKUP_SERVICES` env.
//!
//! ## Optional env vars
//!
//! - `SONICSTAR_REFERENCE_URL` — defaults to
//!   `https://sonicstar.net/api/overlay-parity` (Ruth's published live
//!   reference). Override only if she stands up a staging mirror. The
//!   test appends `/admit`, `/lookup`, `/docs` as needed.
//! - `SONICSTAR_REFERENCE_TXIDS` — CSV of mainnet sssp txids (e.g. the
//!   12 Ruth provided). When set, [`live_admit_diff_for_known_txids`]
//!   fetches BEEF for each from WhatsOnChain and POSTs to both `/admit`
//!   endpoints; outpoints are diffed. Without this var, the BEEF
//!   admission diff is skipped — the lookup matrix still runs.
//! - `WHATSONCHAIN_URL` — defaults to `https://api.whatsonchain.com`.
//! - `OVERLAY_TOPIC_HEADER` — value for our worker's `X-Topics` header
//!   on the `/submit` call. Defaults to `["tm_sonicstar"]`.
//!
//! ## What gets diffed
//!
//! For every comparison we extract the outpoint set
//! `{(txid, outputIndex)}` from each side, sort by `(txid,
//! outputIndex)`, and assert equality. Indexer-state fields like
//! `indexBuiltAt` / `indexSize` are ignored. The richer record shape
//! that Ruth's `/lookup` endpoint can return is not asserted here —
//! the engine's `/lookup` contract is outpoints-only, so a record-shape
//! diff would require a separate `/sonicstar/records` route on our
//! side. That is out of scope per plan §10 / Q2.

#![cfg(not(target_arch = "wasm32"))]

use bsv_rs::transaction::Transaction;
use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::env;
use std::time::Duration;

const SONICSTAR_TOPIC: &str = "tm_sonicstar";
const SONICSTAR_SERVICE: &str = "ls_sonicstar";

const ENV_REFERENCE_URL: &str = "SONICSTAR_REFERENCE_URL";
const ENV_OVERLAY_URL: &str = "OVERLAY_URL";
const ENV_TXIDS: &str = "SONICSTAR_REFERENCE_TXIDS";
const ENV_WOC_URL: &str = "WHATSONCHAIN_URL";
const ENV_TOPIC_HEADER: &str = "OVERLAY_TOPIC_HEADER";

/// Ruth's published live reference. See plan §5 Layer C and the SonicStar
/// project brief. Override via [`ENV_REFERENCE_URL`] only when targeting
/// a staging mirror.
const DEFAULT_REFERENCE_URL: &str = "https://sonicstar.net/api/overlay-parity";
const DEFAULT_WOC_URL: &str = "https://api.whatsonchain.com";
const DEFAULT_TOPIC_HEADER: &str = "[\"tm_sonicstar\"]";

fn require_env(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| {
        panic!(
            "{name} not set. This is an --ignored integration test; export the var to opt in."
        )
    })
}

/// Reference URL with default fallback to [`DEFAULT_REFERENCE_URL`]. The
/// reference is a public endpoint — defaulting it makes the test
/// runnable with just `OVERLAY_URL` set.
fn reference_url() -> String {
    env::var(ENV_REFERENCE_URL).unwrap_or_else(|_| DEFAULT_REFERENCE_URL.to_string())
}

fn http_client() -> Client {
    Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("build reqwest client")
}

/// Outpoint canonical form for set comparison.
type Outpoint = (String, u32);

fn sort_outpoints(mut v: Vec<Outpoint>) -> Vec<Outpoint> {
    v.sort();
    v
}

fn dedup_sorted(v: Vec<Outpoint>) -> Vec<Outpoint> {
    let set: BTreeSet<Outpoint> = v.into_iter().collect();
    set.into_iter().collect()
}

// ---------- response normalization ----------------------------------

/// Extract outpoints from our `/lookup` response. Our worker returns
/// the standard overlay-express shape:
///
/// ```text
/// { "type": "output-list", "outputs": [{ "beef": [...], "outputIndex": n }, ...] }
/// ```
///
/// Each output's `txid` lives inside its BEEF, so we parse the BEEF
/// and call `Transaction::id()`.
fn outpoints_from_our_lookup(resp: &Value) -> Vec<Outpoint> {
    let outputs = resp
        .get("outputs")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut out = Vec::with_capacity(outputs.len());
    for o in outputs {
        let beef_bytes: Vec<u8> = match o.get("beef").and_then(Value::as_array) {
            Some(arr) => arr
                .iter()
                .filter_map(|v| v.as_u64().map(|n| n as u8))
                .collect(),
            None => continue,
        };
        let output_index = match o.get("outputIndex").and_then(Value::as_u64) {
            Some(n) => n as u32,
            None => continue,
        };
        let tx = match Transaction::from_beef(&beef_bytes, None) {
            Ok(t) => t,
            Err(e) => {
                eprintln!("WARN: skipping un-parsable BEEF in /lookup response: {e}");
                continue;
            }
        };
        out.push((tx.id(), output_index));
    }
    out
}

/// Extract outpoints from Ruth's `/api/overlay-parity/lookup` response.
/// Per plan §5 Layer C the endpoint returns:
///
/// ```text
/// { "outpoints": [{ "txid": "...", "outputIndex": n }, ...], ... }
/// ```
///
/// Indexer-state fields (`indexBuiltAt`, `indexSize`) are ignored.
fn outpoints_from_ruths_lookup(resp: &Value) -> Vec<Outpoint> {
    let arr = resp
        .get("outpoints")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    arr.into_iter()
        .filter_map(|o| {
            let txid = o.get("txid")?.as_str()?.to_string();
            let oi = o.get("outputIndex")?.as_u64()? as u32;
            Some((txid, oi))
        })
        .collect()
}

fn assert_outpoint_sets_match(
    label: &str,
    ours: Vec<Outpoint>,
    theirs: Vec<Outpoint>,
) {
    let ours = dedup_sorted(sort_outpoints(ours));
    let theirs = dedup_sorted(sort_outpoints(theirs));
    if ours != theirs {
        eprintln!("\n[{label}] outpoint diff:");
        eprintln!("  ours   ({}): {:?}", ours.len(), ours);
        eprintln!("  theirs ({}): {:?}", theirs.len(), theirs);
        let only_ours: Vec<_> = ours.iter().filter(|o| !theirs.contains(o)).collect();
        let only_theirs: Vec<_> = theirs.iter().filter(|o| !ours.contains(o)).collect();
        if !only_ours.is_empty() {
            eprintln!("  only ours:   {only_ours:?}");
        }
        if !only_theirs.is_empty() {
            eprintln!("  only theirs: {only_theirs:?}");
        }
        panic!("[{label}] outpoint sets diverge");
    }
}

// ---------- shared HTTP helpers ------------------------------------

async fn post_lookup(client: &Client, base: &str, query: Value) -> Value {
    let url = format!("{}/lookup", base.trim_end_matches('/'));
    let body = json!({ "service": SONICSTAR_SERVICE, "query": query });
    let resp = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .unwrap_or_else(|e| panic!("POST {url}: {e}"));
    let status = resp.status();
    let bytes = resp
        .bytes()
        .await
        .unwrap_or_else(|e| panic!("read {url}: {e}"));
    if !status.is_success() {
        // Some divergences may legitimately surface here (e.g. invalid
        // query rejections). Return the body as JSON if parsable so
        // callers can assert on the shape.
        return serde_json::from_slice(&bytes).unwrap_or_else(|e| {
            json!({
                "_http_status": status.as_u16(),
                "_parse_error": format!("{e}"),
                "_body_text": String::from_utf8_lossy(&bytes).into_owned(),
            })
        });
    }
    serde_json::from_slice(&bytes).unwrap_or_else(|e| {
        panic!("parse {url} body as JSON: {e} — body: {}", String::from_utf8_lossy(&bytes))
    })
}

async fn fetch_beef_from_woc(client: &Client, txid: &str) -> Result<Vec<u8>, String> {
    let base = env::var(ENV_WOC_URL).unwrap_or_else(|_| DEFAULT_WOC_URL.to_string());
    let url = format!("{}/v1/bsv/main/tx/{txid}/beef", base.trim_end_matches('/'));
    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("GET {url}: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("GET {url} -> HTTP {}", resp.status()));
    }
    Ok(resp.bytes().await.map_err(|e| e.to_string())?.to_vec())
}

async fn submit_beef_to_our_worker(
    client: &Client,
    overlay_url: &str,
    beef: &[u8],
) -> (StatusCode, Value) {
    let url = format!("{}/submit", overlay_url.trim_end_matches('/'));
    let topics_header = env::var(ENV_TOPIC_HEADER)
        .unwrap_or_else(|_| DEFAULT_TOPIC_HEADER.to_string());
    let resp = client
        .post(&url)
        .header("X-Topics", topics_header)
        .header("Content-Type", "application/octet-stream")
        .body(beef.to_vec())
        .send()
        .await
        .unwrap_or_else(|e| panic!("POST {url}: {e}"));
    let status = resp.status();
    let bytes = resp.bytes().await.unwrap_or_default();
    let json: Value = serde_json::from_slice(&bytes).unwrap_or_else(|_| {
        json!({ "_body_text": String::from_utf8_lossy(&bytes).into_owned() })
    });
    (status, json)
}

async fn submit_beef_to_ruth(
    client: &Client,
    reference_url: &str,
    beef: &[u8],
) -> (StatusCode, Value) {
    // Ruth's `/api/overlay-parity/admit` accepts the BEEF as raw bytes
    // (the same shape our `/submit` accepts); see plan §5 Layer C. If the
    // production endpoint expects a JSON wrapper (`{ "beef": [...] }`)
    // instead, this is the single place to flip the convention.
    let url = format!("{}/admit", reference_url.trim_end_matches('/'));
    let resp = client
        .post(&url)
        .header("Content-Type", "application/octet-stream")
        .body(beef.to_vec())
        .send()
        .await
        .unwrap_or_else(|e| panic!("POST {url}: {e}"));
    let status = resp.status();
    let bytes = resp.bytes().await.unwrap_or_default();
    let json: Value = serde_json::from_slice(&bytes).unwrap_or_else(|_| {
        json!({ "_body_text": String::from_utf8_lossy(&bytes).into_owned() })
    });
    (status, json)
}

// ---------- tests ----------------------------------------------------

/// Diff the full enumeration. `findAll` (string) is the canonical TS
/// sentinel, so we use that form here. Empty store → both sides return
/// `[]` and the diff is trivially equal, which still validates that
/// both endpoints are reachable and respond.
#[ignore]
#[tokio::test]
async fn live_lookup_findall_diff() {
    let reference = reference_url();
    let overlay = require_env(ENV_OVERLAY_URL);
    let client = http_client();

    let ours = post_lookup(&client, &overlay, json!("findAll")).await;
    let theirs = post_lookup(&client, &reference, json!("findAll")).await;

    assert_outpoint_sets_match(
        "lookup_findall",
        outpoints_from_our_lookup(&ours),
        outpoints_from_ruths_lookup(&theirs),
    );
}

#[ignore]
#[tokio::test]
async fn live_lookup_findall_object_diff() {
    let reference = reference_url();
    let overlay = require_env(ENV_OVERLAY_URL);
    let client = http_client();

    let ours = post_lookup(&client, &overlay, json!({ "findAll": true })).await;
    let theirs = post_lookup(&client, &reference, json!({ "findAll": true })).await;

    assert_outpoint_sets_match(
        "lookup_findall_object",
        outpoints_from_our_lookup(&ours),
        outpoints_from_ruths_lookup(&theirs),
    );
}

#[ignore]
#[tokio::test]
async fn live_lookup_by_artist_name_diff() {
    let reference = reference_url();
    let overlay = require_env(ENV_OVERLAY_URL);
    let client = http_client();

    // The substring "a" is a sentinel — at the scale of Ruth's catalog
    // it should match many records but is harmless if it matches none.
    // Override via a future env var (e.g. SONICSTAR_TEST_ARTIST_SUBSTR)
    // if a more specific probe is needed.
    let q = json!({ "artistName": "a" });
    let ours = post_lookup(&client, &overlay, q.clone()).await;
    let theirs = post_lookup(&client, &reference, q).await;

    assert_outpoint_sets_match(
        "lookup_by_artist",
        outpoints_from_our_lookup(&ours),
        outpoints_from_ruths_lookup(&theirs),
    );
}

#[ignore]
#[tokio::test]
async fn live_lookup_by_search_text_diff() {
    let reference = reference_url();
    let overlay = require_env(ENV_OVERLAY_URL);
    let client = http_client();

    let q = json!({ "searchText": "the" });
    let ours = post_lookup(&client, &overlay, q.clone()).await;
    let theirs = post_lookup(&client, &reference, q).await;

    assert_outpoint_sets_match(
        "lookup_by_search_text",
        outpoints_from_our_lookup(&ours),
        outpoints_from_ruths_lookup(&theirs),
    );
}

#[ignore]
#[tokio::test]
async fn live_lookup_pagination_consistent() {
    let reference = reference_url();
    let overlay = require_env(ENV_OVERLAY_URL);
    let client = http_client();

    let q = json!({ "findAll": true, "limit": 5, "skip": 0 });
    let ours = post_lookup(&client, &overlay, q.clone()).await;
    let theirs = post_lookup(&client, &reference, q).await;
    assert_outpoint_sets_match(
        "lookup_pagination",
        outpoints_from_our_lookup(&ours),
        outpoints_from_ruths_lookup(&theirs),
    );
}

/// Both sides must reject a `null` query. The TS reference throws "A
/// valid query must be provided!"; our worker returns an error of
/// `LookupServiceError::InvalidQuery` shape. Strict text parity is
/// not required here — only that both reject (non-2xx OR error body).
#[ignore]
#[tokio::test]
async fn live_lookup_null_query_both_reject() {
    let reference = reference_url();
    let overlay = require_env(ENV_OVERLAY_URL);
    let client = http_client();

    let q = Value::Null;
    let ours = post_lookup(&client, &overlay, q.clone()).await;
    let theirs = post_lookup(&client, &reference, q).await;

    let ours_rejected = ours.get("_http_status").is_some()
        || ours.get("status").and_then(Value::as_str) == Some("error");
    let theirs_rejected = theirs.get("_http_status").is_some()
        || theirs.get("error").is_some()
        || theirs.get("status").and_then(Value::as_str) == Some("error");
    assert!(ours_rejected, "our worker did not reject null query: {ours:?}");
    assert!(theirs_rejected, "Ruth's reference did not reject null query: {theirs:?}");
}

/// Submit each of the known sssp txids' BEEFs to both endpoints and
/// confirm the resulting admitted-outpoint sets agree.
///
/// Skipped (with an info-level message) when `SONICSTAR_REFERENCE_TXIDS`
/// is unset — populating it requires the 12 mainnet txids Ruth
/// provides as the live-corpus seed.
#[ignore]
#[tokio::test]
async fn live_admit_diff_for_known_txids() {
    let reference = reference_url();
    let overlay = require_env(ENV_OVERLAY_URL);
    let client = http_client();

    let txids_csv = match env::var(ENV_TXIDS) {
        Ok(v) if !v.trim().is_empty() => v,
        _ => {
            eprintln!(
                "{ENV_TXIDS} unset — skipping admit diff. Populate with a CSV of mainnet sssp txids to enable."
            );
            return;
        }
    };
    let txids: Vec<String> = txids_csv
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    assert!(!txids.is_empty(), "{ENV_TXIDS} must contain at least one txid");

    for txid in &txids {
        eprintln!("[admit-diff] fetching BEEF for {txid}");
        let beef = match fetch_beef_from_woc(&client, txid).await {
            Ok(b) => b,
            Err(e) => panic!("[admit-diff] WhatsOnChain BEEF fetch for {txid} failed: {e}"),
        };

        let (ours_status, ours_body) = submit_beef_to_our_worker(&client, &overlay, &beef).await;
        let (theirs_status, theirs_body) = submit_beef_to_ruth(&client, &reference, &beef).await;

        let admit_succeeded = ours_status.is_success() && theirs_status.is_success();
        if !admit_succeeded {
            panic!(
                "[admit-diff:{txid}] non-2xx response from at least one side\n  ours: {ours_status} {ours_body:?}\n  theirs: {theirs_status} {theirs_body:?}"
            );
        }

        // After admitting, run a per-txid lookup on both sides and diff.
        let q = json!({ "txid": txid });
        let ours = post_lookup(&client, &overlay, q.clone()).await;
        let theirs = post_lookup(&client, &reference, q).await;
        assert_outpoint_sets_match(
            &format!("admit_then_lookup_{txid}"),
            outpoints_from_our_lookup(&ours),
            outpoints_from_ruths_lookup(&theirs),
        );
    }

    eprintln!("[admit-diff] all {} txids agreed", txids.len());
}

/// Both endpoints expose human-readable docs. Confirm both return
/// non-empty content when sonicstar is enabled.
#[ignore]
#[tokio::test]
async fn live_docs_both_endpoints_non_empty() {
    let reference = reference_url();
    let overlay = require_env(ENV_OVERLAY_URL);
    let client = http_client();

    // Our worker — the standard overlay-express path.
    let ours_topic_url = format!(
        "{}/getDocumentationForTopicManager?manager={SONICSTAR_TOPIC}",
        overlay.trim_end_matches('/')
    );
    let ours_lookup_url = format!(
        "{}/getDocumentationForLookupServiceProvider?lookupService={SONICSTAR_SERVICE}",
        overlay.trim_end_matches('/')
    );
    for url in [&ours_topic_url, &ours_lookup_url] {
        let body = client
            .get(url)
            .send()
            .await
            .unwrap_or_else(|e| panic!("GET {url}: {e}"))
            .text()
            .await
            .expect("read docs body");
        assert!(!body.is_empty(), "[docs:ours] empty body from {url}");
        assert!(
            body.to_lowercase().contains("sssp") || body.to_lowercase().contains("sonicstar"),
            "[docs:ours] {url} content does not look like sonicstar docs"
        );
    }

    // Ruth's reference — single `/docs` route per plan §5 Layer C.
    let theirs_url = format!("{}/docs", reference.trim_end_matches('/'));
    let body = client
        .get(&theirs_url)
        .send()
        .await
        .unwrap_or_else(|e| panic!("GET {theirs_url}: {e}"))
        .text()
        .await
        .expect("read theirs docs body");
    assert!(!body.is_empty(), "[docs:theirs] empty body from {theirs_url}");
}

#[cfg(test)]
mod helpers_self_check {
    //! Pure-Rust sanity checks for the helpers. Always runnable (not
    //! `#[ignore]`d), no network, no env vars.

    use super::*;
    use serde_json::json;

    #[test]
    fn outpoints_from_ruths_lookup_handles_canonical_shape() {
        let resp = json!({
            "outpoints": [
                { "txid": "aa", "outputIndex": 0 },
                { "txid": "bb", "outputIndex": 5 },
            ],
            "indexBuiltAt": "ignored",
            "indexSize": 12,
        });
        assert_eq!(
            outpoints_from_ruths_lookup(&resp),
            vec![("aa".to_string(), 0), ("bb".to_string(), 5)]
        );
    }

    #[test]
    fn outpoints_from_ruths_lookup_handles_empty_or_missing() {
        assert!(outpoints_from_ruths_lookup(&json!({})).is_empty());
        assert!(outpoints_from_ruths_lookup(&json!({ "outpoints": [] })).is_empty());
    }

    #[test]
    fn outpoints_from_our_lookup_skips_unparseable_beef() {
        // Outputs with a clearly-invalid BEEF blob are skipped, not
        // panicked. (Realistic BEEFs are validated end-to-end in the
        // ignored network tests.)
        let resp = json!({
            "type": "output-list",
            "outputs": [
                { "beef": [0, 1, 2, 3], "outputIndex": 0 },
            ],
        });
        assert!(outpoints_from_our_lookup(&resp).is_empty());
    }

    #[test]
    fn dedup_sorted_idempotent() {
        let v = vec![
            ("b".to_string(), 0),
            ("a".to_string(), 1),
            ("a".to_string(), 1),
            ("a".to_string(), 0),
        ];
        let dedup = dedup_sorted(sort_outpoints(v));
        assert_eq!(
            dedup,
            vec![
                ("a".to_string(), 0),
                ("a".to_string(), 1),
                ("b".to_string(), 0),
            ]
        );
    }
}
