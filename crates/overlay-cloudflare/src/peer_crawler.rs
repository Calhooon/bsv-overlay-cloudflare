//! Peer crawler — pulls `/lookup` output-lists from configured non-GASP
//! overlay peers and re-submits each returned BEEF into our own engine so
//! the records appear in our D1.
//!
//! # Why this exists
//!
//! The engine already speaks GASP (`engine.start_gasp_sync()`) for peers
//! that expose `/requestSyncResponse`. The only UHRP-carrying peer today
//! (`overlay-us-1.bsvb.tech`) does NOT speak GASP — they return
//! `ERR_ROUTE_NOT_FOUND` on that path (probed 2026-04-21). They do
//! expose `/lookup` and `/submit`, so we HTTP-crawl them instead.
//!
//! This module is **bsvb-specific compatibility glue**, scoped to drop
//! when bsvb (or any other non-GASP peer in the config) adds GASP
//! support. The GASP plumbing in overlay-engine is the long-term
//! correct answer; this is the bridge.
//!
//! # Design
//!
//! For each configured (peer, service → topic) mapping:
//!
//! 1. `POST peer/lookup {"service": "<service>", "query": {"findAll": true}}`
//! 2. Parse the JSON response as `OutputList { outputs: [{ beef, outputIndex }] }`
//! 3. For each output, call our own `engine.submit(TaggedBEEF{beef, topics: [topic]}, CurrentTx)`
//! 4. Skip-on-duplicate is already the engine's behaviour (tm_X
//!    `is_dupe` check), so crawling the same peer repeatedly is
//!    idempotent — no new txs, just admit-decision replay.
//!
//! Errors are per-peer, per-service: a failed lookup logs + continues
//! to the next service; a failed submit logs + continues to the next
//! output. Never returns Err from the crawl driver; partial success
//! is the expected common case (e.g. bsvb sends 80 records, our tm_X
//! admits 70 and rejects 10 for stale-expiry or bad-sig — still a net
//! win).

use overlay_engine::engine::Engine;
use overlay_engine::types::{SubmitMode, TaggedBEEF};
use serde::Deserialize;
use std::collections::HashMap;

/// One (peer_url, service_to_topic) mapping. `service_to_topic` is a
/// per-peer list of `(lookup_service_name, topic_manager_name)` pairs
/// — e.g. `[("ls_uhrp", "tm_uhrp"), ("ls_ship", "tm_ship")]`. Separate
/// per-peer because peers might carry different subsets of services.
#[derive(Debug, Clone)]
pub struct PeerConfig {
    pub peer_url: String,
    pub service_to_topic: Vec<(String, String)>,
}

/// Summary of one crawl run.
#[derive(Debug, Default)]
pub struct CrawlResult {
    /// Per-peer, per-service admission counts.
    ///
    /// Keyed by `"{peer_url}|{service}"`. Value = number of outputs
    /// admitted by the corresponding tm_X on our engine.
    pub admitted_by: HashMap<String, usize>,
    /// Per-peer, per-service total submission attempts (admitted +
    /// rejected). `admitted_by[k] <= attempted[k]` always.
    pub attempted: HashMap<String, usize>,
    /// Errors per `"{peer_url}|{service}"`. Non-empty value = at least
    /// one submission failed but crawl continued.
    pub errors: HashMap<String, Vec<String>>,
    /// Peer-level errors (lookup failed entirely; couldn't even
    /// enumerate records).
    pub peer_errors: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct OutputListResponse {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    outputs: Vec<OutputEntry>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct OutputEntry {
    /// BEEF as a JSON number array — same shape the rest of the
    /// overlay family uses (TS SDK, Go SDK, our own /lookup handler).
    pub(crate) beef: Vec<u8>,
}

/// Crawl every configured peer, admitting whatever their `/lookup`
/// returns into our own engine. Fire-and-forget per-output — errors
/// are logged and stashed in the result, never raised.
///
/// `origin` is just for log prefixing ("cron" vs "admin") so you can
/// tell at a glance whether a log line came from the 15-min cron or
/// from an operator-triggered `/admin/crawlPeers`.
pub async fn crawl_peers(engine: &Engine, peers: &[PeerConfig], origin: &str) -> CrawlResult {
    let mut result = CrawlResult::default();
    for peer in peers {
        for (service, topic) in &peer.service_to_topic {
            let key = format!("{}|{}", peer.peer_url, service);
            match crawl_one(engine, &peer.peer_url, service, topic, origin).await {
                Ok((attempted, admitted, errors)) => {
                    result.attempted.insert(key.clone(), attempted);
                    result.admitted_by.insert(key.clone(), admitted);
                    if !errors.is_empty() {
                        result.errors.insert(key, errors);
                    }
                }
                Err(e) => {
                    worker::console_log!(
                        "[{origin}] peer-crawl: {peer}/{service} lookup failed: {err}",
                        peer = peer.peer_url,
                        service = service,
                        err = e,
                    );
                    result.peer_errors.insert(key, e);
                }
            }
        }
    }
    result
}

/// Crawl one (peer, service) and return `(attempted, admitted, errors)`.
/// Errs only if the lookup itself failed; per-output submit failures are
/// bundled into the `errors` vec and keep the crawl going.
async fn crawl_one(
    engine: &Engine,
    peer_url: &str,
    service: &str,
    topic: &str,
    origin: &str,
) -> Result<(usize, usize, Vec<String>), String> {
    let outputs = fetch_lookup(peer_url, service).await?;
    let mut admitted_total: usize = 0;
    let mut errors: Vec<String> = Vec::new();
    for (i, entry) in outputs.iter().enumerate() {
        let tagged = TaggedBEEF {
            beef: entry.beef.clone(),
            topics: vec![topic.to_string()],
            off_chain_values: None,
        };
        match engine.submit(&tagged, SubmitMode::CurrentTx).await {
            Ok(steak) => {
                let admitted: usize = steak.values().map(|a| a.outputs_to_admit.len()).sum();
                admitted_total += admitted;
            }
            Err(e) => {
                let msg = format!("output[{i}] submit failed: {e}");
                worker::console_log!(
                    "[{origin}] peer-crawl: {peer}/{service} {msg}",
                    peer = peer_url,
                    service = service,
                );
                errors.push(msg);
            }
        }
    }
    worker::console_log!(
        "[{origin}] peer-crawl: {peer}/{service} attempted={attempted} admitted={admitted} errors={errs}",
        peer = peer_url,
        service = service,
        attempted = outputs.len(),
        admitted = admitted_total,
        errs = errors.len(),
    );
    Ok((outputs.len(), admitted_total, errors))
}

/// POST `{peer_url}/lookup` with a `findAll` query and parse the
/// output-list response. Returns just the output entries — any
/// `free-text-reply` responses (not applicable for our services) are
/// treated as empty.
async fn fetch_lookup(peer_url: &str, service: &str) -> Result<Vec<OutputEntry>, String> {
    let url = format!("{}/lookup", peer_url.trim_end_matches('/'));

    let body_json = serde_json::json!({
        "service": service,
        "query": { "findAll": true }
    });
    let body_str = body_json.to_string();

    let mut init = worker::RequestInit::new();
    init.with_method(worker::Method::Post);
    let headers = worker::Headers::new();
    let _ = headers.set("Content-Type", "application/json");
    init.with_headers(headers);
    init.with_body(Some(body_str.into()));

    let request =
        worker::Request::new_with_init(&url, &init).map_err(|e| format!("request build: {e}"))?;

    let mut response = worker::Fetch::Request(request)
        .send()
        .await
        .map_err(|e| format!("fetch {url}: {e}"))?;

    let status = response.status_code();
    if !(200..300).contains(&status) {
        return Err(format!("peer {url} returned HTTP {status}"));
    }

    let text = response
        .text()
        .await
        .map_err(|e| format!("read body: {e}"))?;

    parse_output_list(&text)
}

/// Pure parser for the peer's `/lookup` JSON response. Extracted for
/// unit-testability — avoids a network roundtrip in tests.
pub(crate) fn parse_output_list(body: &str) -> Result<Vec<OutputEntry>, String> {
    let parsed: OutputListResponse = serde_json::from_str(body)
        .map_err(|e| format!("parse output-list: {e}; body preview: {}", preview(body)))?;
    if parsed.kind != "output-list" {
        // Not all peers respond with output-list; some return
        // free-text-reply or an error object. Treat non-output-list
        // as "nothing to ingest" rather than failing — matches the
        // engine's lookup_service trait contract.
        return Ok(Vec::new());
    }
    Ok(parsed.outputs)
}

fn preview(s: &str) -> String {
    const N: usize = 200;
    if s.len() <= N {
        s.to_string()
    } else {
        format!("{}…", &s[..N])
    }
}

// ============================================================================
// Tests — wasm-free, no worker::Fetch
// ============================================================================

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn parses_output_list_with_two_entries() {
        let body = r#"{
            "type": "output-list",
            "outputs": [
                {"beef": [1, 2, 3, 4], "outputIndex": 0},
                {"beef": [9, 8, 7], "outputIndex": 1}
            ]
        }"#;
        let outputs = parse_output_list(body).unwrap();
        assert_eq!(outputs.len(), 2);
        assert_eq!(outputs[0].beef, vec![1, 2, 3, 4]);
        assert_eq!(outputs[1].beef, vec![9, 8, 7]);
    }

    #[test]
    fn returns_empty_on_non_output_list_type() {
        // `free-text-reply` is a real LookupAnswer variant we may see
        // from some services. Don't crash; ingest nothing.
        let body = r#"{"type": "free-text-reply", "message": "no results"}"#;
        let outputs = parse_output_list(body).unwrap();
        assert!(outputs.is_empty());
    }

    #[test]
    fn returns_empty_on_missing_outputs_field() {
        let body = r#"{"type": "output-list"}"#;
        let outputs = parse_output_list(body).unwrap();
        assert!(outputs.is_empty());
    }

    #[test]
    fn rejects_invalid_json() {
        let body = r#"{not valid json"#;
        let err = parse_output_list(body).unwrap_err();
        assert!(err.contains("parse output-list"), "err: {err}");
        // preview() included to aid debugging in production logs
        assert!(err.contains("body preview"), "err: {err}");
    }

    #[test]
    fn preview_truncates_long_bodies() {
        let long = "x".repeat(500);
        let p = preview(&long);
        assert!(p.ends_with('…'));
        assert!(p.len() < long.len());
    }

    #[test]
    fn preview_returns_short_bodies_verbatim() {
        let p = preview("short");
        assert_eq!(p, "short");
    }

    #[test]
    fn peer_config_carries_multiple_services() {
        // Shape assertion — catches an accidental type-regression on
        // the config surface without a real crawl.
        let cfg = PeerConfig {
            peer_url: "https://x.example".to_string(),
            service_to_topic: vec![
                ("ls_uhrp".to_string(), "tm_uhrp".to_string()),
                ("ls_ship".to_string(), "tm_ship".to_string()),
                ("ls_slap".to_string(), "tm_slap".to_string()),
            ],
        };
        assert_eq!(cfg.service_to_topic.len(), 3);
    }

    #[test]
    fn crawl_result_keys_by_peer_service_pair() {
        // Lock the key shape — operators grep these in logs. Changing
        // the format is a breaking ops change.
        let key = format!("{}|{}", "https://overlay-us-1.bsvb.tech", "ls_uhrp");
        assert_eq!(key, "https://overlay-us-1.bsvb.tech|ls_uhrp");
    }
}
