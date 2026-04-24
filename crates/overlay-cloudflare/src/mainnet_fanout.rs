//! Mainnet SHIP fan-out — ports `@bsv/sdk` `SHIPBroadcaster` behavior for
//! Cloudflare Workers.
//!
//! When the Engine admits a transaction, its built-in SHIP propagation loop
//! (`engine.rs:654-712`) only broadcasts to peers indexed in our own
//! `ls_ship` storage. That's fine for a mature overlay whose SHIP directory
//! is already populated, but on a fresh node or during BSVA's migration
//! windows, our local view of the tm_uhrp peer set is stale.
//!
//! This module reaches the mainnet DEFAULT_SLAP_TRACKERS directly — the
//! same way a TS-SDK `LookupResolver({networkPreset:'mainnet'})` would —
//! to pull the authoritative peer list for each topic, then POSTs the BEEF
//! to every discovered peer. By running this alongside the Engine's local
//! propagation, our fresh records reach every tm_uhrp overlay on mainnet
//! within seconds of admission.
//!
//! ## Parity references
//!
//! - `bsv-rs/src/overlay/topic_broadcaster.rs::TopicBroadcaster::broadcast_tx`
//!   (SHIPBroadcaster in @bsv/sdk) — discover via `ls_ship` + parallel POST.
//! - `bsv-rs/src/overlay/facilitators.rs` — HTTP facilitator shape, mirrored
//!   here with `worker::Fetch` because `reqwest` isn't available in the
//!   Workers runtime.
//! - `bsv-storage-cloudflare/src/overlay/mod.rs::fan_out_to_mainnet_peers` —
//!   same pattern at the storage-server layer; this module retires that
//!   duplication once we've verified we're covering both call sites.
//!
//! ## Correctness contract
//!
//! - **Best-effort**. Per-peer errors are logged and swallowed; the caller
//!   has already locally admitted the tx, so partial mainnet coverage is
//!   acceptable.
//! - **Self-filter**. If a tracker returns our own URL as a peer, we skip
//!   the POST — otherwise we'd submit to ourselves in a loop.
//! - **Dedup**. Different trackers may return the same peer; we fan out to
//!   each peer exactly once per call.
//! - **Per-topic**. Multi-topic BEEFs get one `ls_ship` query per topic to
//!   discover the union of interested peers, then the BEEF is submitted
//!   once per peer with the full topic list (mirrors SHIPBroadcaster).

use std::collections::HashSet;

use bsv_rs::script::templates::PushDrop;
use bsv_rs::transaction::Transaction;
use overlay_engine::types::TaggedBEEF;
use worker::{Fetch, Headers, Method, Request, RequestInit};

/// Default SLAP trackers for mainnet — lifted verbatim from
/// `@bsv/sdk/src/overlay-tools/LookupResolver.ts:42` via
/// `bsv_rs::overlay::types::NetworkPreset::Mainnet.slap_trackers()`. Kept
/// inline here so the module is self-contained and doesn't require the
/// caller to thread a `NetworkPreset` through.
const DEFAULT_SLAP_TRACKERS: &[&str] = &[
    "https://overlay-us-1.bsvb.tech",
    "https://overlay-eu-1.bsvb.tech",
    "https://overlay-ap-1.bsvb.tech",
    "https://users.bapp.dev",
];

/// SHIP lookup question name — constant per BRC-95.
const LS_SHIP: &str = "ls_ship";

/// Fan out a tagged BEEF to every mainnet peer that runs any of its
/// topics. Discovers peers by querying `ls_ship` at each
/// [`DEFAULT_SLAP_TRACKERS`] and parsing the returned SHIP PushDrop records
/// for host URLs. `self_host`, if set, is filtered out of the peer set so
/// we never round-trip a submit through ourselves.
///
/// All errors are logged; none propagate. The primary admission path — the
/// Engine's `submit()` — has already handled correctness; this is a
/// best-effort fan-out.
pub async fn fan_out(tagged: &TaggedBEEF, self_host: Option<&str>) {
    if tagged.topics.is_empty() {
        return;
    }

    let mut peers: HashSet<String> = HashSet::new();
    for topic in &tagged.topics {
        for tracker in DEFAULT_SLAP_TRACKERS {
            match discover_peers_from_tracker(tracker, topic).await {
                Ok(found) => {
                    for host in found {
                        peers.insert(host);
                    }
                }
                Err(e) => {
                    worker::console_warn!(
                        "mainnet fan-out: tracker {} ls_ship({}) failed: {}",
                        tracker,
                        topic,
                        e
                    );
                }
            }
        }
    }

    // Filter self (we already locally admitted — no need to round-trip)
    if let Some(us) = self_host {
        let normalized = us.trim_end_matches('/').to_string();
        peers.remove(&normalized);
        peers.remove(&format!("{normalized}/"));
    }

    worker::console_log!(
        "mainnet fan-out: submitting topics={} to {} peer(s)",
        tagged.topics.join(","),
        peers.len()
    );

    for peer in peers {
        match post_beef_to_peer(&peer, tagged).await {
            Ok(()) => worker::console_log!("mainnet fan-out: peer {} accepted", peer),
            Err(e) => worker::console_warn!("mainnet fan-out: peer {} rejected: {}", peer, e),
        }
    }
}

/// Query a SLAP tracker's `ls_ship` for all peers running `topic`.
/// Returns host URLs (the `field[2]` of each SHIP PushDrop).
///
/// SHIP PushDrop layout per `bsv-rs/src/overlay/overlay_admin_token_template.rs`:
///
/// - `field[0]` = "SHIP" literal
/// - `field[1]` = host identity key (33-byte compressed pubkey)
/// - `field[2]` = host HTTPS URL (UTF-8)
/// - `field[3]` = topic name (UTF-8)
/// - `field[4]` = signature (ECDSA DER + sighash flag)
async fn discover_peers_from_tracker(
    tracker: &str,
    topic: &str,
) -> Result<Vec<String>, String> {
    let url = format!("{}/lookup", tracker.trim_end_matches('/'));
    let body = serde_json::json!({
        "service": LS_SHIP,
        "query": { "topics": [topic] }
    })
    .to_string();

    let headers = Headers::new();
    headers
        .set("Content-Type", "application/json")
        .map_err(|e| e.to_string())?;

    let mut init = RequestInit::new();
    init.with_method(Method::Post)
        .with_headers(headers)
        .with_body(Some(worker::wasm_bindgen::JsValue::from_str(&body)));

    let request = Request::new_with_init(&url, &init).map_err(|e| e.to_string())?;
    let mut response = Fetch::Request(request)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    let status = response.status_code();
    if !(200..300).contains(&status) {
        return Err(format!("HTTP {status}"));
    }
    let text = response.text().await.map_err(|e| e.to_string())?;

    #[derive(serde::Deserialize)]
    struct LookupOutput {
        beef: Vec<u8>,
        #[serde(rename = "outputIndex")]
        output_index: u32,
    }
    #[derive(serde::Deserialize)]
    struct LookupAnswer {
        #[serde(rename = "type")]
        answer_type: String,
        #[serde(default)]
        outputs: Vec<LookupOutput>,
    }

    let answer: LookupAnswer =
        serde_json::from_str(&text).map_err(|e| format!("parse: {e}"))?;
    if answer.answer_type != "output-list" {
        return Ok(Vec::new());
    }

    let mut urls = Vec::new();
    for out in answer.outputs {
        let tx = match Transaction::from_beef(&out.beef, None) {
            Ok(t) => t,
            Err(_) => continue,
        };
        let vout = out.output_index as usize;
        let locking_script = match tx.outputs.get(vout) {
            Some(o) => &o.locking_script,
            None => continue,
        };
        let decoded = match PushDrop::decode(locking_script) {
            Ok(d) => d,
            Err(_) => continue,
        };
        // SHIP field[2] = host URL as UTF-8
        if let Some(url_bytes) = decoded.fields.get(2) {
            if let Ok(s) = std::str::from_utf8(url_bytes) {
                let trimmed = s.trim().trim_end_matches('/').to_string();
                if !trimmed.is_empty() && trimmed.starts_with("http") {
                    urls.push(trimmed);
                }
            }
        }
    }
    Ok(urls)
}

/// POST a BEEF to a peer's `/submit` endpoint with standard SHIPBroadcaster
/// headers (`Content-Type: application/octet-stream`, `X-Topics: JSON
/// array`). Doesn't parse the Steak body — fan-out targets we don't control
/// may return non-standard shapes; a 2xx status is enough.
async fn post_beef_to_peer(peer_host: &str, tagged: &TaggedBEEF) -> Result<(), String> {
    let url = format!("{}/submit", peer_host.trim_end_matches('/'));
    let topics_header = serde_json::to_string(&tagged.topics).map_err(|e| e.to_string())?;

    let headers = Headers::new();
    headers
        .set("Content-Type", "application/octet-stream")
        .map_err(|e| e.to_string())?;
    headers
        .set("X-Topics", &topics_header)
        .map_err(|e| e.to_string())?;

    let body_js = js_sys::Uint8Array::from(tagged.beef.as_slice());
    let mut init = RequestInit::new();
    init.with_method(Method::Post)
        .with_headers(headers)
        .with_body(Some(worker::wasm_bindgen::JsValue::from(body_js)));

    let request = Request::new_with_init(&url, &init).map_err(|e| e.to_string())?;
    let response = Fetch::Request(request)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    let status = response.status_code();
    if (200..300).contains(&status) {
        Ok(())
    } else {
        Err(format!("HTTP {status}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_trackers_are_https() {
        // Guard: an accidental `http://` slip would silently fail in the
        // Worker runtime (mixed-content rejection). Cheap compile-time-ish
        // check.
        for t in DEFAULT_SLAP_TRACKERS {
            assert!(t.starts_with("https://"), "tracker {t} must be https");
        }
    }

    #[test]
    fn tracker_list_matches_bsv_rs_mainnet() {
        // Sanity: our list should match `bsv_rs::overlay::types::
        // NetworkPreset::Mainnet.slap_trackers()` byte-for-byte. If bsv-rs
        // rotates one, we must too — otherwise fresh clients discover
        // peers we don't know about.
        use bsv_rs::overlay::NetworkPreset;
        let canonical: Vec<String> = NetworkPreset::Mainnet
            .slap_trackers()
            .iter()
            .map(|s| s.to_string())
            .collect();
        let ours: Vec<String> = DEFAULT_SLAP_TRACKERS.iter().map(|s| s.to_string()).collect();
        assert_eq!(
            ours, canonical,
            "DEFAULT_SLAP_TRACKERS drifted from bsv_rs::overlay::NetworkPreset::Mainnet"
        );
    }
}
