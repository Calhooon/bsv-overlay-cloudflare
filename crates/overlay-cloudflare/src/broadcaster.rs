//! Worker-side broadcasters — uses Cloudflare Workers Fetch API to propagate
//! transactions to SHIP peers and to the BSV network via ARC.

use async_trait::async_trait;
use overlay_engine::broadcaster::{ArcBroadcaster, Broadcaster};
use overlay_engine::types::TaggedBEEF;

/// Broadcaster implementation using Cloudflare Workers `Fetch` API.
///
/// POSTs the BEEF bytes to `{host_url}/submit` with appropriate headers.
pub struct WorkerBroadcaster;

#[async_trait(?Send)]
impl Broadcaster for WorkerBroadcaster {
    async fn broadcast_to_host(
        &self,
        host_url: &str,
        tagged_beef: &TaggedBEEF,
    ) -> Result<(), String> {
        let url = format!("{}/submit", host_url.trim_end_matches('/'));

        let topics_json = serde_json::to_string(&tagged_beef.topics).map_err(|e| e.to_string())?;

        // Build the request
        let mut init = worker::RequestInit::new();
        init.with_method(worker::Method::Post);

        let headers = worker::Headers::new();
        let _ = headers.set("Content-Type", "application/octet-stream");
        let _ = headers.set("X-Topics", &topics_json);
        init.with_headers(headers);

        // Convert BEEF bytes to a Uint8Array for the body
        let uint8_array = js_sys::Uint8Array::from(tagged_beef.beef.as_slice());
        init.with_body(Some(uint8_array.into()));

        let request = worker::Request::new_with_init(&url, &init)
            .map_err(|e| format!("Failed to create request: {e}"))?;

        let response = worker::Fetch::Request(request)
            .send()
            .await
            .map_err(|e| format!("Fetch to {url} failed: {e}"))?;

        let status = response.status_code();
        if (200..300).contains(&status) {
            Ok(())
        } else {
            Err(format!("Peer {url} returned HTTP {status}"))
        }
    }
}

// ============================================================================
// ARC Broadcaster — network broadcast to miners via TAAL's ARC API
// ============================================================================

/// ARC broadcaster using Cloudflare Workers `Fetch` API.
///
/// POSTs the raw transaction (JSON `{ "rawTx": "<hex>" }`) to ARC's `/v1/tx`
/// endpoint, matching the TS SDK's `ARC.broadcast()` format.
pub struct WorkerArcBroadcaster {
    api_key: String,
}

impl WorkerArcBroadcaster {
    /// ARC mainnet endpoint.
    const ARC_URL: &'static str = "https://arc.taal.com";

    /// Create a new ARC broadcaster with the given TAAL API key.
    pub fn new(api_key: String) -> Self {
        Self { api_key }
    }
}

/// ARC `/v1/tx` JSON response.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct ArcResponse {
    #[serde(default)]
    txid: String,
    #[serde(default)]
    tx_status: String,
    #[serde(default)]
    extra_info: String,
}

/// The classified outcome of one ARC broadcast attempt (broadcast-gated
/// submit, bsv-low overlay-first 2026-07-17).
///
/// The three-way split is load-bearing for the gate:
/// - `Accepted` → the network took the tx (admit may proceed);
/// - `Rejected` → the network DEFINITIVELY refused it (admit must NOT proceed,
///   and no other broadcaster should be tried — a real rejection is not a
///   transport problem);
/// - transport/parse trouble is the `Err(String)` of [`arc_verdict`]'s caller
///   (retryable against a fallback broadcaster).
#[derive(Debug, PartialEq, Eq)]
pub enum ArcOutcome {
    /// Network accepted (or already knew) the tx; carries ARC's txid.
    Accepted(String),
    /// Network definitively rejected the tx; carries the reason.
    Rejected(String),
}

/// "The network already has this exact tx" — a redundant re-broadcast is
/// SUCCESS, whatever HTTP dress it arrives in (mirrors the bsv-low client's
/// `alreadyKnown`, incl. the literal 257 txn-already-known node code).
/// NEGATED forms are stripped first: "unknown"/"unseen" are failures.
fn already_known(s: &str) -> bool {
    let t = s.to_lowercase().replace("unknown", "").replace("unseen", "");
    t.contains("already")
        || t.contains("known")
        || t.contains("257")
        || t.contains("mined")
        || t.contains("seen")
}

/// PURE: classify one ARC HTTP response into accept / reject / transport
/// trouble (adversarial review 2026-07-17, finding 1 — the classification is
/// LOAD-BEARING: a definitive rejection refuses admission with NO fallback, so
/// only ARC's actual PER-TX verdict class may land there):
/// - "already known/mined" in any dress → `Accepted` (redundant re-broadcast);
/// - HTTP 460–479 (ARC's per-tx validation codes: 460 malformed, 461 unlock
///   invalid, 462/463/464, 465 fee floor, 473…) and 2xx-with-error-`txStatus`
///   → `Rejected` (definitive — a second broadcaster would say the same);
/// - EVERYTHING else non-2xx — 401/403 (a rotated/expired key), 404/405 (a
///   gateway misroute), 400, 429, 5xx — is TRANSPORT trouble (`Err`): the
///   caller tries the fallback host, and the client keeps its direct path.
///   `MINED_IN_STALE_BLOCK` is transient (reorged txs normally re-mine) —
///   transport, never a definitive refusal (finding 6).
pub fn arc_verdict(status: u16, body: &str) -> Result<ArcOutcome, String> {
    if (200..300).contains(&status) {
        // 2xx: the JSON txStatus is the verdict.
        let arc_resp: ArcResponse = match serde_json::from_str(body) {
            Ok(r) => r,
            Err(e) => return Err(format!("unparseable ARC response: {e} — body: {body}")),
        };
        let error_statuses = ["DOUBLE_SPEND_ATTEMPTED", "REJECTED", "INVALID", "MALFORMED"];
        let upper_status = arc_resp.tx_status.to_uppercase();
        let is_orphan = arc_resp.extra_info.to_uppercase().contains("ORPHAN")
            || upper_status.contains("ORPHAN");
        if error_statuses.iter().any(|s| upper_status == *s) || is_orphan {
            // A redundant re-broadcast dressed as an error is SUCCESS.
            if already_known(&arc_resp.extra_info) {
                return Ok(ArcOutcome::Accepted(arc_resp.txid));
            }
            return Ok(ArcOutcome::Rejected(
                format!("{} {}", arc_resp.tx_status, arc_resp.extra_info)
                    .trim()
                    .to_string(),
            ));
        }
        if upper_status == "MINED_IN_STALE_BLOCK" {
            return Err(format!("ARC transient: {upper_status}"));
        }
        return Ok(ArcOutcome::Accepted(arc_resp.txid));
    }
    // Non-2xx: an already-known/mined body is a redundant re-broadcast = ok.
    if already_known(body) {
        let txid = serde_json::from_str::<ArcResponse>(body)
            .map(|r| r.txid)
            .unwrap_or_default();
        return Ok(ArcOutcome::Accepted(txid));
    }
    if (460..480).contains(&status) {
        return Ok(ArcOutcome::Rejected(format!("ARC HTTP {status}: {body}")));
    }
    Err(format!("ARC HTTP {status}: {body}"))
}

/// One raw `{ "rawTx": <hex> }` POST to an ARC-compatible `/v1/tx`, returning
/// the classified verdict. `api_key: None` posts keyless (GorillaPool).
async fn post_arc_tx(base_url: &str, api_key: Option<&str>, tx_hex: &str) -> Result<ArcOutcome, String> {
    let url = format!("{}/v1/tx", base_url.trim_end_matches('/'));
    let body = serde_json::json!({ "rawTx": tx_hex }).to_string();

    let mut init = worker::RequestInit::new();
    init.with_method(worker::Method::Post);
    let headers = worker::Headers::new();
    let _ = headers.set("Content-Type", "application/json");
    if let Some(key) = api_key {
        let _ = headers.set("Authorization", &format!("Bearer {key}"));
    }
    init.with_headers(headers);
    init.with_body(Some(worker::wasm_bindgen::JsValue::from_str(&body)));

    let request = worker::Request::new_with_init(&url, &init)
        .map_err(|e| format!("Failed to create ARC request: {e}"))?;
    let mut response = worker::Fetch::Request(request)
        .send()
        .await
        .map_err(|e| format!("ARC fetch {url} failed: {e}"))?;
    let status = response.status_code();
    let text = response
        .text()
        .await
        .unwrap_or_else(|_| String::from("<no body>"));
    arc_verdict(status, &text)
}

/// GorillaPool's keyless ARC endpoint — the same fallback the bsv-low client
/// proxy uses. Tried only on TRANSPORT trouble, never after a real rejection.
const GORILLAPOOL_ARC_URL: &str = "https://arc.gorillapool.io";

/// Broadcast one Extended-Format (or raw) tx hex with TAAL-then-GorillaPool
/// transport fallback. A DEFINITIVE rejection from either host short-circuits
/// (no second opinion shopping — the gate must refuse); only transport
/// trouble falls through. `Err` = both transports failed (caller: 502).
pub async fn broadcast_tx_hex_gated(
    taal_api_key: Option<&str>,
    tx_hex: &str,
) -> Result<ArcOutcome, String> {
    let taal_err = match post_arc_tx(WorkerArcBroadcaster::ARC_URL, taal_api_key, tx_hex).await {
        Ok(outcome) => return Ok(outcome),
        Err(e) => e,
    };
    worker::console_log!("broadcast-gated: TAAL transport trouble ({taal_err}); trying GorillaPool");
    match post_arc_tx(GORILLAPOOL_ARC_URL, None, tx_hex).await {
        Ok(outcome) => Ok(outcome),
        Err(gp_err) => Err(format!("taal: {taal_err}; gorillapool: {gp_err}")),
    }
}

#[async_trait(?Send)]
impl ArcBroadcaster for WorkerArcBroadcaster {
    async fn broadcast(&self, raw_tx_hex: &str) -> Result<String, String> {
        // Same wire + verdict as the gated path (arc_verdict) — one dialect.
        match post_arc_tx(Self::ARC_URL, Some(&self.api_key), raw_tx_hex).await? {
            ArcOutcome::Accepted(txid) => Ok(txid),
            ArcOutcome::Rejected(reason) => Err(format!("ARC broadcast rejected: {reason}")),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, reason = "test code")]
mod tests {
    use super::*;

    #[test]
    fn verdict_accepts_2xx_ok_status() {
        let body = r#"{"txid":"ab","txStatus":"SEEN_ON_NETWORK","extraInfo":""}"#;
        assert_eq!(arc_verdict(200, body).unwrap(), ArcOutcome::Accepted("ab".into()));
    }

    #[test]
    fn verdict_rejects_200_with_error_status() {
        for s in ["REJECTED", "DOUBLE_SPEND_ATTEMPTED", "INVALID", "MALFORMED"] {
            let body = format!(r#"{{"txid":"ab","txStatus":"{s}","extraInfo":""}}"#);
            assert!(
                matches!(arc_verdict(200, &body).unwrap(), ArcOutcome::Rejected(_)),
                "{s} must classify as Rejected"
            );
        }
    }

    #[test]
    fn verdict_rejects_orphan_extra_info() {
        let body = r#"{"txid":"ab","txStatus":"SEEN_ON_NETWORK","extraInfo":"tx is an ORPHAN"}"#;
        assert!(matches!(arc_verdict(200, body).unwrap(), ArcOutcome::Rejected(_)));
    }

    #[test]
    fn verdict_4xx_verdict_class_is_a_definitive_rejection_never_fallback() {
        // The 460–479 class: a REAL per-tx verdict — the gate must refuse,
        // not shop for a second opinion.
        let v = arc_verdict(465, r#"{"detail":"fee too low"}"#).unwrap();
        assert!(matches!(v, ArcOutcome::Rejected(_)));
        assert!(matches!(arc_verdict(460, "bad").unwrap(), ArcOutcome::Rejected(_)));
        assert!(matches!(arc_verdict(473, "policy").unwrap(), ArcOutcome::Rejected(_)));
    }

    #[test]
    fn verdict_auth_and_routing_failures_are_transport_never_a_rejection() {
        // Adversarial review 2026-07-17 finding 1 (HIGH): a rotated TAAL key
        // (401/403) or a gateway misroute (404/405) must NEVER read as "the
        // network rejected the tx" — that verdict blocks admission with no
        // fallback. Transport ⇒ the GP fallback + the client's direct path run.
        for status in [400u16, 401, 403, 404, 405, 410] {
            assert!(
                arc_verdict(status, "auth/misroute").is_err(),
                "HTTP {status} must classify as transport trouble"
            );
        }
    }

    #[test]
    fn verdict_already_known_is_success_in_any_dress() {
        // Finding 2 (HIGH): a redundant re-broadcast of a tx the network
        // already has is SUCCESS — the client's battle-tested `alreadyKnown`
        // semantics, mirrored (incl. the literal 257 node code).
        assert!(matches!(
            arc_verdict(422, "txn-already-known (code 257)").unwrap(),
            ArcOutcome::Accepted(_)
        ));
        assert!(matches!(
            arc_verdict(465, "already in block chain").unwrap(),
            ArcOutcome::Accepted(_)
        ));
        let dressed =
            r#"{"txid":"ab","txStatus":"REJECTED","extraInfo":"transaction already mined"}"#;
        assert!(matches!(arc_verdict(200, dressed).unwrap(), ArcOutcome::Accepted(_)));
        // NEGATED forms are failures, not already-known.
        assert!(arc_verdict(500, "unknown transaction").is_err());
    }

    #[test]
    fn verdict_mined_in_stale_block_is_transient_not_definitive() {
        // Finding 6: a reorged tx normally re-mines — transport, never a
        // definitive refusal that would wedge a valid settle.
        let body = r#"{"txid":"ab","txStatus":"MINED_IN_STALE_BLOCK","extraInfo":""}"#;
        assert!(arc_verdict(200, body).is_err());
    }

    #[test]
    fn verdict_5xx_and_429_are_transport_trouble() {
        assert!(arc_verdict(502, "bad gateway").is_err());
        assert!(arc_verdict(429, "slow down").is_err());
    }

    #[test]
    fn verdict_unparseable_2xx_body_is_transport_trouble() {
        assert!(arc_verdict(200, "<html>gateway junk</html>").is_err());
    }
}
