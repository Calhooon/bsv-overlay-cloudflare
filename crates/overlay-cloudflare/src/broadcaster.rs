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

#[async_trait(?Send)]
impl ArcBroadcaster for WorkerArcBroadcaster {
    async fn broadcast(&self, raw_tx_hex: &str) -> Result<String, String> {
        let url = format!("{}/v1/tx", Self::ARC_URL);

        let body = serde_json::json!({ "rawTx": raw_tx_hex });
        let body_str = body.to_string();

        // Build the request
        let mut init = worker::RequestInit::new();
        init.with_method(worker::Method::Post);

        let headers = worker::Headers::new();
        let _ = headers.set("Content-Type", "application/json");
        let _ = headers.set("Authorization", &format!("Bearer {}", self.api_key));
        init.with_headers(headers);

        // Set JSON body
        init.with_body(Some(worker::wasm_bindgen::JsValue::from_str(&body_str)));

        let request = worker::Request::new_with_init(&url, &init)
            .map_err(|e| format!("Failed to create ARC request: {e}"))?;

        let mut response = worker::Fetch::Request(request)
            .send()
            .await
            .map_err(|e| format!("ARC fetch failed: {e}"))?;

        let status = response.status_code();
        let response_text = response
            .text()
            .await
            .unwrap_or_else(|_| String::from("<no body>"));

        if !(200..300).contains(&status) {
            return Err(format!("ARC returned HTTP {status}: {response_text}"));
        }

        // Parse the ARC response JSON
        let arc_resp: ArcResponse = serde_json::from_str(&response_text)
            .map_err(|e| format!("Failed to parse ARC response: {e} — body: {response_text}"))?;

        // Check for error statuses that ARC returns with HTTP 200
        let error_statuses = [
            "DOUBLE_SPEND_ATTEMPTED",
            "REJECTED",
            "INVALID",
            "MALFORMED",
            "MINED_IN_STALE_BLOCK",
        ];
        let upper_status = arc_resp.tx_status.to_uppercase();
        let is_orphan = arc_resp.extra_info.to_uppercase().contains("ORPHAN")
            || upper_status.contains("ORPHAN");

        if error_statuses.iter().any(|s| upper_status == *s) || is_orphan {
            return Err(format!(
                "ARC broadcast rejected: {} {}",
                arc_resp.tx_status, arc_resp.extra_info
            )
            .trim()
            .to_string());
        }

        Ok(arc_resp.txid)
    }
}
