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

// ============================================================================
// Arcade V2 broadcaster — the overlay's SOLE network broadcaster (#192/#193)
// ============================================================================
//
// Owner decision (2026-07-19): the overlay broadcasts through Arcade V2
// (`arcade-v2-us-1.bsvblockchain.tech`), not TAAL ARC, because an Arcade submit
// propagates to the whole mainnet AND Arcade delivers the merkle proof for free
// in its MINED callback. Arcade is EF-only (`Arcade never reads BEEF`) and
// asynchronous: `POST /tx` (single) / `POST /txs` (batch) returns 202, and the
// verdict lands later. We gate admission on `SEEN_ON_NETWORK` by polling
// `GET /tx/{txid}` (bounded), and register `X-CallbackUrl` (→ our /arc-ingest),
// `X-CallbackToken`, `X-FullStatusUpdates:true` so a later MINED status pushes
// the free merkle path back for proof completion (the PRIMARY proof source).
//
// Ported/adapted from `~/bsv/btc-relay-rs/src/broadcast.rs` (arcade_broadcast /
// arcade_tx_status) + `~/bsv/zanaadu/overlay/src/broadcaster.rs`
// (ArcadeBroadcaster). This uses bounded POLLING (worker setTimeout) rather
// than an SSE stream so it stays wasm-clean with no extra deps.

use crate::ef::{beef_to_ef_batch, EfTx};

/// Default live Arcade V2 mainnet endpoint (overridable via `ARCADE_URL`).
pub const ARCADE_DEFAULT_URL: &str = "https://arcade-v2-us-1.bsvblockchain.tech";

/// Gate admission on this status (or better). `SEEN_ON_NETWORK` lands ~3s after
/// submit and is reliable; `SEEN_MULTIPLE_NODES` is erratic so we do NOT gate on
/// it (btc-relay-rs arcade-v2-integration.md §4).
const ARCADE_GATE_STATUS: &str = "SEEN_ON_NETWORK";

/// Arcade statuses that are hard rejects — never wait these out, never admit.
const ARCADE_FATAL_STATUSES: &[&str] = &["REJECTED", "DOUBLE_SPEND_ATTEMPTED"];

/// Give up waiting for propagation after this long — the tx was submitted but
/// never became demonstrably SEEN, so the caller must NOT admit it (fail-closed).
const ARCADE_WAIT_TIMEOUT_MS: u64 = 20_000;

/// Poll `GET /tx/{txid}` at this cadence while gating.
const ARCADE_POLL_INTERVAL_MS: u64 = 2_000;

/// Rank Arcade lifecycle statuses so "target or better" comparisons work.
/// Unknown statuses rank lowest (0).
fn arcade_status_rank(status: &str) -> u8 {
    match status {
        "RECEIVED" => 1,
        "STORED" => 2,
        "ANNOUNCED_TO_NETWORK" => 3,
        "REQUESTED_BY_NETWORK" => 4,
        "SENT_TO_NETWORK" => 5,
        "ACCEPTED_BY_NETWORK" => 6,
        "SEEN_ON_NETWORK" => 7,
        "SEEN_MULTIPLE_NODES" => 8,
        "MINED" => 9,
        "IMMUTABLE" => 10,
        _ => 0,
    }
}

/// Classify one Arcade status against the gate target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GateVerdict {
    /// Reached the target status (or better) → safe to admit.
    Reached,
    /// A fatal status (REJECTED / DOUBLE_SPEND_ATTEMPTED) → never admit.
    Fatal,
    /// A non-terminal status below the target → keep waiting.
    Pending,
}

fn classify_arcade_status(status: &str, target: &str) -> GateVerdict {
    if ARCADE_FATAL_STATUSES.contains(&status) {
        return GateVerdict::Fatal;
    }
    if arcade_status_rank(target) > 0 && arcade_status_rank(status) >= arcade_status_rank(target) {
        return GateVerdict::Reached;
    }
    GateVerdict::Pending
}

/// Async sleep via JS `setTimeout` (Cloudflare Workers runtime). Compiles on the
/// host for unit tests (js-sys is a normal crate); only exercised at runtime on
/// wasm — the pure classification tests never call it.
async fn sleep_ms(ms: u64) {
    use worker::js_sys;
    use worker::wasm_bindgen::prelude::*;
    use worker::wasm_bindgen::JsCast;
    use worker::wasm_bindgen_futures::JsFuture;

    let promise = js_sys::Promise::new(&mut |resolve, _reject| {
        let global = js_sys::global();
        let _ = js_sys::Reflect::get(&global, &JsValue::from_str("setTimeout")).and_then(
            |set_timeout| {
                let set_timeout = set_timeout.dyn_into::<js_sys::Function>()?;
                set_timeout.call2(&JsValue::NULL, &resolve, &JsValue::from_f64(ms as f64))
            },
        );
    });
    let _ = JsFuture::from(promise).await;
}

/// Arcade `GET /tx/{txid}` / `POST /tx` JSON envelope (single submit).
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct ArcadeStatusResponse {
    #[serde(default)]
    txid: String,
    #[serde(default)]
    tx_status: String,
}

/// Arcade V2 broadcaster (async EF, SEEN-gated, callback-registering).
///
/// Not an `ArcBroadcaster` by construction — the primary path takes the full
/// BEEF's Extended-Format legs (`broadcast_efs_gated`) and gates D1 admission
/// on the returned `ArcOutcome`. It ALSO implements `ArcBroadcaster` (best-effort
/// single-tx submit) so it can occupy the engine's generic-broadcast slot.
pub struct ArcadeBroadcaster {
    /// Base URL, e.g. `https://arcade-v2-us-1.bsvblockchain.tech` (no trailing `/tx`).
    base_url: String,
    /// `X-CallbackUrl` for the MINED webhook (our `/arc-ingest`). `None` → no
    /// callback registered (SEEN is still gated by polling).
    callback_url: Option<String>,
}

impl ArcadeBroadcaster {
    /// Create a broadcaster against `base_url` (default endpoint if empty).
    pub fn new(base_url: impl Into<String>) -> Self {
        let base_url = base_url.into();
        let base_url = if base_url.trim().is_empty() {
            ARCADE_DEFAULT_URL.to_string()
        } else {
            base_url.trim_end_matches('/').to_string()
        };
        Self {
            base_url,
            callback_url: None,
        }
    }

    /// Register the MINED webhook (`X-CallbackUrl`), typically
    /// `{HOSTING_URL}/arc-ingest`. Empty → no-op.
    #[must_use]
    pub fn with_callback(mut self, url: impl Into<String>) -> Self {
        let url = url.into();
        if !url.trim().is_empty() {
            self.callback_url = Some(url);
        }
        self
    }

    fn tx_endpoint(&self) -> String {
        format!("{}/tx", self.base_url)
    }
    fn txs_endpoint(&self) -> String {
        format!("{}/txs", self.base_url)
    }
    fn status_endpoint(&self, txid: &str) -> String {
        format!("{}/tx/{}", self.base_url, txid)
    }

    /// Convert a BEEF hex to its unproven EF legs and gate on SEEN. Convenience
    /// wrapper over [`broadcast_efs_gated`](Self::broadcast_efs_gated).
    pub async fn broadcast_beef_gated(&self, beef_hex: &str) -> Result<ArcOutcome, String> {
        let beef_bytes = hex::decode(beef_hex.trim()).map_err(|e| format!("BEEF hex: {e}"))?;
        let (efs, subject_txid) =
            beef_to_ef_batch(&beef_bytes).map_err(|e| format!("EF conversion: {e}"))?;
        self.broadcast_efs_gated(&efs, &subject_txid).await
    }

    /// Submit `efs` (unproven Extended-Format legs, dependency order) to Arcade
    /// and gate on the SUBJECT reaching `SEEN_ON_NETWORK`.
    ///
    /// Mirrors [`broadcast_tx_hex_gated`]'s `Result<ArcOutcome, String>`
    /// contract so the broadcast-gated route is a drop-in swap:
    /// - `Ok(Accepted(txid))` — the network took the subject (admit may proceed);
    /// - `Ok(Rejected(reason))` — Arcade definitively refused it (admit nothing);
    /// - `Err(transport)` — submit/gate transport trouble or never-SEEN timeout
    ///   (fail-closed: the caller falls back to its own direct broadcast).
    ///
    /// An empty `efs` (every tx already mined) is `Ok(Accepted(subject))` — a
    /// no-op success, mirroring the engine's skip-broadcast-when-mined path.
    pub async fn broadcast_efs_gated(
        &self,
        efs: &[EfTx],
        subject_txid: &str,
    ) -> Result<ArcOutcome, String> {
        if efs.is_empty() {
            worker::console_log!("[arcade] {subject_txid} already mined — skipping broadcast");
            return Ok(ArcOutcome::Accepted(subject_txid.to_string()));
        }

        let (endpoint, body): (String, Vec<u8>) = if efs.len() == 1 {
            (self.tx_endpoint(), efs[0].ef.clone())
        } else {
            let mut concat = Vec::with_capacity(efs.iter().map(|e| e.ef.len()).sum());
            for e in efs {
                concat.extend_from_slice(&e.ef);
            }
            (self.txs_endpoint(), concat)
        };

        let submit_body = self.post_ef(&endpoint, subject_txid, &body).await?;

        // A single submit echoes the current status; a resubmit of a known tx
        // can come back already SEEN/MINED, satisfying the gate without a poll.
        if efs.len() == 1 {
            if let Ok(parsed) = serde_json::from_str::<ArcadeStatusResponse>(&submit_body) {
                if !parsed.txid.is_empty() && parsed.txid != subject_txid {
                    // Never gate/admit under a mismatched identity.
                    return Err(format!(
                        "Arcade txid {} != local subject txid {subject_txid}",
                        parsed.txid
                    ));
                }
                match classify_arcade_status(&parsed.tx_status, ARCADE_GATE_STATUS) {
                    GateVerdict::Reached => {
                        worker::console_log!(
                            "[arcade] {subject_txid} accepted at {} (no poll needed)",
                            parsed.tx_status
                        );
                        return Ok(ArcOutcome::Accepted(subject_txid.to_string()));
                    }
                    GateVerdict::Fatal => {
                        return Ok(ArcOutcome::Rejected(format!(
                            "Arcade {} {subject_txid}",
                            parsed.tx_status
                        )));
                    }
                    GateVerdict::Pending => {}
                }
            }
        }

        worker::console_log!(
            "[arcade] submitted {} EF leg(s) → gating {subject_txid} on {ARCADE_GATE_STATUS}",
            efs.len()
        );
        self.poll_for_status(subject_txid).await
    }

    /// POST the EF body to `endpoint`, registering the callback headers.
    /// Returns the response body on 2xx; any non-2xx is TRANSPORT trouble
    /// (`Err`) — Arcade returns 202 for a valid-script submit and reports
    /// REJECTED asynchronously, so an HTTP failure is never a per-tx verdict.
    async fn post_ef(&self, endpoint: &str, token: &str, body: &[u8]) -> Result<String, String> {
        use worker::js_sys::Uint8Array;

        let headers = worker::Headers::new();
        let _ = headers.set("Content-Type", "application/octet-stream");
        // Subject txid doubles as the callback token — scopes the status stream
        // and (P2.5) authenticates the MINED webhook to /arc-ingest.
        let _ = headers.set("X-CallbackToken", token);
        // REQUIRED to receive the non-terminal SEEN_ON_NETWORK.
        let _ = headers.set("X-FullStatusUpdates", "true");
        if let Some(ref cb) = self.callback_url {
            let _ = headers.set("X-CallbackUrl", cb);
        }

        let mut init = worker::RequestInit::new();
        init.with_method(worker::Method::Post);
        init.with_headers(headers);
        init.with_body(Some(Uint8Array::from(body).into()));

        let request = worker::Request::new_with_init(endpoint, &init)
            .map_err(|e| format!("Failed to create Arcade request: {e}"))?;
        let mut response = worker::Fetch::Request(request)
            .send()
            .await
            .map_err(|e| format!("Arcade fetch {endpoint} failed: {e}"))?;

        let status = response.status_code();
        let text = response.text().await.unwrap_or_default();
        if !(200..300).contains(&status) {
            return Err(format!("Arcade submit HTTP {status}: {text}"));
        }
        Ok(text)
    }

    /// Poll `GET /tx/{txid}` until the subject reaches the gate (or better),
    /// hits a fatal status, or the deadline elapses. Timeout → `Err` (never
    /// admit a tx that never became SEEN).
    async fn poll_for_status(&self, txid: &str) -> Result<ArcOutcome, String> {
        let mut waited = 0u64;
        loop {
            if let Some(status) = self.tx_status(txid).await {
                match classify_arcade_status(&status, ARCADE_GATE_STATUS) {
                    GateVerdict::Reached => {
                        worker::console_log!("[arcade] {txid} reached {status}");
                        return Ok(ArcOutcome::Accepted(txid.to_string()));
                    }
                    GateVerdict::Fatal => {
                        return Ok(ArcOutcome::Rejected(format!("Arcade {status} {txid}")));
                    }
                    GateVerdict::Pending => {}
                }
            }
            if waited >= ARCADE_WAIT_TIMEOUT_MS {
                return Err(format!(
                    "Arcade {txid} never reached {ARCADE_GATE_STATUS} within {}s — do not admit",
                    ARCADE_WAIT_TIMEOUT_MS / 1000
                ));
            }
            sleep_ms(ARCADE_POLL_INTERVAL_MS).await;
            waited += ARCADE_POLL_INTERVAL_MS;
        }
    }

    /// `GET /tx/{txid}` → `Some(txStatus)` if Arcade knows the txid, else `None`.
    async fn tx_status(&self, txid: &str) -> Option<String> {
        let url = self.status_endpoint(txid);
        let mut init = worker::RequestInit::new();
        init.with_method(worker::Method::Get);
        let request = worker::Request::new_with_init(&url, &init).ok()?;
        let mut response = worker::Fetch::Request(request).send().await.ok()?;
        if !(200..300).contains(&response.status_code()) {
            return None;
        }
        let text = response.text().await.ok()?;
        let parsed: ArcadeStatusResponse = serde_json::from_str(&text).ok()?;
        if parsed.tx_status.is_empty() {
            None
        } else {
            Some(parsed.tx_status)
        }
    }
}

#[async_trait(?Send)]
impl ArcBroadcaster for ArcadeBroadcaster {
    /// Engine generic-broadcast slot (non-money `CurrentTx` submits). Arcade is
    /// EF-only, so a bare raw tx is submitted best-effort and this returns the
    /// content-addressed txid on a 2xx accept-for-processing; the engine treats
    /// any error here as non-fatal. The money path uses `broadcast_efs_gated`.
    async fn broadcast(&self, raw_tx_hex: &str) -> Result<String, String> {
        let bytes = hex::decode(raw_tx_hex.trim()).map_err(|e| format!("raw tx hex: {e}"))?;
        let txid = bsv_rs::transaction::Transaction::from_hex(raw_tx_hex.trim())
            .map_err(|e| format!("parse raw tx: {e}"))?
            .id();
        // 2xx accept-for-processing is success for the engine's non-fatal path.
        let _ = self.post_ef(&self.tx_endpoint(), &txid, &bytes).await?;
        Ok(txid)
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

    // ── Arcade V2 broadcaster ────────────────────────────────────────────────

    #[test]
    fn arcade_status_rank_is_monotonic() {
        let ladder = [
            "RECEIVED",
            "STORED",
            "ANNOUNCED_TO_NETWORK",
            "REQUESTED_BY_NETWORK",
            "SENT_TO_NETWORK",
            "ACCEPTED_BY_NETWORK",
            "SEEN_ON_NETWORK",
            "SEEN_MULTIPLE_NODES",
            "MINED",
            "IMMUTABLE",
        ];
        for pair in ladder.windows(2) {
            assert!(
                arcade_status_rank(pair[0]) < arcade_status_rank(pair[1]),
                "{} should rank below {}",
                pair[0],
                pair[1]
            );
        }
        assert_eq!(arcade_status_rank("WAT"), 0, "unknown ranks lowest");
    }

    #[test]
    fn arcade_classify_gates_on_seen_and_above() {
        assert_eq!(
            classify_arcade_status("ACCEPTED_BY_NETWORK", ARCADE_GATE_STATUS),
            GateVerdict::Pending
        );
        assert_eq!(
            classify_arcade_status("SEEN_ON_NETWORK", ARCADE_GATE_STATUS),
            GateVerdict::Reached
        );
        assert_eq!(
            classify_arcade_status("MINED", ARCADE_GATE_STATUS),
            GateVerdict::Reached
        );
    }

    #[test]
    fn arcade_classify_rejects_and_double_spend_are_fatal() {
        assert_eq!(
            classify_arcade_status("REJECTED", ARCADE_GATE_STATUS),
            GateVerdict::Fatal
        );
        assert_eq!(
            classify_arcade_status("DOUBLE_SPEND_ATTEMPTED", ARCADE_GATE_STATUS),
            GateVerdict::Fatal
        );
    }

    #[test]
    fn arcade_new_normalizes_url_and_defaults_when_empty() {
        assert_eq!(
            ArcadeBroadcaster::new("https://host.example/").tx_endpoint(),
            "https://host.example/tx"
        );
        assert_eq!(
            ArcadeBroadcaster::new("").tx_endpoint(),
            format!("{ARCADE_DEFAULT_URL}/tx")
        );
        let b = ArcadeBroadcaster::new("https://h.example");
        assert_eq!(b.txs_endpoint(), "https://h.example/txs");
        assert_eq!(b.status_endpoint("deadbeef"), "https://h.example/tx/deadbeef");
    }

    #[test]
    fn arcade_with_callback_ignores_empty() {
        let b = ArcadeBroadcaster::new("https://h.example").with_callback("");
        assert!(b.callback_url.is_none());
        let b = ArcadeBroadcaster::new("https://h.example")
            .with_callback("https://h.example/arc-ingest");
        assert_eq!(
            b.callback_url.as_deref(),
            Some("https://h.example/arc-ingest")
        );
    }

    #[test]
    fn arcade_submit_response_parses_received_below_gate() {
        let json = r#"{"txid":"abc123","status":202,"txStatus":"RECEIVED"}"#;
        let parsed: ArcadeStatusResponse = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.txid, "abc123");
        assert_eq!(
            classify_arcade_status(&parsed.tx_status, ARCADE_GATE_STATUS),
            GateVerdict::Pending
        );
    }
}
