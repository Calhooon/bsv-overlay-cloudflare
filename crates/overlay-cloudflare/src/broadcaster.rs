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

/// A hex run this long is a txid / script / BEEF blob, i.e. RANDOM DATA — not
/// status text. `already_known` is applied to non-2xx ARC bodies that ECHO the
/// subject txid, and a txid is 64 chars of uniformly random hex, so an
/// all-DIGIT needle like the `257` node code occurs in it by chance (measured
/// on bsv-low's own ledger: 6 of 158 real txids contain "257" — 3.8%, ~1 in
/// 26). See bsv-low #212.
const MIN_HEX_RUN: usize = 8;

/// Is `b` a regex `\w` byte (`[A-Za-z0-9_]`)? — mirrors JS `\b` semantics.
fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Replace every run of ≥[`MIN_HEX_RUN`] hex chars with a SPACE (never "", so
/// the strip can't splice two fragments into a keyword). None of the alpha
/// needles below can survive inside hex anyway — `k`, `l`, `m`, `n`, `o`, `r`,
/// `s`, `w`, `y` are not hex digits — so this only removes random data.
fn strip_long_hex_runs(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut run = String::new();
    for c in s.chars() {
        if c.is_ascii_hexdigit() {
            run.push(c);
            continue;
        }
        if run.len() >= MIN_HEX_RUN {
            out.push(' ');
        } else {
            out.push_str(&run);
        }
        run.clear();
        out.push(c);
    }
    if run.len() >= MIN_HEX_RUN {
        out.push(' ');
    } else {
        out.push_str(&run);
    }
    out
}

/// Words that DECLARE a status code, i.e. the only tokens that may introduce a
/// bare `257`. Kept as SHORT as the true-positive corpus allows — every extra
/// marker is another way to say "already known" to a number in prose.
///
/// Dropped deliberately: `arc`/`rpc`/`status` (nothing needs them) and
/// `reject`/`rejected` — the latter are live false-positive surface, because
/// `routes.rs` wraps every refusal as `network rejected: {reason}` and one
/// reason is `{txStatus} {extraInfo}` = `REJECTED {extraInfo}`, so an extraInfo
/// merely BEGINNING with a number would put a bare `257` right after
/// "rejected". Mirrors the client's `CODE_257_MARKED` alternation exactly.
const CODE_MARKERS: &[&str] = &["returned", "error", "code"];

/// `needle` present as a whole word (JS `\b<needle>\b`). Inside a txid the
/// digits sit between hex word-chars, so a bounded match cannot fire.
fn contains_word(hay: &str, needle: &str) -> bool {
    let bytes = hay.as_bytes();
    let n = needle.len();
    let mut from = 0usize;
    while let Some(i) = hay[from..].find(needle) {
        let at = from + i;
        let before_ok = at == 0 || !is_word_byte(bytes[at - 1]);
        let end = at + n;
        let after_ok = end >= bytes.len() || !is_word_byte(bytes[end]);
        if before_ok && after_ok {
            return true;
        }
        // `needle` is ASCII, so `at + 1` is always a char boundary.
        from = at + 1;
    }
    false
}

/// Is `257` present as the already-known STATUS CODE (as opposed to an
/// incidental NUMBER in prose)? — the #212 residual. Exact mirror of the
/// client's `code257` in `bsv-low` `app/src/lib/broadcast.ts`.
///
/// 257 is the node's `txn-already-known` reject code and the only needle with
/// no alpha content — which is precisely why it is dangerous: it is also an
/// ordinary number a rejection body can quote. All three of these are REAL,
/// plausible ARC rejection shapes that a bare `\b257\b` called "already known":
///   {"detail":"Fee too low","extraInfo":"minimum expected fee is 257 sat, …"}
///   {"detail":"Unlocking scripts not valid","extraInfo":"script evaluated false at op 257"}
///   nLockTime 257 not satisfied
///
/// WHICH WAY TO FAIL — the two errors are NOT symmetric, so this is biased on
/// purpose. A FALSE POSITIVE turns a definitive network rejection into
/// `ArcOutcome::Accepted`, admitting the tx and letting the client stamp
/// `broadcast_ok` (its 0-conf credit authority) — money-visible and silent,
/// the #212 bug itself. A FALSE NEGATIVE makes a redundant re-broadcast look
/// like a failure: the caller retries an idempotent step, costing a retry and
/// nothing else. Where the evidence is ambiguous, take the false NEGATIVE.
///
/// A code appears in exactly three dresses; nothing else counts:
///  1. WHOLE FIELD — 257 is the entire value, no other word content;
///  2. QUOTED VALUE — `"257"` / `'257'`, the JSON dress of (1);
///  3. MARKER-ADJACENT — a [`CODE_MARKERS`] word immediately precedes it with
///     only 1–4 non-word chars between (`code 257`, `(code 257)`,
///     `arc error 257`, `node returned 257`).
///
/// In the prose counter-examples the preceding word is `is` / `op` /
/// `nlocktime` — never a marker, never quoted, never the whole field.
fn code_257(t: &str) -> bool {
    let bytes = t.as_bytes();
    // 1. WHOLE FIELD: JS `/^\W*257\W*$/` — trimming non-word chars off both
    //    ends leaves exactly "257".
    if t.trim_matches(|c: char| !(c.is_ascii_alphanumeric() || c == '_')) == "257" {
        return true;
    }
    // 2. QUOTED VALUE: JS `/["']257["']/`.
    if t.contains("\"257\"") || t.contains("'257'") {
        return true;
    }
    // 3. MARKER-ADJACENT: JS
    //    `/(^|[^0-9a-z_])(?:returned|error|code)[^0-9a-z_]{1,4}257([^0-9a-z_]|$)/`.
    let mut from = 0usize;
    while let Some(i) = t[from..].find("257") {
        let at = from + i;
        from = at + 1; // "257" is ASCII, so at+1 is always a char boundary.
        // Word boundaries around the digits (a longer number is not the code).
        if at > 0 && is_word_byte(bytes[at - 1]) {
            continue;
        }
        let end = at + 3;
        if end < bytes.len() && is_word_byte(bytes[end]) {
            continue;
        }
        // Walk back over 1..=4 non-word separator bytes (the regex quantifier).
        let mut j = at;
        let mut seps = 0usize;
        while j > 0 && seps < 4 && !is_word_byte(bytes[j - 1]) {
            j -= 1;
            seps += 1;
        }
        if seps == 0 {
            continue;
        }
        // Byte comparison, never `&t[..j]`: `j` can land mid-UTF-8 (a
        // continuation byte is non-word), and slicing there would PANIC.
        for marker in CODE_MARKERS {
            let m = marker.as_bytes();
            if j >= m.len() && &bytes[j - m.len()..j] == m {
                let start = j - m.len();
                if start == 0 || !is_word_byte(bytes[start - 1]) {
                    return true;
                }
            }
        }
    }
    false
}

/// "The network already has this exact tx" — a redundant re-broadcast is
/// SUCCESS, whatever HTTP dress it arrives in (mirrors the bsv-low client's
/// `alreadyKnown`, incl. the literal 257 txn-already-known node code).
/// NEGATED forms are stripped first: "unknown"/"unseen" are failures.
///
/// bsv-low #212, belt AND braces on a money path — a false positive here turns
/// a DEFINITIVE network rejection into `ArcOutcome::Accepted`, which admits the
/// tx and lets the client stamp `broadcast_ok` (its 0-conf credit authority):
///  1. long hex runs are stripped first, so an echoed txid cannot supply a
///     needle;
///  2. the numeric node code must appear as a CODE and not as a number in prose
///     ([`code_257`]) — and each of its three dresses is word-bounded, so it
///     could not fire from inside a txid even if step 1 were bypassed.
///
/// The alpha needles stay unbounded on purpose — bounding would MISS the real
/// `ARC_ALREADY_KNOWN` / `already_known` dress (`_` is a word char). `mined` is
/// the ONE exception: it is WORD-BOUNDED here to match the client's
/// `\bmined\b`. Unbounded (as this was) `MINED_IN_STALE_BLOCK` read as
/// "already known", so a non-2xx stale-block body returned `Accepted` instead
/// of the transient `Err` finding 6 requires — and any body containing
/// `undetermined` / `examined` was accepted outright. That was a real
/// TS/Rust divergence AND a false positive in the money-visible direction.
///
/// This function is a character-for-character mirror of the bsv-low client's
/// `alreadyKnown` (`app/src/lib/broadcast.ts`); the two test suites share one
/// corpus and both must agree on every case in it.
fn already_known(s: &str) -> bool {
    let stripped = strip_long_hex_runs(&s.to_lowercase());
    let t = stripped.replace("unknown", " ").replace("unseen", " ");
    t.contains("already")
        || t.contains("known")
        || contains_word(&t, "mined")
        || t.contains("seen")
        || code_257(&t)
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
    /// The node's human-readable status detail. #209: previously DISCARDED —
    /// now captured so a definitive rejection threads a structured reason a
    /// fallback can key on (rather than a bare `Arcade REJECTED <txid>`).
    ///
    /// STALE-`extraInfo` TRAP (#213) — DO NOT gate on this field. After an
    /// orphan recovers via an explicit resubmit, `GET /tx` returns a HEALTHY
    /// `txStatus` (e.g. `SEEN_MULTIPLE_NODES`) ALONGSIDE the OLD failure
    /// `extraInfo` (`PROCESSING (4): … failed to validate transaction`). Every
    /// gate/classification here reads ONLY `tx_status` ([`classify_arcade_status`]),
    /// so it is correct today; any future rule that consulted `extra_info` to
    /// refuse/concede would mis-gate a recovered, healthy transaction. This
    /// field is for REASON TEXT ONLY.
    #[serde(default)]
    extra_info: String,
}

/// Arcade's SYNCHRONOUS per-tx validation-failure body (#213). Arcade
/// validates script + fee synchronously (EF inlines each input's source
/// script and satoshis, so only UTXO *existence* is deferred), and a
/// definitive verdict lands as `HTTP 400`:
///
/// ```json
/// {"error":"transaction failed validation",
///  "reason":"TX_INVALID (31): … -> UNKNOWN (0): insufficient-fee"}
/// ```
///
/// We key on the STRUCTURED `error` field value (never a substring of prose —
/// this repo has been bitten twice by free-text matching on a money path,
/// #210/#212). The `reason` is version-brittle node wording, captured for the
/// human-readable message only, never matched on.
#[derive(Debug, serde::Deserialize)]
struct ArcadeSubmitError {
    #[serde(default)]
    error: String,
    #[serde(default)]
    reason: String,
}

/// The exact value of Arcade's `error` field for a definitive per-tx
/// validation failure. A whole-FIELD equality (not a prose substring).
const ARCADE_VALIDATION_FAILED_ERROR: &str = "transaction failed validation";

/// Classified outcome of ONE Arcade EF submit POST (`POST /tx` | `POST /txs`).
#[derive(Debug, PartialEq, Eq)]
enum SubmitOutcome {
    /// 2xx accept-for-processing — carries the response body for status parse.
    Processing(String),
    /// A SYNCHRONOUS, DEFINITIVE per-tx rejection (#213): `HTTP 400` carrying
    /// the structured `{"error":"transaction failed validation", …}` body.
    /// Script/fee already failed — a resubmit cannot change it; admit nothing.
    SyncRejected(String),
    /// TRANSPORT trouble — 5xx, auth (401/403), gateway misroute (404/405),
    /// rate-limit (429), timeouts, an unrecognised 400. The caller falls back.
    Transport(String),
}

/// One gate step's outcome (submit + SEEN-gate of a single body).
#[derive(Debug, PartialEq, Eq)]
enum GateStep {
    /// Subject reached `SEEN_ON_NETWORK` (or better) — admit may proceed.
    Accepted,
    /// Synchronous definitive per-tx rejection ([`SubmitOutcome::SyncRejected`]).
    SyncRejected(String),
    /// The 202-then-async-`REJECTED` shape (#211): submit was ACCEPTED for
    /// processing (2xx) but the subject never became SEEN and went to a fatal
    /// status. This is AMBIGUOUS — a missing parent and a genuine double-spend
    /// are character-identical here — so the caller RESUBMITS (waiting is
    /// proven useless) rather than concluding "missing parent".
    AsyncRejected(String),
}

/// Classify one Arcade submit HTTP response (#213). PURE — unit-tested.
fn classify_submit_response(status: u16, body: &str) -> SubmitOutcome {
    if (200..300).contains(&status) {
        return SubmitOutcome::Processing(body.to_string());
    }
    // #213: a SYNCHRONOUS HTTP 400 carrying the structured validation-failure
    // body is a DEFINITIVE per-tx verdict — NOT transport. The old code comment
    // ("an HTTP failure is never a per-tx verdict") was empirically false: a
    // definitive refusal must return 422/admit-nothing, never fall through to a
    // re-broadcast of a tx the network already refused.
    if status == 400 {
        if let Ok(err) = serde_json::from_str::<ArcadeSubmitError>(body) {
            if err.error == ARCADE_VALIDATION_FAILED_ERROR {
                let reason = if err.reason.is_empty() {
                    err.error
                } else {
                    format!("{}: {}", err.error, err.reason)
                };
                return SubmitOutcome::SyncRejected(reason);
            }
        }
    }
    // Everything else non-2xx is transport trouble → fall back. A bare 400 that
    // is NOT the structured validation-failure shape fails SAFE this way (we
    // never fabricate a definitive rejection from an unrecognised body).
    SubmitOutcome::Transport(format!("Arcade submit HTTP {status}: {body}"))
}

/// One rung of the subject-only resubmit ladder (#211): given a gate step,
/// either RETURN a terminal outcome or RETRY (advance to the next resubmit).
/// PURE — this is the real producer of the ladder's control flow, so the
/// "resubmit fires on async REJECTED, but NOT on a synchronous rejection"
/// behaviour is unit-tested without the worker runtime.
#[derive(Debug, PartialEq, Eq)]
enum Ladder {
    /// Terminal — return this outcome now.
    Return(ArcOutcome),
    /// The 202-then-async-REJECTED shape — advance to the next resubmit.
    Retry,
}

fn ladder_step(step: GateStep, subject_txid: &str) -> Ladder {
    match step {
        GateStep::Accepted => Ladder::Return(ArcOutcome::Accepted(subject_txid.to_string())),
        // A SYNCHRONOUS validation failure (bad script / low fee) is definitive
        // — a resubmit cannot change it. Admit nothing; do NOT retry.
        GateStep::SyncRejected(r) => Ladder::Return(ArcOutcome::Rejected(r)),
        // Network did not accept — ambiguous (missing parent vs double-spend);
        // an explicit resubmit is the only recovery. Retry.
        GateStep::AsyncRejected(_) => Ladder::Retry,
    }
}

/// Concatenate an EF batch (dependency order) into a single `POST /txs` body.
fn concat_efs(efs: &[EfTx]) -> Vec<u8> {
    let mut concat = Vec::with_capacity(efs.iter().map(|e| e.ef.len()).sum());
    for e in efs {
        concat.extend_from_slice(&e.ef);
    }
    concat
}

/// Human-readable fatal reason for a status response, folding in the captured
/// `extra_info` when present (#209). Never used to GATE — reason text only.
fn arcade_fatal_reason(txid: &str, status: &str, extra_info: &str) -> String {
    if extra_info.is_empty() {
        format!("Arcade {status} {txid}")
    } else {
        format!("Arcade {status} {txid} ({extra_info})")
    }
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
    ///
    /// SUBJECT-ONLY + ADAPTIVE RESUBMIT (#209/#211). Mainnet-proven: Arcade
    /// resolves unconfirmed parents from the live network, so submitting the
    /// SUBJECT ALONE succeeds even 8+ unconfirmed ancestors deep — we no longer
    /// push the whole ancestry batch on the money path. If the subject is
    /// submitted (202) but never becomes SEEN and goes to a fatal status, that
    /// shape is AMBIGUOUS (a missing parent is character-identical to a genuine
    /// double-spend) and Arcade does NOT self-heal orphans — so we EXPLICITLY
    /// resubmit (subject again, then the full ancestry batch); waiting is proven
    /// useless. A resubmit of a real double-spend is safe: it fails identically,
    /// costing one round-trip on an already-terminal case. Because of that
    /// ambiguity we NEVER report "missing parent" — the reason stays
    /// "network did not accept; retried".
    pub async fn broadcast_efs_gated(
        &self,
        efs: &[EfTx],
        subject_txid: &str,
    ) -> Result<ArcOutcome, String> {
        if efs.is_empty() {
            worker::console_log!("[arcade] {subject_txid} already mined — skipping broadcast");
            return Ok(ArcOutcome::Accepted(subject_txid.to_string()));
        }

        // The subject's own EF is what we broadcast first (subject-only).
        let subject_ef = efs
            .iter()
            .find(|e| e.txid == subject_txid)
            .ok_or_else(|| format!("subject {subject_txid} not present in EF batch"))?;

        // ── Attempt 1: SUBJECT ONLY. Arcade sources unconfirmed parents itself.
        worker::console_log!(
            "[arcade] submitting subject-only {subject_txid} → gating on {ARCADE_GATE_STATUS}"
        );
        let step = self
            .submit_once_and_gate(&self.tx_endpoint(), &subject_ef.ef, subject_txid, 1)
            .await?;
        if let Ladder::Return(outcome) = ladder_step(step, subject_txid) {
            return Ok(outcome);
        }

        // ── Attempt 2: RESUBMIT the subject alone (waiting is proven useless;
        // Arcade needs an explicit resubmit to re-attempt orphan resolution).
        worker::console_log!("[arcade] {subject_txid} not accepted — resubmitting subject-only");
        let step = self
            .submit_once_and_gate(&self.tx_endpoint(), &subject_ef.ef, subject_txid, 1)
            .await?;
        if let Ladder::Return(outcome) = ladder_step(step, subject_txid) {
            return Ok(outcome);
        }

        // ── Attempt 3: FULL ANCESTRY BATCH — feed any parent Arcade could not
        // source from the live network. Only meaningful when ancestors exist;
        // with just the subject this repeats attempt 2, so we skip it.
        if efs.len() > 1 {
            let concat = concat_efs(efs);
            worker::console_log!(
                "[arcade] {subject_txid} still not accepted — resubmitting full batch ({} legs)",
                efs.len()
            );
            let step = self
                .submit_once_and_gate(&self.txs_endpoint(), &concat, subject_txid, efs.len())
                .await?;
            if let Ladder::Return(outcome) = ladder_step(step, subject_txid) {
                return Ok(outcome);
            }
        }

        // Exhausted the resubmit ladder — the network genuinely did not accept
        // the subject. Ambiguous with a double-spend, so DO NOT say "missing
        // parent"; admit nothing.
        Ok(ArcOutcome::Rejected(format!(
            "network did not accept {subject_txid}; retried"
        )))
    }

    /// Submit one EF body and SEEN-gate the subject: submit → (echoed-status
    /// short-circuit) → poll. Returns a [`GateStep`]; `Err` is transport
    /// trouble the caller falls back on. `batch_len == 1` enables the
    /// echoed-status short-circuit (a single-tx submit body carries the current
    /// txStatus; a resubmit of a known tx can come back already SEEN/MINED).
    async fn submit_once_and_gate(
        &self,
        endpoint: &str,
        body: &[u8],
        subject_txid: &str,
        batch_len: usize,
    ) -> Result<GateStep, String> {
        let submit_body = match self.submit_ef(endpoint, subject_txid, body).await {
            SubmitOutcome::Processing(b) => b,
            SubmitOutcome::SyncRejected(r) => return Ok(GateStep::SyncRejected(r)),
            SubmitOutcome::Transport(e) => return Err(e),
        };

        // A single submit echoes the current status; a resubmit of a known tx
        // can come back already SEEN/MINED, satisfying the gate without a poll.
        if batch_len == 1 {
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
                        return Ok(GateStep::Accepted);
                    }
                    GateVerdict::Fatal => {
                        return Ok(GateStep::AsyncRejected(arcade_fatal_reason(
                            subject_txid,
                            &parsed.tx_status,
                            &parsed.extra_info,
                        )));
                    }
                    GateVerdict::Pending => {}
                }
            }
        }

        match self.poll_for_status(subject_txid).await? {
            ArcOutcome::Accepted(_) => Ok(GateStep::Accepted),
            ArcOutcome::Rejected(r) => Ok(GateStep::AsyncRejected(r)),
        }
    }

    /// POST the EF body to `endpoint` (callback headers set) and CLASSIFY the
    /// response (#213): a synchronous HTTP 400 validation-failure is a
    /// definitive per-tx rejection ([`SubmitOutcome::SyncRejected`]), NOT
    /// transport. Genuine transport failures (5xx, auth, misroute, 429,
    /// timeouts, connection errors) stay [`SubmitOutcome::Transport`].
    async fn submit_ef(&self, endpoint: &str, token: &str, body: &[u8]) -> SubmitOutcome {
        match self.post_ef_raw(endpoint, token, body).await {
            Ok((status, text)) => classify_submit_response(status, &text),
            Err(transport) => SubmitOutcome::Transport(transport),
        }
    }

    /// POST the EF body to `endpoint`, registering the callback headers, and
    /// return `(http_status, body)`. `Err` only for a genuine fetch/transport
    /// failure (connection refused, DNS, etc.) — the HTTP status is handed back
    /// verbatim for the caller to classify.
    async fn post_ef_raw(
        &self,
        endpoint: &str,
        token: &str,
        body: &[u8],
    ) -> Result<(u16, String), String> {
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
        Ok((status, text))
    }

    /// Best-effort EF submit for the engine's generic-broadcast slot (non-money
    /// `CurrentTx`). 2xx accept-for-processing → the body; anything else → `Err`
    /// (the engine treats it as non-fatal). The money path uses
    /// [`submit_once_and_gate`](Self::submit_once_and_gate).
    async fn post_ef(&self, endpoint: &str, token: &str, body: &[u8]) -> Result<String, String> {
        let (status, text) = self.post_ef_raw(endpoint, token, body).await?;
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
            if let Some(resp) = self.tx_status(txid).await {
                // GATE on `tx_status` ONLY — never `extra_info` (stale-extraInfo
                // trap, #213: a recovered orphan returns a healthy status with
                // the OLD failure extraInfo still attached).
                match classify_arcade_status(&resp.tx_status, ARCADE_GATE_STATUS) {
                    GateVerdict::Reached => {
                        worker::console_log!("[arcade] {txid} reached {}", resp.tx_status);
                        return Ok(ArcOutcome::Accepted(txid.to_string()));
                    }
                    GateVerdict::Fatal => {
                        // #209: fold the captured extra_info into the reason text
                        // (reason ONLY — the gate above already decided on status).
                        return Ok(ArcOutcome::Rejected(arcade_fatal_reason(
                            txid,
                            &resp.tx_status,
                            &resp.extra_info,
                        )));
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

    /// `GET /tx/{txid}` → the parsed status response if Arcade knows the txid
    /// (non-empty `txStatus`), else `None`. Carries `extra_info` for reason
    /// text — see the [`ArcadeStatusResponse`] stale-extraInfo trap note.
    async fn tx_status(&self, txid: &str) -> Option<ArcadeStatusResponse> {
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
            Some(parsed)
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

    // ── #213: SYNCHRONOUS submit classification ─────────────────────────────
    //
    // These feed `classify_submit_response` the EXACT bodies Arcade returned on
    // mainnet (issue #213, real proof txids). routes.rs maps `SyncRejected` →
    // HTTP 422 (admit nothing) and `Transport` → HTTP 502 (fall back).

    /// The real HTTP 400 bodies Arcade returns SYNCHRONOUSLY for a definitive
    /// per-tx verdict (script + fee validate synchronously because EF inlines
    /// each input's source). Proof txids in #213.
    const ARCADE_SYNC_400_LOW_FEE: &str = r#"{"error":"transaction failed validation","reason":"TX_INVALID (31): GoBDK fail to ValidateTransaction -> TX_POLICY (39): transaction fee is too low -> UNKNOWN (0): insufficient-fee"}"#;
    const ARCADE_SYNC_400_BAD_SIG: &str = r#"{"error":"transaction failed validation","reason":"TX_INVALID (31): GoBDK fail to ValidateTransaction -> UNKNOWN (0): Script failed an OP_EQUALVERIFY operation"}"#;

    #[test]
    fn submit_sync_400_validation_failure_is_a_definitive_rejection() {
        // #213: the load-bearing fix — a synchronous 400 with the structured
        // {"error":"transaction failed validation",…} body is a DEFINITIVE
        // per-tx verdict (→ 422), never transport (→ 502 → the client
        // re-broadcasts a tx the network already refused).
        for body in [ARCADE_SYNC_400_LOW_FEE, ARCADE_SYNC_400_BAD_SIG] {
            match classify_submit_response(400, body) {
                SubmitOutcome::SyncRejected(reason) => {
                    // The structured `reason` is threaded through for the caller.
                    assert!(reason.contains("transaction failed validation"), "{reason}");
                }
                other => panic!("sync 400 must be SyncRejected, got {other:?}"),
            }
        }
    }

    #[test]
    fn submit_transport_failures_stay_transport_never_a_rejection() {
        // 5xx, auth (401/403), gateway misroute (404/405), rate-limit (429) and
        // an UNRECOGNISED 400 all fail SAFE to Transport — the caller falls back
        // (502) and we NEVER fabricate a definitive rejection from a body that
        // isn't the structured validation-failure shape.
        for (status, body) in [
            (500u16, "upstream boom"),
            (502, "bad gateway"),
            (503, "unavailable"),
            (401, "unauthorized"),
            (403, "forbidden"),
            (404, "not found"),
            (429, "slow down"),
            // A 400 that is NOT the {error:"transaction failed validation"} shape.
            (400, r#"{"error":"bad request","message":"missing header"}"#),
            (400, "plain text bad request"),
        ] {
            assert!(
                matches!(classify_submit_response(status, body), SubmitOutcome::Transport(_)),
                "HTTP {status} must be Transport"
            );
        }
    }

    #[test]
    fn submit_2xx_is_processing_for_the_gate() {
        let body = r#"{"txid":"ab","txStatus":"RECEIVED"}"#;
        assert!(matches!(
            classify_submit_response(202, body),
            SubmitOutcome::Processing(_)
        ));
    }

    // ── #211: the resubmit ladder (control flow, real producer) ─────────────

    #[test]
    fn ladder_retries_on_async_reject_but_not_on_sync_reject() {
        // The async-REJECTED shape (202 then never-SEEN → fatal status) is the
        // ONLY step that fires a resubmit; a synchronous validation failure is
        // definitive and terminates immediately.
        assert_eq!(
            ladder_step(GateStep::AsyncRejected("Arcade REJECTED ab".into()), "ab"),
            Ladder::Retry,
            "async REJECTED must resubmit"
        );
        assert_eq!(
            ladder_step(GateStep::SyncRejected("insufficient-fee".into()), "ab"),
            Ladder::Return(ArcOutcome::Rejected("insufficient-fee".into())),
            "sync rejection must NOT resubmit (definitive)"
        );
        assert_eq!(
            ladder_step(GateStep::Accepted, "ab"),
            Ladder::Return(ArcOutcome::Accepted("ab".into())),
        );
    }

    #[test]
    fn concat_efs_is_dependency_order_concatenation() {
        // Subject-only vs full-batch bodies: attempt 1 submits ONE tx (the
        // subject's EF); the fallback batch is the concatenation of all legs.
        let efs = vec![
            EfTx { txid: "parent".into(), ef: vec![1, 2, 3] },
            EfTx { txid: "subject".into(), ef: vec![4, 5] },
        ];
        let subject_only = &efs.iter().find(|e| e.txid == "subject").unwrap().ef;
        assert_eq!(subject_only.len(), 2, "subject-only body is the subject EF");
        let batch = concat_efs(&efs);
        assert_eq!(batch, vec![1, 2, 3, 4, 5], "batch concatenates in order");
        assert_eq!(batch.len(), 5);
    }

    #[test]
    fn arcade_fatal_reason_folds_in_extra_info_when_present() {
        // #209: the captured extraInfo is threaded into the reason text.
        assert_eq!(arcade_fatal_reason("ab", "REJECTED", ""), "Arcade REJECTED ab");
        assert_eq!(
            arcade_fatal_reason("ab", "REJECTED", "PROCESSING (4): failed to validate"),
            "Arcade REJECTED ab (PROCESSING (4): failed to validate)"
        );
    }

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

    // ── bsv-low #212: a rejection body echoing the txid is NOT already-known ──

    /// REAL txids from bsv-low's `docs/DECISION-LOG-spite-relay-2026-07.md`
    /// that happen to contain the digits "257" — the collisions that made the
    /// old substring test a ~1-in-26 lottery on every rejected money broadcast.
    const REAL_LEDGER_TXIDS_CONTAINING_257: &[&str] = &[
        "2c50a257da80421f8a31c98bedc728b19e437edff0e2e84b74278f4b20d82256",
        "66cf740bef1e10b549e652cf049ee0257fe2830c733c3aa09d554df73ed6ecab",
        "03925754b46492ca4e9d9072e399d73f0c66479d314ef83a3a5723a3424047b0",
    ];

    #[test]
    fn already_known_never_fires_on_a_real_txid_that_contains_257() {
        for txid in REAL_LEDGER_TXIDS_CONTAINING_257 {
            assert_eq!(txid.len(), 64);
            assert!(txid.contains("257"), "{txid} must exercise the hazard");
            assert!(!already_known(txid), "bare txid {txid}");
            // The REAL producer shapes this function is fed:
            // `broadcaster.rs` Arcade fatal reason …
            for status in ["REJECTED", "DOUBLE_SPEND_ATTEMPTED"] {
                assert!(!already_known(&format!("Arcade {status} {txid}")), "{txid}");
            }
            // … and `routes.rs::json_error("network rejected: {reason}", 422)`,
            // the body the bsv-low client reads back as `gated.detail`.
            let body = format!(
                r#"{{"status":"error","message":"network rejected: Arcade REJECTED {txid}"}}"#
            );
            assert!(!already_known(&body), "{body}");
        }
    }

    #[test]
    fn verdict_460_class_body_echoing_a_257_txid_stays_rejected() {
        // The load-bearing one: ARC's own 461/465 bodies ECHO the txid, and
        // `arc_verdict` runs `already_known(body)` BEFORE the 460–479 verdict
        // class. A false positive there returns `Accepted` — admitting a tx
        // the network definitively refused.
        for txid in REAL_LEDGER_TXIDS_CONTAINING_257 {
            for status in [460u16, 461, 465, 473] {
                let body = format!(
                    r#"{{"detail":"Transaction is not valid","status":{status},"title":"Unlocking scripts not valid","txid":"{txid}"}}"#
                );
                match arc_verdict(status, &body).unwrap() {
                    ArcOutcome::Rejected(_) => {}
                    other => panic!("HTTP {status} {txid} must stay Rejected, got {other:?}"),
                }
            }
        }
    }

    #[test]
    fn verdict_2xx_error_dress_with_a_257_txid_stays_rejected() {
        for txid in REAL_LEDGER_TXIDS_CONTAINING_257 {
            let body = format!(
                r#"{{"txid":"{txid}","txStatus":"REJECTED","extraInfo":"fee too low for {txid}"}}"#
            );
            match arc_verdict(200, &body).unwrap() {
                ArcOutcome::Rejected(_) => {}
                other => panic!("{txid} must stay Rejected, got {other:?}"),
            }
        }
    }

    #[test]
    fn already_known_property_no_random_txid_ever_matches() {
        // Deterministic LCG (Numerical Recipes), HIGH bits only — an LCG mod
        // 2^32 has near-degenerate low bits, and `% 16` yields a vacuous corpus
        // with ZERO "257" collisions. Seeded, never random: a property cell
        // that can flake is a bug.
        let mut s: u32 = 0x0212_c0de;
        let mut next = || {
            s = s.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            s
        };
        let hex = b"0123456789abcdef";
        let mut collisions = 0usize;
        for _ in 0..2000 {
            let txid: String = (0..64)
                .map(|_| hex[(next() >> 28) as usize] as char)
                .collect();
            if txid.contains("257") {
                collisions += 1;
            }
            let body = format!(
                r#"{{"status":"error","message":"network rejected: Arcade REJECTED {txid}"}}"#
            );
            assert!(!already_known(&body), "{txid}");
            assert!(
                matches!(
                    arc_verdict(
                        461,
                        &format!(r#"{{"detail":"invalid","status":461,"txid":"{txid}"}}"#)
                    )
                    .unwrap(),
                    ArcOutcome::Rejected(_)
                ),
                "{txid}"
            );
        }
        // Guard the guard: the corpus must actually exercise the hazard.
        assert!(collisions > 5, "vacuous corpus: only {collisions} '257' hits");
    }

    #[test]
    fn already_known_true_positives_survive_the_hardening() {
        // The fix must NOT disable the feature — a redundant re-broadcast is
        // genuinely success.
        for s in [
            "txn-already-known (code 257)",
            "257: txn-already-known",
            // The bare node code, with no alpha needle to carry it — this is
            // what `contains_word("257")` alone must still catch.
            "node returned 257",
            "reject code 257.",
            "already in block chain",
            "transaction already mined",
            "ARC_ALREADY_KNOWN",
            "already_known",
            "SEEN_ON_NETWORK",
            // With the txid alongside it — the words survive the hex strip.
            r#"{"txid":"2c50a257da80421f8a31c98bedc728b19e437edff0e2e84b74278f4b20d82256","txStatus":"REJECTED","extraInfo":"transaction already mined"}"#,
        ] {
            assert!(already_known(s), "true positive lost: {s}");
        }
        // …and the 2xx already-known dress still classifies as Accepted.
        let dressed = r#"{"txid":"2c50a257da80421f8a31c98bedc728b19e437edff0e2e84b74278f4b20d82256","txStatus":"REJECTED","extraInfo":"txn-already-known (code 257)"}"#;
        assert!(matches!(
            arc_verdict(200, dressed).unwrap(),
            ArcOutcome::Accepted(_)
        ));
    }

    // ── #212 RESIDUAL: `257` as a NUMBER in prose vs `257` as a STATUS CODE ──
    //
    // The hex strip closed the txid channel, but a bare `\b257\b` still fired
    // on any standalone decimal 257 — and a rejection body quoting a fee floor,
    // a script op index or an nLockTime height is entirely plausible. Same
    // money-bug class: a false positive turns a definitive rejection into
    // `Accepted`, admitting the tx and letting the client stamp `broadcast_ok`.
    //
    // THIS CORPUS IS SHARED, verbatim, with the client mirror
    // (`bsv-low` `app/src/lib/broadcast.alreadyKnown.test.ts`, `CODE_257_TRUE`
    // / `CODE_257_PROSE_FALSE`). The two implementations must agree on every
    // entry — that equivalence is the whole point of the shared list.

    const CODE_257_TRUE: &[&str] = &[
        "txn-already-known",
        "257: txn-already-known",
        "arc error 257",
        "code 257",
        "(code 257)",
        "257", // bare — the whole field
        r#"{"txStatus":"REJECTED","extraInfo":"257"}"#,
        "already in block chain",
        "transaction already mined",
        "ARC_ALREADY_KNOWN",
        "already_known",
        "SEEN_ON_NETWORK",
        "node returned 257",
        "reject code 257.",
        "error: 257",
        r#""code": 257"#,
    ];

    /// Plausible REJECTION prose that quotes 257 as an ordinary number. Every
    /// one of these returned TRUE under the old `contains_word("257")`.
    const CODE_257_PROSE_FALSE: &[&str] = &[
        "minimum expected fee is 257 sat, got 200",
        "script evaluated false at op 257",
        "nLockTime 257 not satisfied",
        // Why `reject`/`rejected` are NOT code markers: `routes.rs` wraps every
        // refusal as `network rejected: {reason}` and `arc_verdict`'s 2xx
        // reason is `REJECTED {extraInfo}`, so an extraInfo merely BEGINNING
        // with a number would otherwise sit right after the word "rejected".
        "257 sat minimum fee required",
        // Longer numbers merely containing 257 are not the code either.
        "expected 2570 sat",
        "nLockTime 1257 not satisfied",
        "block height 257000 reached",
        "fee rate 0.257 sat/byte",
        // The marker must be a WHOLE word with 1–4 non-word chars of
        // separation: `codes` is not `code`, `code257` has no separator, and
        // >4 chars of separation falls off the quantifier. All three land on
        // the RECOVERABLE side (a retry) — the direction this rule is biased
        // toward.
        "codes 257",
        "code257",
        "error:    257",
    ];

    /// ARC's RFC7807-ish non-2xx error body (461 unlock-invalid / 465 fee
    /// floor) — the real producer shape, `txid` field and all.
    fn arc_error_body(status: u16, title: &str, extra_info: &str, txid: &str) -> String {
        format!(
            r#"{{"detail":"Transaction is not valid","status":{status},"title":"{title}","txid":"{txid}","extraInfo":"{extra_info}"}}"#
        )
    }

    /// `routes.rs::json_error(&format!("network rejected: {reason}"), 422)`.
    fn overlay_422(reason: &str) -> String {
        format!(r#"{{"status":"error","message":"network rejected: {reason}"}}"#)
    }

    #[test]
    fn code_257_true_dresses_still_read_as_already_known() {
        for s in CODE_257_TRUE {
            assert!(already_known(s), "code dress lost: {s}");
        }
    }

    #[test]
    fn code_257_as_prose_is_never_already_known() {
        let txid = REAL_LEDGER_TXIDS_CONTAINING_257[0];
        for prose in CODE_257_PROSE_FALSE {
            // 1. bare — what `arc_verdict` passes on the 2xx-error path.
            assert!(!already_known(prose), "bare: {prose}");
            // 2. ARC's own non-2xx error body, echoing the txid, and
            // 3. the overlay 422 wrapper the client reads as `gated.detail`.
            for body in [
                arc_error_body(461, "Unlocking scripts not valid", prose, txid),
                arc_error_body(465, "Fee too low", prose, txid),
            ] {
                assert!(!already_known(&body), "arc body: {body}");
                let wrapped = overlay_422(&format!("ARC HTTP 465: {body}"));
                assert!(!already_known(&wrapped), "wrapped: {wrapped}");
                // …and the verdict itself must stay a definitive rejection.
                match arc_verdict(465, &body).unwrap() {
                    ArcOutcome::Rejected(_) => {}
                    other => panic!("{body} must stay Rejected, got {other:?}"),
                }
            }
            // 4. the 2xx-error dress: `{txStatus} {extraInfo}`.
            let two_xx = format!(
                r#"{{"txid":"{txid}","txStatus":"REJECTED","extraInfo":"{prose}"}}"#
            );
            match arc_verdict(200, &two_xx).unwrap() {
                ArcOutcome::Rejected(_) => {}
                other => panic!("2xx {prose} must stay Rejected, got {other:?}"),
            }
            assert!(
                !already_known(&overlay_422(&format!("REJECTED {prose}"))),
                "overlay 2xx reason: {prose}"
            );
        }
    }

    #[test]
    fn code_257_genuine_verdict_survives_every_wrapper() {
        let known = "txn-already-known (code 257)";
        let txid = REAL_LEDGER_TXIDS_CONTAINING_257[1];
        let body = arc_error_body(465, "Fee too low", known, txid);
        assert!(already_known(&body));
        assert!(already_known(&overlay_422(&format!("ARC HTTP 465: {body}"))));
        assert!(already_known(&overlay_422(&format!("REJECTED {known}"))));
        match arc_verdict(465, &body).unwrap() {
            ArcOutcome::Accepted(_) => {}
            other => panic!("genuine already-known must be Accepted, got {other:?}"),
        }
    }

    #[test]
    fn mined_is_word_bounded_closing_the_ts_rust_divergence() {
        // Rust matched `mined` UNBOUNDED while the client matched `\bmined\b`,
        // so these disagreed. Unbounded is ALSO a false positive in the
        // money-visible direction: a non-2xx body containing
        // `MINED_IN_STALE_BLOCK` returned `Accepted` instead of the transient
        // `Err` finding 6 requires, and any body saying `undetermined` /
        // `examined` was accepted outright.
        for s in [
            "MINED_IN_STALE_BLOCK",
            "status undetermined",
            "script examined and rejected",
            r#"{"txStatus":"MINED_IN_STALE_BLOCK","extraInfo":""}"#,
        ] {
            assert!(!already_known(s), "substring 'mined' read as known: {s}");
        }
        for s in ["MINED", "transaction already mined", "tx was mined in block"] {
            assert!(already_known(s), "real mined dress lost: {s}");
        }
        // The classification consequence: a non-2xx stale-block body is
        // TRANSPORT trouble, never an acceptance.
        assert!(arc_verdict(503, "MINED_IN_STALE_BLOCK, retry").is_err());
    }

    #[test]
    fn already_known_negation_still_holds_with_a_txid_present() {
        for s in [
            "unknown transaction",
            "UNKNOWN",
            "tx unseen by the network",
            "unknown transaction 66cf740bef1e10b549e652cf049ee0257fe2830c733c3aa09d554df73ed6ecab",
            r#"{"status":"error","message":"network rejected: ARC HTTP 404: unknown tx 03925754b46492ca4e9d9072e399d73f0c66479d314ef83a3a5723a3424047b0"}"#,
        ] {
            assert!(!already_known(s), "negated form read as known: {s}");
        }
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
